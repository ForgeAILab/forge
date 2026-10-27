//! Dependencies repository tests.
use super::*;

#[tokio::test]
async fn test_add_dependency_success() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let (project_id, _repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let dependency_id = seed_task(
        &db,
        &project_id,
        Some(&agent_id),
        "todo".to_string(),
        "Dependency",
    )
    .await;
    let task_id = seed_task(&db, &project_id, None, "todo".to_string(), "Dependent").await;

    TaskDependencyRepo::add_dependency(&db, &task_id, &dependency_id, &now)
        .await
        .expect("dependency adds");

    assert_eq!(
        TaskDependencyRepo::list_dependencies(&db, &task_id)
            .await
            .expect("dependencies list"),
        vec![dependency_id.clone()]
    );
    assert_eq!(
        TaskDependencyRepo::list_dependents(&db, &dependency_id)
            .await
            .expect("dependents list"),
        vec![task_id]
    );
}

#[tokio::test]
async fn test_add_dependency_cycle_rejected() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let (project_id, _repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let first_id = seed_task(
        &db,
        &project_id,
        Some(&agent_id),
        "todo".to_string(),
        "First",
    )
    .await;
    let second_id = seed_task(&db, &project_id, None, "todo".to_string(), "Second").await;

    TaskDependencyRepo::add_dependency(&db, &second_id, &first_id, &now)
        .await
        .expect("initial dependency adds");
    let result = TaskDependencyRepo::add_dependency(&db, &first_id, &second_id, &now).await;

    assert!(matches!(result, Err(DbError::CycleDetected)));
}

/// The first child's retry needs its prerequisites before it runs again;
/// the later child cannot run until that first child completes. These
/// implicit sequence edges belong in cycle admission just like explicit ones.
#[tokio::test]
async fn dependency_cycles_include_ordered_siblings_and_reordering() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let (project_id, _, _) = seed_project_repo_agent(&db).await;
    let root = seed_task(&db, &project_id, None, "in_progress".into(), "root").await;
    let first = seed_task(
        &db,
        &project_id,
        None,
        "in_progress".into(),
        "retrying first",
    )
    .await;
    let later = seed_task(&db, &project_id, None, "todo".into(), "later").await;
    let external = seed_task(&db, &project_id, None, "todo".into(), "external").await;
    for (id, order) in [(&first, 0), (&later, 1)] {
        sqlx::query("UPDATE task SET parent_task_id=?, subtask_order=? WHERE id=?")
            .bind(&root)
            .bind(order)
            .bind(id)
            .execute(db.pool())
            .await
            .unwrap();
    }
    assert!(matches!(
        TaskDependencyRepo::add_dependency(&db, &first, &later, &now).await,
        Err(DbError::CycleDetected)
    ));
    assert!(TaskDependencyRepo::list_dependencies(&db, &first)
        .await
        .unwrap()
        .is_empty());
    TaskDependencyRepo::add_dependency(&db, &first, &external, &now)
        .await
        .unwrap();
    assert!(
        matches!(
            TaskDependencyRepo::add_dependency(&db, &external, &later, &now).await,
            Err(DbError::CycleDetected)
        ),
        "transitive dependency plus sibling order is also a cycle"
    );
    TaskDependencyRepo::remove_dependency(&db, &first, &external)
        .await
        .unwrap();
    TaskDependencyRepo::add_dependency(&db, &later, &first, &now)
        .await
        .unwrap();
    assert!(matches!(
        TaskRepo::reorder_subtasks(&db, &root, &[later.clone(), first.clone()], &now).await,
        Err(DbError::CycleDetected)
    ));
    let children = TaskRepo::list_subtasks_ordered(&db, &root).await.unwrap();
    assert_eq!(
        children.iter().map(|t| &t.id).collect::<Vec<_>>(),
        [&first, &later],
        "refused reordering rolls back"
    );
    assert!(
        matches!(
            TaskDependencyRepo::add_dependency(&db, &root, &first, &now).await,
            Err(DbError::CycleDetected)
        ),
        "planning cannot wait on a child that needs the parent to start"
    );
    TaskDependencyRepo::add_dependency(&db, &root, &external, &now)
        .await
        .unwrap();
    assert!(
        matches!(
            TaskDependencyRepo::add_dependency(&db, &external, &first, &now).await,
            Err(DbError::CycleDetected)
        ),
        "the parent-start wait also closes transitive cycles"
    );
}

/// Every legal graph is still admitted: a later sibling on an earlier one,
/// a child on a Task outside its root, a root on another root's child,
/// prerequisites that are done, and a reorder that reverses no prerequisite.
/// A Task in another Project (or a settled one) contributes no waits.
#[tokio::test]
async fn dependency_cycle_admission_accepts_every_legal_graph() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let (project_id, _, _) = seed_project_repo_agent(&db).await;
    let root = seed_task(&db, &project_id, None, "in_progress".into(), "root").await;
    let first = seed_child(&db, &project_id, &root, 0, "first").await;
    let second = seed_child(&db, &project_id, &root, 1, "second").await;
    let third = seed_child(&db, &project_id, &root, 2, "third").await;
    let other_root = seed_task(&db, &project_id, None, "in_progress".into(), "other root").await;
    let other_child = seed_child(&db, &project_id, &other_root, 0, "other child").await;
    let outside = seed_task(&db, &project_id, None, "todo".into(), "outside").await;
    let finished = seed_task(&db, &project_id, None, "done".into(), "finished").await;
    sqlx::query(
        "UPDATE task SET condition_json=json_set(condition_json,'$.kind','settled') WHERE id=?",
    )
    .bind(&finished)
    .execute(db.pool())
    .await
    .unwrap();
    for (task, prerequisite) in [
        (&third, &first),
        (&second, &first),
        (&first, &outside),
        (&root, &other_child),
        (&other_child, &outside),
        (&second, &finished),
        (&finished, &third),
    ] {
        TaskDependencyRepo::add_dependency(&db, task, prerequisite, &now)
            .await
            .unwrap_or_else(|error| panic!("{task} on {prerequisite} is legal: {error}"));
    }
    // Swapping the two later siblings reverses no prerequisite.
    TaskRepo::reorder_subtasks(
        &db,
        &root,
        &[first.clone(), third.clone(), second.clone()],
        &now,
    )
    .await
    .expect("a reorder that reverses no prerequisite is admitted");
    // Moving a prerequisite after its dependent is refused and rolls back.
    assert!(matches!(
        TaskRepo::reorder_subtasks(
            &db,
            &root,
            &[third.clone(), first.clone(), second.clone()],
            &now
        )
        .await,
        Err(DbError::CycleDetected)
    ));
    // The same Task ids in another Project are another graph.
    let (other_project, _, _) = seed_project_repo_agent(&db).await;
    let foreign_root = seed_task(&db, &other_project, None, "in_progress".into(), "foreign").await;
    let foreign_a = seed_child(&db, &other_project, &foreign_root, 0, "a").await;
    let foreign_b = seed_child(&db, &other_project, &foreign_root, 1, "b").await;
    TaskDependencyRepo::add_dependency(&db, &foreign_b, &foreign_a, &now)
        .await
        .unwrap();
    assert!(matches!(
        TaskDependencyRepo::add_dependency(&db, &foreign_a, &foreign_b, &now).await,
        Err(DbError::CycleDetected)
    ));
    assert!(!db.dependency_wait_is_cycle(&first).await.unwrap());
}

/// The evaluator runs inside the writer transaction. One root with 200
/// subtasks and 500 dependency rows: an admission, a refusal and a whole
/// reorder each run one statement and a search linear in the Project's
/// graph (each node and edge visited once), whatever the number of Tasks
/// asked about. Counted, not timed; the durations are informational.
#[tokio::test]
async fn dependency_cycle_admission_cost_on_a_large_root() {
    use crate::sqlite::task_dependency::{take_wait_graph_cost, WaitGraphCost};
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let (project_id, _, _) = seed_project_repo_agent(&db).await;
    let root = seed_task(&db, &project_id, None, "in_progress".into(), "root").await;
    let mut children = Vec::new();
    for order in 0..200 {
        children.push(seed_child(&db, &project_id, &root, order, &format!("child {order}")).await);
    }
    // 499 legal rows: each later child on up to three earlier ones.
    let mut rows = 0;
    'outer: for later in 1..children.len() {
        for back in [1, 7, 31] {
            if later >= back {
                sqlx::query(
                    "INSERT INTO task_dependency(task_id,depends_on_id,created_at) VALUES(?,?,?)",
                )
                .bind(&children[later])
                .bind(&children[later - back])
                .bind(&now)
                .execute(db.pool())
                .await
                .unwrap();
                rows += 1;
                if rows == 499 {
                    break 'outer;
                }
            }
        }
    }
    // The graph of 201 live Tasks: two nodes each; edges are the 500
    // dependency rows, two per child (parent start, parent completion),
    // one per adjacent sibling pair and one completion-to-start per Task.
    let tasks = children.len() + 1;
    let max_nodes = 2 * tasks;
    let max_edges = 500 + 2 * children.len() + (children.len() - 1) + tasks + 1;
    let linear = |what: &str, cost: WaitGraphCost, elapsed: std::time::Duration| {
        assert_eq!(
            cost.statements, 1,
            "{what}: one statement loads the graph ({elapsed:?}): {cost:?}"
        );
        assert!(
            cost.nodes <= max_nodes && cost.edges <= max_edges,
            "{what}: the graph is the Project's live Tasks only ({elapsed:?}): {cost:?}"
        );
        // The admission's own candidate edge is the one edge not loaded.
        assert!(
            cost.steps <= cost.nodes + cost.edges + 1,
            "{what}: each node and edge is visited once ({elapsed:?}): {cost:?}"
        );
    };
    take_wait_graph_cost();
    let started = std::time::Instant::now();
    TaskDependencyRepo::add_dependency(&db, &children[199], &children[0], &now)
        .await
        .unwrap();
    linear("admission", take_wait_graph_cost(), started.elapsed());
    let started = std::time::Instant::now();
    assert!(matches!(
        TaskDependencyRepo::add_dependency(&db, &children[0], &children[199], &now).await,
        Err(DbError::CycleDetected)
    ));
    linear("refusal", take_wait_graph_cost(), started.elapsed());
    let started = std::time::Instant::now();
    TaskRepo::reorder_subtasks(&db, &root, &children, &now)
        .await
        .unwrap();
    linear(
        "reorder of 200 subtasks",
        take_wait_graph_cost(),
        started.elapsed(),
    );
}

#[tokio::test]
async fn test_dependency_gate_blocks_non_context_holder() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let (project_id, _repo_id, context_agent_id) = seed_project_repo_agent(&db).await;
    let other_agent_id = new_uuid_v4();
    let other_daemon_id = seed_daemon(&db).await;
    AgentRepo::create(
        &db,
        CreateAgent {
            id: other_agent_id.clone(),
            name: "other".to_owned(),
            description: None,
            executor_type: "shell".to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            capabilities_json: "[]".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: Some(other_daemon_id),
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: None,
            visibility: "global".to_owned(),
            prompt_template: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("second agent creates");
    let dependency_id = seed_task(
        &db,
        &project_id,
        Some(&context_agent_id),
        "review".to_string(),
        "Dependency",
    )
    .await;
    let task_id = seed_task(&db, &project_id, None, "todo".to_string(), "Dependent").await;
    ExecutionRepo::create(
        &db,
        CreateExecution {
            id: new_uuid_v4(),
            task_id: dependency_id.clone(),
            agent_id: Some(context_agent_id),
            role: "executor".to_string(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("dependency execution creates");
    TaskDependencyRepo::add_dependency(&db, &task_id, &dependency_id, &now)
        .await
        .expect("dependency adds");

    let mut tx = crate::begin_immediate(db.pool())
        .await
        .expect("transaction starts");
    let execution_id = new_uuid_v4();
    let result = claim_in_test_step(
        &db,
        &mut tx,
        ClaimTask {
            task_id: task_id.clone(),
            assignee_type: "agent".to_owned(),
            assignee_id: Some(other_agent_id.clone()),
            expected_version: 1,
            source_status: "todo".to_owned(),
            target_status: "in_progress".to_owned(),
            execution_admission: None,
            expected_project_version: None,
            expected_workflow_definition: None,
            execution: CreateExecution {
                id: execution_id.clone(),
                task_id,
                agent_id: Some(other_agent_id),
                role: "executor".to_string(),
                status: ExecutionStatus::Running,
                stop_reason: None,
                stopped_by: None,
                resume_policy: None,
                stopped_at: None,
                parent_execution_id: None,
                agent_session_id: None,
                agent_message_id: None,
                last_activity_at: None,
                summary: None,
                logs_path: None,
                before_sha: None,
                after_sha: None,
                error: None,
                executor_config_snapshot_json: None,
                workspace_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
            execution_lease: pending_claim_lease(&execution_id, &now),
            claimed_at: now,
        },
    )
    .await;

    assert!(matches!(result, Err(DbError::DependencyGate)));
}

#[tokio::test]
async fn dependency_gate_finds_context_holder_behind_100_newer_attempts() {
    let db = sqlite_db().await;
    let context_at = "2026-09-11T00:00:00Z";
    let newer_at = "2026-09-12T00:00:00Z";
    let (project_id, _repo_id, context_agent_id) = seed_project_repo_agent(&db).await;
    let dependency_id = seed_task(
        &db,
        &project_id,
        Some(&context_agent_id),
        "review".to_owned(),
        "Dependency",
    )
    .await;
    let task_id = seed_task(&db, &project_id, None, "todo".to_owned(), "Dependent").await;

    ExecutionRepo::create(
        &db,
        CreateExecution {
            id: "dependency-context-holder-000".to_owned(),
            task_id: dependency_id.clone(),
            agent_id: Some(context_agent_id.clone()),
            role: "executor".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: context_at.to_owned(),
            updated_at: context_at.to_owned(),
        },
    )
    .await
    .expect("context-holder execution creates");

    for index in 1..=100 {
        ExecutionRepo::create(
            &db,
            CreateExecution {
                id: format!("dependency-newer-attempt-{index:03}"),
                task_id: dependency_id.clone(),
                agent_id: None,
                role: "executor".to_owned(),
                status: ExecutionStatus::Completed,
                stop_reason: None,
                stopped_by: None,
                resume_policy: None,
                stopped_at: None,
                parent_execution_id: None,
                agent_session_id: None,
                agent_message_id: None,
                last_activity_at: None,
                summary: None,
                logs_path: None,
                before_sha: None,
                after_sha: None,
                error: None,
                executor_config_snapshot_json: None,
                workspace_id: None,
                created_at: newer_at.to_owned(),
                updated_at: newer_at.to_owned(),
            },
        )
        .await
        .expect("newer dependency execution creates");
    }

    TaskDependencyRepo::add_dependency(&db, &task_id, &dependency_id, newer_at)
        .await
        .expect("dependency adds");

    let mut transaction = crate::begin_immediate(db.pool())
        .await
        .expect("transaction starts");
    let execution_id = "dependent-context-holder-000".to_owned();
    let result = claim_in_test_step(
        &db,
        &mut transaction,
        ClaimTask {
            task_id: task_id.clone(),
            assignee_type: "agent".to_owned(),
            assignee_id: Some(context_agent_id.clone()),
            expected_version: 1,
            source_status: "todo".to_owned(),
            target_status: "in_progress".to_owned(),
            execution_admission: None,
            expected_project_version: None,
            expected_workflow_definition: None,
            execution: CreateExecution {
                id: execution_id.clone(),
                task_id: task_id.clone(),
                agent_id: Some(context_agent_id),
                role: "executor".to_owned(),
                status: ExecutionStatus::Running,
                stop_reason: None,
                stopped_by: None,
                resume_policy: None,
                stopped_at: None,
                parent_execution_id: None,
                agent_session_id: None,
                agent_message_id: None,
                last_activity_at: None,
                summary: None,
                logs_path: None,
                before_sha: None,
                after_sha: None,
                error: None,
                executor_config_snapshot_json: None,
                workspace_id: None,
                created_at: newer_at.to_owned(),
                updated_at: newer_at.to_owned(),
            },
            execution_lease: pending_claim_lease(&execution_id, newer_at),
            claimed_at: newer_at.to_owned(),
        },
    )
    .await;

    assert!(
        result.is_ok(),
        "older context-holder execution must satisfy dependency gate"
    );
}

#[tokio::test]
async fn test_unsatisfied_dependencies_empty_when_done() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let (project_id, _repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let dependency_id = seed_task(
        &db,
        &project_id,
        Some(&agent_id),
        "done".to_string(),
        "Dependency",
    )
    .await;
    let task_id = seed_task(&db, &project_id, None, "todo".to_string(), "Dependent").await;
    TaskDependencyRepo::add_dependency(&db, &task_id, &dependency_id, &now)
        .await
        .expect("dependency adds");

    assert!(TaskDependencyRepo::unsatisfied_dependencies(&db, &task_id)
        .await
        .expect("unsatisfied dependencies list")
        .is_empty());
}

async fn seed_child(
    db: &SqliteDb,
    project_id: &str,
    root: &str,
    order: i64,
    title: &str,
) -> String {
    let id = seed_task(db, project_id, None, "todo".into(), title).await;
    sqlx::query("UPDATE task SET parent_task_id=?, subtask_order=? WHERE id=?")
        .bind(root)
        .bind(order)
        .bind(&id)
        .execute(db.pool())
        .await
        .unwrap();
    id
}
