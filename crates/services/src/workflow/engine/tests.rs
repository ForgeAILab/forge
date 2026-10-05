use std::{path::PathBuf, sync::Arc};

use api_types::{
    FailurePolicy, GateConfig, HookAudience, HookResultEntry, HookSpec, StateDefinition,
    StateHooks, StateKind, WorkflowDefinition, WorkflowTrigger, WorkflowTriggerDefinition,
};
use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, CreateProject, CreateRepo,
    CreateTask, CreateTaskRoleAssignment, ProjectRepo, RepoRepo, SqliteDb, TaskRepo,
    TaskRoleAssignmentRepo, TaskStepRepo, TransitionLogRepo, UpdateProject, WorkspaceRepo,
};
use events::{EventBus, ForgeEvent};
use serde_json::json;
use tempfile::TempDir;
use tokio::sync::broadcast;

use super::WorkflowEngine;
use crate::{
    workflow::{default_roles, default_states, default_workflow},
    ServiceError,
};

async fn sqlite_db() -> SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    SqliteDb::new(pool)
}

async fn seed_project_repo_and_task(db: &SqliteDb, task_id: &str, status: &str) {
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();

    ProjectRepo::create(
        db,
        CreateProject {
            id: project_id.clone(),
            name: "Forge".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");

    RepoRepo::create(
        db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "forge".to_owned(),
            remote_url: Some("https://example.com/forge.git".to_owned()),
            local_path: None,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo creates");
    ProjectRepo::update_at_version(
        db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        ProjectRepo::get_by_id(db, &project_id)
            .await
            .expect("fixture Project lookup")
            .expect("fixture Project exists")
            .version,
        None,
    )
    .await
    .expect("project primary repo updates");

    TaskRepo::create(
        db,
        CreateTask {
            id: task_id.to_owned(),
            project_id,
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "test task".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: status.to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("task creates");
}

async fn assign_agent_role_without_agent(db: &SqliteDb, task_id: &str, role: &str) {
    let now = now_rfc3339();
    TaskRoleAssignmentRepo::assign(
        db,
        CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: task_id.to_owned(),
            role_name: role.to_owned(),
            assignee_type: Some(db::AssigneeKind::Agent),
            assignee_id: Some("deleted-agent".to_owned()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("role assignment creates");
    sqlx::query(
        "UPDATE task_role_assignment SET assignee_id = NULL WHERE task_id = ? AND role_name = ?",
    )
    .bind(task_id)
    .bind(role)
    .execute(db.pool())
    .await
    .expect("role assignment marks deleted agent");
}

async fn assign_user_role(db: &SqliteDb, task_id: &str, role: &str) {
    let now = now_rfc3339();
    TaskRoleAssignmentRepo::assign(
        db,
        CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: task_id.to_owned(),
            role_name: role.to_owned(),
            assignee_type: Some(db::AssigneeKind::User),
            assignee_id: Some("human".to_owned()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("role assignment creates");
}

async fn assign_agent_role(db: &SqliteDb, task_id: &str, role: &str, agent_id: &str) {
    let now = now_rfc3339();
    TaskRoleAssignmentRepo::assign(
        db,
        CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: task_id.to_owned(),
            role_name: role.to_owned(),
            assignee_type: Some(db::AssigneeKind::Agent),
            assignee_id: Some(agent_id.to_owned()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("agent role assignment creates");
}

fn run_git(path: &std::path::Path, args: &[&str]) {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(path)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .output()
        .expect("git runs");
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn initialize_git_repo(path: &std::path::Path) {
    run_git(path, &["init", "--initial-branch=main"]);
    run_git(path, &["config", "user.email", "test@forge.dev"]);
    run_git(path, &["config", "user.name", "Forge Test"]);
    std::fs::write(path.join("README.md"), "# Forge\n").expect("README writes");
    run_git(path, &["add", "-A"]);
    run_git(path, &["commit", "-m", "initial commit"]);
}

fn engine(db: Arc<SqliteDb>, event_bus: Arc<EventBus>) -> crate::TaskService {
    crate::TaskService::new_for_test(db, event_bus)
}

fn hook(action: &str, on_failure: FailurePolicy) -> HookSpec {
    HookSpec {
        action: action.to_owned(),
        params: json!({}),
        applies_to: HookAudience::All,
        on_failure,
    }
}

fn state(name: &str, kind: StateKind, role: Option<&str>, hooks: StateHooks) -> StateDefinition {
    StateDefinition {
        name: name.to_owned(),
        kind,
        column: name.to_owned(),
        display_name: name.to_owned(),
        role: role.map(str::to_owned),
        hooks,
        cleanup: None,
        canonical_phase: Some(match kind {
            StateKind::Backlog => api_types::CanonicalPhase::Backlog,
            StateKind::Initial => api_types::CanonicalPhase::Ready,
            StateKind::Active => api_types::CanonicalPhase::Working,
            StateKind::Gate => api_types::CanonicalPhase::Working,
            StateKind::Terminal => api_types::CanonicalPhase::Done,
            StateKind::Custom => api_types::CanonicalPhase::Working,
        }),
        gate_config: None,
        dispatch: None,
        triggers: std::collections::BTreeMap::new(),
        config: json!({}),
    }
}

fn with_trigger(mut state: StateDefinition, trigger: WorkflowTrigger, to: &str) -> StateDefinition {
    state.triggers.insert(
        trigger,
        WorkflowTriggerDefinition {
            to: to.to_owned(),
            dispatch: None,
        },
    );
    state
}

fn user_approval_review_workflow(
    before_enter_hook: HookSpec,
    after_enter_hooks: Vec<HookSpec>,
) -> WorkflowDefinition {
    let working = with_trigger(
        state("working", StateKind::Active, None, StateHooks::default()),
        WorkflowTrigger::Accept,
        "review",
    );
    let mut review = state(
        "review",
        StateKind::Gate,
        None,
        StateHooks {
            before_enter: vec![before_enter_hook],
            after_enter: after_enter_hooks,
            ..StateHooks::default()
        },
    );
    review.gate_config = Some(GateConfig {
        reject_target: Some("working".to_owned()),
        max_rejections: Some(3),
        approve_label: Some("Approve".to_owned()),
        reject_label: Some("Reject".to_owned()),
        requires_user_approval: Some(true),
        optional_when_unassigned: Some(false),
    });
    review.triggers.insert(
        WorkflowTrigger::Accept,
        WorkflowTriggerDefinition {
            to: "done".to_owned(),
            dispatch: None,
        },
    );
    review.triggers.insert(
        WorkflowTrigger::Reject,
        WorkflowTriggerDefinition {
            to: "working".to_owned(),
            dispatch: None,
        },
    );

    WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            working,
            review,
            state("done", StateKind::Terminal, None, StateHooks::default()),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    }
}

fn phases(results: &[HookResultEntry]) -> Vec<&str> {
    results.iter().map(|entry| entry.phase.as_str()).collect()
}

fn assert_phases_are_ordered(results: &[HookResultEntry]) {
    let order = [
        "before_exit",
        "on_exit",
        "before_enter",
        "on_enter",
        "after_enter",
    ];
    let mut previous_index = 0;

    for phase in phases(results) {
        let current_index = order
            .iter()
            .position(|candidate| *candidate == phase)
            .unwrap_or_else(|| panic!("unexpected phase: {phase}"));
        assert!(
            current_index >= previous_index,
            "hook phases are out of order: {:?}",
            phases(results)
        );
        previous_index = current_index;
    }
}

async fn hook_results(db: &SqliteDb, task_id: &str) -> Vec<HookResultEntry> {
    let logs = TransitionLogRepo::list_by_task(db, task_id)
        .await
        .expect("transition logs list");
    assert!(!logs.is_empty(), "expected at least one transition log row");
    let payload = logs[0]
        .hook_results_json
        .as_deref()
        .expect("hook results are written");
    serde_json::from_str(payload).expect("hook results deserialize")
}

fn drain_events(rx: &mut broadcast::Receiver<ForgeEvent>) -> Vec<ForgeEvent> {
    let mut events = Vec::new();
    loop {
        match rx.try_recv() {
            Ok(event) => events.push(event),
            Err(broadcast::error::TryRecvError::Empty) => break,
            Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
            Err(broadcast::error::TryRecvError::Closed) => break,
        }
    }
    events
}

fn cascade_chain_workflow(steps: usize) -> WorkflowDefinition {
    let mut states = vec![with_trigger(
        state("start", StateKind::Initial, None, StateHooks::default()),
        WorkflowTrigger::Accept,
        "step_0",
    )];
    for index in 0..steps {
        let last = index + 1 == steps;
        let mut step = with_trigger(
            state(
                &format!("step_{index}"),
                StateKind::Active,
                Some(default_roles::CODER),
                StateHooks {
                    on_enter: vec![hook(
                        if last {
                            "auto_cascade_on_completion"
                        } else {
                            "auto_cascade_on_unassigned_role"
                        },
                        FailurePolicy::Log,
                    )],
                    ..StateHooks::default()
                },
            ),
            WorkflowTrigger::Accept,
            &if last {
                "done".to_owned()
            } else {
                format!("step_{}", index + 1)
            },
        );
        step.gate_config = Some(GateConfig {
            reject_target: None,
            max_rejections: None,
            approve_label: None,
            reject_label: None,
            requires_user_approval: Some(false),
            optional_when_unassigned: Some(true),
        });
        states.push(step);
    }
    states.push(state(
        "done",
        StateKind::Terminal,
        None,
        StateHooks::default(),
    ));
    WorkflowDefinition {
        roles: Vec::new(),
        states,
        configuration: Vec::new(),
        cancellation_state: None,
    }
}

async fn drain(engine: crate::TaskService, task_id: &str) -> db::Task {
    crate::worker_runtime::queue::TaskStepWorker::new(engine)
        .drain(task_id)
        .await
        .expect("cascade queue drains")
}

async fn drain_result(
    driver: crate::TaskService,
    mut result: super::TransitionResult,
) -> super::TransitionResult {
    result.task = drain(driver.clone(), &result.task.id).await;
    result.review = db::ReviewRepo::list_by_task(&*driver.db, &result.task.id)
        .await
        .unwrap()
        .into_iter()
        .max_by_key(|r| r.attempt_number);
    result.pending_steps = 0;
    result
}

#[tokio::test]
async fn long_legitimate_cascade_returns_own_commit_then_drains_without_parking() {
    let db = Arc::new(sqlite_db().await);
    let bus = Arc::new(EventBus::new(256));
    let mut events = bus.subscribe();
    let id = "long-chain";
    seed_project_repo_and_task(&db, id, "start").await;
    // 40 distinct edges, over four times the hotfix's nine-hop carry chain.
    let workflow = cascade_chain_workflow(40);
    let result = engine(db.clone(), bus.clone())
        .workflow_execution()
        .transition(
            id,
            "step_0",
            1,
            &workflow,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "start",
            false,
            Default::default(),
        )
        .await
        .unwrap();
    assert_eq!(result.task.status, "step_0");
    assert_eq!(result.pending_steps, 1);
    assert_eq!(
        TaskRepo::get_by_id(&*db, id, false)
            .await
            .unwrap()
            .unwrap()
            .status,
        "step_0"
    );
    let settled = drain(engine(db.clone(), bus), id).await;
    assert_eq!(settled.status, "done");
    assert!(settled.blocked_json.is_none());
    assert_eq!(
        TransitionLogRepo::list_by_task(&*db, id)
            .await
            .unwrap()
            .len(),
        41
    );
    assert!(db::TaskStepRepo::task_steps(&*db, id)
        .await
        .unwrap()
        .iter()
        .all(|s| s.status == "done"));
    assert!(!drain_events(&mut events)
        .iter()
        .any(|e| e.event_type == "transition.loop_detected"));
}

#[tokio::test]
async fn automatic_chain_bound_parks_and_publishes_loop_event() {
    let db = Arc::new(sqlite_db().await);
    let bus = Arc::new(EventBus::new(512));
    let mut events = bus.subscribe();
    let id = "bounded-chain";
    seed_project_repo_and_task(&db, id, "start").await;
    let workflow = cascade_chain_workflow(66);
    engine(db.clone(), bus.clone())
        .workflow_execution()
        .transition(
            id,
            "step_0",
            1,
            &workflow,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "start",
            false,
            Default::default(),
        )
        .await
        .unwrap();
    let settled = drain(engine(db.clone(), bus), id).await;
    assert_eq!(settled.status, "step_64");
    assert!(settled
        .blocked_json
        .as_deref()
        .unwrap()
        .contains("limit 64"));
    let steps = db::TaskStepRepo::task_steps(&*db, id).await.unwrap();
    assert_eq!(steps.last().unwrap().status, "parked");
    assert_eq!(steps.last().unwrap().chain_position, 65);
    assert_eq!(
        drain_events(&mut events)
            .iter()
            .filter(|e| e.event_type == "transition.loop_detected")
            .count(),
        1
    );
}

#[tokio::test]
async fn review_carry_two_refresh_rounds_complete_without_parking() {
    use db::{ReviewRepo, TaskStepRepo};
    let db = Arc::new(sqlite_db().await);
    let bus = Arc::new(EventBus::new(128));
    let id = "review-carry-chain";
    seed_project_repo_and_task(&db, id, "review").await;
    let mut merging = with_trigger(
        state("merging", StateKind::Gate, None, StateHooks::default()),
        WorkflowTrigger::Accept,
        "done",
    );
    merging.triggers.insert(
        WorkflowTrigger::Reject,
        WorkflowTriggerDefinition {
            to: "merge_failed".into(),
            dispatch: None,
        },
    );
    let workflow = WorkflowDefinition {
        roles: vec![],
        configuration: vec![],
        cancellation_state: None,
        states: vec![
            with_trigger(
                state("review", StateKind::Gate, None, StateHooks::default()),
                WorkflowTrigger::Accept,
                "merging",
            ),
            merging,
            with_trigger(
                state("merge_failed", StateKind::Gate, None, StateHooks::default()),
                WorkflowTrigger::Accept,
                "review",
            ),
            state("done", StateKind::Terminal, None, StateHooks::default()),
        ],
    };
    let driver = engine(db.clone(), bus);
    let mut parent: Option<db::TaskStep> = None;
    let mut previous_chain = None;
    // Each rebase/CI result creates a new Review, exactly the carry producer's
    // new evidence boundary. A repeated edge without that result parks.
    for (index, target) in [
        "merging",
        "merge_failed",
        "review",
        "merging",
        "merge_failed",
        "review",
        "merging",
        "done",
    ]
    .iter()
    .enumerate()
    {
        if index == 3 || index == 6 {
            let now = now_rfc3339();
            let execution_id = new_uuid_v4();
            sqlx::query("INSERT INTO execution(id,task_id,role,status,created_at,updated_at) VALUES (?,?,'coder','completed',?,?)")
                .bind(&execution_id).bind(id).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
            ReviewRepo::create(
                &*db,
                db::CreateReview {
                    id: new_uuid_v4(),
                    task_id: id.into(),
                    execution_id,
                    attempt_number: index as i64,
                    status: db::ReviewStatus::Passed,
                    step_results_json: "{}".into(),
                    started_at: now.clone(),
                    created_at: now.clone(),
                    updated_at: now,
                },
            )
            .await
            .unwrap();
        }
        let task = TaskRepo::get_by_id(&*db, id, false).await.unwrap().unwrap();
        let input = driver
            .workflow_execution()
            .cascade_step_input(
                &task,
                &workflow,
                (*target).into(),
                "review carry fixture".into(),
                Default::default(),
                false,
                false,
                None,
                parent.as_ref(),
                format!("carry:{index}"),
                None,
            )
            .await
            .unwrap();
        if index == 3 || index == 6 {
            assert_ne!(previous_chain.as_ref(), Some(&input.chain_id));
            assert_eq!(input.chain_position, 1);
        }
        previous_chain = Some(input.chain_id.clone());
        db.enqueue_step(&input).await.unwrap();
        db.ready_step(&input.id).await.unwrap();
        let settled = drain(driver.clone(), id).await;
        assert_eq!(settled.status, *target);
        assert!(settled.blocked_json.is_none());
        parent = db.task_steps(id).await.unwrap().last().cloned();
    }
    assert_eq!(
        TransitionLogRepo::list_by_task(&*db, id)
            .await
            .unwrap()
            .len(),
        8
    );
    assert!(db
        .task_steps(id)
        .await
        .unwrap()
        .iter()
        .all(|s| s.status == "done"));
}

#[tokio::test]
async fn repeated_edge_parks_instead_of_transitioning_again() {
    let db = Arc::new(sqlite_db().await);
    let bus = Arc::new(EventBus::new(64));
    let id = "loop-chain";
    seed_project_repo_and_task(&db, id, "start").await;
    let mut workflow = cascade_chain_workflow(2);
    workflow.states[2]
        .triggers
        .get_mut(&WorkflowTrigger::Accept)
        .unwrap()
        .to = "step_0".into();
    workflow.states[2].hooks.on_enter =
        vec![hook("auto_cascade_on_unassigned_role", FailurePolicy::Log)];
    engine(db.clone(), bus.clone())
        .workflow_execution()
        .transition(
            id,
            "step_0",
            1,
            &workflow,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "start",
            false,
            Default::default(),
        )
        .await
        .unwrap();
    let settled = drain(engine(db.clone(), bus), id).await;
    assert_eq!(settled.status, "step_0");
    assert!(settled.blocked_json.is_some());
    assert_eq!(
        TransitionLogRepo::list_by_task(&*db, id)
            .await
            .unwrap()
            .len(),
        3
    );
    assert_eq!(
        db::TaskStepRepo::task_steps(&*db, id)
            .await
            .unwrap()
            .last()
            .unwrap()
            .status,
        "parked"
    );
}

#[tokio::test]
async fn moved_task_supersedes_pending_cascade() {
    let db = Arc::new(sqlite_db().await);
    let bus = Arc::new(EventBus::new(64));
    let id = "superseded-chain";
    seed_project_repo_and_task(&db, id, "start").await;
    let workflow = cascade_chain_workflow(2);
    engine(db.clone(), bus.clone())
        .workflow_execution()
        .transition(
            id,
            "step_0",
            1,
            &workflow,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "start",
            false,
            Default::default(),
        )
        .await
        .unwrap();
    sqlx::query("UPDATE task SET status='done',version=version+1 WHERE id=?")
        .bind(id)
        .execute(db.pool())
        .await
        .unwrap();
    let settled = drain(engine(db.clone(), bus), id).await;
    assert_eq!(settled.status, "done");
    assert_eq!(
        TransitionLogRepo::list_by_task(&*db, id)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        db::TaskStepRepo::task_steps(&*db, id)
            .await
            .unwrap()
            .into_iter()
            .find(|step| step.kind == "hooks")
            .unwrap()
            .status,
        "superseded"
    );
}

#[tokio::test]
async fn lifecycle_ordering() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-lifecycle-ordering";
    seed_project_repo_and_task(&db, task_id, default_states::TODO).await;
    let workflow = default_workflow::default_workflow();
    let current_task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let result = engine(Arc::clone(&db), Arc::clone(&event_bus))
        .workflow_execution()
        .transition(
            task_id,
            default_states::PLANNING,
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "start work",
            false,
            Default::default(),
        )
        .await
        .expect("transition succeeds");

    let result = drain_result(engine(Arc::clone(&db), Arc::clone(&event_bus)), result).await;

    assert_eq!(result.task.status.to_string(), default_states::IN_PROGRESS);
    let results = hook_results(&db, task_id).await;
    assert_phases_are_ordered(&results);
    assert!(
        results
            .iter()
            .any(|entry| entry.action == "auto_cascade_on_unassigned_role"
                && entry.phase == "after_enter"),
        "default todo -> planning should cascade to in_progress when no planner is assigned"
    );
}

#[tokio::test]
async fn transition_rejects_stale_project_workflow_authority_without_mutation() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-stale-project-workflow";
    seed_project_repo_and_task(&db, task_id, default_states::TODO).await;
    let task_before = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let project = ProjectRepo::get_by_id(&*db, &task_before.project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    let workflow = default_workflow::default_workflow();
    ProjectRepo::update_workflow(
        &*db,
        &project.id,
        "{\"workflow\":\"w2\"}",
        None,
        project.version,
        &now_rfc3339(),
    )
    .await
    .expect("workflow update wins the race");

    let result = engine(Arc::clone(&db), event_bus)
        .workflow_execution()
        .transition_with_authority(
            task_id,
            default_states::IN_PROGRESS,
            task_before.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "stale workflow authority",
            false,
            super::WorkflowAuthority {
                project_version: project.version,
                workflow_definition: project.workflow_definition.clone(),
                clear_review_passed_at_on_commit: false,
            },
        )
        .await;
    assert!(
        matches!(result, Err(ServiceError::Db(db::DbError::VersionConflict))),
        "expected stale workflow authority conflict"
    );

    let task_after = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert_eq!(task_after.status, task_before.status);
    assert_eq!(task_after.version, task_before.version);
    assert!(
        TransitionLogRepo::list_by_task(&*db, task_id)
            .await
            .expect("transition logs load")
            .is_empty(),
        "stale transition must not append a state-change log"
    );
    let running_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM execution WHERE task_id = ? AND status = 'running'",
    )
    .bind(task_id)
    .fetch_one(db.pool())
    .await
    .expect("execution count loads");
    assert_eq!(running_count, 0, "stale transition must not dispatch work");
}

#[tokio::test]
async fn retry_entry_barrier_rejects_stale_project_workflow_authority_without_mutation() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-stale-barrier-workflow";
    seed_project_repo_and_task(&db, task_id, default_states::TODO).await;
    let task_before = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let project = ProjectRepo::get_by_id(&*db, &task_before.project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    let blocked = TaskRepo::set_entry_barrier(
        &*db,
        task_id,
        task_before.version,
        Some(
            serde_json::json!({
                "state": default_states::TODO,
                "status": "blocked",
                "started_at": now_rfc3339(),
            })
            .to_string(),
        ),
        &now_rfc3339(),
    )
    .await
    .expect("entry barrier is seeded");
    ProjectRepo::update_workflow(
        &*db,
        &project.id,
        "{\"workflow\":\"w2\"}",
        None,
        project.version,
        &now_rfc3339(),
    )
    .await
    .expect("workflow update wins the race");

    let workflow = default_workflow::default_workflow();
    let result = engine(Arc::clone(&db), event_bus)
        .workflow_execution()
        .retry_entry_barrier_with_authority(
            task_id,
            blocked.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "stale workflow authority",
            super::WorkflowAuthority {
                project_version: project.version,
                workflow_definition: project.workflow_definition.clone(),
                clear_review_passed_at_on_commit: false,
            },
        )
        .await;
    assert!(
        matches!(result, Err(ServiceError::Db(db::DbError::VersionConflict))),
        "expected stale workflow authority conflict"
    );

    let task_after = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert_eq!(task_after.version, blocked.version);
    assert_eq!(task_after.entry_barrier_json, blocked.entry_barrier_json);
}

#[tokio::test]
async fn reset_to_initial_rejects_stale_project_workflow_authority_without_mutation() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-stale-reset-workflow";
    seed_project_repo_and_task(&db, task_id, default_states::TODO).await;
    let task_before = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let project = ProjectRepo::get_by_id(&*db, &task_before.project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    ProjectRepo::update_workflow(
        &*db,
        &project.id,
        "{\"workflow\":\"w2\"}",
        None,
        project.version,
        &now_rfc3339(),
    )
    .await
    .expect("workflow update wins the race");

    let result = engine(Arc::clone(&db), event_bus)
        .workflow_execution()
        .restart_with_authority(
            task_id,
            default_states::TODO,
            task_before.version,
            &default_workflow::default_workflow(),
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "stale workflow authority",
            super::WorkflowAuthority {
                project_version: project.version,
                workflow_definition: project.workflow_definition.clone(),
                clear_review_passed_at_on_commit: false,
            },
        )
        .await;
    assert!(
        matches!(result, Err(ServiceError::Db(db::DbError::VersionConflict))),
        "expected stale workflow authority conflict"
    );

    let task_after = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert_eq!(task_after.status, task_before.status);
    assert_eq!(task_after.version, task_before.version);
    assert!(
        TransitionLogRepo::list_by_task(&*db, task_id)
            .await
            .expect("transition logs load")
            .is_empty(),
        "stale reset must not append a state-change log"
    );
}

#[tokio::test]
async fn default_workflow_allows_user_to_leave_planning() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-planning-manual-exit";
    seed_project_repo_and_task(&db, task_id, default_states::PLANNING).await;
    let workflow = default_workflow::default_workflow();
    let current_task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let result = engine(Arc::clone(&db), event_bus)
        .workflow_execution()
        .transition(
            task_id,
            default_states::IN_PROGRESS,
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "start work after planning",
            false,
            Default::default(),
        )
        .await
        .expect("planning can be advanced by a user");

    assert_eq!(result.task.status.to_string(), default_states::IN_PROGRESS);
}

#[tokio::test]
async fn default_workflow_allows_user_to_start_work_from_todo() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-human-start-work";
    seed_project_repo_and_task(&db, task_id, default_states::TODO).await;
    let workflow = default_workflow::default_workflow();
    let current_task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let result = engine(Arc::clone(&db), event_bus)
        .workflow_execution()
        .transition(
            task_id,
            default_states::IN_PROGRESS,
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "start human work",
            false,
            Default::default(),
        )
        .await
        .expect("todo can be moved directly into active work by a user");

    assert_eq!(result.task.status.to_string(), default_states::IN_PROGRESS);
    let transition_logs = TransitionLogRepo::list_by_task(&*db, task_id)
        .await
        .expect("transition logs load");
    assert!(transition_logs.iter().any(|log| {
        log.from_state == default_states::TODO
            && log.to_state == default_states::IN_PROGRESS
            && log.triggered_by == "user:test"
    }));
    assert!(
        !transition_logs
            .iter()
            .any(|log| log.to_state == default_states::PLANNING),
        "dragging to the main In Progress column should not implicitly enter planning"
    );
}

#[tokio::test]
async fn default_workflow_skips_planning_when_no_planner_is_assigned() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-planning-no-planner";
    seed_project_repo_and_task(&db, task_id, default_states::TODO).await;
    let workflow = default_workflow::default_workflow();
    let current_task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let result = engine(Arc::clone(&db), Arc::clone(&event_bus))
        .workflow_execution()
        .transition(
            task_id,
            default_states::PLANNING,
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "enter planning",
            false,
            Default::default(),
        )
        .await
        .expect("planning gate is skipped");

    let result = drain_result(engine(Arc::clone(&db), Arc::clone(&event_bus)), result).await;

    assert_eq!(result.task.status.to_string(), default_states::IN_PROGRESS);
    let transition_logs = TransitionLogRepo::list_by_task(&*db, task_id)
        .await
        .expect("transition logs load");
    assert!(transition_logs.iter().any(|log| {
        log.from_state == default_states::PLANNING
            && log.to_state == default_states::IN_PROGRESS
            && log.bridge.bridge_kind == Some(api_types::TransitionBridgeKind::GateSkipped)
            && !log.rejection
    }));
}

#[tokio::test]
async fn default_workflow_keeps_planning_when_planner_is_human() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-planning-human-planner";
    seed_project_repo_and_task(&db, task_id, default_states::TODO).await;
    assign_user_role(&db, task_id, default_roles::PLANNER).await;
    let workflow = default_workflow::default_workflow();
    let current_task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let result = engine(Arc::clone(&db), event_bus)
        .workflow_execution()
        .transition(
            task_id,
            default_states::PLANNING,
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "enter planning",
            false,
            Default::default(),
        )
        .await
        .expect("human planning gate is entered");

    assert_eq!(result.task.status.to_string(), default_states::PLANNING);
}

#[tokio::test]
async fn default_workflow_skips_system_review_when_no_checks_or_reviewer_are_assigned() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-review-no-checks";
    seed_project_repo_and_task(&db, task_id, default_states::IN_PROGRESS).await;
    let workflow = default_workflow::default_workflow();
    let current_task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let result = engine(Arc::clone(&db), Arc::clone(&event_bus))
        .workflow_execution()
        .transition(
            task_id,
            default_states::REVIEW,
            current_task.version,
            &workflow,
            &api_types::Actor::system(api_types::SystemComponent::General),
            "request review after agent completion",
            false,
            Default::default(),
        )
        .await
        .expect("system-entered unconfigured review skips to merge gate");

    let result = drain_result(engine(Arc::clone(&db), Arc::clone(&event_bus)), result).await;

    assert_eq!(result.task.status.to_string(), default_states::MERGING);
    let transition_logs = TransitionLogRepo::list_by_task(&*db, task_id)
        .await
        .expect("transition logs load");
    assert!(transition_logs.iter().any(|log| {
        log.from_state == default_states::REVIEW
            && log.to_state == default_states::MERGING
            && log.trigger_reason == "review skipped: no checks or reviewer assigned"
    }));
}

#[tokio::test]
async fn default_workflow_keeps_user_requested_review_when_unassigned() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-review-human-requested";
    seed_project_repo_and_task(&db, task_id, default_states::IN_PROGRESS).await;
    let workflow = default_workflow::default_workflow();
    let current_task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let result = engine(Arc::clone(&db), event_bus)
        .workflow_execution()
        .transition(
            task_id,
            default_states::REVIEW,
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "request human review",
            false,
            Default::default(),
        )
        .await
        .expect("user-requested review gate is entered");

    let result = drain_result(engine(db.clone(), Arc::new(EventBus::new(32))), result).await;

    assert_eq!(result.task.status.to_string(), default_states::REVIEW);
    let stored = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let metadata = stored.metadata().expect("metadata parses");
    assert_eq!(metadata.extra.get("awaiting_human"), Some(&json!(true)));
    assert_eq!(
        metadata.extra.get("awaiting_human_reason"),
        Some(&json!("manual_review"))
    );
    let transition_logs = TransitionLogRepo::list_by_task(&*db, task_id)
        .await
        .expect("transition logs load");
    assert!(
        !transition_logs
            .iter()
            .any(|log| log.from_state == default_states::REVIEW
                && log.to_state == default_states::MERGING),
        "user-requested review should not immediately cascade into merge"
    );
}

#[tokio::test]
async fn default_workflow_keeps_review_when_reviewer_is_human() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-review-human-reviewer";
    seed_project_repo_and_task(&db, task_id, default_states::IN_PROGRESS).await;
    assign_user_role(&db, task_id, default_roles::REVIEWER).await;
    let workflow = default_workflow::default_workflow();
    let current_task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let result = engine(Arc::clone(&db), event_bus)
        .workflow_execution()
        .transition(
            task_id,
            default_states::REVIEW,
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "request review",
            false,
            Default::default(),
        )
        .await
        .expect("human review gate is entered");

    assert_eq!(result.task.status.to_string(), default_states::REVIEW);
}

#[tokio::test]
async fn guard_rejection_returns_412_error() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let mut rx = event_bus.subscribe();
    let task_id = "task-guard-rejection";
    seed_project_repo_and_task(&db, task_id, default_states::TODO).await;
    assign_agent_role_without_agent(&db, task_id, default_roles::PLANNER).await;

    let workflow = WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            with_trigger(
                state(
                    default_states::TODO,
                    StateKind::Initial,
                    None,
                    StateHooks {
                        before_exit: vec![hook(
                            "require_upstream_roles_completed",
                            FailurePolicy::Block,
                        )],
                        ..StateHooks::default()
                    },
                ),
                WorkflowTrigger::Accept,
                default_states::IN_PROGRESS,
            ),
            with_trigger(
                state(
                    default_states::PLANNING,
                    StateKind::Gate,
                    Some(default_roles::PLANNER),
                    StateHooks::default(),
                ),
                WorkflowTrigger::Accept,
                default_states::IN_PROGRESS,
            ),
            state(
                default_states::IN_PROGRESS,
                StateKind::Active,
                None,
                StateHooks::default(),
            ),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    };
    let current_task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let result = engine(Arc::clone(&db), Arc::clone(&event_bus))
        .workflow_execution()
        .transition(
            task_id,
            default_states::IN_PROGRESS,
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "start work",
            false,
            Default::default(),
        )
        .await;

    match result {
        Err(ServiceError::GuardRejection { guard, reason }) => {
            assert_eq!(guard, "require_upstream_roles_completed");
            assert!(reason.contains(default_roles::PLANNER));
        }
        Err(error) => panic!("expected guard rejection, got {error:?}"),
        Ok(_) => panic!("expected guard rejection, got successful transition"),
    }

    let task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(task.status.to_string(), default_states::TODO);

    let events = drain_events(&mut rx);
    assert!(
        events
            .iter()
            .all(|event| event.event_type != "task.status_changed"),
        "guard rejection must not publish task.status_changed: {events:?}"
    );
    assert!(
        events
            .iter()
            .any(|event| event.event_type == "transition.guard_rejected"),
        "guard rejection event should be published"
    );
}

#[tokio::test]
async fn effect_failure_log_policy_continues() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-effect-failure-log";
    seed_project_repo_and_task(&db, task_id, default_states::TODO).await;
    assign_agent_role_without_agent(&db, task_id, default_roles::CODER).await;

    let workflow = WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            with_trigger(
                state(
                    default_states::TODO,
                    StateKind::Initial,
                    None,
                    StateHooks::default(),
                ),
                WorkflowTrigger::Accept,
                default_states::IN_PROGRESS,
            ),
            state(
                default_states::IN_PROGRESS,
                StateKind::Active,
                Some(default_roles::CODER),
                StateHooks {
                    // A non-dispatch effect: Log-policy failures never move
                    // the task. (A failed *dispatch* entering an active state
                    // rolls the task back — covered separately below.)
                    on_enter: vec![hook("notify_role_holder", FailurePolicy::Log)],
                    ..StateHooks::default()
                },
            ),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    };
    let current_task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let result = engine(Arc::clone(&db), event_bus)
        .workflow_execution()
        .transition(
            task_id,
            default_states::IN_PROGRESS,
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "start work",
            false,
            Default::default(),
        )
        .await
        .expect("transition succeeds despite logged effect failure");

    let result = drain_result(engine(db.clone(), Arc::new(EventBus::new(32))), result).await;

    assert_eq!(result.task.status.to_string(), default_states::IN_PROGRESS);
    let results = hook_results(&db, task_id).await;
    assert!(results.iter().any(|entry| {
        entry.action == "notify_role_holder"
            && entry.phase == "on_enter"
            && entry.outcome == "failed"
    }));
    assert!(result.task.error_annotation.is_none());
}

#[tokio::test]
async fn dispatch_failure_entering_active_state_rolls_task_back_to_initial() {
    // Incident repro: a task without an approved execution baseline was
    // scheduled by the dispatcher, planning was skipped (no planner), and the
    // coder dispatch failed on entering in_progress — yet the task stayed in
    // in_progress with zero executions, looking in-flight forever.
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-dispatch-failure-rollback";
    seed_project_repo_and_task(&db, task_id, default_states::TODO).await;
    // A coder assignment whose agent is gone makes dispatch_role_agent FAIL
    // (not skip) when the task enters in_progress.
    assign_agent_role_without_agent(&db, task_id, default_roles::CODER).await;
    let workflow = default_workflow::default_workflow();
    let current_task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let result = engine(Arc::clone(&db), Arc::clone(&event_bus))
        .workflow_execution()
        .transition(
            task_id,
            default_states::PLANNING,
            current_task.version,
            &workflow,
            &api_types::Actor::system(api_types::SystemComponent::TaskDispatcher),
            "scheduled by task dispatcher",
            false,
            Default::default(),
        )
        .await
        .expect("transition succeeds");

    let result = drain_result(engine(Arc::clone(&db), Arc::clone(&event_bus)), result).await;
    assert!(db
        .task_steps(task_id)
        .await
        .unwrap()
        .iter()
        .any(|s| s.kind == "cascade"));
    assert_eq!(
        result.task.status.to_string(),
        default_states::TODO,
        "a failed dispatch must not leave the task in the working state"
    );

    let task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(task.status.to_string(), default_states::TODO);
    let annotation = task
        .error_annotation
        .expect("dispatch failure is annotated on the task");
    let annotation: serde_json::Value =
        serde_json::from_str(&annotation).expect("annotation is json");
    assert_eq!(annotation["type"], "dispatch_failed");
    assert!(
        annotation["message"]
            .as_str()
            .expect("annotation message is a string")
            .contains("invalid coder role assignment"),
        "annotation carries the dispatch error: {annotation}"
    );

    let logs = TransitionLogRepo::list_by_task(&*db, task_id)
        .await
        .expect("transition logs list");
    let enter = logs
        .iter()
        .find(|entry| entry.to_state == default_states::IN_PROGRESS)
        .expect("planning -> in_progress transition is logged");
    let entries: Vec<HookResultEntry> = serde_json::from_str(
        enter
            .hook_results_json
            .as_deref()
            .expect("hook results are written"),
    )
    .expect("hook results deserialize");
    assert!(
        entries.iter().any(|entry| {
            entry.action == "dispatch_role_agent"
                && entry.phase == "on_enter"
                && entry.outcome == "failed"
        }),
        "failed dispatch hook is recorded: {entries:?}"
    );
    let rollback = logs
        .iter()
        .find(|entry| {
            entry.from_state == default_states::IN_PROGRESS
                && entry.to_state == default_states::TODO
        })
        .expect("in_progress -> todo rollback transition is logged");
    assert!(
        rollback
            .trigger_reason
            .starts_with("dispatch failed entering in_progress"),
        "rollback records the dispatch failure: {}",
        rollback.trigger_reason
    );
    assert_eq!(
        rollback.triggered_by,
        api_types::Actor::system(api_types::SystemComponent::Workflow).display()
    );
}

#[tokio::test]
async fn successful_dispatch_clears_stale_dispatch_failure_annotation() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-dispatch-annotation-clears";
    seed_project_repo_and_task(&db, task_id, default_states::TODO).await;
    // A user-held coder role makes dispatch_role_agent return Ok
    // (task.awaiting_human) without needing a task executor.
    assign_user_role(&db, task_id, default_roles::CODER).await;
    sqlx::query("UPDATE task SET error_annotation = ? WHERE id = ?")
        .bind(
            json!({
                "type": "dispatch_failed",
                "message": "repository Task is not runnable",
            })
            .to_string(),
        )
        .bind(task_id)
        .execute(db.pool())
        .await
        .expect("stale dispatch failure annotation seeds");
    let workflow = default_workflow::default_workflow();
    let current_task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let result = engine(Arc::clone(&db), event_bus)
        .workflow_execution()
        .transition(
            task_id,
            default_states::IN_PROGRESS,
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "restart work after baseline approval",
            false,
            Default::default(),
        )
        .await
        .expect("transition succeeds");

    let result = drain_result(engine(db.clone(), Arc::new(EventBus::new(32))), result).await;

    assert_eq!(result.task.status.to_string(), default_states::IN_PROGRESS);
    let task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(task.status.to_string(), default_states::IN_PROGRESS);
    assert_eq!(
        task.error_annotation, None,
        "successful dispatch clears the stale dispatch_failed annotation"
    );
}

#[test]
fn validate_claimable_backlog_rejection() {
    let workflow = default_workflow::default_workflow();

    let result = WorkflowEngine::validate_claimable(&workflow, default_states::BACKLOG);
    match result {
        Err(ServiceError::InvalidOperation { message }) => {
            assert!(
                message.contains(default_states::BACKLOG) || message.contains("not claimable"),
                "unexpected validation message: {message}"
            );
        }
        other => panic!("expected invalid operation, got {other:?}"),
    }

    WorkflowEngine::validate_claimable(&workflow, default_states::TODO)
        .expect("todo should be claimable");
}

#[tokio::test]
async fn cancellation_implicit_edge() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-cancellation-implicit-edge";
    seed_project_repo_and_task(&db, task_id, default_states::IN_PROGRESS).await;

    let workflow = WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            state(
                default_states::IN_PROGRESS,
                StateKind::Active,
                None,
                StateHooks {
                    before_exit: vec![hook("dispatch_fix_agent", FailurePolicy::Block)],
                    on_exit: vec![hook("dispatch_fix_agent", FailurePolicy::Log)],
                    ..StateHooks::default()
                },
            ),
            state(
                default_states::CANCELLED,
                StateKind::Terminal,
                None,
                StateHooks::default(),
            ),
        ],
        configuration: Vec::new(),
        cancellation_state: Some(default_states::CANCELLED.to_owned()),
    };
    let current_task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let result = engine(Arc::clone(&db), event_bus)
        .workflow_execution()
        .transition(
            task_id,
            default_states::CANCELLED,
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "cancel",
            false,
            Default::default(),
        )
        .await
        .expect("implicit cancellation transition succeeds");

    let result = drain_result(engine(db.clone(), Arc::new(EventBus::new(32))), result).await;

    assert_eq!(result.task.status.to_string(), default_states::CANCELLED);
    let results = hook_results(&db, task_id).await;
    assert!(
        results
            .iter()
            .filter(|entry| entry.phase == "before_exit")
            .all(|entry| entry.outcome == "skipped"),
        "implicit cancellation should skip before_exit hooks: {results:?}"
    );
    assert!(
        results
            .iter()
            .any(|entry| entry.action == "dispatch_fix_agent" && entry.phase == "on_exit"),
        "implicit cancellation should still run on_exit hooks"
    );
}

async fn seed_custom_workflow_task(
    db: &SqliteDb,
    task_id: &str,
    status: &str,
    workflow: &WorkflowDefinition,
) -> String {
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();
    ProjectRepo::create(
        db,
        CreateProject {
            id: project_id.clone(),
            name: "Custom".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: serde_json::to_string(workflow).unwrap(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");
    RepoRepo::create(
        db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "repo".to_owned(),
            remote_url: Some("https://example.com/repo.git".to_owned()),
            local_path: None,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo creates");
    ProjectRepo::update_at_version(
        db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        ProjectRepo::get_by_id(db, &project_id)
            .await
            .expect("fixture Project lookup")
            .expect("fixture Project exists")
            .version,
        None,
    )
    .await
    .expect("project primary repo updates");
    TaskRepo::create(
        db,
        CreateTask {
            id: task_id.to_owned(),
            project_id: project_id.clone(),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "custom workflow task".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: status.to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("task creates");
    project_id
}

#[tokio::test]
async fn review_refresh_bridge_skips_merge_repair_entry_hooks() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();

    let mut merging = with_trigger(
        state(
            default_states::MERGING,
            StateKind::Gate,
            None,
            StateHooks::default(),
        ),
        WorkflowTrigger::Reject,
        default_states::MERGE_FAILED,
    );
    merging.canonical_phase = Some(api_types::CanonicalPhase::Review);

    let mut merge_failed = with_trigger(
        state(
            default_states::MERGE_FAILED,
            StateKind::Active,
            Some(default_roles::CODER),
            StateHooks {
                before_enter: vec![hook("run_before_work_hooks", FailurePolicy::Block)],
                on_enter: vec![hook("dispatch_role_agent", FailurePolicy::Log)],
                ..StateHooks::default()
            },
        ),
        WorkflowTrigger::Accept,
        default_states::REVIEW,
    );
    merge_failed.canonical_phase = Some(api_types::CanonicalPhase::Review);

    let mut review = state(
        default_states::REVIEW,
        StateKind::Gate,
        Some(default_roles::REVIEWER),
        StateHooks::default(),
    );
    review.canonical_phase = Some(api_types::CanonicalPhase::Review);

    let workflow = WorkflowDefinition {
        roles: Vec::new(),
        states: vec![merging, merge_failed, review],
        configuration: Vec::new(),
        cancellation_state: None,
    };
    let project_id =
        seed_custom_workflow_task(&db, &task_id, default_states::MERGING, &workflow).await;

    // If the intermediate repair state's entry hook runs, this deliberately
    // invalid settings payload makes it block before the review cascade.
    sqlx::query("UPDATE project SET settings = ? WHERE id = ?")
        .bind("not-json")
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project settings become invalid");

    let result = engine(Arc::clone(&db), Arc::clone(&event_bus))
        .workflow_execution()
        .transition(
            &task_id,
            default_states::MERGE_FAILED,
            1,
            &workflow,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "Reviewed commit changed; fresh review required",
            true,
            api_types::TransitionBridge::new(api_types::TransitionBridgeKind::ReviewRefresh),
        )
        .await
        .expect("review refresh crosses the repair bridge");

    let result = drain_result(engine(Arc::clone(&db), Arc::clone(&event_bus)), result).await;

    assert_eq!(result.task.status, default_states::REVIEW);
    let logs = TransitionLogRepo::list_by_task(&*db, &task_id)
        .await
        .expect("transition logs load");
    let bridge = logs
        .iter()
        .find(|entry| {
            entry.from_state == default_states::MERGING
                && entry.to_state == default_states::MERGE_FAILED
        })
        .expect("review-refresh bridge transition exists");
    assert!(
        !bridge.rejection,
        "review-refresh bridges are not retry rejections"
    );
    let results: Vec<HookResultEntry> = serde_json::from_str(
        bridge
            .hook_results_json
            .as_deref()
            .expect("bridge hook results are recorded"),
    )
    .expect("bridge hook results deserialize");
    assert!(
        results.iter().all(|entry| entry.phase != "before_enter"),
        "repair entry hooks must not run for the review-refresh bridge: {results:?}"
    );
    assert!(results.iter().any(|entry| {
        entry.phase == "on_enter"
            && entry.action == "dispatch_role_agent"
            && entry.outcome == "cascade"
    }));
}

#[tokio::test]
async fn custom_workflow_renamed_states_lifecycle() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow = WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            with_trigger(
                state("pending", StateKind::Initial, None, StateHooks::default()),
                WorkflowTrigger::Accept,
                "working",
            ),
            with_trigger(
                state("working", StateKind::Active, None, StateHooks::default()),
                WorkflowTrigger::Accept,
                "checking",
            ),
            with_trigger(
                state("checking", StateKind::Gate, None, StateHooks::default()),
                WorkflowTrigger::Accept,
                "shipped",
            ),
            state("shipped", StateKind::Terminal, None, StateHooks::default()),
            state(
                "abandoned",
                StateKind::Terminal,
                None,
                StateHooks::default(),
            ),
        ],
        configuration: Vec::new(),
        cancellation_state: Some("abandoned".to_owned()),
    };
    seed_custom_workflow_task(&db, &task_id, "pending", &workflow).await;
    let eng = engine(Arc::clone(&db), Arc::clone(&event_bus));

    let r1 = eng
        .workflow_execution()
        .transition(
            &task_id,
            "working",
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "claim",
            false,
            Default::default(),
        )
        .await
        .expect("pending → working");
    assert_eq!(r1.task.status, "working");

    let r2 = eng
        .workflow_execution()
        .transition(
            &task_id,
            "checking",
            r1.task.version,
            &workflow,
            &api_types::Actor::system(api_types::SystemComponent::General),
            "completed",
            false,
            Default::default(),
        )
        .await
        .expect("working → checking");
    assert_eq!(r2.task.status, "checking");

    let r3 = eng
        .workflow_execution()
        .transition(
            &task_id,
            "shipped",
            r2.task.version,
            &workflow,
            &api_types::Actor::system(api_types::SystemComponent::General),
            "approved",
            false,
            Default::default(),
        )
        .await
        .expect("checking → shipped");
    assert_eq!(r3.task.status, "shipped");
}

#[tokio::test]
async fn implicit_accept_transition_moves_to_next_declared_state() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow = WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            state("pending", StateKind::Initial, None, StateHooks::default()),
            state("working", StateKind::Active, None, StateHooks::default()),
            state("shipped", StateKind::Terminal, None, StateHooks::default()),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    };
    seed_custom_workflow_task(&db, &task_id, "pending", &workflow).await;

    let result = engine(Arc::clone(&db), event_bus)
        .workflow_execution()
        .transition(
            &task_id,
            "working",
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "accept",
            false,
            Default::default(),
        )
        .await
        .expect("pending implicitly accepts to working");

    assert_eq!(result.task.status, "working");
}

#[tokio::test]
async fn gate_approve_reject_on_custom_gate_states() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let mut start_state = state("start", StateKind::Initial, None, StateHooks::default());
    start_state.triggers.insert(
        WorkflowTrigger::Accept,
        WorkflowTriggerDefinition {
            to: "coding".to_owned(),
            dispatch: None,
        },
    );
    let mut coding_state = state("coding", StateKind::Active, None, StateHooks::default());
    coding_state.triggers.insert(
        WorkflowTrigger::Accept,
        WorkflowTriggerDefinition {
            to: "qa".to_owned(),
            dispatch: None,
        },
    );
    let mut qa_state = state("qa", StateKind::Gate, None, StateHooks::default());
    qa_state.gate_config = Some(api_types::GateConfig {
        reject_target: Some("coding".to_owned()),
        max_rejections: Some(2),
        approve_label: None,
        reject_label: None,
        requires_user_approval: Some(false),
        optional_when_unassigned: Some(false),
    });
    qa_state.triggers.insert(
        WorkflowTrigger::Accept,
        WorkflowTriggerDefinition {
            to: "released".to_owned(),
            dispatch: None,
        },
    );
    qa_state.triggers.insert(
        WorkflowTrigger::Reject,
        WorkflowTriggerDefinition {
            to: "coding".to_owned(),
            dispatch: None,
        },
    );
    let workflow = WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            start_state,
            coding_state,
            qa_state,
            state("released", StateKind::Terminal, None, StateHooks::default()),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    };
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();
    let task_a_id = new_uuid_v4();
    let task_b_id = new_uuid_v4();
    ProjectRepo::create(
        &*db,
        CreateProject {
            id: project_id.clone(),
            name: "Custom".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: serde_json::to_string(&workflow).unwrap(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");
    RepoRepo::create(
        &*db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "repo".to_owned(),
            remote_url: Some("https://example.com/repo.git".to_owned()),
            local_path: None,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo creates");
    ProjectRepo::update_at_version(
        &*db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        ProjectRepo::get_by_id(&*db, &project_id)
            .await
            .expect("fixture Project lookup")
            .expect("fixture Project exists")
            .version,
        None,
    )
    .await
    .expect("project primary repo updates");
    for (task_id, title) in [
        (task_a_id.as_str(), "custom gate approve"),
        (task_b_id.as_str(), "custom gate reject"),
    ] {
        TaskRepo::create(
            &*db,
            CreateTask {
                id: task_id.to_owned(),
                project_id: project_id.clone(),
                parent_task_id: None,
                subtask_order: None,
                assignee_type: None,
                assignee_id: None,
                title: title.to_owned(),
                description: None,
                task_type: "task".to_owned(),
                status: "start".to_owned(),
                is_automation: false,
                priority: 0,
                task_state_config: None,
                merge_config: None,
                plan: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("task creates");
    }
    let eng = engine(Arc::clone(&db), Arc::clone(&event_bus));

    let current_task = TaskRepo::get_by_id(&*db, &task_a_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let result = eng
        .workflow_execution()
        .transition(
            &task_a_id,
            "coding",
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "start work",
            false,
            Default::default(),
        )
        .await
        .expect("start → coding");
    assert_eq!(result.task.status, "coding");

    let current_task = TaskRepo::get_by_id(&*db, &task_a_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let result = eng
        .workflow_execution()
        .transition(
            &task_a_id,
            "qa",
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "request qa",
            false,
            Default::default(),
        )
        .await
        .expect("coding → qa");
    assert_eq!(result.task.status, "qa");

    let current_task = TaskRepo::get_by_id(&*db, &task_a_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let result = eng
        .workflow_execution()
        .transition(
            &task_a_id,
            "released",
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "gate approved",
            false,
            api_types::TransitionBridge::new(api_types::TransitionBridgeKind::GateApproved),
        )
        .await
        .expect("qa → released");
    assert_eq!(result.task.status, "released");

    let current_task = TaskRepo::get_by_id(&*db, &task_b_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let result = eng
        .workflow_execution()
        .transition(
            &task_b_id,
            "coding",
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "start work",
            false,
            Default::default(),
        )
        .await
        .expect("start → coding");
    assert_eq!(result.task.status, "coding");

    let current_task = TaskRepo::get_by_id(&*db, &task_b_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let result = eng
        .workflow_execution()
        .transition(
            &task_b_id,
            "qa",
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "request qa",
            false,
            Default::default(),
        )
        .await
        .expect("coding → qa");
    assert_eq!(result.task.status, "qa");

    let current_task = TaskRepo::get_by_id(&*db, &task_b_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let result = eng
        .workflow_execution()
        .transition(
            &task_b_id,
            "coding",
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "gate rejected",
            true,
            api_types::TransitionBridge::new(api_types::TransitionBridgeKind::GateRejected),
        )
        .await
        .expect("qa → coding (reject)");
    assert_eq!(result.task.status, "coding");
}

#[tokio::test]
async fn user_approval_gate_failed_blocking_before_enter_cascades_to_reject_target() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow =
        user_approval_review_workflow(hook("run_ci_steps", FailurePolicy::Block), Vec::new());
    seed_custom_workflow_task(&db, &task_id, "working", &workflow).await;
    sqlx::query("UPDATE task SET task_state_config = ? WHERE id = ?")
        .bind(r#"{"review":{"ci_steps":"not-an-array"}}"#)
        .bind(&task_id)
        .execute(db.pool())
        .await
        .expect("task review config updates");

    let result = engine(Arc::clone(&db), Arc::clone(&event_bus))
        .workflow_execution()
        .transition(
            &task_id,
            "review",
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "submit for validation",
            false,
            Default::default(),
        )
        .await
        .expect("failed validation cascades back to working");

    let result = drain_result(engine(Arc::clone(&db), Arc::clone(&event_bus)), result).await;

    assert_eq!(result.task.status, "working");
    assert!(result.task.entry_barrier_json.is_none());
    let logs = TransitionLogRepo::list_by_task(&*db, &task_id)
        .await
        .expect("transition logs load");
    assert!(
        logs.iter().any(|entry| {
            entry.from_state == "review" && entry.to_state == "working" && entry.rejection
        }),
        "validation rejection should be recorded on the automatic reject cascade"
    );
}

struct FailedCiFixture {
    db: Arc<SqliteDb>,
    engine: crate::TaskService,
    workflow: WorkflowDefinition,
    task: db::Task,
    workspace: db::Workspace,
    _repo_dir: TempDir,
    _workspace_root: TempDir,
}

impl FailedCiFixture {
    async fn workflow_authority(&self) -> super::WorkflowAuthority {
        // Match TaskService's transition entry: review hooks require the
        // current Project snapshot before they can create a Review attempt.
        // Read it here because a test may change the workflow after setup.
        let project = ProjectRepo::get_by_id(&*self.db, &self.task.project_id)
            .await
            .unwrap()
            .unwrap();
        super::WorkflowAuthority {
            project_version: project.version,
            workflow_definition: project.workflow_definition,
            clear_review_passed_at_on_commit: false,
        }
    }
}

async fn failed_ci_fixture(budget: i32, policy: FailurePolicy) -> FailedCiFixture {
    let db = Arc::new(sqlite_db().await);
    let task_id = new_uuid_v4();
    seed_project_repo_and_task(&db, &task_id, "merge_failed").await;
    let repo_dir = TempDir::new().unwrap();
    let workspace_root = TempDir::new().unwrap();
    initialize_git_repo(repo_dir.path());
    let mut workflow = default_workflow::default_workflow();
    let review_state = workflow
        .states
        .iter_mut()
        .find(|state| state.name == "review")
        .unwrap();
    review_state
        .hooks
        .before_enter
        .iter_mut()
        .find(|hook| hook.action == "run_ci_steps")
        .unwrap()
        .on_failure = policy;
    // Task-level budget must win over the gate's default.
    review_state.gate_config.as_mut().unwrap().max_rejections = Some(99);
    let task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    sqlx::query("UPDATE repo SET local_path = ?, remote_url = ? WHERE project_id = ?")
        .bind(repo_dir.path().to_string_lossy().as_ref())
        .bind(repo_dir.path().to_string_lossy().as_ref())
        .bind(&task.project_id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE project SET workflow_definition = ? WHERE id = ?")
        .bind(serde_json::to_string(&workflow).unwrap())
        .bind(&task.project_id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE task SET task_state_config = ? WHERE id = ?")
        .bind(json!({"retry_budgets":{"review":budget}, "review":{"ci_steps":["echo 'entry-ci-failed [conflict-handoff]'; exit 101"]}}).to_string())
        .bind(&task.id).execute(db.pool()).await.unwrap();
    let task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    let workspace = crate::task_service::workspace::prepare_workspace(
        &db,
        workspace_root.path(),
        &task,
        &task.id,
        None,
        &crate::lifecycle::context::embedded_workspace_router_for_test(
            Arc::clone(&db),
            workspace_root.path().to_path_buf(),
            None,
        ),
    )
    .await
    .unwrap();
    let sha = git::get_current_sha(std::path::Path::new(
        &workspace.embedded_worktree_path_for_backend(),
    ))
    .await
    .unwrap();
    let now = now_rfc3339();
    db::ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: None,
            role: default_roles::CODER.to_owned(),
            status: db::ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: Some(now.clone()),
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: Some(sha.clone()),
            after_sha: Some(sha),
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: Some(workspace.id.clone()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
    let mut eng = engine(Arc::clone(&db), Arc::new(EventBus::new(32)));
    eng.workspace_root = workspace_root.path().to_path_buf();
    eng = eng.with_workspace_root(workspace_root.path().to_path_buf());
    FailedCiFixture {
        db,
        engine: eng,
        workflow,
        task,
        workspace,
        _repo_dir: repo_dir,
        _workspace_root: workspace_root,
    }
}

#[tokio::test]
async fn before_work_workspace_reset_required_keeps_typed_recovery_annotation() {
    let fixture = failed_ci_fixture(3, FailurePolicy::Block).await;
    sqlx::query("UPDATE project SET settings = ? WHERE id = ?")
        .bind(
            json!({
                "lifecycle_hooks": {
                    "before_work": [{
                        "type": "script",
                        "command": "true",
                        "timeout_seconds": 5,
                        "blocking": true
                    }]
                }
            })
            .to_string(),
        )
        .bind(&fixture.task.project_id)
        .execute(fixture.db.pool())
        .await
        .expect("blocking before-work hook config writes");
    let worktree_path =
        std::path::PathBuf::from(fixture.workspace.embedded_worktree_path_for_backend());
    std::fs::remove_dir_all(&worktree_path).expect("worktree directory removes");
    run_git(fixture._repo_dir.path(), &["worktree", "prune"]);
    run_git(
        fixture._repo_dir.path(),
        &["branch", "-D", &fixture.workspace.branch],
    );

    let result = fixture
        .engine
        .workflow_execution()
        .transition_with_authority(
            &fixture.task.id,
            "review",
            fixture.task.version,
            &fixture.workflow,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "worker completed",
            false,
            fixture.workflow_authority().await,
        )
        .await
        .expect("typed workspace blocker settles the transition");

    let result = drain_result(fixture.engine.clone(), result).await;

    assert_eq!(result.task.status, "in_progress");
    let task = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    let annotation: serde_json::Value = serde_json::from_str(
        task.error_annotation
            .as_deref()
            .expect("workspace reset annotation exists"),
    )
    .expect("workspace reset annotation is JSON");
    assert_eq!(annotation["type"], "workspace_reset_required");
    assert!(annotation["message"]
        .as_str()
        .expect("annotation message is text")
        .contains("workspace reset required"));
    assert!(annotation.get("recovery_actions").is_none());
    assert!(
        WorkspaceRepo::get_by_id(&*fixture.db, &fixture.workspace.id)
            .await
            .expect("workspace reloads")
            .is_some()
    );
    assert!(!worktree_path.exists());
}

#[tokio::test]
async fn system_review_ci_failure_routes_to_coder_and_spends_review_budget() {
    // Log-policy hooks used by existing Projects must settle the same verdict
    // as the current blocking entry hook. Also exercise deleted-tree recovery.
    for (policy, requires_approval) in [(FailurePolicy::Block, false), (FailurePolicy::Log, true)] {
        let mut fixture = failed_ci_fixture(3, policy).await;
        fixture
            .workflow
            .states
            .iter_mut()
            .find(|state| state.name == "review")
            .unwrap()
            .gate_config
            .as_mut()
            .unwrap()
            .requires_user_approval = Some(requires_approval);
        sqlx::query("UPDATE project SET workflow_definition = ? WHERE id = ?")
            .bind(serde_json::to_string(&fixture.workflow).unwrap())
            .bind(&fixture.task.project_id)
            .execute(fixture.db.pool())
            .await
            .unwrap();
        std::fs::remove_dir_all(fixture.workspace.embedded_worktree_path_for_backend()).unwrap();
        let result = fixture
            .engine
            .workflow_execution()
            .transition_with_authority(
                &fixture.task.id,
                "review",
                fixture.task.version,
                &fixture.workflow,
                &api_types::Actor::system(api_types::SystemComponent::Workflow),
                "user action",
                false,
                fixture.workflow_authority().await,
            )
            .await
            .unwrap();
        let result = drain_result(fixture.engine.clone(), result).await;
        assert_eq!(result.task.status, "in_progress");
        assert!(result.task.entry_barrier_json.is_none());
        assert!(result.task.blocked_json.is_none());
        let review = result.review.unwrap();
        assert_eq!(review.status, db::ReviewStatus::Failed);
        let details: serde_json::Value = serde_json::from_str(&review.step_results_json).unwrap();
        assert_eq!(details["ci_steps"][0]["exit_code"], 101);
        let entries = TransitionLogRepo::list_by_task(&*fixture.db, &fixture.task.id)
            .await
            .unwrap();
        let rejection = entries
            .iter()
            .find(|entry| entry.from_state == "review" && entry.rejection)
            .unwrap();
        assert_eq!(rejection.to_state, "in_progress");
        // CI diagnostics use the stored zero-based step index. Check the
        // stable prefix, including the command, without depending on output.
        assert!(
            rejection.trigger_reason.starts_with(
                "CI step 0 failed (exit 101): `echo 'entry-ci-failed [conflict-handoff]'; exit 101`"
            ),
            "unexpected CI rejection reason: {}",
            rejection.trigger_reason,
        );
        assert_eq!(
            crate::task_diagnostics::audit_gate_rejections_since_boundary(&entries, "review"),
            1
        );
        assert!(
            std::path::Path::new(&fixture.workspace.embedded_worktree_path_for_backend()).exists()
        );
    }
}

#[tokio::test]
async fn system_review_ci_failure_records_review_budget_exhausted_blocker() {
    let fixture = failed_ci_fixture(1, FailurePolicy::Block).await;
    let result = fixture
        .engine
        .workflow_execution()
        .transition_with_authority(
            &fixture.task.id,
            "review",
            fixture.task.version,
            &fixture.workflow,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "repair completed",
            false,
            fixture.workflow_authority().await,
        )
        .await
        .unwrap();
    let result = drain_result(fixture.engine.clone(), result).await;
    assert_eq!(result.task.status, "review");
    assert!(result.task.entry_barrier_json.is_none());
    assert!(result.task.blocked_json.is_some());
    let annotation: serde_json::Value =
        serde_json::from_str(result.task.error_annotation.as_deref().unwrap()).unwrap();
    assert_eq!(annotation["type"], "review_budget_exhausted");
    let review = result.review.unwrap();
    assert_eq!(review.status, db::ReviewStatus::Failed);
    let details: serde_json::Value = serde_json::from_str(&review.step_results_json).unwrap();
    assert_eq!(details["ci_steps"][0]["exit_code"], 101);
}

#[tokio::test]
async fn user_review_ci_success_still_waits_for_human() {
    let fixture = failed_ci_fixture(3, FailurePolicy::Block).await;
    sqlx::query("UPDATE task SET task_state_config = ? WHERE id = ?")
        .bind(r#"{"review":{"ci_steps":["test -d ."]}}"#)
        .bind(&fixture.task.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let result = fixture
        .engine
        .workflow_execution()
        .transition_with_authority(
            &fixture.task.id,
            "review",
            fixture.task.version,
            &fixture.workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "request human review",
            false,
            fixture.workflow_authority().await,
        )
        .await
        .unwrap();
    let result = drain_result(fixture.engine.clone(), result).await;
    assert_eq!(result.task.status, "review");
    assert!(result.task.entry_barrier_json.is_none());
    assert!(
        !db::TaskStepRepo::task_steps(&*fixture.db, &fixture.task.id)
            .await
            .unwrap()
            .iter()
            .any(|s| s.kind == "cascade")
    );
    let review = result.review.unwrap();
    assert_eq!(review.status, db::ReviewStatus::AwaitingHuman);
    let details: serde_json::Value = serde_json::from_str(&review.step_results_json).unwrap();
    assert_eq!(details["ci_steps"][0]["exit_code"], 0);
}

#[tokio::test]
async fn user_approval_gate_passing_hooks_pauses_forward_cascade_for_human() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow = user_approval_review_workflow(
        hook("check_retry_budget", FailurePolicy::Block),
        vec![hook("auto_cascade_on_completion", FailurePolicy::Log)],
    );
    seed_custom_workflow_task(&db, &task_id, "working", &workflow).await;

    let result = engine(Arc::clone(&db), event_bus)
        .workflow_execution()
        .transition(
            &task_id,
            "review",
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "submit for validation",
            false,
            Default::default(),
        )
        .await
        .expect("passing validation pauses for human approval");

    assert_eq!(result.task.status, "review");
    assert!(result.task.entry_barrier_json.is_none());
    assert!(!db::TaskStepRepo::task_steps(&*db, &task_id)
        .await
        .unwrap()
        .iter()
        .any(|s| s.kind == "cascade"));
    let logs = TransitionLogRepo::list_by_task(&*db, &task_id)
        .await
        .expect("transition logs load");
    assert!(!logs.iter().any(|entry| entry.rejection));
}

#[tokio::test]
async fn review_gate_exhausted_budget_defers_blocking_until_review_failure() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let mut review = state("review", StateKind::Gate, None, StateHooks::default());
    review.gate_config = Some(api_types::GateConfig {
        reject_target: Some("coding".to_owned()),
        max_rejections: Some(1),
        approve_label: None,
        reject_label: None,
        requires_user_approval: Some(false),
        optional_when_unassigned: Some(false),
    });
    review.triggers.insert(
        WorkflowTrigger::Accept,
        WorkflowTriggerDefinition {
            to: "shipped".to_owned(),
            dispatch: None,
        },
    );
    review.triggers.insert(
        WorkflowTrigger::Reject,
        WorkflowTriggerDefinition {
            to: "coding".to_owned(),
            dispatch: None,
        },
    );
    review.triggers.insert(
        WorkflowTrigger::Fail,
        WorkflowTriggerDefinition {
            to: "needs_help".to_owned(),
            dispatch: None,
        },
    );
    let workflow = WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            with_trigger(
                state("start", StateKind::Initial, None, StateHooks::default()),
                WorkflowTrigger::Accept,
                "coding",
            ),
            with_trigger(
                state("coding", StateKind::Active, None, StateHooks::default()),
                WorkflowTrigger::Accept,
                "review",
            ),
            review,
            state("needs_help", StateKind::Custom, None, StateHooks::default()),
            state("shipped", StateKind::Terminal, None, StateHooks::default()),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    };
    seed_custom_workflow_task(&db, &task_id, "start", &workflow).await;
    let eng = engine(Arc::clone(&db), Arc::clone(&event_bus));

    let current_task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let result = eng
        .workflow_execution()
        .transition(
            &task_id,
            "coding",
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "start work",
            false,
            Default::default(),
        )
        .await
        .expect("start → coding");
    assert_eq!(result.task.status, "coding");

    let current_task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let result = eng
        .workflow_execution()
        .transition(
            &task_id,
            "review",
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "request review",
            false,
            Default::default(),
        )
        .await
        .expect("coding → review");
    let result = drain_result(eng.clone(), result).await;
    assert_eq!(result.task.status, "review");

    let transition_logs = TransitionLogRepo::list_by_task(&*db, &task_id)
        .await
        .expect("transition logs load");
    let payload = transition_logs
        .last()
        .and_then(|entry| entry.hook_results_json.as_deref())
        .expect("hook results are written");
    let results: Vec<HookResultEntry> =
        serde_json::from_str(payload).expect("hook results deserialize");
    assert!(results
        .iter()
        .any(|entry| { entry.action == "check_retry_budget" && entry.phase == "after_enter" }));

    let current_task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let result = eng
        .workflow_execution()
        .transition(
            &task_id,
            "coding",
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "gate rejected",
            true,
            api_types::TransitionBridge::new(api_types::TransitionBridgeKind::GateRejected),
        )
        .await
        .expect("review → coding (reject)");
    assert_eq!(result.task.status, "coding");

    let current_task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    eng.workflow_execution()
        .transition(
            &task_id,
            "review",
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "request review again",
            false,
            Default::default(),
        )
        .await
        .expect("review budget resolves on re-entry");

    let current_task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(current_task.status, "review");
    assert!(
        current_task.blocked_json.is_none(),
        "review entry defers retry exhaustion enforcement until a review failure is recorded"
    );

    let transition_logs = TransitionLogRepo::list_by_task(&*db, &task_id)
        .await
        .expect("transition logs load");
    assert!(!transition_logs
        .iter()
        .any(|entry| { entry.from_state == "review" && entry.to_state == "needs_help" }));
}

#[tokio::test]
async fn planning_gate_reject_back_to_itself() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow = default_workflow::default_workflow();
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();

    ProjectRepo::create(
        &*db,
        CreateProject {
            id: project_id.clone(),
            name: "Default".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: serde_json::to_string(&workflow).unwrap(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");
    RepoRepo::create(
        &*db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "repo".to_owned(),
            remote_url: Some("https://example.com/repo.git".to_owned()),
            local_path: None,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo creates");
    ProjectRepo::update_at_version(
        &*db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        ProjectRepo::get_by_id(&*db, &project_id)
            .await
            .expect("fixture Project lookup")
            .expect("fixture Project exists")
            .version,
        None,
    )
    .await
    .expect("project primary repo updates");
    TaskRepo::create(
        &*db,
        CreateTask {
            id: task_id.clone(),
            project_id: project_id.clone(),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "planning reject".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: default_states::TODO.to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("task creates");
    assign_user_role(&db, &task_id, default_roles::PLANNER).await;
    let eng = engine(Arc::clone(&db), Arc::clone(&event_bus));

    let current_task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let result = eng
        .workflow_execution()
        .transition(
            &task_id,
            default_states::PLANNING,
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "enter planning",
            false,
            Default::default(),
        )
        .await
        .expect("todo → planning");
    assert_eq!(result.task.status, default_states::PLANNING);

    let current_task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let result = eng
        .workflow_execution()
        .transition(
            &task_id,
            default_states::PLANNING,
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "gate rejected",
            true,
            api_types::TransitionBridge::new(api_types::TransitionBridgeKind::GateRejected),
        )
        .await
        .expect("planning rejects back to itself");
    assert_eq!(result.task.status, default_states::PLANNING);
}

#[tokio::test]
async fn gate_reject_back_to_itself() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let mut planning = state("planning", StateKind::Gate, None, StateHooks::default());
    planning.gate_config = Some(api_types::GateConfig {
        reject_target: Some("planning".to_owned()),
        max_rejections: Some(3),
        approve_label: None,
        reject_label: None,
        requires_user_approval: Some(false),
        optional_when_unassigned: Some(false),
    });
    planning.triggers.insert(
        WorkflowTrigger::Reject,
        WorkflowTriggerDefinition {
            to: "planning".to_owned(),
            dispatch: None,
        },
    );
    planning.triggers.insert(
        WorkflowTrigger::Accept,
        WorkflowTriggerDefinition {
            to: "working".to_owned(),
            dispatch: None,
        },
    );
    let workflow = WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            with_trigger(
                state("todo", StateKind::Initial, None, StateHooks::default()),
                WorkflowTrigger::Accept,
                "planning",
            ),
            planning,
            with_trigger(
                state("working", StateKind::Active, None, StateHooks::default()),
                WorkflowTrigger::Accept,
                "done",
            ),
            state("done", StateKind::Terminal, None, StateHooks::default()),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    };
    seed_custom_workflow_task(&db, &task_id, "planning", &workflow).await;
    let eng = engine(Arc::clone(&db), Arc::clone(&event_bus));

    let reject_target = workflow
        .gate_reject_target("planning")
        .expect("planning has reject_target");
    assert_eq!(reject_target, "planning");

    let result = eng
        .workflow_execution()
        .transition(
            &task_id,
            reject_target,
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "gate rejected: needs more detail",
            true,
            api_types::TransitionBridge::new(api_types::TransitionBridgeKind::GateRejected),
        )
        .await
        .expect("planning → planning (reject)");
    assert_eq!(result.task.status, "planning");

    let logs = TransitionLogRepo::list_by_task(&*db, &task_id)
        .await
        .expect("transition log loads");
    assert!(
        logs.iter().any(|entry| entry.from_state == "planning"
            && entry.to_state == "planning"
            && entry.rejection),
        "transition log should record the rejection"
    );
}

fn missing_edge_workflow() -> WorkflowDefinition {
    WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            with_trigger(
                state("working", StateKind::Active, None, StateHooks::default()),
                WorkflowTrigger::Accept,
                "paused",
            ),
            state("paused", StateKind::Custom, None, StateHooks::default()),
            state("done", StateKind::Terminal, None, StateHooks::default()),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    }
}

fn system_only_fail_edge_workflow() -> WorkflowDefinition {
    WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            with_trigger(
                state("working", StateKind::Active, None, StateHooks::default()),
                WorkflowTrigger::Fail,
                "failed",
            ),
            state("failed", StateKind::Custom, None, StateHooks::default()),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    }
}

fn workflow_without_review_state() -> WorkflowDefinition {
    WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            with_trigger(
                state("pending", StateKind::Initial, None, StateHooks::default()),
                WorkflowTrigger::Accept,
                "working",
            ),
            with_trigger(
                state("working", StateKind::Active, None, StateHooks::default()),
                WorkflowTrigger::Accept,
                "shipped",
            ),
            state("shipped", StateKind::Terminal, None, StateHooks::default()),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    }
}

async fn seed_parent_and_subtask(
    db: &SqliteDb,
    parent_id: &str,
    subtask_id: &str,
    subtask_status: &str,
    task_state_config: Option<String>,
) {
    seed_project_repo_and_task(db, parent_id, default_states::IN_PROGRESS).await;
    let parent = TaskRepo::get_by_id(db, parent_id, false)
        .await
        .expect("parent loads")
        .expect("parent exists");
    let now = now_rfc3339();
    TaskRepo::create(
        db,
        CreateTask {
            id: subtask_id.to_owned(),
            project_id: parent.project_id.clone(),
            parent_task_id: Some(parent_id.to_owned()),
            subtask_order: Some(0),
            assignee_type: None,
            assignee_id: None,
            title: "subtask".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: subtask_status.to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("subtask creates");
}

#[tokio::test]
async fn user_override_succeeds_across_missing_edge() {
    // Delta: User moves a task across a missing edge
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow = missing_edge_workflow();
    seed_custom_workflow_task(&db, &task_id, "working", &workflow).await;
    let eng = engine(Arc::clone(&db), event_bus);

    let result = eng
        .workflow_execution()
        .transition(
            &task_id,
            "done",
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "override",
            false,
            Default::default(),
        )
        .await
        .expect("user override across missing edge succeeds");

    assert_eq!(result.task.status, "done");
}

#[tokio::test]
async fn user_override_does_not_reopen_terminal_state() {
    // Terminal reopen must not use user routing override (missing edge from terminal -> non-terminal).
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow = missing_edge_workflow();
    seed_custom_workflow_task(&db, &task_id, "done", &workflow).await;
    let eng = engine(Arc::clone(&db), event_bus);

    let result = eng
        .workflow_execution()
        .transition(
            &task_id,
            "working",
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "override",
            false,
            Default::default(),
        )
        .await;

    assert!(
        matches!(
            result,
            Err(ServiceError::Db(db::DbError::InvalidTransition))
        ),
        "user override must not reopen a terminal state"
    );
}

#[tokio::test]
async fn user_override_succeeds_along_system_only_edge() {
    // Delta: User moves a task along a system-only edge
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow = system_only_fail_edge_workflow();
    seed_custom_workflow_task(&db, &task_id, "working", &workflow).await;
    let eng = engine(Arc::clone(&db), Arc::clone(&event_bus));

    let user_result = eng
        .workflow_execution()
        .transition(
            &task_id,
            "failed",
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "user override along fail edge",
            false,
            Default::default(),
        )
        .await
        .expect("user override along system-only edge succeeds");
    assert_eq!(user_result.task.status, "failed");

    let task_id_system = new_uuid_v4();
    seed_custom_workflow_task(&db, &task_id_system, "working", &workflow).await;
    let system_result = eng
        .workflow_execution()
        .transition(
            &task_id_system,
            "failed",
            1,
            &workflow,
            &api_types::Actor::system(api_types::SystemComponent::General),
            "system fail transition",
            false,
            Default::default(),
        )
        .await
        .expect("system actor may use system-only edge");
    assert_eq!(system_result.task.status, "failed");

    let task_id_agent = new_uuid_v4();
    seed_custom_workflow_task(&db, &task_id_agent, "working", &workflow).await;
    let agent_result = eng
        .workflow_execution()
        .transition(
            &task_id_agent,
            "failed",
            1,
            &workflow,
            &api_types::Actor::agent("unit"),
            "agent attempt",
            false,
            Default::default(),
        )
        .await;
    match agent_result {
        Err(ServiceError::InvalidOperation { message }) => {
            assert!(
                message.contains("system-only"),
                "expected system-only rejection, got: {message}"
            );
        }
        Ok(_) => panic!("expected system-only rejection for agent, got Ok"),
        Err(other) => panic!("expected system-only rejection for agent, got {other:?}"),
    }
}

#[tokio::test]
async fn agent_can_use_a_system_trigger_only_for_the_cancellation_target() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let mut workflow = system_only_fail_edge_workflow();
    workflow.cancellation_state = Some("failed".to_owned());
    workflow.states[1].kind = StateKind::Terminal;
    seed_custom_workflow_task(&db, &task_id, "working", &workflow).await;
    let eng = engine(Arc::clone(&db), event_bus);

    let result = eng
        .workflow_execution()
        .transition(
            &task_id,
            "failed",
            1,
            &workflow,
            &api_types::Actor::agent("project-agent"),
            "cancelled through the scoped Task command",
            false,
            Default::default(),
        )
        .await
        .expect("an authorized Agent cancellation can reach the cancellation target");

    assert_eq!(result.task.status, "failed");
    let triggered_by: String = sqlx::query_scalar(
        "SELECT triggered_by FROM transition_log WHERE task_id = ? ORDER BY rowid DESC LIMIT 1",
    )
    .bind(&task_id)
    .fetch_one(db.pool())
    .await
    .expect("transition audit row");
    assert_eq!(triggered_by, "agent:project-agent");
}

#[tokio::test]
async fn override_not_granted_to_agents_or_system() {
    // Delta: Override authority is not granted to agents or system
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workflow = missing_edge_workflow();
    let eng = engine(Arc::clone(&db), event_bus);

    for (actor, label) in [
        (
            api_types::Actor::system(api_types::SystemComponent::General),
            "system",
        ),
        (api_types::Actor::agent("unit"), "agent"),
    ] {
        let task_id = new_uuid_v4();
        seed_custom_workflow_task(&db, &task_id, "working", &workflow).await;
        let result = eng
            .workflow_execution()
            .transition(
                &task_id,
                "done",
                1,
                &workflow,
                &actor,
                "should not override",
                false,
                Default::default(),
            )
            .await;
        assert!(
            matches!(
                result,
                Err(ServiceError::Db(db::DbError::InvalidTransition))
            ),
            "{label} actor should be rejected on missing edge without override"
        );
    }
}

#[tokio::test]
async fn override_to_undefined_target_rejected_with_enumerated_states() {
    // Delta: Target state not in workflow
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow = missing_edge_workflow();
    seed_custom_workflow_task(&db, &task_id, "working", &workflow).await;
    let eng = engine(Arc::clone(&db), event_bus);

    let result = eng
        .workflow_execution()
        .transition(
            &task_id,
            "nonexistent",
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "invalid target",
            false,
            Default::default(),
        )
        .await;

    match result {
        Err(ServiceError::InvalidOperation { message }) => {
            assert!(
                message.contains("not defined in workflow"),
                "expected undefined-state message, got: {message}"
            );
            assert!(
                message.contains("working"),
                "expected defined state 'working' in message, got: {message}"
            );
            assert!(
                message.contains("done"),
                "expected defined state 'done' in message, got: {message}"
            );
        }
        Ok(_) => panic!("expected undefined target rejection, got Ok"),
        Err(other) => panic!("expected undefined target rejection, got {other:?}"),
    }
}

#[tokio::test]
async fn custom_workflow_lacking_review_rejects_with_enumerated_states() {
    // Delta: Custom project workflow lacks the requested target
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow = workflow_without_review_state();
    seed_custom_workflow_task(&db, &task_id, "working", &workflow).await;
    let eng = engine(Arc::clone(&db), event_bus);

    let result = eng
        .workflow_execution()
        .transition(
            &task_id,
            "review",
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "request review",
            false,
            Default::default(),
        )
        .await;

    match result {
        Err(ServiceError::InvalidOperation { message }) => {
            assert!(
                message.contains("not defined in workflow"),
                "expected undefined-state message, got: {message}"
            );
            for state_name in ["pending", "working", "shipped"] {
                assert!(
                    message.contains(state_name),
                    "expected defined state '{state_name}' in message, got: {message}"
                );
            }
            assert!(
                !message
                    .split("defined states are: ")
                    .nth(1)
                    .unwrap_or("")
                    .contains("review"),
                "review must not appear among defined states, got: {message}"
            );
        }
        Ok(_) => panic!("expected review target rejection, got Ok"),
        Err(other) => panic!("expected review target rejection, got {other:?}"),
    }
}

#[tokio::test]
async fn content_guard_blocks_user_override() {
    // Delta: A content guard may still block a user override
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow = WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            with_trigger(
                state(
                    "working",
                    StateKind::Active,
                    None,
                    StateHooks {
                        before_exit: vec![hook(
                            "require_upstream_roles_completed",
                            FailurePolicy::Block,
                        )],
                        ..StateHooks::default()
                    },
                ),
                WorkflowTrigger::Accept,
                "paused",
            ),
            state("paused", StateKind::Custom, None, StateHooks::default()),
            with_trigger(
                state(
                    default_states::PLANNING,
                    StateKind::Gate,
                    Some(default_roles::PLANNER),
                    StateHooks::default(),
                ),
                WorkflowTrigger::Accept,
                "done",
            ),
            state("done", StateKind::Terminal, None, StateHooks::default()),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    };
    seed_custom_workflow_task(&db, &task_id, "working", &workflow).await;
    assign_agent_role_without_agent(&db, &task_id, default_roles::PLANNER).await;
    let eng = engine(Arc::clone(&db), event_bus);

    let result = eng
        .workflow_execution()
        .transition(
            &task_id,
            "done",
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "override blocked by guard",
            false,
            Default::default(),
        )
        .await;

    match result {
        Err(ServiceError::GuardRejection { guard, reason: _ }) => {
            assert_eq!(guard, "require_upstream_roles_completed");
        }
        Ok(_) => panic!("expected guard rejection, got Ok"),
        Err(other) => panic!("expected guard rejection, got {other:?}"),
    }

    let task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(task.status, "working");
}

#[tokio::test]
async fn version_conflict_still_applies_to_override() {
    // Delta: Version conflict still applies to override
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow = missing_edge_workflow();
    seed_custom_workflow_task(&db, &task_id, "working", &workflow).await;
    let eng = engine(Arc::clone(&db), event_bus);

    let result = eng
        .workflow_execution()
        .transition(
            &task_id,
            "done",
            2,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "stale version",
            false,
            Default::default(),
        )
        .await;

    assert!(
        matches!(result, Err(ServiceError::Db(db::DbError::VersionConflict))),
        "expected version conflict"
    );
}

#[tokio::test]
async fn override_is_auditable() {
    // Delta: Override is auditable
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow = missing_edge_workflow();
    seed_custom_workflow_task(&db, &task_id, "working", &workflow).await;
    let reason = "audit this override";
    let eng = engine(Arc::clone(&db), event_bus);

    eng.workflow_execution()
        .transition(
            &task_id,
            "done",
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            reason,
            false,
            Default::default(),
        )
        .await
        .expect("override transition succeeds");

    let logs = TransitionLogRepo::list_by_task(&*db, &task_id)
        .await
        .expect("transition logs load");
    let latest = logs
        .iter()
        .find(|entry| entry.to_state == "done" && !entry.rejection)
        .expect("successful override transition log exists");
    assert_eq!(latest.triggered_by, "user:override:test");
    assert_eq!(latest.trigger_reason, reason);
}

#[tokio::test]
async fn subtask_user_override_into_review_no_workspace_no_reviewer_completes() {
    // Task 6.1: no workspace, no reviewer — subtask user override into review must not panic
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let parent_id = new_uuid_v4();
    let subtask_id = new_uuid_v4();
    seed_parent_and_subtask(
        &db,
        &parent_id,
        &subtask_id,
        default_states::IN_PROGRESS,
        None,
    )
    .await;
    let workflow = default_workflow::default_workflow();
    let eng = engine(Arc::clone(&db), event_bus);

    let result = eng
        .workflow_execution()
        .transition(
            &subtask_id,
            default_states::REVIEW,
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "subtask into review",
            false,
            Default::default(),
        )
        .await
        .expect("subtask user override into review completes without panic");

    assert_eq!(result.task.status, default_states::REVIEW);
}

#[tokio::test]
async fn subtask_user_override_into_review_with_ci_steps_completes_or_fails_gracefully() {
    // Task 6.1: CI step configured — must not panic; may fail with ServiceError only
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let parent_id = new_uuid_v4();
    let subtask_id = new_uuid_v4();
    seed_parent_and_subtask(
        &db,
        &parent_id,
        &subtask_id,
        default_states::IN_PROGRESS,
        Some(r#"{"review":{"ci_steps":["test -d ."]}}"#.to_owned()),
    )
    .await;
    let workflow = default_workflow::default_workflow();
    let eng = engine(Arc::clone(&db), event_bus);

    let result = eng
        .workflow_execution()
        .transition(
            &subtask_id,
            default_states::REVIEW,
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "subtask review with ci",
            false,
            Default::default(),
        )
        .await;

    match result {
        Ok(transition) => assert_eq!(transition.task.status, default_states::REVIEW),
        Err(error) => assert!(
            matches!(
                error,
                ServiceError::GuardRejection { .. }
                    | ServiceError::InvalidOperation { .. }
                    | ServiceError::Db(_)
            ),
            "CI-configured subtask review must fail via ServiceError, not panic: {error:?}"
        ),
    }
}

#[tokio::test]
async fn subtask_user_override_into_review_empty_ci_auto_passes() {
    // Task 6.1: empty CI auto-passes — system unconfigured review skips; user stays in review
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let parent_id = new_uuid_v4();
    let subtask_id = new_uuid_v4();
    seed_parent_and_subtask(
        &db,
        &parent_id,
        &subtask_id,
        default_states::IN_PROGRESS,
        None,
    )
    .await;
    let workflow = default_workflow::default_workflow();
    let eng = engine(Arc::clone(&db), event_bus);

    let result = eng
        .workflow_execution()
        .transition(
            &subtask_id,
            default_states::REVIEW,
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "subtask review no ci",
            false,
            Default::default(),
        )
        .await
        .expect("subtask user override into review with empty CI completes");

    assert_eq!(result.task.status, default_states::REVIEW);
}

#[tokio::test]
async fn subtask_user_override_into_merging_without_merge_service_completes() {
    // Task 6.1: merge service absent — subtask user override into merging must not panic
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let parent_id = new_uuid_v4();
    let subtask_id = new_uuid_v4();
    seed_parent_and_subtask(
        &db,
        &parent_id,
        &subtask_id,
        default_states::IN_PROGRESS,
        None,
    )
    .await;
    let workflow = default_workflow::default_workflow();
    let eng = engine(Arc::clone(&db), event_bus);

    let result = eng
        .workflow_execution()
        .transition(
            &subtask_id,
            default_states::MERGING,
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "subtask into merging",
            false,
            Default::default(),
        )
        .await;

    match result {
        Ok(transition) => assert!(
            transition.task.status == default_states::MERGING
                || transition.task.status == default_states::DONE,
            "merge hook may cascade when merge service is absent, got {}",
            transition.task.status
        ),
        Err(error) => assert!(
            matches!(
                error,
                ServiceError::GuardRejection { .. }
                    | ServiceError::InvalidOperation { .. }
                    | ServiceError::Db(_)
            ),
            "subtask merge override must fail via ServiceError, not panic: {error:?}"
        ),
    }
}

#[tokio::test]
async fn system_review_ci_placement_error_retries_without_review_rejection() {
    use crate::workspace_backend as ws;
    struct UnreachableOnce(std::sync::atomic::AtomicBool);
    #[async_trait::async_trait]
    impl ws::WorkspaceBackend for UnreachableOnce {
        async fn prepare(
            &self,
            _: &db::WorkspacePlacement,
            _: &ws::PrepareSpec,
        ) -> ws::Result<ws::PreparedWorkspace> {
            unreachable!()
        }
        async fn describe(&self, _: &db::WorkspacePlacement) -> ws::Result<ws::WorkspaceState> {
            unreachable!()
        }
        async fn run(
            &self,
            _: &db::WorkspacePlacement,
            _: &ws::RunSpec,
        ) -> ws::Result<ws::RunResult> {
            if self.0.swap(false, std::sync::atomic::Ordering::SeqCst) {
                Err(ws::WorkspaceBackendError::OwnerUnreachable {
                    daemon_id: "review-owner".into(),
                })
            } else {
                Ok(ws::RunResult {
                    exit_code: 0,
                    stdout_tail: String::new(),
                    stderr_tail: String::new(),
                    duration_ms: 1,
                })
            }
        }
        async fn diff(&self, _: &db::WorkspacePlacement, _: &ws::DiffSpec) -> ws::Result<ws::Diff> {
            unreachable!()
        }
        async fn read(&self, _: &db::WorkspacePlacement, _: &str, _: u64) -> ws::Result<Vec<u8>> {
            unreachable!()
        }
        async fn merge(
            &self,
            _: &db::WorkspacePlacement,
            _: &ws::MergeSpec,
        ) -> ws::Result<ws::MergeOutcome> {
            unreachable!()
        }
        async fn reset(
            &self,
            _: &db::WorkspacePlacement,
            _: &ws::ResetSpec,
        ) -> ws::Result<ws::PreparedWorkspace> {
            unreachable!()
        }
        async fn cleanup(&self, _: &db::WorkspacePlacement) -> ws::Result<ws::CleanupAck> {
            unreachable!()
        }
        async fn harvest_outbox(
            &self,
            _: &db::WorkspacePlacement,
            _: &str,
        ) -> ws::Result<ws::OutboxHarvest> {
            unreachable!()
        }
        async fn consume_outbox(&self, _: &db::WorkspacePlacement, _: &str) -> ws::Result<()> {
            unreachable!()
        }
    }
    // Budget 1 would immediately block if this outage were a CI rejection.
    let mut fixture = failed_ci_fixture(1, FailurePolicy::Block).await;
    fixture
        .workflow
        .states
        .iter_mut()
        .find(|state| state.name == "review")
        .unwrap()
        .gate_config
        .as_mut()
        .unwrap()
        .requires_user_approval = Some(true);
    sqlx::query("UPDATE project SET workflow_definition = ? WHERE id = ?")
        .bind(serde_json::to_string(&fixture.workflow).unwrap())
        .bind(&fixture.task.project_id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    fixture.engine.workspace_backend_router = Arc::new(ws::WorkspaceBackendRouter::new(Arc::new(
        UnreachableOnce(std::sync::atomic::AtomicBool::new(true)),
    )));
    let result = fixture
        .engine
        .workflow_execution()
        .transition_with_authority(
            &fixture.task.id,
            "review",
            fixture.task.version,
            &fixture.workflow,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "worker completed",
            false,
            fixture.workflow_authority().await,
        )
        .await
        .unwrap();
    let result = drain_result(fixture.engine.clone(), result).await;
    assert_eq!(result.task.status, "review");
    assert!(result.review.is_none());
    let annotation: serde_json::Value =
        serde_json::from_str(result.task.error_annotation.as_deref().unwrap()).unwrap();
    assert_eq!(annotation["blocking_reason"], "review_ci_infrastructure");
    assert!(annotation["message"]
        .as_str()
        .unwrap()
        .contains("owner_unreachable"));
    let entries = TransitionLogRepo::list_by_task(&*fixture.db, &fixture.task.id)
        .await
        .unwrap();
    assert_eq!(
        crate::task_diagnostics::audit_gate_rejections_since_boundary(&entries, "review"),
        0
    );
    let retried = fixture
        .engine
        .workflow_execution()
        .retry_entry_barrier_with_authority(
            &fixture.task.id,
            result.task.version,
            &fixture.workflow,
            &api_types::Actor::system(api_types::SystemComponent::TaskDispatcher),
            "owner reconnected",
            fixture.workflow_authority().await,
        )
        .await
        .unwrap();
    let retried = drain_result(fixture.engine.clone(), retried).await;
    assert!(retried.task.error_annotation.is_none());
    assert!(retried.task.entry_barrier_json.is_none());
    assert_eq!(
        retried.review.unwrap().status,
        db::ReviewStatus::AwaitingHuman
    );
    let entries = TransitionLogRepo::list_by_task(&*fixture.db, &fixture.task.id)
        .await
        .unwrap();
    assert_eq!(
        crate::task_diagnostics::audit_gate_rejections_since_boundary(&entries, "review"),
        0
    );
}

#[tokio::test]
async fn system_review_ci_infrastructure_retry_is_capped_and_does_not_create_attempts() {
    use crate::workspace_backend as ws;
    struct ReviewCiFault {
        db: Arc<db::SqliteDb>,
        task_id: String,
        authority_loss: bool,
    }
    #[async_trait::async_trait]
    impl ws::WorkspaceBackend for ReviewCiFault {
        async fn prepare(
            &self,
            _: &db::WorkspacePlacement,
            _: &ws::PrepareSpec,
        ) -> ws::Result<ws::PreparedWorkspace> {
            unreachable!()
        }
        async fn describe(&self, _: &db::WorkspacePlacement) -> ws::Result<ws::WorkspaceState> {
            unreachable!()
        }
        async fn run(
            &self,
            _: &db::WorkspacePlacement,
            _: &ws::RunSpec,
        ) -> ws::Result<ws::RunResult> {
            if self.authority_loss {
                sqlx::query("UPDATE task SET version = version + 1 WHERE id = ?")
                    .bind(&self.task_id)
                    .execute(self.db.pool())
                    .await
                    .unwrap();
                Ok(ws::RunResult {
                    exit_code: 0,
                    stdout_tail: String::new(),
                    stderr_tail: String::new(),
                    duration_ms: 1,
                })
            } else {
                sqlx::query("UPDATE task SET metadata_json = json_set(COALESCE(metadata_json, '{}'), '$.concurrent_ci_note', 'retained') WHERE id = ?")
                    .bind(&self.task_id).execute(self.db.pool()).await.unwrap();
                Err(ws::WorkspaceBackendError::OwnerUnreachable {
                    daemon_id: "review-owner".into(),
                })
            }
        }

        async fn diff(&self, _: &db::WorkspacePlacement, _: &ws::DiffSpec) -> ws::Result<ws::Diff> {
            unreachable!()
        }
        async fn read(&self, _: &db::WorkspacePlacement, _: &str, _: u64) -> ws::Result<Vec<u8>> {
            unreachable!()
        }
        async fn merge(
            &self,
            _: &db::WorkspacePlacement,
            _: &ws::MergeSpec,
        ) -> ws::Result<ws::MergeOutcome> {
            unreachable!()
        }
        async fn reset(
            &self,
            _: &db::WorkspacePlacement,
            _: &ws::ResetSpec,
        ) -> ws::Result<ws::PreparedWorkspace> {
            unreachable!()
        }
        async fn cleanup(&self, _: &db::WorkspacePlacement) -> ws::Result<ws::CleanupAck> {
            unreachable!()
        }
        async fn harvest_outbox(
            &self,
            _: &db::WorkspacePlacement,
            _: &str,
        ) -> ws::Result<ws::OutboxHarvest> {
            unreachable!()
        }
        async fn consume_outbox(&self, _: &db::WorkspacePlacement, _: &str) -> ws::Result<()> {
            unreachable!()
        }
    }

    let mut fixture = failed_ci_fixture(1, FailurePolicy::Block).await;
    fixture.engine.workspace_backend_router =
        Arc::new(ws::WorkspaceBackendRouter::new(Arc::new(ReviewCiFault {
            db: fixture.db.clone(),
            task_id: fixture.task.id.clone(),
            authority_loss: false,
        })));
    let mut result = fixture
        .engine
        .workflow_execution()
        .transition_with_authority(
            &fixture.task.id,
            "review",
            fixture.task.version,
            &fixture.workflow,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "worker completed",
            false,
            fixture.workflow_authority().await,
        )
        .await
        .unwrap();
    result = drain_result(fixture.engine.clone(), result).await;
    let placement = db::WorkspacePlacementRepo::get_for_task(&*fixture.db, &fixture.task.id)
        .await
        .unwrap()
        .unwrap();
    sqlx::query("UPDATE workspace_placement SET state = 'disconnected' WHERE id = ?")
        .bind(&placement.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    for _ in 0..6 {
        result = fixture
            .engine
            .workflow_execution()
            .retry_entry_barrier_with_authority(
                &fixture.task.id,
                result.task.version,
                &fixture.workflow,
                &api_types::Actor::system(api_types::SystemComponent::TaskDispatcher),
                "same owner remains offline",
                fixture.workflow_authority().await,
            )
            .await
            .unwrap();
        result = drain_result(fixture.engine.clone(), result).await;
        assert!(
            result.task.blocked_json.is_none(),
            "an owner outage waits for max_disconnect"
        );
        let barrier: serde_json::Value =
            serde_json::from_str(result.task.entry_barrier_json.as_deref().unwrap()).unwrap();
        assert_eq!(barrier["status"], "blocked");
        assert_eq!(
            db::budget::spent(
                fixture.db.pool(),
                &fixture.task.id,
                db::budget::Kind::ReviewCiInfrastructure.key()
            )
            .await
            .unwrap(),
            1,
            "disconnected attempts do not spend the infrastructure cap"
        );
    }
    sqlx::query("UPDATE workspace_placement SET state = 'ready' WHERE id = ?")
        .bind(&placement.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    for attempt in 1..=5 {
        let barrier: serde_json::Value =
            serde_json::from_str(result.task.entry_barrier_json.as_deref().unwrap()).unwrap();
        assert_eq!(barrier["status"], "blocked");
        assert_eq!(
            db::budget::spent(
                fixture.db.pool(),
                &fixture.task.id,
                db::budget::Kind::ReviewCiInfrastructure.key()
            )
            .await
            .unwrap(),
            attempt
        );
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM review WHERE task_id = ?")
            .bind(&fixture.task.id)
            .fetch_one(fixture.db.pool())
            .await
            .unwrap();
        assert_eq!(count, 0, "an unreachable owner never ran CI");
        if attempt == 5 {
            break;
        }
        let deferred = crate::deferred_dispatch::pending_until(&result.task).unwrap();
        let delay = chrono::DateTime::parse_from_rfc3339(&deferred.not_before)
            .unwrap()
            .with_timezone(&chrono::Utc)
            - chrono::Utc::now();
        assert!(delay.num_milliseconds() > (5 * (1_i64 << (attempt - 1)) - 1) * 1000);
        result = fixture
            .engine
            .workflow_execution()
            .retry_entry_barrier_with_authority(
                &fixture.task.id,
                result.task.version,
                &fixture.workflow,
                &api_types::Actor::system(api_types::SystemComponent::TaskDispatcher),
                "retry owner",
                fixture.workflow_authority().await,
            )
            .await
            .unwrap();
        result = drain_result(fixture.engine.clone(), result).await;
    }
    let annotation: serde_json::Value =
        serde_json::from_str(result.task.error_annotation.as_deref().unwrap()).unwrap();
    assert_eq!(
        annotation["blocking_reason"],
        "review_ci_infrastructure_exhausted"
    );
    let blocked: api_types::InterruptionMetadata =
        serde_json::from_str(result.task.blocked_json.as_deref().unwrap()).unwrap();
    assert_eq!(
        blocked.kind,
        Some(api_types::FailureKind::BeforeWorkHookFailed)
    );
    assert!(!blocked.reason.is_empty());
    assert!(!blocked.created_at.is_empty());
    assert!(crate::deferred_dispatch::pending_until(&result.task).is_none());
    let metadata: serde_json::Value =
        serde_json::from_str(result.task.metadata_json.as_deref().unwrap()).unwrap();
    assert_eq!(metadata["concurrent_ci_note"], "retained");
    let feed = crate::AttentionService::new(fixture.db.clone())
        .mission_control_home("test-user", None, 50)
        .await
        .unwrap();
    let item = feed
        .needs_attention
        .iter()
        .find(|item| item.dedupe_key == format!("review-ci:{}", fixture.task.id))
        .expect("review CI park is visible in Mission Control");
    assert_eq!(item.category, api_types::AttentionCategory::ExecutionFailed);
    assert!(item.details["cause"]
        .as_str()
        .unwrap()
        .contains("review_ci_infrastructure_exhausted"));
    let entries = TransitionLogRepo::list_by_task(&*fixture.db, &fixture.task.id)
        .await
        .unwrap();
    assert_eq!(
        crate::task_diagnostics::audit_gate_rejections_since_boundary(&entries, "review"),
        0
    );
}

#[tokio::test]
async fn system_review_ci_authority_loss_keeps_base_cancellation_routing() {
    use crate::workspace_backend as ws;
    struct ReviewCiFault {
        db: Arc<db::SqliteDb>,
        task_id: String,
        authority_loss: bool,
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl ws::WorkspaceBackend for ReviewCiFault {
        async fn prepare(
            &self,
            _: &db::WorkspacePlacement,
            _: &ws::PrepareSpec,
        ) -> ws::Result<ws::PreparedWorkspace> {
            unreachable!()
        }
        async fn describe(&self, _: &db::WorkspacePlacement) -> ws::Result<ws::WorkspaceState> {
            unreachable!()
        }
        async fn run(
            &self,
            _: &db::WorkspacePlacement,
            _: &ws::RunSpec,
        ) -> ws::Result<ws::RunResult> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.authority_loss {
                sqlx::query("UPDATE review SET updated_at='2099-01-01T00:00:00Z' WHERE task_id=? AND status='running'")
                    .bind(&self.task_id)
                    .execute(self.db.pool())
                    .await
                    .unwrap();
                Ok(ws::RunResult {
                    exit_code: 0,
                    stdout_tail: String::new(),
                    stderr_tail: String::new(),
                    duration_ms: 1,
                })
            } else {
                Err(ws::WorkspaceBackendError::OwnerUnreachable {
                    daemon_id: "review-owner".into(),
                })
            }
        }

        async fn diff(&self, _: &db::WorkspacePlacement, _: &ws::DiffSpec) -> ws::Result<ws::Diff> {
            unreachable!()
        }
        async fn read(&self, _: &db::WorkspacePlacement, _: &str, _: u64) -> ws::Result<Vec<u8>> {
            unreachable!()
        }
        async fn merge(
            &self,
            _: &db::WorkspacePlacement,
            _: &ws::MergeSpec,
        ) -> ws::Result<ws::MergeOutcome> {
            unreachable!()
        }
        async fn reset(
            &self,
            _: &db::WorkspacePlacement,
            _: &ws::ResetSpec,
        ) -> ws::Result<ws::PreparedWorkspace> {
            unreachable!()
        }
        async fn cleanup(&self, _: &db::WorkspacePlacement) -> ws::Result<ws::CleanupAck> {
            unreachable!()
        }
        async fn harvest_outbox(
            &self,
            _: &db::WorkspacePlacement,
            _: &str,
        ) -> ws::Result<ws::OutboxHarvest> {
            unreachable!()
        }
        async fn consume_outbox(&self, _: &db::WorkspacePlacement, _: &str) -> ws::Result<()> {
            unreachable!()
        }
    }

    let mut fixture = failed_ci_fixture(1, FailurePolicy::Block).await;
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    fixture.engine.workspace_backend_router =
        Arc::new(ws::WorkspaceBackendRouter::new(Arc::new(ReviewCiFault {
            db: fixture.db.clone(),
            task_id: fixture.task.id.clone(),
            authority_loss: true,
            calls: calls.clone(),
        })));
    let result = fixture
        .engine
        .workflow_execution()
        .transition_with_authority(
            &fixture.task.id,
            "review",
            fixture.task.version,
            &fixture.workflow,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "worker completed",
            false,
            fixture.workflow_authority().await,
        )
        .await;
    let result = result.unwrap();
    let result = drain_result(fixture.engine.clone(), result).await;
    assert_eq!(
        calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "injected CI backend runs"
    );
    assert_eq!(result.task.status, "in_progress");
    assert!(result.task.entry_barrier_json.is_none());
    let task = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(!task
        .error_annotation
        .as_deref()
        .is_some_and(|raw| raw.contains("review_ci_")));
    assert!(crate::deferred_dispatch::pending_until(&task).is_none());
    let status: String = sqlx::query_scalar(
        "SELECT status FROM review WHERE task_id = ? ORDER BY attempt_number DESC LIMIT 1",
    )
    .bind(&fixture.task.id)
    .fetch_one(fixture.db.pool())
    .await
    .unwrap();
    assert_eq!(status, "cancelled");
}

#[tokio::test]
async fn dispatch_failure_preserves_other_annotations_and_wakes_only_its_upgrade_owner() {
    let db = sqlite_db().await;
    for kind in [
        "manual_stop",
        "workspace_error",
        "agent_timeout",
        "recovery_required",
        "workspace_reset_required",
        "max_turns_exceeded",
        "before_work_hook_failed",
        "before_work_hook_timeout",
    ] {
        let task_id = new_uuid_v4();
        seed_project_repo_and_task(&db, &task_id, "todo").await;
        let original = json!({"type":kind,"message":"keep"}).to_string();
        sqlx::query("UPDATE task SET error_annotation = ? WHERE id = ?")
            .bind(&original)
            .bind(&task_id)
            .execute(db.pool())
            .await
            .unwrap();
        super::annotate_dispatch_failure(&db, &task_id, "in_progress", "upgrade", None)
            .await
            .unwrap();
        assert_eq!(
            TaskRepo::get_by_id(&db, &task_id, false)
                .await
                .unwrap()
                .unwrap()
                .error_annotation
                .as_deref(),
            Some(original.as_str())
        );
    }
    let task_id = new_uuid_v4();
    seed_project_repo_and_task(&db, &task_id, "todo").await;
    sqlx::query("UPDATE task SET metadata_json = ? WHERE id = ?")
        .bind(
            json!({"owner_wait":{"daemon_id":"owner","started_at":"1970-01-01T00:00:00Z"}})
                .to_string(),
        )
        .bind(&task_id)
        .execute(db.pool())
        .await
        .unwrap();
    let error = ServiceError::DaemonUpgradeRequired {
        daemon_id: "owner".into(),
    };
    super::annotate_upgrade_dispatch_refusal(&db, &task_id, "in_progress", &error)
        .await
        .unwrap();
    super::annotate_dispatch_failure(&db, &task_id, "in_progress", &error.to_string(), None)
        .await
        .unwrap();
    let observed = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    let metadata: serde_json::Value =
        serde_json::from_str(observed.metadata_json.as_deref().unwrap()).unwrap();
    assert!(metadata.get("owner_wait").is_none());
    super::wake_upgraded_daemon_tasks(&db, &["other-owner".into()])
        .await
        .unwrap();
    assert!(TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .unwrap()
        .unwrap()
        .error_annotation
        .is_some());
    super::wake_upgraded_daemon_tasks(&db, &["owner".into()])
        .await
        .unwrap();
    assert!(TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .unwrap()
        .unwrap()
        .error_annotation
        .is_none());
}

#[tokio::test]
async fn dispatch_failure_overwrites_nonblocking_and_malformed_annotations() {
    let db = sqlite_db().await;
    for original in [
        r#"{"type":"old_notice"}"#,
        r#"{"message":"old"}"#,
        "{broken",
    ] {
        let task_id = new_uuid_v4();
        seed_project_repo_and_task(&db, &task_id, "todo").await;
        sqlx::query("UPDATE task SET error_annotation = ? WHERE id = ?")
            .bind(original)
            .bind(&task_id)
            .execute(db.pool())
            .await
            .unwrap();
        super::annotate_dispatch_failure(&db, &task_id, "in_progress", "new failure", None)
            .await
            .unwrap();
        let task = TaskRepo::get_by_id(&db, &task_id, false)
            .await
            .unwrap()
            .unwrap();
        let annotation: serde_json::Value =
            serde_json::from_str(task.error_annotation.as_deref().unwrap()).unwrap();
        assert_eq!(annotation["type"], "dispatch_failed");
        assert_eq!(annotation["message"], "new failure");
    }
}

#[tokio::test]
async fn dispatch_failure_upgrade_metadata_wakes_with_blocking_annotation_and_skips_deleted_tasks()
{
    let db = sqlite_db().await;
    for deleted in [false, true] {
        let task_id = new_uuid_v4();
        seed_project_repo_and_task(&db, &task_id, "todo").await;
        let original = json!({"type":"manual_stop","message":"keep"}).to_string();
        sqlx::query("UPDATE task SET error_annotation = ? WHERE id = ?")
            .bind(&original)
            .bind(&task_id)
            .execute(db.pool())
            .await
            .unwrap();
        let task = TaskRepo::get_by_id(&db, &task_id, false)
            .await
            .unwrap()
            .unwrap();
        crate::deferred_dispatch::record_dispatch_disposition(&db, &task, "coder", "upgrade")
            .await
            .unwrap();
        super::annotate_upgrade_dispatch_refusal(
            &db,
            &task_id,
            "in_progress",
            &ServiceError::DaemonUpgradeRequired {
                daemon_id: "owner".into(),
            },
        )
        .await
        .unwrap();
        if deleted {
            sqlx::query("UPDATE task SET deleted_at = ? WHERE id = ?")
                .bind(now_rfc3339())
                .bind(&task_id)
                .execute(db.pool())
                .await
                .unwrap();
        }
        super::wake_upgraded_daemon_tasks(&db, &["owner".into(), "other-owner".into()])
            .await
            .unwrap();
        let task = TaskRepo::get_by_id(&db, &task_id, true)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.error_annotation.as_deref(), Some(original.as_str()));
        let metadata: serde_json::Value = task
            .metadata_json
            .as_deref()
            .map(|raw| serde_json::from_str(raw).unwrap())
            .unwrap_or(json!({}));
        assert_eq!(metadata.get("daemon_upgrade_refusal").is_some(), deleted);
        assert_eq!(metadata.get("dispatch_disposition").is_some(), deleted);
    }
}

#[tokio::test]
async fn dispatch_failure_upgrade_wake_preserves_a_concurrent_manual_deferral() {
    let db = sqlite_db().await;
    let task_id = new_uuid_v4();
    seed_project_repo_and_task(&db, &task_id, "todo").await;
    super::annotate_upgrade_dispatch_refusal(
        &db,
        &task_id,
        "in_progress",
        &ServiceError::DaemonUpgradeRequired {
            daemon_id: "owner".into(),
        },
    )
    .await
    .unwrap();
    let observed = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    // Metadata writes need not bump Task.version. The wake fences those too.
    crate::deferred_dispatch::set(
        &db,
        &observed,
        "in_progress",
        "2099-01-01T00:00:00Z",
        "manual action",
    )
    .await
    .unwrap();
    assert!(!super::clear_upgrade_dispatch_refusal(&db, &observed)
        .await
        .unwrap());
    let current = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        crate::deferred_dispatch::pending_until(&current)
            .unwrap()
            .reason,
        "manual action"
    );
    assert!(!super::clear_upgrade_dispatch_refusal(&db, &current)
        .await
        .unwrap());
    super::wake_upgraded_daemon_tasks(&db, &["owner".into()])
        .await
        .unwrap();
    let current = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(crate::deferred_dispatch::pending_until(&current).is_some());
    crate::deferred_dispatch::clear(&db, &current)
        .await
        .unwrap();
    let current = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(super::clear_upgrade_dispatch_refusal(&db, &current)
        .await
        .unwrap());
    let cleared = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    crate::deferred_dispatch::set(
        &db,
        &cleared,
        "in_progress",
        "2099-01-01T00:00:00Z",
        "new manual action",
    )
    .await
    .unwrap();
    let current = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(!super::clear_upgrade_dispatch_refusal(&db, &current)
        .await
        .unwrap());
    assert!(crate::deferred_dispatch::pending_until(
        &TaskRepo::get_by_id(&db, &task_id, false)
            .await
            .unwrap()
            .unwrap()
    )
    .is_some());
}

/// Audit 2.3a: with no ci_steps and no reviewer (the default workflow's
/// "unconfigured review"), one target-moved refresh round repeats
/// review -> merging inside one chain without creating a Review row, so the
/// loop detector parks a legitimate integration retry. The old depth guard
/// allowed two such rounds.
#[tokio::test]
async fn audit_23a_unconfigured_review_target_moved_round_keeps_running() {
    use db::TaskStepRepo;
    let db = Arc::new(sqlite_db().await);
    let bus = Arc::new(EventBus::new(256));
    let id = "audit-unconfigured-refresh";
    seed_project_repo_and_task(&db, id, default_states::IN_PROGRESS).await;
    let workflow = default_workflow::default_workflow();
    let driver = engine(Arc::clone(&db), Arc::clone(&bus));

    // Real producer: unconfigured review cascades review -> merging (step 1).
    let result = driver
        .workflow_execution()
        .transition(
            id,
            default_states::REVIEW,
            1,
            &workflow,
            &api_types::Actor::system(api_types::SystemComponent::General),
            "agent completed",
            false,
            Default::default(),
        )
        .await
        .unwrap();
    assert_eq!(result.pending_steps, 1);
    let merging = drain(driver.clone(), id).await;
    assert_eq!(merging.status, default_states::MERGING);
    let step1 = db.task_steps(id).await.unwrap().last().cloned().unwrap();
    assert_eq!(step1.status, "done");

    // RunMerge's TargetMoved outcome (target_moved_result) returns exactly
    // this cascade from the step-1 execution; enqueue it the way the engine
    // does, with step 1 as parent.
    let reason = "main advanced; rebased onto main, re-review required".to_owned();
    let input = driver
        .workflow_execution()
        .cascade_step_input(
            &merging,
            &workflow,
            default_states::MERGE_FAILED.into(),
            reason,
            api_types::TransitionBridge::new(api_types::TransitionBridgeKind::TargetMovedRebase),
            false,
            false,
            None,
            Some(&step1),
            "audit:target-moved".into(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(input.chain_id, step1.chain_id);
    db.enqueue_step(&input).await.unwrap();
    db.ready_step(&input.id).await.unwrap();

    // Real producers from here: merge_failed bridge -> review, then the
    // unconfigured review cascades review -> merging again.
    let settled = drain(driver.clone(), id).await;
    let steps = db.task_steps(id).await.unwrap();
    println!(
        "audit steps: {:?}",
        steps
            .iter()
            .map(|s| (
                s.expected_status.as_str(),
                s.status.as_str(),
                s.chain_position
            ))
            .collect::<Vec<_>>()
    );
    assert_eq!(settled.status, default_states::MERGING);
    assert_eq!(steps.last().unwrap().status, "done");
    assert!(settled.blocked_json.is_none());
    let snapshot = driver
        .task_action_snapshot(
            id,
            &api_types::Actor::user(api_types::UserActionSource::Test),
        )
        .await
        .unwrap();
    let offers = crate::available_actions(&snapshot);
    println!(
        "audit offers: {:?} annotation: {:?}",
        offers
            .iter()
            .map(|o| (o.reason.clone(), o.label.clone()))
            .collect::<Vec<_>>(),
        settled.error_annotation
    );
}

#[path = "tests/audit_23a_worker.rs"]
mod audit_worker;

#[path = "tests/durable_23b.rs"]
mod durable_23b;

#[tokio::test]
async fn removed_dispatch_annotation_retries_preserve_a_queued_manual_stop() {
    for clear in [false, true] {
        let db = Arc::new(sqlite_db().await);
        let id = new_uuid_v4();
        seed_project_repo_and_task(&db, &id, "todo").await;
        let original = json!({"type":"dispatch_failed","message":"old refusal"}).to_string();
        sqlx::query("UPDATE task SET error_annotation=? WHERE id=?")
            .bind(&original)
            .bind(&id)
            .execute(db.pool())
            .await
            .unwrap();
        let blocker_id = db
            .enqueue_task_mutation(
                &id,
                db::TaskMutation::Sql {
                    task_id: id.clone(),
                    query: "UPDATE task SET title=title WHERE id=?".into(),
                    arguments: vec![json!(id)],
                },
            )
            .await
            .unwrap();
        let blocker = db
            .claim_step(
                "blocker",
                Some(&id),
                &(chrono::Utc::now() + chrono::Duration::seconds(60)).to_rfc3339(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(blocker.id, blocker_id);
        let activity = db.hold_task_step(&blocker);
        let manual = json!({"type":"manual_stop","message":"owner held"}).to_string();
        db.enqueue_task_mutation(
            &id,
            db::TaskMutation::Sql {
                task_id: id.clone(),
                query: "UPDATE task SET error_annotation=?,version=version+1 WHERE id=?".into(),
                arguments: vec![json!(manual), json!(id)],
            },
        )
        .await
        .unwrap();
        let producer_db = db.clone();
        let producer_id = id.clone();
        let producer = tokio::spawn(async move {
            if clear {
                super::clear_dispatch_failure_annotation(&producer_db, &producer_id, None).await
            } else {
                super::annotate_dispatch_failure(
                    &producer_db,
                    &producer_id,
                    "todo",
                    "new refusal",
                    None,
                )
                .await
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let notify = db.domain_event_notify();
                let changed = notify.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if db.task_steps(&id).await.unwrap().len() == 3 {
                    break;
                }
                changed.await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            TaskRepo::get_by_id(&*db, &id, false)
                .await
                .unwrap()
                .unwrap()
                .error_annotation
                .as_deref(),
            Some(original.as_str())
        );
        db::task_writer::in_task_step(blocker.clone(), db.execute_task_mutation(&blocker))
            .await
            .unwrap();
        db.release_step(&blocker.id, "blocker").await.unwrap();
        drop(activity);
        producer.await.unwrap().unwrap();
        let task = TaskRepo::get_by_id(&*db, &id, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.error_annotation.as_deref(), Some(manual.as_str()));
        assert_eq!(task.version, 2);
        assert!(db
            .task_steps(&id)
            .await
            .unwrap()
            .iter()
            .all(|s| s.status == "done" && s.attempts == 1));
    }
}

#[test]
fn optional_gate_cascade_uses_typed_skip_instead_of_reason_words() {
    let mut source = state("approval", StateKind::Gate, None, StateHooks::default());
    source.gate_config = Some(GateConfig {
        requires_user_approval: Some(true),
        optional_when_unassigned: Some(true),
        reject_target: None,
        max_rejections: None,
        approve_label: None,
        reject_label: None,
    });
    assert!(WorkflowEngine::cascade_allowed(
        &source,
        &api_types::TransitionBridge::new(api_types::TransitionBridgeKind::GateSkipped),
        false
    ));
    assert!(!WorkflowEngine::cascade_allowed(
        &source,
        &api_types::TransitionBridge::default(),
        false
    ));
}

async fn transition_memory_count(db: &SqliteDb, transition_log_id: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM memory_item WHERE source_type = 'transition'
         AND json_extract(metadata_json, '$.source_ref') = ?",
    )
    .bind(transition_log_id)
    .fetch_one(db.pool())
    .await
    .expect("memory items count")
}

/// Failure memory keeps its coverage from typed signals once reason and hook
/// message words stopped counting: a failed hook on an ordinary move, and the
/// dispatch-failure rollback whose only base signal was the word "failed".
#[tokio::test]
async fn failure_memory_records_failed_hook_and_dispatch_rollback_from_typed_evidence() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));

    // A Log-policy hook failure leaves the Task in an ordinary state.
    let task_id = "task-memory-failed-hook";
    seed_project_repo_and_task(&db, task_id, default_states::TODO).await;
    assign_agent_role_without_agent(&db, task_id, default_roles::CODER).await;
    let workflow = WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            with_trigger(
                state(
                    default_states::TODO,
                    StateKind::Initial,
                    None,
                    StateHooks::default(),
                ),
                WorkflowTrigger::Accept,
                default_states::IN_PROGRESS,
            ),
            state(
                default_states::IN_PROGRESS,
                StateKind::Active,
                Some(default_roles::CODER),
                StateHooks {
                    on_enter: vec![hook("notify_role_holder", FailurePolicy::Log)],
                    ..StateHooks::default()
                },
            ),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    };
    let task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .unwrap()
        .unwrap();
    let result = engine(Arc::clone(&db), Arc::clone(&event_bus))
        .workflow_execution()
        .transition(
            task_id,
            default_states::IN_PROGRESS,
            task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "start work",
            false,
            Default::default(),
        )
        .await
        .unwrap();
    drain_result(engine(Arc::clone(&db), Arc::clone(&event_bus)), result).await;
    let logs = TransitionLogRepo::list_by_task(&*db, task_id)
        .await
        .unwrap();
    let entered = logs
        .iter()
        .find(|entry| entry.to_state == default_states::IN_PROGRESS)
        .unwrap();
    assert!(!entered.rejection);
    assert!(hook_results(&db, task_id)
        .await
        .iter()
        .any(|entry| entry.action == "notify_role_holder" && entry.outcome == "failed"));
    assert_eq!(transition_memory_count(&db, &entered.id).await, 1);

    // The rollback cascade's reason is the only base signal; its typed signal
    // is the DispatchFailed interruption the failed dispatch recorded.
    let task_id = "task-memory-dispatch-rollback";
    seed_project_repo_and_task(&db, task_id, default_states::TODO).await;
    assign_agent_role_without_agent(&db, task_id, default_roles::CODER).await;
    // A benign entry hook on the initial state gives the rollback its own
    // hooks step, which is where failure memory is recorded.
    let mut workflow = default_workflow::default_workflow();
    workflow
        .states
        .iter_mut()
        .find(|state| state.name == default_states::TODO)
        .unwrap()
        .hooks
        .on_enter = vec![hook("satisfy_dependents", FailurePolicy::Log)];
    let task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .unwrap()
        .unwrap();
    let result = engine(Arc::clone(&db), Arc::clone(&event_bus))
        .workflow_execution()
        .transition(
            task_id,
            default_states::PLANNING,
            task.version,
            &workflow,
            &api_types::Actor::system(api_types::SystemComponent::TaskDispatcher),
            "scheduled by task dispatcher",
            false,
            Default::default(),
        )
        .await
        .unwrap();
    let settled = drain_result(engine(Arc::clone(&db), Arc::clone(&event_bus)), result).await;
    assert_eq!(settled.task.status, default_states::TODO);
    let logs = TransitionLogRepo::list_by_task(&*db, task_id)
        .await
        .unwrap();
    let rollback = logs
        .iter()
        .find(|entry| {
            entry.from_state == default_states::IN_PROGRESS
                && entry.to_state == default_states::TODO
        })
        .unwrap();
    assert!(!rollback.rejection);
    let rollback_hooks: Vec<HookResultEntry> =
        serde_json::from_str(rollback.hook_results_json.as_deref().unwrap()).unwrap();
    assert!(!rollback_hooks.is_empty());
    assert!(rollback_hooks.iter().all(|entry| entry.outcome != "failed"));
    assert!(settled
        .task
        .error_annotation
        .as_deref()
        .is_some_and(|annotation| annotation.contains("dispatch_failed")));
    assert_eq!(transition_memory_count(&db, &rollback.id).await, 1);
}
