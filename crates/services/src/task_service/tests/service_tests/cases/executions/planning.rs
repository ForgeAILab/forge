//! Planner completion, publication and approval gates.
use super::*;

#[tokio::test]
async fn plan_publication_cleanup_release_preserves_public_task_version() {
    let db = Arc::new(sqlite_db().await);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(
        &db,
        &project_id,
        crate::workflow::default_states::PLANNING.to_owned(),
    )
    .await;
    let execution_id = "execution-with-private-plan-cleanup";
    let cleanup = json!({
        "execution_id": execution_id,
        "state": task.status,
        "state_entry_token": null,
        "project_version": 0,
    });
    let marked = TaskRepo::mutate_metadata(
        &*db,
        &task.id,
        Some(task.version),
        vec![db::TaskMetadataMutation::Set {
            key: "plan_publication_cleanup".to_owned(),
            value: cleanup,
        }],
        &now_rfc3339(),
    )
    .await
    .expect("cleanup marker writes");

    let cleared =
        crate::task_service::execution::clear_plan_publication_cleanup(&db, &marked, execution_id)
            .await
            .expect("cleanup marker clears");

    assert_eq!(
        cleared.version, marked.version,
        "host-private cleanup must not invalidate a client-visible Task version"
    );
    assert!(cleared
        .metadata()
        .expect("metadata parses")
        .extra
        .get("plan_publication_cleanup")
        .is_none());
}

#[tokio::test]
async fn approval_gated_planner_completion_waits_for_human() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let embedded = Arc::new(crate::test_support::TestExecutionSupport::new(Arc::clone(
        &db,
    )));
    let service = TaskService::new(Arc::clone(&db), event_bus)
        .with_task_executor(Arc::new(OutboxPlanExecutor {
            plan: "- [ ] approved implementation\n",
        }))
        .with_provider_credential_env(embedded)
        .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let mut workflow = crate::workflow::default_workflow::default_workflow();
    workflow
        .states
        .iter_mut()
        .find(|state| state.name == crate::workflow::default_states::PLANNING)
        .and_then(|state| state.gate_config.as_mut())
        .expect("planning gate config")
        .requires_user_approval = Some(true);
    sqlx::query("UPDATE project SET workflow_definition = ? WHERE id = ?")
        .bind(serde_json::to_string(&workflow).expect("workflow serializes"))
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project workflow updates");
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(
        &db,
        &project_id,
        crate::workflow::default_states::PLANNING.to_owned(),
    )
    .await;
    seed_role_assignment(
        &db,
        &task.id,
        crate::workflow::default_roles::PLANNER,
        Some(&agent_id),
    )
    .await;

    let execution = service
        .dispatch_initial_role_execution(
            &task.id,
            &agent_id,
            crate::workflow::default_roles::PLANNER,
            "plan the approval-gated task".to_owned(),
        )
        .await
        .expect("planner dispatch succeeds");

    let waiting = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let current = TaskRepo::get_by_id(&*db, &task.id, false)
                .await
                .expect("task loads")
                .expect("task exists");
            let metadata = current.metadata().expect("metadata parses");
            if metadata
                .extra
                .get("awaiting_human_reason")
                .and_then(Value::as_str)
                == Some("plan_review")
            {
                break current;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;
    let waiting = match waiting {
        Ok(task) => task,
        Err(error) => {
            let task = TaskRepo::get_by_id(&*db, &task.id, false)
                .await
                .unwrap()
                .unwrap();
            let execution = ExecutionRepo::get_by_id(&*db, &execution.id)
                .await
                .unwrap()
                .unwrap();
            panic!("planner completion must mark the gate: {error:?}; status={} annotation={:?} metadata={:?} execution_status={:?} execution_error={:?}", task.status, task.error_annotation, task.metadata_json, execution.status, execution.error);
        }
    };
    assert_eq!(waiting.status, crate::workflow::default_states::PLANNING);
    assert_eq!(
        waiting
            .metadata()
            .expect("metadata parses")
            .extra
            .get("planning_execution_id")
            .and_then(Value::as_str),
        Some(execution.id.as_str())
    );
    assert_eq!(
        std::fs::read_to_string(workspace_root.path().join(&task.id).join("plan.md"))
            .expect("approved plan publishes before review"),
        "- [ ] approved implementation\n"
    );
    assert!(!TransitionLogRepo::list_by_task(&*db, &task.id)
        .await
        .expect("transition logs load")
        .iter()
        .any(
            |log| log.from_state == crate::workflow::default_states::PLANNING
                && log.to_state == crate::workflow::default_states::IN_PROGRESS
        ));

    let approved = service
        .transition(
            task.id.clone(),
            crate::workflow::default_states::IN_PROGRESS.to_owned(),
            waiting.version,
        )
        .await
        .expect("explicit approval advances planning");
    assert_eq!(
        approved.task.status,
        crate::workflow::default_states::IN_PROGRESS
    );
}

#[tokio::test]
async fn approval_gated_planner_without_a_valid_plan_retries_instead_of_waiting() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(32));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let service = TaskService::new(Arc::clone(&db), event_bus)
        .with_task_executor(Arc::new(SessionNoPlanExecutor))
        .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let mut workflow = crate::workflow::default_workflow::default_workflow();
    workflow
        .states
        .iter_mut()
        .find(|state| state.name == crate::workflow::default_states::PLANNING)
        .and_then(|state| state.gate_config.as_mut())
        .expect("planning gate config")
        .requires_user_approval = Some(true);
    sqlx::query("UPDATE project SET workflow_definition = ? WHERE id = ?")
        .bind(serde_json::to_string(&workflow).expect("workflow serializes"))
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project workflow updates");
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(
        &db,
        &project_id,
        crate::workflow::default_states::PLANNING.to_owned(),
    )
    .await;
    seed_role_assignment(
        &db,
        &task.id,
        crate::workflow::default_roles::PLANNER,
        Some(&agent_id),
    )
    .await;

    service
        .dispatch_initial_role_execution(
            &task.id,
            &agent_id,
            crate::workflow::default_roles::PLANNER,
            "plan but omit the artifact".to_owned(),
        )
        .await
        .expect("planner dispatch succeeds");

    let blocked = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let current = TaskRepo::get_by_id(&*db, &task.id, false)
                .await
                .expect("task loads")
                .expect("task exists");
            if current.blocked_json.is_some() {
                break current;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("missing plan exhausts its bounded retry budget");
    assert_eq!(blocked.status, crate::workflow::default_states::PLANNING);
    assert!(blocked.error_annotation.is_some());
    let blocked_metadata = blocked.metadata().expect("metadata parses");
    assert!(
        blocked_metadata.extra.get("awaiting_human").is_none(),
        "an approval gate cannot wait on a missing plan"
    );
    assert_eq!(
        blocked
            .blocked_json
            .as_deref()
            .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
            .and_then(|value| value.get("source").cloned())
            .and_then(|value| value.as_str().map(str::to_owned))
            .as_deref(),
        Some("planning_plan_ready")
    );
    let attempts = ExecutionRepo::count_by_task_and_role(
        &*db,
        &task.id,
        crate::workflow::default_roles::PLANNER,
    )
    .await
    .expect("planner attempt count loads");
    assert!(attempts > 1, "a resumable planner receives a bounded retry");
    assert!(
        attempts < 10,
        "the retry budget must prevent an unbounded loop"
    );
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::PLANNER,
        )
        .await
        .expect("stable planner attempt count loads"),
        attempts,
        "a blocked plan contract must not launch another planner"
    );
}

#[tokio::test]
async fn planner_completion_advances_default_planning_gate() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let embedded = Arc::new(crate::test_support::TestExecutionSupport::new(Arc::clone(
        &db,
    )));
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus))
        .with_task_executor(Arc::new(OutboxPlanExecutor {
            plan: "- [ ] implement the plan\n",
        }))
        .with_provider_credential_env(embedded)
        .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(
        &db,
        &project_id,
        crate::workflow::default_states::TODO.to_owned(),
    )
    .await;
    seed_role_assignment(
        &db,
        &task.id,
        crate::workflow::default_roles::PLANNER,
        Some(&agent_id),
    )
    .await;

    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads after role assignment")
        .expect("task exists");
    service
        .transition(
            task.id.clone(),
            crate::workflow::default_states::PLANNING.to_owned(),
            task.version,
        )
        .await
        .expect("workflow transition dispatches the planner");
    service.drain(&task.id).await.expect("planner hook settles");
    let execution = ExecutionRepo::list_by_task(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 10,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("planner executions load")
    .items
    .into_iter()
    .find(|execution| execution.role == crate::workflow::default_roles::PLANNER)
    .expect("the workflow hook creates a planner execution");

    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let current = ExecutionRepo::get_by_id(&*db, &execution.id)
                .await
                .expect("execution loads")
                .expect("execution exists");
            if current.status == ExecutionStatus::Completed {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("planner execution completes");

    // The default planning gate is not a human approval boundary: the
    // finished planner advances the Task instead of leaving a plan-review
    // marker the dispatcher would keep relaunching the planner against.
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let logs = TransitionLogRepo::list_by_task(&*db, &task.id)
                .await
                .expect("transition logs load");
            if logs.iter().any(|log| {
                log.from_state == crate::workflow::default_states::PLANNING
                    && log.to_state == crate::workflow::default_states::IN_PROGRESS
            }) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("planner completion advances the planning gate");
    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let metadata = task.metadata().expect("metadata parses");
    assert!(metadata.extra.get("awaiting_human").is_none());
    assert!(metadata.extra.get("awaiting_human_reason").is_none());
    assert_eq!(
        std::fs::read_to_string(workspace_root.path().join(&task.id).join("plan.md"))
            .expect("the terminal-CAS winner publishes the plan"),
        "- [ ] implement the plan\n"
    );
    let outbox = executors::execution_outbox_path(
        &workspace_root.path().join(&task.id).join("forge"),
        &execution.id,
    )
    .expect("outbox path");
    assert!(!outbox.exists(), "published execution outbox is consumed");
}
