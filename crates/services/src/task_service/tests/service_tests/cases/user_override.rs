use super::super::super::helpers::{seed_execution, seed_passed_review};
use super::super::*;

#[tokio::test]
async fn user_subtask_into_review_review_pass_cascade_and_hooks_succeed() {
    // Task 1.1: user routes subtask into review; destination hooks and review-pass cascade
    // must validate against the project workflow without undefined-state errors.
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let root = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let subtask = seed_subtask_with_status(&db, &root, "child", "in_progress".to_owned(), 0).await;
    let execution = seed_execution(
        &db,
        &subtask.id,
        None,
        "executor",
        ExecutionStatus::Completed,
        None,
        &now_rfc3339(),
    )
    .await;
    seed_passed_review(&db, &subtask.id, &execution.id, 1).await;

    let result = service
        .transition(
            subtask.id.clone(),
            crate::workflow::default_states::REVIEW.to_owned(),
            (subtask.version, None),
        )
        .await
        .expect("user subtask into review with review-pass cascade completes");

    assert!(
        result.task.status == crate::workflow::default_states::MERGING
            || result.task.status == crate::workflow::default_states::DONE,
        "review-pass cascade should advance past review, got {}",
        result.task.status
    );

    let logs = TransitionLogRepo::list_by_task(&*db, &subtask.id)
        .await
        .expect("transition logs load");
    assert!(
        logs.iter().any(|log| {
            log.from_state == crate::workflow::default_states::REVIEW
                && log.to_state == crate::workflow::default_states::MERGING
                && log.trigger_reason.contains("review passed")
        }),
        "review-pass cascade transition should be logged"
    );
}

#[tokio::test]
async fn user_subtask_in_progress_to_review_succeeds() {
    // Delta: User routes a subtask into review
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let root = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let subtask = seed_subtask_with_status(&db, &root, "child", "in_progress".to_owned(), 0).await;

    let result = service
        .transition(
            subtask.id.clone(),
            crate::workflow::default_states::REVIEW.to_owned(),
            (subtask.version, None),
        )
        .await
        .expect("user subtask in_progress -> review succeeds");

    assert_eq!(result.task.status, crate::workflow::default_states::REVIEW);
}

#[tokio::test]
async fn root_task_resolution_unchanged() {
    // Delta: Root task resolution is unchanged
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(&db, &project_id, "todo".to_owned()).await;

    let valid = service
        .transition(
            task.id.clone(),
            crate::workflow::default_states::IN_PROGRESS.to_owned(),
            (task.version, None),
        )
        .await
        .expect("valid root transition along project workflow succeeds");
    assert_eq!(
        valid.task.status,
        crate::workflow::default_states::IN_PROGRESS
    );

    let invalid = service
        .transition(
            task.id,
            "nonexistent".to_owned(),
            (valid.task.version, None),
        )
        .await;
    match invalid {
        Err(ServiceError::InvalidOperation { message }) => {
            assert!(
                message.contains("not defined in workflow"),
                "root task still validates against project workflow: {message}"
            );
        }
        other => panic!("expected undefined-state rejection for root task, got {other:?}"),
    }
}

#[tokio::test]
async fn system_subtask_transition_still_uses_subtask_workflow() {
    // Delta: Automatic subtask lifecycle is unchanged for subtask-workflow states
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let root = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let subtask = seed_subtask_with_status(&db, &root, "child", "in_progress".to_owned(), 0).await;

    let result = service
        .transition(
            subtask.id.clone(),
            crate::workflow::default_states::REVIEW.to_owned(),
            TransitionOptions {
                version: subtask.version,
                reason: Some("system cascade attempt".to_owned()),
                triggered_by: api_types::Actor::system(api_types::SystemComponent::General),
                rejection: false,
                defer_dispatch_seconds: None,
            },
        )
        .await;

    match result {
        Err(ServiceError::InvalidOperation { message }) => {
            assert!(
                message.contains("not defined in workflow"),
                "system subtask transition should use subtask workflow without review: {message}"
            );
        }
        other => panic!("system subtask in_progress -> review should be rejected, got {other:?}"),
    }
}

#[tokio::test]
async fn no_agent_override_move_writes_log_and_no_executor() {
    // Delta: User move with no agent assigned succeeds without execution
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;

    let workflow = WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            {
                let mut working =
                    workflow_state("working", StateKind::Active, None, StateHooks::default());
                working.triggers.insert(
                    WorkflowTrigger::Accept,
                    WorkflowTriggerDefinition {
                        to: "paused".to_owned(),
                        dispatch: None,
                    },
                );
                working
            },
            workflow_state("paused", StateKind::Custom, None, StateHooks::default()),
            workflow_state(
                "coding",
                StateKind::Active,
                Some(default_roles::CODER),
                StateHooks {
                    on_enter: vec![hook("dispatch_role_agent")],
                    ..StateHooks::default()
                },
            ),
            workflow_state("done", StateKind::Terminal, None, StateHooks::default()),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    };
    update_project_workflow(&db, &project_id, &workflow).await;

    let task = seed_task_with_status(&db, &project_id, "working".to_owned()).await;
    let reason = "override into dispatchable state without agent";

    let result = service
        .transition(
            task.id.clone(),
            "coding".to_owned(),
            TransitionOptions {
                version: task.version,
                reason: Some(reason.to_owned()),
                triggered_by: api_types::Actor::user(api_types::UserActionSource::Api),
                rejection: false,
                defer_dispatch_seconds: None,
            },
        )
        .await
        .expect("user override into dispatchable state succeeds without agent");

    assert_eq!(result.task.status, "coding");

    let logs = TransitionLogRepo::list_by_task(&*db, &task.id)
        .await
        .expect("transition logs load");
    let latest = logs
        .iter()
        .find(|entry| entry.to_state == "coding" && !entry.rejection)
        .expect("transition log written for override move");
    assert_eq!(latest.triggered_by, "user:override:api");
    assert_eq!(latest.trigger_reason, reason);

    let executions = ExecutionRepo::list_by_task(
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
    .expect("executions list");
    assert!(
        executions.items.is_empty(),
        "no executor should launch when no agent is assigned"
    );
}

#[tokio::test]
async fn override_move_out_of_active_state_cancels_running_execution() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;

    let workflow = WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            {
                let mut working =
                    workflow_state("working", StateKind::Active, None, StateHooks::default());
                working.triggers.insert(
                    WorkflowTrigger::Accept,
                    WorkflowTriggerDefinition {
                        to: "paused".to_owned(),
                        dispatch: None,
                    },
                );
                working
            },
            workflow_state("paused", StateKind::Custom, None, StateHooks::default()),
            workflow_state("done", StateKind::Terminal, None, StateHooks::default()),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    };
    update_project_workflow(&db, &project_id, &workflow).await;

    let task = seed_task_with_status(&db, &project_id, "working".to_owned()).await;
    let execution = seed_running_coder_execution(&db, &task.id, Some(agent_id), None).await;

    let result = service
        .transition(
            task.id.clone(),
            "done".to_owned(),
            TransitionOptions {
                version: task.version,
                reason: Some("override across missing edge".to_owned()),
                triggered_by: api_types::Actor::user(api_types::UserActionSource::Api),
                rejection: false,
                defer_dispatch_seconds: None,
            },
        )
        .await
        .expect("user override across missing edge succeeds");

    assert_eq!(result.task.status, "done");
    let execution_after = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    assert_eq!(execution_after.status, ExecutionStatus::Cancelled);
    assert_eq!(
        execution_after.error.as_deref(),
        Some("cancelled by user transition")
    );
}

#[tokio::test]
async fn subtask_in_project_only_state_cannot_be_deleted() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let root = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let subtask = seed_subtask_with_status(&db, &root, "child", "in_progress".to_owned(), 0).await;

    let routed = service
        .transition(
            subtask.id.clone(),
            crate::workflow::default_states::REVIEW.to_owned(),
            (subtask.version, None),
        )
        .await
        .expect("user routes subtask into review");

    assert_eq!(routed.task.status, crate::workflow::default_states::REVIEW);

    let delete_result = service.soft_delete(routed.task.id).await;
    match delete_result {
        Err(ServiceError::InvalidOperation { message }) => {
            assert!(
                message.contains("tasks can only be deleted from inactive states"),
                "subtask in review gate must not be deletable: {message}"
            );
        }
        other => panic!("expected soft_delete rejection for subtask in review, got {other:?}"),
    }
}

#[tokio::test]
async fn park_running_task_to_backlog() {
    use std::{sync::Arc, time::Duration};

    use ::workspace::RepoCacheLockManager;
    use db::{ExecutionRepo, StopReason, TaskRoleAssignmentRepo};
    use executors::TaskExecutor;
    use tempfile::TempDir;

    use crate::task_dispatcher::TaskDispatcher;
    use crate::workflow::default_roles;

    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(
        &db,
        &project_id,
        crate::workflow::default_states::IN_PROGRESS.to_owned(),
    )
    .await;
    TaskRoleAssignmentRepo::assign(
        &*db,
        role_assignment_input(&task.id, default_roles::CODER, Some(agent_id.clone()), None),
    )
    .await
    .expect("coder assignment");
    let execution = seed_running_coder_execution(&db, &task.id, Some(agent_id.clone()), None).await;

    let task_executor: Arc<dyn TaskExecutor> = Arc::new(NoDiffExecutor);
    let service = Arc::new(
        TaskService::new(Arc::clone(&db), Arc::clone(&event_bus))
            .with_task_executor(task_executor)
            .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default()))
            .with_workspace_root(workspace_dir.path().to_path_buf()),
    );

    let result = service
        .transition(
            task.id.clone(),
            crate::workflow::default_states::BACKLOG.to_owned(),
            TransitionOptions {
                version: task.version,
                reason: Some("user parks running task".to_owned()),
                triggered_by: api_types::Actor::user(api_types::UserActionSource::Api),
                rejection: false,
                defer_dispatch_seconds: None,
            },
        )
        .await
        .expect("user parks running agent-assigned task to backlog");

    assert_eq!(result.task.status, crate::workflow::default_states::BACKLOG);
    assert!(result.task.error_annotation.is_none());

    let execution_after = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    assert_eq!(execution_after.status, ExecutionStatus::Cancelled);
    assert_eq!(execution_after.stop_reason, Some(StopReason::UserCancelled));
    assert_eq!(
        execution_after.error.as_deref(),
        Some("cancelled by user transition")
    );

    let assignment =
        TaskRoleAssignmentRepo::get_by_task_and_role(&*db, &task.id, default_roles::CODER)
            .await
            .expect("assignment loads")
            .expect("coder assignment retained");
    assert_eq!(assignment.assignee_type, Some(db::AssigneeKind::Agent));
    assert_eq!(assignment.assignee_id.as_deref(), Some(agent_id.as_str()));

    let service_for_dispatcher = Arc::clone(&service);
    let dispatcher = TaskDispatcher::with_check_interval(
        Arc::clone(&db),
        Arc::clone(&event_bus),
        service_for_dispatcher,
        Duration::from_millis(10),
    );
    let dispatched = dispatcher.check_once().await.expect("dispatcher runs");
    assert_eq!(dispatched, 0);
    assert_eq!(
        service
            .executor_attempt_count(&task.id)
            .await
            .expect("execution count loads"),
        1,
        "dispatcher must not launch a replacement execution for a parked task"
    );
}

#[tokio::test]
async fn user_assigned_task_moves_anywhere() {
    use db::TaskRoleAssignmentRepo;

    use crate::workflow::default_roles;

    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(
        &db,
        &project_id,
        crate::workflow::default_states::IN_PROGRESS.to_owned(),
    )
    .await;
    TaskRoleAssignmentRepo::assign(
        &*db,
        role_assignment_input(
            &task.id,
            default_roles::CODER,
            None,
            Some("human-user".to_owned()),
        ),
    )
    .await
    .expect("user coder assignment");

    let parked = service
        .transition(
            task.id.clone(),
            crate::workflow::default_states::BACKLOG.to_owned(),
            TransitionOptions {
                version: task.version,
                reason: Some("user parks own task".to_owned()),
                triggered_by: api_types::Actor::user(api_types::UserActionSource::Api),
                rejection: false,
                defer_dispatch_seconds: None,
            },
        )
        .await
        .expect("user-assigned task move to backlog succeeds");
    assert_eq!(parked.task.status, crate::workflow::default_states::BACKLOG);

    let review = service
        .transition(
            parked.task.id.clone(),
            crate::workflow::default_states::REVIEW.to_owned(),
            TransitionOptions {
                version: parked.task.version,
                reason: Some("user override to review".to_owned()),
                triggered_by: api_types::Actor::user(api_types::UserActionSource::Board),
                rejection: false,
                defer_dispatch_seconds: None,
            },
        )
        .await
        .expect("user-assigned task override to review succeeds");
    assert_eq!(review.task.status, crate::workflow::default_states::REVIEW);
}

#[tokio::test]
async fn undefined_target_still_enumerates() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(
        &db,
        &project_id,
        crate::workflow::default_states::IN_PROGRESS.to_owned(),
    )
    .await;

    let result = service
        .transition(task.id, "bogus-state".to_owned(), (task.version, None))
        .await;

    match result {
        Err(ServiceError::InvalidOperation { message }) => {
            assert!(
                message.contains("; defined states are:"),
                "undefined-state rejection should enumerate workflow states: {message}"
            );
        }
        other => panic!("expected enumerating undefined-state rejection, got {other:?}"),
    }
}
