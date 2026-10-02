use super::helpers::*;
use super::*;
use api_types::{FailureKind, TaskAction};

async fn fixture(db: &SqliteDb, project: &str, agent: &str, state: &str, kind: Option<FailureKind>) -> Task {
    let task = seed_task_with_status(db, project, state).await;
    seed_role_assignment(db, &task.id, "coder", Some(agent)).await;
    seed_role_assignment(db, &task.id, "planner", Some(agent)).await;
    seed_role_assignment(db, &task.id, "reviewer", Some(agent)).await;
    if let Some(kind) = kind {
        sqlx::query("UPDATE task SET error_annotation = ? WHERE id = ?").bind(json!({"type":kind,"blocking_reason":"fixture","recovery_actions":["return_to_implementation","retry_pr_publication"]}).to_string()).bind(&task.id).execute(db.pool()).await.unwrap();
    }
    TaskRepo::get_by_id(db, &task.id, false).await.unwrap().unwrap()
}

#[tokio::test]
async fn every_offer_applies_and_every_absent_verb_returns_current_offers() {
    let db = Arc::new(sqlite_db().await);
    let (project, _, repo_dir) = seed_project_repo(&db).await;
    initialize_primary_repository(&repo_dir);
    let agent = seed_agent(&db).await;
    let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(1024)));
    let verbs = [TaskAction::Start, TaskAction::Hold, TaskAction::Release, TaskAction::retry(), TaskAction::SendBack { guidance: "Return for revisions.".to_owned() }, TaskAction::Approve { override_checks: false }, TaskAction::Restart, TaskAction::Cancel];
    for state in ["backlog", "todo", "planning", "in_progress", "review", "merging", "merge_failed", "done", "cancelled"] {
        for kind in [None, Some(FailureKind::ExecutorFailed), Some(FailureKind::RetryExhausted), Some(FailureKind::MergeConflict), Some(FailureKind::WorkspaceResetRequired), Some(FailureKind::BeforeWorkHookFailed), Some(FailureKind::ReviewNeedsOwner), Some(FailureKind::ManualStop), Some(FailureKind::Unknown)] {
            let task = fixture(&db, &project, &agent, state, kind).await;
            let offers = service.task_action_offers(&task.id, &Actor::user(UserActionSource::Test)).await.unwrap().available_actions;
            for offer in &offers {
                let copy = fixture(&db, &project, &agent, state, kind).await;
                match service.perform_task_action(copy.id.clone(), offer.action.clone(), copy.version).await {
                    Ok(_) | Err(ServiceError::Db(DbError::VersionConflict | DbError::TaskVersionConflict { .. })) => {}
                    Err(error) => panic!("offered {} in {state}/{kind:?} failed: {error:?}", offer.action),
                }
            }
            for action in &verbs {
                if offers.iter().any(|offer| offer.action.verb() == action.verb()) { continue; }
                let error = service.perform_task_action(task.id.clone(), action.clone(), task.version).await.unwrap_err();
                match error { ServiceError::TaskActionUnavailable { available_actions, .. } => assert_eq!(available_actions, offers), error => panic!("absent {} returned {error:?}", action.verb()) }
            }
        }
    }
}

#[tokio::test]
async fn unauthorized_caller_gets_no_offers_and_one_typed_error_for_each_verb() {
    let db = Arc::new(sqlite_db().await);
    let (project, _, _repo_dir) = seed_project_repo(&db).await;
    let agent = seed_agent(&db).await;
    let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(32)));
    let task = fixture(&db, &project, &agent, "in_progress", Some(FailureKind::ExecutorFailed)).await;
    let actor = Actor::agent("unassigned");
    assert!(service.task_action_offers(&task.id, &actor).await.unwrap().available_actions.is_empty());
    for action in [TaskAction::Start, TaskAction::Hold, TaskAction::Release, TaskAction::retry(), TaskAction::SendBack { guidance: "revision".to_owned() }, TaskAction::Approve { override_checks: true }, TaskAction::Restart, TaskAction::Cancel] {
        let error = service.perform_task_action_as(task.id.clone(), action, task.version, actor.clone()).await.unwrap_err();
        assert!(matches!(error, ServiceError::TaskActionUnavailable { available_actions, .. } if available_actions.is_empty()));
    }
}

#[tokio::test]
async fn stale_version_is_a_version_conflict_before_any_action_effect() {
    let db = Arc::new(sqlite_db().await);
    let (project, _, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(&db, &project, "in_progress").await;
    let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(16)));
    assert!(matches!(service.perform_task_action(&task.id, TaskAction::Cancel, task.version + 1).await, Err(ServiceError::Db(DbError::TaskVersionConflict { .. }))));
    assert_eq!(TaskRepo::get_by_id(&*db, &task.id, false).await.unwrap().unwrap(), task);
}

struct RecordingExecutor { started: tokio::sync::mpsc::UnboundedSender<String> }
#[async_trait::async_trait]
impl TaskExecutor for RecordingExecutor {
    async fn execute(&self, context: ExecutionContext) -> std::result::Result<executors::ExecutionResult, executors::ExecutorError> {
        self.started.send(context.execution_id.clone()).unwrap();
        std::future::pending().await
    }
    async fn cancel(&self, _execution_id: &str) -> std::result::Result<(), executors::ExecutorError> { Ok(()) }
}

#[tokio::test]
async fn recovery_commits_at_capacity_then_dispatches_when_the_slot_frees() {
    let db = Arc::new(sqlite_db().await);
    let (project, _, repo_dir) = seed_project_repo(&db).await;
    initialize_primary_repository(&repo_dir);
    let agent = seed_agent(&db).await;
    let busy = seed_task_with_status(&db, &project, "in_progress").await;
    let occupied = seed_execution(&db, &busy.id, Some(&agent), "coder", ExecutionStatus::Running, None, "2026-10-02T00:00:00Z").await;
    let agent_row = db::AgentRepo::get_by_id(&*db, &agent).await.unwrap().unwrap();
    sqlx::query("UPDATE daemon SET machine_id = ? WHERE id = ?").bind(crate::embedded_daemon::embedded_machine_id()).bind(agent_row.daemon_id.as_deref()).execute(db.pool()).await.unwrap();
    let (started, mut starts) = tokio::sync::mpsc::unbounded_channel();
    let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(128))).with_workspace_root(repo_dir.path().join("workspaces")).with_task_executor(Arc::new(RecordingExecutor { started }));
    for state in ["planning", "in_progress", "merge_failed"] {
        let task = fixture(&db, &project, &agent, state, Some(FailureKind::ExecutorFailed)).await;
        let accepted = service.perform_task_action(&task.id, TaskAction::retry(), task.version).await.unwrap().task;
        assert!(accepted.error_annotation.is_none());
        assert!(crate::deferred_dispatch::queued_recovery(&accepted).is_some());
        assert!(!service.dispatch_queued_recovery(&accepted).await.unwrap());
        assert!(starts.try_recv().is_err());
        // Free the unrelated Agent slot; the accepted intent, not another command, runs next.
        sqlx::query("UPDATE execution SET status = 'cancelled' WHERE id = ?").bind(&occupied.id).execute(db.pool()).await.unwrap();
        assert!(service.dispatch_queued_recovery(&accepted).await.unwrap());
        let execution_id = tokio::time::timeout(Duration::from_secs(5), starts.recv()).await.unwrap().unwrap();
        let execution = ExecutionRepo::get_by_id(&*db, &execution_id).await.unwrap().unwrap();
        assert_eq!(execution.task_id, task.id);
        assert!(crate::deferred_dispatch::queued_recovery(&TaskRepo::get_by_id(&*db, &task.id, false).await.unwrap().unwrap()).is_none());
        sqlx::query("UPDATE execution SET status = 'cancelled' WHERE id = ?").bind(&execution_id).execute(db.pool()).await.unwrap();
        sqlx::query("UPDATE execution SET status = 'running' WHERE id = ?").bind(&occupied.id).execute(db.pool()).await.unwrap();
    }
}
