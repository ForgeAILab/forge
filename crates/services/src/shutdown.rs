use crate::{recovery::cancel_running_executions, Result};
use db::{ResumePolicy, SqliteDb, StopReason};
use events::{event_timestamp, EventBus, EventContext, ForgeEvent};
use executors::TaskExecutor;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{task::JoinHandle, time::sleep};
use tracing::Instrument;

const SHUTDOWN_ERROR_TYPE: &str = "shutdown";

#[derive(Clone)]
pub struct GracefulShutdown {
    accepting_new_work: Arc<AtomicBool>,
    db: Arc<SqliteDb>,
    event_bus: Arc<EventBus>,
    task_executor: Option<Arc<dyn TaskExecutor>>,
}

impl GracefulShutdown {
    pub fn new(db: Arc<SqliteDb>, event_bus: Arc<EventBus>) -> Self {
        Self {
            accepting_new_work: Arc::new(AtomicBool::new(true)),
            db,
            event_bus,
            task_executor: None,
        }
    }

    pub fn with_task_executor(mut self, task_executor: Arc<dyn TaskExecutor>) -> Self {
        self.task_executor = Some(task_executor);
        self
    }

    pub fn is_accepting_work(&self) -> bool {
        self.accepting_new_work.load(Ordering::SeqCst)
    }

    #[tracing::instrument(skip(self))]
    pub async fn shutdown(&self) -> Result<()> {
        self.accepting_new_work.store(false, Ordering::SeqCst);
        tracing::info!("graceful shutdown started");
        sleep(transaction_drain_wait()).await;

        let rows = sqlx::query(
            "SELECT id, project_id FROM task WHERE status = 'in_progress' AND deleted_at IS NULL",
        )
        .fetch_all(self.db.pool())
        .await?;
        let in_progress_count = rows.len();

        for row in rows {
            use sqlx::Row;

            let id: String = row.get("id");
            let project_id: String = row.get("project_id");

            self.cancel_executor_processes_for_task(&id).await?;
            let _cancelled = cancel_running_executions(
                &self.db,
                &id,
                StopReason::GracefulShutdown,
                &api_types::Actor::system(api_types::SystemComponent::GracefulShutdown),
                ResumePolicy::Auto,
            )
            .await
            .unwrap_or_default();

            self.event_bus.publish(ForgeEvent {
                event_type: "task.recovered".to_owned(),
                entity_id: id,
                timestamp: event_timestamp(),
                context: EventContext::TaskRecovered {
                    project_id,
                    reason: SHUTDOWN_ERROR_TYPE.to_owned(),
                },
            });
        }

        tracing::info!(
            recovered_tasks = in_progress_count,
            "graceful shutdown completed"
        );
        Ok(())
    }

    async fn cancel_executor_processes_for_task(&self, task_id: &str) -> Result<()> {
        let Some(task_executor) = self.task_executor.as_ref() else {
            return Ok(());
        };
        let rows = sqlx::query("SELECT id FROM execution WHERE task_id = ? AND status = 'running'")
            .bind(task_id)
            .fetch_all(self.db.pool())
            .await?;
        for row in rows {
            use sqlx::Row;

            let execution_id: String = row.get("id");
            if let Err(error) = task_executor.cancel(&execution_id).await {
                tracing::warn!(
                    task_id = %task_id,
                    execution_id = %execution_id,
                    %error,
                    "executor cancellation failed during graceful shutdown"
                );
            }
        }
        Ok(())
    }
}

pub fn install_signal_handler(shutdown: Arc<GracefulShutdown>) -> JoinHandle<()> {
    tokio::spawn(
        async move {
            tracing::info!("shutdown signal handler installed");
            if tokio::signal::ctrl_c().await.is_ok() {
                tracing::info!("shutdown signal received");
                let _ = shutdown.shutdown().await;
            }
        }
        .instrument(tracing::info_span!("shutdown.signal_handler")),
    )
}

#[cfg(not(test))]
fn transaction_drain_wait() -> Duration {
    Duration::from_secs(2)
}

#[cfg(test)]
fn transaction_drain_wait() -> Duration {
    Duration::from_millis(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use db::{
        create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, AgentRepo, AgentStatus,
        CreateAgent, CreateExecution, CreateProject, CreateRepo, CreateTask, DaemonRepo,
        DaemonStatus, ExecutionRepo, ExecutionStatus, ProjectRepo, RepoRepo, TaskRepo, TaskStatus,
        UpdateProject, UpsertDaemon,
    };
    use executors::{ExecutionContext, ExecutionResult, ExecutorError, TaskExecutor};
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingCancelExecutor {
        cancelled: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl TaskExecutor for RecordingCancelExecutor {
        async fn execute(
            &self,
            _ctx: ExecutionContext,
        ) -> std::result::Result<ExecutionResult, ExecutorError> {
            Err(ExecutorError::Other("not used".to_owned()))
        }

        async fn cancel(&self, execution_id: &str) -> std::result::Result<(), ExecutorError> {
            self.cancelled
                .lock()
                .expect("cancelled lock")
                .push(execution_id.to_owned());
            Ok(())
        }
    }

    async fn sqlite_db() -> SqliteDb {
        let pool = create_sqlite_pool("sqlite::memory:")
            .await
            .expect("pool creates");
        run_migrations(&pool).await.expect("migrations run");
        SqliteDb::new(pool)
    }

    async fn seed_project_repo(db: &SqliteDb) -> (String, String) {
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
                remote_url: "https://example.com/forge.git".to_owned(),
                local_path: None,
                work_mode: db::WorkMode::DirectMerge,
                default_branch: "main".to_owned(),
                created_at: now.clone(),
                updated_at: now,
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
        (project_id, repo_id)
    }

    async fn seed_agent(db: &SqliteDb) -> String {
        let now = now_rfc3339();
        let daemon_id = new_uuid_v4();
        DaemonRepo::upsert_by_machine_id(
            db,
            UpsertDaemon {
                id: daemon_id.clone(),
                machine_id: format!("machine-{daemon_id}"),
                hostname: "test-host".to_owned(),
                os: "linux".to_owned(),
                arch: "x86_64".to_owned(),
                agent_version: None,
                labels_json: "{}".to_owned(),
                status: DaemonStatus::Online,
                registration_token_hash: None,
                owner_id: None,
                visibility: "global".to_owned(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("daemon creates");

        let agent_id = new_uuid_v4();
        AgentRepo::create(
            db,
            CreateAgent {
                id: agent_id.clone(),
                name: "codex".to_owned(),
                description: None,
                executor_type: "shell".to_owned(),
                model: None,
                reasoning_effort: None,
                permission_policy: None,
                capabilities_json: "[]".to_owned(),
                config_json: "{}".to_owned(),
                credential_ref: None,
                daemon_id: Some(daemon_id),
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
                updated_at: now,
            },
        )
        .await
        .expect("agent creates");
        agent_id
    }

    async fn seed_task(
        db: &SqliteDb,
        project_id: String,
        repo_id: String,
        status: TaskStatus,
        agent_id: Option<String>,
    ) -> String {
        let now = now_rfc3339();
        TaskRepo::create(
            db,
            CreateTask {
                id: new_uuid_v4(),
                project_id,
                repo_id: Some(repo_id),
                parent_task_id: None,
                subtask_order: None,
                assignee_type: agent_id.as_ref().map(|_| "agent".to_owned()),
                assignee_id: agent_id,
                title: "Shutdown test".to_owned(),
                description: None,
                task_type: "task".to_owned(),
                status,
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
        .expect("task creates")
        .id
    }

    async fn seed_running_execution(
        db: &SqliteDb,
        task_id: String,
        agent_id: String,
        agent_session_id: Option<String>,
    ) -> db::Execution {
        let now = now_rfc3339();
        ExecutionRepo::create(
            db,
            CreateExecution {
                id: new_uuid_v4(),
                task_id,
                agent_id: Some(agent_id),
                role: "coder".to_owned(),
                status: ExecutionStatus::Running,
                stop_reason: None,
                stopped_by: None,
                resume_policy: None,
                stopped_at: None,
                parent_execution_id: None,
                agent_session_id,
                agent_message_id: None,
                last_activity_at: None,
                summary: None,
                logs_path: None,
                before_sha: None,
                after_sha: None,
                error: None,
                executor_config_snapshot_json: Some(
                    r#"{"executor_type":"shell","config":{}}"#.to_owned(),
                ),
                workspace_id: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("execution creates")
    }

    #[tokio::test]
    async fn shutdown_stops_accepting_work_and_recovers_in_progress_tasks() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(16));
        let mut rx = event_bus.subscribe();
        let (project_id, repo_id) = seed_project_repo(&db).await;
        let agent_id = seed_agent(&db).await;
        let task_id = seed_task(
            &db,
            project_id,
            repo_id,
            "in_progress".to_owned(),
            Some(agent_id.clone()),
        )
        .await;
        let execution = seed_running_execution(&db, task_id.clone(), agent_id, None).await;
        let shutdown = GracefulShutdown::new(Arc::clone(&db), event_bus);

        assert!(shutdown.is_accepting_work());
        shutdown.shutdown().await.expect("shutdown succeeds");
        assert!(!shutdown.is_accepting_work());

        let task = TaskRepo::get_by_id(&*db, &task_id, false)
            .await
            .expect("task fetches")
            .expect("task exists");
        assert_eq!(task.status, "in_progress".to_owned());
        assert_eq!(task.error_annotation, None);
        let execution = ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .expect("execution fetches")
            .expect("execution exists");
        assert_eq!(execution.status, ExecutionStatus::Cancelled);

        let event = rx.recv().await.expect("recovery event");
        assert_eq!(event.event_type, "task.recovered");
        assert_eq!(event.entity_id, task_id);
        match event.context {
            EventContext::TaskRecovered { reason, .. } => assert_eq!(reason, SHUTDOWN_ERROR_TYPE),
            _ => panic!("expected task recovered event"),
        }
    }

    #[tokio::test]
    async fn shutdown_cancels_running_executor_processes_before_recovery() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(16));
        let (project_id, repo_id) = seed_project_repo(&db).await;
        let agent_id = seed_agent(&db).await;
        let task_id = seed_task(
            &db,
            project_id,
            repo_id,
            "in_progress".to_owned(),
            Some(agent_id.clone()),
        )
        .await;
        let execution = seed_running_execution(&db, task_id, agent_id, None).await;
        let executor = Arc::new(RecordingCancelExecutor::default());

        GracefulShutdown::new(Arc::clone(&db), event_bus)
            .with_task_executor(executor.clone())
            .shutdown()
            .await
            .expect("shutdown succeeds");

        assert_eq!(
            executor
                .cancelled
                .lock()
                .expect("cancelled lock")
                .as_slice(),
            &[execution.id]
        );
    }

    #[tokio::test]
    async fn shutdown_keeps_active_task_with_resumable_execution() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(16));
        let (project_id, repo_id) = seed_project_repo(&db).await;
        let task_agent_id = seed_agent(&db).await;
        let task_id = seed_task(
            &db,
            project_id,
            repo_id,
            "in_progress".to_owned(),
            Some(task_agent_id.clone()),
        )
        .await;
        let execution = seed_running_execution(
            &db,
            task_id.clone(),
            task_agent_id,
            Some("session-789".to_owned()),
        )
        .await;

        GracefulShutdown::new(Arc::clone(&db), event_bus)
            .shutdown()
            .await
            .expect("shutdown succeeds");

        let task = TaskRepo::get_by_id(&*db, &task_id, false)
            .await
            .expect("task fetches")
            .expect("task exists");
        assert_eq!(task.status, "in_progress");

        let execution = ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .expect("execution fetches")
            .expect("execution exists");
        assert_eq!(execution.status, ExecutionStatus::Cancelled);
        assert_eq!(execution.agent_session_id.as_deref(), Some("session-789"));
    }

    #[tokio::test]
    async fn shutdown_leaves_non_running_tasks_unchanged() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(16));
        let (project_id, repo_id) = seed_project_repo(&db).await;
        let task_id = seed_task(&db, project_id, repo_id, "todo".to_owned(), None).await;
        let version = TaskRepo::get_by_id(&*db, &task_id, false)
            .await
            .expect("task fetches")
            .expect("task exists")
            .version;

        GracefulShutdown::new(Arc::clone(&db), event_bus)
            .shutdown()
            .await
            .expect("shutdown succeeds");

        let task = TaskRepo::get_by_id(&*db, &task_id, false)
            .await
            .expect("task fetches")
            .expect("task exists");
        assert_eq!(task.status, "todo".to_owned());
        assert_eq!(task.error_annotation, None);
        assert_eq!(task.version, version);
    }
}
