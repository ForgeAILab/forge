use std::sync::Arc;

use async_trait::async_trait;
use db::{now_rfc3339, SqliteDb};
use serde_json::json;

pub(crate) struct TestAgentEventConsumerNames;

impl crate::worker_runtime::assembly::AgentEventConsumerNames for TestAgentEventConsumerNames {
    fn memory_consumer_name(&self) -> &'static str {
        "scoped-memory-agent-chat-indexer"
    }

    fn wake_turn_consumer_name(&self) -> &'static str {
        "agent-wake-turns"
    }
}

pub(crate) fn memory_consumer_name() -> &'static str {
    crate::worker_runtime::assembly::AgentEventConsumerNames::memory_consumer_name(
        &TestAgentEventConsumerNames,
    )
}

pub(crate) async fn initialize_agent_event_consumer(db: &Arc<SqliteDb>, name: &str) {
    let health = db::WorkerHealth::new(Arc::clone(db), name);
    let mut transaction = db::begin_immediate(db.pool())
        .await
        .expect("test worker transaction begins");
    db.initialize_event_worker_in_tx(
        &mut transaction,
        &health,
        &db::EventSubscription::Exact(vec![
            "agent_chat.message.admitted".to_owned(),
            "agent_chat.response.completed".to_owned(),
            "agent_chat.message.completed".to_owned(),
        ]),
    )
    .await
    .expect("test event worker initializes");
    transaction
        .commit()
        .await
        .expect("test worker initialization commits");
}

pub(crate) struct NoopWakeTurns;

#[async_trait]
impl crate::wake_turns::WakeTurns for NoopWakeTurns {
    async fn prepare_attention(
        &self,
        _attention: &db::AttentionProjection,
        _dedupe_key: &str,
        _causation_id: Option<&str>,
        _depth: i64,
        _batch: &[db::AttentionProjection],
    ) -> crate::Result<Option<crate::wake_turns::PreparedWakeTurn>> {
        Ok(None)
    }

    fn build_prepared_turn(
        &self,
        _event: &db::DomainEvent,
        _input: &crate::wake_turns::PreparedWakeTurn,
        _attention: &db::AttentionProjection,
    ) -> crate::Result<db::AdmitAgentChatTurn> {
        Err(crate::ServiceError::invalid_operation(
            "the core test wake adapter never prepares a turn",
        ))
    }

    fn consumer_name(&self) -> &'static str {
        "core-test-noop-wake-turns"
    }
}

pub(crate) fn noop_wake_turns() -> Arc<dyn crate::wake_turns::WakeTurns> {
    Arc::new(NoopWakeTurns)
}

/// Core-only adapter used where an agent execution provider is merely a
/// fixture for Task/daemon ordering. It implements the two effects those
/// tests exercise: staged plans and idempotent owner worklog entries.
pub(crate) struct TestExecutionSupport {
    db: Arc<SqliteDb>,
}

impl TestExecutionSupport {
    pub(crate) fn new(db: Arc<SqliteDb>) -> Self {
        Self { db }
    }

    async fn append_worklog(
        &self,
        input: &crate::task_service::execution::ExecutionOutboxInput<'_>,
        position: &str,
        kind: &str,
        summary: &str,
    ) -> std::result::Result<(), String> {
        let idempotency_key = format!("outbox:{}:worklog:{position}", input.execution_id);
        if self
            .db
            .native_task_worklog_exists(input.task_id, &idempotency_key)
            .await
            .map_err(|error| error.to_string())?
        {
            return Ok(());
        }
        let now = db::now_rfc3339();
        let result = db::TaskCommentRepo::create_comment(
            &*self.db,
            db::CreateTaskComment {
                id: db::new_uuid_v4(),
                task_id: input.task_id.to_owned(),
                author_type: db::CommentAuthorType::Agent,
                author_id: Some(input.agent_id.to_owned()),
                author_name: input.role.unwrap_or("agent").to_owned(),
                content: summary.to_owned(),
                execution_id: Some(input.execution_id.to_owned()),
                role: input.role.map(str::to_owned),
                worklog_kind: Some(kind.to_owned()),
                idempotency_key: Some(idempotency_key.clone()),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await;
        match result {
            Ok(_) => Ok(()),
            Err(_error)
                if self
                    .db
                    .native_task_worklog_exists(input.task_id, &idempotency_key)
                    .await
                    .unwrap_or(false) =>
            {
                Ok(())
            }
            Err(error) => Err(error.to_string()),
        }
    }
}

#[async_trait]
impl crate::task_service::execution::ExecutionSupport for TestExecutionSupport {
    async fn inject_provider_env(
        &self,
        _agent_config: &mut serde_json::Value,
    ) -> crate::Result<()> {
        Ok(())
    }

    async fn ingest_execution_outbox(
        &self,
        input: &crate::task_service::execution::ExecutionOutboxInput<'_>,
    ) -> crate::task_service::execution::ExecutionOutboxReport {
        let mut report = crate::task_service::execution::ExecutionOutboxReport::default();
        let worktree = std::path::Path::new(input.worktree_path);
        let outbox = match executors::existing_execution_outbox(worktree, input.execution_id) {
            Ok(Some(outbox)) => outbox,
            Ok(None) => return report,
            Err(error) => {
                report
                    .rejected
                    .push(format!("execution outbox is unsafe: {error}"));
                return report;
            }
        };
        if executors::task_role_can_write_plan(input.role) {
            match crate::plan_artifact::stage_execution_outbox_plan(
                &outbox,
                worktree,
                input.execution_id,
            ) {
                Ok(true) => report.plan_candidate = true,
                Ok(false) => {}
                Err(error) => {
                    report.plan_rejected = true;
                    report.rejected.push(format!("plan.md: {error}"));
                }
            }
        }
        if !report.plan_rejected {
            if let Err(error) = executors::remove_execution_outbox(worktree, input.execution_id) {
                report.rejected.push(format!(
                    "outbox could not be removed after ingestion: {error}"
                ));
            }
        }
        report
    }

    async fn ingest_execution_outbox_entries(
        &self,
        input: &crate::task_service::execution::ExecutionOutboxInput<'_>,
        entries: Vec<api_types::ExecutionOutboxEntry>,
    ) -> crate::task_service::execution::ExecutionOutboxReport {
        use api_types::{ExecutionOutboxEntry, ExecutionOutboxWorklogKind};

        let mut report = crate::task_service::execution::ExecutionOutboxReport::default();
        for entry in entries {
            match entry {
                ExecutionOutboxEntry::Worklog {
                    position,
                    kind,
                    summary,
                } => {
                    let kind = match kind {
                        ExecutionOutboxWorklogKind::Progress => "progress",
                        ExecutionOutboxWorklogKind::Decision => "decision",
                        ExecutionOutboxWorklogKind::Validation => "validation",
                        ExecutionOutboxWorklogKind::Blocker => "blocker",
                    };
                    match self
                        .append_worklog(input, &position, kind, summary.trim())
                        .await
                    {
                        Ok(()) => report.worklog_entries += 1,
                        Err(error) => report.rejected.push(format!(
                            "{}:{position}: {error}",
                            executors::OUTBOX_WORKLOG_FILE
                        )),
                    }
                }
                ExecutionOutboxEntry::Evidence { position, .. } => report.rejected.push(format!(
                    "{}:{position}: core test adapter does not store evidence",
                    executors::OUTBOX_EVIDENCE_FILE
                )),
            }
        }
        report
    }
}

/// Make a legacy Project fixture explicit about the repository execution
/// roles introduced by V087.  These tests intentionally keep the legacy
/// Charter state, but their repository execution paths still cross the same
/// role/lease admission boundary as a Charter-backed Project.
pub(crate) async fn configure_project_execution_test_setup(
    db: &SqliteDb,
    project_id: &str,
    worker_id: &str,
    reviewer_id: &str,
) {
    let now = now_rfc3339();
    let existing_settings: Option<String> =
        sqlx::query_scalar("SELECT settings FROM project WHERE id = ?")
            .bind(project_id)
            .fetch_optional(db.pool())
            .await
            .expect("test project settings lookup");
    let mut settings = existing_settings
        .as_deref()
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
        .filter(serde_json::Value::is_object)
        .unwrap_or_else(|| json!({}));
    settings["default_role_assignments"] = json!([
        {
            "role_name": "planner",
            "assignee_type": "agent",
            "assignee_id": worker_id
        },
        {
            "role_name": "coder",
            "assignee_type": "agent",
            "assignee_id": worker_id
        },
        {
            "role_name": "reviewer",
            "assignee_type": "agent",
            "assignee_id": reviewer_id
        }
    ]);
    sqlx::query("UPDATE project SET settings = ?, updated_at = ? WHERE id = ?")
        .bind(settings.to_string())
        .bind(&now)
        .bind(project_id)
        .execute(db.pool())
        .await
        .expect("test project execution role settings update");

    let operation_id: String =
        sqlx::query_scalar("SELECT id FROM project_provisioning_operation WHERE project_id = ?")
            .bind(project_id)
            .fetch_one(db.pool())
            .await
            .expect("test project provisioning operation lookup");
    sqlx::query(
        "UPDATE project_provisioning_operation
         SET status = 'ready', current_checkpoint = 'completed',
             lease_owner = NULL, lease_expires_at = NULL, next_retry_at = NULL,
             retryable = 0, completed_at = ?, updated_at = ?, version = version + 1
         WHERE id = ? AND project_id = ?",
    )
    .bind(&now)
    .bind(&now)
    .bind(&operation_id)
    .bind(project_id)
    .execute(db.pool())
    .await
    .expect("test project provisioning operation ready update");
    for checkpoint in [
        "preflight",
        "repository_scaffolded",
        "repository_initialized",
        "repository_registered",
        "repository_linked",
        "roles_assigned",
    ] {
        sqlx::query(
            "UPDATE project_provisioning_checkpoint
             SET status = 'completed', completed_at = ?, updated_at = ?,
                 version = version + 1
             WHERE operation_id = ? AND checkpoint = ?",
        )
        .bind(&now)
        .bind(&now)
        .bind(&operation_id)
        .bind(checkpoint)
        .execute(db.pool())
        .await
        .expect("test project provisioning checkpoint ready update");
    }
}

/// Opt an interactive-only service fixture out of the project default-role
/// dispatch that would otherwise run as an on-enter side effect while the
/// launch API moves a task from its initial state into active work.
pub(crate) async fn clear_project_execution_role_defaults(db: &SqliteDb, project_id: &str) {
    let existing_settings: Option<String> =
        sqlx::query_scalar("SELECT settings FROM project WHERE id = ?")
            .bind(project_id)
            .fetch_optional(db.pool())
            .await
            .expect("test project settings lookup");
    let mut settings = existing_settings
        .as_deref()
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
        .filter(serde_json::Value::is_object)
        .unwrap_or_else(|| json!({}));
    settings
        .as_object_mut()
        .expect("project settings object")
        .remove("default_role_assignments");
    sqlx::query("UPDATE project SET settings = ?, updated_at = ? WHERE id = ?")
        .bind(settings.to_string())
        .bind(now_rfc3339())
        .bind(project_id)
        .execute(db.pool())
        .await
        .expect("test project execution role settings clear");
}

/// Restore a fixture agent's capacity after a normal execution fixture has
/// been seeded.  The normal helpers reserve one slot because lease admission
/// revalidates the identity after the new execution row is visible; tests
/// whose purpose is capacity behavior opt back into the exact one-slot limit.
pub(crate) async fn set_test_agent_capacity(db: &SqliteDb, agent_id: &str, capacity: i64) {
    sqlx::query("UPDATE agent_identity SET max_concurrent_tasks = ?, updated_at = ? WHERE id = ?")
        .bind(capacity)
        .bind(now_rfc3339())
        .bind(agent_id)
        .execute(db.pool())
        .await
        .expect("test agent capacity update");
}

/// Make a lower-priority candidate lose the scheduler's optimistic version
/// race after the higher-priority candidate has been dispatched.  The
/// dispatcher snapshots all initial candidates before transitioning them, so
/// this leaves the lower-priority Task in its original state while keeping
/// the first dispatch's asynchronous execution lease untouched.
pub(crate) async fn force_task_version_conflict_after_transition(
    db: &SqliteDb,
    first_task_id: &str,
    to_state: &str,
    competing_task_id: &str,
) {
    let trigger_name = format!(
        "test_priority_version_conflict_{}",
        first_task_id.replace(['-', '.'], "_")
    );
    let first_task_id = first_task_id.replace('\'', "''");
    let to_state = to_state.replace('\'', "''");
    let competing_task_id = competing_task_id.replace('\'', "''");
    let now = now_rfc3339().replace('\'', "''");
    let sql = format!(
        "CREATE TRIGGER \"{trigger_name}\"
         AFTER INSERT ON task_step
         WHEN NEW.task_id = '{first_task_id}'
          AND NEW.expected_status = '{to_state}'
          AND NEW.kind = 'hooks'
         BEGIN
             UPDATE task
             SET version = version + 1, updated_at = '{now}'
             WHERE id = '{competing_task_id}';
         END"
    );
    sqlx::query(&sql)
        .execute(db.pool())
        .await
        .expect("test agent capacity cutover trigger creates");
}

/// Preserve final-state assertions while testing the asynchronous cascade path.
pub(crate) async fn drain_transition(
    service: &crate::TaskService,
    mut result: crate::task_service::TransitionResult,
) -> crate::task_service::TransitionResult {
    result.task = service
        .drain(&result.task.id)
        .await
        .expect("Task steps drain");
    result.review = db::ReviewRepo::list_by_task(&*service.workflow_engine().db, &result.task.id)
        .await
        .expect("Review history loads")
        .into_iter()
        .max_by_key(|r| r.attempt_number);
    result.pending_steps = 0;
    result
}

/// Enqueue-only producers return before their Task projection changes.
pub(crate) async fn drain_task_steps(db: &SqliteDb, task_id: &str) {
    let service = crate::TaskService::new(
        std::sync::Arc::new(db.clone()),
        std::sync::Arc::new(events::EventBus::default()),
    );
    service
        .drain(task_id)
        .await
        .expect("queued Task effects settle");
}

/// A claimed step for a Task, as the worker holds while it executes a step.
/// Unit tests of in-lease helpers run them inside it, as production does;
/// outside a lease those helpers only queue their writes. Claim it before
/// opening a write transaction the helper will use.
pub(crate) struct TestTaskLease {
    db: SqliteDb,
    step: db::TaskStep,
}

impl TestTaskLease {
    pub(crate) async fn claim(db: &SqliteDb, task_id: &str) -> Self {
        use db::TaskStepRepo;
        let id = db::new_uuid_v4();
        let now = db::now_rfc3339();
        sqlx::query("INSERT INTO task_step (id,task_id,seq,kind,payload_json,causation_key,chain_id,chain_position,expected_status,expected_version,expected_epoch,lane,status,claimed_by,lease_until,available_at,attempts,created_at,updated_at) SELECT ?,t.id,COALESCE((SELECT MAX(seq) FROM task_step WHERE task_id=t.id),0)+1,'command','{}',?,?,1,t.status,t.version,t.status_epoch,'fast','claimed','test-lease',?,?,1,?,? FROM task t WHERE t.id=?")
            .bind(&id).bind(&id).bind(&id).bind(db::task_writer::lease_deadline()).bind(&now).bind(&now).bind(&now).bind(task_id)
            .execute(db.pool()).await.expect("test lease step");
        let step = db
            .task_steps(task_id)
            .await
            .expect("test lease reads steps")
            .into_iter()
            .find(|step| step.id == id)
            .expect("test lease step exists");
        Self {
            db: db.clone(),
            step,
        }
    }

    pub(crate) async fn run<T>(&self, future: impl std::future::Future<Output = T>) -> T {
        db::task_writer::in_task_step(self.step.clone(), future).await
    }

    pub(crate) async fn release(self) {
        use db::TaskStepRepo;
        let mut tx = db::begin_immediate(self.db.pool())
            .await
            .expect("test lease settles");
        let _ = self
            .db
            .finish_step_in_tx(&mut tx, &self.step, "done", None)
            .await;
        tx.commit().await.expect("test lease commits");
        self.db
            .release_step(&self.step.id, "test-lease")
            .await
            .expect("test lease releases");
    }
}

/// Run `future` under a fresh [`TestTaskLease`] for `task_id`.
pub(crate) async fn with_task_lease<T>(
    db: &SqliteDb,
    task_id: &str,
    future: impl std::future::Future<Output = T>,
) -> T {
    let lease = TestTaskLease::claim(db, task_id).await;
    let output = lease.run(future).await;
    lease.release().await;
    output
}

/// A native Agent with a healthy provider connection: the only kind of
/// Agent that may lead a coordination root.
pub(crate) async fn seed_native_agent(db: &SqliteDb) -> String {
    use db::{AgentConnectionHealthRepo, AgentRepo};
    let now = now_rfc3339();
    let id = db::new_uuid_v4();
    let agent = AgentRepo::create_identity_with_profile(
        db,
        db::CreateAgentIdentity {
            id: id.clone(),
            name: "lead".to_owned(),
            description: None,
            max_concurrent_tasks: 2,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: db::AgentStatus::Idle,
            last_heartbeat_at: Some(now.clone()),
            is_default: false,
            paused: false,
            owner_id: None,
            visibility: "global".to_owned(),
            account_permission_ceiling: "{}".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        db::CreateAgentProfile {
            id: db::new_uuid_v4(),
            identity_id: id.clone(),
            backend_kind: "native".to_owned(),
            executor_type: "embedded".to_owned(),
            provider: Some("fixture".to_owned()),
            model: Some("fixture-model".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: "{}".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("native agent creates");
    assert_eq!(agent.backend_kind, "native");
    AgentConnectionHealthRepo::upsert_connection_health(
        db,
        db::UpsertAgentConnectionHealth {
            profile_id: agent.profile_id.clone(),
            status: "healthy".to_owned(),
            capability_status_json: "{}".to_owned(),
            checked_at: Some(now.clone()),
            error_code: None,
            updated_at: now,
        },
    )
    .await
    .expect("connection health records");
    id
}
