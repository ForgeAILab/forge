use crate::{agent_capacity::has_running_execution_capacity, Result, ServiceError, TaskService};
use db::{
    new_uuid_v4, now_rfc3339, Agent, AgentConnectionHealthRepo, AgentListQuery, AgentRepo,
    AgentStatus, CreateAgent, CredentialHandleRepo, Daemon, DaemonRepo, DaemonStatus, PageRequest,
    SortBy, SortOrder, SqliteDb, UpdateAgent,
};
use events::{event_timestamp, EventBus, EventContext, ForgeEvent};
use serde_json::Value;
use std::{fmt, sync::Arc};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EffectiveStatus {
    Active,
    Deactivated,
    DaemonOffline,
    DaemonUnavailable,
    ConnectionDegraded,
    ConnectionUnavailable,
    SourceDisabled,
    Busy,
    Error,
    Paused,
}

impl EffectiveStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Deactivated => "deactivated",
            Self::DaemonOffline => "daemon_offline",
            Self::DaemonUnavailable => "daemon_unavailable",
            Self::ConnectionDegraded => "connection_degraded",
            Self::ConnectionUnavailable => "connection_unavailable",
            Self::SourceDisabled => "source_disabled",
            Self::Busy => "busy",
            Self::Error => "error",
            Self::Paused => "paused",
        }
    }
}

impl fmt::Display for EffectiveStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[tracing::instrument(skip(db, agent), fields(agent_id = %agent.id, daemon_id = ?agent.daemon_id))]
pub async fn compute_effective_status(db: &SqliteDb, agent: &Agent) -> Result<EffectiveStatus> {
    if agent.status == AgentStatus::Error {
        return Ok(EffectiveStatus::Error);
    }

    if agent.paused {
        return Ok(EffectiveStatus::Paused);
    }

    if let Some(credential_ref) = agent.credential_ref.as_deref() {
        let source_enabled = CredentialHandleRepo::get_credential_handle(db, credential_ref)
            .await?
            .is_some_and(|handle| handle.status == "configured" && handle.enabled);
        if !source_enabled {
            return Ok(EffectiveStatus::SourceDisabled);
        }
    }

    if agent.backend_kind == "native" {
        let health =
            AgentConnectionHealthRepo::get_connection_health(db, &agent.profile_id).await?;
        match health.as_ref().map(|health| health.status.as_str()) {
            Some("healthy") => {}
            Some("degraded") => return Ok(EffectiveStatus::ConnectionDegraded),
            _ => return Ok(EffectiveStatus::ConnectionUnavailable),
        }
        if !has_running_execution_capacity(db, agent).await? {
            return Ok(EffectiveStatus::Busy);
        }
        return Ok(EffectiveStatus::Active);
    }

    if let Some(daemon_id) = &agent.daemon_id {
        let Some(daemon) = DaemonRepo::get_by_id(db, daemon_id).await? else {
            return Ok(EffectiveStatus::DaemonOffline);
        };
        if daemon.status == DaemonStatus::Offline {
            return Ok(EffectiveStatus::DaemonOffline);
        }
        if !daemon_supports_executor(&daemon, &agent.executor_type)? {
            return Ok(EffectiveStatus::Deactivated);
        }
        if !cli_runtime_enabled(
            db,
            agent.owner_id.as_deref(),
            daemon_id,
            &agent.executor_type,
        )
        .await?
        {
            return Ok(EffectiveStatus::SourceDisabled);
        }
    } else {
        let candidates = DaemonRepo::list_available_for_executor(db, &agent.executor_type).await?;
        if candidates.is_empty() {
            return Ok(EffectiveStatus::DaemonUnavailable);
        }
        if !has_enabled_cli_daemon(db, agent, &candidates).await? {
            return Ok(EffectiveStatus::SourceDisabled);
        }
    }

    if !has_running_execution_capacity(db, agent).await? {
        return Ok(EffectiveStatus::Busy);
    }

    Ok(EffectiveStatus::Active)
}

pub async fn resolve_daemon_for_agent(db: &SqliteDb, agent: &Agent) -> Result<Daemon> {
    if let Some(daemon_id) = &agent.daemon_id {
        let daemon = DaemonRepo::get_by_id(db, daemon_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("daemon", daemon_id.clone()))?;
        if daemon.status == DaemonStatus::Offline {
            return Err(ServiceError::invalid_operation(format!(
                "pinned daemon {daemon_id} is offline"
            )));
        }
        if !daemon_supports_executor(&daemon, &agent.executor_type)? {
            return Err(ServiceError::invalid_operation(format!(
                "pinned daemon {daemon_id} does not have authenticated {} executor",
                agent.executor_type
            )));
        }
        if !cli_runtime_enabled(
            db,
            agent.owner_id.as_deref(),
            daemon_id,
            &agent.executor_type,
        )
        .await?
        {
            return Err(ServiceError::invalid_operation(format!(
                "{} executor is disabled on pinned daemon {daemon_id}",
                agent.executor_type
            )));
        }
        return Ok(daemon);
    }

    for daemon in DaemonRepo::list_available_for_executor(db, &agent.executor_type).await? {
        if cli_runtime_enabled(
            db,
            agent.owner_id.as_deref(),
            &daemon.id,
            &agent.executor_type,
        )
        .await?
        {
            return Ok(daemon);
        }
    }
    Err(ServiceError::invalid_operation(format!(
        "No enabled daemon with authenticated {} executor found",
        agent.executor_type
    )))
}

pub async fn cli_runtime_source_enabled(db: &SqliteDb, agent: &Agent) -> Result<bool> {
    if agent.backend_kind == "native" || agent.credential_ref.is_some() {
        return Ok(true);
    }
    if let Some(daemon_id) = agent.daemon_id.as_deref() {
        return cli_runtime_enabled(
            db,
            agent.owner_id.as_deref(),
            daemon_id,
            &agent.executor_type,
        )
        .await;
    }
    let candidates = DaemonRepo::list_available_for_executor(db, &agent.executor_type).await?;
    has_enabled_cli_daemon(db, agent, &candidates).await
}

async fn has_enabled_cli_daemon(db: &SqliteDb, agent: &Agent, daemons: &[Daemon]) -> Result<bool> {
    for daemon in daemons {
        if cli_runtime_enabled(
            db,
            agent.owner_id.as_deref(),
            &daemon.id,
            &agent.executor_type,
        )
        .await?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn cli_runtime_enabled(
    db: &SqliteDb,
    owner_user_id: Option<&str>,
    daemon_id: &str,
    executor_type: &str,
) -> Result<bool> {
    if let Some(owner_user_id) = owner_user_id {
        return Ok(sqlx::query_scalar::<_, bool>(
            "SELECT enabled FROM cli_runtime_policy
             WHERE owner_user_id = ? AND daemon_id = ? AND executor_type = ?",
        )
        .bind(owner_user_id)
        .bind(daemon_id)
        .bind(executor_type)
        .fetch_optional(db.pool())
        .await?
        .unwrap_or(true));
    }

    // Legacy/global Agents have no owner to scope the local policy lookup.
    // In the current single-user product, any explicit disable for the exact
    // daemon/runtime source must still deactivate those Agents; otherwise the
    // provider screen would claim the source is disabled while global Agents
    // continued to receive work through it.
    Ok(sqlx::query_scalar::<_, Option<bool>>(
        "SELECT MIN(enabled) FROM cli_runtime_policy
         WHERE daemon_id = ? AND executor_type = ?",
    )
    .bind(daemon_id)
    .bind(executor_type)
    .fetch_one(db.pool())
    .await?
    .unwrap_or(true))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DetectedCli {
    kind: String,
    availability: Option<String>,
}

fn parse_detected_clis(value: &str) -> Result<Vec<DetectedCli>> {
    let value = serde_json::from_str::<Value>(value).map_err(|error| {
        ServiceError::invalid_operation(format!("invalid daemon detected_clis_json: {error}"))
    })?;
    let Value::Array(items) = value else {
        return Ok(Vec::new());
    };
    Ok(items
        .into_iter()
        .filter_map(|item| {
            let object = item.as_object()?;
            Some(DetectedCli {
                kind: object.get("kind")?.as_str()?.to_owned(),
                availability: object
                    .get("availability")
                    .and_then(|value| value.as_str())
                    .map(str::to_owned),
            })
        })
        .collect())
}

fn daemon_supports_executor(daemon: &Daemon, executor_type: &str) -> Result<bool> {
    let detected_clis = parse_detected_clis(&daemon.detected_clis_json)?;
    Ok(detected_clis.iter().any(|cli| {
        cli.kind == executor_type && cli.availability.as_deref() == Some("authenticated")
    }))
}

#[derive(Clone)]
pub struct AgentService {
    db: Arc<SqliteDb>,
    event_bus: Arc<EventBus>,
}

impl AgentService {
    pub fn new(db: Arc<SqliteDb>, event_bus: Arc<EventBus>) -> Self {
        Self { db, event_bus }
    }

    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(
        skip(self, name, config_json, capabilities_json),
        fields(agent_name = tracing::field::Empty, executor_type = %executor_type, daemon_id = ?daemon_id)
    )]
    pub async fn register(
        &self,
        name: impl Into<String>,
        description: Option<String>,
        executor_type: String,
        model: Option<String>,
        reasoning_effort: Option<String>,
        permission_policy: Option<String>,
        prompt_template: Option<String>,
        capabilities_json: String,
        config_json: String,
        credential_ref: Option<String>,
        daemon_id: Option<String>,
        max_concurrent: Option<i64>,
        heartbeat_interval: Option<i64>,
        max_missed: Option<i64>,
        is_default: bool,
        owner_id: Option<String>,
        visibility: Option<String>,
    ) -> Result<Agent> {
        let name = name.into();
        tracing::Span::current().record("agent_name", tracing::field::display(&name));
        if executor_type.trim().eq_ignore_ascii_case("embedded") {
            return Err(ServiceError::invalid_operation(
                "embedded identities must be created through the protected embedded-agent connection",
            ));
        }
        validate_positive("max_concurrent", max_concurrent.unwrap_or(1))?;
        validate_positive("heartbeat_interval", heartbeat_interval.unwrap_or(30))?;
        validate_positive("max_missed", max_missed.unwrap_or(3))?;

        let now = now_rfc3339();
        let agent = AgentRepo::create(
            &*self.db,
            CreateAgent {
                id: new_uuid_v4(),
                name,
                description,
                executor_type,
                model,
                reasoning_effort,
                permission_policy,
                prompt_template,
                capabilities_json,
                config_json,
                credential_ref,
                daemon_id,
                max_concurrent_tasks: max_concurrent.unwrap_or(1),
                heartbeat_interval_seconds: heartbeat_interval.unwrap_or(30),
                max_missed_heartbeats: max_missed.unwrap_or(3),
                status: AgentStatus::Idle,
                last_heartbeat_at: Some(now.clone()),
                is_default,
                paused: false,
                owner_id,
                visibility: visibility.unwrap_or_else(|| "global".to_owned()),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await?;

        self.publish(ForgeEvent {
            event_type: "agent.created".to_owned(),
            entity_id: agent.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::AgentCreated {
                name: agent.name.clone(),
            },
        });

        Ok(agent)
    }

    #[tracing::instrument(
        skip(self, agent_id),
        fields(agent_id = tracing::field::Empty, status = %status, version = version)
    )]
    pub async fn update_status(
        &self,
        agent_id: impl Into<String>,
        status: AgentStatus,
        version: i64,
    ) -> Result<Agent> {
        let agent_id = agent_id.into();
        tracing::Span::current().record("agent_id", tracing::field::display(&agent_id));
        validate_required("agent_id", &agent_id)?;
        let old = AgentRepo::get_by_id(&*self.db, &agent_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent", agent_id.clone()))?;

        let agent = AgentRepo::update(
            &*self.db,
            UpdateAgent {
                id: agent_id,
                expected_version: version,
                name: None,
                description: None,
                max_concurrent_tasks: None,
                heartbeat_interval_seconds: None,
                max_missed_heartbeats: None,
                status: Some(status),
                last_heartbeat_at: None,
                model: None,
                reasoning_effort: None,
                permission_policy: None,
                capabilities_json: None,
                config_json: None,
                daemon_id: None,
                is_default: None,
                paused: None,
                prompt_template: None,
                updated_at: now_rfc3339(),
            },
        )
        .await?;

        self.publish(ForgeEvent {
            event_type: "agent.status_changed".to_owned(),
            entity_id: agent.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::AgentStatusChanged {
                old_status: old.status.to_string(),
                new_status: agent.status.to_string(),
            },
        });

        Ok(agent)
    }

    #[tracing::instrument(skip(self, agent_id), fields(agent_id = tracing::field::Empty))]
    pub async fn update_heartbeat(&self, agent_id: impl Into<String>) -> Result<()> {
        let agent_id = agent_id.into();
        tracing::Span::current().record("agent_id", tracing::field::display(&agent_id));
        validate_required("agent_id", &agent_id)?;
        let agent = AgentRepo::get_by_id(&*self.db, &agent_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent", agent_id.clone()))?;
        AgentRepo::update(
            &*self.db,
            UpdateAgent {
                id: agent_id,
                expected_version: agent.version,
                name: None,
                description: None,
                max_concurrent_tasks: None,
                heartbeat_interval_seconds: None,
                max_missed_heartbeats: None,
                status: None,
                last_heartbeat_at: Some(Some(now_rfc3339())),
                model: None,
                reasoning_effort: None,
                permission_policy: None,
                capabilities_json: None,
                config_json: None,
                daemon_id: None,
                is_default: None,
                paused: None,
                prompt_template: None,
                updated_at: now_rfc3339(),
            },
        )
        .await?;
        Ok(())
    }

    #[tracing::instrument(
        skip(self, capabilities_filter),
        fields(capabilities_count = capabilities_filter.as_ref().map(Vec::len).unwrap_or_default())
    )]
    pub async fn list_available(
        &self,
        capabilities_filter: Option<Vec<String>>,
    ) -> Result<Vec<Agent>> {
        let page = AgentRepo::list(
            &*self.db,
            AgentListQuery {
                status: Some(AgentStatus::Idle),
                executor_type: None,
                capabilities: capabilities_filter.unwrap_or_default(),
                page: PageRequest {
                    cursor: None,
                    limit: 500,
                    include_total: false,
                    sort_by: SortBy::CreatedAt,
                    sort_order: SortOrder::Asc,
                },
            },
        )
        .await?;

        let mut available = Vec::new();
        for agent in page.items {
            if has_running_execution_capacity(&self.db, &agent).await? {
                available.push(agent);
            }
        }
        Ok(available)
    }

    #[tracing::instrument(skip(self, agent_id), fields(agent_id = tracing::field::Empty))]
    pub async fn archive(&self, agent_id: impl Into<String>) -> Result<()> {
        let agent_id = agent_id.into();
        tracing::Span::current().record("agent_id", tracing::field::display(&agent_id));
        validate_required("agent_id", &agent_id)?;
        let task_service = TaskService::new(Arc::clone(&self.db), Arc::clone(&self.event_bus));
        let mut transaction = db::begin_immediate(self.db.pool()).await?;
        let role_events = task_service
            .on_agent_deleted_in_tx(&mut transaction, &agent_id)
            .await?;
        let now = now_rfc3339();
        let result = sqlx::query(
            "UPDATE agent_identity
             SET archived_at = ?, paused = 1, is_default = 0, status = 'offline',
                 version = version + 1, updated_at = ?
             WHERE id = ? AND archived_at IS NULL",
        )
        .bind(&now)
        .bind(&now)
        .bind(&agent_id)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            return Err(ServiceError::not_found("agent", agent_id));
        }
        transaction.commit().await?;
        task_service.publish_role_sweep_events(role_events);
        self.publish(ForgeEvent {
            event_type: "agent.archived".to_owned(),
            entity_id: agent_id,
            timestamp: event_timestamp(),
            context: EventContext::AgentArchived {},
        });
        Ok(())
    }

    fn publish(&self, event: ForgeEvent) {
        self.event_bus.publish(event);
    }
}

fn validate_required(field: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(ServiceError::invalid_operation(format!(
            "{field} must not be empty"
        )));
    }
    Ok(())
}

fn validate_positive(field: &str, value: i64) -> Result<()> {
    if value <= 0 {
        return Err(ServiceError::invalid_operation(format!(
            "{field} must be positive"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use db::{
        create_sqlite_pool, run_migrations, CreateExecution, CreateProject, CreateRepo, CreateTask,
        CreateTaskRoleAssignment, ExecutionRepo, ExecutionStatus, ProjectRepo, RepoRepo, TaskRepo,
        TaskRoleAssignmentRepo, UpdateProject, UpsertDaemon,
    };

    async fn sqlite_db() -> SqliteDb {
        let pool = create_sqlite_pool("sqlite::memory:")
            .await
            .expect("pool creates");
        run_migrations(&pool).await.expect("migrations run");
        SqliteDb::new(pool)
    }

    async fn seed_daemon(db: &SqliteDb) -> String {
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

        daemon_id
    }

    async fn seed_effective_agent(
        db: &SqliteDb,
        agent_status: AgentStatus,
        daemon_status: DaemonStatus,
        detected_clis_json: &str,
        max_concurrent_tasks: i64,
    ) -> Agent {
        let daemon_id = seed_daemon(db).await;
        DaemonRepo::update_report(
            db,
            db::UpdateDaemonReport {
                id: daemon_id.clone(),
                detected_clis_json: detected_clis_json.to_owned(),
                labels_json: None,
                status: daemon_status,
                last_report_at: now_rfc3339(),
                updated_at: now_rfc3339(),
            },
        )
        .await
        .expect("daemon report updates");

        let now = now_rfc3339();
        AgentRepo::create(
            db,
            CreateAgent {
                id: new_uuid_v4(),
                name: "shell".to_owned(),
                description: None,
                executor_type: "shell".to_owned(),
                model: None,
                reasoning_effort: None,
                permission_policy: None,
                capabilities_json: "[]".to_owned(),
                config_json: "{}".to_owned(),
                credential_ref: None,
                daemon_id: Some(daemon_id),
                max_concurrent_tasks,
                heartbeat_interval_seconds: 30,
                max_missed_heartbeats: 3,
                status: agent_status,
                last_heartbeat_at: Some(now.clone()),
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
        .expect("agent creates")
    }

    fn detached_agent(status: AgentStatus) -> Agent {
        Agent {
            id: new_uuid_v4(),
            name: "detached".to_owned(),
            description: None,
            profile_id: new_uuid_v4(),
            backend_kind: "cli".to_owned(),
            executor_type: "shell".to_owned(),
            provider: None,
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            capabilities_json: "[]".to_owned(),
            tool_policy_json: "{}".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: Some("missing-daemon".to_owned()),
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: None,
            visibility: "global".to_owned(),
            prompt_template: None,
            version: 1,
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        }
    }

    async fn seed_active_task(db: &SqliteDb, agent_id: &str) {
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
        let task = TaskRepo::create(
            db,
            CreateTask {
                id: new_uuid_v4(),
                project_id,
                repo_id: Some(repo_id),
                parent_task_id: None,
                subtask_order: None,
                assignee_type: Some("agent".to_owned()),
                assignee_id: Some(agent_id.to_owned()),
                title: "Active task".to_owned(),
                description: None,
                task_type: "task".to_owned(),
                status: "in_progress".to_owned(),
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
        TaskRoleAssignmentRepo::assign(
            db,
            CreateTaskRoleAssignment {
                id: new_uuid_v4(),
                task_id: task.id.clone(),
                role_name: "coder".to_owned(),
                assignee_type: Some(db::AssigneeKind::Agent),
                assignee_id: Some(agent_id.to_owned()),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("role assignment creates");
        ExecutionRepo::create(
            db,
            CreateExecution {
                id: new_uuid_v4(),
                task_id: task.id,
                agent_id: Some(agent_id.to_owned()),
                role: "coder".to_owned(),
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
                updated_at: now,
            },
        )
        .await
        .expect("execution creates");
    }

    #[tokio::test]
    async fn register_update_heartbeat_list_and_archive_agent() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(16));
        let service = AgentService::new(Arc::clone(&db), Arc::clone(&event_bus));
        let mut rx = event_bus.subscribe();
        let daemon_id = seed_daemon(&db).await;

        let agent = service
            .register(
                "codex",
                None,
                "shell".to_owned(),
                None,
                None,
                None,
                None,
                r#"["rust","sqlite"]"#.to_owned(),
                "{}".to_owned(),
                None,
                Some(daemon_id),
                Some(2),
                None,
                None,
                false,
                None,
                None,
            )
            .await
            .expect("agent registers");
        assert_eq!(agent.status, AgentStatus::Idle);
        assert_eq!(agent.max_concurrent_tasks, 2);
        assert_eq!(rx.recv().await.unwrap().event_type, "agent.created");

        let updated = service
            .update_status(agent.id.clone(), AgentStatus::Busy, agent.version)
            .await
            .expect("agent status updates");
        assert_eq!(updated.status, AgentStatus::Busy);
        assert_eq!(rx.recv().await.unwrap().event_type, "agent.status_changed");

        service
            .update_heartbeat(updated.id.clone())
            .await
            .expect("heartbeat updates");
        let reloaded = AgentRepo::get_by_id(&*db, &updated.id)
            .await
            .expect("agent loads")
            .expect("agent exists");
        assert!(reloaded.last_heartbeat_at.is_some());

        let available = service
            .list_available(Some(vec!["rust".to_owned()]))
            .await
            .expect("available agents list");
        assert!(available.is_empty());

        let idle = service
            .update_status(reloaded.id.clone(), AgentStatus::Idle, reloaded.version)
            .await
            .expect("agent returns idle");
        let available = service
            .list_available(Some(vec!["rust".to_owned()]))
            .await
            .expect("available agents list");
        assert_eq!(available, vec![idle.clone()]);

        service.archive(idle.id).await.expect("agent archives");
        assert_eq!(rx.recv().await.unwrap().event_type, "agent.status_changed");
        assert_eq!(rx.recv().await.unwrap().event_type, "agent.archived");
    }

    #[tokio::test]
    async fn register_validates_positive_fields() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(16));
        let service = AgentService::new(db, event_bus);

        let result = service
            .register(
                "codex",
                None,
                "shell".to_owned(),
                None,
                None,
                None,
                None,
                "[]".to_owned(),
                "{}".to_owned(),
                None,
                None,
                Some(0),
                None,
                None,
                false,
                None,
                None,
            )
            .await;
        assert!(matches!(result, Err(ServiceError::InvalidOperation { .. })));
    }

    #[tokio::test]
    async fn effective_status_is_error_when_agent_status_is_error() {
        let db = sqlite_db().await;
        let agent = detached_agent(AgentStatus::Error);

        let status = compute_effective_status(&db, &agent)
            .await
            .expect("status computes");

        assert_eq!(status, EffectiveStatus::Error);
        assert_eq!(status.as_str(), "error");
        assert_eq!(status.to_string(), "error");
    }

    #[tokio::test]
    async fn effective_status_is_daemon_offline_when_daemon_missing() {
        let db = sqlite_db().await;
        let agent = detached_agent(AgentStatus::Idle);

        let status = compute_effective_status(&db, &agent)
            .await
            .expect("status computes");

        assert_eq!(status, EffectiveStatus::DaemonOffline);
    }

    #[tokio::test]
    async fn effective_status_is_daemon_offline_when_daemon_offline() {
        let db = sqlite_db().await;
        let agent = seed_effective_agent(
            &db,
            AgentStatus::Idle,
            DaemonStatus::Offline,
            r#"[{"kind":"shell","availability":"authenticated"}]"#,
            1,
        )
        .await;

        let status = compute_effective_status(&db, &agent)
            .await
            .expect("status computes");

        assert_eq!(status, EffectiveStatus::DaemonOffline);
    }

    #[tokio::test]
    async fn effective_status_is_deactivated_without_authenticated_cli() {
        let db = sqlite_db().await;
        let agent = seed_effective_agent(
            &db,
            AgentStatus::Idle,
            DaemonStatus::Online,
            r#"[{"kind":"shell","availability":"installed"}]"#,
            1,
        )
        .await;

        let status = compute_effective_status(&db, &agent)
            .await
            .expect("status computes");

        assert_eq!(status, EffectiveStatus::Deactivated);
    }

    #[tokio::test]
    async fn effective_status_is_busy_at_capacity() {
        let db = sqlite_db().await;
        let agent = seed_effective_agent(
            &db,
            AgentStatus::Idle,
            DaemonStatus::Online,
            r#"[{"kind":"shell","availability":"authenticated"}]"#,
            1,
        )
        .await;
        seed_active_task(&db, &agent.id).await;

        let status = compute_effective_status(&db, &agent)
            .await
            .expect("status computes");

        assert_eq!(status, EffectiveStatus::Busy);
    }

    #[tokio::test]
    async fn effective_status_is_paused() {
        let db = sqlite_db().await;
        let mut agent = seed_effective_agent(
            &db,
            AgentStatus::Idle,
            DaemonStatus::Online,
            r#"[{"kind":"shell","availability":"authenticated"}]"#,
            1,
        )
        .await;
        agent.paused = true;

        let status = compute_effective_status(&db, &agent)
            .await
            .expect("status computes");

        assert_eq!(status, EffectiveStatus::Paused);
    }

    #[tokio::test]
    async fn effective_status_is_active_when_authenticated_and_not_busy() {
        let db = sqlite_db().await;
        let agent = seed_effective_agent(
            &db,
            AgentStatus::Idle,
            DaemonStatus::Online,
            r#"[{"kind":"shell","availability":"authenticated"}]"#,
            1,
        )
        .await;

        let status = compute_effective_status(&db, &agent)
            .await
            .expect("status computes");

        assert_eq!(status, EffectiveStatus::Active);
    }

    #[tokio::test]
    async fn disabling_exact_cli_runtime_deactivates_and_reenables_global_agents() {
        let db = sqlite_db().await;
        let agent = seed_effective_agent(
            &db,
            AgentStatus::Idle,
            DaemonStatus::Online,
            r#"[{"kind":"shell","availability":"authenticated"}]"#,
            1,
        )
        .await;
        let owner_id = new_uuid_v4();
        let now = now_rfc3339();
        sqlx::query(
            "INSERT INTO user (id, email, password_hash, created_at, updated_at)
             VALUES (?, ?, 'test', ?, ?)",
        )
        .bind(&owner_id)
        .bind(format!("{owner_id}@example.test"))
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("policy owner creates");
        sqlx::query(
            "INSERT INTO cli_runtime_policy (
                 owner_user_id, daemon_id, executor_type, enabled,
                 version, created_at, updated_at
             ) VALUES (?, ?, 'shell', 0, 1, ?, ?)",
        )
        .bind(&owner_id)
        .bind(agent.daemon_id.as_deref().expect("Agent has daemon"))
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("CLI runtime disables");

        assert_eq!(
            compute_effective_status(&db, &agent)
                .await
                .expect("disabled status computes"),
            EffectiveStatus::SourceDisabled
        );

        sqlx::query(
            "UPDATE cli_runtime_policy SET enabled = 1, version = 2, updated_at = ?
             WHERE owner_user_id = ? AND daemon_id = ? AND executor_type = 'shell'",
        )
        .bind(&now)
        .bind(&owner_id)
        .bind(agent.daemon_id.as_deref().expect("Agent has daemon"))
        .execute(db.pool())
        .await
        .expect("CLI runtime reenables");
        assert_eq!(
            compute_effective_status(&db, &agent)
                .await
                .expect("reenabled status computes"),
            EffectiveStatus::Active
        );
    }

    #[tokio::test]
    async fn disabling_provider_entry_deactivates_dependent_agent() {
        let db = sqlite_db().await;
        let owner_id = new_uuid_v4();
        let credential_id = new_uuid_v4();
        let now = now_rfc3339();
        sqlx::query(
            "INSERT INTO user (id, email, password_hash, created_at, updated_at)
             VALUES (?, ?, 'test', ?, ?)",
        )
        .bind(&owner_id)
        .bind(format!("{owner_id}@example.test"))
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("provider owner creates");
        sqlx::query(
            "INSERT INTO credential_handle (
                 id, owner_user_id, provider, label, status, enabled,
                 created_at, updated_at
             ) VALUES (?, ?, 'openai', 'Disabled provider', 'configured', 0, ?, ?)",
        )
        .bind(&credential_id)
        .bind(&owner_id)
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("disabled provider creates");
        let daemon_id = seed_daemon(&db).await;
        DaemonRepo::update_report(
            &db,
            db::UpdateDaemonReport {
                id: daemon_id.clone(),
                detected_clis_json: r#"[{"kind":"shell","availability":"authenticated"}]"#
                    .to_owned(),
                labels_json: None,
                status: DaemonStatus::Online,
                last_report_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("daemon report updates");
        let agent = AgentRepo::create(
            &db,
            CreateAgent {
                id: new_uuid_v4(),
                name: "provider-agent".to_owned(),
                description: None,
                executor_type: "shell".to_owned(),
                model: None,
                reasoning_effort: None,
                permission_policy: None,
                capabilities_json: "[]".to_owned(),
                config_json: "{}".to_owned(),
                credential_ref: Some(credential_id),
                daemon_id: Some(daemon_id),
                max_concurrent_tasks: 1,
                heartbeat_interval_seconds: 30,
                max_missed_heartbeats: 3,
                status: AgentStatus::Idle,
                last_heartbeat_at: Some(now.clone()),
                is_default: false,
                paused: false,
                owner_id: Some(owner_id),
                visibility: "account".to_owned(),
                prompt_template: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("provider Agent creates");

        assert_eq!(
            compute_effective_status(&db, &agent)
                .await
                .expect("provider-disabled status computes"),
            EffectiveStatus::SourceDisabled
        );
    }
}
