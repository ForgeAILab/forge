use crate::{recovery::reconcile_daemon_report_executions, Result, ServiceError, TaskService};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use db::{
    new_uuid_v4, now_rfc3339, AgentRepo, AgentStatus, CreateAgent, CreateRuntime, Daemon,
    DaemonRepo, DaemonStatus, Page, PageRequest, RuntimeRepo, RuntimeStatus, SqliteDb,
    UpdateDaemonReport, UpsertDaemon,
};
use events::{event_timestamp, EventBus, EventContext, ForgeEvent};
use executors::ExecutorKind;
use rand::{rngs::OsRng, TryRngCore};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::sync::Arc;

#[derive(Clone)]
pub struct DaemonService {
    db: Arc<SqliteDb>,
    event_bus: Arc<EventBus>,
    task_service: Option<Arc<TaskService>>,
}

#[derive(Debug, Clone)]
pub struct DaemonRegisterInput {
    pub machine_id: String,
    pub hostname: String,
    pub os: String,
    pub arch: String,
    pub agent_version: Option<String>,
    pub labels: Value,
    pub runtimes: Vec<RuntimeReportInput>,
    pub owner_id: Option<String>,
    pub visibility: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DaemonRegistration {
    pub daemon_id: String,
    pub plaintext_token: String,
}

#[derive(Debug, Clone)]
pub struct DaemonReportInput {
    pub detected_clis: Vec<DetectedCliInput>,
    pub runtimes: Vec<RuntimeReportInput>,
    pub labels: Option<Value>,
    pub active_execution_ids: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetectedCliInput {
    pub kind: String,
    pub availability: String,
    pub config_path: Option<String>,
    pub version: Option<String>,
    pub path: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RuntimeReportInput {
    pub kind: String,
    pub workspace_root: String,
    pub status: Option<String>,
}

impl DaemonService {
    pub fn new(db: Arc<SqliteDb>, event_bus: Arc<EventBus>) -> Self {
        Self {
            db,
            event_bus,
            task_service: None,
        }
    }

    pub fn with_task_service(mut self, task_service: Arc<TaskService>) -> Self {
        self.task_service = Some(task_service);
        self
    }

    #[tracing::instrument(
        skip(self, input),
        fields(machine_id = %input.machine_id, hostname = %input.hostname, os = %input.os, arch = %input.arch)
    )]
    pub async fn register(&self, input: DaemonRegisterInput) -> Result<DaemonRegistration> {
        validate_required("machine_id", &input.machine_id)?;
        validate_required("hostname", &input.hostname)?;
        validate_required("os", &input.os)?;
        validate_required("arch", &input.arch)?;

        let token = generate_token();
        let token_hash = hash_token(&token);
        let now = now_rfc3339();
        let visibility = input.visibility.unwrap_or_else(|| "global".to_owned());
        let daemon = DaemonRepo::upsert_by_machine_id(
            &*self.db,
            UpsertDaemon {
                id: new_uuid_v4(),
                machine_id: input.machine_id,
                hostname: input.hostname,
                os: input.os,
                arch: input.arch,
                agent_version: input.agent_version,
                labels_json: serialize_value(input.labels, "labels")?,
                status: DaemonStatus::Online,
                registration_token_hash: Some(token_hash),
                owner_id: input.owner_id,
                visibility,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await?;

        self.upsert_runtimes(&daemon.id, input.runtimes).await?;

        self.publish(ForgeEvent {
            event_type: "daemon.registered".to_owned(),
            entity_id: daemon.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::DaemonRegistered {},
        });

        Ok(DaemonRegistration {
            daemon_id: daemon.id,
            plaintext_token: token,
        })
    }

    #[tracing::instrument(skip(self, token), fields(daemon_id = %daemon_id))]
    pub async fn authenticate(&self, daemon_id: &str, token: &str) -> Result<Daemon> {
        validate_required("daemon_id", daemon_id)?;
        validate_required("token", token)?;

        let daemon = DaemonRepo::get_by_id(&*self.db, daemon_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("daemon", daemon_id.to_owned()))?;
        let Some(expected_hash) = daemon.registration_token_hash.as_deref() else {
            return Err(ServiceError::invalid_operation(
                "daemon has no registration token",
            ));
        };
        if expected_hash != hash_token(token) {
            return Err(ServiceError::invalid_operation(
                "invalid daemon registration token",
            ));
        }
        Ok(daemon)
    }

    #[tracing::instrument(
        skip(self, input),
        fields(
            daemon_id = %daemon_id,
            detected_clis_count = input.detected_clis.len(),
            runtimes_count = input.runtimes.len(),
        )
    )]
    pub async fn ingest_report(&self, daemon_id: &str, input: DaemonReportInput) -> Result<Daemon> {
        validate_required("daemon_id", daemon_id)?;
        let detected_clis = filter_detected_clis(input.detected_clis);
        let now = now_rfc3339();
        let daemon = DaemonRepo::update_report(
            &*self.db,
            UpdateDaemonReport {
                id: daemon_id.to_owned(),
                last_report_at: now.clone(),
                status: DaemonStatus::Online,
                detected_clis_json: serialize_value(&detected_clis, "detected_clis")?,
                labels_json: input
                    .labels
                    .map(|labels| serialize_value(labels, "labels"))
                    .transpose()?,
                updated_at: now,
            },
        )
        .await?;

        self.upsert_runtimes(daemon_id, input.runtimes).await?;
        self.ensure_agents_for_daemon(&daemon, &detected_clis)
            .await?;

        if let Some(active_execution_ids) = input.active_execution_ids.as_ref() {
            let interrupted = reconcile_daemon_report_executions(
                &self.db,
                &self.event_bus,
                self.task_service.as_deref(),
                &daemon,
                active_execution_ids,
            )
            .await?;
            if interrupted > 0 {
                tracing::info!(
                    daemon_id = %daemon_id,
                    interrupted_executions = interrupted,
                    "daemon report reconciled missing active executions"
                );
            }
        }

        self.publish(ForgeEvent {
            event_type: "daemon.report_received".to_owned(),
            entity_id: daemon_id.to_owned(),
            timestamp: event_timestamp(),
            context: EventContext::DaemonReportReceived {
                detected_clis_count: detected_clis.len(),
            },
        });

        Ok(daemon)
    }

    #[tracing::instrument(skip(self), fields(daemon_id = %daemon_id))]
    pub async fn mark_connected(&self, daemon_id: &str) -> Result<Daemon> {
        let daemon = self.touch_connection(daemon_id).await?;
        self.publish(ForgeEvent {
            event_type: "daemon.connected".to_owned(),
            entity_id: daemon.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::DaemonConnected {},
        });
        Ok(daemon)
    }

    #[tracing::instrument(skip(self), fields(daemon_id = %daemon_id))]
    pub async fn touch_connection(&self, daemon_id: &str) -> Result<Daemon> {
        validate_required("daemon_id", daemon_id)?;
        let now = now_rfc3339();
        DaemonRepo::mark_online(&*self.db, daemon_id, &now)
            .await
            .map_err(Into::into)
    }

    #[tracing::instrument(skip(self), fields(daemon_id = %daemon_id))]
    pub async fn mark_disconnected(&self, daemon_id: &str) -> Result<Daemon> {
        validate_required("daemon_id", daemon_id)?;
        let now = now_rfc3339();
        let daemon = DaemonRepo::mark_offline(&*self.db, daemon_id, &now).await?;
        self.publish(ForgeEvent {
            event_type: "daemon.offline".to_owned(),
            entity_id: daemon.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::DaemonOffline {},
        });
        self.publish(ForgeEvent {
            event_type: "reconciliation.event".to_owned(),
            entity_id: daemon.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::ReconciliationEvent {
                task_id: None,
                execution_id: None,
                reason: "daemon disconnected".to_owned(),
            },
        });
        Ok(daemon)
    }

    #[tracing::instrument(skip(self), fields(retained_machine_id = %retained_machine_id))]
    pub async fn mark_external_daemons_disconnected(
        &self,
        retained_machine_id: &str,
        reason: &str,
    ) -> Result<u64> {
        validate_required("retained_machine_id", retained_machine_id)?;
        validate_required("reason", reason)?;

        let daemon_ids = sqlx::query_scalar::<_, String>(
            "SELECT id
             FROM daemon
             WHERE status = 'online'
               AND machine_id != ?",
        )
        .bind(retained_machine_id)
        .fetch_all(self.db.pool())
        .await?;
        if daemon_ids.is_empty() {
            return Ok(0);
        }

        let now = now_rfc3339();
        let result = sqlx::query(
            "UPDATE daemon
             SET status = 'offline',
                 updated_at = ?,
                 version = version + 1
             WHERE status = 'online'
               AND machine_id != ?",
        )
        .bind(&now)
        .bind(retained_machine_id)
        .execute(self.db.pool())
        .await?;

        for daemon_id in daemon_ids {
            self.publish(ForgeEvent {
                event_type: "daemon.offline".to_owned(),
                entity_id: daemon_id.clone(),
                timestamp: event_timestamp(),
                context: EventContext::DaemonOffline {},
            });
            self.publish(ForgeEvent {
                event_type: "reconciliation.event".to_owned(),
                entity_id: daemon_id,
                timestamp: event_timestamp(),
                context: EventContext::ReconciliationEvent {
                    task_id: None,
                    execution_id: None,
                    reason: reason.to_owned(),
                },
            });
        }

        Ok(result.rows_affected())
    }

    async fn ensure_agents_for_daemon(
        &self,
        daemon: &Daemon,
        detected_clis: &[DetectedCliInput],
    ) -> Result<()> {
        let Some(owner_id) = daemon.owner_id.clone() else {
            return Ok(());
        };

        for cli in detected_clis {
            if cli.availability != "authenticated" {
                continue;
            }
            if self
                .daemon_agent_exists(&daemon.id, &cli.kind, owner_id.as_str())
                .await?
            {
                continue;
            }

            let now = now_rfc3339();
            let agent = AgentRepo::create(
                &*self.db,
                CreateAgent {
                    id: new_uuid_v4(),
                    name: daemon_agent_name(&cli.kind, daemon),
                    description: None,
                    executor_type: cli.kind.clone(),
                    model: None,
                    reasoning_effort: None,
                    permission_policy: None,
                    prompt_template: None,
                    capabilities_json: "[]".to_owned(),
                    config_json: "{}".to_owned(),
                    credential_ref: None,
                    daemon_id: Some(daemon.id.clone()),
                    max_concurrent_tasks: 1,
                    heartbeat_interval_seconds: 30,
                    max_missed_heartbeats: 3,
                    status: AgentStatus::Idle,
                    last_heartbeat_at: Some(now.clone()),
                    is_default: false,
                    paused: false,
                    owner_id: Some(owner_id.clone()),
                    visibility: daemon.visibility.clone(),
                    created_at: now.clone(),
                    updated_at: now,
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
        }

        Ok(())
    }

    async fn daemon_agent_exists(
        &self,
        daemon_id: &str,
        executor_type: &str,
        owner_id: &str,
    ) -> Result<bool> {
        let exists = sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(
                SELECT 1
                FROM agent_current
                WHERE daemon_id = ?
                  AND executor_type = ?
                  AND owner_id = ?
                LIMIT 1
            )",
        )
        .bind(daemon_id)
        .bind(executor_type)
        .bind(owner_id)
        .fetch_one(self.db.pool())
        .await?;

        Ok(exists != 0)
    }

    #[tracing::instrument(skip(self, page), fields(limit = page.limit))]
    pub async fn list(&self, page: PageRequest) -> Result<Page<Daemon>> {
        DaemonRepo::list(&*self.db, page).await.map_err(Into::into)
    }

    #[tracing::instrument(skip(self, page), fields(limit = page.limit))]
    pub async fn list_visible(
        &self,
        user_id: Option<&str>,
        page: PageRequest,
    ) -> Result<Page<Daemon>> {
        DaemonRepo::list_visible(&*self.db, user_id, page)
            .await
            .map_err(Into::into)
    }

    #[tracing::instrument(skip(self), fields(daemon_id = %id))]
    pub async fn get(&self, id: &str) -> Result<Option<Daemon>> {
        validate_required("daemon_id", id)?;
        DaemonRepo::get_by_id(&*self.db, id)
            .await
            .map_err(Into::into)
    }

    #[tracing::instrument(skip(self), fields(daemon_id = %id))]
    pub async fn get_visible(&self, id: &str, user_id: Option<&str>) -> Result<Option<Daemon>> {
        validate_required("daemon_id", id)?;
        DaemonRepo::get_visible(&*self.db, id, user_id)
            .await
            .map_err(Into::into)
    }

    #[tracing::instrument(skip(self, runtimes), fields(daemon_id = %daemon_id, runtimes_count = runtimes.len()))]
    async fn upsert_runtimes(
        &self,
        daemon_id: &str,
        runtimes: Vec<RuntimeReportInput>,
    ) -> Result<()> {
        for runtime in runtimes {
            validate_required("runtime.kind", &runtime.kind)?;
            validate_required("runtime.workspace_root", &runtime.workspace_root)?;
            let status = runtime
                .status
                .as_deref()
                .unwrap_or("ready")
                .parse::<RuntimeStatus>()
                .map_err(ServiceError::invalid_operation)?;
            let now = now_rfc3339();
            RuntimeRepo::upsert_by_daemon_kind(
                &*self.db,
                CreateRuntime {
                    id: new_uuid_v4(),
                    daemon_id: daemon_id.to_owned(),
                    kind: runtime.kind,
                    workspace_root: runtime.workspace_root,
                    status,
                    labels_json: "{}".to_owned(),
                    created_at: now.clone(),
                    updated_at: now,
                },
            )
            .await?;
        }
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

fn serialize_value<T>(value: T, field: &str) -> Result<String>
where
    T: Serialize,
{
    serde_json::to_string(&value)
        .map_err(|error| ServiceError::invalid_operation(format!("invalid {field} JSON: {error}")))
}

fn generate_token() -> String {
    let mut bytes = [0_u8; 32];
    OsRng
        .try_fill_bytes(&mut bytes)
        .expect("OS RNG unavailable");
    URL_SAFE_NO_PAD.encode(bytes)
}

fn hash_token(token: &str) -> String {
    let digest = Sha256::digest(token.as_bytes());
    URL_SAFE_NO_PAD.encode(digest)
}

fn filter_detected_clis(detected_clis: Vec<DetectedCliInput>) -> Vec<DetectedCliInput> {
    detected_clis
        .into_iter()
        .filter(|detected_cli| {
            if detected_cli.kind.parse::<ExecutorKind>().is_ok() {
                true
            } else {
                tracing::warn!(
                    executor_kind = %detected_cli.kind,
                    "ignoring daemon report entry with unknown executor kind"
                );
                false
            }
        })
        .collect()
}

fn daemon_agent_name(executor_type: &str, daemon: &Daemon) -> String {
    let display = executor_display_name(executor_type);
    let host = if daemon.hostname.trim().is_empty() {
        daemon.machine_id.as_str()
    } else {
        daemon.hostname.as_str()
    };
    format!("{display} on {host}")
}

fn executor_display_name(executor_type: &str) -> &str {
    match executor_type {
        "claude_code" => "Claude Code",
        "codex" => "Codex",
        "cursor" => "Cursor",
        "gemini" => "Gemini",
        "opencode" => "OpenCode",
        "shell" => "Shell",
        "smith" => "Smith",
        "null" => "Null",
        _ => executor_type,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use db::{create_sqlite_pool, run_migrations, DaemonStatus, User, UserRepo};

    async fn service() -> DaemonService {
        let pool = create_sqlite_pool("sqlite::memory:")
            .await
            .expect("pool creates");
        run_migrations(&pool).await.expect("migrations run");
        let db = Arc::new(SqliteDb::new(pool));
        let event_bus = Arc::new(EventBus::new(16));
        DaemonService::new(db, event_bus)
    }

    fn register_input(machine_id: &str) -> DaemonRegisterInput {
        DaemonRegisterInput {
            machine_id: machine_id.to_owned(),
            hostname: "test-host".to_owned(),
            os: "linux".to_owned(),
            arch: "x86_64".to_owned(),
            agent_version: Some("0.1.0".to_owned()),
            labels: serde_json::json!({}),
            runtimes: Vec::new(),
            owner_id: None,
            visibility: Some("global".to_owned()),
        }
    }

    #[tokio::test]
    async fn register_authenticate_roundtrip() {
        let service = service().await;
        let registration = service
            .register(register_input("machine-1"))
            .await
            .expect("register succeeds");

        let daemon = service
            .authenticate(&registration.daemon_id, &registration.plaintext_token)
            .await
            .expect("token authenticates");

        assert_eq!(daemon.id, registration.daemon_id);
        assert_eq!(daemon.machine_id, "machine-1");
    }

    #[tokio::test]
    async fn reregister_rotates_token() {
        let service = service().await;
        let first = service
            .register(register_input("machine-1"))
            .await
            .expect("first register succeeds");
        let second = service
            .register(register_input("machine-1"))
            .await
            .expect("second register succeeds");

        assert_eq!(first.daemon_id, second.daemon_id);
        assert_ne!(first.plaintext_token, second.plaintext_token);
        assert!(service
            .authenticate(&first.daemon_id, &first.plaintext_token)
            .await
            .is_err());
        service
            .authenticate(&second.daemon_id, &second.plaintext_token)
            .await
            .expect("new token authenticates");
    }

    #[tokio::test]
    async fn report_full_cli_set_is_idempotent_for_daemon_agents() {
        let service = service().await;
        let now = now_rfc3339();
        let user_id = "user-1".to_owned();
        UserRepo::create_user(
            &*service.db,
            &User {
                id: user_id.clone(),
                email: "daemon-owner@example.com".to_owned(),
                password_hash: "hash".to_owned(),
                display_name: None,
                is_admin: true,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("user creates");

        let mut input = register_input("machine-1");
        input.owner_id = Some(user_id);
        input.visibility = Some("account".to_owned());
        let registration = service.register(input).await.expect("register succeeds");

        let report = || DaemonReportInput {
            detected_clis: [
                "claude_code",
                "codex",
                "cursor",
                "gemini",
                "opencode",
                "shell",
                "smith",
            ]
            .into_iter()
            .map(|kind| DetectedCliInput {
                kind: kind.to_owned(),
                availability: "authenticated".to_owned(),
                config_path: None,
                version: None,
                path: None,
            })
            .collect(),
            runtimes: Vec::new(),
            labels: None,
            active_execution_ids: None,
        };

        service
            .ingest_report(&registration.daemon_id, report())
            .await
            .expect("first report creates daemon agents");
        service
            .ingest_report(&registration.daemon_id, report())
            .await
            .expect("second report sees existing daemon agents");
    }

    #[tokio::test]
    async fn startup_disconnect_marks_only_external_daemons_offline() {
        let service = service().await;
        let external = service
            .register(register_input("external-machine"))
            .await
            .expect("external daemon registers");
        let embedded_machine_id = crate::embedded_daemon::embedded_machine_id();
        let embedded = service
            .register(register_input(&embedded_machine_id))
            .await
            .expect("embedded daemon registers");

        let count = service
            .mark_external_daemons_disconnected(&embedded_machine_id, "server startup")
            .await
            .expect("startup disconnect succeeds");

        assert_eq!(count, 1);
        let external = DaemonRepo::get_by_id(&*service.db, &external.daemon_id)
            .await
            .expect("external daemon loads")
            .expect("external daemon exists");
        let embedded = DaemonRepo::get_by_id(&*service.db, &embedded.daemon_id)
            .await
            .expect("embedded daemon loads")
            .expect("embedded daemon exists");
        assert_eq!(external.status, DaemonStatus::Offline);
        assert_eq!(embedded.status, DaemonStatus::Online);
    }
}
