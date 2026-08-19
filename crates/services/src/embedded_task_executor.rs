//! Forge-hosted native Task execution.
//!
//! This adapter deliberately implements the existing `TaskExecutor` contract
//! instead of creating a second Task mutation path.  `TaskService::run_execution`
//! remains responsible for claims, execution rows, workspace locking, reviewer
//! restoration, outcome persistence, workflow transitions, and delivery.  The
//! adapter only translates one admitted Task execution into a native runtime
//! turn and returns the normal executor result.

use std::{collections::HashMap, path::Path, sync::Arc};

use async_trait::async_trait;
use db::{AgentProfileRepo, AgentRepo, ExecutionRepo, ExecutionStatus, SqliteDb};
use executors::{
    ExecutionContext, ExecutionOutcome, ExecutionResult, ExecutorError, LogKind, LogStream,
    LogWriter, TaskExecutor, TokenUsage,
};
use forge_agent_host::{
    AgentSessionBackend, AgentTurnRequest, CanonicalScope, CanonicalScopeType,
    NativeAgentRuntimeBackend, NativeProviderConfig, RuntimeContextManifestLink, TurnEventSink,
    WorkspaceAccess,
};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;

use crate::{
    embedded_agent_service::{CreateScopedSession, EmbeddedAgentService, RequestedCanonicalScope},
    ContextManifestInput, ContextManifestService, ContextSourceInput, Result, ServiceError,
};

const EMBEDDED_EXECUTOR_TYPE: &str = "embedded";
const TASK_ROLE_MARKER: &str = "_forge_task_role";

/// The native Task adapter used by the existing Task execution supervisor.
#[derive(Clone)]
pub struct EmbeddedTaskExecutor {
    db: Arc<SqliteDb>,
    embedded_agents: Arc<EmbeddedAgentService>,
    backend: Arc<NativeAgentRuntimeBackend>,
    active: Arc<RwLock<HashMap<String, ActiveTaskTurn>>>,
}

#[derive(Clone)]
struct ActiveTaskTurn {
    cancellation: CancellationToken,
    runtime_session_id: String,
}

impl std::fmt::Debug for EmbeddedTaskExecutor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EmbeddedTaskExecutor")
            .field(
                "active",
                &self.active.try_read().map(|active| active.len()).ok(),
            )
            .finish_non_exhaustive()
    }
}

impl EmbeddedTaskExecutor {
    pub fn new(db: Arc<SqliteDb>, embedded_agents: Arc<EmbeddedAgentService>) -> Self {
        let backend = embedded_agents.native_backend();
        Self {
            db,
            embedded_agents,
            backend,
            active: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn run_native_turn(
        &self,
        ctx: &ExecutionContext,
        agent: db::Agent,
        task_role: &str,
        log_sink: Arc<NativeTaskLogSink>,
        cancellation: CancellationToken,
    ) -> Result<executors::ExecutionResult> {
        let owner_user_id = agent.owner_id.clone().ok_or_else(|| {
            ServiceError::invalid_operation(
                "embedded Task identity has no owner for protected credential access",
            )
        })?;
        let snapshot_profile_id = ctx
            .agent_config
            .get("profile_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                ServiceError::invalid_operation("embedded snapshot has no profile_id")
            })?;
        let profile = AgentProfileRepo::get_profile(&*self.db, snapshot_profile_id)
            .await?
            .filter(|profile| {
                profile.identity_id == agent.id
                    && profile.backend_kind == "native"
                    && profile.executor_type == EMBEDDED_EXECUTOR_TYPE
            })
            .ok_or_else(|| {
                ServiceError::invalid_operation(
                    "embedded snapshot profile is unavailable or no longer native",
                )
            })?;
        let credential_ref = profile
            .credential_ref
            .as_deref()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                ServiceError::invalid_operation("embedded profile has no credential handle")
            })?;
        let config = ctx
            .agent_config
            .get("config")
            .cloned()
            .ok_or_else(|| {
                ServiceError::invalid_operation("embedded snapshot has no profile config")
            })
            .and_then(|value| {
                serde_json::from_value::<NativeTaskProfileConfig>(value).map_err(|_| {
                    ServiceError::invalid_operation("embedded profile config is invalid")
                })
            })?;
        let provider = ctx
            .agent_config
            .get("provider")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .map(str::to_owned)
            .ok_or_else(|| ServiceError::invalid_operation("embedded profile has no provider"))?;
        let model = ctx
            .agent_config
            .get("model")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .map(str::to_owned)
            .ok_or_else(|| ServiceError::invalid_operation("embedded profile has no model"))?;
        let system_prompt = ctx
            .agent_config
            .get("prompt_template")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);

        let role = canonical_task_role(task_role)?;
        let session = self
            .embedded_agents
            .create_or_resume_session(CreateScopedSession {
                actor_user_id: owner_user_id.clone(),
                identity_id: agent.id.clone(),
                profile_id: Some(profile.id.clone()),
                scope: RequestedCanonicalScope::Task {
                    task_id: ctx.task_id.clone(),
                    role: role.to_owned(),
                },
            })
            .await?;
        let runtime_session_id = session
            .runtime_session_id
            .clone()
            .ok_or_else(|| ServiceError::invalid_operation("Task session has no runtime id"))?;
        let provider_account_id = self
            .embedded_agents
            .credential_provider_account_id(credential_ref)
            .await?;

        self.active.write().await.insert(
            ctx.execution_id.clone(),
            ActiveTaskTurn {
                cancellation: cancellation.clone(),
                runtime_session_id: runtime_session_id.clone(),
            },
        );

        let output = self
            .backend
            .run_turn(
                AgentTurnRequest {
                    forge_session_id: session.id.clone(),
                    runtime_session_id: runtime_session_id.clone(),
                    scope: CanonicalScope {
                        scope_type: CanonicalScopeType::Task,
                        scope_id: ctx.task_id.clone(),
                        workspace_access: if is_read_only_task_role(role)
                            || executors::is_worktree_read_only(&ctx.agent_config)
                        {
                            WorkspaceAccess::TaskRead
                        } else {
                            WorkspaceAccess::TaskWrite
                        },
                    },
                    workspace_path: Some(ctx.worktree_path.clone()),
                    provider: {
                        let (context_tokens, max_input_tokens, max_output_tokens) =
                            crate::embedded_agent_service::effective_native_limits(
                                &provider,
                                config.context_tokens,
                                config.max_input_tokens,
                                config.max_output_tokens,
                            );
                        NativeProviderConfig {
                            provider,
                            base_url: config.base_url,
                            model: model.clone(),
                            credential_handle_id: credential_ref.to_owned(),
                            owner_user_id: owner_user_id.clone(),
                            provider_account_id,
                            context_tokens,
                            max_input_tokens,
                            max_output_tokens,
                        }
                    },
                    system_prompt,
                    history: Vec::new(),
                    input: ctx.description.clone(),
                    cancellation: cancellation.clone(),
                },
                log_sink.clone(),
            )
            .await;
        self.active.write().await.remove(&ctx.execution_id);

        let output = match output {
            Ok(output) => output,
            Err(_error) if cancellation.is_cancelled() => {
                return Ok(ExecutionResult {
                    status: ExecutionOutcome::Cancelled,
                    ..ExecutionResult::default()
                });
            }
            Err(error) => return Err(ServiceError::invalid_operation(error.to_string())),
        };
        if cancellation.is_cancelled() {
            return Ok(ExecutionResult {
                status: ExecutionOutcome::Cancelled,
                agent_session_id: Some(output.runtime_session_id),
                ..ExecutionResult::default()
            });
        }

        if let Some(manifest) = output.context_manifest.as_ref() {
            self.persist_runtime_context_manifest(ctx, &agent, &session, manifest)
                .await?;
        }

        if !output.text.trim().is_empty() {
            log_sink
                .write(
                    LogKind::Assistant,
                    serde_json::json!({"text": output.text.clone()}),
                )
                .await
                .map_err(|error| {
                    ServiceError::invalid_operation(format!("Task log write failed: {error}"))
                })?;
        }
        let after_sha = git::get_current_sha(Path::new(&ctx.worktree_path))
            .await
            .map_err(ServiceError::from)?;
        Ok(ExecutionResult {
            status: ExecutionOutcome::Completed,
            after_sha: Some(after_sha),
            agent_session_id: Some(output.runtime_session_id),
            summary: Some(output.text),
            usage: Some(TokenUsage {
                input_tokens: i64::try_from(output.input_tokens).unwrap_or(i64::MAX),
                output_tokens: i64::try_from(output.output_tokens).unwrap_or(i64::MAX),
                model: Some(model),
                ..TokenUsage::default()
            }),
            ..ExecutionResult::default()
        })
    }

    /// Links the final Agent Runtime manifest to Forge's immutable Task
    /// context-admission record.  The runtime remains the only component
    /// deciding order, token budgets, serialization, and LCM compaction; this
    /// method only records final segment/coverage dispositions and hashes.
    async fn persist_runtime_context_manifest(
        &self,
        ctx: &ExecutionContext,
        agent: &db::Agent,
        session: &db::AgentSession,
        runtime_manifest: &RuntimeContextManifestLink,
    ) -> Result<()> {
        let source_revision = runtime_manifest.context_fingerprint.clone();
        let covered = runtime_manifest
            .summaries
            .iter()
            .flat_map(|summary| summary.covered.iter().cloned())
            .collect::<std::collections::BTreeSet<_>>();
        let summary_ids = runtime_manifest
            .summaries
            .iter()
            .map(|summary| summary.summary.clone())
            .collect::<std::collections::BTreeSet<_>>();
        let segment_ids = runtime_manifest
            .segments
            .iter()
            .map(|segment| segment.id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        let mut sources = Vec::new();
        let mut source_ids = std::collections::BTreeSet::new();
        let mut ordinal = 0_i64;
        if let Some(timeline_id) = runtime_manifest.lcm_timeline_id.as_deref() {
            source_ids.insert(timeline_id.to_owned());
            sources.push(ContextSourceInput {
                ordinal,
                source_id: timeline_id.to_owned(),
                source_type: "runtime_lcm_timeline".to_owned(),
                source_revision: runtime_manifest
                    .lcm_binding_revision
                    .clone()
                    .unwrap_or_else(|| "unknown".to_owned()),
                selection_reason: "agent_runtime_lcm_binding".to_owned(),
                disposition: "included".to_owned(),
                retention_priority: 100,
                fragment_fingerprint: fingerprint_id(timeline_id),
                sensitivity: "internal".to_owned(),
            });
            ordinal = ordinal.saturating_add(1);
        }
        for segment in &runtime_manifest.segments {
            if !source_ids.insert(segment.id.clone()) {
                continue;
            }
            sources.push(ContextSourceInput {
                ordinal,
                source_id: segment.id.clone(),
                source_type: "runtime_segment".to_owned(),
                source_revision: source_revision.clone(),
                selection_reason: "agent_runtime_final_segment".to_owned(),
                disposition: if covered.contains(&segment.id) && !summary_ids.contains(&segment.id)
                {
                    "summarized"
                } else {
                    "included"
                }
                .to_owned(),
                retention_priority: if summary_ids.contains(&segment.id) {
                    100
                } else {
                    10
                },
                fragment_fingerprint: segment.content_hash.clone(),
                sensitivity: segment.sensitivity.clone(),
            });
            ordinal = ordinal.saturating_add(1);
        }
        for summary in &runtime_manifest.summaries {
            if source_ids.insert(summary.summary.clone()) && !segment_ids.contains(&summary.summary)
            {
                sources.push(ContextSourceInput {
                    ordinal,
                    source_id: summary.summary.clone(),
                    source_type: "runtime_lcm_summary".to_owned(),
                    source_revision: source_revision.clone(),
                    selection_reason: "agent_runtime_summary_coverage".to_owned(),
                    disposition: "included".to_owned(),
                    retention_priority: 100,
                    fragment_fingerprint: fingerprint_id(&summary.summary),
                    sensitivity: "sensitive".to_owned(),
                });
                ordinal = ordinal.saturating_add(1);
            }
            for covered_id in &summary.covered {
                if source_ids.insert(covered_id.clone()) {
                    sources.push(ContextSourceInput {
                        ordinal,
                        source_id: covered_id.clone(),
                        source_type: "runtime_lcm_covered".to_owned(),
                        source_revision: source_revision.clone(),
                        selection_reason: "agent_runtime_summary_coverage".to_owned(),
                        disposition: "summarized".to_owned(),
                        retention_priority: 10,
                        fragment_fingerprint: fingerprint_id(covered_id),
                        sensitivity: "sensitive".to_owned(),
                    });
                    ordinal = ordinal.saturating_add(1);
                }
            }
        }
        let manifest_id = runtime_manifest_id(&agent.id, &session.id, runtime_manifest);
        let identity_id = uuid::Uuid::parse_str(&agent.id)
            .map_err(|_| ServiceError::invalid_operation("embedded identity id is invalid"))?;
        let context_scope_id = uuid::Uuid::parse_str(&session.context_scope_id)
            .map_err(|_| ServiceError::invalid_operation("Task context scope id is invalid"))?;
        let request_fingerprint = runtime_request_fingerprint(ctx, runtime_manifest);
        let service = ContextManifestService::new(Arc::clone(&self.db));
        if let Some(existing) = service
            .get_authorized(manifest_id, identity_id, context_scope_id)
            .await?
        {
            if existing.runtime_manifest_fingerprint.as_deref()
                != Some(runtime_manifest.runtime_manifest_fingerprint.as_str())
                || existing.request_fingerprint != request_fingerprint
            {
                return Err(ServiceError::invalid_operation(
                    "Task runtime context manifest idempotency conflict",
                ));
            }
            let existing_sources = service
                .sources(manifest_id, identity_id, context_scope_id)
                .await?;
            for source in &sources {
                if existing_sources.iter().any(|stored| {
                    stored.ordinal == source.ordinal
                        && stored.source_id == source.source_id
                        && stored.source_revision == source.source_revision
                }) {
                    continue;
                }
                service
                    .append_source(manifest_id, identity_id, context_scope_id, source.clone())
                    .await?;
            }
            return Ok(());
        }
        let created = service
            .create(
                ContextManifestInput {
                    id: manifest_id,
                    identity_id,
                    agent_session_id: Some(uuid::Uuid::parse_str(&session.id).map_err(|_| {
                        ServiceError::invalid_operation("Task agent session id is invalid")
                    })?),
                    context_scope_id,
                    scope_type: "task".to_owned(),
                    scope_id: ctx.task_id.clone(),
                    policy_revision: "forge-task-context-policy-1".to_owned(),
                    domain_revision: "forge-task-runtime-link-1".to_owned(),
                    lcm_binding_revision: runtime_manifest.lcm_binding_revision.clone(),
                    runtime_manifest_id: Some(runtime_manifest.turn_id.clone()),
                    runtime_manifest_fingerprint: Some(
                        runtime_manifest.runtime_manifest_fingerprint.clone(),
                    ),
                    request_fingerprint,
                },
                &sources,
            )
            .await?;
        for source in sources {
            service
                .append_source(
                    manifest_uuid(&created)?,
                    identity_id,
                    context_scope_id,
                    source,
                )
                .await?;
        }
        Ok(())
    }
}

fn manifest_uuid(manifest: &db::ContextManifest) -> Result<uuid::Uuid> {
    uuid::Uuid::parse_str(&manifest.id)
        .map_err(|_| ServiceError::invalid_operation("persisted context manifest id is invalid"))
}

fn runtime_manifest_id(
    identity_id: &str,
    session_id: &str,
    runtime_manifest: &RuntimeContextManifestLink,
) -> uuid::Uuid {
    let mut digest = Sha256::new();
    digest.update(b"forge-task-context-manifest-v1\0");
    digest.update(identity_id.as_bytes());
    digest.update([0]);
    digest.update(session_id.as_bytes());
    digest.update([0]);
    digest.update(runtime_manifest.turn_id.as_bytes());
    let bytes = digest.finalize();
    let mut id = [0_u8; 16];
    id.copy_from_slice(&bytes[..16]);
    // RFC 4122 version/variant bits make this deterministic digest a valid
    // UUID without adding a new dependency feature.
    id[6] = (id[6] & 0x0f) | 0x50;
    id[8] = (id[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(id)
}

fn fingerprint_id(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))
}

fn runtime_request_fingerprint(
    ctx: &ExecutionContext,
    runtime_manifest: &RuntimeContextManifestLink,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"forge-task-runtime-request-v1\0");
    hasher.update(ctx.task_id.as_bytes());
    hasher.update([0]);
    hasher.update(ctx.execution_id.as_bytes());
    hasher.update([0]);
    hasher.update(runtime_manifest.turn_id.as_bytes());
    hasher.update([0]);
    hasher.update(runtime_manifest.context_fingerprint.as_bytes());
    hasher.update([0]);
    hasher.update(runtime_manifest.cache_plan_fingerprint.as_bytes());
    hasher.update([0]);
    hasher.update(runtime_manifest.runtime_manifest_fingerprint.as_bytes());
    hex::encode(hasher.finalize())
}

#[async_trait]
impl TaskExecutor for EmbeddedTaskExecutor {
    async fn execute(
        &self,
        ctx: ExecutionContext,
    ) -> std::result::Result<ExecutionResult, ExecutorError> {
        let executor_type = ctx
            .agent_config
            .get("executor_type")
            .and_then(serde_json::Value::as_str)
            .or_else(|| {
                ctx.agent_config
                    .get("config")
                    .and_then(|config| config.get("executor_type"))
                    .and_then(serde_json::Value::as_str)
            });
        if executor_type != Some(EMBEDDED_EXECUTOR_TYPE) {
            return Err(ExecutorError::Other(
                "embedded Task executor received a non-embedded snapshot".to_owned(),
            ));
        }
        let task_role = ctx
            .agent_config
            .get(TASK_ROLE_MARKER)
            .and_then(serde_json::Value::as_str)
            .unwrap_or("worker");
        let agent_id = ctx
            .agent_config
            .get("agent_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| ExecutorError::Other("embedded snapshot has no agent_id".to_owned()))?;
        let agent = AgentRepo::get_by_id(&*self.db, agent_id)
            .await
            .map_err(|error| ExecutorError::Other(error.to_string()))?
            .ok_or_else(|| ExecutorError::Other("embedded identity no longer exists".to_owned()))?;
        let snapshot_profile_id = ctx
            .agent_config
            .get("profile_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                ExecutorError::Other("embedded snapshot has no profile_id".to_owned())
            })?;
        let _snapshot_profile = AgentProfileRepo::get_profile(&*self.db, snapshot_profile_id)
            .await
            .map_err(|error| ExecutorError::Other(error.to_string()))?
            .filter(|profile| {
                profile.identity_id == agent.id
                    && profile.backend_kind == "native"
                    && profile.executor_type == EMBEDDED_EXECUTOR_TYPE
            })
            .ok_or_else(|| {
                ExecutorError::Other(
                    "embedded Task snapshot does not reference an available native profile"
                        .to_owned(),
                )
            })?;
        let execution = ExecutionRepo::get_by_id(&*self.db, &ctx.execution_id)
            .await
            .map_err(|error| ExecutorError::Other(error.to_string()))?
            .ok_or_else(|| {
                ExecutorError::Other("embedded execution no longer exists".to_owned())
            })?;
        let execution_role = canonical_task_role(&execution.role)
            .map_err(|error| ExecutorError::Other(error.to_string()))?;
        let requested_role = canonical_task_role(task_role)
            .map_err(|error| ExecutorError::Other(error.to_string()))?;
        if is_read_only_task_role(requested_role)
            && !executors::is_worktree_read_only(&ctx.agent_config)
        {
            return Err(ExecutorError::Other(format!(
                "embedded {requested_role} execution must use the existing read-only worktree path"
            )));
        }
        if execution.status != ExecutionStatus::Running
            || execution.task_id != ctx.task_id
            || execution.agent_id.as_deref() != Some(agent.id.as_str())
            || execution_role != requested_role
        {
            return Err(ExecutorError::Other(
                "embedded Task execution is not the claimed role execution".to_owned(),
            ));
        }

        let cancellation = CancellationToken::new();
        let log_sink = Arc::new(NativeTaskLogSink::new(
            &ctx.logs_path,
            &ctx.execution_id,
            ctx.log_sender.clone(),
        ));
        let result = self
            .run_native_turn(
                &ctx,
                agent,
                task_role,
                Arc::clone(&log_sink),
                cancellation.clone(),
            )
            .await
            .map_err(|error| ExecutorError::Other(error.to_string()));
        result
    }

    async fn cancel(&self, execution_id: &str) -> std::result::Result<(), ExecutorError> {
        let active = self.active.read().await.get(execution_id).cloned();
        let Some(active) = active else {
            return Ok(());
        };
        active.cancellation.cancel();
        self.backend
            .cancel(&active.runtime_session_id)
            .await
            .map_err(|error| ExecutorError::Other(error.to_string()))
    }
}

/// Log sink preserving the standard Forge JSONL/event stream contract.
struct NativeTaskLogSink {
    writer: Mutex<LogWriter>,
}

impl std::fmt::Debug for NativeTaskLogSink {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativeTaskLogSink")
            .finish_non_exhaustive()
    }
}

impl NativeTaskLogSink {
    fn new(
        path: &str,
        execution_id: &str,
        sender: Option<tokio::sync::mpsc::UnboundedSender<executors::LogEntry>>,
    ) -> Self {
        let mut writer = LogWriter::new(path, execution_id.to_owned(), 10 * 1024 * 1024);
        if let Some(sender) = sender {
            writer.set_log_sender(sender);
        }
        Self {
            writer: Mutex::new(writer),
        }
    }

    async fn write(&self, kind: LogKind, payload: serde_json::Value) -> std::io::Result<()> {
        self.writer
            .lock()
            .await
            .write(kind, LogStream::Main, payload)
            .await
    }
}

#[async_trait]
impl TurnEventSink for NativeTaskLogSink {
    async fn text_delta(&self, text: &str) {
        let _ = self
            .write(LogKind::AssistantDelta, serde_json::json!({"text": text}))
            .await;
    }
}

#[derive(Debug, serde::Deserialize)]
struct NativeTaskProfileConfig {
    base_url: String,
    #[serde(default = "default_context_tokens")]
    context_tokens: u32,
    #[serde(default = "default_max_input_tokens")]
    max_input_tokens: u32,
    #[serde(default = "default_max_output_tokens")]
    max_output_tokens: u32,
}

fn default_context_tokens() -> u32 {
    128_000
}

fn default_max_input_tokens() -> u32 {
    96_000
}

fn default_max_output_tokens() -> u32 {
    16_000
}

fn canonical_task_role(role: &str) -> Result<&'static str> {
    match role {
        "worker" | "coder" => Ok("worker"),
        "reviewer" => Ok("reviewer"),
        // Planner keeps its own canonical identity end-to-end: the Task role
        // assignment lookup and the workflow-state admission both match on
        // the literal `planner` role, and the native session receives the
        // read-only Task workspace surface.
        "planner" => Ok("planner"),
        other => Err(ServiceError::invalid_operation(format!(
            "embedded Task execution is not admitted for role `{other}`"
        ))),
    }
}

/// Roles whose native Task sessions never receive worktree write access.
fn is_read_only_task_role(role: &str) -> bool {
    matches!(role, "reviewer" | "planner")
}

/// Marker used by the Task runner when it invokes the executor.  Keeping this
/// helper in services avoids exposing an executor-specific authority field to
/// API callers or persisted profile JSON.
pub(crate) fn set_task_role_marker(config: &mut serde_json::Value, role: &str) {
    if let Some(object) = config.as_object_mut() {
        object.insert(
            TASK_ROLE_MARKER.to_owned(),
            serde_json::Value::String(role.to_owned()),
        );
    }
}

/// Routes embedded snapshots to the Forge-native Task adapter while keeping
/// every existing CLI/fallback executor on its original path.
#[derive(Clone)]
pub struct TaskExecutorRouter {
    cli: Arc<dyn TaskExecutor>,
    embedded: Arc<EmbeddedTaskExecutor>,
}

impl std::fmt::Debug for TaskExecutorRouter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TaskExecutorRouter")
            .field("cli", &"configured")
            .field("embedded", &self.embedded)
            .finish()
    }
}

impl TaskExecutorRouter {
    pub fn new(cli: Arc<dyn TaskExecutor>, embedded: Arc<EmbeddedTaskExecutor>) -> Self {
        Self { cli, embedded }
    }
}

#[async_trait]
impl TaskExecutor for TaskExecutorRouter {
    async fn execute(
        &self,
        ctx: ExecutionContext,
    ) -> std::result::Result<ExecutionResult, ExecutorError> {
        if ctx
            .agent_config
            .get("executor_type")
            .and_then(serde_json::Value::as_str)
            == Some(EMBEDDED_EXECUTOR_TYPE)
        {
            self.embedded.execute(ctx).await
        } else {
            self.cli.execute(ctx).await
        }
    }

    async fn cancel(&self, execution_id: &str) -> std::result::Result<(), ExecutorError> {
        // Both implementations are intentionally called: a cancellation can
        // race a route switch, and each backend owns only its own active map.
        let embedded_result = self.embedded.cancel(execution_id).await;
        let cli_result = self.cli.cancel(execution_id).await;
        embedded_result.and(cli_result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_known_task_roles_receive_task_authority() {
        assert_eq!(canonical_task_role("worker").unwrap(), "worker");
        assert_eq!(canonical_task_role("coder").unwrap(), "worker");
        assert_eq!(canonical_task_role("reviewer").unwrap(), "reviewer");
        assert_eq!(canonical_task_role("planner").unwrap(), "planner");
        assert!(canonical_task_role("merge_fixer").is_err());
        assert!(canonical_task_role("").is_err());
    }

    #[test]
    fn planner_and_reviewer_are_read_only_task_roles() {
        assert!(is_read_only_task_role("planner"));
        assert!(is_read_only_task_role("reviewer"));
        assert!(!is_read_only_task_role("worker"));
        assert!(!is_read_only_task_role("coder"));
    }

    #[test]
    fn dispatch_marker_overrides_profile_supplied_task_role() {
        let mut config = serde_json::json!({
            "executor_type": "embedded",
            TASK_ROLE_MARKER: "reviewer",
        });
        set_task_role_marker(&mut config, "coder");
        assert_eq!(config[TASK_ROLE_MARKER], "coder");
    }
}
