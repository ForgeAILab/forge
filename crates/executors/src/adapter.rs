use crate::{
    config::resolve_config_value, ExecutionContext, ExecutionResult, ExecutorError,
    ProviderCallAdmission, TaskExecutor, UsageReport,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Registry of known CLI executor families.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutorKind {
    /// Forge-hosted Agent Runtime profile.  This is intentionally not a CLI
    /// adapter; services route it to the Forge-owned native task backend.
    Embedded,
    Shell,
    Codex,
    ClaudeCode,
    Cursor,
    Opencode,
    Gemini,
    Smith,
    Null,
}

impl std::fmt::Display for ExecutorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Embedded => write!(f, "embedded"),
            Self::Shell => write!(f, "shell"),
            Self::Codex => write!(f, "codex"),
            Self::ClaudeCode => write!(f, "claude_code"),
            Self::Cursor => write!(f, "cursor"),
            Self::Opencode => write!(f, "opencode"),
            Self::Gemini => write!(f, "gemini"),
            Self::Smith => write!(f, "smith"),
            Self::Null => write!(f, "null"),
        }
    }
}

impl std::str::FromStr for ExecutorKind {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "embedded" => Ok(Self::Embedded),
            "shell" => Ok(Self::Shell),
            "codex" => Ok(Self::Codex),
            "claude_code" => Ok(Self::ClaudeCode),
            "cursor" => Ok(Self::Cursor),
            "opencode" => Ok(Self::Opencode),
            "gemini" => Ok(Self::Gemini),
            "smith" => Ok(Self::Smith),
            "null" => Ok(Self::Null),
            other => Err(format!("unknown executor kind: {other}")),
        }
    }
}

/// Availability state reported by an adapter.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AvailabilityStatus {
    Authenticated,
    Installed,
    NotFound,
}

/// Availability info returned by an adapter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AvailabilityInfo {
    pub status: AvailabilityStatus,
    pub authenticated_at: Option<String>,
    pub config_path: Option<String>,
}

/// Context for adapter discovery.
#[derive(Debug, Clone)]
pub struct DiscoverContext {
    pub project_path: Option<String>,
}

/// Options discovered by an adapter.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiscoveredOptions {
    pub models: Vec<String>,
    pub permission_policies: Vec<String>,
    pub cli_specific: serde_json::Value,
}

/// Per-execution overrides applied on top of profile config.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ExecutionOverrides {
    pub model_id: Option<String>,
    pub reasoning_effort: Option<String>,
    pub permission_policy: Option<String>,
}

/// Typed adapter trait for CLI-specific executor implementations.
#[async_trait]
pub trait CodingExecutorAdapter: Send + Sync {
    fn kind(&self) -> ExecutorKind;

    fn check_availability(&self) -> AvailabilityInfo;

    /// Availability of one specific candidate config. Defaults to the
    /// executor-family-level check; adapters whose config selects an account
    /// (Smith profiles, Codex profiles) may override with a per-account
    /// check. Account state that no precheck can see is still discovered
    /// through typed runtime errors during execution.
    fn check_candidate_availability(&self, _config: &serde_json::Value) -> AvailabilityInfo {
        self.check_availability()
    }

    async fn discover_options(
        &self,
        ctx: DiscoverContext,
    ) -> Result<DiscoveredOptions, ExecutorError>;

    async fn execute(&self, ctx: ExecutionContext) -> Result<ExecutionResult, ExecutorError>;

    async fn cancel(&self, execution_id: &str) -> Result<(), ExecutorError>;
}

/// Registry mapping ExecutorKind to adapter implementations.
pub struct AdapterRegistry {
    adapters: HashMap<ExecutorKind, Box<dyn CodingExecutorAdapter>>,
}

impl AdapterRegistry {
    pub fn new() -> Self {
        Self {
            adapters: HashMap::new(),
        }
    }

    pub fn register(&mut self, adapter: Box<dyn CodingExecutorAdapter>) {
        let kind = adapter.kind();
        self.adapters.insert(kind, adapter);
    }

    pub fn get(&self, kind: &ExecutorKind) -> Option<&dyn CodingExecutorAdapter> {
        self.adapters.get(kind).map(|a| a.as_ref())
    }

    pub fn kinds(&self) -> Vec<ExecutorKind> {
        self.adapters.keys().cloned().collect()
    }
}

impl Default for AdapterRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Supervisor-facing executor that dispatches to a typed CLI adapter.
pub struct AdapterExecutor {
    registry: Arc<AdapterRegistry>,
    provider_admissions: std::sync::Mutex<HashMap<String, Arc<dyn ProviderCallAdmission>>>,
}

impl AdapterExecutor {
    pub fn new(registry: Arc<AdapterRegistry>) -> Self {
        Self {
            registry,
            provider_admissions: std::sync::Mutex::new(HashMap::new()),
        }
    }
}

/// Resolve only identity fields that the candidate actually configured. The
/// executor family is intentionally not a provider fallback: `codex`,
/// `embedded`, and `smith` are runtimes, not billable provider identities.
fn configured_provider_model(config: &serde_json::Value) -> (Option<&str>, Option<&str>) {
    let provider = config
        .get("provider")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty());
    let model = config
        .get("model")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty());
    (provider, model)
}

/// Add deterministic route identity and configured identity to adapter
/// reports. An adapter that made a provider call but has no telemetry receives
/// one unmetered disposition; no zero counters are fabricated.
fn normalize_candidate_reports(
    execution_id: &str,
    candidate: &RouteCandidate,
    attempt_ordinal: u32,
    reports: Vec<UsageReport>,
    outcome: crate::config::RouteAttemptOutcome,
    add_unmetered_when_empty: bool,
) -> Vec<UsageReport> {
    let (configured_provider, configured_model) = configured_provider_model(&candidate.config);
    let mut reports = reports;
    if reports.is_empty() && add_unmetered_when_empty {
        reports.push(UsageReport::unmetered(String::new()));
    }
    reports
        .into_iter()
        .enumerate()
        .map(|(sequence, mut report)| {
            report.fill_identity(configured_provider, configured_model);
            report.outcome = Some(outcome);
            report.for_candidate(
                execution_id,
                &candidate.candidate_key,
                attempt_ordinal,
                sequence as u32,
            )
        })
        .collect()
}

#[async_trait]
impl TaskExecutor for AdapterExecutor {
    async fn execute_with_provider_call_admission(
        &self,
        ctx: ExecutionContext,
        admission: Arc<dyn ProviderCallAdmission>,
    ) -> Result<ExecutionResult, ExecutorError> {
        self.provider_admissions
            .lock()
            .expect("provider admission lock poisoned")
            .insert(ctx.execution_id.clone(), admission);
        let result = self.execute(ctx.clone()).await;
        self.provider_admissions
            .lock()
            .expect("provider admission lock poisoned")
            .remove(&ctx.execution_id);
        result
    }

    async fn execute(&self, mut ctx: ExecutionContext) -> Result<ExecutionResult, ExecutorError> {
        let (kind, config) = resolve_context_config(&ctx.agent_config)?;
        let adapter = self.registry.get(&kind).ok_or_else(|| {
            ExecutorError::Other(format!("No adapter registered for executor type: {kind}"))
        })?;

        ctx.agent_config = config;
        let candidate = RouteCandidate {
            candidate_key: crate::config::candidate_key(&kind, &ctx.agent_config),
            account_key: crate::config::account_key(&kind, &ctx.agent_config),
            kind,
            config: ctx.agent_config.clone(),
        };
        let admission = self
            .provider_admissions
            .lock()
            .expect("provider admission lock poisoned")
            .get(&ctx.execution_id)
            .cloned();
        if let Some(admission) = admission {
            admission
                .before_provider_call(&ctx, &candidate.candidate_key, 0)
                .await?;
        }
        match adapter.execute(ctx.clone()).await {
            Ok(mut result) => {
                let outcome = if result.status == crate::ExecutionOutcome::Cancelled {
                    crate::config::RouteAttemptOutcome::Cancelled
                } else if result.status == crate::ExecutionOutcome::Completed {
                    crate::config::RouteAttemptOutcome::Completed
                } else {
                    crate::config::RouteAttemptOutcome::Failed
                };
                result.usage_reports = normalize_candidate_reports(
                    &ctx.execution_id,
                    &candidate,
                    0,
                    result.usage_reports,
                    outcome,
                    true,
                );
                Ok(result)
            }
            Err(ExecutorError::UsageExhausted {
                retry_after,
                usage_reports,
            }) => Err(ExecutorError::UsageExhausted {
                retry_after,
                usage_reports: normalize_candidate_reports(
                    &ctx.execution_id,
                    &candidate,
                    0,
                    usage_reports,
                    crate::config::RouteAttemptOutcome::UsageExhausted,
                    true,
                ),
            }),
            Err(ExecutorError::Unavailable {
                reason,
                usage_reports,
            }) => Err(ExecutorError::Unavailable {
                reason,
                usage_reports: normalize_candidate_reports(
                    &ctx.execution_id,
                    &candidate,
                    0,
                    usage_reports,
                    crate::config::RouteAttemptOutcome::Unavailable,
                    false,
                ),
            }),
            Err(error) => Err(error),
        }
    }

    async fn cancel(&self, execution_id: &str) -> Result<(), ExecutorError> {
        for kind in self.registry.kinds() {
            if let Some(adapter) = self.registry.get(&kind) {
                adapter.cancel(execution_id).await?;
            }
        }
        Ok(())
    }
}

/// Cooldown applied to an exhausted account when the provider gives no
/// retry-after hint.
pub const DEFAULT_ACCOUNT_COOLDOWN: std::time::Duration = std::time::Duration::from_secs(15 * 60);

/// Supervisor-facing executor that walks an ordered candidate route,
/// advancing only on availability failures. Snapshots without a `routing`
/// block behave exactly like `AdapterExecutor`.
pub struct FallbackExecutor {
    registry: Arc<AdapterRegistry>,
    cooldowns: std::sync::Mutex<HashMap<String, std::time::Instant>>,
    cancellations: std::sync::Mutex<HashMap<String, Arc<std::sync::atomic::AtomicBool>>>,
    provider_admissions: std::sync::Mutex<HashMap<String, Arc<dyn ProviderCallAdmission>>>,
}

struct RouteCandidate {
    kind: ExecutorKind,
    config: serde_json::Value,
    candidate_key: String,
    account_key: String,
}

impl FallbackExecutor {
    pub fn new(registry: Arc<AdapterRegistry>) -> Self {
        Self {
            registry,
            cooldowns: std::sync::Mutex::new(HashMap::new()),
            cancellations: std::sync::Mutex::new(HashMap::new()),
            provider_admissions: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// The preferred candidate is the snapshot's top-level pair (the launch
    /// path points it at the sticky winner); remaining route candidates
    /// follow in configured order.
    fn route(agent_config: &serde_json::Value) -> Result<Vec<RouteCandidate>, ExecutorError> {
        let (preferred_kind, preferred_config) = resolve_context_config(agent_config)?;
        let preferred = RouteCandidate {
            candidate_key: crate::config::candidate_key(&preferred_kind, &preferred_config),
            account_key: crate::config::account_key(&preferred_kind, &preferred_config),
            kind: preferred_kind,
            config: preferred_config,
        };

        let mut candidates = vec![preferred];
        let routing = agent_config.get(crate::config::ROUTING_SNAPSHOT_KEY);
        if let Some(routing) = routing {
            let routing: crate::config::ExecutorRouting = serde_json::from_value(routing.clone())
                .map_err(|error| {
                ExecutorError::Other(format!("invalid routing block in snapshot: {error}"))
            })?;
            if routing.policy != crate::config::ROUTING_POLICY_ORDERED_FALLBACK_V1 {
                return Err(ExecutorError::Other(format!(
                    "unknown routing policy: {}",
                    routing.policy
                )));
            }
            for candidate in routing.candidates {
                // Normalize (idempotent for snapshot-built routes) so keys are
                // consistent regardless of how the routing block was authored.
                let config = resolve_config_value(
                    candidate.executor_type.clone(),
                    &candidate.config,
                    &ExecutionOverrides::default(),
                )?;
                let key = crate::config::candidate_key(&candidate.executor_type, &config);
                if candidates.iter().any(|c| c.candidate_key == key) {
                    continue;
                }
                candidates.push(RouteCandidate {
                    account_key: crate::config::account_key(&candidate.executor_type, &config),
                    candidate_key: key,
                    kind: candidate.executor_type,
                    config,
                });
            }
        }
        Ok(candidates)
    }

    fn cooldown_remaining(&self, account_key: &str) -> Option<std::time::Duration> {
        let mut cooldowns = self.cooldowns.lock().expect("cooldown lock poisoned");
        let now = std::time::Instant::now();
        match cooldowns.get(account_key) {
            Some(expiry) if *expiry > now => Some(*expiry - now),
            Some(_) => {
                cooldowns.remove(account_key);
                None
            }
            None => None,
        }
    }

    fn note_exhausted(&self, account_key: &str, retry_after: Option<std::time::Duration>) {
        let cooldown = retry_after.unwrap_or(DEFAULT_ACCOUNT_COOLDOWN);
        self.cooldowns
            .lock()
            .expect("cooldown lock poisoned")
            .insert(account_key.to_owned(), std::time::Instant::now() + cooldown);
    }

    fn cancellation_flag(&self, execution_id: &str) -> Arc<std::sync::atomic::AtomicBool> {
        self.cancellations
            .lock()
            .expect("cancellation lock poisoned")
            .entry(execution_id.to_owned())
            .or_default()
            .clone()
    }

    fn clear_cancellation(&self, execution_id: &str) {
        self.cancellations
            .lock()
            .expect("cancellation lock poisoned")
            .remove(execution_id);
    }

    async fn log_hop(
        writer: &mut crate::LogWriter,
        event: &str,
        candidate: &RouteCandidate,
        detail: serde_json::Value,
    ) {
        let mut payload = serde_json::json!({
            "source": "executor_fallback",
            "event": event,
            "candidate_key": candidate.candidate_key,
            "executor_type": candidate.kind.to_string(),
        });
        if let (Some(object), Some(extra)) = (payload.as_object_mut(), detail.as_object()) {
            for (key, value) in extra {
                object.insert(key.clone(), value.clone());
            }
        }
        // Hop logging is best-effort; a full log must not mask the run itself.
        let _ = writer
            .write(crate::LogKind::System, crate::LogStream::Main, payload)
            .await;
    }

    fn unavailable_result(
        attempts: Vec<crate::config::RouteAttempt>,
        usage_reports: Vec<UsageReport>,
        retry_after: Option<std::time::Duration>,
        summary: String,
    ) -> ExecutionResult {
        ExecutionResult {
            status: crate::ExecutionOutcome::Failed,
            error: Some(summary),
            usage_reports,
            failure_class: Some(crate::ExecutionFailureClass::ExecutorUnavailable),
            retry_after,
            route_attempts: attempts,
            ..Default::default()
        }
    }
}

#[async_trait]
impl TaskExecutor for FallbackExecutor {
    async fn execute_with_provider_call_admission(
        &self,
        ctx: ExecutionContext,
        admission: Arc<dyn ProviderCallAdmission>,
    ) -> Result<ExecutionResult, ExecutorError> {
        self.provider_admissions
            .lock()
            .expect("provider admission lock poisoned")
            .insert(ctx.execution_id.clone(), admission);
        let result = self.execute(ctx.clone()).await;
        self.provider_admissions
            .lock()
            .expect("provider admission lock poisoned")
            .remove(&ctx.execution_id);
        result
    }

    async fn execute(&self, ctx: ExecutionContext) -> Result<ExecutionResult, ExecutorError> {
        let candidates = Self::route(&ctx.agent_config)?;
        let cancelled = self.cancellation_flag(&ctx.execution_id);
        let single_candidate = candidates.len() == 1;

        let mut writer = crate::LogWriter::new(
            std::path::Path::new(&ctx.logs_path),
            ctx.execution_id.clone(),
            crate::log_writer::DEFAULT_MAX_OUTPUT_BYTES,
        );
        if let Some(sender) = ctx.log_sender.clone() {
            writer.set_log_sender(sender);
        }

        let mut attempts: Vec<crate::config::RouteAttempt> = Vec::new();
        let mut usage_reports: Vec<UsageReport> = Vec::new();
        let mut earliest_retry: Option<std::time::Duration> = None;
        let mut skip_reasons: Vec<String> = Vec::new();

        let outcome = 'chain: {
            for (attempt_ordinal, candidate) in candidates.iter().enumerate() {
                if cancelled.load(std::sync::atomic::Ordering::SeqCst) {
                    break 'chain Some(ExecutionResult {
                        status: crate::ExecutionOutcome::Cancelled,
                        usage_reports: std::mem::take(&mut usage_reports),
                        route_attempts: std::mem::take(&mut attempts),
                        ..Default::default()
                    });
                }

                if let Some(remaining) = self.cooldown_remaining(&candidate.account_key) {
                    attempts.push(crate::config::RouteAttempt {
                        candidate_key: candidate.candidate_key.clone(),
                        outcome: crate::config::RouteAttemptOutcome::SkippedCooldown,
                    });
                    earliest_retry = Some(earliest_retry.map_or(remaining, |e| e.min(remaining)));
                    skip_reasons.push(format!(
                        "{} (cooldown {}s)",
                        candidate.candidate_key,
                        remaining.as_secs()
                    ));
                    Self::log_hop(
                        &mut writer,
                        "candidate_skipped_cooldown",
                        candidate,
                        serde_json::json!({"cooldown_remaining_seconds": remaining.as_secs()}),
                    )
                    .await;
                    continue;
                }

                let Some(adapter) = self.registry.get(&candidate.kind) else {
                    break 'chain None; // fall through to the registry error below
                };

                if matches!(
                    adapter
                        .check_candidate_availability(&candidate.config)
                        .status,
                    AvailabilityStatus::NotFound
                ) {
                    attempts.push(crate::config::RouteAttempt {
                        candidate_key: candidate.candidate_key.clone(),
                        outcome: crate::config::RouteAttemptOutcome::Unavailable,
                    });
                    skip_reasons.push(format!("{} (not installed)", candidate.candidate_key));
                    Self::log_hop(
                        &mut writer,
                        "candidate_unavailable",
                        candidate,
                        serde_json::json!({}),
                    )
                    .await;
                    continue;
                }

                if !single_candidate {
                    Self::log_hop(
                        &mut writer,
                        "candidate_selected",
                        candidate,
                        serde_json::json!({}),
                    )
                    .await;
                }

                let mut candidate_ctx = ctx.clone();
                candidate_ctx.agent_config = candidate.config.clone();
                let admission = self
                    .provider_admissions
                    .lock()
                    .expect("provider admission lock poisoned")
                    .get(&ctx.execution_id)
                    .cloned();
                if let Some(admission) = admission {
                    admission
                        .before_provider_call(
                            &candidate_ctx,
                            &candidate.candidate_key,
                            attempt_ordinal as u32,
                        )
                        .await?;
                }
                let attempt = adapter.execute(candidate_ctx).await;

                match attempt {
                    Ok(mut result) => {
                        let was_cancelled = cancelled.load(std::sync::atomic::Ordering::SeqCst)
                            || result.status == crate::ExecutionOutcome::Cancelled;
                        let route_outcome = if was_cancelled {
                            crate::config::RouteAttemptOutcome::Cancelled
                        } else if result.status == crate::ExecutionOutcome::Completed {
                            crate::config::RouteAttemptOutcome::Completed
                        } else {
                            crate::config::RouteAttemptOutcome::Failed
                        };
                        attempts.push(crate::config::RouteAttempt {
                            candidate_key: candidate.candidate_key.clone(),
                            outcome: route_outcome,
                        });
                        if was_cancelled {
                            result.status = crate::ExecutionOutcome::Cancelled;
                        } else if result.status == crate::ExecutionOutcome::Failed {
                            result.failure_class = Some(crate::ExecutionFailureClass::TaskFailed);
                        }
                        let reports = normalize_candidate_reports(
                            &ctx.execution_id,
                            candidate,
                            attempt_ordinal as u32,
                            result.usage_reports,
                            route_outcome,
                            true,
                        );
                        usage_reports.extend(reports.iter().cloned());
                        result.usage_reports = std::mem::take(&mut usage_reports);
                        result.resolved_candidate = Some(crate::ResolvedExecutorCandidate {
                            candidate_key: candidate.candidate_key.clone(),
                            executor_type: candidate.kind.clone(),
                            config: candidate.config.clone(),
                        });
                        result.route_attempts = std::mem::take(&mut attempts);
                        break 'chain Some(result);
                    }
                    Err(error) if error.is_availability() => {
                        let (outcome, retry_after, reason) = match &error {
                            ExecutorError::UsageExhausted {
                                retry_after,
                                usage_reports: candidate_reports,
                            } => {
                                self.note_exhausted(&candidate.account_key, *retry_after);
                                let reports = normalize_candidate_reports(
                                    &ctx.execution_id,
                                    candidate,
                                    attempt_ordinal as u32,
                                    candidate_reports.clone(),
                                    crate::config::RouteAttemptOutcome::UsageExhausted,
                                    true,
                                );
                                usage_reports.extend(reports);
                                (
                                    crate::config::RouteAttemptOutcome::UsageExhausted,
                                    retry_after.unwrap_or(DEFAULT_ACCOUNT_COOLDOWN),
                                    "usage exhausted".to_owned(),
                                )
                            }
                            ExecutorError::Unavailable {
                                reason,
                                usage_reports: candidate_reports,
                            } => {
                                let reports = normalize_candidate_reports(
                                    &ctx.execution_id,
                                    candidate,
                                    attempt_ordinal as u32,
                                    candidate_reports.clone(),
                                    crate::config::RouteAttemptOutcome::Unavailable,
                                    false,
                                );
                                usage_reports.extend(reports);
                                (
                                    crate::config::RouteAttemptOutcome::Unavailable,
                                    DEFAULT_ACCOUNT_COOLDOWN,
                                    reason.clone(),
                                )
                            }
                            _ => unreachable!("is_availability covers both variants"),
                        };
                        attempts.push(crate::config::RouteAttempt {
                            candidate_key: candidate.candidate_key.clone(),
                            outcome,
                        });
                        earliest_retry =
                            Some(earliest_retry.map_or(retry_after, |e| e.min(retry_after)));
                        skip_reasons.push(format!("{} ({reason})", candidate.candidate_key));
                        Self::log_hop(
                            &mut writer,
                            "candidate_exhausted",
                            candidate,
                            serde_json::json!({
                                "reason": reason,
                                "retry_after_seconds": retry_after.as_secs(),
                            }),
                        )
                        .await;
                        continue;
                    }
                    Err(error) => return Err(error),
                }
            }
            None
        };

        self.clear_cancellation(&ctx.execution_id);

        if let Some(result) = outcome {
            return Ok(result);
        }

        // Registry gap is an infra error, not an availability disposition.
        if let Some(candidate) = candidates
            .iter()
            .find(|c| self.registry.get(&c.kind).is_none())
        {
            return Err(ExecutorError::Other(format!(
                "No adapter registered for executor type: {}",
                candidate.kind
            )));
        }

        let summary = format!(
            "no executor candidate available: {}",
            skip_reasons.join(", ")
        );
        Ok(Self::unavailable_result(
            attempts,
            usage_reports,
            earliest_retry,
            summary,
        ))
    }

    async fn cancel(&self, execution_id: &str) -> Result<(), ExecutorError> {
        if let Some(flag) = self
            .cancellations
            .lock()
            .expect("cancellation lock poisoned")
            .get(execution_id)
        {
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
        }
        for kind in self.registry.kinds() {
            if let Some(adapter) = self.registry.get(&kind) {
                adapter.cancel(execution_id).await?;
            }
        }
        Ok(())
    }
}

fn resolve_context_config(
    agent_config: &serde_json::Value,
) -> Result<(ExecutorKind, serde_json::Value), ExecutorError> {
    let object = agent_config.as_object().ok_or_else(|| {
        ExecutorError::Other("executor config snapshot must be a JSON object".to_owned())
    })?;
    let executor_type = object
        .get("executor_type")
        .and_then(|value| value.as_str())
        .ok_or_else(|| {
            ExecutorError::Other("executor config snapshot missing executor_type".to_owned())
        })?;
    let kind = executor_type.parse::<ExecutorKind>().map_err(|_| {
        ExecutorError::Other(format!(
            "No adapter registered for executor type: {executor_type}"
        ))
    })?;
    let config = object.get("config").unwrap_or(agent_config);
    let config = resolve_config_value(kind.clone(), config, &ExecutionOverrides::default())?;
    Ok((kind, config))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{build_shell_command_plan, ExecutionOutcome, ExecutionResult, ShellConfig};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    struct CapturingAdapter;

    #[async_trait]
    impl CodingExecutorAdapter for CapturingAdapter {
        fn kind(&self) -> ExecutorKind {
            ExecutorKind::Codex
        }

        fn check_availability(&self) -> AvailabilityInfo {
            AvailabilityInfo {
                status: AvailabilityStatus::Authenticated,
                authenticated_at: None,
                config_path: None,
            }
        }

        async fn discover_options(
            &self,
            _ctx: DiscoverContext,
        ) -> Result<DiscoveredOptions, ExecutorError> {
            Ok(DiscoveredOptions::default())
        }

        async fn execute(&self, ctx: ExecutionContext) -> Result<ExecutionResult, ExecutorError> {
            assert_eq!(ctx.agent_config["model"], "gpt-5-codex");
            assert_eq!(ctx.agent_config["model_reasoning_effort"], "high");
            assert_eq!(ctx.agent_config["permission_policy"], "auto");
            assert_eq!(ctx.agent_config["sandbox"], "danger-full-access");
            assert!(ctx.agent_config.get("effort").is_none());
            Ok(ExecutionResult {
                status: ExecutionOutcome::Completed,
                after_sha: None,
                agent_session_id: None,
                summary: None,
                error: None,
                usage_reports: Vec::new(),
                ..Default::default()
            })
        }

        async fn cancel(&self, _execution_id: &str) -> Result<(), ExecutorError> {
            Ok(())
        }
    }

    struct CancelTrackingAdapter {
        cancel_calls: Arc<AtomicUsize>,
    }

    struct RejectingAdmission {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl ProviderCallAdmission for RejectingAdmission {
        async fn before_provider_call(
            &self,
            _ctx: &ExecutionContext,
            _candidate_key: &str,
            _attempt_ordinal: u32,
        ) -> Result<(), ExecutorError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(ExecutorError::Other("durable start failed".to_owned()))
        }
    }

    #[async_trait]
    impl CodingExecutorAdapter for CancelTrackingAdapter {
        fn kind(&self) -> ExecutorKind {
            ExecutorKind::Shell
        }

        fn check_availability(&self) -> AvailabilityInfo {
            AvailabilityInfo {
                status: AvailabilityStatus::Authenticated,
                authenticated_at: None,
                config_path: None,
            }
        }

        async fn discover_options(
            &self,
            _ctx: DiscoverContext,
        ) -> Result<DiscoveredOptions, ExecutorError> {
            Ok(DiscoveredOptions::default())
        }

        async fn execute(&self, _ctx: ExecutionContext) -> Result<ExecutionResult, ExecutorError> {
            Ok(ExecutionResult {
                status: ExecutionOutcome::Completed,
                after_sha: None,
                agent_session_id: None,
                summary: None,
                error: None,
                usage_reports: Vec::new(),
                ..Default::default()
            })
        }

        async fn cancel(&self, execution_id: &str) -> Result<(), ExecutorError> {
            assert_eq!(execution_id, "execution-to-cancel");
            self.cancel_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn adapter_executor_dispatches_using_snapshot_config() {
        let mut registry = AdapterRegistry::new();
        registry.register(Box::new(CapturingAdapter));
        let executor = AdapterExecutor::new(Arc::new(registry));

        let result = executor
            .execute(ExecutionContext {
                task_id: "task".to_owned(),
                execution_id: "execution".to_owned(),
                worktree_path: ".".to_owned(),
                description: "do it".to_owned(),
                agent_config: serde_json::json!({
                    "executor_type": "codex",
                    "config": {
                        "model": "gpt-5-codex",
                        "model_reasoning_effort": "high",
                        "effort": "high",
                        "permission_policy": "auto",
                        "sandbox": "danger-full-access"
                    }
                }),
                logs_path: "logs.jsonl".to_owned(),
                heartbeat_interval_seconds: 1,
                max_turns: None,
                log_sender: None,
            })
            .await
            .expect("dispatch succeeds");

        assert_eq!(result.status, ExecutionOutcome::Completed);
    }

    #[test]
    fn resolve_context_config_builds_shell_command_plan() {
        let snapshot = serde_json::json!({
            "executor_type": "shell",
            "config": {
                "command": "bash",
                "args": ["-lc", "make test"],
                "permission_policy": "supervised",
                "additional_params": ["--verbose"],
                "env": { "CI": "1" }
            }
        });

        let (kind, config) = resolve_context_config(&snapshot).expect("snapshot resolves");
        assert_eq!(kind, ExecutorKind::Shell);

        let shell_config: ShellConfig = serde_json::from_value(config).expect("shell config");
        let plan =
            build_shell_command_plan("ignored", "/tmp/worktree", Some(2), Some(&shell_config));

        assert_eq!(plan.program, "bash");
        assert_eq!(plan.args, vec!["-lc", "make test", "--verbose"]);
        assert_eq!(plan.cwd.to_string_lossy(), "/tmp/worktree");
        assert_eq!(plan.env_set.get("CI").map(String::as_str), Some("1"));
        assert_eq!(
            plan.env_set.get("FORGE_MAX_TURNS").map(String::as_str),
            Some("2")
        );
    }

    #[test]
    fn resolve_context_config_resolves_codex_snapshot() {
        let snapshot = serde_json::json!({
            "executor_type": "codex",
            "config": {
                "model": "gpt-5-codex",
                "sandbox": "danger-full-access",
                "model_reasoning_effort": "high",
                "permission_policy": "auto",
                "additional_params": ["--verbose"],
                "env": { "CUSTOM": "1" }
            }
        });

        let (kind, config) = resolve_context_config(&snapshot).expect("snapshot resolves");
        assert_eq!(kind, ExecutorKind::Codex);

        let codex_config: crate::CodexConfig =
            serde_json::from_value(config).expect("codex config");
        assert_eq!(codex_config.model.as_deref(), Some("gpt-5-codex"));
        assert_eq!(
            codex_config.command_overrides.additional_params.as_deref(),
            Some(["--verbose".to_owned()].as_slice())
        );
    }

    #[derive(Clone)]
    enum ScriptedBehavior {
        Complete {
            usage_output_tokens: i64,
        },
        FailTask,
        Exhausted {
            retry_after_ms: u64,
            usage_output_tokens: i64,
        },
        AwaitCancel,
    }

    struct ScriptedAdapter {
        behaviors: std::collections::HashMap<String, ScriptedBehavior>,
        calls: Arc<std::sync::Mutex<Vec<String>>>,
        started: Arc<tokio::sync::Notify>,
        cancelled: Arc<std::sync::atomic::AtomicBool>,
    }

    impl ScriptedAdapter {
        fn new(behaviors: &[(&str, ScriptedBehavior)]) -> Self {
            Self {
                behaviors: behaviors
                    .iter()
                    .map(|(profile, behavior)| ((*profile).to_owned(), behavior.clone()))
                    .collect(),
                calls: Arc::default(),
                started: Arc::new(tokio::sync::Notify::new()),
                cancelled: Arc::default(),
            }
        }
    }

    #[async_trait]
    impl CodingExecutorAdapter for ScriptedAdapter {
        fn kind(&self) -> ExecutorKind {
            ExecutorKind::Smith
        }

        fn check_availability(&self) -> AvailabilityInfo {
            AvailabilityInfo {
                status: AvailabilityStatus::Authenticated,
                authenticated_at: None,
                config_path: None,
            }
        }

        async fn discover_options(
            &self,
            _ctx: DiscoverContext,
        ) -> Result<DiscoveredOptions, ExecutorError> {
            Ok(DiscoveredOptions::default())
        }

        async fn execute(&self, ctx: ExecutionContext) -> Result<ExecutionResult, ExecutorError> {
            let profile = ctx.agent_config["profile"]
                .as_str()
                .expect("scripted config has profile")
                .to_owned();
            self.calls.lock().unwrap().push(profile.clone());
            let usage = |output_tokens: i64| {
                UsageReport::metered(
                    String::new(),
                    crate::UsageCounters {
                        output_tokens: Some(u64::try_from(output_tokens).unwrap()),
                        ..Default::default()
                    },
                )
            };
            match self.behaviors.get(&profile).expect("scripted behavior") {
                ScriptedBehavior::Complete {
                    usage_output_tokens,
                } => Ok(ExecutionResult {
                    status: ExecutionOutcome::Completed,
                    usage_reports: vec![usage(*usage_output_tokens)],
                    ..Default::default()
                }),
                ScriptedBehavior::FailTask => Ok(ExecutionResult {
                    status: ExecutionOutcome::Failed,
                    error: Some("tests failed".to_owned()),
                    ..Default::default()
                }),
                ScriptedBehavior::Exhausted {
                    retry_after_ms,
                    usage_output_tokens,
                } => Err(ExecutorError::UsageExhausted {
                    retry_after: Some(std::time::Duration::from_millis(*retry_after_ms)),
                    usage_reports: vec![usage(*usage_output_tokens)],
                }),
                ScriptedBehavior::AwaitCancel => {
                    self.started.notify_one();
                    while !self.cancelled.load(Ordering::SeqCst) {
                        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                    }
                    Ok(ExecutionResult {
                        status: ExecutionOutcome::Cancelled,
                        ..Default::default()
                    })
                }
            }
        }

        async fn cancel(&self, _execution_id: &str) -> Result<(), ExecutorError> {
            self.cancelled.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    fn routed_ctx(execution_id: &str, logs_dir: &std::path::Path) -> ExecutionContext {
        ExecutionContext {
            task_id: "task".to_owned(),
            execution_id: execution_id.to_owned(),
            worktree_path: ".".to_owned(),
            description: "do it".to_owned(),
            agent_config: serde_json::json!({
                "executor_type": "smith",
                "config": {
                    "profile": "acct-1",
                    "provider": "zai",
                    "model": "glm-5"
                },
                "routing": {
                    "policy": "ordered_fallback_v1",
                    "candidates": [
                        {"executor_type": "smith", "config": {
                            "profile": "acct-1",
                            "provider": "zai",
                            "model": "glm-5"
                        }},
                        {"executor_type": "smith", "config": {
                            "profile": "acct-2",
                            "provider": "google",
                            "model": "gemini-3.6-flash"
                        }}
                    ]
                }
            }),
            logs_path: logs_dir
                .join(format!("{execution_id}.jsonl"))
                .to_string_lossy()
                .into_owned(),
            heartbeat_interval_seconds: 1,
            max_turns: None,
            log_sender: None,
        }
    }

    fn fallback_executor(
        behaviors: &[(&str, ScriptedBehavior)],
    ) -> (FallbackExecutor, Arc<std::sync::Mutex<Vec<String>>>) {
        let adapter = ScriptedAdapter::new(behaviors);
        let calls = Arc::clone(&adapter.calls);
        let mut registry = AdapterRegistry::new();
        registry.register(Box::new(adapter));
        (FallbackExecutor::new(Arc::new(registry)), calls)
    }

    #[tokio::test]
    async fn fallback_advances_on_usage_exhaustion_and_retains_each_report() {
        let dir = tempfile::tempdir().unwrap();
        let (executor, calls) = fallback_executor(&[
            (
                "acct-1",
                ScriptedBehavior::Exhausted {
                    retry_after_ms: 60_000,
                    usage_output_tokens: 10,
                },
            ),
            (
                "acct-2",
                ScriptedBehavior::Complete {
                    usage_output_tokens: 5,
                },
            ),
        ]);

        let result = executor
            .execute(routed_ctx("exec-1", dir.path()))
            .await
            .expect("chain succeeds");

        assert_eq!(result.status, ExecutionOutcome::Completed);
        assert_eq!(*calls.lock().unwrap(), vec!["acct-1", "acct-2"]);
        assert_eq!(result.usage_reports.len(), 2);
        assert_eq!(result.usage_reports[0].counters.output_tokens, Some(10));
        assert_eq!(result.usage_reports[1].counters.output_tokens, Some(5));
        assert_eq!(result.usage_reports[0].provider_id.as_deref(), Some("zai"));
        assert_eq!(result.usage_reports[0].model_id.as_deref(), Some("glm-5"));
        assert_eq!(
            result.usage_reports[1].provider_id.as_deref(),
            Some("google")
        );
        assert_eq!(
            result.usage_reports[1].model_id.as_deref(),
            Some("gemini-3.6-flash")
        );
        assert_eq!(
            result.usage_reports[0].outcome,
            Some(crate::config::RouteAttemptOutcome::UsageExhausted)
        );
        assert_eq!(
            result.usage_reports[1].outcome,
            Some(crate::config::RouteAttemptOutcome::Completed)
        );
        assert_ne!(
            result.usage_reports[0].candidate_key,
            result.usage_reports[1].candidate_key
        );
        let resolved = result.resolved_candidate.expect("winner recorded");
        assert!(resolved.candidate_key.contains("profile=acct-2"));
        let outcomes: Vec<_> = result.route_attempts.iter().map(|a| a.outcome).collect();
        assert_eq!(
            outcomes,
            vec![
                crate::config::RouteAttemptOutcome::UsageExhausted,
                crate::config::RouteAttemptOutcome::Completed
            ]
        );
    }

    #[tokio::test]
    async fn failed_provider_call_admission_prevents_adapter_invocation() {
        let dir = tempfile::tempdir().unwrap();
        let (executor, calls) = fallback_executor(&[(
            "acct-1",
            ScriptedBehavior::Complete {
                usage_output_tokens: 1,
            },
        )]);
        let admission_calls = Arc::new(AtomicUsize::new(0));
        let result = executor
            .execute_with_provider_call_admission(
                routed_ctx("exec-admission-failure", dir.path()),
                Arc::new(RejectingAdmission {
                    calls: Arc::clone(&admission_calls),
                }),
            )
            .await;

        assert!(result.is_err());
        assert_eq!(admission_calls.load(Ordering::SeqCst), 1);
        assert!(calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn task_failure_stops_chain_without_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let (executor, calls) = fallback_executor(&[
            ("acct-1", ScriptedBehavior::FailTask),
            (
                "acct-2",
                ScriptedBehavior::Complete {
                    usage_output_tokens: 5,
                },
            ),
        ]);

        let result = executor
            .execute(routed_ctx("exec-1", dir.path()))
            .await
            .expect("terminal result");

        assert_eq!(result.status, ExecutionOutcome::Failed);
        assert_eq!(
            result.failure_class,
            Some(crate::ExecutionFailureClass::TaskFailed)
        );
        assert_eq!(*calls.lock().unwrap(), vec!["acct-1"]);
    }

    #[tokio::test]
    async fn all_candidates_cooling_fails_fast_and_cooldowns_are_shared() {
        let dir = tempfile::tempdir().unwrap();
        let (executor, calls) = fallback_executor(&[
            (
                "acct-1",
                ScriptedBehavior::Exhausted {
                    retry_after_ms: 60_000,
                    usage_output_tokens: 0,
                },
            ),
            (
                "acct-2",
                ScriptedBehavior::Exhausted {
                    retry_after_ms: 30_000,
                    usage_output_tokens: 0,
                },
            ),
        ]);

        let first = executor
            .execute(routed_ctx("exec-1", dir.path()))
            .await
            .expect("terminal result");
        assert_eq!(
            first.failure_class,
            Some(crate::ExecutionFailureClass::ExecutorUnavailable)
        );
        let retry = first.retry_after.expect("earliest retry propagated");
        assert!(retry <= std::time::Duration::from_millis(30_000));
        assert_eq!(calls.lock().unwrap().len(), 2);

        // A second execution sees both accounts cooling: fail fast, no spawns.
        let second = executor
            .execute(routed_ctx("exec-2", dir.path()))
            .await
            .expect("terminal result");
        assert_eq!(
            second.failure_class,
            Some(crate::ExecutionFailureClass::ExecutorUnavailable)
        );
        assert_eq!(
            calls.lock().unwrap().len(),
            2,
            "no CLI spawned while cooling"
        );
        let outcomes: Vec<_> = second.route_attempts.iter().map(|a| a.outcome).collect();
        assert_eq!(
            outcomes,
            vec![
                crate::config::RouteAttemptOutcome::SkippedCooldown,
                crate::config::RouteAttemptOutcome::SkippedCooldown
            ]
        );
    }

    #[tokio::test]
    async fn cancel_between_hops_spawns_no_further_candidates() {
        let dir = tempfile::tempdir().unwrap();
        let adapter = ScriptedAdapter::new(&[
            ("acct-1", ScriptedBehavior::AwaitCancel),
            (
                "acct-2",
                ScriptedBehavior::Complete {
                    usage_output_tokens: 5,
                },
            ),
        ]);
        let calls = Arc::clone(&adapter.calls);
        let started = Arc::clone(&adapter.started);
        let mut registry = AdapterRegistry::new();
        registry.register(Box::new(adapter));
        let executor = Arc::new(FallbackExecutor::new(Arc::new(registry)));

        let run = tokio::spawn({
            let executor = Arc::clone(&executor);
            let ctx = routed_ctx("exec-cancel", dir.path());
            async move { executor.execute(ctx).await }
        });

        started.notified().await;
        executor
            .cancel("exec-cancel")
            .await
            .expect("cancel succeeds");

        let result = run.await.expect("join").expect("terminal result");
        assert_eq!(result.status, ExecutionOutcome::Cancelled);
        assert_eq!(*calls.lock().unwrap(), vec!["acct-1"]);
    }

    #[tokio::test]
    async fn snapshot_without_routing_dispatches_single_candidate() {
        let dir = tempfile::tempdir().unwrap();
        let (executor, calls) = fallback_executor(&[(
            "acct-1",
            ScriptedBehavior::Complete {
                usage_output_tokens: 1,
            },
        )]);

        let mut ctx = routed_ctx("exec-1", dir.path());
        ctx.agent_config = serde_json::json!({
            "executor_type": "smith",
            "config": {"profile": "acct-1"}
        });

        let result = executor.execute(ctx).await.expect("dispatch succeeds");
        assert_eq!(result.status, ExecutionOutcome::Completed);
        assert_eq!(*calls.lock().unwrap(), vec!["acct-1"]);
        assert!(result
            .resolved_candidate
            .expect("winner recorded")
            .candidate_key
            .contains("profile=acct-1"));
    }

    #[tokio::test]
    async fn adapter_executor_cancel_passthrough_reaches_registered_adapter() {
        let cancel_calls = Arc::new(AtomicUsize::new(0));
        let mut registry = AdapterRegistry::new();
        registry.register(Box::new(CancelTrackingAdapter {
            cancel_calls: Arc::clone(&cancel_calls),
        }));
        let executor = AdapterExecutor::new(Arc::new(registry));

        executor
            .cancel("execution-to-cancel")
            .await
            .expect("cancel succeeds");

        assert_eq!(cancel_calls.load(Ordering::SeqCst), 1);
    }
}
