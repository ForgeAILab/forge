//! Dispatching one ephemeral inquiry sub-agent.
//!
//! An inquiry is deliberately not a Task. There is no worktree, no workflow
//! state machine, no review, and no dispatch queue: one nested native turn
//! runs with the account read surface and a scratch directory, and the
//! calling turn blocks on its answer. The sub-agent's transcript never enters
//! the caller's history -- only a bounded abstract and the path to a findings
//! file come back -- which is the entire reason the operation exists.
//!
//! The visible run record in `agent_inquiry` is a log, not a work item. Its
//! only user verb is cancel.

use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, Weak},
    time::Duration,
};

use async_trait::async_trait;
use db::{
    new_uuid_v4, AgentInquiry, AgentInquiryRepo, AgentInquiryStatus, AgentProfile,
    AgentProfileRepo, AgentSession, CompleteAgentInquiry, CreateAgentInquiry, CredentialHandleRepo,
    SqliteDb,
};
use forge_agent_host::{
    AgentSessionBackend, AgentTurnRequest, CanonicalScope, CanonicalScopeType,
    NativeProviderConfig, WorkspaceAccess,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::{
    agent_chat_turn_worker::{AgentChatTurnLogRoot, NativeProfileConfig},
    embedded_agent_service::EmbeddedAgentService,
    turn_log_sink::TurnLogSink,
    Result, ServiceError,
};

/// The abstract that comes back into the caller's context. An unbounded
/// sub-agent reply would only move tokens from one conversation into another
/// instead of saving any, so the returned text is clipped hard and the full
/// account stays on disk.
pub const MAX_FINDINGS_ABSTRACT_CHARS: usize = 2_000;

/// The wall-clock ceiling on one inquiry. The caller's turn is blocked and
/// its provider connection is open for the whole time, so an inquiry that
/// cannot finish inside this is failed rather than left to strand the turn.
pub const INQUIRY_TIMEOUT: Duration = Duration::from_secs(600);
const INQUIRY_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

/// The file the sub-agent is asked to write its full findings into, relative
/// to its own directory.
pub const FINDINGS_FILENAME: &str = "findings.md";

/// What the caller asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InquiryRequest {
    pub chat_id: String,
    pub turn_job_id: Option<String>,
    pub identity_id: String,
    /// The account that owns the dispatching identity. An inquiry can never
    /// reach past it.
    pub account_id: String,
    pub title: String,
    pub question: String,
    pub context: Option<String>,
}

/// What the caller gets back. Deliberately small.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InquiryOutcome {
    pub inquiry_id: String,
    pub status: AgentInquiryStatus,
    /// The bounded abstract, or the failure message.
    pub findings: String,
    /// Where the sub-agent's full account lives, relative to the caller's own
    /// scratch root, so the caller can read it with its file tools when the
    /// abstract is not enough.
    pub findings_path: Option<String>,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_write_tokens: i64,
    pub duration_ms: i64,
}

#[async_trait]
pub trait InquiryRunner: Send + Sync + std::fmt::Debug {
    async fn dispatch(
        &self,
        request: InquiryRequest,
        cancellation: CancellationToken,
    ) -> Result<InquiryOutcome>;

    /// Stop a running inquiry's provider call.
    ///
    /// Marking the record cancelled is not enough on its own: without this
    /// the sub-agent keeps talking to the provider, and the user's cancel
    /// only takes effect when the turn happens to finish. Returns whether a
    /// live run was actually signalled -- `false` means it had already
    /// finished, which is not an error.
    async fn cancel_inquiry(&self, inquiry_id: &str) -> bool;
}

/// Runs inquiries on the embedded native runtime.
#[derive(Clone)]
pub struct EmbeddedInquiryRunner {
    db: Arc<SqliteDb>,
    /// Weak on purpose. The service owns the native backend, the backend owns
    /// the tool provider, and the provider owns this runner -- a strong
    /// handle here would close that cycle and leak the whole graph.
    embedded_agents: Weak<EmbeddedAgentService>,
    /// One inquiry at a time per account.
    ///
    /// Runs share an authority binding and scratch root, but native inquiry
    /// state is ephemeral. Keep execution serial so two turns cannot collide
    /// in the backend's active-session registry or shared findings directory.
    account_locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
    /// Live runs, so a cancel from the REST surface can reach the turn that
    /// is actually talking to the provider.
    active: Arc<Mutex<HashMap<String, CancellationToken>>>,
    /// Provider reports observed by this process, including reports carried
    /// by a runtime failure. The terminal CAS drains this side channel only
    /// after it has won the race with cancellation.
    observed_usage: Arc<Mutex<HashMap<String, Vec<executors::UsageReport>>>>,
    /// Inquiries write the same Forge JSONL activity log an Agent Chat turn
    /// writes, keyed by inquiry id, so one log reader and one renderer serve
    /// both and a sub-agent's work is watchable while it runs.
    turn_logs: AgentChatTurnLogRoot,
}

impl std::fmt::Debug for EmbeddedInquiryRunner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EmbeddedInquiryRunner")
            .finish_non_exhaustive()
    }
}

impl EmbeddedInquiryRunner {
    pub fn new(
        db: Arc<SqliteDb>,
        embedded_agents: Weak<EmbeddedAgentService>,
        turn_logs: AgentChatTurnLogRoot,
    ) -> Self {
        Self {
            db,
            embedded_agents,
            account_locks: Arc::new(Mutex::new(HashMap::new())),
            active: Arc::new(Mutex::new(HashMap::new())),
            observed_usage: Arc::new(Mutex::new(HashMap::new())),
            turn_logs,
        }
    }

    fn embedded_agents(&self) -> Result<Arc<EmbeddedAgentService>> {
        self.embedded_agents.upgrade().ok_or_else(|| {
            ServiceError::invalid_operation("the embedded agent runtime is shutting down")
        })
    }

    async fn account_lock(&self, account_id: &str) -> Arc<Mutex<()>> {
        let mut locks = self.account_locks.lock().await;
        Arc::clone(
            locks
                .entry(account_id.to_owned())
                .or_insert_with(|| Arc::new(Mutex::new(()))),
        )
    }

    async fn remember_usage(&self, inquiry_id: &str, reports: Vec<executors::UsageReport>) {
        if reports.is_empty() {
            return;
        }
        self.observed_usage
            .lock()
            .await
            .entry(inquiry_id.to_owned())
            .or_default()
            .extend(reports);
    }

    async fn take_usage(&self, inquiry_id: &str) -> Vec<executors::UsageReport> {
        self.observed_usage
            .lock()
            .await
            .remove(inquiry_id)
            .unwrap_or_default()
    }

    /// The sub-agent's entire brief. It does not see the calling
    /// conversation, so everything it needs is stated here.
    fn system_prompt(inquiry_id: &str, findings_relative_path: &str) -> String {
        format!(
            "You are a Forge inquiry sub-agent. Another Agent dispatched you to answer one \
bounded question and it is blocked waiting for your answer.\n\n\
You do not see the conversation that dispatched you. Everything you are told below is \
everything you get; do not ask follow-up questions, because nobody is there to answer them.\n\n\
What you can do: read this account's bounded projections through your read tools, search the \
public web if that tool is composed, and read, write, and run commands inside your own \
directory. You cannot create Projects, publish handoffs, propose anything, touch any \
repository, or dispatch another inquiry. There is no repository anywhere in your workspace.\n\n\
How to answer, in this order:\n\
1. Do the research.\n\
2. Write your full account -- evidence, reasoning, what you checked, what you could not \
determine -- to `{findings_relative_path}`. Be as long as the work deserves; nothing here \
costs the caller anything.\n\
3. Reply with a short abstract of at most {MAX_FINDINGS_ABSTRACT_CHARS} characters. This \
reply, and only this reply, enters the caller's context. Lead with the answer. If you could \
not answer, say so plainly and say why -- a clear negative is a useful result, an invented \
answer is not.\n\n\
Your inquiry id is {inquiry_id}."
        )
    }

    fn user_input(request: &InquiryRequest) -> String {
        match request
            .context
            .as_deref()
            .map(str::trim)
            .filter(|context| !context.is_empty())
        {
            Some(context) => format!(
                "{}\n\n--- Supporting context from the caller ---\n{}",
                request.question.trim(),
                context
            ),
            None => request.question.trim().to_owned(),
        }
    }

    /// Run the nested turn. Split out so every failure path can still close
    /// the visible run record rather than leaving it stuck on `running`.
    #[allow(clippy::too_many_arguments)]
    async fn run_turn(
        &self,
        request: &InquiryRequest,
        inquiry_id: &str,
        findings_relative_path: &str,
        cancellation: CancellationToken,
        sink: Arc<TurnLogSink>,
        session: AgentSession,
        profile: AgentProfile,
    ) -> Result<forge_agent_host::AgentTurnOutput> {
        if cancellation.is_cancelled() {
            return Err(ServiceError::invalid_operation(
                "inquiry cancelled before startup",
            ));
        }
        let embedded_agents = self.embedded_agents()?;
        let runtime_session_id = session
            .runtime_session_id
            .clone()
            .ok_or_else(|| ServiceError::invalid_operation("inquiry session has no runtime id"))?;
        if session.identity_id != request.identity_id || profile.identity_id != request.identity_id
        {
            return Err(ServiceError::invalid_operation(
                "inquiry session/profile does not match its identity",
            ));
        }
        if session.profile_id != profile.id {
            return Err(ServiceError::invalid_operation(
                "inquiry session/profile snapshot disagrees",
            ));
        }
        let credential_ref = profile
            .credential_ref
            .as_deref()
            .ok_or_else(|| ServiceError::invalid_operation("Agent profile has no credential"))?;
        let provider = profile
            .provider
            .clone()
            .ok_or_else(|| ServiceError::invalid_operation("Agent profile has no provider"))?;
        let model = profile
            .model
            .clone()
            .ok_or_else(|| ServiceError::invalid_operation("Agent profile has no model"))?;
        let config: NativeProfileConfig = serde_json::from_str(&profile.config_json)
            .map_err(|_| ServiceError::invalid_operation("Agent profile config is invalid"))?;
        let provider_account_id =
            CredentialHandleRepo::get_credential_handle(&*self.db, credential_ref)
                .await?
                .as_ref()
                .and_then(crate::embedded_agent_service::entry_provider_account_id);
        let (context_tokens, max_input_tokens, max_output_tokens) =
            crate::embedded_agent_service::effective_native_limits(
                &provider,
                config.context_tokens,
                config.max_input_tokens,
                config.max_output_tokens,
            );

        let turn_cancellation = cancellation.child_token();
        let provider_id = provider.clone();
        let model_id = model.clone();
        let turn = AgentTurnRequest {
            forge_session_id: session.id.clone(),
            runtime_session_id,
            scope: CanonicalScope {
                scope_type: CanonicalScopeType::Account,
                scope_id: request.account_id.clone(),
                workspace_access: WorkspaceAccess::AccountScratch,
            },
            // The scratch root, not the inquiry's own directory: the session
            // binding is per account, and the runtime validates the request
            // path against it.
            workspace_path: Some(
                embedded_agents
                    .main_agent_workspace(&request.account_id)
                    .await
                    .ok_or_else(|| {
                        ServiceError::invalid_operation(
                            "the Main Agent scratch workspace is unavailable",
                        )
                    })?
                    .to_string_lossy()
                    .into_owned(),
            ),
            provider: NativeProviderConfig {
                provider,
                base_url: config.base_url,
                model,
                credential_handle_id: credential_ref.to_owned(),
                owner_user_id: request.account_id.clone(),
                provider_account_id,
                context_tokens,
                max_input_tokens,
                max_output_tokens,
            },
            system_prompt: Some(Self::system_prompt(inquiry_id, findings_relative_path)),
            // The native backend also omits persistent stores for inquiry
            // scope: empty initial history alone cannot override a snapshot.
            history: Vec::new(),
            input: Self::user_input(request),
            cancellation: turn_cancellation.clone(),
        };

        let backend = embedded_agents.native_backend();
        let observed_usage = Arc::clone(&self.observed_usage);
        let inquiry_id_for_reports = inquiry_id.to_owned();
        let output = await_inquiry_turn(
            async move {
                match backend.run_turn(turn, sink).await {
                    Ok(output) => Ok(output),
                    Err(forge_agent_host::AgentHostError::RuntimeWithUsage {
                        message,
                        usage_reports,
                    }) => {
                        let mapped = usage_reports
                            .iter()
                            .enumerate()
                            .map(|(sequence, report)| {
                                let sequence = u32::try_from(sequence).map_err(|_| {
                                    ServiceError::invalid_operation(
                                        "usage report sequence overflows",
                                    )
                                });
                                sequence.and_then(|sequence| {
                                    crate::chat_usage::usage_report_from_host(
                                        report, "chat", 0, sequence,
                                    )
                                })
                            })
                            .collect::<Result<Vec<_>>>();
                        if let Ok(mapped) = mapped {
                            if !mapped.is_empty() {
                                observed_usage
                                    .lock()
                                    .await
                                    .entry(inquiry_id_for_reports)
                                    .or_default()
                                    .extend(mapped);
                            }
                        }
                        Err(forge_agent_host::AgentHostError::Runtime(message))
                    }
                    Err(error) => Err(error),
                }
            },
            turn_cancellation,
            INQUIRY_TIMEOUT,
        )
        .await?;
        let mut reports = output
            .usage_reports
            .iter()
            .enumerate()
            .map(|(sequence, report)| {
                crate::chat_usage::usage_report_from_host(
                    report,
                    "chat",
                    0,
                    u32::try_from(sequence).map_err(|_| {
                        ServiceError::invalid_operation("usage report sequence overflows")
                    })?,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        if reports.is_empty()
            && output.telemetry_state == forge_agent_host::AgentTurnTelemetryState::Metered
        {
            reports.push(executors::UsageReport {
                report_id: format!("{inquiry_id}:native"),
                request_id: None,
                report_sequence: 0,
                candidate_key: Some("chat".to_owned()),
                attempt_ordinal: 0,
                provider_id: Some(provider_id),
                model_id: Some(model_id),
                counters: executors::UsageCounters {
                    input_tokens: Some(output.input_tokens),
                    output_tokens: Some(output.output_tokens),
                    cache_read_tokens: Some(output.cache_read_tokens),
                    cache_write_tokens: Some(output.cache_write_tokens),
                },
                telemetry_state: executors::UsageTelemetryState::Metered,
                context_tokens: None,
                selected_tier: None,
                reported_cost_usd: None,
                outcome: None,
                partial: false,
            });
        }
        self.remember_usage(inquiry_id, reports).await;
        Ok(output)
    }
}

#[async_trait]
impl InquiryRunner for EmbeddedInquiryRunner {
    async fn cancel_inquiry(&self, inquiry_id: &str) -> bool {
        let token = self.active.lock().await.get(inquiry_id).cloned();
        match token {
            Some(token) => {
                token.cancel();
                true
            }
            // Already finished, or never ran on this process. The record's
            // own status is the authority either way.
            None => false,
        }
    }

    async fn dispatch(
        &self,
        request: InquiryRequest,
        cancellation: CancellationToken,
    ) -> Result<InquiryOutcome> {
        let runner = self.clone();
        // The runtime drops a tool future on cancellation. An owned task
        // continues just long enough to stop its backend and terminalize the
        // run record; dropping the caller always signals that cleanup.
        await_owned_inquiry(cancellation, move |cancellation| async move {
            runner.dispatch_owned(request, cancellation).await
        })
        .await
    }
}

impl EmbeddedInquiryRunner {
    async fn dispatch_owned(
        &self,
        request: InquiryRequest,
        cancellation: CancellationToken,
    ) -> Result<InquiryOutcome> {
        let title = request.title.trim();
        let question = request.question.trim();
        if title.is_empty() || question.is_empty() {
            return Err(ServiceError::invalid_operation(
                "an inquiry needs both a title and a question",
            ));
        }

        let lock = self.account_lock(&request.account_id).await;
        let _serialized = tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                return Err(ServiceError::invalid_operation("inquiry cancelled before dispatch"));
            }
            guard = lock.lock() => guard,
        };

        let inquiry_id = new_uuid_v4();
        let workspace = self
            .embedded_agents()?
            .inquiry_workspace(&request.account_id, &inquiry_id)
            .await
            .ok_or_else(|| {
                ServiceError::invalid_operation("the inquiry workspace is unavailable")
            })?;
        // Relative to the account scratch root, which is what both the
        // sub-agent and the caller compose paths against.
        let findings_relative_path = format!(
            "{}/{inquiry_id}/{FINDINGS_FILENAME}",
            crate::task_service::workspace::MAIN_AGENT_INQUIRIES_DIR
        );

        // Register before the row becomes visible so a REST cancellation
        // can always reach a run, including while its backend initializes.
        let run_token = cancellation.child_token();
        self.active
            .lock()
            .await
            .insert(inquiry_id.clone(), run_token.clone());
        let record = AgentInquiryRepo::create_agent_inquiry(
            &*self.db,
            CreateAgentInquiry {
                id: inquiry_id.clone(),
                chat_id: request.chat_id.clone(),
                turn_job_id: request.turn_job_id.clone(),
                identity_id: request.identity_id.clone(),
                owner_user_id: request.account_id.clone(),
                title: title.to_owned(),
                question: question.to_owned(),
                workspace_path: Some(workspace.to_string_lossy().into_owned()),
            },
        )
        .await;
        let record = match record {
            Ok(record) => record,
            Err(error) => {
                self.active.lock().await.remove(&inquiry_id);
                return Err(error.into());
            }
        };
        let sink = Arc::new(TurnLogSink::new(
            self.turn_logs.path_for(&inquiry_id),
            &inquiry_id,
            None,
            None,
        ));

        // Freeze the pricing subject/selection and durably start the one
        // inquiry invocation before the backend can make a provider call.
        // Admission failures never reach the provider and are terminalized
        // through the same transaction boundary with an empty settlement.
        let started = std::time::Instant::now();
        let admission: Result<(db::UsageInvocation, AgentSession, AgentProfile)> = if run_token
            .is_cancelled()
        {
            Err(ServiceError::invalid_operation(
                "inquiry cancelled before provider admission",
            ))
        } else {
            let embedded_agents = self.embedded_agents();
            match embedded_agents {
                Err(error) => Err(error),
                Ok(embedded_agents) => {
                    let session = embedded_agents
                        .create_inquiry_session(&request.account_id, &request.identity_id)
                        .await;
                    match session {
                        Err(error) => Err(error),
                        Ok(session) => {
                            let profile =
                                match AgentProfileRepo::get_profile(&*self.db, &session.profile_id)
                                    .await
                                {
                                    Ok(Some(profile)) => Ok(profile),
                                    Ok(None) => Err(ServiceError::not_found(
                                        "agent_profile",
                                        session.profile_id.clone(),
                                    )),
                                    Err(error) => Err(error.into()),
                                };
                            match profile {
                                Err(error) => Err(error),
                                Ok(profile) => {
                                    if profile.identity_id != request.identity_id {
                                        Err(ServiceError::invalid_operation(
                                            "inquiry profile does not match its identity",
                                        ))
                                    } else {
                                        crate::chat_usage::admit_inquiry_usage(
                                            &self.db, &record, &profile,
                                        )
                                        .await
                                        .map(|invocation| (invocation, session, profile))
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };
        let (result, observed_reports) = match admission {
            Ok((_, session, profile)) => {
                let result = self
                    .run_turn(
                        &request,
                        &inquiry_id,
                        &findings_relative_path,
                        run_token.clone(),
                        sink,
                        session,
                        profile,
                    )
                    .await;
                let observed_reports = self.take_usage(&inquiry_id).await;
                (result, observed_reports)
            }
            Err(error) => (Err(error), Vec::new()),
        };
        let duration_ms = i64::try_from(started.elapsed().as_millis()).map_err(|_| {
            ServiceError::invalid_operation("inquiry duration overflows the persisted range")
        })?;
        let settlement_now = db::now_rfc3339();
        let settlements = match crate::chat_usage::build_chat_usage_settlements(
            &self.db,
            &record.id,
            &observed_reports,
            &settlement_now,
        )
        .await
        {
            Ok(settlements) => settlements,
            Err(error) => {
                let cancelled = AgentInquiryRepo::get_agent_inquiry(&*self.db, &record.id)
                    .await
                    .ok()
                    .flatten()
                    .is_some_and(|record| record.status == AgentInquiryStatus::Cancelled);
                if cancelled && !observed_reports.is_empty() {
                    let _ = crate::chat_usage::settle_late_chat_usage(
                        &self.db,
                        &record.id,
                        &observed_reports,
                        &settlement_now,
                    )
                    .await;
                }
                self.active.lock().await.remove(&inquiry_id);
                return Err(error);
            }
        };
        let outcome = async {
            match result {
                Ok(output) => {
                    let findings = clip(&output.text, MAX_FINDINGS_ABSTRACT_CHARS);
                    // Only claim a findings file when the sub-agent actually
                    // wrote one; a path the caller cannot open is worse than no
                    // path at all.
                    let findings_path = workspace
                        .join(FINDINGS_FILENAME)
                        .is_file()
                        .then(|| findings_relative_path.clone());
                    let completed = complete_with_usage(
                        &self.db,
                        &record,
                        AgentInquiryStatus::Succeeded,
                        Some(findings.clone()),
                        findings_path.clone(),
                        None,
                        &output,
                        duration_ms,
                        settlements.clone(),
                        &observed_reports,
                    )
                    .await?;
                    Ok(inquiry_outcome(completed))
                }
                Err(error) => {
                    let status = if run_token.is_cancelled() {
                        AgentInquiryStatus::Cancelled
                    } else {
                        AgentInquiryStatus::Failed
                    };
                    let message = error.to_string();
                    // Close the visible record even on the failure path, so a
                    // run never sits on `running` forever.
                    let completed = complete_or_cancelled_with_usage(
                        &self.db,
                        CompleteAgentInquiry {
                            id: record.id.clone(),
                            expected_version: record.version,
                            status: status.clone(),
                            findings: None,
                            findings_path: None,
                            error: Some(message.clone()),
                            input_tokens: 0,
                            output_tokens: 0,
                            cache_read_tokens: 0,
                            cache_write_tokens: 0,
                            duration_ms: Some(duration_ms),
                        },
                        settlements.clone(),
                        &observed_reports,
                    )
                    .await?;
                    Ok(inquiry_outcome(completed))
                }
            }
        }
        .await;
        // Keep cancellation reachable until its durable terminal state is
        // resolved. Storage failures remain errors, never invented outcomes.
        self.active.lock().await.remove(&inquiry_id);
        outcome
    }
}

async fn await_owned_inquiry<F, Fut, T>(cancellation: CancellationToken, run: F) -> Result<T>
where
    F: FnOnce(CancellationToken) -> Fut,
    Fut: Future<Output = Result<T>> + Send + 'static,
    T: Send + 'static,
{
    let cancellation = cancellation.child_token();
    let _cancel_on_drop = cancellation.clone().drop_guard();
    tokio::spawn(run(cancellation)).await.map_err(|error| {
        ServiceError::invalid_operation(format!("inquiry runner stopped: {error}"))
    })?
}

async fn await_inquiry_turn<F, T>(
    turn: F,
    cancellation: CancellationToken,
    timeout: Duration,
) -> Result<T>
where
    F: Future<Output = std::result::Result<T, forge_agent_host::AgentHostError>>,
{
    tokio::pin!(turn);
    tokio::select! {
        output = &mut turn => output.map_err(|error| ServiceError::invalid_operation(error.to_string())),
        _ = tokio::time::sleep(timeout) => {
            cancellation.cancel();
            // The backend owns a spawned runtime driver. Dropping its future
            // would skip shutdown and allow provider work to outlive failure.
            let _ = tokio::time::timeout(INQUIRY_SHUTDOWN_TIMEOUT, turn).await;
            Err(ServiceError::invalid_operation(format!(
                "the inquiry did not finish within {} seconds",
                timeout.as_secs()
            )))
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn complete_with_usage(
    db: &SqliteDb,
    record: &AgentInquiry,
    status: AgentInquiryStatus,
    findings: Option<String>,
    findings_path: Option<String>,
    error: Option<String>,
    output: &forge_agent_host::AgentTurnOutput,
    duration_ms: i64,
    settlements: Vec<db::UsageLedgerSettlement>,
    reports: &[executors::UsageReport],
) -> Result<AgentInquiry> {
    complete_or_cancelled_with_usage(
        db,
        CompleteAgentInquiry {
            id: record.id.clone(),
            expected_version: record.version,
            status,
            findings,
            findings_path,
            error,
            input_tokens: i64::try_from(output.input_tokens).map_err(|_| {
                ServiceError::invalid_operation(
                    "inquiry input token count overflows the persisted range",
                )
            })?,
            output_tokens: i64::try_from(output.output_tokens).map_err(|_| {
                ServiceError::invalid_operation(
                    "inquiry output token count overflows the persisted range",
                )
            })?,
            cache_read_tokens: i64::try_from(output.cache_read_tokens).map_err(|_| {
                ServiceError::invalid_operation(
                    "inquiry cache-read token count overflows the persisted range",
                )
            })?,
            cache_write_tokens: i64::try_from(output.cache_write_tokens).map_err(|_| {
                ServiceError::invalid_operation(
                    "inquiry cache-write token count overflows the persisted range",
                )
            })?,
            duration_ms: Some(duration_ms),
        },
        settlements,
        reports,
    )
    .await
}

async fn complete_or_cancelled_with_usage(
    db: &SqliteDb,
    input: CompleteAgentInquiry,
    settlements: Vec<db::UsageLedgerSettlement>,
    reports: &[executors::UsageReport],
) -> Result<AgentInquiry> {
    let id = input.id.clone();
    match AgentInquiryRepo::complete_agent_inquiry_with_usage(
        db,
        db::CompleteAgentInquiryWithUsage {
            terminal: input,
            settlements,
        },
    )
    .await
    {
        Ok(record) => {
            // Cancellation is the visible-state authority.  The SQLite
            // composite deliberately leaves a provider invocation in
            // `pending_settlement` when that state has already won; drain
            // reports only after the domain CAS, including the branch where
            // this completion arrived with an already-cancelled terminal
            // payload rather than a VersionConflict.
            if record.status == AgentInquiryStatus::Cancelled && !reports.is_empty() {
                crate::chat_usage::settle_late_chat_usage(db, &id, reports, &db::now_rfc3339())
                    .await?;
            }
            Ok(record)
        }
        Err(db::DbError::VersionConflict) => {
            let current = AgentInquiryRepo::get_agent_inquiry(db, &id).await?;
            match current {
                Some(record) if record.status == AgentInquiryStatus::Cancelled => {
                    if !reports.is_empty() {
                        crate::chat_usage::settle_late_chat_usage(
                            db,
                            &id,
                            reports,
                            &db::now_rfc3339(),
                        )
                        .await?;
                    }
                    Ok(record)
                }
                _ => Err(db::DbError::VersionConflict.into()),
            }
        }
        Err(error) => Err(error.into()),
    }
}

fn inquiry_outcome(record: AgentInquiry) -> InquiryOutcome {
    InquiryOutcome {
        inquiry_id: record.id,
        status: record.status,
        findings: record
            .findings
            .or(record.error)
            .unwrap_or_else(|| "The inquiry was cancelled before it reported.".to_owned()),
        findings_path: record.findings_path,
        input_tokens: record.input_tokens,
        output_tokens: record.output_tokens,
        cache_read_tokens: record.cache_read_tokens,
        cache_write_tokens: record.cache_write_tokens,
        duration_ms: record.duration_ms.unwrap_or(0),
    }
}

/// Clip on a character boundary, marking that the text was cut so the caller
/// can tell a short answer from a truncated one.
fn clip(text: &str, limit: usize) -> String {
    let text = text.trim();
    if text.chars().count() <= limit {
        return text.to_owned();
    }
    let kept: String = text.chars().take(limit).collect();
    format!("{kept}\n\n[abstract truncated; the full findings file has the rest]")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The cancellation registry is pure in-process state, so these tests
    /// need a runner value rather than a working runtime: the database and
    /// the agent service behind it are never reached.
    async fn runner() -> EmbeddedInquiryRunner {
        let pool = db::create_sqlite_pool("sqlite::memory:")
            .await
            .expect("in-memory pool");
        EmbeddedInquiryRunner::new(
            Arc::new(SqliteDb::new(pool)),
            Weak::new(),
            AgentChatTurnLogRoot::new(std::path::PathBuf::from("/tmp")),
        )
    }

    async fn running_record() -> (Arc<SqliteDb>, AgentInquiry) {
        let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let db = Arc::new(SqliteDb::new(pool));
        let now = db::now_rfc3339();
        db::UserRepo::create_user(
            &*db,
            &db::User {
                id: "inquiry-user".to_owned(),
                email: "inquiry@example.test".to_owned(),
                password_hash: "test".to_owned(),
                display_name: None,
                is_admin: false,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .unwrap();
        let chat = db::AgentChatRepo::get_main_chat(&*db, "inquiry-user")
            .await
            .unwrap()
            .unwrap();
        let record = AgentInquiryRepo::create_agent_inquiry(
            &*db,
            CreateAgentInquiry {
                id: "inquiry-1".to_owned(),
                chat_id: chat.id,
                turn_job_id: None,
                identity_id: "identity-1".to_owned(),
                owner_user_id: "inquiry-user".to_owned(),
                title: "Research".to_owned(),
                question: "What changed?".to_owned(),
                workspace_path: None,
            },
        )
        .await
        .unwrap();
        (db, record)
    }

    #[tokio::test]
    async fn forged_cross_account_inquiry_fails_before_provider_and_usage_admission() {
        let pool = db::create_sqlite_pool("sqlite::memory:")
            .await
            .expect("in-memory pool");
        db::run_migrations(&pool).await.expect("migrations apply");
        let db = Arc::new(SqliteDb::new(pool));
        let now = db::now_rfc3339();
        for (id, email) in [
            ("inquiry-owner", "inquiry-owner@example.test"),
            ("agent-owner", "agent-owner@example.test"),
        ] {
            db::UserRepo::create_user(
                &*db,
                &db::User {
                    id: id.to_owned(),
                    email: email.to_owned(),
                    password_hash: "test".to_owned(),
                    display_name: None,
                    is_admin: false,
                    created_at: now.clone(),
                    updated_at: now.clone(),
                },
            )
            .await
            .expect("test user");
        }

        db::AgentRepo::create_identity_with_profile(
            &*db,
            db::CreateAgentIdentity {
                id: "forged-inquiry-agent".to_owned(),
                name: "Forged Inquiry Agent".to_owned(),
                description: None,
                max_concurrent_tasks: 1,
                heartbeat_interval_seconds: 30,
                max_missed_heartbeats: 3,
                status: db::AgentStatus::Idle,
                last_heartbeat_at: None,
                is_default: false,
                paused: false,
                owner_id: Some("agent-owner".to_owned()),
                visibility: "account".to_owned(),
                account_permission_ceiling: "{}".to_owned(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
            db::CreateAgentProfile {
                id: "forged-inquiry-profile".to_owned(),
                identity_id: "forged-inquiry-agent".to_owned(),
                backend_kind: "native".to_owned(),
                executor_type: "embedded".to_owned(),
                provider: Some("openai".to_owned()),
                model: Some("test-model".to_owned()),
                reasoning_effort: None,
                permission_policy: None,
                prompt_template: None,
                capabilities_json: "[]".to_owned(),
                tool_policy_json: "{}".to_owned(),
                config_json: "{}".to_owned(),
                credential_ref: None,
                daemon_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("test agent");

        let chat = db::AgentChatRepo::get_main_chat(&*db, "inquiry-owner")
            .await
            .expect("test chat lookup")
            .expect("test Main Chat");
        let workspace_root = std::env::temp_dir().join(format!("forge-inquiry-{}", new_uuid_v4()));
        let embedded_agents = Arc::new(EmbeddedAgentService::new(Arc::clone(&db), b"test-key"));
        embedded_agents.set_workspace_root(workspace_root.clone(), workspace_root.clone());
        let runner = EmbeddedInquiryRunner::new(
            Arc::clone(&db),
            Arc::downgrade(&embedded_agents),
            AgentChatTurnLogRoot::new(workspace_root.join("logs")),
        );

        let outcome = runner
            .dispatch(
                InquiryRequest {
                    chat_id: chat.id,
                    turn_job_id: None,
                    identity_id: "forged-inquiry-agent".to_owned(),
                    account_id: "inquiry-owner".to_owned(),
                    title: "Cross-account inquiry".to_owned(),
                    question: "This must never reach the provider".to_owned(),
                    context: None,
                },
                CancellationToken::new(),
            )
            .await
            .expect("admission failure is terminalized as an inquiry outcome");

        assert_eq!(outcome.status, AgentInquiryStatus::Failed);
        assert!(
            outcome.findings.to_ascii_lowercase().contains("agent"),
            "the outcome should preserve the authority failure: {}",
            outcome.findings
        );

        let provider_session_rows: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM agent_session WHERE identity_id = ?")
                .bind("forged-inquiry-agent")
                .fetch_one(db.pool())
                .await
                .expect("provider session count");
        assert_eq!(
            provider_session_rows, 0,
            "authority must fail before an inquiry session can reach the provider"
        );

        let usage_rows: i64 = sqlx::query_scalar(
            "SELECT (SELECT COUNT(*) FROM pricing_selection)
                  + (SELECT COUNT(*) FROM usage_invocation)
                  + (SELECT COUNT(*) FROM usage_event)",
        )
        .fetch_one(db.pool())
        .await
        .expect("usage rows count");
        assert_eq!(
            usage_rows, 0,
            "forged authority must not write pricing or usage rows"
        );

        let _ = std::fs::remove_dir_all(workspace_root);
    }

    fn completion(record: &AgentInquiry) -> CompleteAgentInquiry {
        CompleteAgentInquiry {
            id: record.id.clone(),
            expected_version: record.version,
            status: AgentInquiryStatus::Succeeded,
            findings: Some("Answer".to_owned()),
            findings_path: None,
            error: None,
            input_tokens: 10,
            output_tokens: 3,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
            duration_ms: Some(10),
        }
    }

    #[tokio::test]
    async fn inquiry_completion_storage_failure_is_not_reported_as_cancellation() {
        let (db, record) = running_record().await;
        sqlx::query("CREATE TRIGGER reject_inquiry_completion BEFORE UPDATE ON agent_inquiry BEGIN SELECT RAISE(ABORT, 'storage fault'); END")
            .execute(db.pool()).await.unwrap();
        assert!(
            complete_or_cancelled_with_usage(&db, completion(&record), Vec::new(), &[])
                .await
                .is_err()
        );
        let current = AgentInquiryRepo::get_agent_inquiry(&*db, &record.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.status, AgentInquiryStatus::Running);
    }

    #[tokio::test]
    async fn inquiry_completion_only_accepts_a_persisted_cancellation_conflict() {
        let (db, record) = running_record().await;
        let mut stale = completion(&record);
        stale.expected_version += 1;
        assert!(
            complete_or_cancelled_with_usage(&db, stale, Vec::new(), &[])
                .await
                .is_err()
        );
        AgentInquiryRepo::cancel_agent_inquiry(&*db, &record.id, record.version)
            .await
            .unwrap();
        let result = complete_or_cancelled_with_usage(&db, completion(&record), Vec::new(), &[])
            .await
            .unwrap();
        assert_eq!(result.status, AgentInquiryStatus::Cancelled);
        assert_eq!(
            inquiry_outcome(result).status,
            AgentInquiryStatus::Cancelled
        );
    }

    #[tokio::test]
    async fn inquiry_terminalization_rejects_counter_overflow_without_clamping() {
        let (db, record) = running_record().await;
        let output = forge_agent_host::AgentTurnOutput {
            runtime_session_id: "runtime-session".to_owned(),
            text: "Answer".to_owned(),
            input_tokens: u64::MAX,
            output_tokens: 1,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
            usage_reports: Vec::new(),
            telemetry_state: forge_agent_host::AgentTurnTelemetryState::Metered,
            context_manifest: None,
            pending_interaction_id: None,
        };
        assert!(complete_with_usage(
            &db,
            &record,
            AgentInquiryStatus::Succeeded,
            Some("Answer".to_owned()),
            None,
            None,
            &output,
            10,
            Vec::new(),
            &[],
        )
        .await
        .is_err());
        let current = AgentInquiryRepo::get_agent_inquiry(&*db, &record.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.status, AgentInquiryStatus::Running);
    }

    /// Cancelling must reach the token the turn is actually running under.
    /// Marking the record alone would leave the sub-agent talking to its
    /// provider until the timeout.
    #[tokio::test]
    async fn cancelling_a_live_inquiry_signals_its_running_turn() {
        let runner = runner().await;
        let token = CancellationToken::new();
        runner
            .active
            .lock()
            .await
            .insert("inq-live".to_owned(), token.clone());

        assert!(runner.cancel_inquiry("inq-live").await);
        assert!(token.is_cancelled(), "the running turn must be signalled");
    }

    /// A run that already finished is deregistered, so a late cancel reports
    /// that it signalled nothing rather than claiming a stop that never
    /// happened.
    #[tokio::test]
    async fn cancelling_a_finished_inquiry_signals_nothing() {
        let runner = runner().await;
        assert!(!runner.cancel_inquiry("inq-gone").await);
    }

    #[tokio::test]
    async fn cancelling_the_caller_cancels_the_inquiry_it_is_waiting_on() {
        let parent = CancellationToken::new();
        let (started, ready) = tokio::sync::oneshot::channel();
        let caller = tokio::spawn(await_owned_inquiry(
            parent.clone(),
            move |child| async move {
                started.send(()).unwrap();
                child.cancelled().await;
                Ok("terminalized")
            },
        ));
        ready.await.unwrap();
        parent.cancel();
        assert_eq!(caller.await.unwrap().unwrap(), "terminalized");
    }

    #[tokio::test]
    async fn dropping_the_caller_stops_the_owned_inquiry_and_finishes_cleanup() {
        let (started, ready) = tokio::sync::oneshot::channel();
        let (finished, cleanup) = tokio::sync::oneshot::channel();
        let caller = tokio::spawn(await_owned_inquiry(
            CancellationToken::new(),
            move |child| async move {
                started.send(()).unwrap();
                child.cancelled().await;
                // This stands for the record/registry cleanup after backend
                // shutdown: it must still run after the tool future is gone.
                finished.send(()).unwrap();
                Ok(())
            },
        ));
        ready.await.unwrap();
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        tokio::time::timeout(Duration::from_secs(2), cleanup)
            .await
            .expect("owned cleanup must complete")
            .unwrap();
    }

    #[tokio::test]
    async fn an_inquiry_timeout_cancels_and_drains_the_backend() {
        let parent = CancellationToken::new();
        let child = parent.child_token();
        let observed = child.clone();
        let stopped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let backend_stopped = Arc::clone(&stopped);
        let result: Result<()> = await_inquiry_turn(
            async move {
                observed.cancelled().await;
                tokio::task::yield_now().await;
                backend_stopped.store(true, std::sync::atomic::Ordering::SeqCst);
                Err(forge_agent_host::AgentHostError::Runtime(
                    "cancelled".to_owned(),
                ))
            },
            child,
            Duration::from_millis(1),
        )
        .await;
        assert!(result.unwrap_err().to_string().contains("did not finish"));
        assert!(stopped.load(std::sync::atomic::Ordering::SeqCst));
        assert!(
            !parent.is_cancelled(),
            "timeout must remain a failed run, not user cancellation"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_unresponsive_inquiry_shutdown_cannot_block_its_caller_forever() {
        let cancellation = CancellationToken::new();
        let started = tokio::time::Instant::now();
        let result: Result<()> = await_inquiry_turn(
            std::future::pending(),
            cancellation.clone(),
            Duration::from_secs(1),
        )
        .await;
        assert!(result.is_err());
        assert!(cancellation.is_cancelled());
        assert_eq!(
            started.elapsed(),
            Duration::from_secs(1) + INQUIRY_SHUTDOWN_TIMEOUT
        );
    }

    #[tokio::test]
    async fn a_cancelled_inquiry_waiting_for_the_account_lock_never_starts() {
        let runner = runner().await;
        let lock = runner.account_lock("account-1").await;
        let _held = lock.lock().await;
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let result = runner
            .dispatch_owned(
                InquiryRequest {
                    chat_id: "chat-1".to_owned(),
                    turn_job_id: None,
                    identity_id: "identity-1".to_owned(),
                    account_id: "account-1".to_owned(),
                    title: "Waiting".to_owned(),
                    question: "What changed?".to_owned(),
                    context: None,
                },
                cancellation,
            )
            .await;
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("cancelled before dispatch"));
    }

    #[test]
    fn a_short_abstract_is_returned_whole() {
        assert_eq!(clip("  the answer is 42  ", 100), "the answer is 42");
    }

    #[test]
    fn a_long_abstract_is_clipped_and_says_so() {
        let clipped = clip(&"x".repeat(50), 10);
        assert!(clipped.starts_with(&"x".repeat(10)));
        assert!(clipped.contains("truncated"));
        // The caller is told where the rest is rather than being handed a
        // silently shortened answer.
        assert!(clipped.contains("findings file"));
    }

    #[test]
    fn clipping_respects_character_boundaries() {
        // A byte-wise cut here would panic or produce invalid UTF-8.
        let clipped = clip(&"日本語".repeat(10), 4);
        assert!(clipped.starts_with("日本語日"));
    }

    #[test]
    fn supporting_context_is_appended_not_substituted() {
        let request = InquiryRequest {
            chat_id: "chat-1".to_owned(),
            turn_job_id: None,
            identity_id: "identity-1".to_owned(),
            account_id: "account-1".to_owned(),
            title: "Pricing".to_owned(),
            question: "  Which projects stalled?  ".to_owned(),
            context: Some("  prior notes  ".to_owned()),
        };
        let input = EmbeddedInquiryRunner::user_input(&request);
        assert!(input.starts_with("Which projects stalled?"));
        assert!(input.contains("prior notes"));

        let without = InquiryRequest {
            context: Some("   ".to_owned()),
            ..request
        };
        assert_eq!(
            EmbeddedInquiryRunner::user_input(&without),
            "Which projects stalled?"
        );
    }

    #[test]
    fn the_brief_tells_the_sub_agent_it_is_alone_and_bounded() {
        let prompt = EmbeddedInquiryRunner::system_prompt("inq-1", "inquiries/inq-1/findings.md");
        assert!(prompt.contains("inquiries/inq-1/findings.md"));
        assert!(prompt.contains("inq-1"));
        // The three properties the composition already enforces are also
        // stated, so the model does not waste a turn discovering them.
        assert!(prompt.contains("dispatch another inquiry"));
        assert!(prompt.contains("do not ask follow-up questions"));
        assert!(prompt.contains(&MAX_FINDINGS_ABSTRACT_CHARS.to_string()));
    }
}
