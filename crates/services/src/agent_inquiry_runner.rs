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
    new_uuid_v4, AgentInquiry, AgentInquiryRepo, AgentInquiryStatus, AgentProfileRepo,
    CompleteAgentInquiry, CreateAgentInquiry, CredentialHandleRepo, SqliteDb,
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
    async fn run_turn(
        &self,
        request: &InquiryRequest,
        inquiry_id: &str,
        findings_relative_path: &str,
        cancellation: CancellationToken,
        sink: Arc<TurnLogSink>,
    ) -> Result<forge_agent_host::AgentTurnOutput> {
        if cancellation.is_cancelled() {
            return Err(ServiceError::invalid_operation(
                "inquiry cancelled before startup",
            ));
        }
        let embedded_agents = self.embedded_agents()?;
        let session = embedded_agents
            .create_inquiry_session(&request.account_id, &request.identity_id)
            .await?;
        let runtime_session_id = session
            .runtime_session_id
            .clone()
            .ok_or_else(|| ServiceError::invalid_operation("inquiry session has no runtime id"))?;
        let profile = AgentProfileRepo::get_profile(&*self.db, &session.profile_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent_profile", session.profile_id.clone()))?;
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
        await_inquiry_turn(
            backend.run_turn(turn, sink),
            turn_cancellation,
            INQUIRY_TIMEOUT,
        )
        .await
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

        let started = std::time::Instant::now();
        let result = self
            .run_turn(
                &request,
                &inquiry_id,
                &findings_relative_path,
                run_token.clone(),
                sink,
            )
            .await;
        let duration_ms = i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX);
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
                    let completed = complete(
                        &self.db,
                        &record,
                        AgentInquiryStatus::Succeeded,
                        Some(findings.clone()),
                        findings_path.clone(),
                        None,
                        &output,
                        duration_ms,
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
                    let completed = complete_or_cancelled(
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
async fn complete(
    db: &SqliteDb,
    record: &AgentInquiry,
    status: AgentInquiryStatus,
    findings: Option<String>,
    findings_path: Option<String>,
    error: Option<String>,
    output: &forge_agent_host::AgentTurnOutput,
    duration_ms: i64,
) -> Result<AgentInquiry> {
    complete_or_cancelled(
        db,
        CompleteAgentInquiry {
            id: record.id.clone(),
            expected_version: record.version,
            status,
            findings,
            findings_path,
            error,
            // The four counters stay disjoint all the way through: context
            // size is input + cache_read + cache_write, and collapsing them
            // here would silently under-report what an inquiry cost.
            input_tokens: i64::try_from(output.input_tokens).unwrap_or(i64::MAX),
            output_tokens: i64::try_from(output.output_tokens).unwrap_or(i64::MAX),
            cache_read_tokens: i64::try_from(output.cache_read_tokens).unwrap_or(i64::MAX),
            cache_write_tokens: i64::try_from(output.cache_write_tokens).unwrap_or(i64::MAX),
            duration_ms: Some(duration_ms),
        },
    )
    .await
}

async fn complete_or_cancelled(db: &SqliteDb, input: CompleteAgentInquiry) -> Result<AgentInquiry> {
    let id = input.id.clone();
    match AgentInquiryRepo::complete_agent_inquiry(db, input).await {
        Ok(record) => Ok(record),
        Err(db::DbError::VersionConflict) => {
            let current = AgentInquiryRepo::get_agent_inquiry(db, &id).await?;
            match current {
                Some(record) if record.status == AgentInquiryStatus::Cancelled => Ok(record),
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
        assert!(complete_or_cancelled(&db, completion(&record))
            .await
            .is_err());
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
        assert!(complete_or_cancelled(&db, stale).await.is_err());
        AgentInquiryRepo::cancel_agent_inquiry(&*db, &record.id, record.version)
            .await
            .unwrap();
        let result = complete_or_cancelled(&db, completion(&record))
            .await
            .unwrap();
        assert_eq!(result.status, AgentInquiryStatus::Cancelled);
        assert_eq!(
            inquiry_outcome(result).status,
            AgentInquiryStatus::Cancelled
        );
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
