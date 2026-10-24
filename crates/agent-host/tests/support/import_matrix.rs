//! Capture genuine v3 barriers with the pinned runtime, then import through
//! Forge's real encrypted legacy decoder and SQL journal. No fabricated tool
//! preparation fingerprints or private journal metadata decoders.
use super::*;
use agent_runtime::{
    core::{
        approval::AllowAll,
        cancel::{CancelReason, Cancellation},
        checkpoint::TurnState,
        clock::{Clock, Timestamp},
        error::RuntimeError,
        ids::{CacheOperationId, InteractionRequestId},
        interaction::{
            InteractionBroker, InteractionOutcomeKind, InteractionReadiness, InteractionRequest,
            InteractionResponse,
        },
        tool::{InvocationContext, LegacyTool, ToolEffects, ToolOutcome},
    },
    harness::{QUESTIONNAIRE_TOOL_NAME, QuestionnaireTool},
    provider::fake::tool_call_fragments,
    runtime::CheckpointRecoveryPolicy,
};
use async_trait::async_trait;
use serde_json::{Value, json};
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::Notify;

#[derive(Debug)]
struct FixedClock;
impl Clock for FixedClock {
    fn now(&self) -> Timestamp {
        Timestamp(100)
    }
}

#[derive(Debug, Default)]
struct Capture {
    snapshots: Mutex<Vec<SessionSnapshot>>,
    checkpoints: Mutex<Vec<TurnCheckpoint>>,
    saved: Notify,
}
#[async_trait]
impl SessionStore for Capture {
    async fn load(&self, _: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        Ok(self.snapshots.lock().unwrap().last().cloned())
    }
    async fn save(&self, s: &SessionSnapshot) -> Result<(), RuntimeError> {
        self.snapshots.lock().unwrap().push(s.clone());
        Ok(())
    }
}
#[async_trait]
impl CheckpointStore for Capture {
    async fn load_latest(&self, _: &SessionId) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        Ok(self.checkpoints.lock().unwrap().last().cloned())
    }
    async fn save(&self, c: &TurnCheckpoint) -> Result<(), RuntimeError> {
        c.validate()?;
        self.checkpoints.lock().unwrap().push(c.clone());
        self.saved.notify_one();
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct IoCounts {
    pub tools: AtomicUsize,
    pub interactions: AtomicUsize,
}
#[derive(Debug)]
struct Echo(Arc<IoCounts>);
#[async_trait]
impl LegacyTool for Echo {
    fn name(&self) -> &str {
        "echo"
    }
    fn description(&self) -> &str {
        "Returns text"
    }
    fn input_schema(&self) -> Value {
        json!({"type":"object","properties":{"text":{"type":"string"}},"required":["text"],"additionalProperties":false})
    }
    fn effects(&self) -> ToolEffects {
        ToolEffects::default().with_spawn()
    }
    async fn invoke_legacy(
        &self,
        a: Value,
        _: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        self.0.tools.fetch_add(1, Ordering::SeqCst);
        Ok(ToolOutcome::text(a["text"].as_str().unwrap_or_default()))
    }
}
#[derive(Debug)]
struct PendingInteraction(Arc<IoCounts>);
#[async_trait]
impl InteractionBroker for PendingInteraction {
    fn readiness(&self) -> InteractionReadiness {
        InteractionReadiness::Ready
    }
    async fn interact(&self, _: &InteractionRequest) -> InteractionResponse {
        self.0.interactions.fetch_add(1, Ordering::SeqCst);
        std::future::pending().await
    }
    fn close(&self, _: &InteractionRequestId, _: InteractionOutcomeKind) {}
}

#[derive(Clone, Copy, Debug)]
pub enum Profile {
    Plain,
    Tools,
    Interaction,
    Cache,
}
pub fn configure(b: RuntimeBuilder, profile: Profile, io: Arc<IoCounts>) -> RuntimeBuilder {
    match profile {
        Profile::Plain => b,
        Profile::Tools => b
            .legacy_approval_authority()
            .approval(Arc::new(AllowAll))
            .tool(Arc::new(Echo(io))),
        Profile::Interaction => b
            .legacy_approval_authority()
            .approval(Arc::new(AllowAll))
            .tool(Arc::new(QuestionnaireTool::new()))
            .interaction_broker(Arc::new(PendingInteraction(io))),
        Profile::Cache => {
            use agent_runtime::{
                context::ProviderCacheCapability, core::provider::CacheEndpointIdentity,
            };
            b.system_prompt("stable cache prefix")
                .cache_endpoint_identity(CacheEndpointIdentity::from_opaque(
                    "matrix-cache",
                    RegistryRevision::new("endpoint-1"),
                ))
                .cache_capability(ProviderCacheCapability::from_contract(
                    RegistryRevision::new("cache-1"),
                    "fake",
                    cache_capabilities().cache_contract.unwrap(),
                ))
        }
    }
}
pub fn builder(
    p: Arc<FakeProvider>,
    s: Arc<dyn SessionStore>,
    c: Arc<dyn CheckpointStore>,
    profile: Profile,
    io: Arc<IoCounts>,
) -> RuntimeBuilder {
    configure(
        RuntimeBuilder::new(ModelId::new("fake"))
            .provider_name("fake")
            .provider(p)
            .model_profile(ResolvedModelProfile::explicit(
                "fake",
                ModelId::new("fake"),
                ModelLimits::new(32_000, 32_000, 4_096),
            ))
            .clock(Arc::new(FixedClock))
            .session_store(s)
            .checkpoint_store(c),
        profile,
        io,
    )
}
pub fn fake(scripts: Vec<ScriptedStream>, profile: Profile) -> Arc<FakeProvider> {
    Arc::new(FakeProvider::new(
        "fake",
        if matches!(profile, Profile::Cache) {
            cache_capabilities()
        } else {
            Capabilities::basic_streaming()
        },
        scripts,
    ))
}
pub fn reply(text: &str) -> ScriptedStream {
    ScriptedStream::new(vec![
        ProviderStreamEvent::TextDelta { text: text.into() },
        ProviderStreamEvent::Finish {
            reason: FinishReason::Stop,
        },
    ])
}
pub fn tool_round() -> ScriptedStream {
    let mut events = tool_call_fragments(
        0,
        "matrix-call",
        "echo",
        &json!({"text":"durable tool result"}).to_string(),
    );
    events.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    ScriptedStream::new(events)
}
fn cache_capabilities() -> Capabilities {
    use agent_runtime::core::provider::*;
    Capabilities {
        cache: true,
        prompt_cache: PromptCacheControl::Implicit,
        cache_contract: Some(ProviderCacheContract {
            behavior: ProviderCacheBehavior::ImplicitPrefix,
            evidence: CacheEvidenceCapabilities {
                stream: true,
                ..Default::default()
            },
            maintenance: [ProviderAttemptPurpose::CacheKeepalive].into(),
            conformance: Some(SyntheticConformance::complete()),
            ..Default::default()
        }),
        ..Capabilities::basic_streaming()
    }
}

#[derive(Clone, Debug)]
pub struct ImportCase {
    pub label: &'static str,
    pub snapshot: SessionSnapshot,
    pub checkpoint: Option<TurnCheckpoint>,
    pub profile: Profile,
}
impl ImportCase {
    pub fn no_io(&self) -> Arc<FakeProvider> {
        fake(vec![], self.profile)
    }
}
pub async fn cases() -> Vec<ImportCase> {
    let capture = Arc::new(Capture::default());
    let rt = builder(
        fake(
            vec![reply("seed reply"), tool_round(), reply("tool complete")],
            Profile::Tools,
        ),
        capture.clone(),
        capture.clone(),
        Profile::Tools,
        Arc::default(),
    )
    .build()
    .unwrap();
    let s = rt
        .start_session(StartSession::create(SessionId::new("matrix-tools"), vec![]))
        .await
        .unwrap();
    s.run(UserInput::text("seed")).await.unwrap();
    let before = s.snapshot();
    let start = capture.checkpoints.lock().unwrap().len();
    s.run(UserInput::text("tool turn")).await.unwrap();
    let cps = capture.checkpoints.lock().unwrap()[start..].to_vec();
    let mut result = vec![];
    let find = |label, predicate: fn(&TurnState) -> bool| ImportCase {
        label,
        snapshot: before.clone(),
        checkpoint: Some(
            cps.iter()
                .find(|c| predicate(&c.state))
                .unwrap_or_else(|| panic!("missing {label}"))
                .clone(),
        ),
        profile: Profile::Tools,
    };
    result.push(find("CallingModel", |s| {
        matches!(s, TurnState::CallingModel { .. })
    }));
    result.push(find("ModelResponseReady", |s| {
        matches!(s, TurnState::ModelResponseReady { .. })
    }));
    result.push(find("AwaitingApproval", |s| {
        matches!(s, TurnState::AwaitingApproval { .. })
    }));
    result.push(find("ExecutingTools", |s| {
        matches!(s, TurnState::ExecutingTools { .. })
    }));
    result.push(find("ToolOutcomeReady", |s| {
        matches!(s, TurnState::ToolOutcomeReady { .. })
    }));
    let terminal = cps.last().unwrap().clone();
    assert!(terminal.state.is_terminal());
    result.push(ImportCase {
        label: "Idle",
        snapshot: s.snapshot(),
        checkpoint: Some(terminal),
        profile: Profile::Tools,
    });
    result.push(ImportCase {
        label: "snapshot-only",
        snapshot: before,
        checkpoint: None,
        profile: Profile::Tools,
    });
    s.shutdown().await.unwrap();

    let capture = Arc::new(Capture::default());
    let mut question=tool_call_fragments(0,"matrix-question",QUESTIONNAIRE_TOOL_NAME,&json!({"questions":[{"id":"choice","header":"Choice","prompt":"Choose one","allow_free_form":true}]}).to_string());
    question.push(ProviderStreamEvent::Finish {
        reason: FinishReason::ToolCalls,
    });
    let rt = builder(
        fake(
            vec![reply("seed"), ScriptedStream::new(question)],
            Profile::Interaction,
        ),
        capture.clone(),
        capture.clone(),
        Profile::Interaction,
        Arc::default(),
    )
    .build()
    .unwrap();
    let s = rt
        .start_session(StartSession::create(
            SessionId::new("matrix-interaction"),
            vec![],
        ))
        .await
        .unwrap();
    s.run(UserInput::text("seed")).await.unwrap();
    let before = s.snapshot();
    let turn = s.send(UserInput::text("ask me")).unwrap();
    let cp = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let notified = capture.saved.notified();
            if let Some(c) = capture
                .checkpoints
                .lock()
                .unwrap()
                .iter()
                .find(|c| matches!(c.state, TurnState::AwaitingInteraction { .. }))
                .cloned()
            {
                break c;
            }
            notified.await;
        }
    })
    .await
    .unwrap();
    turn.interrupt(CancelReason::UserRequested);
    turn.completed().await;
    s.shutdown().await.unwrap();
    result.push(ImportCase {
        label: "AwaitingInteraction",
        snapshot: before,
        checkpoint: Some(cp),
        profile: Profile::Interaction,
    });

    let capture = Arc::new(Capture::default());
    let rt = builder(
        fake((0..4).map(|_| reply("cached")).collect(), Profile::Cache),
        capture.clone(),
        capture.clone(),
        Profile::Cache,
        Arc::default(),
    )
    .build()
    .unwrap();
    let s = rt
        .start_session(StartSession::create(SessionId::new("matrix-cache"), vec![]))
        .await
        .unwrap();
    s.run(UserInput::text("seed")).await.unwrap();
    s.run(UserInput::text("second")).await.unwrap();
    let before = s.snapshot();
    use agent_runtime::core::provider::{
        CacheAuthority, CacheOperationBudget, ProviderAttemptPurpose,
    };
    let op = s
        .cache_operation_from_last_plan(
            CacheOperationId::new("matrix-cache"),
            ProviderAttemptPurpose::CacheKeepalive,
            CacheAuthority::new("matrix-cache"),
            CacheOperationBudget::default(),
            Cancellation::new(),
            Deadline::at(Timestamp(10_000)),
        )
        .unwrap();
    s.dispatch_cache_operation(op).await.unwrap();
    let cp = capture
        .checkpoints
        .lock()
        .unwrap()
        .iter()
        .find(|c| matches!(c.state, TurnState::CacheOperationStarted { .. }))
        .unwrap()
        .clone();
    s.shutdown().await.unwrap();
    result.push(ImportCase {
        label: "cache-started",
        snapshot: before,
        checkpoint: Some(cp),
        profile: Profile::Cache,
    });
    // A sensitive diagnostic exercises policy equality independently of exact recovery.
    for case in &mut result {
        let diagnostic = VersionedSessionState::new(
            RegistryRevision::new("matrix-1"),
            json!({"diagnostic":"optional private diagnostic"}),
        );
        case.snapshot
            .extension_state
            .insert("forge.diagnostics.matrix".into(), diagnostic.clone());
        if let Some(c) = &mut case.checkpoint {
            c.snapshot
                .extension_state
                .insert("forge.diagnostics.matrix".into(), diagnostic);
            c.validate().unwrap();
        }
    }
    result
}

#[derive(Debug)]
pub struct Stores {
    pub inner: Arc<SqliteProtectedRuntimeStore>,
    pub loads: AtomicUsize,
    pub saves: AtomicUsize,
}
impl Stores {
    pub fn new(inner: Arc<SqliteProtectedRuntimeStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            loads: AtomicUsize::new(0),
            saves: AtomicUsize::new(0),
        })
    }
}
#[async_trait]
impl SessionStore for Stores {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        SessionStore::journal(&*self.inner)
    }
    async fn load(&self, id: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        self.loads.fetch_add(1, Ordering::SeqCst);
        SessionStore::load(&*self.inner, id).await
    }
    async fn save(&self, s: &SessionSnapshot) -> Result<(), RuntimeError> {
        self.saves.fetch_add(1, Ordering::SeqCst);
        SessionStore::save(&*self.inner, s).await
    }
}
#[async_trait]
impl CheckpointStore for Stores {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        CheckpointStore::journal(&*self.inner)
    }
    async fn load_latest(&self, id: &SessionId) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        self.loads.fetch_add(1, Ordering::SeqCst);
        CheckpointStore::load_latest(&*self.inner, id).await
    }
    async fn save(&self, c: &TurnCheckpoint) -> Result<(), RuntimeError> {
        self.saves.fetch_add(1, Ordering::SeqCst);
        CheckpointStore::save(&*self.inner, c).await
    }
}
pub async fn seed(store: &SqliteProtectedRuntimeStore, case: &ImportCase) {
    store
        .seed_legacy_bytes(
            &case.snapshot.id,
            Some(&serde_json::to_vec(&case.snapshot).unwrap()),
            case.checkpoint
                .as_ref()
                .map(serde_json::to_vec)
                .transpose()
                .unwrap()
                .as_deref(),
            true,
        )
        .await
        .unwrap();
}
/// All 14 legacy columns, preserving NULL versus empty BLOB and exact bytes.
pub async fn legacy_row(f: &FileDb) -> Vec<Value> {
    sqlx::query_scalar::<_, String>(
        "SELECT json_object(
            'session_id',session_id,
            'snapshot_ciphertext',iif(snapshot_ciphertext IS NULL,NULL,hex(snapshot_ciphertext)),
            'snapshot_nonce',iif(snapshot_nonce IS NULL,NULL,hex(snapshot_nonce)),
            'checkpoint_ciphertext',iif(checkpoint_ciphertext IS NULL,NULL,hex(checkpoint_ciphertext)),
            'checkpoint_nonce',iif(checkpoint_nonce IS NULL,NULL,hex(checkpoint_nonce)),
            'checkpoint_turn_id',checkpoint_turn_id,
            'checkpoint_revision',checkpoint_revision,
            'checkpoint_fingerprint',checkpoint_fingerprint,
            'key_revision',key_revision,
            'state_revision',state_revision,
            'updated_at',updated_at,
            'lcm_policy_revision',lcm_policy_revision,
            'snapshot_digest',snapshot_digest,
            'checkpoint_lcm_policy_revision',checkpoint_lcm_policy_revision
        ) FROM protected_agent_session_state ORDER BY session_id",
    )
    .fetch_all(f.database().pool())
    .await.unwrap().into_iter().map(|s|serde_json::from_str(&s).unwrap()).collect()
}
pub async fn deferred(
    stores: Arc<Stores>,
    case: &ImportCase,
    io: Arc<IoCounts>,
) -> (
    Runtime,
    agent_runtime::runtime::SessionHandle,
    Arc<FakeProvider>,
) {
    let p = case.no_io();
    let rt = builder(p.clone(), stores.clone(), stores, case.profile, io)
        .build()
        .unwrap();
    let s = rt
        .start_session(
            StartSession::resume(case.snapshot.id.clone())
                .with_checkpoint_recovery(CheckpointRecoveryPolicy::Defer),
        )
        .await
        .unwrap();
    assert!(
        p.calls().is_empty(),
        "deferred import/reopen issued a provider call"
    );
    assert!(
        p.resource_requests().is_empty(),
        "deferred import/reopen dispatched a cache resource operation"
    );
    (rt, s, p)
}
pub async fn assert_import(store: &SqliteProtectedRuntimeStore, case: &ImportCase) {
    let owner = store.journals();
    assert_eq!(
        snapshot(&owner.ordinary(), &case.snapshot.id).await,
        ForgeJournalProjection
            .project(case.snapshot.clone())
            .unwrap(),
        "{} ordinary",
        case.label
    );
    let protected = owner.protected();
    let lease = protected
        .open(
            &case.snapshot.id,
            open("exact-read", JournalOpenIntent::ReadOnly),
        )
        .await
        .unwrap();
    let actual = JournalReader::new(&protected, &lease, limits())
        .unwrap()
        .checkpoint()
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_vec(&actual).unwrap(),
        serde_json::to_vec(&case.checkpoint).unwrap(),
        "{} protected",
        case.label
    );
    if case.checkpoint.is_none() {
        assert!(lease.head().is_none(), "no invented protected publication");
    }
    protected.close(lease).await.unwrap();
}
pub async fn audit_publications(f: &FileDb) {
    for sql in [
        "CREATE TABLE import_publication_order (n INTEGER PRIMARY KEY, role TEXT NOT NULL)",
        "CREATE TRIGGER import_publication_order AFTER UPDATE ON agent_session_journal_session WHEN OLD.published=0 AND NEW.published=1 BEGIN INSERT INTO import_publication_order(role) VALUES (NEW.role); END",
    ] {
        sqlx::query(sql).execute(f.database().pool()).await.unwrap();
    }
}
