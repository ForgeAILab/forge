//! Runtime-driven failure matrices over real SQL. The legacy oracle fails the
//! same save barrier as the native journal and uses the identical typed error.
use super::import_matrix::*;
use super::*;
use agent_runtime::core::error::{FailureClass, FailureComponent, FailureStage, RuntimeError};
use agent_runtime::runtime::CheckpointRecoveryPolicy;
use async_trait::async_trait;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

fn injected() -> RuntimeError {
    RuntimeError::internal("injected journal commit failure")
        .with_class(FailureClass::HostComponent {
            stage: FailureStage::PreProvider,
            component: FailureComponent::Checkpoint,
        })
        .retryable()
}
#[derive(Debug, Default)]
pub struct Trace {
    pub snapshots: Mutex<Vec<SessionSnapshot>>,
    pub checkpoints: Mutex<Vec<TurnCheckpoint>>,
}
impl Trace {
    pub fn values(&self) -> (Vec<u8>, Vec<u8>) {
        (
            serde_json::to_vec(&*self.snapshots.lock().unwrap()).unwrap(),
            serde_json::to_vec(&*self.checkpoints.lock().unwrap()).unwrap(),
        )
    }
    pub fn count(&self) -> usize {
        self.snapshots.lock().unwrap().len() + self.checkpoints.lock().unwrap().len()
    }
}
#[derive(Debug)]
pub struct LegacyOracle {
    pub trace: Arc<Trace>,
    target: Option<usize>,
    attempts: AtomicUsize,
}
impl LegacyOracle {
    pub fn new(target: Option<usize>) -> Arc<Self> {
        Arc::new(Self {
            trace: Arc::default(),
            target,
            attempts: AtomicUsize::new(0),
        })
    }
    fn fail(&self) -> bool {
        Some(self.attempts.fetch_add(1, Ordering::SeqCst)) == self.target
    }
}
#[async_trait]
impl SessionStore for LegacyOracle {
    async fn load(&self, _: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        Ok(self.trace.snapshots.lock().unwrap().last().cloned())
    }
    async fn save(&self, s: &SessionSnapshot) -> Result<(), RuntimeError> {
        if self.fail() {
            return Err(injected());
        }
        self.trace
            .snapshots
            .lock()
            .unwrap()
            .push(ForgeJournalProjection.project(s.clone())?);
        Ok(())
    }
}
#[async_trait]
impl CheckpointStore for LegacyOracle {
    async fn load_latest(&self, _: &SessionId) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        Ok(self.trace.checkpoints.lock().unwrap().last().cloned())
    }
    async fn save(&self, c: &TurnCheckpoint) -> Result<(), RuntimeError> {
        if self.fail() {
            return Err(injected());
        }
        c.validate()?;
        self.trace.checkpoints.lock().unwrap().push(c.clone());
        Ok(())
    }
}
#[derive(Debug)]
pub struct Fault {
    pub attempts: AtomicUsize,
    pub target: usize,
    pub point: JournalCommitFailurePoint,
    pub stop_renew: AtomicBool,
}
impl Fault {
    pub fn new(target: usize, point: JournalCommitFailurePoint, crash: bool) -> Arc<Self> {
        Arc::new(Self {
            attempts: AtomicUsize::new(0),
            target,
            point,
            stop_renew: AtomicBool::new(crash && point.is_after_commit()),
        })
    }
}
#[derive(Debug)]
struct TapJournal {
    inner: JournalEngine<ForgeJournalStorage>,
    role: JournalRole,
    fault: Arc<Fault>,
    trace: Arc<Trace>,
}
#[async_trait]
impl SessionJournal for TapJournal {
    fn storage_id(&self) -> JournalStorageId {
        self.inner.storage_id()
    }
    async fn open(&self, s: &SessionId, r: JournalOpen) -> Result<JournalLease, RuntimeError> {
        self.inner.open(s, r).await
    }
    async fn read(&self, l: &JournalLease, id: &JournalObjectId) -> Result<Vec<u8>, RuntimeError> {
        self.inner.read(l, id).await
    }
    async fn commit(
        &self,
        l: &JournalLease,
        c: JournalCommit,
    ) -> Result<JournalHead, RuntimeError> {
        if self.fault.attempts.fetch_add(1, Ordering::SeqCst) == self.fault.target {
            self.inner.fail_next_commit_at(self.fault.point)?;
        }
        let head = self.inner.commit(l, c).await?;
        let lease = self
            .inner
            .open(
                l.session(),
                open(
                    &format!("trace-{}", head.revision.0),
                    JournalOpenIntent::ReadOnly,
                ),
            )
            .await?;
        let mut reader = JournalReader::new(&self.inner, &lease, limits())?;
        match self.role {
            JournalRole::Ordinary => {
                let value = reader.snapshot().await?.unwrap();
                self.trace.snapshots.lock().unwrap().push(value);
            }
            JournalRole::Protected => {
                let value = reader.checkpoint().await?.unwrap();
                self.trace.checkpoints.lock().unwrap().push(value);
            }
        }
        self.inner.close(lease).await?;
        Ok(head)
    }
    async fn renew(&self, l: &JournalLease) -> Result<JournalLease, RuntimeError> {
        if self.fault.stop_renew.swap(false, Ordering::SeqCst) {
            return Err(RuntimeError::internal(
                "process exited before ambiguous resolution",
            ));
        }
        self.inner.renew(l).await
    }
    async fn close(&self, l: JournalLease) -> Result<(), RuntimeError> {
        self.inner.close(l).await
    }
    async fn retire(
        &self,
        s: &SessionId,
        r: JournalRevision,
    ) -> Result<JournalRetirement, RuntimeError> {
        self.inner.retire(s, r).await
    }
    async fn collect(&self, r: JournalGcRequest) -> Result<JournalGcReport, RuntimeError> {
        self.inner.collect(r).await
    }
}
#[derive(Debug)]
pub struct FaultStores {
    pub legacy: Arc<Stores>,
    ordinary: Arc<TapJournal>,
    protected: Arc<TapJournal>,
    pub trace: Arc<Trace>,
}
impl FaultStores {
    pub fn new(inner: Arc<SqliteProtectedRuntimeStore>, fault: Arc<Fault>) -> Arc<Self> {
        let trace = Arc::new(Trace::default());
        let owner = inner.journals();
        Arc::new(Self {
            legacy: Stores::new(inner),
            ordinary: Arc::new(TapJournal {
                inner: owner.ordinary(),
                role: JournalRole::Ordinary,
                fault: fault.clone(),
                trace: trace.clone(),
            }),
            protected: Arc::new(TapJournal {
                inner: owner.protected(),
                role: JournalRole::Protected,
                fault,
                trace: trace.clone(),
            }),
            trace,
        })
    }
}
#[async_trait]
impl SessionStore for FaultStores {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        Some(self.ordinary.clone())
    }
    async fn load(&self, s: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        SessionStore::load(&*self.legacy, s).await
    }
    async fn save(&self, s: &SessionSnapshot) -> Result<(), RuntimeError> {
        SessionStore::save(&*self.legacy, s).await
    }
}
#[async_trait]
impl CheckpointStore for FaultStores {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        Some(self.protected.clone())
    }
    async fn load_latest(&self, s: &SessionId) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        CheckpointStore::load_latest(&*self.legacy, s).await
    }
    async fn save(&self, c: &TurnCheckpoint) -> Result<(), RuntimeError> {
        CheckpointStore::save(&*self.legacy, c).await
    }
}

pub async fn import_cuts() -> usize {
    let mut count = 0;
    for case in cases().await {
        for target in 0..if case.checkpoint.is_some() { 2 } else { 1 } {
            for &point in JournalCommitFailurePoint::ALL {
                let label = format!("{} target={target} {point:?}", case.label);
                let (f, store) = fixture(&case.snapshot.id).await;
                seed(&store, &case).await;
                let before = legacy_row(&f).await;
                let fault = Fault::new(target, point, true);
                let stores = FaultStores::new(store.clone(), fault.clone());
                let io = Arc::new(IoCounts::default());
                let p = case.no_io();
                let rt = builder(
                    p.clone(),
                    stores.clone(),
                    stores.clone(),
                    case.profile,
                    io.clone(),
                )
                .build()
                .unwrap();
                let first = rt
                    .start_session(
                        StartSession::resume(case.snapshot.id.clone())
                            .with_checkpoint_recovery(CheckpointRecoveryPolicy::Defer),
                    )
                    .await;
                assert!(first.is_err(), "{label}: fault did not stop import");
                drop(first);
                drop(rt);
                assert_eq!(fault.attempts.load(Ordering::SeqCst), target + 1, "{label}");
                let flags = store.persistence_flags(&case.snapshot.id).await.unwrap();
                if case.checkpoint.is_some() {
                    assert_eq!(
                        flags.protected_published,
                        target == 1 || point.is_after_commit(),
                        "{label} exact authority"
                    );
                    assert_eq!(
                        flags.ordinary_published,
                        target == 1 && point.is_after_commit(),
                        "{label} ordinary rollback"
                    );
                } else {
                    assert!(!flags.protected_published);
                    assert_eq!(flags.ordinary_published, point.is_after_commit());
                }
                assert!(p.requests().is_empty());
                assert!(p.calls().is_empty());
                assert!(p.resource_requests().is_empty());
                drop(stores);
                drop(store);
                // New pool, storage, engines and memo: a process restart, not a warm reopen.
                let fresh = Arc::new(SqliteProtectedRuntimeStore::new(
                    f.reopen().await.unwrap(),
                    [7; 32],
                    1,
                ));
                let fresh_stores = Stores::new(fresh.clone());
                let (rt, s, p) = deferred(fresh_stores.clone(), &case, io.clone()).await;
                assert_import(&fresh, &case).await;
                assert!(p.requests().is_empty());
                drop(s);
                drop(rt);
                let loads = fresh_stores.loads.load(Ordering::SeqCst);
                let (rt, s, p) = deferred(fresh_stores.clone(), &case, io.clone()).await;
                assert_eq!(
                    fresh_stores.loads.load(Ordering::SeqCst),
                    loads,
                    "{label} R5"
                );
                assert!(p.requests().is_empty());
                assert_eq!(io.tools.load(Ordering::SeqCst), 0);
                assert_eq!(io.interactions.load(Ordering::SeqCst), 0);
                assert_eq!(legacy_row(&f).await, before);
                assert_eq!(fresh_stores.saves.load(Ordering::SeqCst), 0);
                println!(
                    "IMPORT-CUT {label}: rollback/authority/restart/repair/R5/unchanged/no-I-O OK"
                );
                drop(s);
                drop(rt);
                count += 1;
            }
        }
    }
    count
}

fn scripts(profile: Profile) -> Vec<ScriptedStream> {
    if matches!(profile, Profile::Tools) {
        vec![tool_round(), reply("complete")]
    } else {
        vec![reply("complete")]
    }
}
async fn drive(rt: Runtime, id: &SessionId) -> Option<SessionSnapshot> {
    let s = rt
        .start_session(StartSession::create(id.clone(), vec![]))
        .await
        .ok()?;
    s.run(UserInput::text("turn")).await.ok()?;
    let snapshot = s.snapshot();
    drop(s);
    drop(rt);
    Some(snapshot)
}
// Resume with Defer to inspect the exact durable recovery boundary, then let a
// fresh runtime recover it. Provider/tool totals must equal legacy recovery from
// that same boundary; frozen requests and completed outcomes cannot be replayed.
async fn resume(rt: Runtime, id: &SessionId) -> Option<SessionSnapshot> {
    let s = rt
        .start_session(StartSession::resume(id.clone()))
        .await
        .ok()?;
    s.run(UserInput::text("continuation")).await.ok()?;
    let value = s.snapshot();
    drop(s);
    drop(rt);
    Some(value)
}
pub async fn turn_cuts() -> usize {
    let mut count = 0;
    for profile in [Profile::Plain, Profile::Tools] {
        let id = SessionId::new(format!("turn-cuts-{profile:?}"));
        let baseline = LegacyOracle::new(None);
        let rt = builder(
            fake(scripts(profile), profile),
            baseline.clone(),
            baseline.clone(),
            profile,
            Arc::default(),
        )
        .build()
        .unwrap();
        drive(rt, &id).await.unwrap();
        let barriers = baseline.trace.count();
        for target in 0..barriers {
            for &point in JournalCommitFailurePoint::ALL {
                let label = format!("{profile:?} barrier={target}/{barriers} {point:?}");
                let oracle = LegacyOracle::new((!point.is_after_commit()).then_some(target));
                let expected_io = Arc::new(IoCounts::default());
                let expected_p = fake(scripts(profile), profile);
                let expected = drive(
                    builder(
                        expected_p.clone(),
                        oracle.clone(),
                        oracle.clone(),
                        profile,
                        expected_io.clone(),
                    )
                    .build()
                    .unwrap(),
                    &id,
                )
                .await;
                let (f, store) = fixture(&id).await;
                let fault = Fault::new(target, point, false);
                let stores = FaultStores::new(store.clone(), fault.clone());
                let io = Arc::new(IoCounts::default());
                let p = fake(scripts(profile), profile);
                let actual = drive(
                    builder(
                        p.clone(),
                        stores.clone(),
                        stores.clone(),
                        profile,
                        io.clone(),
                    )
                    .build()
                    .unwrap(),
                    &id,
                )
                .await;
                assert!(
                    fault.attempts.load(Ordering::SeqCst) > target,
                    "{label} fault fired"
                );
                assert_eq!(
                    serde_json::to_vec(&actual).unwrap(),
                    serde_json::to_vec(&expected).unwrap(),
                    "{label} live snapshot"
                );
                assert_eq!(
                    stores.trace.values(),
                    oracle.trace.values(),
                    "{label} every live barrier"
                );
                assert_eq!(
                    p.requests().len(),
                    expected_p.requests().len(),
                    "{label} live model calls"
                );
                assert_eq!(
                    io.tools.load(Ordering::SeqCst),
                    expected_io.tools.load(Ordering::SeqCst),
                    "{label} live tool calls"
                );
                drop(stores);
                drop(store);
                let fresh = Arc::new(SqliteProtectedRuntimeStore::new(
                    f.reopen().await.unwrap(),
                    [7; 32],
                    1,
                ));
                let stores = FaultStores::new(fresh, Fault::new(usize::MAX, point, false));
                let p = fake(
                    vec![reply("resumed"), reply("resumed"), reply("resumed")],
                    profile,
                );
                let expected_p = fake(
                    vec![reply("resumed"), reply("resumed"), reply("resumed")],
                    profile,
                );
                let snapshot_start = oracle.trace.snapshots.lock().unwrap().len();
                let checkpoint_start = oracle.trace.checkpoints.lock().unwrap().len();
                let expected = resume(
                    builder(
                        expected_p.clone(),
                        oracle.clone(),
                        oracle.clone(),
                        profile,
                        expected_io.clone(),
                    )
                    .build()
                    .unwrap(),
                    &id,
                )
                .await;
                let actual = resume(
                    builder(
                        p.clone(),
                        stores.clone(),
                        stores.clone(),
                        profile,
                        io.clone(),
                    )
                    .build()
                    .unwrap(),
                    &id,
                )
                .await;
                assert_eq!(
                    serde_json::to_vec(&actual).unwrap(),
                    serde_json::to_vec(&expected).unwrap(),
                    "{label} cold recovery"
                );
                assert_eq!(
                    p.requests().len(),
                    expected_p.requests().len(),
                    "{label} replayed model"
                );
                assert_eq!(
                    io.tools.load(Ordering::SeqCst),
                    expected_io.tools.load(Ordering::SeqCst),
                    "{label} replayed tool"
                );
                let expected_tail = (
                    serde_json::to_vec(&oracle.trace.snapshots.lock().unwrap()[snapshot_start..])
                        .unwrap(),
                    serde_json::to_vec(
                        &oracle.trace.checkpoints.lock().unwrap()[checkpoint_start..],
                    )
                    .unwrap(),
                );
                assert_eq!(
                    stores.trace.values(),
                    expected_tail,
                    "{label} every recovered barrier"
                );
                assert_eq!(stores.legacy.saves.load(Ordering::SeqCst), 0);
                println!("TURN-CUT {label}: live equivalence/cold recovery/no replay OK");
                count += 1;
            }
        }
    }
    count
}

pub async fn stale_legacy_is_not_overlaid() {
    let case = cases()
        .await
        .into_iter()
        .find(|c| c.label == "Idle")
        .unwrap();
    let (f, store) = fixture(&case.snapshot.id).await;
    seed(&store, &case).await;
    let before = legacy_row(&f).await;
    let stores = Stores::new(store.clone());
    let (rt, s, _) = deferred(stores, &case, Arc::default()).await;
    drop(s);
    drop(rt);
    let rt = builder(
        fake(vec![reply("new native result")], case.profile),
        store.clone(),
        store.clone(),
        case.profile,
        Arc::default(),
    )
    .build()
    .unwrap();
    let s = rt
        .start_session(StartSession::resume(case.snapshot.id.clone()))
        .await
        .unwrap();
    s.run(UserInput::text("advanced native state"))
        .await
        .unwrap();
    s.persist().await.unwrap();
    let expected = s.snapshot();
    drop(s);
    drop(rt);
    drop(store);
    let store = Arc::new(SqliteProtectedRuntimeStore::new(
        f.reopen().await.unwrap(),
        [7; 32],
        1,
    ));
    let stores = Stores::new(store.clone());
    let (rt, s, p) = deferred(stores.clone(), &case, Arc::default()).await;
    assert_eq!(s.history(), expected.history);
    assert_eq!(
        snapshot(&store.journals().ordinary(), &case.snapshot.id)
            .await
            .history,
        expected.history
    );
    assert_eq!(stores.loads.load(Ordering::SeqCst), 0);
    assert!(p.requests().is_empty());
    assert_eq!(legacy_row(&f).await, before);
    drop(s);
    drop(rt);
}
