//! Forge D10 measurements. The upstream bench is a method/fixture reference only.
#![recursion_limit = "256"]
#[path = "support/bench_fixture.rs"]
mod fixture;
#[path = "support/bench_storage.rs"]
mod metered;
#[path = "support/bench_metrics.rs"]
mod metrics;
use agent_runtime::core::{
    checkpoint::{CheckpointStore, TurnCheckpoint},
    clock::Timestamp,
    error::RuntimeError,
    ids::SessionId,
    journal::*,
    store::{SessionSnapshot, SessionStore},
};
use async_trait::async_trait;
use fixture::TerminalMaintenance;
use forge_agent_host::{
    SqliteProtectedRuntimeStore,
    session_journal::ForgeJournalStorage,
    session_journals::{ForgeJournalProjection, JOURNAL_MEMO_BYTES, SessionJournals},
    test_support::FileDb,
};
use metrics::*;
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

type Engine = JournalEngine<metered::Metered<ForgeJournalStorage>>;
#[derive(Debug, Default, serde::Serialize)]
struct Maintenance {
    calls: u64,
    collection_pack_bytes: u64,
    largest_collection_call_write: u64,
    longest_collect_us: u64,
    s2_pack_bytes: u64,
    turn_cumulative_pack_bytes: Vec<u64>,
    reports: Vec<Value>,
}
#[derive(Debug)]
struct Native {
    file: FileDb,
    raw: Arc<ForgeJournalStorage>,
    meter: Arc<metered::Metered<ForgeJournalStorage>>,
    ordinary: Engine,
    protected: Engine,
    per_turn: AtomicBool,
    maintenance: Mutex<Maintenance>,
    opens: Arc<Mutex<Vec<Value>>>,
}
impl Native {
    fn new(file: FileDb, identity: bool) -> Self {
        let raw = Arc::new(ForgeJournalStorage::new(file.database(), [7; 32], 1).unwrap());
        let meter = Arc::new(metered::Metered::new(raw.clone()));
        let owner = SessionJournals::with_test_memo_budget(meter.clone(), JOURNAL_MEMO_BYTES);
        let ordinary = if identity {
            JournalEngine::ordinary(
                meter.clone(),
                owner.ordinary().storage_id(),
                Arc::new(IdentityJournalProjection),
                JournalEngineConfig {
                    collection_enabled: true,
                    validated_root_memo_bytes: JOURNAL_MEMO_BYTES,
                },
            )
        } else {
            owner.ordinary()
        };
        Self {
            file,
            raw,
            meter,
            ordinary,
            protected: owner.protected(),
            per_turn: AtomicBool::new(true),
            maintenance: Mutex::new(Maintenance::default()),
            opens: Arc::new(Mutex::new(vec![])),
        }
    }
    fn domains(&self) -> [(Engine, JournalDomain); 2] {
        [
            (
                self.protected.clone(),
                JournalDomain {
                    storage_id: self.protected.storage_id(),
                    role: JournalRole::Protected,
                },
            ),
            (
                self.ordinary.clone(),
                JournalDomain {
                    storage_id: self.ordinary.storage_id(),
                    role: JournalRole::Ordinary,
                },
            ),
        ]
    }
    async fn collect(&self) {
        for (engine, _) in self.domains() {
            // Production max_objects is a membership-release bound. Sweep until
            // the durable dirty index is empty, never cancel an engine call.
            loop {
                let before = if profile::clean() {
                    0
                } else {
                    writes(self.file.database().pool()).await.packs()
                };
                if !profile::clean() {
                    rewrite_phase(self.file.database().pool(), true).await;
                }
                let collect_before = forge_agent_host::session_journal::profile::snapshot();
                let start = Instant::now();
                let collect_profile = forge_agent_host::session_journal::profile::timer(
                    format!(
                        "host.collect.{}",
                        if engine.storage_id() == self.protected.storage_id() {
                            "protected"
                        } else {
                            "ordinary"
                        }
                    ),
                    0,
                );
                let report = engine
                    .collect(JournalGcRequest {
                        storage_id: engine.storage_id(),
                        expected_root_epoch: engine.root_epoch().unwrap(),
                        max_objects: 20_000,
                    })
                    .await
                    .unwrap();
                let elapsed = start.elapsed().as_micros() as u64;
                drop(collect_profile);
                profile::collect_sample(
                    if engine.storage_id() == self.protected.storage_id() {
                        "protected"
                    } else {
                        "ordinary"
                    },
                    elapsed,
                    collect_before,
                );
                if !profile::clean() {
                    rewrite_phase(self.file.database().pool(), false).await;
                }
                let written = if profile::clean() {
                    0
                } else {
                    writes(self.file.database().pool()).await.packs()
                } - before;
                assert!(report.unreadable_sessions.is_empty());
                {
                    let mut m = self.maintenance.lock().unwrap();
                    m.calls += 1;
                    m.collection_pack_bytes += written;
                    m.largest_collection_call_write = m.largest_collection_call_write.max(written);
                    m.longest_collect_us = m.longest_collect_us.max(elapsed);
                    m.reports.push(serde_json::to_value(&report).unwrap());
                }
                let domain = JournalDomain {
                    storage_id: engine.storage_id(),
                    role: if engine.storage_id() == self.protected.storage_id() {
                        JournalRole::Protected
                    } else {
                        JournalRole::Ordinary
                    },
                };
                if self
                    .raw
                    .dirty_sessions(&domain, None, 1)
                    .await
                    .unwrap()
                    .is_empty()
                {
                    break;
                }
            }
        }
    }
}
#[async_trait]
impl SessionStore for Native {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        Some(Arc::new(Observed {
            inner: self.ordinary.clone(),
            meter: self.meter.clone(),
            samples: self.opens.clone(),
        }))
    }
    async fn load(&self, _: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        Ok(None)
    }
    async fn save(&self, _: &SessionSnapshot) -> Result<(), RuntimeError> {
        panic!("native legacy fallback")
    }
}
#[async_trait]
impl CheckpointStore for Native {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        Some(Arc::new(Observed {
            inner: self.protected.clone(),
            meter: self.meter.clone(),
            samples: self.opens.clone(),
        }))
    }
    async fn load_latest(&self, _: &SessionId) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        Ok(None)
    }
    async fn save(&self, _: &TurnCheckpoint) -> Result<(), RuntimeError> {
        panic!("native legacy fallback")
    }
}
#[async_trait]
impl TerminalMaintenance for Native {
    async fn terminal(&self, id: &SessionId) {
        let before = if profile::clean() {
            0
        } else {
            writes(self.file.database().pool()).await.packs()
        };
        if !profile::clean() {
            rewrite_phase(self.file.database().pool(), true).await;
        }
        let s2_profile = forge_agent_host::session_journal::profile::timer("host.s2", 0);
        for (_, domain) in self.domains() {
            self.raw.post_terminal(&domain, id).await.unwrap();
        }
        drop(s2_profile);
        if !profile::clean() {
            rewrite_phase(self.file.database().pool(), false).await;
        }
        self.maintenance.lock().unwrap().s2_pack_bytes += if profile::clean() {
            0
        } else {
            writes(self.file.database().pool()).await.packs()
        } - before;
        if self.per_turn.load(Ordering::Relaxed) {
            self.collect().await;
        }
        let total = if profile::clean() {
            0
        } else {
            writes(self.file.database().pool()).await.packs()
        };
        self.maintenance
            .lock()
            .unwrap()
            .turn_cumulative_pack_bytes
            .push(total);
    }
    async fn sample_wal(&self) -> u64 {
        if profile::clean() {
            0
        } else {
            wal(self.file.database().pool()).await
        }
    }
}
#[derive(Debug)]
struct Legacy {
    file: FileDb,
    store: SqliteProtectedRuntimeStore,
    saves: Mutex<(u64, u64)>,
    turn_bytes: Mutex<Vec<u64>>,
}
#[async_trait]
impl SessionStore for Legacy {
    async fn load(&self, id: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        SessionStore::load(&self.store, id).await
    }
    async fn save(&self, s: &SessionSnapshot) -> Result<(), RuntimeError> {
        self.store.save_legacy_snapshot_baseline(s).await?;
        self.saves.lock().unwrap().0 += 1;
        Ok(())
    }
}
#[async_trait]
impl CheckpointStore for Legacy {
    async fn load_latest(&self, id: &SessionId) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        CheckpointStore::load_latest(&self.store, id).await
    }
    async fn save(&self, c: &TurnCheckpoint) -> Result<(), RuntimeError> {
        self.store.save_legacy_checkpoint_baseline(c).await?;
        self.saves.lock().unwrap().1 += 1;
        Ok(())
    }
}
#[async_trait]
impl TerminalMaintenance for Legacy {
    async fn terminal(&self, _: &SessionId) {
        let total = if profile::clean() {
            0
        } else {
            writes(self.file.database().pool()).await.legacy_bytes as u64
        };
        self.turn_bytes.lock().unwrap().push(total);
    }
    async fn sample_wal(&self) -> u64 {
        if profile::clean() {
            0
        } else {
            wal(self.file.database().pool()).await
        }
    }
}
fn open(label: &str) -> JournalOpen {
    JournalOpen {
        root: JournalRootSelection::Latest,
        expected_revision: None,
        operation: JournalOperationId::new(label),
        pin: JournalPinId::new(label),
        intent: JournalOpenIntent::ReadOnly,
    }
}
fn limits() -> JournalReadLimits {
    JournalReadLimits {
        objects: 1_000_000,
        encoded_bytes: 512 * 1024 * 1024,
        entries: 1_000_000,
    }
}
async fn view(engine: &Engine, id: &SessionId) -> SessionSnapshot {
    let lease = engine.open(id, open("view")).await.unwrap();
    let snapshot = JournalReader::new(engine, &lease, limits())
        .unwrap()
        .snapshot()
        .await
        .unwrap()
        .unwrap();
    engine.close(lease).await.unwrap();
    snapshot
}
fn receipt_json(native: &Native) -> Value {
    let r = native.meter.receipt.lock().unwrap();
    json!({"reclaimed_objects":r.reclaimed_objects,"reclaimed_bytes":r.reclaimed_bytes})
}
/// Physical retained canonical bytes of the measured session. Packs are
/// session-owned and shared by both domains, and each domain's diagnostic
/// covers every pack of the sessions it participates in, so the per-domain
/// values overlap: both domains participate in the one fixture session.
fn retained(stats: &Value) -> u64 {
    stats
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["retained"].as_u64().unwrap())
        .max()
        .unwrap_or(0)
}
async fn byte_stats(native: &Native) -> Value {
    let mut stats = vec![];
    for (_, domain) in native.domains() {
        let s = native.raw.byte_stats(&domain).await.unwrap();
        stats.push(json!({"role":domain.role,"retained":s.retained_bytes,"live":s.live_bytes,"deferred":s.deferred_bytes,"skipped":s.skipped_sessions}));
    }
    json!(stats)
}
async fn replay(native: &Native, id: &SessionId) -> Value {
    let operations = native.meter.operations.lock().unwrap().clone();
    let mut rows = vec![];
    for (_, domain) in native.domains() {
        let record = native.raw.load(&domain, id).await.unwrap().unwrap();
        let floor = record.revision.0.saturating_sub(64);
        let mut retained = vec![];
        let mut pruned = vec![];
        let current_last = operations
            .iter()
            .filter(|(d, s, op)| {
                d == &domain && s == id && op.result.writer_epoch == record.writer_epoch
            })
            .map(|(_, _, op)| op.revision.0)
            .max();
        for (d, s, op) in &operations {
            if d != &domain || s != id {
                continue;
            }
            let actual = native.raw.operation(d, s, &op.operation).await.unwrap();
            let keep = op.revision.0 >= floor || Some(op.revision.0) == current_last;
            assert_eq!(
                actual.is_some(),
                keep,
                "inclusive replay floor/current epoch"
            );
            if let Some(actual) = actual {
                assert_eq!(&actual, op);
                retained.push(op.revision.0);
            } else {
                pruned.push(op.revision.0);
            }
        }
        if record.revision.0 > 65 {
            assert!(retained.contains(&floor));
            assert!(!retained.contains(&(floor - 1)));
        }
        rows.push(json!({"role":domain.role,"revision":record.revision.0,"writer_epoch":record.writer_epoch,"floor":floor,"current_epoch_last":current_last,"retained_operations":retained.len(),"pruned_operations":pruned.len(),"floor_kept":retained.contains(&floor),"floor_minus_one_pruned":pruned.contains(&floor.saturating_sub(1))}));
    }
    json!(rows)
}
async fn cold_warm(file: &FileDb, id: &SessionId) -> Value {
    let raw = Arc::new(ForgeJournalStorage::new(file.reopen().await.unwrap(), [7; 32], 1).unwrap());
    assert_eq!(raw.cache_stats().unwrap().index_decodes, 0);
    let meter = Arc::new(metered::Metered::new(raw.clone()));
    let owner = SessionJournals::with_test_memo_budget(meter.clone(), JOURNAL_MEMO_BYTES);
    let mut samples = vec![];
    for (role, engine) in [
        ("protected", owner.protected()),
        ("ordinary", owner.ordinary()),
    ] {
        let mut hits = 0;
        let mut misses = 0;
        for phase in ["cold", "warm"] {
            profile::phase(&format!("{phase}.{role}"), 0);
            meter.take();
            let start = Instant::now();
            let lease = engine.open(id, open(phase)).await.unwrap();
            let us = start.elapsed().as_micros() as u64;
            let reads = meter.take();
            if reads == 0 {
                hits += 1;
            } else {
                misses += 1;
            }
            engine.close(lease).await.unwrap();
            samples.push(json!({"role":role,"phase":phase,"validation_us":us,"storage_object_reads":reads,"observed_root_hit":reads==0}));
            if phase == "cold" {
                assert!(reads > 0);
            }
        }
        samples.push(json!({"role":role,"observed_open_hits":hits,"observed_open_misses":misses}));
    }
    json!({"samples":samples,"cache":cache_json(&raw),"memo_budget_per_engine":JOURNAL_MEMO_BYTES,"memo_charge_bytes":null,"memo_heap_bytes":null,"internal_hits":null,"internal_misses":null,"internal_evictions":null,"internal_oversize":null,"unavailable_reason":"v0.9.0 keeps RootMemo private and provides no telemetry API; observed open hits/misses are storage-read classification, not internal lookup counters","configured_charge_headroom_1_24_bytes":JOURNAL_MEMO_BYTES as f64*1.24,"peak_process_rss_bytes":rss_bytes()})
}
fn resume_runtime<S: SessionStore + CheckpointStore + 'static>(
    store: Arc<S>,
) -> agent_runtime::runtime::Runtime {
    agent_runtime::runtime::RuntimeBuilder::new(agent_runtime::core::provider::ModelId::new("fake"))
        .provider(Arc::new(agent_runtime::provider::fake::FakeProvider::new(
            "fake",
            agent_runtime::core::provider::Capabilities::basic_streaming(),
            vec![],
        )))
        .model_profile(
            agent_runtime::core::catalog::ResolvedModelProfile::explicit(
                "fake",
                agent_runtime::core::provider::ModelId::new("fake"),
                agent_runtime::core::catalog::ModelLimits::new(4_000_000, 4_000_000, 4096),
            ),
        )
        .session_store(store.clone())
        .checkpoint_store(store)
        .build()
        .unwrap()
}
async fn copy_legacy_row(row: &sqlx::sqlite::SqliteRow, pool: &sqlx::SqlitePool) {
    use sqlx::Row;
    sqlx::query("INSERT INTO protected_agent_session_state (session_id,snapshot_ciphertext,snapshot_nonce,checkpoint_ciphertext,checkpoint_nonce,checkpoint_turn_id,checkpoint_revision,checkpoint_fingerprint,key_revision,state_revision,updated_at,lcm_policy_revision,snapshot_digest,checkpoint_lcm_policy_revision) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
    .bind(row.get::<String,_>("session_id"))
    .bind(row.get::<Option<Vec<u8>>,_>("snapshot_ciphertext")).bind(row.get::<Option<Vec<u8>>,_>("snapshot_nonce"))
    .bind(row.get::<Option<Vec<u8>>,_>("checkpoint_ciphertext")).bind(row.get::<Option<Vec<u8>>,_>("checkpoint_nonce"))
    .bind(row.get::<Option<String>,_>("checkpoint_turn_id")).bind(row.get::<Option<i64>,_>("checkpoint_revision")).bind(row.get::<Option<String>,_>("checkpoint_fingerprint"))
    .bind(row.get::<i64,_>("key_revision")).bind(row.get::<i64,_>("state_revision")).bind(row.get::<String,_>("updated_at"))
    .bind(row.get::<Option<String>,_>("lcm_policy_revision")).bind(row.get::<Option<String>,_>("snapshot_digest")).bind(row.get::<Option<String>,_>("checkpoint_lcm_policy_revision"))
    .execute(pool).await.unwrap();
}
async fn measure(name: &str, n: usize, tools: bool, identity: bool) -> Value {
    let id = SessionId::new(format!(
        "task-27-{}-{n}",
        if tools { "tool" } else { "plain" }
    ));
    let (legacy_file, legacy_empty) = setup(&id).await;
    let legacy = Arc::new(Legacy {
        store: SqliteProtectedRuntimeStore::new(legacy_file.database(), [7; 32], 1),
        file: legacy_file.clone(),
        saves: Mutex::new((0, 0)),
        turn_bytes: Mutex::new(vec![]),
    });
    let legacy_drive = fixture::drive(legacy.clone(), name, n, tools, id.clone()).await;
    let legacy_writes = writes(legacy_file.database().pool()).await;
    let blobs=sqlx::query_as::<_,(i64,i64,i64,i64)>("SELECT length(snapshot_ciphertext),length(checkpoint_ciphertext),length(snapshot_nonce),length(checkpoint_nonce) FROM protected_agent_session_state").fetch_one(legacy_file.database().pool()).await.unwrap();
    assert!(blobs.0 > 16 && blobs.1 > 16);
    assert_eq!((blobs.2, blobs.3), (24, 24));
    let legacy_l2 = size(&legacy_file, legacy_empty, true).await;
    // L1 is an explicit checkpoint-only diagnostic, never a gate denominator.
    let snapshot = SessionStore::load(&legacy.store, &id)
        .await
        .unwrap()
        .unwrap();

    let row = sqlx::query("SELECT * FROM protected_agent_session_state")
        .fetch_one(legacy_file.database().pool())
        .await
        .unwrap();
    sqlx::query(
        "UPDATE protected_agent_session_state SET snapshot_ciphertext=NULL,snapshot_nonce=NULL",
    )
    .execute(legacy_file.database().pool())
    .await
    .unwrap();
    let legacy_l1 = size(&legacy_file, legacy_empty, true).await;
    use sqlx::Row;
    sqlx::query("UPDATE protected_agent_session_state SET snapshot_ciphertext=?,snapshot_nonce=?")
        .bind(row.get::<Vec<u8>, _>("snapshot_ciphertext"))
        .bind(row.get::<Vec<u8>, _>("snapshot_nonce"))
        .execute(legacy_file.database().pool())
        .await
        .unwrap();
    let (file, empty) = setup(&id).await;
    assert_eq!(empty, legacy_empty, "common setup accounted identically");
    let native = Arc::new(Native::new(file.clone(), identity));
    let native_drive = fixture::drive(native.clone(), name, n, tools, id.clone()).await;
    assert_eq!(native_drive.snapshot.history, legacy_drive.snapshot.history);
    let published_ordinary = view(&native.ordinary, &id).await;
    assert!(
        published_ordinary.history == native_drive.snapshot.history,
        "published history preserved"
    );
    assert!(
        ForgeJournalProjection
            .project(published_ordinary.clone())
            .unwrap()
            == published_ordinary,
        "production projection is already applied"
    );
    let identity_same = IdentityJournalProjection
        .project(published_ordinary.clone())
        .unwrap()
        == published_ordinary;
    let receipt_before = receipt_json(&native);
    let before_stats = byte_stats(&native).await;
    let before = size(&file, empty, true).await;
    native.collect().await;
    let final_sweep_wal = wal(file.database().pool()).await;
    let after_stats = byte_stats(&native).await;
    let r = receipt_json(&native);
    assert_eq!(
        retained(&before_stats) - retained(&after_stats),
        r["reclaimed_bytes"].as_u64().unwrap()
            - receipt_before["reclaimed_bytes"].as_u64().unwrap()
    );
    let after = size(&file, empty, true).await;
    let native_writes = writes(file.database().pool()).await;
    let receipt_after_collection = receipt_json(&native);
    let maintenance_after_collection =
        serde_json::to_value(&*native.maintenance.lock().unwrap()).unwrap();
    let replay_evidence = replay(&native, &id).await;
    let cold = cold_warm(&file, &id).await;
    let cache = cache_json(&native.raw);
    let max_write_hold = native.raw.max_write_hold_us_for_test();
    let db = file.database();
    let probe_start = Instant::now();
    let flags = db
        .session_persistence_flags(&format!("journal-fixture:{}", id.as_str()))
        .await
        .unwrap();
    let scalar_probe_us = probe_start.elapsed().as_micros() as u64;
    assert!(flags.saved && flags.checkpoint_present);
    let native_only_ratio = ratio(after.physical_bytes, legacy_l2.physical_bytes);
    // Copy the exact retained L2 row through SQL values, then measure the actual
    // combined file. Do not add two page totals and call that a measured peak.
    copy_legacy_row(&row, file.database().pool()).await;
    let overlap = size(&file, empty, true).await;
    let legacy_runtime = resume_runtime(legacy.clone());
    let resume_start = Instant::now();
    let resumed = legacy_runtime
        .start_session(agent_runtime::runtime::StartSession::resume(id.clone()))
        .await
        .unwrap();
    let legacy_resume_us = resume_start.elapsed().as_micros() as u64;
    drop(resumed);
    drop(legacy_runtime);
    let importer = Arc::new(SqliteProtectedRuntimeStore::new(
        legacy_file.database(),
        [7; 32],
        1,
    ));
    let import = resume_runtime(importer);
    let import_start = Instant::now();
    let imported = import
        .start_session(agent_runtime::runtime::StartSession::resume(id.clone()))
        .await
        .unwrap();
    let import_us = import_start.elapsed().as_micros() as u64;
    assert_eq!(imported.snapshot().history, snapshot.history);
    drop(imported);
    drop(import);
    let legacy_after=sqlx::query("SELECT snapshot_ciphertext,checkpoint_ciphertext,state_revision,updated_at FROM protected_agent_session_state").fetch_one(legacy_file.database().pool()).await.unwrap();
    for column in ["snapshot_ciphertext", "checkpoint_ciphertext"] {
        assert!(
            legacy_after.get::<Option<Vec<u8>>, _>(column) == row.get::<Option<Vec<u8>>, _>(column),
            "import retained legacy bytes"
        );
    }
    assert_eq!(
        legacy_after.get::<i64, _>("state_revision"),
        row.get::<i64, _>("state_revision")
    );
    let legacy_calls = {
        let calls = legacy.saves.lock().unwrap();
        calls.0 + calls.1
    };
    let skipped = legacy_calls - row.get::<i64, _>("state_revision") as u64;
    assert!(skipped > 0, "persist digest/idempotency skips");
    let import_overlap = size(&legacy_file, legacy_empty, true).await;
    for (engine, domain) in native.domains() {
        let record = native.raw.load(&domain, &id).await.unwrap().unwrap();
        engine
            .authorize_retirement(&id, record.revision, Timestamp(101))
            .await
            .unwrap();
        engine.retire(&id, record.revision).await.unwrap();
    }
    native.collect().await;
    let retired_stats = byte_stats(&native).await;
    let retire_receipt = receipt_json(&native);
    assert_eq!(
        retained(&after_stats) - retained(&retired_stats),
        retire_receipt["reclaimed_bytes"].as_u64().unwrap()
            - receipt_after_collection["reclaimed_bytes"]
                .as_u64()
                .unwrap()
    );
    // Remove overlap only for native post-retirement accounting, then restore no
    // payload. The stale legacy row was measured separately above.
    sqlx::query("DELETE FROM protected_agent_session_state")
        .execute(file.database().pool())
        .await
        .unwrap();
    let retired = size(&file, empty, true).await;
    for (engine, _) in native.domains() {
        assert!(engine.open(&id, open("retired")).await.is_err());
    }
    let native_first = native
        .maintenance
        .lock()
        .unwrap()
        .turn_cumulative_pack_bytes[0];
    let legacy_first = legacy.turn_bytes.lock().unwrap()[0];
    let delta_native = native_writes.packs() - native_first;
    let delta_legacy = legacy_writes.legacy_bytes as u64 - legacy_first;
    let delta_ratio = ratio(delta_native, delta_legacy);
    let nt = mean(&native_drive.turns_us) + mean(&native_drive.maintenance_us);
    let lt = mean(&legacy_drive.turns_us);
    let native_delta_turn_us =
        native_drive.turns_us.last().unwrap() + native_drive.maintenance_us.last().unwrap();
    let legacy_delta_turn_us = *legacy_drive.turns_us.last().unwrap();
    let perf = json!({"delta_write_ratio":delta_ratio,"delta_native_bytes_after_first_turn":delta_native,"delta_legacy_bytes_after_first_turn":delta_legacy,"delta_definition":"all writes after first terminal, including subsequent S2/collection and final shutdown sweep; cumulative totals also reported","delta_writes_le_10pct":if n>=1000{delta_ratio.map(|r|r<=0.1)}else{None},"native_mean_turn_including_maintenance_us":nt,"legacy_mean_turn_us":lt,"mean_turn_pass":nt<=lt*if n==100{1.1}else{1.0},"native_delta_turn_us":native_delta_turn_us,"native_delta_run_only_us":native_drive.turns_us.last(),"run_only_turn_pass":*native_drive.turns_us.last().unwrap() as f64<=legacy_delta_turn_us as f64*if n==100{1.1}else{1.0},"legacy_delta_turn_us":legacy_delta_turn_us,"turn_pass":native_delta_turn_us as f64<=legacy_delta_turn_us as f64*if n==100{1.1}else{1.0},"scalar_probe_us":scalar_probe_us,"import_us":import_us,"legacy_resume_us":legacy_resume_us,"import_le_2x_resume":import_us<=legacy_resume_us*2,"max_write_transaction_us":max_write_hold});
    let maintenance = serde_json::to_value(&*native.maintenance.lock().unwrap()).unwrap();
    let s2_bytes = maintenance_after_collection["s2_pack_bytes"]
        .as_u64()
        .unwrap();
    let collect_bytes = maintenance_after_collection["collection_pack_bytes"]
        .as_u64()
        .unwrap();
    let commit_pack_bytes = native_writes.packs() - s2_bytes - collect_bytes;
    let native_turn_cumulative = native
        .maintenance
        .lock()
        .unwrap()
        .turn_cumulative_pack_bytes
        .clone();
    let legacy_turn_cumulative = legacy.turn_bytes.lock().unwrap().clone();
    let old_plus_new_peak = overlap.physical_bytes.max(import_overlap.physical_bytes);
    json!({"fixture":name,"n":n,"metering":"TEMP counters on all five SQLx connections; no main-DB or WAL instrumentation pages","tools":tools,"identity_policy_diagnostic":identity,"projection":"ForgeJournalProjection (conservative D3); independent encrypted domain copies","identity_projection_equal_for_fixture":identity_same,"cadence":"S2 + collect after every turn; final shutdown sweep (20,000 releases/call, drain to stable)","runtime_reference_c4000_cadence":"collect-at-end only; no upstream per-turn C4000 claim","legacy_blobs":2,"legacy_save_calls":legacy_calls,"legacy_digest_or_idempotent_skips":skipped,"legacy_row_unchanged_by_import":true,"legacy_ciphertext_lengths":[blobs.0,blobs.1],"legacy_nonce_lengths":[blobs.2,blobs.3],"legacy_empty":legacy_empty,"native_empty":empty,"legacy_l1":legacy_l1,"legacy_l2":legacy_l2,"before_final_collection":before,"after_collection":after,"after_retirement":retired,"actual_old_plus_new":overlap,"uncollected_import_old_plus_new":import_overlap,"runtime_open_observations":*native.opens.lock().unwrap(),"after_l2_ratio":native_only_ratio,"hard_disk_gate":if name=="B" && n==4000 && !identity {native_only_ratio.map(|r|r<=2.0)}else{None},"before_canonical":before_stats,"after_canonical":after_stats,"retired_canonical":retired_stats,"native_writes":native_writes,"commit_pack_bytes":commit_pack_bytes,"geometric_s2_write_amplification":ratio(commit_pack_bytes+s2_bytes,commit_pack_bytes),"all_maintenance_write_amplification":ratio(native_writes.packs(),commit_pack_bytes),"native_per_turn_pack_bytes":deltas(&native_turn_cumulative),"legacy_per_turn_payload_bytes":deltas(&legacy_turn_cumulative),"old_plus_new_peak_physical_bytes":old_plus_new_peak,"storage_cache_heap_bytes":null,"legacy_writes":legacy_writes,"native_drive":native_drive,"legacy_drive":legacy_drive,"final_sweep_wal_bytes":final_sweep_wal,"maintenance":maintenance_after_collection,"maintenance_including_retirement":maintenance,"replay":replay_evidence,"cold_warm":cold,"storage_cache":cache,"performance":perf,"physical_receipts_after_retirement":receipt_json(&native),"physical_receipts_after_collection":receipt_after_collection,"storage_object_reads":native.meter.reads(),"admitted_object_submissions":native.meter.admitted_objects.load(Ordering::Relaxed),"admitted_canonical_byte_submissions":native.meter.admitted_bytes.load(Ordering::Relaxed),"peak_process_rss_bytes":rss_bytes()})
}
fn deltas(cumulative: &[u64]) -> Vec<u64> {
    let mut previous = 0;
    cumulative
        .iter()
        .map(|&n| {
            let delta = n - previous;
            previous = n;
            delta
        })
        .collect()
}
fn output() -> PathBuf {
    std::env::var_os("FORGE_JOURNAL_MEASUREMENT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("forge-session-journal-measurement"))
}
fn markdown(rows: &[Value]) -> String {
    use std::fmt::Write;
    let mut out = String::from(
        "Forge production sqlx / real AEAD / within-session domain sharing. All physical bytes subtract the same full-schema/session baseline after checkpoint/VACUUM; file equals dbstat plus verified pointer-map pages. B and C use per-turn S2/collection plus final sweep.\n\n| Fixture | N | Turn | L1 | Actual L2 | Before final collect | After collect | After/L2 | Retired | Actual old+new | Pack writes | Max collect write |\n| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n",
    );
    for r in rows {
        writeln!(
            out,
            "| {}{} | {} | {} | {} | {} | {} | {} | {:.5} | {} | {} | {} | {} |",
            r["fixture"].as_str().unwrap(),
            if r["identity_policy_diagnostic"] == true {
                " identity diagnostic"
            } else {
                ""
            },
            r["n"],
            if r["tools"] == true { "tool" } else { "plain" },
            r["legacy_l1"]["physical_bytes"],
            r["legacy_l2"]["physical_bytes"],
            r["before_final_collection"]["physical_bytes"],
            r["after_collection"]["physical_bytes"],
            r["after_l2_ratio"].as_f64().unwrap_or(f64::NAN),
            r["after_retirement"]["physical_bytes"],
            r["actual_old_plus_new"]["physical_bytes"],
            r["native_writes"]["object_pack_bytes"].as_u64().unwrap()
                + r["native_writes"]["operation_pack_bytes"].as_u64().unwrap(),
            r["maintenance"]["largest_collection_call_write"]
        )
        .unwrap();
    }
    out
}
fn save(rows: &[Value]) {
    let root = output();
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(
        root.join("measurements.json"),
        serde_json::to_vec_pretty(rows).unwrap(),
    )
    .unwrap();
    std::fs::write(root.join("measurements.md"), markdown(rows)).unwrap();
}
#[tokio::test]
async fn packed_n100_smoke() {
    let mut rows = vec![];
    for name in ["A", "B"] {
        for tools in [false, true] {
            rows.push(measure(name, 100, tools, false).await);
        }
    }
    save(&rows);
}
#[tokio::test]
#[ignore = "D10 full production physical report; release only"]
async fn forge_packed_report() {
    if cfg!(debug_assertions) {
        panic!("full report requires --release");
    }
    let mut rows = vec![];
    if std::env::var_os("FORGE_JOURNAL_GATE_RECHECK").is_some() {
        for tools in [false, true] {
            let mut row = measure("B", 4000, tools, false).await;
            row["measurement_scope"] = json!(
                "bounded storage-only gate recheck: B/N=4000 plain and tool, unchanged fixtures/seeds/cadence"
            );
            eprintln!(
                "bounded B/4000/tools={tools}: ratio={} perf={}",
                row["after_l2_ratio"], row["performance"]
            );
            rows.push(row);
            save(&rows);
        }
        return;
    }
    for name in ["A", "B", "C"] {
        let sizes: &[usize] = if name == "C" {
            &[1000, 4000]
        } else {
            &[100, 1000, 4000]
        };
        for &n in sizes {
            let variants: &[bool] = if name == "C" { &[true] } else { &[false, true] };
            for &tools in variants {
                eprintln!("measuring Forge {name}/{n}/tools={tools}");
                let row = measure(name, n, tools, false).await;
                eprintln!(
                    "Forge {name}/{n}/tools={tools}: ratio={} disk={} perf={}",
                    row["after_l2_ratio"], row["hard_disk_gate"], row["performance"]
                );
                rows.push(row);
                save(&rows);
            }
        }
    }
    rows.push(measure("B", 100, false, true).await);
    let supplemental = scoped_and_dirty().await;
    std::fs::write(
        output().join("scoped-and-dirty.json"),
        serde_json::to_vec_pretty(&supplemental).unwrap(),
    )
    .unwrap();
    save(&rows);
}

/// Observe validation reads without exposing or decoding runtime memo state.
#[derive(Debug)]
struct Observed {
    inner: Engine,
    meter: Arc<metered::Metered<ForgeJournalStorage>>,
    samples: Arc<Mutex<Vec<Value>>>,
}
#[async_trait]
impl SessionJournal for Observed {
    fn storage_id(&self) -> JournalStorageId {
        self.inner.storage_id()
    }
    async fn open(&self, s: &SessionId, r: JournalOpen) -> Result<JournalLease, RuntimeError> {
        let _profile = forge_agent_host::session_journal::profile::timer("engine.open", 0);
        let before = self.meter.reads();
        let start = Instant::now();
        let result = self.inner.open(s, r).await;
        let reads = self.meter.reads() - before;
        self.samples.lock().unwrap().push(json!({"storage_id":self.inner.storage_id(),"validation_us":start.elapsed().as_micros() as u64,"object_reads":reads,"observed_root_hit":reads==0,"success":result.is_ok()}));
        result
    }
    async fn read(&self, l: &JournalLease, id: &JournalObjectId) -> Result<Vec<u8>, RuntimeError> {
        let _profile = forge_agent_host::session_journal::profile::timer("engine.read", 0);
        self.inner.read(l, id).await
    }
    fn begin_read<'a>(
        &'a self,
        lease: &'a JournalLease,
    ) -> Result<Option<Box<dyn JournalReadOperation + 'a>>, RuntimeError> {
        self.inner.begin_read(lease)
    }
    async fn commit(
        &self,
        l: &JournalLease,
        c: JournalCommit,
    ) -> Result<JournalHead, RuntimeError> {
        let _profile = forge_agent_host::session_journal::profile::timer("engine.commit", 0);
        self.inner.commit(l, c).await
    }
    async fn renew(&self, l: &JournalLease) -> Result<JournalLease, RuntimeError> {
        let _profile = forge_agent_host::session_journal::profile::timer("engine.renew", 0);
        self.inner.renew(l).await
    }
    async fn close(&self, l: JournalLease) -> Result<(), RuntimeError> {
        let _profile = forge_agent_host::session_journal::profile::timer("engine.close", 0);
        self.inner.close(l).await
    }
    async fn retire(
        &self,
        s: &SessionId,
        r: JournalRevision,
    ) -> Result<JournalRetirement, RuntimeError> {
        let _profile = forge_agent_host::session_journal::profile::timer("engine.retire", 0);
        self.inner.retire(s, r).await
    }
    async fn collect(&self, r: JournalGcRequest) -> Result<JournalGcReport, RuntimeError> {
        let _profile = forge_agent_host::session_journal::profile::timer("engine.collect", 0);
        self.inner.collect(r).await
    }
}

async fn scoped_and_dirty() -> Value {
    let id = SessionId::new("scoped-target");
    let (file, _) = setup(&id).await;
    let native = Arc::new(Native::new(file.clone(), false));
    fixture::drive(native.clone(), "B", 100, false, id.clone()).await;
    native.collect().await;
    native.per_turn.store(false, Ordering::Relaxed);
    for index in 0..100 {
        let other = SessionId::new(format!("unrelated-{index:03}"));
        file.ensure_session(&other).await.unwrap();
        fixture::drive(native.clone(), "A", 0, false, other).await;
    }
    let start = Instant::now();
    let raw = Arc::new(ForgeJournalStorage::new(file.reopen().await.unwrap(), [7; 32], 1).unwrap());
    let construction_us = start.elapsed().as_micros() as u64;
    assert_eq!(raw.cache_stats().unwrap().index_decodes, 0);
    assert_eq!(raw.cache_stats().unwrap().body_decodes, 0);
    let domain = native.domains()[1].1.clone();
    // Packs are session-owned and shared by both domains: hydration decodes
    // exactly the target session's indexes, never an unrelated session's.
    let own_packs = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM agent_session_journal_pack WHERE runtime_session_id=?",
    )
    .bind(id.as_str())
    .fetch_one(file.database().pool())
    .await
    .unwrap() as u64;
    let all_packs = scalar(
        file.database().pool(),
        "SELECT count(*) FROM agent_session_journal_pack",
    )
    .await;
    let start = Instant::now();
    let members = raw.members(&domain, &id).await.unwrap();
    let hydration_us = start.elapsed().as_micros() as u64;
    let cache = raw.cache_stats().unwrap();
    assert_eq!(
        cache.index_decodes, own_packs,
        "no unrelated indexes decoded"
    );
    assert!(cache.charged_bytes <= cache.budget);
    assert!(cache.directory_charged_bytes > 0);
    raw.set_cache_budget(1).unwrap();
    assert_eq!(raw.members(&domain, &id).await.unwrap(), members);
    let oversize = raw.cache_stats().unwrap();
    assert_eq!(oversize.charged_bytes, 0);
    assert!(oversize.oversize > 0);
    raw.set_cache_budget(16 * 1024).unwrap();
    for index in 0..100 {
        raw.members(&domain, &SessionId::new(format!("unrelated-{index:03}")))
            .await
            .unwrap();
    }
    let bounded = raw.cache_stats().unwrap();
    assert!(bounded.charged_bytes <= bounded.budget);
    assert!(bounded.evictions > 0 || bounded.oversize > oversize.oversize);
    let mut dirty = vec![];
    let mut collect = vec![];
    for (engine, domain) in native.domains() {
        let count = native
            .raw
            .dirty_sessions(&domain, None, 1000)
            .await
            .unwrap()
            .len();
        assert!(count >= 100);
        dirty.push(count);
        let start = Instant::now();
        let report = engine
            .collect(JournalGcRequest {
                storage_id: engine.storage_id(),
                expected_root_epoch: engine.root_epoch().unwrap(),
                max_objects: 20_000,
            })
            .await
            .unwrap();
        let us = start.elapsed().as_micros() as u64;
        collect.push(json!({"role":domain.role,"collect_gate_hold_upper_bound_us":us,"le_1second":us<=1_000_000,"report":report}));
    }
    json!({"unrelated_sessions":100,"storage_construction_us":construction_us,"construction_decodes":0,"target_scope_pack_count":own_packs,"all_pack_count":all_packs,"target_members":members.len(),"lazy_hydration_us":hydration_us,"cache_after_target":cache,"one_byte_cache":oversize,"bounded_cache_after_100_scopes":bounded,"dirty_sessions_per_domain":dirty,"collect_100_dirty":collect,"cold_validation_with_unrelated":cold_warm(&file,&id).await,"peak_process_rss_bytes":rss_bytes()})
}

#[path = "support/bench_profile.rs"]
mod profile;
