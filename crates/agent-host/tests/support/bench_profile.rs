//! Profiling controls for the existing gate fixture. This integration target
//! requires test-support; none of these controls exist in the product.
use super::*;
use forge_agent_host::session_journal::profile as timers;
use std::{
    fs::OpenOptions,
    io::Write,
    sync::{OnceLock, atomic::AtomicU8},
};
#[path = "profile_reference/mod.rs"]
mod reference;
static MODE: AtomicU8 = AtomicU8::new(0);
struct Phase {
    case: String,
    name: String,
    turn: usize,
    start: Instant,
}
static PHASE: OnceLock<Mutex<Option<Phase>>> = OnceLock::new();
pub fn clean() -> bool {
    MODE.load(Ordering::Relaxed) == 2
}
fn dir() -> PathBuf {
    std::env::var_os("FORGE_PROFILE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/Volumes/Data/tmp/refactor/c3-profile"))
}
fn flush(previous: Phase) {
    let wall_ns = previous.start.elapsed().as_nanos() as u64;
    let data = timers::take();
    let row = json!({"case":previous.case,"phase":previous.name,"turn":previous.turn,"wall_ns":wall_ns,"metrics":data});
    let mut f = OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir().join("phases.jsonl"))
        .unwrap();
    writeln!(f, "{}", row).unwrap();
}
pub fn phase(name: &str, turn: usize) {
    if MODE.load(Ordering::Relaxed) == 0 {
        return;
    }
    let mut p = PHASE.get_or_init(|| Mutex::new(None)).lock().unwrap();
    let case = p.as_ref().map(|p| p.case.clone()).unwrap_or_default();
    if let Some(old) = p.take() {
        flush(old);
    }
    std::fs::write(
        dir().join("current-phase.json"),
        serde_json::to_vec(&json!({"case":case,"phase":name,"turn":turn,"pid":std::process::id()}))
            .unwrap(),
    )
    .unwrap();
    *p = Some(Phase {
        case,
        name: name.to_owned(),
        turn,
        start: Instant::now(),
    });
}
fn case(name: String, clean: bool) {
    MODE.store(if clean { 2 } else { 1 }, Ordering::Relaxed);
    let mut p = PHASE.get_or_init(|| Mutex::new(None)).lock().unwrap();
    if let Some(old) = p.take() {
        flush(old);
    }
    timers::take();
    timers::enable(true);
    *p = Some(Phase {
        case: name,
        name: "setup".into(),
        turn: 0,
        start: Instant::now(),
    });
}
fn init() {
    if cfg!(debug_assertions) {
        panic!("profile requires release");
    }
    std::fs::create_dir_all(dir()).unwrap();
    std::fs::write(dir().join("test.pid"), std::process::id().to_string()).unwrap();
}
fn finish() {
    phase("done", 0);
    timers::enable(false);
    MODE.store(0, Ordering::Relaxed);
}
async fn remove_measurement_overhead(file: &FileDb) {
    let db = file.database();
    let mut all = vec![];
    for _ in 0..5 {
        all.push(db.pool().acquire().await.unwrap());
    }
    for conn in &mut all {
        for name in [
            "bench_object_INSERT",
            "bench_object_UPDATE",
            "bench_operation_INSERT",
            "bench_operation_UPDATE",
            "bench_legacy_INSERT",
            "bench_legacy_UPDATE",
        ] {
            sqlx::query(&format!("DROP TRIGGER IF EXISTS temp.{name}"))
                .execute(&mut **conn)
                .await
                .unwrap();
        }
        sqlx::query("PRAGMA wal_autocheckpoint=1000")
            .execute(&mut **conn)
            .await
            .unwrap();
    }
}
fn save_row(name: &str, value: &Value) {
    std::fs::write(dir().join(name), serde_json::to_vec_pretty(value).unwrap()).unwrap();
}
async fn one_b(n: usize, tools: bool, without_meter: bool) {
    let shape = if tools { "tool" } else { "plain" };
    let mode = if without_meter { "clean" } else { "gate" };
    let id = SessionId::new(format!("task-27-{shape}-{n}"));
    case(format!("B/{n}/{shape}/{mode}/legacy"), without_meter);
    let (legacy_file, legacy_empty) = setup(&id).await;
    if without_meter {
        remove_measurement_overhead(&legacy_file).await;
    }
    let legacy = Arc::new(Legacy {
        store: SqliteProtectedRuntimeStore::new(legacy_file.database(), [7; 32], 1),
        file: legacy_file.clone(),
        saves: Mutex::new((0, 0)),
        turn_bytes: Mutex::new(vec![]),
    });
    let legacy_drive = fixture::drive(legacy.clone(), "B", n, tools, id.clone()).await;
    let legacy_size = size(&legacy_file, legacy_empty, true).await;
    case(format!("B/{n}/{shape}/{mode}/native"), without_meter);
    let (file, empty) = setup(&id).await;
    assert_eq!(empty, legacy_empty);
    if without_meter {
        remove_measurement_overhead(&file).await;
    }
    let native = Arc::new(Native::new(file.clone(), false));
    let native_drive = fixture::drive(native.clone(), "B", n, tools, id.clone()).await;
    assert_eq!(native_drive.snapshot.history, legacy_drive.snapshot.history);
    phase("final_collect", 0);
    native.collect().await;
    phase("forensics", 0);
    let native_size = size(&file, empty, true).await;
    timers::enable(false);
    let disk = native.raw.profile_disk_for_test().await;
    timers::enable(true);
    timers::take();
    std::fs::copy(
        file.directory().join("journal.db"),
        dir().join(format!("B-{n}-{shape}-{mode}-native.db")),
    )
    .unwrap();
    phase("cold_warm", 0);
    let cold = cold_warm(&file, &id).await;
    phase("legacy_resume", 0);
    let runtime = resume_runtime(legacy.clone());
    let start = Instant::now();
    let s = runtime
        .start_session(agent_runtime::runtime::StartSession::resume(id.clone()))
        .await
        .unwrap();
    let legacy_resume_us = start.elapsed().as_micros() as u64;
    drop(s);
    drop(runtime);
    phase("import", 0);
    let importer = Arc::new(SqliteProtectedRuntimeStore::new(
        legacy_file.database(),
        [7; 32],
        1,
    ));
    let runtime = resume_runtime(importer.clone());
    let start = Instant::now();
    let s = runtime
        .start_session(agent_runtime::runtime::StartSession::resume(id.clone()))
        .await
        .unwrap();
    let import_us = start.elapsed().as_micros() as u64;
    assert_eq!(s.snapshot().history, legacy_drive.snapshot.history);
    drop(s);
    drop(runtime);
    phase("native_warm_resume", 0);
    let runtime = resume_runtime(importer);
    let start = Instant::now();
    let s = runtime
        .start_session(agent_runtime::runtime::StartSession::resume(id.clone()))
        .await
        .unwrap();
    let warm_resume_us = start.elapsed().as_micros() as u64;
    drop(s);
    drop(runtime);
    phase("native_cold_resume", 0);
    let cold_store = Arc::new(SqliteProtectedRuntimeStore::new(
        legacy_file.reopen().await.unwrap(),
        [7; 32],
        1,
    ));
    let runtime = resume_runtime(cold_store);
    let start = Instant::now();
    let s = runtime
        .start_session(agent_runtime::runtime::StartSession::resume(id.clone()))
        .await
        .unwrap();
    let cold_resume_us = start.elapsed().as_micros() as u64;
    drop(s);
    drop(runtime);
    phase("output", 0);
    let row = json!({"n":n,"tools":tools,"mode":mode,"native_drive":native_drive,"legacy_drive":legacy_drive,"native_size":native_size,"legacy_size":legacy_size,"disk":disk,"cold_warm":cold,"legacy_resume_us":legacy_resume_us,"import_us":import_us,"warm_resume_us":warm_resume_us,"cold_resume_us":cold_resume_us,"max_write_us":native.raw.max_write_hold_us_for_test()});
    save_row(&format!("B-{n}-{shape}-{mode}.json"), &row);
    eprintln!(
        "profile B/{n}/{shape}/{mode} native={:?}+{:?} import={import_us} us",
        native_drive.turns_us, native_drive.maintenance_us
    );
}
#[tokio::test]
#[ignore = "offline profiling only, release"]
async fn forge_profile_b() {
    init();
    for clean in [false, true] {
        for n in [1000, 4000] {
            for tools in [false, true] {
                one_b(n, tools, clean).await;
            }
        }
    }
    finish();
}
#[tokio::test]
#[ignore = "offline C/4000 profiling only, release"]
async fn forge_profile_c() {
    init();
    let n = 4000;
    let id = SessionId::new("task-27-tool-4000");
    case("C/4000/tool/gate/native".into(), false);
    let (file, empty) = setup(&id).await;
    let native = Arc::new(Native::new(file.clone(), false));
    let drive = fixture::drive(native.clone(), "C", n, true, id).await;
    phase("final_collect", 0);
    native.collect().await;
    phase("forensics", 0);
    let size = size(&file, empty, true).await;
    timers::enable(false);
    let disk = native.raw.profile_disk_for_test().await;
    timers::enable(true);
    timers::take();
    std::fs::copy(
        file.directory().join("journal.db"),
        dir().join("C-4000-tool-native.db"),
    )
    .unwrap();
    save_row(
        "C-4000-tool.json",
        &json!({"drive":drive,"size":size,"disk":disk,"max_write_us":native.raw.max_write_hold_us_for_test()}),
    );
    finish();
}

type RefEngine = JournalEngine<reference::PackedSqliteJournalStorage>;
#[derive(Debug)]
struct Reference {
    raw: Arc<reference::PackedSqliteJournalStorage>,
    ordinary: RefEngine,
    protected: RefEngine,
}
#[async_trait]
impl SessionStore for Reference {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        Some(Arc::new(self.ordinary.clone()))
    }
    async fn load(&self, _: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        Ok(None)
    }
    async fn save(&self, _: &SessionSnapshot) -> Result<(), RuntimeError> {
        panic!("reference fallback")
    }
}
#[async_trait]
impl CheckpointStore for Reference {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        Some(Arc::new(self.protected.clone()))
    }
    async fn load_latest(&self, _: &SessionId) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        Ok(None)
    }
    async fn save(&self, _: &TurnCheckpoint) -> Result<(), RuntimeError> {
        panic!("reference fallback")
    }
}
impl Reference {
    async fn collect(&self) {
        for engine in [&self.protected, &self.ordinary] {
            let _p = timers::timer("reference.collect", 0);
            engine
                .collect(JournalGcRequest {
                    storage_id: engine.storage_id(),
                    expected_root_epoch: engine.root_epoch().unwrap(),
                    max_objects: 20_000,
                })
                .await
                .unwrap();
        }
    }
}
#[async_trait]
impl TerminalMaintenance for Reference {
    async fn terminal(&self, id: &SessionId) {
        let p = timers::timer("reference.s2", 0);
        self.raw.post_terminal(id).unwrap();
        drop(p);
        self.collect().await;
    }
    async fn sample_wal(&self) -> u64 {
        0
    }
}
fn reference_size(path: &std::path::Path) -> u64 {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE); VACUUM; PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();
    std::fs::metadata(path).unwrap().len()
}
#[tokio::test]
#[ignore = "same Forge fixture over copied upstream reference, release"]
async fn forge_profile_reference() {
    init();
    for n in [1000, 4000] {
        for tools in [false, true] {
            let shape = if tools { "tool" } else { "plain" };
            case(format!("B/{n}/{shape}/reference"), false);
            let path = dir().join(format!("B-{n}-{shape}-reference.db"));
            assert!(!path.exists());
            let raw = Arc::new(reference::PackedSqliteJournalStorage::open(&path).unwrap());
            let empty = reference_size(&path);
            let owner = SessionJournals::with_test_memo_budget(raw.clone(), JOURNAL_MEMO_BYTES);
            let store = Arc::new(Reference {
                raw,
                ordinary: owner.ordinary(),
                protected: owner.protected(),
            });
            let drive = fixture::drive(
                store.clone(),
                "B",
                n,
                tools,
                SessionId::new(format!("task-27-{shape}-{n}")),
            )
            .await;
            phase("final_collect", 0);
            store.collect().await;
            phase("forensics", 0);
            let bytes = reference_size(&path) - empty;
            save_row(
                &format!("B-{n}-{shape}-reference.json"),
                &json!({"n":n,"tools":tools,"drive":drive,"physical_bytes":bytes,"empty":empty,"pack_writes":store.raw.pack_write_bytes().unwrap()}),
            );
        }
    }
    finish();
}

pub fn collect_sample(
    role: &str,
    elapsed_us: u64,
    before: std::collections::BTreeMap<String, timers::Metric>,
) {
    if MODE.load(Ordering::Relaxed) == 0 {
        return;
    }
    let p = PHASE.get_or_init(|| Mutex::new(None)).lock().unwrap();
    let Some(p) = p.as_ref() else {
        return;
    };
    let after = timers::snapshot();
    let mut delta = std::collections::BTreeMap::new();
    for (key, mut value) in after {
        if let Some(old) = before.get(&key) {
            value.calls -= old.calls;
            value.bytes -= old.bytes;
            value.ns -= old.ns;
        }
        if value.calls != 0 {
            delta.insert(key, value);
        }
    }
    let row = json!({"case":p.case,"phase":p.name,"turn":p.turn,"role":role,"elapsed_us":elapsed_us,"metrics":delta});
    let mut f = OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir().join("collect.jsonl"))
        .unwrap();
    writeln!(f, "{}", row).unwrap();
}

#[test]
#[ignore = "release-only profiling-counter calibration; run alone"]
fn timer_overhead_profile() {
    init();
    let iterations = 200_000u64;
    let mut rows = Vec::new();
    for enabled in [false, true] {
        timers::take();
        timers::enable(true);
        for index in 0..80 {
            timers::event(&format!("calibration-{index:02}"), 0);
        }
        timers::enable(enabled);
        let start = Instant::now();
        for _ in 0..iterations {
            drop(timers::timer(std::hint::black_box("codec.validate"), 0));
        }
        let elapsed_ns = start.elapsed().as_nanos() as u64;
        timers::enable(false);
        let data = timers::take();
        let observations = data.get("codec.validate").map_or(0, |m| m.calls);
        assert_eq!(observations, if enabled { iterations } else { 0 });
        rows.push(json!({"enabled":enabled,"iterations":iterations,"elapsed_ns":elapsed_ns,"ns_per_call":elapsed_ns as f64/iterations as f64,"observations":observations}));
    }
    timers::enable(true);
    for index in 0..80 {
        timers::event(&format!("calibration-{index:02}"), 0);
    }
    let start = Instant::now();
    for _ in 0..iterations {
        timers::record(std::hint::black_box("rt.memo.get"), 0, 0);
    }
    let callback_ns = start.elapsed().as_nanos() as u64;
    timers::enable(false);
    let data = timers::take();
    assert_eq!(data["rt.memo.get"].calls, iterations);
    save_row(
        "timer-overhead.json",
        &json!({"timer":rows,"callback_ns_per_call":callback_ns as f64/iterations as f64,"scope":"empty bookkeeping only; not a bound on cache/scheduling perturbation"}),
    );
    eprintln!(
        "timer calibration {}",
        serde_json::to_string(&rows).unwrap()
    );
}
