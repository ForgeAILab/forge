use super::*;
use executors::gc::{FreeFloor, GcReport, GC_DIR};
use std::time::SystemTime;

const HOUR: Duration = Duration::from_secs(60 * 60);

async fn sweep_at(
    backend: &DaemonWorkspaceBackend,
    active: &[String],
    after: Duration,
) -> GcReport {
    backend
        .gc_sweep_at(active, SystemTime::now() + after, FreeFloor::default())
        .await
}

fn quarantined(root: &Path) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(root.join(GC_DIR)) else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter(|name| name.starts_with("workspace-"))
        .collect()
}

fn backend_on(root: &Path, journal: &Path) -> DaemonWorkspaceBackend {
    DaemonWorkspaceBackend::new(
        root.to_owned(),
        "daemon-1".into(),
        crate::daemon_config::DaemonConfig::default().run_policy(),
        Arc::new(DaemonJournal::new(journal)),
    )
    .unwrap()
}

/// A handle directory Forge made, with build output, and no handle.
fn forge_made_orphan(root: &Path) -> PathBuf {
    let orphan = root
        .join(WORKTREE_DIRECTORY)
        .join(format!("workspace-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(orphan.join(".forge-task/build/cargo")).unwrap();
    std::fs::write(orphan.join(".forge-task/build/cargo/marker"), "x").unwrap();
    orphan
}

#[tokio::test]
async fn a_daemon_whose_state_was_lost_never_sweeps_the_root_the_old_state_owns() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("root");
    let (old_state, new_state) = (dir.path().join("old"), dir.path().join("new"));
    for path in [&root, &old_state, &new_state] {
        std::fs::create_dir_all(path).unwrap();
    }
    let root = root.canonicalize().unwrap();
    // The first daemon adopts the root; its directories are live to it.
    let first = backend_on(&root, &old_state);
    assert!(first.gc_lock.is_some());
    assert_eq!(
        gc::disk_report(&root)
            .and_then(|facts| facts.gc_state)
            .as_deref(),
        Some("owned"),
        "the daemon reports that its collector runs on this root"
    );
    let live = forge_made_orphan(&root);
    drop(first);

    // Same root, empty handle table: every directory is unknown to it.
    let amnesiac = backend_on(&root, &new_state);
    assert!(amnesiac.gc_lock.is_none());
    let report = sweep_at(&amnesiac, &[], 30 * 24 * HOUR).await;
    assert_eq!(report, GcReport::default());
    assert!(live.join(".forge-task/build/cargo/marker").exists());
    assert!(quarantined(&root).is_empty());

    // The old state, back again, still owns it and sweeps.
    drop(amnesiac);
    let back = backend_on(&root, &old_state);
    assert!(back.gc_lock.is_some());
    assert_eq!(sweep_at(&back, &[], 30 * 24 * HOUR).await.quarantined, 1);
}

#[tokio::test]
async fn a_root_that_is_a_repository_is_never_adopted_or_swept() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    std::fs::create_dir_all(root.join(".git")).unwrap();
    let orphan = forge_made_orphan(&root);
    let backend = backend_on(&root, &root);
    assert!(backend.gc_lock.is_none());
    assert!(!root.join(GC_DIR).exists());
    assert_eq!(
        sweep_at(&backend, &[], 30 * 24 * HOUR).await,
        GcReport::default()
    );
    assert!(orphan.exists());
}

#[tokio::test]
async fn build_output_is_evicted_under_the_floor_but_never_for_a_busy_handle() {
    let fixture = Fixture::new().await;
    let handle = fixture.prepared.workspace.workspace_handle.clone();
    let task_root = fixture.path().parent().unwrap().to_path_buf();
    let build = task_root.join(".forge-task/build");
    std::fs::create_dir_all(build.join("cargo")).unwrap();
    std::fs::write(build.join("cargo/marker"), "x").unwrap();
    let execution = "exec-busy".to_owned();
    fixture
        .backend
        .state
        .lock()
        .unwrap()
        .handles
        .get_mut(&handle)
        .unwrap()
        .execution_ids
        .push(execution.clone());
    let under = FreeFloor::of_bytes(u64::MAX, 100);
    let sweep = |active: Vec<String>| {
        let backend = &fixture.backend;
        async move { backend.gc_sweep_at(&active, SystemTime::now(), under).await }
    };

    // An execution of the handle is running.
    assert_eq!(sweep(vec![execution.clone()]).await.builds_evicted, 0);
    assert!(build.join("cargo/marker").exists());
    // A hook of this process is running in it.
    let hook = executors::sandbox::SandboxEnv::for_command(
        fixture.path(),
        executors::sandbox::RunPurpose::Hook,
    );
    if hook.env().tmp_dir().is_some() {
        assert_eq!(sweep(Vec::new()).await.builds_evicted, 0);
        assert!(build.join("cargo/marker").exists());
    }
    drop(hook);
    // Plenty of room: nothing goes even when idle.
    let report = fixture
        .backend
        .gc_sweep_at(&[], SystemTime::now(), FreeFloor::of_bytes(0, 0))
        .await;
    assert_eq!(report.builds_evicted, 0);
    // Idle and under the floor.
    assert_eq!(sweep(Vec::new()).await.builds_evicted, 1);
    assert!(!build.exists());
    assert!(fixture.path().join("README.md").exists());
}

#[tokio::test]
async fn gc_quarantines_a_directory_without_a_handle_and_deletes_it_a_day_later() {
    let fixture = Fixture::new().await;
    let root = fixture.dir.path().canonicalize().unwrap();
    let roots = root.join(WORKTREE_DIRECTORY);
    let orphan = roots.join(format!("workspace-{}", uuid::Uuid::new_v4()));
    let kept = [
        roots.join("notes"),
        roots.join("workspace-not-a-handle"),
        root.join(".forge/probes/keep"),
        root.join("user-repo"),
    ];
    for path in kept.iter().chain([&orphan.join("repo")]) {
        std::fs::create_dir_all(path).unwrap();
        std::fs::write(path.join("file"), "content").unwrap();
    }
    // What makes it a directory Forge made, and not a user's.
    std::fs::create_dir_all(orphan.join(".forge-task")).unwrap();
    // A handle-shaped directory with nothing of Forge's in it is a user's.
    let users = roots.join(format!("workspace-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(users.join("photos")).unwrap();

    // Made a moment ago: a prepare may be about to record it.
    assert_eq!(
        sweep_at(&fixture.backend, &[], Duration::ZERO)
            .await
            .quarantined,
        0
    );
    assert!(orphan.exists());

    // Eleven hours old: a slow clone may still be filling it.
    assert_eq!(
        sweep_at(&fixture.backend, &[], 11 * HOUR).await.quarantined,
        0
    );
    assert!(orphan.exists());

    let report = sweep_at(&fixture.backend, &[], 25 * HOUR).await;
    assert_eq!(
        (report.quarantined, report.removed, report.errors),
        (1, 0, 0)
    );
    assert!(users.join("photos").exists());
    assert!(!orphan.exists());
    assert_eq!(quarantined(&root).len(), 1);
    // The prepared workspace and everything that is not handle-shaped stay.
    assert!(fixture.path().join("README.md").exists());
    for path in &kept {
        assert!(path.join("file").exists(), "{} was touched", path.display());
    }

    assert_eq!(sweep_at(&fixture.backend, &[], 48 * HOUR).await.removed, 0);
    assert_eq!(sweep_at(&fixture.backend, &[], 50 * HOUR).await.removed, 1);
    assert!(quarantined(&root).is_empty());
    assert!(users.join("photos").exists());
    assert!(fixture.path().join("README.md").exists());
}

#[tokio::test]
async fn second_backend_on_the_same_root_does_not_sweep_the_first_ones_live_directories() {
    let fixture = Fixture::new().await;
    // A second runtime on the same root loads the table as it is now.
    let second = DaemonWorkspaceBackend::new(
        fixture.dir.path().to_owned(),
        "daemon-1".into(),
        crate::daemon_config::DaemonConfig::default().run_policy(),
        Arc::new(DaemonJournal::new(fixture.dir.path())),
    )
    .unwrap();
    // The first one then prepares a workspace the second never heard of...
    let mut fence = fence("prepare-2", 1, &fixture.prepared.workspace.base_sha);
    fence.placement_id = "placement-2".into();
    let later: WorkspacePrepareResult = serde_json::from_value(
        fixture
            .backend
            .handle(
                METHOD_WORKSPACE_PREPARE,
                serde_json::to_value(WorkspacePrepareParams {
                    fence,
                    repo_location_id: "location-1".into(),
                    workspace_id: "workspace-2".into(),
                    task_id: "task-2".into(),
                    base_ref: "main".into(),
                    branch: "task/second".into(),
                })
                .unwrap(),
                Vec::new,
            )
            .await
            .unwrap(),
    )
    .unwrap();
    let later_path = PathBuf::from(&later.workspace.workspace_path);
    // ...and runs a hook in the first one.
    let hook = executors::sandbox::SandboxEnv::for_command(
        fixture.path(),
        executors::sandbox::RunPurpose::Hook,
    );

    // Only one backend holds the root's sweeper lock.
    assert!(fixture.backend.gc_lock.is_some());
    assert!(second.gc_lock.is_none());

    // Long after any grace period or run age, the second backend sweeps.
    let report = sweep_at(&second, &[], 3 * 24 * HOUR).await;

    assert_eq!(report, GcReport::default());
    assert!(later_path.join("README.md").exists());
    assert!(fixture.path().join("README.md").exists());
    if let Some(tmp) = hook.env().tmp_dir() {
        assert!(tmp.exists(), "a live run lost its temp directory");
    }
    // Constructing yet another backend (its start-up sweep) spares it too.
    DaemonWorkspaceBackend::new(
        fixture.dir.path().to_owned(),
        "daemon-1".into(),
        crate::daemon_config::DaemonConfig::default().run_policy(),
        Arc::new(DaemonJournal::new(fixture.dir.path())),
    )
    .unwrap();
    if let Some(tmp) = hook.env().tmp_dir() {
        assert!(tmp.exists(), "a live run lost its temp directory");
    }
}

#[tokio::test]
async fn gc_removes_cleaned_handle_leftovers_and_the_legacy_codex_home_only_when_idle() {
    let fixture = Fixture::new().await;
    let root = fixture.dir.path().canonicalize().unwrap();
    let handle = fixture.prepared.workspace.workspace_handle.clone();
    let task_root = fixture.path().parent().unwrap().to_path_buf();
    fixture
        .backend
        .handle(
            METHOD_WORKSPACE_CLEANUP,
            serde_json::to_value(WorkspaceCleanupParams {
                fence: fence("gc-cleanup", 1, &fixture.prepared.workspace.base_sha),
                workspace_handle: handle.clone(),
            })
            .unwrap(),
            Vec::new,
        )
        .await
        .unwrap();
    assert!(!task_root.exists());
    // A late writer left something in the cleaned handle's directory.
    std::fs::create_dir_all(task_root.join(".forge-outbox/late")).unwrap();
    std::fs::write(task_root.join(".forge-outbox/late/result.json"), "{}").unwrap();
    let legacy = root.join(".forge-daemon/execution-logs/.codex-managed-home");
    std::fs::create_dir_all(legacy.join("task-scratch")).unwrap();
    let log = root.join(".forge-daemon/execution-logs/execution.jsonl");
    std::fs::write(&log, "log").unwrap();

    // Something is running: the shared legacy home may still be in use.
    let report = sweep_at(&fixture.backend, &["exec-1".to_owned()], Duration::ZERO).await;
    assert_eq!((report.removed, report.errors), (1, 0));
    assert!(!task_root.exists());
    assert!(legacy.exists());

    let report = sweep_at(&fixture.backend, &[], Duration::ZERO).await;
    assert_eq!((report.removed, report.errors), (1, 0));
    assert!(!legacy.exists());
    assert!(log.exists());
}

/// The report of a daemon carries the disk facts of its workspace root and
/// whether its collector runs there; the floor comes from the server.
#[test]
fn disk_report_reads_the_root_and_says_whether_its_collector_runs() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let report = gc::disk_report(&root).expect("a real directory is readable");
    assert!(report.total_bytes >= report.free_bytes && report.total_bytes > 0);
    assert!(chrono::DateTime::parse_from_rfc3339(&report.measured_at).is_ok());
    // Nobody adopted this root: the server is told nothing collects here.
    assert_eq!(report.gc_state.as_deref(), Some("unclaimed"));
    // Unreadable: no facts, and the server then refuses nothing for disk.
    assert!(gc::disk_report(&root.join("no/such/place")).is_none());
    // The wire shape the server parses.
    let wire = serde_json::to_value(api_types::DaemonReportRequest {
        max_concurrent_runs: None,
        disk: Some(report.clone()),
        detected_clis: Vec::new(),
        runtimes: None,
        labels: None,
        active_execution_ids: None,
    })
    .unwrap();
    assert_eq!(wire["disk"]["free_bytes"], report.free_bytes);
    assert_eq!(wire["disk"]["gc_state"], "unclaimed");
}

#[test]
fn collector_floor_is_the_one_the_server_sent() {
    let sent = FreeFloor {
        min_free_bytes: 123,
        min_free_percent: 7,
        min_free_inode_percent: 3,
        gc_free_bytes: Some(456),
        gc_free_percent: None,
    };
    let dir = tempfile::tempdir().unwrap();
    let (first, second) = (dir.path().join("first"), dir.path().join("second"));
    std::fs::create_dir_all(&first).unwrap();
    std::fs::create_dir_all(&second).unwrap();
    // Before the first reply: the built-in default for the collector, and
    // no floor to refuse new work by.
    assert_eq!(gc::floor(&first), FreeFloor::default());
    assert_eq!(gc::server_floor(&first), None);
    // A reply without a floor changes nothing.
    gc::accept_floor(&first, None);
    gc::accept_floor(&first, Some(sent));
    assert_eq!(gc::floor(&first), sent);
    gc::accept_floor(&first, None);
    assert_eq!(gc::floor(&first), sent);
    // However the root is spelled, it is one root.
    assert_eq!(gc::floor(&first.join("../first")), sent);
    // Another backend in the same process (a test, a daemon beside a
    // server) has its own server and never sees this floor.
    assert_eq!(gc::server_floor(&second), None);
    let other = FreeFloor::of_bytes(1, 1);
    gc::accept_floor(&second, Some(other));
    assert_eq!((gc::floor(&first), gc::floor(&second)), (sent, other));
}

/// The daemon's own reading backs the server's admission: a worktree that
/// would have to be made is refused with the typed `disk_pressure` error
/// while the reading is under the floor the server sent; never without a
/// floor, and never when the disk cannot be read.
#[test]
fn new_disk_is_refused_with_a_typed_error_only_under_the_servers_floor() {
    let space = |free: u64| executors::gc::DiskSpace {
        free,
        total: 1_000,
        free_inodes: None,
        total_inodes: None,
    };
    let floor = FreeFloor::of_bytes(100, 0);
    let refusal = gc::disk_pressure_refusal(Some(floor), Some(space(99))).unwrap();
    assert_eq!(refusal.code, api_types::DISK_PRESSURE);
    assert_eq!(refusal.details.unwrap()["kind"], "bytes");
    assert!(gc::disk_pressure_refusal(Some(floor), Some(space(100))).is_none());
    assert!(gc::disk_pressure_refusal(None, Some(space(0))).is_none());
    assert!(gc::disk_pressure_refusal(Some(floor), None).is_none());
}

/// End to end on the daemon: with a floor from its server that its disk is
/// under, `workspace.prepare` of a new worktree is refused `disk_pressure`
/// and records nothing; preparing the worktree that already exists is
/// admitted; and the new one is made once the floor is met.
#[tokio::test]
async fn prepare_of_a_new_worktree_is_refused_under_the_floor_and_an_existing_one_is_not() {
    let fixture = Fixture::new().await;
    let root = fixture.dir.path().to_owned();
    let prepare = |placement: &str, operation: &str, branch: &str| {
        let mut fence = fence(operation, 1, &fixture.prepared.workspace.base_sha);
        fence.placement_id = placement.into();
        fixture.backend.handle(
            METHOD_WORKSPACE_PREPARE,
            serde_json::to_value(WorkspacePrepareParams {
                fence,
                repo_location_id: "location-1".into(),
                workspace_id: format!("workspace-{placement}"),
                task_id: format!("task-{placement}"),
                base_ref: "main".into(),
                branch: branch.into(),
            })
            .unwrap(),
            Vec::new,
        )
    };
    // A floor no disk meets (every byte must be free): deterministic
    // whatever the test machine's disk holds.
    gc::accept_floor(&root, Some(FreeFloor::of_bytes(0, 100)));
    let handles_before = std::fs::read_dir(root.join(WORKTREE_DIRECTORY))
        .unwrap()
        .count();
    let refused = prepare("placement-2", "prepare-2", "task/second")
        .await
        .expect_err("a new worktree is refused under the floor");
    assert_eq!(refused.code, api_types::DISK_PRESSURE);
    assert_eq!(
        std::fs::read_dir(root.join(WORKTREE_DIRECTORY))
            .unwrap()
            .count(),
        handles_before,
        "nothing was created"
    );
    assert!(fixture
        .backend
        .workspace_for_placement("placement-2")
        .is_none());
    // The floor is met again: the same request is admitted.
    gc::accept_floor(&root, Some(FreeFloor::of_bytes(0, 0)));
    let made: WorkspacePrepareResult = serde_json::from_value(
        prepare("placement-2", "prepare-2", "task/second")
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(PathBuf::from(&made.workspace.workspace_path).is_dir());
    // Under the floor again, preparing that existing worktree is admitted:
    // work in a worktree that exists is never refused for disk.
    gc::accept_floor(&root, Some(FreeFloor::of_bytes(0, 100)));
    prepare("placement-2", "prepare-2", "task/second")
        .await
        .expect("an existing worktree is not refused");
    gc::accept_floor(&root, Some(FreeFloor::of_bytes(0, 0)));
}

/// Plan 3.4 F: under the floor a daemon's collector trims its shared
/// compiler cache before any handle's build output, and its disk report
/// carries the size its collector measured.
#[tokio::test]
async fn compiler_cache_is_trimmed_under_the_floor_and_reported() {
    let fixture = Fixture::new().await;
    let root = fixture.dir.path();
    let store = root
        .join(executors::compiler_cache::CACHE_DIR)
        .join("repo-a");
    std::fs::create_dir_all(store.join("0")).unwrap();
    std::fs::write(
        store.join(executors::compiler_cache::MARKER_FILE),
        "sccache",
    )
    .unwrap();
    let entry = store.join("0").join("entry");
    std::fs::write(&entry, vec![0_u8; 4096]).unwrap();

    let report = fixture
        .backend
        .gc_sweep_at(&[], SystemTime::now(), FreeFloor::of_bytes(0, 0))
        .await;
    assert_eq!(report.cache_entries_evicted, 0);
    assert!(entry.exists());
    let facts = gc::disk_report(root).expect("the test filesystem is readable");
    assert!(facts
        .compiler_cache_bytes
        .is_some_and(|bytes| bytes >= 4096));

    let report = fixture
        .backend
        .gc_sweep_at(&[], SystemTime::now(), FreeFloor::of_bytes(u64::MAX, 100))
        .await;
    assert_eq!((report.cache_entries_evicted, report.errors), (1, 0));
    assert!(!entry.exists() && store.join("0").is_dir());
}
