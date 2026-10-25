//! Workspace garbage collection ownership, confinement, eviction, and disk accounting tests.

use super::*;
use crate::sandbox::{RunPurpose, SandboxEnv, TaskRoot};

const DAY: Duration = Duration::from_secs(24 * 60 * 60);

fn shaped(name: &str) -> bool {
    !name.starts_with('.') && name != "main-agents" && name != "notes"
}

fn sweep(root: &Path) -> Sweep {
    Sweep::new(root, root, shaped, Duration::from_secs(30))
}

/// The same pass, run `later` from now.
fn later(root: &Path, later: Duration) -> Sweep {
    let mut sweep = sweep(root);
    sweep.now += later;
    sweep
}

fn states(entries: &[(&str, RootState)]) -> HashMap<String, RootState> {
    entries
        .iter()
        .map(|(name, state)| ((*name).to_owned(), *state))
        .collect()
}

fn task_root(root: &Path, name: &str) -> PathBuf {
    let path = root.join(name);
    fs::create_dir_all(path.join("repo")).unwrap();
    fs::create_dir_all(path.join(TASK_DIR_NAME)).unwrap();
    fs::write(path.join("repo/file"), "content").unwrap();
    path
}

#[test]
fn unknown_root_is_quarantined_then_deleted_only_after_a_day() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let orphan = task_root(&root, "orphan");
    let unknown = states(&[("orphan", RootState::Unknown)]);

    // Created a moment ago: a create may be in progress.
    let mut report = GcReport::default();
    sweep(&root).task_roots(&unknown, &mut report);
    assert!(orphan.exists());
    assert_eq!(report, GcReport::default());

    // Eleven hours on a slow clone may still be writing below it.
    later(&root, 11 * 3600 * Duration::from_secs(1)).task_roots(&unknown, &mut report);
    assert!(orphan.exists());
    assert_eq!(report, GcReport::default());

    // First sight after the grace period: moved aside, not deleted.
    let first = later(&root, 25 * Duration::from_secs(3600));
    first.task_roots(&unknown, &mut report);
    assert_eq!(report.quarantined, 1);
    assert!(!orphan.exists());
    assert_eq!(first.quarantined_names(), ["orphan"]);
    let kept = fs::read_dir(root.join(GC_DIR))
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(
        fs::read_to_string(kept.path().join("repo/file")).unwrap(),
        "content"
    );

    // Still there 23 hours on.
    let mut report = GcReport::default();
    later(&root, Duration::from_secs(48 * 3600)).quarantine_settle(&HashSet::new(), &mut report);
    assert_eq!(report.removed, 0);
    assert!(kept.path().exists());

    later(&root, Duration::from_secs(50 * 3600)).quarantine_settle(&HashSet::new(), &mut report);
    assert_eq!(report.removed, 1);
    assert!(!kept.path().exists());
}

#[test]
fn quarantined_root_whose_record_appears_is_restored_or_left_never_deleted() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    task_root(&root, "late");
    task_root(&root, "taken");
    let pass = later(&root, 2 * DAY);
    let mut report = GcReport::default();
    pass.task_roots(
        &states(&[("late", RootState::Unknown), ("taken", RootState::Unknown)]),
        &mut report,
    );
    assert_eq!(report.quarantined, 2);
    // `taken` was created again while its old directory sat in quarantine.
    fs::create_dir_all(root.join("taken/new")).unwrap();

    let known: HashSet<String> = ["late".to_owned(), "taken".to_owned()].into();
    let mut report = GcReport::default();
    later(&root, 30 * DAY).quarantine_settle(&known, &mut report);
    assert_eq!((report.restored, report.removed), (1, 0));
    assert_eq!(
        fs::read_to_string(root.join("late/repo/file")).unwrap(),
        "content"
    );
    assert!(root.join("taken/new").exists());
    // The older copy of `taken` is left in quarantine.
    assert_eq!(pass.quarantined_names(), ["taken"]);
}

#[test]
fn names_that_are_not_forge_shaped_and_links_are_never_listed() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    for name in [
        ".forge",
        ".forge-tmp",
        ".repos",
        "main-agents",
        "notes",
        "task-1",
    ] {
        fs::create_dir_all(root.join(name)).unwrap();
    }
    fs::write(root.join("task-file"), "x").unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(root.join("notes"), root.join("task-link")).unwrap();
    let pass = sweep(&root);
    assert_eq!(pass.task_root_names("", 10), ["task-1"]);
    assert!(pass.task_root_names("task-1", 10).is_empty());

    // Even when the owner's table calls one of them unknown, a name that
    // is not Task-root shaped is never quarantined or removed, and a
    // quarantine entry of another owner is not this pass's to settle.
    fs::create_dir_all(root.join(GC_DIR).join("notes-1000")).unwrap();
    let mut report = GcReport::default();
    let pass = later(&root, 30 * DAY);
    pass.task_roots(
        &states(&[
            ("notes", RootState::Unknown),
            (".repos", RootState::Cleaned),
        ]),
        &mut report,
    );
    pass.quarantine_settle(&HashSet::new(), &mut report);
    assert_eq!(report, GcReport::default());
    assert!(root.join("notes").exists() && root.join(".repos").exists());
    assert!(root.join(GC_DIR).join("notes-1000").exists());
}

/// A link inside the managed root to another directory inside it: the
/// parent "resolves inside the root", so a removal or a move by path
/// would take the entry of the directory the link points at (another
/// Task's). Both go down from the root by descriptor and refuse.
#[cfg(unix)]
#[test]
fn nothing_is_removed_or_trashed_through_a_link_to_another_directory_of_the_root() {
    use std::os::unix::fs::symlink;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap().join("root");
    let victim = task_root(&root, "victim");
    fs::write(victim.join("repo").join("work"), "keep").unwrap();
    fs::create_dir_all(root.join("attacker")).unwrap();
    symlink(&victim, root.join("attacker").join("swapped")).unwrap();
    let through = root.join("attacker").join("swapped").join("repo");
    let pass = sweep(&root);
    assert!(pass.confined(&through), "the check by path is satisfied");

    let mut report = GcReport::default();
    pass.remove(&through, &mut report);
    assert_eq!((report.removed, report.errors), (0, 1));
    let mut report = GcReport::default();
    assert!(!pass.trash(&through, &mut report));
    assert_eq!(report.errors, 1);
    assert_eq!(
        fs::read_to_string(victim.join("repo").join("work")).unwrap(),
        "keep"
    );

    // The link itself, and a real entry, still go.
    let mut report = GcReport::default();
    pass.remove(&root.join("attacker").join("swapped"), &mut report);
    assert!(pass.trash(&root.join("attacker"), &mut report));
    assert_eq!((report.removed, report.errors), (1, 0));
    assert!(!root.join("attacker").exists() && victim.join("repo").join("work").exists());
}

#[cfg(unix)]
#[test]
fn links_are_removed_as_entries_and_nothing_outside_the_root_is_touched() {
    use std::os::unix::fs::symlink;
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().canonicalize().unwrap();
    let root = base.join("root");
    let outside = base.join("outside");
    fs::create_dir_all(outside.join("user")).unwrap();
    fs::write(outside.join("user/data"), "keep").unwrap();

    // A cleaned Task root holding a link out of the root.
    let cleaned = task_root(&root, "cleaned");
    symlink(&outside, cleaned.join("escape")).unwrap();
    // A link planted as a broken copy, a quarantine entry and a check checkout.
    let live = task_root(&root, "live");
    symlink(&outside, live.join("repo.broken-1000")).unwrap();
    fs::create_dir_all(root.join(GC_DIR)).unwrap();
    symlink(&outside, root.join(GC_DIR).join("gone-1000")).unwrap();
    fs::create_dir_all(root.join(CHECKS_DIR)).unwrap();
    symlink(&outside, root.join(CHECKS_DIR).join("check-link")).unwrap();
    // A Task root reached through a link is not a Task root.
    symlink(&outside, root.join("linked")).unwrap();

    let pass = later(&root, 30 * DAY);
    let mut report = GcReport::default();
    pass.task_roots(
        &states(&[
            ("cleaned", RootState::Cleaned),
            ("live", RootState::Live),
            ("linked", RootState::Cleaned),
            ("../outside", RootState::Cleaned),
        ]),
        &mut report,
    );
    pass.quarantine_settle(&HashSet::new(), &mut report);
    pass.check_checkouts(0, &mut report);
    // Outside the root, and through a link into it: refused.
    pass.remove(&outside.join("user"), &mut report);
    pass.remove(&root.join("linked/user"), &mut report);

    assert_eq!(report.errors, 2);
    assert!(!cleaned.exists());
    assert!(!live.join("repo.broken-1000").exists());
    assert!(live.join("repo/file").exists());
    assert!(fs::symlink_metadata(root.join(GC_DIR).join("gone-1000")).is_err());
    assert!(fs::symlink_metadata(root.join(CHECKS_DIR).join("check-link")).is_err());
    assert!(fs::symlink_metadata(root.join("linked")).is_ok());
    assert_eq!(
        fs::read_to_string(outside.join("user/data")).unwrap(),
        "keep"
    );

    // An exact legacy path that is a link goes as a link.
    let legacy = base.join("legacy-home");
    symlink(&outside, &legacy).unwrap();
    remove_exact(&legacy, &mut report);
    assert!(fs::symlink_metadata(&legacy).is_err());
    assert_eq!(
        fs::read_to_string(outside.join("user/data")).unwrap(),
        "keep"
    );
}

#[cfg(unix)]
#[test]
fn read_only_tree_is_removed() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let cleaned = task_root(&root, "cleaned");
    let module = cleaned.join("pkg/mod/example@v1");
    fs::create_dir_all(&module).unwrap();
    fs::write(module.join("go.mod"), "module example").unwrap();
    for path in [&module, &cleaned.join("pkg/mod"), &cleaned.join("pkg")] {
        fs::set_permissions(path, fs::Permissions::from_mode(0o555)).unwrap();
    }
    let mut report = GcReport::default();
    sweep(&root).task_roots(&states(&[("cleaned", RootState::Cleaned)]), &mut report);
    assert_eq!((report.removed, report.errors), (1, 0));
    assert!(!cleaned.exists());
}

#[test]
fn broken_copy_is_kept_seven_days_and_a_live_root_keeps_everything_else() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let live = task_root(&root, "live");
    let made = unix_secs(SystemTime::now()) * 1000;
    let broken = live.join(format!("repo.broken-{made}"));
    fs::create_dir_all(broken.join("src")).unwrap();
    fs::create_dir_all(live.join("notes.broken-soon")).unwrap();
    let live_state = states(&[("live", RootState::Live)]);

    let mut report = GcReport::default();
    later(&root, 6 * DAY).task_roots(&live_state, &mut report);
    assert!(broken.exists());
    later(&root, 8 * DAY).task_roots(&live_state, &mut report);
    assert_eq!(report.removed, 1);
    assert!(!broken.exists());
    assert!(live.join("repo/file").exists());
    assert!(live.join("notes.broken-soon").exists());
}

#[cfg(unix)]
#[test]
fn an_entry_that_cannot_be_removed_does_not_stop_the_pass() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    task_root(&root, "a-escapes");
    task_root(&root, "b-cleaned");
    let mut report = GcReport::default();
    // `a/../..` is refused; the pass goes on to the next root.
    let pass = sweep(&root);
    pass.remove(&root.join("a-escapes/../.."), &mut report);
    pass.task_roots(&states(&[("b-cleaned", RootState::Cleaned)]), &mut report);
    assert_eq!((report.errors, report.removed), (1, 1));
    assert!(root.join("a-escapes").exists());
}

#[test]
fn pass_stops_at_its_deadline_and_says_so() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let cleaned = task_root(&root, "cleaned");
    let later_root = task_root(&root, "later");
    let both = states(&[
        ("cleaned", RootState::Cleaned),
        ("later", RootState::Cleaned),
    ]);
    let mut pass = sweep(&root);
    pass.deadline = Instant::now();
    let mut report = GcReport::default();
    // Out of time from the start: one entry is still finished, so a
    // cursor always moves, and the pass says it did not get to the rest.
    let finished = pass.task_roots(&both, &mut report);
    assert!(report.out_of_time);
    assert_eq!(finished.as_deref(), Some("cleaned"));
    assert!(!cleaned.exists() && later_root.exists());
    // The next pass, with time, finishes the work.
    let mut report = GcReport::default();
    let finished = sweep(&root).task_roots(&both, &mut report);
    assert_eq!(finished.as_deref(), Some("later"));
    assert!(!report.out_of_time && !later_root.exists());
}

#[test]
fn a_tree_larger_than_the_pass_is_removed_in_part_and_finished_by_the_next_pass() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let big = task_root(&root, "big");
    for sub in 0..3 {
        let sub = big.join(format!("dir-{sub}"));
        fs::create_dir_all(&sub).unwrap();
        for file in 0..300 {
            fs::write(sub.join(format!("f{file}")), "x").unwrap();
        }
    }
    let small = task_root(&root, "small");
    // Out of time from the start: the removal stops between entries.
    let mut pass = sweep(&root);
    pass.deadline = Instant::now();
    let mut report = GcReport::default();
    pass.remove(&big, &mut report);
    assert!(report.out_of_time && big.exists());
    assert_eq!((report.removed, report.errors), (0, 0));
    let files = |path: &Path| {
        (0..3)
            .map(|sub| fs::read_dir(path.join(format!("dir-{sub}"))).map_or(0, Iterator::count))
            .sum::<usize>()
    };
    assert!((1..900).contains(&files(&big)), "some of it went, not all");
    // A small tree always goes whole, so every pass makes progress.
    let mut report = GcReport::default();
    pass.remove(&small, &mut report);
    assert!(!small.exists() && !report.out_of_time);
    // The next pass, with time, finishes the large one.
    let mut report = GcReport::default();
    sweep(&root).remove(&big, &mut report);
    assert!(!big.exists());
    assert_eq!((report.removed, report.out_of_time), (1, false));
}

/// A run that starts while eviction is deciding either is seen or gets
/// its build directory made again; the directory it runs with is never
/// deleted under it.
#[test]
fn a_run_starting_during_eviction_never_loses_its_build_directory() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    for round in 0..40 {
        let name = format!("t{round}");
        let worktree = root.join(&name).join("repo");
        fs::create_dir_all(&worktree).unwrap();
        let task_root = TaskRoot::reserve(worktree.parent().unwrap()).unwrap();
        let build = task_root.build_dir();
        fs::create_dir_all(build.join("cargo")).unwrap();
        fs::write(build.join("cargo/.rustc_info.json"), "{}").unwrap();
        // The environment is computed while the Task is idle, as the
        // caller of the eviction saw it.
        let env = SandboxEnv::for_run(&worktree, &format!("run{round}"), RunPurpose::Check);
        let pass = sweep(&root);
        let evictor = {
            let name = name.clone();
            std::thread::spawn(move || {
                let mut report = GcReport::default();
                pass.evict_build(&name, &mut report)
            })
        };
        let run = env.prepared();
        let evicted = evictor.join().unwrap();
        if run.tmp_dir().is_some() && run.build_dir("CARGO_TARGET_DIR").is_some() {
            assert!(build.is_dir(), "round {round}: evicted={evicted}");
            // Live now: nothing takes it.
            let mut report = GcReport::default();
            assert!(!sweep(&root).evict_build(&name, &mut report));
            assert!(build.is_dir());
        }
        run.settle();
    }
}

#[test]
fn check_checkout_goes_only_when_old_and_no_live_check_can_own_it() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    // The checkout below is made by this process, after it started.
    let _ = sandbox::process_start();
    std::thread::sleep(Duration::from_millis(1100));
    let checkout = root.join(CHECKS_DIR).join("check-abc");
    fs::create_dir_all(checkout.join("src")).unwrap();
    fs::create_dir_all(root.join(CHECKS_DIR).join("target")).unwrap();
    let mut report = GcReport::default();
    // Young: a check may be running in it.
    sweep(&root).check_checkouts(0, &mut report);
    // Old, but created by this process while a check operation is live.
    later(&root, DAY).check_checkouts(1, &mut report);
    assert!(checkout.exists());
    assert_eq!(report.removed, 0);
    later(&root, DAY).check_checkouts(0, &mut report);
    assert_eq!(report.removed, 1);
    assert!(!checkout.exists());
    // Not a checkout: left alone.
    assert!(root.join(CHECKS_DIR).join("target").exists());
}

#[test]
fn live_run_temp_dir_survives_every_pass_and_a_stale_one_goes() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let worktree = root.join("t").join("repo");
    fs::create_dir_all(&worktree).unwrap();
    let reserved = TaskRoot::reserve(worktree.parent().unwrap()).unwrap();
    let run = SandboxEnv::for_run(&worktree, "live-run", RunPurpose::Hook).prepared();
    let Some(tmp) = run.tmp_dir().map(Path::to_path_buf) else {
        // The test temp dir is too long for a per-run directory.
        return;
    };
    assert!(sandbox::has_live_run_in(reserved.path()));
    let stale = tmp.parent().unwrap().join("stalerun00");
    fs::create_dir_all(&stale).unwrap();

    let mut report = GcReport::default();
    // Now: the stale directory is younger than any allowed run.
    let roomy = FreeFloor::of_bytes(0, 0);
    sweep(&root).run_dirs([], &roomy, &mut report);
    assert!(stale.exists() && tmp.exists());
    // Much later it is older than any run; the live one still stands.
    later(&root, 3 * DAY).run_dirs([], &roomy, &mut report);
    assert_eq!(report.run_dirs_removed, 1);
    assert!(!stale.exists());
    assert!(tmp.exists());

    run.settle();
    assert!(!sandbox::has_live_run_in(reserved.path()));
}

#[test]
fn build_output_is_evicted_least_recently_used_first_and_never_under_a_live_run() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let build = |name: &str| {
        let worktree = root.join(name).join("repo");
        fs::create_dir_all(&worktree).unwrap();
        TaskRoot::reserve(worktree.parent().unwrap()).unwrap();
        let build = root.join(name).join(TASK_DIR_NAME).join("build");
        fs::create_dir_all(build.join("cargo")).unwrap();
        fs::write(build.join("cargo/.rustc_info.json"), "{}").unwrap();
        (worktree, build)
    };
    let (_, old) = build("old");
    std::thread::sleep(Duration::from_millis(1100));
    let (busy_worktree, busy) = build("busy");
    fs::create_dir_all(root.join("empty").join(TASK_DIR_NAME).join("build")).unwrap();
    assert!(build_last_used(&root.join("empty")).is_none());
    assert!(build_last_used(&root.join("old")) < build_last_used(&root.join("busy")));
    let run = SandboxEnv::for_run(&busy_worktree, "busy-run", RunPurpose::Check).prepared();
    let names = ["busy".to_owned(), "old".to_owned(), "empty".to_owned()];

    // Plenty of room: nothing is evicted.
    let mut report = GcReport::default();
    let none = FreeFloor::of_bytes(0, 0);
    sweep(&root).evict_builds(&names, &none, &mut report);
    assert_eq!(report.builds_evicted, 0);
    assert!(old.exists() && busy.exists());

    // A floor no disk can meet: every idle build goes, the busy one
    // stays, whether or not its run has a temp directory (this test's
    // own root may be too long for one).
    let all = FreeFloor::of_bytes(u64::MAX, 100);
    sweep(&root).evict_builds(&names, &all, &mut report);
    assert!(!old.exists());
    assert!(busy.exists());
    assert_eq!(report.builds_evicted, 1);
    assert_eq!(report.removed, 0);
    run.settle();
}

/// A disk that is under any floor while `<root>/short` exists or the
/// `repo-a` store of the default cache directory holds an entry.
fn short_while_cached(root: &Path) -> Option<DiskSpace> {
    let cached = fs::read_dir(root.join(compiler_cache::CACHE_DIR).join("repo-a/0"))
        .is_ok_and(|mut entries| entries.next().is_some());
    let short = cached || root.join("short").exists();
    Some(DiskSpace {
        free: if short { 0 } else { 1000 },
        total: 1000,
        free_inodes: None,
        total_inodes: None,
    })
}

/// Under the floor the shared compiler cache is trimmed before any
/// Task loses its build output, least recently used entries first; a
/// Task's build output goes only when that was not enough. On a roomy
/// disk nothing is touched.
#[test]
fn compiler_cache_entries_go_before_any_task_build_output() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let worktree = root.join("idle").join("repo");
    fs::create_dir_all(&worktree).unwrap();
    TaskRoot::reserve(worktree.parent().unwrap()).unwrap();
    let build = root.join("idle").join(TASK_DIR_NAME).join("build");
    fs::create_dir_all(build.join("cargo")).unwrap();
    fs::write(build.join("cargo/.rustc_info.json"), "{}").unwrap();

    let cache_dir = root.join(compiler_cache::CACHE_DIR);
    let entry = |repository: &str, kind: &str, name: &str, age: u64| {
        let store = cache_dir.join(repository);
        fs::create_dir_all(store.join("0")).unwrap();
        fs::write(store.join(compiler_cache::MARKER_FILE), kind).unwrap();
        let path = store.join("0").join(name);
        fs::write(&path, vec![0_u8; 1000]).unwrap();
        let then = SystemTime::now() - Duration::from_secs(age);
        fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_times(fs::FileTimes::new().set_accessed(then).set_modified(then))
            .unwrap();
        path
    };
    let names = ["idle".to_owned()];
    let floor = FreeFloor::of_bytes(100, 0);
    // The collector's order: the cache, then Task build output.
    let pass = |live_elsewhere: bool| {
        let mut report = GcReport::default();
        let mut sweep = sweep(&root);
        sweep.disk_space = short_while_cached;
        sweep.evict_compiler_cache(&floor, live_elsewhere, &mut report);
        sweep.evict_builds(&names, &floor, &mut report);
        report
    };

    // Plenty of room: nothing goes, and the cache is measured.
    let held = entry("repo-b", "kache", "held", 5000);
    let report = pass(false);
    assert!(!report.did_something(), "{report:?}");
    assert!(compiler_cache::measured_bytes(&root).is_some_and(|bytes| bytes >= 1000));

    // Short of disk with entries to give: they go (nothing is installed
    // for this root, so a leftover cache goes down to nothing), that is
    // enough, and the Task keeps its build output.
    let (old, new) = (
        entry("repo-a", "sccache", "old", 900),
        entry("repo-a", "sccache", "new", 10),
    );
    // Another repository's store, of a wrapper whose entries are not
    // known to be safe to delete under a live build, is held by a live
    // run of that repository although its entry is the oldest.
    let holder = root.join("holder").join("repo");
    fs::create_dir_all(&holder).unwrap();
    TaskRoot::reserve(holder.parent().unwrap()).unwrap();
    let live = SandboxEnv::for_run(&holder, "holder", RunPurpose::Execution)
        .with_compiler_cache(Some(compiler_cache::CacheEnv {
            wrapper: PathBuf::from("/opt/kache"),
            kind: compiler_cache::WrapperKind::Kache,
            dir: Some(cache_dir.join("repo-b")),
            vars: Vec::new(),
        }))
        .prepared();
    let report = pass(false);
    assert_eq!(
        (report.cache_entries_evicted, report.builds_evicted),
        (2, 0)
    );
    assert!(!old.exists() && !new.exists() && held.exists() && build.exists());
    live.settle();

    // Still short and the cache cannot give (a check runs that the
    // registry names no repository for): now the idle Task's build goes.
    fs::write(root.join("short"), "").unwrap();
    let report = pass(true);
    assert_eq!(
        (report.cache_entries_evicted, report.builds_evicted),
        (0, 1)
    );
    assert!(held.exists() && !build.exists());
    let report = pass(false);
    assert_eq!(report.cache_entries_evicted, 1);
    assert!(!held.exists());
    // The stores themselves stay: a wrapper's server may hold them.
    assert!(cache_dir.join("repo-a").join("0").is_dir());
    assert!(cache_dir
        .join("repo-b")
        .join(compiler_cache::MARKER_FILE)
        .exists());

    // With a cache installed, half its configured size is kept.
    let wrapper = root.join("kache");
    fs::write(&wrapper, "").unwrap();
    compiler_cache::install(
        &root,
        Some(compiler_cache::CompilerCache {
            wrapper,
            kind: compiler_cache::WrapperKind::Kache,
            dir: cache_dir.clone(),
            max_bytes: 4000,
        }),
    );
    let (oldest, kept_a, kept_b) = (
        entry("repo-b", "kache", "a", 900),
        entry("repo-b", "kache", "b", 100),
        entry("repo-b", "kache", "c", 50),
    );
    let report = pass(false);
    assert_eq!(report.cache_entries_evicted, 1);
    assert!(!oldest.exists() && kept_a.exists() && kept_b.exists());
    compiler_cache::install(&root, None);
}

/// The store of a repository the owner no longer has goes a day after
/// its last run start: never a known repository's, never a fresh one,
/// never one a run holds, never a directory Forge did not mark, and its
/// sccache server is stopped first.
#[test]
fn compiler_cache_stores_of_deleted_repositories_are_removed_after_the_grace() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap().join("root");
    fs::create_dir_all(&root).unwrap();
    let wrapper = dir.path().join("bin").join("sccache");
    fs::create_dir_all(wrapper.parent().unwrap()).unwrap();
    let calls = dir.path().join("calls");
    process_supervisor::executable::write_executable(
        &wrapper,
        format!(
            "#!/bin/sh\nprintf '%s|%s\\n' \"$1\" \"$SCCACHE_SERVER_UDS\" >> '{}'\n",
            calls.display()
        ),
    )
    .unwrap();
    let cache_dir = root.join(compiler_cache::CACHE_DIR);
    compiler_cache::install(
        &root,
        Some(compiler_cache::CompilerCache {
            kind: compiler_cache::WrapperKind::of(&wrapper),
            wrapper,
            dir: cache_dir.clone(),
            max_bytes: 1 << 30,
        }),
    );
    let day = Duration::from_secs(24 * 60 * 60);
    let store = |repository: &str, kind: Option<&str>, age: Duration| {
        let store = cache_dir.join(repository);
        fs::create_dir_all(store.join("0")).unwrap();
        fs::write(store.join("0").join("entry"), "x").unwrap();
        if let Some(kind) = kind {
            let marker = store.join(compiler_cache::MARKER_FILE);
            fs::write(&marker, kind).unwrap();
            let then = SystemTime::now() - age;
            fs::File::options()
                .write(true)
                .open(&marker)
                .unwrap()
                .set_times(fs::FileTimes::new().set_accessed(then).set_modified(then))
                .unwrap();
        }
        store
    };
    let known_old = store("repo-known", Some("kache"), day * 3);
    let gone_fresh = store("repo-gone-fresh", Some("kache"), day / 2);
    let gone_held = store("repo-gone-held", Some("kache"), day * 3);
    let gone = store("repo-gone", Some("sccache"), day * 3);
    let unmarked = store("not-forges", None, day * 3);
    // The dead store's server still answers on its socket.
    let server = std::os::unix::net::UnixListener::bind(gone.join("s")).unwrap();
    let holder = root.join("holder").join("repo");
    fs::create_dir_all(&holder).unwrap();
    TaskRoot::reserve(holder.parent().unwrap()).unwrap();
    let live = SandboxEnv::for_run(&holder, "holder", RunPurpose::Execution)
        .with_compiler_cache(Some(compiler_cache::CacheEnv {
            wrapper: PathBuf::from("/opt/kache"),
            kind: compiler_cache::WrapperKind::Kache,
            dir: Some(gone_held.clone()),
            vars: Vec::new(),
        }))
        .prepared();
    let known: HashSet<String> = ["repo-known".to_owned()].into();
    let pass = |live_elsewhere: bool| {
        let mut report = GcReport::default();
        sweep(&root).remove_cache_stores_of_deleted_repositories(
            &known,
            live_elsewhere,
            &mut report,
        );
        report
    };

    // The owner runs something the registry names no repository for.
    assert_eq!(pass(true).cache_stores_removed, 0);
    assert!(gone.exists());
    // An owner that knows no repository at all proves nothing.
    let mut report = GcReport::default();
    sweep(&root).remove_cache_stores_of_deleted_repositories(&HashSet::new(), false, &mut report);
    assert_eq!(report, GcReport::default());
    assert!(gone.exists() && known_old.exists());
    // A cache directory outside the managed root may be another
    // server's too: nothing in it is removed for a missing repository.
    let elsewhere = dir.path().canonicalize().unwrap().join("shared-cache");
    let others = elsewhere.join("repo-of-another-server");
    fs::create_dir_all(others.join("0")).unwrap();
    let marker = others.join(compiler_cache::MARKER_FILE);
    fs::write(&marker, "kache").unwrap();
    let then = SystemTime::now() - day * 3;
    fs::File::options()
        .write(true)
        .open(&marker)
        .unwrap()
        .set_times(fs::FileTimes::new().set_accessed(then).set_modified(then))
        .unwrap();
    let installed = compiler_cache::installed(&root).unwrap();
    compiler_cache::install(
        &root,
        Some(compiler_cache::CompilerCache {
            kind: installed.kind,
            wrapper: installed.wrapper.clone(),
            dir: elsewhere.clone(),
            max_bytes: installed.max_bytes,
        }),
    );
    assert_eq!(pass(false), GcReport::default());
    assert!(others.join("0").exists());
    compiler_cache::install(&root, Some((*installed).clone()));

    let report = pass(false);
    assert_eq!((report.cache_stores_removed, report.errors), (1, 0));
    assert!(!gone.exists(), "the dead repository's store is gone");
    for kept in [&known_old, &gone_fresh, &gone_held, &unmarked] {
        assert!(kept.join("0").join("entry").exists(), "{}", kept.display());
    }
    assert_eq!(
        fs::read_dir(&cache_dir).unwrap().count(),
        4,
        "nothing is left renamed aside"
    );
    // Stopped through the socket of the store as it was moved aside,
    // after the lock every run start takes was released.
    let stopped = fs::read_to_string(&calls).unwrap();
    let prefix = format!(
        "--stop-server|{}",
        cache_dir.join(".forge-removed-").display()
    );
    assert!(
        stopped.starts_with(&prefix) && stopped.ends_with("/s\n") && stopped.lines().count() == 1,
        "{stopped}"
    );
    drop(server);

    // The run settles: its store goes on the next pass.
    live.settle();
    assert_eq!(pass(false).cache_stores_removed, 1);
    assert!(!gone_held.exists() && known_old.exists() && gone_fresh.exists());
    compiler_cache::install(&root, None);
}

/// With the feature off a pass does nothing about a compiler cache: it
/// creates nothing, measures nothing and stats no Task root for a
/// marker. Turned on, all stores together are held to the configured
/// size on a disk with plenty of room; a store of a wrapper not known
/// to tolerate it is left alone while a run uses it.
#[test]
fn compiler_cache_is_untouched_when_off_and_held_to_its_size_when_on() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let roomy = |_: &Path| {
        Some(DiskSpace {
            free: 1000,
            total: 1000,
            free_inodes: None,
            total_inodes: None,
        })
    };
    let floor = FreeFloor::of_bytes(100, 0);
    let pass = || {
        let mut report = GcReport::default();
        let mut sweep = sweep(&root);
        sweep.disk_space = roomy;
        sweep.evict_compiler_cache(&floor, false, &mut report);
        report
    };

    // Off, never used: nothing.
    task_root(&root, "t1");
    let before: Vec<_> = fs::read_dir(&root)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .collect();
    assert!(!pass().did_something());
    assert_eq!(compiler_cache::collected(&root), None);
    assert_eq!(compiler_cache::measured_bytes(&root), None);
    assert!(!root.join(".forge").exists());
    let after: Vec<_> = fs::read_dir(&root)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .collect();
    assert_eq!(before, after);
    assert!(!sweep(&root).holds_compiler_cache(&root.join("t1")));

    let cache_dir = root.join(compiler_cache::CACHE_DIR);
    let entry = |repository: &str, name: &str, age: u64| {
        let store = cache_dir.join(repository);
        fs::create_dir_all(store.join("0")).unwrap();
        fs::write(store.join(compiler_cache::MARKER_FILE), "kache").unwrap();
        let path = store.join("0").join(name);
        fs::write(&path, vec![0_u8; 1000]).unwrap();
        let then = SystemTime::now() - Duration::from_secs(age);
        fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_times(fs::FileTimes::new().set_accessed(then).set_modified(then))
            .unwrap();
        path
    };
    // Off with a cache left behind, roomy disk: measured, not trimmed.
    let (oldest, old, new) = (
        entry("repo-a", "oldest", 900),
        entry("repo-b", "old", 500),
        entry("repo-a", "new", 10),
    );
    assert!(!pass().did_something());
    assert!(compiler_cache::measured_bytes(&root).is_some_and(|bytes| bytes >= 3000));

    // On, 2000 bytes for all stores together: the least recently used
    // entry goes, whichever repository it belongs to.
    let wrapper = root.join("kache");
    fs::write(&wrapper, "").unwrap();
    compiler_cache::install(
        &root,
        Some(compiler_cache::CompilerCache {
            wrapper,
            kind: compiler_cache::WrapperKind::Kache,
            dir: cache_dir.clone(),
            max_bytes: 2000,
        }),
    );
    let report = pass();
    assert_eq!(report.cache_entries_evicted, 1, "{report:?}");
    assert!(!oldest.exists() && old.exists() && new.exists());
    assert_eq!(pass().cache_entries_evicted, 0);

    // Over the size again while a run uses the store that holds the
    // oldest entry: that store waits, the pass does not fail.
    let again = entry("repo-b", "again", 700);
    let holder = root.join("holder").join("repo");
    fs::create_dir_all(&holder).unwrap();
    TaskRoot::reserve(holder.parent().unwrap()).unwrap();
    let live = SandboxEnv::for_run(&holder, "holder", RunPurpose::Check)
        .with_compiler_cache(Some(compiler_cache::CacheEnv {
            wrapper: PathBuf::from("/opt/kache"),
            kind: compiler_cache::WrapperKind::Kache,
            dir: Some(cache_dir.join("repo-b")),
            vars: Vec::new(),
        }))
        .prepared();
    let report = pass();
    assert_eq!((report.cache_entries_evicted, report.errors), (1, 0));
    assert!(again.exists() && old.exists() && !new.exists());
    live.settle();
    compiler_cache::install(&root, None);
}

/// The shared compiler cache is never a Task root: not quarantined and
/// not removed as a cleaned Task's leftover, wherever the operator put
/// it and whatever its directories are named.
#[test]
fn the_compiler_cache_is_never_quarantined_or_removed_as_a_task_root() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    // The default place: under `.forge`, which is not Task-root shaped.
    let default_store = root.join(compiler_cache::CACHE_DIR).join("repo-a");
    fs::create_dir_all(default_store.join("0")).unwrap();
    fs::write(default_store.join(compiler_cache::MARKER_FILE), "sccache").unwrap();
    // A cache directory the operator put among the Task roots, and a
    // store that even looks like a Task root Forge made.
    let cache_dir = root.join("cache");
    let store = cache_dir.join("repo-a");
    fs::create_dir_all(store.join(TASK_DIR_NAME)).unwrap();
    fs::write(store.join(compiler_cache::MARKER_FILE), "kache").unwrap();
    let marked = task_root(&root, "marked");
    fs::write(marked.join(compiler_cache::MARKER_FILE), "kache").unwrap();
    fs::create_dir_all(cache_dir.join(TASK_DIR_NAME)).unwrap();
    let orphan = task_root(&root, "orphan");
    let wrapper = root.join("kache");
    fs::write(&wrapper, "").unwrap();
    compiler_cache::install(
        &root,
        Some(compiler_cache::CompilerCache {
            wrapper,
            kind: compiler_cache::WrapperKind::Kache,
            dir: cache_dir.clone(),
            max_bytes: 1,
        }),
    );

    let pass = later(&root, 30 * DAY);
    assert!(!pass.task_root_names("", 100).contains(&".forge".to_owned()));
    for state in [RootState::Unknown, RootState::Cleaned] {
        let mut report = GcReport::default();
        pass.task_roots(
            &states(&[(".forge", state), ("cache", state), ("marked", state)]),
            &mut report,
        );
        assert!(!report.did_something(), "{state:?}: {report:?}");
    }
    assert!(default_store.join("0").is_dir() && store.is_dir() && marked.is_dir());
    // The rule itself still works beside it.
    let mut report = GcReport::default();
    pass.task_roots(&states(&[("orphan", RootState::Unknown)]), &mut report);
    assert_eq!(report.quarantined, 1);
    assert!(!orphan.exists());
    compiler_cache::install(&root, None);
}

/// Under the free-space floor a crashed run's temp directory is kept
/// only as long as the longest configured run limit allows, not for the
/// 25 hours it gets on a roomy disk; a live run keeps its own whatever
/// the disk says.
#[test]
fn under_the_floor_a_dead_run_dir_goes_after_the_configured_run_limit() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let worktree = root.join("t").join("repo");
    fs::create_dir_all(&worktree).unwrap();
    TaskRoot::reserve(worktree.parent().unwrap()).unwrap();
    let tmp_root = root.join("t").join(TASK_DIR_NAME).join("tmp");
    let (dead, live) = (tmp_root.join("deadrun000"), tmp_root.join("liverun000"));
    fs::create_dir_all(&dead).unwrap();
    fs::create_dir_all(&live).unwrap();
    let (roomy, short) = (
        FreeFloor::of_bytes(0, 0),
        FreeFloor::of_bytes(u64::MAX, 100),
    );
    let mut pass = later(&root, Duration::from_secs(3 * 60 * 60));
    pass.check_checkout_age = check_checkout_age(30 * 60);
    assert_eq!(pass.run_dir_age(&roomy), MAX_RUN_AGE);
    assert_eq!(pass.run_dir_age(&short), Duration::from_secs(2 * 60 * 60));
    // A limit longer than a day never lengthens the gate.
    let mut long = pass.clone();
    long.check_checkout_age = check_checkout_age(86_400);
    assert_eq!(long.run_dir_age(&short), MAX_RUN_AGE);

    let mut report = GcReport::default();
    // Three hours old on a roomy disk: kept.
    pass.run_dirs(["liverun000"], &roomy, &mut report);
    assert_eq!(report.run_dirs_removed, 0);
    // Under the floor: the dead one goes, the live one stays.
    pass.run_dirs(["liverun000"], &short, &mut report);
    assert_eq!(report.run_dirs_removed, 1);
    assert!(!dead.exists() && live.exists());
}

/// A run with no per-run temp directory (its Task root is too long for
/// a socket path: the usual case on macOS) is in the live-run registry
/// all the same, so its build output is never evicted under it.
#[test]
fn a_run_without_a_temp_dir_still_keeps_its_build_output_from_eviction() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let worktree = root.join("t").join("repo");
    fs::create_dir_all(&worktree).unwrap();
    let reserved = TaskRoot::reserve(worktree.parent().unwrap()).unwrap();
    let build = root.join("t").join(TASK_DIR_NAME).join("build");
    fs::create_dir_all(build.join("cargo")).unwrap();
    fs::write(build.join("cargo/.rustc_info.json"), "{}").unwrap();

    let run = SandboxEnv::for_run(&worktree, "no-tmp-run", RunPurpose::Execution)
        .without_tmp()
        .prepared();
    assert!(run.tmp_dir().is_none());
    assert!(sandbox::has_live_run_in(reserved.path()));
    // A second run of the same Task, settled first, does not end the
    // first one's registration.
    let other = SandboxEnv::for_run(&worktree, "other-run", RunPurpose::Hook)
        .without_tmp()
        .prepared();
    other.settle();
    assert!(sandbox::has_live_run_in(reserved.path()));
    let mut report = GcReport::default();
    assert!(!sweep(&root).evict_build("t", &mut report));
    assert!(build.join("cargo/.rustc_info.json").exists());
    // Nothing was created for the registration.
    assert!(!root.join("t").join(TASK_DIR_NAME).join("live").exists());

    // Settled by its starter, or by whoever settles the execution.
    sandbox::settle_run(&worktree, "no-tmp-run");
    assert!(!sandbox::has_live_run_in(reserved.path()));
    run.settle();
    assert!(sweep(&root).evict_build("t", &mut report));
    assert_eq!(report.builds_evicted, 1);
    assert!(!build.exists());
}

#[test]
fn only_an_adopted_root_has_an_owner_and_only_an_operator_changes_it() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let pass = sweep(&root);
    // Asking never claims: no marker, no `.forge` directory.
    assert_eq!(pass.ownership("first"), Ownership::Unclaimed);
    assert!(!root.join(".forge").exists());
    assert_eq!(pass.adopt(""), Ownership::Unclaimed);
    assert_eq!(pass.adopt("first"), Ownership::Mine);
    assert_eq!(pass.adopt("first"), Ownership::Mine);
    assert_eq!(pass.adopt("second"), Ownership::Other);
    assert_eq!(pass.ownership("second"), Ownership::Other);
    assert_eq!(pass.ownership(""), Ownership::Other);
    assert_eq!(
        fs::read_to_string(root.join(GC_DIR).join(OWNER_FILE)).unwrap(),
        "first"
    );
    // Nothing staged is left behind, and the claim is not a quarantine entry.
    assert_eq!(fs::read_dir(root.join(GC_DIR)).unwrap().count(), 1);
    assert!(pass.quarantined_names().is_empty());
    // An explicit re-claim is the only way the owner changes.
    assert_eq!(pass.reclaim("second"), Ownership::Mine);
    assert_eq!(pass.ownership("first"), Ownership::Other);
    assert_eq!(fs::read_dir(root.join(GC_DIR)).unwrap().count(), 1);
    // A root that does not exist cannot be adopted (or created).
    assert!(matches!(
        sweep(&root.join("missing")).adopt("first"),
        Ownership::Refused(_)
    ));
    assert!(!root.join("missing").exists());
}

#[test]
fn two_owners_adopting_together_leave_exactly_one_whole_marker() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
    let threads: Vec<_> = (0..8)
        .map(|index| {
            let (root, barrier) = (root.clone(), barrier.clone());
            std::thread::spawn(move || {
                barrier.wait();
                sweep(&root).adopt(&format!("owner-{index}")) == Ownership::Mine
            })
        })
        .collect();
    let winners = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .filter(|won| *won)
        .count();
    assert_eq!(winners, 1);
    let owner = fs::read_to_string(root.join(GC_DIR).join(OWNER_FILE)).unwrap();
    assert!(owner.starts_with("owner-") && owner.len() == 7, "{owner}");
    assert_eq!(fs::read_dir(root.join(GC_DIR)).unwrap().count(), 1);
}

#[cfg(unix)]
#[test]
fn a_root_that_is_a_home_a_repository_a_link_or_the_top_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().canonicalize().unwrap();
    for path in [
        Path::new("/"),
        Path::new("/Volumes"),
        Path::new("/tmp"),
        Path::new("/usr"),
    ] {
        assert!(refuse_root(path).is_some(), "{}", path.display());
    }
    // The home directory of this test process, and every parent of it.
    let home = PathBuf::from(std::env::var_os("HOME").unwrap());
    if let Ok(home) = home.canonicalize() {
        assert!(refuse_root(&home).is_some());
        assert!(refuse_root(home.parent().unwrap()).is_some());
    }
    let repo = base.join("repo");
    fs::create_dir_all(repo.join(".git")).unwrap();
    assert_eq!(
        sweep(&repo).adopt("owner"),
        Ownership::Refused("it is a git repository")
    );
    assert!(!repo.join(".forge").exists());
    // A root reached through a link is not the directory that was adopted.
    let real = base.join("real");
    fs::create_dir_all(&real).unwrap();
    assert_eq!(sweep(&real).adopt("owner"), Ownership::Mine);
    let link = base.join("link");
    std::os::unix::fs::symlink(&real, &link).unwrap();
    assert!(matches!(
        sweep(&link).ownership("owner"),
        Ownership::Refused(_)
    ));
    // A marker that is a link, or a `.forge/gc` that is one, is no claim.
    let planted = base.join("planted");
    fs::create_dir_all(planted.join(".forge")).unwrap();
    std::os::unix::fs::symlink(real.join(GC_DIR), planted.join(GC_DIR)).unwrap();
    assert_eq!(sweep(&planted).ownership("owner"), Ownership::Other);
}

#[cfg(unix)]
#[test]
fn a_user_directory_with_a_task_shaped_name_is_never_quarantined() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    // The user's own: plain files, and a full clone (`.git` is a directory).
    fs::create_dir_all(root.join("mine/photos")).unwrap();
    fs::create_dir_all(root.join("mine/clone/.git")).unwrap();
    fs::create_dir_all(root.join("line\nbreak/repo")).unwrap();
    // Forge's own, three ways.
    task_root(&root, "reserved");
    fs::create_dir_all(root.join("worktree/repo")).unwrap();
    fs::write(root.join("worktree/repo/.git"), "gitdir: elsewhere").unwrap();
    fs::create_dir_all(root.join("outbox/.forge-outbox")).unwrap();
    // In flight in this process, however old.
    task_root(&root, "creating");
    let mut pass = later(&root, 30 * DAY);
    pass.creating = |path| path.ends_with("creating");
    let names = [
        "mine",
        "line\nbreak",
        "reserved",
        "worktree",
        "outbox",
        "creating",
        "/etc",
        "..",
        "a/b",
    ];
    let unknown: HashMap<String, RootState> = names
        .iter()
        .map(|name| ((*name).to_owned(), RootState::Unknown))
        .collect();
    let mut report = GcReport::default();
    pass.task_roots(&unknown, &mut report);
    assert_eq!(
        (report.quarantined, report.removed, report.errors),
        (3, 0, 0)
    );
    assert!(root.join("mine/photos").exists() && root.join("mine/clone/.git").exists());
    assert!(root.join("line\nbreak/repo").exists() && root.join("creating/repo/file").exists());
    let mut held = pass.quarantined_names();
    held.sort();
    assert_eq!(held, ["outbox", "reserved", "worktree"]);
}

#[cfg(unix)]
#[test]
fn quarantine_never_goes_through_a_linked_gc_directory_and_never_copies() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().canonicalize().unwrap();
    let (root, elsewhere) = (base.join("root"), base.join("elsewhere"));
    fs::create_dir_all(&elsewhere).unwrap();
    let orphan = task_root(&root, "orphan");
    fs::create_dir_all(root.join(".forge")).unwrap();
    // `.forge/gc` points at another place (another filesystem, say).
    std::os::unix::fs::symlink(&elsewhere, root.join(GC_DIR)).unwrap();
    let pass = later(&root, 30 * DAY);
    let mut report = GcReport::default();
    pass.task_roots(&states(&[("orphan", RootState::Unknown)]), &mut report);
    assert!(!pass.trash(&orphan, &mut report));
    assert_eq!(
        (report.quarantined, report.removed, report.errors),
        (0, 0, 2)
    );
    assert!(orphan.join("repo/file").exists());
    assert_eq!(fs::read_dir(&elsewhere).unwrap().count(), 0);
}

#[test]
fn trash_is_a_rename_and_is_emptied_later() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let cleaned = task_root(&root, "cleaned");
    let pass = sweep(&root);
    let mut report = GcReport::default();
    assert!(pass.trash(&cleaned, &mut report));
    assert!(!cleaned.exists());
    assert_eq!(report, GcReport::default());
    // Condemned directories are not quarantine entries.
    assert!(pass.quarantined_names().is_empty());
    pass.quarantine_settle(&HashSet::new(), &mut report);
    assert_eq!(fs::read_dir(root.join(TRASH_DIR)).unwrap().count(), 1);
    pass.empty_trash(&mut report);
    assert_eq!((report.removed, report.errors), (1, 0));
    assert_eq!(fs::read_dir(root.join(TRASH_DIR)).unwrap().count(), 0);
    // Outside the root: refused.
    assert!(!pass.trash(dir.path().parent().unwrap(), &mut report));
}

#[test]
fn free_floor_is_the_larger_of_bytes_and_percent() {
    let floor = FreeFloor::default();
    let gib = 1024 * 1024 * 1024;
    assert_eq!(floor.bytes(100 * gib), 10 * gib);
    assert_eq!(floor.bytes(1000 * gib), 50 * gib);
    let space = disk_space(Path::new("/")).unwrap();
    assert!(space.total >= space.free && space.total > 0);
    // Inodes are read with the bytes, where the filesystem counts them.
    if let (Some(free), Some(total)) = (space.free_inodes, space.total_inodes) {
        assert!(total >= free && total > 0);
    }
    let facts = space.facts("now".to_owned(), Some("owned".to_owned()));
    assert_eq!(
        (
            facts.free_bytes,
            facts.total_inodes,
            facts.gc_state.as_deref()
        ),
        (space.free, space.total_inodes, Some("owned"))
    );
    assert!(disk_space(Path::new("/no/such/place")).is_none());
    assert_eq!(check_checkout_age(60), CHECK_CHECKOUT_AGE);
    assert_eq!(check_checkout_age(7200), Duration::from_secs(5 * 3600));
}

#[test]
fn nothing_is_evicted_when_the_disk_cannot_be_read_and_the_reading_decides() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let build = root.join("idle").join(TASK_DIR_NAME).join("build");
    fs::create_dir_all(build.join("cargo")).unwrap();
    fs::write(build.join("cargo/.rustc_info.json"), "{}").unwrap();
    let names = ["idle".to_owned(), "../idle".to_owned()];
    let floor = FreeFloor::default();
    let mut report = GcReport::default();
    let mut pass = sweep(&root);
    // Unreadable: no eviction, whatever the floor.
    pass.disk_space = |_| None;
    pass.evict_builds(&names, &floor, &mut report);
    // Plenty of room.
    pass.disk_space = |_| {
        Some(DiskSpace {
            free: 900,
            total: 1000,
            ..DiskSpace::default()
        })
    };
    pass.evict_builds(&names, &FreeFloor::of_bytes(0, 5), &mut report);
    assert!(build.exists());
    assert_eq!(report, GcReport::default());
    // Under the floor.
    pass.disk_space = |_| {
        Some(DiskSpace {
            free: 10,
            total: 1000,
            ..DiskSpace::default()
        })
    };
    pass.evict_builds(&names, &FreeFloor::of_bytes(0, 5), &mut report);
    assert!(!build.exists());
    assert_eq!(
        (report.builds_evicted, report.removed, report.errors),
        (1, 0, 0)
    );
}

#[test]
fn legacy_location_counts_as_untouched_only_when_nothing_in_it_is_recent() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().join("home");
    fs::create_dir_all(home.join("inner")).unwrap();
    let now = SystemTime::now();
    assert!(!untouched_for(&home, now, LEGACY_UNTOUCHED));
    assert!(!untouched_for(&home, now + 6 * DAY, LEGACY_UNTOUCHED));
    assert!(untouched_for(&home, now + 8 * DAY, LEGACY_UNTOUCHED));
    assert!(!untouched_for(
        &dir.path().join("missing"),
        now + 8 * DAY,
        LEGACY_UNTOUCHED
    ));
}

#[cfg(unix)]
#[test]
fn measure_is_bounded_and_does_not_follow_links() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let outside = root.join("outside");
    fs::create_dir_all(&outside).unwrap();
    fs::write(outside.join("big"), vec![1_u8; 1024 * 1024]).unwrap();
    let task = task_root(&root, "task");
    fs::write(task.join("repo/data"), vec![1_u8; 64 * 1024]).unwrap();
    std::os::unix::fs::symlink(&outside, task.join("link")).unwrap();
    let far = Instant::now() + Duration::from_secs(30);
    let bytes = measure(&task, far).unwrap();
    assert!((64 * 1024..512 * 1024).contains(&bytes), "{bytes}");
    assert!(measure(&task.join("link"), far).is_none());
    // Over budget: no number rather than a partial one.
    for index in 0..600 {
        fs::write(task.join(format!("repo/f{index}")), "x").unwrap();
    }
    assert!(measure(&task, Instant::now()).is_none());
}
