//! Managed Codex sandbox, cache admission, and home isolation tests.

use super::*;
use executors::CommandOverrides;
use std::fs;

#[cfg(unix)]
#[test]
fn writable_roots_resolve_symlinked_components() {
    let temp = tempfile::tempdir().unwrap();
    let base = std::fs::canonicalize(temp.path()).unwrap();
    let target = base.join("relocated-npm");
    std::fs::create_dir(&target).unwrap();
    let link = base.join(".npm");
    std::os::unix::fs::symlink(&target, &link).unwrap();

    assert_eq!(resolve_path_for_overlap(&link), target);
    assert_eq!(
        resolve_path_for_overlap(&link.join("missing/child")),
        target.join("missing/child")
    );
    assert_eq!(
        resolve_path_for_overlap(&base.join("absent")),
        base.join("absent")
    );
}

#[cfg(unix)]
#[test]
fn codex_writable_roots_drop_resolved_protected_ancestors() {
    use std::os::unix::fs::symlink;
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().canonicalize().unwrap();
    let home = base.join("home");
    let daemon = base.join("daemon");
    let worktree = base.join("workspaces/task/repo");
    let managed = base.join("logs/.codex-managed-home");
    for path in [&home, &daemon, &worktree, &managed] {
        std::fs::create_dir_all(path).unwrap();
    }
    std::fs::create_dir(daemon.join(".forge")).unwrap();
    for (name, target) in [
        ("filesystem", Path::new("/")),
        ("home", home.as_path()),
        ("worktree", worktree.as_path()),
        ("daemon-state", daemon.join(".forge").as_path()),
        ("managed", managed.as_path()),
        ("ancestor", base.as_path()),
    ] {
        let link = base.join(format!("link-{name}"));
        symlink(target, &link).unwrap();
        assert!(
            safe_writable_root(
                &link,
                &link,
                Some(&home),
                &worktree,
                Some(&daemon),
                Some(&managed)
            )
            .is_none(),
            "{name}"
        );
    }
    let chain = base.join("chain");
    symlink(base.join("link-ancestor"), &chain).unwrap();
    assert!(
        safe_writable_root(
            &chain,
            &chain,
            Some(&home),
            &worktree,
            Some(&daemon),
            Some(&managed)
        )
        .is_none()
    );
    let relative = home.join(".cache");
    symlink("..", &relative).unwrap();
    assert!(
        safe_writable_root(
            &relative,
            &relative,
            Some(&home),
            &worktree,
            Some(&daemon),
            Some(&managed)
        )
        .is_none()
    );
    let safe = base.join("actual-cache");
    std::fs::create_dir(&safe).unwrap();
    let safe_link = home.join(".npm");
    symlink("../actual-cache", &safe_link).unwrap();
    assert_eq!(
        safe_writable_root(
            &safe_link,
            &safe_link,
            Some(&home),
            &worktree,
            Some(&daemon),
            Some(&managed)
        ),
        Some(safe)
    );
}

#[cfg(unix)]
#[test]
fn codex_writable_roots_refuse_descendants_credentials_and_mounts() {
    use std::os::unix::fs::symlink;
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().canonicalize().unwrap();
    let home = base.join("home");
    let daemon = base.join("daemon");
    let worktree = base.join("workspaces/task/repo");
    let managed = base.join("managed");
    for path in [&home, &daemon, &worktree, &managed] {
        std::fs::create_dir_all(path).unwrap();
    }
    let refused = |path: &Path| {
        assert!(
            safe_writable_root(
                path,
                path,
                Some(&home),
                &worktree,
                Some(&daemon),
                Some(&managed)
            )
            .is_none(),
            "{}",
            path.display()
        );
    };
    for path in [
        daemon.join(".forge/child/cache"),
        managed.join("plugins/cache"),
        managed.join("task-scratch/child"),
        base.join("workspaces/other-task/repo/cache"),
        worktree.parent().unwrap().join("notes/cache"),
    ] {
        std::fs::create_dir_all(&path).unwrap();
        refused(&path);
    }
    for name in [
        ".ssh",
        ".gnupg",
        ".aws",
        ".kube",
        ".docker",
        ".config/gh",
        ".codex",
        ".claude",
        ".forge",
    ] {
        let credential = home.join(name).join("child");
        std::fs::create_dir_all(&credential).unwrap();
        let link = base.join("credential-link");
        symlink(&credential, &link).unwrap();
        refused(&link);
        std::fs::remove_file(link).unwrap();
    }
    let scratch = managed.join("task-scratch");
    assert_eq!(
        safe_writable_root(
            &scratch,
            &scratch,
            Some(&home),
            &worktree,
            Some(&daemon),
            Some(&managed)
        ),
        Some(scratch)
    );
    // A relocated root at the filesystem root is always refused.
    let volume = home.join(".cache");
    symlink("/", &volume).unwrap();
    refused(&volume);
    let ordinary = base.join("caches/npm");
    std::fs::create_dir_all(&ordinary).unwrap();
    let npm = home.join(".npm");
    symlink(&ordinary, &npm).unwrap();
    assert_eq!(
        safe_writable_root(
            &npm,
            &npm,
            Some(&home),
            &worktree,
            Some(&daemon),
            Some(&managed)
        ),
        Some(ordinary)
    );
}

#[test]
fn codex_macos_data_prefix_normalization_is_component_aware() {
    for (input, expected) in [
        ("/System/Volumes/Data", "/"),
        ("/System/Volumes/Data/Users/x", "/Users/x"),
        ("/System/Volumes/Data/Users/x/.ssh", "/Users/x/.ssh"),
        (
            "/System/Volumes/Database/Users/x",
            "/System/Volumes/Database/Users/x",
        ),
        ("/Users/x/.cache", "/Users/x/.cache"),
    ] {
        assert_eq!(
            normalize_macos_data_path(Path::new(input)),
            PathBuf::from(expected)
        );
    }
}

#[cfg(unix)]
#[test]
fn codex_default_caches_allow_home_workspace_roots_and_unredirected_mounts() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().canonicalize().unwrap();
    let worktree = home.join(".forge-worktrees/task/repo");
    let managed = home.join("logs/managed");
    std::fs::create_dir_all(&worktree).unwrap();
    std::fs::create_dir_all(&managed).unwrap();
    for cache in package_cache_roots(&home, |_| None) {
        std::fs::create_dir_all(&cache).unwrap();
        assert_eq!(
            safe_writable_root(
                &cache,
                &cache,
                Some(&home),
                &worktree,
                Some(&home),
                Some(&managed)
            ),
            Some(cache.clone())
        );
        // Simulate this exact default cache being a Docker volume/dataset.
        assert!(!redirected_volume_refusal(
            &cache,
            &cache,
            &cache,
            Some(&home),
            |_| true
        ));
    }
    let cache = home.join(".npm");
    let volume = home.join("volume");
    std::fs::create_dir(&volume).unwrap();
    std::fs::remove_dir(&cache).unwrap();
    std::os::unix::fs::symlink(&volume, &cache).unwrap();
    assert!(redirected_volume_refusal(
        &cache,
        &resolve_path_for_overlap(&cache),
        &cache,
        Some(&home),
        |path| path == volume
    ));
    // An explicit environment relocation is exempt from the mount rule.
    assert!(!redirected_volume_refusal(
        &volume,
        &volume,
        &cache,
        Some(&home),
        |path| path == volume
    ));
    assert!(!redirected_volume_refusal(
        &cache,
        &volume.join("npm"),
        &cache,
        Some(&home),
        |path| path == volume
    ));
}

#[test]
fn codex_mount_table_parser_compares_escaped_mount_strings() {
    let table = r"21 1 0:2 / / rw - rootfs rootfs rw
22 21 0:3 / /dead/network rw - nfs unreachable:/share rw
23 21 0:4 / /cache\040volume rw - ext4 /dev/test rw
24 21 0:5 / /cache\134name rw - ext4 /dev/test rw";
    assert!(mount_table_contains_root(table, Path::new("/cache volume")));
    assert!(mount_table_contains_root(table, Path::new(r"/cache\name")));
    assert!(!mount_table_contains_root(
        table,
        Path::new("/cache volume/npm")
    ));
    assert!(!mount_table_contains_root(
        table,
        Path::new("/dead/network/cache")
    ));
}

#[cfg(unix)]
#[test]
fn codex_default_caches_under_symlinked_home_are_not_redirected() {
    let dir = tempfile::tempdir().unwrap();
    let actual_home = dir.path().join("actual-home");
    std::fs::create_dir(&actual_home).unwrap();
    let home = dir.path().join("home");
    std::os::unix::fs::symlink(&actual_home, &home).unwrap();
    let worktree = home.join("tasks/task/repo");
    std::fs::create_dir_all(&worktree).unwrap();
    for cache in package_cache_roots(&home, |_| None) {
        std::fs::create_dir_all(&cache).unwrap();
        let resolved = resolve_path_for_overlap(&cache);
        assert!(!redirected_volume_refusal(
            &cache,
            &resolved,
            &cache,
            Some(&home),
            |_| true
        ));
        assert_eq!(
            safe_writable_root(&cache, &cache, Some(&home), &worktree, Some(&home), None),
            Some(resolved)
        );
    }
}

#[test]
fn package_cache_roots_honor_relocation_variables() {
    let home = Path::new("/home/u");
    let defaults = package_cache_roots(home, |_| None);
    for expected in [
        "/home/u/.npm",
        "/home/u/.cargo/registry",
        "/home/u/.cargo/git",
        "/home/u/go/pkg/mod",
        "/home/u/.cache",
    ] {
        assert!(defaults.contains(&PathBuf::from(expected)), "{expected}");
    }

    let relocated = package_cache_roots(home, |key| match key {
        "CARGO_HOME" => Some("/opt/cargo".into()),
        "GOMODCACHE" => Some("/opt/gomod".into()),
        "npm_config_cache" => Some("/opt/npm".into()),
        "XDG_CACHE_HOME" => Some(String::new().into()),
        _ => None,
    });
    for expected in [
        "/opt/npm",
        "/opt/cargo/registry",
        "/opt/cargo/git",
        "/opt/gomod",
        "/home/u/.cache",
    ] {
        assert!(relocated.contains(&PathBuf::from(expected)), "{expected}");
    }
}

#[test]
fn package_cache_roots_cannot_overlap_task_directory() {
    let tempdir = tempfile::tempdir().expect("tempdir creates");
    let task_root = tempdir.path().join("task");
    let worktree = task_root.join("repo");
    fs::create_dir_all(&worktree).expect("worktree creates");

    assert!(!cache_root_is_disjoint_from_task(
        &task_root.join("cache"),
        &worktree,
    ));
    assert!(!cache_root_is_disjoint_from_task(tempdir.path(), &worktree,));
    assert!(cache_root_is_disjoint_from_task(
        &tempdir.path().join("sibling-cache"),
        &worktree,
    ));
}

#[cfg(unix)]
#[test]
fn managed_codex_home_resets_scratch_left_read_only_by_a_test() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().expect("tempdir creates");
    let ambient = dir.path().join("ambient");
    let managed = dir.path().join("logs/task/.codex-managed-home");
    fs::create_dir_all(&ambient).expect("ambient home creates");
    prepare_managed_codex_home(&managed, &ambient).expect("first execution prepares");

    // A project test migrated a fixture it had made read-only and never
    // restored the mode, so its tempdir could not clean up after itself.
    let legacy = managed.join("task-scratch/.tmpXYZ/legacy.novelkit");
    fs::create_dir_all(legacy.join("chapters")).expect("fixture tree creates");
    fs::write(legacy.join("project.json"), "{}").expect("fixture file writes");
    fs::set_permissions(&legacy, fs::Permissions::from_mode(0o555))
        .expect("fixture made read-only");

    prepare_managed_codex_home(&managed, &ambient).expect("second execution prepares");

    let scratch = managed.join("task-scratch");
    assert!(scratch.is_dir());
    assert_eq!(
        fs::read_dir(&scratch).expect("scratch lists").count(),
        0,
        "the next execution starts from an empty scratch directory"
    );
}

fn managed_ctx(worktree: &Path, task_id: &str, logs_path: &Path) -> ExecutionContext {
    let mut agent_config = json!({});
    executors::mark_task_role(&mut agent_config, "coder");
    ExecutionContext {
        task_id: task_id.to_owned(),
        execution_id: format!("exec-{task_id}"),
        worktree_path: worktree.to_string_lossy().into_owned(),
        description: String::new(),
        agent_config,
        logs_path: logs_path.to_string_lossy().into_owned(),
        heartbeat_interval_seconds: 30,
        max_turns: None,
        log_sender: None,
    }
}

/// A daemon writes every execution log into one flat directory. The home
/// used to be derived from it, so concurrent executions of different
/// Tasks shared and reset one home.
#[test]
fn concurrent_codex_executions_of_two_tasks_get_distinct_homes_in_their_task_roots() {
    let dir = tempfile::tempdir().expect("tempdir creates");
    let logs = dir.path().join("execution-logs");
    let mut homes = Vec::new();
    for task in ["task-a", "task-b"] {
        let worktree = dir.path().join(task).join("repo");
        fs::create_dir_all(&worktree).expect("worktree creates");
        executors::sandbox::TaskRoot::reserve(worktree.parent().unwrap()).expect("reserved");
        let ctx = managed_ctx(&worktree, task, &logs.join(format!("{task}.jsonl")));
        let home = CodexAdapter::managed_codex_home(&ctx)
            .expect("home prepares")
            .expect("managed execution has a home");
        assert_eq!(home, dir.path().join(task).join(".forge-task/home/codex"));
        assert!(home.join("config.toml").is_file());
        homes.push(home);
    }
    // Preparing the second home reset nothing in the first.
    fs::write(homes[0].join("config.toml"), "first").expect("marker writes");
    let worktree = dir.path().join("task-b/repo");
    CodexAdapter::managed_codex_home(&managed_ctx(&worktree, "task-b", &logs.join("b.jsonl")))
        .expect("home prepares again");
    assert_eq!(
        fs::read_to_string(homes[0].join("config.toml")).unwrap(),
        "first"
    );
    assert!(!logs.join(".codex-managed-home").exists());
}

#[test]
fn managed_codex_home_adopts_the_per_task_legacy_home_and_legacy_roots_keep_theirs() {
    let dir = tempfile::tempdir().expect("tempdir creates");
    // Server layout before the move: logs/<project>/<task>/.codex-managed-home.
    let logs = dir.path().join("logs/project/task-a");
    let legacy = logs.join(".codex-managed-home");
    fs::create_dir_all(legacy.join("sessions")).expect("legacy home creates");
    fs::write(legacy.join("sessions/rollout.jsonl"), "thread").expect("session writes");
    let worktree = dir.path().join("task-a/repo");
    fs::create_dir_all(&worktree).expect("worktree creates");
    executors::sandbox::TaskRoot::reserve(worktree.parent().unwrap()).expect("reserved");
    let home = CodexAdapter::managed_codex_home(&managed_ctx(
        &worktree,
        "task-a",
        &logs.join("exec.jsonl"),
    ))
    .expect("home prepares")
    .expect("managed home");
    assert_eq!(home, dir.path().join("task-a/.forge-task/home/codex"));
    assert_eq!(
        fs::read_to_string(home.join("sessions/rollout.jsonl")).unwrap(),
        "thread"
    );
    assert!(!legacy.exists());

    // A Task root Forge did not reserve: today's home, nothing beside it.
    let unreserved = dir.path().join("user/repo");
    fs::create_dir_all(&unreserved).expect("worktree creates");
    let logs = dir.path().join("logs/project/task-b");
    let home = CodexAdapter::managed_codex_home(&managed_ctx(
        &unreserved,
        "task-b",
        &logs.join("exec.jsonl"),
    ))
    .expect("home prepares")
    .expect("managed home");
    assert_eq!(home, logs.join(".codex-managed-home"));
    assert!(!dir.path().join("user/.forge-task").exists());
}

/// An execution that outlived the upgrade (an orphaned child of the old
/// server) may still hold its old home. Moving the home keeps every file
/// it has open: nothing it writes is lost, and nothing is deleted.
#[cfg(unix)]
#[test]
fn adopting_the_legacy_home_under_a_live_execution_loses_nothing() {
    use std::io::Write;
    let dir = tempfile::tempdir().expect("tempdir creates");
    let logs = dir.path().join("logs/project/task-a");
    let legacy = logs.join(".codex-managed-home");
    fs::create_dir_all(legacy.join("sessions")).expect("legacy home creates");
    let mut live = fs::File::create(legacy.join("sessions/rollout.jsonl")).expect("opens");
    live.write_all(b"before\n").expect("writes");
    let worktree = dir.path().join("task-a/repo");
    fs::create_dir_all(&worktree).expect("worktree creates");
    executors::sandbox::TaskRoot::reserve(worktree.parent().unwrap()).expect("reserved");
    let ctx = managed_ctx(&worktree, "task-a", &logs.join("exec.jsonl"));
    let home = CodexAdapter::managed_codex_home(&ctx)
        .expect("home prepares")
        .expect("managed home");
    live.write_all(b"after\n")
        .expect("the live execution still writes");
    live.sync_all().expect("syncs");
    assert_eq!(
        fs::read_to_string(home.join("sessions/rollout.jsonl")).unwrap(),
        "before\nafter\n"
    );
    // A second execution adopts nothing and resets nothing of the first's sessions.
    CodexAdapter::managed_codex_home(&ctx).expect("home prepares again");
    assert_eq!(
        fs::read_to_string(home.join("sessions/rollout.jsonl")).unwrap(),
        "before\nafter\n"
    );
}

/// A managed Codex run is handed only directories its sandbox can write.
#[cfg(unix)]
#[test]
fn codex_sandbox_keeps_only_what_the_execution_can_write() {
    use executors::sandbox::{RunPurpose, SandboxEnv, TaskRoot};
    let dir = tempfile::tempdir().expect("tempdir creates");
    let worktree = dir.path().join("t/repo");
    fs::create_dir_all(&worktree).expect("worktree creates");
    TaskRoot::reserve(worktree.parent().unwrap()).expect("reserved");
    let offered = || SandboxEnv::for_run(&worktree, "run-1", RunPurpose::Execution);
    let reserved = dir.path().join("t/.forge-task");
    assert_eq!(
        offered().tmp_dir(),
        Some(reserved.join("tmp/run1").as_path())
    );

    // Managed: both are granted as writable roots, so both are kept.
    assert_eq!(
        codex_admitted_sandbox(offered(), &worktree, true, false),
        offered()
    );
    // Yolo has no sandbox.
    assert_eq!(
        codex_admitted_sandbox(offered(), &worktree, false, true),
        offered()
    );
    // The operator's own sandbox: Forge cannot know what it can write.
    assert_eq!(
        codex_admitted_sandbox(offered(), &worktree, false, false),
        SandboxEnv::none()
    );

    // A temp directory that resolves outside the Task root is not handed out.
    let outside = dir.path().join("outside");
    fs::create_dir_all(&outside).expect("outside creates");
    std::os::unix::fs::symlink(&outside, reserved.join("tmp")).expect("link plants");
    let admitted = codex_admitted_sandbox(offered(), &worktree, true, false);
    assert_eq!(admitted.tmp_dir(), None);
    assert_eq!(
        admitted.build_dir("CARGO_TARGET_DIR"),
        Some(reserved.join("build/cargo").as_path())
    );
}

/// The shared compiler cache reaches a Codex execution only where the
/// run cannot be broken by it.
#[cfg(unix)]
#[test]
fn codex_sandbox_gets_the_compiler_cache_only_when_it_cannot_break_the_run() {
    use executors::compiler_cache::{CACHE_DIR, CompilerCache, WrapperKind};
    use executors::sandbox::{RunPurpose, SandboxEnv, TaskRoot};
    let dir = tempfile::tempdir().expect("tempdir creates");
    let root = dir.path().join("root");
    let worktree = root.join("t/repo");
    fs::create_dir_all(&worktree).expect("worktree creates");
    TaskRoot::reserve(worktree.parent().unwrap()).expect("reserved");
    fs::write(
        worktree.join(".git"),
        format!(
            "gitdir: {}\n",
            root.join(".repos/repo-a/worktrees/t").display()
        ),
    )
    .expect("worktree link writes");
    // The repository names the worktree back, as Git does; without it the
    // worktree's own `.git` file claims nothing.
    fs::create_dir_all(root.join(".repos/repo-a/worktrees/t")).unwrap();
    fs::write(
        root.join(".repos/repo-a/worktrees/t/gitdir"),
        format!("{}\n", worktree.join(".git").display()),
    )
    .unwrap();
    let wrapper = |name: &str| {
        let path = dir.path().join("bin").join(name);
        fs::create_dir_all(path.parent().unwrap()).expect("bin creates");
        process_supervisor::executable::write_executable(
            &path,
            "#!/bin/sh\n[ \"$1\" = --start-server ] && exit 0\nexec \"$@\"\n",
        )
        .expect("wrapper writes");
        path
    };
    let offered = |name: &str| {
        let wrapper = wrapper(name);
        let cache = CompilerCache {
            kind: WrapperKind::of(&wrapper),
            wrapper,
            dir: root.join(CACHE_DIR),
            max_bytes: 1 << 30,
        };
        SandboxEnv::for_run(&worktree, "run-1", RunPurpose::Execution)
            .with_compiler_cache(cache.for_worktree(&worktree))
    };
    let store = root.join(CACHE_DIR).join("repo-a");
    // The store's sccache server: a start that leaves nothing listening
    // on the store's socket is a failed start, and no cache is offered.
    assert!(offered("kache").compiler_cache().is_some());
    let _server = std::os::unix::net::UnixListener::bind(store.join("s")).expect("server");
    for name in ["kache", "sccache", "cachepot"] {
        assert!(offered(name).compiler_cache().is_some(), "{name}");
        // Yolo has no sandbox: whatever the operator configured.
        assert_eq!(
            codex_admitted_sandbox(offered(name), &worktree, false, true),
            offered(name)
        );
        // The operator's own sandbox: nothing, so no wrapper either.
        assert_eq!(
            codex_admitted_sandbox(offered(name), &worktree, false, false),
            SandboxEnv::none()
        );
    }
    // Managed: only the wrapper that builds uncached when the sandbox
    // keeps it from its store. sccache fails the compile instead, and
    // an unknown wrapper is not known to do either.
    let managed = |name: &str| codex_admitted_sandbox(offered(name), &worktree, true, false);
    assert_eq!(
        managed("kache")
            .compiler_cache()
            .and_then(|cache| cache.dir()),
        Some(store.as_path())
    );
    for name in ["sccache", "cachepot"] {
        let admitted = managed(name);
        assert!(admitted.compiler_cache().is_none(), "{name}");
        // The rest of the Task root's environment is untouched.
        assert!(admitted.build_dir("CARGO_TARGET_DIR").is_some());
    }

    // The store is a writable root only when the child really carries
    // Forge's wrapper and variables.
    let admitted = managed("kache");
    let mut command = std::process::Command::new("codex");
    assert_eq!(admitted.compiler_cache_dir_in_use(&command), None);
    for (key, value) in admitted.compiler_cache().unwrap().variables() {
        command.env(key, value);
    }
    assert_eq!(
        admitted.compiler_cache_dir_in_use(&command),
        Some(store.as_path())
    );
    command.env("RUSTC_WRAPPER", "/operator/sccache");
    assert_eq!(admitted.compiler_cache_dir_in_use(&command), None);

    // A store that resolves somewhere else is never handed to a sandbox.
    let outside = dir.path().join("outside");
    fs::create_dir_all(&outside).expect("outside creates");
    let env = offered("kache");
    fs::remove_dir_all(&store).expect("store removes");
    std::os::unix::fs::symlink(&outside, &store).expect("link plants");
    assert!(
        codex_admitted_sandbox(env, &worktree, true, false)
            .compiler_cache()
            .is_none()
    );
}

#[test]
fn managed_codex_home_shares_only_authentication() {
    let dir = tempfile::tempdir().expect("tempdir creates");
    let ambient = dir.path().join("ambient");
    let managed = dir.path().join("logs/task/.codex-managed-home");
    fs::create_dir_all(ambient.join("rules")).expect("ambient rules dir creates");
    fs::write(ambient.join("auth.json"), "{\"auth_mode\":\"chatgpt\"}")
        .expect("ambient auth writes");
    fs::write(ambient.join("config.toml"), "approval_policy = 'never'")
        .expect("ambient config writes");
    fs::write(
        ambient.join("rules/default.rules"),
        "prefix_rule(pattern=[\"git\",\"merge\"], decision=\"allow\")",
    )
    .expect("ambient rule writes");

    prepare_managed_codex_home(&managed, &ambient).expect("managed home prepares");

    assert_eq!(
        managed
            .join("auth.json")
            .canonicalize()
            .expect("managed auth resolves"),
        ambient
            .join("auth.json")
            .canonicalize()
            .expect("ambient auth resolves")
    );
    assert_eq!(
        fs::read_to_string(managed.join("config.toml")).expect("managed config reads"),
        MANAGED_CONFIG
    );
    let managed_rules = fs::read_to_string(managed.join("rules").join(MANAGED_RULES_FILE))
        .expect("managed deny rules read");
    assert!(managed_rules.contains("decision=\"forbidden\""));
    assert!(managed_rules.contains("[\"git\", \"merge\"]"));
    assert!(!managed_rules.contains("decision=\"allow\""));

    fs::write(
        managed.join("config.toml"),
        format!("{MANAGED_CONFIG}\n[projects.\"/tmp/generated\"]\ntrust_level = \"trusted\"\n"),
    )
    .expect("Codex-generated project trust writes");
    fs::create_dir_all(managed.join("skills/.system/generated"))
        .expect("Codex-generated skill directory writes");
    fs::write(
        managed.join("skills/.system/generated/SKILL.md"),
        "runtime-generated",
    )
    .expect("Codex-generated skill writes");
    fs::write(managed.join("task-scratch/transient"), "temporary").expect("Task scratch writes");

    prepare_managed_codex_home(&managed, &ambient)
        .expect("managed home safely prepares for another execution");
    assert_eq!(
        fs::read_to_string(managed.join("config.toml")).expect("reset managed config reads"),
        MANAGED_CONFIG
    );
    assert!(!managed.join("skills").exists());
    assert!(managed.join("task-scratch").is_dir());
    assert!(
        fs::read_dir(managed.join("task-scratch"))
            .expect("Task scratch reads")
            .next()
            .is_none(),
        "Task scratch is reset between attempts"
    );

    fs::write(managed.join("rules/foreign.rules"), "# not Forge-owned")
        .expect("foreign rule writes");
    let error = prepare_managed_codex_home(&managed, &ambient)
        .expect_err("managed authority inputs fail closed");
    assert!(error.to_string().contains("unexpected rules file"));
}

#[test]
fn a_plugins_directory_codex_created_does_not_block_the_next_execution() {
    // Codex writes `plugins/` into its home while it runs. Treating that
    // as a forbidden configuration input meant the first execution in a
    // Task's log directory always succeeded and every later one died
    // deterministically — so a re-review, or any retry after a review,
    // could not run at all. Observed as three identical reviewer failures
    // on a Task whose first reviewer attempt had completed normally.
    let root = std::env::temp_dir().join(format!(
        "forge-codex-plugins-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos()
    ));
    let managed = root.join("managed");
    let ambient = root.join("ambient");
    fs::create_dir_all(&ambient).expect("ambient home writes");
    fs::write(ambient.join("auth.json"), "{}").expect("ambient auth writes");

    prepare_managed_codex_home(&managed, &ambient).expect("first execution prepares");

    // Exactly what Codex leaves behind: the directory plus its own
    // staging and cache children.
    fs::create_dir_all(managed.join("plugins/.remote-plugin-install-staging"))
        .expect("Codex plugin staging writes");
    fs::create_dir_all(managed.join("plugins/cache")).expect("Codex plugin cache writes");

    prepare_managed_codex_home(&managed, &ambient)
        .expect("a second execution must prepare over Codex's own plugins directory");
    assert!(
        !managed.join("plugins").exists(),
        "the plugins directory is runtime cache and is discarded, not inherited"
    );

    // The inputs Codex never writes stay fail-closed.
    for forbidden in ["AGENTS.md", "hooks.json"] {
        fs::write(managed.join(forbidden), "planted").expect("planted input writes");
        let error = prepare_managed_codex_home(&managed, &ambient)
            .expect_err("a planted configuration input must fail closed");
        assert!(
            error.to_string().contains("forbidden configuration input"),
            "{forbidden} must still be refused: {error}"
        );
        fs::remove_file(managed.join(forbidden)).expect("planted input clears");
    }

    fs::remove_dir_all(&root).ok();
}

#[test]
fn managed_codex_home_overrides_profile_environment() {
    let config = CodexConfig {
        command_overrides: CommandOverrides {
            env: Some(HashMap::from([(
                "CODEX_HOME".to_owned(),
                "/tmp/profile-home".to_owned(),
            )])),
            ..CommandOverrides::default()
        },
        ..CodexConfig::default()
    };
    let managed = Path::new("/tmp/forge-managed-home");

    let cmd = CodexAdapter::build_command(&config, Some(managed));
    let command = cmd.as_std();
    let actual = command
        .get_envs()
        .find_map(|(key, value)| {
            (key == "CODEX_HOME").then(|| value.expect("CODEX_HOME has value"))
        })
        .expect("CODEX_HOME is set");

    assert_eq!(actual, managed.as_os_str());
    let expected_scratch = managed.join("task-scratch");
    for key in ["TMPDIR", "TMP", "TEMP"] {
        let actual = command
            .get_envs()
            .find_map(|(candidate, value)| {
                (candidate == key).then(|| value.expect("scratch variable has value"))
            })
            .unwrap_or_else(|| panic!("{key} is set"));
        assert_eq!(actual, expected_scratch.as_os_str());
    }
}
