//! The shared compiler cache, end to end: two Task worktrees of one
//! repository, each with its own build directory, reach one store through
//! the configured wrapper.
#![cfg(unix)]

use executors::{
    compiler_cache::{self, CompilerCache, WrapperKind},
    sandbox::{RunPurpose, SandboxEnv, TaskRoot},
};
use std::{
    collections::BTreeMap,
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

fn git(dir: &Path, args: &[&str]) {
    let output = Command::new("git")
        .args([
            "-c",
            "user.name=Forge Test",
            "-c",
            "user.email=test@forge.invalid",
        ])
        .args([
            "-c",
            "commit.gpgsign=false",
            "-c",
            "init.defaultBranch=main",
        ])
        .args(args)
        .current_dir(dir)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .output()
        .expect("git runs");
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// `<root>/.repos/<repository>`: a bare repository with one commit of
/// `files`, the way the server keeps a repository.
fn repository(root: &Path, repository: &str, files: &[(&str, String)]) -> PathBuf {
    let source = root.join("source").join(repository);
    fs::create_dir_all(&source).unwrap();
    git(&source, &["init", "-q"]);
    for (name, content) in files {
        let path = source.join(name);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, content).unwrap();
    }
    git(&source, &["add", "-A"]);
    git(&source, &["commit", "-q", "-m", "initial"]);
    let repo = root.join(".repos").join(repository);
    fs::create_dir_all(repo.parent().unwrap()).unwrap();
    git(
        root,
        &[
            "clone",
            "-q",
            "--bare",
            source.to_str().unwrap(),
            repo.to_str().unwrap(),
        ],
    );
    repo
}

/// A Task worktree of `repo` at `<root>/<task>/repo`, in a reserved Task root.
fn task_worktree(root: &Path, repo: &Path, task: &str) -> PathBuf {
    let worktree = root.join(task).join("repo");
    fs::create_dir_all(worktree.parent().unwrap()).unwrap();
    git(
        repo,
        &[
            "worktree",
            "add",
            "-q",
            "-b",
            task,
            worktree.to_str().unwrap(),
        ],
    );
    TaskRoot::reserve(worktree.parent().unwrap()).unwrap();
    worktree
}

/// Apply what the Task root gives a run to `command`, with no Project
/// environment and an operator environment that names no wrapper, whatever
/// the machine running this test has set.
fn apply(command: &mut Command, env: &SandboxEnv) {
    for key in [
        "RUSTC_WRAPPER",
        "CARGO_TARGET_DIR",
        "CARGO_BUILD_TARGET_DIR",
        "SCCACHE_DIR",
        "SCCACHE_CACHE_SIZE",
        "SCCACHE_SERVER_UDS",
        "SCCACHE_SERVER_PORT",
        "KACHE_CACHE_DIR",
    ] {
        command.env_remove(key);
    }
    for (key, value) in env.variables(&BTreeMap::new(), |_| false, |_| None) {
        match value {
            Some(value) => command.env(key, value),
            None => command.env_remove(key),
        };
    }
}

fn on_path(program: &str) -> Option<PathBuf> {
    std::env::split_paths(&std::env::var_os("PATH")?)
        .map(|dir| dir.join(program))
        .find(|path| path.is_file())
}

/// A wrapper that records its invocation and runs the compiler: both Task
/// worktrees of one repository call it with the same store, each with its
/// own build directory, and without that directory in the wrapper's
/// environment.
#[test]
fn two_task_worktrees_of_one_repository_call_the_wrapper_with_one_store() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap().join("root");
    let repo = repository(&root, "repo-1", &[("README.md", "hello\n".to_owned())]);
    let bin = dir.path().join("bin");
    fs::create_dir_all(&bin).unwrap();
    let wrapper = bin.join("sccache");
    let calls = dir.path().join("calls");
    fs::write(
        &wrapper,
        format!(
            "#!/bin/sh\n[ \"$1\" = --start-server ] && exit 0\nprintf '%s|%s|%s\\n' \"$1\" \"$SCCACHE_DIR\" \"${{CARGO_TARGET_DIR:-unset}}\" >> '{}'\nexec \"$@\"\n",
            calls.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o755)).unwrap();
    compiler_cache::install(
        &root,
        Some(CompilerCache {
            kind: WrapperKind::of(&wrapper),
            wrapper,
            dir: root.join(compiler_cache::CACHE_DIR),
            max_bytes: 1 << 30,
        }),
    );

    let mut target_dirs = Vec::new();
    for task in ["task-a", "task-b"] {
        let worktree = task_worktree(&root, &repo, task);
        let scope = SandboxEnv::for_run(&worktree, task, RunPurpose::Execution).scoped();
        let mut command = Command::new("sh");
        command
            .args([
                "-c",
                "\"$RUSTC_WRAPPER\" rustc -vV && printf '%s' \"$CARGO_TARGET_DIR\"",
            ])
            .current_dir(&worktree);
        apply(&mut command, scope.env());
        let output = command.output().unwrap();
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(
            output.status.success() && stdout.contains("rustc "),
            "{stdout}"
        );
        target_dirs.push(stdout.lines().last().unwrap().to_owned());
    }
    compiler_cache::install(&root, None);

    // Each Task builds into its own directory...
    assert_eq!(
        target_dirs,
        ["task-a", "task-b"].map(|task| {
            root.join(task)
                .join(".forge-task/build/cargo")
                .to_str()
                .unwrap()
                .to_owned()
        })
    );
    // ...and both reached the wrapper with the repository's one store.
    let store = root.join(compiler_cache::CACHE_DIR).join("repo-1");
    let line = format!("rustc|{}|unset", store.display());
    assert_eq!(
        fs::read_to_string(&calls)
            .unwrap()
            .lines()
            .collect::<Vec<_>>(),
        [line.as_str(), line.as_str()]
    );
}

/// Stops the per-store sccache server Forge started, whatever the test did.
struct StopServer {
    sccache: PathBuf,
    socket: PathBuf,
}

impl Drop for StopServer {
    fn drop(&mut self) {
        let _ = Command::new(&self.sccache)
            .arg("--stop-server")
            .env("SCCACHE_SERVER_UDS", &self.socket)
            .output();
    }
}

fn rust_hits(sccache: &Path, socket: &Path) -> u64 {
    let output = Command::new(sccache)
        .args(["--show-stats", "--stats-format", "json"])
        .env("SCCACHE_SERVER_UDS", socket)
        .output()
        .unwrap();
    let stats: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    stats["stats"]["cache_hits"]["counts"]["Rust"]
        .as_u64()
        .unwrap_or(0)
}

/// A crate `app` in a repository, depending on a crate at one absolute path
/// outside every worktree (like a registry crate). Returns the repository.
fn app_with_a_shared_dependency(base: &Path, root: &Path) -> PathBuf {
    let dep = base.join("shared").join("dep");
    fs::create_dir_all(dep.join("src")).unwrap();
    fs::write(
        dep.join("Cargo.toml"),
        "[package]\nname = \"dep\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[workspace]\n",
    )
    .unwrap();
    fs::write(dep.join("src/lib.rs"), "pub fn answer() -> u32 { 42 }\n").unwrap();
    repository(
        root,
        "repo-1",
        &[
            (
                "Cargo.toml",
                format!(
                    "[package]\nname = \"app\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[dependencies]\ndep = {{ path = \"{}\" }}\n",
                    dep.display()
                ),
            ),
            (
                "src/lib.rs",
                "pub fn answer() -> u32 { dep::answer() }\n".to_owned(),
            ),
        ],
    )
}

/// `cargo build` in a new Task worktree of `repo`, with what its Task root
/// gives the run. The run's temp directory is removed when this returns.
fn cargo_build(root: &Path, repo: &Path, task: &str, store: &Path) {
    let worktree = task_worktree(root, repo, task);
    let scope = SandboxEnv::for_run(&worktree, task, RunPurpose::Execution).scoped();
    assert_eq!(
        scope.env().compiler_cache().and_then(|cache| cache.dir()),
        Some(store),
        "the cache is usable for {task}"
    );
    let mut command = Command::new("cargo");
    command
        .args(["build", "--offline", "--quiet"])
        .current_dir(&worktree)
        .env("CARGO_INCREMENTAL", "0");
    apply(&mut command, scope.env());
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "{task}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let target = root.join(task).join(".forge-task/build/cargo");
    assert!(
        target.join("debug").is_dir(),
        "{task} built into its own directory"
    );
}

/// The real thing, when sccache and cargo are installed: the second Task's
/// build of a dependency both Tasks share is a cache hit although each Task
/// has its own `CARGO_TARGET_DIR`; a run's removed temp directory does not
/// break the next run; and evicting every entry under the live server costs
/// a recompile, never a failed build.
#[test]
fn sccache_shares_a_dependency_between_two_task_worktrees() {
    let (Some(sccache), Some(_cargo)) = (on_path("sccache"), on_path("cargo")) else {
        eprintln!("skipped: sccache or cargo is not installed");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().canonicalize().unwrap();
    let root = base.join("root");
    // The dependency sits at one absolute path for every Task, like a
    // registry crate: sccache keys an entry on the directory the compiler
    // runs in, so a crate inside a worktree is never shared between
    // worktrees.
    let repo = app_with_a_shared_dependency(&base, &root);
    let cache_dir = base.join("c");
    compiler_cache::install(
        &root,
        Some(CompilerCache {
            kind: WrapperKind::of(&sccache),
            wrapper: sccache.clone(),
            dir: cache_dir.clone(),
            max_bytes: 1 << 30,
        }),
    );
    let store = cache_dir.join("repo-1");
    let socket = store.join("s");
    let _stop = StopServer {
        sccache: sccache.clone(),
        socket: socket.clone(),
    };

    let build = |task: &str| cargo_build(&root, &repo, task, &store);

    build("task-a");
    assert_eq!(rust_hits(&sccache, &socket), 0);
    build("task-b");
    assert!(
        rust_hits(&sccache, &socket) >= 1,
        "the shared dependency is a hit for the second Task"
    );

    // Evict everything under the live server: the next build recompiles.
    let evicted = compiler_cache::evict(
        &cache_dir,
        0,
        false,
        Instant::now() + Duration::from_secs(30),
        || false,
    );
    assert!(evicted.files_removed >= 1, "{evicted:?}");
    let hits = rust_hits(&sccache, &socket);
    build("task-c");
    assert_eq!(rust_hits(&sccache, &socket), hits, "nothing left to hit");
    build("task-d");
    assert!(
        rust_hits(&sccache, &socket) > hits,
        "the entry was stored again"
    );
    compiler_cache::install(&root, None);
}

/// Stops the daemon kache started for one store, whatever the test did.
struct StopDaemon {
    kache: PathBuf,
    store: PathBuf,
}

impl Drop for StopDaemon {
    fn drop(&mut self) {
        let _ = Command::new(&self.kache)
            .args(["daemon", "stop"])
            .env("KACHE_CACHE_DIR", &self.store)
            .output();
    }
}

/// The same with kache, when it is installed: the second Task's build is
/// served from the first Task's entries although each Task has its own
/// worktree, `CARGO_TARGET_DIR` and (removed) temp directory.
#[test]
fn kache_shares_a_build_between_two_task_worktrees() {
    let (Some(kache), Some(_cargo)) = (on_path("kache"), on_path("cargo")) else {
        eprintln!("skipped: kache or cargo is not installed");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().canonicalize().unwrap();
    let root = base.join("root");
    let repo = app_with_a_shared_dependency(&base, &root);
    let cache_dir = base.join("c");
    compiler_cache::install(
        &root,
        Some(CompilerCache {
            kind: WrapperKind::of(&kache),
            wrapper: kache.clone(),
            dir: cache_dir.clone(),
            max_bytes: 1 << 30,
        }),
    );
    let store = cache_dir.join("repo-1");
    let _stop = StopDaemon {
        kache: kache.clone(),
        store: store.clone(),
    };
    let local_hits = || {
        let output = Command::new(&kache)
            .args(["stats", "--json"])
            .env("KACHE_CACHE_DIR", &store)
            .output()
            .unwrap();
        let stats: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        stats["local_hits"].as_u64().unwrap_or(0)
    };

    cargo_build(&root, &repo, "task-a", &store);
    assert_eq!(local_hits(), 0);
    cargo_build(&root, &repo, "task-b", &store);
    assert!(
        local_hits() >= 1,
        "the second Task hit the first Task's entries"
    );
    compiler_cache::install(&root, None);
}
