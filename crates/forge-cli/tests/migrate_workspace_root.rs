//! `forge --migrate-workspace-root` as an operator runs it. Every directory
//! here is a temp directory, including the child's `HOME` and `TMPDIR`: the
//! real ones are never read.

use std::{
    path::{Path, PathBuf},
    process::{Command, Output},
};
use tempfile::TempDir;

struct Host {
    dir: TempDir,
}

impl Host {
    fn new() -> Self {
        let host = Self {
            dir: TempDir::new().expect("temp dir"),
        };
        for name in ["home", "tmp", "data"] {
            std::fs::create_dir_all(host.path(name)).expect("directory creates");
        }
        host
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir
            .path()
            .canonicalize()
            .expect("temp dir resolves")
            .join(name)
    }

    /// The root an older release used: `<system temp>/forge/worktrees`.
    fn legacy_root(&self) -> PathBuf {
        self.path("tmp").join("forge").join("worktrees")
    }

    fn forge(&self, home: &Path, args: &[&str]) -> Output {
        let mut command = Command::new(env!("CARGO_BIN_EXE_forge"));
        command
            .env_clear()
            .env("HOME", home)
            .env("TMPDIR", self.path("tmp"))
            .arg("--data-dir")
            .arg(self.path("data"))
            .args(args)
            .current_dir(self.dir.path());
        if let Some(path) = std::env::var_os("PATH") {
            command.env("PATH", path);
        }
        command.output().expect("forge runs")
    }

    fn migrate(&self, args: &[&str]) -> Output {
        let mut all = vec!["--migrate-workspace-root"];
        all.extend_from_slice(args);
        self.forge(&self.path("home"), &all)
    }

    /// What an older release left behind for this data directory.
    fn seed_legacy_root(&self) -> PathBuf {
        let root = self.legacy_root();
        let task_root = root.join("3f0c2b0e-6d53-4b7e-9d0c-0d5a4f3f8a11");
        std::fs::create_dir_all(task_root.join("repo")).expect("task root creates");
        std::fs::write(task_root.join("repo/work.txt"), "uncommitted work\n").expect("file writes");
        std::fs::create_dir_all(root.join(".repos")).expect("clone directory creates");
        std::fs::create_dir_all(root.join(".forge/logs/project/task")).expect("logs create");
        std::fs::write(root.join(".forge/logs/project/task/run.jsonl"), "{}\n")
            .expect("log writes");
        task_root
    }
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

#[test]
fn moves_a_temp_directory_root_into_the_data_directory_and_says_what_it_did() {
    let host = Host::new();
    let task_root = host.seed_legacy_root();
    let output = host.migrate(&[]);
    let (stdout, stderr) = (text(&output.stdout), text(&output.stderr));
    assert_eq!(output.status.code(), Some(0), "{stdout}\n{stderr}");

    let target = host.path("data").join("worktrees");
    let moved = target.join(task_root.file_name().expect("task id"));
    assert_eq!(
        std::fs::read_to_string(moved.join("repo/work.txt")).expect("moved file"),
        "uncommitted work\n"
    );
    assert!(target.join(".forge/logs/project/task/run.jsonl").is_file());
    assert!(target.join(".repos").is_dir());
    assert!(!task_root.exists());
    assert!(host.legacy_root().join("MOVED").is_file());
    assert!(!host
        .path("data")
        .join("workspace-root-migration.json")
        .exists());
    assert!(stdout.contains("Workspace root moved"), "{stdout}");
    assert!(stdout.contains(&target.display().to_string()), "{stdout}");
    assert!(
        stdout.contains("Daemon-owned workspaces were not touched"),
        "{stdout}"
    );

    // Asking again finds nothing to do and is not an error.
    let again = host.migrate(&[]);
    assert_eq!(again.status.code(), Some(0), "{}", text(&again.stderr));
    assert!(text(&again.stdout).contains("Nothing to move"));
}

#[test]
fn a_fresh_data_directory_has_nothing_to_move() {
    let host = Host::new();
    let output = host.migrate(&[]);
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
    assert!(text(&output.stdout).contains("Nothing to move"));
    assert!(!host.path("data").join("worktrees").exists());
}

#[test]
fn refuses_while_a_server_holds_the_data_directory() {
    let host = Host::new();
    let task_root = host.seed_legacy_root();
    // The lock a running server (or another maintenance command) holds.
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(host.path("data").join("runtime.lock"))
        .expect("lock file opens");
    fs2::FileExt::try_lock_exclusive(&lock).expect("lock is free");

    let output = host.migrate(&[]);
    assert_eq!(output.status.code(), Some(1));
    assert!(
        text(&output.stderr).contains("another Forge runtime"),
        "{}",
        text(&output.stderr)
    );
    assert!(task_root.join("repo/work.txt").is_file());
    assert!(!host.path("data").join("worktrees").exists());
    assert!(!host.path("data").join("forge.db").exists());
}

#[test]
fn refuses_a_target_that_is_not_empty_and_exits_non_zero() {
    let host = Host::new();
    let task_root = host.seed_legacy_root();
    let target = host.path("elsewhere");
    std::fs::create_dir_all(&target).expect("target creates");
    std::fs::write(target.join("theirs"), "keep").expect("file writes");
    let output = host.migrate(&[target.to_str().expect("utf-8 path")]);
    assert_eq!(output.status.code(), Some(1));
    assert!(
        text(&output.stderr).contains("not empty"),
        "{}",
        text(&output.stderr)
    );
    assert!(task_root.join("repo/work.txt").is_file());
    assert!(!host.legacy_root().join("MOVED").exists());
}

#[test]
fn refuses_a_target_that_can_never_be_a_workspace_root() {
    let host = Host::new();
    let task_root = host.seed_legacy_root();
    // The home directory itself: the garbage collector never sweeps it, so
    // the move never makes it a root.
    let home = host.path("empty-home");
    let output = host.forge(
        &home,
        &[
            "--migrate-workspace-root",
            home.to_str().expect("utf-8 path"),
        ],
    );
    assert_eq!(output.status.code(), Some(1));
    let stderr = text(&output.stderr);
    assert!(stderr.contains("cannot be a workspace root"), "{stderr}");
    assert!(stderr.contains("home directory"), "{stderr}");
    assert!(task_root.join("repo/work.txt").is_file());
    // The directory the refused run created is gone again.
    assert!(!home.exists());
}

#[test]
fn refuses_a_target_inside_the_old_root() {
    let host = Host::new();
    let task_root = host.seed_legacy_root();
    let inside = host.legacy_root().join("new-root");
    let output = host.migrate(&[inside.to_str().expect("utf-8 path")]);
    assert_eq!(output.status.code(), Some(1));
    assert!(
        text(&output.stderr).contains("inside the old one"),
        "{}",
        text(&output.stderr)
    );
    assert!(!inside.exists());
    assert!(task_root.join("repo/work.txt").is_file());
}
