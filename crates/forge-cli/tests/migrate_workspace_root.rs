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
        self.forge_with(home, args, &[])
    }

    fn forge_with(&self, home: &Path, args: &[&str], environment: &[(&str, &Path)]) -> Output {
        let mut command = Command::new(env!("CARGO_BIN_EXE_forge"));
        command
            .env_clear()
            .env("HOME", home)
            .envs(environment.iter().copied())
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

    /// What an older release left behind for this data directory: a
    /// database that recorded the temp-directory root, and files in it.
    fn seed_legacy_root(&self) -> PathBuf {
        self.seed_root(&self.legacy_root())
    }

    /// The same, in any root.
    fn seed_root(&self, root: &Path) -> PathBuf {
        let database = self.path("data").join("forge.db");
        let recorded = root.to_string_lossy().into_owned();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime builds")
            .block_on(async move {
                let pool = db::create_sqlite_pool(&format!("sqlite:{}", database.display()))
                    .await
                    .expect("database opens");
                db::run_migrations(&pool).await.expect("migrations run");
                sqlx::query(
                    "INSERT INTO system_setting (key, value, updated_at) VALUES ('workspace_root', ?, ?)",
                )
                .bind(recorded)
                .bind(db::now_rfc3339())
                .execute(&pool)
                .await
                .expect("root is recorded");
                pool.close().await;
            });
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

/// With `FORGE_WORKSPACE_ROOT` (or `workspace.root`) set and no path given,
/// the move goes where that configuration starts: the next start is not
/// refused for a root that is set and differs from the recorded one.
#[test]
fn moves_to_the_configured_root_when_no_path_is_given() {
    let host = Host::new();
    let task_root = host.seed_legacy_root();
    let configured = host.path("configured-root");
    let output = host.forge_with(
        &host.path("home"),
        &["--migrate-workspace-root"],
        &[("FORGE_WORKSPACE_ROOT", &configured)],
    );
    assert_eq!(output.status.code(), Some(0), "{}", text(&output.stderr));
    assert!(configured
        .join(task_root.file_name().expect("task id"))
        .join("repo/work.txt")
        .is_file());
    assert!(!host.path("data").join("worktrees").exists());
}

/// Messages name the command with this data directory, as it must be typed.
#[test]
fn a_refusal_names_the_command_with_the_data_directory() {
    let host = Host::new();
    host.seed_legacy_root();
    let target = host.path("elsewhere");
    std::fs::create_dir_all(&target).expect("target creates");
    std::fs::write(target.join("theirs"), "keep").expect("file writes");
    // A journal for another target: the refusal names how to finish it.
    std::fs::write(
        host.path("data").join("workspace-root-migration.json"),
        format!(
            r#"{{"version":1,"source":"{}","source_spellings":["{}"],"target":"{}","copy":false,"units":[],"copied":[],"moved":[],"phase":"Moving"}}"#,
            host.legacy_root().display(),
            host.legacy_root().display(),
            host.path("data").join("worktrees").display()
        ),
    )
    .expect("journal writes");
    let output = host.migrate(&[target.to_str().expect("utf-8 path")]);
    assert_eq!(output.status.code(), Some(1));
    let stderr = text(&output.stderr);
    assert!(
        stderr.contains(&format!(
            "forge --data-dir {} --migrate-workspace-root",
            host.path("data").display()
        )),
        "{stderr}"
    );
}

/// Removes the directory this test made on the other filesystem.
struct Removed(PathBuf);

impl Drop for Removed {
    fn drop(&mut self) {
        let _ = Command::new("chmod")
            .arg("-R")
            .arg("u+rwx")
            .arg(&self.0)
            .status();
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A real copy between two filesystems, when this machine has two: the old
/// root in `/private/tmp` (made and removed here, a few kilobytes) and the
/// data directory in the test's temp directory. Skipped when both are on
/// one device.
#[cfg(unix)]
#[test]
fn moves_between_two_real_filesystems() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let host = Host::new();
    let other = Path::new("/private/tmp");
    let (Ok(there), Ok(here)) = (
        std::fs::metadata(other),
        std::fs::metadata(host.path("data")),
    ) else {
        eprintln!("skipped: /private/tmp is not available");
        return;
    };
    if there.dev() == here.dev() {
        eprintln!("skipped: /private/tmp and the test temp directory are one filesystem");
        return;
    }
    let scratch = Removed(other.join(format!(
        "forge-review-{}-{}",
        std::process::id(),
        db::new_uuid_v4()
    )));
    let root = scratch.0.join("forge").join("worktrees");
    let task_root = host.seed_root(&root);
    let git = |cwd: &Path, args: &[&str]| {
        let output = Command::new("git")
            .arg("-C")
            .arg(cwd)
            .args([
                "-c",
                "user.name=Forge Test",
                "-c",
                "user.email=test@forge.dev",
            ])
            .args(args)
            .output()
            .expect("git runs");
        assert!(
            output.status.success(),
            "git {args:?}: {}",
            text(&output.stderr)
        );
        text(&output.stdout)
    };
    // A real clone with a worktree holding uncommitted work, a link, two
    // names of one file and a read-only directory.
    let clone = root.join(".repos/repository");
    std::fs::create_dir_all(&clone).expect("clone directory");
    git(&clone, &["init", "-q", "-b", "main"]);
    std::fs::write(clone.join("README.md"), "# Test\n").expect("file writes");
    git(&clone, &["add", "-A"]);
    git(&clone, &["commit", "-q", "-m", "initial"]);
    let worktree = root.join("7b0c9f6e-1234-4c57-9d4e-1f2a3b4c5d6e/repository");
    std::fs::create_dir_all(worktree.parent().expect("task root")).expect("task root");
    git(
        &clone,
        &[
            "worktree",
            "add",
            "-q",
            "-b",
            "task",
            worktree.to_str().expect("utf-8"),
        ],
    );
    std::fs::write(worktree.join("README.md"), "# Test\nuncommitted\n").expect("edit");
    std::fs::write(worktree.join("first-name"), "one file\n").expect("file");
    std::fs::hard_link(worktree.join("first-name"), worktree.join("second-name")).expect("link");
    std::os::unix::fs::symlink("first-name", worktree.join("link")).expect("symlink");
    let frozen = worktree.join("frozen");
    std::fs::create_dir_all(&frozen).expect("directory");
    std::fs::write(frozen.join("module.txt"), "read-only\n").expect("file");
    std::fs::set_permissions(&frozen, std::fs::Permissions::from_mode(0o555)).expect("chmod");
    let status_before = git(&worktree, &["status", "--porcelain"]);

    let output = host.migrate(&[]);
    let (stdout, stderr) = (text(&output.stdout), text(&output.stderr));
    assert_eq!(output.status.code(), Some(0), "{stdout}\n{stderr}");
    assert!(
        stdout.contains("copied, compared byte for byte"),
        "{stdout}"
    );

    let target = host.path("data").join("worktrees");
    assert_ne!(
        std::fs::metadata(&target).expect("target").dev(),
        there.dev()
    );
    let moved = target.join("7b0c9f6e-1234-4c57-9d4e-1f2a3b4c5d6e/repository");
    assert_eq!(git(&moved, &["status", "--porcelain"]), status_before);
    assert!(status_before.contains(" M README.md"), "{status_before}");
    git(
        &target.join(".repos/repository"),
        &["fsck", "--connectivity-only"],
    );
    assert!(
        git(&target.join(".repos/repository"), &["worktree", "list"])
            .contains(&moved.display().to_string())
    );
    let (first, second) = (
        std::fs::metadata(moved.join("first-name")).expect("first"),
        std::fs::metadata(moved.join("second-name")).expect("second"),
    );
    assert_eq!((first.ino(), first.nlink()), (second.ino(), 2));
    assert_eq!(
        std::fs::read_link(moved.join("link")).expect("link"),
        Path::new("first-name")
    );
    assert_eq!(
        std::fs::metadata(moved.join("frozen"))
            .expect("frozen")
            .permissions()
            .mode()
            & 0o777,
        0o555
    );
    assert_eq!(
        std::fs::read_to_string(moved.join("frozen/module.txt")).expect("module"),
        "read-only\n"
    );
    assert!(target
        .join(task_root.file_name().expect("task id"))
        .join("repo/work.txt")
        .is_file());
    // The old root holds the marker and the emptied `.forge` directory.
    let mut left: Vec<_> = std::fs::read_dir(&root)
        .expect("old root")
        .flatten()
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect();
    left.sort();
    assert_eq!(left, [".forge", "MOVED"]);
    assert_eq!(
        std::fs::read_dir(root.join(".forge"))
            .expect("old .forge")
            .count(),
        0
    );
}
