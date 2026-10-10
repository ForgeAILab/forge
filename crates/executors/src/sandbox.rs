//! The Task-root layout and the environment every run gets from it.
//!
//! ```text
//! <task root>/
//!   <worktree>/              the Git worktree (unchanged)
//!   .forge-outbox/<exec>/    execution outbox (unchanged)
//!   .forge-task/             reserved; created only by the workspace owner
//!     tmp/<run key>/         TMPDIR, TMP, TEMP of one run; removed when it settles
//!     home/<family>/         config homes Forge populates (codex, gemini)
//!     build/cargo/           per-Task CARGO_TARGET_DIR
//! ```
//!
//! A Task root is *Forge-shaped* only when the workspace owner reserved it
//! ([`TaskRoot::reserve`]): the `.forge-task` directory is the proof. A
//! worktree whose parent carries no such directory (a legacy recorded path, a
//! user directory, an exact-commit check checkout) gets [`SandboxEnv::none`]
//! and runs exactly as before; nothing is ever created beside it.
//!
//! Every path is a pure function of the worktree path and the run id, so the
//! server and a daemon derive the same directories without a protocol change,
//! and a run can be settled by a caller that never saw its [`SandboxEnv`].

use std::{
    collections::BTreeMap,
    ffi::{OsStr, OsString},
    fs, io,
    path::{Path, PathBuf},
};
use tokio::process::Command;

/// The reserved directory beside the worktree. A repository may not use this
/// name: its worktree would be the reserved directory.
pub const TASK_DIR_NAME: &str = ".forge-task";
/// Per-run temp directories that do not fit [`SAFE_TMPDIR_BYTES`] inside the
/// Task root live here, beside the Task roots.
pub const SHORT_TMP_DIR_NAME: &str = ".forge-tmp";
/// Longest `TMPDIR` Forge will hand to a run. A Unix socket path is limited
/// to 104 bytes on macOS (108 on Linux) and tools create sockets under
/// `TMPDIR`; this leaves 31 bytes for `/<name>`.
pub const SAFE_TMPDIR_BYTES: usize = 72;
const RUN_KEY_CHARS: usize = 10;
const TEMP_KEYS: [&str; 3] = ["TMPDIR", "TMP", "TEMP"];

/// What a run is, for the parts of the environment that depend on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunPurpose {
    /// An agent execution (CLI adapter, shell executor); settles with the execution.
    Execution,
    /// One command of the native command tool.
    Command,
    /// One lifecycle script hook.
    Hook,
    /// One check or CI command running in the Task worktree.
    Check,
    /// An environment probe or owner-local workspace command.
    Probe,
}

/// Build-output variables Forge points into the Task root: `(variable,
/// directory under .forge-task/build)`. Rust only. Other toolchains keep
/// their output where they put it today, and shared caches (`GOCACHE`,
/// `npm_config_cache`, `XDG_CACHE_HOME`) are never redirected.
pub const BUILD_DIR_TABLE: [(&str, &str); 1] = [("CARGO_TARGET_DIR", "cargo")];

/// A Task root the workspace owner reserved for Forge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskRoot(PathBuf);

impl TaskRoot {
    /// Mark `task_root` as Forge's by creating `.forge-task` in it. Only the
    /// workspace owner calls this, after it has confined the Task root to its
    /// own workspace root.
    pub fn reserve(task_root: &Path) -> io::Result<Self> {
        let reserved = task_root.join(TASK_DIR_NAME);
        create_private_dir(&reserved)?;
        if !is_real_dir(&reserved) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{} is not a directory", reserved.display()),
            ));
        }
        Ok(Self(task_root.to_path_buf()))
    }

    /// The reserved Task root of `worktree`, or `None` when its parent was
    /// never reserved or either directory is a symbolic link.
    pub fn of_worktree(worktree: &Path) -> Option<Self> {
        let task_root = worktree.parent()?;
        (is_real_dir(worktree) && is_real_dir(task_root)).then_some(())?;
        Self::at(task_root)
    }

    /// `task_root` itself, when it is reserved.
    pub fn at(task_root: &Path) -> Option<Self> {
        is_real_dir(&task_root.join(TASK_DIR_NAME)).then(|| Self(task_root.to_path_buf()))
    }

    pub fn path(&self) -> &Path {
        &self.0
    }

    /// `<task root>/.forge-task/home/<family>`: a config home Forge populates.
    pub fn home(&self, family: &str) -> PathBuf {
        self.0.join(TASK_DIR_NAME).join("home").join(family)
    }

    /// `<task root>/.forge-task/build`.
    pub fn build_dir(&self) -> PathBuf {
        self.0.join(TASK_DIR_NAME).join("build")
    }

    /// The temp directory of run `run_id`.
    ///
    /// Inside the Task root when that path is short enough for a socket,
    /// otherwise the short directory beside the Task roots; `None` when even
    /// that is too long or the run id has no usable characters, in which case
    /// the run keeps the temp directory it would have had without Forge.
    pub fn run_tmp(&self, run_id: &str) -> Option<PathBuf> {
        let key = run_key(run_id)?;
        let inside = self.0.join(TASK_DIR_NAME).join("tmp").join(&key);
        if fits(&inside) {
            return Some(inside);
        }
        let short = self.0.parent()?.join(SHORT_TMP_DIR_NAME).join(key);
        fits(&short).then_some(short)
    }
}

/// The environment Forge adds to one run from its Task root.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SandboxEnv {
    tmp: Option<PathBuf>,
    build: Vec<(&'static str, PathBuf)>,
}

impl SandboxEnv {
    /// No Task root: the run keeps today's environment.
    pub fn none() -> Self {
        Self::default()
    }

    /// The environment of run `run_id` in `task_root`.
    pub fn for_task_root(task_root: &TaskRoot, run_id: &str, purpose: RunPurpose) -> Self {
        Self {
            tmp: task_root.run_tmp(run_id),
            build: if purpose == RunPurpose::Probe {
                Vec::new()
            } else {
                Self::build_dirs(task_root)
            },
        }
    }

    /// A scoped environment for one bounded command in `worktree` that has
    /// no run id of its own.
    pub fn for_command(worktree: &Path, purpose: RunPurpose) -> RunScope {
        Self::for_run(worktree, &fresh_run_id(), purpose).scoped()
    }

    /// [`Self::for_task_root`] for the Task root of `worktree`, or
    /// [`Self::none`] when that root is not Forge-shaped.
    pub fn for_run(worktree: &Path, run_id: &str, purpose: RunPurpose) -> Self {
        TaskRoot::of_worktree(worktree)
            .map(|root| Self::for_task_root(&root, run_id, purpose))
            .unwrap_or_default()
    }

    /// The Task-level part only (build directories, no per-run temp
    /// directory), for a command whose caller owns no run scope.
    pub fn for_task(worktree: &Path) -> Self {
        TaskRoot::of_worktree(worktree)
            .map(|root| Self {
                tmp: None,
                build: Self::build_dirs(&root),
            })
            .unwrap_or_default()
    }

    fn build_dirs(task_root: &TaskRoot) -> Vec<(&'static str, PathBuf)> {
        BUILD_DIR_TABLE
            .iter()
            .map(|(key, dir)| (*key, task_root.build_dir().join(dir)))
            .collect()
    }

    /// The per-run temp directory, when this run has one.
    pub fn tmp_dir(&self) -> Option<&Path> {
        self.tmp.as_deref()
    }

    /// The directory `key` (a [`BUILD_DIR_TABLE`] variable) points at.
    pub fn build_dir(&self, key: &str) -> Option<&Path> {
        self.build
            .iter()
            .find(|(name, _)| *name == key)
            .map(|(_, path)| path.as_path())
    }

    /// Create the per-run temp directory. A run whose directory cannot be
    /// created keeps today's temp directory instead of failing.
    #[must_use = "the returned environment is the one to apply"]
    pub fn prepared(mut self) -> Self {
        if let Some(tmp) = &self.tmp {
            if let Err(error) = prepare_run_tmp(tmp) {
                tracing::warn!(path = %tmp.display(), %error, "per-run temp directory could not be created; run keeps the inherited one");
                self.tmp = None;
            }
        }
        self
    }

    /// Remove the per-run temp directory. Idempotent.
    pub fn settle(&self) {
        if let Some(tmp) = &self.tmp {
            remove_tree(tmp);
        }
    }

    /// Create the temp directory now and remove it when the guard drops:
    /// for a run that starts and ends inside one function.
    pub fn scoped(self) -> RunScope {
        RunScope(self.prepared())
    }

    /// The variables this environment sets on a command, given the Project
    /// environment and the operator's process environment.
    ///
    /// - `TMPDIR`, `TMP`, `TEMP`: the per-run directory, over anything
    ///   inherited. A key the Project environment declares is left to it.
    /// - Build directories: Project > command > operator process > Forge. A
    ///   Project value that is empty disables the redirect for that key.
    pub fn variables(
        &self,
        project: &BTreeMap<String, String>,
        preset: impl Fn(&str) -> bool,
        operator: impl Fn(&str) -> Option<OsString>,
    ) -> Vec<(&'static str, Option<OsString>)> {
        let mut vars = Vec::new();
        if let Some(tmp) = &self.tmp {
            for key in TEMP_KEYS {
                if !project.contains_key(key) {
                    vars.push((key, Some(tmp.clone().into_os_string())));
                }
            }
        }
        for (key, path) in &self.build {
            match project.get(*key) {
                // Disabled: an empty value must not reach the toolchain.
                Some(value) if value.is_empty() => {
                    vars.push((key, operator(key).filter(|value| !value.is_empty())));
                }
                Some(_) => {}
                None if preset(key) || operator(key).is_some_and(|value| !value.is_empty()) => {}
                None => vars.push((key, Some(path.clone().into_os_string()))),
            }
        }
        vars
    }

    pub(crate) fn apply_to(&self, command: &mut Command, project: &BTreeMap<String, String>) {
        self.apply_with(command, project, |key| std::env::var_os(key));
    }

    pub(crate) fn apply_with(
        &self,
        command: &mut Command,
        project: &BTreeMap<String, String>,
        operator: impl Fn(&str) -> Option<OsString>,
    ) {
        let preset = command
            .as_std()
            .get_envs()
            .filter(|(_, value)| value.is_some_and(|value| !value.is_empty()))
            .map(|(key, _)| key.to_owned())
            .collect::<Vec<_>>();
        let vars = self.variables(
            project,
            |key| preset.iter().any(|name| name == OsStr::new(key)),
            operator,
        );
        for (key, value) in vars {
            match value {
                Some(value) => command.env(key, value),
                None => command.env_remove(key),
            };
        }
    }
}

/// A prepared [`SandboxEnv`] whose temp directory is removed on drop.
#[derive(Debug)]
pub struct RunScope(SandboxEnv);

impl RunScope {
    pub fn env(&self) -> &SandboxEnv {
        &self.0
    }
}

impl Drop for RunScope {
    fn drop(&mut self) {
        self.0.settle();
    }
}

/// Remove the temp directory of run `run_id` of the Task root of `worktree`.
/// For callers that settle a run they did not start (execution settlement,
/// cancellation, crash recovery). The worktree itself may already be gone.
pub fn settle_run(worktree: &Path, run_id: &str) {
    let Some(task_root) = worktree.parent().and_then(TaskRoot::at) else {
        return;
    };
    if let Some(tmp) = task_root.run_tmp(run_id) {
        remove_tree(&tmp);
    }
}

/// Remove the temp directories of runs that are no longer alive.
///
/// `task_roots` is the directory whose children are Task roots (the server
/// workspace root; `<root>/.forge/workspaces` on a daemon). Every per-run
/// directory under a reserved Task root and under the short directory is
/// removed unless its key belongs to a run id in `live`. Returns the number
/// of directories removed.
pub fn sweep_dead_runs<'a>(task_roots: &Path, live: impl IntoIterator<Item = &'a str>) -> usize {
    let live: Vec<String> = live.into_iter().filter_map(run_key).collect();
    let mut parents = vec![task_roots.join(SHORT_TMP_DIR_NAME)];
    if let Ok(entries) = fs::read_dir(task_roots) {
        parents.extend(
            entries
                .flatten()
                .filter_map(|entry| TaskRoot::at(&entry.path()))
                .map(|root| root.0.join(TASK_DIR_NAME).join("tmp")),
        );
    }
    let mut removed = 0;
    for parent in parents {
        if !is_real_dir(&parent) {
            continue;
        }
        let Ok(entries) = fs::read_dir(&parent) else {
            continue;
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            if live.iter().any(|key| OsStr::new(key) == name) {
                continue;
            }
            remove_tree(&entry.path());
            removed += 1;
        }
    }
    removed
}

/// A run id for a run that has no identity of its own (one hook, one check
/// command, one tool command).
pub fn fresh_run_id() -> String {
    use std::hash::{BuildHasher, Hasher};
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u64(NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
    format!("{:016x}", hasher.finish())
}

/// A short, path-safe key for a run id: its first alphanumeric characters.
fn run_key(run_id: &str) -> Option<String> {
    let key: String = run_id
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .take(RUN_KEY_CHARS)
        .collect::<String>()
        .to_ascii_lowercase();
    (!key.is_empty()).then_some(key)
}

fn fits(path: &Path) -> bool {
    path.as_os_str().len() <= SAFE_TMPDIR_BYTES
}

fn is_real_dir(path: &Path) -> bool {
    fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_dir())
}

fn create_private_dir(path: &Path) -> io::Result<()> {
    let mut builder = fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    match builder.create(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(error),
    }
}

/// Create `<parent>/<key>` and its Forge-owned parent, never through a link
/// and never recursively through a component Forge does not own.
fn prepare_run_tmp(tmp: &Path) -> io::Result<()> {
    let parent = tmp.parent().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "temp directory has no parent")
    })?;
    // `.forge-task` exists (the root is reserved) and `.forge-tmp` sits in
    // the directory that holds the Task roots, so one level is enough.
    create_private_dir(parent)?;
    if !is_real_dir(parent) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{} is not a directory", parent.display()),
        ));
    }
    // A leftover of an earlier run with the same key is not this run's.
    remove_tree(tmp);
    let mut builder = fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(tmp)
}

/// Remove a directory tree, including directories a run left read-only.
/// Symbolic links are removed, never followed.
fn remove_tree(path: &Path) {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return;
    };
    if !metadata.file_type().is_dir() {
        let _ = fs::remove_file(path);
        return;
    }
    if fs::remove_dir_all(path).is_ok() {
        return;
    }
    #[cfg(unix)]
    make_dirs_owner_writable(path);
    if let Err(error) = fs::remove_dir_all(path) {
        if error.kind() != io::ErrorKind::NotFound {
            tracing::warn!(path = %path.display(), %error, "per-run temp directory could not be removed");
        }
    }
}

#[cfg(unix)]
fn make_dirs_owner_writable(root: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        let Ok(metadata) = fs::symlink_metadata(&dir) else {
            continue;
        };
        let mode = metadata.permissions().mode();
        if mode & 0o700 != 0o700 {
            let _ = fs::set_permissions(&dir, fs::Permissions::from_mode(mode | 0o700));
        }
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            if entry
                .file_type()
                .is_ok_and(|kind| kind.is_dir() && !kind.is_symlink())
            {
                pending.push(entry.path());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run_process::apply_sandboxed;

    /// `<tmp>/t/repo` with a reserved Task root, short enough for the
    /// in-root temp directory under a normal test temp dir.
    fn reserved() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let worktree = dir.path().join("t").join("repo");
        fs::create_dir_all(&worktree).unwrap();
        TaskRoot::reserve(worktree.parent().unwrap()).unwrap();
        (dir, worktree)
    }

    async fn shell(
        sandbox: &SandboxEnv,
        project: &BTreeMap<String, String>,
        script: &str,
    ) -> String {
        let mut command = Command::new("sh");
        command.arg("-c").arg(script).env("TMPDIR", "/inherited");
        apply_sandboxed(&mut command, project, sandbox);
        let output = command.output().await.unwrap();
        assert!(output.status.success());
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    }

    #[tokio::test]
    async fn run_sees_tmpdir_in_the_task_root_and_it_is_gone_when_the_run_settles() {
        let (dir, worktree) = reserved();
        let task_root = worktree.parent().unwrap();
        assert!(
            task_root.as_os_str().len() + 30 <= SAFE_TMPDIR_BYTES,
            "test temp dir {} is too long for the in-root case",
            dir.path().display()
        );
        let sandbox = SandboxEnv::for_run(&worktree, "9f2c1d7e-aaaa", RunPurpose::Hook).prepared();
        let tmp = sandbox.tmp_dir().unwrap().to_path_buf();
        assert_eq!(tmp, task_root.join(".forge-task/tmp/9f2c1d7eaa"));
        // An inherited TMPDIR (every CLI adapter copies the server's) loses.
        let seen = shell(
            &sandbox,
            &BTreeMap::new(),
            "touch \"$TMPDIR/file\" && printf '%s|%s|%s' \"$TMPDIR\" \"$TMP\" \"$TEMP\"",
        )
        .await;
        let tmp_text = tmp.to_str().unwrap();
        assert_eq!(seen, format!("{tmp_text}|{tmp_text}|{tmp_text}"));
        assert!(tmp.join("file").exists());
        sandbox.settle();
        assert!(!tmp.exists());
        sandbox.settle();
    }

    #[tokio::test]
    async fn scope_removes_tmpdir_after_failure_and_read_only_leftovers() {
        let (_dir, worktree) = reserved();
        let scope = SandboxEnv::for_run(&worktree, "run-1", RunPurpose::Check).scoped();
        let tmp = scope.env().tmp_dir().unwrap().to_path_buf();
        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg("mkdir -p \"$TMPDIR/ro/inner\" && touch \"$TMPDIR/ro/inner/f\" && chmod 500 \"$TMPDIR/ro/inner\" \"$TMPDIR/ro\" && exit 7");
        apply_sandboxed(&mut command, &BTreeMap::new(), scope.env());
        assert_eq!(command.status().await.unwrap().code(), Some(7));
        assert!(tmp.join("ro/inner/f").exists());
        drop(scope);
        assert!(!tmp.exists());
    }

    #[test]
    fn unreserved_root_gets_nothing_and_nothing_is_created_beside_it() {
        let dir = tempfile::tempdir().unwrap();
        let worktree = dir.path().join("user").join("repo");
        fs::create_dir_all(&worktree).unwrap();
        let sandbox = SandboxEnv::for_run(&worktree, "run-1", RunPurpose::Execution).prepared();
        assert_eq!(sandbox, SandboxEnv::none());
        settle_run(&worktree, "run-1");
        let mut command = Command::new("true");
        apply_sandboxed(&mut command, &BTreeMap::new(), &sandbox);
        assert!(!command.as_std().get_envs().any(|(key, _)| {
            TEMP_KEYS.iter().any(|name| key == *name) || key == "CARGO_TARGET_DIR"
        }));
        let siblings: Vec<_> = fs::read_dir(dir.path().join("user"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert_eq!(siblings, ["repo"]);
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn linked_task_directory_is_not_a_reserved_root() {
        let dir = tempfile::tempdir().unwrap();
        let worktree = dir.path().join("t").join("repo");
        fs::create_dir_all(&worktree).unwrap();
        let outside = dir.path().join("outside");
        fs::create_dir_all(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, dir.path().join("t").join(TASK_DIR_NAME)).unwrap();
        assert!(TaskRoot::of_worktree(&worktree).is_none());
        assert!(TaskRoot::reserve(worktree.parent().unwrap()).is_err());
    }

    #[test]
    fn long_task_root_falls_back_to_the_short_directory_then_to_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let task = "0d9d6a3e-5f0b-4c57-9d4e-1f2a3b4c5d6e";
        let worktree = dir.path().join(task).join("repository-name");
        fs::create_dir_all(&worktree).unwrap();
        let root = TaskRoot::reserve(worktree.parent().unwrap()).unwrap();
        let tmp = root.run_tmp("7b0c9f6e-1234").unwrap();
        assert_eq!(tmp, dir.path().join(".forge-tmp/7b0c9f6e12"));
        assert!(tmp.as_os_str().len() <= SAFE_TMPDIR_BYTES);
        let sandbox =
            SandboxEnv::for_task_root(&root, "7b0c9f6e-1234", RunPurpose::Execution).prepared();
        assert!(tmp.is_dir());
        settle_run(&worktree, "7b0c9f6e-1234");
        assert!(!tmp.exists());
        drop(sandbox);

        // A socket in the fallback directory binds; that is the point of it.
        #[cfg(unix)]
        {
            let scope = SandboxEnv::for_task_root(&root, "sock", RunPurpose::Hook).scoped();
            let path = scope
                .env()
                .tmp_dir()
                .unwrap()
                .join("tool-ipc-0123456789.sock");
            std::os::unix::net::UnixListener::bind(&path).expect("socket binds under TMPDIR");
        }

        let deep = dir.path().join("a".repeat(SAFE_TMPDIR_BYTES)).join(task);
        fs::create_dir_all(deep.join("repo")).unwrap();
        let deep_root = TaskRoot::reserve(&deep).unwrap();
        assert_eq!(deep_root.run_tmp("run"), None);
        let sandbox = SandboxEnv::for_task_root(&deep_root, "run", RunPurpose::Execution);
        assert_eq!(sandbox.tmp_dir(), None);
        assert!(sandbox.build_dir("CARGO_TARGET_DIR").is_some());
    }

    #[test]
    fn cargo_target_dir_is_set_overridden_and_disabled() {
        let (_dir, worktree) = reserved();
        let sandbox = SandboxEnv::for_run(&worktree, "run", RunPurpose::Execution);
        let build = worktree.parent().unwrap().join(".forge-task/build/cargo");
        let target = |project: &[(&str, &str)], preset: bool, operator: Option<&str>| {
            let project = project
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect();
            sandbox
                .variables(&project, |_| preset, |_| operator.map(Into::into))
                .into_iter()
                .find(|(key, _)| *key == "CARGO_TARGET_DIR")
                .map(|(_, value)| value)
        };
        // Set by Forge.
        assert_eq!(target(&[], false, None), Some(Some(build.into_os_string())));
        // The command, the operator and the Project environment each win.
        assert_eq!(target(&[], true, None), None);
        assert_eq!(target(&[], false, Some("/operator")), None);
        assert_eq!(
            target(&[("CARGO_TARGET_DIR", "/project")], false, None),
            None
        );
        // Disabled: unset, or back to the operator's.
        assert_eq!(target(&[("CARGO_TARGET_DIR", "")], false, None), Some(None));
        assert_eq!(
            target(&[("CARGO_TARGET_DIR", "")], false, Some("/operator")),
            Some(Some("/operator".into()))
        );
        // A probe has no build directory; a Project TMPDIR is the Project's.
        let probe = SandboxEnv::for_run(&worktree, "run", RunPurpose::Probe);
        assert_eq!(probe.build_dir("CARGO_TARGET_DIR"), None);
        let project = BTreeMap::from([("TMPDIR".to_owned(), "/project-tmp".to_owned())]);
        let keys: Vec<_> = probe
            .variables(&project, |_| false, |_| None)
            .into_iter()
            .map(|(key, _)| key)
            .collect();
        assert_eq!(keys, ["TMP", "TEMP"]);
    }

    #[tokio::test]
    async fn real_process_sees_the_build_directory_unless_disabled() {
        let (_dir, worktree) = reserved();
        let sandbox = SandboxEnv::for_task(&worktree);
        let run = |project: BTreeMap<String, String>| {
            let sandbox = sandbox.clone();
            async move {
                let mut command = Command::new("sh");
                command
                    .arg("-c")
                    .arg("printf '%s' \"${CARGO_TARGET_DIR-unset}\"")
                    .env_remove("CARGO_TARGET_DIR")
                    .envs(&project);
                sandbox.apply_with(&mut command, &project, |_| None);
                String::from_utf8(command.output().await.unwrap().stdout).unwrap()
            }
        };
        let build = worktree.parent().unwrap().join(".forge-task/build/cargo");
        assert_eq!(run(BTreeMap::new()).await, build.to_str().unwrap());
        let over = BTreeMap::from([("CARGO_TARGET_DIR".to_owned(), "/project".to_owned())]);
        assert_eq!(run(over).await, "/project");
        let off = BTreeMap::from([("CARGO_TARGET_DIR".to_owned(), String::new())]);
        assert_eq!(run(off).await, "unset");
    }

    #[test]
    fn sweep_removes_dead_runs_and_keeps_live_ones() {
        let dir = tempfile::tempdir().unwrap();
        let short = dir.path().join("t1");
        let long = dir
            .path()
            .join("0d9d6a3e-5f0b-4c57-9d4e-1f2a3b4c5d6e-long-enough-name");
        let unreserved = dir.path().join("user");
        for root in [&short, &long, &unreserved] {
            fs::create_dir_all(root.join("repo")).unwrap();
        }
        let short_root = TaskRoot::reserve(&short).unwrap();
        let long_root = TaskRoot::reserve(&long).unwrap();
        let mut dirs = Vec::new();
        for (root, run) in [
            (&short_root, "dead-run"),
            (&short_root, "live-run"),
            (&long_root, "dead-long"),
            (&long_root, "live-long"),
        ] {
            let sandbox = SandboxEnv::for_task_root(root, run, RunPurpose::Execution).prepared();
            dirs.push(sandbox.tmp_dir().unwrap().to_path_buf());
        }
        assert!(dirs[2].starts_with(dir.path().join(SHORT_TMP_DIR_NAME)));
        fs::create_dir_all(unreserved.join("tmp/kept")).unwrap();
        assert_eq!(sweep_dead_runs(dir.path(), ["live-run", "live-long"]), 2);
        assert!(!dirs[0].exists() && dirs[1].exists());
        assert!(!dirs[2].exists() && dirs[3].exists());
        assert!(unreserved.join("tmp/kept").exists());
    }
}
