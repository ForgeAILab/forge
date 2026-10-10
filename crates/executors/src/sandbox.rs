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
    collections::{BTreeMap, HashMap},
    ffi::{OsStr, OsString},
    fs, io,
    path::{Path, PathBuf},
    sync::{LazyLock, Mutex},
    time::{Instant, SystemTime},
};
use crate::compiler_cache::CacheEnv;
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

/// Per-run temp directories this process created and has not yet removed.
/// [`sweep_stale_runs`] never touches one: a hook, check, tool command or
/// execution running in this process is live whatever any table says.
///
/// Each maps to the Task root it belongs to (the per-run directory itself may
/// live in the short directory beside the Task roots), so the garbage
/// collector can tell which Task roots have a run in this process.
static LIVE_RUN_DIRS: LazyLock<Mutex<HashMap<PathBuf, LiveRun>>> = LazyLock::new(Mutex::default);

/// One live run: its Task root, and the repository store of the shared
/// compiler cache it was handed (the repository it builds, for the
/// collector of that cache).
#[derive(Debug, Clone)]
struct LiveRun {
    root: PathBuf,
    cache: Option<PathBuf>,
}
/// First use of this module by the process. A directory modified after it was
/// not left by a previous process, so the sweep leaves it alone.
static PROCESS_START: LazyLock<SystemTime> = LazyLock::new(SystemTime::now);

fn live_run_dirs() -> std::sync::MutexGuard<'static, HashMap<PathBuf, LiveRun>> {
    LIVE_RUN_DIRS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Whether a hook, check, tool command, probe or execution started by this
/// process is running in `task_root` right now. Covers every run that got a
/// per-run temp directory; a run without one is not visible here, so callers
/// that reclaim something a run uses add their own table as well.
pub fn has_live_run_in(task_root: &Path) -> bool {
    live_run_dirs().values().any(|run| run.root == task_root)
}

/// Run `reclaim` unless a run of this process is live in `task_root`, holding
/// the lock every run start takes ([`SandboxEnv::prepared`]) for the length
/// of the call. A run therefore starts either before the check (and nothing
/// is reclaimed) or after `reclaim` returned (and it gets its directories
/// made again): never in between. `reclaim` must be short, a rename and not
/// a delete, and must not start or settle a run.
pub fn unless_live_run_in<T>(task_root: &Path, reclaim: impl FnOnce() -> T) -> Option<T> {
    let live = live_run_dirs();
    if live.values().any(|run| run.root == task_root) {
        return None;
    }
    Some(reclaim())
}

/// [`unless_live_run_in`] for one repository store of the shared compiler
/// cache: run `reclaim` unless a run of this process was handed `store`.
pub fn unless_live_cache_run_in<T>(store: &Path, reclaim: impl FnOnce() -> T) -> Option<T> {
    let live = live_run_dirs();
    if live.values().any(|run| run.cache.as_deref() == Some(store)) {
        return None;
    }
    Some(reclaim())
}

/// When this process first used the sandbox. Nothing it runs is older.
pub fn process_start() -> SystemTime {
    *PROCESS_START
}

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

    /// [`Self::home`], created one level at a time and never through a
    /// link or a file: `None` when `home` or `home/<family>` exists as
    /// anything but a real directory, so the caller keeps its fallback.
    pub fn prepare_home(&self, family: &str) -> Option<PathBuf> {
        let homes = self.0.join(TASK_DIR_NAME).join("home");
        let home = homes.join(family);
        for dir in [&homes, &home] {
            create_private_dir(dir).ok()?;
            is_real_dir(dir).then_some(())?;
        }
        Some(home)
    }

    /// `<task root>/.forge-task/build`.
    pub fn build_dir(&self) -> PathBuf {
        self.0.join(TASK_DIR_NAME).join("build")
    }

    /// Whether `.forge-task/build` is a real directory Forge can point a
    /// toolchain at (created here when missing). A link or a file planted
    /// there is never followed: the run builds where it did before.
    fn build_dir_is_usable(&self) -> bool {
        let build = self.build_dir();
        create_private_dir(&build).is_ok() && is_real_dir(&build)
    }

    /// The live-run registry name of run `run_id` when it has no temp
    /// directory. Only a map key: nothing is created there.
    fn live_key(&self, run_id: &str) -> PathBuf {
        self.0
            .join(TASK_DIR_NAME)
            .join("live")
            .join(run_key(run_id).unwrap_or_else(fresh_run_id))
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
    /// The Task root this run belongs to, for the live-run registry.
    root: Option<PathBuf>,
    tmp: Option<PathBuf>,
    build: Vec<(&'static str, PathBuf)>,
    /// This run's entry in the live-run registry when it has no per-run
    /// temp directory to be registered by: a name under the Task root that
    /// is never created on disk.
    live: Option<PathBuf>,
    /// The shared compiler cache, when the operator configured one and it
    /// is usable for this worktree's repository right now.
    cache: Option<CacheEnv>,
}

impl SandboxEnv {
    /// No Task root: the run keeps today's environment.
    pub fn none() -> Self {
        Self::default()
    }

    /// The environment of run `run_id` in `task_root`.
    pub fn for_task_root(task_root: &TaskRoot, run_id: &str, purpose: RunPurpose) -> Self {
        Self {
            root: Some(task_root.0.clone()),
            tmp: task_root.run_tmp(run_id),
            build: if purpose == RunPurpose::Probe {
                Vec::new()
            } else {
                Self::build_dirs(task_root)
            },
            live: Some(task_root.live_key(run_id)),
            cache: None,
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
            .map(|root| {
                let env = Self::for_task_root(&root, run_id, purpose);
                if purpose == RunPurpose::Probe {
                    env
                } else {
                    env.with_compiler_cache(Self::cache_of(&root, worktree))
                }
            })
            .unwrap_or_default()
    }

    /// What the compiler cache installed for the workspace root of
    /// `task_root` offers a run in `worktree`.
    fn cache_of(task_root: &TaskRoot, worktree: &Path) -> Option<CacheEnv> {
        crate::compiler_cache::for_task_roots(task_root.0.parent()?)?.for_worktree(worktree)
    }

    /// This environment with `cache` as its shared compiler cache.
    #[must_use]
    pub fn with_compiler_cache(mut self, cache: Option<CacheEnv>) -> Self {
        self.cache = cache;
        self
    }

    /// This environment without the shared compiler cache: for a run whose
    /// own sandbox could not use it. The run builds without a wrapper.
    #[must_use]
    pub fn without_compiler_cache(self) -> Self {
        self.with_compiler_cache(None)
    }

    /// The shared compiler cache this run is offered.
    pub fn compiler_cache(&self) -> Option<&CacheEnv> {
        self.cache.as_ref()
    }

    /// The repository store of the shared compiler cache, when `command`
    /// really carries Forge's wrapper and every one of its variables (the
    /// Project, the command or the operator may have chosen otherwise).
    pub fn compiler_cache_dir_in_use(&self, command: &std::process::Command) -> Option<&Path> {
        let cache = self.cache.as_ref()?;
        cache
            .variables()
            .all(|(key, value)| {
                command
                    .get_envs()
                    .any(|(name, set)| name == OsStr::new(key) && set == Some(value.as_os_str()))
            })
            .then_some(())?;
        cache.dir()
    }

    /// The Task-level part only (build directories, no per-run temp
    /// directory), for a command whose caller owns no run scope.
    pub fn for_task(worktree: &Path) -> Self {
        TaskRoot::of_worktree(worktree)
            .map(|root| Self {
                root: None,
                tmp: None,
                build: Self::build_dirs(&root),
                live: None,
                cache: Self::cache_of(&root, worktree),
            })
            .unwrap_or_default()
    }

    fn build_dirs(task_root: &TaskRoot) -> Vec<(&'static str, PathBuf)> {
        if !task_root.build_dir_is_usable() {
            return Vec::new();
        }
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

    /// This environment without the per-run temp directory: for a run whose
    /// own sandbox could not write it. The run keeps the inherited one.
    #[must_use]
    pub fn without_tmp(mut self) -> Self {
        self.tmp = None;
        self
    }

    /// This environment without the build directories: for a run whose own
    /// sandbox could not write them. The run builds where it did before.
    #[must_use]
    pub fn without_build(mut self) -> Self {
        self.build.clear();
        self
    }

    /// Start the run: enter it in the live-run registry by its Task root
    /// and create its per-run temp directory. A run whose directory cannot
    /// be created keeps today's temp directory instead of failing.
    ///
    /// Every run of a Task root is registered, with or without a temp
    /// directory (a Task root whose path is too long for a socket has none,
    /// which is the usual case on macOS): the registry is what keeps the
    /// garbage collector from taking the build output of a run that
    /// started after it looked ([`unless_live_run_in`]).
    #[must_use = "the returned environment is the one to apply"]
    pub fn prepared(mut self) -> Self {
        let Some(root) = self.root.clone() else {
            return self;
        };
        LazyLock::force(&PROCESS_START);
        // Live before anything exists, so a concurrent sweep or eviction
        // cannot take it. The no-temp name is entered first and replaced by
        // the temp directory's once that exists, so the run is never out of
        // the registry in between.
        let live = self.live.clone();
        let run = LiveRun {
            root: root.clone(),
            cache: self
                .cache
                .as_ref()
                .and_then(|cache| cache.dir().map(Path::to_path_buf)),
        };
        if let Some(live) = &live {
            live_run_dirs().insert(live.clone(), run.clone());
        }
        if let Some(tmp) = self.tmp.clone() {
            live_run_dirs().insert(tmp.clone(), run);
            if let Err(error) = prepare_run_tmp(&tmp) {
                live_run_dirs().remove(&tmp);
                tracing::warn!(path = %tmp.display(), %error, "per-run temp directory could not be created; run keeps the inherited one");
                self.tmp = None;
            } else if let Some(live) = &live {
                live_run_dirs().remove(live);
                self.live = None;
            }
        }
        // The build output may have been evicted between the moment this
        // environment was computed and the registration above. From here on
        // the run is live and nothing takes it again.
        if let Some(root) = TaskRoot::at(&root) {
            if !self.build.is_empty() && !root.build_dir_is_usable() {
                self.build.clear();
            }
        }
        self
    }

    /// End the run: remove the per-run temp directory and leave the
    /// live-run registry. Idempotent.
    pub fn settle(&self) {
        if let Some(tmp) = &self.tmp {
            remove_run_tmp(tmp);
            live_run_dirs().remove(tmp);
        }
        if let Some(live) = &self.live {
            live_run_dirs().remove(live);
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
    /// - The shared compiler cache: the same order. `RUSTC_WRAPPER` decides
    ///   for all of it: when the Project (an empty value included, which
    ///   turns the wrapper off for that Project), the command or the
    ///   operator's environment already names a wrapper, Forge sets nothing.
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
        if let Some(cache) = &self.cache {
            let taken = |key: &str| {
                project.contains_key(key)
                    || preset(key)
                    || operator(key).is_some_and(|value| !value.is_empty())
            };
            if !taken(crate::compiler_cache::WRAPPER_KEY) {
                vars.extend(
                    cache
                        .variables()
                        .filter(|(key, _)| {
                            *key == crate::compiler_cache::WRAPPER_KEY || !taken(key)
                        })
                        .map(|(key, value)| (key, Some(value))),
                );
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
        if !live_run_dirs().contains_key(&tmp) {
            remove_run_tmp(&tmp);
        }
    }
    // A run without a temp directory that its starter never settled (the
    // execution was settled by someone else) leaves the registry here.
    live_run_dirs().remove(&task_root.live_key(run_id));
}

/// Remove one per-run directory, but only out of a real directory: a
/// `.forge-task/tmp` or `.forge-tmp` that is a link (or a file) is never
/// removed through.
fn remove_run_tmp(tmp: &Path) {
    if tmp.parent().is_some_and(is_real_dir) {
        remove_tree(tmp);
    }
}

/// Remove the temp directories of runs that are dead.
///
/// `task_roots` is the directory whose children are Task roots (the server
/// workspace root; `<root>/.forge/workspaces` on a daemon). A per-run
/// directory under a reserved Task root or under the short directory is
/// removed only when all of these hold:
///
/// - this process did not create it (a hook, check, tool command, probe or
///   execution running here is never touched, whenever the sweep runs and
///   however often);
/// - its key belongs to no run id in `live`;
/// - it was last modified more than `max_run_age` before `now`.
///
/// The same rule holds at start-up and on the timer. "Older than this
/// process" is not proof of death: a run a previous process started and left
/// detached (a protected push, a check still settling) outlives a restart
/// and is in nobody's table, so only its age can condemn its directory. Age
/// never removes a directory younger than `max_run_age`, and a run longer
/// than that is kept by `live` and by this process's registry.
///
/// Links are never followed: not a linked Task root, not a linked
/// `.forge-task`, `tmp` or `.forge-tmp`. Returns the number removed.
pub fn sweep_stale_runs<'a>(
    task_roots: &Path,
    live: impl IntoIterator<Item = &'a str>,
    now: SystemTime,
    max_run_age: std::time::Duration,
) -> usize {
    let aged = now
        .checked_sub(max_run_age)
        .unwrap_or(SystemTime::UNIX_EPOCH);
    sweep_dead_runs_older_than(task_roots, live, aged)
}

fn sweep_dead_runs_older_than<'a>(
    task_roots: &Path,
    live: impl IntoIterator<Item = &'a str>,
    cutoff: SystemTime,
) -> usize {
    let live: Vec<String> = live.into_iter().filter_map(run_key).collect();
    let mut parents = vec![task_roots.join(SHORT_TMP_DIR_NAME)];
    if let Ok(entries) = fs::read_dir(task_roots) {
        parents.extend(
            entries
                .flatten()
                // `file_type` does not follow a link.
                .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
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
            let path = entry.path();
            let old = fs::symlink_metadata(&path)
                .and_then(|metadata| metadata.modified())
                .is_ok_and(|modified| modified < cutoff);
            if !old
                || live.iter().any(|key| OsStr::new(key) == name)
                || live_run_dirs().contains_key(&path)
            {
                continue;
            }
            remove_tree(&path);
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

pub(crate) fn is_real_dir(path: &Path) -> bool {
    fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_dir())
}

pub(crate) fn create_private_dir(path: &Path) -> io::Result<()> {
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
pub(crate) fn remove_tree(path: &Path) {
    remove_tree_until(path, None);
}

/// Entries removed between two looks at the deadline. A tree smaller than
/// this always goes in one call, so every pass makes progress.
const REMOVE_STRIDE: usize = 256;

/// [`remove_tree`] that stops between entries once `deadline` has passed.
/// `false` when it stopped early: what is left is still a tree under `path`
/// and the next call carries on. `true` otherwise, whether or not every
/// entry could be removed.
pub(crate) fn remove_tree_until(path: &Path, deadline: Option<Instant>) -> bool {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return true;
    };
    if !metadata.file_type().is_dir() {
        let _ = fs::remove_file(path);
        return true;
    }
    let Some(deadline) = deadline else {
        if fs::remove_dir_all(path).is_ok() {
            return true;
        }
        #[cfg(unix)]
        make_dirs_owner_writable(path);
        if let Err(error) = fs::remove_dir_all(path) {
            if error.kind() != io::ErrorKind::NotFound {
                tracing::warn!(path = %path.display(), %error, "directory tree could not be removed");
            }
        }
        return true;
    };
    // Depth first, children before their directory. An entry that cannot be
    // removed is remembered so the walk goes past it and ends.
    let mut stuck: std::collections::HashSet<PathBuf> = std::collections::HashSet::new();
    let mut stack = vec![path.to_path_buf()];
    let mut removed = 0_usize;
    while let Some(dir) = stack.last().cloned() {
        make_owner_writable(&dir);
        let mut descended = false;
        if let Ok(entries) = fs::read_dir(&dir) {
            for entry in entries.flatten() {
                let child = entry.path();
                if stuck.contains(&child) {
                    continue;
                }
                // `file_type` does not follow a link: a link is a file here.
                if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
                    stack.push(child);
                    descended = true;
                    break;
                }
                if fs::remove_file(&child).is_err() {
                    stuck.insert(child);
                }
                removed += 1;
                if removed.is_multiple_of(REMOVE_STRIDE) && Instant::now() >= deadline {
                    return false;
                }
            }
        }
        if descended {
            continue;
        }
        stack.pop();
        if fs::remove_dir(&dir).is_err() && fs::symlink_metadata(&dir).is_ok() {
            stuck.insert(dir);
        }
        removed += 1;
        if !stack.is_empty() && removed.is_multiple_of(REMOVE_STRIDE) && Instant::now() >= deadline
        {
            return false;
        }
    }
    if fs::symlink_metadata(path).is_ok() {
        tracing::warn!(path = %path.display(), "directory tree could not be removed");
    }
    true
}

#[cfg(unix)]
fn make_owner_writable(dir: &Path) {
    use std::os::unix::fs::PermissionsExt;
    if let Ok(metadata) = fs::symlink_metadata(dir) {
        let mode = metadata.permissions().mode();
        if metadata.file_type().is_dir() && mode & 0o700 != 0o700 {
            let _ = fs::set_permissions(dir, fs::Permissions::from_mode(mode | 0o700));
        }
    }
}

#[cfg(not(unix))]
fn make_owner_writable(_dir: &Path) {}

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
        // A run this process still holds is not settled from elsewhere.
        settle_run(&worktree, "7b0c9f6e-1234");
        assert!(tmp.is_dir());
        sandbox.settle();
        assert!(!tmp.exists());
        // A leftover of a dead process is.
        fs::create_dir(&tmp).unwrap();
        settle_run(&worktree, "7b0c9f6e-1234");
        assert!(!tmp.exists());

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

    fn later() -> SystemTime {
        SystemTime::now() + std::time::Duration::from_secs(60)
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
        // What a dead process left: directories this process never prepared.
        let mut dirs = Vec::new();
        for (root, run) in [
            (&short_root, "dead-run"),
            (&short_root, "live-run"),
            (&long_root, "dead-long"),
            (&long_root, "live-long"),
        ] {
            let tmp = root.run_tmp(run).unwrap();
            fs::create_dir_all(&tmp).unwrap();
            dirs.push(tmp);
        }
        assert!(dirs[2].starts_with(dir.path().join(SHORT_TMP_DIR_NAME)));
        fs::create_dir_all(unreserved.join("tmp/kept")).unwrap();
        // A restart proves nothing about a run the previous process left
        // detached: at start-up, as on the timer, only a directory older
        // than the longest run goes.
        let day = std::time::Duration::from_secs(25 * 60 * 60);
        assert_eq!(sweep_stale_runs(dir.path(), [], SystemTime::now(), day), 0);
        assert!(dirs.iter().all(|tmp| tmp.is_dir()));
        assert_eq!(
            sweep_dead_runs_older_than(dir.path(), ["live-run", "live-long"], later()),
            2
        );
        assert!(!dirs[0].exists() && dirs[1].exists());
        assert!(!dirs[2].exists() && dirs[3].exists());
        assert!(unreserved.join("tmp/kept").exists());
    }

    /// Hooks, checks, tool commands and probes are in no table. The sweep
    /// must not take the directory of one that is running in this process,
    /// even with an empty live list and no age limit.
    #[test]
    fn sweep_never_removes_a_run_this_process_is_still_running() {
        let dir = tempfile::tempdir().unwrap();
        let worktree = dir.path().join("t").join("repo");
        fs::create_dir_all(&worktree).unwrap();
        TaskRoot::reserve(worktree.parent().unwrap()).unwrap();
        let scope = SandboxEnv::for_command(&worktree, RunPurpose::Hook);
        let tmp = scope.env().tmp_dir().unwrap().to_path_buf();
        assert_eq!(sweep_dead_runs_older_than(dir.path(), [], later()), 0);
        assert!(tmp.is_dir());
        // Settling it by id from elsewhere is refused too.
        settle_run(&worktree, tmp.file_name().unwrap().to_str().unwrap());
        assert!(tmp.is_dir());
        drop(scope);
        assert!(!tmp.exists());
        // Once settled it is an ordinary leftover again.
        fs::create_dir(&tmp).unwrap();
        assert_eq!(sweep_dead_runs_older_than(dir.path(), [], later()), 1);
    }

    /// `.forge-task`, its `tmp`, `home` and `build`, the short directory and
    /// a Task-root entry that already exist as links (or files) are never
    /// written through, removed through or handed to a run.
    #[cfg(unix)]
    #[test]
    fn planted_links_and_files_are_never_followed() {
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let outside = dir.path().join("outside");
        let victim = |name: &str| {
            let path = outside.join(name);
            fs::create_dir_all(path.join("runabc")).unwrap();
            fs::write(path.join("runabc/keep"), "keep").unwrap();
            path
        };
        let tasks = dir.path().join("w");

        // `.forge-task` is a file: not a Task root, nothing set, nothing written.
        let file_root = tasks.join("file");
        fs::create_dir_all(file_root.join("repo")).unwrap();
        fs::write(file_root.join(TASK_DIR_NAME), "not a directory").unwrap();
        assert!(TaskRoot::reserve(&file_root).is_err());
        let sandbox = SandboxEnv::for_run(&file_root.join("repo"), "runabc", RunPurpose::Execution)
            .prepared();
        assert_eq!(sandbox, SandboxEnv::none());
        assert_eq!(
            fs::read_to_string(file_root.join(TASK_DIR_NAME)).unwrap(),
            "not a directory"
        );

        // `.forge-task/tmp`, `home` and `build` are links out of the root.
        let linked = tasks.join("linked");
        fs::create_dir_all(linked.join("repo")).unwrap();
        let root = TaskRoot::reserve(&linked).unwrap();
        for name in ["tmp", "home", "build"] {
            symlink(victim(name), linked.join(TASK_DIR_NAME).join(name)).unwrap();
        }
        let sandbox =
            SandboxEnv::for_run(&linked.join("repo"), "runabc", RunPurpose::Execution).prepared();
        assert_eq!(sandbox.tmp_dir(), None, "a linked tmp is not handed out");
        assert_eq!(sandbox.build_dir("CARGO_TARGET_DIR"), None);
        assert_eq!(root.prepare_home("codex"), None);
        settle_run(&linked.join("repo"), "runabc");
        SandboxEnv::for_run(&linked.join("repo"), "runabc", RunPurpose::Hook).settle();

        // A Task-root entry that is itself a link, and a linked short directory.
        let real_root = outside.join("real-root");
        fs::create_dir_all(real_root.join(".forge-task/tmp/runabc")).unwrap();
        symlink(&real_root, tasks.join("alias")).unwrap();
        symlink(victim("short"), tasks.join(SHORT_TMP_DIR_NAME)).unwrap();
        let long = tasks.join("0d9d6a3e-5f0b-4c57-9d4e-1f2a3b4c5d6e-long-enough-name-for-fallback");
        fs::create_dir_all(long.join("repo")).unwrap();
        let long_root = TaskRoot::reserve(&long).unwrap();
        if let Some(tmp) = long_root.run_tmp("runabc") {
            assert!(tmp.starts_with(tasks.join(SHORT_TMP_DIR_NAME)));
            let sandbox =
                SandboxEnv::for_task_root(&long_root, "runabc", RunPurpose::Check).prepared();
            assert_eq!(sandbox.tmp_dir(), None);
            settle_run(&long.join("repo"), "runabc");
        }

        assert_eq!(sweep_dead_runs_older_than(&tasks, [], later()), 0);
        for name in ["tmp", "home", "build", "short"] {
            assert!(outside.join(name).join("runabc/keep").exists(), "{name}");
            assert_eq!(
                fs::read_dir(outside.join(name)).unwrap().count(),
                1,
                "{name}"
            );
        }
        assert!(real_root.join(".forge-task/tmp/runabc").exists());
    }

    use crate::compiler_cache::{self, tests as cache_tests, CACHE_DIR, WRAPPER_KEY};

    fn value(vars: &[(&'static str, Option<OsString>)], key: &str) -> Option<Option<OsString>> {
        vars.iter()
            .find(|(name, _)| *name == key)
            .map(|(_, value)| value.clone())
    }

    #[test]
    fn compiler_cache_is_off_until_installed_and_then_every_run_of_a_repository_shares_one_store() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        let first = cache_tests::linked_worktree(&root, "t1", "repo-a");
        let second = cache_tests::linked_worktree(&root, "t2", "repo-a");
        let other = cache_tests::linked_worktree(&root, "t3", "repo-b");
        let none = |_: &str| None;
        let unset = |_: &str| false;
        let project = BTreeMap::new();

        // Off by default: nothing about a wrapper reaches a run.
        let off = SandboxEnv::for_run(&first, "run-1", RunPurpose::Execution);
        assert!(off.compiler_cache().is_none());
        assert!(value(&off.variables(&project, unset, none), WRAPPER_KEY).is_none());
        assert!(!root.join(CACHE_DIR).exists());

        let wrapper = cache_tests::fake_wrapper(&dir.path().join("bin"), "kache");
        compiler_cache::install(&root, Some(cache_tests::cache(&root, &wrapper)));
        let store = root.join(CACHE_DIR).join("repo-a");
        // Every run family that gets a build directory gets the cache:
        // executions, tool commands, hooks, checks, and the Task-level
        // environment of a check or review command.
        let families = [
            SandboxEnv::for_run(&first, "run-1", RunPurpose::Execution),
            SandboxEnv::for_run(&first, "run-2", RunPurpose::Command),
            SandboxEnv::for_run(&second, "run-3", RunPurpose::Hook),
            SandboxEnv::for_run(&second, "run-4", RunPurpose::Check),
            SandboxEnv::for_task(&second),
        ];
        for env in &families {
            let vars = env.variables(&project, unset, none);
            assert_eq!(
                value(&vars, WRAPPER_KEY),
                Some(Some(wrapper.clone().into_os_string()))
            );
            assert_eq!(
                value(&vars, "KACHE_CACHE_DIR"),
                Some(Some(store.clone().into_os_string()))
            );
            assert!(value(&vars, "CARGO_TARGET_DIR").is_some());
        }
        let scoped = SandboxEnv::for_command(&first, RunPurpose::Command);
        assert_eq!(scoped.env().compiler_cache().unwrap().dir(), Some(store.as_path()));
        drop(scoped);
        // A probe builds nothing; another repository has its own store.
        assert!(SandboxEnv::for_run(&first, "p", RunPurpose::Probe).compiler_cache().is_none());
        assert_eq!(
            SandboxEnv::for_run(&other, "run-5", RunPurpose::Check)
                .compiler_cache()
                .unwrap()
                .dir(),
            Some(root.join(CACHE_DIR).join("repo-b").as_path())
        );

        // While a run that was handed the store is live, the collector of
        // a store whose wrapper is not known to tolerate it stays away.
        let live = SandboxEnv::for_run(&first, "live", RunPurpose::Execution).prepared();
        assert_eq!(unless_live_cache_run_in(&store, || ()), None);
        assert_eq!(
            unless_live_cache_run_in(&root.join(CACHE_DIR).join("repo-b"), || ()),
            Some(())
        );
        live.settle();
        assert_eq!(unless_live_cache_run_in(&store, || ()), Some(()));

        compiler_cache::install(&root, None);
        assert!(SandboxEnv::for_run(&first, "run-6", RunPurpose::Check)
            .compiler_cache()
            .is_none());
    }

    #[tokio::test]
    async fn compiler_cache_yields_to_the_project_the_command_and_the_operator() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        let worktree = cache_tests::linked_worktree(&root, "t1", "repo-a");
        let wrapper = cache_tests::fake_wrapper(&dir.path().join("bin"), "kache");
        let env = SandboxEnv::for_run(&worktree, "run-1", RunPurpose::Execution)
            .with_compiler_cache(cache_tests::cache(&root, &wrapper).for_worktree(&worktree));
        let store = root.join(CACHE_DIR).join("repo-a");
        let none = |_: &str| None;
        let unset = |_: &str| false;
        let cache_vars = |vars: Vec<(&'static str, Option<OsString>)>| {
            vars.into_iter()
                .filter(|(key, _)| *key == WRAPPER_KEY || key.starts_with("KACHE_"))
                .map(|(key, _)| key)
                .collect::<Vec<_>>()
        };
        let all = vec![WRAPPER_KEY, "KACHE_CACHE_DIR", "KACHE_MAX_SIZE"];
        assert_eq!(cache_vars(env.variables(&BTreeMap::new(), unset, none)), all);

        // The Project names its own wrapper, or turns the wrapper off with
        // an empty value: Forge sets nothing at all.
        for project_value in ["/project/wrapper", ""] {
            let project = BTreeMap::from([(WRAPPER_KEY.to_owned(), project_value.to_owned())]);
            assert!(cache_vars(env.variables(&project, unset, none)).is_empty());
            let mut command = Command::new("sh");
            command
                .arg("-c")
                .arg("printf '%s|%s' \"${RUSTC_WRAPPER-unset}\" \"${KACHE_CACHE_DIR-unset}\"")
                .env_remove("KACHE_CACHE_DIR")
                .envs(&project);
            env.apply_with(&mut command, &project, none);
            let output = command.output().await.unwrap();
            assert_eq!(
                String::from_utf8(output.stdout).unwrap(),
                format!("{project_value}|unset")
            );
            assert_eq!(env.compiler_cache_dir_in_use(command.as_std()), None);
        }
        // A value already on the command, then the operator's environment.
        assert!(cache_vars(env.variables(&BTreeMap::new(), |key| key == WRAPPER_KEY, none)).is_empty());
        let operator = |key: &str| (key == WRAPPER_KEY).then(|| OsString::from("/usr/bin/sccache"));
        assert!(cache_vars(env.variables(&BTreeMap::new(), unset, operator)).is_empty());
        // An empty operator value is no wrapper of the operator's.
        let empty = |key: &str| (key == WRAPPER_KEY).then(OsString::new);
        assert_eq!(cache_vars(env.variables(&BTreeMap::new(), unset, empty)), all);
        // One of the wrapper's own variables set by the operator stays theirs,
        // and the store is then not the one in use.
        let own_dir = |key: &str| (key == "KACHE_CACHE_DIR").then(|| OsString::from("/operator/cache"));
        assert_eq!(
            cache_vars(env.variables(&BTreeMap::new(), unset, own_dir)),
            vec![WRAPPER_KEY, "KACHE_MAX_SIZE"]
        );

        let mut command = Command::new("sh");
        command.env_remove(WRAPPER_KEY).env_remove("KACHE_CACHE_DIR");
        env.apply_with(&mut command, &BTreeMap::new(), none);
        assert_eq!(env.compiler_cache_dir_in_use(command.as_std()), Some(store.as_path()));
        assert_eq!(
            env.clone().without_compiler_cache().compiler_cache_dir_in_use(command.as_std()),
            None
        );
    }
}
