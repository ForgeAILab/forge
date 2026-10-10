//! The opt-in shared compiler cache.
//!
//! Every Task builds into its own `CARGO_TARGET_DIR`, so every Task builds
//! cold. An operator who has a compiler-cache wrapper installed (sccache,
//! kache) can name it in `workspace.compiler_cache.wrapper`; runs then get
//! `RUSTC_WRAPPER` and the wrapper's cache directory, one per repository:
//!
//! ```text
//! <cache dir>/                 default <workspace root>/.forge/build/cache
//!   <repository id>/           one store per repository, shared by its Tasks
//!     .forge-compiler-cache    marker: Forge made this directory (wrapper kind)
//!     s, tmp/, rustc-wrapper   sccache only: its server socket, its temp
//!                              files and the launcher runs are handed
//! ```
//!
//! Forge installs nothing and turns nothing on by itself, and a cache that
//! cannot be used never fails a run: the run simply gets no wrapper.
//!
//! Machine local, like the run budget: a daemon reads its own configuration
//! and never receives the server's.

use crate::sandbox;
use std::{
    collections::HashMap,
    ffi::OsString,
    fs, io,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, LazyLock, Mutex, RwLock,
    },
    time::{Duration, Instant, SystemTime},
};

/// The default cache directory, under the workspace root.
pub const CACHE_DIR: &str = ".forge/build/cache";
/// The file that marks a repository store as Forge's. The collector never
/// touches a directory without it.
pub const MARKER_FILE: &str = ".forge-compiler-cache";
/// The variable every supported wrapper is passed through.
pub const WRAPPER_KEY: &str = "RUSTC_WRAPPER";
/// Where a daemon keeps its Task roots, under its workspace root.
pub const DAEMON_TASK_ROOTS: &str = ".forge/workspaces";
const SCCACHE_SOCKET: &str = "s";
const SERVER_TMP_DIR: &str = "tmp";
const SCCACHE_LAUNCHER: &str = "rustc-wrapper";
/// Longest socket path handed to sccache: a Unix socket path is limited to
/// 104 bytes on macOS (108 on Linux).
const MAX_SOCKET_BYTES: usize = 100;
#[cfg_attr(not(unix), allow(dead_code))]
const SERVER_START_TIMEOUT: Duration = Duration::from_secs(10);
/// Files one eviction may look at before it stops collecting.
const EVICTION_ENTRY_LIMIT: usize = 500_000;

/// The wrappers Forge knows the cache-directory variables of, by the base
/// name of the configured program.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WrapperKind {
    /// `SCCACHE_DIR`, `SCCACHE_CACHE_SIZE`, `SCCACHE_SERVER_UDS`, through a
    /// launcher in the store (see [`write_sccache_launcher`]).
    Sccache,
    /// `KACHE_CACHE_DIR`, `KACHE_MAX_SIZE`.
    Kache,
    /// Any other program: only `RUSTC_WRAPPER` is set.
    Unknown,
}

impl WrapperKind {
    pub fn of(wrapper: &Path) -> Self {
        match wrapper
            .file_stem()
            .and_then(|name| name.to_str())
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("sccache") => Self::Sccache,
            Some("kache") => Self::Kache,
            _ => Self::Unknown,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sccache => "sccache",
            Self::Kache => "kache",
            Self::Unknown => "unknown",
        }
    }

    /// Whether the wrapper may be handed to a CLI that confines its own
    /// writes and network (a Codex workspace-write sandbox).
    ///
    /// Only a wrapper known to compile uncached when it cannot reach its
    /// store does. kache does (checked against an unwritable cache
    /// directory: the build succeeds and says caching is off). sccache does
    /// not: its client fails the compile when it cannot reach or start its
    /// server, and a sandbox without network cannot reach it. An unknown
    /// wrapper is not known to, so it is never passed into a sandbox.
    pub fn fails_open(self) -> bool {
        self == Self::Kache
    }

    /// Whether cache entries may be deleted while a build uses the store.
    ///
    /// Checked for the sccache local disk cache (0.17): an entry file deleted
    /// under a live server is a miss on the next lookup and is written again;
    /// the build succeeds with no read or write error. Removing the store
    /// *directory* is different (the server then fails every write until it
    /// restarts), so only files are ever deleted. For every other wrapper
    /// entries go only while no run of this process uses the store.
    fn evictable_under_a_live_build(self) -> bool {
        self == Self::Sccache
    }
}

/// The resolved compiler cache of one workspace root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompilerCache {
    /// Absolute path of the wrapper program.
    pub wrapper: PathBuf,
    pub kind: WrapperKind,
    /// The directory holding one store per repository.
    pub dir: PathBuf,
    pub max_bytes: u64,
}

impl CompilerCache {
    /// Resolve the configured wrapper for `workspace_root`, searching
    /// `path_var` (a `PATH` value) for a bare name. `None` when the feature
    /// is off, and, with one warning, when the wrapper cannot be found.
    pub fn resolve(
        config: &config::CompilerCacheConfig,
        workspace_root: &Path,
        path_var: Option<OsString>,
    ) -> Option<Self> {
        let name = config.wrapper.as_deref().map(str::trim)?;
        if name.is_empty() {
            return None;
        }
        let configured = Path::new(name);
        let wrapper = if configured.is_absolute() {
            is_executable(configured).then(|| configured.to_path_buf())
        } else if configured.components().count() == 1 {
            path_var.and_then(|paths| {
                std::env::split_paths(&paths)
                    .filter(|dir| dir.is_absolute())
                    .map(|dir| dir.join(configured))
                    .find(|candidate| is_executable(candidate))
            })
        } else {
            None
        };
        let Some(wrapper) = wrapper else {
            tracing::warn!(
                wrapper = name,
                "workspace.compiler_cache.wrapper is not an executable (an absolute path, or a name on PATH): the shared compiler cache is off and runs build as before"
            );
            return None;
        };
        let dir = match &config.dir {
            Some(dir) if dir.is_absolute() => dir.clone(),
            Some(dir) => {
                tracing::warn!(dir = %dir.display(), "workspace.compiler_cache.dir is not an absolute path: the default under the workspace root is used");
                workspace_root.join(CACHE_DIR)
            }
            None => workspace_root.join(CACHE_DIR),
        };
        Some(Self {
            kind: WrapperKind::of(&wrapper),
            wrapper,
            dir,
            max_bytes: config.max_bytes,
        })
    }

    /// The store of one repository.
    pub fn repository_dir(&self, repository_id: &str) -> PathBuf {
        self.dir.join(repository_id)
    }

    /// What a run in `worktree` gets, or `None` when the cache cannot be
    /// used there right now (the run then builds as it did before, and the
    /// reason is logged once per process).
    pub fn for_worktree(&self, worktree: &Path) -> Option<CacheEnv> {
        if !is_executable(&self.wrapper) {
            warn_once(&WARNED_WRAPPER, || {
                tracing::warn!(wrapper = %self.wrapper.display(), "the compiler-cache wrapper is missing or not executable: runs build without it");
            });
            return None;
        }
        if self.kind == WrapperKind::Unknown {
            return Some(CacheEnv {
                wrapper: self.wrapper.clone(),
                kind: self.kind,
                dir: None,
                vars: Vec::new(),
            });
        }
        let dir = self.repository_dir(&repository_id(worktree)?);
        if let Err(error) = prepare_store(&self.dir, &dir, self.kind) {
            warn_once(&WARNED_DIR, || {
                tracing::warn!(path = %dir.display(), %error, "the compiler-cache directory cannot be created or written: runs build without the wrapper");
            });
            return None;
        }
        let path = |path: &Path| path.as_os_str().to_owned();
        let (wrapper, vars) = match self.kind {
            WrapperKind::Sccache => {
                let socket = dir.join(SCCACHE_SOCKET);
                if socket.as_os_str().len() > MAX_SOCKET_BYTES {
                    warn_once(&WARNED_SOCKET, || {
                        tracing::warn!(path = %socket.display(), limit = MAX_SOCKET_BYTES, "the sccache server socket path is too long: runs build without the wrapper. Set workspace.compiler_cache.dir to a shorter path");
                    });
                    return None;
                }
                let size = sccache_size(self.max_bytes);
                let started = ensure_sccache_server(&self.wrapper, &dir, &socket, &size)
                    .and_then(|()| write_sccache_launcher(&self.wrapper, &dir));
                let launcher = match started {
                    Ok(launcher) => launcher,
                    Err(error) => {
                        warn_once(&WARNED_SERVER, || {
                            tracing::warn!(wrapper = %self.wrapper.display(), %error, "the sccache server could not be started: runs build without the wrapper");
                        });
                        return None;
                    }
                };
                let vars = vec![
                    ("SCCACHE_DIR", path(&dir)),
                    ("SCCACHE_CACHE_SIZE", size.into()),
                    ("SCCACHE_SERVER_UDS", path(&socket)),
                ];
                (launcher, vars)
            }
            WrapperKind::Kache => (
                self.wrapper.clone(),
                vec![
                    ("KACHE_CACHE_DIR", path(&dir)),
                    ("KACHE_MAX_SIZE", self.max_bytes.to_string().into()),
                ],
            ),
            WrapperKind::Unknown => (self.wrapper.clone(), Vec::new()),
        };
        Some(CacheEnv {
            wrapper,
            kind: self.kind,
            dir: Some(dir),
            vars,
        })
    }
}

/// The compiler-cache part of one run's environment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheEnv {
    pub(crate) wrapper: PathBuf,
    pub(crate) kind: WrapperKind,
    /// The repository store, for a wrapper Forge knows the variables of.
    pub(crate) dir: Option<PathBuf>,
    pub(crate) vars: Vec<(&'static str, OsString)>,
}

impl CacheEnv {
    pub fn wrapper(&self) -> &Path {
        &self.wrapper
    }

    pub fn kind(&self) -> WrapperKind {
        self.kind
    }

    /// The repository store this run would write.
    pub fn dir(&self) -> Option<&Path> {
        self.dir.as_deref()
    }

    /// `(variable, value)` of everything this cache sets, wrapper first.
    pub fn variables(&self) -> impl Iterator<Item = (&'static str, OsString)> + '_ {
        std::iter::once((WRAPPER_KEY, self.wrapper.as_os_str().to_owned()))
            .chain(self.vars.iter().cloned())
    }
}

static WARNED_WRAPPER: AtomicBool = AtomicBool::new(false);
static WARNED_DIR: AtomicBool = AtomicBool::new(false);
static WARNED_SOCKET: AtomicBool = AtomicBool::new(false);
static WARNED_SERVER: AtomicBool = AtomicBool::new(false);
/// Warnings this process has logged, for tests: one per reason, not per run.
static WARNINGS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

fn warn_once(flag: &AtomicBool, warn: impl FnOnce()) {
    if !flag.swap(true, Ordering::Relaxed) {
        WARNINGS.fetch_add(1, Ordering::Relaxed);
        warn();
    }
}

/// How many "runs build without the wrapper" warnings this process logged.
pub fn warnings_logged() -> usize {
    WARNINGS.load(Ordering::Relaxed)
}

type Installed = HashMap<PathBuf, Arc<CompilerCache>>;
static INSTALLED: LazyLock<RwLock<Installed>> = LazyLock::new(RwLock::default);
/// Bytes of each workspace root's cache as its collector last measured them.
static MEASURED: LazyLock<Mutex<HashMap<PathBuf, u64>>> = LazyLock::new(Mutex::default);

fn root_key(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

/// Set (or, with `None`, clear) the compiler cache of `workspace_root`. Called
/// by the process entrypoint; kept per root so a daemon beside a server, and
/// tests, never see each other's.
pub fn install(workspace_root: &Path, cache: Option<CompilerCache>) {
    let mut installed = INSTALLED.write().unwrap_or_else(|p| p.into_inner());
    match cache {
        Some(cache) => {
            tracing::info!(
                root = %workspace_root.display(),
                wrapper = %cache.wrapper.display(),
                kind = cache.kind.as_str(),
                dir = %cache.dir.display(),
                max_bytes = cache.max_bytes,
                "shared compiler cache is on for this workspace root"
            );
            installed.insert(root_key(workspace_root), Arc::new(cache));
        }
        None => {
            installed.remove(&root_key(workspace_root));
        }
    }
}

/// The compiler cache installed for `workspace_root`.
pub fn installed(workspace_root: &Path) -> Option<Arc<CompilerCache>> {
    INSTALLED
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .get(&root_key(workspace_root))
        .cloned()
}

/// The compiler cache of the workspace root `task_roots` belongs to:
/// `task_roots` is the directory holding the Task roots, which is the
/// workspace root itself on the server and `<root>/.forge/workspaces` on a
/// daemon.
pub fn for_task_roots(task_roots: &Path) -> Option<Arc<CompilerCache>> {
    let installed = INSTALLED.read().unwrap_or_else(|p| p.into_inner());
    if installed.is_empty() {
        return None;
    }
    let key = root_key(task_roots);
    installed.get(&key).cloned().or_else(|| {
        let root = key
            .ends_with(DAEMON_TASK_ROOTS)
            .then(|| key.parent()?.parent())??;
        installed.get(root).cloned()
    })
}

/// Where the collector of `workspace_root` finds the cache, and the size it
/// trims it to under the floor's pressure. A root with no cache installed
/// (the feature was turned off) still has its default directory collected,
/// down to nothing.
pub fn store_of(workspace_root: &Path) -> (PathBuf, u64) {
    installed(workspace_root).map_or_else(
        || (workspace_root.join(CACHE_DIR), 0),
        |cache| (cache.dir.clone(), cache.max_bytes / 2),
    )
}

/// Whether `cache_dir` is on the filesystem that holds `workspace_root`.
/// Evicting a cache on another disk frees nothing on the one that is short;
/// such a cache is bounded by its wrapper's own size cap only.
pub fn shares_filesystem(cache_dir: &Path, workspace_root: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        matches!(
            (fs::metadata(cache_dir), fs::metadata(workspace_root)),
            (Ok(cache), Ok(root)) if cache.dev() == root.dev()
        )
    }
    #[cfg(not(unix))]
    {
        cache_dir.starts_with(workspace_root)
    }
}

/// Record what the collector of `workspace_root` measured.
pub fn note_measured(workspace_root: &Path, bytes: u64) {
    MEASURED
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(root_key(workspace_root), bytes);
}

/// Bytes of the compiler cache of `workspace_root` at its collector's last
/// pass. `None` before the first pass.
pub fn measured_bytes(workspace_root: &Path) -> Option<u64> {
    MEASURED
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(&root_key(workspace_root))
        .copied()
}

/// The repository a linked Git worktree belongs to, as a directory name.
///
/// A Task worktree's `.git` is a file naming `<repository>/worktrees/<name>`.
/// The repository directory is `<root>/.repos/<repository id>` on the server
/// and `<root>/repos/<repository id>` on a daemon, so its name is the id; a
/// repository whose Git directory is a `.git` inside a checkout is named
/// after that checkout plus a digest of its path. `None` for anything that
/// is not a linked worktree: such a run gets no shared cache.
pub fn repository_id(worktree: &Path) -> Option<String> {
    let dot_git = worktree.join(".git");
    let metadata = fs::symlink_metadata(&dot_git).ok()?;
    if !metadata.file_type().is_file() || metadata.len() > 4096 {
        return None;
    }
    let text = fs::read_to_string(&dot_git).ok()?;
    let git_dir = Path::new(text.lines().next()?.strip_prefix("gitdir:")?.trim());
    let git_dir = if git_dir.is_absolute() {
        git_dir.to_path_buf()
    } else {
        worktree.join(git_dir)
    };
    let worktrees = git_dir.parent()?;
    if worktrees.file_name()? != "worktrees" {
        return None;
    }
    let common = worktrees.parent()?;
    let name = common.file_name()?.to_str()?;
    let name = if name == ".git" {
        let checkout = common.parent()?.file_name()?.to_str()?;
        format!(
            "{checkout}-{:08x}",
            fnv1a(common.as_os_str().as_encoded_bytes()) as u32
        )
    } else {
        name.to_owned()
    };
    let id: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .take(96)
        .collect();
    let id = id.trim_start_matches('.');
    (!id.is_empty()).then(|| id.to_owned())
}

fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

fn is_executable(path: &Path) -> bool {
    let Ok(metadata) = fs::metadata(path) else {
        return false;
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.is_file() && metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        metadata.is_file()
    }
}

/// `SCCACHE_CACHE_SIZE` takes a number with a unit (a plain byte count is
/// ignored and the 10 GiB default applies): whole mebibytes, at least one.
fn sccache_size(max_bytes: u64) -> String {
    format!("{}M", (max_bytes >> 20).max(1))
}

/// Create the repository store and prove this process can write it. The
/// cache directory is the operator's (any depth); the store inside it is
/// Forge's and is never created through a link.
fn prepare_store(cache_dir: &Path, dir: &Path, kind: WrapperKind) -> io::Result<()> {
    fs::create_dir_all(cache_dir)?;
    sandbox::create_private_dir(dir)?;
    if !sandbox::is_real_dir(dir) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "the repository store is not a directory",
        ));
    }
    // Rewritten on every run start: the proof of write access, and the
    // record of which wrapper's layout the collector will find here.
    fs::write(dir.join(MARKER_FILE), kind.as_str())
}

/// Make sure the sccache server of one repository store runs, started by
/// Forge and not by a run.
///
/// sccache keeps one server per socket, and the server keeps the cache
/// directory and the environment it was started with. Left to itself the
/// first compile of a run would start it, inside that run's sandbox and with
/// that run's per-run `TMPDIR`; once the run settles and its `TMPDIR` is
/// removed, every later compile through that server fails ("Failed to create
/// temp dir"). So Forge starts it here, with a temp directory inside the
/// store, a socket per store (which is what makes the store per repository)
/// and no idle exit, before any run can.
fn ensure_sccache_server(wrapper: &Path, dir: &Path, socket: &Path, size: &str) -> io::Result<()> {
    #[cfg(unix)]
    {
        let running = || std::os::unix::net::UnixStream::connect(socket).is_ok();
        if running() {
            return Ok(());
        }
        // One starter at a time across every Forge process on this store: a
        // second `--start-server` would take the socket over and leave the
        // first server running for ever.
        let lock = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(dir.join("start.lock"))?;
        lock.lock()?;
        if running() {
            return Ok(());
        }
        let tmp = dir.join(SERVER_TMP_DIR);
        sandbox::create_private_dir(&tmp)?;
        let mut child = std::process::Command::new(wrapper)
            .arg("--start-server")
            .env("SCCACHE_DIR", dir)
            .env("SCCACHE_CACHE_SIZE", size)
            .env("SCCACHE_SERVER_UDS", socket)
            .env("SCCACHE_IDLE_TIMEOUT", "0")
            .env("TMPDIR", &tmp)
            .env_remove("TMP")
            .env_remove("TEMP")
            .current_dir(dir)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()?;
        let deadline = Instant::now() + SERVER_START_TIMEOUT;
        loop {
            if let Some(status) = child.try_wait()? {
                return if status.success() {
                    Ok(())
                } else {
                    Err(io::Error::other(format!(
                        "--start-server exited with {status}"
                    )))
                };
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "--start-server did not return",
                ));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (wrapper, dir, socket, size);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "a per-repository sccache server needs a Unix socket",
        ))
    }
}

/// Write the program runs are handed as `RUSTC_WRAPPER` for sccache:
/// `<store>/rustc-wrapper`, a two-line shell script that runs the operator's
/// sccache without `CARGO_TARGET_DIR` in its environment.
///
/// sccache makes every `CARGO_*` variable of the compile part of its cache
/// key. Each Task has its own `CARGO_TARGET_DIR`, set through the
/// environment, so without this no Task would ever hit an entry another
/// Task stored (checked with sccache 0.17: same crate, same directory, two
/// values of the variable, two misses). rustc does not read the variable
/// and Cargo does not set it for rustc; it is only inherited.
///
/// Written through a rename and only when it differs, so a run never
/// executes a half-written file.
fn write_sccache_launcher(wrapper: &Path, dir: &Path) -> io::Result<PathBuf> {
    let program = wrapper
        .to_str()
        .filter(|path| !path.contains('\''))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "the wrapper path cannot be quoted for a shell",
            )
        })?;
    let script = format!(
        "#!/bin/sh\n# Written by Forge. Runs the configured compiler-cache wrapper without the\n# per-Task target directory, which would otherwise be part of every cache key.\nunset CARGO_TARGET_DIR CARGO_BUILD_TARGET_DIR\nexec '{program}' \"$@\"\n"
    );
    let launcher = dir.join(SCCACHE_LAUNCHER);
    if fs::read_to_string(&launcher).is_ok_and(|current| current == script)
        && is_executable(&launcher)
    {
        return Ok(launcher);
    }
    let staged = dir.join(format!(".{SCCACHE_LAUNCHER}.{}", std::process::id()));
    fs::write(&staged, script)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&staged, fs::Permissions::from_mode(0o755))?;
    }
    fs::rename(&staged, &launcher)?;
    Ok(launcher)
}

/// The repository stores of a cache directory that Forge made: real
/// directories carrying the marker file, with the wrapper kind it records.
fn stores(cache_dir: &Path) -> Vec<(PathBuf, WrapperKind)> {
    if !sandbox::is_real_dir(cache_dir) {
        return Vec::new();
    }
    let Ok(entries) = fs::read_dir(cache_dir) else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
        .filter_map(|entry| {
            let marker = entry.path().join(MARKER_FILE);
            fs::symlink_metadata(&marker)
                .is_ok_and(|metadata| metadata.file_type().is_file() && metadata.len() <= 64)
                .then_some(())?;
            let kind = match fs::read_to_string(&marker).ok()?.trim() {
                "sccache" => WrapperKind::Sccache,
                "kache" => WrapperKind::Kache,
                _ => WrapperKind::Unknown,
            };
            Some((entry.path(), kind))
        })
        .collect()
}

/// Disk bytes of every repository store under `cache_dir`. `None` when a
/// store is too large to walk before `deadline`.
pub fn measure(cache_dir: &Path, deadline: Instant) -> Option<u64> {
    stores(cache_dir)
        .iter()
        .try_fold(0_u64, |total, (store, _)| {
            Some(total.saturating_add(crate::gc::measure(store, deadline)?))
        })
}

/// What one eviction did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Eviction {
    pub files_removed: usize,
    pub bytes_freed: u64,
    pub out_of_time: bool,
}

struct Entry {
    last_used: SystemTime,
    bytes: u64,
    path: PathBuf,
    store: usize,
}

/// Delete cache entries under `cache_dir`, least recently used first (the
/// later of a file's access and modification time), until the stores hold
/// at most `keep_bytes`, `enough()` says the disk has what it needs, or
/// `deadline` passes.
///
/// Only regular files inside a store Forge marked are deleted, never a
/// directory, a link, a socket, a lock file, a file directly in the store
/// (its marker, a wrapper's index) or anything under its `tmp`. A store of
/// a wrapper whose entries are not known to be safe to delete under a live
/// build is left alone while a run of this process uses it, and entirely
/// while `live_elsewhere` (the caller knows of runs the live-run registry
/// does not name a repository for).
pub fn evict(
    cache_dir: &Path,
    keep_bytes: u64,
    live_elsewhere: bool,
    deadline: Instant,
    enough: impl Fn() -> bool,
) -> Eviction {
    let mut done = Eviction::default();
    let stores = stores(cache_dir);
    let mut entries = Vec::new();
    let mut total = 0_u64;
    for (index, (store, _)) in stores.iter().enumerate() {
        if done.out_of_time {
            break;
        }
        let Ok(top) = fs::read_dir(store) else {
            continue;
        };
        let mut pending: Vec<PathBuf> = top
            .flatten()
            .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
            .filter(|entry| entry.file_name() != SERVER_TMP_DIR)
            .map(|entry| entry.path())
            .collect();
        while let Some(dir) = pending.pop() {
            let Ok(children) = fs::read_dir(&dir) else {
                continue;
            };
            for child in children.flatten() {
                // Neither call follows a link.
                let (Ok(kind), Ok(metadata)) = (child.file_type(), child.metadata()) else {
                    continue;
                };
                if kind.is_dir() {
                    pending.push(child.path());
                } else if kind.is_file() && child.path().extension().is_none_or(|ext| ext != "lock")
                {
                    let bytes = metadata.len();
                    total = total.saturating_add(bytes);
                    let modified = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
                    entries.push(Entry {
                        last_used: metadata.accessed().map_or(modified, |at| at.max(modified)),
                        bytes,
                        path: child.path(),
                        store: index,
                    });
                }
                if entries.len() >= EVICTION_ENTRY_LIMIT || Instant::now() >= deadline {
                    done.out_of_time = true;
                    break;
                }
            }
            if done.out_of_time {
                break;
            }
        }
    }
    entries.sort_by(|a, b| {
        a.last_used
            .cmp(&b.last_used)
            .then_with(|| a.path.cmp(&b.path))
    });
    for (count, entry) in entries.iter().enumerate() {
        if total <= keep_bytes {
            break;
        }
        if count.is_multiple_of(64) {
            if enough() {
                break;
            }
            if Instant::now() >= deadline {
                done.out_of_time = true;
                break;
            }
        }
        let (store, kind) = &stores[entry.store];
        let remove = || fs::remove_file(&entry.path).is_ok();
        let removed = if kind.evictable_under_a_live_build() {
            remove()
        } else if live_elsewhere {
            false
        } else {
            sandbox::unless_live_cache_run_in(store, remove).unwrap_or(false)
        };
        if removed {
            total = total.saturating_sub(entry.bytes);
            done.files_removed += 1;
            done.bytes_freed = done.bytes_freed.saturating_add(entry.bytes);
        }
    }
    done
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// An executable shell script at `<dir>/<name>` that appends
    /// `arguments|SCCACHE_DIR|KACHE_CACHE_DIR|TMPDIR` to `<dir>/calls`, answers
    /// `--start-server`, and otherwise runs the command it wraps.
    pub(crate) fn fake_wrapper(dir: &Path, name: &str) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        fs::create_dir_all(dir).unwrap();
        let path = dir.join(name);
        fs::write(
            &path,
            format!(
                "#!/bin/sh\nprintf '%s|%s|%s|%s\\n' \"$*\" \"${{SCCACHE_DIR:-}}\" \"${{KACHE_CACHE_DIR:-}}\" \"${{TMPDIR:-}}\" >> '{}'\n[ \"$1\" = --start-server ] && exit 0\nexec \"$@\"\n",
                dir.join("calls").display()
            ),
        )
        .unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    /// A worktree at `<root>/<task>/repo` whose `.git` names the repository
    /// `<root>/.repos/<repository>`, in a reserved Task root.
    pub(crate) fn linked_worktree(root: &Path, task: &str, repository: &str) -> PathBuf {
        let worktree = root.join(task).join("repo");
        fs::create_dir_all(&worktree).unwrap();
        sandbox::TaskRoot::reserve(worktree.parent().unwrap()).unwrap();
        fs::write(
            worktree.join(".git"),
            format!(
                "gitdir: {}\n",
                root.join(".repos")
                    .join(repository)
                    .join("worktrees")
                    .join(task)
                    .display()
            ),
        )
        .unwrap();
        worktree
    }

    pub(crate) fn cache(root: &Path, wrapper: &Path) -> CompilerCache {
        CompilerCache {
            wrapper: wrapper.to_path_buf(),
            kind: WrapperKind::of(wrapper),
            dir: root.join(CACHE_DIR),
            max_bytes: 3 << 20,
        }
    }

    fn config(wrapper: &str) -> config::CompilerCacheConfig {
        config::CompilerCacheConfig::default().overridden(Some(wrapper.to_owned()), None, None)
    }

    #[test]
    fn wrapper_resolves_from_an_absolute_path_or_path_and_is_off_otherwise() {
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("bin");
        let wrapper = fake_wrapper(&bin, "sccache");
        let root = dir.path().join("root");
        let path_var = Some(bin.clone().into_os_string());

        assert_eq!(
            CompilerCache::resolve(
                &config::CompilerCacheConfig::default(),
                &root,
                path_var.clone()
            ),
            None,
            "unset wrapper: the feature is off"
        );
        let by_name = CompilerCache::resolve(&config("sccache"), &root, path_var.clone()).unwrap();
        assert_eq!(by_name.wrapper, wrapper);
        assert_eq!(by_name.kind, WrapperKind::Sccache);
        assert_eq!(by_name.dir, root.join(CACHE_DIR));
        assert_eq!(by_name.max_bytes, 20 * 1024 * 1024 * 1024);
        let absolute =
            CompilerCache::resolve(&config(wrapper.to_str().unwrap()), &root, None).unwrap();
        assert_eq!(absolute.wrapper, wrapper);

        assert_eq!(
            CompilerCache::resolve(&config("sccache"), &root, None),
            None
        );
        assert_eq!(
            CompilerCache::resolve(&config("no-such-tool"), &root, path_var.clone()),
            None
        );
        assert_eq!(
            CompilerCache::resolve(&config("bin/sccache"), &root, path_var),
            None
        );
        fs::write(bin.join("plain"), "not executable").unwrap();
        assert_eq!(
            CompilerCache::resolve(&config(bin.join("plain").to_str().unwrap()), &root, None),
            None
        );

        let custom = config::CompilerCacheConfig::default().overridden(
            Some(wrapper.to_str().unwrap().to_owned()),
            Some(99),
            Some(dir.path().join("elsewhere")),
        );
        let custom = CompilerCache::resolve(&custom, &root, None).unwrap();
        assert_eq!(
            (custom.dir, custom.max_bytes),
            (dir.path().join("elsewhere"), 99)
        );
    }

    #[test]
    fn wrapper_kind_is_the_program_name_and_sets_only_what_forge_knows() {
        assert_eq!(
            WrapperKind::of(Path::new("/opt/bin/sccache")),
            WrapperKind::Sccache
        );
        assert_eq!(
            WrapperKind::of(Path::new("/opt/bin/Kache.exe")),
            WrapperKind::Kache
        );
        assert_eq!(
            WrapperKind::of(Path::new("/opt/bin/cachepot")),
            WrapperKind::Unknown
        );
        assert_eq!(sccache_size(20 * 1024 * 1024 * 1024), "20480M");
        assert_eq!(sccache_size(10), "1M");

        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        let worktree = linked_worktree(&root, "t1", "repo-a");

        let sccache = cache(&root, &fake_wrapper(&dir.path().join("a"), "sccache"));
        let env = sccache.for_worktree(&worktree).unwrap();
        let store = root.join(CACHE_DIR).join("repo-a");
        assert_eq!(env.dir(), Some(store.as_path()));
        let vars: Vec<_> = env.variables().collect();
        assert_eq!(
            vars,
            vec![
                (WRAPPER_KEY, store.join("rustc-wrapper").into_os_string()),
                ("SCCACHE_DIR", store.clone().into_os_string()),
                ("SCCACHE_CACHE_SIZE", "3M".into()),
                ("SCCACHE_SERVER_UDS", store.join("s").into_os_string()),
            ]
        );
        assert_eq!(
            fs::read_to_string(store.join(MARKER_FILE)).unwrap(),
            "sccache"
        );
        // Forge started the server itself, with a temp directory in the store.
        let calls = fs::read_to_string(dir.path().join("a").join("calls")).unwrap();
        let call: Vec<&str> = calls.trim_end().split('|').collect();
        assert_eq!(
            (call[0], call[1], call[3]),
            (
                "--start-server",
                store.to_str().unwrap(),
                store.join("tmp").to_str().unwrap()
            )
        );

        // What runs are handed drops the per-Task target directory and
        // runs the operator's program.
        let launched = std::process::Command::new(env.wrapper())
            .args(["sh", "-c", "printf '%s' \"${CARGO_TARGET_DIR:-unset}\""])
            .env("CARGO_TARGET_DIR", "/task/target")
            .output()
            .unwrap();
        assert_eq!(String::from_utf8(launched.stdout).unwrap(), "unset");
        let calls = fs::read_to_string(dir.path().join("a").join("calls")).unwrap();
        assert_eq!(calls.lines().count(), 2, "{calls}");
        assert!(
            calls.lines().nth(1).unwrap().starts_with("sh -c "),
            "{calls}"
        );

        let kache = cache(&root, &fake_wrapper(&dir.path().join("b"), "kache"));
        let other = linked_worktree(&root, "t2", "repo-b");
        let env = kache.for_worktree(&other).unwrap();
        let store = root.join(CACHE_DIR).join("repo-b");
        assert_eq!(
            env.variables().collect::<Vec<_>>(),
            vec![
                (WRAPPER_KEY, kache.wrapper.clone().into_os_string()),
                ("KACHE_CACHE_DIR", store.into_os_string()),
                ("KACHE_MAX_SIZE", (3_u64 << 20).to_string().into()),
            ]
        );
        assert!(
            !dir.path().join("b").join("calls").exists(),
            "kache starts nothing"
        );

        let unknown = cache(&root, &fake_wrapper(&dir.path().join("c"), "cachepot"));
        let env = unknown.for_worktree(&other).unwrap();
        assert_eq!(env.dir(), None);
        assert_eq!(
            env.variables().collect::<Vec<_>>(),
            vec![(WRAPPER_KEY, unknown.wrapper.clone().into_os_string())]
        );
    }

    #[test]
    fn repository_id_comes_from_the_worktree_link_and_nothing_else() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let worktree = linked_worktree(root, "t1", "3f2b0c1e-repo");
        assert_eq!(repository_id(&worktree).as_deref(), Some("3f2b0c1e-repo"));

        // A repository that is a checkout: named after it, with its path.
        fs::write(
            worktree.join(".git"),
            format!(
                "gitdir: {}\n",
                root.join("my app/.git/worktrees/t1").display()
            ),
        )
        .unwrap();
        let id = repository_id(&worktree).unwrap();
        assert!(
            id.starts_with("my_app-") && id.len() == "my_app-".len() + 8,
            "{id}"
        );

        // Not a linked worktree: a clone, a plain directory, a submodule.
        fs::write(worktree.join(".git"), "gitdir: ../.git/modules/sub\n").unwrap();
        assert_eq!(repository_id(&worktree), None);
        fs::remove_file(worktree.join(".git")).unwrap();
        assert_eq!(repository_id(&worktree), None);
        fs::create_dir(worktree.join(".git")).unwrap();
        assert_eq!(repository_id(&worktree), None);
        let no_repository = cache(root, &fake_wrapper(&root.join("bin"), "kache"));
        assert_eq!(no_repository.for_worktree(&worktree), None);
    }

    #[test]
    fn a_cache_that_cannot_be_used_gives_no_wrapper_and_warns_once() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        let worktree = linked_worktree(&root, "t1", "repo-a");
        let wrapper = fake_wrapper(&dir.path().join("bin"), "kache");
        let mut kache = cache(&root, &wrapper);

        // The cache directory cannot be created: its parent is read-only.
        let locked = dir.path().join("locked");
        fs::create_dir(&locked).unwrap();
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o500)).unwrap();
        kache.dir = locked.join("cache");
        assert_eq!(kache.for_worktree(&worktree), None);
        assert_eq!(kache.for_worktree(&worktree), None);
        assert!(WARNED_DIR.load(Ordering::Relaxed));
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o700)).unwrap();

        // The store is a link somewhere else: never written through.
        kache.dir = root.join(CACHE_DIR);
        fs::create_dir_all(&kache.dir).unwrap();
        std::os::unix::fs::symlink(dir.path(), kache.dir.join("repo-a")).unwrap();
        assert_eq!(kache.for_worktree(&worktree), None);
        assert!(!dir.path().join(MARKER_FILE).exists());
        fs::remove_file(kache.dir.join("repo-a")).unwrap();
        assert!(kache.for_worktree(&worktree).is_some());

        // The wrapper went away after start.
        fs::remove_file(&wrapper).unwrap();
        assert_eq!(kache.for_worktree(&worktree), None);
        assert_eq!(kache.for_worktree(&worktree), None);
        assert!(WARNED_WRAPPER.load(Ordering::Relaxed));

        // An sccache whose server cannot be started, or whose socket path
        // is too long for a Unix socket.
        let failing = dir.path().join("failing").join("sccache");
        fs::create_dir_all(failing.parent().unwrap()).unwrap();
        fs::write(&failing, "#!/bin/sh\nexit 3\n").unwrap();
        fs::set_permissions(&failing, fs::Permissions::from_mode(0o755)).unwrap();
        assert_eq!(cache(&root, &failing).for_worktree(&worktree), None);
        assert!(WARNED_SERVER.load(Ordering::Relaxed));
        let mut long = cache(&root, &fake_wrapper(&dir.path().join("ok"), "sccache"));
        long.dir = root.join("a".repeat(MAX_SOCKET_BYTES));
        assert_eq!(long.for_worktree(&worktree), None);
        assert!(WARNED_SOCKET.load(Ordering::Relaxed));

        // One warning per reason for the whole process, however many runs.
        let logged = warnings_logged();
        assert!(logged >= 4, "{logged}");
        for _ in 0..3 {
            assert_eq!(long.for_worktree(&worktree), None);
            assert_eq!(cache(&root, &failing).for_worktree(&worktree), None);
        }
        assert_eq!(warnings_logged(), logged);
    }

    #[test]
    fn install_is_per_workspace_root_and_covers_the_daemon_layout() {
        let dir = tempfile::tempdir().unwrap();
        let (server, daemon) = (dir.path().join("server"), dir.path().join("daemon"));
        for root in [&server, &daemon] {
            fs::create_dir_all(root.join(DAEMON_TASK_ROOTS)).unwrap();
        }
        let wrapper = fake_wrapper(&dir.path().join("bin"), "kache");
        assert_eq!(for_task_roots(&server), None);
        install(&server, Some(cache(&server, &wrapper)));
        assert_eq!(for_task_roots(&server).unwrap().dir, server.join(CACHE_DIR));
        assert_eq!(for_task_roots(&daemon), None);
        assert_eq!(for_task_roots(&daemon.join(DAEMON_TASK_ROOTS)), None);
        assert_eq!(store_of(&daemon), (daemon.join(CACHE_DIR), 0));

        install(&daemon, Some(cache(&daemon, &wrapper)));
        assert_eq!(
            for_task_roots(&daemon.join(DAEMON_TASK_ROOTS)).unwrap().dir,
            daemon.join(CACHE_DIR)
        );
        assert_eq!(store_of(&daemon), (daemon.join(CACHE_DIR), (3 << 20) / 2));
        install(&daemon, None);
        assert_eq!(for_task_roots(&daemon.join(DAEMON_TASK_ROOTS)), None);
        install(&server, None);
    }

    fn entry(store: &Path, name: &str, bytes: usize, age_secs: u64) -> PathBuf {
        let path = store.join(name);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, vec![0_u8; bytes]).unwrap();
        let then = SystemTime::now() - Duration::from_secs(age_secs);
        let file = fs::File::options().write(true).open(&path).unwrap();
        file.set_times(fs::FileTimes::new().set_accessed(then).set_modified(then))
            .unwrap();
        path
    }

    fn store(cache_dir: &Path, repository: &str, kind: WrapperKind) -> PathBuf {
        let store = cache_dir.join(repository);
        prepare_store(cache_dir, &store, kind).unwrap();
        store
    }

    #[test]
    fn eviction_takes_the_least_recently_used_entries_down_to_the_size_kept() {
        let dir = tempfile::tempdir().unwrap();
        let cache_dir = dir.path().join("cache");
        let a = store(&cache_dir, "repo-a", WrapperKind::Sccache);
        let b = store(&cache_dir, "repo-b", WrapperKind::Sccache);
        let oldest = entry(&a, "0/1/oldest", 1000, 4000);
        let old = entry(&b, "0/2/old", 1000, 3000);
        let recent = entry(&a, "0/3/recent", 1000, 2000);
        let newest = entry(&b, "0/4/newest", 1000, 10);
        // Never entries: the socket stand-in and index beside the marker,
        // the server's temp files, a lock file, a link, and a directory
        // Forge did not mark.
        let index = entry(&a, "index.db", 5000, 9000);
        let server_tmp = entry(&a, "tmp/scratch", 5000, 9000);
        let lock = entry(&a, "0/gc.lock", 10, 9000);
        std::os::unix::fs::symlink(&newest, a.join("0").join("link")).unwrap();
        let foreign = entry(&cache_dir.join("not-forge"), "0/1/x", 9000, 9000);
        let deadline = Instant::now() + Duration::from_secs(30);

        assert!(measure(&cache_dir, deadline).is_some_and(|bytes| bytes > 0));
        // Nothing while the disk already has what it needs.
        assert_eq!(
            evict(&cache_dir, 0, false, deadline, || true),
            Eviction::default()
        );
        let done = evict(&cache_dir, 2000, true, deadline, || false);
        assert_eq!(
            (done.files_removed, done.bytes_freed, done.out_of_time),
            (2, 2000, false)
        );
        assert!(!oldest.exists() && !old.exists());
        assert!(recent.exists() && newest.exists());
        // Down to nothing: every entry, and still none of the rest.
        let done = evict(&cache_dir, 0, false, deadline, || false);
        assert_eq!(done.files_removed, 2);
        for kept in [&index, &server_tmp, &lock, &foreign] {
            assert!(kept.exists(), "{}", kept.display());
        }
        assert!(a.join("0").join("link").symlink_metadata().is_ok());
        assert!(a.join(MARKER_FILE).exists() && a.join("0").join("3").is_dir());
        // A cache directory that is a link is not walked at all.
        let linked = dir.path().join("linked");
        std::os::unix::fs::symlink(&cache_dir, &linked).unwrap();
        entry(&a, "0/5/again", 100, 50);
        assert_eq!(
            evict(&linked, 0, false, deadline, || false),
            Eviction::default()
        );
        assert_eq!(measure(&linked, deadline), Some(0));
    }
}
