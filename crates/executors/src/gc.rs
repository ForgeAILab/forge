//! Filesystem mechanics of the workspace garbage collector.
//!
//! The server and each daemon run the same sweep over their own managed
//! root. Everything here is synchronous, takes no database and decides
//! nothing about ownership: the caller classifies each Task root from its own
//! table (workspace rows on the server, the persisted handle table on a
//! daemon) and this module does the directory work.
//!
//! Rules that hold for every function:
//!
//! - Symbolic links are never followed. A link (or a file) found where a
//!   directory is expected is removed as an entry or skipped.
//! - Nothing is removed unless its parent resolves inside the managed root.
//! - An error on one entry is counted and the pass continues.
//! - A pass stops at its deadline and reports that it did not finish.

use crate::compiler_cache;
use crate::sandbox::{self, TASK_DIR_NAME};
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Component, Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

/// Quarantine and sweep state under the managed root.
pub const GC_DIR: &str = ".forge/gc";
/// The file in [`GC_DIR`] naming the one owner allowed to sweep this root.
/// It is also Forge's marker: a root without it is never swept, and only an
/// explicit start-up step ([`Sweep::adopt`]) writes it.
pub const OWNER_FILE: &str = "owner";
/// The marker of a daemon's handle table. A server and its embedded daemon
/// share one root and sweep different directories of it (`<root>/<task id>`
/// and `<root>/.forge/workspaces/workspace-<id>`), so each has its own.
pub const DAEMON_OWNER_FILE: &str = "daemon-owner";
/// Directories already condemned (renamed out of their place under the
/// owner's lock) and waiting to be deleted outside it.
pub const TRASH_DIR: &str = ".forge/gc/trash";
/// Exact-commit check checkouts.
pub const CHECKS_DIR: &str = ".forge/build/checks";
/// An unknown Task-root directory is kept this long after it was quarantined.
pub const QUARANTINE_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);
/// A directory younger than this is never quarantined: whoever is creating it
/// may not have recorded it yet. A create by this process is excluded outright
/// through the create registry (`Sweep::creating`), so this only has to cover
/// what no registry can see, and a large clone can take hours.
pub const ORPHAN_GRACE: Duration = Duration::from_secs(24 * 60 * 60);
/// A `<name>.broken-<ms>` copy left by a worktree recovery is kept this long.
pub const BROKEN_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);
/// A per-run temp directory that no table knows and that was not touched for
/// this long belongs to nothing alive. This is not a bound on how long a run
/// may take (an execution deadline is set per agent and has no ceiling): a
/// run of any length is kept by the live set and by this process's registry,
/// never by its age.
pub const MAX_RUN_AGE: Duration = Duration::from_secs(25 * 60 * 60);
/// Default age of a dead check checkout: see [`check_checkout_age`].
pub const CHECK_CHECKOUT_AGE: Duration = Duration::from_secs(2 * 60 * 60);
/// A legacy location outside the workspace root goes only after nothing
/// touched it for this long: another Forge on the machine may still use it.
pub const LEGACY_UNTOUCHED: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// The age after which a check checkout nobody holds is dead, for a check
/// wall limit of `timeout_seconds`: twice the limit (the run and its cleanup
/// phase) plus an hour, and never under [`CHECK_CHECKOUT_AGE`].
pub fn check_checkout_age(timeout_seconds: u64) -> Duration {
    CHECK_CHECKOUT_AGE.max(Duration::from_secs(
        timeout_seconds.saturating_mul(2).saturating_add(3600),
    ))
}

/// Who may sweep a managed root.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ownership {
    /// The marker names this owner.
    Mine,
    /// The marker names somebody else (another database, another daemon
    /// state). Nothing is swept until an operator re-claims the root.
    Other,
    /// No marker: the root was never adopted by a running Forge.
    Unclaimed,
    /// The directory must never be swept, whatever any marker says.
    Refused(&'static str),
}

impl Ownership {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Mine => "owned",
            Self::Other => "claimed_by_other",
            Self::Unclaimed => "unclaimed",
            Self::Refused(_) => "refused",
        }
    }
}

/// Why `root` can never be a managed root, if it cannot: it is not a
/// resolved real directory, it is too close to the filesystem root, it is or
/// contains the home directory, or it is a git repository.
pub fn refuse_root(root: &Path) -> Option<&'static str> {
    if !sandbox::is_real_dir(root) || fs::canonicalize(root).ok().as_deref() != Some(root) {
        return Some("it is not a resolved real directory (a link on the way, or missing)");
    }
    let depth = root
        .components()
        .filter(|part| matches!(part, Component::Normal(_)))
        .count();
    if depth < 2 {
        return Some("it is the filesystem root or one of its top-level directories");
    }
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .filter(|home| home.is_absolute());
    if let Some(home) = home {
        let resolved = fs::canonicalize(&home).unwrap_or_else(|_| home.clone());
        if home.starts_with(root) || resolved.starts_with(root) {
            return Some("it is the home directory or a parent of it");
        }
    }
    if fs::symlink_metadata(root.join(".git")).is_ok() {
        return Some("it is a git repository");
    }
    None
}
/// Entries one Task-root measurement may visit before it is abandoned.
pub const MEASURE_ENTRY_LIMIT: usize = 500_000;

/// What the owner's table says about one Task-root directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RootState {
    /// A recorded, live workspace: only its dead leftovers are touched.
    Live,
    /// Its workspace was reclaimed: whatever is left is removed.
    Cleaned,
    /// No record of any kind: quarantined, never deleted on sight.
    Unknown,
}

/// What one pass did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct GcReport {
    pub quarantined: usize,
    pub restored: usize,
    pub removed: usize,
    pub run_dirs_removed: usize,
    pub builds_evicted: usize,
    /// Entries deleted from the shared compiler cache.
    pub cache_entries_evicted: usize,
    /// Compiler-cache stores of deleted repositories removed whole.
    pub cache_stores_removed: usize,
    pub errors: usize,
    /// The pass ran out of time; the caller keeps its cursor.
    pub out_of_time: bool,
}

impl GcReport {
    pub fn merge(&mut self, other: &Self) {
        self.quarantined += other.quarantined;
        self.restored += other.restored;
        self.removed += other.removed;
        self.run_dirs_removed += other.run_dirs_removed;
        self.builds_evicted += other.builds_evicted;
        self.cache_entries_evicted += other.cache_entries_evicted;
        self.cache_stores_removed += other.cache_stores_removed;
        self.errors += other.errors;
        self.out_of_time |= other.out_of_time;
    }

    pub fn did_something(&self) -> bool {
        self.quarantined
            + self.restored
            + self.removed
            + self.run_dirs_removed
            + self.builds_evicted
            + self.cache_entries_evicted
            + self.cache_stores_removed
            + self.errors
            > 0
    }
}

/// One pass over a managed root.
#[derive(Debug, Clone)]
pub struct Sweep {
    /// The managed root. Nothing outside it is ever removed.
    pub root: PathBuf,
    /// The directory whose children are Task roots: the root itself on the
    /// server, `<root>/.forge/workspaces` on a daemon.
    pub task_roots: PathBuf,
    /// Whether a directory name is a Task root of this owner (a Task id on
    /// the server, a workspace handle on a daemon). Every other name, in the
    /// Task-root directory and in quarantine, is invisible to the pass.
    pub shaped: fn(&str) -> bool,
    /// The marker file in [`GC_DIR`] that names this pass's owner.
    pub owner_file: &'static str,
    /// Whether a create of this process is in flight for a Task-root path.
    /// Such a directory is never quarantined, whatever its age.
    pub creating: fn(&Path) -> bool,
    /// Free and total bytes of the filesystem holding a path; `None` when
    /// unreadable, and then nothing is evicted.
    pub disk_space: fn(&Path) -> Option<DiskSpace>,
    /// Age after which a check checkout nobody holds is dead.
    pub check_checkout_age: Duration,
    pub now: SystemTime,
    pub deadline: Instant,
}

impl Sweep {
    pub fn new(root: &Path, task_roots: &Path, shaped: fn(&str) -> bool, budget: Duration) -> Self {
        Self {
            root: root.to_path_buf(),
            task_roots: task_roots.to_path_buf(),
            shaped,
            owner_file: OWNER_FILE,
            creating: |_| false,
            disk_space,
            check_checkout_age: CHECK_CHECKOUT_AGE,
            now: SystemTime::now(),
            deadline: Instant::now() + budget,
        }
    }

    fn out_of_time(&self) -> bool {
        Instant::now() >= self.deadline
    }

    fn gc_dir(&self) -> PathBuf {
        self.root.join(GC_DIR)
    }

    /// Whether `owner_id` may sweep this root. Read-only: nothing is created.
    ///
    /// A sweep decides from one owner's table what is unknown or dead. Two
    /// owners with different tables on one root (two servers with different
    /// databases, a daemon whose state was wiped, a test beside a running
    /// server) would each see the other's live directories as garbage, so
    /// only the owner named in `<root>/.forge/gc/owner` ever sweeps it, and a
    /// root without that file is not swept at all.
    pub fn ownership(&self, owner_id: &str) -> Ownership {
        if let Some(reason) = refuse_root(&self.root) {
            return Ownership::Refused(reason);
        }
        let gc_dir = self.gc_dir();
        let marker = gc_dir.join(self.owner_file);
        match fs::symlink_metadata(&marker) {
            Ok(metadata)
                if metadata.file_type().is_file()
                    && gc_dir.parent().is_some_and(sandbox::is_real_dir)
                    && sandbox::is_real_dir(&gc_dir) =>
            {
                match fs::read_to_string(&marker) {
                    Ok(owner) if !owner_id.is_empty() && owner.trim() == owner_id => {
                        Ownership::Mine
                    }
                    _ => Ownership::Other,
                }
            }
            // A link or a directory there is not a claim anyone can hold.
            Ok(_) => Ownership::Other,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ownership::Unclaimed,
            Err(_) => Ownership::Other,
        }
    }

    /// Adopt the root for `owner_id` when nobody has: the one step that
    /// writes Forge's marker. Called once by a running server or daemon at
    /// start-up, never by a sweep. The marker appears whole or not at all
    /// (written beside its place, then hard-linked into it), and of two
    /// owners starting together exactly one gets it.
    pub fn adopt(&self, owner_id: &str) -> Ownership {
        match self.ownership(owner_id) {
            Ownership::Unclaimed if !owner_id.is_empty() => {}
            settled => return settled,
        }
        if let Some(staged) = self.stage_owner(owner_id) {
            let _ = fs::hard_link(&staged, self.gc_dir().join(self.owner_file));
            let _ = fs::remove_file(&staged);
        }
        self.ownership(owner_id)
    }

    /// Replace the marker with `owner_id`. Only ever run on an operator's
    /// explicit request: the previous owner's live directories become
    /// unknown to the sweep (quarantined, then deleted a day later).
    pub fn reclaim(&self, owner_id: &str) -> Ownership {
        if owner_id.is_empty() || refuse_root(&self.root).is_some() {
            return self.ownership(owner_id);
        }
        if let Some(staged) = self.stage_owner(owner_id) {
            let _ = fs::rename(&staged, self.gc_dir().join(self.owner_file));
            let _ = fs::remove_file(&staged);
        }
        self.ownership(owner_id)
    }

    /// The owner id written in full to a private file beside the marker.
    fn stage_owner(&self, owner_id: &str) -> Option<PathBuf> {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let gc_dir = self.gc_dir();
        if !self.prepare_gc_dir(&gc_dir) {
            return None;
        }
        let staged = gc_dir.join(format!(
            ".owner-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let _ = fs::remove_file(&staged);
        let written = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&staged)
            .and_then(|mut file| {
                std::io::Write::write_all(&mut file, owner_id.as_bytes())?;
                file.sync_all()
            });
        match written {
            Ok(()) => Some(staged),
            Err(_) => {
                let _ = fs::remove_file(&staged);
                None
            }
        }
    }

    /// Names of real directories under the Task-root directory that are
    /// Task-root shaped, in name order after `after`, at most `limit`. Links,
    /// files and every name that is not Forge-shaped are not listed, so no
    /// rule ever sees them.
    pub fn task_root_names(&self, after: &str, limit: usize) -> Vec<String> {
        let shaped = self.shaped;
        let Ok(entries) = fs::read_dir(&self.task_roots) else {
            return Vec::new();
        };
        let mut names: Vec<String> = entries
            .flatten()
            // `file_type` does not follow a link.
            .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
            .filter_map(|entry| entry.file_name().into_string().ok())
            .filter(|name| name.as_str() > after && shaped(name))
            .collect();
        names.sort();
        names.truncate(limit);
        names
    }

    /// Apply the Task-root rules to `states`:
    ///
    /// - `Unknown`: renamed into `<root>/.forge/gc/<name>-<unix
    ///   seconds>` once it is older than [`ORPHAN_GRACE`]. Never deleted here.
    /// - `Cleaned`: removed.
    /// - `Live`: `<name>.broken-<ms>` copies older than
    ///   [`BROKEN_RETENTION`] are removed; nothing else is touched.
    ///
    /// A name that is not in `states` is skipped: it appeared after the
    /// caller read its table.
    ///
    /// Returns the last name it finished, in name order. The deadline is
    /// checked after each entry, so a pass always finishes at least one and
    /// the caller's cursor always moves: one slow entry cannot starve the
    /// names after it.
    pub fn task_roots(
        &self,
        states: &HashMap<String, RootState>,
        report: &mut GcReport,
    ) -> Option<String> {
        let mut names: Vec<&String> = states.keys().collect();
        names.sort();
        let mut finished = None;
        for (index, name) in names.iter().enumerate() {
            let path = self.task_roots.join(name);
            if plain_name(name)
                && (self.shaped)(name)
                && sandbox::is_real_dir(&path)
                && !self.holds_compiler_cache(&path)
            {
                match states[*name] {
                    RootState::Unknown => self.quarantine(name, &path, report),
                    RootState::Cleaned => self.remove(&path, report),
                    RootState::Live => self.broken_copies(&path, report),
                }
            }
            finished = Some((*name).clone());
            if self.out_of_time() && index + 1 < names.len() {
                report.out_of_time = true;
                break;
            }
        }
        finished
    }

    /// Whether `path` is a store of the shared compiler cache, the cache
    /// directory, or a directory the cache lives under. Such a directory
    /// is never a Task root, whatever its name and whatever a table says:
    /// it is never quarantined and never removed as a cleaned Task's
    /// leftover. (The default cache directory is under `.forge`, which no
    /// owner's Task-root shape matches; this is for a cache directory an
    /// operator put somewhere a Task-root name could reach.) Asked only
    /// while a cache is installed for this root: with the feature off a
    /// pass does nothing it did not do before.
    fn holds_compiler_cache(&self, path: &Path) -> bool {
        compiler_cache::installed(&self.root).is_some_and(|cache| {
            cache.dir.starts_with(path)
                || fs::symlink_metadata(path.join(compiler_cache::MARKER_FILE)).is_ok()
        })
    }

    fn quarantine(&self, name: &str, path: &Path, report: &mut GcReport) {
        let fresh = fs::symlink_metadata(path)
            .and_then(|metadata| metadata.modified())
            .map(|modified| age(self.now, modified) < ORPHAN_GRACE)
            .unwrap_or(true);
        // A create in flight in this process, and anything that does not
        // look like a Task root Forge made (a directory a user happened to
        // give such a name), is left exactly where it is.
        if fresh || (self.creating)(path) || !forge_made(path) || !self.confined(path) {
            return;
        }
        let gc_dir = self.gc_dir();
        if !self.prepare_gc_dir(&gc_dir) {
            report.errors += 1;
            return;
        }
        let target = gc_dir.join(format!("{name}-{}", unix_secs(self.now)));
        if fs::symlink_metadata(&target).is_ok() {
            return;
        }
        // A rename only: across filesystems it fails and the directory stays.
        // There is no copy-and-delete fallback and nothing is deleted here.
        match fs::rename(path, &target) {
            Ok(()) => {
                tracing::warn!(path = %path.display(), quarantine = %target.display(), "unknown Task-root directory quarantined; it is deleted after 24 hours");
                report.quarantined += 1;
            }
            Err(error) => {
                tracing::warn!(path = %path.display(), %error, "unknown Task-root directory could not be quarantined");
                report.errors += 1;
            }
        }
    }

    /// `.forge` and `.forge/gc` as real directories, created one level at a
    /// time and never through a link.
    fn prepare_gc_dir(&self, gc_dir: &Path) -> bool {
        let Some(forge) = gc_dir.parent() else {
            return false;
        };
        [forge, gc_dir]
            .into_iter()
            .all(|dir| sandbox::create_private_dir(dir).is_ok() && sandbox::is_real_dir(dir))
    }

    /// The names (without the timestamp) of everything in quarantine.
    pub fn quarantined_names(&self) -> Vec<String> {
        self.quarantine_entries()
            .into_iter()
            .map(|(name, _, _)| name)
            .collect()
    }

    fn quarantine_entries(&self) -> Vec<(String, u64, PathBuf)> {
        let gc_dir = self.gc_dir();
        if !sandbox::is_real_dir(&gc_dir) {
            return Vec::new();
        }
        let Ok(entries) = fs::read_dir(&gc_dir) else {
            return Vec::new();
        };
        let mut found: Vec<_> = entries
            .flatten()
            .filter_map(|entry| {
                let file_name = entry.file_name().into_string().ok()?;
                let (name, at) = file_name.rsplit_once('-')?;
                // Another owner may share this root; its entries are its own.
                (self.shaped)(name).then_some(())?;
                Some((name.to_owned(), at.parse::<u64>().ok()?, entry.path()))
            })
            .collect();
        found.sort();
        found
    }

    /// Settle the quarantine. An entry whose name is in `known` (its record
    /// appeared after it was quarantined) is moved back when its place is
    /// still empty and otherwise left where it is; it is never deleted. Any
    /// other entry is deleted [`QUARANTINE_RETENTION`] after it was
    /// quarantined.
    pub fn quarantine_settle(&self, known: &HashSet<String>, report: &mut GcReport) {
        for (name, at, path) in self.quarantine_entries() {
            if self.out_of_time() {
                report.out_of_time = true;
                return;
            }
            if known.contains(&name) {
                let home = self.task_roots.join(&name);
                if plain_name(&name)
                    && sandbox::is_real_dir(&path)
                    && fs::symlink_metadata(&home).is_err()
                    && fs::rename(&path, &home).is_ok()
                {
                    tracing::info!(path = %home.display(), "quarantined Task root restored: its record appeared");
                    report.restored += 1;
                }
                continue;
            }
            let quarantined_at = UNIX_EPOCH + Duration::from_secs(at);
            if age(self.now, quarantined_at) >= QUARANTINE_RETENTION {
                self.remove(&path, report);
            }
        }
    }

    fn broken_copies(&self, task_root: &Path, report: &mut GcReport) {
        let Ok(entries) = fs::read_dir(task_root) else {
            return;
        };
        for entry in entries.flatten() {
            let Some(made) = entry
                .file_name()
                .to_str()
                .and_then(|name| name.rsplit_once(".broken-"))
                .and_then(|(_, millis)| millis.parse::<u64>().ok())
                .map(|millis| UNIX_EPOCH + Duration::from_millis(millis))
            else {
                continue;
            };
            if age(self.now, made) >= BROKEN_RETENTION {
                self.remove(&entry.path(), report);
            }
        }
    }

    /// Remove `check-*` checkouts under `<root>/.forge/build/checks` that no
    /// live check can own: last modified more than [`CHECK_CHECKOUT_AGE`]
    /// ago, and either the owner's table holds no live check operation or
    /// the checkout predates this process (every operation in the table was
    /// started by it).
    pub fn check_checkouts(&self, live_check_operations: usize, report: &mut GcReport) {
        let checks = self.root.join(CHECKS_DIR);
        if !sandbox::is_real_dir(&checks) {
            return;
        }
        let Ok(entries) = fs::read_dir(&checks) else {
            return;
        };
        let started = sandbox::process_start();
        for entry in entries.flatten() {
            if self.out_of_time() {
                report.out_of_time = true;
                return;
            }
            if !entry.file_name().to_string_lossy().starts_with("check-") {
                continue;
            }
            let path = entry.path();
            let Ok(modified) = fs::symlink_metadata(&path).and_then(|metadata| metadata.modified())
            else {
                continue;
            };
            if age(self.now, modified) >= self.check_checkout_age
                && (live_check_operations == 0 || modified < started)
            {
                self.remove(&path, report);
            }
        }
    }

    /// Remove per-run temp directories no live run owns; see
    /// [`sandbox::sweep_stale_runs`]. `floor` shortens how long a dead
    /// run's directory is kept while the disk is short
    /// ([`Self::run_dir_age`]).
    pub fn run_dirs<'a>(
        &self,
        live: impl IntoIterator<Item = &'a str>,
        floor: &FreeFloor,
        report: &mut GcReport,
    ) {
        report.run_dirs_removed +=
            sandbox::sweep_stale_runs(&self.task_roots, live, self.now, self.run_dir_age(floor));
    }

    /// How long a per-run temp directory nobody claims is kept:
    /// [`MAX_RUN_AGE`], and under the free-space floor only as long as the
    /// longest configured limit of a run that is in no table allows.
    ///
    /// What age protects is a run no table knows (a hook, a check, a
    /// detached push); executions are kept by the live set whatever their
    /// age. The one configured limit on such a run is the check-run wall
    /// limit, and [`Sweep::check_checkout_age`] is that limit doubled plus
    /// an hour, the age at which a check's own checkout is already taken as
    /// dead. So under the floor the directories crashed runs leave behind
    /// stop accumulating for a day: with the default 30-minute limit they go
    /// after two hours. Never longer than [`MAX_RUN_AGE`].
    pub fn run_dir_age(&self, floor: &FreeFloor) -> Duration {
        if self.under_floor(floor) {
            self.check_checkout_age.min(MAX_RUN_AGE)
        } else {
            MAX_RUN_AGE
        }
    }

    /// The given Task roots that have build output, least recently used
    /// first. `candidates` are Task-root names.
    pub fn builds_by_last_use(&self, candidates: &[String]) -> Vec<String> {
        let mut builds: Vec<(SystemTime, &String)> = candidates
            .iter()
            .filter(|name| plain_name(name))
            .filter_map(|name| {
                let task_root = self.task_roots.join(name);
                sandbox::is_real_dir(&task_root).then_some(())?;
                Some((build_last_used(&task_root)?, name))
            })
            .collect();
        builds.sort();
        builds.into_iter().map(|(_, name)| name.clone()).collect()
    }

    /// Whether the filesystem of the root is under `floor`. `false` when it
    /// cannot be read: an unreadable disk evicts nothing.
    pub fn under_floor(&self, floor: &FreeFloor) -> bool {
        (self.disk_space)(&self.root)
            .is_some_and(|space| floor.pressure(&space.facts(String::new(), None)).is_some())
    }

    /// Collect the shared compiler cache of this root.
    ///
    /// - Under `floor`: least recently used entries go first, until the
    ///   disk is back above the floor or the cache is down to half its
    ///   configured size. This runs before any per-Task build output is
    ///   evicted: a cache entry costs one recompile of one crate, a Task's
    ///   build output costs that Task a cold build. Only for a cache on the
    ///   root's own filesystem: entries deleted from another disk give the
    ///   short one nothing back.
    /// - On every pass, whatever the disk has free and wherever the cache
    ///   is: all stores together are held to the configured size. Each
    ///   wrapper caps its own store, so without this a machine with many
    ///   repositories holds that many times the size the operator set.
    ///
    /// Only entry files of stores Forge marked are deleted
    /// ([`compiler_cache::evict`]). Also records the cache's size for the
    /// machine's disk facts. With no cache installed and none left behind
    /// this does nothing at all.
    ///
    /// `live_elsewhere`: the owner runs something its table knows and the
    /// live-run registry names no repository for (a check, a command). A
    /// store whose wrapper is not known to tolerate eviction under a live
    /// build is then left alone.
    pub fn evict_compiler_cache(
        &self,
        floor: &FreeFloor,
        live_elsewhere: bool,
        report: &mut GcReport,
    ) {
        let Some(cache) = compiler_cache::collected(&self.root) else {
            return;
        };
        let dir = &cache.dir;
        let mut note = |done: compiler_cache::Eviction, why: &str| {
            report.cache_entries_evicted += done.files_removed;
            report.out_of_time |= done.out_of_time;
            if done.files_removed > 0 {
                tracing::warn!(path = %dir.display(), entries = done.files_removed, bytes = done.bytes_freed, "{why}: evicted least recently used compiler-cache entries");
            }
        };
        if self.under_floor(floor) && compiler_cache::shares_filesystem(dir, &self.root) {
            let done = compiler_cache::evict(
                dir,
                cache.keep_under_floor,
                live_elsewhere,
                self.deadline,
                || !self.under_floor(floor),
            );
            note(done, "disk is under its free-space floor");
        }
        let mut measured = compiler_cache::measure(dir, self.deadline);
        if let (Some(cap), Some(bytes)) = (cache.cap, measured) {
            if bytes > cap {
                let done = compiler_cache::evict(dir, cap, live_elsewhere, self.deadline, || false);
                measured = Some(bytes.saturating_sub(done.bytes_freed));
                note(
                    done,
                    "the shared compiler cache is over its configured size",
                );
            }
        }
        if let Some(bytes) = measured {
            compiler_cache::note_measured(&self.root, bytes);
        }
    }

    /// Remove the compiler-cache stores of repositories the owner no longer
    /// has: `known` is every repository id in the owner's records (the
    /// server's repository rows; a daemon's repository locations and the
    /// repositories of its handles). A store goes [`ORPHAN_GRACE`] after it
    /// was last handed to a run, like an orphaned Task root, and never while
    /// a run of this process holds it or while `live_elsewhere` (the owner
    /// runs something the live-run registry names no repository for).
    /// Nothing is looked at on a root with no cache, nothing is removed for
    /// an owner that knows no repository at all, and a cache directory
    /// configured outside the managed root is never swept this way (it is
    /// bounded by its size cap only).
    pub fn remove_cache_stores_of_deleted_repositories(
        &self,
        known: &HashSet<String>,
        live_elsewhere: bool,
        report: &mut GcReport,
    ) {
        if live_elsewhere {
            return;
        }
        // An owner with no repository at all proves nothing about any
        // store (a reset database, a daemon that lost its table).
        if known.is_empty() {
            return;
        }
        let Some(cache) = compiler_cache::collected(&self.root) else {
            return;
        };
        // Only a cache inside the managed root is this owner's alone: the
        // root's ownership marker keeps every other database out of it. A
        // directory configured elsewhere can be shared by several servers,
        // and the repositories of the others are not in `known`.
        let inside_root = matches!(
            (fs::canonicalize(&cache.dir), fs::canonicalize(&self.root)),
            (Ok(dir), Ok(root)) if dir != root && dir.starts_with(&root)
        );
        if !inside_root {
            return;
        }
        let wrapper = compiler_cache::installed(&self.root).map(|cache| cache.wrapper.clone());
        let done = compiler_cache::remove_stores_of_deleted_repositories(
            &cache.dir,
            wrapper.as_deref(),
            known,
            self.now,
            ORPHAN_GRACE,
            self.deadline,
        );
        report.cache_stores_removed += done.stores;
        report.out_of_time |= done.out_of_time;
    }

    /// Take the build output of one Task root the caller proved idle: moved
    /// to the trash (one rename) and deleted later by [`Sweep::empty_trash`].
    ///
    /// The live-run check and the rename happen under the lock every run
    /// start takes ([`sandbox::unless_live_run_in`]), so a run that starts
    /// while this is deciding either is seen (and keeps its build output) or
    /// starts after the rename and gets a fresh directory. It can never have
    /// the directory deleted from under it. `false` when nothing was taken.
    pub fn evict_build(&self, name: &str, report: &mut GcReport) -> bool {
        let task_root = self.task_roots.join(name);
        if !plain_name(name) || !sandbox::is_real_dir(&task_root) {
            return false;
        }
        let build = task_root.join(TASK_DIR_NAME).join("build");
        if !sandbox::is_real_dir(&task_root.join(TASK_DIR_NAME)) || !sandbox::is_real_dir(&build) {
            return false;
        }
        // Reached from the Task-root directory by file descriptor: the
        // Task can write its root and could put a link where `.forge-task`
        // was just seen, and a rename by path would then take the `build`
        // of some directory outside the root.
        let moved = sandbox::unless_live_run_in(&task_root, || {
            self.trash_from(
                &build,
                Some((
                    &self.task_roots,
                    &Path::new(name).join(TASK_DIR_NAME).join("build"),
                )),
                report,
            )
        });
        if moved == Some(true) {
            report.builds_evicted += 1;
            tracing::warn!(path = %build.display(), "disk is under its free-space floor: evicted the build output of an idle Task");
            return true;
        }
        false
    }

    /// Free space by evicting the build output of the given Task roots,
    /// least recently used first, until the filesystem of the root is back
    /// above `floor` (bytes and inodes). `candidates` are Task-root names the caller
    /// proved idle; a root with a run of this process is skipped regardless.
    /// For an owner whose table is its own lock (a daemon); the server evicts
    /// one root at a time under the Task's lifecycle lock.
    pub fn evict_builds(&self, candidates: &[String], floor: &FreeFloor, report: &mut GcReport) {
        if !self.under_floor(floor) {
            return;
        }
        for name in self.builds_by_last_use(candidates) {
            if self.out_of_time() {
                report.out_of_time = true;
                return;
            }
            if !self.evict_build(&name, report) {
                continue;
            }
            // Space comes back only when the trash is emptied.
            let before = report.removed;
            self.empty_trash(report);
            // The build output itself is counted as an eviction.
            if report.removed > before {
                report.removed -= 1;
            }
            if !self.under_floor(floor) {
                return;
            }
        }
    }

    /// Move a condemned directory out of its place into
    /// `<root>/.forge/gc/trash`, to be deleted by [`Sweep::empty_trash`]. A
    /// rename, so a caller may do it under a lock it must not hold for the
    /// length of a delete. `false` when it could not be moved; it then stays.
    pub fn trash(&self, path: &Path, report: &mut GcReport) -> bool {
        self.trash_from(path, None, report)
    }

    /// [`Self::trash`], with `path` given a second time as `(base,
    /// relative)` when directories between `base` and the entry are
    /// writable by a Task: the entry is then moved without following any of
    /// them, and stays when one has become a link.
    fn trash_from(
        &self,
        path: &Path,
        anchored: Option<(&Path, &Path)>,
        report: &mut GcReport,
    ) -> bool {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        if fs::symlink_metadata(path).is_err() {
            return false;
        }
        let gc_dir = self.gc_dir();
        let trash = self.root.join(TRASH_DIR);
        let name = path.file_name().and_then(|name| name.to_str());
        let (Some(name), true) = (
            name,
            self.confined(path)
                && self.prepare_gc_dir(&gc_dir)
                && sandbox::create_private_dir(&trash).is_ok()
                && sandbox::is_real_dir(&trash),
        ) else {
            report.errors += 1;
            return false;
        };
        let target = trash.join(format!(
            "{name}-{}-{}",
            self.now
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        // Every entry is moved from the root by file descriptor, so no
        // directory on the way is followed when a Task made it a link.
        let from_root = self.anchored(path);
        let anchored = anchored.or(from_root
            .as_ref()
            .map(|(root, relative)| (root.as_path(), relative.as_path())));
        let rename = || match anchored {
            #[cfg(unix)]
            Some((base, relative)) => crate::fd_tree::rename_under(base, relative, &target),
            #[cfg(unix)]
            None => Err(std::io::Error::from(std::io::ErrorKind::InvalidInput)),
            #[cfg(not(unix))]
            _ => fs::rename(path, &target),
        };
        if fs::symlink_metadata(&target).is_ok() || rename().is_err() {
            report.errors += 1;
            return false;
        }
        true
    }

    /// Delete everything in the trash. Each entry was condemned by its owner
    /// before it was moved there, so nothing is decided again.
    pub fn empty_trash(&self, report: &mut GcReport) {
        let trash = self.root.join(TRASH_DIR);
        if !sandbox::is_real_dir(&self.gc_dir()) || !sandbox::is_real_dir(&trash) {
            return;
        }
        let Ok(entries) = fs::read_dir(&trash) else {
            return;
        };
        for entry in entries.flatten() {
            self.remove(&entry.path(), report);
            if self.out_of_time() {
                report.out_of_time = true;
                return;
            }
        }
    }

    /// Remove one entry of the managed root: a directory with everything in
    /// it (read-only trees included), a link or a file as the entry it is.
    /// A tree too large for what is left of the pass is removed in part and
    /// reported as `out_of_time`, never as an error.
    pub fn remove(&self, path: &Path, report: &mut GcReport) {
        if fs::symlink_metadata(path).is_err() {
            return;
        }
        if !self.confined(path) {
            tracing::warn!(path = %path.display(), "refusing to remove a path outside the managed workspace root");
            report.errors += 1;
            return;
        }
        // Reached from the root by file descriptor: `confined` looked at
        // where the parent resolves, and a Task that can write a directory
        // on the way could have put a link there since.
        match self.anchored(path) {
            Some((root, relative)) => {
                remove_entry_under(&root, &relative, report, Some(self.deadline));
            }
            None => report.errors += 1,
        }
    }

    /// `path` as the resolved managed root and the plain names from there
    /// to the entry.
    fn anchored(&self, path: &Path) -> Option<(PathBuf, PathBuf)> {
        let relative = path.strip_prefix(&self.root).ok()?;
        let plain = relative
            .components()
            .all(|part| matches!(part, Component::Normal(_)));
        (plain && relative.file_name().is_some())
            .then(|| fs::canonicalize(&self.root).ok())
            .flatten()
            .map(|root| (root, relative.to_path_buf()))
    }

    /// `path` is strictly inside the managed root and its parent resolves
    /// there too, so removing the entry cannot reach outside through a link.
    fn confined(&self, path: &Path) -> bool {
        if path
            .components()
            .any(|part| matches!(part, Component::ParentDir))
        {
            return false;
        }
        let (Some(parent), Ok(root)) = (path.parent(), fs::canonicalize(&self.root)) else {
            return false;
        };
        path != self.root
            && path.starts_with(&self.root)
            && fs::canonicalize(parent).is_ok_and(|parent| parent.starts_with(root))
    }
}

/// Remove one exact path the caller owns outright (a legacy location outside
/// any Task root). A link is removed as a link; its target is not touched.
pub fn remove_exact(path: &Path, report: &mut GcReport) {
    if fs::symlink_metadata(path).is_ok() {
        remove_entry(path, report, None);
    }
}

/// Whether `path` and everything directly inside it were last modified at
/// least `quiet` before `now`. `false` when it cannot be read.
pub fn untouched_for(path: &Path, now: SystemTime, quiet: Duration) -> bool {
    let old = |path: &Path| {
        fs::symlink_metadata(path)
            .and_then(|metadata| metadata.modified())
            .is_ok_and(|modified| age(now, modified) >= quiet)
    };
    if !old(path) {
        return false;
    }
    if !sandbox::is_real_dir(path) {
        return true;
    }
    let Ok(entries) = fs::read_dir(path) else {
        return false;
    };
    entries.flatten().take(4096).all(|entry| old(&entry.path()))
}

/// Whether a directory looks like a Task root Forge made: it carries the
/// reserved `.forge-task` directory or an outbox, or one of its children is a
/// linked git worktree (a directory whose `.git` is a file). A plain
/// directory a user created under a Task-shaped name has none of these.
fn forge_made(task_root: &Path) -> bool {
    if sandbox::is_real_dir(&task_root.join(TASK_DIR_NAME))
        || sandbox::is_real_dir(&task_root.join(".forge-outbox"))
    {
        return true;
    }
    let Ok(entries) = fs::read_dir(task_root) else {
        return false;
    };
    entries.flatten().take(256).any(|entry| {
        entry.file_type().is_ok_and(|kind| kind.is_dir())
            && fs::symlink_metadata(entry.path().join(".git"))
                .is_ok_and(|metadata| metadata.file_type().is_file())
    })
}

/// Remove one entry. With a deadline the removal stops between entries when
/// the pass is out of time: what is left stays where it is (in the trash, in
/// quarantine, in its Task root) and the next pass carries on with it.
fn remove_entry(path: &Path, report: &mut GcReport, deadline: Option<Instant>) {
    let finished = sandbox::remove_tree_until(path, deadline);
    removal_outcome(path, finished, report);
}

/// [`remove_entry`] for `base.join(relative)`, with no directory between
/// the two followed when it is a link.
fn remove_entry_under(
    base: &Path,
    relative: &Path,
    report: &mut GcReport,
    deadline: Option<Instant>,
) {
    let finished = sandbox::remove_tree_under_until(base, relative, deadline);
    removal_outcome(&base.join(relative), finished, report);
}

fn removal_outcome(path: &Path, finished: bool, report: &mut GcReport) {
    if !finished {
        report.out_of_time = true;
        return;
    }
    if fs::symlink_metadata(path).is_ok() {
        tracing::warn!(path = %path.display(), "workspace garbage could not be removed");
        report.errors += 1;
    } else {
        report.removed += 1;
    }
}

/// A directory name with no separator and no traversal.
fn plain_name(name: &str) -> bool {
    let mut parts = Path::new(name).components();
    matches!(parts.next(), Some(Component::Normal(_))) && parts.next().is_none()
}

fn age(now: SystemTime, then: SystemTime) -> Duration {
    now.duration_since(then).unwrap_or_default()
}

fn unix_secs(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// The free-space floor of one managed root: one type for the eviction here
/// and for the admission refusal, so the two can never disagree.
pub use api_types::DiskFloor as FreeFloor;

/// One `statvfs` reading of the filesystem holding a path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DiskSpace {
    /// Bytes free to an unprivileged process.
    pub free: u64,
    pub total: u64,
    /// Inodes free to an unprivileged process; `None` on a filesystem that
    /// does not count them.
    pub free_inodes: Option<u64>,
    pub total_inodes: Option<u64>,
}

impl DiskSpace {
    /// This reading as the fact a machine reports.
    pub fn facts(
        &self,
        measured_at: String,
        gc_state: Option<String>,
    ) -> api_types::MachineDiskFacts {
        api_types::MachineDiskFacts {
            free_bytes: self.free,
            total_bytes: self.total,
            free_inodes: self.free_inodes,
            total_inodes: self.total_inodes,
            measured_at,
            gc_state,
            compiler_cache_bytes: None,
        }
    }
}

/// Free (to an unprivileged process) and total bytes and inodes of the
/// filesystem holding `path`, from `statvfs`. `None` when it cannot be read;
/// callers then leave everything alone.
#[cfg(unix)]
#[allow(unsafe_code)]
pub fn disk_space(path: &Path) -> Option<DiskSpace> {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::zeroed();
    // SAFETY: `path` is a valid NUL-terminated string and `stat` is a
    // writable `statvfs` that the call fills in when it returns 0.
    let stat = unsafe {
        if libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) != 0 {
            return None;
        }
        stat.assume_init()
    };
    #[allow(clippy::unnecessary_cast)]
    let (block, free, total, inodes_free, inodes) = (
        stat.f_frsize as u64,
        stat.f_bavail as u64,
        stat.f_blocks as u64,
        stat.f_favail as u64,
        stat.f_files as u64,
    );
    // A filesystem that reports no size reports nothing usable; one that
    // reports no inodes simply does not count them.
    (block > 0 && total > 0).then(|| DiskSpace {
        free: free.saturating_mul(block),
        total: total.saturating_mul(block),
        free_inodes: (inodes > 0).then_some(inodes_free),
        total_inodes: (inodes > 0).then_some(inodes),
    })
}

#[cfg(not(unix))]
pub fn disk_space(_path: &Path) -> Option<DiskSpace> {
    None
}

/// When the build output of `task_root` was last used: the newest
/// modification time among `.forge-task/build` and what lies two levels
/// below it (toolchains rewrite a marker there on every invocation). `None`
/// when there is no build output to evict.
pub fn build_last_used(task_root: &Path) -> Option<SystemTime> {
    let build = task_root.join(TASK_DIR_NAME).join("build");
    if !sandbox::is_real_dir(&task_root.join(TASK_DIR_NAME)) || !sandbox::is_real_dir(&build) {
        return None;
    }
    let mut newest = None;
    let mut pending = vec![(build, 0_u8)];
    let mut entries_seen = 0_usize;
    while let Some((dir, depth)) = pending.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            entries_seen += 1;
            let Ok(metadata) = fs::symlink_metadata(entry.path()) else {
                continue;
            };
            if let Ok(modified) = metadata.modified() {
                newest = newest.max(Some(modified));
            }
            if metadata.file_type().is_dir() && depth < 2 && entries_seen < 4096 {
                pending.push((entry.path(), depth + 1));
            }
        }
    }
    // An empty build directory holds nothing worth evicting.
    newest
}

/// Disk bytes under `path`, never following a link. `None` when the walk
/// passes `deadline` or [`MEASURE_ENTRY_LIMIT`]: a partial number is not a
/// measurement.
pub fn measure(path: &Path, deadline: Instant) -> Option<u64> {
    let root = fs::symlink_metadata(path).ok()?;
    if !root.file_type().is_dir() {
        return None;
    }
    let mut bytes = disk_bytes(&root);
    let mut visited = 0_usize;
    let mut pending = vec![path.to_path_buf()];
    while let Some(dir) = pending.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            visited += 1;
            if visited > MEASURE_ENTRY_LIMIT
                || (visited.is_multiple_of(256) && Instant::now() >= deadline)
            {
                return None;
            }
            // `DirEntry::metadata` does not follow a link.
            let Ok(metadata) = entry.metadata() else {
                continue;
            };
            bytes = bytes.saturating_add(disk_bytes(&metadata));
            if metadata.file_type().is_dir() {
                pending.push(entry.path());
            }
        }
    }
    Some(bytes)
}

#[cfg(unix)]
fn disk_bytes(metadata: &fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    metadata.blocks().saturating_mul(512)
}

#[cfg(not(unix))]
fn disk_bytes(metadata: &fs::Metadata) -> u64 {
    metadata.len()
}

#[cfg(test)]
mod tests;
