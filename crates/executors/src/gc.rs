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
/// Default free-space floor: the larger of this many bytes and
/// [`DEFAULT_MIN_FREE_PERCENT`] of the filesystem.
pub const DEFAULT_MIN_FREE_BYTES: u64 = 10 * 1024 * 1024 * 1024;
pub const DEFAULT_MIN_FREE_PERCENT: u8 = 5;
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
        self.errors += other.errors;
        self.out_of_time |= other.out_of_time;
    }

    pub fn did_something(&self) -> bool {
        self.quarantined
            + self.restored
            + self.removed
            + self.run_dirs_removed
            + self.builds_evicted
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
            if plain_name(name) && (self.shaped)(name) && sandbox::is_real_dir(&path) {
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
    /// [`sandbox::sweep_stale_runs`].
    pub fn run_dirs<'a>(&self, live: impl IntoIterator<Item = &'a str>, report: &mut GcReport) {
        report.run_dirs_removed +=
            sandbox::sweep_stale_runs(&self.task_roots, live, self.now, MAX_RUN_AGE);
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
        let moved = sandbox::unless_live_run_in(&task_root, || self.trash(&build, report));
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
        if fs::symlink_metadata(&target).is_ok() || fs::rename(path, &target).is_err() {
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
        remove_entry(path, report, Some(self.deadline));
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
    if !sandbox::remove_tree_until(path, deadline) {
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
mod tests {
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
        later(&root, Duration::from_secs(48 * 3600))
            .quarantine_settle(&HashSet::new(), &mut report);
        assert_eq!(report.removed, 0);
        assert!(kept.path().exists());

        later(&root, Duration::from_secs(50 * 3600))
            .quarantine_settle(&HashSet::new(), &mut report);
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
        sweep(&root).run_dirs([], &mut report);
        assert!(stale.exists() && tmp.exists());
        // Much later it is older than any run; the live one still stands.
        later(&root, 3 * DAY).run_dirs([], &mut report);
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

        // A floor no disk can meet: every idle build goes, the busy one stays
        // whenever its run is visible to the registry.
        let all = FreeFloor::of_bytes(u64::MAX, 0);
        sweep(&root).evict_builds(&names, &all, &mut report);
        assert!(!old.exists());
        if run.tmp_dir().is_some() {
            assert!(busy.exists());
            assert_eq!(report.builds_evicted, 1);
        }
        assert_eq!(report.removed, 0);
        run.settle();
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
}
