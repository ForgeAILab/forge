//! The daemon's garbage-collection pass over its workspace root.
//!
//! The same sweep the server runs (`executors::gc`), decided from this
//! daemon's persisted handle table instead of a database: a
//! `<root>/.forge/workspaces/workspace-<uuid>` directory with no handle is
//! quarantined (never deleted on sight), the leftovers of a cleaned handle
//! are removed, and dead run directories, check checkouts, broken copies and
//! the legacy shared Codex home go once nothing alive can own them.
//!
//! Every other name under the root is invisible to it. A daemon has no log
//! retention rule: its execution logs are not kept per Task.
//!
//! Only one sweeper per root. The handle table carries an owner id, the root
//! carries it in `<root>/.forge/gc/daemon-owner` (written once, when the backend is
//! built on a root nobody owns), and the backend holds an exclusive lock on
//! `<root>/.forge/gc/daemon.lock` for its whole life. A daemon whose state
//! was lost, and a second daemon process on the same root, sweep nothing.

use super::*;
use executors::gc::{FreeFloor, GcReport, Ownership, RootState, Sweep, DAEMON_OWNER_FILE, GC_DIR};
use std::time::SystemTime;

/// How often a running daemon sweeps its root.
pub const GC_INTERVAL: Duration = Duration::from_secs(10 * 60);
/// How often it looks at its disk between sweeps.
pub const GC_DISK_CHECK_INTERVAL: Duration = Duration::from_secs(30);
/// While the disk is under the collector mark it sweeps this often instead.
pub const GC_PRESSURE_INTERVAL: Duration = Duration::from_secs(60);

/// The free-space floor each workspace root of this process was given by its
/// server, in the reply to its report. A daemon has no floor of its own: the
/// server holds every machine to one, so eviction here and refusal there
/// agree. Kept per root: two backends in one process (tests, a daemon beside
/// a server) answer to their own servers and never see each other's floor.
static SERVER_FLOORS: std::sync::Mutex<Option<HashMap<PathBuf, FreeFloor>>> =
    std::sync::Mutex::new(None);
/// What each root's ownership check found, for the disk report.
static GC_STATES: std::sync::Mutex<Option<HashMap<PathBuf, &'static str>>> =
    std::sync::Mutex::new(None);

/// One name for a root however it was spelled (`--workspace-root` through a
/// link, a relative path): every per-root fact is kept under it.
fn root_key(workspace_root: &Path) -> PathBuf {
    workspace_root
        .canonicalize()
        .unwrap_or_else(|_| workspace_root.to_path_buf())
}

/// Take the floor for `workspace_root` from the server's reply to a report.
pub fn accept_floor(workspace_root: &Path, floor: Option<FreeFloor>) {
    let Some(floor) = floor else {
        return;
    };
    let key = root_key(workspace_root);
    let mut floors = SERVER_FLOORS.lock().unwrap_or_else(|p| p.into_inner());
    let floors = floors.get_or_insert_with(HashMap::new);
    if floors.get(&key) != Some(&floor) {
        tracing::info!(
            root = %key.display(),
            ?floor,
            "workspace free-space floor received from the server"
        );
        floors.insert(key, floor);
    }
}

/// The floor the server sent for `workspace_root`, if it has sent one.
pub fn server_floor(workspace_root: &Path) -> Option<FreeFloor> {
    SERVER_FLOORS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .as_ref()
        .and_then(|floors| floors.get(&root_key(workspace_root)).copied())
}

/// The floor the collector of `workspace_root` evicts to: the server's.
/// Before the first reply (and with a server that sends none) it is the
/// built-in default, and says so once.
pub fn floor(workspace_root: &Path) -> FreeFloor {
    static WARNED: std::sync::Once = std::sync::Once::new();
    server_floor(workspace_root).unwrap_or_else(|| {
        WARNED.call_once(|| {
            tracing::warn!("the server has sent no workspace free-space floor yet; the collector uses the built-in default (10 GiB or 5 % of the filesystem, 5 % of its inodes) until it does");
        });
        FreeFloor::default()
    })
}

/// The typed refusal of work that needs new disk (a new or recreated
/// worktree, a check checkout) while this machine's own reading, taken now,
/// is under the floor its server sent.
///
/// The server decides admission from this daemon's last report, which can be
/// a minute old or, from a daemon that could not report, much older. This is
/// what keeps a full disk from being filled on such a reading. No floor from
/// the server yet, or a disk that cannot be read: nothing is refused.
pub fn disk_pressure_refusal(
    floor: Option<FreeFloor>,
    reading: Option<executors::gc::DiskSpace>,
) -> Option<api_types::DaemonErrorPayload> {
    let facts = reading?.facts(String::new(), None);
    let kind = floor?.pressure(&facts)?;
    Some(api_types::DaemonErrorPayload {
        code: api_types::DISK_PRESSURE.to_owned(),
        message: format!(
            "the workspace filesystem of this machine is under its free-space floor ({}): {} bytes free of {}",
            kind.as_str(),
            facts.free_bytes,
            facts.total_bytes
        ),
        details: Some(serde_json::json!({
            "kind": kind.as_str(),
            "free_bytes": facts.free_bytes,
            "total_bytes": facts.total_bytes,
        })),
    })
}

fn note_gc_state(workspace_root: &Path, state: &'static str) {
    GC_STATES
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get_or_insert_with(HashMap::new)
        .insert(root_key(workspace_root), state);
}

/// The disk facts of `workspace_root` for a daemon report: free bytes and
/// inodes of its filesystem, read now, and whether this daemon's collector
/// runs on it. `None` when the filesystem cannot be read; the server then
/// refuses nothing for disk on this machine.
pub fn disk_report(workspace_root: &Path) -> Option<api_types::MachineDiskFacts> {
    // One warning while it stays unreadable, not one per report.
    static UNREADABLE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    let Some(space) = executors::gc::disk_space(workspace_root) else {
        if !UNREADABLE.swap(true, std::sync::atomic::Ordering::Relaxed) {
            tracing::warn!(root = %workspace_root.display(), "free space of the workspace root cannot be read; no disk facts are reported and nothing is refused for disk on this machine until it can");
        }
        return None;
    };
    UNREADABLE.store(false, std::sync::atomic::Ordering::Relaxed);
    let resolved = root_key(workspace_root);
    let gc_state = GC_STATES
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .as_ref()
        .and_then(|states| states.get(&resolved).copied())
        .unwrap_or("unclaimed");
    Some(space.facts(chrono::Utc::now().to_rfc3339(), Some(gc_state.to_owned())))
}
const GC_BUDGET: Duration = Duration::from_secs(60);
const GC_PAGE: usize = 64;
/// One directory for every Codex execution of a daemon, used before each
/// Task root got its own home.
const LEGACY_CODEX_HOME: &str = ".forge-daemon/execution-logs/.codex-managed-home";

fn is_handle(name: &str) -> bool {
    name.strip_prefix("workspace-")
        .is_some_and(|id| id.len() == 36 && uuid::Uuid::parse_str(id).is_ok())
}

/// Give the handle table an owner id, adopt the root for it when nobody owns
/// the root, and take the root's sweeper lock. `None` when this backend must
/// never sweep: the root is owned by another table, cannot be a workspace
/// root, or another live backend holds the lock.
pub(super) fn adopt_root(
    workspace_root: &Path,
    state: &mut WorkspaceRegistry,
    journal: &DaemonJournal,
) -> Option<std::fs::File> {
    if state.gc_owner_id.is_none() {
        let mut updated = state.clone();
        updated.gc_owner_id = Some(uuid::Uuid::new_v4().to_string());
        // An id that was not persisted is not an identity.
        journal.save_workspace_state(&updated).ok()?;
        *state = updated;
    }
    let owner_id = state.gc_owner_id.clone()?;
    let mut sweep = Sweep::new(
        workspace_root,
        &workspace_root.join(WORKTREE_DIRECTORY),
        is_handle,
        Duration::ZERO,
    );
    sweep.owner_file = DAEMON_OWNER_FILE;
    let ownership = sweep.adopt(&owner_id);
    note_gc_state(workspace_root, ownership.as_str());
    match ownership {
        Ownership::Mine => {}
        ownership => {
            tracing::warn!(
                root = %workspace_root.display(),
                state = ownership.as_str(),
                "workspace garbage collection is off for this daemon: its workspace root is owned by another daemon state or cannot be a workspace root. Nothing is reclaimed and the disk can fill. If the root belongs to this daemon (its state was reset), stop it and delete .forge/gc/daemon-owner under the root"
            );
            return None;
        }
    }
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(workspace_root.join(GC_DIR).join("daemon.lock"))
        .ok()?;
    match lock.try_lock() {
        Ok(()) => Some(lock),
        Err(_) => {
            tracing::warn!(
                root = %workspace_root.display(),
                "another daemon is running on this workspace root; this one leaves garbage collection to it"
            );
            note_gc_state(workspace_root, Ownership::Other.as_str());
            None
        }
    }
}

async fn blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> Option<T> {
    tokio::task::spawn_blocking(work).await.ok()
}

impl DaemonWorkspaceBackend {
    /// One budgeted pass. `active_ids` are the executions this daemon is
    /// running. Errors on single entries are counted, never returned.
    pub async fn gc_sweep(&self, active_ids: &[String]) -> GcReport {
        self.gc_sweep_at(active_ids, SystemTime::now(), floor(&self.workspace_root))
            .await
    }

    /// Whether the root's filesystem is under the mark at which the
    /// collector should not wait for its timer. `false` when unreadable.
    pub fn disk_is_short(&self) -> bool {
        executors::gc::disk_space(&self.workspace_root).is_some_and(|space| {
            floor(&self.workspace_root).wants_gc(&space.facts(String::new(), None))
        })
    }

    /// Refuse work that needs new disk while this machine's own reading is
    /// under the floor its server sent: see [`disk_pressure_refusal`].
    pub(super) fn refuse_new_disk_under_pressure(
        &self,
    ) -> Result<(), api_types::DaemonErrorPayload> {
        match disk_pressure_refusal(
            server_floor(&self.workspace_root),
            executors::gc::disk_space(&self.workspace_root),
        ) {
            Some(refusal) => Err(refusal),
            None => Ok(()),
        }
    }

    /// The handle table as every backend on this root knows it: this one's
    /// memory joined with what is persisted. A handle another runtime
    /// recorded since this one loaded is as real as its own, and a handle
    /// counts as cleaned only when nobody holds it live.
    fn gc_handles(&self) -> Option<HashMap<String, OwnedWorkspace>> {
        let mut handles = self
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .handles
            .clone();
        // An unreadable table proves nothing about any directory.
        let persisted = self
            .journal
            .load_workspace_state::<WorkspaceRegistry>()
            .ok()?;
        for (handle, owned) in persisted.handles {
            match handles.get_mut(&handle) {
                Some(known) => {
                    known.cleaned &= owned.cleaned;
                    known.execution_ids.extend(owned.execution_ids);
                }
                None => {
                    handles.insert(handle, owned);
                }
            }
        }
        Some(handles)
    }

    pub(super) async fn gc_sweep_at(
        &self,
        active_ids: &[String],
        now: SystemTime,
        floor: FreeFloor,
    ) -> GcReport {
        let task_roots = self.workspace_root.join(WORKTREE_DIRECTORY);
        let mut sweep = Sweep::new(&self.workspace_root, &task_roots, is_handle, GC_BUDGET);
        sweep.now = now;
        sweep.creating = workspace::is_creating;
        sweep.owner_file = DAEMON_OWNER_FILE;
        let mut report = GcReport::default();
        let out_of_time = |sweep: &Sweep| std::time::Instant::now() >= sweep.deadline;
        // Only the holder of the root lock sweeps, and only while the root
        // still names this handle table as its owner.
        let owner_id = self
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .gc_owner_id
            .clone();
        let (Some(_), Some(owner_id)) = (&self.gc_lock, owner_id) else {
            return report;
        };
        let pass = sweep.clone();
        if blocking(move || pass.ownership(&owner_id)).await != Some(Ownership::Mine) {
            tracing::warn!(root = %self.workspace_root.display(), "workspace gc: this daemon no longer owns its workspace root; nothing swept");
            return report;
        }

        // Directory names first, the table second: a prepare records its
        // handle before it creates the directory, so a directory seen here
        // without a handle read afterwards has none.
        let after = self
            .gc_cursor
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let pass = sweep.clone();
        let names = blocking(move || pass.task_root_names(&after, GC_PAGE))
            .await
            .unwrap_or_default();
        let Some(handles) = self.gc_handles() else {
            tracing::warn!("workspace gc: handle table unreadable; nothing swept");
            report.errors += 1;
            return report;
        };
        let mut states = HashMap::new();
        for name in &names {
            match handles.get(name) {
                None => {
                    states.insert(name.clone(), RootState::Unknown);
                }
                Some(owned) if owned.cleaned => {
                    self.gc_cleaned_root(&sweep, name, active_ids, &mut report)
                        .await;
                }
                Some(_) => {
                    states.insert(name.clone(), RootState::Live);
                }
            }
        }
        let pass = sweep.clone();
        let (done, finished) = blocking(move || {
            let mut report = GcReport::default();
            let finished = pass.task_roots(&states, &mut report);
            // Condemned under a handle's lock above, deleted outside it.
            pass.empty_trash(&mut report);
            (report, finished)
        })
        .await
        .unwrap_or_default();
        report.merge(&done);
        {
            // Resume after the last name this pass finished, so one slow
            // entry never starves the names after it.
            let mut cursor = self.gc_cursor.lock().unwrap_or_else(|p| p.into_inner());
            match (finished, names.last()) {
                (Some(finished), _) if done.out_of_time => *cursor = finished,
                (_, Some(last)) if names.len() >= GC_PAGE => cursor.clone_from(last),
                _ => cursor.clear(),
            }
        }

        // Everything below reads the table again: time has passed.
        let Some(handles) = self.gc_handles() else {
            report.errors += 1;
            return report;
        };
        let live_commands = self
            .running_commands
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .len();
        let known: std::collections::HashSet<String> = handles.keys().cloned().collect();
        let live: Vec<String> = active_ids.to_vec();
        // The legacy home is shared by every execution that still falls back
        // to it (one whose Task root was never reserved), so it goes only
        // while this daemon runs no execution and no command at all.
        let idle = active_ids.is_empty() && live_commands == 0;
        let legacy = self.workspace_root.join(LEGACY_CODEX_HOME);
        // A handle with an active execution or any running command keeps its
        // build output; so does one with a run of this process (checked by
        // the eviction itself).
        let candidates: Vec<String> = if live_commands == 0 {
            handles
                .iter()
                .filter(|(_, owned)| !owned.cleaned && owned.prepared)
                .filter(|(_, owned)| !owned.execution_ids.iter().any(|id| active_ids.contains(id)))
                .map(|(handle, _)| handle.clone())
                .collect()
        } else {
            Vec::new()
        };
        let pass = sweep.clone();
        let done = blocking(move || {
            let mut report = GcReport::default();
            pass.quarantine_settle(&known, &mut report);
            pass.run_dirs(live.iter().map(String::as_str), &floor, &mut report);
            pass.check_checkouts(live_commands, &mut report);
            if idle {
                pass.remove(&legacy, &mut report);
            }
            pass.evict_builds(&candidates, &floor, &mut report);
            report
        })
        .await
        .unwrap_or_default();
        report.merge(&done);
        report.out_of_time |= out_of_time(&sweep);
        if report.did_something() {
            tracing::info!(?report, "workspace gc pass");
        }
        report
    }

    /// Leftovers of a handle that was cleaned: moved to the trash under the
    /// handle's owner lock, after reading the table again. The move is one
    /// rename; the delete happens after the lock is released.
    async fn gc_cleaned_root(
        &self,
        sweep: &Sweep,
        handle: &str,
        active_ids: &[String],
        report: &mut GcReport,
    ) {
        let lock = self.owner_lock(&format!("workspace:{handle}"));
        let _guard = lock.lock().await;
        let still_cleaned = self.gc_handles().is_some_and(|handles| {
            handles.get(handle).is_some_and(|owned| {
                owned.cleaned && !owned.execution_ids.iter().any(|id| active_ids.contains(id))
            })
        });
        if !still_cleaned {
            return;
        }
        let (pass, path) = (
            sweep.clone(),
            self.workspace_root.join(WORKTREE_DIRECTORY).join(handle),
        );
        let done = blocking(move || {
            let mut report = GcReport::default();
            if !executors::sandbox::has_live_run_in(&path) && !workspace::is_creating(&path) {
                pass.trash(&path, &mut report);
            }
            report
        })
        .await
        .unwrap_or_default();
        report.merge(&done);
    }
}
