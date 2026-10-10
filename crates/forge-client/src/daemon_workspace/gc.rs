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

use super::*;
use executors::gc::{FreeFloor, GcReport, RootState, Sweep};
use std::time::SystemTime;

/// How often a running daemon sweeps its root.
pub const GC_INTERVAL: Duration = Duration::from_secs(10 * 60);
const GC_BUDGET: Duration = Duration::from_secs(60);
const GC_PAGE: usize = 64;
/// One directory for every Codex execution of a daemon, used before each
/// Task root got its own home.
const LEGACY_CODEX_HOME: &str = ".forge-daemon/execution-logs/.codex-managed-home";

fn is_handle(name: &str) -> bool {
    name.strip_prefix("workspace-")
        .is_some_and(|id| id.len() == 36 && uuid::Uuid::parse_str(id).is_ok())
}

async fn blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> Option<T> {
    tokio::task::spawn_blocking(work).await.ok()
}

impl DaemonWorkspaceBackend {
    /// One budgeted pass. `active_ids` are the executions this daemon is
    /// running. Errors on single entries are counted, never returned.
    pub async fn gc_sweep(&self, active_ids: &[String]) -> GcReport {
        self.gc_sweep_at(active_ids, SystemTime::now(), FreeFloor::default())
            .await
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
        let mut report = GcReport::default();
        let out_of_time = |sweep: &Sweep| std::time::Instant::now() >= sweep.deadline;

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
        let done = blocking(move || {
            let mut report = GcReport::default();
            pass.task_roots(&states, &mut report);
            report
        })
        .await
        .unwrap_or_default();
        report.merge(&done);
        if !done.out_of_time && !out_of_time(&sweep) {
            let mut cursor = self.gc_cursor.lock().unwrap_or_else(|p| p.into_inner());
            match names.last() {
                Some(last) if names.len() >= GC_PAGE => cursor.clone_from(last),
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
            pass.run_dirs(live.iter().map(String::as_str), &mut report);
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

    /// Leftovers of a handle that was cleaned: removed under the handle's
    /// owner lock, after reading the table again.
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
            if !executors::sandbox::has_live_run_in(&path) {
                pass.remove(&path, &mut report);
            }
            report
        })
        .await
        .unwrap_or_default();
        report.merge(&done);
    }
}
