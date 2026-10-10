//! The server's garbage-collection pass over its workspace root.
//!
//! It runs at the end of every terminal-Task sweep, inside the same time
//! budget, and resumes from a cursor. The filesystem mechanics (and their
//! link, confinement and budget guarantees) are `executors::gc`; this module
//! decides, from the database, what each directory is.
//!
//! What it never touches: `.forge`, `.forge-tmp`, `.repos`, `main-agents`,
//! any directory whose name is not a Task id (36-character UUID), a directory
//! named like an existing Project, and anything outside the workspace root
//! except the exact legacy locations Forge itself used to create.

use super::{SweepCursor, WorkspaceCleanupScheduler, SWEEP_LIMIT};
use crate::{workflow::engine::WorkflowEngine, Result};
use db::{now_rfc3339, ProjectRepo, TaskRepo};
use executors::gc::{self, FreeFloor, GcReport, RootState, Sweep};
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime},
};

/// Workspace rows measured per pass, least recently measured first.
const MEASURE_LIMIT: i64 = 32;
/// Live workspaces considered for build eviction per pass.
const EVICTION_CANDIDATES: i64 = 256;

pub type LiveCheckCounter = Arc<dyn Fn() -> usize + Send + Sync>;

/// Operator settings and wiring of the garbage-collection pass.
#[derive(Clone)]
pub struct GcSettings {
    /// Off by default: a scheduler built for a test (often on the shared
    /// default workspace root) sweeps and claims nothing. The running
    /// server turns it on.
    pub enabled: bool,
    /// Days the logs of a terminal Task are kept; `0` keeps them forever.
    pub log_retention_days: u32,
    pub free_floor: FreeFloor,
    /// The system temp directory Forge used before the Task-root layout.
    /// Only the running server sets it: without it the pass removes nothing
    /// outside the workspace root.
    pub legacy_temp_dir: Option<PathBuf>,
    /// Live operations in the server check runner's table.
    pub live_check_operations: Option<LiveCheckCounter>,
}

impl Default for GcSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            log_retention_days: config::DEFAULT_LOG_RETENTION_DAYS,
            free_floor: FreeFloor::default(),
            legacy_temp_dir: None,
            live_check_operations: None,
        }
    }
}

enum RootClass {
    /// Recorded and in use (or not ours to judge): leftovers only.
    Live,
    /// Every workspace row for it is `cleaned`.
    Cleaned,
    /// No workspace row, no Task, no Project.
    Unknown,
}

fn is_task_id(name: &str) -> bool {
    name.len() == 36 && uuid::Uuid::parse_str(name).is_ok()
}

async fn blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> Option<T> {
    tokio::task::spawn_blocking(work).await.ok()
}

impl WorkspaceCleanupScheduler {
    pub fn set_gc_limits(&self, log_retention_days: u32, free_floor: FreeFloor) {
        self.update_gc_settings(|settings| {
            settings.log_retention_days = log_retention_days;
            settings.free_floor = free_floor;
        });
    }

    /// Turn the garbage-collection pass on for this workspace root.
    pub fn enable_gc(&self) {
        self.update_gc_settings(|settings| settings.enabled = true);
    }

    /// Let the pass remove the legacy locations under the system temp
    /// directory. Called by the running server only, never by a test.
    pub fn set_legacy_temp_dir(&self, temp_dir: PathBuf) {
        self.update_gc_settings(|settings| settings.legacy_temp_dir = Some(temp_dir));
    }

    pub fn set_live_check_counter(&self, counter: LiveCheckCounter) {
        self.update_gc_settings(|settings| settings.live_check_operations = Some(counter));
    }

    fn update_gc_settings(&self, update: impl FnOnce(&mut GcSettings)) {
        match self.gc_settings.write() {
            Ok(mut settings) => update(&mut settings),
            Err(error) => tracing::warn!(%error, "workspace gc settings lock poisoned"),
        }
    }

    pub(super) fn gc_settings(&self) -> GcSettings {
        self.gc_settings
            .read()
            .map(|settings| settings.clone())
            .unwrap_or_default()
    }

    /// The directory a Task's logs live in.
    pub(super) fn task_logs_dir(&self, project_id: &str, task_id: &str) -> PathBuf {
        self.workspace_root
            .join(".forge")
            .join("logs")
            .join(project_id)
            .join(task_id)
    }

    /// Remove the logs of a terminal Task once they are older than the
    /// retention. The caller has proven the Task terminal and idle.
    pub(super) async fn expire_task_logs(&self, task: &db::Task) {
        let days = self.gc_settings().log_retention_days;
        if days == 0 || !is_plain(&task.project_id) || !is_plain(&task.id) {
            return;
        }
        let Ok(terminal_since) = chrono::DateTime::parse_from_rfc3339(&task.updated_at) else {
            return;
        };
        if terminal_since + chrono::Duration::days(i64::from(days)) > chrono::Utc::now() {
            return;
        }
        let logs = self.task_logs_dir(&task.project_id, &task.id);
        let sweep = Sweep::new(
            &self.workspace_root,
            &self.workspace_root,
            is_task_id,
            Duration::ZERO,
        );
        let report = blocking(move || {
            let mut report = GcReport::default();
            sweep.remove(&logs, &mut report);
            report
        })
        .await
        .unwrap_or_default();
        if report.removed > 0 {
            tracing::info!(task_id = %task.id, days, "logs of a terminal Task removed after their retention");
        }
    }

    pub(super) async fn gc_pass(&self, cursor: &mut SweepCursor, budget: Duration) -> GcReport {
        self.gc_pass_at(cursor, budget, SystemTime::now()).await
    }

    /// One pass as of `now`. Each step is independent: a failure in one is
    /// logged and the pass moves on; running out of time ends the pass and
    /// leaves the cursor where the next one resumes.
    pub(super) async fn gc_pass_at(
        &self,
        cursor: &mut SweepCursor,
        budget: Duration,
        now: SystemTime,
    ) -> GcReport {
        let mut sweep = Sweep::new(
            &self.workspace_root,
            &self.workspace_root,
            is_task_id,
            budget,
        );
        sweep.now = now;
        let settings = self.gc_settings();
        let mut report = GcReport::default();
        if !settings.enabled {
            return report;
        }
        let out_of_time = |sweep: &Sweep| std::time::Instant::now() >= sweep.deadline;
        match self.owns_root(&sweep).await {
            Ok(true) => {}
            Ok(false) => {
                if std::fs::symlink_metadata(
                    self.workspace_root.join(gc::GC_DIR).join(gc::OWNER_FILE),
                )
                .is_ok()
                {
                    tracing::warn!(
                        root = %self.workspace_root.display(),
                        "workspace root is claimed by another Forge database; garbage collection is off for it. Give each server its own workspace root, or delete .forge/gc/owner under the root if it belongs to this server"
                    );
                }
                return report;
            }
            Err(error) => {
                tracing::warn!(%error, "workspace gc: root ownership unreadable; nothing swept");
                return report;
            }
        }

        if let Err(error) = self.gc_task_roots(&sweep, cursor, &mut report).await {
            tracing::warn!(%error, "workspace gc: Task-root pass failed");
        }
        if !out_of_time(&sweep) {
            if let Err(error) = self.gc_quarantine(&sweep, &mut report).await {
                tracing::warn!(%error, "workspace gc: quarantine pass failed");
            }
        }
        let running = match self.running_execution_ids().await {
            Ok(running) => Some(running),
            Err(error) => {
                tracing::warn!(%error, "workspace gc: running executions unreadable; run directories and legacy homes are left alone");
                None
            }
        };
        let live_checks = settings
            .live_check_operations
            .as_ref()
            .map_or(0, |counter| counter());
        if let Some(running) = running.filter(|_| !out_of_time(&sweep)) {
            let legacy = settings.legacy_temp_dir.clone();
            let pass = sweep.clone();
            let swept = blocking(move || {
                let mut report = GcReport::default();
                pass.run_dirs(running.iter().map(String::as_str), &mut report);
                pass.check_checkouts(live_checks, &mut report);
                // The legacy homes are shared by every run that still falls
                // back to them, so they go only while nothing runs at all.
                if let (Some(temp), true) = (legacy, running.is_empty()) {
                    legacy_temp_locations(&temp, &mut report);
                }
                report
            })
            .await
            .unwrap_or_default();
            report.merge(&swept);
        }
        if !out_of_time(&sweep) && live_checks == 0 {
            if let Err(error) = self
                .gc_evict_builds(&sweep, &settings.free_floor, &mut report)
                .await
            {
                tracing::warn!(%error, "workspace gc: build eviction failed");
            }
        }
        if !out_of_time(&sweep) {
            if let Err(error) = self.gc_measure(&sweep).await {
                tracing::warn!(%error, "workspace gc: measurement failed");
            }
        }
        report.out_of_time |= out_of_time(&sweep);
        if report.did_something() {
            tracing::info!(?report, "workspace gc pass");
        }
        report
    }

    /// Whether this database is the one that sweeps the workspace root; see
    /// [`Sweep::claim`]. The identity is created once and kept with the data.
    async fn owns_root(&self, sweep: &Sweep) -> Result<bool> {
        const KEY: &str = "workspace_gc_owner_id";
        sqlx::query(
            "INSERT INTO system_setting (key, value, updated_at) VALUES (?, ?, ?)
             ON CONFLICT(key) DO NOTHING",
        )
        .bind(KEY)
        .bind(db::new_uuid_v4())
        .bind(now_rfc3339())
        .execute(self.db.pool())
        .await?;
        let owner_id =
            sqlx::query_scalar::<_, String>("SELECT value FROM system_setting WHERE key = ?")
                .bind(KEY)
                .fetch_one(self.db.pool())
                .await?;
        let pass = sweep.clone();
        Ok(blocking(move || pass.claim(&owner_id))
            .await
            .unwrap_or(false))
    }

    async fn gc_task_roots(
        &self,
        sweep: &Sweep,
        cursor: &mut SweepCursor,
        report: &mut GcReport,
    ) -> Result<()> {
        let (pass, after) = (sweep.clone(), cursor.root_name.clone());
        let names = blocking(move || pass.task_root_names(&after, SWEEP_LIMIT as usize))
            .await
            .unwrap_or_default();
        let mut states = HashMap::new();
        for name in &names {
            match self.classify_root(name).await {
                Ok(RootClass::Unknown) => {
                    states.insert(name.clone(), RootState::Unknown);
                }
                // Removed below under the Task's lifecycle lock.
                Ok(RootClass::Cleaned) => {
                    if let Err(error) = self.remove_cleaned_root(sweep, name, report).await {
                        tracing::warn!(task_id = %name, %error, "workspace gc: cleaned Task root not removed");
                        report.errors += 1;
                    }
                }
                Ok(RootClass::Live) => {
                    states.insert(name.clone(), RootState::Live);
                }
                Err(error) => {
                    tracing::warn!(directory = %name, %error, "workspace gc: directory not classified; left alone");
                    report.errors += 1;
                }
            }
            if std::time::Instant::now() >= sweep.deadline {
                break;
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
        if done.out_of_time || std::time::Instant::now() >= sweep.deadline {
            // Resume at the same page: nothing in it is skipped.
            return Ok(());
        }
        match names.last() {
            Some(last) if names.len() >= SWEEP_LIMIT as usize => cursor.root_name = last.clone(),
            _ => cursor.root_name.clear(),
        }
        Ok(())
    }

    /// Path prefixes of the Task root `name`, as recorded and as resolved.
    fn root_prefixes(&self, name: &str) -> Vec<String> {
        let mut prefixes = vec![format!("{}/", self.workspace_root.join(name).display())];
        if let Ok(resolved) = std::fs::canonicalize(&self.workspace_root) {
            let resolved = format!("{}/", resolved.join(name).display());
            if !prefixes.contains(&resolved) {
                prefixes.push(resolved);
            }
        }
        prefixes
    }

    async fn classify_root(&self, name: &str) -> Result<RootClass> {
        let project =
            sqlx::query_scalar::<_, i64>("SELECT EXISTS (SELECT 1 FROM project WHERE id = ?)")
                .bind(name)
                .fetch_one(self.db.pool())
                .await?;
        if project != 0 {
            return Ok(RootClass::Live);
        }
        let mut statuses =
            sqlx::query_scalar::<_, String>("SELECT status FROM workspace WHERE task_id = ?")
                .bind(name)
                .fetch_all(self.db.pool())
                .await?;
        for prefix in self.root_prefixes(name) {
            statuses.extend(
                sqlx::query_scalar::<_, String>(
                    "SELECT status FROM workspace
                     WHERE substr(worktree_path, 1, length(?1)) = ?1",
                )
                .bind(prefix)
                .fetch_all(self.db.pool())
                .await?,
            );
        }
        if statuses.iter().any(|status| status != "cleaned") {
            return Ok(RootClass::Live);
        }
        if !statuses.is_empty() {
            return Ok(RootClass::Cleaned);
        }
        // A create has its directory before its workspace row, but never
        // before its Task.
        let task = sqlx::query_scalar::<_, i64>("SELECT EXISTS (SELECT 1 FROM task WHERE id = ?)")
            .bind(name)
            .fetch_one(self.db.pool())
            .await?;
        Ok(if task != 0 {
            RootClass::Live
        } else {
            RootClass::Unknown
        })
    }

    async fn task_is_terminal(&self, task: &db::Task) -> Result<bool> {
        let Some(project) = ProjectRepo::get_by_id(&*self.db, &task.project_id).await? else {
            return Ok(false);
        };
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            task,
            &project.workflow_definition,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
        );
        Ok(workflow.state_kind(&task.status) == Some(api_types::StateKind::Terminal))
    }

    /// A running execution or an active lease of the Task or of a workspace
    /// it owns.
    async fn task_is_active(&self, task_id: &str) -> Result<bool> {
        let active = sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS (
                SELECT 1 FROM execution
                WHERE (task_id = ?1 OR workspace_id IN (SELECT id FROM workspace WHERE task_id = ?1))
                  AND status = 'running'
             ) OR EXISTS (
                SELECT 1 FROM workspace_lease wl
                LEFT JOIN execution e ON e.id = wl.execution_id
                WHERE (wl.task_id = ?1
                       OR e.workspace_id IN (SELECT id FROM workspace WHERE task_id = ?1))
                  AND wl.status = 'active'
             ) OR EXISTS (
                SELECT 1 FROM task child JOIN execution e ON e.task_id = child.id
                WHERE child.parent_task_id = ?1 AND e.status = 'running'
             )",
        )
        .bind(task_id)
        .fetch_one(self.db.pool())
        .await?;
        Ok(active != 0)
    }

    /// Leftovers of a workspace that was reclaimed: removed under the Task's
    /// lifecycle lock, and only while the Task is terminal and idle, so a
    /// reopened Task never loses a root it is about to use.
    async fn remove_cleaned_root(
        &self,
        sweep: &Sweep,
        name: &str,
        report: &mut GcReport,
    ) -> Result<()> {
        let Some(task) = TaskRepo::get_by_id(&*self.db, name, true).await? else {
            return Ok(());
        };
        let _guard = self.lock_task(&task).await;
        let Some(task) = TaskRepo::get_by_id(&*self.db, name, true).await? else {
            return Ok(());
        };
        if !matches!(self.classify_root(name).await?, RootClass::Cleaned)
            || !self.task_is_terminal(&task).await?
            || self.task_is_active(name).await?
        {
            return Ok(());
        }
        let (pass, path) = (sweep.clone(), self.workspace_root.join(name));
        let done = blocking(move || {
            let mut report = GcReport::default();
            if executors::sandbox::has_live_run_in(&path) {
                return report;
            }
            pass.remove(&path, &mut report);
            report
        })
        .await
        .unwrap_or_default();
        report.merge(&done);
        Ok(())
    }

    async fn gc_quarantine(&self, sweep: &Sweep, report: &mut GcReport) -> Result<()> {
        let pass = sweep.clone();
        let names = blocking(move || pass.quarantined_names())
            .await
            .unwrap_or_default();
        if names.is_empty() {
            return Ok(());
        }
        let mut known = HashSet::new();
        for name in names {
            // Anything but "no record at all" keeps the directory, and so
            // does a record that cannot be read.
            if !matches!(self.classify_root(&name).await, Ok(RootClass::Unknown)) {
                known.insert(name);
            }
        }
        let pass = sweep.clone();
        let done = blocking(move || {
            let mut report = GcReport::default();
            pass.quarantine_settle(&known, &mut report);
            report
        })
        .await
        .unwrap_or_default();
        report.merge(&done);
        Ok(())
    }

    async fn running_execution_ids(&self) -> Result<Vec<String>> {
        Ok(
            sqlx::query_scalar::<_, String>("SELECT id FROM execution WHERE status = 'running'")
                .fetch_all(self.db.pool())
                .await?,
        )
    }

    /// The Task-root directory name of a recorded worktree path, when the
    /// path is `<workspace root>/<name>/<worktree>`.
    fn task_root_name(&self, worktree_path: &str) -> Option<String> {
        let task_root = Path::new(worktree_path).parent()?;
        let name = task_root.file_name()?.to_str()?;
        let parent = task_root.parent()?;
        let ours = parent == self.workspace_root
            || std::fs::canonicalize(&self.workspace_root).is_ok_and(|root| parent == root);
        (ours && is_task_id(name)).then(|| name.to_owned())
    }

    async fn gc_evict_builds(
        &self,
        sweep: &Sweep,
        floor: &FreeFloor,
        report: &mut GcReport,
    ) -> Result<()> {
        let (root, floor) = (self.workspace_root.clone(), *floor);
        let under_floor = blocking(move || {
            gc::disk_space(&root).is_some_and(|space| space.free < floor.bytes(space.total))
        })
        .await
        .unwrap_or(false);
        if !under_floor {
            return Ok(());
        }
        let rows = sqlx::query_as::<_, (String, String)>(
            "SELECT task_id, worktree_path FROM workspace
             WHERE status = 'ready' ORDER BY updated_at, id LIMIT ?",
        )
        .bind(EVICTION_CANDIDATES)
        .fetch_all(self.db.pool())
        .await?;
        let mut candidates = Vec::new();
        for (task_id, worktree_path) in rows {
            let Some(name) = self.task_root_name(&worktree_path) else {
                continue;
            };
            let Some(task) = TaskRepo::get_by_id(&*self.db, &task_id, false).await? else {
                continue;
            };
            // A terminal Task's build output goes with its Task root; a busy
            // Task keeps what it is building with.
            if self.task_is_terminal(&task).await? || self.task_is_active(&task_id).await? {
                continue;
            }
            candidates.push(name);
        }
        let pass = sweep.clone();
        let done = blocking(move || {
            let mut report = GcReport::default();
            pass.evict_builds(&candidates, &floor, &mut report);
            report
        })
        .await
        .unwrap_or_default();
        report.merge(&done);
        Ok(())
    }

    /// Measure live server-owned Task roots, least recently measured first.
    /// A walk that does not finish writes nothing.
    async fn gc_measure(&self, sweep: &Sweep) -> Result<()> {
        let mut rows = Vec::new();
        for prefix in self.root_prefixes("") {
            // `<root>//` → `<root>/`: every worktree under this root.
            let prefix = prefix.trim_end_matches('/').to_owned() + "/";
            rows.extend(
                sqlx::query_as::<_, (String, String)>(
                    "SELECT id, worktree_path FROM workspace
                     WHERE status NOT IN ('cleaned', 'cleaning')
                       AND substr(worktree_path, 1, length(?1)) = ?1
                     ORDER BY disk_measured_at IS NOT NULL, disk_measured_at, id LIMIT ?2",
                )
                .bind(prefix)
                .bind(MEASURE_LIMIT)
                .fetch_all(self.db.pool())
                .await?,
            );
        }
        for (workspace_id, worktree_path) in rows {
            let Some(name) = self.task_root_name(&worktree_path) else {
                continue;
            };
            let (task_root, deadline) = (self.workspace_root.join(name), sweep.deadline);
            let measured = blocking(move || gc::measure(&task_root, deadline))
                .await
                .flatten();
            if std::time::Instant::now() >= sweep.deadline {
                return Ok(());
            }
            // A root too large to walk inside one pass keeps `disk_bytes`
            // NULL and moves to the back of the queue.
            sqlx::query(
                "UPDATE workspace SET disk_bytes = COALESCE(?, disk_bytes), disk_measured_at = ?
                 WHERE id = ? AND status NOT IN ('cleaned', 'cleaning')",
            )
            .bind(measured.map(|bytes| i64::try_from(bytes).unwrap_or(i64::MAX)))
            .bind(now_rfc3339())
            .bind(&workspace_id)
            .execute(self.db.pool())
            .await?;
        }
        Ok(())
    }
}

fn is_plain(name: &str) -> bool {
    let mut parts = Path::new(name).components();
    matches!(parts.next(), Some(std::path::Component::Normal(_))) && parts.next().is_none()
}

fn real_dir(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_dir())
}

/// The exact locations under the system temp directory that Forge created
/// before the Task-root layout and no longer writes:
///
/// - `<temp>/forge-gemini-api-key-home`
/// - `<temp>/forge/logs/<task id>/hooks` (and the `<task id>` directory when
///   that leaves it empty)
///
/// Nothing else under the temp directory is read or removed, and no link on
/// the way is followed.
pub(super) fn legacy_temp_locations(temp: &Path, report: &mut GcReport) {
    gc::remove_exact(&temp.join("forge-gemini-api-key-home"), report);
    let logs = temp.join("forge").join("logs");
    if !real_dir(&temp.join("forge")) || !real_dir(&logs) {
        return;
    }
    let Ok(entries) = std::fs::read_dir(&logs) else {
        return;
    };
    for entry in entries.flatten() {
        let task_dir = entry.path();
        if !entry.file_name().to_str().is_some_and(is_task_id) || !real_dir(&task_dir) {
            continue;
        }
        gc::remove_exact(&task_dir.join("hooks"), report);
        // Only when empty: anything else in it is not ours to judge.
        let _ = std::fs::remove_dir(&task_dir);
    }
}
