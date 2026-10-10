//! The server's garbage-collection pass over its workspace root.
//!
//! It runs at the end of every terminal-Task sweep, inside the same time
//! budget, and resumes from a cursor. It runs only on a root this database
//! owns: `<root>/.forge/gc/owner`, written once by a running server at
//! start-up ([`WorkspaceCleanupScheduler::adopt_workspace_root`]), names it.
//! A scheduler nobody adopted a root for (every test, every tool) sweeps
//! nothing and writes nothing. The filesystem mechanics (and their
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
use executors::gc::{self, FreeFloor, GcReport, Ownership, RootState, Sweep};
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

/// Legacy hook-log directories looked at per pass.
const LEGACY_LOG_DIRS: usize = 256;
const OWNER_KEY: &str = "workspace_gc_owner_id";
/// What the last ownership check found, for operator status.
pub const STATUS_KEY: &str = "workspace_gc_status";

pub type LiveCheckCounter = Arc<dyn Fn() -> usize + Send + Sync>;

/// Operator settings and wiring of the garbage-collection pass. Nothing here
/// turns the pass on or off: it runs exactly when this database owns the
/// root.
#[derive(Clone)]
pub struct GcSettings {
    /// Days the logs of a terminal Task are kept; `0` keeps them forever.
    pub log_retention_days: u32,
    pub free_floor: FreeFloor,
    /// Age after which a check checkout nobody holds is dead, from the
    /// configured check wall limit.
    pub check_checkout_age: Duration,
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
            log_retention_days: config::DEFAULT_LOG_RETENTION_DAYS,
            free_floor: FreeFloor::default(),
            check_checkout_age: gc::CHECK_CHECKOUT_AGE,
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

    /// The configured wall limit of one check run, in seconds.
    pub fn set_check_timeout(&self, seconds: u64) {
        self.update_gc_settings(|settings| {
            settings.check_checkout_age = gc::check_checkout_age(seconds);
        });
    }

    /// Adopt the workspace root for this database when nobody owns it yet:
    /// the one step that writes Forge's marker, and so the one step that
    /// lets the garbage collector run. Called once by a running server at
    /// start-up, never by a test builder, never by the sweep. A root owned
    /// by another database stays theirs.
    pub async fn adopt_workspace_root(&self) -> Result<Ownership> {
        self.settle_ownership(|sweep, owner| sweep.adopt(owner))
            .await
    }

    /// Take the workspace root over from whoever owns it. Only on an
    /// operator's explicit request (`forge --reclaim-workspace-gc`).
    pub async fn reclaim_workspace_root(&self) -> Result<Ownership> {
        self.settle_ownership(|sweep, owner| sweep.reclaim(owner))
            .await
    }

    async fn settle_ownership(
        &self,
        settle: impl FnOnce(&Sweep, &str) -> Ownership + Send + 'static,
    ) -> Result<Ownership> {
        let owner_id = self.gc_owner_id().await?;
        let configured = self.workspace_root.clone();
        let (root, ownership) = blocking(move || {
            // The configured root of a running server may not exist yet.
            let _ = std::fs::create_dir_all(&configured);
            let root = std::fs::canonicalize(&configured).unwrap_or(configured);
            let sweep = Sweep::new(&root, &root, is_task_id, Duration::ZERO);
            let ownership = settle(&sweep, &owner_id);
            (root, ownership)
        })
        .await
        .ok_or_else(|| crate::ServiceError::invalid_operation("workspace gc ownership check"))?;
        self.record_gc_status(&root, ownership).await;
        Ok(ownership)
    }

    /// This database's identity as a workspace-root owner: created once and
    /// kept with the data, so a reset database is a different owner.
    async fn gc_owner_id(&self) -> Result<String> {
        sqlx::query(
            "INSERT INTO system_setting (key, value, updated_at) VALUES (?, ?, ?)
             ON CONFLICT(key) DO NOTHING",
        )
        .bind(OWNER_KEY)
        .bind(db::new_uuid_v4())
        .bind(now_rfc3339())
        .execute(self.db.pool())
        .await?;
        Ok(
            sqlx::query_scalar::<_, String>("SELECT value FROM system_setting WHERE key = ?")
                .bind(OWNER_KEY)
                .fetch_one(self.db.pool())
                .await?,
        )
    }

    /// Whether this database owns `root` right now. Read-only on disk.
    async fn gc_ownership(&self, root: &Path) -> Result<Ownership> {
        let owner_id = self.gc_owner_id().await?;
        let sweep = Sweep::new(root, root, is_task_id, Duration::ZERO);
        Ok(blocking(move || sweep.ownership(&owner_id))
            .await
            .unwrap_or(Ownership::Other))
    }

    /// Keep what the last check found where operator status reads it.
    /// Written only when it changes.
    async fn record_gc_status(&self, root: &Path, ownership: Ownership) {
        let reason = match ownership {
            Ownership::Refused(reason) => Some(reason),
            _ => None,
        };
        let value = serde_json::json!({
            "state": ownership.as_str(),
            "root": root.display().to_string(),
            "reason": reason,
        })
        .to_string();
        let written = sqlx::query(
            "INSERT INTO system_setting (key, value, updated_at) VALUES (?1, ?2, ?3)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
             WHERE system_setting.value != excluded.value",
        )
        .bind(STATUS_KEY)
        .bind(value)
        .bind(now_rfc3339())
        .execute(self.db.pool())
        .await;
        if let Err(error) = written {
            tracing::warn!(%error, "workspace gc status not recorded");
        }
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
        let Ok(root) = std::fs::canonicalize(&self.workspace_root) else {
            return;
        };
        // Like every other deletion of the pass: only on a root this
        // database owns.
        if !matches!(self.gc_ownership(&root).await, Ok(Ownership::Mine)) {
            return;
        }
        let logs = root
            .join(".forge")
            .join("logs")
            .join(&task.project_id)
            .join(&task.id);
        let sweep = Sweep::new(&root, &root, is_task_id, Duration::ZERO);
        let quiet = Duration::from_secs(u64::from(days) * 24 * 60 * 60);
        let report = blocking(move || {
            let mut report = GcReport::default();
            // A Task imported, restored or finished again with an old
            // timestamp keeps logs that were written inside the retention.
            if gc::untouched_for(&logs, SystemTime::now(), quiet) {
                sweep.remove(&logs, &mut report);
            }
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
        let settings = self.gc_settings();
        let mut report = GcReport::default();
        // The root as it resolves now: every path of the pass is built on
        // it, and the marker must be inside it.
        let Ok(root) = std::fs::canonicalize(&self.workspace_root) else {
            return report;
        };
        let mut sweep = Sweep::new(&root, &root, is_task_id, budget);
        sweep.now = now;
        sweep.creating = workspace::is_creating;
        sweep.check_checkout_age = settings.check_checkout_age;
        let out_of_time = |sweep: &Sweep| std::time::Instant::now() >= sweep.deadline;
        match self.gc_ownership(&root).await {
            Ok(Ownership::Mine) => {}
            // Never adopted: a test, a tool, a server that has not started.
            Ok(Ownership::Unclaimed) => return report,
            Ok(ownership) => {
                self.record_gc_status(&root, ownership).await;
                tracing::warn!(
                    root = %root.display(),
                    state = ownership.as_str(),
                    "workspace garbage collection is off for this root: it is owned by another Forge database or cannot be a workspace root. Nothing is reclaimed and the disk can fill. Give each server its own workspace root; if this root belongs to this server (its database was reset), stop Forge and start it once with --reclaim-workspace-gc"
                );
                return report;
            }
            Err(error) => {
                tracing::warn!(%error, "workspace gc: root ownership unreadable; nothing swept");
                return report;
            }
        }
        self.record_gc_status(&root, Ownership::Mine).await;

        if let Err(error) = self.gc_task_roots(&sweep, cursor, &mut report).await {
            tracing::warn!(%error, "workspace gc: Task-root pass failed");
        }
        // What was condemned under a Task's lock (in this pass or one that
        // was interrupted) is deleted here, outside every lock.
        let pass = sweep.clone();
        let emptied = blocking(move || {
            let mut report = GcReport::default();
            pass.empty_trash(&mut report);
            report
        })
        .await
        .unwrap_or_default();
        report.merge(&emptied);
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
            let known_tasks = match &legacy {
                Some(temp) if running.is_empty() => self.legacy_log_tasks(temp).await,
                _ => HashSet::new(),
            };
            let pass = sweep.clone();
            let swept = blocking(move || {
                let mut report = GcReport::default();
                pass.run_dirs(running.iter().map(String::as_str), &mut report);
                pass.check_checkouts(live_checks, &mut report);
                // The legacy homes are shared by every run that still falls
                // back to them, so they go only while nothing runs at all.
                if let (Some(temp), true) = (legacy, running.is_empty()) {
                    legacy_temp_locations(&temp, &known_tasks, pass.now, &mut report);
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

    /// The Task ids among the legacy hook-log directories that are Tasks of
    /// this database. The system temp directory is shared by every Forge on
    /// the machine; only a directory named after one of our own Tasks is
    /// provably ours.
    async fn legacy_log_tasks(&self, temp: &Path) -> HashSet<String> {
        let logs = temp.join("forge").join("logs");
        let names = blocking(move || {
            let Ok(entries) = std::fs::read_dir(&logs) else {
                return Vec::new();
            };
            entries
                .flatten()
                .filter_map(|entry| entry.file_name().into_string().ok())
                .filter(|name| is_task_id(name))
                .take(LEGACY_LOG_DIRS)
                .collect::<Vec<_>>()
        })
        .await
        .unwrap_or_default();
        let mut known = HashSet::new();
        for name in names {
            let exists =
                sqlx::query_scalar::<_, i64>("SELECT EXISTS (SELECT 1 FROM task WHERE id = ?)")
                    .bind(&name)
                    .fetch_one(self.db.pool())
                    .await;
            if matches!(exists, Ok(found) if found != 0) {
                known.insert(name);
            }
        }
        known
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
        // The deadline is checked after each name, so every pass classifies
        // at least one and the cursor always moves.
        let mut classified = 0;
        for name in &names {
            match self.classify_root(name).await {
                Ok(RootClass::Unknown) => {
                    states.insert(name.clone(), RootState::Unknown);
                }
                // Moved aside below under the Task's lifecycle lock.
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
            classified += 1;
            if std::time::Instant::now() >= sweep.deadline {
                break;
            }
        }
        let pass = sweep.clone();
        let (done, finished) = blocking(move || {
            let mut report = GcReport::default();
            let finished = pass.task_roots(&states, &mut report);
            (report, finished)
        })
        .await
        .unwrap_or_default();
        report.merge(&done);
        // Resume after the last name this pass finished. A page that ran out
        // of time is not started over: the entry that took the time is done
        // or will be skipped past, and the names after it get their turn.
        if done.out_of_time {
            if let Some(finished) = finished {
                cursor.root_name = finished;
            }
        } else if classified < names.len() {
            cursor.root_name = names[classified - 1].clone();
        } else {
            match names.last() {
                Some(last) if names.len() >= SWEEP_LIMIT as usize => {
                    cursor.root_name = last.clone();
                }
                _ => cursor.root_name.clear(),
            }
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

    /// Leftovers of a workspace that was reclaimed: moved out of their place
    /// under the Task's lifecycle lock, and only while the Task is terminal
    /// and idle, so a reopened Task never loses a root it is about to use.
    /// The move is one rename; the delete, however long, happens after the
    /// lock is released ([`Sweep::empty_trash`]).
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
        let (pass, path) = (sweep.clone(), sweep.root.join(name));
        let done = blocking(move || {
            let mut report = GcReport::default();
            if executors::sandbox::has_live_run_in(&path) || workspace::is_creating(&path) {
                return report;
            }
            pass.trash(&path, &mut report);
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
        let (root, floor, disk_space) = (sweep.root.clone(), *floor, sweep.disk_space);
        let under_floor = blocking(move || {
            disk_space(&root).is_some_and(|space| space.free < floor.bytes(space.total))
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
///   that leaves it empty), for a `<task id>` in `known_tasks` only
///
/// The system temp directory is shared by every Forge on the machine (a dev
/// server beside a real one, an older version still running), and nobody can
/// know what they run. So a location goes only after nothing touched it for
/// [`gc::LEGACY_UNTOUCHED`], and a hook-log directory only when it is named
/// after a Task of this database. Nothing else under the temp directory is
/// read or removed, and no link on the way is followed.
pub(super) fn legacy_temp_locations(
    temp: &Path,
    known_tasks: &HashSet<String>,
    now: SystemTime,
    report: &mut GcReport,
) {
    let home = temp.join("forge-gemini-api-key-home");
    if gc::untouched_for(&home, now, gc::LEGACY_UNTOUCHED) {
        gc::remove_exact(&home, report);
    }
    let logs = temp.join("forge").join("logs");
    if !real_dir(&temp.join("forge")) || !real_dir(&logs) {
        return;
    }
    for task_id in known_tasks {
        let task_dir = logs.join(task_id);
        let hooks = task_dir.join("hooks");
        if !is_task_id(task_id)
            || !real_dir(&task_dir)
            || !gc::untouched_for(&hooks, now, gc::LEGACY_UNTOUCHED)
        {
            continue;
        }
        gc::remove_exact(&hooks, report);
        // Only when empty: anything else in it is not ours to judge.
        let _ = std::fs::remove_dir(&task_dir);
    }
}
