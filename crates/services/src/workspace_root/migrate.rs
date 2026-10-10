//! `forge --migrate-workspace-root`: move a stopped server's workspace root.
//!
//! The move is explicit, never part of a start, and keeps every file: the
//! contents of the old root are renamed into the new one (or, across
//! filesystems, copied, compared byte for byte and only then removed), Git's
//! worktree links are repaired, and every absolute path the database stores
//! under the old root is rewritten in one transaction that also records the
//! new root.
//!
//! A journal in the data directory lists each finished step. While it exists
//! no server starts ([`super::settle`] refuses), and running the command
//! again goes on from the last finished step: the database is rewritten only
//! after every directory is in place, so it never names a directory that the
//! finished move does not have.

use super::{
    is_under, real_dir, record_root, recorded_root, same_path, spellings, status_value, under_sql,
    WorkspaceRootError, JOURNAL_FILE, MIGRATE_COMMAND, STATUS_KEY,
};
use db::{now_rfc3339, SqliteDb};
use executors::gc;
use serde::{Deserialize, Serialize};
use std::{
    fs,
    io::Read,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
};

/// Left in the old root: where its contents went.
pub const MOVED_MARKER: &str = "MOVED";

/// Every column that stores an absolute path under the server's workspace
/// root, and whether a rewrite bumps the row's optimistic-lock version.
const STORED_PATHS: [StoredPath; 13] = [
    StoredPath::plain("workspace", "worktree_path"),
    StoredPath::versioned("repo_location", "path").only("owner_kind = 'server'"),
    // A server-owned placement's handle is its worktree path; a daemon's
    // handle is the daemon's own name for a directory under its own root.
    StoredPath::versioned("workspace_placement", "workspace_handle").only("owner_kind = 'server'"),
    StoredPath::touched("repo", "local_path"),
    StoredPath::touched("repo", "remote_url"),
    StoredPath::plain("execution", "logs_path"),
    StoredPath::plain("agent_context_scope", "workspace_path"),
    StoredPath::plain("agent_inquiry", "workspace_path"),
    StoredPath::plain("agent_inquiry", "findings_path"),
    StoredPath::plain("runtime", "workspace_root"),
    StoredPath::plain("integration_attempt", "repo_location_ref"),
    // Durable steps carry the paths they act on inside their JSON; a step
    // that has not run yet must act on the new root.
    StoredPath::embedded("task_step", "payload_json"),
    StoredPath::embedded("task_step", "result_json"),
];

struct StoredPath {
    table: &'static str,
    column: &'static str,
    /// `version = version + 1`: a holder of the old row must re-read.
    bump_version: bool,
    touch_updated_at: bool,
    /// The path is inside a JSON document, not the whole value.
    embedded: bool,
    /// Rows the rewrite is limited to.
    only: Option<&'static str>,
}

impl StoredPath {
    const fn plain(table: &'static str, column: &'static str) -> Self {
        Self {
            table,
            column,
            bump_version: false,
            touch_updated_at: false,
            embedded: false,
            only: None,
        }
    }

    const fn touched(table: &'static str, column: &'static str) -> Self {
        Self {
            touch_updated_at: true,
            ..Self::plain(table, column)
        }
    }

    const fn versioned(table: &'static str, column: &'static str) -> Self {
        Self {
            bump_version: true,
            ..Self::touched(table, column)
        }
    }

    const fn embedded(table: &'static str, column: &'static str) -> Self {
        Self {
            embedded: true,
            ..Self::plain(table, column)
        }
    }

    const fn only(mut self, rows: &'static str) -> Self {
        self.only = Some(rows);
        self
    }

    /// The `UPDATE` for one spelling of the old root. `?1` is the old root,
    /// `?2` the new one (both JSON-escaped for an embedded path) and `?3`
    /// the time, when the row's `updated_at` is touched.
    fn update_sql(&self) -> String {
        let column = self.column;
        let (mut set, mut rows) = if self.embedded {
            (
                format!(
                    "{column} = replace(replace({column}, ?1 || '/', ?2 || '/'), ?1 || '\"', ?2 || '\"')"
                ),
                format!("(instr({column}, ?1 || '/') > 0 OR instr({column}, ?1 || '\"') > 0)"),
            )
        } else {
            (
                format!("{column} = ?2 || substr({column}, length(?1) + 1)"),
                under_sql(column),
            )
        };
        if self.bump_version {
            set.push_str(", version = version + 1");
        }
        if self.touch_updated_at {
            set.push_str(", updated_at = ?3");
        }
        if let Some(only) = self.only {
            rows = format!("{rows} AND {only}");
        }
        format!("UPDATE {} SET {set} WHERE {rows}", self.table)
    }
}

/// `text` as it appears inside a JSON string.
fn json_escaped(text: &str) -> String {
    let quoted = serde_json::Value::String(text.to_owned()).to_string();
    quoted[1..quoted.len() - 1].to_owned()
}

/// The stored paths a move rewrites, as `table.column`.
#[must_use]
pub fn stored_path_columns() -> Vec<String> {
    let mut all: Vec<String> = STORED_PATHS
        .iter()
        .map(|stored| format!("{}.{}", stored.table, stored.column))
        .collect();
    all.push(format!("system_setting[{}]", super::ROOT_KEY));
    all.push(format!("system_setting[{STATUS_KEY}]"));
    all.push(format!(
        "system_setting[{}]",
        crate::workspace_cleanup::GC_STATUS_KEY
    ));
    all
}

/// What the operator asked for.
#[derive(Debug, Clone)]
pub struct MigrateRequest {
    data_dir: PathBuf,
    target: Option<PathBuf>,
    system_temp: PathBuf,
    free_floor_bytes: u64,
    always_copy: bool,
    #[cfg(test)]
    crash_at: Option<usize>,
}

impl MigrateRequest {
    /// `target`: the new root; `<data dir>/worktrees` when not given.
    /// `free_floor_bytes`: free space that must remain on the target's
    /// filesystem after a copy (the server's `workspace.min_free_bytes`).
    #[must_use]
    pub fn new(
        data_dir: PathBuf,
        target: Option<PathBuf>,
        system_temp: PathBuf,
        free_floor_bytes: u64,
    ) -> Self {
        Self {
            data_dir,
            target,
            system_temp,
            free_floor_bytes,
            always_copy: false,
            #[cfg(test)]
            crash_at: None,
        }
    }

    /// Copy, compare and remove even on one filesystem.
    #[cfg(test)]
    pub(crate) fn copying(mut self) -> Self {
        self.always_copy = true;
        self
    }

    /// Stop as a crash would, right after the step with this index (Task
    /// ids differ per install, so steps are counted, not named).
    #[cfg(test)]
    pub(crate) fn crashing_at(mut self, step: usize) -> Self {
        self.crash_at = Some(step);
        self
    }
}

/// What a finished move did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MigrateReport {
    pub source: PathBuf,
    pub target: PathBuf,
    /// Nothing was moved: no root in use yet, or already at the target.
    pub nothing_to_move: Option<String>,
    /// This run finished a move an earlier run started.
    pub resumed: bool,
    pub copied: bool,
    /// Entries of the old root now in the new one.
    pub moved: Vec<String>,
    /// Git worktrees whose links were repaired and checked.
    pub worktrees: Vec<PathBuf>,
    /// Rows rewritten per `table.column`.
    pub rows: Vec<(String, u64)>,
    /// Text columns that still mention the old root (history, not paths
    /// Forge opens): `table.column` and how many rows.
    pub remaining_mentions: Vec<(String, i64)>,
    /// Garbage-collection ownership of the new root.
    pub gc_state: String,
    /// Every step this run went through, in order.
    pub steps: Vec<String>,
}

impl std::fmt::Display for MigrateReport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(reason) = &self.nothing_to_move {
            return writeln!(formatter, "Nothing to move: {reason}");
        }
        writeln!(
            formatter,
            "Workspace root moved{}:",
            if self.resumed {
                " (finishing an earlier run)"
            } else {
                ""
            }
        )?;
        writeln!(formatter, "  from  {}", self.source.display())?;
        writeln!(formatter, "  to    {}", self.target.display())?;
        writeln!(
            formatter,
            "  {} entr{} {}",
            self.moved.len(),
            if self.moved.len() == 1 { "y" } else { "ies" },
            if self.copied {
                "copied, compared byte for byte, then removed from the old root"
            } else {
                "renamed (same filesystem)"
            }
        )?;
        writeln!(
            formatter,
            "  {} Git worktree(s) relinked and checked with `git status`",
            self.worktrees.len()
        )?;
        for (column, rows) in &self.rows {
            if *rows > 0 {
                writeln!(formatter, "  {rows} row(s) rewritten in {column}")?;
            }
        }
        for (column, rows) in &self.remaining_mentions {
            writeln!(
                formatter,
                "  note: {rows} row(s) of {column} still mention the old root as history; no path Forge opens"
            )?;
        }
        writeln!(
            formatter,
            "  garbage collection on the new root: {}",
            self.gc_state
        )?;
        writeln!(
            formatter,
            "  the old root keeps a {MOVED_MARKER} file and nothing else was deleted from it"
        )?;
        writeln!(
            formatter,
            "Daemon-owned workspaces were not touched: a daemon keeps its own root under its own data directory."
        )?;
        writeln!(
            formatter,
            "If workspace.root or FORGE_WORKSPACE_ROOT is set, set it to {} (or remove it) before starting Forge.",
            self.target.display()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum Phase {
    /// Entries are being moved; the database still names the old root.
    Moving,
    /// Every entry is in the new root and Git's links are repaired.
    Repaired,
    /// The database names the new root.
    Database,
}

/// The durable record of a move in progress.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Journal {
    version: u32,
    /// The old root as recorded, and every way a stored path may spell it.
    source: String,
    source_spellings: Vec<String>,
    /// The new root, resolved.
    target: String,
    copy: bool,
    /// Entries to move, relative to the root.
    units: Vec<String>,
    /// Copied and compared; the source copy may still be there.
    copied: Vec<String>,
    /// In the new root and gone from the old one.
    moved: Vec<String>,
    phase: Phase,
}

impl Journal {
    fn load(path: &Path) -> Result<Option<Self>, WorkspaceRootError> {
        match fs::read_to_string(path) {
            Ok(text) => serde_json::from_str::<Self>(&text)
                .map(Some)
                .map_err(|error| {
                    WorkspaceRootError::Refused(format!(
                        "the move journal {} cannot be read ({error}); nothing was changed. Restore it or ask for help before moving anything by hand",
                        path.display()
                    ))
                }),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Written whole or not at all, and on disk before the next step.
    fn save(&self, path: &Path) -> Result<(), WorkspaceRootError> {
        let staged = path.with_extension("json.tmp");
        let text = serde_json::to_string_pretty(self)
            .map_err(|error| WorkspaceRootError::Incomplete(error.to_string()))?;
        {
            let mut file = fs::File::create(&staged)?;
            std::io::Write::write_all(&mut file, text.as_bytes())?;
            file.sync_all()?;
        }
        fs::rename(&staged, path)?;
        if let Some(parent) = path.parent() {
            if let Ok(directory) = fs::File::open(parent) {
                let _ = directory.sync_all();
            }
        }
        Ok(())
    }
}

struct Run<'a> {
    request: &'a MigrateRequest,
    journal_path: PathBuf,
    journal: Journal,
    steps: Vec<String>,
}

impl Run<'_> {
    /// A step is finished. In tests, the place a crash is injected.
    fn step(&mut self, name: String) -> Result<(), WorkspaceRootError> {
        self.steps.push(name);
        #[cfg(test)]
        if self.request.crash_at == Some(self.steps.len() - 1) {
            return Err(WorkspaceRootError::Incomplete(format!(
                "injected crash after {}",
                self.steps.last().map(String::as_str).unwrap_or_default()
            )));
        }
        Ok(())
    }

    fn save(&self) -> Result<(), WorkspaceRootError> {
        self.journal.save(&self.journal_path)
    }
}

/// Move the workspace root of the stopped server on `db`.
///
/// The caller holds the data directory's runtime lock, so no server runs.
/// Every refusal leaves disk and database as they were.
pub async fn migrate(
    db: &SqliteDb,
    request: &MigrateRequest,
) -> Result<MigrateReport, WorkspaceRootError> {
    let journal_path = request.data_dir.join(JOURNAL_FILE);
    let (journal, resumed) = match Journal::load(&journal_path)? {
        Some(journal) => {
            if let Some(target) = &request.target {
                if !same_path(target, Path::new(&journal.target)) {
                    return Err(WorkspaceRootError::Refused(format!(
                        "a move from {} to {} did not finish; run `{MIGRATE_COMMAND}` without a target to finish it before moving anywhere else",
                        journal.source, journal.target
                    )));
                }
            }
            (journal, true)
        }
        None => match plan(db, request).await? {
            Planned::Nothing(report) => return Ok(*report),
            Planned::Move(journal) => {
                journal.save(&journal_path)?;
                (*journal, false)
            }
        },
    };
    let mut run = Run {
        request,
        journal_path,
        journal,
        steps: Vec::new(),
    };
    if !resumed {
        run.step("plan".to_owned())?;
    }
    let source = PathBuf::from(&run.journal.source);
    let target = PathBuf::from(&run.journal.target);

    if run.journal.phase == Phase::Moving {
        move_units(&mut run, &source, &target)?;
    }
    // Idempotent, and cheap: repeated on every run that has not yet
    // rewritten the database, so a crash in the middle is never trusted.
    let worktrees = if run.journal.phase == Phase::Database {
        Vec::new()
    } else {
        let worktrees = repair_worktrees(db, &run.journal, &target).await?;
        run.step("repair".to_owned())?;
        run.journal.phase = Phase::Repaired;
        run.save()?;
        run.step("repaired".to_owned())?;
        worktrees
    };
    let rows = if run.journal.phase == Phase::Repaired {
        let rows = rewrite_database(db, &mut run, &target).await?;
        run.journal.phase = Phase::Database;
        run.save()?;
        run.step("database-recorded".to_owned())?;
        rows
    } else {
        Vec::new()
    };

    // The marker in `.forge/gc` moved with the root; a root nobody had
    // adopted is adopted now, as the next start would.
    let scheduler = crate::workspace_cleanup::WorkspaceCleanupScheduler::new(
        Arc::new(db.clone()),
        Arc::new(events::EventBus::new(16)),
        target.clone(),
    );
    let gc_state = scheduler
        .adopt_workspace_root()
        .await
        .map_or("unknown", |ownership| ownership.as_str())
        .to_owned();
    run.step("gc".to_owned())?;
    if source.is_dir() {
        fs::write(
            source.join(MOVED_MARKER),
            format!(
                "Forge moved this workspace root to {} on {}.\nNothing here is used any more.\n",
                target.display(),
                now_rfc3339()
            ),
        )?;
    }
    run.step("marker".to_owned())?;
    fs::remove_file(&run.journal_path)?;

    let mut remaining_mentions: Vec<(String, i64)> = Vec::new();
    for spelling in &run.journal.source_spellings {
        for (column, count) in mentions(db, spelling).await? {
            if let Some(known) = remaining_mentions
                .iter_mut()
                .find(|known| known.0 == column)
            {
                known.1 = known.1.max(count);
            } else {
                remaining_mentions.push((column, count));
            }
        }
    }
    Ok(MigrateReport {
        source,
        target,
        nothing_to_move: None,
        resumed,
        copied: run.journal.copy,
        moved: run.journal.moved.clone(),
        worktrees,
        rows,
        remaining_mentions,
        gc_state,
        steps: run.steps,
    })
}

enum Planned {
    Nothing(Box<MigrateReport>),
    Move(Box<Journal>),
}

/// Check everything that can refuse the move, before anything changes.
async fn plan(db: &SqliteDb, request: &MigrateRequest) -> Result<Planned, WorkspaceRootError> {
    let default_target = config::default_workspace_root(&request.data_dir);
    let wanted = absolute(request.target.as_deref().unwrap_or(&default_target))?;
    let legacy = super::legacy_temp_root(&request.system_temp);
    let source = match recorded_root(db).await? {
        Some(recorded) => recorded,
        None if super::has_legacy_layout(db, &legacy).await? => legacy,
        None => {
            return Ok(Planned::Nothing(Box::new(MigrateReport {
                target: wanted,
                nothing_to_move: Some(
                    "this database has no workspace root in use yet; the server records one at its first start".to_owned(),
                ),
                ..MigrateReport::default()
            })));
        }
    };
    if same_path(&source, &wanted) {
        return Ok(Planned::Nothing(Box::new(MigrateReport {
            source: source.clone(),
            target: wanted,
            nothing_to_move: Some(format!(
                "the workspace root already is {}",
                source.display()
            )),
            ..MigrateReport::default()
        })));
    }
    let refuse = |reason: String| -> Result<Planned, WorkspaceRootError> {
        Err(WorkspaceRootError::Refused(format!(
            "the workspace root was not moved: {reason}"
        )))
    };

    let running = Running::read(db).await?;
    if !running.is_empty() {
        return refuse(format!(
            "{running} recorded as running. Start Forge and let them finish (a start also settles runs a crash left behind), stop it, and run `{MIGRATE_COMMAND}` again"
        ));
    }
    if is_under(&wanted, &source) {
        return refuse(format!(
            "the new root {} is inside the old one ({})",
            wanted.display(),
            source.display()
        ));
    }
    if is_under(&source, &wanted) {
        return refuse(format!(
            "the old root {} is inside the new one ({})",
            source.display(),
            wanted.display()
        ));
    }
    let existed = match fs::symlink_metadata(&wanted) {
        Ok(metadata) if metadata.file_type().is_dir() => {
            if fs::read_dir(&wanted)?.next().is_some() {
                return refuse(format!(
                    "the new root {} is not empty; the move only fills an empty or absent directory",
                    wanted.display()
                ));
            }
            true
        }
        Ok(_) => {
            return refuse(format!(
                "the new root {} is not a directory (a file or a link is there)",
                wanted.display()
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => return Err(error.into()),
    };
    fs::create_dir_all(&wanted)?;
    // From here a refusal removes the directory this run created.
    let undo = |reason: String| {
        if !existed {
            let _ = fs::remove_dir(&wanted);
        }
        refuse(reason)
    };
    let target = match fs::canonicalize(&wanted) {
        Ok(target) => target,
        Err(error) => {
            return undo(format!(
                "the new root {} cannot be resolved ({error})",
                wanted.display()
            ))
        }
    };
    if is_under(&target, &source) || is_under(&source, &target) {
        return undo(format!(
            "the new root {} and the old one ({}) contain each other",
            target.display(),
            source.display()
        ));
    }
    if let Some(reason) = gc::refuse_root(&target) {
        return undo(format!(
            "{} cannot be a workspace root: {reason}",
            target.display()
        ));
    }
    // A root reached through a link is still a root.
    let source_exists = source.is_dir();
    if !source_exists && fs::symlink_metadata(&source).is_ok() {
        return undo(format!(
            "the old root {} is not a directory",
            source.display()
        ));
    }
    let copy = request.always_copy || (source_exists && !same_filesystem(&source, &target));
    if copy && source_exists {
        let needed = tree_bytes(&fs::canonicalize(&source).unwrap_or_else(|_| source.clone()))
            .saturating_add(request.free_floor_bytes);
        match gc::disk_space(&target) {
            Some(space) if space.free >= needed => {}
            Some(space) => {
                return undo(format!(
                    "the filesystem of {} has {} bytes free and the copy needs {needed} (the old root's size plus the free-space floor of {})",
                    target.display(),
                    space.free,
                    request.free_floor_bytes
                ));
            }
            None => {
                return undo(format!(
                    "the free space of the filesystem of {} cannot be read",
                    target.display()
                ));
            }
        }
    }
    let units = if source_exists {
        match list_units(&source) {
            Ok(units) => units,
            Err(error) => {
                return undo(format!(
                    "the old root {} cannot be listed ({error})",
                    source.display()
                ))
            }
        }
    } else {
        Vec::new()
    };
    Ok(Planned::Move(Box::new(Journal {
        version: 1,
        source_spellings: spellings(&source),
        source: source.to_string_lossy().into_owned(),
        target: target.to_string_lossy().into_owned(),
        copy,
        units,
        copied: Vec::new(),
        moved: Vec::new(),
        phase: Phase::Moving,
    })))
}

/// Runs the database records as in flight: nothing may be using a worktree.
#[derive(Debug, Default)]
struct Running {
    executions: i64,
    checks: i64,
    hooks: i64,
}

impl Running {
    async fn read(db: &SqliteDb) -> Result<Self, sqlx::Error> {
        Ok(Self {
            executions: sqlx::query_scalar(
                "SELECT COUNT(*) FROM execution WHERE status = 'running'",
            )
            .fetch_one(db.pool())
            .await?,
            checks: sqlx::query_scalar(
                "SELECT COUNT(*) FROM check_run WHERE state IN ('running', 'cancelling', 'cleaning')",
            )
            .fetch_one(db.pool())
            .await?,
            hooks: sqlx::query_scalar(
                "SELECT COUNT(*) FROM task_step WHERE kind = 'hooks' AND status = 'claimed'",
            )
            .fetch_one(db.pool())
            .await?,
        })
    }

    fn is_empty(&self) -> bool {
        self.executions == 0 && self.checks == 0 && self.hooks == 0
    }
}

impl std::fmt::Display for Running {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{} execution(s), {} check run(s) and {} hook step(s) are",
            self.executions, self.checks, self.hooks
        )
    }
}

fn absolute(path: &Path) -> std::io::Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

#[cfg(unix)]
fn same_filesystem(left: &Path, right: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    match (fs::metadata(left), fs::metadata(right)) {
        (Ok(left), Ok(right)) => left.dev() == right.dev(),
        _ => false,
    }
}

#[cfg(not(unix))]
fn same_filesystem(_left: &Path, _right: &Path) -> bool {
    false
}

/// What the old root holds, as paths relative to it: each top-level entry,
/// with `.forge` opened one level (its logs, its garbage-collection state
/// and its build directories move one by one).
fn list_units(source: &Path) -> std::io::Result<Vec<String>> {
    let mut units = Vec::new();
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let Ok(name) = entry.file_name().into_string() else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("{:?} is not a UTF-8 name", entry.file_name()),
            ));
        };
        if name == MOVED_MARKER {
            continue;
        }
        if name == ".forge" && real_dir(&entry.path()) {
            for inner in fs::read_dir(entry.path())? {
                let inner = inner?;
                let Ok(inner) = inner.file_name().into_string() else {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "an entry of .forge is not a UTF-8 name",
                    ));
                };
                units.push(format!(".forge/{inner}"));
            }
            continue;
        }
        units.push(name);
    }
    units.sort();
    Ok(units)
}

fn incomplete(message: String) -> WorkspaceRootError {
    WorkspaceRootError::Incomplete(format!(
        "{message}. The move is unfinished and Forge will not start until it is: fix the cause and run `{MIGRATE_COMMAND}` again"
    ))
}

fn move_units(run: &mut Run<'_>, source: &Path, target: &Path) -> Result<(), WorkspaceRootError> {
    for unit in run.journal.units.clone() {
        if run.journal.moved.contains(&unit) {
            continue;
        }
        let from = source.join(&unit);
        let to = target.join(&unit);
        if let Some(parent) = to.parent() {
            fs::create_dir_all(parent)?;
        }
        let from_exists = fs::symlink_metadata(&from).is_ok();
        let to_exists = fs::symlink_metadata(&to).is_ok();
        if run.journal.copy {
            if !run.journal.copied.contains(&unit) {
                if !from_exists {
                    return Err(incomplete(format!(
                        "{} disappeared from the old root before it was copied",
                        from.display()
                    )));
                }
                // Whatever is there is a copy an earlier run did not finish.
                remove_all(&to)?;
                copy_tree(&from, &to)
                    .map_err(|error| incomplete(format!("copying {}: {error}", from.display())))?;
                compare_trees(&from, &to).map_err(|difference| {
                    incomplete(format!(
                        "the copy of {} is not identical to it ({difference}); the original was not removed",
                        from.display()
                    ))
                })?;
                run.step(format!("copy:{unit}"))?;
                run.journal.copied.push(unit.clone());
                run.save()?;
                run.step(format!("copied:{unit}"))?;
            }
            remove_all(&from)?;
            run.step(format!("remove:{unit}"))?;
        } else {
            match (from_exists, to_exists) {
                (true, false) => fs::rename(&from, &to).map_err(|error| {
                    incomplete(format!(
                        "renaming {} to {}: {error}",
                        from.display(),
                        to.display()
                    ))
                })?,
                // Renamed by a run that stopped before it wrote that down.
                (false, true) => {}
                (true, true) => {
                    return Err(incomplete(format!(
                        "{} exists in both the old and the new root",
                        unit
                    )))
                }
                (false, false) => {
                    return Err(incomplete(format!(
                        "{} is in neither the old nor the new root",
                        unit
                    )))
                }
            }
            run.step(format!("rename:{unit}"))?;
        }
        run.journal.moved.push(unit.clone());
        run.save()?;
        run.step(format!("moved:{unit}"))?;
    }
    Ok(())
}

/// Remove a tree this move copied (or a half-made copy of it), including
/// directories a toolchain left read-only. Links are removed, not followed.
fn remove_all(path: &Path) -> Result<(), WorkspaceRootError> {
    if fs::symlink_metadata(path).is_err() {
        return Ok(());
    }
    gc::remove_exact(path, &mut gc::GcReport::default());
    if fs::symlink_metadata(path).is_ok() {
        return Err(incomplete(format!(
            "{} could not be removed",
            path.display()
        )));
    }
    Ok(())
}

fn tree_bytes(path: &Path) -> u64 {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return 0;
    };
    if !metadata.file_type().is_dir() {
        return metadata.len();
    }
    fs::read_dir(path)
        .map(|entries| {
            entries
                .flatten()
                .map(|entry| tree_bytes(&entry.path()))
                .sum::<u64>()
        })
        .unwrap_or(0)
        .saturating_add(metadata.len())
}

/// Copy a file, a link or a directory tree. Links are copied as links;
/// permissions and modification times are kept. Sockets, pipes and devices
/// (what a dead run left in its temp directory) are not copied.
fn copy_tree(from: &Path, to: &Path) -> std::io::Result<()> {
    let metadata = fs::symlink_metadata(from)?;
    let kind = metadata.file_type();
    if kind.is_symlink() {
        let link = fs::read_link(from)?;
        #[cfg(unix)]
        std::os::unix::fs::symlink(link, to)?;
        #[cfg(not(unix))]
        let _ = link;
        return Ok(());
    }
    if kind.is_file() {
        fs::copy(from, to)?;
        keep_modified(&metadata, to);
        return Ok(());
    }
    if !kind.is_dir() {
        return Ok(());
    }
    fs::create_dir(to)?;
    for entry in fs::read_dir(from)? {
        let entry = entry?;
        copy_tree(&entry.path(), &to.join(entry.file_name()))?;
    }
    fs::set_permissions(to, metadata.permissions())?;
    keep_modified(&metadata, to);
    Ok(())
}

fn keep_modified(metadata: &fs::Metadata, to: &Path) {
    if let (Ok(modified), Ok(file)) = (metadata.modified(), fs::File::open(to)) {
        let _ = file.set_modified(modified);
    }
}

/// `Err(what differs)` unless the two trees hold the same entries, the same
/// bytes, the same link targets and the same permissions.
fn compare_trees(left: &Path, right: &Path) -> Result<(), String> {
    let differs = |what: &str| Err(format!("{}: {what}", right.display()));
    let left_meta = fs::symlink_metadata(left).map_err(|error| error.to_string())?;
    let Ok(right_meta) = fs::symlink_metadata(right) else {
        return if special(&left_meta) {
            Ok(())
        } else {
            differs("missing")
        };
    };
    let (left_kind, right_kind) = (left_meta.file_type(), right_meta.file_type());
    if left_kind.is_symlink() {
        return if right_kind.is_symlink() && fs::read_link(left).ok() == fs::read_link(right).ok() {
            Ok(())
        } else {
            differs("link target")
        };
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if left_meta.permissions().mode() != right_meta.permissions().mode() {
            return differs("permissions");
        }
    }
    if left_kind.is_file() {
        if !right_kind.is_file() || left_meta.len() != right_meta.len() {
            return differs("size");
        }
        return if same_bytes(left, right).map_err(|error| error.to_string())? {
            Ok(())
        } else {
            differs("content")
        };
    }
    if !left_kind.is_dir() || !right_kind.is_dir() {
        return differs("kind");
    }
    let names = |path: &Path| -> Result<Vec<std::ffi::OsString>, String> {
        let mut names = Vec::new();
        for entry in fs::read_dir(path).map_err(|error| error.to_string())? {
            let entry = entry.map_err(|error| error.to_string())?;
            let metadata = entry.metadata().map_err(|error| error.to_string())?;
            if !special(&metadata) {
                names.push(entry.file_name());
            }
        }
        names.sort();
        Ok(names)
    };
    let left_names = names(left)?;
    if left_names != names(right)? {
        return differs("entries");
    }
    for name in left_names {
        compare_trees(&left.join(&name), &right.join(&name))?;
    }
    Ok(())
}

/// A socket, pipe or device: not a file a move can or should copy.
fn special(metadata: &fs::Metadata) -> bool {
    let kind = metadata.file_type();
    !(kind.is_file() || kind.is_dir() || kind.is_symlink())
}

fn same_bytes(left: &Path, right: &Path) -> std::io::Result<bool> {
    let mut left = std::io::BufReader::new(fs::File::open(left)?);
    let mut right = std::io::BufReader::new(fs::File::open(right)?);
    let (mut left_buffer, mut right_buffer) = (vec![0_u8; 64 * 1024], vec![0_u8; 64 * 1024]);
    loop {
        let read = left.read(&mut left_buffer)?;
        if read == 0 {
            return Ok(right.read(&mut right_buffer)? == 0);
        }
        right
            .read_exact(&mut right_buffer[..read])
            .or_else(|error| {
                if error.kind() == std::io::ErrorKind::UnexpectedEof {
                    Ok(())
                } else {
                    Err(error)
                }
            })?;
        if left_buffer[..read] != right_buffer[..read] {
            return Ok(false);
        }
    }
}

/// `path` with the old root (in any spelling) replaced by the new one.
fn rehome(path: &str, journal: &Journal) -> Option<String> {
    journal.source_spellings.iter().find_map(|old| {
        if path == old {
            return Some(journal.target.clone());
        }
        path.strip_prefix(old.as_str())
            .filter(|rest| rest.starts_with('/'))
            .map(|rest| format!("{}{rest}", journal.target))
    })
}

/// A repository's Git directory: `<repo>/.git`, or the repository itself
/// when it is bare. `None` for anything else (including a linked worktree).
fn git_common_dir(repo: &Path) -> Option<PathBuf> {
    let dot_git = repo.join(".git");
    if real_dir(&dot_git) {
        return Some(dot_git);
    }
    (repo.join("HEAD").is_file() && real_dir(&repo.join("objects"))).then(|| repo.to_path_buf())
}

fn git(cwd: &Path, args: &[&str]) -> std::io::Result<std::process::Output> {
    Command::new("git")
        .arg("-C")
        .arg(cwd)
        .args(args)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
}

/// Make every Git worktree that moved, and every repository that moved or
/// has worktrees that did, point at each other again.
///
/// A worktree and its repository name each other by absolute path: the
/// worktree's `.git` file names `<repository>/worktrees/<name>`, and that
/// directory's `gitdir` file names the worktree's `.git`. Both are rewritten
/// for the new root, `git worktree repair` is run from each repository, and
/// the result is checked: both links name existing paths outside the old
/// root, and `git status` works in the worktree.
async fn repair_worktrees(
    db: &SqliteDb,
    journal: &Journal,
    target: &Path,
) -> Result<Vec<PathBuf>, WorkspaceRootError> {
    // Repositories that can hold a moved worktree: the clones under the
    // root, and every repository the database knows by a local path (a
    // user's own checkout stays where it is, but its worktrees moved).
    let mut repositories: Vec<PathBuf> = Vec::new();
    if let Ok(entries) = fs::read_dir(target.join(".repos")) {
        repositories.extend(entries.flatten().map(|entry| entry.path()));
    }
    let known: Vec<String> = sqlx::query_scalar(
        "SELECT local_path FROM repo WHERE local_path IS NOT NULL AND local_path != ''
         UNION SELECT path FROM repo_location WHERE owner_kind = 'server'",
    )
    .fetch_all(db.pool())
    .await?;
    for path in known {
        let path = PathBuf::from(rehome(&path, journal).unwrap_or(path));
        if !repositories.contains(&path) {
            repositories.push(path);
        }
    }
    repositories.sort();

    let mut repaired = Vec::new();
    for repository in repositories {
        let Some(common) = git_common_dir(&repository) else {
            continue;
        };
        let moved_repository = is_under(&repository, target);
        if moved_repository {
            rehome_git_config(&common, journal)?;
        }
        let Ok(entries) = fs::read_dir(common.join("worktrees")) else {
            continue;
        };
        let mut worktrees = Vec::new();
        for entry in entries.flatten() {
            let admin = entry.path();
            let Ok(recorded) = fs::read_to_string(admin.join("gitdir")) else {
                continue;
            };
            let recorded = recorded.trim().to_owned();
            let dot_git = PathBuf::from(rehome(&recorded, journal).unwrap_or(recorded.clone()));
            let Some(worktree) = dot_git.parent().map(Path::to_path_buf) else {
                continue;
            };
            let moved_worktree = is_under(&worktree, target);
            if !(moved_worktree || moved_repository) || !real_dir(&worktree) {
                // Not part of this move, or a registration whose worktree is
                // gone: Git prunes those, the move leaves them alone.
                continue;
            }
            if dot_git.to_string_lossy() != recorded.as_str() {
                fs::write(
                    admin.join("gitdir"),
                    format!("{}\n", dot_git.to_string_lossy()),
                )?;
            }
            fs::write(&dot_git, format!("gitdir: {}\n", admin.to_string_lossy()))?;
            worktrees.push((worktree, admin));
        }
        if worktrees.is_empty() {
            continue;
        }
        let mut arguments = vec!["worktree".to_owned(), "repair".to_owned()];
        arguments.extend(
            worktrees
                .iter()
                .map(|(worktree, _)| worktree.to_string_lossy().into_owned()),
        );
        let arguments: Vec<&str> = arguments.iter().map(String::as_str).collect();
        let output = git(&repository, &arguments)?;
        if !output.status.success() {
            return Err(incomplete(format!(
                "`git worktree repair` failed in {}: {}",
                repository.display(),
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        for (worktree, admin) in worktrees {
            check_worktree(&worktree, &admin, journal)?;
            repaired.push(worktree);
        }
    }
    repaired.sort();
    Ok(repaired)
}

/// A clone's own configuration may name the old root (a remote that is a
/// repository under it).
fn rehome_git_config(common: &Path, journal: &Journal) -> Result<(), WorkspaceRootError> {
    let config = common.join("config");
    let Ok(text) = fs::read_to_string(&config) else {
        return Ok(());
    };
    let mut rewritten = text.clone();
    for old in &journal.source_spellings {
        rewritten = rewritten.replace(&format!("{old}/"), &format!("{}/", journal.target));
    }
    if rewritten != text {
        fs::write(&config, rewritten)?;
    }
    Ok(())
}

fn check_worktree(
    worktree: &Path,
    admin: &Path,
    journal: &Journal,
) -> Result<(), WorkspaceRootError> {
    let under_old = |path: &str| rehome(path, journal).is_some();
    let dot_git = worktree.join(".git");
    let forward = fs::read_to_string(&dot_git).unwrap_or_default();
    let forward = forward.trim().strip_prefix("gitdir:").map(str::trim);
    let forward_ok = forward.is_some_and(|named| {
        let named = worktree.join(named);
        !under_old(&named.to_string_lossy())
            && fs::canonicalize(&named).ok() == fs::canonicalize(admin).ok()
            && real_dir(&named)
    });
    let back = fs::read_to_string(admin.join("gitdir")).unwrap_or_default();
    let back = admin.join(back.trim());
    let back_ok = !under_old(&back.to_string_lossy())
        && fs::canonicalize(&back).ok() == fs::canonicalize(&dot_git).ok()
        && fs::symlink_metadata(&back).is_ok();
    if !forward_ok || !back_ok {
        return Err(incomplete(format!(
            "the Git links of the worktree {} do not name its new location",
            worktree.display()
        )));
    }
    let status = git(worktree, &["status", "--porcelain"])?;
    if !status.status.success() {
        return Err(incomplete(format!(
            "`git status` fails in the moved worktree {}: {}",
            worktree.display(),
            String::from_utf8_lossy(&status.stderr).trim()
        )));
    }
    Ok(())
}

/// Rewrite every stored path under the old root and record the new root, in
/// one transaction: the database names the old root, or the new one, never
/// both.
async fn rewrite_database(
    db: &SqliteDb,
    run: &mut Run<'_>,
    target: &Path,
) -> Result<Vec<(String, u64)>, WorkspaceRootError> {
    let now = now_rfc3339();
    let new_root = run.journal.target.clone();
    let mut rows = Vec::new();
    let mut tx = db.pool().begin().await?;
    for stored in &STORED_PATHS {
        let mut changed = 0;
        let sql = stored.update_sql();
        for old in &run.journal.source_spellings {
            let (old, new) = if stored.embedded {
                (json_escaped(old), json_escaped(&new_root))
            } else {
                (old.clone(), new_root.clone())
            };
            let mut query = sqlx::query(&sql).bind(old).bind(new);
            if stored.touch_updated_at {
                query = query.bind(&now);
            }
            changed += query.execute(&mut *tx).await?.rows_affected();
        }
        rows.push((format!("{}.{}", stored.table, stored.column), changed));
    }
    record_root(&mut *tx, target).await?;
    sqlx::query(
        "INSERT INTO system_setting (key, value, updated_at) VALUES (?1, ?2, ?3)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
    )
    .bind(STATUS_KEY)
    .bind(status_value(
        target,
        is_under(target, &run.request.system_temp),
    ))
    .bind(&now)
    .execute(&mut *tx)
    .await?;
    // What the last ownership check found is about the old root; the check
    // after the move writes it again.
    sqlx::query("DELETE FROM system_setting WHERE key = ?")
        .bind(crate::workspace_cleanup::GC_STATUS_KEY)
        .execute(&mut *tx)
        .await?;
    // A crash here loses the whole transaction, never half of it.
    run.step("database:uncommitted".to_owned())?;
    tx.commit().await?;
    run.step("database".to_owned())?;
    Ok(rows)
}

/// Text columns that mention `needle`, as `table.column` and a row count.
pub(crate) async fn mentions(
    db: &SqliteDb,
    needle: &str,
) -> Result<Vec<(String, i64)>, sqlx::Error> {
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
         AND sql NOT LIKE 'CREATE VIRTUAL%' ORDER BY name",
    )
    .fetch_all(db.pool())
    .await?;
    let mut found = Vec::new();
    for table in tables {
        let columns: Vec<(String, String)> = sqlx::query_as(&format!(
            "SELECT name, type FROM pragma_table_info('{}')",
            table.replace('\'', "''")
        ))
        .fetch_all(db.pool())
        .await?;
        for (column, kind) in columns {
            let kind = kind.to_ascii_uppercase();
            if !(kind.is_empty() || kind.contains("TEXT") || kind.contains("JSON")) {
                continue;
            }
            let quoted = |name: &str| format!("\"{}\"", name.replace('"', "\"\""));
            let Ok(count) = sqlx::query_scalar::<_, i64>(&format!(
                "SELECT COUNT(*) FROM {} WHERE instr({}, ?1) > 0",
                quoted(&table),
                quoted(&column)
            ))
            .bind(needle)
            .fetch_one(db.pool())
            .await
            else {
                continue;
            };
            if count > 0 {
                found.push((format!("{table}.{column}"), count));
            }
        }
    }
    Ok(found)
}

#[cfg(test)]
mod tests;
