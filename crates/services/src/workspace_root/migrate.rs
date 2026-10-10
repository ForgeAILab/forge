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
    is_under, real_dir, record_root, recorded_root, same_path, spellings, status_value,
    stored_roots, under_sql, WorkspaceRootError, JOURNAL_FILE, MIGRATE_COMMAND, STATUS_KEY,
};
use db::{now_rfc3339, SqliteDb};
use executors::gc;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
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
    // A runtime row is a daemon's statement about its own root. Only the
    // daemon inside the server process runs on the server's root; any other
    // daemon keeps what it reported (one sharing the root keeps its Task
    // roots in the old one) and reports again when it connects.
    StoredPath::plain("runtime", "workspace_root")
        .only("daemon_id IN (SELECT id FROM daemon WHERE machine_id LIKE 'embedded:%')"),
    StoredPath::plain("integration_attempt", "repo_location_ref"),
    // Durable steps carry the paths they act on inside their JSON; a step
    // that has not run yet must act on the new root. The value is replaced
    // as text, not parsed: the old root is matched only where a `/` or the
    // closing `"` of a JSON string follows it, and both needle and
    // replacement are JSON-escaped, so a root that is a prefix of another
    // path (`/data/wt` and `/data/wt2`) is never touched, the document
    // stays valid JSON, and keys, numbers and other strings are unchanged
    // (proved by the `/data/wt2` rows of the fixture).
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

impl StoredPath {
    /// Counts the rows of this column that still name the root bound as
    /// `?1` (JSON-escaped for an embedded path), within the rows the
    /// rewrite is limited to.
    fn remaining_sql(&self) -> String {
        let column = self.column;
        let mut rows = if self.embedded {
            format!("(instr({column}, ?1 || '/') > 0 OR instr({column}, ?1 || '\"') > 0)")
        } else {
            under_sql(column)
        };
        if let Some(only) = self.only {
            rows = format!("{rows} AND {only}");
        }
        format!("SELECT COUNT(*) FROM {} WHERE {rows}", self.table)
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
    /// The command as its operator types it, for messages.
    command: String,
    always_copy: bool,
    #[cfg(test)]
    crash_at: Option<usize>,
    /// A known column the rewrite "forgets", to prove the post-condition.
    #[cfg(test)]
    skip_column: Option<&'static str>,
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
            command: MIGRATE_COMMAND.to_owned(),
            always_copy: false,
            #[cfg(test)]
            crash_at: None,
            #[cfg(test)]
            skip_column: None,
        }
    }

    /// The command as the operator types it (with `--data-dir` when the
    /// data directory is not the default one): named by every message.
    #[must_use]
    pub fn with_command(mut self, command: String) -> Self {
        self.command = command;
        self
    }

    #[cfg(test)]
    pub(crate) fn skipping_column(mut self, column: &'static str) -> Self {
        self.skip_column = Some(column);
        self
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
    /// Entries of the old root that are not Forge's (no stored path names
    /// them, not a Task root, not a directory Forge makes): left there.
    pub left_behind: Vec<String>,
    /// Sockets, pipes and devices a copy cannot carry: left in the old root.
    pub not_copied: Vec<PathBuf>,
    /// Repositories outside the root (a user's own checkout) in which
    /// `git worktree repair` re-registered worktrees that moved. Nothing
    /// else in them was touched.
    pub user_repositories: Vec<PathBuf>,
    /// What the checks after the move found and did not block on.
    pub warnings: Vec<String>,
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
        if self.copied {
            writeln!(
                formatter,
                "  kept: contents, permissions, modification times, links (as links) and hard links within an entry; not kept: extended attributes"
            )?;
        }
        writeln!(
            formatter,
            "  {} Git worktree(s) relinked and checked with `git status`; every clone checked with `git fsck --connectivity-only`",
            self.worktrees.len()
        )?;
        for repository in &self.user_repositories {
            writeln!(
                formatter,
                "  `git worktree repair` was run in your repository {} so it finds its moved worktrees; nothing else there was touched",
                repository.display()
            )?;
        }
        for entry in &self.left_behind {
            writeln!(formatter, "  left in the old root (not Forge's): {entry}")?;
        }
        for path in &self.not_copied {
            writeln!(
                formatter,
                "  left in the old root (a socket, pipe or device cannot be copied): {}",
                path.display()
            )?;
        }
        for warning in &self.warnings {
            writeln!(formatter, "  warning: {warning}")?;
        }
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
            "  the old root keeps a {MOVED_MARKER} file; nothing was deleted from it except what was moved, and it is free for another Forge to adopt"
        )?;
        writeln!(
            formatter,
            "Daemon-owned workspaces were not touched: a daemon keeps its own root, and one that shares this root keeps .forge/workspaces in the old one."
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
    /// Top-level entries of the old root that are not Forge's: never moved.
    #[serde(default)]
    left_behind: Vec<String>,
    /// Sockets, pipes and devices left in the old root by a copy.
    #[serde(default)]
    not_copied: Vec<String>,
    /// The old root did not exist: only the database is pointed at the new.
    #[serde(default)]
    source_missing: bool,
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
                        "a move from {} to {} did not finish; run `{}` without a target to finish it before moving anywhere else",
                        journal.source, journal.target, request.command
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
    let (mut user_repositories, mut warnings) = (Vec::new(), Vec::new());
    // Idempotent, and cheap: repeated on every run that has not yet
    // rewritten the database, so a crash in the middle is never trusted.
    let worktrees = if run.journal.phase == Phase::Database {
        Vec::new()
    } else {
        let repaired = repair_worktrees(db, &run.journal, &target, &request.command).await?;
        user_repositories = repaired.user_repositories;
        warnings = repaired.warnings;
        let worktrees = repaired.worktrees;
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
        left_behind: run.journal.left_behind.clone(),
        not_copied: run.journal.not_copied.iter().map(PathBuf::from).collect(),
        user_repositories: std::mem::take(&mut user_repositories),
        warnings: std::mem::take(&mut warnings),
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
    let command = request.command.as_str();
    // The root a start would run on: the recorded one, else the one this
    // database's rows were written under (never today's temp directory).
    let source = match recorded_root(db).await? {
        Some(recorded) => recorded,
        None => match stored_roots(db).await?.first() {
            Some(stored) => PathBuf::from(&stored.root),
            None => {
                return Ok(Planned::Nothing(Box::new(MigrateReport {
                target: wanted,
                nothing_to_move: Some(
                    "this database has no workspace root in use yet; the server records one at its first start".to_owned(),
                ),
                ..MigrateReport::default()
            })));
            }
        },
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
            "{running}. Start Forge and let them finish (a start also settles what a crash left behind), stop it, and run `{command}` again"
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
    // A root reached through a link is still a root.
    let source_exists = source.is_dir();
    if !source_exists && fs::symlink_metadata(&source).is_ok() {
        return refuse(format!(
            "the old root {} is not a directory",
            source.display()
        ));
    }
    // The directories this run creates, deepest first: a refusal removes
    // exactly these again.
    let mut created: Vec<PathBuf> = Vec::new();
    let mut above = Some(wanted.as_path());
    while let Some(path) = above {
        if fs::symlink_metadata(path).is_ok() {
            break;
        }
        created.push(path.to_path_buf());
        above = path.parent();
    }
    match fs::symlink_metadata(&wanted) {
        Ok(metadata) if metadata.file_type().is_dir() => {
            // With the old root gone nothing is moved, so what the new one
            // holds is not in the way: only the database is pointed at it
            // (a data directory that was moved with its worktrees inside).
            if source_exists && fs::read_dir(&wanted)?.next().is_some() {
                return refuse(format!(
                    "the new root {} is not empty; the move only fills an empty or absent directory",
                    wanted.display()
                ));
            }
        }
        Ok(_) => {
            return refuse(format!(
                "the new root {} is not a directory (a file or a link is there)",
                wanted.display()
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    // From here a refusal removes the directories this run created.
    let undo = |reason: String| {
        for made in &created {
            let _ = fs::remove_dir(made);
        }
        refuse(reason)
    };
    if let Err(error) = fs::create_dir_all(&wanted) {
        return undo(format!(
            "the new root {} cannot be created ({error})",
            wanted.display()
        ));
    }
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
    let copy = request.always_copy || (source_exists && !same_filesystem(&source, &target));
    if copy && source_exists {
        let needed = tree_bytes(&fs::canonicalize(&source).unwrap_or_else(|_| source.clone()))
            .saturating_add(request.free_floor_bytes);
        match gc::disk_space(&target) {
            Some(space) if space.free >= needed => {}
            Some(space) => {
                return undo(format!(
                    "the filesystem of {} has {} bytes free and the copy needs {needed} (the old root's size, every hard link counted as a full file, plus the free-space floor of {})",
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
    let source_spellings = spellings(&source);
    let (units, left_behind) = if source_exists {
        let listed = match list_units(&source) {
            Ok(units) => units,
            Err(error) => {
                return undo(format!(
                    "the old root {} cannot be listed ({error})",
                    source.display()
                ))
            }
        };
        // Only what Forge made moves: anything else in the old root (a
        // directory somebody else put in a shared temp directory) stays.
        let (mut units, mut left_behind) = (Vec::new(), Vec::new());
        for unit in listed {
            if forge_made(&unit) || named_by_database(db, &source_spellings, &unit).await? {
                units.push(unit);
            } else {
                left_behind.push(unit);
            }
        }
        (units, left_behind)
    } else {
        (Vec::new(), Vec::new())
    };
    if let Some(nested) = units.iter().find_map(|unit| {
        nested_repository_with_absolute_link(&source.join(unit), &source_spellings)
    }) {
        return undo(format!(
            "{} is a Git submodule or nested worktree whose link names the old root by absolute path; the move cannot repair it. Remove it or make its link relative (`git submodule absorbgitdirs`), then run `{command}` again",
            nested.display()
        ));
    }
    Ok(Planned::Move(Box::new(Journal {
        version: 1,
        source_spellings,
        source: source.to_string_lossy().into_owned(),
        target: target.to_string_lossy().into_owned(),
        copy,
        units,
        copied: Vec::new(),
        moved: Vec::new(),
        phase: Phase::Moving,
        left_behind,
        not_copied: Vec::new(),
        source_missing: !source_exists,
    })))
}

/// A top-level entry Forge makes by a fixed name, or a Task root.
fn forge_made(unit: &str) -> bool {
    const KNOWN: [&str; 4] = [".repos", "repos", ".forge-tmp", "main-agents"];
    unit.starts_with(".forge/")
        || KNOWN.contains(&unit)
        || (unit.len() == 36 && uuid::Uuid::parse_str(unit).is_ok())
}

/// Whether any text column names `<old root>/<unit>` or a path inside it.
/// The rewrite re-homes every such path, so its directory must move too.
async fn named_by_database(
    db: &SqliteDb,
    source_spellings: &[String],
    unit: &str,
) -> Result<bool, sqlx::Error> {
    for spelling in source_spellings {
        let path = format!("{spelling}/{unit}");
        let found = sweep(
            db,
            "({column} = ?1 OR instr({column}, ?1 || '/') > 0 OR instr({column}, ?2 || '/') > 0 OR instr({column}, ?2 || '\"') > 0)",
            &[path.clone(), json_escaped(&path)],
        )
        .await?;
        if !found.is_empty() {
            return Ok(true);
        }
    }
    Ok(false)
}

/// A `.git` *file* below a worktree's top level (a submodule, or a worktree
/// made inside a Task root) that names the old root by absolute path.
/// Relative links, which Git writes for submodules, move unharmed.
fn nested_repository_with_absolute_link(
    unit: &Path,
    source_spellings: &[String],
) -> Option<PathBuf> {
    fn walk(path: &Path, depth: usize, spellings: &[String]) -> Option<PathBuf> {
        if depth > 6 || !real_dir(path) {
            return None;
        }
        for entry in fs::read_dir(path).ok()?.flatten() {
            let child = entry.path();
            let name = entry.file_name();
            if name == ".git" {
                // Depth 2 is `<task>/<repository>/.git`: the worktree's
                // own link, which the move repairs.
                if depth > 2 && child.is_file() {
                    let link = fs::read_to_string(&child).unwrap_or_default();
                    let named = link.trim().strip_prefix("gitdir:").map(str::trim);
                    if named.is_some_and(|named| {
                        spellings
                            .iter()
                            .any(|old| named.starts_with(&format!("{old}/")))
                    }) {
                        return Some(child);
                    }
                }
                continue;
            }
            if name == "node_modules" || name == "target" {
                continue;
            }
            if let Some(found) = walk(&child, depth + 1, spellings) {
                return Some(found);
            }
        }
        None
    }
    let name = unit.file_name()?.to_string_lossy().into_owned();
    (name.len() == 36 && uuid::Uuid::parse_str(&name).is_ok())
        .then(|| walk(unit, 1, source_spellings))
        .flatten()
}

/// What the database records as in flight or holding a worktree: nothing
/// may be using one while it moves.
#[derive(Debug, Default)]
struct Running {
    executions: i64,
    checks: i64,
    steps: i64,
    integrations: i64,
    leases: i64,
}

impl Running {
    async fn read(db: &SqliteDb) -> Result<Self, sqlx::Error> {
        let count = |sql: &'static str| sqlx::query_scalar::<_, i64>(sql).fetch_one(db.pool());
        Ok(Self {
            executions: count("SELECT COUNT(*) FROM execution WHERE status = 'running'").await?,
            checks: count(
                "SELECT COUNT(*) FROM check_run WHERE state IN ('running', 'cancelling', 'cleaning')",
            )
            .await?,
            // A claimed step is being run; a suspended one waits for a
            // check it started in a worktree.
            steps: count("SELECT COUNT(*) FROM task_step WHERE status IN ('claimed', 'suspended')")
                .await?,
            // An integration attempt carries its repository location and
            // owner state through every state but the final and parked ones.
            integrations: count(
                "SELECT COUNT(*) FROM integration_attempt
                 WHERE state NOT IN ('completed', 'cancelled', 'superseded', 'parked')",
            )
            .await?,
            leases: sqlx::query_scalar(
                "SELECT COUNT(*) FROM workspace_lease WHERE status = 'active' AND expires_at > ?",
            )
            .bind(now_rfc3339())
            .fetch_one(db.pool())
            .await?,
        })
    }

    fn is_empty(&self) -> bool {
        self.executions == 0
            && self.checks == 0
            && self.steps == 0
            && self.integrations == 0
            && self.leases == 0
    }
}

impl std::fmt::Display for Running {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "the database records work in flight: {} running execution(s), {} running check run(s), {} claimed or suspended task step(s), {} integration attempt(s) that are neither finished nor parked and {} active workspace lease(s)",
            self.executions, self.checks, self.steps, self.integrations, self.leases
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

/// A daemon that shares the server's root keeps its own Task roots here and
/// its own garbage-collection marker in `.forge/gc`: neither is the
/// server's to move.
const DAEMON_WORKSPACES: &str = "workspaces";

/// What the old root holds that the server owns, as paths relative to it:
/// each top-level entry, with `.forge` and `.forge/gc` opened one level
/// (logs, build directories and each piece of garbage-collection state move
/// one by one) so that what a daemon keeps there stays where it is.
fn list_units(source: &Path) -> std::io::Result<Vec<String>> {
    fn names(directory: &Path) -> std::io::Result<Vec<String>> {
        let mut names = Vec::new();
        for entry in fs::read_dir(directory)? {
            let name = entry?.file_name();
            names.push(name.into_string().map_err(|name| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("{name:?} in {} is not a UTF-8 name", directory.display()),
                )
            })?);
        }
        Ok(names)
    }
    let mut units = Vec::new();
    for name in names(source)? {
        if name == MOVED_MARKER {
            continue;
        }
        if name != ".forge" || !real_dir(&source.join(&name)) {
            units.push(name);
            continue;
        }
        for inner in names(&source.join(".forge"))? {
            if inner == DAEMON_WORKSPACES {
                continue;
            }
            if inner != "gc" || !real_dir(&source.join(gc::GC_DIR)) {
                units.push(format!(".forge/{inner}"));
                continue;
            }
            for state in names(&source.join(gc::GC_DIR))? {
                if state != gc::DAEMON_OWNER_FILE {
                    units.push(format!("{}/{state}", gc::GC_DIR));
                }
            }
        }
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
        mirror_parents(source, target, &unit)?;
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
                copy_tree(&from, &to, &mut HashMap::new()).map_err(|error| {
                    incomplete(format!(
                        "copying {} to {}: {error}; nothing was removed from the old root",
                        from.display(),
                        to.display()
                    ))
                })?;
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
            // Only what was copied is removed: a socket, pipe or device
            // stays in the old root, with the directories above it.
            let mut left = Vec::new();
            remove_copied(&from, &mut left).map_err(|error| {
                incomplete(format!(
                    "removing the copied {} from the old root: {error}; its copy in the new root is complete",
                    from.display()
                ))
            })?;
            for path in left {
                let path = path.to_string_lossy().into_owned();
                if !run.journal.not_copied.contains(&path) {
                    run.journal.not_copied.push(path);
                }
            }
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

/// Make the directories above `unit` in the new root (`.forge`, `.forge/gc`)
/// with the permissions they have in the old one.
fn mirror_parents(source: &Path, target: &Path, unit: &str) -> std::io::Result<()> {
    let mut above = PathBuf::new();
    let mut parts: Vec<&str> = unit.split('/').collect();
    parts.pop();
    for part in parts {
        above.push(part);
        let made = target.join(&above);
        if fs::symlink_metadata(&made).is_ok() {
            continue;
        }
        fs::create_dir(&made)?;
        if let Ok(metadata) = fs::metadata(source.join(&above)) {
            fs::set_permissions(&made, metadata.permissions())?;
        }
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

/// Remove a tree that was copied and compared, leaving every socket, pipe
/// and device (which the copy does not carry) and the directories above
/// them. `Ok(true)`: `path` is gone.
fn remove_copied(path: &Path, left: &mut Vec<PathBuf>) -> std::io::Result<bool> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(true),
        Err(error) => return Err(error),
    };
    if special(&metadata) {
        left.push(path.to_path_buf());
        return Ok(false);
    }
    if !metadata.file_type().is_dir() {
        fs::remove_file(path)?;
        return Ok(true);
    }
    // A toolchain's read-only directory: its entries cannot be removed
    // until its owner may write it.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = metadata.permissions().mode();
        if mode & 0o700 != 0o700 {
            fs::set_permissions(path, fs::Permissions::from_mode(mode | 0o700))?;
        }
    }
    let mut empty = true;
    for entry in fs::read_dir(path)? {
        empty &= remove_copied(&entry?.path(), left)?;
    }
    if empty {
        fs::remove_dir(path)?;
    } else {
        // Kept for what stays in it, as it was.
        fs::set_permissions(path, metadata.permissions())?;
    }
    Ok(empty)
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
///
/// Two names of one file (a hard link: Git object stores and build output
/// use them) stay two names of one file when both are inside the tree being
/// copied; `links` remembers the first copy of each.
fn copy_tree(
    from: &Path,
    to: &Path,
    links: &mut HashMap<(u64, u64), PathBuf>,
) -> std::io::Result<()> {
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
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if metadata.nlink() > 1 {
                let identity = (metadata.dev(), metadata.ino());
                if let Some(first) = links.get(&identity) {
                    if fs::hard_link(first, to).is_ok() {
                        return Ok(());
                    }
                } else {
                    links.insert(identity, to.to_path_buf());
                }
            }
        }
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
        copy_tree(&entry.path(), &to.join(entry.file_name()), links)?;
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
/// for the new root where both files moved (Forge's own clone and its
/// worktree), `git worktree repair` is run from each repository, and the
/// result is checked: both links name existing paths outside the old root,
/// and `git status` works in the worktree.
///
/// A repository outside the root is a user's own checkout. Nothing in it is
/// written by hand: `git worktree repair <moved worktrees>` run in it
/// rewrites the `worktrees/<name>/gitdir` of exactly those worktrees, and
/// the repository is named in the summary.
async fn repair_worktrees(
    db: &SqliteDb,
    journal: &Journal,
    target: &Path,
    command: &str,
) -> Result<Repaired, WorkspaceRootError> {
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
    let mut user_repositories = Vec::new();
    let mut warnings = Vec::new();
    for repository in repositories {
        let Some(common) = git_common_dir(&repository) else {
            continue;
        };
        let moved_repository = is_under(&repository, target);
        if moved_repository {
            rehome_git_config(&common, journal)?;
            // Every object the clone's refs need is there. A clone that
            // was damaged before the move must not make it unfinishable,
            // so this is reported, not refused.
            let fsck = git(
                &repository,
                &["fsck", "--connectivity-only", "--no-dangling"],
            )?;
            if !fsck.status.success() {
                warnings.push(format!(
                    "`git fsck --connectivity-only` reports problems in the clone {}: {}",
                    repository.display(),
                    String::from_utf8_lossy(&fsck.stderr)
                        .lines()
                        .take(3)
                        .collect::<Vec<_>>()
                        .join("; ")
                ));
            }
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
            // Written by hand only where both ends are Forge's and moved:
            // the clone's record of the worktree, and the worktree's link
            // to a clone that moved. A user's repository is left to Git.
            if moved_repository && dot_git.to_string_lossy() != recorded.as_str() {
                fs::write(
                    admin.join("gitdir"),
                    format!("{}\n", dot_git.to_string_lossy()),
                )?;
            }
            if moved_repository && moved_worktree {
                fs::write(&dot_git, format!("gitdir: {}\n", admin.to_string_lossy()))?;
            }
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
        if !moved_repository {
            user_repositories.push(repository.clone());
        }
        for (worktree, admin) in worktrees {
            check_worktree(&worktree, &admin, journal)?;
            if worktree.join(".gitmodules").is_file() {
                let submodules = git(&worktree, &["submodule", "status", "--recursive"])?;
                if !submodules.status.success() {
                    warnings.push(format!(
                        "`git submodule status` fails in the moved worktree {}: {}. Run `git submodule update --init --recursive` there",
                        worktree.display(),
                        String::from_utf8_lossy(&submodules.stderr).trim()
                    ));
                }
            }
            repaired.push(worktree);
        }
    }
    let _ = command;
    repaired.sort();
    Ok(Repaired {
        worktrees: repaired,
        user_repositories,
        warnings,
    })
}

struct Repaired {
    worktrees: Vec<PathBuf>,
    user_repositories: Vec<PathBuf>,
    warnings: Vec<String>,
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
        #[cfg(test)]
        if run.request.skip_column == Some(stored.column) {
            continue;
        }
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
        &run.request.command,
        &[],
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
    // The post-condition, inside the transaction: no column that stores a
    // path still names the old root. If one does, nothing is committed.
    let mut pending = Vec::new();
    for stored in &STORED_PATHS {
        let sql = stored.remaining_sql();
        for old in &run.journal.source_spellings {
            let old = if stored.embedded {
                json_escaped(old)
            } else {
                old.clone()
            };
            let left: i64 = sqlx::query_scalar(&sql)
                .bind(old)
                .fetch_one(&mut *tx)
                .await?;
            if left > 0 {
                pending.push(format!(
                    "{}.{} ({left} row(s))",
                    stored.table, stored.column
                ));
            }
        }
    }
    if !pending.is_empty() {
        tx.rollback().await?;
        return Err(WorkspaceRootError::Incomplete(format!(
            "db pending: every directory is in {} but the database was not changed, because after the rewrite {} would still name the old root. The move is unfinished and Forge will not start until it is: run `{}` again, and report this if it repeats",
            run.journal.target,
            pending.join(", "),
            run.request.command
        )));
    }
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
    sweep(db, "instr({column}, ?1) > 0", &[needle.to_owned()]).await
}

/// Every text column of every table with rows matching `predicate`
/// (`{column}` is the quoted column; `binds` are `?1`, `?2`, ...), as
/// `table.column` and a row count.
async fn sweep(
    db: &SqliteDb,
    predicate: &str,
    binds: &[String],
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
            let sql = format!(
                "SELECT COUNT(*) FROM {} WHERE {}",
                quoted(&table),
                predicate.replace("{column}", &quoted(&column))
            );
            let mut query = sqlx::query_scalar::<_, i64>(&sql);
            for bind in binds {
                query = query.bind(bind);
            }
            let Ok(count) = query.fetch_one(db.pool()).await else {
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
