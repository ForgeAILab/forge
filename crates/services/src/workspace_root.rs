//! Which directory this database's server-owned workspaces live in.
//!
//! The workspace root holds Task worktrees, repository clones and execution
//! logs, and the database stores absolute paths into it. So the root in use
//! is recorded once (`system_setting.workspace_root`) and a start never
//! silently switches to another one: a default that changed between releases
//! does not move an install, and an operator's changed setting is honoured
//! only while nothing lives under the recorded root. Everything else goes
//! through the explicit move, [`migrate`] (`forge --migrate-workspace-root`).

pub mod migrate;

use db::{now_rfc3339, SqliteDb};
use std::path::{Path, PathBuf};

/// The recorded workspace root: an absolute path, written at first start.
pub const ROOT_KEY: &str = "workspace_root";
/// What the last start found about the root, for operator status.
pub const STATUS_KEY: &str = "workspace_root_status";
/// The command every refusal and warning names.
pub const MIGRATE_COMMAND: &str = "forge --migrate-workspace-root";
/// The journal of a move in progress, in the data directory. While it exists
/// the database and the disk disagree, so no server starts.
pub const JOURNAL_FILE: &str = "workspace-root-migration.json";

/// What a starting server knows about the root it was asked to use.
#[derive(Debug, Clone)]
pub struct RootChoice {
    /// The root from configuration: the operator's, or the default.
    pub configured: PathBuf,
    /// Whether the operator chose it (config file, environment, override).
    pub explicit: bool,
    /// The data directory of this server (where a move keeps its journal).
    pub data_dir: PathBuf,
    /// The system temp directory. Releases before the default moved kept
    /// the root at `<system temp>/forge/worktrees`.
    pub system_temp: PathBuf,
    /// The exact command that moves this install's root, as its operator
    /// types it (`forge --data-dir <dir> --migrate-workspace-root` when the
    /// data directory is not the default one). Every refusal and warning
    /// names it.
    pub migrate_command: String,
}

impl RootChoice {
    /// The move command for a data directory: [`MIGRATE_COMMAND`] alone for
    /// the default one, with `--data-dir` for any other.
    #[must_use]
    pub fn migrate_command_for(data_dir: &Path, default_data_dir: &Path) -> String {
        if same_path(data_dir, default_data_dir) {
            MIGRATE_COMMAND.to_owned()
        } else {
            format!(
                "forge --data-dir {} --migrate-workspace-root",
                data_dir.display()
            )
        }
    }
}

/// The root a server runs on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettledRoot {
    pub root: PathBuf,
    /// The root is inside the system temp directory, where the operating
    /// system deletes worktrees. Reported at start and in operator status.
    pub in_system_temp: bool,
    /// This start wrote the record (first start, or an allowed change).
    pub recorded_now: bool,
    /// What the operator should know about this start: stored paths outside
    /// the root, a recorded root that was missing, a root another database
    /// adopted. Never a reason to refuse; logged and in operator status.
    pub warnings: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum WorkspaceRootError {
    /// The start (or the move) must not go on. The message is for the
    /// operator and names what to do.
    #[error("{0}")]
    Refused(String),
    /// A move stopped part way. Its journal is kept: the same command
    /// finishes it, and no server starts before that.
    #[error("{0}")]
    Incomplete(String),
    #[error("workspace root record: {0}")]
    Database(#[from] sqlx::Error),
    #[error("workspace root: {0}")]
    Io(#[from] std::io::Error),
}

/// The default root of releases that kept it in the system temp directory.
#[must_use]
pub fn legacy_temp_root(system_temp: &Path) -> PathBuf {
    system_temp.join("forge").join("worktrees")
}

/// The warning a server on a temp-directory root logs and reports.
#[must_use]
pub fn system_temp_warning(root: &Path, migrate_command: &str) -> String {
    format!(
        "workspace root is in the system temp directory ({}); the operating system removes files there, so worktrees and uncommitted work can vanish. Nothing moves by itself: stop Forge and run `{migrate_command}`",
        root.display()
    )
}

/// The root of a service that is built without a server start: a test, or
/// an embedded fixture. One directory per process under the temp directory,
/// never a root any install uses (not the old `<temp>/forge/worktrees`
/// default, not an environment variable), so a fixture cannot write into a
/// real install's worktrees.
#[must_use]
pub fn fixture_root() -> PathBuf {
    static ROOT: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    ROOT.get_or_init(|| {
        std::env::temp_dir()
            .join(format!("forge-fixture-{}", std::process::id()))
            .join("worktrees")
    })
    .clone()
}

/// The workspace root of the server on `db`, for code that is handed a
/// database and no root: the root the start settled and recorded. There is
/// no other source: not the environment, not a default.
///
/// A database no server ever started on has no record. That is an error,
/// except in a test build, where it is the [`fixture_root`].
pub async fn root_of(db: &SqliteDb) -> Result<PathBuf, WorkspaceRootError> {
    if let Some(recorded) = recorded_root(db).await? {
        return Ok(recorded);
    }
    #[cfg(any(test, feature = "test-support"))]
    {
        Ok(fixture_root())
    }
    #[cfg(not(any(test, feature = "test-support")))]
    {
        Err(WorkspaceRootError::Refused(
            "this database has no recorded workspace root: the server records one when it starts"
                .to_owned(),
        ))
    }
}

/// The recorded root, if a server ever started on this database.
pub async fn recorded_root(db: &SqliteDb) -> Result<Option<PathBuf>, WorkspaceRootError> {
    Ok(
        sqlx::query_scalar::<_, String>("SELECT value FROM system_setting WHERE key = ?")
            .bind(ROOT_KEY)
            .fetch_optional(db.pool())
            .await?
            .filter(|value| !value.is_empty())
            .map(PathBuf::from),
    )
}

/// Decide the workspace root of a starting server and record it.
///
/// - A move that did not finish: refused until the same command finishes it.
/// - Nothing recorded, a root chosen (config, environment, flag): that root.
/// - Nothing recorded, nothing chosen: the root this database's own rows
///   name ([`stored_roots`]; an install from before the default moved keeps
///   the temp-directory root it was written under, whatever today's temp
///   directory is), else `<data dir>/worktrees`.
/// - Recorded, nothing chosen: the recorded root, whatever today's default.
/// - Chosen and different from the record: allowed only when no workspace
///   that is not `cleaned`, no repository clone and no running run is under
///   the recorded root. Otherwise the start is refused.
/// - The root to run on does not exist and cannot be created (its volume is
///   not mounted, the data directory came from another machine): refused,
///   naming the command that points the database at a new root.
///
/// A refusal is the last resort and always names the command that resolves
/// it; no install that started before roots were recorded is refused.
pub async fn settle(db: &SqliteDb, choice: &RootChoice) -> Result<SettledRoot, WorkspaceRootError> {
    let command = choice.migrate_command.as_str();
    let journal = choice.data_dir.join(JOURNAL_FILE);
    if std::fs::symlink_metadata(&journal).is_ok() {
        return Err(WorkspaceRootError::Refused(format!(
            "migration in progress: a workspace root move did not finish ({} exists), so some directories are in the new root while the database still names the old one. Run `{command}` to finish it; it goes on from the last finished step",
            journal.display()
        )));
    }
    let recorded = recorded_root(db).await?;
    let stored = stored_roots(db).await?;
    let mut warnings = Vec::new();
    let mut written_under_temp_default = false;
    let (root, recorded_now) = match recorded {
        Some(recorded) if !choice.explicit || same_path(&recorded, &choice.configured) => {
            (recorded, false)
        }
        Some(recorded) => {
            let live = LiveData::under(db, &recorded).await?;
            if !live.is_empty() {
                let gone = if real_dir_or_link(&recorded) {
                    String::new()
                } else {
                    format!(" ({} no longer exists: nothing is there to move, and the command only points the database at the new root)", recorded.display())
                };
                return Err(WorkspaceRootError::Refused(format!(
                    "the configured workspace root {configured} is not the one this database uses ({recorded}), which still holds {live}{gone}. Forge never runs on two workspace roots. Stop Forge and run `{command} {configured}` to move the data, or set the workspace root back to {recorded}",
                    configured = choice.configured.display(),
                    recorded = recorded.display(),
                )));
            }
            (choice.configured.clone(), true)
        }
        None if choice.explicit => (choice.configured.clone(), true),
        None => match stored.first() {
            // The rows of an install from before roots were recorded: keep
            // the root they were written under. Such an install never chose
            // a root, so that root was the temp-directory default of the
            // launch that wrote them, wherever today's temp directory is.
            Some(first) => {
                let root = PathBuf::from(&first.root);
                written_under_temp_default = !same_path(&root, &choice.configured);
                (root, true)
            }
            None => (choice.configured.clone(), true),
        },
    };
    // Rows of this database under some other root: they keep working (the
    // paths are absolute) and are never moved or swept from here. Say so.
    let elsewhere: Vec<&StoredRoot> = stored
        .iter()
        .filter(|other| !same_path(Path::new(&other.root), &root))
        .collect();
    if !elsewhere.is_empty() {
        warnings.push(format!(
            "this database also stores paths outside its workspace root {}: {}. They stay where they are and keep working; Forge neither moves nor cleans them. `{command}` moves the workspace root only",
            root.display(),
            elsewhere
                .iter()
                .map(|other| other.to_string())
                .collect::<Vec<_>>()
                .join("; ")
        ));
    }
    if !real_dir_or_link(&root) {
        let live = LiveData::under(db, &root).await?;
        if let Err(error) = std::fs::create_dir_all(&root) {
            return Err(WorkspaceRootError::Refused(format!(
                "the workspace root {root} does not exist and cannot be created ({error}). If it is on a volume that is not mounted, mount it and start Forge again. If it is gone for good (the data directory was moved or copied from another machine), run `{command}`: nothing is there to move, and the command points the database at the new root",
                root = root.display(),
            )));
        }
        if live.workspaces > 0 || live.running > 0 {
            warnings.push(format!(
                "the workspace root {} was missing (a temp directory the system emptied?) and was created again; {} stored workspace(s) under it lost their files. Forge recreates a Task's worktree from its branch when the Task next runs; uncommitted work in them is gone",
                root.display(),
                live.workspaces
            ));
        }
    }
    if let Some(owner) = foreign_owner(db, &root).await? {
        warnings.push(format!(
            "the workspace root {} was adopted for garbage collection by another Forge database (owner {owner}); this database's worktrees there keep working and nothing is swept. See `forge --reclaim-workspace-gc`",
            root.display()
        ));
    }
    if recorded_now {
        record_root(db.pool(), &root).await?;
    }
    let in_system_temp = written_under_temp_default
        || is_under(&root, &choice.system_temp)
        || looks_like_temp_default(&root)
        || was_reported_in_temp(db, &root).await?;
    record_status(db, &root, in_system_temp, command, &warnings).await?;
    Ok(SettledRoot {
        root,
        in_system_temp,
        recorded_now,
        warnings,
    })
}

/// A root this database's server-owned rows name, with what they hold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredRoot {
    pub root: String,
    /// Workspaces that are not `cleaned`.
    pub workspaces: i64,
    pub clones: i64,
    pub logs: i64,
}

impl std::fmt::Display for StoredRoot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{} ({} workspace(s) that are not cleaned, {} repository clone(s), {} execution log(s))",
            self.root, self.workspaces, self.clones, self.logs
        )
    }
}

/// The workspace roots this database's own rows were written under, the one
/// holding the most live data first.
///
/// Only the database can say where an install from before roots were
/// recorded lives: the temp directory of this launch need not be the one
/// its rows were written under (a service manager and a shell disagree, and
/// so do `/tmp` and macOS's per-user `/var/folders/...`), and every data
/// directory on a machine used to share `<system temp>/forge/worktrees`, so
/// directories prove nothing. Each stored path has one shape:
/// a Task worktree is `<root>/<task id>/<repository>`, a clone is
/// `<root>/.repos/<repository id>`, a log is `<root>/.forge/logs/...`.
/// Daemon-owned rows name a daemon's root and are left out.
pub(crate) async fn stored_roots(db: &SqliteDb) -> Result<Vec<StoredRoot>, sqlx::Error> {
    fn entry<'a>(all: &'a mut Vec<StoredRoot>, root: &str) -> &'a mut StoredRoot {
        let root = root.trim_end_matches('/');
        let known = all
            .iter()
            .position(|known| same_path(Path::new(&known.root), Path::new(root)));
        let at = known.unwrap_or_else(|| {
            all.push(StoredRoot {
                root: root.to_owned(),
                workspaces: 0,
                clones: 0,
                logs: 0,
            });
            all.len() - 1
        });
        &mut all[at]
    }
    let mut all: Vec<StoredRoot> = Vec::new();
    let worktrees: Vec<(String, String)> = sqlx::query_as(
        "SELECT w.worktree_path, w.task_id FROM workspace w
         LEFT JOIN workspace_placement p ON p.workspace_id = w.id
         WHERE w.status != 'cleaned' AND COALESCE(p.owner_kind, 'server') = 'server'",
    )
    .fetch_all(db.pool())
    .await?;
    for (path, task_id) in worktrees {
        let task_root = Path::new(&path).parent();
        let named_for_task = task_root
            .and_then(Path::file_name)
            .is_some_and(|name| name.to_string_lossy() == task_id.as_str());
        if let Some(root) = task_root.and_then(Path::parent).filter(|_| named_for_task) {
            if root.is_absolute() && root.parent().is_some() {
                entry(&mut all, &root.to_string_lossy()).workspaces += 1;
            }
        }
    }
    let clones: Vec<String> =
        sqlx::query_scalar("SELECT path FROM repo_location WHERE owner_kind = 'server'")
            .fetch_all(db.pool())
            .await?;
    for path in clones {
        let path = Path::new(&path);
        let in_repos = path
            .parent()
            .and_then(Path::file_name)
            .is_some_and(|name| name == ".repos");
        if let Some(root) = path.parent().and_then(Path::parent).filter(|_| in_repos) {
            if root.is_absolute() && root.parent().is_some() {
                entry(&mut all, &root.to_string_lossy()).clones += 1;
            }
        }
    }
    let logs: Vec<(String, i64)> = sqlx::query_as(
        "SELECT substr(logs_path, 1, instr(logs_path, '/.forge/logs/') - 1), COUNT(*)
         FROM execution WHERE instr(logs_path, '/.forge/logs/') > 1 GROUP BY 1",
    )
    .fetch_all(db.pool())
    .await?;
    for (root, count) in logs {
        if Path::new(&root).is_absolute() && Path::new(&root).parent().is_some() {
            entry(&mut all, &root).logs += count;
        }
    }
    all.sort_by(|left, right| {
        (right.workspaces, right.clones, right.logs, &left.root).cmp(&(
            left.workspaces,
            left.clones,
            left.logs,
            &right.root,
        ))
    });
    Ok(all)
}

/// `<a system temp directory>/forge/worktrees`: the default of releases
/// before the root moved, under a temp directory this launch does not use.
fn looks_like_temp_default(root: &Path) -> bool {
    const TEMP_DIRS: [&str; 6] = [
        "/tmp",
        "/private/tmp",
        "/var/tmp",
        "/private/var/tmp",
        "/var/folders",
        "/private/var/folders",
    ];
    root.ends_with("forge/worktrees") && TEMP_DIRS.iter().any(|temp| root.starts_with(temp))
}

/// An earlier start found this root in a system temp directory. Kept, so a
/// launch with another temp directory still reports it.
async fn was_reported_in_temp(db: &SqliteDb, root: &Path) -> Result<bool, sqlx::Error> {
    let status = sqlx::query_scalar::<_, String>("SELECT value FROM system_setting WHERE key = ?")
        .bind(STATUS_KEY)
        .fetch_optional(db.pool())
        .await?;
    Ok(status
        .and_then(|value| serde_json::from_str::<serde_json::Value>(&value).ok())
        .is_some_and(|status| {
            status["state"].as_str() == Some("system_temp")
                && status["root"]
                    .as_str()
                    .is_some_and(|known| same_path(Path::new(known), root))
        }))
}

/// The owner named by the root's garbage-collection marker, when it is not
/// this database (a data directory copied beside its original, or two data
/// directories that shared the old temp-directory default).
async fn foreign_owner(db: &SqliteDb, root: &Path) -> Result<Option<String>, sqlx::Error> {
    let Ok(marker) = std::fs::read_to_string(
        root.join(executors::gc::GC_DIR)
            .join(executors::gc::OWNER_FILE),
    ) else {
        return Ok(None);
    };
    let mine = sqlx::query_scalar::<_, String>("SELECT value FROM system_setting WHERE key = ?")
        .bind(crate::workspace_cleanup::GC_OWNER_KEY)
        .fetch_optional(db.pool())
        .await?;
    let marker = marker.trim();
    Ok((mine.as_deref() != Some(marker) && !marker.is_empty())
        .then(|| marker.chars().take(64).collect()))
}

/// A directory, or a link to one: a root reached through a link is a root.
fn real_dir_or_link(path: &Path) -> bool {
    path.is_dir()
}

pub(crate) async fn record_root<'e>(
    executor: impl sqlx::Executor<'e, Database = sqlx::Sqlite>,
    root: &Path,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO system_setting (key, value, updated_at) VALUES (?1, ?2, ?3)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
    )
    .bind(ROOT_KEY)
    .bind(root.to_string_lossy().as_ref())
    .bind(now_rfc3339())
    .execute(executor)
    .await?;
    Ok(())
}

pub(crate) fn status_value(
    root: &Path,
    in_system_temp: bool,
    migrate_command: &str,
    warnings: &[String],
) -> String {
    serde_json::json!({
        "state": if in_system_temp { "system_temp" } else { "ok" },
        "root": root.display().to_string(),
        "migrate_command": migrate_command,
        "warnings": warnings,
    })
    .to_string()
}

/// Keep what this start found where operator status reads it. Written only
/// when it changes, so `updated_at` is when the state began.
async fn record_status(
    db: &SqliteDb,
    root: &Path,
    in_system_temp: bool,
    migrate_command: &str,
    warnings: &[String],
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO system_setting (key, value, updated_at) VALUES (?1, ?2, ?3)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
         WHERE system_setting.value != excluded.value",
    )
    .bind(STATUS_KEY)
    .bind(status_value(
        root,
        in_system_temp,
        migrate_command,
        warnings,
    ))
    .bind(now_rfc3339())
    .execute(db.pool())
    .await?;
    Ok(())
}

/// What still lives under a root.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LiveData {
    pub workspaces: i64,
    pub clones: usize,
    pub running: i64,
}

impl LiveData {
    pub(crate) fn is_empty(&self) -> bool {
        self.workspaces == 0 && self.clones == 0 && self.running == 0
    }

    pub(crate) async fn under(db: &SqliteDb, root: &Path) -> Result<Self, sqlx::Error> {
        let mut workspaces = 0;
        let mut running = 0;
        for spelling in spellings(root) {
            workspaces += sqlx::query_scalar::<_, i64>(&format!(
                "SELECT COUNT(*) FROM workspace WHERE status != 'cleaned' AND {}",
                under_sql("worktree_path")
            ))
            .bind(&spelling)
            .fetch_one(db.pool())
            .await?;
            running += sqlx::query_scalar::<_, i64>(&format!(
                "SELECT COUNT(*) FROM execution e
                 LEFT JOIN workspace w ON w.id = e.workspace_id
                 WHERE e.status = 'running' AND ({} OR {})",
                under_sql("e.logs_path"),
                under_sql("w.worktree_path")
            ))
            .bind(&spelling)
            .fetch_one(db.pool())
            .await?;
        }
        let clones = std::fs::read_dir(root.join(".repos"))
            .map(|entries| {
                entries
                    .flatten()
                    .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
                    .count()
            })
            .unwrap_or(0);
        Ok(Self {
            workspaces,
            clones,
            running,
        })
    }
}

impl std::fmt::Display for LiveData {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{} workspace(s) that are not cleaned, {} repository clone(s) and {} running run(s)",
            self.workspaces, self.clones, self.running
        )
    }
}

/// SQL: `column` is the path bound as `?1` or a path inside it.
pub(crate) fn under_sql(column: &str) -> String {
    format!("({column} = ?1 OR substr({column}, 1, length(?1) + 1) = ?1 || '/')")
}

/// The ways a stored path may spell `path`: as given, and resolved (on
/// macOS the temp directory is reached through `/var` and resolves to
/// `/private/var`; rows hold either form).
pub(crate) fn spellings(path: &Path) -> Vec<String> {
    let given = trimmed(path);
    let mut all = vec![given];
    if let Ok(resolved) = std::fs::canonicalize(path) {
        let resolved = trimmed(&resolved);
        if !all.contains(&resolved) {
            all.push(resolved);
        }
    }
    all
}

fn trimmed(path: &Path) -> String {
    let text = path.to_string_lossy();
    let cut = text.trim_end_matches('/');
    if cut.is_empty() { "/" } else { cut }.to_owned()
}

pub(crate) fn same_path(left: &Path, right: &Path) -> bool {
    let (left_all, right_all) = (spellings(left), spellings(right));
    left_all.iter().any(|spelling| right_all.contains(spelling))
}

/// Whether `path` is `parent` or inside it, in any spelling of either.
pub(crate) fn is_under(path: &Path, parent: &Path) -> bool {
    spellings(path).iter().any(|path| {
        spellings(parent).iter().any(|parent| {
            path == parent
                || path
                    .strip_prefix(parent.as_str())
                    .is_some_and(|rest| rest.starts_with('/'))
        })
    })
}

pub(crate) fn real_dir(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_dir())
}

#[cfg(test)]
mod tests;
