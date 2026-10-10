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
pub fn system_temp_warning(root: &Path) -> String {
    format!(
        "workspace root is in the system temp directory ({}); the operating system removes files there, so worktrees and uncommitted work can vanish. Stop Forge and run `{MIGRATE_COMMAND}`",
        root.display()
    )
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
/// - Nothing recorded, nothing chosen: `<data dir>/worktrees`, unless an
///   older release left this database's workspaces in
///   `<system temp>/forge/worktrees`, which is then kept (and reported).
/// - Recorded, nothing chosen: the recorded root, whatever today's default.
/// - Chosen and different from the record: allowed only when no workspace
///   that is not `cleaned`, no repository clone and no running run is under
///   the recorded root. Otherwise the start is refused.
/// - A move that did not finish: refused until it is finished.
pub async fn settle(db: &SqliteDb, choice: &RootChoice) -> Result<SettledRoot, WorkspaceRootError> {
    let journal = choice.data_dir.join(JOURNAL_FILE);
    if std::fs::symlink_metadata(&journal).is_ok() {
        return Err(WorkspaceRootError::Refused(format!(
            "a workspace root move did not finish ({} exists): the database and the disk disagree until it does. Run `{MIGRATE_COMMAND}` again to finish it",
            journal.display()
        )));
    }
    let recorded = recorded_root(db).await?;
    let legacy = legacy_temp_root(&choice.system_temp);
    let (root, recorded_now) = match recorded {
        Some(recorded) if !choice.explicit || same_path(&recorded, &choice.configured) => {
            (recorded, false)
        }
        Some(recorded) => {
            let live = LiveData::under(db, &recorded).await?;
            if !live.is_empty() {
                return Err(WorkspaceRootError::Refused(format!(
                    "the configured workspace root {configured} is not the one this database uses ({recorded}), which still holds {live}. Forge never runs on two workspace roots. Stop Forge and run `{MIGRATE_COMMAND} {configured}` to move the data, or set the workspace root back to {recorded}",
                    configured = choice.configured.display(),
                    recorded = recorded.display(),
                )));
            }
            (choice.configured.clone(), true)
        }
        None if choice.explicit => (choice.configured.clone(), true),
        None if has_legacy_layout(db, &legacy).await? => (legacy, true),
        None => (choice.configured.clone(), true),
    };
    if recorded_now {
        record_root(db.pool(), &root).await?;
    }
    let in_system_temp = is_under(&root, &choice.system_temp);
    record_status(db, &root, in_system_temp).await?;
    Ok(SettledRoot {
        root,
        in_system_temp,
        recorded_now,
    })
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

pub(crate) fn status_value(root: &Path, in_system_temp: bool) -> String {
    serde_json::json!({
        "state": if in_system_temp { "system_temp" } else { "ok" },
        "root": root.display().to_string(),
    })
    .to_string()
}

/// Keep what this start found where operator status reads it. Written only
/// when it changes, so `updated_at` is when the state began.
async fn record_status(
    db: &SqliteDb,
    root: &Path,
    in_system_temp: bool,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO system_setting (key, value, updated_at) VALUES (?1, ?2, ?3)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
         WHERE system_setting.value != excluded.value",
    )
    .bind(STATUS_KEY)
    .bind(status_value(root, in_system_temp))
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

/// Whether an older release left this database's workspaces in `legacy`.
async fn has_legacy_layout(db: &SqliteDb, legacy: &Path) -> Result<bool, sqlx::Error> {
    if real_dir(&legacy.join(".repos")) || real_dir(&legacy.join(".forge")) {
        return Ok(true);
    }
    for spelling in spellings(legacy) {
        let rows = sqlx::query_scalar::<_, i64>(&format!(
            "SELECT COUNT(*) FROM workspace WHERE status != 'cleaned' AND {}",
            under_sql("worktree_path")
        ))
        .bind(&spelling)
        .fetch_one(db.pool())
        .await?;
        if rows > 0 {
            return Ok(true);
        }
    }
    Ok(false)
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
