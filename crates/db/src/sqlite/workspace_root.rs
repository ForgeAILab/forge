//! SQL for the server workspace root and its explicit path rewrite.
use super::*;

mod cleanup;
pub use cleanup::ProjectCleanupOwner;

/// SQL: `column` is the path bound as `?1` or a path inside it.
pub(super) fn under_sql(column: &str) -> String {
    format!("({column} = ?1 OR substr({column}, 1, length(?1) + 1) = ?1 || '/')")
}

/// Every column that stores an absolute path under the server's workspace
/// root, and whether a rewrite bumps the row's optimistic-lock version.
pub const WORKSPACE_ROOT_STORED_PATHS: [WorkspaceRootStoredPath; 13] = [
    WorkspaceRootStoredPath::plain("workspace", "worktree_path"),
    WorkspaceRootStoredPath::versioned("repo_location", "path").only("owner_kind = 'server'"),
    // A server-owned placement's handle is its worktree path; a daemon's
    // handle is the daemon's own name for a directory under its own root.
    WorkspaceRootStoredPath::versioned("workspace_placement", "workspace_handle")
        .only("owner_kind = 'server'"),
    WorkspaceRootStoredPath::touched("repo", "local_path"),
    WorkspaceRootStoredPath::touched("repo", "remote_url"),
    WorkspaceRootStoredPath::plain("execution", "logs_path"),
    WorkspaceRootStoredPath::plain("agent_context_scope", "workspace_path"),
    WorkspaceRootStoredPath::plain("agent_inquiry", "workspace_path"),
    WorkspaceRootStoredPath::plain("agent_inquiry", "findings_path"),
    // A runtime row is a daemon's statement about its own root. Only the
    // daemon inside the server process runs on the server's root; any other
    // daemon keeps what it reported (one sharing the root keeps its Task
    // roots in the old one) and reports again when it connects.
    WorkspaceRootStoredPath::plain("runtime", "workspace_root")
        .only("daemon_id IN (SELECT id FROM daemon WHERE machine_id LIKE 'embedded:%')"),
    WorkspaceRootStoredPath::plain("integration_attempt", "repo_location_ref"),
    // Durable steps carry the paths they act on inside their JSON; a step
    // that has not run yet must act on the new root. The value is replaced
    // as text, not parsed: the old root is matched only where a `/` or the
    // closing `"` of a JSON string follows it, and both needle and
    // replacement are JSON-escaped, so a root that is a prefix of another
    // path (`/data/wt` and `/data/wt2`) is never touched, the document
    // stays valid JSON, and keys, numbers and other strings are unchanged
    // (proved by the `/data/wt2` rows of the fixture).
    WorkspaceRootStoredPath::embedded("task_step", "payload_json"),
    WorkspaceRootStoredPath::embedded("task_step", "result_json"),
];

pub struct WorkspaceRootStoredPath {
    pub table: &'static str,
    pub column: &'static str,
    /// `version = version + 1`: a holder of the old row must re-read.
    bump_version: bool,
    touch_updated_at: bool,
    /// The path is inside a JSON document, not the whole value.
    pub embedded: bool,
    /// Rows the rewrite is limited to.
    only: Option<&'static str>,
}

impl WorkspaceRootStoredPath {
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
                moved_sql(column),
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

impl WorkspaceRootStoredPath {
    /// Counts the rows of this column that still name the root bound as
    /// `?1` (JSON-escaped for an embedded path), within the rows the
    /// rewrite is limited to.
    fn remaining_sql(&self) -> String {
        let column = self.column;
        let mut rows = if self.embedded {
            format!("(instr({column}, ?1 || '/') > 0 OR instr({column}, ?1 || '\"') > 0)")
        } else {
            moved_sql(column)
        };
        if let Some(only) = self.only {
            rows = format!("{rows} AND {only}");
        }
        format!("SELECT COUNT(*) FROM {} WHERE {rows}", self.table)
    }
}

/// SQL: `column` is the root bound as `?1` or a path inside it, and not a
/// path inside `<root>/.forge/workspaces`: what a daemon sharing the root
/// keeps there stays in the old root, so a path into it stays as it is.
fn moved_sql(column: &str) -> String {
    format!(
        "({} AND NOT ({column} = ?1 || '/.forge/{DAEMON_WORKSPACES}' OR substr({column}, 1, length(?1 || '/.forge/{DAEMON_WORKSPACES}/')) = ?1 || '/.forge/{DAEMON_WORKSPACES}/'))",
        under_sql(column)
    )
}

const DAEMON_WORKSPACES: &str = "workspaces";

impl SqliteDb {
    pub async fn workspace_root_running_check_total(&self) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM check_run WHERE state IN ('running', 'cancelling', 'cleaning')",
        )
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn workspace_root_claimed_step_total(&self) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM task_step WHERE status IN ('claimed', 'suspended')",
        )
        .fetch_one(self.pool())
        .await?)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum WorkspaceRootEntity {
    Task,
    Repo,
    Project,
}
impl WorkspaceRootEntity {
    fn table(self) -> &'static str {
        match self {
            Self::Task => "task",
            Self::Repo => "repo",
            Self::Project => "project",
        }
    }
}

impl SqliteDb {
    pub async fn workspace_root_entity_count(
        &self,
        entity: WorkspaceRootEntity,
        id: &str,
    ) -> crate::Result<i64> {
        let table = entity.table();
        let sql = format!("SELECT COUNT(*) FROM {table} WHERE id = ?");
        Ok(sqlx::query_scalar(&sql)
            .bind(id)
            .fetch_one(self.pool())
            .await?)
    }
}

pub struct WorkspaceRootPathRewrite {
    sql: String,
    touch_updated_at: bool,
}
impl WorkspaceRootStoredPath {
    pub fn rewrite(&self) -> WorkspaceRootPathRewrite {
        WorkspaceRootPathRewrite {
            sql: self.update_sql(),
            touch_updated_at: self.touch_updated_at,
        }
    }
}
impl WorkspaceRootPathRewrite {
    pub async fn execute_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        old: String,
        new: String,
        now: &str,
    ) -> crate::Result<u64> {
        let mut query = sqlx::query(&self.sql).bind(old).bind(new);
        if self.touch_updated_at {
            query = query.bind(now);
        }
        Ok(query.execute(connection).await?.rows_affected())
    }
}

pub struct WorkspaceRootPathRemaining {
    sql: String,
}
impl WorkspaceRootStoredPath {
    pub fn remaining(&self) -> WorkspaceRootPathRemaining {
        WorkspaceRootPathRemaining {
            sql: self.remaining_sql(),
        }
    }
}
impl WorkspaceRootPathRemaining {
    pub async fn count_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        old: String,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar(&self.sql)
            .bind(old)
            .fetch_one(connection)
            .await?)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum WorkspaceRootTextPredicate {
    Mentions,
    NamedUnit,
}
impl WorkspaceRootTextPredicate {
    fn sql(self) -> &'static str {
        match self {
            Self::Mentions => "instr({column}, ?1) > 0",
            Self::NamedUnit => "({column} = ?1 OR instr({column}, ?1 || '/') > 0 OR instr({column}, ?2 || '/') > 0 OR instr({column}, ?2 || '\"') > 0)",
        }
    }
}

impl SqliteDb {
    pub async fn workspace_root_text_tables(&self) -> crate::Result<Vec<String>> {
        Ok(sqlx::query_scalar(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
         AND sql NOT LIKE 'CREATE VIRTUAL%' ORDER BY name",
        )
        .fetch_all(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn workspace_root_table_columns(
        &self,
        table: &str,
    ) -> crate::Result<Vec<(String, String)>> {
        Ok(sqlx::query_as(&format!(
            "SELECT name, type FROM pragma_table_info('{}')",
            table.replace('\'', "''")
        ))
        .fetch_all(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn workspace_root_column_match_count(
        &self,
        table: &str,
        column: &str,
        predicate: WorkspaceRootTextPredicate,
        binds: &[String],
    ) -> crate::Result<i64> {
        let quoted = |name: &str| format!("\"{}\"", name.replace('"', "\"\""));
        let sql = format!(
            "SELECT COUNT(*) FROM {} WHERE {}",
            quoted(table),
            predicate.sql().replace("{column}", &quoted(column))
        );
        let mut query = sqlx::query_scalar::<_, i64>(&sql);
        for bind in binds {
            query = query.bind(bind);
        }
        Ok(query.fetch_one(self.pool()).await?)
    }
}
