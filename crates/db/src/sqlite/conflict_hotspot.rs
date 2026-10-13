//! Conflict-hotspot episode boundaries; caller-owned writer transactions.
use crate::SqliteDb;

impl SqliteDb {
    pub async fn conflict_hotspot_episode_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        project_id: &str,
        path: &str,
    ) -> crate::Result<Option<(Option<String>, Option<String>)>> {
        Ok(sqlx::query_as(
            "SELECT resolved_after, open_since
    FROM conflict_hotspot_boundary WHERE project_id = ? AND path = ?",
        )
        .bind(project_id)
        .bind(path)
        .fetch_optional(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn close_conflict_hotspot_episode_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        resolved_after: &Option<String>,
        project_id: &str,
        path: &str,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
            "UPDATE conflict_hotspot_boundary
    SET resolved_after = ?, open_since = NULL WHERE project_id = ? AND path = ?",
        )
        .bind(resolved_after)
        .bind(project_id)
        .bind(path)
        .execute(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn open_conflict_hotspot_episode_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        project_id: &str,
        path: &str,
        resolved_after: Option<String>,
        open_since: &str,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
            "INSERT INTO conflict_hotspot_boundary
    (project_id, path, resolved_after, open_since) VALUES (?, ?, ?, ?)
    ON CONFLICT(project_id, path) DO UPDATE SET open_since = excluded.open_since",
        )
        .bind(project_id)
        .bind(path)
        .bind(resolved_after)
        .bind(open_since)
        .execute(connection)
        .await?)
    }
}
