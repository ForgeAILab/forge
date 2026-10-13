use super::*;
use crate::SystemSettingRepo;

#[async_trait]
impl SystemSettingRepo for SqliteDb {
    async fn get_setting(&self, key: &str) -> Result<Option<String>> {
        let row = sqlx::query_scalar::<_, String>("SELECT value FROM system_setting WHERE key = ?")
            .bind(key)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row)
    }

    async fn set_setting(&self, key: &str, value: &str, updated_at: &str) -> Result<()> {
        sqlx::query(
            "INSERT INTO system_setting (key, value, updated_at) VALUES (?, ?, ?) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
        )
        .bind(key)
        .bind(value)
        .bind(updated_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn list_settings(&self) -> Result<Vec<(String, String)>> {
        let rows = sqlx::query_as::<_, (String, String)>(
            "SELECT key, value FROM system_setting ORDER BY key ASC",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn delete_setting(&self, key: &str) -> Result<()> {
        let result = sqlx::query("DELETE FROM system_setting WHERE key = ?")
            .bind(key)
            .execute(&self.pool)
            .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound);
        }
        Ok(())
    }
}

impl SqliteDb {
    pub async fn setting_value_with_updated_at(
        &self,
        key: &str,
    ) -> crate::Result<Option<(String, String)>> {
        let result = sqlx::query_as::<_, (String, String)>(
            "SELECT value, updated_at FROM system_setting WHERE key = ?",
        )
        .bind(key)
        .fetch_optional(self.pool())
        .await?;
        Ok(result)
    }
}

impl SqliteDb {
    pub async fn bootstrap_completion_count_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM system_setting WHERE key = 'bootstrap_completed'",
        )
        .fetch_one(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn mark_bootstrap_completed_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        updated_at: &str,
    ) -> crate::Result<u64> {
        let result = sqlx::query("INSERT INTO system_setting (key, value, updated_at) VALUES ('bootstrap_completed', 'true', ?) \
             ON CONFLICT(key) DO NOTHING")
            .bind(updated_at)
            .execute(connection)
            .await?;
        Ok(result.rows_affected())
    }
}

impl SqliteDb {
    pub async fn insert_workspace_gc_owner(
        &self,
        key: &str,
        owner_id: String,
        updated_at: String,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
            "INSERT INTO system_setting (key, value, updated_at) VALUES (?, ?, ?)
             ON CONFLICT(key) DO NOTHING",
        )
        .bind(key)
        .bind(owner_id)
        .bind(updated_at)
        .execute(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn workspace_gc_owner_value(&self, key: &str) -> crate::Result<String> {
        Ok(
            sqlx::query_scalar::<_, String>("SELECT value FROM system_setting WHERE key = ?")
                .bind(key)
                .fetch_one(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn record_workspace_gc_status(
        &self,
        key: &str,
        value: String,
        updated_at: String,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
            "INSERT INTO system_setting (key, value, updated_at) VALUES (?1, ?2, ?3)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
             WHERE system_setting.value != excluded.value",
        )
        .bind(key)
        .bind(value)
        .bind(updated_at)
        .execute(self.pool())
        .await?)
    }
}
