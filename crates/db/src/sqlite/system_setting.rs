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
