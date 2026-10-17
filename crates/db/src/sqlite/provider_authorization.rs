use crate::{
    CreateProviderAuthorizationOperation, DbError, ProviderAuthorizationOperation,
    ProviderAuthorizationRepo, Result, SqliteDb, UpdateProviderAuthorizationOperation,
};
use async_trait::async_trait;
use sqlx::{sqlite::SqliteRow, Row};

#[async_trait]
impl ProviderAuthorizationRepo for SqliteDb {
    async fn create_provider_authorization(
        &self,
        input: CreateProviderAuthorizationOperation,
    ) -> Result<ProviderAuthorizationOperation> {
        sqlx::query(
            "INSERT INTO provider_authorization_operation (
                id, owner_user_id, provider, method, status, authorization_url, user_code,
                redirect_origin, callback_state_hash, request_json, poll_interval_seconds,
                expires_at, version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.owner_user_id)
        .bind(&input.provider)
        .bind(&input.method)
        .bind(&input.status)
        .bind(input.authorization_url.as_deref())
        .bind(input.user_code.as_deref())
        .bind(&input.redirect_origin)
        .bind(input.callback_state_hash.as_deref())
        .bind(&input.request_json)
        .bind(input.poll_interval_seconds)
        .bind(&input.expires_at)
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&self.pool)
        .await?;
        self.get_provider_authorization(&input.id, &input.owner_user_id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn get_provider_authorization(
        &self,
        id: &str,
        owner_user_id: &str,
    ) -> Result<Option<ProviderAuthorizationOperation>> {
        sqlx::query(
            "SELECT * FROM provider_authorization_operation
             WHERE id = ? AND owner_user_id = ?",
        )
        .bind(id)
        .bind(owner_user_id)
        .fetch_optional(&self.pool)
        .await?
        .map(map_provider_authorization)
        .transpose()
    }

    async fn get_provider_authorization_by_state_hash(
        &self,
        callback_state_hash: &str,
    ) -> Result<Option<ProviderAuthorizationOperation>> {
        sqlx::query(
            "SELECT * FROM provider_authorization_operation
             WHERE callback_state_hash = ?",
        )
        .bind(callback_state_hash)
        .fetch_optional(&self.pool)
        .await?
        .map(map_provider_authorization)
        .transpose()
    }

    async fn update_provider_authorization(
        &self,
        input: UpdateProviderAuthorizationOperation,
    ) -> Result<ProviderAuthorizationOperation> {
        let result = sqlx::query(
            "UPDATE provider_authorization_operation
             SET status = ?, authorization_url = ?, user_code = ?,
                 poll_interval_seconds = ?, profile_id = ?, credential_handle_id = ?,
                 error_code = ?, error_message = ?, completed_at = ?,
                 version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(&input.status)
        .bind(input.authorization_url.as_deref())
        .bind(input.user_code.as_deref())
        .bind(input.poll_interval_seconds)
        .bind(input.profile_id.as_deref())
        .bind(input.credential_handle_id.as_deref())
        .bind(input.error_code.as_deref())
        .bind(input.error_message.as_deref())
        .bind(input.completed_at.as_deref())
        .bind(&input.updated_at)
        .bind(&input.id)
        .bind(input.expected_version)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }
        sqlx::query("SELECT * FROM provider_authorization_operation WHERE id = ?")
            .bind(&input.id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_provider_authorization)
            .transpose()?
            .ok_or(DbError::NotFound)
    }
}

fn map_provider_authorization(row: SqliteRow) -> Result<ProviderAuthorizationOperation> {
    Ok(ProviderAuthorizationOperation {
        id: row.try_get("id")?,
        owner_user_id: row.try_get("owner_user_id")?,
        provider: row.try_get("provider")?,
        method: row.try_get("method")?,
        status: row.try_get("status")?,
        authorization_url: row.try_get("authorization_url")?,
        user_code: row.try_get("user_code")?,
        redirect_origin: row.try_get("redirect_origin")?,
        callback_state_hash: row.try_get("callback_state_hash")?,
        request_json: row.try_get("request_json")?,
        poll_interval_seconds: row.try_get("poll_interval_seconds")?,
        expires_at: row.try_get("expires_at")?,
        profile_id: row.try_get("profile_id")?,
        credential_handle_id: row.try_get("credential_handle_id")?,
        error_code: row.try_get("error_code")?,
        error_message: row.try_get("error_message")?,
        version: row.try_get("version")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        completed_at: row.try_get("completed_at")?,
    })
}

impl SqliteDb {
    pub async fn recent_provider_authorization_count(
        &self,
        owner_user_id: &str,
        since: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM provider_authorization_operation
             WHERE owner_user_id = ? AND created_at >= ?",
        )
        .bind(owner_user_id)
        .bind(since)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn update_provider_device_authorization_expiry(
        &self,
        expires_at: String,
        operation_id: &str,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(
            sqlx::query("UPDATE provider_authorization_operation SET expires_at = ? WHERE id = ?")
                .bind(expires_at)
                .bind(operation_id)
                .execute(self.pool())
                .await?,
        )
    }
}
