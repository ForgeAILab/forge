use super::*;
use crate::{CreateOAuthRefreshToken, OAuthRefreshToken, OAuthRefreshTokenRepo};

#[async_trait]
impl OAuthRefreshTokenRepo for SqliteDb {
    async fn create_refresh_token(
        &self,
        input: CreateOAuthRefreshToken,
    ) -> Result<OAuthRefreshToken> {
        sqlx::query(
            "INSERT INTO oauth_refresh_token (id, token_hash, family_id, user_id, client_id, resource, scopes, expires_at, created_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.token_hash)
        .bind(&input.family_id)
        .bind(&input.user_id)
        .bind(&input.client_id)
        .bind(&input.resource)
        .bind(&input.scopes)
        .bind(&input.expires_at)
        .bind(&input.created_at)
        .execute(&self.pool)
        .await?;

        sqlx::query("SELECT * FROM oauth_refresh_token WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&self.pool)
            .await
            .map(map_oauth_refresh_token)?
    }

    async fn create_refresh_token_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreateOAuthRefreshToken,
    ) -> Result<OAuthRefreshToken> {
        sqlx::query(
            "INSERT INTO oauth_refresh_token (id, token_hash, family_id, user_id, client_id, resource, scopes, expires_at, created_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.token_hash)
        .bind(&input.family_id)
        .bind(&input.user_id)
        .bind(&input.client_id)
        .bind(&input.resource)
        .bind(&input.scopes)
        .bind(&input.expires_at)
        .bind(&input.created_at)
        .execute(&mut **transaction)
        .await?;

        sqlx::query("SELECT * FROM oauth_refresh_token WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut **transaction)
            .await
            .map(map_oauth_refresh_token)?
    }

    async fn get_refresh_token_by_hash(
        &self,
        token_hash: &str,
    ) -> Result<Option<OAuthRefreshToken>> {
        sqlx::query("SELECT * FROM oauth_refresh_token WHERE token_hash = ?")
            .bind(token_hash)
            .fetch_optional(&self.pool)
            .await?
            .map(map_oauth_refresh_token)
            .transpose()
    }

    async fn claim_refresh_token_for_rotation(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        id: &str,
        revoked_at: &str,
    ) -> Result<bool> {
        let result = sqlx::query(
            "UPDATE oauth_refresh_token SET revoked_at = ? WHERE id = ? AND revoked_at IS NULL",
        )
        .bind(revoked_at)
        .bind(id)
        .execute(&mut **transaction)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    async fn revoke_refresh_token(&self, id: &str, revoked_at: &str) -> Result<()> {
        sqlx::query("UPDATE oauth_refresh_token SET revoked_at = ? WHERE id = ?")
            .bind(revoked_at)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn revoke_refresh_token_family(&self, family_id: &str, revoked_at: &str) -> Result<u64> {
        let result = sqlx::query(
            "UPDATE oauth_refresh_token SET revoked_at = ? WHERE family_id = ? AND revoked_at IS NULL",
        )
        .bind(revoked_at)
        .bind(family_id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    async fn revoke_refresh_token_family_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        family_id: &str,
        revoked_at: &str,
    ) -> Result<u64> {
        let result = sqlx::query(
            "UPDATE oauth_refresh_token SET revoked_at = ? WHERE family_id = ? AND revoked_at IS NULL",
        )
        .bind(revoked_at)
        .bind(family_id)
        .execute(&mut **transaction)
        .await?;
        Ok(result.rows_affected())
    }

    async fn delete_expired_refresh_tokens(&self, now_rfc3339: &str) -> Result<u64> {
        let result = sqlx::query("DELETE FROM oauth_refresh_token WHERE expires_at < ?")
            .bind(now_rfc3339)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }
}

fn map_oauth_refresh_token(row: SqliteRow) -> Result<OAuthRefreshToken> {
    Ok(OAuthRefreshToken {
        id: row.get("id"),
        token_hash: row.get("token_hash"),
        family_id: row.get("family_id"),
        user_id: row.get("user_id"),
        client_id: row.get("client_id"),
        resource: row.get("resource"),
        scopes: row.get("scopes"),
        expires_at: row.get("expires_at"),
        revoked_at: row.get("revoked_at"),
        created_at: row.get("created_at"),
    })
}

impl SqliteDb {
    pub async fn oauth_refresh_family_oldest_created_at(
        &self,
        family_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, Option<String>>(
            "SELECT MIN(created_at) FROM oauth_refresh_token WHERE family_id = ?",
        )
        .bind(family_id)
        .fetch_one(self.pool())
        .await?)
    }
}
