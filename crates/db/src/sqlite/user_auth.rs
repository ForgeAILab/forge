use super::*;
use crate::{RefreshToken, RefreshTokenRepo, User, UserRepo};

#[async_trait]
impl UserRepo for SqliteDb {
    async fn create_user(&self, user: &User) -> Result<()> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        sqlx::query(
            "INSERT INTO user (id, email, password_hash, display_name, is_admin, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&user.id)
        .bind(&user.email)
        .bind(&user.password_hash)
        .bind(user.display_name.as_deref())
        .bind(if user.is_admin { 1 } else { 0 })
        .bind(&user.created_at)
        .bind(&user.updated_at)
        .execute(&mut *transaction)
        .await?;
        bootstrap_account_main_chat_in_tx(
            &mut transaction,
            &user.id,
            &user.created_at,
            &user.updated_at,
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn get_user_by_id(&self, id: &str) -> Result<Option<User>> {
        sqlx::query("SELECT * FROM user WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_user)
            .transpose()
    }

    async fn get_user_by_email(&self, email: &str) -> Result<Option<User>> {
        sqlx::query("SELECT * FROM user WHERE email = ?")
            .bind(email)
            .fetch_optional(&self.pool)
            .await?
            .map(map_user)
            .transpose()
    }

    async fn search_users(&self, query: &str, limit: i64) -> Result<Vec<User>> {
        let pattern = format!("%{}%", query.to_lowercase());
        let rows = sqlx::query(
            "SELECT * FROM user WHERE lower(email) LIKE ? OR lower(coalesce(display_name,'')) LIKE ? ORDER BY email ASC LIMIT ?",
        )
        .bind(&pattern)
        .bind(&pattern)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(map_user).collect()
    }

    async fn list_users(&self, page: PageRequest) -> Result<Page<User>> {
        let offset = decode_offset(&page.cursor)?;
        let sql = format!(
            "SELECT * FROM user ORDER BY {} LIMIT ? OFFSET ?",
            order_clause_without_priority(&page)
        );
        let rows = sqlx::query(&sql)
            .bind(limit(&page) + 1)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?;
        let items = rows.into_iter().map(map_user).collect::<Result<Vec<_>>>()?;
        let total = if page.include_total {
            total_count(&self.pool, "SELECT COUNT(*) FROM user").await?
        } else {
            None
        };
        page_from_items(items, &page, offset, total)
    }

    async fn set_admin(&self, id: &str, is_admin: bool) -> Result<()> {
        let result = sqlx::query("UPDATE user SET is_admin = ?, updated_at = ? WHERE id = ?")
            .bind(if is_admin { 1 } else { 0 })
            .bind(crate::now_rfc3339())
            .bind(id)
            .execute(&self.pool)
            .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound);
        }
        Ok(())
    }

    async fn update_profile(
        &self,
        id: &str,
        email: &str,
        display_name: Option<&str>,
        updated_at: &str,
    ) -> Result<()> {
        let result =
            sqlx::query("UPDATE user SET email = ?, display_name = ?, updated_at = ? WHERE id = ?")
                .bind(email)
                .bind(display_name)
                .bind(updated_at)
                .bind(id)
                .execute(&self.pool)
                .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound);
        }
        Ok(())
    }

    async fn count_admins(&self) -> Result<i64> {
        Ok(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM user WHERE is_admin = 1")
                .fetch_one(&self.pool)
                .await?,
        )
    }

    async fn delete_user(&self, id: &str) -> Result<bool> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        sqlx::query(
            "INSERT OR IGNORE INTO pricing_account_deletion_guard (owner_user_id) VALUES (?)",
        )
        .bind(id)
        .execute(&mut *transaction)
        .await?;
        // agent_chat's polymorphic scopes have no FK to the chat. Remove that
        // edge explicitly before the account's chat FK cascades.
        let chats: Vec<String> = sqlx::query_scalar("SELECT id FROM agent_chat WHERE account_id=?")
            .bind(id)
            .fetch_all(&mut *transaction)
            .await?;
        for chat in chats {
            self.delete_agent_chat_scopes_in_tx(&mut transaction, &chat)
                .await?;
        }
        let result = sqlx::query("DELETE FROM user WHERE id = ?")
            .bind(id)
            .execute(&mut *transaction)
            .await?;
        let deleted = result.rows_affected() > 0;
        sqlx::query("DELETE FROM pricing_account_deletion_guard WHERE owner_user_id = ?")
            .bind(id)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(deleted)
    }
}

#[async_trait]
impl RefreshTokenRepo for SqliteDb {
    async fn create_refresh_token(&self, token: &RefreshToken) -> Result<()> {
        sqlx::query(
            "INSERT INTO refresh_token (id, user_id, token_hash, family_id, expires_at, created_at) \
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(&token.id)
        .bind(&token.user_id)
        .bind(&token.token_hash)
        .bind(&token.family_id)
        .bind(&token.expires_at)
        .bind(&token.created_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn delete_refresh_token_by_hash(&self, token_hash: &str) -> Result<Option<RefreshToken>> {
        // DELETE RETURNING is atomic: exactly one concurrent caller gets the row back.
        let row = sqlx::query("DELETE FROM refresh_token WHERE token_hash = ? RETURNING *")
            .bind(token_hash)
            .fetch_optional(&self.pool)
            .await?;
        row.map(map_refresh_token).transpose()
    }

    async fn delete_refresh_tokens_by_family(&self, family_id: &str) -> Result<u64> {
        let result = sqlx::query("DELETE FROM refresh_token WHERE family_id = ?")
            .bind(family_id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }

    async fn delete_expired_refresh_tokens(&self) -> Result<u64> {
        let now = crate::now_rfc3339();
        let result = sqlx::query("DELETE FROM refresh_token WHERE expires_at < ?")
            .bind(&now)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }

    async fn get_refresh_tokens_by_user(&self, user_id: &str) -> Result<Vec<RefreshToken>> {
        let rows =
            sqlx::query("SELECT * FROM refresh_token WHERE user_id = ? ORDER BY created_at ASC")
                .bind(user_id)
                .fetch_all(&self.pool)
                .await?;
        rows.into_iter().map(map_refresh_token).collect()
    }
}

fn map_user(row: SqliteRow) -> Result<User> {
    Ok(User {
        id: row.try_get("id")?,
        email: row.try_get("email")?,
        password_hash: row.try_get("password_hash")?,
        display_name: row.try_get("display_name")?,
        is_admin: row.try_get::<i64, _>("is_admin")? != 0,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_refresh_token(row: SqliteRow) -> Result<RefreshToken> {
    Ok(RefreshToken {
        id: row.try_get("id")?,
        user_id: row.try_get("user_id")?,
        token_hash: row.try_get("token_hash")?,
        family_id: row.try_get("family_id")?,
        expires_at: row.try_get("expires_at")?,
        created_at: row.try_get("created_at")?,
    })
}

impl SqliteDb {
    pub async fn bootstrap_first_user_admin_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        updated_at: String,
        user_id: &str,
    ) -> crate::Result<u64> {
        let result = sqlx::query("UPDATE user SET is_admin = 1, updated_at = ? WHERE id = ?")
            .bind(updated_at)
            .bind(user_id)
            .execute(connection)
            .await?;
        Ok(result.rows_affected())
    }
}

pub async fn execution_admission_account_in_tx(
    connection: &mut sqlx::SqliteConnection,
    owner_id: String,
) -> crate::Result<Option<String>> {
    Ok(
        sqlx::query_scalar::<_, String>("SELECT id FROM user WHERE id = ?")
            .bind(owner_id)
            .fetch_optional(connection)
            .await?,
    )
}

pub async fn execution_admission_default_account_in_tx(
    connection: &mut sqlx::SqliteConnection,
) -> crate::Result<Option<String>> {
    Ok(sqlx::query_scalar::<_, String>(
        "SELECT id FROM user ORDER BY is_admin DESC, created_at, id LIMIT 1",
    )
    .fetch_optional(connection)
    .await?)
}

/// Every account gets its canonical Main Chat in the transaction that creates it.
pub(super) async fn bootstrap_account_main_chat_in_tx(
    connection: &mut sqlx::SqliteConnection,
    account_id: &str,
    created_at: &str,
    updated_at: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO agent_chat (
            id, kind, account_id, project_id, status,
            instruction_revision, message_count, version, created_at, updated_at
         )
         SELECT ?, 'account_main', ?, NULL, 'agent_setup_required', 0, 0, 1, ?, ?
         WHERE NOT EXISTS (
            SELECT 1 FROM agent_chat
            WHERE kind = 'account_main' AND account_id = ?
         )",
    )
    .bind(crate::new_uuid_v4())
    .bind(account_id)
    .bind(created_at)
    .bind(updated_at)
    .bind(account_id)
    .execute(connection)
    .await?;
    Ok(())
}
