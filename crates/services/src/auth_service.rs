use std::sync::Arc;

use api_types::McpAccessTokenClaims;
use db::{
    new_uuid_v4, now_rfc3339, PersonalAccessTokenRepo, RefreshToken, RefreshTokenRepo, SqliteDb,
    SystemSettingRepo, User, UserRepo,
};
use sha2::{Digest, Sha256};

use crate::{Result, ServiceError};

const ACCESS_TOKEN_EXPIRY_SECS: u64 = 900; // 15 minutes
const REFRESH_TOKEN_EXPIRY_SECS: u64 = 7 * 24 * 3600; // 7 days

#[derive(Clone)]
pub struct AuthService {
    db: Arc<SqliteDb>,
    jwt_secret: Vec<u8>,
    bcrypt_cost: u32,
}

#[derive(Debug, Clone)]
pub struct TokenPair {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_in: u64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Claims {
    sub: String,
    email: String,
    #[serde(default)]
    is_admin: bool,
    iat: u64,
    exp: u64,
}

impl AuthService {
    pub fn new(db: Arc<SqliteDb>, jwt_secret: Vec<u8>, bcrypt_cost: u32) -> Self {
        Self {
            db,
            jwt_secret,
            bcrypt_cost,
        }
    }

    pub async fn register(
        &self,
        email: &str,
        password: &str,
        display_name: Option<&str>,
    ) -> Result<User> {
        let email = email.trim().to_lowercase();
        if !is_valid_email(&email) {
            return Err(ServiceError::InvalidOperation {
                message: "invalid_email".into(),
            });
        }
        if password.len() < 8 {
            return Err(ServiceError::InvalidOperation {
                message: "password_too_weak".into(),
            });
        }
        if let Some(name) = display_name {
            if name.len() > 255 {
                return Err(ServiceError::InvalidOperation {
                    message: "display_name_too_long".into(),
                });
            }
        }

        let password_hash = bcrypt::hash(password, self.bcrypt_cost).map_err(|e| {
            ServiceError::InvalidOperation {
                message: format!("hash error: {e}"),
            }
        })?;

        let now = now_rfc3339();
        let user = User {
            id: new_uuid_v4(),
            email,
            password_hash,
            display_name: display_name.map(String::from),
            is_admin: false,
            created_at: now.clone(),
            updated_at: now,
        };

        UserRepo::create_user(&*self.db, &user).await.map_err(|e| {
            if e.to_string().contains("UNIQUE") {
                ServiceError::Conflict("email_exists".into())
            } else {
                ServiceError::from(e)
            }
        })?;

        if let Err(error) = self.bootstrap_first_user(&user.id).await {
            tracing::warn!(%error, "bootstrap assignment failed (non-fatal)");
        }

        Ok(user)
    }

    pub async fn login(&self, email: &str, password: &str) -> Result<TokenPair> {
        let email = email.trim().to_lowercase();
        let user = UserRepo::get_user_by_email(&*self.db, &email).await?;

        match user {
            Some(user) => {
                let valid = bcrypt::verify(password, &user.password_hash).unwrap_or(false);
                if !valid {
                    return Err(ServiceError::InvalidOperation {
                        message: "invalid_credentials".into(),
                    });
                }
                self.issue_token_pair(&user).await
            }
            None => {
                // Timing-safe: perform dummy bcrypt verify to prevent enumeration
                let dummy_hash = "$2b$12$LJ3m4ys3Lg7ECg8Mmpfmkea3RADRCnFXaOJsDaF5LxlWAyrVaoHDu";
                let _ = bcrypt::verify(password, dummy_hash);
                Err(ServiceError::InvalidOperation {
                    message: "invalid_credentials".into(),
                })
            }
        }
    }

    pub async fn refresh(&self, raw_refresh_token: &str) -> Result<TokenPair> {
        let token_hash = hash_token(raw_refresh_token);

        let stored = RefreshTokenRepo::delete_refresh_token_by_hash(&*self.db, &token_hash).await?;

        let stored = match stored {
            Some(t) => t,
            None => {
                // Could be a reuse attempt — try to find the family by checking if
                // any token with this hash was previously rotated. Since we delete on
                // use, absence means either invalid or reused. We can't distinguish
                // without extra tracking, so just reject.
                return Err(ServiceError::InvalidOperation {
                    message: "invalid_refresh_token".into(),
                });
            }
        };

        // Check expiry
        let now = now_rfc3339();
        if stored.expires_at < now {
            return Err(ServiceError::InvalidOperation {
                message: "refresh_token_expired".into(),
            });
        }

        // Look up user
        let user = UserRepo::get_user_by_id(&*self.db, &stored.user_id)
            .await?
            .ok_or_else(|| ServiceError::InvalidOperation {
                message: "user_not_found".into(),
            })?;

        // Issue new pair with same family
        self.issue_token_pair_with_family(&user, &stored.family_id)
            .await
    }

    pub async fn logout(&self, raw_refresh_token: &str) -> Result<()> {
        let token_hash = hash_token(raw_refresh_token);
        let _ = RefreshTokenRepo::delete_refresh_token_by_hash(&*self.db, &token_hash).await?;
        Ok(())
    }

    pub async fn get_user(&self, user_id: &str) -> Result<User> {
        UserRepo::get_user_by_id(&*self.db, user_id)
            .await?
            .ok_or_else(|| ServiceError::InvalidOperation {
                message: "user_not_found".into(),
            })
    }

    pub async fn update_profile(
        &self,
        user_id: &str,
        email: Option<&str>,
        display_name: Option<Option<&str>>,
    ) -> Result<User> {
        let current = UserRepo::get_user_by_id(&*self.db, user_id)
            .await?
            .ok_or_else(|| ServiceError::InvalidOperation {
                message: "user_not_found".into(),
            })?;

        let new_email = if let Some(e) = email {
            let e = e.trim().to_lowercase();
            if !is_valid_email(&e) {
                return Err(ServiceError::InvalidOperation {
                    message: "invalid_email".into(),
                });
            }
            e
        } else {
            current.email.clone()
        };

        let new_display_name: Option<String> = match display_name {
            Some(Some(name)) => {
                if name.len() > 255 {
                    return Err(ServiceError::InvalidOperation {
                        message: "display_name_too_long".into(),
                    });
                }
                Some(name.to_string())
            }
            Some(None) => None,
            None => current.display_name.clone(),
        };

        UserRepo::update_profile(
            &*self.db,
            user_id,
            &new_email,
            new_display_name.as_deref(),
            &now_rfc3339(),
        )
        .await
        .map_err(|e| {
            if e.to_string().contains("UNIQUE") {
                ServiceError::Conflict("email_exists".into())
            } else {
                ServiceError::from(e)
            }
        })?;

        UserRepo::get_user_by_id(&*self.db, user_id)
            .await?
            .ok_or_else(|| ServiceError::InvalidOperation {
                message: "user_not_found".into(),
            })
    }

    pub async fn cleanup_expired_tokens(&self) -> Result<u64> {
        Ok(RefreshTokenRepo::delete_expired_refresh_tokens(&*self.db).await?)
    }

    pub fn verify_token(&self, token: &str) -> std::result::Result<(String, String, bool), String> {
        use jsonwebtoken::{Algorithm, DecodingKey, Validation};

        let mut validation = Validation::new(Algorithm::HS256);
        validation.set_required_spec_claims(&["sub", "email", "iat", "exp"]);
        validation.algorithms = vec![Algorithm::HS256];

        let token_data = jsonwebtoken::decode::<Claims>(
            token,
            &DecodingKey::from_secret(&self.jwt_secret),
            &validation,
        )
        .map_err(|e| match e.kind() {
            jsonwebtoken::errors::ErrorKind::ExpiredSignature => "token_expired".to_string(),
            _ => "invalid_token".to_string(),
        })?;

        Ok((
            token_data.claims.sub,
            token_data.claims.email,
            token_data.claims.is_admin,
        ))
    }

    pub fn issue_mcp_token(&self, claims: McpAccessTokenClaims) -> Result<String> {
        use jsonwebtoken::{Algorithm, EncodingKey, Header};

        let header = Header::new(Algorithm::HS256);
        jsonwebtoken::encode(
            &header,
            &claims,
            &EncodingKey::from_secret(&self.jwt_secret),
        )
        .map_err(|e| ServiceError::InvalidOperation {
            message: format!("jwt encode error: {e}"),
        })
    }

    pub fn verify_mcp_token(
        &self,
        token: &str,
    ) -> std::result::Result<McpAccessTokenClaims, String> {
        use jsonwebtoken::{Algorithm, DecodingKey, Validation};

        let mut validation = Validation::new(Algorithm::HS256);
        validation.validate_aud = false;
        validation.algorithms = vec![Algorithm::HS256];

        let token_data = jsonwebtoken::decode::<McpAccessTokenClaims>(
            token,
            &DecodingKey::from_secret(&self.jwt_secret),
            &validation,
        )
        .map_err(|e| match e.kind() {
            jsonwebtoken::errors::ErrorKind::ExpiredSignature => "token_expired".to_string(),
            _ => "invalid_token".to_string(),
        })?;

        Ok(token_data.claims)
    }

    pub async fn verify_pat(
        &self,
        raw_token: &str,
    ) -> std::result::Result<(String, String, bool), String> {
        let token_hash = hash_token(raw_token);
        let pat = PersonalAccessTokenRepo::get_pat_by_token_hash(&*self.db, &token_hash)
            .await
            .map_err(|_| "invalid_token".to_string())?
            .ok_or_else(|| "invalid_token".to_string())?;

        if let Some(ref expires_at) = pat.expires_at {
            let now = now_rfc3339();
            if *expires_at < now {
                return Err("token_expired".to_string());
            }
        }

        let user = UserRepo::get_user_by_id(&*self.db, &pat.user_id)
            .await
            .map_err(|_| "invalid_token".to_string())?
            .ok_or_else(|| "invalid_token".to_string())?;

        let now = now_rfc3339();
        let _ = PersonalAccessTokenRepo::update_last_used(&*self.db, &pat.id, &now).await;

        Ok((user.id, user.email, user.is_admin))
    }

    async fn bootstrap_first_user(&self, user_id: &str) -> Result<()> {
        let existing = SystemSettingRepo::get_setting(&*self.db, "bootstrap_completed").await?;
        if existing.is_some() {
            return Ok(());
        }

        let pool = self.db.pool();
        let mut tx = db::begin_immediate(pool).await?;

        // Double-check inside the transaction to handle concurrent first registrations.
        let already_done: bool = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM system_setting WHERE key = 'bootstrap_completed'",
        )
        .fetch_one(&mut *tx)
        .await?
            > 0;
        if already_done {
            return Ok(());
        }

        sqlx::query("UPDATE user SET is_admin = 1, updated_at = ? WHERE id = ?")
            .bind(now_rfc3339())
            .bind(user_id)
            .execute(&mut *tx)
            .await?;

        sqlx::query(
            "UPDATE agent_identity
             SET owner_id = ?, visibility = 'account'
             WHERE owner_id IS NULL",
        )
        .bind(user_id)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            "UPDATE daemon SET owner_id = ?, visibility = 'account' WHERE owner_id IS NULL",
        )
        .bind(user_id)
        .execute(&mut *tx)
        .await?;

        let orphaned_projects: Vec<(String,)> =
            sqlx::query_as("SELECT id FROM project WHERE owner_id IS NULL")
                .fetch_all(&mut *tx)
                .await?;

        let now = now_rfc3339();
        for (project_id,) in &orphaned_projects {
            sqlx::query("UPDATE project SET owner_id = ? WHERE id = ? AND owner_id IS NULL")
                .bind(user_id)
                .bind(project_id)
                .execute(&mut *tx)
                .await?;

            let member_exists: bool = sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM project_member WHERE project_id = ? AND user_id = ?",
            )
            .bind(project_id)
            .bind(user_id)
            .fetch_one(&mut *tx)
            .await?
                > 0;

            if !member_exists {
                sqlx::query(
                    "INSERT INTO project_member (id, project_id, user_id, role, created_at, updated_at) \
                     VALUES (?, ?, ?, 'owner', ?, ?)",
                )
                .bind(new_uuid_v4())
                .bind(project_id)
                .bind(user_id)
                .bind(&now)
                .bind(&now)
                .execute(&mut *tx)
                .await?;
            }
        }

        sqlx::query(
            "INSERT INTO system_setting (key, value, updated_at) VALUES ('bootstrap_completed', 'true', ?) \
             ON CONFLICT(key) DO NOTHING",
        )
        .bind(&now)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(())
    }

    async fn issue_token_pair(&self, user: &User) -> Result<TokenPair> {
        let family_id = new_uuid_v4();
        self.issue_token_pair_with_family(user, &family_id).await
    }

    async fn issue_token_pair_with_family(
        &self,
        user: &User,
        family_id: &str,
    ) -> Result<TokenPair> {
        use jsonwebtoken::{Algorithm, EncodingKey, Header};

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let claims = Claims {
            sub: user.id.clone(),
            email: user.email.clone(),
            is_admin: user.is_admin,
            iat: now,
            exp: now + ACCESS_TOKEN_EXPIRY_SECS,
        };

        let header = Header::new(Algorithm::HS256);
        let access_token = jsonwebtoken::encode(
            &header,
            &claims,
            &EncodingKey::from_secret(&self.jwt_secret),
        )
        .map_err(|e| ServiceError::InvalidOperation {
            message: format!("jwt encode error: {e}"),
        })?;

        // Generate opaque refresh token
        let raw_refresh = new_uuid_v4();
        let token_hash = hash_token(&raw_refresh);
        let now_rfc = now_rfc3339();

        let expires_at = {
            let dt =
                chrono::Utc::now() + chrono::Duration::seconds(REFRESH_TOKEN_EXPIRY_SECS as i64);
            dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        };

        let refresh_token = RefreshToken {
            id: new_uuid_v4(),
            user_id: user.id.clone(),
            token_hash,
            family_id: family_id.to_string(),
            expires_at,
            created_at: now_rfc,
        };

        RefreshTokenRepo::create_refresh_token(&*self.db, &refresh_token).await?;

        Ok(TokenPair {
            access_token,
            refresh_token: raw_refresh,
            expires_in: ACCESS_TOKEN_EXPIRY_SECS,
        })
    }
}

fn hash_token(token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    hex::encode(hasher.finalize())
}

fn is_valid_email(email: &str) -> bool {
    let parts: Vec<&str> = email.splitn(2, '@').collect();
    if parts.len() != 2 {
        return false;
    }
    let (local, domain) = (parts[0], parts[1]);
    !local.is_empty() && !domain.is_empty() && domain.contains('.')
}

#[cfg(test)]
mod tests {
    use super::*;
    use db::{create_sqlite_pool, run_migrations, AgentRepo, AgentStatus, CreateAgent, SqliteDb};

    async fn test_service() -> AuthService {
        let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
        run_migrations(&pool).await.unwrap();
        AuthService::new(Arc::new(SqliteDb::new(pool)), b"test-secret".to_vec(), 4)
    }

    async fn seed_orphan_agent(db: &SqliteDb, id: &str, name: &str, is_default: bool) {
        let now = now_rfc3339();
        AgentRepo::create(
            db,
            CreateAgent {
                id: id.to_owned(),
                name: name.to_owned(),
                description: None,
                executor_type: "shell".to_owned(),
                model: None,
                reasoning_effort: None,
                permission_policy: None,
                prompt_template: None,
                capabilities_json: "[]".to_owned(),
                config_json: "{}".to_owned(),
                credential_ref: None,
                daemon_id: None,
                max_concurrent_tasks: 1,
                heartbeat_interval_seconds: 60,
                max_missed_heartbeats: 3,
                status: AgentStatus::Idle,
                last_heartbeat_at: None,
                is_default,
                paused: false,
                owner_id: None,
                visibility: "global".to_owned(),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("orphan agent inserts");
    }

    #[tokio::test]
    async fn register_success() {
        let svc = test_service().await;
        let user = svc
            .register("Test@Example.COM", "password123", Some("Alice"))
            .await
            .expect("register succeeds");
        assert_eq!(
            user.email, "test@example.com",
            "email normalized to lowercase"
        );
        assert_eq!(user.display_name.as_deref(), Some("Alice"));
    }

    #[tokio::test]
    async fn bootstrap_first_user_is_admin_and_second_is_not() {
        let svc = test_service().await;

        let first_registered = svc
            .register("first-admin@test.com", "password123", None)
            .await
            .expect("first user registers");
        let second_registered = svc
            .register("second-user@test.com", "password123", None)
            .await
            .expect("second user registers");

        let first = svc
            .get_user(&first_registered.id)
            .await
            .expect("first user loads");
        let second = svc
            .get_user(&second_registered.id)
            .await
            .expect("second user loads");

        assert!(first.is_admin, "bootstrap should grant first user admin");
        assert!(
            !second.is_admin,
            "bootstrap should not grant later users admin"
        );
    }

    #[tokio::test]
    async fn register_invalid_email() {
        let svc = test_service().await;
        let err = svc.register("not-an-email", "password123", None).await;
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("invalid_email"));
    }

    #[tokio::test]
    async fn register_weak_password() {
        let svc = test_service().await;
        let err = svc.register("a@b.com", "short", None).await;
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("password_too_weak"));
    }

    #[tokio::test]
    async fn register_duplicate_email() {
        let svc = test_service().await;
        svc.register("dup@test.com", "password123", None)
            .await
            .expect("first registration");
        let err = svc.register("dup@test.com", "password456", None).await;
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("email_exists"));
    }

    #[tokio::test]
    async fn login_success_returns_token_pair() {
        let svc = test_service().await;
        svc.register("login@test.com", "password123", None)
            .await
            .expect("register");
        let pair = svc
            .login("login@test.com", "password123")
            .await
            .expect("login");
        assert!(!pair.access_token.is_empty());
        assert!(!pair.refresh_token.is_empty());
        assert_eq!(pair.expires_in, ACCESS_TOKEN_EXPIRY_SECS);
    }

    #[tokio::test]
    async fn login_wrong_password() {
        let svc = test_service().await;
        svc.register("wp@test.com", "password123", None)
            .await
            .expect("register");
        let err = svc.login("wp@test.com", "wrongpassword").await;
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("invalid_credentials"));
    }

    #[tokio::test]
    async fn login_nonexistent_user_returns_invalid_credentials() {
        let svc = test_service().await;
        let err = svc.login("ghost@test.com", "password123").await;
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("invalid_credentials"));
    }

    #[tokio::test]
    async fn refresh_rotates_token() {
        let svc = test_service().await;
        svc.register("refresh@test.com", "password123", None)
            .await
            .expect("register");
        let pair1 = svc
            .login("refresh@test.com", "password123")
            .await
            .expect("login");

        let pair2 = svc
            .refresh(&pair1.refresh_token)
            .await
            .expect("refresh succeeds");
        assert_ne!(pair2.refresh_token, pair1.refresh_token, "new token issued");

        let err = svc.refresh(&pair1.refresh_token).await;
        assert!(
            err.is_err(),
            "old refresh token must be invalid after rotation"
        );
    }

    #[tokio::test]
    async fn logout_invalidates_refresh_token() {
        let svc = test_service().await;
        svc.register("logout@test.com", "password123", None)
            .await
            .expect("register");
        let pair = svc
            .login("logout@test.com", "password123")
            .await
            .expect("login");

        svc.logout(&pair.refresh_token).await.expect("logout");

        let err = svc.refresh(&pair.refresh_token).await;
        assert!(err.is_err(), "refresh after logout must fail");
    }

    #[tokio::test]
    async fn verify_token_valid() {
        let svc = test_service().await;
        svc.register("vt@test.com", "password123", None)
            .await
            .expect("register");
        let pair = svc
            .login("vt@test.com", "password123")
            .await
            .expect("login");
        let (user_id, email, _is_admin) = svc
            .verify_token(&pair.access_token)
            .expect("verify succeeds");
        assert!(!user_id.is_empty());
        assert_eq!(email, "vt@test.com");
    }

    #[tokio::test]
    async fn verify_token_expired() {
        let svc = test_service().await;
        use jsonwebtoken::{Algorithm, EncodingKey, Header};
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let expired = jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &serde_json::json!({
                "sub": "uid", "email": "e@e.com",
                "iat": now - 3600, "exp": now - 1800,
            }),
            &EncodingKey::from_secret(b"test-secret"),
        )
        .unwrap();
        let err = svc.verify_token(&expired).unwrap_err();
        assert_eq!(err, "token_expired");
    }

    #[tokio::test]
    async fn verify_token_wrong_secret() {
        let svc = test_service().await;
        use jsonwebtoken::{Algorithm, EncodingKey, Header};
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let bad = jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &serde_json::json!({
                "sub": "uid", "email": "e@e.com",
                "iat": now, "exp": now + 900,
            }),
            &EncodingKey::from_secret(b"wrong-secret"),
        )
        .unwrap();
        assert!(svc.verify_token(&bad).is_err());
    }

    #[tokio::test]
    async fn verify_token_algorithm_confusion() {
        let svc = test_service().await;
        use jsonwebtoken::{Algorithm, EncodingKey, Header};
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let hs512_token = jsonwebtoken::encode(
            &Header::new(Algorithm::HS512),
            &serde_json::json!({
                "sub": "uid", "email": "e@e.com",
                "iat": now, "exp": now + 900,
            }),
            &EncodingKey::from_secret(b"test-secret"),
        )
        .unwrap();
        assert!(
            svc.verify_token(&hs512_token).is_err(),
            "HS512 token must be rejected when only HS256 is accepted"
        );
    }

    #[tokio::test]
    async fn bootstrap_first_user_claims_orphaned_resources() {
        let svc = test_service().await;
        let db = &svc.db;
        let now = now_rfc3339();

        seed_orphan_agent(db, "a1", "agent1", true).await;

        sqlx::query("INSERT INTO daemon (id, machine_id, hostname, os, arch, labels_json, status, detected_clis_json, visibility, version, created_at, updated_at) VALUES ('d1', 'm1', 'h1', 'linux', 'x86_64', '{}', 'online', '[]', 'global', 1, ?, ?)")
            .bind(&now).bind(&now)
            .execute(db.pool()).await.unwrap();

        sqlx::query("INSERT INTO project (id, name, settings, workflow_definition, created_at, updated_at) VALUES ('p1', 'proj1', '{}', '{}', ?, ?)")
            .bind(&now).bind(&now)
            .execute(db.pool()).await.unwrap();

        let user = svc
            .register("admin@test.com", "password123", None)
            .await
            .expect("register succeeds");

        let (agent_owner, agent_visibility): (Option<String>, String) =
            sqlx::query_as("SELECT owner_id, visibility FROM agent_identity WHERE id = 'a1'")
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(agent_owner, Some(user.id.clone()));
        assert_eq!(agent_visibility, "account");

        let (daemon_owner, daemon_visibility): (Option<String>, String) =
            sqlx::query_as("SELECT owner_id, visibility FROM daemon WHERE id = 'd1'")
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(daemon_owner, Some(user.id.clone()));
        assert_eq!(daemon_visibility, "account");

        let project_owner: Option<String> =
            sqlx::query_scalar("SELECT owner_id FROM project WHERE id = 'p1'")
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(project_owner, Some(user.id.clone()));

        let member_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM project_member WHERE project_id = 'p1' AND user_id = ?",
        )
        .bind(&user.id)
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(member_count, 1);
    }

    #[tokio::test]
    async fn bootstrap_second_user_does_not_claim() {
        let svc = test_service().await;
        let db = &svc.db;

        let _first = svc
            .register("first@test.com", "password123", None)
            .await
            .expect("first user");

        seed_orphan_agent(db, "a2", "agent2", false).await;

        let _second = svc
            .register("second@test.com", "password456", None)
            .await
            .expect("second user");

        let agent_owner: Option<String> =
            sqlx::query_scalar("SELECT owner_id FROM agent_identity WHERE id = 'a2'")
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(
            agent_owner, None,
            "new orphaned agent should NOT be claimed"
        );
    }
}
