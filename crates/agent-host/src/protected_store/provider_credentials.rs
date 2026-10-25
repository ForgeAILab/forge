//! Protected provider credentials, OAuth leases, refresh, and revocation.

use super::*;

const OAUTH_REFRESH_SKEW_MS: u64 = 30_000;

const MAX_PROVIDER_CREDENTIAL_RESPONSE_BYTES: usize = 1024 * 1024;

/// Logs a swallowed sqlx error before mapping it to the given fallback.
/// Mirrors the diagnosability fix in `lcm.rs`: a bare `.map_err(|_| ...)`
/// discarded the cause, making a SQLite busy/busy-snapshot failure
/// indistinguishable from any other store error in the logs.
fn log_store_error<E>(error: impl std::fmt::Display, fallback: E) -> E {
    tracing::warn!(error = %error, "protected store operation failed");
    fallback
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialRevocationOutcome {
    NotSupported,
    Succeeded,
    Failed,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct OAuthCredentialBundle {
    pub schema_version: u32,
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at_ms: u64,
    pub token_endpoint: String,
    pub client_id: String,
    #[serde(default)]
    pub client_secret: Option<String>,
    #[serde(default)]
    pub scopes: Vec<String>,
    #[serde(default)]
    pub provider_account_id: Option<String>,
}

pub struct CreateOAuthCredential<'a> {
    pub id: &'a str,
    pub owner_user_id: &'a str,
    pub provider: &'a str,
    pub label: &'a str,
    pub bundle: &'a OAuthCredentialBundle,
    pub metadata_json: &'a str,
    pub now: &'a str,
}

impl fmt::Debug for OAuthCredentialBundle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OAuthCredentialBundle")
            .field("schema_version", &self.schema_version)
            .field("expires_at_ms", &self.expires_at_ms)
            .field("token_endpoint", &self.token_endpoint)
            .field("scopes", &self.scopes)
            .field("provider_account_id", &self.provider_account_id)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
struct StoredCredential {
    provider: String,
    method: String,
    version: i64,
    plaintext: String,
}

impl SqliteProtectedRuntimeStore {
    pub async fn create_credential(
        &self,
        id: &str,
        owner_user_id: &str,
        provider: &str,
        label: &str,
        secret: Secret,
        now: &str,
    ) -> Result<CredentialHandle, crate::AgentHostError> {
        let (ciphertext, nonce) = self
            .seal(secret.expose().as_bytes())
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        let mut transaction = {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.BEGIN_IMMEDIATE", 0);
            db::begin_immediate(self.db.pool()).await
        }
        .map_err(|error| log_store_error(error, crate::AgentHostError::ProtectedPersistence))?;
        {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.INSERT", 0);
            sqlx::query(
                "INSERT INTO credential_handle (
                id, owner_user_id, provider, label, status,
                credential_method, metadata_json, version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, 'configured', 'api_key', '{}', 1, ?, ?)",
            )
            .bind(id)
            .bind(owner_user_id)
            .bind(provider)
            .bind(label)
            .bind(now)
            .bind(now)
            .execute(&mut *transaction)
            .await
        }
        .map_err(|error| log_store_error(error, crate::AgentHostError::ProtectedPersistence))?;
        {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.INSERT", 0);
            sqlx::query(
                "INSERT INTO protected_credential_secret (
                handle_id, ciphertext, nonce, key_revision, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(id)
            .bind(ciphertext)
            .bind(nonce)
            .bind(self.key_revision)
            .bind(now)
            .bind(now)
            .execute(&mut *transaction)
            .await
        }
        .map_err(|error| log_store_error(error, crate::AgentHostError::ProtectedPersistence))?;
        transaction
            .commit()
            .await
            .map_err(|error| log_store_error(error, crate::AgentHostError::ProtectedPersistence))?;
        Ok(CredentialHandle {
            id: id.to_owned(),
            owner_user_id: owner_user_id.to_owned(),
            provider: provider.to_owned(),
            label: label.to_owned(),
            status: "configured".to_owned(),
            enabled: true,
            credential_method: "api_key".to_owned(),
            metadata_json: "{}".to_owned(),
            version: 1,
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        })
    }

    pub async fn load_credential(
        &self,
        handle_id: &str,
        owner_user_id: &str,
    ) -> Result<Secret, crate::AgentHostError> {
        let row = {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.SELECT", 0);
            sqlx::query(
                "SELECT secret.ciphertext, secret.nonce
             FROM protected_credential_secret AS secret
             JOIN credential_handle AS handle ON handle.id = secret.handle_id
             WHERE handle.id = ? AND handle.owner_user_id = ? AND handle.status = 'configured'",
            )
            .bind(handle_id)
            .bind(owner_user_id)
            .fetch_optional(self.db.pool())
            .await
        }
        .map_err(|_| crate::AgentHostError::ProtectedPersistence)?
        .ok_or(crate::AgentHostError::CredentialNotFound)?;
        let ciphertext: Vec<u8> = row
            .try_get("ciphertext")
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        let nonce: Vec<u8> = row
            .try_get("nonce")
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        let plaintext = self
            .open(&ciphertext, &nonce)
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        let value = String::from_utf8(plaintext)
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        Ok(Secret::new(value))
    }

    pub async fn create_oauth_credential(
        &self,
        input: CreateOAuthCredential<'_>,
    ) -> Result<CredentialHandle, crate::AgentHostError> {
        let plaintext = serde_json::to_vec(input.bundle)
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        let (ciphertext, nonce) = self
            .seal(&plaintext)
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        let mut transaction = {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.BEGIN_IMMEDIATE", 0);
            db::begin_immediate(self.db.pool()).await
        }
        .map_err(|error| log_store_error(error, crate::AgentHostError::ProtectedPersistence))?;
        {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.INSERT", 0);
            sqlx::query(
                "INSERT INTO credential_handle (
                id, owner_user_id, provider, label, status,
                credential_method, metadata_json, version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, 'configured', 'oauth_bundle', ?, 1, ?, ?)",
            )
            .bind(input.id)
            .bind(input.owner_user_id)
            .bind(input.provider)
            .bind(input.label)
            .bind(input.metadata_json)
            .bind(input.now)
            .bind(input.now)
            .execute(&mut *transaction)
            .await
        }
        .map_err(|error| log_store_error(error, crate::AgentHostError::ProtectedPersistence))?;
        {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.INSERT", 0);
            sqlx::query(
                "INSERT INTO protected_credential_secret (
                handle_id, ciphertext, nonce, key_revision, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(input.id)
            .bind(ciphertext)
            .bind(nonce)
            .bind(self.key_revision)
            .bind(input.now)
            .bind(input.now)
            .execute(&mut *transaction)
            .await
        }
        .map_err(|error| log_store_error(error, crate::AgentHostError::ProtectedPersistence))?;
        transaction
            .commit()
            .await
            .map_err(|error| log_store_error(error, crate::AgentHostError::ProtectedPersistence))?;
        Ok(CredentialHandle {
            id: input.id.to_owned(),
            owner_user_id: input.owner_user_id.to_owned(),
            provider: input.provider.to_owned(),
            label: input.label.to_owned(),
            status: "configured".to_owned(),
            enabled: true,
            credential_method: "oauth_bundle".to_owned(),
            metadata_json: input.metadata_json.to_owned(),
            version: 1,
            created_at: input.now.to_owned(),
            updated_at: input.now.to_owned(),
        })
    }

    pub fn credential_source(
        &self,
        owner_user_id: impl Into<String>,
        handle_id: impl Into<String>,
    ) -> Arc<dyn ProviderCredentialSource> {
        let handle_id = handle_id.into();
        Arc::new(SqliteProviderCredentialSource {
            store: self.clone(),
            owner_user_id: owner_user_id.into(),
            refresh_lock: self.refresh_lock(&handle_id),
            handle_id,
            client: reqwest::Client::new(),
        })
    }

    pub async fn acquire_provider_credential(
        &self,
        owner_user_id: &str,
        handle_id: &str,
        minimum_validity_ms: u64,
    ) -> Result<Secret, crate::AgentHostError> {
        let source = SqliteProviderCredentialSource {
            store: self.clone(),
            owner_user_id: owner_user_id.to_owned(),
            handle_id: handle_id.to_owned(),
            refresh_lock: self.refresh_lock(handle_id),
            client: reqwest::Client::new(),
        };
        let target = ProviderCredentialTarget::new(handle_id.to_owned())
            .map_err(|_| crate::AgentHostError::CredentialNotFound)?;
        let lease = source
            .acquire(
                &target,
                minimum_validity_ms,
                &agent_runtime::core::cancel::Cancellation::new(),
                Deadline::after(&SystemClock, 8_000),
            )
            .await
            .map_err(|_| crate::AgentHostError::CredentialNotFound)?;
        Ok(lease.secret().clone())
    }

    pub async fn seal_provider_authorization_state(
        &self,
        operation_id: &str,
        plaintext: &[u8],
        now: &str,
    ) -> Result<(), crate::AgentHostError> {
        let (ciphertext, nonce) = self
            .seal(plaintext)
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.INSERT", 0);
            sqlx::query(
                "INSERT INTO protected_provider_authorization_state (
                operation_id, ciphertext, nonce, key_revision, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?)
             ON CONFLICT(operation_id) DO UPDATE SET
                ciphertext = excluded.ciphertext, nonce = excluded.nonce,
                key_revision = excluded.key_revision, updated_at = excluded.updated_at",
            )
            .bind(operation_id)
            .bind(ciphertext)
            .bind(nonce)
            .bind(self.key_revision)
            .bind(now)
            .bind(now)
            .execute(self.db.pool())
            .await
        }
        .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        Ok(())
    }

    pub async fn open_provider_authorization_state(
        &self,
        operation_id: &str,
    ) -> Result<Vec<u8>, crate::AgentHostError> {
        let row = {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.SELECT", 0);
            sqlx::query(
                "SELECT ciphertext, nonce FROM protected_provider_authorization_state
             WHERE operation_id = ?",
            )
            .bind(operation_id)
            .fetch_optional(self.db.pool())
            .await
        }
        .map_err(|_| crate::AgentHostError::ProtectedPersistence)?
        .ok_or(crate::AgentHostError::ProtectedPersistence)?;
        let ciphertext: Vec<u8> = row
            .try_get("ciphertext")
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        let nonce: Vec<u8> = row
            .try_get("nonce")
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        self.open(&ciphertext, &nonce)
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)
    }

    pub async fn delete_provider_authorization_state(
        &self,
        operation_id: &str,
    ) -> Result<(), crate::AgentHostError> {
        {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.DELETE", 0);
            sqlx::query("DELETE FROM protected_provider_authorization_state WHERE operation_id = ?")
                .bind(operation_id)
                .execute(self.db.pool())
                .await
        }
        .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        Ok(())
    }

    async fn load_stored_credential(
        &self,
        handle_id: &str,
        owner_user_id: &str,
    ) -> Result<StoredCredential, ProviderCredentialError> {
        let row = {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.SELECT", 0);
            sqlx::query(
                "SELECT handle.provider, handle.credential_method, handle.version,
                    secret.ciphertext, secret.nonce
             FROM protected_credential_secret AS secret
             JOIN credential_handle AS handle ON handle.id = secret.handle_id
             WHERE handle.id = ? AND handle.owner_user_id = ?
               AND handle.status = 'configured'",
            )
            .bind(handle_id)
            .bind(owner_user_id)
            .fetch_optional(self.db.pool())
            .await
        }
        .map_err(|_| ProviderCredentialError::RefreshFailed)?
        .ok_or(ProviderCredentialError::Unavailable)?;
        let ciphertext: Vec<u8> = row
            .try_get("ciphertext")
            .map_err(|_| ProviderCredentialError::RefreshFailed)?;
        let nonce: Vec<u8> = row
            .try_get("nonce")
            .map_err(|_| ProviderCredentialError::RefreshFailed)?;
        let plaintext = self
            .open(&ciphertext, &nonce)
            .map_err(|_| ProviderCredentialError::RefreshFailed)?;
        Ok(StoredCredential {
            provider: row
                .try_get("provider")
                .map_err(|_| ProviderCredentialError::RefreshFailed)?,
            method: row
                .try_get("credential_method")
                .map_err(|_| ProviderCredentialError::RefreshFailed)?,
            version: row
                .try_get("version")
                .map_err(|_| ProviderCredentialError::RefreshFailed)?,
            plaintext: String::from_utf8(plaintext)
                .map_err(|_| ProviderCredentialError::RefreshFailed)?,
        })
    }

    fn refresh_lock(&self, handle_id: &str) -> Arc<Mutex<()>> {
        let mut locks = self
            .refresh_locks
            .lock()
            .expect("provider credential refresh lock registry poisoned");
        Arc::clone(
            locks
                .entry(handle_id.to_owned())
                .or_insert_with(|| Arc::new(Mutex::new(()))),
        )
    }

    async fn mark_credential_invalid(
        &self,
        handle_id: &str,
        owner_user_id: &str,
        expected_version: i64,
    ) {
        let _ = {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.UPDATE", 0);
            sqlx::query(
                "UPDATE credential_handle
             SET status = 'invalid', version = version + 1, updated_at = ?
             WHERE id = ? AND owner_user_id = ? AND version = ?",
            )
            .bind(db::now_rfc3339())
            .bind(handle_id)
            .bind(owner_user_id)
            .bind(expected_version)
            .execute(self.db.pool())
            .await
        };
    }

    async fn rotate_oauth_bundle(
        &self,
        handle_id: &str,
        owner_user_id: &str,
        expected_version: i64,
        bundle: &OAuthCredentialBundle,
    ) -> Result<i64, ProviderCredentialError> {
        let plaintext =
            serde_json::to_vec(bundle).map_err(|_| ProviderCredentialError::RefreshFailed)?;
        let (ciphertext, nonce) = self
            .seal(&plaintext)
            .map_err(|_| ProviderCredentialError::RefreshFailed)?;
        let now = db::now_rfc3339();
        let mut transaction = {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.BEGIN_IMMEDIATE", 0);
            db::begin_immediate(self.db.pool()).await
        }
        .map_err(|error| log_store_error(error, ProviderCredentialError::RefreshFailed))?;
        let result = {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.UPDATE", 0);
            sqlx::query(
                "UPDATE credential_handle
             SET version = version + 1, status = 'configured', updated_at = ?
             WHERE id = ? AND owner_user_id = ? AND version = ?
               AND credential_method = 'oauth_bundle' AND status = 'configured'",
            )
            .bind(&now)
            .bind(handle_id)
            .bind(owner_user_id)
            .bind(expected_version)
            .execute(&mut *transaction)
            .await
        }
        .map_err(|error| log_store_error(error, ProviderCredentialError::RefreshFailed))?;
        if result.rows_affected() == 0 {
            transaction
                .rollback()
                .await
                .map_err(|error| log_store_error(error, ProviderCredentialError::RefreshFailed))?;
            return Err(ProviderCredentialError::RefreshFailed);
        }
        {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.UPDATE", 0);
            sqlx::query(
                "UPDATE protected_credential_secret
             SET ciphertext = ?, nonce = ?, key_revision = ?, updated_at = ?
             WHERE handle_id = ?",
            )
            .bind(ciphertext)
            .bind(nonce)
            .bind(self.key_revision)
            .bind(&now)
            .bind(handle_id)
            .execute(&mut *transaction)
            .await
        }
        .map_err(|error| log_store_error(error, ProviderCredentialError::RefreshFailed))?;
        transaction
            .commit()
            .await
            .map_err(|error| log_store_error(error, ProviderCredentialError::RefreshFailed))?;
        Ok(expected_version + 1)
    }

    pub async fn revoke_credential(
        &self,
        handle_id: &str,
        owner_user_id: &str,
        now: &str,
    ) -> Result<CredentialRevocationOutcome, crate::AgentHostError> {
        let remote_bundle = self
            .remote_revocation_bundle(handle_id, owner_user_id)
            .await;
        let mut transaction = {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.BEGIN_IMMEDIATE", 0);
            db::begin_immediate(self.db.pool()).await
        }
        .map_err(|error| log_store_error(error, crate::AgentHostError::ProtectedPersistence))?;
        let result = {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.UPDATE", 0);
            sqlx::query(
                "UPDATE credential_handle
             SET status = 'revoked', version = version + 1, updated_at = ?
             WHERE id = ? AND owner_user_id = ?",
            )
            .bind(now)
            .bind(handle_id)
            .bind(owner_user_id)
            .execute(&mut *transaction)
            .await
        }
        .map_err(|error| log_store_error(error, crate::AgentHostError::ProtectedPersistence))?;
        if result.rows_affected() == 0 {
            return Err(crate::AgentHostError::CredentialNotFound);
        }
        retire_provider_pricing_subject(&mut transaction, owner_user_id, handle_id, now).await?;
        {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.DELETE", 0);
            sqlx::query("DELETE FROM protected_credential_secret WHERE handle_id = ?")
                .bind(handle_id)
                .execute(&mut *transaction)
                .await
        }
        .map_err(|error| log_store_error(error, crate::AgentHostError::ProtectedPersistence))?;
        mark_credential_dependents_unavailable(&mut transaction, handle_id, now).await?;
        transaction
            .commit()
            .await
            .map_err(|error| log_store_error(error, crate::AgentHostError::ProtectedPersistence))?;
        Ok(self.best_effort_remote_revocation(remote_bundle).await)
    }

    pub async fn revoke_credential_at_version(
        &self,
        handle_id: &str,
        owner_user_id: &str,
        expected_version: i64,
        now: &str,
    ) -> Result<CredentialRevocationOutcome, crate::AgentHostError> {
        let remote_bundle = self
            .remote_revocation_bundle(handle_id, owner_user_id)
            .await;
        let mut transaction = {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.BEGIN_IMMEDIATE", 0);
            db::begin_immediate(self.db.pool()).await
        }
        .map_err(|error| log_store_error(error, crate::AgentHostError::ProtectedPersistence))?;
        let result = {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.UPDATE", 0);
            sqlx::query(
                "UPDATE credential_handle
             SET status = 'revoked', version = version + 1, updated_at = ?
             WHERE id = ? AND owner_user_id = ? AND version = ?",
            )
            .bind(now)
            .bind(handle_id)
            .bind(owner_user_id)
            .bind(expected_version)
            .execute(&mut *transaction)
            .await
        }
        .map_err(|error| log_store_error(error, crate::AgentHostError::ProtectedPersistence))?;
        if result.rows_affected() == 0 {
            return Err(crate::AgentHostError::VersionConflict);
        }
        retire_provider_pricing_subject(&mut transaction, owner_user_id, handle_id, now).await?;
        {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.DELETE", 0);
            sqlx::query("DELETE FROM protected_credential_secret WHERE handle_id = ?")
                .bind(handle_id)
                .execute(&mut *transaction)
                .await
        }
        .map_err(|error| log_store_error(error, crate::AgentHostError::ProtectedPersistence))?;
        mark_credential_dependents_unavailable(&mut transaction, handle_id, now).await?;
        transaction
            .commit()
            .await
            .map_err(|error| log_store_error(error, crate::AgentHostError::ProtectedPersistence))?;
        Ok(self.best_effort_remote_revocation(remote_bundle).await)
    }

    async fn remote_revocation_bundle(
        &self,
        handle_id: &str,
        owner_user_id: &str,
    ) -> Option<OAuthCredentialBundle> {
        let Ok(stored) = self.load_stored_credential(handle_id, owner_user_id).await else {
            return None;
        };
        if stored.method != "oauth_bundle" || stored.provider != "gemini" {
            return None;
        }
        serde_json::from_str::<OAuthCredentialBundle>(&stored.plaintext).ok()
    }

    async fn best_effort_remote_revocation(
        &self,
        bundle: Option<OAuthCredentialBundle>,
    ) -> CredentialRevocationOutcome {
        let Some(bundle) = bundle else {
            return CredentialRevocationOutcome::NotSupported;
        };
        match self
            .revocation_client
            .post(self.gemini_revocation_endpoint.as_ref())
            .form(&[("token", bundle.refresh_token)])
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => {
                CredentialRevocationOutcome::Succeeded
            }
            _ => CredentialRevocationOutcome::Failed,
        }
    }
}

/// Revoking a provider entry is a terminal source lifecycle transition. Keep
/// the matching pricing subject terminal in the same local transaction while
/// leaving its immutable revisions, bindings, and usage provenance intact.
/// A provider can be disconnected before it has ever been priced, so the
/// update intentionally treats a missing (or already-retired) subject as a
/// no-op.
async fn retire_provider_pricing_subject(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    owner_user_id: &str,
    provider_entry_id: &str,
    now: &str,
) -> Result<(), crate::AgentHostError> {
    {
        #[cfg(feature = "test-support")]
        let _sql_profile = crate::session_journal::profile::timer("sql.UPDATE", 0);
        sqlx::query(
            "UPDATE pricing_subject
         SET state = 'retired', version = version + 1, updated_at = ?
         WHERE owner_user_id = ? AND subject_kind = 'provider_entry'
           AND provider_entry_id = ? AND state = 'active'",
        )
        .bind(now)
        .bind(owner_user_id)
        .bind(provider_entry_id)
        .execute(&mut **transaction)
        .await
    }
    .map_err(|error| log_store_error(error, crate::AgentHostError::ProtectedPersistence))?;
    Ok(())
}

async fn mark_credential_dependents_unavailable(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    handle_id: &str,
    now: &str,
) -> Result<(), crate::AgentHostError> {
    {
        #[cfg(feature = "test-support")]
        let _sql_profile = crate::session_journal::profile::timer("sql.UPDATE", 0);
        sqlx::query(
            "UPDATE agent_connection_health
         SET status = 'unavailable', error_code = 'credential_revoked', updated_at = ?
         WHERE profile_id IN (
             SELECT id FROM agent_profile WHERE credential_ref = ?
         )",
        )
        .bind(now)
        .bind(handle_id)
        .execute(&mut **transaction)
        .await
    }
    .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
    {
        #[cfg(feature = "test-support")]
        let _sql_profile = crate::session_journal::profile::timer("sql.UPDATE", 0);
        sqlx::query(
            "UPDATE agent_session
         SET status = 'degraded', connection_status = 'unavailable',
             version = version + 1, updated_at = ?
         WHERE profile_id IN (
             SELECT id FROM agent_profile WHERE credential_ref = ?
         ) AND status NOT IN ('replaced', 'terminated')",
        )
        .bind(now)
        .bind(handle_id)
        .execute(&mut **transaction)
        .await
    }
    .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
    Ok(())
}

#[derive(Deserialize)]
struct RefreshTokenResponse {
    access_token: String,
    #[serde(default)]
    refresh_token: Option<String>,
    expires_in: u64,
    #[serde(default)]
    scope: Option<String>,
}

impl SqliteProviderCredentialSource {
    fn ensure_active(
        cancel: &agent_runtime::core::cancel::Cancellation,
        deadline: Deadline,
    ) -> Result<(), ProviderCredentialError> {
        if cancel.is_cancelled() {
            return Err(ProviderCredentialError::Cancelled);
        }
        if deadline.is_expired(&SystemClock) {
            return Err(ProviderCredentialError::Timeout);
        }
        Ok(())
    }

    async fn refresh(
        &self,
        cancel: &agent_runtime::core::cancel::Cancellation,
        deadline: Deadline,
    ) -> Result<(OAuthCredentialBundle, i64), ProviderCredentialError> {
        Self::ensure_active(cancel, deadline)?;
        let current = self
            .store
            .load_stored_credential(&self.handle_id, &self.owner_user_id)
            .await?;
        let mut bundle: OAuthCredentialBundle = serde_json::from_str(&current.plaintext)
            .map_err(|_| ProviderCredentialError::RefreshFailed)?;
        let now_ms = SystemClock.now().as_millis();
        if bundle.expires_at_ms > now_ms.saturating_add(OAUTH_REFRESH_SKEW_MS) {
            return Ok((bundle, current.version));
        }
        let mut form = vec![
            ("grant_type", "refresh_token".to_owned()),
            ("refresh_token", bundle.refresh_token.clone()),
            ("client_id", bundle.client_id.clone()),
        ];
        if let Some(client_secret) = bundle.client_secret.clone() {
            form.push(("client_secret", client_secret));
        }
        let request = self.client.post(&bundle.token_endpoint).form(&form);
        let remaining = deadline
            .remaining_millis(&SystemClock)
            .unwrap_or(30_000)
            .max(1);
        let response = tokio::select! {
            _ = cancel.cancelled() => return Err(ProviderCredentialError::Cancelled),
            result = tokio::time::timeout(
                std::time::Duration::from_millis(remaining),
                request.send(),
            ) => result
                .map_err(|_| ProviderCredentialError::Timeout)?
                .map_err(|_| ProviderCredentialError::RefreshFailed)?,
        };
        if !response.status().is_success() {
            if matches!(response.status().as_u16(), 400 | 401 | 403) {
                self.store
                    .mark_credential_invalid(&self.handle_id, &self.owner_user_id, current.version)
                    .await;
            }
            return Err(ProviderCredentialError::RefreshFailed);
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_PROVIDER_CREDENTIAL_RESPONSE_BYTES as u64)
        {
            return Err(ProviderCredentialError::RefreshFailed);
        }
        let body = response
            .bytes()
            .await
            .map_err(|_| ProviderCredentialError::RefreshFailed)?;
        if body.len() > MAX_PROVIDER_CREDENTIAL_RESPONSE_BYTES {
            return Err(ProviderCredentialError::RefreshFailed);
        }
        let refreshed: RefreshTokenResponse =
            serde_json::from_slice(&body).map_err(|_| ProviderCredentialError::RefreshFailed)?;
        if refreshed.access_token.trim().is_empty() || refreshed.expires_in == 0 {
            return Err(ProviderCredentialError::RefreshFailed);
        }
        bundle.access_token = refreshed.access_token;
        if let Some(refresh_token) = refreshed.refresh_token {
            bundle.refresh_token = refresh_token;
        }
        bundle.expires_at_ms = SystemClock
            .now()
            .as_millis()
            .saturating_add(refreshed.expires_in.saturating_mul(1000));
        if let Some(scope) = refreshed.scope {
            bundle.scopes = scope.split_whitespace().map(str::to_owned).collect();
        }
        let version = self
            .store
            .rotate_oauth_bundle(
                &self.handle_id,
                &self.owner_user_id,
                current.version,
                &bundle,
            )
            .await?;
        Ok((bundle, version))
    }

    fn lease(
        stored: StoredCredential,
        minimum_validity_ms: u64,
    ) -> Result<ProviderCredentialLease, ProviderCredentialError> {
        let revision = ProviderCredentialRevision::new(format!("credential-v{}", stored.version))?;
        if stored.method == "api_key" {
            return Ok(ProviderCredentialLease::non_expiring(
                Secret::new(stored.plaintext),
                revision,
            ));
        }
        let bundle: OAuthCredentialBundle = serde_json::from_str(&stored.plaintext)
            .map_err(|_| ProviderCredentialError::RefreshFailed)?;
        if bundle.expires_at_ms
            <= SystemClock
                .now()
                .as_millis()
                .saturating_add(minimum_validity_ms)
        {
            return Err(ProviderCredentialError::InvalidLease);
        }
        let lease = ProviderCredentialLease::expiring(
            Secret::new(bundle.access_token),
            Timestamp(bundle.expires_at_ms),
            revision,
        );
        Ok(match bundle.provider_account_id {
            Some(account) => lease.with_account(account),
            None => lease,
        })
    }
}

#[async_trait]
impl ProviderCredentialSource for SqliteProviderCredentialSource {
    async fn acquire(
        &self,
        target: &ProviderCredentialTarget,
        minimum_validity_ms: u64,
        cancel: &agent_runtime::core::cancel::Cancellation,
        deadline: Deadline,
    ) -> Result<ProviderCredentialLease, ProviderCredentialError> {
        Self::ensure_active(cancel, deadline)?;
        if target.as_str() != self.handle_id {
            return Err(ProviderCredentialError::Unavailable);
        }
        let stored = self
            .store
            .load_stored_credential(&self.handle_id, &self.owner_user_id)
            .await?;
        if stored.method == "api_key" {
            return Self::lease(stored, minimum_validity_ms);
        }
        let bundle: OAuthCredentialBundle = serde_json::from_str(&stored.plaintext)
            .map_err(|_| ProviderCredentialError::RefreshFailed)?;
        let now_ms = SystemClock.now().as_millis();
        if bundle.expires_at_ms > now_ms.saturating_add(minimum_validity_ms) {
            return Self::lease(stored, minimum_validity_ms);
        }
        let _guard = self.refresh_lock.lock().await;
        let (bundle, version) = self.refresh(cancel, deadline).await?;
        Self::lease(
            StoredCredential {
                provider: stored.provider,
                method: "oauth_bundle".to_owned(),
                version,
                plaintext: serde_json::to_string(&bundle)
                    .map_err(|_| ProviderCredentialError::RefreshFailed)?,
            },
            minimum_validity_ms,
        )
    }

    async fn invalidate(
        &self,
        target: &ProviderCredentialTarget,
        rejected_revision: &ProviderCredentialRevision,
        _rejection: ProviderAuthRejection,
        cancel: &agent_runtime::core::cancel::Cancellation,
        deadline: Deadline,
    ) -> Result<CredentialInvalidation, ProviderCredentialError> {
        Self::ensure_active(cancel, deadline)?;
        if target.as_str() != self.handle_id {
            return Err(ProviderCredentialError::Unavailable);
        }
        let stored = self
            .store
            .load_stored_credential(&self.handle_id, &self.owner_user_id)
            .await?;
        let current = ProviderCredentialRevision::new(format!("credential-v{}", stored.version))?;
        if &current != rejected_revision {
            return Ok(CredentialInvalidation::StaleRevision);
        }
        if stored.method == "api_key" {
            return Ok(CredentialInvalidation::NoReplacement);
        }
        let _guard = self.refresh_lock.lock().await;
        self.store
            .rotate_oauth_bundle(
                &self.handle_id,
                &self.owner_user_id,
                stored.version,
                &OAuthCredentialBundle {
                    expires_at_ms: 0,
                    ..serde_json::from_str(&stored.plaintext)
                        .map_err(|_| ProviderCredentialError::RefreshFailed)?
                },
            )
            .await?;
        self.refresh(cancel, deadline).await?;
        Ok(CredentialInvalidation::ReplacementPossible)
    }
}

#[cfg(test)]
mod tests;
