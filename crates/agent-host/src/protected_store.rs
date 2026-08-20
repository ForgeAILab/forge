use std::{
    collections::{BTreeSet, HashMap},
    fmt,
    sync::{Arc, Mutex as StdMutex},
};

use agent_runtime::core::{
    checkpoint::{CheckpointStore, TurnCheckpoint},
    error::{ErrorKind, RuntimeError},
    ids::SessionId,
    prelude::{
        Clock, CredentialInvalidation, Deadline, ProviderAuthRejection, ProviderCredentialError,
        ProviderCredentialLease, ProviderCredentialRevision, ProviderCredentialSource,
        ProviderCredentialTarget, SystemClock, Timestamp,
    },
    store::{Secret, SessionSnapshot, SessionStore},
};
use async_trait::async_trait;
use chacha20poly1305::{
    XChaCha20Poly1305, XNonce,
    aead::{Aead, KeyInit, OsRng, rand_core::RngCore},
};
use db::{CredentialHandle, SqliteDb};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use tokio::sync::Mutex;

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

#[derive(Clone)]
pub struct SqliteProviderCredentialSource {
    store: SqliteProtectedRuntimeStore,
    owner_user_id: String,
    handle_id: String,
    refresh_lock: Arc<Mutex<()>>,
    client: reqwest::Client,
}

impl fmt::Debug for SqliteProviderCredentialSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqliteProviderCredentialSource")
            .field("handle", &"[opaque]")
            .finish()
    }
}

#[derive(Clone)]
pub struct SqliteProtectedRuntimeStore {
    db: Arc<SqliteDb>,
    cipher: Arc<XChaCha20Poly1305>,
    key_revision: i64,
    refresh_locks: Arc<StdMutex<HashMap<String, Arc<Mutex<()>>>>>,
    revocation_client: reqwest::Client,
    gemini_revocation_endpoint: Arc<str>,
}

impl SqliteProtectedRuntimeStore {
    pub fn new(db: Arc<SqliteDb>, master_key: [u8; 32], key_revision: i64) -> Self {
        Self {
            db,
            cipher: Arc::new(XChaCha20Poly1305::new((&master_key).into())),
            key_revision,
            refresh_locks: Arc::new(StdMutex::new(HashMap::new())),
            revocation_client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(5))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("credential revocation client configuration is valid"),
            gemini_revocation_endpoint: Arc::from("https://oauth2.googleapis.com/revoke"),
        }
    }

    fn seal(&self, plaintext: &[u8]) -> Result<(Vec<u8>, Vec<u8>), RuntimeError> {
        let mut nonce = [0_u8; 24];
        OsRng.fill_bytes(&mut nonce);
        let ciphertext = self
            .cipher
            .encrypt(XNonce::from_slice(&nonce), plaintext)
            .map_err(|_| {
                RuntimeError::new(ErrorKind::Internal, "protected state encryption failed")
            })?;
        Ok((ciphertext, nonce.to_vec()))
    }

    fn open(&self, ciphertext: &[u8], nonce: &[u8]) -> Result<Vec<u8>, RuntimeError> {
        if nonce.len() != 24 {
            return Err(RuntimeError::new(
                ErrorKind::Serialization,
                "protected state nonce is invalid",
            ));
        }
        self.cipher
            .decrypt(XNonce::from_slice(nonce), ciphertext)
            .map_err(|_| {
                RuntimeError::new(
                    ErrorKind::Serialization,
                    "protected state could not be opened",
                )
            })
    }

    /// Internal protected-payload seam used by the interaction broker.  The
    /// bytes never cross into public profile/session/domain projections.
    pub(crate) fn seal_protected(
        &self,
        plaintext: &[u8],
    ) -> Result<(Vec<u8>, Vec<u8>), RuntimeError> {
        self.seal(plaintext)
    }

    /// Internal protected-payload seam used by the interaction broker.
    pub(crate) fn open_protected(
        &self,
        ciphertext: &[u8],
        nonce: &[u8],
    ) -> Result<Vec<u8>, RuntimeError> {
        self.open(ciphertext, nonce)
    }

    pub(crate) fn database(&self) -> Arc<SqliteDb> {
        Arc::clone(&self.db)
    }

    pub(crate) async fn forge_session_id_for_runtime(
        &self,
        runtime_id: &SessionId,
    ) -> Result<String, crate::AgentHostError> {
        self.forge_session_id(runtime_id)
            .await
            .map_err(|_| crate::AgentHostError::SessionNotFound)
    }

    async fn forge_session_id(&self, runtime_id: &SessionId) -> Result<String, RuntimeError> {
        sqlx::query_scalar::<_, String>("SELECT id FROM agent_session WHERE runtime_session_id = ?")
            .bind(runtime_id.as_str())
            .fetch_optional(self.db.pool())
            .await
            .map_err(|_| RuntimeError::internal("protected state lookup failed"))?
            .ok_or_else(|| RuntimeError::not_found("runtime session mapping not found"))
    }

    /// Loads the server-issued identity/scope binding for one runtime session.
    ///
    /// The optional Task workspace is joined by the exact host-supplied path;
    /// a path that is not the current persisted workspace is therefore
    /// rejected before RuntimeBuilder receives a filesystem-capable tool.
    pub(crate) async fn runtime_scope_binding(
        &self,
        forge_session_id: &str,
        runtime_session_id: &str,
        workspace_path: Option<&str>,
    ) -> Result<crate::RuntimeScopeBinding, crate::AgentHostError> {
        let row = sqlx::query(
            "SELECT session.identity_id,
                    identity.account_permission_ceiling,
                    identity.paused,
                    identity.archived_at,
                    profile.tool_policy_json,
                    scope.scope_type,
                    scope.scope_id,
                    scope.project_id,
                    scope.task_role,
                    scope.workspace_access,
                    chat.kind AS agent_chat_kind,
                    chat.project_id AS agent_chat_project_id,
                    binding.permission_ceiling_json AS binding_permission_ceiling,
                    bound_project.charter_setup_required AS project_charter_setup_required,
                    workspace.worktree_path
             FROM agent_session AS session
             JOIN agent_identity AS identity
               ON identity.id = session.identity_id
             JOIN agent_profile AS profile
               ON profile.id = session.profile_id
             JOIN agent_context_scope AS scope
               ON scope.id = session.context_scope_id
             LEFT JOIN agent_chat AS chat
               ON scope.scope_type = 'agent_chat'
              AND chat.id = scope.scope_id
             LEFT JOIN project_agent_binding AS binding
               ON binding.project_id = CASE
                    WHEN scope.scope_type = 'agent_chat' THEN chat.project_id
                    ELSE scope.project_id
                  END
             AND binding.identity_id = session.identity_id
             AND binding.state = 'active'
             LEFT JOIN project AS bound_project
               ON bound_project.id = CASE
                    WHEN scope.scope_type = 'agent_chat' THEN chat.project_id
                    ELSE scope.project_id
                  END
             LEFT JOIN workspace
               ON workspace.task_id = scope.scope_id
              AND workspace.status IN ('creating', 'ready', 'error')
              AND workspace.worktree_path = ?
             WHERE session.id = ?
               AND session.runtime_session_id = ?
             LIMIT 1",
        )
        .bind(workspace_path)
        .bind(forge_session_id)
        .bind(runtime_session_id)
        .fetch_optional(self.db.pool())
        .await
        .map_err(|_| crate::AgentHostError::ProtectedPersistence)?
        .ok_or(crate::AgentHostError::SessionNotFound)?;

        let identity_id: String = row
            .try_get("identity_id")
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        let account_permission_ceiling: String = row
            .try_get("account_permission_ceiling")
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        let identity_paused: i64 = row
            .try_get("paused")
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        let identity_archived_at: Option<String> = row
            .try_get("archived_at")
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        let profile_tool_policy: String = row
            .try_get("tool_policy_json")
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        let scope_type: String = row
            .try_get("scope_type")
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        let scope_id: String = row
            .try_get("scope_id")
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        let project_id: Option<String> = row
            .try_get("project_id")
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        let agent_chat_kind: Option<String> = row
            .try_get("agent_chat_kind")
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        let agent_chat_project_id: Option<String> = row
            .try_get("agent_chat_project_id")
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        let task_role: Option<String> = row
            .try_get("task_role")
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        let workspace_access: String = row
            .try_get("workspace_access")
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        let persisted_workspace_path: Option<String> = row
            .try_get("worktree_path")
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        let binding_permission_ceiling: Option<String> = row
            .try_get("binding_permission_ceiling")
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        let project_charter_setup_required: bool = row
            .try_get::<Option<i64>, _>("project_charter_setup_required")
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?
            .is_some_and(|value| value != 0);
        let scope_type = match scope_type.as_str() {
            "account" => crate::CanonicalScopeType::Account,
            "project" => crate::CanonicalScopeType::Project,
            "agent_chat" => crate::CanonicalScopeType::AgentChat,
            "task" => crate::CanonicalScopeType::Task,
            _ => {
                return Err(crate::AgentHostError::Authority(
                    "persisted canonical scope type is invalid".to_owned(),
                ));
            }
        };
        let workspace_access = match workspace_access.as_str() {
            "deny" => crate::WorkspaceAccess::Deny,
            "task_read" => crate::WorkspaceAccess::TaskRead,
            "task_write" => crate::WorkspaceAccess::TaskWrite,
            _ => {
                return Err(crate::AgentHostError::Authority(
                    "persisted workspace access is invalid".to_owned(),
                ));
            }
        };
        let scope = crate::CanonicalScope {
            scope_type,
            scope_id,
            workspace_access,
        };
        scope.validate()?;
        let agent_chat_project_id =
            if matches!(scope.scope_type, crate::CanonicalScopeType::AgentChat) {
                match agent_chat_kind.as_deref() {
                    Some("account_main") => {
                        if agent_chat_project_id.is_some() || project_id.is_some() {
                            return Err(crate::AgentHostError::Authority(
                                "Main Agent Chat has an invalid Project binding".to_owned(),
                            ));
                        }
                        None
                    }
                    Some("project") => {
                        let Some(chat_project_id) = agent_chat_project_id else {
                            return Err(crate::AgentHostError::Authority(
                                "Project Agent Chat has no owning Project".to_owned(),
                            ));
                        };
                        if project_id.as_deref() != Some(chat_project_id.as_str()) {
                            return Err(crate::AgentHostError::Authority(
                                "Project Agent Chat scope does not match its owning Project"
                                    .to_owned(),
                            ));
                        }
                        Some(chat_project_id)
                    }
                    _ => {
                        return Err(crate::AgentHostError::Authority(
                            "persisted Agent Chat kind is not admitted".to_owned(),
                        ));
                    }
                }
            } else {
                None
            };
        if matches!(scope.scope_type, crate::CanonicalScopeType::Task)
            && persisted_workspace_path.is_none()
        {
            return Err(crate::AgentHostError::Authority(
                "Task session has no active persisted workspace".to_owned(),
            ));
        }
        if !matches!(scope.scope_type, crate::CanonicalScopeType::Task)
            && persisted_workspace_path.is_some()
        {
            return Err(crate::AgentHostError::Authority(
                "non-Task session is bound to a workspace".to_owned(),
            ));
        }
        if identity_paused != 0 || identity_archived_at.is_some() {
            return Err(crate::AgentHostError::Authority(
                "native session identity is no longer active".to_owned(),
            ));
        }
        if project_id.is_some()
            && !matches!(scope.scope_type, crate::CanonicalScopeType::Task)
            && binding_permission_ceiling.is_none()
        {
            return Err(crate::AgentHostError::Authority(
                "native session Project authority is no longer active".to_owned(),
            ));
        }
        let mut allowed_permissions = permission_set(&account_permission_ceiling);
        intersect_permissions(
            &mut allowed_permissions,
            &permission_set(&profile_tool_policy),
        );
        intersect_permissions(
            &mut allowed_permissions,
            &scope_permission_set(
                scope.scope_type,
                scope.workspace_access,
                agent_chat_project_id.is_some(),
                project_charter_setup_required,
            ),
        );
        if let Some(binding_permissions) = binding_permission_ceiling {
            intersect_permissions(
                &mut allowed_permissions,
                &permission_set(&binding_permissions),
            );
        }
        Ok(crate::RuntimeScopeBinding {
            identity_id,
            scope,
            task_role,
            workspace_path: persisted_workspace_path,
            agent_chat_project_id,
            project_charter_setup_required,
            allowed_permissions,
        })
    }

    /// Resolves a replaceable runtime session to the stable identity/scope
    /// LCM timeline. The runtime id and canonical scope must both match the
    /// persisted Forge session; a timeline id alone cannot be used to open
    /// the store.
    pub async fn lcm_store_for_runtime_session(
        &self,
        runtime_id: &str,
        scope_type: &str,
        scope_id: &str,
    ) -> Result<crate::SqliteLcmStore, crate::AgentHostError> {
        let row = sqlx::query(
            "SELECT session.identity_id, scope.scope_type, scope.scope_id
             FROM agent_session AS session
             JOIN agent_context_scope AS scope
               ON scope.id = session.context_scope_id
             WHERE session.runtime_session_id = ?
               AND scope.scope_type = ? AND scope.scope_id = ?
             LIMIT 1",
        )
        .bind(runtime_id)
        .bind(scope_type)
        .bind(scope_id)
        .fetch_optional(self.db.pool())
        .await
        .map_err(|_| crate::AgentHostError::ProtectedPersistence)?
        .ok_or(crate::AgentHostError::SessionNotFound)?;
        let identity_id: String = row
            .try_get("identity_id")
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        let stored_scope_type: String = row
            .try_get("scope_type")
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        let stored_scope_id: String = row
            .try_get("scope_id")
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        let authorization_revision =
            agent_runtime::registry::RegistryRevision::from_content(format!(
                "forge-lcm-authorization-v1\n{identity_id}\n{stored_scope_type}\n{stored_scope_id}"
            ));
        crate::SqliteLcmStore::open_for_binding(
            Arc::clone(&self.db),
            &identity_id,
            &stored_scope_type,
            &stored_scope_id,
            authorization_revision.as_str(),
            &db::now_rfc3339(),
        )
        .await
    }

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
        let mut transaction = db::begin_immediate(self.db.pool())
            .await
            .map_err(|error| log_store_error(error, crate::AgentHostError::ProtectedPersistence))?;
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
        .map_err(|error| log_store_error(error, crate::AgentHostError::ProtectedPersistence))?;
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
        let row = sqlx::query(
            "SELECT secret.ciphertext, secret.nonce
             FROM protected_credential_secret AS secret
             JOIN credential_handle AS handle ON handle.id = secret.handle_id
             WHERE handle.id = ? AND handle.owner_user_id = ? AND handle.status = 'configured'",
        )
        .bind(handle_id)
        .bind(owner_user_id)
        .fetch_optional(self.db.pool())
        .await
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
        let mut transaction = db::begin_immediate(self.db.pool())
            .await
            .map_err(|error| log_store_error(error, crate::AgentHostError::ProtectedPersistence))?;
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
        .map_err(|error| log_store_error(error, crate::AgentHostError::ProtectedPersistence))?;
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
        .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        Ok(())
    }

    pub async fn open_provider_authorization_state(
        &self,
        operation_id: &str,
    ) -> Result<Vec<u8>, crate::AgentHostError> {
        let row = sqlx::query(
            "SELECT ciphertext, nonce FROM protected_provider_authorization_state
             WHERE operation_id = ?",
        )
        .bind(operation_id)
        .fetch_optional(self.db.pool())
        .await
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
        sqlx::query("DELETE FROM protected_provider_authorization_state WHERE operation_id = ?")
            .bind(operation_id)
            .execute(self.db.pool())
            .await
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        Ok(())
    }

    async fn load_stored_credential(
        &self,
        handle_id: &str,
        owner_user_id: &str,
    ) -> Result<StoredCredential, ProviderCredentialError> {
        let row = sqlx::query(
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
        let _ = sqlx::query(
            "UPDATE credential_handle
             SET status = 'invalid', version = version + 1, updated_at = ?
             WHERE id = ? AND owner_user_id = ? AND version = ?",
        )
        .bind(db::now_rfc3339())
        .bind(handle_id)
        .bind(owner_user_id)
        .bind(expected_version)
        .execute(self.db.pool())
        .await;
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
        let mut transaction = db::begin_immediate(self.db.pool())
            .await
            .map_err(|error| log_store_error(error, ProviderCredentialError::RefreshFailed))?;
        let result = sqlx::query(
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
        .map_err(|error| log_store_error(error, ProviderCredentialError::RefreshFailed))?;
        if result.rows_affected() == 0 {
            transaction
                .rollback()
                .await
                .map_err(|error| log_store_error(error, ProviderCredentialError::RefreshFailed))?;
            return Err(ProviderCredentialError::RefreshFailed);
        }
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
        let mut transaction = db::begin_immediate(self.db.pool())
            .await
            .map_err(|error| log_store_error(error, crate::AgentHostError::ProtectedPersistence))?;
        let result = sqlx::query(
            "UPDATE credential_handle
             SET status = 'revoked', version = version + 1, updated_at = ?
             WHERE id = ? AND owner_user_id = ?",
        )
        .bind(now)
        .bind(handle_id)
        .bind(owner_user_id)
        .execute(&mut *transaction)
        .await
        .map_err(|error| log_store_error(error, crate::AgentHostError::ProtectedPersistence))?;
        if result.rows_affected() == 0 {
            return Err(crate::AgentHostError::CredentialNotFound);
        }
        sqlx::query("DELETE FROM protected_credential_secret WHERE handle_id = ?")
            .bind(handle_id)
            .execute(&mut *transaction)
            .await
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
        let mut transaction = db::begin_immediate(self.db.pool())
            .await
            .map_err(|error| log_store_error(error, crate::AgentHostError::ProtectedPersistence))?;
        let result = sqlx::query(
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
        .map_err(|error| log_store_error(error, crate::AgentHostError::ProtectedPersistence))?;
        if result.rows_affected() == 0 {
            return Err(crate::AgentHostError::VersionConflict);
        }
        sqlx::query("DELETE FROM protected_credential_secret WHERE handle_id = ?")
            .bind(handle_id)
            .execute(&mut *transaction)
            .await
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

async fn mark_credential_dependents_unavailable(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    handle_id: &str,
    now: &str,
) -> Result<(), crate::AgentHostError> {
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
    .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
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

fn permission_set(value: &str) -> BTreeSet<String> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(value) else {
        return BTreeSet::new();
    };
    match value {
        serde_json::Value::Array(values) => values
            .into_iter()
            .filter_map(|value| value.as_str().map(str::to_owned))
            .collect(),
        serde_json::Value::Object(map) => map
            .get("permissions")
            .or_else(|| map.get("allowed"))
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|value| value.as_str().map(str::to_owned))
            .collect(),
        _ => BTreeSet::new(),
    }
}

fn intersect_permissions(target: &mut BTreeSet<String>, layer: &BTreeSet<String>) {
    *target = target.intersection(layer).cloned().collect();
}

fn scope_permission_set(
    scope_type: crate::CanonicalScopeType,
    workspace_access: crate::WorkspaceAccess,
    project_agent_chat: bool,
    project_charter_setup_required: bool,
) -> BTreeSet<String> {
    let mut values: Vec<&str> = match scope_type {
        crate::CanonicalScopeType::Account => vec![
            "read_account",
            "propose_discovery",
            "propose_project",
            "propose_handoff",
        ],
        crate::CanonicalScopeType::Project => {
            let mut project = vec![
                "read_project",
                "read_memory",
                "propose_project",
                "propose_message",
            ];
            if !project_charter_setup_required {
                project.extend([
                    "propose_task",
                    "propose_commitment",
                    "propose_memory",
                    "propose_review",
                    "propose_decision",
                    "propose_session",
                ]);
            }
            project
        }
        crate::CanonicalScopeType::AgentChat => {
            let mut chat = vec!["read_agent_chat", "read_memory", "propose_message"];
            if project_agent_chat && !project_charter_setup_required {
                chat.extend(["propose_commitment", "propose_memory", "propose_session"]);
            }
            chat
        }
        crate::CanonicalScopeType::Task => match workspace_access {
            crate::WorkspaceAccess::TaskRead => {
                vec!["read_task", "read_memory", "task_read", "propose_review"]
            }
            crate::WorkspaceAccess::TaskWrite => {
                vec!["read_task", "read_memory", "task_read", "task_write"]
            }
            crate::WorkspaceAccess::Deny => vec![],
        },
    };
    if matches!(scope_type, crate::CanonicalScopeType::AgentChat) && project_agent_chat {
        values.push("propose_project");
        if !project_charter_setup_required {
            values.push("propose_task");
        }
    } else if matches!(scope_type, crate::CanonicalScopeType::AgentChat) {
        values.extend(["propose_discovery", "propose_project", "propose_handoff"]);
    }
    values.into_iter().map(str::to_owned).collect()
}

impl fmt::Debug for SqliteProtectedRuntimeStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqliteProtectedRuntimeStore")
            .field("key_revision", &self.key_revision)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SessionStore for SqliteProtectedRuntimeStore {
    async fn load(&self, id: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        let forge_session_id = match self.forge_session_id(id).await {
            Ok(value) => value,
            Err(error) if error.kind == ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let row = sqlx::query(
            "SELECT snapshot_ciphertext, snapshot_nonce
             FROM protected_agent_session_state WHERE session_id = ?",
        )
        .bind(forge_session_id)
        .fetch_optional(self.db.pool())
        .await
        .map_err(|_| RuntimeError::internal("protected session load failed"))?;
        let Some(row) = row else {
            return Ok(None);
        };
        let ciphertext: Option<Vec<u8>> = row
            .try_get("snapshot_ciphertext")
            .map_err(|_| RuntimeError::internal("protected session row is invalid"))?;
        let nonce: Option<Vec<u8>> = row
            .try_get("snapshot_nonce")
            .map_err(|_| RuntimeError::internal("protected session row is invalid"))?;
        match (ciphertext, nonce) {
            (Some(ciphertext), Some(nonce)) => {
                let bytes = self.open(&ciphertext, &nonce)?;
                serde_json::from_slice(&bytes).map(Some).map_err(Into::into)
            }
            _ => Ok(None),
        }
    }

    async fn save(&self, snapshot: &SessionSnapshot) -> Result<(), RuntimeError> {
        let forge_session_id = self.forge_session_id(&snapshot.id).await?;
        let bytes = serde_json::to_vec(snapshot)?;
        let (ciphertext, nonce) = self.seal(&bytes)?;
        sqlx::query(
            "INSERT INTO protected_agent_session_state (
                session_id, snapshot_ciphertext, snapshot_nonce,
                key_revision, state_revision, updated_at
             ) VALUES (?, ?, ?, ?, 1, ?)
             ON CONFLICT(session_id) DO UPDATE SET
                snapshot_ciphertext = excluded.snapshot_ciphertext,
                snapshot_nonce = excluded.snapshot_nonce,
                key_revision = excluded.key_revision,
                state_revision = protected_agent_session_state.state_revision + 1,
                updated_at = excluded.updated_at",
        )
        .bind(forge_session_id)
        .bind(ciphertext)
        .bind(nonce)
        .bind(self.key_revision)
        .bind(db::now_rfc3339())
        .execute(self.db.pool())
        .await
        .map_err(|_| RuntimeError::internal("protected session save failed"))?;
        Ok(())
    }
}

#[async_trait]
impl CheckpointStore for SqliteProtectedRuntimeStore {
    async fn load_latest(
        &self,
        session: &SessionId,
    ) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        let forge_session_id = match self.forge_session_id(session).await {
            Ok(value) => value,
            Err(error) if error.kind == ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let row = sqlx::query(
            "SELECT checkpoint_ciphertext, checkpoint_nonce
             FROM protected_agent_session_state WHERE session_id = ?",
        )
        .bind(forge_session_id)
        .fetch_optional(self.db.pool())
        .await
        .map_err(|_| RuntimeError::internal("protected checkpoint load failed"))?;
        let Some(row) = row else {
            return Ok(None);
        };
        let ciphertext: Option<Vec<u8>> = row
            .try_get("checkpoint_ciphertext")
            .map_err(|_| RuntimeError::internal("protected checkpoint row is invalid"))?;
        let nonce: Option<Vec<u8>> = row
            .try_get("checkpoint_nonce")
            .map_err(|_| RuntimeError::internal("protected checkpoint row is invalid"))?;
        match (ciphertext, nonce) {
            (Some(ciphertext), Some(nonce)) => {
                let bytes = self.open(&ciphertext, &nonce)?;
                serde_json::from_slice(&bytes).map(Some).map_err(Into::into)
            }
            _ => Ok(None),
        }
    }

    async fn save(&self, checkpoint: &TurnCheckpoint) -> Result<(), RuntimeError> {
        let forge_session_id = self.forge_session_id(&checkpoint.session).await?;
        let bytes = serde_json::to_vec(checkpoint)?;
        let (ciphertext, nonce) = self.seal(&bytes)?;
        let fingerprint = checkpoint.operation_fingerprint.to_string();
        let result = sqlx::query(
            "INSERT INTO protected_agent_session_state (
                session_id, checkpoint_ciphertext, checkpoint_nonce,
                checkpoint_turn_id, checkpoint_revision, checkpoint_fingerprint,
                key_revision, state_revision, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)
             ON CONFLICT(session_id) DO UPDATE SET
                checkpoint_ciphertext = excluded.checkpoint_ciphertext,
                checkpoint_nonce = excluded.checkpoint_nonce,
                checkpoint_turn_id = excluded.checkpoint_turn_id,
                checkpoint_revision = excluded.checkpoint_revision,
                checkpoint_fingerprint = excluded.checkpoint_fingerprint,
                key_revision = excluded.key_revision,
                state_revision = protected_agent_session_state.state_revision + 1,
                updated_at = excluded.updated_at
             WHERE protected_agent_session_state.checkpoint_revision IS NULL
                OR excluded.checkpoint_turn_id != protected_agent_session_state.checkpoint_turn_id
                OR excluded.checkpoint_revision > protected_agent_session_state.checkpoint_revision
                OR (
                    excluded.checkpoint_revision = protected_agent_session_state.checkpoint_revision
                    AND excluded.checkpoint_fingerprint = protected_agent_session_state.checkpoint_fingerprint
                )",
        )
        .bind(forge_session_id)
        .bind(ciphertext)
        .bind(nonce)
        .bind(checkpoint.turn.as_str())
        .bind(i64::try_from(checkpoint.state_revision).unwrap_or(i64::MAX))
        .bind(fingerprint)
        .bind(self.key_revision)
        .bind(db::now_rfc3339())
        .execute(self.db.pool())
        .await
        .map_err(|_| RuntimeError::internal("protected checkpoint save failed"))?;
        if result.rows_affected() == 0 {
            return Err(RuntimeError::conflict(
                "protected checkpoint revision moved backwards or changed fingerprint",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn test_store() -> (SqliteProtectedRuntimeStore, Arc<SqliteDb>) {
        use db::{User, UserRepo};

        let pool = db::create_sqlite_pool("sqlite::memory:")
            .await
            .expect("pool creates");
        db::run_migrations(&pool).await.expect("migrations run");
        let db = Arc::new(SqliteDb::new(pool));
        let now = db::now_rfc3339();
        UserRepo::create_user(
            &*db,
            &User {
                id: "credential-owner".to_owned(),
                email: "credential-owner@example.com".to_owned(),
                password_hash: "hash".to_owned(),
                display_name: None,
                is_admin: false,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("owner creates");
        (
            SqliteProtectedRuntimeStore::new(Arc::clone(&db), [7_u8; 32], 1),
            db,
        )
    }

    #[test]
    fn oauth_bundle_debug_redacts_token_material() {
        let bundle = OAuthCredentialBundle {
            schema_version: 1,
            access_token: "sensitive-access".to_owned(),
            refresh_token: "sensitive-refresh".to_owned(),
            expires_at_ms: 42,
            token_endpoint: "https://example.com/token".to_owned(),
            client_id: "public-client".to_owned(),
            client_secret: Some("sensitive-client-secret".to_owned()),
            scopes: vec!["openid".to_owned()],
            provider_account_id: Some("account".to_owned()),
        };
        let debug = format!("{bundle:?}");
        assert!(!debug.contains("sensitive-access"));
        assert!(!debug.contains("sensitive-refresh"));
        assert!(!debug.contains("sensitive-client-secret"));
    }

    #[tokio::test]
    async fn renewable_bundle_is_acquired_through_runtime_credential_contract() {
        let (store, db) = test_store().await;
        let now = db::now_rfc3339();
        store
            .create_oauth_credential(CreateOAuthCredential {
                id: "oauth-handle",
                owner_user_id: "credential-owner",
                provider: "xai",
                label: "xAI login",
                bundle: &OAuthCredentialBundle {
                    schema_version: 1,
                    access_token: "short-lived-access".to_owned(),
                    refresh_token: "renewable-refresh".to_owned(),
                    expires_at_ms: SystemClock.now().as_millis().saturating_add(60_000),
                    token_endpoint: "https://auth.x.ai/token".to_owned(),
                    client_id: "client".to_owned(),
                    client_secret: None,
                    scopes: vec!["openid".to_owned()],
                    provider_account_id: None,
                },
                metadata_json: "{}",
                now: &now,
            })
            .await
            .expect("bundle stores");
        let secret = store
            .acquire_provider_credential("credential-owner", "oauth-handle", 30_000)
            .await
            .expect("lease acquires");
        assert_eq!(secret.expose(), "short-lived-access");
        let ciphertext: Vec<u8> = sqlx::query_scalar(
            "SELECT ciphertext FROM protected_credential_secret WHERE handle_id = 'oauth-handle'",
        )
        .fetch_one(db.pool())
        .await
        .expect("ciphertext reads");
        assert!(!String::from_utf8_lossy(&ciphertext).contains("short-lived-access"));
    }

    #[tokio::test]
    async fn concurrent_expired_leases_single_flight_and_rotate_tokens_atomically() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("mock provider binds");
        let address = listener.local_addr().expect("mock address exists");
        let request_count = Arc::new(AtomicUsize::new(0));
        let server_count = Arc::clone(&request_count);
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("refresh request arrives");
            server_count.fetch_add(1, Ordering::SeqCst);
            let mut request = vec![0_u8; 4096];
            let _ = stream.read(&mut request).await.expect("request reads");
            tokio::time::sleep(std::time::Duration::from_millis(40)).await;
            let body = r#"{"access_token":"rotated-access","refresh_token":"rotated-refresh","expires_in":3600,"scope":"openid profile"}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("response writes");
        });

        let (store, _) = test_store().await;
        let now = db::now_rfc3339();
        store
            .create_oauth_credential(CreateOAuthCredential {
                id: "refresh-handle",
                owner_user_id: "credential-owner",
                provider: "xai",
                label: "xAI login",
                bundle: &OAuthCredentialBundle {
                    schema_version: 1,
                    access_token: "expired-access".to_owned(),
                    refresh_token: "initial-refresh".to_owned(),
                    expires_at_ms: 0,
                    token_endpoint: format!("http://{address}/token"),
                    client_id: "client".to_owned(),
                    client_secret: None,
                    scopes: vec!["openid".to_owned()],
                    provider_account_id: None,
                },
                metadata_json: "{}",
                now: &now,
            })
            .await
            .expect("expired bundle stores");
        let source = store.credential_source("credential-owner", "refresh-handle");
        let target = ProviderCredentialTarget::new("refresh-handle").expect("target is valid");
        let cancel = agent_runtime::core::cancel::Cancellation::new();
        let deadline = Deadline::after(&SystemClock, 5_000);
        let (first, second) = tokio::join!(
            source.acquire(&target, 30_000, &cancel, deadline),
            source.acquire(&target, 30_000, &cancel, deadline),
        );
        let first = first.expect("first lease refreshes");
        let second = second.expect("second lease reuses refresh");
        assert_eq!(first.secret().expose(), "rotated-access");
        assert_eq!(second.secret().expose(), "rotated-access");
        server.await.expect("mock provider completes");
        assert_eq!(request_count.load(Ordering::SeqCst), 1);

        let stored = store
            .load_stored_credential("refresh-handle", "credential-owner")
            .await
            .expect("rotated bundle loads");
        let bundle: OAuthCredentialBundle =
            serde_json::from_str(&stored.plaintext).expect("bundle decodes");
        assert_eq!(stored.version, 2);
        assert_eq!(bundle.refresh_token, "rotated-refresh");
        assert_eq!(bundle.scopes, vec!["openid", "profile"]);
    }

    #[tokio::test]
    async fn stale_disconnect_preserves_the_current_credential() {
        let (store, db) = test_store().await;
        let now = db::now_rfc3339();
        store
            .create_oauth_credential(CreateOAuthCredential {
                id: "disconnect-handle",
                owner_user_id: "credential-owner",
                provider: "xai",
                label: "xAI login",
                bundle: &OAuthCredentialBundle {
                    schema_version: 1,
                    access_token: "access".to_owned(),
                    refresh_token: "refresh".to_owned(),
                    expires_at_ms: SystemClock.now().as_millis().saturating_add(60_000),
                    token_endpoint: "https://auth.x.ai/token".to_owned(),
                    client_id: "client".to_owned(),
                    client_secret: None,
                    scopes: vec![],
                    provider_account_id: None,
                },
                metadata_json: "{}",
                now: &now,
            })
            .await
            .expect("bundle stores");
        assert!(matches!(
            store
                .revoke_credential_at_version("disconnect-handle", "credential-owner", 0, &now,)
                .await,
            Err(crate::AgentHostError::VersionConflict)
        ));
        let preserved = store
            .load_stored_credential("disconnect-handle", "credential-owner")
            .await
            .expect("stale disconnect preserves secret");
        assert_eq!(preserved.version, 1);
        let preserved_bundle: OAuthCredentialBundle =
            serde_json::from_str(&preserved.plaintext).expect("preserved bundle decodes");
        assert_eq!(preserved_bundle.access_token, "access");
        store
            .revoke_credential_at_version("disconnect-handle", "credential-owner", 1, &now)
            .await
            .expect("current disconnect succeeds");
        assert!(matches!(
            store
                .load_credential("disconnect-handle", "credential-owner")
                .await,
            Err(crate::AgentHostError::CredentialNotFound)
        ));
        let (status, version): (String, i64) = sqlx::query_as(
            "SELECT status, version FROM credential_handle WHERE id = 'disconnect-handle'",
        )
        .fetch_one(db.pool())
        .await
        .expect("handle remains as safe metadata");
        assert_eq!(status, "revoked");
        assert_eq!(version, 2);
    }

    #[tokio::test]
    async fn provider_revocation_failure_never_rolls_back_local_disconnect() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("mock revocation provider binds");
        let address = listener.local_addr().expect("mock address exists");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("revocation request arrives");
            let mut request = vec![0_u8; 4096];
            let size = stream.read(&mut request).await.expect("request reads");
            assert!(String::from_utf8_lossy(&request[..size]).contains("token=refresh"));
            stream
                .write_all(
                    b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await
                .expect("failure response writes");
        });

        let (mut store, db) = test_store().await;
        store.gemini_revocation_endpoint = Arc::from(format!("http://{address}/revoke"));
        let now = db::now_rfc3339();
        store
            .create_oauth_credential(CreateOAuthCredential {
                id: "gemini-disconnect-handle",
                owner_user_id: "credential-owner",
                provider: "gemini",
                label: "Google login",
                bundle: &OAuthCredentialBundle {
                    schema_version: 1,
                    access_token: "access".to_owned(),
                    refresh_token: "refresh".to_owned(),
                    expires_at_ms: SystemClock.now().as_millis().saturating_add(60_000),
                    token_endpoint: "https://oauth2.googleapis.com/token".to_owned(),
                    client_id: "client".to_owned(),
                    client_secret: None,
                    scopes: vec![],
                    provider_account_id: None,
                },
                metadata_json: "{}",
                now: &now,
            })
            .await
            .expect("bundle stores");

        let outcome = store
            .revoke_credential_at_version("gemini-disconnect-handle", "credential-owner", 1, &now)
            .await
            .expect("local disconnect commits");
        assert_eq!(outcome, CredentialRevocationOutcome::Failed);
        server.await.expect("mock provider completes");

        assert!(matches!(
            store
                .load_credential("gemini-disconnect-handle", "credential-owner")
                .await,
            Err(crate::AgentHostError::CredentialNotFound)
        ));
        let status: String = sqlx::query_scalar(
            "SELECT status FROM credential_handle WHERE id = 'gemini-disconnect-handle'",
        )
        .fetch_one(db.pool())
        .await
        .expect("revoked metadata remains");
        assert_eq!(status, "revoked");
    }

    #[test]
    fn effective_permission_intersection_fails_closed() {
        let mut effective = permission_set(r#"{"allowed":["read_project","task_write"]}"#);
        intersect_permissions(&mut effective, &permission_set(r#"["read_project"]"#));
        assert_eq!(effective, BTreeSet::from(["read_project".to_owned()]));

        let mut malformed = permission_set("not-json");
        intersect_permissions(
            &mut malformed,
            &scope_permission_set(
                crate::CanonicalScopeType::Project,
                crate::WorkspaceAccess::Deny,
                false,
                false,
            ),
        );
        assert!(malformed.is_empty());
    }

    #[test]
    fn task_scope_ceiling_is_the_only_filesystem_policy() {
        let task = scope_permission_set(
            crate::CanonicalScopeType::Task,
            crate::WorkspaceAccess::TaskWrite,
            false,
            false,
        );
        assert!(task.contains("task_read"));
        assert!(task.contains("task_write"));

        let project = scope_permission_set(
            crate::CanonicalScopeType::Project,
            crate::WorkspaceAccess::Deny,
            false,
            false,
        );
        assert!(!project.contains("task_read"));
        assert!(!project.contains("task_write"));
    }

    #[test]
    fn legacy_project_setup_scope_keeps_only_adoption_permission() {
        let setup = scope_permission_set(
            crate::CanonicalScopeType::Project,
            crate::WorkspaceAccess::Deny,
            true,
            true,
        );
        assert!(setup.contains("read_project"));
        assert!(setup.contains("propose_project"));
        assert!(setup.contains("propose_message"));
        assert!(!setup.contains("propose_task"));
        assert!(!setup.contains("propose_review"));
        assert!(!setup.contains("propose_session"));
    }
}
