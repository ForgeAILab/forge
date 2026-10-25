#[cfg(test)]
use operation_registry::authority::permission_set;
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

const SESSION_STATE_MAGIC: &[u8] = b"FORGE-SESSION\0";
const SESSION_STATE_FORMAT: u8 = 1;

#[cfg(any(test, feature = "test-support"))]
#[path = "test_support/legacy_baseline.rs"]
mod legacy_baseline;
#[cfg(any(test, feature = "test-support"))]
#[path = "test_support/legacy.rs"]
mod legacy_test_support;

#[path = "session_probe.rs"]
mod session_probe;

#[path = "lcm_legacy_owner.rs"]
mod lcm_legacy_owner;

pub(crate) enum LcmResolveAdmission<'a> {
    Foreground,
    Conversion {
        runtime: &'a str,
        timeline: Option<&'a str>,
    },
}

fn decode_checkpoint(session: &SessionId, bytes: &[u8]) -> Result<TurnCheckpoint, RuntimeError> {
    serde_json::from_slice(bytes).map_err(|_| {
        RuntimeError::new(
            ErrorKind::Serialization,
            format!(
                "protected checkpoint for session {} contains invalid checkpoint JSON",
                session.as_str()
            ),
        )
    })
}

pub(crate) fn protected_database_error(error: impl Into<db::DbError>) -> RuntimeError {
    let error = error.into();
    let failure = RuntimeError::internal("protected persistence database operation failed");
    if error.is_transient() {
        failure.retryable()
    } else {
        failure
    }
}

mod lcm_resolution;
mod provider_credentials;
mod scope_binding;
mod session_persistence;

pub use provider_credentials::{
    CreateOAuthCredential, CredentialRevocationOutcome, OAuthCredentialBundle,
};

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
    journals: crate::session_journals::SessionJournals,
    pub(crate) conversion_admission: Arc<crate::session_journal_conversion::ConversionAdmission>,
    #[cfg(any(test, feature = "test-support"))]
    legacy_snapshot_mac: ring::hmac::Context,
    legacy_lcm_receipt_stamps: Arc<StdMutex<HashMap<String, String>>>,
    #[cfg(test)]
    legacy_lcm_sibling_preparations: Arc<std::sync::atomic::AtomicUsize>,
    refresh_locks: Arc<StdMutex<HashMap<String, Arc<Mutex<()>>>>>,
    revocation_client: reqwest::Client,
    gemini_revocation_endpoint: Arc<str>,
}

impl SqliteProtectedRuntimeStore {
    pub fn new(db: Arc<SqliteDb>, master_key: [u8; 32], key_revision: i64) -> Self {
        #[cfg(any(test, feature = "test-support"))]
        let legacy_snapshot_mac = {
            let digest_key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, &master_key);
            let mut mac = ring::hmac::Context::with_key(&digest_key);
            mac.update(b"forge/protected-session/snapshot-digest\0");
            mac
        };
        let storage = Arc::new(
            crate::session_journal::ForgeJournalStorage::new(db.clone(), master_key, key_revision)
                .expect("protected store key revision must be positive"),
        );
        let journals = crate::session_journals::SessionJournals::new(storage);
        Self {
            db,
            journals,
            conversion_admission: Arc::default(),
            #[cfg(any(test, feature = "test-support"))]
            legacy_snapshot_mac,
            cipher: Arc::new(XChaCha20Poly1305::new((&master_key).into())),
            key_revision,
            legacy_lcm_receipt_stamps: Arc::default(),
            #[cfg(test)]
            legacy_lcm_sibling_preparations: Arc::default(),
            refresh_locks: Arc::new(StdMutex::new(HashMap::new())),
            revocation_client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(5))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("credential revocation client configuration is valid"),
            gemini_revocation_endpoint: Arc::from("https://oauth2.googleapis.com/revoke"),
        }
    }

    /// The same engine clones used by every persistent runtime and maintenance.
    pub fn journals(&self) -> crate::session_journals::SessionJournals {
        self.journals.clone()
    }

    fn seal(&self, plaintext: &[u8]) -> Result<(Vec<u8>, Vec<u8>), RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::session_journal::profile::timer("legacy.seal", plaintext.len());
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
        #[cfg(feature = "test-support")]
        let _profile = crate::session_journal::profile::timer("legacy.open", ciphertext.len());
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

    #[cfg(any(test, feature = "test-support"))]
    async fn seal_session_state(&self, bytes: Vec<u8>) -> Result<(Vec<u8>, Vec<u8>), RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile =
            crate::session_journal::profile::timer("legacy.seal_session_state", bytes.len());
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            let mut payload = SESSION_STATE_MAGIC.to_vec();
            payload.push(SESSION_STATE_FORMAT);
            zstd::stream::copy_encode(bytes.as_slice(), &mut payload, 3)
                .map_err(|_| RuntimeError::internal("protected session compression failed"))?;
            store.seal(&payload)
        })
        .await
        .map_err(|_| RuntimeError::internal("protected session compression task failed"))?
    }

    async fn open_session_state(
        &self,
        ciphertext: Vec<u8>,
        nonce: Vec<u8>,
    ) -> Result<Vec<u8>, RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile =
            crate::session_journal::profile::timer("legacy.open_session_state", ciphertext.len());
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            let payload = store.open(&ciphertext, &nonce)?;
            let Some(encoded) = payload.strip_prefix(SESSION_STATE_MAGIC) else {
                return Ok(payload);
            };
            if encoded.first() != Some(&SESSION_STATE_FORMAT) {
                return Err(RuntimeError::new(
                    ErrorKind::Serialization,
                    "protected session format is unsupported",
                ));
            }
            zstd::stream::decode_all(&encoded[1..]).map_err(|_| {
                RuntimeError::new(
                    ErrorKind::Serialization,
                    "protected session decompression failed",
                )
            })
        })
        .await
        .map_err(|_| RuntimeError::internal("protected session decompression task failed"))?
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
        {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.SELECT", 0);
            sqlx::query_scalar::<_, String>(
                "SELECT id FROM agent_session WHERE runtime_session_id = ?",
            )
            .bind(runtime_id.as_str())
            .fetch_optional(self.db.pool())
            .await
        }
        .map_err(protected_database_error)?
        .ok_or_else(|| RuntimeError::not_found("runtime session mapping not found"))
    }
}

impl fmt::Debug for SqliteProtectedRuntimeStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqliteProtectedRuntimeStore")
            .field("key_revision", &self.key_revision)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "runtime_upgrade_tests.rs"]
mod runtime_upgrade_tests;
