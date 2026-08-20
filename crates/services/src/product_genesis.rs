//! The versioned Product Genesis discovery protocol.
//!
//! Genesis is an instruction revision admitted by the existing Main Agent
//! Chat.  It is deliberately kept as a pure renderer: persistence, chat
//! admission, and the eventual Project handoff live in the owning services.
//! Keeping the prompt here free of database/runtime dependencies makes prompt
//! revisions deterministic and easy to test.

use api_types::{ProductGenesisLifecycle, ProductGenesisSession, ProductMaturity};
use async_trait::async_trait;
use std::sync::Arc;

use db::{new_uuid_v4, now_rfc3339, DbError, SqliteDb};
use sqlx::Row;

use crate::{Result, ServiceError};

/// The immutable server-owned operating-skill revision used by Product Genesis.
pub const PRODUCT_GENESIS_PROMPT_VERSION: &str = crate::MAIN_OPERATING_SKILL_KEY;

/// Validate a durable Genesis state transition.  A session is single-use:
/// discovery may be made ready, and a ready session may be handed off or
/// cancelled.  Historical terminal states never reopen.
pub fn validate_genesis_transition(
    from: ProductGenesisLifecycle,
    to: ProductGenesisLifecycle,
) -> std::result::Result<(), GenesisLifecycleError> {
    let allowed = matches!(
        (from, to),
        (
            ProductGenesisLifecycle::Discovering,
            ProductGenesisLifecycle::ReadyForProject
        ) | (
            ProductGenesisLifecycle::Discovering,
            ProductGenesisLifecycle::Cancelled
        ) | (
            ProductGenesisLifecycle::ReadyForProject,
            ProductGenesisLifecycle::HandedOff
        ) | (
            ProductGenesisLifecycle::ReadyForProject,
            ProductGenesisLifecycle::Cancelled
        )
    );
    if allowed {
        Ok(())
    } else {
        Err(GenesisLifecycleError { from, to })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GenesisLifecycleError {
    pub from: ProductGenesisLifecycle,
    pub to: ProductGenesisLifecycle,
}

impl std::fmt::Display for GenesisLifecycleError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "invalid Product Genesis transition: {:?} -> {:?}",
            self.from, self.to
        )
    }
}

impl std::error::Error for GenesisLifecycleError {}

/// Deterministic id and body for the durable Main Chat system message that
/// anchors one proposed Charter revision in the conversation history. The id
/// is derived from the revision so replays never duplicate the message.
pub fn charter_proposal_chat_message(
    revision_id: &str,
    revision_number: i64,
    working_name: &str,
    one_line_vision: &str,
    change_summary: Option<&str>,
) -> (String, String) {
    let summary = change_summary
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("Initial Charter revision.");
    (
        format!("charter-proposal:{revision_id}"),
        format!(
            "Charter revision {revision_number} proposed — {working_name}: {one_line_vision}\n\
             {summary}\n\
             Review it on the Charter card above the chat, then approve there or type /approve."
        ),
    )
}

/// Deterministic id and body for the durable Main Chat system message that
/// records an exact Charter approval receipt.
pub fn charter_approval_chat_message(
    approval_id: &str,
    revision_number: i64,
    project_name: &str,
) -> (String, String) {
    (
        format!("charter-approval:{approval_id}"),
        format!(
            "Charter revision {revision_number} approved — Project \u{201c}{project_name}\u{201d} \
             can now be created from this exact revision."
        ),
    )
}

/// The durable store boundary for Genesis.  The DB adapter owns the physical
/// table and transaction; the service owns admission, lifecycle, and prompt
/// invariants.  Keeping this boundary explicit prevents a route or model turn
/// from manufacturing a `ready_for_project`/`handed_off` state directly.
#[async_trait]
pub trait ProductGenesisStore: Send + Sync {
    async fn validate_main_chat(&self, account_id: &str, main_chat_id: &str) -> Result<bool>;
    async fn get(&self, id: &str) -> Result<Option<ProductGenesisSession>>;
    async fn get_active(&self, account_id: &str) -> Result<Option<ProductGenesisSession>>;
    async fn create(&self, input: NewProductGenesisSession) -> Result<ProductGenesisSession>;
    async fn cancel(
        &self,
        id: &str,
        expected_version: i64,
        reason: Option<String>,
    ) -> Result<ProductGenesisSession>;
    async fn transition(&self, input: TransitionProductGenesis) -> Result<ProductGenesisSession>;
    async fn add_source_message(
        &self,
        id: &str,
        expected_version: i64,
        source_message_id: &str,
    ) -> Result<ProductGenesisSession>;
    async fn apply_guided_setup(
        &self,
        id: &str,
        expected_version: i64,
        maturity: Option<ProductMaturity>,
        preferred_project_agent_identity_id: Option<String>,
        provenance: Option<String>,
    ) -> Result<ProductGenesisSession>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewProductGenesisSession {
    pub id: String,
    pub account_id: String,
    pub main_chat_id: String,
    pub prompt_revision: String,
    pub prompt_body: String,
    pub maturity: ProductMaturity,
    pub initial_idea: Option<String>,
    pub preferred_project_agent_identity_id: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransitionProductGenesis {
    pub id: String,
    pub expected_version: i64,
    pub lifecycle: ProductGenesisLifecycle,
    pub project_id: Option<String>,
    pub handoff_id: Option<String>,
    pub failure_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductGenesisStart {
    pub session: ProductGenesisSession,
    pub prompt: String,
}

#[derive(Clone)]
pub struct ProductGenesisService {
    store: Arc<dyn ProductGenesisStore>,
}

impl ProductGenesisService {
    pub fn new(store: Arc<dyn ProductGenesisStore>) -> Self {
        Self { store }
    }

    /// Construct the durable SQLite-backed Genesis service.  The adapter only
    /// touches the V071 chat tables and V072 Genesis table; chat admission and
    /// handoff orchestration remain owned by their normal services.
    pub fn for_sqlite(db: Arc<SqliteDb>) -> Self {
        Self::new(Arc::new(SqliteProductGenesisStore { db }))
    }

    /// Start exactly one Genesis session for an account.  `main_chat_id` is
    /// supplied by the Main Chat binding service; `None` is an explicit setup
    /// required result and does not write durable state or admit a turn.
    pub async fn start(
        &self,
        account_id: &str,
        main_chat_id: Option<&str>,
        maturity: ProductMaturity,
        initial_idea: Option<String>,
        preferred_project_agent_identity_id: Option<String>,
        context: GenesisPromptContext,
    ) -> Result<ProductGenesisStart> {
        let account_id = required_value("account id", account_id)?;
        let Some(main_chat_id) = main_chat_id
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            return Err(ServiceError::InvalidOperation {
                message: "Main Agent setup is required before starting Product Genesis".to_owned(),
            });
        };
        if !self
            .store
            .validate_main_chat(&account_id, main_chat_id)
            .await?
        {
            return Err(ServiceError::NotFound {
                entity: "main_agent_chat",
                id: main_chat_id.to_owned(),
            });
        }
        if let Some(active) = self.store.get_active(&account_id).await? {
            return Err(ServiceError::Conflict(format!(
                "Product Genesis session {} is already active",
                active.id
            )));
        }

        let initial_idea = initial_idea
            .map(|idea| bounded_text(&idea))
            .filter(|idea| !idea.is_empty());
        let prompt_context = GenesisPromptContext {
            initial_idea: initial_idea.clone(),
            ..context
        };
        let prompt = render_product_genesis_prompt(maturity, &prompt_context);
        let now = now_rfc3339();
        let session = self
            .store
            .create(NewProductGenesisSession {
                id: new_uuid_v4(),
                account_id,
                main_chat_id: main_chat_id.to_owned(),
                prompt_revision: PRODUCT_GENESIS_PROMPT_VERSION.to_owned(),
                prompt_body: prompt.clone(),
                maturity,
                initial_idea,
                preferred_project_agent_identity_id,
                created_at: now.clone(),
                updated_at: now,
            })
            .await?;
        Ok(ProductGenesisStart { session, prompt })
    }

    pub async fn get(&self, id: &str) -> Result<ProductGenesisSession> {
        self.store
            .get(id)
            .await?
            .ok_or_else(|| ServiceError::not_found("product_genesis_session", id.to_owned()))
    }

    pub async fn active(&self, account_id: &str) -> Result<Option<ProductGenesisSession>> {
        let account_id = required_value("account id", account_id)?;
        self.store.get_active(&account_id).await
    }

    pub async fn cancel(
        &self,
        id: &str,
        expected_version: i64,
        reason: Option<String>,
    ) -> Result<ProductGenesisSession> {
        let current = self.get(id).await?;
        validate_genesis_transition(current.lifecycle, ProductGenesisLifecycle::Cancelled)
            .map_err(|error| ServiceError::InvalidOperation {
                message: error.to_string(),
            })?;
        self.store.cancel(id, expected_version, reason).await
    }

    pub async fn transition(
        &self,
        input: TransitionProductGenesis,
    ) -> Result<ProductGenesisSession> {
        let current = self.get(&input.id).await?;
        validate_genesis_transition(current.lifecycle, input.lifecycle).map_err(|error| {
            ServiceError::InvalidOperation {
                message: error.to_string(),
            }
        })?;
        if input.lifecycle == ProductGenesisLifecycle::HandedOff
            && (input.project_id.is_none() || input.handoff_id.is_none())
        {
            return Err(ServiceError::InvalidOperation {
                message: "handed_off Genesis requires both Project and handoff ids".to_owned(),
            });
        }
        self.store.transition(input).await
    }

    pub async fn record_source_message(
        &self,
        id: &str,
        expected_version: i64,
        source_message_id: &str,
    ) -> Result<ProductGenesisSession> {
        let source_message_id = required_value("source message id", source_message_id)?;
        let current = self.get(id).await?;
        if !matches!(
            current.lifecycle,
            ProductGenesisLifecycle::Discovering | ProductGenesisLifecycle::ReadyForProject
        ) {
            return Err(ServiceError::InvalidOperation {
                message: "source messages can only be recorded on an active Genesis session"
                    .to_owned(),
            });
        }
        self.store
            .add_source_message(id, expected_version, &source_message_id)
            .await
    }

    pub async fn apply_guided_setup(
        &self,
        id: &str,
        expected_version: i64,
        maturity: Option<ProductMaturity>,
        preferred_project_agent_identity_id: Option<String>,
        provenance: Option<String>,
    ) -> Result<ProductGenesisSession> {
        let current = self.get(id).await?;
        if current.lifecycle != ProductGenesisLifecycle::Discovering {
            return Err(ServiceError::InvalidOperation {
                message: "guided setup can only be applied while discovering".to_owned(),
            });
        }
        if current.version != expected_version {
            return Err(ServiceError::Conflict(format!(
                "Product Genesis session version changed (expected {expected_version}, current {})",
                current.version
            )));
        }
        if maturity.is_none() && preferred_project_agent_identity_id.is_none() {
            return Err(ServiceError::InvalidOperation {
                message: "guided setup requires either maturity or preferred Project Agent"
                    .to_owned(),
            });
        }
        self.store
            .apply_guided_setup(
                id,
                expected_version,
                maturity,
                preferred_project_agent_identity_id,
                provenance,
            )
            .await
    }
}

/// SQLite implementation kept in the service crate until the shared DB chat
/// repository is complete.  It still uses the same pool/transaction boundary
/// as the rest of Forge and is fully durable across process restarts.
#[derive(Clone)]
pub struct SqliteProductGenesisStore {
    db: Arc<SqliteDb>,
}

#[async_trait]
impl ProductGenesisStore for SqliteProductGenesisStore {
    async fn validate_main_chat(&self, account_id: &str, main_chat_id: &str) -> Result<bool> {
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agent_chat
             JOIN account_main_agent_binding AS binding
               ON binding.account_id = agent_chat.account_id
              AND binding.state = 'active'
             WHERE agent_chat.id = ?
               AND agent_chat.account_id = ?
               AND agent_chat.kind = 'account_main'
               AND agent_chat.status = 'ready'",
        )
        .bind(main_chat_id)
        .bind(account_id)
        .fetch_one(self.db.pool())
        .await
        .map(|count| count > 0)
        .map_err(db_error)
    }

    async fn get(&self, id: &str) -> Result<Option<ProductGenesisSession>> {
        sqlx::query(
            "SELECT id, account_id, main_chat_id, prompt_revision, prompt_body, maturity,
                    initial_idea, lifecycle, source_message_ids_json,
                    preferred_project_agent_identity_id, charter_id,
                    charter_revision_id, charter_approval_id, charter_version,
                    project_id, handoff_id,
                    failure_reason, version, created_at, updated_at
             FROM product_genesis_session WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(self.db.pool())
        .await
        .map_err(db_error)
        .and_then(|row| row.map(map_genesis_row).transpose())
    }

    async fn get_active(&self, account_id: &str) -> Result<Option<ProductGenesisSession>> {
        sqlx::query(
            "SELECT id, account_id, main_chat_id, prompt_revision, prompt_body, maturity,
                    initial_idea, lifecycle, source_message_ids_json,
                    preferred_project_agent_identity_id, charter_id,
                    charter_revision_id, charter_approval_id, charter_version,
                    project_id, handoff_id,
                    failure_reason, version, created_at, updated_at
             FROM product_genesis_session
             WHERE account_id = ? AND lifecycle IN ('discovering', 'ready_for_project')
             ORDER BY created_at DESC, id DESC LIMIT 1",
        )
        .bind(account_id)
        .fetch_optional(self.db.pool())
        .await
        .map_err(db_error)
        .and_then(|row| row.map(map_genesis_row).transpose())
    }

    async fn create(&self, input: NewProductGenesisSession) -> Result<ProductGenesisSession> {
        // Session and its immutable chat-instruction overlay are one durable
        // admission.  A failed instruction insert must not leave a Genesis
        // session that the runner cannot reconstruct after restart.
        let mut transaction = db::begin_immediate(self.db.pool())
            .await
            .map_err(db_error)?;
        sqlx::query(
            "INSERT INTO product_genesis_session (
                id, account_id, main_chat_id, prompt_revision, prompt_body, maturity,
                initial_idea, lifecycle, source_message_ids_json,
                preferred_project_agent_identity_id, version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, 'discovering', '[]', ?, 1, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.account_id)
        .bind(&input.main_chat_id)
        .bind(&input.prompt_revision)
        .bind(&input.prompt_body)
        .bind(input.maturity.as_str())
        .bind(input.initial_idea.as_deref())
        .bind(input.preferred_project_agent_identity_id.as_deref())
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&mut *transaction)
        .await
        .map_err(db_error)?;

        let revision = sqlx::query_scalar::<_, i64>(
            "SELECT COALESCE(MAX(revision), 0) + 1
             FROM agent_chat_instruction_revision
             WHERE chat_id = ?",
        )
        .bind(&input.main_chat_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(db_error)?;
        sqlx::query(
            "INSERT INTO agent_chat_instruction_revision (
                id, chat_id, source_type, source_id, revision, body,
                content_guard_json, sensitivity, created_by_type, created_by_id,
                created_at
             ) VALUES (?, ?, 'native', ?, ?, ?, '{}', 'internal',
                       'product_genesis', ?, ?)",
        )
        .bind(new_uuid_v4())
        .bind(&input.main_chat_id)
        .bind(&input.id)
        .bind(revision)
        .bind(&input.prompt_body)
        .bind(&input.account_id)
        .bind(&input.created_at)
        .execute(&mut *transaction)
        .await
        .map_err(db_error)?;
        sqlx::query(
            "UPDATE agent_chat
             SET instruction_revision = ?, version = version + 1, updated_at = ?
             WHERE id = ?",
        )
        .bind(revision)
        .bind(&input.updated_at)
        .bind(&input.main_chat_id)
        .execute(&mut *transaction)
        .await
        .map_err(db_error)?;
        transaction.commit().await.map_err(db_error)?;
        self.get(&input.id)
            .await?
            .ok_or_else(|| ServiceError::not_found("product_genesis_session", input.id))
    }

    async fn cancel(
        &self,
        id: &str,
        expected_version: i64,
        reason: Option<String>,
    ) -> Result<ProductGenesisSession> {
        let result = sqlx::query(
            "UPDATE product_genesis_session
             SET lifecycle = 'cancelled', failure_reason = ?,
                 version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?
               AND lifecycle IN ('discovering', 'ready_for_project')",
        )
        .bind(reason.as_deref())
        .bind(now_rfc3339())
        .bind(id)
        .bind(expected_version)
        .execute(self.db.pool())
        .await
        .map_err(db_error)?;
        if result.rows_affected() == 0 {
            return Err(version_or_not_found(&self.db, id, expected_version).await?);
        }
        self.get(id)
            .await?
            .ok_or_else(|| ServiceError::not_found("product_genesis_session", id))
    }

    async fn transition(&self, input: TransitionProductGenesis) -> Result<ProductGenesisSession> {
        let target = input.lifecycle.as_str();
        let result = sqlx::query(
            "UPDATE product_genesis_session
             SET lifecycle = ?, project_id = COALESCE(?, project_id),
                 handoff_id = COALESCE(?, handoff_id), failure_reason = ?,
                 version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?
               AND ((lifecycle = 'discovering' AND ? IN ('ready_for_project', 'cancelled'))
                 OR (lifecycle = 'ready_for_project' AND ? IN ('handed_off', 'cancelled'))) ",
        )
        .bind(target)
        .bind(input.project_id.as_deref())
        .bind(input.handoff_id.as_deref())
        .bind(input.failure_reason.as_deref())
        .bind(now_rfc3339())
        .bind(&input.id)
        .bind(input.expected_version)
        .bind(target)
        .bind(target)
        .execute(self.db.pool())
        .await
        .map_err(db_error)?;
        if result.rows_affected() == 0 {
            return Err(version_or_not_found(&self.db, &input.id, input.expected_version).await?);
        }
        self.get(&input.id)
            .await?
            .ok_or_else(|| ServiceError::not_found("product_genesis_session", input.id))
    }

    async fn add_source_message(
        &self,
        id: &str,
        expected_version: i64,
        source_message_id: &str,
    ) -> Result<ProductGenesisSession> {
        let mut transaction = db::begin_immediate(self.db.pool())
            .await
            .map_err(db_error)?;
        let row = sqlx::query(
            "SELECT version, source_message_ids_json
             FROM product_genesis_session WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(db_error)?
        .ok_or_else(|| ServiceError::not_found("product_genesis_session", id))?;
        let version = row.try_get::<i64, _>("version").map_err(db_error)?;
        let mut source_ids = serde_json::from_str::<Vec<String>>(
            row.try_get::<String, _>("source_message_ids_json")
                .map_err(db_error)?
                .as_str(),
        )
        .map_err(|error| ServiceError::InvalidOperation {
            message: format!("invalid Genesis source references: {error}"),
        })?;
        if source_ids
            .iter()
            .any(|existing| existing == source_message_id)
        {
            transaction.commit().await.map_err(db_error)?;
            return self
                .get(id)
                .await?
                .ok_or_else(|| ServiceError::not_found("product_genesis_session", id));
        }
        if version != expected_version {
            return Err(ServiceError::Db(DbError::VersionConflict));
        }
        source_ids.push(source_message_id.to_owned());
        let result = sqlx::query(
            "UPDATE product_genesis_session
             SET source_message_ids_json = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?
               AND lifecycle IN ('discovering', 'ready_for_project')",
        )
        .bind(serde_json::to_string(&source_ids).map_err(|error| {
            ServiceError::InvalidOperation {
                message: format!("serialize Genesis source references: {error}"),
            }
        })?)
        .bind(now_rfc3339())
        .bind(id)
        .bind(expected_version)
        .execute(&mut *transaction)
        .await
        .map_err(db_error)?;
        if result.rows_affected() == 0 {
            return Err(ServiceError::Db(DbError::VersionConflict));
        }
        transaction.commit().await.map_err(db_error)?;
        self.get(id)
            .await?
            .ok_or_else(|| ServiceError::not_found("product_genesis_session", id))
    }

    async fn apply_guided_setup(
        &self,
        id: &str,
        expected_version: i64,
        maturity: Option<ProductMaturity>,
        preferred_project_agent_identity_id: Option<String>,
        _provenance: Option<String>,
    ) -> Result<ProductGenesisSession> {
        let now = now_rfc3339();
        let maturity_str = maturity.map(|m| m.as_str().to_owned());
        let changed = sqlx::query(
            "UPDATE product_genesis_session
             SET maturity = COALESCE(?, maturity),
                 preferred_project_agent_identity_id = COALESCE(?, preferred_project_agent_identity_id),
                 setup_applied_at = ?,
                 version = version + 1,
                 updated_at = ?
             WHERE id = ? AND version = ? AND lifecycle = 'discovering' AND setup_applied_at IS NULL",
        )
        .bind(maturity_str.as_deref())
        .bind(preferred_project_agent_identity_id.as_deref())
        .bind(&now)
        .bind(&now)
        .bind(id)
        .bind(expected_version)
        .execute(self.db.pool())
        .await
        .map_err(db_error)?;

        if changed.rows_affected() == 0 {
            let current = self.get(id).await?;
            let Some(current) = current else {
                return Err(ServiceError::not_found("product_genesis_session", id));
            };
            if current.version != expected_version {
                return Err(ServiceError::Conflict(format!(
                    "Product Genesis session version changed (expected {expected_version}, current {})",
                    current.version
                )));
            }
            if current.lifecycle != ProductGenesisLifecycle::Discovering {
                return Err(ServiceError::Conflict(format!(
                    "Product Genesis session is in {:?} state, guided setup can only be applied while discovering",
                    current.lifecycle
                )));
            }
            return Err(ServiceError::Conflict(
                "Product Genesis guided setup has already been applied for this session".to_owned(),
            ));
        }

        self.get(id)
            .await?
            .ok_or_else(|| ServiceError::not_found("product_genesis_session", id))
    }
}

fn db_error(error: sqlx::Error) -> ServiceError {
    let message = error.to_string();
    if message.contains("UNIQUE constraint failed: product_genesis_session") {
        return ServiceError::Conflict(
            "an active Product Genesis session already exists for this account/Main Chat"
                .to_owned(),
        );
    }
    ServiceError::Db(DbError::from(error))
}

async fn version_or_not_found(
    db: &SqliteDb,
    id: &str,
    expected_version: i64,
) -> Result<ServiceError> {
    let current =
        sqlx::query_scalar::<_, i64>("SELECT version FROM product_genesis_session WHERE id = ?")
            .bind(id)
            .fetch_optional(db.pool())
            .await
            .map_err(db_error)?;
    Ok(match current {
        None => ServiceError::not_found("product_genesis_session", id),
        Some(actual) if actual != expected_version => ServiceError::Db(DbError::VersionConflict),
        Some(_) => ServiceError::InvalidOperation {
            message: "Product Genesis lifecycle transition is no longer valid".to_owned(),
        },
    })
}

fn map_genesis_row(row: sqlx::sqlite::SqliteRow) -> Result<ProductGenesisSession> {
    let maturity = match row
        .try_get::<String, _>("maturity")
        .map_err(db_error)?
        .as_str()
    {
        "prototype" => ProductMaturity::Prototype,
        "mvp" => ProductMaturity::Mvp,
        "production" => ProductMaturity::Production,
        "critical" => ProductMaturity::Critical,
        value => {
            return Err(ServiceError::InvalidOperation {
                message: format!("invalid persisted Genesis maturity `{value}`"),
            });
        }
    };
    let lifecycle = match row
        .try_get::<String, _>("lifecycle")
        .map_err(db_error)?
        .as_str()
    {
        "discovering" => ProductGenesisLifecycle::Discovering,
        "ready_for_project" => ProductGenesisLifecycle::ReadyForProject,
        "handed_off" => ProductGenesisLifecycle::HandedOff,
        "cancelled" => ProductGenesisLifecycle::Cancelled,
        value => {
            return Err(ServiceError::InvalidOperation {
                message: format!("invalid persisted Genesis lifecycle `{value}`"),
            });
        }
    };
    let source_json = row
        .try_get::<String, _>("source_message_ids_json")
        .map_err(db_error)?;
    let source_message_ids =
        serde_json::from_str(&source_json).map_err(|error| ServiceError::InvalidOperation {
            message: format!("invalid persisted Genesis source references: {error}"),
        })?;
    Ok(ProductGenesisSession {
        id: row.try_get("id").map_err(db_error)?,
        account_id: row.try_get("account_id").map_err(db_error)?,
        main_chat_id: row.try_get("main_chat_id").map_err(db_error)?,
        prompt_revision: row.try_get("prompt_revision").map_err(db_error)?,
        maturity,
        initial_idea: row.try_get("initial_idea").map_err(db_error)?,
        lifecycle,
        source_message_ids,
        preferred_project_agent_identity_id: row
            .try_get("preferred_project_agent_identity_id")
            .map_err(db_error)?,
        charter_id: row.try_get("charter_id").map_err(db_error)?,
        charter_revision_id: row.try_get("charter_revision_id").map_err(db_error)?,
        charter_approval_id: row.try_get("charter_approval_id").map_err(db_error)?,
        charter_version: row.try_get("charter_version").map_err(db_error)?,
        project_id: row.try_get("project_id").map_err(db_error)?,
        handoff_id: row.try_get("handoff_id").map_err(db_error)?,
        failure_reason: row.try_get("failure_reason").map_err(db_error)?,
        version: row.try_get("version").map_err(db_error)?,
        created_at: row.try_get("created_at").map_err(db_error)?,
        updated_at: row.try_get("updated_at").map_err(db_error)?,
    })
}

fn required_value(field: &'static str, value: &str) -> Result<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ServiceError::InvalidOperation {
            message: format!("{field} must not be empty"),
        });
    }
    Ok(value.to_owned())
}

/// The maximum amount of caller-provided context included in one rendered
/// section.  Genesis context is bounded before it reaches the model, both to
/// keep the instruction stable and to prevent an unbounded initial idea from
/// becoming an accidental prompt channel.
const MAX_CONTEXT_CHARS: usize = 2_000;
const MAX_CONTEXT_ITEMS: usize = 8;

/// A bounded snapshot of discovery state used to render a Genesis revision.
/// The fields are facts/context for the protocol, not additional instructions.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GenesisPromptContext {
    pub current_understanding: String,
    pub assumptions: Vec<String>,
    pub decisions_still_required: Vec<String>,
    pub product_outline: String,
    pub observed_facts: Vec<String>,
    pub initial_idea: Option<String>,
}

/// Render the Product Genesis Main operating skill for a bounded discovery snapshot.
///
/// This function has no I/O and does not grant tools.  In particular, the
/// Main Agent policy must still issue the effective action catalog; the
/// wording below is an additional contract, not an authority mechanism.
pub fn render_product_genesis_prompt(
    maturity: ProductMaturity,
    context: &GenesisPromptContext,
) -> String {
    let mut current_understanding = bounded_text(&context.current_understanding);
    let product_outline = bounded_text(&context.product_outline);
    let initial_idea = context
        .initial_idea
        .as_deref()
        .map(bounded_text)
        .filter(|value| !value.is_empty());
    if current_understanding.is_empty() {
        current_understanding = initial_idea.clone().unwrap_or_default();
    }
    if !product_outline.is_empty() {
        if !current_understanding.is_empty() {
            current_understanding.push_str(" | Product outline: ");
        }
        current_understanding.push_str(&product_outline);
    }

    let operating_context = crate::MainOperatingSkillContext {
        lifecycle: ProductGenesisLifecycle::Discovering,
        maturity,
        current_understanding,
        observed_facts: bounded_items(&context.observed_facts),
        assumptions: bounded_items(&context.assumptions),
        decisions_still_required: bounded_items(&context.decisions_still_required),
        ..crate::MainOperatingSkillContext::default()
    };
    crate::render_main_operating_skill(&operating_context)
        .expect("discovering Product Genesis always activates the Main operating skill")
}

fn bounded_text(value: &str) -> String {
    let value = value.trim();
    value.chars().take(MAX_CONTEXT_CHARS).collect()
}

fn bounded_items(values: &[String]) -> Vec<String> {
    values
        .iter()
        .take(MAX_CONTEXT_ITEMS)
        .map(|value| bounded_text(value))
        .filter(|value| !value.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::HashMap, sync::Mutex};

    #[derive(Default)]
    struct MemoryStore {
        sessions: Mutex<HashMap<String, ProductGenesisSession>>,
    }

    #[async_trait]
    impl ProductGenesisStore for MemoryStore {
        async fn validate_main_chat(&self, _account_id: &str, _main_chat_id: &str) -> Result<bool> {
            Ok(true)
        }

        async fn get(&self, id: &str) -> Result<Option<ProductGenesisSession>> {
            Ok(self.sessions.lock().expect("store lock").get(id).cloned())
        }

        async fn get_active(&self, account_id: &str) -> Result<Option<ProductGenesisSession>> {
            Ok(self
                .sessions
                .lock()
                .expect("store lock")
                .values()
                .find(|session| {
                    session.account_id == account_id
                        && matches!(
                            session.lifecycle,
                            ProductGenesisLifecycle::Discovering
                                | ProductGenesisLifecycle::ReadyForProject
                        )
                })
                .cloned())
        }

        async fn create(&self, input: NewProductGenesisSession) -> Result<ProductGenesisSession> {
            let mut sessions = self.sessions.lock().expect("store lock");
            if sessions.values().any(|session| {
                session.account_id == input.account_id
                    && matches!(
                        session.lifecycle,
                        ProductGenesisLifecycle::Discovering
                            | ProductGenesisLifecycle::ReadyForProject
                    )
            }) {
                return Err(ServiceError::Conflict(
                    "active Product Genesis session exists".to_owned(),
                ));
            }
            let session = ProductGenesisSession {
                id: input.id,
                account_id: input.account_id,
                main_chat_id: input.main_chat_id,
                prompt_revision: input.prompt_revision,
                maturity: input.maturity,
                initial_idea: input.initial_idea,
                lifecycle: ProductGenesisLifecycle::Discovering,
                source_message_ids: Vec::new(),
                preferred_project_agent_identity_id: input.preferred_project_agent_identity_id,
                charter_id: None,
                charter_revision_id: None,
                charter_approval_id: None,
                charter_version: 0,
                project_id: None,
                handoff_id: None,
                failure_reason: None,
                version: 1,
                created_at: input.created_at,
                updated_at: input.updated_at,
            };
            sessions.insert(session.id.clone(), session.clone());
            Ok(session)
        }

        async fn cancel(
            &self,
            id: &str,
            expected_version: i64,
            reason: Option<String>,
        ) -> Result<ProductGenesisSession> {
            let mut sessions = self.sessions.lock().expect("store lock");
            let session = sessions
                .get_mut(id)
                .ok_or_else(|| ServiceError::not_found("product_genesis_session", id))?;
            if session.version != expected_version {
                return Err(ServiceError::Conflict("version conflict".to_owned()));
            }
            session.lifecycle = ProductGenesisLifecycle::Cancelled;
            session.failure_reason = reason;
            session.version += 1;
            Ok(session.clone())
        }

        async fn transition(
            &self,
            input: TransitionProductGenesis,
        ) -> Result<ProductGenesisSession> {
            let mut sessions = self.sessions.lock().expect("store lock");
            let session = sessions
                .get_mut(&input.id)
                .ok_or_else(|| ServiceError::not_found("product_genesis_session", input.id))?;
            if session.version != input.expected_version {
                return Err(ServiceError::Conflict("version conflict".to_owned()));
            }
            session.lifecycle = input.lifecycle;
            session.project_id = input.project_id;
            session.handoff_id = input.handoff_id;
            session.failure_reason = input.failure_reason;
            session.version += 1;
            Ok(session.clone())
        }

        async fn add_source_message(
            &self,
            id: &str,
            expected_version: i64,
            source_message_id: &str,
        ) -> Result<ProductGenesisSession> {
            let mut sessions = self.sessions.lock().expect("store lock");
            let session = sessions
                .get_mut(id)
                .ok_or_else(|| ServiceError::not_found("product_genesis_session", id))?;
            if !session
                .source_message_ids
                .iter()
                .any(|existing| existing == source_message_id)
            {
                if session.version != expected_version {
                    return Err(ServiceError::Db(DbError::VersionConflict));
                }
                session
                    .source_message_ids
                    .push(source_message_id.to_owned());
                session.version += 1;
            }
            Ok(session.clone())
        }

        async fn apply_guided_setup(
            &self,
            id: &str,
            expected_version: i64,
            maturity: Option<ProductMaturity>,
            preferred_project_agent_identity_id: Option<String>,
            _provenance: Option<String>,
        ) -> Result<ProductGenesisSession> {
            let mut sessions = self.sessions.lock().expect("store lock");
            let session = sessions
                .get_mut(id)
                .ok_or_else(|| ServiceError::not_found("product_genesis_session", id))?;
            if session.version != expected_version {
                return Err(ServiceError::Conflict(format!(
                    "Product Genesis session version changed (expected {expected_version}, current {})",
                    session.version
                )));
            }
            if session.lifecycle != ProductGenesisLifecycle::Discovering {
                return Err(ServiceError::Conflict(format!(
                    "Product Genesis session is in {:?} state, guided setup can only be applied while discovering",
                    session.lifecycle
                )));
            }
            if let Some(m) = maturity {
                session.maturity = m;
            }
            if let Some(agent) = preferred_project_agent_identity_id {
                session.preferred_project_agent_identity_id = Some(agent);
            }
            session.version += 1;
            Ok(session.clone())
        }
    }

    fn context() -> GenesisPromptContext {
        GenesisPromptContext {
            current_understanding: "A bounded product idea".to_owned(),
            assumptions: vec!["The user has a narrow first audience".to_owned()],
            decisions_still_required: vec![
                "Who is the first user?".to_owned(),
                "What is the smallest useful outcome?".to_owned(),
                "This third question must not be rendered".to_owned(),
            ],
            product_outline: "Problem -> outcome -> first workflow".to_owned(),
            observed_facts: vec!["The user supplied an initial idea".to_owned()],
            initial_idea: Some("A small, useful tool".to_owned()),
        }
    }

    #[test]
    fn every_maturity_uses_the_versioned_contract() {
        for maturity in [
            ProductMaturity::Prototype,
            ProductMaturity::Mvp,
            ProductMaturity::Production,
            ProductMaturity::Critical,
        ] {
            let rendered = render_product_genesis_prompt(maturity, &context());
            assert!(rendered
                .starts_with("Forge Main Agent — Project Discovery and Portfolio Protocol v2\n"));
            assert!(rendered.contains("Current understanding:"));
            assert!(rendered.contains("### Assumptions"));
            assert!(rendered.contains("### Decisions still required"));
            assert!(rendered.contains("Product outline:"));
            assert!(rendered.contains("Maturity:"));
            assert!(rendered.contains("at most two high-information questions"));
            assert!(rendered.contains("whether a Project or handoff was created"));
            assert!(rendered.contains("Main has no Task"));
            assert!(!rendered.contains("This third question must not be rendered"));
        }
    }

    #[test]
    fn maturity_changes_depth_requirements() {
        let rendered = [
            ProductMaturity::Prototype,
            ProductMaturity::Mvp,
            ProductMaturity::Production,
            ProductMaturity::Critical,
        ]
        .map(|maturity| render_product_genesis_prompt(maturity, &GenesisPromptContext::default()));

        for (index, prompt) in rendered.iter().enumerate() {
            for (other_index, other_prompt) in rendered.iter().enumerate() {
                if index != other_index {
                    assert_ne!(prompt, other_prompt);
                }
            }
        }
        assert!(rendered[0].contains("Maturity: prototype"));
        assert!(rendered[1].contains("Maturity: mvp"));
        assert!(rendered[2].contains("Maturity: production"));
        assert!(rendered[3].contains("Maturity: critical"));
    }

    #[test]
    fn caller_context_is_bounded() {
        let context = GenesisPromptContext {
            current_understanding: "x".repeat(MAX_CONTEXT_CHARS + 100),
            assumptions: (0..MAX_CONTEXT_ITEMS + 5)
                .map(|index| format!("assumption-{index}"))
                .collect(),
            ..GenesisPromptContext::default()
        };

        let rendered = render_product_genesis_prompt(ProductMaturity::Mvp, &context);
        assert!(rendered.contains(&"x".repeat(MAX_CONTEXT_CHARS)));
        assert!(!rendered.contains(&"x".repeat(MAX_CONTEXT_CHARS + 1)));
        assert!(rendered.contains("assumption-7"));
        assert!(!rendered.contains("assumption-8"));
    }

    #[test]
    fn empty_context_remains_explicit_and_safe() {
        let rendered =
            render_product_genesis_prompt(ProductMaturity::Mvp, &GenesisPromptContext::default());
        assert!(rendered.contains("- (none recorded)"));
        assert!(rendered.contains("Current understanding: (none recorded)"));
        assert!(rendered.contains("Main has no Task"));
    }

    #[test]
    fn lifecycle_is_forward_only_and_terminal() {
        use ProductGenesisLifecycle::*;

        assert!(validate_genesis_transition(Discovering, ReadyForProject).is_ok());
        assert!(validate_genesis_transition(Discovering, Cancelled).is_ok());
        assert!(validate_genesis_transition(ReadyForProject, HandedOff).is_ok());
        assert!(validate_genesis_transition(ReadyForProject, Cancelled).is_ok());
        for terminal in [HandedOff, Cancelled] {
            for next in [Discovering, ReadyForProject, HandedOff, Cancelled] {
                assert!(validate_genesis_transition(terminal, next).is_err());
            }
        }
        assert!(validate_genesis_transition(Discovering, HandedOff).is_err());
    }

    #[tokio::test]
    async fn service_requires_main_binding_and_allows_one_active_session() {
        let store = Arc::new(MemoryStore::default());
        let service = ProductGenesisService::new(store);
        let error = service
            .start(
                "account-1",
                None,
                ProductMaturity::Mvp,
                None,
                None,
                GenesisPromptContext::default(),
            )
            .await
            .expect_err("missing Main binding is setup-required");
        assert!(error.to_string().contains("setup is required"));

        let first = service
            .start(
                "account-1",
                Some("main-chat-1"),
                ProductMaturity::Mvp,
                Some("  build a useful thing  ".to_owned()),
                None,
                GenesisPromptContext::default(),
            )
            .await
            .expect("first session starts");
        assert_eq!(
            first.session.lifecycle,
            ProductGenesisLifecycle::Discovering
        );
        assert_eq!(
            first.session.initial_idea.as_deref(),
            Some("build a useful thing")
        );
        assert!(first
            .prompt
            .starts_with("Forge Main Agent — Project Discovery and Portfolio Protocol v2\n"));

        let ready = service
            .transition(TransitionProductGenesis {
                id: first.session.id.clone(),
                expected_version: first.session.version,
                lifecycle: ProductGenesisLifecycle::ReadyForProject,
                project_id: None,
                handoff_id: None,
                failure_reason: None,
            })
            .await
            .expect("an approved Charter advances discovery exactly once");
        assert_eq!(ready.lifecycle, ProductGenesisLifecycle::ReadyForProject);

        let error = service
            .start(
                "account-1",
                Some("main-chat-1"),
                ProductMaturity::Mvp,
                None,
                None,
                GenesisPromptContext::default(),
            )
            .await
            .expect_err("second active session is rejected");
        assert!(error.to_string().contains("already active"));
    }

    #[tokio::test]
    async fn service_cancel_and_handoff_require_optimistic_version_and_ids() {
        let store = Arc::new(MemoryStore::default());
        let service = ProductGenesisService::new(store);
        let first = service
            .start(
                "account-1",
                Some("main-chat-1"),
                ProductMaturity::Mvp,
                None,
                None,
                GenesisPromptContext::default(),
            )
            .await
            .expect("session starts");

        let ready = service
            .transition(TransitionProductGenesis {
                id: first.session.id.clone(),
                expected_version: first.session.version,
                lifecycle: ProductGenesisLifecycle::ReadyForProject,
                project_id: None,
                handoff_id: None,
                failure_reason: None,
            })
            .await
            .expect("session becomes ready");

        let error = service
            .transition(TransitionProductGenesis {
                id: ready.id.clone(),
                expected_version: ready.version,
                lifecycle: ProductGenesisLifecycle::HandedOff,
                project_id: None,
                handoff_id: None,
                failure_reason: None,
            })
            .await
            .expect_err("handoff must include result ids");
        assert!(error
            .to_string()
            .contains("requires both Project and handoff"));

        let handed_off = service
            .transition(TransitionProductGenesis {
                id: ready.id.clone(),
                expected_version: ready.version,
                lifecycle: ProductGenesisLifecycle::HandedOff,
                project_id: Some("project-1".to_owned()),
                handoff_id: Some("handoff-1".to_owned()),
                failure_reason: None,
            })
            .await
            .expect("handoff records ids");
        assert_eq!(handed_off.lifecycle, ProductGenesisLifecycle::HandedOff);
        assert_eq!(handed_off.project_id.as_deref(), Some("project-1"));

        let error = service
            .cancel(&handed_off.id, handed_off.version, None)
            .await
            .expect_err("terminal handoff cannot be cancelled");
        assert!(error
            .to_string()
            .contains("invalid Product Genesis transition"));
    }
}
