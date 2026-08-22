//! Transport-neutral Main/Product Genesis commands.
//!
//! This module is the single mutation boundary for a Main Charter draft.  REST
//! and the native Main tool both supply an already authenticated principal and
//! an idempotency envelope; the service derives the account/Main scope,
//! validates the Genesis lifecycle and Charter state, and commits the Charter
//! shell/revision, domain event, and command receipt in one repository
//! transaction.  A Charter draft is deliberately not an `AgentAction`: the
//! operation is directly admitted by the Main policy and has no independent
//! approval step.

use std::{collections::BTreeMap, sync::Arc};

use api_types::{
    PrincipalKind, ProductGenesisLifecycle, ProductGenesisSession, ProductMaturity,
    ProjectCharterContent, ProjectCharterReadiness, ProjectMode, ProvenanceRef, RevisionProvenance,
};
use db::{
    new_uuid_v4, now_rfc3339, AdmitAgentChatTurn, AgentChat, AgentChatMessageAuthorType,
    AgentChatMessageRepo, AgentChatMessageStatus, AgentChatRepo, AgentChatTransactionRepo,
    AgentChatTurnJobRepo, CommandReceipt, CommandReceiptRepo, CreateAgentChatMessage,
    CreateAgentChatTurnJob, CreateCommandReceipt, CreateDomainEvent, CreateProjectCharter,
    CreateProjectCharterRevision, CreateProjectCharterRevisionAtomically, DomainEventRepo,
    ProjectCharterRecord, ProjectCharterRevisionRecord, ProjectOrchestrationRepo, SqliteDb,
};
use forge_agent_host::{
    CanonicalScope, CanonicalScopeType, MAIN_CHARTER_DRAFT_OPERATION, MAIN_GENESIS_START_OPERATION,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::Row;

use crate::{
    agent_chat_policy::guard_agent_chat_content,
    agent_turn_admission::{
        content_digest, AgentTurnAdmissionInput, AgentTurnAdmissionService, AgentTurnTrigger,
        PreparedAgentTurnAdmission,
    },
    evaluate_project_charter_readiness, render_and_digest_charter, render_product_genesis_prompt,
    AuthorizationProvenance, CommandContext, CommandPrincipal, CommandScope, CommandScopeType,
    ExpectedCommandState, GenesisPromptContext, NewCommandContext, ProductGenesisService, Result,
    ServiceError, CHARTER_READINESS_POLICY_VERSION, MAIN_OPERATING_SKILL_KEY,
};

const CHARTER_SCHEMA_VERSION: &str = "forge.project-charter/v1";

/// Transport-neutral input for starting Product Genesis. Native callers omit
/// `initial_idea`; the command derives it from the currently leased Main turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MainGenesisStartRequest {
    #[serde(default)]
    pub maturity: Option<ProductMaturity>,
    #[serde(default)]
    pub initial_idea: Option<String>,
    #[serde(default)]
    pub preferred_project_agent_identity_id: Option<String>,
}

/// The authenticated principal and source shape accepted by the shared start
/// command. The native variant intentionally carries no message or turn id;
/// both are resolved from the live Main Chat lease by the server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MainGenesisStartPrincipal {
    User {
        user_id: String,
    },
    MainAgent {
        identity_id: String,
        scope: CanonicalScope,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MainGenesisStartCommandInput {
    pub principal: MainGenesisStartPrincipal,
    pub request: MainGenesisStartRequest,
    pub idempotency_key: String,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub causation_depth: i64,
    pub policy_result: String,
    pub requested_permission: String,
}

/// Frozen command outcome shared by REST and the native Main tool.
#[derive(Debug, Clone, PartialEq)]
pub struct MainGenesisStartResult {
    pub receipt_id: String,
    pub event_id: String,
    pub replayed: bool,
    pub session: ProductGenesisSession,
    pub main_chat_id: String,
    pub source_message_id: String,
    pub source_turn_id: Option<String>,
    pub admitted_turn_id: String,
    pub control_transfer: bool,
    pub result: Value,
}

#[derive(Debug, Clone)]
struct GenesisStartSource {
    message_id: String,
    turn_id: Option<String>,
    content: String,
    source_correlation_id: Option<String>,
    source_causation_depth: i64,
    create_visible_message: bool,
}

#[derive(Debug, Serialize)]
struct GenesisStartDigestInput<'a> {
    maturity: ProductMaturity,
    initial_idea: &'a str,
    preferred_project_agent_identity_id: Option<&'a str>,
    main_chat_id: &'a str,
    source_message_id: Option<&'a str>,
    source_turn_id: Option<&'a str>,
}

/// Typed request accepted by every Main/Genesis Charter-draft adapter.
/// Transport-specific envelopes are converted into this type before the
/// command service is called.  The service, not the adapter, owns all
/// lifecycle, rendering, digest, and version checks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MainGenesisCharterDraftRequest {
    #[serde(default)]
    pub genesis_session_id: Option<String>,
    pub charter_id: String,
    #[serde(default)]
    pub expected_charter_version: Option<i64>,
    #[serde(default)]
    pub base_revision_id: Option<String>,
    pub project_mode: ProjectMode,
    pub maturity: ProductMaturity,
    pub content: ProjectCharterContent,
    #[serde(default)]
    pub change_summary: Option<String>,
    #[serde(default)]
    pub source_refs: Vec<ProvenanceRef>,
    pub provenance: RevisionProvenance,
    #[serde(default)]
    pub rendered_view: Option<String>,
    #[serde(default)]
    pub render_version: Option<String>,
    #[serde(default)]
    pub content_digest: Option<String>,
    #[serde(default)]
    pub render_digest: Option<String>,
}

/// The only principals admitted to the Main/Genesis draft command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MainGenesisDraftPrincipal {
    /// REST/user adapters pass the authenticated account user id.
    User { user_id: String },
    /// Native adapters pass the bound Main identity and the host-derived
    /// scope.  The account is resolved from the identity owner and the Main
    /// Chat row; neither account id nor authority is accepted from payload.
    MainAgent {
        identity_id: String,
        scope: CanonicalScope,
    },
}

/// Server-owned authorization and delivery envelope for a direct command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MainGenesisDraftCommandInput {
    pub principal: MainGenesisDraftPrincipal,
    pub request: MainGenesisCharterDraftRequest,
    pub idempotency_key: String,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub causation_depth: i64,
    pub policy_result: String,
    pub requested_permission: String,
}

/// Frozen result returned by the service.  The `revision` and `readiness`
/// values are stable across a replay; `receipt_id` and `event_id` identify
/// the same durable commit for both adapters.
#[derive(Debug, Clone, PartialEq)]
pub struct MainGenesisCharterDraftResult {
    pub receipt_id: String,
    pub event_id: String,
    pub revision: ProjectCharterRevisionRecord,
    pub charter: ProjectCharterRecord,
    pub readiness: ProjectCharterReadiness,
    pub result: Value,
}

#[derive(Clone)]
pub struct MainGenesisCommandService {
    db: Arc<SqliteDb>,
}

impl MainGenesisCommandService {
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self { db }
    }

    /// Start Product Genesis through the same receipt-backed boundary for an
    /// authenticated REST user or the currently leased Main Agent turn.
    /// Native callers never supply their source message/turn identifiers.
    pub async fn start(
        &self,
        mut input: MainGenesisStartCommandInput,
    ) -> Result<MainGenesisStartResult> {
        let draft_principal = match &input.principal {
            MainGenesisStartPrincipal::User { user_id } => MainGenesisDraftPrincipal::User {
                user_id: user_id.clone(),
            },
            MainGenesisStartPrincipal::MainAgent { identity_id, scope } => {
                MainGenesisDraftPrincipal::MainAgent {
                    identity_id: identity_id.clone(),
                    scope: scope.clone(),
                }
            }
        };
        let (principal, account_id, canonical_scope, actor_type, actor_id) =
            self.resolve_principal(&draft_principal).await?;
        let idempotency_key = required_value("idempotency key", &input.idempotency_key)?;
        let correlation_id = required_value("correlation id", &input.correlation_id)?;
        input.request.preferred_project_agent_identity_id = input
            .request
            .preferred_project_agent_identity_id
            .filter(|value| !value.trim().is_empty());
        let maturity = input.request.maturity.unwrap_or(ProductMaturity::Mvp);

        let chat = self.main_chat_for_account(&account_id).await?;
        if let MainGenesisStartPrincipal::MainAgent { scope, .. } = &input.principal {
            if scope.scope_type == CanonicalScopeType::AgentChat && scope.scope_id != chat.id {
                return Err(ServiceError::AuthorizationDenied {
                    message: "Main identity is not operating in the account Main Chat".to_owned(),
                });
            }
        }

        // An identity-only lookup is used solely to reconstruct immutable
        // source ids for digest verification after response loss. The exact
        // digest-aware lookup below remains the replay authority.
        let existing_receipt = CommandReceiptRepo::get_command_receipt_by_identity(
            &*self.db,
            principal.principal_type(),
            principal.principal_id(),
            canonical_scope.scope_type().as_str(),
            canonical_scope.scope_id(),
            MAIN_GENESIS_START_OPERATION,
            &idempotency_key,
        )
        .await?;
        let source = self
            .resolve_genesis_start_source(
                &input.principal,
                &chat,
                input.request.initial_idea.as_deref(),
                existing_receipt.as_ref(),
            )
            .await?;
        let guarded = guard_agent_chat_content(&source.content)?;
        // The guarded, server-derived source is canonical for both adapters.
        // Native payload text can therefore never replace the visible user
        // message that caused the control transfer.
        input.request.initial_idea = Some(guarded.content.clone());
        let command_context = CommandContext::from_authorized_input(
            NewCommandContext {
                principal: principal.clone(),
                canonical_scope: canonical_scope.clone(),
                operation: MAIN_GENESIS_START_OPERATION.to_owned(),
                idempotency_key: idempotency_key.clone(),
                expected_state: ExpectedCommandState::default(),
                authorization_provenance: Some(AuthorizationProvenance {
                    policy_result: input.policy_result.clone(),
                    policy_revision: None,
                    policy_digest: None,
                    requested_permission: Some(input.requested_permission.clone()),
                }),
                action_provenance: None,
                correlation_id: correlation_id.clone(),
                causation_id: input.causation_id.clone(),
                causation_depth: input.causation_depth,
            },
            &GenesisStartDigestInput {
                maturity,
                initial_idea: &guarded.content,
                preferred_project_agent_identity_id: input
                    .request
                    .preferred_project_agent_identity_id
                    .as_deref(),
                main_chat_id: &chat.id,
                source_message_id: source.turn_id.as_ref().map(|_| source.message_id.as_str()),
                source_turn_id: source.turn_id.as_deref(),
            },
        )
        .map_err(|error| {
            ServiceError::invalid_operation(format!(
                "serialize Product Genesis start input digest: {error}"
            ))
        })?;

        if let Some(receipt) = CommandReceiptRepo::get_command_receipt(
            &*self.db,
            command_context.principal().principal_type(),
            command_context.principal().principal_id(),
            command_context.canonical_scope().scope_type().as_str(),
            command_context.canonical_scope().scope_id(),
            command_context.operation(),
            command_context.idempotency_key(),
            command_context.input_digest(),
        )
        .await?
        {
            return self.replay_start(receipt).await;
        }

        self.authorize_current_principal(&draft_principal, &account_id)
            .await?;
        if input.policy_result != "allowed" {
            return Err(ServiceError::AuthorizationDenied {
                message: "Product Genesis start policy did not admit this command".to_owned(),
            });
        }
        if input.requested_permission != "propose_discovery" {
            return Err(ServiceError::AuthorizationDenied {
                message: "Product Genesis start requires propose_discovery".to_owned(),
            });
        }

        let trigger = if source.turn_id.is_some() {
            AgentTurnTrigger::GenesisContinuation
        } else {
            AgentTurnTrigger::UserMessage
        };
        let turn_dedupe_key = format!("genesis.start:{account_id}:{idempotency_key}:turn");
        let turn_causation_id = source
            .source_correlation_id
            .as_deref()
            .or(input.causation_id.as_deref());
        let turn_causation_depth = if source.turn_id.is_some() {
            source.source_causation_depth.saturating_add(1).min(16)
        } else {
            input.causation_depth
        };
        let prepared = self
            .prepare_genesis_turn(
                &chat,
                &input.principal,
                trigger,
                &turn_dedupe_key,
                &guarded.content,
                turn_causation_id,
                turn_causation_depth,
            )
            .await?;

        let now = now_rfc3339();
        let session_id = new_uuid_v4();
        let turn_id = new_uuid_v4();
        let event_id = new_uuid_v4();
        let receipt_id = new_uuid_v4();
        let prompt = render_product_genesis_prompt(
            maturity,
            &GenesisPromptContext {
                initial_idea: Some(guarded.content.clone()),
                ..GenesisPromptContext::default()
            },
        );
        let predicted_result = json!({
            "operation": MAIN_GENESIS_START_OPERATION,
            "session_id": session_id,
            "main_chat_id": chat.id,
            "source_message_id": source.message_id,
            "source_turn_id": source.turn_id,
            "admitted_turn_id": turn_id,
            "control_transfer": source.turn_id.is_some(),
            "receipt_id": receipt_id,
            "event_id": event_id,
        });
        let outcome_json = serde_json::to_string(&predicted_result).map_err(|error| {
            ServiceError::invalid_operation(format!(
                "serialize Product Genesis start outcome: {error}"
            ))
        })?;

        let mut transaction = db::begin_immediate(self.db.pool()).await?;
        if let Some(receipt) = CommandReceiptRepo::get_command_receipt_in_tx(
            &*self.db,
            &mut transaction,
            command_context.principal().principal_type(),
            command_context.principal().principal_id(),
            command_context.canonical_scope().scope_type().as_str(),
            command_context.canonical_scope().scope_id(),
            command_context.operation(),
            command_context.idempotency_key(),
            command_context.input_digest(),
        )
        .await?
        {
            transaction.rollback().await?;
            return self.replay_start(receipt).await;
        }

        let active_session: Option<String> = sqlx::query_scalar(
            "SELECT id FROM product_genesis_session
             WHERE account_id = ? AND lifecycle IN ('discovering', 'ready_for_project')
             ORDER BY created_at DESC, id DESC LIMIT 1",
        )
        .bind(&account_id)
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(active_session) = active_session {
            return Err(ServiceError::ProductGenesisActiveSession {
                session_id: active_session,
            });
        }
        if let Some(source_turn_id) = source.turn_id.as_deref() {
            let live_source: Option<i64> = sqlx::query_scalar(
                "SELECT 1 FROM agent_chat_turn_job
                 WHERE id = ? AND chat_id = ? AND triggering_message_id = ?
                   AND responder_identity_id = ? AND status = 'leased'",
            )
            .bind(source_turn_id)
            .bind(&chat.id)
            .bind(&source.message_id)
            .bind(&actor_id)
            .fetch_optional(&mut *transaction)
            .await?;
            if live_source.is_none() {
                return Err(ServiceError::Conflict(
                    "the Main turn that requested Product Genesis is no longer active".to_owned(),
                ));
            }
        }

        let source_message_ids_json = serde_json::to_string(&vec![source.message_id.clone()])
            .map_err(|error| {
                ServiceError::invalid_operation(format!(
                    "serialize Product Genesis source message: {error}"
                ))
            })?;
        sqlx::query(
            "INSERT INTO product_genesis_session (
                id, account_id, main_chat_id, prompt_revision, prompt_body, maturity,
                initial_idea, lifecycle, source_message_ids_json,
                preferred_project_agent_identity_id, version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, 'discovering', ?, ?, 1, ?, ?)",
        )
        .bind(&session_id)
        .bind(&account_id)
        .bind(&chat.id)
        .bind(crate::PRODUCT_GENESIS_PROMPT_VERSION)
        .bind(&prompt)
        .bind(maturity.as_str())
        .bind(&guarded.content)
        .bind(&source_message_ids_json)
        .bind(input.request.preferred_project_agent_identity_id.as_deref())
        .bind(&now)
        .bind(&now)
        .execute(&mut *transaction)
        .await?;

        let instruction_revision = sqlx::query_scalar::<_, i64>(
            "SELECT COALESCE(MAX(revision), 0) + 1
             FROM agent_chat_instruction_revision WHERE chat_id = ?",
        )
        .bind(&chat.id)
        .fetch_one(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO agent_chat_instruction_revision (
                id, chat_id, source_type, source_id, revision, body,
                content_guard_json, sensitivity, created_by_type, created_by_id,
                created_at
             ) VALUES (?, ?, 'native', ?, ?, ?, '{}', 'internal',
                       'product_genesis', ?, ?)",
        )
        .bind(new_uuid_v4())
        .bind(&chat.id)
        .bind(&session_id)
        .bind(instruction_revision)
        .bind(&prompt)
        .bind(&account_id)
        .bind(&now)
        .execute(&mut *transaction)
        .await?;
        let updated = sqlx::query(
            "UPDATE agent_chat
             SET instruction_revision = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND kind = 'account_main' AND account_id = ? AND status = 'ready'",
        )
        .bind(instruction_revision)
        .bind(&now)
        .bind(&chat.id)
        .bind(&account_id)
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(ServiceError::Conflict(
                "Main Agent setup changed while Product Genesis was starting".to_owned(),
            ));
        }

        let bare_turn = CreateAgentChatTurnJob {
            id: turn_id.clone(),
            chat_id: chat.id.clone(),
            triggering_message_id: source.message_id.clone(),
            responder_identity_id: String::new(),
            profile_id: String::new(),
            responder_binding_id: None,
            responder_binding_version: None,
            responder_identity_version: None,
            profile_version: None,
            operating_skill_revision_id: None,
            policy_revision: None,
            policy_digest: None,
            permission_policy_digest: None,
            tool_policy_digest: None,
            admission_digest: None,
            canonical_scope_provenance_json: None,
            canonical_scope_type: String::new(),
            canonical_scope_id: String::new(),
            dedupe_key: String::new(),
            max_attempts: 3,
            correlation_id: correlation_id.clone(),
            causation_id: None,
            causation_depth: 0,
            created_at: now.clone(),
            updated_at: now.clone(),
        };
        let frozen_turn = prepared.apply_to_turn(bare_turn)?;
        if source.create_visible_message {
            AgentChatTransactionRepo::admit_agent_chat_turn_in_tx(
                &*self.db,
                &mut transaction,
                AdmitAgentChatTurn {
                    message: CreateAgentChatMessage {
                        id: source.message_id.clone(),
                        chat_id: chat.id.clone(),
                        sequence: chat.message_count,
                        author_type: AgentChatMessageAuthorType::User,
                        author_id: Some(actor_id.clone()),
                        content: guarded.content.clone(),
                        content_guard_json: guarded.guard_json.clone(),
                        sensitivity: guarded.sensitivity.clone(),
                        status: AgentChatMessageStatus::Complete,
                        outcome: None,
                        model: None,
                        profile_id: None,
                        session_id: None,
                        context_manifest_id: None,
                        token_usage_json: None,
                        duration_ms: None,
                        error: None,
                        correlation_id: correlation_id.clone(),
                        causation_id: input.causation_id.clone(),
                        handoff_id: None,
                        source_type: "native".to_owned(),
                        source_id: Some(session_id.clone()),
                        source_message_id: None,
                        source_room_id: None,
                        source_conversation_id: None,
                        source_sequence: None,
                        source_metadata_json: json!({
                            "operation": MAIN_GENESIS_START_OPERATION,
                            "session_id": session_id,
                        })
                        .to_string(),
                        created_at: now.clone(),
                    },
                    turn: frozen_turn,
                },
            )
            .await?;
        } else {
            AgentChatTransactionRepo::admit_agent_chat_continuation_in_tx(
                &*self.db,
                &mut transaction,
                frozen_turn,
            )
            .await?;
        }

        let event = DomainEventRepo::append_event_in_tx(
            &*self.db,
            &mut transaction,
            &CreateDomainEvent {
                id: event_id.clone(),
                event_type: "product_genesis.started".to_owned(),
                entity_type: "product_genesis_session".to_owned(),
                entity_id: session_id.clone(),
                actor_type: actor_type.clone(),
                actor_id: Some(actor_id.clone()),
                scope_type: "account".to_owned(),
                scope_id: account_id.clone(),
                correlation_id: correlation_id.clone(),
                causation_id: input.causation_id.clone(),
                causation_depth: input.causation_depth,
                dedupe_key: Some(format!("genesis.start:{receipt_id}:event")),
                payload_json: outcome_json.clone(),
                created_at: now.clone(),
            },
        )
        .await?;
        let receipt = CommandReceiptRepo::create_command_receipt_in_tx(
            &*self.db,
            &mut transaction,
            CreateCommandReceipt {
                id: receipt_id,
                principal_type: command_context.principal().principal_type().to_owned(),
                principal_id: command_context.principal().principal_id().to_owned(),
                scope_type: command_context
                    .canonical_scope()
                    .scope_type()
                    .as_str()
                    .to_owned(),
                scope_id: command_context.canonical_scope().scope_id().to_owned(),
                operation: command_context.operation().to_owned(),
                idempotency_key: command_context.idempotency_key().to_owned(),
                input_digest: command_context.input_digest().to_owned(),
                policy_result: input.policy_result,
                correlation_id,
                causation_id: input.causation_id,
                causation_depth: input.causation_depth,
                event_id: event.id,
                agent_action_execution_id: None,
                outcome_json,
                committed_at: now,
            },
        )
        .await?;
        transaction.commit().await?;

        self.start_result(receipt, false).await
    }

    async fn main_chat_for_account(&self, account_id: &str) -> Result<AgentChat> {
        let chat_id = sqlx::query_scalar::<_, String>(
            "SELECT chat.id
             FROM agent_chat AS chat
             JOIN account_main_agent_binding AS binding
               ON binding.account_id = chat.account_id
              AND binding.state = 'active'
             WHERE chat.account_id = ? AND chat.kind = 'account_main'
               AND chat.status = 'ready'
             ORDER BY chat.created_at ASC, chat.id ASC LIMIT 1",
        )
        .bind(account_id)
        .fetch_optional(self.db.pool())
        .await?
        .ok_or_else(|| ServiceError::ExecutionSetupRequired {
            message: "Main Agent setup is required before starting Product Genesis".to_owned(),
            requirements: vec![api_types::SetupRequirement::new("main_agent")],
        })?;
        AgentChatRepo::get_agent_chat(&*self.db, &chat_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent_chat", chat_id))
    }

    async fn resolve_genesis_start_source(
        &self,
        principal: &MainGenesisStartPrincipal,
        chat: &AgentChat,
        requested_initial_idea: Option<&str>,
        receipt: Option<&CommandReceipt>,
    ) -> Result<GenesisStartSource> {
        if let Some(receipt) = receipt {
            let outcome: Value = serde_json::from_str(&receipt.outcome_json)
                .map_err(|_| ServiceError::Db(db::DbError::IdempotencyConflict))?;
            let message_id = outcome
                .get("source_message_id")
                .and_then(Value::as_str)
                .ok_or(ServiceError::Db(db::DbError::IdempotencyConflict))?
                .to_owned();
            let turn_id = outcome
                .get("source_turn_id")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let message = AgentChatMessageRepo::get_agent_chat_message(&*self.db, &message_id)
                .await?
                .filter(|message| message.chat_id == chat.id)
                .ok_or(ServiceError::Db(db::DbError::IdempotencyConflict))?;
            let source_job = match turn_id.as_deref() {
                Some(turn_id) => Some(
                    AgentChatTurnJobRepo::get_agent_chat_turn_job(&*self.db, turn_id)
                        .await?
                        .filter(|job| {
                            job.chat_id == chat.id && job.triggering_message_id == message_id
                        })
                        .ok_or(ServiceError::Db(db::DbError::IdempotencyConflict))?,
                ),
                None => None,
            };
            let content = match principal {
                MainGenesisStartPrincipal::User { .. } => requested_initial_idea
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .unwrap_or("I want to start a new Project.")
                    .to_owned(),
                MainGenesisStartPrincipal::MainAgent { .. } => message.content,
            };
            return Ok(GenesisStartSource {
                message_id,
                turn_id,
                content,
                source_correlation_id: source_job.as_ref().map(|job| job.correlation_id.clone()),
                source_causation_depth: source_job.as_ref().map_or(0, |job| job.causation_depth),
                create_visible_message: false,
            });
        }

        match principal {
            MainGenesisStartPrincipal::User { .. } => Ok(GenesisStartSource {
                message_id: new_uuid_v4(),
                turn_id: None,
                content: requested_initial_idea
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .unwrap_or("I want to start a new Project.")
                    .to_owned(),
                source_correlation_id: None,
                source_causation_depth: 0,
                create_visible_message: true,
            }),
            MainGenesisStartPrincipal::MainAgent { identity_id, .. } => {
                let row = sqlx::query(
                    "SELECT id, triggering_message_id, correlation_id, causation_depth
                     FROM agent_chat_turn_job
                     WHERE chat_id = ? AND responder_identity_id = ? AND status = 'leased'
                     ORDER BY created_at DESC, id DESC LIMIT 1",
                )
                .bind(&chat.id)
                .bind(identity_id)
                .fetch_optional(self.db.pool())
                .await?
                .ok_or_else(|| ServiceError::AuthorizationDenied {
                    message: "genesis.start requires the currently leased Main Chat turn"
                        .to_owned(),
                })?;
                let turn_id: String = row.try_get("id")?;
                let message_id: String = row.try_get("triggering_message_id")?;
                let message = AgentChatMessageRepo::get_agent_chat_message(&*self.db, &message_id)
                    .await?
                    .filter(|message| {
                        message.chat_id == chat.id
                            && matches!(message.author_type, AgentChatMessageAuthorType::User)
                    })
                    .ok_or_else(|| {
                        ServiceError::invalid_operation(
                            "genesis.start requires a visible Main Chat user message",
                        )
                    })?;
                Ok(GenesisStartSource {
                    message_id,
                    turn_id: Some(turn_id),
                    content: message.content,
                    source_correlation_id: Some(row.try_get("correlation_id")?),
                    source_causation_depth: row.try_get("causation_depth")?,
                    create_visible_message: false,
                })
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn prepare_genesis_turn(
        &self,
        chat: &AgentChat,
        principal: &MainGenesisStartPrincipal,
        trigger: AgentTurnTrigger,
        dedupe_key: &str,
        content: &str,
        causation_id: Option<&str>,
        causation_depth: i64,
    ) -> Result<PreparedAgentTurnAdmission> {
        let admission = AgentTurnAdmissionService::new(Arc::clone(&self.db));
        let mut responder = admission.resolve(chat).await?;
        if let MainGenesisStartPrincipal::MainAgent { identity_id, .. } = principal {
            if responder.identity_id()? != identity_id {
                return Err(ServiceError::AuthorizationDenied {
                    message: "genesis.start caller is not the resolved Main responder".to_owned(),
                });
            }
        }
        let discovery_revision = sqlx::query_scalar::<_, String>(
            "SELECT current_revision_id FROM operating_skill
             WHERE id = ? AND lifecycle = 'active'",
        )
        .bind(MAIN_OPERATING_SKILL_KEY)
        .fetch_optional(self.db.pool())
        .await?
        .ok_or_else(|| {
            ServiceError::invalid_operation(
                "Product Genesis operating skill is unavailable for turn admission",
            )
        })?;
        responder.operating_skill_revision = Some(discovery_revision);
        responder.require_frozen()?;
        let trigger_content_digest = content_digest(content)?;
        let admission_digest = admission.digest(&AgentTurnAdmissionInput {
            trigger,
            dedupe_key,
            content_digest: &trigger_content_digest,
            causation_id,
            causation_depth,
            responder: &responder,
            source_responder: None,
        })?;
        Ok(PreparedAgentTurnAdmission {
            canonical_scope_provenance_json: responder.provenance_json()?,
            responder,
            trigger,
            dedupe_key: dedupe_key.to_owned(),
            content_digest: trigger_content_digest,
            causation_id: causation_id.map(str::to_owned),
            causation_depth,
            admission_digest,
        })
    }

    async fn replay_start(&self, receipt: CommandReceipt) -> Result<MainGenesisStartResult> {
        self.start_result(receipt, true).await
    }

    async fn start_result(
        &self,
        receipt: CommandReceipt,
        replayed: bool,
    ) -> Result<MainGenesisStartResult> {
        let mut result: Value = serde_json::from_str(&receipt.outcome_json)
            .map_err(|_| ServiceError::Db(db::DbError::IdempotencyConflict))?;
        let string_field = |field: &str| {
            result
                .get(field)
                .and_then(Value::as_str)
                .map(str::to_owned)
                .ok_or(ServiceError::Db(db::DbError::IdempotencyConflict))
        };
        let session_id = string_field("session_id")?;
        let main_chat_id = string_field("main_chat_id")?;
        let source_message_id = string_field("source_message_id")?;
        let admitted_turn_id = string_field("admitted_turn_id")?;
        let source_turn_id = result
            .get("source_turn_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let control_transfer = result
            .get("control_transfer")
            .and_then(Value::as_bool)
            .ok_or(ServiceError::Db(db::DbError::IdempotencyConflict))?;
        let session = ProductGenesisService::for_sqlite(Arc::clone(&self.db))
            .get(&session_id)
            .await?;
        AgentChatTurnJobRepo::get_agent_chat_turn_job(&*self.db, &admitted_turn_id)
            .await?
            .filter(|turn| {
                turn.chat_id == main_chat_id && turn.triggering_message_id == source_message_id
            })
            .ok_or_else(|| {
                ServiceError::Conflict(
                    "Product Genesis start receipt lost its continuation turn".to_owned(),
                )
            })?;
        result["replayed"] = Value::Bool(replayed);
        Ok(MainGenesisStartResult {
            receipt_id: receipt.id,
            event_id: receipt.event_id,
            replayed,
            session,
            main_chat_id,
            source_message_id,
            source_turn_id,
            admitted_turn_id,
            control_transfer,
            result,
        })
    }

    /// Execute a direct Main Charter draft.  Receipt lookup intentionally
    /// happens before mutable Genesis/Charter checks so a lost response or a
    /// changed current lifecycle cannot rerun a committed command.
    pub async fn execute(
        &self,
        input: MainGenesisDraftCommandInput,
    ) -> Result<MainGenesisCharterDraftResult> {
        let (principal, account_id, canonical_scope, actor_type, actor_id) =
            self.resolve_principal(&input.principal).await?;
        let mut request = normalize_request(input.request.clone())?;
        // The authenticated principal is the only correct Charter author, and
        // the server has just resolved it.  A Main Agent cannot know its own
        // identity id, so treating its guess as an authorization failure sent
        // it hunting for a permission problem that does not exist.  Stamping
        // the resolved principal makes a supplied author impossible to spoof
        // rather than merely refused, and keeps it inside the receipt digest
        // computed below.
        request.provenance.author.kind = match actor_type.as_str() {
            "user" => PrincipalKind::User,
            _ => PrincipalKind::Agent,
        };
        request.provenance.author.id = actor_id.clone();
        let request = request;
        let command_context =
            self.command_context(&principal, canonical_scope, &request, &input)?;

        if let Some(receipt) = CommandReceiptRepo::get_command_receipt(
            &*self.db,
            command_context.principal().principal_type(),
            command_context.principal().principal_id(),
            command_context.canonical_scope().scope_type().as_str(),
            command_context.canonical_scope().scope_id(),
            command_context.operation(),
            command_context.idempotency_key(),
            command_context.input_digest(),
        )
        .await?
        {
            return self.replay(receipt).await;
        }

        self.authorize_current_principal(&input.principal, &account_id)
            .await?;

        if input.policy_result != "allowed" {
            return Err(ServiceError::AuthorizationDenied {
                message: "Main Charter draft policy did not admit this command".to_owned(),
            });
        }
        if input.requested_permission != "propose_discovery" {
            return Err(ServiceError::AuthorizationDenied {
                message: "Main Charter draft requires the propose_discovery permission".to_owned(),
            });
        }

        let (session, charter, revision, readiness, receipt) = self
            .materialize(
                &request,
                &account_id,
                &actor_type,
                &actor_id,
                &command_context,
                &input,
            )
            .await?;
        let result = draft_result_json(&session, &revision, &readiness);
        Ok(MainGenesisCharterDraftResult {
            receipt_id: receipt.id,
            event_id: receipt.event_id,
            revision,
            charter,
            readiness,
            result,
        })
    }

    async fn resolve_principal(
        &self,
        principal: &MainGenesisDraftPrincipal,
    ) -> Result<(CommandPrincipal, String, CommandScope, String, String)> {
        match principal {
            MainGenesisDraftPrincipal::User { user_id } => {
                let user_id = required_value("authenticated user id", user_id)?;
                // The authenticated REST layer proves the user token.  The
                // command still resolves its Main scope from the durable
                // Genesis session below, so a caller cannot choose another
                // account through the request body.
                Ok((
                    CommandPrincipal {
                        principal_type: "user".to_owned(),
                        principal_id: user_id.clone(),
                    },
                    user_id.clone(),
                    CommandScope {
                        scope_type: CommandScopeType::Account,
                        scope_id: user_id.clone(),
                    },
                    "user".to_owned(),
                    user_id,
                ))
            }
            MainGenesisDraftPrincipal::MainAgent { identity_id, scope } => {
                let identity_id = required_value("Main identity id", identity_id)?;
                let row = sqlx::query(
                    "SELECT owner_id, paused, archived_at
                     FROM agent_identity WHERE id = ?",
                )
                .bind(&identity_id)
                .fetch_optional(self.db.pool())
                .await?
                .ok_or_else(|| ServiceError::not_found("agent_identity", identity_id.clone()))?;
                let owner_id: Option<String> = row.try_get("owner_id")?;
                let account_id = owner_id.ok_or_else(|| ServiceError::AuthorizationDenied {
                    message: "Main identity is not bound to an account".to_owned(),
                })?;
                let scope_account = match scope.scope_type {
                    CanonicalScopeType::Account => {
                        if scope.scope_id != account_id {
                            return Err(ServiceError::AuthorizationDenied {
                                message: "Main identity does not own the requested account scope"
                                    .to_owned(),
                            });
                        }
                        account_id.clone()
                    }
                    CanonicalScopeType::AgentChat => {
                        let chat =
                            sqlx::query("SELECT kind, account_id FROM agent_chat WHERE id = ?")
                                .bind(&scope.scope_id)
                                .fetch_optional(self.db.pool())
                                .await?
                                .ok_or_else(|| {
                                    ServiceError::not_found("agent_chat", scope.scope_id.clone())
                                })?;
                        let kind: String = chat.try_get("kind")?;
                        let chat_account: Option<String> = chat.try_get("account_id")?;
                        if kind != "account_main"
                            || chat_account.as_deref() != Some(account_id.as_str())
                        {
                            return Err(ServiceError::AuthorizationDenied {
                                message: "Main identity is not actively bound to the Main Chat"
                                    .to_owned(),
                            });
                        }
                        account_id.clone()
                    }
                    _ => {
                        return Err(ServiceError::AuthorizationDenied {
                            message: "Main Charter drafts require an account or Main Chat scope"
                                .to_owned(),
                        });
                    }
                };
                Ok((
                    CommandPrincipal {
                        principal_type: "agent".to_owned(),
                        principal_id: identity_id.clone(),
                    },
                    scope_account.clone(),
                    CommandScope {
                        scope_type: CommandScopeType::Account,
                        scope_id: scope_account,
                    },
                    "agent".to_owned(),
                    identity_id,
                ))
            }
        }
    }

    async fn authorize_current_principal(
        &self,
        principal: &MainGenesisDraftPrincipal,
        account_id: &str,
    ) -> Result<()> {
        let MainGenesisDraftPrincipal::MainAgent { identity_id, scope } = principal else {
            return Ok(());
        };
        let eligible: Option<i64> = sqlx::query_scalar(
            "SELECT 1
             FROM agent_identity AS identity
             JOIN account_main_agent_binding AS binding
               ON binding.identity_id = identity.id
              AND binding.account_id = identity.owner_id
              AND binding.state = 'active'
             JOIN agent_chat AS chat
               ON chat.account_id = binding.account_id
              AND chat.kind = 'account_main'
             WHERE identity.id = ?
               AND identity.owner_id = ?
               AND identity.paused = 0
               AND identity.archived_at IS NULL
               AND (? <> 'agent_chat' OR chat.id = ?)
             LIMIT 1",
        )
        .bind(identity_id)
        .bind(account_id)
        .bind(if scope.scope_type == CanonicalScopeType::AgentChat {
            "agent_chat"
        } else {
            "account"
        })
        .bind(&scope.scope_id)
        .fetch_optional(self.db.pool())
        .await?;
        if eligible.is_none() {
            return Err(ServiceError::AuthorizationDenied {
                message: "Main identity is not the active account Main binding".to_owned(),
            });
        }
        Ok(())
    }

    fn command_context(
        &self,
        principal: &CommandPrincipal,
        canonical_scope: CommandScope,
        request: &MainGenesisCharterDraftRequest,
        input: &MainGenesisDraftCommandInput,
    ) -> Result<CommandContext> {
        let expected_version = request.expected_charter_version.unwrap_or(1);
        CommandContext::from_authorized_input(
            NewCommandContext {
                principal: principal.clone(),
                canonical_scope,
                operation: MAIN_CHARTER_DRAFT_OPERATION.to_owned(),
                idempotency_key: required_value("idempotency key", &input.idempotency_key)?,
                expected_state: ExpectedCommandState {
                    versions: BTreeMap::from([("charter".to_owned(), expected_version)]),
                    digests: request
                        .base_revision_id
                        .as_ref()
                        .zip(request.provenance.source_refs.first())
                        .map(|_| BTreeMap::new())
                        .unwrap_or_default(),
                },
                authorization_provenance: Some(AuthorizationProvenance {
                    policy_result: input.policy_result.clone(),
                    policy_revision: None,
                    policy_digest: None,
                    requested_permission: Some(input.requested_permission.clone()),
                }),
                action_provenance: None,
                correlation_id: required_value("correlation id", &input.correlation_id)?,
                causation_id: input.causation_id.clone(),
                causation_depth: input.causation_depth,
            },
            request,
        )
        .map_err(|error| {
            ServiceError::invalid_operation(format!("serialize Main command input digest: {error}"))
        })
    }

    async fn replay(&self, receipt: CommandReceipt) -> Result<MainGenesisCharterDraftResult> {
        let outcome: Value = serde_json::from_str(&receipt.outcome_json)
            .map_err(|_| ServiceError::Db(db::DbError::IdempotencyConflict))?;
        let revision_id = outcome
            .get("revision_id")
            .and_then(Value::as_str)
            .ok_or(ServiceError::Db(db::DbError::IdempotencyConflict))?;
        let revision =
            ProjectOrchestrationRepo::get_project_charter_revision(&*self.db, revision_id)
                .await?
                .ok_or_else(|| {
                    ServiceError::Conflict("Main Charter receipt lost its revision".to_owned())
                })?;
        let charter =
            ProjectOrchestrationRepo::get_project_charter(&*self.db, &revision.charter_id)
                .await?
                .ok_or_else(|| {
                    ServiceError::Conflict("Main Charter receipt lost its Charter".to_owned())
                })?;
        let readiness = outcome
            .get("readiness")
            .cloned()
            .ok_or(ServiceError::Db(db::DbError::IdempotencyConflict))
            .and_then(|value| {
                serde_json::from_value(value)
                    .map_err(|_| ServiceError::Db(db::DbError::IdempotencyConflict))
            })?;
        Ok(MainGenesisCharterDraftResult {
            receipt_id: receipt.id,
            event_id: receipt.event_id,
            revision,
            charter,
            readiness,
            result: outcome,
        })
    }

    async fn materialize(
        &self,
        request: &MainGenesisCharterDraftRequest,
        account_id: &str,
        actor_type: &str,
        actor_id: &str,
        command_context: &CommandContext,
        input: &MainGenesisDraftCommandInput,
    ) -> Result<(
        api_types::ProductGenesisSession,
        ProjectCharterRecord,
        ProjectCharterRevisionRecord,
        ProjectCharterReadiness,
        CommandReceipt,
    )> {
        let session_id = match request.genesis_session_id.as_deref() {
            Some(id) => id.to_owned(),
            None => sqlx::query_scalar::<_, String>(
                "SELECT id FROM product_genesis_session
                 WHERE account_id = ? AND lifecycle IN ('discovering', 'ready_for_project')
                 ORDER BY updated_at DESC, id DESC LIMIT 1",
            )
            .bind(account_id)
            .fetch_optional(self.db.pool())
            .await?
            .ok_or_else(|| {
                ServiceError::not_found("product_genesis_session", account_id.to_owned())
            })?,
        };
        let session = crate::ProductGenesisService::for_sqlite(Arc::clone(&self.db))
            .get(&session_id)
            .await?;
        if session.account_id != account_id {
            return Err(ServiceError::NotFound {
                entity: "product_genesis_session",
                id: session_id,
            });
        }
        if matches!(
            session.lifecycle,
            ProductGenesisLifecycle::HandedOff | ProductGenesisLifecycle::Cancelled
        ) {
            return Err(ServiceError::invalid_operation(
                "a Charter cannot be drafted after Genesis handoff or cancellation",
            ));
        }
        if request.maturity != session.maturity {
            return Err(ServiceError::Conflict(
                "Charter maturity must match the Product Genesis session".to_owned(),
            ));
        }

        let expected_charter_id = session.charter_id.as_deref();
        if let Some(existing_charter_id) = expected_charter_id {
            if existing_charter_id != request.charter_id {
                return Err(ServiceError::Conflict(
                    "Charter draft target does not match the Genesis Charter".to_owned(),
                ));
            }
        }
        if request.content.identity.maturity != request.maturity {
            return Err(ServiceError::Conflict(
                "Charter identity maturity must match the requested Charter maturity".to_owned(),
            ));
        }

        let creating_charter = session.charter_id.is_none();
        let charter = match session.charter_id.as_deref() {
            Some(charter_id) => {
                let charter = ProjectOrchestrationRepo::get_project_charter_for_account(
                    &*self.db, charter_id, account_id,
                )
                .await?
                .ok_or_else(|| ServiceError::not_found("project_charter", charter_id))?;
                if charter.project_id.is_some() {
                    return Err(ServiceError::invalid_operation(
                        "an attached Charter is owned by the Project Agent and cannot be drafted in Main",
                    ));
                }
                charter
            }
            None => {
                let now = now_rfc3339();
                ProjectCharterRecord {
                    id: request.charter_id.clone(),
                    account_id: account_id.to_owned(),
                    genesis_session_id: Some(session.id.clone()),
                    project_id: None,
                    current_draft_revision_id: None,
                    current_approved_revision_id: None,
                    project_mode: request.project_mode.as_str().to_owned(),
                    maturity: request.maturity.as_str().to_owned(),
                    lifecycle: "draft".to_owned(),
                    version: 1,
                    created_at: now.clone(),
                    updated_at: now,
                }
            }
        };
        if charter.project_mode != request.project_mode.as_str()
            || charter.maturity != request.maturity.as_str()
        {
            return Err(ServiceError::Conflict(
                "Charter mode or maturity changed since the draft command was admitted".to_owned(),
            ));
        }
        let expected_charter_version = request.expected_charter_version.unwrap_or(charter.version);
        if charter.version != expected_charter_version {
            return Err(ServiceError::Db(db::DbError::VersionConflict));
        }
        let previous = match request.base_revision_id.as_deref() {
            Some(base_id) => {
                let record =
                    ProjectOrchestrationRepo::get_project_charter_revision(&*self.db, base_id)
                        .await?
                        .filter(|record| record.charter_id == charter.id)
                        .ok_or_else(|| {
                            ServiceError::not_found("project_charter_revision", base_id)
                        })?;
                if charter.current_draft_revision_id.as_deref() != Some(base_id) {
                    return Err(ServiceError::Db(db::DbError::VersionConflict));
                }
                Some(record)
            }
            None if charter.current_draft_revision_id.is_none() => None,
            None => {
                return Err(ServiceError::invalid_operation(
                    "base_revision_id is required when replacing an existing Charter draft",
                ));
            }
        };
        let rendered = render_and_digest_charter(&request.content);
        if request
            .rendered_view
            .as_deref()
            .is_some_and(|value| value != rendered.rendered_view)
        {
            return Err(ServiceError::Conflict(
                "rendered Charter view does not match the server renderer".to_owned(),
            ));
        }
        if request
            .render_version
            .as_deref()
            .is_some_and(|value| value != rendered.render_version)
        {
            return Err(ServiceError::Conflict(
                "Charter render version is stale".to_owned(),
            ));
        }
        if request
            .content_digest
            .as_deref()
            .is_some_and(|value| value != rendered.content_digest)
        {
            return Err(ServiceError::Conflict(
                "Charter content digest does not match canonical content".to_owned(),
            ));
        }
        if request
            .render_digest
            .as_deref()
            .is_some_and(|value| value != rendered.render_digest)
        {
            return Err(ServiceError::Conflict(
                "Charter render digest does not match canonical content".to_owned(),
            ));
        }
        let source_refs = if request.source_refs.is_empty() {
            request.provenance.source_refs.as_slice()
        } else {
            request.source_refs.as_slice()
        };
        let source_refs_json = source_refs_with_command(source_refs, command_context)?;
        let change_summary = request
            .change_summary
            .clone()
            .unwrap_or_else(|| request.provenance.change_summary.clone());
        let now = now_rfc3339();
        let revision_input = CreateProjectCharterRevision {
            id: new_uuid_v4(),
            charter_id: charter.id.clone(),
            expected_charter_version,
            project_mode: request.project_mode.as_str().to_owned(),
            maturity: request.maturity.as_str().to_owned(),
            base_revision: previous.as_ref().map(|record| record.revision).unwrap_or(0),
            base_revision_id: previous.as_ref().map(|record| record.id.clone()),
            lifecycle: "proposed".to_owned(),
            schema_version: CHARTER_SCHEMA_VERSION.to_owned(),
            render_version: rendered.render_version,
            content_json: serde_json::to_string(&request.content).map_err(|error| {
                ServiceError::invalid_operation(format!("serialize Charter content: {error}"))
            })?,
            rendered_view: rendered.rendered_view,
            change_summary,
            author_type: actor_type.to_owned(),
            author_id: Some(actor_id.to_owned()),
            source_message_id: None,
            source_turn_job_id: None,
            source_refs_json,
            content_digest: rendered.content_digest,
            rendered_digest: rendered.render_digest,
            created_at: now.clone(),
            command_receipt: None,
            action_execution: None,
        };
        let readiness = evaluate_project_charter_readiness(
            &request.content,
            request.project_mode,
            request.maturity,
            CHARTER_READINESS_POLICY_VERSION,
            &revision_input.created_at,
        );
        let predicted_revision = previous
            .as_ref()
            .map(|record| record.revision.saturating_add(1))
            .unwrap_or(1);
        let predicted_result = json!({
            "operation": MAIN_CHARTER_DRAFT_OPERATION,
            "genesis_session_id": session.id,
            "charter_id": revision_input.charter_id,
            "revision_id": revision_input.id,
            "revision": predicted_revision,
            "content_digest": revision_input.content_digest,
            "render_digest": revision_input.rendered_digest,
            "readiness": readiness,
        });
        let result_json = serde_json::to_string(&predicted_result).map_err(|error| {
            ServiceError::invalid_operation(format!("serialize Main Charter result: {error}"))
        })?;
        let receipt_input = CreateCommandReceipt {
            id: new_uuid_v4(),
            principal_type: command_context.principal().principal_type().to_owned(),
            principal_id: command_context.principal().principal_id().to_owned(),
            scope_type: command_context
                .canonical_scope()
                .scope_type()
                .as_str()
                .to_owned(),
            scope_id: command_context.canonical_scope().scope_id().to_owned(),
            operation: command_context.operation().to_owned(),
            idempotency_key: command_context.idempotency_key().to_owned(),
            input_digest: command_context.input_digest().to_owned(),
            policy_result: input.policy_result.clone(),
            correlation_id: command_context.correlation_id().to_owned(),
            causation_id: command_context.causation_id.clone(),
            causation_depth: command_context.causation_depth,
            event_id: String::new(),
            agent_action_execution_id: None,
            outcome_json: result_json,
            committed_at: now,
        };
        let revision = if creating_charter {
            ProjectOrchestrationRepo::create_project_charter_revision_atomically(
                &*self.db,
                CreateProjectCharterRevisionAtomically {
                    project_id: None,
                    genesis_session_id: Some(session.id.clone()),
                    account_id: account_id.to_owned(),
                    charter: CreateProjectCharter {
                        id: charter.id.clone(),
                        account_id: account_id.to_owned(),
                        genesis_session_id: Some(session.id.clone()),
                        project_mode: request.project_mode.as_str().to_owned(),
                        maturity: request.maturity.as_str().to_owned(),
                        created_at: charter.created_at.clone(),
                        updated_at: charter.updated_at.clone(),
                    },
                    revision: CreateProjectCharterRevision {
                        command_receipt: Some(receipt_input.clone()),
                        ..revision_input
                    },
                    command_receipt: Some(receipt_input.clone()),
                    action_execution: None,
                },
            )
            .await?
        } else {
            ProjectOrchestrationRepo::create_project_charter_revision(
                &*self.db,
                CreateProjectCharterRevision {
                    command_receipt: Some(receipt_input.clone()),
                    ..revision_input
                },
            )
            .await?
        };
        // The characterization suite models a lost response after the single
        // repository transaction has committed.  Keep this seam test-only so
        // the direct command remains one durable commit in production.
        #[cfg(test)]
        if crate::test_support::take_after_domain_commit(command_context.idempotency_key()) {
            return Err(ServiceError::Conflict(
                "characterization failpoint after Main Charter commit".to_owned(),
            ));
        }
        let charter =
            ProjectOrchestrationRepo::get_project_charter(&*self.db, &revision.charter_id)
                .await?
                .ok_or_else(|| {
                    ServiceError::not_found("project_charter", revision.charter_id.clone())
                })?;
        let receipt = CommandReceiptRepo::get_command_receipt(
            &*self.db,
            command_context.principal().principal_type(),
            command_context.principal().principal_id(),
            command_context.canonical_scope().scope_type().as_str(),
            command_context.canonical_scope().scope_id(),
            command_context.operation(),
            command_context.idempotency_key(),
            command_context.input_digest(),
        )
        .await?
        .ok_or_else(|| {
            ServiceError::Conflict("Main Charter command receipt was not committed".to_owned())
        })?;
        if let Err(error) = crate::append_system_chat_message(
            &self.db,
            &session.main_chat_id,
            &format!("charter-proposal:{}", revision.id),
            &format!(
                "Charter proposal: {}",
                request.content.identity.working_name
            ),
        )
        .await
        {
            tracing::warn!(%error, revision_id = %revision.id, "Charter proposal chat anchor failed");
        }
        Ok((session, charter, revision, readiness, receipt))
    }
}

fn normalize_request(
    mut request: MainGenesisCharterDraftRequest,
) -> Result<MainGenesisCharterDraftRequest> {
    if request.charter_id.trim().is_empty() {
        return Err(ServiceError::invalid_operation("charter_id is required"));
    }
    if request.provenance.change_summary.trim().is_empty() {
        return Err(ServiceError::invalid_operation(
            "Charter provenance change_summary is required",
        ));
    }
    if request.provenance.author.id.trim().is_empty() {
        return Err(ServiceError::invalid_operation(
            "Charter provenance author id is required",
        ));
    }
    if request
        .expected_charter_version
        .is_some_and(|version| version <= 0)
    {
        return Err(ServiceError::invalid_operation(
            "expected_charter_version must be positive",
        ));
    }
    request.genesis_session_id = request
        .genesis_session_id
        .filter(|value| !value.trim().is_empty());
    Ok(request)
}

fn source_refs_with_command(
    source_refs: &[ProvenanceRef],
    context: &CommandContext,
) -> Result<String> {
    let mut values = source_refs
        .iter()
        .map(serde_json::to_value)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| {
            ServiceError::invalid_operation(format!("serialize Charter provenance: {error}"))
        })?;
    values.push(json!({
        "source_kind": "system",
        "source_id": context.idempotency_key(),
        "label": "main_genesis_charter_command",
    }));
    serde_json::to_string(&values).map_err(|error| {
        ServiceError::invalid_operation(format!("serialize Charter provenance: {error}"))
    })
}

fn draft_result_json(
    session: &api_types::ProductGenesisSession,
    revision: &ProjectCharterRevisionRecord,
    readiness: &ProjectCharterReadiness,
) -> Value {
    json!({
        "operation": MAIN_CHARTER_DRAFT_OPERATION,
        "genesis_session_id": session.id,
        "charter_id": revision.charter_id,
        "revision_id": revision.id,
        "revision": revision.revision,
        "content_digest": revision.content_digest,
        "render_digest": revision.rendered_digest,
        "readiness": readiness,
    })
}

fn required_value(field: &'static str, value: &str) -> Result<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ServiceError::invalid_operation(format!(
            "{field} must not be empty"
        )));
    }
    Ok(value.to_owned())
}
