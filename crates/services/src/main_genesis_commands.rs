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
    PrincipalKind, ProductGenesisLifecycle, ProductMaturity, ProjectCharterContent,
    ProjectCharterReadiness, ProjectMode, ProvenanceRef, RevisionProvenance,
};
use db::{
    new_uuid_v4, now_rfc3339, CommandReceipt, CommandReceiptRepo, CreateCommandReceipt,
    CreateProjectCharter, CreateProjectCharterRevision, CreateProjectCharterRevisionAtomically,
    ProjectCharterRecord, ProjectCharterRevisionRecord, ProjectOrchestrationRepo, SqliteDb,
};
use forge_agent_host::{CanonicalScope, CanonicalScopeType, MAIN_CHARTER_DRAFT_OPERATION};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::Row;

use crate::{
    evaluate_project_charter_readiness, render_and_digest_charter, AuthorizationProvenance,
    CommandContext, CommandPrincipal, CommandScope, CommandScopeType, ExpectedCommandState,
    NewCommandContext, Result, ServiceError, CHARTER_READINESS_POLICY_VERSION,
};

const CHARTER_SCHEMA_VERSION: &str = "forge.project-charter/v1";

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

    /// Execute a direct Main Charter draft.  Receipt lookup intentionally
    /// happens before mutable Genesis/Charter checks so a lost response or a
    /// changed current lifecycle cannot rerun a committed command.
    pub async fn execute(
        &self,
        input: MainGenesisDraftCommandInput,
    ) -> Result<MainGenesisCharterDraftResult> {
        let (principal, account_id, canonical_scope, actor_type, actor_id) =
            self.resolve_principal(&input.principal).await?;
        let request = normalize_request(input.request.clone())?;
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
        if request.provenance.author.id != actor_id
            || !matches!(
                (actor_type, request.provenance.author.kind),
                ("user", PrincipalKind::User) | ("agent", PrincipalKind::Agent)
            )
        {
            return Err(ServiceError::AuthorizationDenied {
                message: "Charter provenance must identify the authenticated Main principal"
                    .to_owned(),
            });
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
