//! Typed materialization for Main Agent orchestration proposals.
//!
//! Main orchestration tools intentionally create an `AgentAction` proposal
//! first.  This module is the only service boundary which may turn the
//! proposal into a Charter revision, a Charter projection, or the atomic
//! Charter-backed Project handoff.  The generic action executor must not be
//! used for these operations because accepting an arbitrary result there
//! would make a successful ledger row without doing the domain work.

use std::sync::Arc;

use api_types::{
    ProductGenesisLifecycle, ProductMaturity, ProjectCharterContent, ProjectMode, ProvenanceRef,
    RevisionProvenance,
};
use db::{
    new_uuid_v4, now_rfc3339, AgentAction, AgentActionExecution, AgentActionExecutionStatus,
    AgentActionPolicyResult, AgentActionStatus, CreateProjectCharter, CreateProjectCharterRevision,
    CreateProjectCharterRevisionAtomically, ProjectCharterRecord, ProjectCharterRevisionRecord,
    ProjectOrchestrationRepo, SqliteDb,
};
use forge_agent_host::{
    MAIN_CHARTER_APPROVAL_TARGET_OPERATION, MAIN_CHARTER_DIFF_OPERATION,
    MAIN_CHARTER_DRAFT_OPERATION, MAIN_CHARTER_READINESS_OPERATION, MAIN_PROJECT_CREATE_OPERATION,
};
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::Row;

use crate::{
    create_project_from_charter_approval, evaluate_project_charter_readiness,
    render_and_digest_charter, resolve_genesis_project_agent, semantic_revision_diff,
    AgentActionService, CreateProjectAuthorization, CreateProjectFromCharterApprovalInput, Result,
    ServiceError, CHARTER_READINESS_POLICY_VERSION,
};

const CHARTER_SCHEMA_VERSION: &str = "forge.project-charter/v1";

/// Input to the typed Main orchestration execution boundary.  `executed_by`
/// is deliberately explicit: Project creation may only be executed by the
/// authenticated user who owns the Main Agent account, while draft/projection
/// operations may be executed by the bound Main identity or that user.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecuteMainOrchestrationActionInput {
    pub action_id: String,
    pub expected_version: i64,
    pub executed_by_type: String,
    pub executed_by_id: String,
    pub idempotency_key: String,
}

#[derive(Clone)]
pub struct MainOrchestrationActionService {
    db: Arc<SqliteDb>,
    actions: AgentActionService,
}

impl MainOrchestrationActionService {
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self {
            actions: AgentActionService::new(Arc::clone(&db)),
            db,
        }
    }

    /// Execute a Main orchestration proposal through its typed domain path.
    /// Replays are resolved from the action execution ledger before checking
    /// mutable Charter/approval state, so a response lost after commit is
    /// safe to retry with the same idempotency key.
    pub async fn execute(
        &self,
        input: ExecuteMainOrchestrationActionInput,
    ) -> Result<AgentActionExecution> {
        let action = self.actions.get(&input.action_id).await?;
        if !is_main_orchestration_operation(&action.operation) {
            return Err(ServiceError::invalid_operation(
                "action is not a Main orchestration proposal",
            ));
        }
        authorize_action_actor(
            &self.db,
            &action,
            &input.executed_by_type,
            &input.executed_by_id,
        )
        .await?;

        if let Some(existing) =
            db::AgentActionRepo::get_successful_action_execution(&*self.db, &input.action_id)
                .await?
        {
            if existing.idempotency_key != input.idempotency_key {
                return Err(ServiceError::conflict(
                    "Main orchestration action already has a successful execution with a different idempotency key",
                ));
            }
            // A replay is an exact authorization replay, not merely a lookup
            // by dedupe key.  AgentActionExecution does not persist the
            // expected action version, so bind the replay to the durable
            // executor envelope that is actually recorded rather than
            // pretending an unavailable version field exists.
            if existing.executed_by_type != input.executed_by_type
                || existing.executed_by_id != input.executed_by_id
                || input
                    .expected_version
                    .checked_add(1)
                    .is_none_or(|version| action.version != version)
            {
                return Err(ServiceError::conflict(
                    "Main orchestration replay authorization differs from the committed execution",
                ));
            }
            return Ok(existing);
        }

        if action.policy_result == AgentActionPolicyResult::Denied
            || matches!(
                action.status,
                AgentActionStatus::Denied | AgentActionStatus::Cancelled
            )
        {
            return Err(ServiceError::invalid_operation(
                "denied or cancelled Main orchestration action cannot execute",
            ));
        }
        let admitted = matches!(
            (&action.policy_result, &action.status),
            (
                AgentActionPolicyResult::Allowed,
                AgentActionStatus::Proposed
            ) | (
                AgentActionPolicyResult::ApprovalRequired,
                AgentActionStatus::Approved,
            )
        );
        if !admitted {
            return Err(ServiceError::invalid_operation(
                "Main orchestration action requires an admitted policy result and status",
            ));
        }
        if action.version != input.expected_version {
            return Err(ServiceError::Db(db::DbError::VersionConflict));
        }

        let payload: Value = serde_json::from_str(&action.payload_json).map_err(|_| {
            ServiceError::invalid_operation("Main orchestration action payload is invalid")
        })?;
        let result = match action.operation.as_str() {
            MAIN_CHARTER_DRAFT_OPERATION => self.execute_charter_draft(&action, &payload).await?,
            MAIN_CHARTER_READINESS_OPERATION => {
                self.execute_charter_readiness(&action, &payload).await?
            }
            MAIN_CHARTER_DIFF_OPERATION => self.execute_charter_diff(&action, &payload).await?,
            MAIN_CHARTER_APPROVAL_TARGET_OPERATION => {
                self.execute_charter_approval_target(&action, &payload)
                    .await?
            }
            MAIN_PROJECT_CREATE_OPERATION => {
                if input.executed_by_type != "user" {
                    return Err(ServiceError::invalid_operation(
                        "Project creation from a Charter approval is user-only",
                    ));
                }
                self.execute_project_create(
                    &action,
                    &payload,
                    &input.executed_by_id,
                    &input.idempotency_key,
                )
                .await?
            }
            _ => unreachable!("operation was validated above"),
        };

        let result_json = serde_json::to_string(&result).map_err(|error| {
            ServiceError::invalid_operation(format!(
                "serialize Main orchestration execution result: {error}"
            ))
        })?;
        let completed_at = now_rfc3339();
        db::AgentActionRepo::record_action_execution(
            &*self.db,
            db::CreateAgentActionExecution {
                id: new_uuid_v4(),
                action_id: input.action_id,
                expected_action_version: input.expected_version,
                attempt: 1,
                status: AgentActionExecutionStatus::Succeeded,
                result_json: Some(result_json.clone()),
                error: None,
                executed_by_type: input.executed_by_type,
                executed_by_id: input.executed_by_id,
                idempotency_key: required_value(
                    "execution idempotency key",
                    &input.idempotency_key,
                )?,
                action_status: AgentActionStatus::Executed,
                action_outcome_json: Some(result_json),
                created_at: completed_at.clone(),
                completed_at: Some(completed_at.clone()),
                updated_at: completed_at,
            },
        )
        .await
        .map_err(Into::into)
    }

    async fn execute_charter_draft(&self, action: &AgentAction, payload: &Value) -> Result<Value> {
        let draft: CharterDraftPayload = parse_payload(payload, "charter.draft")?;
        let (account_id, session) = self
            .main_genesis(
                action,
                draft.genesis_session_id.as_deref(),
                Some(&draft.charter_id),
            )
            .await?;
        if matches!(
            session.lifecycle,
            ProductGenesisLifecycle::HandedOff | ProductGenesisLifecycle::Cancelled
        ) {
            return Err(ServiceError::invalid_operation(
                "a Charter cannot be drafted after Genesis handoff or cancellation",
            ));
        }
        if session.maturity != draft.maturity {
            return Err(ServiceError::conflict(
                "Charter maturity must match the Product Genesis session",
            ));
        }
        if draft.content.identity.maturity != draft.maturity {
            return Err(ServiceError::conflict(
                "Charter identity maturity must match the requested Charter maturity",
            ));
        }
        if let Some(existing_charter_id) = session.charter_id.as_deref() {
            if existing_charter_id != draft.charter_id {
                return Err(ServiceError::conflict(
                    "Charter draft target does not match the Genesis Charter",
                ));
            }
        }

        let creating_charter = session.charter_id.is_none();
        let charter = match session.charter_id.as_deref() {
            Some(charter_id) => {
                let charter = ProjectOrchestrationRepo::get_project_charter_for_account(
                    &*self.db,
                    charter_id,
                    &account_id,
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
                // Keep the first Charter shell in memory.  The composite
                // repository call below writes this shell and its first
                // revision in one transaction, so a failed revision cannot
                // leave Genesis pointing at an empty Charter.
                ProjectCharterRecord {
                    id: draft.charter_id.clone(),
                    account_id: account_id.clone(),
                    genesis_session_id: Some(session.id.clone()),
                    project_id: None,
                    current_draft_revision_id: None,
                    current_approved_revision_id: None,
                    project_mode: draft.project_mode.as_str().to_owned(),
                    maturity: draft.maturity.as_str().to_owned(),
                    lifecycle: "draft".to_owned(),
                    version: 1,
                    created_at: now.clone(),
                    updated_at: now,
                }
            }
        };

        if charter.project_mode != draft.project_mode.as_str()
            || charter.maturity != draft.maturity.as_str()
        {
            return Err(ServiceError::conflict(
                "Charter mode or maturity changed since the Main proposal was created",
            ));
        }
        let expected_charter_version = draft.expected_charter_version.unwrap_or(charter.version);
        if charter.version != expected_charter_version {
            return Err(ServiceError::Db(db::DbError::VersionConflict));
        }
        let previous = match draft.base_revision_id.as_deref() {
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
        let rendered = render_and_digest_charter(&draft.content);
        if let Some(expected) = draft.rendered_view.as_deref() {
            if expected != rendered.rendered_view {
                return Err(ServiceError::conflict(
                    "rendered Charter view does not match the server renderer",
                ));
            }
        }
        if let Some(expected) = draft.render_version.as_deref() {
            if expected != rendered.render_version {
                return Err(ServiceError::conflict("Charter render version is stale"));
            }
        }
        if let Some(expected) = draft.content_digest.as_deref() {
            if expected != rendered.content_digest {
                return Err(ServiceError::conflict(
                    "Charter content digest does not match canonical content",
                ));
            }
        }
        if let Some(expected) = draft.render_digest.as_deref() {
            if expected != rendered.render_digest {
                return Err(ServiceError::conflict(
                    "Charter render digest does not match canonical content",
                ));
            }
        }
        let source_refs = draft
            .provenance
            .as_ref()
            .map(|provenance| provenance.source_refs.as_slice())
            .unwrap_or(draft.source_refs.as_slice());
        let source_refs_json = source_refs_with_action(source_refs, &action.id)?;
        let change_summary = draft
            .change_summary
            .or_else(|| {
                draft
                    .provenance
                    .as_ref()
                    .map(|provenance| provenance.change_summary.clone())
            })
            .unwrap_or_else(|| {
                let previous_content = previous.as_ref().and_then(|record| {
                    serde_json::from_str::<ProjectCharterContent>(&record.content_json).ok()
                });
                semantic_revision_diff(previous_content.as_ref(), &draft.content).change_summary()
            });
        let now = now_rfc3339();
        let revision_input = CreateProjectCharterRevision {
            id: new_uuid_v4(),
            charter_id: charter.id.clone(),
            expected_charter_version,
            project_mode: draft.project_mode.as_str().to_owned(),
            maturity: draft.maturity.as_str().to_owned(),
            base_revision: previous.as_ref().map(|record| record.revision).unwrap_or(0),
            base_revision_id: previous.as_ref().map(|record| record.id.clone()),
            lifecycle: "proposed".to_owned(),
            schema_version: CHARTER_SCHEMA_VERSION.to_owned(),
            render_version: rendered.render_version,
            content_json: serde_json::to_string(&draft.content).map_err(|error| {
                ServiceError::invalid_operation(format!("serialize Charter content: {error}"))
            })?,
            rendered_view: rendered.rendered_view,
            change_summary,
            author_type: "agent".to_owned(),
            author_id: Some(action.actor_identity_id.clone()),
            source_message_id: None,
            source_turn_job_id: None,
            source_refs_json,
            content_digest: rendered.content_digest,
            rendered_digest: rendered.render_digest,
            created_at: now,
        };
        let revision = if creating_charter {
            ProjectOrchestrationRepo::create_project_charter_revision_atomically(
                &*self.db,
                CreateProjectCharterRevisionAtomically {
                    project_id: None,
                    genesis_session_id: Some(session.id.clone()),
                    account_id: account_id.clone(),
                    charter: CreateProjectCharter {
                        id: charter.id.clone(),
                        account_id: account_id.clone(),
                        genesis_session_id: Some(session.id.clone()),
                        project_mode: draft.project_mode.as_str().to_owned(),
                        maturity: draft.maturity.as_str().to_owned(),
                        created_at: charter.created_at.clone(),
                        updated_at: charter.updated_at.clone(),
                    },
                    revision: revision_input,
                },
            )
            .await?
        } else {
            ProjectOrchestrationRepo::create_project_charter_revision(&*self.db, revision_input)
                .await?
        };
        let readiness = evaluate_project_charter_readiness(
            &draft.content,
            draft.project_mode,
            draft.maturity,
            CHARTER_READINESS_POLICY_VERSION,
            &revision.created_at,
        );
        // Durable, refresh-stable anchor in the Main Chat history. The
        // deterministic id makes action replays append-once; a failed append
        // never rolls back the committed revision.
        let (message_id, content) = crate::charter_proposal_chat_message(
            &revision.id,
            revision.revision,
            &draft.content.identity.working_name,
            &draft.content.identity.one_line_vision,
            Some(revision.change_summary.as_str()),
        );
        if let Err(error) = crate::append_system_chat_message(
            &self.db,
            &session.main_chat_id,
            &message_id,
            &content,
        )
        .await
        {
            tracing::warn!(%error, revision_id = %revision.id, "Charter proposal chat anchor failed");
        }
        Ok(json!({
            "operation": MAIN_CHARTER_DRAFT_OPERATION,
            "genesis_session_id": session.id,
            "charter_id": revision.charter_id,
            "revision_id": revision.id,
            "revision": revision.revision,
            "content_digest": revision.content_digest,
            "render_digest": revision.rendered_digest,
            "readiness": readiness,
        }))
    }

    async fn execute_charter_readiness(
        &self,
        action: &AgentAction,
        payload: &Value,
    ) -> Result<Value> {
        let projection: CharterProjectionPayload = parse_payload(payload, "charter.readiness")?;
        let (_account_id, session) = self
            .main_genesis(
                action,
                projection.genesis_session_id.as_deref(),
                Some(&projection.charter_id),
            )
            .await?;
        let charter_id = projection.charter_id.as_str();
        if session.charter_id.as_deref() != Some(charter_id) {
            return Err(ServiceError::invalid_operation(
                "Charter readiness target is not owned by this Genesis session",
            ));
        }
        let charter = ProjectOrchestrationRepo::get_project_charter_for_account(
            &*self.db,
            charter_id,
            &session.account_id,
        )
        .await?
        .ok_or_else(|| ServiceError::not_found("project_charter", charter_id))?;
        let revision = self
            .charter_revision_for(&charter, &projection.revision_id)
            .await?;
        if let Some(expected) = projection.expected_charter_version {
            if charter.version != expected {
                return Err(ServiceError::Db(db::DbError::VersionConflict));
            }
        }
        if let Some(expected) = projection.content_digest.as_deref() {
            if revision.content_digest != expected {
                return Err(ServiceError::conflict(
                    "Charter readiness target content digest is stale",
                ));
            }
        }
        if let Some(expected) = projection.render_digest.as_deref() {
            if revision.rendered_digest != expected {
                return Err(ServiceError::conflict(
                    "Charter readiness target render digest is stale",
                ));
            }
        }
        let content: ProjectCharterContent = serde_json::from_str(&revision.content_json)
            .map_err(|_| ServiceError::invalid_operation("persisted Charter content is invalid"))?;
        let project_mode = parse_project_mode(&charter.project_mode)?;
        let maturity = parse_maturity(&charter.maturity)?;
        let readiness_at = now_rfc3339();
        let readiness = evaluate_project_charter_readiness(
            &content,
            project_mode,
            maturity,
            CHARTER_READINESS_POLICY_VERSION,
            &readiness_at,
        );
        Ok(json!({
            "operation": MAIN_CHARTER_READINESS_OPERATION,
            "genesis_session_id": session.id,
            "charter_id": charter.id,
            "revision_id": revision.id,
            "readiness": readiness,
        }))
    }

    async fn execute_charter_diff(&self, action: &AgentAction, payload: &Value) -> Result<Value> {
        let projection: CharterDiffPayload = parse_payload(payload, "charter.diff")?;
        let (_account_id, session) = self
            .main_genesis(
                action,
                projection.genesis_session_id.as_deref(),
                Some(&projection.charter_id),
            )
            .await?;
        let charter_id = projection.charter_id.as_str();
        if session.charter_id.as_deref() != Some(charter_id) {
            return Err(ServiceError::invalid_operation(
                "Charter diff target is not owned by this Genesis session",
            ));
        }
        let charter = ProjectOrchestrationRepo::get_project_charter_for_account(
            &*self.db,
            charter_id,
            &session.account_id,
        )
        .await?
        .ok_or_else(|| ServiceError::not_found("project_charter", charter_id))?;
        let current = self
            .charter_revision_for(&charter, &projection.candidate_revision_id)
            .await?;
        let current_content: ProjectCharterContent = serde_json::from_str(&current.content_json)
            .map_err(|_| ServiceError::invalid_operation("persisted Charter content is invalid"))?;
        let previous = self
            .charter_revision_for(&charter, &projection.base_revision_id)
            .await?;
        let previous_content = Some(
            serde_json::from_str::<ProjectCharterContent>(&previous.content_json).map_err(
                |_| ServiceError::invalid_operation("persisted Charter content is invalid"),
            )?,
        );
        let diff = semantic_revision_diff(previous_content.as_ref(), &current_content);
        Ok(json!({
            "operation": MAIN_CHARTER_DIFF_OPERATION,
            "genesis_session_id": session.id,
            "charter_id": charter.id,
            "revision_id": current.id,
            "schema_version": diff.schema_version,
            "changed_sections": diff.changed_sections,
            "changes": diff.changes.into_iter().map(|change| json!({
                "section": change.section,
                "field": change.field,
                "before": change.before,
                "after": change.after,
            })).collect::<Vec<_>>(),
        }))
    }

    async fn execute_charter_approval_target(
        &self,
        action: &AgentAction,
        payload: &Value,
    ) -> Result<Value> {
        let projection: CharterProjectionPayload =
            parse_payload(payload, "charter.approval_target")?;
        let (_account_id, session) = self
            .main_genesis(
                action,
                projection.genesis_session_id.as_deref(),
                Some(&projection.charter_id),
            )
            .await?;
        let charter_id = projection.charter_id.as_str();
        if session.charter_id.as_deref() != Some(charter_id) {
            return Err(ServiceError::invalid_operation(
                "Charter approval target is not owned by this Genesis session",
            ));
        }
        let charter = ProjectOrchestrationRepo::get_project_charter_for_account(
            &*self.db,
            charter_id,
            &session.account_id,
        )
        .await?
        .ok_or_else(|| ServiceError::not_found("project_charter", charter_id))?;
        let revision = self
            .charter_revision_for(&charter, &projection.revision_id)
            .await?;
        let content: ProjectCharterContent = serde_json::from_str(&revision.content_json)
            .map_err(|_| ServiceError::invalid_operation("persisted Charter content is invalid"))?;
        let project_mode = parse_project_mode(&charter.project_mode)?;
        let maturity = parse_maturity(&charter.maturity)?;
        let readiness_at = now_rfc3339();
        let readiness = evaluate_project_charter_readiness(
            &content,
            project_mode,
            maturity,
            CHARTER_READINESS_POLICY_VERSION,
            &readiness_at,
        );
        let selected = self.selected_project_agent(&session).await?;
        Ok(json!({
            "operation": MAIN_CHARTER_APPROVAL_TARGET_OPERATION,
            "genesis_session_id": session.id,
            "charter_id": charter.id,
            "revision_id": revision.id,
            "expected_charter_version": charter.version,
            "approved_project_name": content.identity.working_name,
            "approved_project_slug": content.identity.slug_proposal,
            "project_mode": project_mode,
            "maturity": maturity,
            "content_digest": revision.content_digest,
            "render_digest": revision.rendered_digest,
            "readiness": readiness,
            "selected_project_agent": selected,
        }))
    }

    async fn execute_project_create(
        &self,
        action: &AgentAction,
        payload: &Value,
        user_id: &str,
        create_idempotency_key: &str,
    ) -> Result<Value> {
        let approval_id = payload
            .get("approval_id")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| ServiceError::invalid_operation("approval_id is required"))?;
        let created = create_project_from_charter_approval(
            Arc::clone(&self.db),
            CreateProjectFromCharterApprovalInput {
                approval_id: approval_id.to_owned(),
                idempotency_key: create_idempotency_key.to_owned(),
                account_id: user_id.to_owned(),
                authorization: CreateProjectAuthorization {
                    principal_type: "user".to_owned(),
                    principal_id: user_id.to_owned(),
                    action: "product_genesis.create_project_from_approval".to_owned(),
                    authorization_basis: "authenticated user executed typed project.create action"
                        .to_owned(),
                    event_id: action.id.clone(),
                    occurred_at: action.created_at.clone(),
                },
                correlation_id: action.correlation_id.clone(),
                causation_depth: action.causation_depth + 1,
            },
        )
        .await?;
        Ok(json!({
            "operation": MAIN_PROJECT_CREATE_OPERATION,
            "project_id": created.project.id,
            "project_agent_binding_id": created.project_agent_binding_id,
            "project_chat_id": created.project_chat_id,
            "charter_id": created.charter_id,
            "charter_revision_id": created.charter_revision_id,
            "handoff_id": created.handoff_id,
            "target_message_id": created.target_message_id,
            "target_turn_id": created.target_turn_id,
        }))
    }

    async fn main_genesis(
        &self,
        action: &AgentAction,
        session_id: Option<&str>,
        charter_id: Option<&str>,
    ) -> Result<(String, api_types::ProductGenesisSession)> {
        let account_id = action_account_id(&self.db, action).await?;
        let session_id = match session_id {
            Some(session_id) => session_id.to_owned(),
            None => {
                // The first draft may not have attached a Charter yet, so do
                // not require a `charter_id` match in the lookup. Prefer an
                // exact match when one exists, then fall back to the active
                // session that is still waiting for its first Charter. The
                // caller performs the exact Charter ownership check for all
                // projection operations after loading the session.
                let query = if charter_id.is_some() {
                    "SELECT id FROM product_genesis_session
                     WHERE account_id = ? AND lifecycle IN ('discovering', 'ready_for_project')
                     ORDER BY CASE WHEN charter_id = ? THEN 0
                                   WHEN charter_id IS NULL THEN 1
                                   ELSE 2 END,
                              updated_at DESC, id DESC LIMIT 1"
                } else {
                    "SELECT id FROM product_genesis_session
                     WHERE account_id = ? AND lifecycle IN ('discovering', 'ready_for_project')
                     ORDER BY updated_at DESC, id DESC LIMIT 1"
                };
                let mut request = sqlx::query_scalar::<_, String>(query).bind(&account_id);
                if let Some(charter_id) = charter_id {
                    request = request.bind(charter_id);
                }
                request
                    .fetch_optional(self.db.pool())
                    .await?
                    .ok_or_else(|| {
                        ServiceError::not_found("product_genesis_session", account_id.clone())
                    })?
            }
        };
        let session = crate::ProductGenesisService::for_sqlite(Arc::clone(&self.db))
            .get(&session_id)
            .await?;
        if session.account_id != account_id {
            return Err(ServiceError::not_found(
                "product_genesis_session",
                session_id.to_owned(),
            ));
        }
        if action.scope_type == "agent_chat" && action.scope_id != session.main_chat_id {
            return Err(ServiceError::invalid_operation(
                "Main action scope does not match the Genesis Main Chat",
            ));
        }
        if !matches!(
            session.lifecycle,
            ProductGenesisLifecycle::Discovering | ProductGenesisLifecycle::ReadyForProject
        ) {
            return Err(ServiceError::invalid_operation(
                "Main Charter orchestration is only available during active Product Genesis",
            ));
        }
        Ok((account_id, session))
    }

    async fn charter_revision_for(
        &self,
        charter: &ProjectCharterRecord,
        revision_id: &str,
    ) -> Result<ProjectCharterRevisionRecord> {
        ProjectOrchestrationRepo::get_project_charter_revision(&*self.db, revision_id)
            .await?
            .filter(|revision| revision.charter_id == charter.id)
            .ok_or_else(|| {
                ServiceError::not_found("project_charter_revision", revision_id.to_owned())
            })
    }

    async fn selected_project_agent(
        &self,
        session: &api_types::ProductGenesisSession,
    ) -> Result<Option<Value>> {
        Ok(resolve_genesis_project_agent(&self.db, session)
            .await?
            .map(|selection| {
                json!({
                    "identity_id": selection.identity_id,
                    "display_name": selection.display_name,
                    "profile_revision_id": selection.profile_revision_id,
                    "operating_skill_revision": selection.operating_skill_revision,
                    "policy_digest": selection.policy_digest,
                })
            }))
    }
}

#[derive(Debug, Deserialize)]
struct CharterDraftPayload {
    #[serde(default)]
    genesis_session_id: Option<String>,
    charter_id: String,
    #[serde(default)]
    expected_charter_version: Option<i64>,
    #[serde(default)]
    base_revision_id: Option<String>,
    project_mode: ProjectMode,
    maturity: ProductMaturity,
    content: ProjectCharterContent,
    #[serde(default)]
    change_summary: Option<String>,
    #[serde(default)]
    source_refs: Vec<ProvenanceRef>,
    #[serde(default)]
    provenance: Option<RevisionProvenance>,
    #[serde(default)]
    rendered_view: Option<String>,
    #[serde(default)]
    render_version: Option<String>,
    #[serde(default)]
    content_digest: Option<String>,
    #[serde(default)]
    render_digest: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CharterProjectionPayload {
    #[serde(default)]
    genesis_session_id: Option<String>,
    charter_id: String,
    revision_id: String,
    #[serde(default)]
    content_digest: Option<String>,
    #[serde(default)]
    render_digest: Option<String>,
    #[serde(default)]
    expected_charter_version: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct CharterDiffPayload {
    #[serde(default)]
    genesis_session_id: Option<String>,
    charter_id: String,
    base_revision_id: String,
    candidate_revision_id: String,
}

fn parse_payload<T: for<'de> Deserialize<'de>>(payload: &Value, operation: &str) -> Result<T> {
    serde_json::from_value(payload.clone()).map_err(|error| {
        ServiceError::invalid_operation(format!("{operation} payload is invalid: {error}"))
    })
}

pub fn is_main_orchestration_operation(operation: &str) -> bool {
    matches!(
        operation,
        MAIN_CHARTER_DRAFT_OPERATION
            | MAIN_CHARTER_READINESS_OPERATION
            | MAIN_CHARTER_DIFF_OPERATION
            | MAIN_CHARTER_APPROVAL_TARGET_OPERATION
            | MAIN_PROJECT_CREATE_OPERATION
    )
}

async fn action_account_id(db: &SqliteDb, action: &AgentAction) -> Result<String> {
    let account_id =
        sqlx::query_scalar::<_, Option<String>>("SELECT owner_id FROM agent_identity WHERE id = ?")
            .bind(&action.actor_identity_id)
            .fetch_optional(db.pool())
            .await?
            .flatten()
            .ok_or_else(|| {
                ServiceError::not_found("agent_identity", action.actor_identity_id.clone())
            })?;
    match action.scope_type.as_str() {
        "account" if action.scope_id == account_id => Ok(account_id),
        "agent_chat" => {
            let row = sqlx::query(
                "SELECT kind, account_id FROM agent_chat WHERE id = ? AND kind = 'account_main'",
            )
            .bind(&action.scope_id)
            .fetch_optional(db.pool())
            .await?
            .ok_or_else(|| {
                ServiceError::invalid_operation("Main action scope is not a Main Chat")
            })?;
            let chat_account =
                row.try_get::<Option<String>, _>("account_id")?
                    .ok_or_else(|| {
                        ServiceError::invalid_operation("Main Chat has no owning account")
                    })?;
            if chat_account != account_id {
                return Err(ServiceError::invalid_operation(
                    "Main action identity does not own the Main Chat account",
                ));
            }
            Ok(account_id)
        }
        _ => Err(ServiceError::invalid_operation(
            "Main orchestration action must be account- or Main-Chat-scoped",
        )),
    }
}

async fn authorize_action_actor(
    db: &SqliteDb,
    action: &AgentAction,
    executed_by_type: &str,
    executed_by_id: &str,
) -> Result<()> {
    let account_id = action_account_id(db, action).await?;
    if executed_by_id.trim().is_empty() {
        return Err(ServiceError::invalid_operation(
            "typed orchestration executor id is required",
        ));
    }
    if executed_by_type == "agent" && executed_by_id == action.actor_identity_id {
        return Ok(());
    }
    if executed_by_type == "user" && executed_by_id == account_id {
        return Ok(());
    }
    Err(ServiceError::invalid_operation(
        "typed orchestration executor is not the bound Main identity or account owner",
    ))
}

fn source_refs_with_action(source_refs: &[ProvenanceRef], action_id: &str) -> Result<String> {
    let mut values = source_refs
        .iter()
        .map(serde_json::to_value)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| {
            ServiceError::invalid_operation(format!("serialize Charter provenance: {error}"))
        })?;
    values.push(json!({
        "source_kind": "system",
        "source_id": action_id,
        "label": "main_orchestration_action",
    }));
    serde_json::to_string(&values).map_err(|error| {
        ServiceError::invalid_operation(format!("serialize Charter provenance: {error}"))
    })
}

fn parse_project_mode(value: &str) -> Result<ProjectMode> {
    match value {
        "compact" => Ok(ProjectMode::Compact),
        "standard" => Ok(ProjectMode::Standard),
        _ => Err(ServiceError::invalid_operation(
            "persisted Charter mode is invalid",
        )),
    }
}

fn parse_maturity(value: &str) -> Result<ProductMaturity> {
    match value {
        "prototype" => Ok(ProductMaturity::Prototype),
        "mvp" => Ok(ProductMaturity::Mvp),
        "production" => Ok(ProductMaturity::Production),
        "critical" => Ok(ProductMaturity::Critical),
        _ => Err(ServiceError::invalid_operation(
            "persisted Charter maturity is invalid",
        )),
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MAIN_OPERATING_SKILL_KEY;
    use db::{
        create_sqlite_pool, run_migrations, AgentActionRepo, AgentRepo, CreateAgentAction,
        CreateAgentIdentity, CreateAgentProfile, User, UserRepo,
    };

    async fn fixture() -> Arc<SqliteDb> {
        let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
        run_migrations(&pool).await.expect("migrations");
        let db = Arc::new(SqliteDb::new(pool));
        let now = now_rfc3339();
        UserRepo::create_user(
            &*db,
            &User {
                id: "user-1".to_owned(),
                email: "user-1@example.test".to_owned(),
                password_hash: "placeholder".to_owned(),
                display_name: None,
                is_admin: false,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("user");
        AgentRepo::create_identity_with_profile(
            &*db,
            CreateAgentIdentity {
                id: "main-agent".to_owned(),
                name: "Main Agent".to_owned(),
                description: None,
                max_concurrent_tasks: 1,
                heartbeat_interval_seconds: 30,
                max_missed_heartbeats: 3,
                status: db::AgentStatus::Idle,
                last_heartbeat_at: None,
                is_default: false,
                paused: false,
                owner_id: Some("user-1".to_owned()),
                visibility: "account".to_owned(),
                account_permission_ceiling: r#"{"permissions":["propose_discovery"]}"#.to_owned(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
            CreateAgentProfile {
                id: "main-profile".to_owned(),
                identity_id: "main-agent".to_owned(),
                backend_kind: "native".to_owned(),
                executor_type: "native".to_owned(),
                provider: None,
                model: None,
                reasoning_effort: None,
                permission_policy: None,
                prompt_template: None,
                capabilities_json: "{}".to_owned(),
                tool_policy_json: r#"{"permissions":["propose_discovery"]}"#.to_owned(),
                config_json: "{}".to_owned(),
                credential_ref: None,
                daemon_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("Main identity");
        // V071 creates a setup-required Main Chat from the user insert
        // trigger. Replace that generated id with the stable fixture id used
        // by the action scope and Genesis rows below.
        sqlx::query("DELETE FROM agent_chat WHERE account_id = 'user-1' AND kind = 'account_main'")
            .execute(db.pool())
            .await
            .expect("generated Main Chat");
        sqlx::query(
            "INSERT INTO agent_chat
             (id, kind, account_id, project_id, status, instruction_revision, version, created_at, updated_at)
             VALUES ('main-chat', 'account_main', 'user-1', NULL, 'ready', 0, 1, ?, ?)",
        )
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("Main Chat");
        sqlx::query(
            "INSERT INTO product_genesis_session
             (id, account_id, main_chat_id, prompt_revision, prompt_body, maturity,
              lifecycle, source_message_ids_json, preferred_project_agent_identity_id,
              version, created_at, updated_at)
             VALUES ('genesis-1', 'user-1', 'main-chat', ?,
                     'Genesis', 'mvp', 'discovering', '[]', NULL, 1, ?, ?)",
        )
        .bind(
            sqlx::query_scalar::<_, String>(
                "SELECT revision.id
                 FROM operating_skill AS skill
                 JOIN operating_skill_revision AS revision
                   ON revision.id = skill.current_revision_id
                  AND revision.operating_skill_id = skill.id
                  AND revision.skill_key = skill.skill_key
                 WHERE skill.skill_key = ? AND skill.lifecycle = 'active'
                 LIMIT 1",
            )
            .bind(MAIN_OPERATING_SKILL_KEY)
            .fetch_one(db.pool())
            .await
            .expect("Main operating skill revision"),
        )
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("Genesis");
        db
    }

    fn content() -> ProjectCharterContent {
        serde_json::from_value(json!({
            "identity": {
                "working_name": "Typed Charter",
                "one_line_vision": "A durable typed Charter",
                "maturity": "mvp"
            },
            "problem_and_people": {
                "problem_or_opportunity": "Intent is lost",
                "target_users": ["builders"]
            },
            "core_experience": {
                "primary_outcome": "Keep intent durable"
            },
            "scope": {
                "must_have_outcomes": ["durable revision"],
                "explicit_non_goals": ["repository mutation"]
            },
            "success": {
                "acceptance_statements": ["revision can be read back"]
            },
            "constraints_and_risks": {},
            "knowledge_ledger": {"items": []}
        }))
        .expect("Charter content")
    }

    async fn action(db: &SqliteDb, id: &str, operation: &str, payload: Value) -> AgentAction {
        AgentActionRepo::create_action(
            db,
            CreateAgentAction {
                id: id.to_owned(),
                actor_identity_id: "main-agent".to_owned(),
                scope_type: "agent_chat".to_owned(),
                scope_id: "main-chat".to_owned(),
                operation: operation.to_owned(),
                payload_json: payload.to_string(),
                payload_hash: "payload-hash".to_owned(),
                dedupe_key: format!("dedupe-{id}"),
                correlation_id: format!("correlation-{id}"),
                causation_id: None,
                causation_depth: 0,
                requested_permission: "propose_discovery".to_owned(),
                policy_result: AgentActionPolicyResult::Allowed,
                policy_reason: None,
                status: AgentActionStatus::Proposed,
                target_type: Some("account".to_owned()),
                target_id: Some("user-1".to_owned()),
                created_at: now_rfc3339(),
                updated_at: now_rfc3339(),
            },
        )
        .await
        .expect("action")
    }

    #[tokio::test]
    async fn charter_draft_materializes_revision_and_replay_is_idempotent() {
        let db = fixture().await;
        let content = content();
        let action = action(
            &db,
            "action-draft",
            MAIN_CHARTER_DRAFT_OPERATION,
            json!({
                "genesis_session_id": "genesis-1",
                "charter_id": "charter-1",
                "expected_charter_version": 1,
                "project_mode": "compact",
                "maturity": "mvp",
                "content": content
            }),
        )
        .await;
        let service = MainOrchestrationActionService::new(Arc::clone(&db));
        let first = service
            .execute(ExecuteMainOrchestrationActionInput {
                action_id: action.id.clone(),
                expected_version: action.version,
                executed_by_type: "agent".to_owned(),
                executed_by_id: "main-agent".to_owned(),
                idempotency_key: "draft-execution-1".to_owned(),
            })
            .await
            .expect("typed draft execution");
        let revision_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM project_charter_revision WHERE charter_id = 'charter-1'",
        )
        .fetch_one(db.pool())
        .await
        .expect("revision count");
        assert_eq!(revision_count, 1);
        let charter_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM project_charter WHERE id = 'charter-1'")
                .fetch_one(db.pool())
                .await
                .expect("charter count");
        let linked_charter: Option<String> = sqlx::query_scalar(
            "SELECT charter_id FROM product_genesis_session WHERE id = 'genesis-1'",
        )
        .fetch_one(db.pool())
        .await
        .expect("genesis charter pointer");
        assert_eq!(charter_count, 1);
        assert_eq!(linked_charter.as_deref(), Some("charter-1"));
        let replay = service
            .execute(ExecuteMainOrchestrationActionInput {
                action_id: action.id,
                expected_version: action.version,
                executed_by_type: "agent".to_owned(),
                executed_by_id: "main-agent".to_owned(),
                idempotency_key: "draft-execution-1".to_owned(),
            })
            .await
            .expect("typed draft replay");
        assert_eq!(first.id, replay.id);
    }

    #[tokio::test]
    async fn generic_or_unauthorized_execution_cannot_materialize_charter() {
        let db = fixture().await;
        let content = content();
        let action = action(
            &db,
            "action-unauthorized",
            MAIN_CHARTER_DRAFT_OPERATION,
            json!({
                "genesis_session_id": "genesis-1",
                "charter_id": "charter-unauthorized",
                "project_mode": "compact",
                "maturity": "mvp",
                "content": content
            }),
        )
        .await;
        let service = MainOrchestrationActionService::new(Arc::clone(&db));
        let unauthorized = service
            .execute(ExecuteMainOrchestrationActionInput {
                action_id: action.id.clone(),
                expected_version: action.version,
                executed_by_type: "user".to_owned(),
                executed_by_id: "other-user".to_owned(),
                idempotency_key: "unauthorized".to_owned(),
            })
            .await;
        assert!(unauthorized.is_err());
        let generic = service
            .actions
            .execute(crate::ExecuteActionInput {
                action_id: action.id,
                expected_version: 1,
                attempt: 1,
                result_json: Some(r#"{"fake":true}"#.to_owned()),
                error: None,
                executed_by_type: "user".to_owned(),
                executed_by_id: "user-1".to_owned(),
                idempotency_key: "generic-fake".to_owned(),
            })
            .await;
        assert!(generic.is_err());
        let revision_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM project_charter_revision")
                .fetch_one(db.pool())
                .await
                .expect("revision count");
        assert_eq!(revision_count, 0);
    }

    #[tokio::test]
    async fn pending_approval_cannot_materialize_charter() {
        let db = fixture().await;
        let content = content();
        let action = action(
            &db,
            "action-pending",
            MAIN_CHARTER_DRAFT_OPERATION,
            json!({
                "genesis_session_id": "genesis-1",
                "charter_id": "charter-pending",
                "expected_charter_version": 1,
                "project_mode": "compact",
                "maturity": "mvp",
                "content": content
            }),
        )
        .await;
        sqlx::query(
            "UPDATE agent_action
             SET policy_result = 'approval_required', status = 'pending_approval'
             WHERE id = ?",
        )
        .bind(&action.id)
        .execute(db.pool())
        .await
        .expect("pending action");
        let result = MainOrchestrationActionService::new(Arc::clone(&db))
            .execute(ExecuteMainOrchestrationActionInput {
                action_id: action.id,
                expected_version: 1,
                executed_by_type: "agent".to_owned(),
                executed_by_id: "main-agent".to_owned(),
                idempotency_key: "pending-execution".to_owned(),
            })
            .await;
        assert!(result.is_err());
        let revision_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM project_charter_revision")
                .fetch_one(db.pool())
                .await
                .expect("revision count");
        assert_eq!(revision_count, 0);
    }
}
