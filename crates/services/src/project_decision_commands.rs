//! Shared Project Decision command boundary.
//!
//! REST and native Project-Agent execution both enter this service. Domain
//! validation happens here, while the repository composites own the atomic
//! Decision/candidate, Project CAS, event, command-receipt, and optional
//! AgentAction-execution transaction.

use std::{collections::BTreeMap, sync::Arc};

use api_types::{
    AdaptiveEnvelope, ArtifactRef, DecisionCandidateContext, DecisionClass, ProvenanceRef,
    ProvenanceSourceKind,
};
use db::{
    new_uuid_v4, now_rfc3339, AgentAction, AgentActionExecutionStatus, AgentActionStatus,
    AppendProjectDecisionCommand, ApproveProjectDecisionCandidateCommand, CommandReceipt,
    CommandReceiptRepo, CreateAgentActionExecution, CreateCommandReceipt, CreateProjectDecision,
    CreateProjectDecisionCandidate, CreateProjectDecisionCandidateCommand,
    ProjectDecisionCandidateRecord, ProjectDecisionRecord, ProjectMemberRepo,
    ProjectOrchestrationRepo, ProjectRepo, RejectProjectDecisionCandidateCommand, SqliteDb,
};
use serde::Serialize;
use serde_json::{json, Value};
use sqlx::Row;

use crate::{
    AgentActionProvenance, AuthorizationProvenance, CommandContext, CommandPrincipal, CommandScope,
    CommandScopeType, ExpectedCommandState, NewCommandContext, ProjectCommandAuthorization, Result,
    ServiceError,
};

pub const PROJECT_DECISION_CANDIDATE_CREATE_COMMAND: &str = "project.decision.candidate.create";
pub const PROJECT_DECISION_CANDIDATE_APPROVE_COMMAND: &str = "project.decision.candidate.approve";
pub const PROJECT_DECISION_CANDIDATE_REJECT_COMMAND: &str = "project.decision.candidate.reject";
pub const PROJECT_DECISION_EFFECTIVE_COMMAND: &str = "project.decision";

#[derive(Debug, Clone, Serialize)]
pub struct ProjectDecisionCandidateCommand {
    pub project_id: String,
    pub question: String,
    pub context: DecisionCandidateContext,
    pub options: Vec<String>,
    pub selected_outcome: Option<String>,
    pub rationale: Option<String>,
    pub decision_class: DecisionClass,
    pub source_refs: Vec<ProvenanceRef>,
    pub expected_project_version: i64,
    pub reconciliation_reason: Option<String>,
    pub idempotency_key: String,
    pub authorization: ProjectCommandAuthorization,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectDecisionEffectiveCommand {
    pub project_id: String,
    pub decision_id: String,
    pub question: String,
    pub context: DecisionCandidateContext,
    pub options: Vec<String>,
    pub selected_outcome: String,
    pub rationale: String,
    pub decision_class: DecisionClass,
    pub authority_basis: String,
    pub charter_revision_id: Option<String>,
    pub baseline_revision_id: Option<String>,
    pub source_refs: Vec<ProvenanceRef>,
    pub supersedes_decision_id: Option<String>,
    pub state: String,
    pub expected_project_version: i64,
    pub idempotency_key: String,
    pub authorization: ProjectCommandAuthorization,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectDecisionApprovalCommand {
    pub project_id: String,
    pub candidate_id: String,
    pub expected_project_version: i64,
    pub idempotency_key: String,
    pub authorization: ProjectCommandAuthorization,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectDecisionRejectionCommand {
    pub project_id: String,
    pub candidate_id: String,
    pub reason: String,
    pub expected_project_version: i64,
    pub idempotency_key: String,
    pub authorization: ProjectCommandAuthorization,
}

#[derive(Clone)]
pub struct ProjectDecisionCommandService {
    db: Arc<SqliteDb>,
}

impl ProjectDecisionCommandService {
    #[must_use]
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self { db }
    }

    /// Execute the Project Agent's bounded implementation-Decision policy.
    /// Baseline and adaptive-envelope validation live here with the shared
    /// Decision writes; the native adapter supplies only admitted provenance.
    pub(crate) async fn execute_project_agent_command(
        &self,
        action: &AgentAction,
        project_id: &str,
        payload: &Value,
        context: &CommandContext,
    ) -> Result<Value> {
        let operation = payload_string(payload, "action")?;
        if !matches!(operation.as_str(), "record_candidate" | "record_effective") {
            return Err(ServiceError::invalid_operation(
                "Project Agent may record implementation choices or propose candidates; supersession, invalidation, and user-scope decisions remain user-only",
            ));
        }
        if payload.get("decision_class").and_then(Value::as_str) != Some("project_implementation") {
            return Err(ServiceError::invalid_operation(
                "Project Agent decisions must use the project_implementation class",
            ));
        }
        let baseline_id = payload_string(payload, "baseline_id")?;
        let baseline_revision_id = payload_string(payload, "baseline_revision_id")?;
        let baseline = sqlx::query(
            "SELECT r.charter_revision_id,
                    r.content_digest AS baseline_content_digest,
                    r.render_version AS baseline_render_version,
                    r.rendered_digest AS baseline_render_digest,
                    r.document_revisions_json, r.milestone_id,
                    r.milestone_ids_json, r.primary_milestone_id,
                    r.adaptive_envelope_json
             FROM project_execution_baseline AS b
             JOIN project_execution_baseline_revision AS r
               ON r.id = b.current_revision_id
             WHERE b.id = ? AND b.project_id = ? AND b.lifecycle = 'active'
               AND b.current_revision_id = ? AND r.lifecycle = 'approved'
             LIMIT 1",
        )
        .bind(&baseline_id)
        .bind(project_id)
        .bind(&baseline_revision_id)
        .fetch_optional(self.db.pool())
        .await?
        .ok_or_else(|| {
            ServiceError::conflict(
                "Project decision must reference the exact approved revision of the active baseline",
            )
        })?;
        let baseline_charter_revision_id: String = baseline.try_get("charter_revision_id")?;
        let baseline_document_revisions: Vec<ArtifactRef> =
            serde_json::from_str(&baseline.try_get::<String, _>("document_revisions_json")?)
                .map_err(|_| {
                    ServiceError::invalid_operation(
                        "active baseline Document references are invalid",
                    )
                })?;
        let baseline_milestone_id: Option<String> = baseline.try_get("milestone_id")?;
        let baseline_milestone_ids_json: String = baseline.try_get("milestone_ids_json")?;
        let baseline_primary_milestone_id: Option<String> =
            baseline.try_get("primary_milestone_id")?;
        let adaptive_envelope = parse_agent_adaptive_envelope(
            &baseline.try_get::<String, _>("adaptive_envelope_json")?,
        )?;
        let affected_artifact_refs: Vec<ArtifactRef> = payload_vec(
            payload,
            "affected_artifact_refs",
            "affected artifact references are invalid",
        )?;
        let affected_task_ids: Vec<String> = payload_vec(
            payload,
            "affected_task_ids",
            "affected Task IDs are invalid",
        )?;
        let affected_milestone_ids: Vec<String> = payload_vec(
            payload,
            "affected_milestone_ids",
            "affected milestone IDs are invalid",
        )?;

        for task_id in &affected_task_ids {
            let governed: Option<i64> = sqlx::query_scalar(
                "SELECT 1
                 FROM task AS t
                 JOIN project_task_governance AS g ON g.task_id = t.id
                 WHERE t.id = ? AND t.project_id = ?
                   AND g.baseline_id = ? AND g.baseline_revision_id = ?
                 LIMIT 1",
            )
            .bind(task_id)
            .bind(project_id)
            .bind(&baseline_id)
            .bind(&baseline_revision_id)
            .fetch_optional(self.db.pool())
            .await?;
            if governed.is_none() {
                return Err(ServiceError::invalid_operation(
                    "decision affected Task crosses Project scope",
                ));
            }
        }

        let baseline_charter = sqlx::query(
            "SELECT c.id AS artifact_id, r.content_digest, r.render_version,
                    r.rendered_digest, r.lifecycle
             FROM project_charter_revision AS r
             JOIN project_charter AS c ON c.id = r.charter_id
             WHERE r.id = ? AND c.project_id = ? LIMIT 1",
        )
        .bind(&baseline_charter_revision_id)
        .bind(project_id)
        .fetch_optional(self.db.pool())
        .await?
        .ok_or_else(|| {
            ServiceError::conflict("active baseline Charter is outside Project scope")
        })?;
        let baseline_charter_id: String = baseline_charter.try_get("artifact_id")?;
        let baseline_charter_content_digest: String = baseline_charter.try_get("content_digest")?;
        let baseline_charter_render_version: String = baseline_charter.try_get("render_version")?;
        let baseline_charter_render_digest: String = baseline_charter.try_get("rendered_digest")?;
        let baseline_charter_lifecycle: String = baseline_charter.try_get("lifecycle")?;
        let baseline_content_digest: String = baseline.try_get("baseline_content_digest")?;
        let baseline_render_version: String = baseline.try_get("baseline_render_version")?;
        let baseline_render_digest: String = baseline.try_get("baseline_render_digest")?;
        if baseline_charter_content_digest.trim().is_empty()
            || baseline_charter_render_version.trim().is_empty()
            || baseline_charter_render_digest.trim().is_empty()
            || baseline_content_digest.trim().is_empty()
            || baseline_render_version.trim().is_empty()
            || baseline_render_digest.trim().is_empty()
            || baseline_charter_lifecycle != "approved"
        {
            return Err(ServiceError::invalid_operation(
                "active baseline references are missing exact immutable digests",
            ));
        }
        let mut baseline_artifacts = baseline_document_revisions;
        baseline_artifacts.push(ArtifactRef {
            artifact_id: baseline_charter_id,
            revision_id: baseline_charter_revision_id.clone(),
            content_digest: baseline_charter_content_digest,
            render_version: Some(baseline_charter_render_version),
            render_digest: Some(baseline_charter_render_digest),
        });
        baseline_artifacts.push(ArtifactRef {
            artifact_id: baseline_id.clone(),
            revision_id: baseline_revision_id.clone(),
            content_digest: baseline_content_digest,
            render_version: Some(baseline_render_version),
            render_digest: Some(baseline_render_digest),
        });
        let references_inside_baseline = affected_artifact_refs.iter().all(|reference| {
            baseline_artifacts
                .iter()
                .any(|allowed| allowed == reference)
        });
        let milestones_inside_baseline = affected_milestone_ids.iter().all(|milestone_id| {
            baseline_milestone_id.as_deref() == Some(milestone_id.as_str())
                || baseline_primary_milestone_id.as_deref() == Some(milestone_id.as_str())
                || json_contains_agent_identifier(&baseline_milestone_ids_json, milestone_id)
        });
        let selected_outcome = payload_optional_string(payload, "selected_outcome");
        let outcome_inside_envelope =
            agent_outcome_inside_envelope(&adaptive_envelope, selected_outcome.as_deref());
        let reconciliation_reason = if !references_inside_baseline {
            Some("affected artifact is outside the active baseline".to_owned())
        } else if !milestones_inside_baseline {
            Some("affected milestone is outside the active baseline".to_owned())
        } else if !outcome_inside_envelope {
            Some("selected outcome is outside the active adaptive envelope".to_owned())
        } else {
            None
        };
        let expected_project_version = payload_integer(payload, "expected_project_version")?;
        let question = payload_string(payload, "question")?;
        let options: Vec<String> = payload_vec(payload, "options", "options are invalid")?;
        let rationale = payload_optional_string(payload, "rationale");
        let decision_context = DecisionCandidateContext {
            summary: Some(reconciliation_reason.as_ref().map_or_else(
                || "Implementation choice inside the active execution baseline".to_owned(),
                |reason| format!("reconciliation_required: {reason}"),
            )),
            constraints: Vec::new(),
            affected_artifact_refs,
            affected_task_ids,
            affected_milestone_ids,
            governing_charter_revision_id: None,
            governing_baseline_revision_id: Some(baseline_revision_id.clone()),
            supersedes_decision_id: None,
            invalidates_decision_id: None,
        };
        let source_refs = vec![ProvenanceRef {
            source_kind: ProvenanceSourceKind::ProjectChat,
            source_id: context
                .action_provenance
                .as_ref()
                .map(|value| value.action_id.clone())
                .unwrap_or_else(|| context.correlation_id().to_owned()),
            revision_id: None,
            digest: None,
            label: None,
            observed_at: Some(now_rfc3339()),
        }];

        if operation == "record_effective" && reconciliation_reason.is_none() {
            let selected_outcome = selected_outcome.ok_or_else(|| {
                ServiceError::invalid_operation(
                    "an effective implementation decision requires selected_outcome",
                )
            })?;
            let rationale = rationale.ok_or_else(|| {
                ServiceError::invalid_operation(
                    "an effective implementation decision requires rationale",
                )
            })?;
            let decision = self
                .append_effective_with_context(
                    ProjectDecisionEffectiveCommand {
                        project_id: project_id.to_owned(),
                        decision_id: payload_string(payload, "decision_id")?,
                        question,
                        context: decision_context,
                        options,
                        selected_outcome,
                        rationale,
                        decision_class: DecisionClass::ProjectImplementation,
                        authority_basis: "active_execution_baseline_adaptive_envelope".to_owned(),
                        charter_revision_id: Some(baseline_charter_revision_id),
                        baseline_revision_id: Some(baseline_revision_id),
                        source_refs,
                        supersedes_decision_id: None,
                        state: "active".to_owned(),
                        expected_project_version,
                        idempotency_key: context.idempotency_key().to_owned(),
                        authorization: agent_authorization(
                            action,
                            context,
                            "project.decision.record_effective",
                        ),
                    },
                    context.clone(),
                )
                .await?;
            return Ok(json!({
                "operation": context.operation(),
                "project_id": project_id,
                "decision_id": decision.id,
                "state": decision.state,
                "authority_basis": decision.authority_basis,
                "domain_committed": true,
                "requires_user_authorization": false,
            }));
        }

        let reconciliation_required = reconciliation_reason.is_some();
        let candidate = self
            .create_candidate_with_context(
                ProjectDecisionCandidateCommand {
                    project_id: project_id.to_owned(),
                    question,
                    context: decision_context,
                    options,
                    selected_outcome,
                    rationale,
                    decision_class: DecisionClass::ProjectImplementation,
                    source_refs,
                    expected_project_version,
                    reconciliation_reason: reconciliation_reason.clone(),
                    idempotency_key: context.idempotency_key().to_owned(),
                    authorization: agent_authorization(
                        action,
                        context,
                        "project.decision.record_candidate",
                    ),
                },
                context.clone(),
            )
            .await?;
        Ok(json!({
            "operation": context.operation(),
            "project_id": project_id,
            "candidate_id": candidate.id,
            "lifecycle": candidate.lifecycle,
            "reconciliation_required": reconciliation_required,
            "reconciliation_reason": reconciliation_reason,
            "domain_committed": true,
            "requires_user_authorization": true,
        }))
    }

    pub async fn create_candidate(
        &self,
        command: ProjectDecisionCandidateCommand,
        action: Option<AgentActionProvenance>,
    ) -> Result<ProjectDecisionCandidateRecord> {
        let context = decision_context(
            PROJECT_DECISION_CANDIDATE_CREATE_COMMAND,
            &command.project_id,
            command.expected_project_version,
            &command.idempotency_key,
            &command.authorization,
            &command,
            action,
        )?;
        self.create_candidate_with_context(command, context).await
    }

    pub(crate) async fn create_candidate_with_context(
        &self,
        command: ProjectDecisionCandidateCommand,
        context: CommandContext,
    ) -> Result<ProjectDecisionCandidateRecord> {
        validate_candidate_envelope(&command)?;
        validate_context(&context, &command.project_id, &command.authorization)?;
        if let Some(receipt) = self.replay(&context).await? {
            return frozen_candidate_from_receipt(&receipt, &command);
        }
        authorize_project_principal(&self.db, &command.project_id, &command.authorization).await?;
        validate_decision_context(&self.db, &command.project_id, &command.context).await?;

        let replay_command = command.clone();
        let candidate_id = new_uuid_v4();
        let now = now_rfc3339();
        let context_json = encoded_context(&command.context, command.decision_class)?;
        let outcome_json = json!({
            "operation": context.operation(),
            "project_id": command.project_id,
            "candidate_id": candidate_id,
            "lifecycle": "proposed",
            "reconciliation_required": command.reconciliation_reason.is_some(),
            "reconciliation_reason": command.reconciliation_reason,
            "created_at": now,
            "domain_committed": true,
            "requires_user_authorization": true,
        })
        .to_string();
        let (receipt, execution) = command_bundle(&context, &outcome_json);
        ProjectOrchestrationRepo::create_project_decision_candidate_command(
            &*self.db,
            CreateProjectDecisionCandidateCommand {
                candidate: CreateProjectDecisionCandidate {
                    id: candidate_id,
                    project_id: command.project_id,
                    lifecycle: "proposed".to_owned(),
                    question: command.question,
                    context_json,
                    options_json: serde_json::to_string(&command.options)
                        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
                    selected_outcome: command.selected_outcome,
                    rationale: command.rationale,
                    principal_type: Some(command.authorization.principal_type),
                    principal_id: Some(command.authorization.principal_id),
                    source_refs_json: serde_json::to_string(&command.source_refs)
                        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
                    expected_project_version: command.expected_project_version,
                    created_at: now.clone(),
                    updated_at: now,
                },
                command_receipt: Some(receipt),
                action_execution: execution,
            },
        )
        .await
        .map_err(ServiceError::from)?;
        let receipt = self
            .replay(&context)
            .await?
            .ok_or_else(|| ServiceError::Db(db::DbError::IdempotencyConflict))?;
        frozen_candidate_from_receipt(&receipt, &replay_command)
    }

    pub async fn append_effective(
        &self,
        command: ProjectDecisionEffectiveCommand,
        action: Option<AgentActionProvenance>,
    ) -> Result<ProjectDecisionRecord> {
        let context = decision_context(
            PROJECT_DECISION_EFFECTIVE_COMMAND,
            &command.project_id,
            command.expected_project_version,
            &command.idempotency_key,
            &command.authorization,
            &command,
            action,
        )?;
        self.append_effective_with_context(command, context).await
    }

    pub(crate) async fn append_effective_with_context(
        &self,
        command: ProjectDecisionEffectiveCommand,
        context: CommandContext,
    ) -> Result<ProjectDecisionRecord> {
        validate_effective_envelope(&command)?;
        validate_context(&context, &command.project_id, &command.authorization)?;
        if let Some(receipt) = self.replay(&context).await? {
            return load_decision(
                &self.db,
                &command.project_id,
                &outcome_string(&receipt, "decision_id")?,
            )
            .await?
            .ok_or_else(|| ServiceError::Db(db::DbError::IdempotencyConflict));
        }
        authorize_project_principal(&self.db, &command.project_id, &command.authorization).await?;
        validate_decision_context(&self.db, &command.project_id, &command.context).await?;
        if let Some(target) = command.supersedes_decision_id.as_deref() {
            require_decision(&self.db, &command.project_id, target).await?;
        }
        let context_json = encoded_context(&command.context, command.decision_class)?;
        let affected_records_json = affected_records_json(&command.context);
        let now = now_rfc3339();
        let outcome_json = json!({
            "operation": context.operation(),
            "project_id": command.project_id,
            "decision_id": command.decision_id,
            "state": command.state,
            "authority_basis": command.authority_basis,
            "domain_committed": true,
            "requires_user_authorization": false,
        })
        .to_string();
        let (receipt, execution) = command_bundle(&context, &outcome_json);
        ProjectOrchestrationRepo::append_project_decision_command(
            &*self.db,
            AppendProjectDecisionCommand {
                decision: CreateProjectDecision {
                    id: command.decision_id,
                    project_id: command.project_id,
                    expected_project_version: command.expected_project_version,
                    state: command.state,
                    decision_class: decision_class_name(command.decision_class).to_owned(),
                    question: command.question,
                    context_json,
                    options_json: serde_json::to_string(&command.options)
                        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
                    selected_outcome: command.selected_outcome,
                    rationale: command.rationale,
                    principal_type: command.authorization.principal_type,
                    principal_id: command.authorization.principal_id,
                    authority_basis: command.authority_basis,
                    authorization_action: command.authorization.authorization_action,
                    explicit_event: command.authorization.authorization_event_id,
                    authorization_occurred_at: command.authorization.authorization_occurred_at,
                    charter_revision_id: command.charter_revision_id,
                    baseline_revision_id: command.baseline_revision_id,
                    source_refs_json: serde_json::to_string(&command.source_refs)
                        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
                    affected_records_json,
                    supersedes_decision_id: command.supersedes_decision_id,
                    created_at: now,
                },
                command_receipt: Some(receipt),
                action_execution: execution,
            },
        )
        .await
        .map_err(Into::into)
    }

    pub async fn approve_candidate(
        &self,
        command: ProjectDecisionApprovalCommand,
        action: Option<AgentActionProvenance>,
    ) -> Result<ProjectDecisionRecord> {
        let context = decision_context(
            PROJECT_DECISION_CANDIDATE_APPROVE_COMMAND,
            &command.project_id,
            command.expected_project_version,
            &command.idempotency_key,
            &command.authorization,
            &command,
            action,
        )?;
        self.approve_candidate_with_context(command, context).await
    }

    pub(crate) async fn approve_candidate_with_context(
        &self,
        command: ProjectDecisionApprovalCommand,
        command_context: CommandContext,
    ) -> Result<ProjectDecisionRecord> {
        validate_approval_envelope(&command)?;
        validate_context(
            &command_context,
            &command.project_id,
            &command.authorization,
        )?;
        if let Some(receipt) = self.replay(&command_context).await? {
            return load_decision(
                &self.db,
                &command.project_id,
                &outcome_string(&receipt, "decision_id")?,
            )
            .await?
            .ok_or_else(|| ServiceError::Db(db::DbError::IdempotencyConflict));
        }
        authorize_project_principal(&self.db, &command.project_id, &command.authorization).await?;
        let project = ProjectRepo::get_by_id(&*self.db, &command.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", &command.project_id))?;
        let candidate = ProjectOrchestrationRepo::get_project_decision_candidate(
            &*self.db,
            &command.candidate_id,
        )
        .await?
        .filter(|candidate| candidate.project_id == command.project_id)
        .ok_or_else(|| ServiceError::not_found("decision_candidate", &command.candidate_id))?;
        if !matches!(candidate.lifecycle.as_str(), "draft" | "proposed") {
            return Err(ServiceError::conflict(
                "the Decision candidate is no longer awaiting approval",
            ));
        }
        let selected_outcome = required_option(&candidate.selected_outcome, "selected_outcome")?;
        let rationale = required_option(&candidate.rationale, "rationale")?;
        let context_value: Value = serde_json::from_str(&candidate.context_json).map_err(|_| {
            ServiceError::conflict("persisted Decision candidate context is invalid")
        })?;
        let candidate_context = decoded_context(&context_value)?;
        validate_decision_context(&self.db, &command.project_id, &candidate_context).await?;
        let decision_class = context_value
            .get("decision_class")
            .and_then(Value::as_str)
            .and_then(parse_decision_class)
            .ok_or_else(|| ServiceError::conflict("Decision candidate class is invalid"))?;
        if candidate_context.supersedes_decision_id.is_some()
            && candidate_context.invalidates_decision_id.is_some()
        {
            return Err(ServiceError::invalid_operation(
                "a Decision candidate may supersede or invalidate one Decision, not both",
            ));
        }
        let invalidates = candidate_context.invalidates_decision_id.is_some();
        let target_id = candidate_context
            .supersedes_decision_id
            .clone()
            .or(candidate_context.invalidates_decision_id.clone());
        if let Some(target) = target_id.as_deref() {
            require_decision(&self.db, &command.project_id, target).await?;
        }
        let decision_id = new_uuid_v4();
        let now = now_rfc3339();
        let affected_records_json = affected_records_json(&candidate_context);
        let outcome_json = json!({
            "operation": command_context.operation(),
            "project_id": command.project_id,
            "candidate_id": command.candidate_id,
            "decision_id": decision_id,
            "expected_project_version": command.expected_project_version,
            "domain_committed": true,
            "requires_user_authorization": false,
        })
        .to_string();
        let (receipt, execution) = command_bundle(&command_context, &outcome_json);
        ProjectOrchestrationRepo::approve_project_decision_candidate_command(
            &*self.db,
            ApproveProjectDecisionCandidateCommand {
                candidate_id: command.candidate_id,
                expected_candidate_version: candidate.version,
                decision: CreateProjectDecision {
                    id: decision_id,
                    project_id: command.project_id,
                    expected_project_version: command.expected_project_version,
                    state: if invalidates { "invalidated" } else { "active" }.to_owned(),
                    decision_class: decision_class_name(decision_class).to_owned(),
                    question: candidate.question,
                    context_json: candidate.context_json,
                    options_json: candidate.options_json,
                    selected_outcome,
                    rationale,
                    principal_type: command.authorization.principal_type,
                    principal_id: command.authorization.principal_id,
                    authority_basis: command.authorization.authorization_basis,
                    authorization_action: command.authorization.authorization_action,
                    explicit_event: command.authorization.authorization_event_id,
                    authorization_occurred_at: command.authorization.authorization_occurred_at,
                    charter_revision_id: candidate_context
                        .governing_charter_revision_id
                        .or(project.current_charter_revision_id),
                    baseline_revision_id: candidate_context.governing_baseline_revision_id,
                    source_refs_json: candidate.source_refs_json,
                    affected_records_json,
                    supersedes_decision_id: target_id,
                    created_at: now,
                },
                command_receipt: Some(receipt),
                action_execution: execution,
            },
        )
        .await
        .map_err(Into::into)
    }

    pub async fn reject_candidate(
        &self,
        command: ProjectDecisionRejectionCommand,
        action: Option<AgentActionProvenance>,
    ) -> Result<ProjectDecisionCandidateRecord> {
        let context = decision_context(
            PROJECT_DECISION_CANDIDATE_REJECT_COMMAND,
            &command.project_id,
            command.expected_project_version,
            &command.idempotency_key,
            &command.authorization,
            &command,
            action,
        )?;
        self.reject_candidate_with_context(command, context).await
    }

    pub(crate) async fn reject_candidate_with_context(
        &self,
        command: ProjectDecisionRejectionCommand,
        context: CommandContext,
    ) -> Result<ProjectDecisionCandidateRecord> {
        validate_rejection_envelope(&command)?;
        validate_context(&context, &command.project_id, &command.authorization)?;
        if let Some(receipt) = self.replay(&context).await? {
            let candidate_id = outcome_string(&receipt, "candidate_id")?;
            return ProjectOrchestrationRepo::get_project_decision_candidate(
                &*self.db,
                &candidate_id,
            )
            .await?
            .filter(|candidate| candidate.project_id == command.project_id)
            .ok_or_else(|| ServiceError::Db(db::DbError::IdempotencyConflict));
        }
        authorize_project_principal(&self.db, &command.project_id, &command.authorization).await?;
        let candidate = ProjectOrchestrationRepo::get_project_decision_candidate(
            &*self.db,
            &command.candidate_id,
        )
        .await?
        .filter(|candidate| candidate.project_id == command.project_id)
        .ok_or_else(|| ServiceError::not_found("decision_candidate", &command.candidate_id))?;
        let outcome_json = json!({
            "operation": context.operation(),
            "project_id": command.project_id,
            "candidate_id": command.candidate_id,
            "reason": command.reason,
            "expected_project_version": command.expected_project_version,
            "domain_committed": true,
            "requires_user_authorization": false,
        })
        .to_string();
        let (receipt, execution) = command_bundle(&context, &outcome_json);
        ProjectOrchestrationRepo::reject_project_decision_candidate_command(
            &*self.db,
            RejectProjectDecisionCandidateCommand {
                candidate_id: command.candidate_id,
                project_id: command.project_id,
                expected_project_version: command.expected_project_version,
                expected_candidate_version: candidate.version,
                reason: command.reason,
                principal_type: command.authorization.principal_type,
                principal_id: command.authorization.principal_id,
                authorization_basis: command.authorization.authorization_basis,
                authorization_action: command.authorization.authorization_action,
                explicit_event: command.authorization.authorization_event_id,
                authorization_occurred_at: command.authorization.authorization_occurred_at,
                command_receipt: Some(receipt),
                action_execution: execution,
                updated_at: now_rfc3339(),
            },
        )
        .await
        .map_err(Into::into)
    }

    async fn replay(&self, context: &CommandContext) -> Result<Option<CommandReceipt>> {
        CommandReceiptRepo::get_command_receipt(
            &*self.db,
            context.principal().principal_type(),
            context.principal().principal_id(),
            context.canonical_scope().scope_type().as_str(),
            context.canonical_scope().scope_id(),
            context.operation(),
            context.idempotency_key(),
            context.input_digest(),
        )
        .await
        .map_err(Into::into)
    }
}

fn decision_context<T: Serialize>(
    operation: &str,
    project_id: &str,
    expected_project_version: i64,
    idempotency_key: &str,
    authorization: &ProjectCommandAuthorization,
    input: &T,
    action: Option<AgentActionProvenance>,
) -> Result<CommandContext> {
    if project_id.trim().is_empty()
        || idempotency_key.trim().is_empty()
        || authorization.principal_type.trim().is_empty()
        || authorization.principal_id.trim().is_empty()
        || authorization.correlation_id.trim().is_empty()
    {
        return Err(ServiceError::invalid_operation(
            "Decision command authorization provenance is incomplete",
        ));
    }
    CommandContext::from_authorized_input(
        NewCommandContext {
            principal: CommandPrincipal {
                principal_type: authorization.principal_type.clone(),
                principal_id: authorization.principal_id.clone(),
            },
            canonical_scope: CommandScope {
                scope_type: CommandScopeType::Project,
                scope_id: project_id.to_owned(),
            },
            operation: operation.to_owned(),
            idempotency_key: idempotency_key.to_owned(),
            expected_state: ExpectedCommandState {
                versions: BTreeMap::from([(
                    "expected_project_version".to_owned(),
                    expected_project_version,
                )]),
                digests: BTreeMap::new(),
            },
            authorization_provenance: Some(AuthorizationProvenance {
                policy_result: authorization.policy_result.clone(),
                policy_revision: authorization.policy_revision.clone(),
                policy_digest: authorization.policy_digest.clone(),
                requested_permission: authorization.requested_permission.clone(),
            }),
            action_provenance: action,
            correlation_id: authorization.correlation_id.clone(),
            causation_id: authorization.causation_id.clone(),
            causation_depth: authorization.causation_depth,
        },
        input,
    )
    .map_err(|error| ServiceError::invalid_operation(format!("Decision command digest: {error}")))
}

fn validate_context(
    context: &CommandContext,
    project_id: &str,
    authorization: &ProjectCommandAuthorization,
) -> Result<()> {
    if context.canonical_scope().scope_type() != CommandScopeType::Project
        || context.canonical_scope().scope_id() != project_id
        || context.principal().principal_type() != authorization.principal_type
        || context.principal().principal_id() != authorization.principal_id
    {
        return Err(ServiceError::invalid_operation(
            "Decision command context does not match its Project authorization",
        ));
    }
    Ok(())
}

fn command_bundle(
    context: &CommandContext,
    outcome_json: &str,
) -> (CreateCommandReceipt, Option<CreateAgentActionExecution>) {
    let now = now_rfc3339();
    let execution = context
        .action_provenance
        .as_ref()
        .map(|action| CreateAgentActionExecution {
            id: new_uuid_v4(),
            action_id: action.action_id.clone(),
            expected_action_version: action.expected_action_version,
            attempt: action.attempt,
            status: AgentActionExecutionStatus::Succeeded,
            result_json: Some(outcome_json.to_owned()),
            error: None,
            executed_by_type: action.executed_by_type.clone(),
            executed_by_id: action.executed_by_id.clone(),
            idempotency_key: action.execution_idempotency_key.clone(),
            action_status: AgentActionStatus::Executed,
            action_outcome_json: Some(outcome_json.to_owned()),
            created_at: now.clone(),
            completed_at: Some(now.clone()),
            updated_at: now.clone(),
        });
    let receipt = CreateCommandReceipt {
        id: new_uuid_v4(),
        principal_type: context.principal().principal_type().to_owned(),
        principal_id: context.principal().principal_id().to_owned(),
        scope_type: context.canonical_scope().scope_type().as_str().to_owned(),
        scope_id: context.canonical_scope().scope_id().to_owned(),
        operation: context.operation().to_owned(),
        idempotency_key: context.idempotency_key().to_owned(),
        input_digest: context.input_digest().to_owned(),
        policy_result: context
            .authorization_provenance
            .as_ref()
            .map_or_else(|| "allowed".to_owned(), |value| value.policy_result.clone()),
        correlation_id: context.correlation_id().to_owned(),
        causation_id: context.causation_id.clone(),
        causation_depth: context.causation_depth,
        event_id: String::new(),
        agent_action_execution_id: execution.as_ref().map(|value| value.id.clone()),
        outcome_json: outcome_json.to_owned(),
        committed_at: now,
    };
    (receipt, execution)
}

fn outcome_string(receipt: &CommandReceipt, field: &str) -> Result<String> {
    serde_json::from_str::<Value>(&receipt.outcome_json)
        .ok()
        .and_then(|value| value.get(field).and_then(Value::as_str).map(str::to_owned))
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| ServiceError::Db(db::DbError::IdempotencyConflict))
}

fn frozen_candidate_from_receipt(
    receipt: &CommandReceipt,
    command: &ProjectDecisionCandidateCommand,
) -> Result<ProjectDecisionCandidateRecord> {
    let created_at = outcome_string(receipt, "created_at")?;
    Ok(ProjectDecisionCandidateRecord {
        id: outcome_string(receipt, "candidate_id")?,
        project_id: command.project_id.clone(),
        lifecycle: "proposed".to_owned(),
        question: command.question.clone(),
        context_json: encoded_context(&command.context, command.decision_class)?,
        options_json: serde_json::to_string(&command.options)
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
        selected_outcome: command.selected_outcome.clone(),
        rationale: command.rationale.clone(),
        principal_type: Some(command.authorization.principal_type.clone()),
        principal_id: Some(command.authorization.principal_id.clone()),
        source_refs_json: serde_json::to_string(&command.source_refs)
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
        expected_project_version: command.expected_project_version,
        effective_decision_id: None,
        version: 1,
        created_at: created_at.clone(),
        updated_at: created_at,
    })
}

fn encoded_context(context: &DecisionCandidateContext, class: DecisionClass) -> Result<String> {
    let mut value = serde_json::to_value(context)
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
    value["decision_class"] = Value::String(decision_class_name(class).to_owned());
    serde_json::to_string(&value)
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))
}

fn decoded_context(value: &Value) -> Result<DecisionCandidateContext> {
    let mut value = value.clone();
    if let Some(object) = value.as_object_mut() {
        object.remove("decision_class");
        object.remove("rejection_reason");
    }
    serde_json::from_value(value)
        .map_err(|_| ServiceError::invalid_operation("Decision context is invalid"))
}

fn affected_records_json(context: &DecisionCandidateContext) -> String {
    json!({
        "artifact_refs": context.affected_artifact_refs,
        "task_ids": context.affected_task_ids,
        "milestone_ids": context.affected_milestone_ids,
    })
    .to_string()
}

fn decision_class_name(class: DecisionClass) -> &'static str {
    match class {
        DecisionClass::UserScope => "user_scope",
        DecisionClass::ProjectImplementation => "project_implementation",
        DecisionClass::Policy => "policy",
        DecisionClass::Waiver => "waiver",
    }
}

fn parse_decision_class(value: &str) -> Option<DecisionClass> {
    match value {
        "user_scope" => Some(DecisionClass::UserScope),
        "project_implementation" => Some(DecisionClass::ProjectImplementation),
        "policy" => Some(DecisionClass::Policy),
        "waiver" => Some(DecisionClass::Waiver),
        _ => None,
    }
}

fn required_option(value: &Option<String>, field: &str) -> Result<String> {
    value
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| ServiceError::conflict(format!("{field} is required")))
}

fn validate_candidate_envelope(command: &ProjectDecisionCandidateCommand) -> Result<()> {
    validate_common(
        &command.project_id,
        command.expected_project_version,
        &command.idempotency_key,
        &command.authorization,
    )?;
    if command.question.trim().is_empty() {
        return Err(ServiceError::invalid_operation("question is required"));
    }
    validate_decision_authority(
        &command.authorization,
        PROJECT_DECISION_CANDIDATE_CREATE_COMMAND,
        "project.decision.record_candidate",
    )?;
    Ok(())
}

fn validate_effective_envelope(command: &ProjectDecisionEffectiveCommand) -> Result<()> {
    validate_common(
        &command.project_id,
        command.expected_project_version,
        &command.idempotency_key,
        &command.authorization,
    )?;
    for (field, value) in [
        ("decision_id", command.decision_id.as_str()),
        ("question", command.question.as_str()),
        ("selected_outcome", command.selected_outcome.as_str()),
        ("rationale", command.rationale.as_str()),
        ("authority_basis", command.authority_basis.as_str()),
        ("state", command.state.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(ServiceError::invalid_operation(format!(
                "{field} is required"
            )));
        }
    }
    if !matches!(command.state.as_str(), "active" | "invalidated") {
        return Err(ServiceError::invalid_operation("Decision state is invalid"));
    }
    if command.authorization.principal_type != "agent"
        || command.authorization.authorization_action != "project.decision.record_effective"
    {
        return Err(ServiceError::AuthorizationDenied {
            message: "effective implementation Decisions require bound Project Agent authority"
                .to_owned(),
        });
    }
    Ok(())
}

fn validate_approval_envelope(command: &ProjectDecisionApprovalCommand) -> Result<()> {
    validate_common(
        &command.project_id,
        command.expected_project_version,
        &command.idempotency_key,
        &command.authorization,
    )?;
    if command.candidate_id.trim().is_empty() {
        return Err(ServiceError::invalid_operation("candidate_id is required"));
    }
    if command.authorization.principal_type != "user"
        || command.authorization.authorization_action != PROJECT_DECISION_CANDIDATE_APPROVE_COMMAND
    {
        return Err(ServiceError::AuthorizationDenied {
            message: "Decision candidate approval requires interactive user authority".to_owned(),
        });
    }
    Ok(())
}

fn validate_rejection_envelope(command: &ProjectDecisionRejectionCommand) -> Result<()> {
    validate_common(
        &command.project_id,
        command.expected_project_version,
        &command.idempotency_key,
        &command.authorization,
    )?;
    if command.candidate_id.trim().is_empty() || command.reason.trim().is_empty() {
        return Err(ServiceError::invalid_operation(
            "candidate_id and rejection reason are required",
        ));
    }
    if command.authorization.principal_type != "user"
        || command.authorization.authorization_action != PROJECT_DECISION_CANDIDATE_REJECT_COMMAND
    {
        return Err(ServiceError::AuthorizationDenied {
            message: "Decision candidate rejection requires interactive user authority".to_owned(),
        });
    }
    Ok(())
}

fn validate_decision_authority(
    authorization: &ProjectCommandAuthorization,
    user_action: &str,
    agent_action: &str,
) -> Result<()> {
    let valid = match authorization.principal_type.as_str() {
        "user" => authorization.authorization_action == user_action,
        "agent" => authorization.authorization_action == agent_action,
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(ServiceError::AuthorizationDenied {
            message: "Decision command authority does not match the requested operation".to_owned(),
        })
    }
}

fn validate_common(
    project_id: &str,
    expected_project_version: i64,
    idempotency_key: &str,
    authorization: &ProjectCommandAuthorization,
) -> Result<()> {
    if project_id.trim().is_empty()
        || expected_project_version < 1
        || idempotency_key.trim().is_empty()
        || authorization.authorization_action.trim().is_empty()
        || authorization.authorization_event_id.trim().is_empty()
        || authorization.authorization_basis.trim().is_empty()
        || authorization.authorization_occurred_at.trim().is_empty()
    {
        return Err(ServiceError::invalid_operation(
            "Decision command envelope is incomplete",
        ));
    }
    Ok(())
}

async fn authorize_project_principal(
    db: &SqliteDb,
    project_id: &str,
    authorization: &ProjectCommandAuthorization,
) -> Result<()> {
    match authorization.principal_type.as_str() {
        "user" => {
            let project = ProjectRepo::get_by_id(db, project_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("project", project_id))?;
            if project.owner_id.as_deref() == Some(authorization.principal_id.as_str())
                || ProjectMemberRepo::get_member(db, project_id, &authorization.principal_id)
                    .await?
                    .is_some()
            {
                Ok(())
            } else {
                Err(ServiceError::AuthorizationDenied {
                    message: "principal is not a member of the Project".to_owned(),
                })
            }
        }
        "agent" => {
            let bound: Option<i64> = sqlx::query_scalar(
                "SELECT 1 FROM project_agent_binding
                 WHERE project_id = ? AND identity_id = ? AND state = 'active' LIMIT 1",
            )
            .bind(project_id)
            .bind(&authorization.principal_id)
            .fetch_optional(db.pool())
            .await?;
            if bound.is_some() {
                Ok(())
            } else {
                Err(ServiceError::AuthorizationDenied {
                    message: "agent principal is not actively bound to the Project".to_owned(),
                })
            }
        }
        _ => Err(ServiceError::AuthorizationDenied {
            message: "Decision commands accept only user or Project Agent principals".to_owned(),
        }),
    }
}

async fn validate_decision_context(
    db: &SqliteDb,
    project_id: &str,
    context: &DecisionCandidateContext,
) -> Result<()> {
    for artifact in &context.affected_artifact_refs {
        let row = sqlx::query(
            "SELECT render_version, rendered_digest
             FROM project_document_revision r JOIN project_document d ON d.id = r.document_id
             WHERE d.project_id = ? AND d.id = ? AND r.id = ? AND r.content_digest = ?
             UNION ALL
             SELECT r.render_version, r.rendered_digest
             FROM project_charter_revision r JOIN project_charter c ON c.id = r.charter_id
             WHERE c.project_id = ? AND c.id = ? AND r.id = ? AND r.content_digest = ?
             UNION ALL
             SELECT r.render_version, r.rendered_digest
             FROM project_execution_baseline_revision r
             JOIN project_execution_baseline b ON b.id = r.baseline_id
             WHERE b.project_id = ? AND b.id = ? AND r.id = ? AND r.content_digest = ?
             UNION ALL
             SELECT r.render_version, r.rendered_digest
             FROM project_milestone_revision r JOIN project_milestone m ON m.id = r.milestone_id
             WHERE m.project_id = ? AND m.id = ? AND r.id = ? AND r.content_digest = ? LIMIT 1",
        )
        .bind(project_id)
        .bind(&artifact.artifact_id)
        .bind(&artifact.revision_id)
        .bind(&artifact.content_digest)
        .bind(project_id)
        .bind(&artifact.artifact_id)
        .bind(&artifact.revision_id)
        .bind(&artifact.content_digest)
        .bind(project_id)
        .bind(&artifact.artifact_id)
        .bind(&artifact.revision_id)
        .bind(&artifact.content_digest)
        .bind(project_id)
        .bind(&artifact.artifact_id)
        .bind(&artifact.revision_id)
        .bind(&artifact.content_digest)
        .fetch_optional(db.pool())
        .await?
        .ok_or_else(|| ServiceError::not_found("decision_reference", &artifact.revision_id))?;
        let render_version: String = row.try_get("render_version")?;
        let render_digest: String = row.try_get("rendered_digest")?;
        if artifact
            .render_version
            .as_deref()
            .is_some_and(|value| value != render_version)
            || artifact
                .render_digest
                .as_deref()
                .is_some_and(|value| value != render_digest)
        {
            return Err(ServiceError::conflict(
                "a Decision artifact reference is stale",
            ));
        }
    }
    for task_id in &context.affected_task_ids {
        require_reference(db, "task", project_id, task_id).await?;
    }
    for milestone_id in &context.affected_milestone_ids {
        require_reference(db, "project_milestone", project_id, milestone_id).await?;
    }
    if let Some(charter_revision_id) = context.governing_charter_revision_id.as_deref() {
        let exists: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM project_charter_revision r
             JOIN project_charter c ON c.id = r.charter_id
             WHERE r.id = ? AND c.project_id = ? LIMIT 1",
        )
        .bind(charter_revision_id)
        .bind(project_id)
        .fetch_optional(db.pool())
        .await?;
        if exists.is_none() {
            return Err(ServiceError::not_found(
                "decision_reference",
                charter_revision_id,
            ));
        }
    }
    if let Some(baseline_revision_id) = context.governing_baseline_revision_id.as_deref() {
        let exists: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM project_execution_baseline_revision r
             JOIN project_execution_baseline b ON b.id = r.baseline_id
             WHERE r.id = ? AND b.project_id = ? LIMIT 1",
        )
        .bind(baseline_revision_id)
        .bind(project_id)
        .fetch_optional(db.pool())
        .await?;
        if exists.is_none() {
            return Err(ServiceError::not_found(
                "decision_reference",
                baseline_revision_id,
            ));
        }
    }
    Ok(())
}

async fn require_reference(db: &SqliteDb, table: &str, project_id: &str, id: &str) -> Result<()> {
    let statement = match table {
        "task" => "SELECT 1 FROM task WHERE id = ? AND project_id = ? LIMIT 1",
        "project_milestone" => {
            "SELECT 1 FROM project_milestone WHERE id = ? AND project_id = ? LIMIT 1"
        }
        _ => {
            return Err(ServiceError::invalid_operation(
                "invalid Decision reference table",
            ));
        }
    };
    let exists: Option<i64> = sqlx::query_scalar(statement)
        .bind(id)
        .bind(project_id)
        .fetch_optional(db.pool())
        .await?;
    if exists.is_none() {
        return Err(ServiceError::not_found("decision_reference", id));
    }
    Ok(())
}

async fn require_decision(db: &SqliteDb, project_id: &str, id: &str) -> Result<()> {
    if load_decision(db, project_id, id).await?.is_none() {
        return Err(ServiceError::not_found("decision", id));
    }
    Ok(())
}

fn payload_string(payload: &Value, field: &str) -> Result<String> {
    payload
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| ServiceError::invalid_operation(format!("{field} is required")))
}

fn payload_optional_string(payload: &Value, field: &str) -> Option<String> {
    payload
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
}

fn payload_integer(payload: &Value, field: &str) -> Result<i64> {
    payload
        .get(field)
        .and_then(Value::as_i64)
        .ok_or_else(|| ServiceError::invalid_operation(format!("{field} is required")))
}

fn payload_vec<T: serde::de::DeserializeOwned>(
    payload: &Value,
    field: &str,
    message: &str,
) -> Result<Vec<T>> {
    serde_json::from_value(
        payload
            .get(field)
            .cloned()
            .unwrap_or_else(|| Value::Array(Vec::new())),
    )
    .map_err(|_| ServiceError::invalid_operation(message))
}

fn parse_agent_adaptive_envelope(value: &str) -> Result<AdaptiveEnvelope> {
    let value: Value = serde_json::from_str(value).map_err(|error| {
        ServiceError::invalid_operation(format!(
            "active execution baseline adaptive envelope is invalid: {error}"
        ))
    })?;
    let object = value.as_object().ok_or_else(|| {
        ServiceError::invalid_operation(
            "active execution baseline adaptive envelope must be an object",
        )
    })?;
    const FIELDS: [&str; 6] = [
        "allowed_task_operations",
        "fixed_outcomes",
        "fixed_acceptance",
        "fixed_risk_classes",
        "forbidden_side_effects",
        "elevated_operations",
    ];
    if object.len() != FIELDS.len() || FIELDS.iter().any(|field| !object.contains_key(*field)) {
        return Err(ServiceError::invalid_operation(
            "active execution baseline adaptive envelope must contain exactly its required arrays",
        ));
    }
    for field in FIELDS {
        if !object.get(field).is_some_and(Value::is_array)
            || object
                .get(field)
                .and_then(Value::as_array)
                .is_some_and(|values| values.iter().any(|value| !value.is_string()))
        {
            return Err(ServiceError::invalid_operation(format!(
                "active execution baseline adaptive envelope field {field} must be a string array"
            )));
        }
    }
    serde_json::from_value(value).map_err(|error| {
        ServiceError::invalid_operation(format!(
            "active execution baseline adaptive envelope is invalid: {error}"
        ))
    })
}

fn agent_outcome_inside_envelope(
    envelope: &AdaptiveEnvelope,
    selected_outcome: Option<&str>,
) -> bool {
    envelope.fixed_outcomes.is_empty()
        || selected_outcome.is_some_and(|outcome| {
            envelope
                .fixed_outcomes
                .iter()
                .any(|fixed| fixed.as_str() == outcome)
        })
}

fn json_contains_agent_identifier(value: &str, identifier: &str) -> bool {
    fn contains(value: &Value, identifier: &str) -> bool {
        match value {
            Value::String(value) => value == identifier,
            Value::Array(values) => values.iter().any(|value| contains(value, identifier)),
            Value::Object(values) => values.values().any(|value| contains(value, identifier)),
            Value::Null | Value::Bool(_) | Value::Number(_) => false,
        }
    }
    serde_json::from_str::<Value>(value)
        .ok()
        .is_some_and(|value| contains(&value, identifier))
}

fn agent_authorization(
    action: &AgentAction,
    context: &CommandContext,
    authorization_action: &str,
) -> ProjectCommandAuthorization {
    let provenance = context.authorization_provenance.as_ref();
    let authorization_event_id = context
        .action_provenance
        .as_ref()
        .map(|value| value.action_id.clone())
        .unwrap_or_else(|| context.correlation_id().to_owned());
    let authorization_basis = "project_agent_binding_policy";
    ProjectCommandAuthorization {
        principal_type: context.principal().principal_type().to_owned(),
        principal_id: context.principal().principal_id().to_owned(),
        policy_result: provenance.map_or_else(
            || action.policy_result.to_string(),
            |value| value.policy_result.clone(),
        ),
        policy_revision: provenance.and_then(|value| value.policy_revision.clone()),
        policy_digest: provenance.and_then(|value| value.policy_digest.clone()),
        requested_permission: provenance.and_then(|value| value.requested_permission.clone()),
        correlation_id: context.correlation_id().to_owned(),
        causation_id: context.causation_id.clone(),
        causation_depth: context.causation_depth,
        authorization_event_id,
        authorization_basis: authorization_basis.to_owned(),
        authorization_action: authorization_action.to_owned(),
        authorization_occurred_at: action.created_at.clone(),
        authorization_json: json!({
            "principal": {
                "kind": context.principal().principal_type(),
                "id": context.principal().principal_id(),
            },
            "authorization_basis": authorization_basis,
            "action": authorization_action,
            "event_id": context
                .action_provenance
                .as_ref()
                .map(|value| value.action_id.as_str())
                .unwrap_or_else(|| context.correlation_id()),
            "correlation_id": context.correlation_id(),
            "causation_id": context.causation_id,
            "occurred_at": action.created_at,
        })
        .to_string(),
    }
}

async fn load_decision(
    db: &SqliteDb,
    project_id: &str,
    decision_id: &str,
) -> Result<Option<ProjectDecisionRecord>> {
    let row = sqlx::query("SELECT * FROM project_decision WHERE id = ? AND project_id = ? LIMIT 1")
        .bind(decision_id)
        .bind(project_id)
        .fetch_optional(db.pool())
        .await?;
    row.map(|row| {
        Ok::<ProjectDecisionRecord, sqlx::Error>(ProjectDecisionRecord {
            id: row.try_get("id")?,
            project_id: row.try_get("project_id")?,
            state: row.try_get("state")?,
            decision_class: row.try_get("decision_class")?,
            question: row.try_get("question")?,
            context_json: row.try_get("context_json")?,
            options_json: row.try_get("options_json")?,
            selected_outcome: row.try_get("selected_outcome")?,
            rationale: row.try_get("rationale")?,
            principal_type: row.try_get("principal_type")?,
            principal_id: row.try_get("principal_id")?,
            authority_basis: row.try_get("authority_basis")?,
            authorization_action: row.try_get("authorization_action")?,
            explicit_event: row.try_get("explicit_event")?,
            authorization_occurred_at: row.try_get("authorization_occurred_at")?,
            charter_revision_id: row.try_get("charter_revision_id")?,
            baseline_revision_id: row.try_get("baseline_revision_id")?,
            source_refs_json: row.try_get("source_refs_json")?,
            affected_records_json: row.try_get("affected_records_json")?,
            supersedes_decision_id: row.try_get("supersedes_decision_id")?,
            created_at: row.try_get("created_at")?,
        })
    })
    .transpose()
    .map_err(Into::into)
}
