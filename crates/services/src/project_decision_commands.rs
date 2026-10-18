//! Shared Project Decision command boundary.
//!
//! REST and native Project-Agent execution both enter this service. Domain
//! validation happens here, while the repository composites own the atomic
//! Decision/candidate, Project CAS, event, command-receipt, and optional
//! AgentAction-execution transaction.

use std::{collections::BTreeMap, sync::Arc};

use api_types::{
    ArtifactRef, DecisionCandidateContext, DecisionClass, ProvenanceRef, ProvenanceSourceKind,
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
    pub async fn execute_project_agent_command(
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
        // The approved Charter governs an Agent decision. Its revision, and
        // the Documents approved under it, are the artifact set a decision may
        // name; anything outside that is reconciliation_required.
        let charter = self
            .db
            .decision_governing_charter_source(project_id)
            .await?
            .ok_or_else(|| {
                ServiceError::conflict(
                    "Project decision requires the current approved Project Charter",
                )
            })?;
        let charter_artifact_id: String = charter.artifact_id()?;
        let charter_revision_id: String = charter.current_charter_revision_id()?;
        let charter_content_digest: String = charter.content_digest()?;
        let charter_render_version: String = charter.render_version()?;
        let charter_render_digest: String = charter.rendered_digest()?;
        let charter_lifecycle: String = charter.lifecycle()?;
        if charter_content_digest.trim().is_empty()
            || charter_render_version.trim().is_empty()
            || charter_render_digest.trim().is_empty()
            || charter_lifecycle != "approved"
        {
            return Err(ServiceError::invalid_operation(
                "governing Charter references are missing exact immutable digests",
            ));
        }

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
            let governed: Option<i64> = self
                .db
                .decision_governed_task_reference(task_id, project_id)
                .await?;
            if governed.is_none() {
                return Err(ServiceError::invalid_operation(
                    "decision affected Task crosses Project scope",
                ));
            }
        }

        let mut governing_artifacts = self
            .db
            .decision_governing_document_sources(project_id)
            .await?
            .into_iter()
            .map(|row| {
                Ok::<_, db::DbError>(ArtifactRef {
                    artifact_id: row.artifact_id()?,
                    revision_id: row.revision_id()?,
                    content_digest: row.content_digest()?,
                    render_version: Some(row.render_version()?),
                    render_digest: Some(row.rendered_digest()?),
                })
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        governing_artifacts.push(ArtifactRef {
            artifact_id: charter_artifact_id,
            revision_id: charter_revision_id.clone(),
            content_digest: charter_content_digest,
            render_version: Some(charter_render_version),
            render_digest: Some(charter_render_digest),
        });
        let references_governed = affected_artifact_refs.iter().all(|reference| {
            governing_artifacts
                .iter()
                .any(|allowed| allowed == reference)
        });
        let selected_outcome = payload_optional_string(payload, "selected_outcome");
        let reconciliation_reason = if references_governed {
            None
        } else {
            Some("affected artifact is outside the approved Charter".to_owned())
        };
        let expected_project_version = payload_integer(payload, "expected_project_version")?;
        let question = payload_string(payload, "question")?;
        let options: Vec<String> = payload_vec(payload, "options", "options are invalid")?;
        let rationale = payload_optional_string(payload, "rationale");
        let decision_context = DecisionCandidateContext {
            summary: Some(reconciliation_reason.as_ref().map_or_else(
                || "Implementation choice inside the approved Charter".to_owned(),
                |reason| format!("reconciliation_required: {reason}"),
            )),
            constraints: Vec::new(),
            affected_artifact_refs,
            affected_task_ids,
            affected_milestone_ids,
            governing_charter_revision_id: Some(charter_revision_id.clone()),
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

        // An in-envelope, already-authorized, reversible implementation
        // choice the Project Agent has already made is written as an
        // effective Decision, not left sitting as a pending approval
        // candidate (D19/F15). A self-declared `record_candidate` action is
        // never sufficient authority to hide an already-decided in-envelope
        // choice behind an approval step the user cannot meaningfully act
        // on -- the server, not the caller's chosen tool action, decides
        // effectiveness. Only a choice the envelope cannot already
        // authorize, or one the Agent has not actually finished deciding,
        // remains a candidate.
        if reconciliation_reason.is_none() {
            if let (Some(selected_outcome), Some(rationale)) =
                (selected_outcome.clone(), rationale.clone())
            {
                let decision_id =
                    payload_optional_string(payload, "decision_id").unwrap_or_else(new_uuid_v4);
                let decision = self
                    .append_effective_with_context(
                        ProjectDecisionEffectiveCommand {
                            project_id: project_id.to_owned(),
                            decision_id,
                            question: question.clone(),
                            context: decision_context.clone(),
                            options: options.clone(),
                            selected_outcome,
                            rationale,
                            decision_class: DecisionClass::ProjectImplementation,
                            authority_basis: "approved_project_charter".to_owned(),
                            charter_revision_id: Some(charter_revision_id.clone()),
                            source_refs: source_refs.clone(),
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
            if operation == "record_effective" {
                return Err(ServiceError::invalid_operation(
                    "an effective implementation decision requires selected_outcome and rationale",
                ));
            }
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
        // Re-check the candidate-shape invariant at approval time, not only
        // at creation: a historical row persisted before this invariant
        // existed (F15) can carry `options_json = []` alongside a populated
        // `selected_outcome`. Approving such a row would promote its
        // malformed shape into a permanent effective Decision, so it must
        // fail closed here and direct the user to reject it instead.
        let persisted_options: Vec<String> = serde_json::from_str(&candidate.options_json)
            .map_err(|_| {
                ServiceError::conflict("persisted Decision candidate options are invalid")
            })?;
        if let Some(reason) =
            candidate_option_invariant_violation(&persisted_options, Some(&selected_outcome))
        {
            return Err(ServiceError::conflict(format!(
                "this Decision candidate cannot be approved because {reason}; reject it instead"
            )));
        }
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

/// A user-choice Decision candidate requires a non-empty question, at
/// least two distinct non-empty options, and a rationale (design D19). A
/// recommended `selected_outcome`, when present, must name one of those
/// options. This is the exact invariant behind finding F15: the live
/// candidate was persisted with `options_json = []` alongside a populated
/// `selected_outcome`, so the user saw an opaque identifier with no
/// question, alternatives, or approve/reject action.
fn candidate_option_invariant_violation(
    options: &[String],
    selected_outcome: Option<&str>,
) -> Option<&'static str> {
    if options.iter().any(|option| option.trim().is_empty()) {
        return Some("an option is empty");
    }
    let distinct: std::collections::BTreeSet<&str> = options.iter().map(String::as_str).collect();
    if distinct.len() < 2 {
        return Some("it does not have at least two distinct options");
    }
    if let Some(outcome) = selected_outcome {
        if !options.iter().any(|option| option == outcome) {
            return Some("its recommendation does not name one of its options");
        }
    }
    None
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
    if let Some(reason) =
        candidate_option_invariant_violation(&command.options, command.selected_outcome.as_deref())
    {
        return Err(ServiceError::invalid_operation(format!(
            "a Decision candidate is invalid: {reason}"
        )));
    }
    if command
        .rationale
        .as_deref()
        .map(str::trim)
        .unwrap_or_default()
        .is_empty()
    {
        return Err(ServiceError::invalid_operation("rationale is required"));
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
            let bound: Option<i64> = db
                .decision_authorized_project_identity(project_id, &authorization.principal_id)
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
        let row = db
            .decision_artifact_render_source(db::DecisionArtifactRenderSourceInput {
                document_project_id: project_id,
                document_id: &artifact.artifact_id,
                document_revision_id: &artifact.revision_id,
                document_digest: &artifact.content_digest,
                charter_project_id: project_id,
                charter_id: &artifact.artifact_id,
                charter_revision_id: &artifact.revision_id,
                charter_digest: &artifact.content_digest,
                milestone_project_id: project_id,
                milestone_id: &artifact.artifact_id,
                milestone_revision_id: &artifact.revision_id,
                milestone_digest: &artifact.content_digest,
            })
            .await?
            .ok_or_else(|| ServiceError::not_found("decision_reference", &artifact.revision_id))?;
        let render_version: String = row.render_version()?;
        let render_digest: String = row.rendered_digest()?;
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
        let exists: Option<i64> = db
            .decision_governing_charter_reference(charter_revision_id, project_id)
            .await?;
        if exists.is_none() {
            return Err(ServiceError::not_found(
                "decision_reference",
                charter_revision_id,
            ));
        }
    }
    Ok(())
}

async fn require_reference(db: &SqliteDb, table: &str, project_id: &str, id: &str) -> Result<()> {
    let exists: Option<i64> = match table {
        "task" => db.decision_task_reference(id, project_id).await?,
        "project_milestone" => db.decision_milestone_reference(id, project_id).await?,
        _ => {
            return Err(ServiceError::invalid_operation(
                "invalid Decision reference table",
            ));
        }
    };
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
    let row = db
        .project_decision_command_source(decision_id, project_id)
        .await?;
    row.map(|row| {
        Ok::<ProjectDecisionRecord, db::DbError>(ProjectDecisionRecord {
            id: row.id()?,
            project_id: row.project_id()?,
            state: row.state()?,
            decision_class: row.decision_class()?,
            question: row.question()?,
            context_json: row.context_json()?,
            options_json: row.options_json()?,
            selected_outcome: row.selected_outcome()?,
            rationale: row.rationale()?,
            principal_type: row.principal_type()?,
            principal_id: row.principal_id()?,
            authority_basis: row.authority_basis()?,
            authorization_action: row.authorization_action()?,
            explicit_event: row.explicit_event()?,
            authorization_occurred_at: row.authorization_occurred_at()?,
            charter_revision_id: row.charter_revision_id()?,
            source_refs_json: row.source_refs_json()?,
            affected_records_json: row.affected_records_json()?,
            supersedes_decision_id: row.supersedes_decision_id()?,
            created_at: row.created_at()?,
        })
    })
    .transpose()
    .map_err(Into::into)
}
