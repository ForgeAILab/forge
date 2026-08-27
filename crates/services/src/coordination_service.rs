use std::{collections::BTreeSet, sync::Arc};

use db::{
    new_uuid_v4, now_rfc3339, AgentAction, AgentActionApproval, AgentActionApprovalDecision,
    AgentActionExecution, AgentActionExecutionStatus, AgentActionListQuery,
    AgentActionPolicyResult, AgentActionStatus, AgentCommitment, AgentCommitmentEvidence,
    AgentCommitmentListQuery, AgentCommitmentStatus, AgentInboxItem, AgentInboxKind,
    AgentInboxListQuery, AgentInboxStatus, AgentQuestion, AgentQuestionListQuery,
    AnswerAgentQuestion, CompleteAgentCommitment, CreateAgentAction, CreateAgentActionApproval,
    CreateAgentActionExecution, CreateAgentCommitment, CreateAgentCommitmentEvidence,
    CreateAgentInboxItem, CreateAgentQuestion, SqliteDb, Task, TransferAgentCommitment,
    UpdateAgentCommitment, UpdateAgentInboxItem,
};
use forge_agent_host::{
    classify_operation, is_allowed_project_direct_payload,
    is_project_orchestration_operation as is_catalog_project_orchestration_operation,
    operation_contract, OperationClassification, MAIN_CHARTER_DRAFT_OPERATION,
    MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION, MAIN_GENESIS_START_OPERATION,
    PROJECT_CHARTER_ADOPTION_OPERATION, PROJECT_RELEASE_OPERATION, TASK_ADAPTIVE_OPERATION,
    TASK_PROPOSE_OPERATION, TASK_REVIEW_OPERATION,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::Row;

use crate::task_service::TaskService;
use crate::{Result, ServiceError};

pub use crate::task_service::TaskProposalPayload;

const MAX_CAUSATION_DEPTH: i64 = 8;

/// Input for creating an identity-owned commitment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateCommitmentInput {
    pub id: Option<String>,
    pub owner_identity_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub title: String,
    pub description: Option<String>,
    pub status: AgentCommitmentStatus,
    pub due_at: Option<String>,
    pub correlation_id: String,
    pub originating_action_id: Option<String>,
    pub originating_task_id: Option<String>,
    pub evidence_required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateCommitmentInput {
    pub id: String,
    pub expected_version: i64,
    pub status: Option<AgentCommitmentStatus>,
    pub due_at: Option<Option<String>>,
    pub description: Option<Option<String>>,
    pub blocked_reason: Option<Option<String>>,
    pub cancellation_reason: Option<Option<String>>,
    pub actor_type: String,
    pub actor_id: String,
    pub reason: Option<String>,
    pub evidence_id: Option<String>,
    pub dedupe_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitmentEvidenceInput {
    pub id: Option<String>,
    pub commitment_id: String,
    pub evidence_type: String,
    pub evidence_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub description: Option<String>,
    pub metadata_json: String,
    pub authorized_by_type: String,
    pub authorized_by_id: String,
    pub dedupe_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompleteCommitmentInput {
    pub id: String,
    pub expected_version: i64,
    pub evidence: CommitmentEvidenceInput,
    pub actor_type: String,
    pub actor_id: String,
    pub reason: Option<String>,
    pub dedupe_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferCommitmentInput {
    pub id: String,
    pub expected_version: i64,
    pub to_identity_id: String,
    pub reason: String,
    pub actor_type: String,
    pub actor_id: String,
    pub dedupe_key: String,
}

#[derive(Clone)]
pub struct CommitmentService {
    db: Arc<SqliteDb>,
}

impl CommitmentService {
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self { db }
    }

    pub async fn create(&self, input: CreateCommitmentInput) -> Result<AgentCommitment> {
        validate_scope(&input.scope_type, &input.scope_id)?;
        let title = required_text("commitment title", &input.title)?;
        let owner_identity_id = required_text("owner identity", &input.owner_identity_id)?;
        let correlation_id = required_text("correlation id", &input.correlation_id)?;
        let now = now_rfc3339();
        db::AgentCommitmentRepo::create_commitment(
            &*self.db,
            CreateAgentCommitment {
                id: input.id.unwrap_or_else(new_uuid_v4),
                owner_identity_id,
                scope_type: input.scope_type,
                scope_id: input.scope_id,
                title,
                description: input.description,
                status: input.status,
                due_at: input.due_at,
                correlation_id,
                originating_action_id: input.originating_action_id,
                originating_task_id: input.originating_task_id,
                evidence_required: input.evidence_required,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .map_err(Into::into)
    }

    pub async fn get(&self, id: &str) -> Result<AgentCommitment> {
        db::AgentCommitmentRepo::get_commitment(&*self.db, id)
            .await?
            .ok_or_else(|| ServiceError::not_found("commitment", id))
    }

    pub async fn list(&self, query: AgentCommitmentListQuery) -> Result<Vec<AgentCommitment>> {
        db::AgentCommitmentRepo::list_commitments(&*self.db, query)
            .await
            .map_err(Into::into)
    }

    pub async fn update(&self, input: UpdateCommitmentInput) -> Result<AgentCommitment> {
        let dedupe_key = required_text("commitment update dedupe key", &input.dedupe_key)?;
        if input.status == Some(AgentCommitmentStatus::Completed) {
            return Err(ServiceError::invalid_operation(
                "completion must include authorized evidence",
            ));
        }
        let current = self.get(&input.id).await?;
        if let Some(status) = &input.status {
            let reason = input.reason.as_deref().or_else(|| {
                input
                    .cancellation_reason
                    .as_ref()
                    .and_then(Option::as_deref)
            });
            validate_commitment_transition(&current.status, status, reason)?;
        }
        db::AgentCommitmentRepo::update_commitment(
            &*self.db,
            UpdateAgentCommitment {
                id: input.id,
                expected_version: input.expected_version,
                status: input.status,
                due_at: input.due_at,
                description: input.description,
                blocked_reason: input.blocked_reason,
                cancellation_reason: input.cancellation_reason,
                actor_type: required_text("actor type", &input.actor_type)?,
                actor_id: required_text("actor id", &input.actor_id)?,
                reason: input.reason,
                evidence_id: input.evidence_id,
                dedupe_key,
                updated_at: now_rfc3339(),
            },
        )
        .await
        .map_err(Into::into)
    }

    pub async fn complete(&self, input: CompleteCommitmentInput) -> Result<AgentCommitment> {
        let commitment = self.get(&input.id).await?;
        if !commitment.evidence_required {
            // Evidence is still required for an authorized completion event;
            // the flag only controls whether a caller may opt into this policy
            // at creation time.  Silent request delivery is never evidence.
        }
        validate_evidence(&input.evidence)?;
        if input.evidence.commitment_id != input.id {
            return Err(ServiceError::invalid_operation(
                "evidence must reference the commitment being completed",
            ));
        }
        validate_commitment_transition(
            &commitment.status,
            &AgentCommitmentStatus::Completed,
            input.reason.as_deref(),
        )?;
        let actor_id = required_text("actor id", &input.actor_id)?;
        let actor_type = required_text("actor type", &input.actor_type)?;
        if input.evidence.authorized_by_id != actor_id
            && input.evidence.authorized_by_type != "forge"
        {
            return Err(ServiceError::invalid_operation(
                "completion evidence is not authorized by the completing actor",
            ));
        }
        let evidence = to_db_evidence(input.evidence, now_rfc3339());
        db::AgentCommitmentRepo::complete_commitment(
            &*self.db,
            CompleteAgentCommitment {
                id: input.id,
                expected_version: input.expected_version,
                evidence,
                actor_type,
                actor_id,
                reason: input.reason,
                dedupe_key: required_text("completion dedupe key", &input.dedupe_key)?,
                completed_at: now_rfc3339(),
                updated_at: now_rfc3339(),
            },
        )
        .await
        .map_err(Into::into)
    }

    pub async fn transfer(&self, input: TransferCommitmentInput) -> Result<AgentCommitment> {
        let reason = required_text("transfer reason", &input.reason)?;
        let target = required_text("transfer target identity", &input.to_identity_id)?;
        let current = self.get(&input.id).await?;
        if current.owner_identity_id == target {
            return Err(ServiceError::invalid_operation(
                "commitment is already owned by the target identity",
            ));
        }
        db::AgentCommitmentRepo::transfer_commitment(
            &*self.db,
            TransferAgentCommitment {
                id: input.id,
                expected_version: input.expected_version,
                to_identity_id: target,
                reason,
                actor_type: required_text("actor type", &input.actor_type)?,
                actor_id: required_text("actor id", &input.actor_id)?,
                dedupe_key: required_text("transfer dedupe key", &input.dedupe_key)?,
                updated_at: now_rfc3339(),
            },
        )
        .await
        .map_err(Into::into)
    }

    pub async fn cancel(
        &self,
        id: String,
        expected_version: i64,
        reason: String,
        actor_type: String,
        actor_id: String,
        dedupe_key: String,
    ) -> Result<AgentCommitment> {
        let reason = required_text("cancellation reason", &reason)?;
        self.update(UpdateCommitmentInput {
            id,
            expected_version,
            status: Some(AgentCommitmentStatus::Cancelled),
            due_at: None,
            description: None,
            blocked_reason: None,
            cancellation_reason: Some(Some(reason.clone())),
            actor_type,
            actor_id,
            reason: Some(reason),
            evidence_id: None,
            dedupe_key,
        })
        .await
    }

    pub async fn add_evidence(
        &self,
        input: CommitmentEvidenceInput,
    ) -> Result<AgentCommitmentEvidence> {
        validate_evidence(&input)?;
        db::AgentCommitmentRepo::add_commitment_evidence(
            &*self.db,
            to_db_evidence(input, now_rfc3339()),
        )
        .await
        .map_err(Into::into)
    }

    pub async fn evidence(&self, commitment_id: &str) -> Result<Vec<AgentCommitmentEvidence>> {
        db::AgentCommitmentRepo::list_commitment_evidence(&*self.db, commitment_id)
            .await
            .map_err(Into::into)
    }
}

/// Input for a durable agent inbox item.  The dedupe key is scoped to the
/// recipient identity, so replaying an outcome returns the original row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliverInboxInput {
    pub id: Option<String>,
    pub recipient_identity_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub kind: AgentInboxKind,
    pub title: String,
    pub body: String,
    pub payload_json: String,
    pub source_type: Option<String>,
    pub source_id: Option<String>,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub dedupe_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AskQuestionInput {
    pub id: Option<String>,
    pub inbox_item_id: Option<String>,
    pub recipient_identity_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub question: String,
    pub context_json: String,
    pub asked_by_type: String,
    pub asked_by_id: String,
    pub due_at: Option<String>,
    pub correlation_id: String,
    pub inbox_title: Option<String>,
    pub inbox_dedupe_key: String,
}

#[derive(Clone)]
pub struct AgentInboxService {
    db: Arc<SqliteDb>,
}

impl AgentInboxService {
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self { db }
    }

    pub async fn deliver(&self, input: DeliverInboxInput) -> Result<AgentInboxItem> {
        validate_scope(&input.scope_type, &input.scope_id)?;
        let body = required_text("inbox body", &input.body)?;
        let title = required_text("inbox title", &input.title)?;
        let recipient = required_text("recipient identity", &input.recipient_identity_id)?;
        let dedupe_key = required_text("inbox dedupe key", &input.dedupe_key)?;
        db::AgentInboxRepo::create_inbox_item(
            &*self.db,
            CreateAgentInboxItem {
                id: input.id.unwrap_or_else(new_uuid_v4),
                recipient_identity_id: recipient,
                scope_type: input.scope_type,
                scope_id: input.scope_id,
                kind: input.kind,
                status: AgentInboxStatus::Unread,
                title,
                body,
                payload_json: input.payload_json,
                source_type: input.source_type,
                source_id: input.source_id,
                correlation_id: required_text("correlation id", &input.correlation_id)?,
                causation_id: input.causation_id,
                dedupe_key,
                created_at: now_rfc3339(),
                updated_at: now_rfc3339(),
            },
        )
        .await
        .map_err(Into::into)
    }

    pub async fn get(&self, id: &str) -> Result<AgentInboxItem> {
        db::AgentInboxRepo::get_inbox_item(&*self.db, id)
            .await?
            .ok_or_else(|| ServiceError::not_found("inbox item", id))
    }

    pub async fn list(&self, query: AgentInboxListQuery) -> Result<Vec<AgentInboxItem>> {
        db::AgentInboxRepo::list_inbox_items(&*self.db, query)
            .await
            .map_err(Into::into)
    }

    pub async fn set_status(
        &self,
        id: String,
        expected_version: i64,
        status: AgentInboxStatus,
    ) -> Result<AgentInboxItem> {
        db::AgentInboxRepo::update_inbox_item(
            &*self.db,
            UpdateAgentInboxItem {
                id,
                expected_version,
                status,
                updated_at: now_rfc3339(),
            },
        )
        .await
        .map_err(Into::into)
    }

    pub async fn ask_question(&self, input: AskQuestionInput) -> Result<AgentQuestion> {
        validate_scope(&input.scope_type, &input.scope_id)?;
        let question = required_text("question", &input.question)?;
        let inbox_id = input.inbox_item_id.unwrap_or_else(new_uuid_v4);
        let question_id = input.id.unwrap_or_else(new_uuid_v4);
        let now = now_rfc3339();
        db::AgentInboxRepo::create_question_with_inbox(
            &*self.db,
            CreateAgentInboxItem {
                id: inbox_id.clone(),
                recipient_identity_id: required_text(
                    "recipient identity",
                    &input.recipient_identity_id,
                )?,
                scope_type: input.scope_type.clone(),
                scope_id: input.scope_id.clone(),
                kind: AgentInboxKind::Question,
                status: AgentInboxStatus::Unread,
                title: input
                    .inbox_title
                    .unwrap_or_else(|| "Question requires an answer".to_owned()),
                body: question.clone(),
                payload_json: input.context_json.clone(),
                source_type: Some("question".to_owned()),
                source_id: Some(question_id.clone()),
                correlation_id: required_text("correlation id", &input.correlation_id)?,
                causation_id: None,
                dedupe_key: required_text("question inbox dedupe key", &input.inbox_dedupe_key)?,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
            CreateAgentQuestion {
                id: question_id,
                recipient_identity_id: required_text(
                    "recipient identity",
                    &input.recipient_identity_id,
                )?,
                scope_type: input.scope_type,
                scope_id: input.scope_id,
                question,
                context_json: input.context_json,
                asked_by_type: required_text("question actor type", &input.asked_by_type)?,
                asked_by_id: required_text("question actor id", &input.asked_by_id)?,
                inbox_item_id: Some(inbox_id),
                due_at: input.due_at,
                correlation_id: required_text("correlation id", &input.correlation_id)?,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .map_err(Into::into)
    }

    pub async fn get_question(&self, id: &str) -> Result<AgentQuestion> {
        db::AgentInboxRepo::get_question(&*self.db, id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent question", id))
    }

    pub async fn list_questions(
        &self,
        query: AgentQuestionListQuery,
    ) -> Result<Vec<AgentQuestion>> {
        db::AgentInboxRepo::list_questions(&*self.db, query)
            .await
            .map_err(Into::into)
    }

    pub async fn answer_question(
        &self,
        id: String,
        expected_version: i64,
        answer: String,
        answered_by_type: String,
        answered_by_id: String,
    ) -> Result<AgentQuestion> {
        db::AgentInboxRepo::answer_question(
            &*self.db,
            AnswerAgentQuestion {
                id,
                expected_version,
                answer: required_text("answer", &answer)?,
                answered_by_type: required_text("answer actor type", &answered_by_type)?,
                answered_by_id: required_text("answer actor id", &answered_by_id)?,
                updated_at: now_rfc3339(),
            },
        )
        .await
        .map_err(Into::into)
    }
}

/// A typed policy result is persisted with every proposal.  The service never
/// treats a denied or pending proposal as authoritative work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProposeActionInput {
    pub id: Option<String>,
    pub actor_identity_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub operation: String,
    pub payload_json: String,
    pub dedupe_key: String,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub causation_depth: i64,
    pub requested_permission: String,
    pub policy_reason: Option<String>,
    pub target_type: Option<String>,
    pub target_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApproveActionInput {
    pub action_id: String,
    pub expected_version: i64,
    pub approver_identity_id: String,
    pub decision: AgentActionApprovalDecision,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecuteActionInput {
    pub action_id: String,
    pub expected_version: i64,
    pub attempt: i64,
    pub result_json: Option<String>,
    pub error: Option<String>,
    pub executed_by_type: String,
    pub executed_by_id: String,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecuteTaskProposalInput {
    pub action_id: String,
    pub expected_version: i64,
    pub executed_by_type: String,
    pub executed_by_id: String,
    pub idempotency_key: String,
}

#[derive(Debug, Clone)]
pub struct ExecutedTaskProposal {
    pub task: Task,
    pub execution: AgentActionExecution,
}

#[derive(Clone)]
pub struct AgentActionService {
    db: Arc<SqliteDb>,
}

impl AgentActionService {
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self { db }
    }

    /// Evaluate the server-issued policy ceiling for a direct command without
    /// creating an AgentAction.  Directly admitted Main commands still use
    /// the same identity/profile/scope policy as approval-queue proposals;
    /// only the durable command receipt, not an action row, records success.
    pub async fn evaluate_direct_command_policy(
        &self,
        actor_identity_id: &str,
        scope_type: &str,
        scope_id: &str,
        requested_permission: &str,
        operation: &str,
        payload_json: Option<&str>,
    ) -> Result<(AgentActionPolicyResult, Option<String>)> {
        evaluate_action_policy(
            &self.db,
            actor_identity_id,
            scope_type,
            scope_id,
            requested_permission,
            operation,
            payload_json,
        )
        .await
    }

    pub async fn propose(&self, input: ProposeActionInput) -> Result<AgentAction> {
        validate_scope(&input.scope_type, &input.scope_id)?;
        if input.operation == MAIN_CHARTER_DRAFT_OPERATION {
            return Err(ServiceError::invalid_operation(
                "charter.draft is a direct Main command and cannot enter the AgentAction queue",
            ));
        }
        if input.causation_depth > MAX_CAUSATION_DEPTH || input.causation_depth < 0 {
            return Err(ServiceError::invalid_operation(
                "action causation depth exceeds the reaction bound",
            ));
        }
        let actor_identity_id = required_text("action actor identity", &input.actor_identity_id)?;
        let operation = required_text("action operation", &input.operation)?;
        let requested_permission =
            required_text("requested permission", &input.requested_permission)?;
        let dedupe_key = required_text("action dedupe key", &input.dedupe_key)?;
        let payload_value: Value = serde_json::from_str(&input.payload_json).map_err(|error| {
            ServiceError::invalid_operation(format!("action payload is invalid: {error}"))
        })?;
        match classify_operation(&operation, Some(&payload_value)) {
            OperationClassification::ApprovalRequiredAction => {}
            OperationClassification::DirectCommand => {
                return Err(ServiceError::invalid_operation(
                    "direct commands use their shared command service and cannot enter the AgentAction queue",
                ));
            }
            OperationClassification::Query => {
                return Err(ServiceError::invalid_operation(
                    "query operations use the shared read boundary and cannot enter the AgentAction queue",
                ));
            }
            OperationClassification::Denied => {
                return Err(ServiceError::AuthorizationDenied {
                    message: "operation is denied by the canonical native operation catalog"
                        .to_owned(),
                });
            }
        }
        if operation == TASK_PROPOSE_OPERATION {
            serde_json::from_str::<TaskProposalPayload>(&input.payload_json).map_err(|error| {
                ServiceError::invalid_operation(format!(
                    "task proposal payload is invalid: {error}"
                ))
            })?;
        }
        let payload_hash = sha256_hex(input.payload_json.as_bytes());
        let expected_payload_hash = payload_hash.clone();
        let (policy_result, evaluated_reason) = evaluate_action_policy(
            &self.db,
            &actor_identity_id,
            &input.scope_type,
            &input.scope_id,
            &requested_permission,
            &operation,
            Some(&input.payload_json),
        )
        .await?;
        let policy_reason = input.policy_reason.or(evaluated_reason);
        let status = match &policy_result {
            AgentActionPolicyResult::Allowed => AgentActionStatus::Proposed,
            AgentActionPolicyResult::ApprovalRequired => AgentActionStatus::PendingApproval,
            AgentActionPolicyResult::Denied => AgentActionStatus::Denied,
        };
        let action = db::AgentActionRepo::create_action(
            &*self.db,
            CreateAgentAction {
                id: input.id.unwrap_or_else(new_uuid_v4),
                actor_identity_id,
                scope_type: input.scope_type,
                scope_id: input.scope_id,
                operation,
                payload_json: input.payload_json,
                payload_hash,
                dedupe_key,
                correlation_id: required_text("correlation id", &input.correlation_id)?,
                causation_id: input.causation_id,
                causation_depth: input.causation_depth,
                requested_permission,
                policy_result,
                policy_reason,
                status,
                target_type: input.target_type,
                target_id: input.target_id,
                created_at: now_rfc3339(),
                updated_at: now_rfc3339(),
            },
        )
        .await
        .map_err(ServiceError::from)?;
        if action.payload_hash != expected_payload_hash {
            return Err(ServiceError::conflict(
                "deduplicated action payload does not match the original proposal",
            ));
        }
        Ok(action)
    }

    pub async fn get(&self, id: &str) -> Result<AgentAction> {
        db::AgentActionRepo::get_action(&*self.db, id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent action", id))
    }

    pub async fn list(&self, query: AgentActionListQuery) -> Result<Vec<AgentAction>> {
        db::AgentActionRepo::list_actions(&*self.db, query)
            .await
            .map_err(Into::into)
    }

    pub async fn approve(&self, input: ApproveActionInput) -> Result<AgentActionApproval> {
        let action = self.get(&input.action_id).await?;
        authorize_action_approver(&self.db, &action, &input.approver_identity_id).await?;
        if action.actor_identity_id == input.approver_identity_id {
            return Err(ServiceError::invalid_operation(
                "an action proposer cannot approve its own protected action",
            ));
        }
        if action.policy_result != AgentActionPolicyResult::ApprovalRequired
            || action.status != AgentActionStatus::PendingApproval
        {
            return Err(ServiceError::invalid_operation(
                "action is not waiting for approval",
            ));
        }
        let decision = input.decision.clone();
        let resulting_status = match &decision {
            AgentActionApprovalDecision::Approved => AgentActionStatus::Approved,
            AgentActionApprovalDecision::Denied => AgentActionStatus::Denied,
        };
        db::AgentActionRepo::record_action_approval(
            &*self.db,
            CreateAgentActionApproval {
                id: new_uuid_v4(),
                action_id: input.action_id,
                expected_action_version: input.expected_version,
                approver_identity_id: required_text(
                    "approver identity",
                    &input.approver_identity_id,
                )?,
                decision,
                reason: input.reason,
                resulting_status,
                created_at: now_rfc3339(),
                updated_at: now_rfc3339(),
            },
        )
        .await
        .map_err(Into::into)
    }

    pub async fn execute(&self, input: ExecuteActionInput) -> Result<AgentActionExecution> {
        let action = self.get(&input.action_id).await?;
        if matches!(
            action.operation.as_str(),
            PROJECT_RELEASE_OPERATION | "project.release" | "project.milestone.release"
        ) {
            return Err(ServiceError::invalid_operation(
                "Project Agent release candidates are request-only and cannot execute a final release",
            ));
        }
        if is_non_executable_authority_operation(&action.operation) {
            return Err(ServiceError::invalid_operation(
                "authority operations require their typed domain executor or explicit user transaction",
            ));
        }
        if is_typed_orchestration_operation(&action.operation) {
            return Err(ServiceError::invalid_operation(
                "typed orchestration actions require their domain executor",
            ));
        }
        // Idempotent retries must be resolved before re-evaluating the
        // proposal's current status.  A successful execution moves the
        // action to `executed`, so checking the approval gate first would
        // incorrectly reject an otherwise identical replay of a protected
        // action.
        if let Some(existing) =
            db::AgentActionRepo::get_successful_action_execution(&*self.db, &input.action_id)
                .await?
        {
            if existing.idempotency_key != input.idempotency_key {
                return Err(ServiceError::conflict(
                    "action already has a successful execution with a different idempotency key",
                ));
            }
            return Ok(existing);
        }
        if matches!(
            action.status,
            AgentActionStatus::Denied | AgentActionStatus::Cancelled
        ) {
            return Err(ServiceError::invalid_operation(
                "denied or cancelled action cannot execute",
            ));
        }
        if action.policy_result == AgentActionPolicyResult::ApprovalRequired
            && action.status != AgentActionStatus::Approved
        {
            return Err(ServiceError::invalid_operation(
                "action requires approval before execution",
            ));
        }
        let success = input.error.is_none();
        let status = if success {
            AgentActionExecutionStatus::Succeeded
        } else {
            AgentActionExecutionStatus::Failed
        };
        let action_status = if success {
            AgentActionStatus::Executed
        } else {
            AgentActionStatus::Failed
        };
        db::AgentActionRepo::record_action_execution(
            &*self.db,
            CreateAgentActionExecution {
                id: new_uuid_v4(),
                action_id: input.action_id,
                expected_action_version: input.expected_version,
                attempt: input.attempt.max(1),
                status,
                result_json: input.result_json.clone(),
                error: input.error.clone(),
                executed_by_type: required_text("executor type", &input.executed_by_type)?,
                executed_by_id: required_text("executor id", &input.executed_by_id)?,
                idempotency_key: required_text(
                    "execution idempotency key",
                    &input.idempotency_key,
                )?,
                action_status,
                action_outcome_json: input.result_json,
                created_at: now_rfc3339(),
                completed_at: Some(now_rfc3339()),
                updated_at: now_rfc3339(),
            },
        )
        .await
        .map_err(Into::into)
    }

    /// Executes an admitted `task.propose` through the atomic TaskService
    /// command.  The command owns Task/governance/event/receipt/action
    /// finalization; this service only resolves the source Action and returns
    /// its durable execution projection.
    pub async fn execute_task_proposal(
        &self,
        task_service: &TaskService,
        input: ExecuteTaskProposalInput,
    ) -> Result<ExecutedTaskProposal> {
        let action = self.get(&input.action_id).await?;
        let task = task_service
            .execute_task_proposal_command(
                &action,
                input.expected_version,
                &input.executed_by_type,
                &input.executed_by_id,
                &input.idempotency_key,
            )
            .await?;
        let execution =
            db::AgentActionRepo::get_successful_action_execution(&*self.db, &input.action_id)
                .await?
                .ok_or_else(|| ServiceError::Db(db::DbError::IdempotencyConflict))?;
        Ok(ExecutedTaskProposal { task, execution })
    }
}

fn required_text(field: &'static str, value: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ServiceError::invalid_operation(format!(
            "{field} must not be empty"
        )));
    }
    Ok(trimmed.to_owned())
}

fn validate_scope(scope_type: &str, scope_id: &str) -> Result<()> {
    if !matches!(
        scope_type,
        "account" | "project" | "agent_chat" | "task" | "agent"
    ) {
        return Err(ServiceError::invalid_operation(
            "unsupported canonical scope type",
        ));
    }
    required_text("scope id", scope_id).map(|_| ())
}

fn validate_commitment_transition(
    from: &AgentCommitmentStatus,
    to: &AgentCommitmentStatus,
    reason: Option<&str>,
) -> Result<()> {
    if from == to {
        return Ok(());
    }
    let allowed = match from {
        AgentCommitmentStatus::Proposed => matches!(
            to,
            AgentCommitmentStatus::Open
                | AgentCommitmentStatus::Accepted
                | AgentCommitmentStatus::Completed
                | AgentCommitmentStatus::Cancelled
        ),
        AgentCommitmentStatus::Open => matches!(
            to,
            AgentCommitmentStatus::Accepted
                | AgentCommitmentStatus::InProgress
                | AgentCommitmentStatus::Blocked
                | AgentCommitmentStatus::Completed
                | AgentCommitmentStatus::Cancelled
        ),
        AgentCommitmentStatus::Accepted => matches!(
            to,
            AgentCommitmentStatus::InProgress
                | AgentCommitmentStatus::Blocked
                | AgentCommitmentStatus::Completed
                | AgentCommitmentStatus::Cancelled
        ),
        AgentCommitmentStatus::InProgress => matches!(
            to,
            AgentCommitmentStatus::Blocked
                | AgentCommitmentStatus::Completed
                | AgentCommitmentStatus::Cancelled
        ),
        AgentCommitmentStatus::Blocked => matches!(
            to,
            AgentCommitmentStatus::Open
                | AgentCommitmentStatus::Accepted
                | AgentCommitmentStatus::InProgress
                | AgentCommitmentStatus::Cancelled
        ),
        AgentCommitmentStatus::Completed | AgentCommitmentStatus::Cancelled => false,
    };
    if !allowed {
        return Err(ServiceError::invalid_operation(format!(
            "commitment cannot transition from {from} to {to}"
        )));
    }
    if matches!(
        to,
        AgentCommitmentStatus::Blocked | AgentCommitmentStatus::Cancelled
    ) && !reason.is_some_and(|value| !value.trim().is_empty())
    {
        return Err(ServiceError::invalid_operation(format!(
            "transition to {to} requires a reason"
        )));
    }
    Ok(())
}

fn validate_evidence(input: &CommitmentEvidenceInput) -> Result<()> {
    validate_scope(&input.scope_type, &input.scope_id)?;
    required_text("evidence type", &input.evidence_type)?;
    required_text("evidence id", &input.evidence_id)?;
    required_text("evidence author type", &input.authorized_by_type)?;
    required_text("evidence author id", &input.authorized_by_id)?;
    required_text("evidence dedupe key", &input.dedupe_key)?;
    Ok(())
}

fn to_db_evidence(input: CommitmentEvidenceInput, now: String) -> CreateAgentCommitmentEvidence {
    CreateAgentCommitmentEvidence {
        id: input.id.unwrap_or_else(new_uuid_v4),
        commitment_id: input.commitment_id,
        evidence_type: input.evidence_type,
        evidence_id: input.evidence_id,
        scope_type: input.scope_type,
        scope_id: input.scope_id,
        description: input.description,
        metadata_json: input.metadata_json,
        authorized_by_type: input.authorized_by_type,
        authorized_by_id: input.authorized_by_id,
        dedupe_key: input.dedupe_key,
        created_at: now,
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

/// Check the concrete canonical-scope reference before evaluating any JSON
/// permission ceiling.  Scope type alone is never authority: an identity
/// must own the account scope, hold an active Project/Agent Chat binding, or
/// be assigned to a Task.  Existing-but-inaccessible scopes are
/// returned as a denial so the proposal can be audited without granting it;
/// missing references are not disclosed as an actionable proposal.
async fn action_scope_access(
    db: &SqliteDb,
    actor_identity_id: &str,
    scope_type: &str,
    scope_id: &str,
    requested_permission: &str,
) -> Result<Option<String>> {
    let owner_id =
        sqlx::query_scalar::<_, Option<String>>("SELECT owner_id FROM agent_identity WHERE id = ?")
            .bind(actor_identity_id)
            .fetch_optional(db.pool())
            .await?
            .ok_or_else(|| ServiceError::not_found("agent identity", actor_identity_id))?;

    match scope_type {
        "account" => {
            if owner_id.as_deref() != Some(scope_id) {
                return Ok(Some(
                    "actor identity is not owned by the requested account scope".to_owned(),
                ));
            }
        }
        "project" => {
            let exists = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM project WHERE id = ?")
                .bind(scope_id)
                .fetch_one(db.pool())
                .await?;
            if exists == 0 {
                return Err(ServiceError::not_found("project", scope_id));
            }
            let binding = sqlx::query(
                "SELECT permission_ceiling_json FROM project_agent_binding
                 WHERE project_id = ? AND identity_id = ? AND state = 'active'",
            )
            .bind(scope_id)
            .bind(actor_identity_id)
            .fetch_optional(db.pool())
            .await?;
            let Some(binding) = binding else {
                return Ok(Some(
                    "actor identity has no active binding in the requested Project".to_owned(),
                ));
            };
            let permission_ceiling: String = binding.try_get("permission_ceiling_json")?;
            if !permission_set(&permission_ceiling).contains(requested_permission) {
                return Ok(Some(
                    "requested permission is outside the Project binding ceiling".to_owned(),
                ));
            }
        }
        "agent_chat" => {
            let chat =
                sqlx::query("SELECT kind, account_id, project_id FROM agent_chat WHERE id = ?")
                    .bind(scope_id)
                    .fetch_optional(db.pool())
                    .await?;
            let Some(chat) = chat else {
                return Err(ServiceError::not_found("agent_chat", scope_id));
            };
            let kind: String = chat.try_get("kind")?;
            match kind.as_str() {
                "account_main" => {
                    let account_id: Option<String> = chat.try_get("account_id")?;
                    if owner_id != account_id {
                        return Ok(Some(
                            "actor identity does not own the requested Main Agent Chat".to_owned(),
                        ));
                    }
                }
                "project" => {
                    let project_id: Option<String> = chat.try_get("project_id")?;
                    let Some(project_id) = project_id else {
                        return Ok(Some("Agent Chat has no Project binding".to_owned()));
                    };
                    let binding = sqlx::query_scalar::<_, String>(
                        "SELECT permission_ceiling_json FROM project_agent_binding
                         WHERE project_id = ? AND identity_id = ? AND state = 'active'",
                    )
                    .bind(project_id)
                    .bind(actor_identity_id)
                    .fetch_optional(db.pool())
                    .await?;
                    let Some(permission_ceiling) = binding else {
                        return Ok(Some(
                            "actor identity has no active Agent Chat binding".to_owned(),
                        ));
                    };
                    if !permission_set(&permission_ceiling).contains(requested_permission) {
                        return Ok(Some(
                            "requested permission is outside the Agent Chat binding ceiling"
                                .to_owned(),
                        ));
                    }
                }
                _ => return Ok(Some("Agent Chat kind is not admitted".to_owned())),
            }
        }
        "task" => {
            let task_exists = sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM task WHERE id = ? AND deleted_at IS NULL",
            )
            .bind(scope_id)
            .fetch_one(db.pool())
            .await?;
            if task_exists == 0 {
                return Err(ServiceError::not_found("task", scope_id));
            }
            let task =
                sqlx::query("SELECT status, assignee_type, assignee_id FROM task WHERE id = ?")
                    .bind(scope_id)
                    .fetch_one(db.pool())
                    .await?;
            let direct_assignee_type: Option<String> = task.try_get("assignee_type")?;
            let direct_assignee_id: Option<String> = task.try_get("assignee_id")?;
            let status: String = task.try_get("status")?;
            let assignments = sqlx::query(
                "SELECT role_name FROM task_role_assignment
                 WHERE task_id = ? AND assignee_type = 'agent' AND assignee_id = ?",
            )
            .bind(scope_id)
            .bind(actor_identity_id)
            .fetch_all(db.pool())
            .await?;
            let assigned = direct_assignee_type.as_deref() == Some("agent")
                && direct_assignee_id.as_deref() == Some(actor_identity_id)
                || !assignments.is_empty();
            if !assigned {
                return Ok(Some(
                    "actor identity is not assigned to the requested Task".to_owned(),
                ));
            }
            if requested_permission == "task_write"
                && matches!(status.as_str(), "done" | "cancelled")
            {
                return Ok(Some(
                    "the Task workflow no longer admits writes in its terminal state".to_owned(),
                ));
            }
            let roles = assignments
                .iter()
                .filter_map(|row| row.try_get::<String, _>("role_name").ok())
                .collect::<Vec<_>>();
            if requested_permission == "task_write"
                && !roles.is_empty()
                && roles
                    .iter()
                    .all(|role| role.eq_ignore_ascii_case("reviewer"))
            {
                return Ok(Some(
                    "reviewer assignments cannot perform Task writes".to_owned(),
                ));
            }
            if requested_permission == "propose_review"
                && !roles.iter().any(|role| {
                    role.eq_ignore_ascii_case("reviewer") || role.eq_ignore_ascii_case("review")
                })
            {
                return Ok(Some(
                    "the Task assignment does not admit review proposals".to_owned(),
                ));
            }
        }
        "agent" if actor_identity_id != scope_id => {
            return Ok(Some(
                "actor identity cannot act through another identity scope".to_owned(),
            ));
        }
        _ => {}
    }
    Ok(None)
}

async fn authorize_action_approver(
    db: &SqliteDb,
    action: &AgentAction,
    approver_identity_id: &str,
) -> Result<()> {
    let approver = sqlx::query(
        "SELECT owner_id, paused, archived_at, account_permission_ceiling,
                selected_profile_id
         FROM agent_identity WHERE id = ?",
    )
    .bind(approver_identity_id)
    .fetch_optional(db.pool())
    .await?
    .ok_or_else(|| ServiceError::not_found("approver identity", approver_identity_id))?;
    let paused: i64 = approver.try_get("paused")?;
    let archived_at: Option<String> = approver.try_get("archived_at")?;
    if paused != 0 || archived_at.is_some() {
        return Err(ServiceError::invalid_operation(
            "paused or archived identities cannot approve actions",
        ));
    }
    let approver_owner: Option<String> = approver.try_get("owner_id")?;
    let account_permission_ceiling: String = approver.try_get("account_permission_ceiling")?;
    let selected_profile_id: Option<String> = approver.try_get("selected_profile_id")?;
    let profile_policy = if let Some(profile_id) = selected_profile_id {
        sqlx::query_scalar::<_, String>(
            "SELECT tool_policy_json FROM agent_profile
             WHERE id = ? AND identity_id = ?",
        )
        .bind(profile_id)
        .bind(approver_identity_id)
        .fetch_optional(db.pool())
        .await?
        .unwrap_or_else(|| "{}".to_owned())
    } else {
        "{}".to_owned()
    };
    if !permission_set(&account_permission_ceiling).contains("approve_actions")
        || !permission_set(&profile_policy).contains("approve_actions")
    {
        return Err(ServiceError::invalid_operation(
            "approver identity lacks the server-issued approval permission",
        ));
    }

    let authorized = match action.scope_type.as_str() {
        "account" => approver_owner.as_deref() == Some(action.scope_id.as_str()),
        "project" => {
            let binding = sqlx::query(
                "SELECT permission_ceiling_json FROM project_agent_binding
                 WHERE project_id = ? AND identity_id = ? AND state = 'active'",
            )
            .bind(&action.scope_id)
            .bind(approver_identity_id)
            .fetch_optional(db.pool())
            .await?;
            binding.is_some_and(|row| {
                row.try_get::<String, _>("permission_ceiling_json")
                    .is_ok_and(|ceiling| permission_set(&ceiling).contains("approve_actions"))
            })
        }
        "agent_chat" => {
            let chat =
                sqlx::query("SELECT kind, account_id, project_id FROM agent_chat WHERE id = ?")
                    .bind(&action.scope_id)
                    .fetch_optional(db.pool())
                    .await?;
            let Some(chat) = chat else {
                return Err(ServiceError::not_found(
                    "agent_chat",
                    action.scope_id.clone(),
                ));
            };
            let kind: String = chat.try_get("kind")?;
            match kind.as_str() {
                "account_main" => {
                    approver_owner == chat.try_get::<Option<String>, _>("account_id")?
                }
                "project" => {
                    let project_id: Option<String> = chat.try_get("project_id")?;
                    if let Some(project_id) = project_id {
                        let permission_ceiling = sqlx::query_scalar::<_, String>(
                            "SELECT permission_ceiling_json FROM project_agent_binding
                             WHERE project_id = ? AND identity_id = ? AND state = 'active'",
                        )
                        .bind(project_id)
                        .bind(approver_identity_id)
                        .fetch_optional(db.pool())
                        .await?;
                        permission_ceiling.as_deref().is_some_and(|ceiling| {
                            permission_set(ceiling).contains("approve_actions")
                        })
                    } else {
                        false
                    }
                }
                _ => false,
            }
        }
        "task" => {
            sqlx::query_scalar::<_, i64>(
                "SELECT (
                    EXISTS (
                        SELECT 1 FROM task
                        WHERE id = ? AND assignee_type = 'agent' AND assignee_id = ?
                    )
                    OR EXISTS (
                        SELECT 1 FROM task_role_assignment
                        WHERE task_id = ? AND assignee_type = 'agent' AND assignee_id = ?
                    )
                )",
            )
            .bind(&action.scope_id)
            .bind(approver_identity_id)
            .bind(&action.scope_id)
            .bind(approver_identity_id)
            .fetch_one(db.pool())
            .await?
                != 0
        }
        "agent" => {
            approver_owner
                == sqlx::query_scalar::<_, Option<String>>(
                    "SELECT owner_id FROM agent_identity WHERE id = ?",
                )
                .bind(&action.scope_id)
                .fetch_optional(db.pool())
                .await?
                .flatten()
        }
        _ => false,
    };
    if !authorized {
        return Err(ServiceError::invalid_operation(
            "approver identity is not authorized for the action scope",
        ));
    }
    Ok(())
}

async fn evaluate_action_policy(
    db: &SqliteDb,
    actor_identity_id: &str,
    scope_type: &str,
    scope_id: &str,
    requested_permission: &str,
    operation: &str,
    payload_json: Option<&str>,
) -> Result<(AgentActionPolicyResult, Option<String>)> {
    if let Some(reason) = action_scope_access(
        db,
        actor_identity_id,
        scope_type,
        scope_id,
        requested_permission,
    )
    .await?
    {
        return Ok((AgentActionPolicyResult::Denied, Some(reason)));
    }
    let row = sqlx::query(
        "SELECT paused, archived_at, account_permission_ceiling, selected_profile_id
         FROM agent_identity WHERE id = ?",
    )
    .bind(actor_identity_id)
    .fetch_optional(db.pool())
    .await?
    .ok_or_else(|| ServiceError::not_found("agent identity", actor_identity_id))?;
    let paused: i64 = row.try_get("paused")?;
    let archived_at: Option<String> = row.try_get("archived_at")?;
    if paused != 0 || archived_at.is_some() {
        return Ok((
            AgentActionPolicyResult::Denied,
            Some("actor identity is paused or archived".to_owned()),
        ));
    }
    let account_policy: String = row.try_get("account_permission_ceiling")?;
    let profile_id: Option<String> = row.try_get("selected_profile_id")?;
    let profile_policy = if let Some(profile_id) = profile_id {
        sqlx::query_scalar::<_, String>(
            "SELECT tool_policy_json FROM agent_profile WHERE id = ? AND identity_id = ?",
        )
        .bind(profile_id)
        .bind(actor_identity_id)
        .fetch_optional(db.pool())
        .await?
        .unwrap_or_else(|| "{}".to_owned())
    } else {
        "{}".to_owned()
    };
    let scope_permissions = scope_permissions(db, scope_type, scope_id).await?;
    let account_permissions = permission_set(&account_policy);
    let profile_permissions = permission_set(&profile_policy);
    let authorized = account_permissions.contains(requested_permission)
        && profile_permissions.contains(requested_permission)
        && scope_permissions.contains(requested_permission);
    if !authorized {
        return Ok((
            AgentActionPolicyResult::Denied,
            Some(format!(
                "permission {requested_permission} is outside the server-issued identity/profile/scope ceiling"
            )),
        ));
    }
    if is_project_orchestration_mutation(operation) {
        let project_id = match scope_type {
            "project" => Some(scope_id.to_owned()),
            "agent_chat" => sqlx::query_scalar::<_, Option<String>>(
                "SELECT project_id FROM agent_chat
                 WHERE id = ? AND kind = 'project' LIMIT 1",
            )
            .bind(scope_id)
            .fetch_optional(db.pool())
            .await?
            .flatten(),
            _ => None,
        };
        let Some(project_id) = project_id else {
            return Ok((
                AgentActionPolicyResult::Denied,
                Some("Project orchestration requires a bound Project scope".to_owned()),
            ));
        };
        let state = sqlx::query(
            "SELECT charter_status, charter_setup_required,
                    current_charter_id, current_charter_revision_id
             FROM project WHERE id = ? LIMIT 1",
        )
        .bind(project_id)
        .fetch_optional(db.pool())
        .await?
        .ok_or_else(|| ServiceError::not_found("project", scope_id.to_owned()))?;
        let charter_status: String = state.try_get("charter_status")?;
        let setup_required: i64 = state.try_get("charter_setup_required")?;
        let has_charter = state
            .try_get::<Option<String>, _>("current_charter_id")?
            .is_some()
            && state
                .try_get::<Option<String>, _>("current_charter_revision_id")?
                .is_some();
        let is_setup = charter_status == "legacy_unverified" && setup_required != 0;
        let is_charter_amendment =
            charter_status == "charter_backed" && setup_required == 0 && has_charter;
        if operation == PROJECT_CHARTER_ADOPTION_OPERATION {
            if !is_setup && !is_charter_amendment {
                return Ok((
                    AgentActionPolicyResult::Denied,
                    Some(
                        "Project Charter adoption is not valid for the current Project state"
                            .to_owned(),
                    ),
                ));
            }
        } else if charter_status != "charter_backed" || setup_required != 0 || !has_charter {
            return Ok((
                AgentActionPolicyResult::Denied,
                Some("Project orchestration remains blocked until a user-approved Charter adoption is committed".to_owned()),
            ));
        }
    }
    // Classify after the scope/Charter gate above.  The payload is part of the
    // canonical operation identity: a reversible Project subaction can be a
    // direct command while an authority action in the same coarse descriptor
    // remains an approval-backed AgentAction.
    let payload_value =
        payload_json.and_then(|payload| serde_json::from_str::<Value>(payload).ok());
    let classification = classify_operation(operation, payload_value.as_ref());
    let project_draft_allowed =
        payload_json.is_some_and(|payload| is_allowed_project_draft_operation(operation, payload));
    let main_orchestration_allowed =
        is_allowed_main_orchestration_operation(requested_permission, operation);
    let direct_allowed = project_draft_allowed
        || main_orchestration_allowed
        || (operation == TASK_ADAPTIVE_OPERATION && requested_permission == "propose_task")
        || (operation == TASK_PROPOSE_OPERATION && requested_permission == "propose_task")
        || (operation == TASK_REVIEW_OPERATION && requested_permission == "propose_task");
    if matches!(classification, OperationClassification::Denied) {
        return Ok((
            AgentActionPolicyResult::Denied,
            Some("operation is denied by the canonical native operation catalog".to_owned()),
        ));
    }
    if matches!(classification, OperationClassification::Query) {
        return Ok((
            AgentActionPolicyResult::Denied,
            Some("query operations execute through the read boundary".to_owned()),
        ));
    }
    if matches!(classification, OperationClassification::DirectCommand) && !direct_allowed {
        return Ok((
            AgentActionPolicyResult::Denied,
            Some("direct command is not admitted for this permission or payload".to_owned()),
        ));
    }
    let approval_required = matches!(
        classification,
        OperationClassification::ApprovalRequiredAction
    ) || requested_permission == "task_write"
        || operation.starts_with("protected.");
    Ok(if approval_required {
        (
            AgentActionPolicyResult::ApprovalRequired,
            Some("protected mutation requires an independent approval".to_owned()),
        )
    } else {
        (AgentActionPolicyResult::Allowed, None)
    })
}

fn is_allowed_main_orchestration_operation(requested_permission: &str, operation: &str) -> bool {
    match operation {
        MAIN_CHARTER_DRAFT_OPERATION
        | MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION
        | MAIN_GENESIS_START_OPERATION => requested_permission == "propose_discovery",
        _ => false,
    }
}

fn is_allowed_project_draft_operation(operation: &str, payload_json: &str) -> bool {
    let Ok(payload) = serde_json::from_str::<Value>(payload_json) else {
        return false;
    };
    matches!(
        classify_operation(operation, Some(&payload)),
        OperationClassification::DirectCommand
    ) && is_allowed_project_direct_payload(operation, &payload)
}

fn is_project_orchestration_mutation(operation: &str) -> bool {
    is_catalog_project_orchestration_operation(operation)
}

fn is_typed_orchestration_operation(operation: &str) -> bool {
    operation_contract(operation).is_some()
}

/// The generic action executor is intentionally limited to ordinary
/// coordination envelopes.  Main/Project authority operations must go
/// through their typed domain executor (or the explicit user API path), so a
/// stale row or a hand-crafted AgentAction cannot turn a generic execution
/// request into project creation, handoff, lifecycle, or release authority.
fn is_non_executable_authority_operation(operation: &str) -> bool {
    matches!(
        operation,
        // Legacy generic authority names are listed explicitly so this
        // boundary is auditable and does not depend on a prefix convention.
        "project.lifecycle" | "handoff.publish" | "decision.request" | "web.search"
    )
}

fn permission_set(value: &str) -> BTreeSet<String> {
    let Ok(value) = serde_json::from_str::<Value>(value) else {
        return BTreeSet::new();
    };
    match value {
        Value::Array(values) => values
            .into_iter()
            .filter_map(|value| value.as_str().map(str::to_owned))
            .collect(),
        Value::Object(map) => map
            .get("permissions")
            .or_else(|| map.get("allowed"))
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|value| value.as_str().map(str::to_owned))
            .collect(),
        _ => BTreeSet::new(),
    }
}

async fn scope_permissions(
    db: &SqliteDb,
    scope_type: &str,
    scope_id: &str,
) -> Result<BTreeSet<String>> {
    let mut values: Vec<&str> = match scope_type {
        "account" => &[
            "read_account",
            "propose_discovery",
            "propose_project",
            "propose_handoff",
        ][..],
        "project" => &[
            "read_project",
            "read_memory",
            "propose_project",
            "propose_task",
            "propose_message",
            "propose_commitment",
            "propose_memory",
            "propose_review",
            "propose_decision",
            "propose_session",
        ][..],
        "agent_chat" => &["read_agent_chat", "read_memory"][..],
        "task" => &[
            "read_task",
            "read_memory",
            "task_read",
            "task_write",
            "propose_review",
        ][..],
        "agent" => &["read_account", "propose_message"][..],
        _ => &[][..],
    }
    .to_vec();
    if scope_type == "agent_chat"
        && sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agent_chat
             WHERE id = ? AND kind = 'project' AND project_id IS NOT NULL",
        )
        .bind(scope_id)
        .fetch_one(db.pool())
        .await?
            > 0
    {
        // The owning Project and active binding are checked separately by
        // action_scope_access.  Only a server-resolved Project Chat receives
        // this extra capability; Main Chat remains permanently denied.
        values.extend([
            "propose_message",
            "propose_project",
            "propose_task",
            "propose_commitment",
            "propose_memory",
            "propose_session",
        ]);
    } else if scope_type == "agent_chat"
        && sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agent_chat
             WHERE id = ? AND kind = 'account_main' AND account_id IS NOT NULL",
        )
        .bind(scope_id)
        .fetch_one(db.pool())
        .await?
            > 0
    {
        // Main Chat receives only global discovery/project organization and
        // explicit handoff proposals. It never receives Project Task tools.
        values.extend(["propose_discovery", "propose_project", "propose_handoff"]);
    }
    let project_id = match scope_type {
        "project" => Some(scope_id.to_owned()),
        "agent_chat" => sqlx::query_scalar::<_, Option<String>>(
            "SELECT project_id FROM agent_chat
             WHERE id = ? AND kind = 'project' LIMIT 1",
        )
        .bind(scope_id)
        .fetch_optional(db.pool())
        .await?
        .flatten(),
        _ => None,
    };
    if let Some(project_id) = project_id {
        let setup_required = sqlx::query_scalar::<_, i64>(
            "SELECT charter_setup_required FROM project WHERE id = ? LIMIT 1",
        )
        .bind(project_id)
        .fetch_optional(db.pool())
        .await?
        .is_some_and(|value| value != 0);
        if setup_required {
            values.retain(|permission| {
                matches!(
                    *permission,
                    "read_project"
                        | "read_agent_chat"
                        | "read_memory"
                        | "propose_message"
                        | "propose_project"
                )
            });
        }
    }
    Ok(values.into_iter().map(str::to_owned).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use db::{
        create_sqlite_pool, run_migrations, AgentRepo, AgentStatus, CreateAgentIdentity,
        CreateAgentProfile, CreateProject, CreateProjectAgentBinding, ProjectAgentBindingRepo,
        ProjectRepo, ReplaceProjectAgentBinding,
    };
    use forge_agent_host::{
        MAIN_PROJECT_CREATE_OPERATION, MIGRATED_OPERATION_CONTRACTS, PROJECT_DECISION_OPERATION,
        PROJECT_DOCUMENT_OPERATION, PROJECT_EVIDENCE_OPERATION,
        PROJECT_EXECUTION_BASELINE_OPERATION, PROJECT_MILESTONE_OPERATION,
        PROJECT_READINESS_OPERATION,
    };

    #[test]
    fn generic_executor_authority_names_are_not_executable() {
        for operation in [
            "project.lifecycle",
            "handoff.publish",
            "decision.request",
            "web.search",
        ] {
            assert!(
                is_non_executable_authority_operation(operation),
                "generic executor must reject authority operation {operation}"
            );
        }
        assert!(!is_non_executable_authority_operation("message.send"));
    }

    #[test]
    fn generic_executor_rejects_every_typed_orchestration_operation() {
        for contract in MIGRATED_OPERATION_CONTRACTS {
            let operation = contract.operation;
            assert!(
                is_typed_orchestration_operation(operation),
                "generic executor must route typed operation {operation} to its domain executor"
            );
        }
        assert!(!is_typed_orchestration_operation("message.send"));
    }

    #[test]
    fn main_genesis_start_is_admitted_as_a_direct_discovery_command() {
        assert!(is_allowed_main_orchestration_operation(
            "propose_discovery",
            MAIN_GENESIS_START_OPERATION,
        ));
        assert!(!is_allowed_main_orchestration_operation(
            "propose_project",
            MAIN_GENESIS_START_OPERATION,
        ));
    }

    #[test]
    fn main_project_agent_selection_is_admitted_as_a_direct_discovery_command() {
        assert!(is_allowed_main_orchestration_operation(
            "propose_discovery",
            MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION,
        ));
        assert!(!is_allowed_main_orchestration_operation(
            "propose_project",
            MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION,
        ));
    }

    #[test]
    fn task_proposal_payload_rejects_unknown_fields() {
        let payload = serde_json::json!({
            "title": "Bounded task",
            "unexpected_field": true,
        });
        assert!(serde_json::from_value::<TaskProposalPayload>(payload).is_err());
    }

    #[test]
    fn nested_artifact_scope_is_not_a_project_authority_override() {
        assert!(!forge_agent_host::contains_authority_override(
            &serde_json::json!({
                "action": "draft_revision",
                "content": {"scope": {"included": ["checkout"]}},
            })
        ));
        assert!(forge_agent_host::contains_authority_override(
            &serde_json::json!({
                "project_id": "forged-project",
                "content": {},
            })
        ));
    }

    #[tokio::test]
    async fn invalid_task_payload_is_rejected_before_action_admission() {
        let db = db().await;
        let result = AgentActionService::new(Arc::clone(&db))
            .propose(ProposeActionInput {
                id: None,
                actor_identity_id: "agent-a".to_owned(),
                scope_type: "project".to_owned(),
                scope_id: "project-1".to_owned(),
                operation: TASK_PROPOSE_OPERATION.to_owned(),
                payload_json: serde_json::json!({
                    "title": "Bounded task",
                    "project_id": "caller-authored-project",
                })
                .to_string(),
                dedupe_key: "invalid-task-payload".to_owned(),
                correlation_id: "corr-invalid-task-payload".to_owned(),
                causation_id: None,
                causation_depth: 0,
                requested_permission: "propose_task".to_owned(),
                policy_reason: None,
                target_type: None,
                target_id: None,
            })
            .await;
        assert!(result.is_err());
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM agent_action WHERE dedupe_key = 'invalid-task-payload'",
        )
        .fetch_one(db.pool())
        .await
        .expect("action count");
        assert_eq!(count, 0);
    }

    async fn db() -> Arc<SqliteDb> {
        let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
        run_migrations(&pool).await.expect("migrations");
        let db = Arc::new(SqliteDb::new(pool));
        for id in ["agent-a", "agent-b"] {
            let now = now_rfc3339();
            AgentRepo::create_identity_with_profile(
                &*db,
                CreateAgentIdentity {
                    id: id.to_owned(),
                    name: id.to_owned(),
                    description: None,
                    max_concurrent_tasks: 1,
                    heartbeat_interval_seconds: 30,
                    max_missed_heartbeats: 3,
                    status: AgentStatus::Idle,
                    last_heartbeat_at: None,
                    is_default: false,
                    paused: false,
                    owner_id: Some("user-1".to_owned()),
                    visibility: "account".to_owned(),
                    account_permission_ceiling:
                        r#"{"permissions":["read_account","propose_discovery","propose_project","propose_task","propose_message","propose_review","task_write","approve_actions"]}"#
                            .to_owned(),
                    created_at: now.clone(),
                    updated_at: now.clone(),
                },
                CreateAgentProfile {
                    id: new_uuid_v4(),
                    identity_id: id.to_owned(),
                    backend_kind: "native".to_owned(),
                    executor_type: "native".to_owned(),
                    provider: None,
                    model: None,
                    reasoning_effort: None,
                    permission_policy: None,
                    prompt_template: None,
                    capabilities_json: "{}".to_owned(),
                    tool_policy_json: r#"{"permissions":["read_account","propose_discovery","propose_project","propose_task","propose_message","propose_review","task_write","approve_actions"]}"#
                        .to_owned(),
                    config_json: "{}".to_owned(),
                    credential_ref: None,
                    daemon_id: None,
                    created_at: now.clone(),
                    updated_at: now,
                },
            )
            .await
            .expect("agent");
        }
        let now = now_rfc3339();
        ProjectRepo::create(
            &*db,
            CreateProject {
                id: "project-1".to_owned(),
                name: "Project 1".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: Some("user-1".to_owned()),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("project");
        // Project Agent authority is singular: the binding is the only
        // durable source of project scope and permission.
        let agent = AgentRepo::get_by_id(&*db, "agent-a")
            .await
            .expect("agent lookup")
            .expect("agent-a");
        let setup = ProjectAgentBindingRepo::get_active_project_binding(&*db, "project-1")
            .await
            .expect("binding lookup")
            .expect("setup binding");
        ProjectAgentBindingRepo::replace_project_binding(
            &*db,
            ReplaceProjectAgentBinding {
                project_id: "project-1".to_owned(),
                expected_version: setup.version,
                replacement: CreateProjectAgentBinding {
                    id: new_uuid_v4(),
                    project_id: "project-1".to_owned(),
                    identity_id: Some(agent.id),
                    profile_id: Some(agent.profile_id),
                    state: "active".to_owned(),
                    autonomy_policy_json: "{}".to_owned(),
                    permission_ceiling_json: r#"{"permissions":["propose_task"]}"#.to_owned(),
                    subscriptions_json: "[]".to_owned(),
                    wake_budget: 10,
                    created_at: now_rfc3339(),
                    updated_at: now_rfc3339(),
                },
                replacement_reason: Some("coordination policy fixture".to_owned()),
            },
        )
        .await
        .expect("project binding");
        db
    }

    #[test]
    fn project_agent_policy_admits_only_bounded_subactions() {
        let allowed = [
            (PROJECT_CHARTER_ADOPTION_OPERATION, "draft_revision"),
            (PROJECT_DOCUMENT_OPERATION, "draft_revision"),
            (PROJECT_DOCUMENT_OPERATION, "propose_approval"),
            (PROJECT_DOCUMENT_OPERATION, "approve"),
            (PROJECT_DECISION_OPERATION, "record_candidate"),
            (PROJECT_DECISION_OPERATION, "record_effective"),
            (PROJECT_EXECUTION_BASELINE_OPERATION, "draft_revision"),
            (PROJECT_EXECUTION_BASELINE_OPERATION, "revise"),
            (PROJECT_EXECUTION_BASELINE_OPERATION, "propose_approval"),
            (PROJECT_MILESTONE_OPERATION, "define"),
            (PROJECT_MILESTONE_OPERATION, "revise"),
            (PROJECT_MILESTONE_OPERATION, "set_primary"),
            (PROJECT_EVIDENCE_OPERATION, "attach"),
            (PROJECT_READINESS_OPERATION, "evaluate"),
        ];
        for (operation, action) in allowed {
            let mut payload = serde_json::json!({"action": action});
            if operation == PROJECT_DECISION_OPERATION {
                payload["decision_class"] = serde_json::json!("project_implementation");
            }
            assert!(
                is_allowed_project_draft_operation(operation, &payload.to_string()),
                "expected bounded action {operation}/{action} to be admitted"
            );
        }

        for (operation, action) in [
            (PROJECT_CHARTER_ADOPTION_OPERATION, "approve"),
            (PROJECT_CHARTER_ADOPTION_OPERATION, "attach"),
            (PROJECT_EXECUTION_BASELINE_OPERATION, "approve"),
            (PROJECT_EXECUTION_BASELINE_OPERATION, "activate"),
            (PROJECT_EVIDENCE_OPERATION, "attest"),
            (PROJECT_RELEASE_OPERATION, "release"),
            (PROJECT_RELEASE_OPERATION, "approve"),
            (PROJECT_DECISION_OPERATION, "supersede"),
            (PROJECT_DECISION_OPERATION, "invalidate"),
            // A release-candidate request is intentionally approval-backed:
            // it enters the Action queue and must not be treated as a direct
            // receipt-backed Project coordination write.
            (PROJECT_RELEASE_OPERATION, "propose_candidate"),
        ] {
            let payload = serde_json::json!({"action": action});
            assert!(
                !is_allowed_project_draft_operation(operation, &payload.to_string()),
                "authority action {operation}/{action} must not be admitted"
            );
        }

        assert_eq!(
            classify_operation(
                PROJECT_RELEASE_OPERATION,
                Some(&serde_json::json!({"action": "propose_candidate"}))
            ),
            OperationClassification::ApprovalRequiredAction,
            "release-candidate requests retain their audited approval boundary"
        );

        for decision_class in ["user_scope", "policy", "waiver", "manual_approval"] {
            let payload = serde_json::json!({
                "action": "record_effective",
                "decision_class": decision_class,
            });
            assert!(
                !is_allowed_project_draft_operation(
                    PROJECT_DECISION_OPERATION,
                    &payload.to_string()
                ),
                "decision class {decision_class} must remain user/system-only"
            );
        }

        let cross_scope_override = serde_json::json!({
            "action": "record_candidate",
            "decision_class": "project_implementation",
            "project_id": "another-project",
            "scope_id": "another-project",
            "actor_identity_id": "another-agent",
        });
        // Scope and identity are server-derived and are never made valid by
        // caller-supplied fields. The native validator rejects these fields;
        // the policy helper must not treat them as authority either.
        assert!(!is_allowed_project_draft_operation(
            PROJECT_DECISION_OPERATION,
            &cross_scope_override.to_string()
        ));
    }

    #[tokio::test]
    async fn main_orchestration_project_create_is_user_only_and_draft_is_direct() {
        let db = db().await;
        let actions = AgentActionService::new(Arc::clone(&db));
        let draft = actions
            .propose(ProposeActionInput {
                id: None,
                actor_identity_id: "agent-a".to_owned(),
                scope_type: "account".to_owned(),
                scope_id: "user-1".to_owned(),
                operation: MAIN_CHARTER_DRAFT_OPERATION.to_owned(),
                payload_json: r#"{"action":"save_revision"}"#.to_owned(),
                dedupe_key: "main-draft-1".to_owned(),
                correlation_id: "main-draft-correlation".to_owned(),
                causation_id: None,
                causation_depth: 0,
                requested_permission: "propose_discovery".to_owned(),
                policy_reason: None,
                target_type: Some("account".to_owned()),
                target_id: Some("user-1".to_owned()),
            })
            .await;
        assert!(
            draft.is_err(),
            "Main Charter drafts do not enter AgentAction"
        );
        let draft_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM agent_action WHERE operation = ?")
                .bind(MAIN_CHARTER_DRAFT_OPERATION)
                .fetch_one(db.pool())
                .await
                .expect("draft action count");
        assert_eq!(draft_count, 0);

        let create = actions
            .propose(ProposeActionInput {
                id: None,
                actor_identity_id: "agent-a".to_owned(),
                scope_type: "account".to_owned(),
                scope_id: "user-1".to_owned(),
                operation: MAIN_PROJECT_CREATE_OPERATION.to_owned(),
                payload_json: r#"{"action":"create_from_approval","approval_id":"approval-1"}"#
                    .to_owned(),
                dedupe_key: "main-create-1".to_owned(),
                correlation_id: "main-create-correlation".to_owned(),
                causation_id: None,
                causation_depth: 0,
                requested_permission: "propose_project".to_owned(),
                policy_reason: None,
                target_type: Some("account".to_owned()),
                target_id: Some("user-1".to_owned()),
            })
            .await
            .expect("Main Project create proposal");
        assert_eq!(
            create.policy_result,
            AgentActionPolicyResult::ApprovalRequired
        );
        assert_eq!(create.status, AgentActionStatus::PendingApproval);
        assert!(actions
            .approve(ApproveActionInput {
                action_id: create.id.clone(),
                expected_version: create.version,
                approver_identity_id: "agent-a".to_owned(),
                decision: AgentActionApprovalDecision::Approved,
                reason: None,
            })
            .await
            .is_err());

        let typed_create = crate::MainOrchestrationActionService::new(Arc::clone(&db))
            .execute(crate::ExecuteMainOrchestrationActionInput {
                action_id: create.id,
                expected_version: create.version,
                executed_by_type: "agent".to_owned(),
                executed_by_id: "agent-a".to_owned(),
                idempotency_key: "main-create-execution".to_owned(),
            })
            .await;
        assert!(
            typed_create.is_err(),
            "Project creation must remain user-only"
        );
    }

    #[tokio::test]
    async fn completion_requires_authorized_evidence_and_is_idempotent() {
        let db = db().await;
        let commitments = CommitmentService::new(Arc::clone(&db));
        let commitment = commitments
            .create(CreateCommitmentInput {
                id: None,
                owner_identity_id: "agent-a".to_owned(),
                scope_type: "account".to_owned(),
                scope_id: "user-1".to_owned(),
                title: "Deliver result".to_owned(),
                description: None,
                status: AgentCommitmentStatus::Open,
                due_at: None,
                correlation_id: "corr-1".to_owned(),
                originating_action_id: None,
                originating_task_id: None,
                evidence_required: true,
            })
            .await
            .expect("commitment");
        let no_evidence = commitments
            .complete(CompleteCommitmentInput {
                id: commitment.id.clone(),
                expected_version: commitment.version,
                evidence: CommitmentEvidenceInput {
                    id: None,
                    commitment_id: commitment.id.clone(),
                    evidence_type: String::new(),
                    evidence_id: String::new(),
                    scope_type: "account".to_owned(),
                    scope_id: "user-1".to_owned(),
                    description: None,
                    metadata_json: "{}".to_owned(),
                    authorized_by_type: "agent".to_owned(),
                    authorized_by_id: "agent-a".to_owned(),
                    dedupe_key: "complete-1".to_owned(),
                },
                actor_type: "agent".to_owned(),
                actor_id: "agent-a".to_owned(),
                reason: None,
                dedupe_key: "complete-1".to_owned(),
            })
            .await;
        assert!(no_evidence.is_err());
        let completed = commitments
            .complete(CompleteCommitmentInput {
                id: commitment.id.clone(),
                expected_version: commitment.version,
                evidence: CommitmentEvidenceInput {
                    id: None,
                    commitment_id: commitment.id.clone(),
                    evidence_type: "task_delivery".to_owned(),
                    evidence_id: "task-1".to_owned(),
                    scope_type: "account".to_owned(),
                    scope_id: "user-1".to_owned(),
                    description: Some("accepted delivery".to_owned()),
                    metadata_json: "{}".to_owned(),
                    authorized_by_type: "agent".to_owned(),
                    authorized_by_id: "agent-a".to_owned(),
                    dedupe_key: "complete-1".to_owned(),
                },
                actor_type: "agent".to_owned(),
                actor_id: "agent-a".to_owned(),
                reason: None,
                dedupe_key: "complete-1".to_owned(),
            })
            .await
            .expect("completion");
        assert_eq!(completed.status, AgentCommitmentStatus::Completed);
        let replay = commitments
            .complete(CompleteCommitmentInput {
                id: commitment.id,
                expected_version: completed.version,
                evidence: CommitmentEvidenceInput {
                    id: None,
                    commitment_id: completed.id.clone(),
                    evidence_type: "task_delivery".to_owned(),
                    evidence_id: "task-1".to_owned(),
                    scope_type: "account".to_owned(),
                    scope_id: "user-1".to_owned(),
                    description: None,
                    metadata_json: "{}".to_owned(),
                    authorized_by_type: "agent".to_owned(),
                    authorized_by_id: "agent-a".to_owned(),
                    dedupe_key: "complete-1".to_owned(),
                },
                actor_type: "agent".to_owned(),
                actor_id: "agent-a".to_owned(),
                reason: None,
                dedupe_key: "complete-1".to_owned(),
            })
            .await
            .expect("replay");
        assert_eq!(replay.version, completed.version);
    }

    #[tokio::test]
    async fn self_approval_is_denied_and_approved_actions_execute_once() {
        let db = db().await;
        let actions = AgentActionService::new(Arc::clone(&db));
        let allowed = actions
            .propose(ProposeActionInput {
                id: None,
                actor_identity_id: "agent-a".to_owned(),
                scope_type: "agent".to_owned(),
                scope_id: "agent-a".to_owned(),
                operation: "message.propose".to_owned(),
                payload_json: "{}".to_owned(),
                dedupe_key: "allowed-1".to_owned(),
                correlation_id: "corr-allowed".to_owned(),
                causation_id: None,
                causation_depth: 0,
                requested_permission: "propose_message".to_owned(),
                policy_reason: None,
                target_type: None,
                target_id: None,
            })
            .await
            .expect("allowed proposal");
        assert_eq!(
            allowed.policy_result,
            AgentActionPolicyResult::ApprovalRequired
        );
        let denied = actions
            .propose(ProposeActionInput {
                id: None,
                actor_identity_id: "agent-a".to_owned(),
                scope_type: "project".to_owned(),
                scope_id: "project-1".to_owned(),
                operation: "project.lifecycle".to_owned(),
                payload_json: "{}".to_owned(),
                dedupe_key: "denied-1".to_owned(),
                correlation_id: "corr-denied".to_owned(),
                causation_id: None,
                causation_depth: 0,
                requested_permission: "read_agent_chat".to_owned(),
                policy_reason: None,
                target_type: None,
                target_id: None,
            })
            .await;
        assert!(matches!(
            denied,
            Err(ServiceError::AuthorizationDenied { .. })
        ));
        // Reuse the admitted identity-scoped proposal. Newly-created Projects
        // are Charter-setup-required and must not admit Task proposals; that
        // boundary is covered separately by Product Genesis acceptance.
        let action = allowed;
        let self_approval = actions
            .approve(ApproveActionInput {
                action_id: action.id.clone(),
                expected_version: action.version,
                approver_identity_id: "agent-a".to_owned(),
                decision: AgentActionApprovalDecision::Approved,
                reason: None,
            })
            .await;
        assert!(self_approval.is_err());
        assert_eq!(
            action.policy_result,
            AgentActionPolicyResult::ApprovalRequired
        );
        assert_eq!(action.status, AgentActionStatus::PendingApproval);
        actions
            .approve(ApproveActionInput {
                action_id: action.id.clone(),
                expected_version: action.version,
                approver_identity_id: "agent-b".to_owned(),
                decision: AgentActionApprovalDecision::Approved,
                reason: None,
            })
            .await
            .expect("independent approval");
        let current = actions.get(&action.id).await.expect("current action");
        let first = actions
            .execute(ExecuteActionInput {
                action_id: action.id.clone(),
                expected_version: current.version,
                attempt: 1,
                result_json: Some(r#"{"task_id":"task-1"}"#.to_owned()),
                error: None,
                executed_by_type: "forge".to_owned(),
                executed_by_id: "forge".to_owned(),
                idempotency_key: "execute-1".to_owned(),
            })
            .await
            .expect("execution");
        let replay = actions
            .execute(ExecuteActionInput {
                action_id: action.id,
                expected_version: current.version,
                attempt: 2,
                result_json: Some(r#"{"task_id":"different"}"#.to_owned()),
                error: None,
                executed_by_type: "forge".to_owned(),
                executed_by_id: "forge".to_owned(),
                idempotency_key: "execute-1".to_owned(),
            })
            .await
            .expect("replay execution");
        assert_eq!(first.id, replay.id);
    }

    #[tokio::test]
    async fn policy_denies_cross_project_and_membership_role_escalation() {
        let db = db().await;
        let now = now_rfc3339();
        ProjectRepo::create(
            &*db,
            CreateProject {
                id: "project-2".to_owned(),
                name: "Project 2".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: Some("user-1".to_owned()),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("project");
        let actions = AgentActionService::new(Arc::clone(&db));
        let (policy_result, policy_reason) = actions
            .evaluate_direct_command_policy(
                "agent-a",
                "project",
                "project-2",
                "propose_task",
                TASK_PROPOSE_OPERATION,
                Some(r#"{"title":"outside role"}"#),
            )
            .await
            .expect("direct-command policy evaluation");
        assert_eq!(policy_result, AgentActionPolicyResult::Denied);
        assert!(policy_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("active binding")));
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM agent_action WHERE dedupe_key = 'role-escalation-1'",
            )
            .fetch_one(db.pool())
            .await
            .expect("direct-command action count"),
            0
        );

        ProjectRepo::create(
            &*db,
            CreateProject {
                id: "project-3".to_owned(),
                name: "Project 3".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: Some("user-1".to_owned()),
                created_at: now_rfc3339(),
                updated_at: now_rfc3339(),
            },
        )
        .await
        .expect("project");
        let project_3_agent = AgentRepo::get_by_id(&*db, "agent-a")
            .await
            .expect("agent lookup")
            .expect("agent-a");
        let project_3_setup =
            ProjectAgentBindingRepo::get_active_project_binding(&*db, "project-3")
                .await
                .expect("binding lookup")
                .expect("setup binding");
        ProjectAgentBindingRepo::replace_project_binding(
            &*db,
            ReplaceProjectAgentBinding {
                project_id: "project-3".to_owned(),
                expected_version: project_3_setup.version,
                replacement: CreateProjectAgentBinding {
                    id: new_uuid_v4(),
                    project_id: "project-3".to_owned(),
                    identity_id: Some(project_3_agent.id),
                    profile_id: Some(project_3_agent.profile_id),
                    state: "active".to_owned(),
                    autonomy_policy_json: "{}".to_owned(),
                    permission_ceiling_json: r#"{"permissions":["propose_message"]}"#.to_owned(),
                    subscriptions_json: "[]".to_owned(),
                    wake_budget: 10,
                    created_at: now_rfc3339(),
                    updated_at: now_rfc3339(),
                },
                replacement_reason: Some("coordination policy fixture".to_owned()),
            },
        )
        .await
        .expect("project binding");
        let protected = actions
            .propose(ProposeActionInput {
                id: None,
                actor_identity_id: "agent-a".to_owned(),
                scope_type: "project".to_owned(),
                scope_id: "project-3".to_owned(),
                operation: "message.propose".to_owned(),
                payload_json: r#"{"message":"needs approval"}"#.to_owned(),
                dedupe_key: "cross-project-approval-1".to_owned(),
                correlation_id: "corr-cross-project-approval".to_owned(),
                causation_id: None,
                causation_depth: 0,
                requested_permission: "propose_message".to_owned(),
                policy_reason: None,
                target_type: Some("project".to_owned()),
                target_id: Some("project-3".to_owned()),
            })
            .await
            .expect("protected proposal");
        let unrelated = actions
            .approve(ApproveActionInput {
                action_id: protected.id,
                expected_version: protected.version,
                approver_identity_id: "agent-b".to_owned(),
                decision: AgentActionApprovalDecision::Approved,
                reason: None,
            })
            .await;
        assert!(unrelated.is_err());
    }
}
