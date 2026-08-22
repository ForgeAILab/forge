use serde::{Deserialize, Serialize};
use serde_json::Value;
use ts_rs::TS;

use crate::{InitialRoleAssignment, TaskGovernanceRequest, TaskResponse, TaskType};

/// Durable commitment representation.  The owner is an identity, while the
/// actor performing a lifecycle operation is authenticated by the API and is
/// intentionally absent from mutation request bodies.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct CommitmentResponse {
    pub id: String,
    pub owner_identity_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub title: String,
    pub description: Option<String>,
    pub status: String,
    pub due_at: Option<String>,
    pub correlation_id: String,
    pub originating_action_id: Option<String>,
    pub originating_task_id: Option<String>,
    pub evidence_required: bool,
    pub cancellation_reason: Option<String>,
    pub blocked_reason: Option<String>,
    pub completed_at: Option<String>,
    pub cancelled_at: Option<String>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct CreateCommitmentRequest {
    pub scope_type: String,
    pub scope_id: String,
    pub title: String,
    pub description: Option<String>,
    pub status: Option<String>,
    pub due_at: Option<String>,
    pub correlation_id: String,
    pub originating_action_id: Option<String>,
    pub originating_task_id: Option<String>,
    pub evidence_required: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct UpdateCommitmentRequest {
    pub expected_version: i64,
    pub status: Option<String>,
    #[serde(default)]
    pub due_at: Option<Option<String>>,
    #[serde(default)]
    pub description: Option<Option<String>>,
    #[serde(default)]
    pub blocked_reason: Option<Option<String>>,
    #[serde(default)]
    pub cancellation_reason: Option<Option<String>>,
    pub reason: Option<String>,
    pub evidence_id: Option<String>,
    pub dedupe_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct CompleteCommitmentRequest {
    pub expected_version: i64,
    pub evidence_type: String,
    pub evidence_id: String,
    pub description: Option<String>,
    #[ts(type = "Record<string, unknown>")]
    pub metadata: Value,
    pub reason: Option<String>,
    pub dedupe_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct TransferCommitmentRequest {
    pub expected_version: i64,
    pub to_identity_id: String,
    pub reason: String,
    pub dedupe_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct CommitmentEvidenceResponse {
    pub id: String,
    pub commitment_id: String,
    pub evidence_type: String,
    pub evidence_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub description: Option<String>,
    #[ts(type = "Record<string, unknown>")]
    pub metadata: Value,
    pub authorized_by_type: String,
    pub authorized_by_id: String,
    pub dedupe_key: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, TS)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct CoordinationListQuery {
    pub status: Option<String>,
    pub scope_type: Option<String>,
    pub scope_id: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct InboxItemResponse {
    pub id: String,
    pub recipient_identity_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub kind: String,
    pub status: String,
    pub title: String,
    pub body: String,
    #[ts(type = "Record<string, unknown>")]
    pub payload: Value,
    pub source_type: Option<String>,
    pub source_id: Option<String>,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub dedupe_key: String,
    pub read_at: Option<String>,
    pub acknowledged_at: Option<String>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct UpdateInboxItemRequest {
    pub expected_version: i64,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct AskQuestionRequest {
    pub scope_type: String,
    pub scope_id: String,
    pub question: String,
    #[ts(type = "Record<string, unknown>")]
    pub context: Value,
    pub due_at: Option<String>,
    pub correlation_id: String,
    pub inbox_title: Option<String>,
    pub inbox_dedupe_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct QuestionResponse {
    pub id: String,
    pub recipient_identity_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub status: String,
    pub question: String,
    #[ts(type = "Record<string, unknown>")]
    pub context: Value,
    pub answer: Option<String>,
    pub asked_by_type: String,
    pub asked_by_id: String,
    pub answered_by_type: Option<String>,
    pub answered_by_id: Option<String>,
    pub inbox_item_id: Option<String>,
    pub due_at: Option<String>,
    pub correlation_id: String,
    pub version: i64,
    pub answered_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct AnswerQuestionRequest {
    pub expected_version: i64,
    pub answer: String,
}

/// Action output intentionally includes the server policy result and payload
/// hash, but not the persisted payload body.  This keeps public audit reads
/// useful without turning a generic action API into a secret-bearing log.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct AgentActionResponse {
    pub id: String,
    pub actor_identity_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub operation: String,
    pub payload_hash: String,
    pub dedupe_key: String,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub causation_depth: i64,
    pub requested_permission: String,
    pub policy_result: String,
    pub policy_reason: Option<String>,
    pub status: String,
    pub target_type: Option<String>,
    pub target_id: Option<String>,
    #[ts(type = "Record<string, unknown> | null")]
    pub outcome: Option<Value>,
    pub materialized: bool,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ProposeActionRequest {
    pub scope_type: String,
    pub scope_id: String,
    pub operation: String,
    #[ts(type = "Record<string, unknown>")]
    pub payload: Value,
    pub dedupe_key: String,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub causation_depth: Option<i64>,
    pub target_type: Option<String>,
    pub target_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct TaskProposalRequest {
    pub project_id: String,
    pub title: String,
    pub description: Option<String>,
    pub parent_task_id: Option<String>,
    pub priority: Option<i64>,
    pub task_type: Option<TaskType>,
    pub task_state_config: Option<String>,
    #[ts(type = "Record<string, unknown> | null")]
    pub merge_config: Option<Value>,
    pub role_assignments: Option<Vec<InitialRoleAssignment>>,
    #[serde(default)]
    pub governance: Option<TaskGovernanceRequest>,
    pub dedupe_key: String,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub causation_depth: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ApproveActionRequest {
    pub expected_version: i64,
    /// The API verifies this identity is owned by the authenticated account
    /// before binding it as the approval actor.
    pub approver_identity_id: String,
    pub decision: String,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ExecuteActionRequest {
    pub expected_version: i64,
    pub attempt: Option<i64>,
    #[ts(type = "Record<string, unknown> | null")]
    pub result: Option<Value>,
    pub error: Option<String>,
    pub idempotency_key: String,
}

/// Executes a Main Agent orchestration proposal through its typed domain
/// materializer. Generic action execution deliberately does not accept these
/// operations because an arbitrary result would not prove that the Charter
/// or Project domain mutation actually occurred.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ExecuteOrchestrationActionRequest {
    pub expected_version: i64,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ActionExecutionResponse {
    pub id: String,
    pub action_id: String,
    pub attempt: i64,
    pub status: String,
    #[ts(type = "Record<string, unknown> | null")]
    pub result: Option<Value>,
    pub error: Option<String>,
    pub executed_by_type: String,
    pub executed_by_id: String,
    pub idempotency_key: String,
    pub created_at: String,
    pub completed_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct TaskProposalCommandResponse {
    pub operation: String,
    pub status: String,
    pub materialized: bool,
    pub domain_committed: bool,
    pub receipt_id: String,
    pub event_id: String,
    pub input_digest: String,
    pub policy_result: String,
    pub correlation_id: String,
    pub replayed: bool,
    pub requires_user_authorization: bool,
    pub task: TaskResponse,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::de::DeserializeOwned;
    use serde_json::json;

    fn assert_rejects_unknown_field<T>(mut request: serde_json::Value)
    where
        T: DeserializeOwned,
    {
        request
            .as_object_mut()
            .expect("request envelope object")
            .insert("unexpected_field".to_owned(), json!(true));
        assert!(
            serde_json::from_value::<T>(request).is_err(),
            "request envelope silently accepted an unknown field"
        );
    }

    #[test]
    fn typed_coordination_envelopes_reject_unknown_fields() {
        assert_rejects_unknown_field::<ExecuteOrchestrationActionRequest>(json!({
            "expected_version": 1,
            "idempotency_key": "execute-1"
        }));
        assert_rejects_unknown_field::<TaskProposalRequest>(json!({
            "project_id": "project-1",
            "title": "Bounded task",
            "dedupe_key": "task-1",
            "correlation_id": "correlation-1"
        }));
        assert_rejects_unknown_field::<ProposeActionRequest>(json!({
            "scope_type": "account",
            "scope_id": "account-1",
            "operation": "charter.draft",
            "payload": {},
            "dedupe_key": "action-1",
            "correlation_id": "correlation-1"
        }));
        assert_rejects_unknown_field::<CoordinationListQuery>(json!({}));
    }

    #[test]
    fn task_proposal_rejects_unknown_task_type() {
        let request = json!({
            "project_id": "project-1",
            "title": "Invalid task type",
            "description": null,
            "parent_task_id": null,
            "priority": null,
            "task_type": "feature",
            "task_state_config": null,
            "merge_config": null,
            "role_assignments": null,
            "dedupe_key": "invalid-type",
            "correlation_id": "correlation-1",
            "causation_id": null,
            "causation_depth": null
        });

        assert!(serde_json::from_value::<TaskProposalRequest>(request).is_err());
    }
}
