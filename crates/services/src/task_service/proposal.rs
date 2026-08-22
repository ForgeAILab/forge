//! Atomic `task.propose` command boundary.
//!
//! Agent actions remain the approval/audit envelope when approval is required,
//! while automatically allowed native proposals use the same Task command
//! directly. The Task, immutable governance projection, durable event,
//! command receipt, and optional action execution are one SQLite transaction.
//! The receipt is resolved before current binding/baseline authorization so a
//! lost response can be replayed exactly after mutable Project state changes.

use super::*;
use crate::command_boundary::{
    AgentActionProvenance, AuthorizationProvenance, CommandContext, CommandPrincipal, CommandScope,
    CommandScopeType, ExpectedCommandState, NewCommandContext,
};
use crate::execution_setup::classify_task_execution;
use db::{
    AgentAction, AgentActionExecutionStatus, AgentActionPolicyResult, AgentActionStatus,
    CommandReceipt, CommandReceiptRepo, CreateAgentActionExecution, CreateCommandReceipt,
    CreateProjectTaskGovernance, CreateTask, CreateTaskProposalCommand, CreateTaskRoleAssignment,
    ProjectOrchestrationRepo, ProjectRepo, Task, TaskRepo,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::Row;
use std::str::FromStr;

pub const TASK_PROPOSE_COMMAND: &str = "task.propose";

/// Closed action payload shared by REST/native proposal execution.  The
/// transport envelope stays in `api-types`; this payload is the server-owned
/// command input used for canonical digesting and replay.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskProposalPayload {
    pub title: String,
    pub description: Option<String>,
    pub parent_task_id: Option<String>,
    pub priority: Option<i64>,
    pub task_type: Option<String>,
    pub task_state_config: Option<String>,
    pub merge_config: Option<Value>,
    pub role_assignments: Option<Vec<api_types::InitialRoleAssignment>>,
    #[serde(default)]
    pub governance: Option<api_types::TaskGovernanceRequest>,
    #[serde(default)]
    pub plan_item_id: Option<String>,
    #[serde(default)]
    pub milestone_id: Option<String>,
    #[serde(default)]
    pub capability_class: Option<String>,
    #[serde(default)]
    pub risk_class: Option<String>,
}

/// Transport-neutral input for an automatically allowed Project Agent
/// `task.propose`.  Native tools provide the authenticated identity and the
/// host-derived source scope; the service derives the Project command scope
/// from `project_id` and binds all of the supplied policy/provenance fields
/// into the receipt digest.  This input deliberately has no AgentAction
/// identifier: direct commands are audited by their command receipt.
#[derive(Debug, Clone, Serialize)]
pub struct DirectTaskProposalInput {
    pub actor_identity_id: String,
    /// The principal that actually executes the command.  This is separate
    /// from `actor_identity_id`: a REST request is authorized by the selected
    /// Project Agent but its durable command receipt is owned by the
    /// authenticated user who submitted it.
    pub executor_type: String,
    pub executor_id: String,
    /// Canonical host-derived source scope (usually `agent_chat` for a
    /// Project Agent turn or `project` for a Project-scoped tool call).
    pub source_scope_type: String,
    pub source_scope_id: String,
    pub project_id: String,
    pub payload: TaskProposalPayload,
    pub idempotency_key: String,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub causation_depth: i64,
    /// The canonical policy result is part of the command identity.  An
    /// adapter may additionally provide a fresh preflight result; the
    /// command defers that mutable result until after receipt replay so a
    /// response-loss retry remains authoritative even after policy rotation.
    pub policy_result: String,
    pub preflight_policy_result: Option<String>,
    pub preflight_policy_reason: Option<String>,
    pub policy_revision: Option<String>,
    pub policy_digest: Option<String>,
    pub requested_permission: String,
}

/// Frozen result returned by the direct Task proposal command.  The receipt
/// is returned alongside the Task so native adapters can expose one audit
/// identity without manufacturing an AgentAction execution envelope.
#[derive(Debug, Clone)]
pub struct TaskProposalCommandResult {
    pub task: Task,
    pub receipt: CommandReceipt,
    pub replayed: bool,
}

impl TaskProposalPayload {
    fn normalized_governance(
        mut self,
        derived: Option<api_types::TaskGovernanceRequest>,
    ) -> Option<api_types::TaskGovernanceRequest> {
        if self.governance.is_some() {
            return self.governance;
        }
        self.governance = derived;
        self.governance
    }
}

impl TaskService {
    /// Execute an admitted action-backed `task.propose` through the shared
    /// Task command. REST/user execution keeps this path so an approval/audit
    /// Action remains the source authorization envelope.
    pub(crate) async fn execute_task_proposal_command(
        &self,
        action: &AgentAction,
        expected_action_version: i64,
        executed_by_type: &str,
        executed_by_id: &str,
        idempotency_key: &str,
    ) -> Result<Task> {
        self.execute_task_proposal_command_with_policy(
            action,
            expected_action_version,
            executed_by_type,
            executed_by_id,
            idempotency_key,
            None,
            None,
            None,
            None,
        )
        .await
        .map(|result| result.task)
    }

    /// Execute an automatically allowed Task proposal without creating an
    /// AgentAction. Native tools call this method with the host-derived
    /// source scope and expected policy provenance; the same atomic command
    /// materializer is used by the action-backed executor above.
    pub async fn execute_task_proposal_direct(
        &self,
        input: DirectTaskProposalInput,
    ) -> Result<TaskProposalCommandResult> {
        if input.actor_identity_id.trim().is_empty()
            || input.executor_type.trim().is_empty()
            || input.executor_id.trim().is_empty()
            || input.source_scope_type.trim().is_empty()
            || input.source_scope_id.trim().is_empty()
            || input.project_id.trim().is_empty()
            || input.idempotency_key.trim().is_empty()
            || input.correlation_id.trim().is_empty()
        {
            return Err(ServiceError::invalid_operation(
                "direct task proposal provenance is incomplete",
            ));
        }
        if input.source_scope_type != "project" && input.source_scope_type != "agent_chat" {
            return Err(ServiceError::invalid_operation(
                "direct task proposal source scope must be a Project or Project Agent Chat",
            ));
        }
        if input.policy_result != "allowed" {
            return Err(ServiceError::AuthorizationDenied {
                message: "task.propose policy did not admit direct execution".to_owned(),
            });
        }
        if input.requested_permission != "propose_task" {
            return Err(ServiceError::AuthorizationDenied {
                message: "task.propose requires the propose_task permission".to_owned(),
            });
        }
        if input.causation_depth < 0 || input.causation_depth > 8 {
            return Err(ServiceError::invalid_operation(
                "task proposal causation depth exceeds the reaction bound",
            ));
        }
        let payload_json = serde_json::to_string(&input.payload).map_err(|error| {
            ServiceError::invalid_operation(format!("task proposal payload is invalid: {error}"))
        })?;
        let payload_hash = sha256_hex(payload_json.as_bytes());
        let policy_result =
            AgentActionPolicyResult::from_str(&input.policy_result).map_err(|_| {
                ServiceError::invalid_operation("direct task proposal policy result is invalid")
            })?;
        let action = AgentAction {
            // An empty id is the explicit marker for the direct command.  It
            // is never persisted or queried as an AgentAction.
            id: String::new(),
            actor_identity_id: input.actor_identity_id.clone(),
            scope_type: input.source_scope_type.clone(),
            scope_id: input.source_scope_id.clone(),
            operation: TASK_PROPOSE_COMMAND.to_owned(),
            payload_json,
            payload_hash,
            dedupe_key: input.idempotency_key.clone(),
            correlation_id: input.correlation_id.clone(),
            causation_id: input.causation_id.clone(),
            causation_depth: input.causation_depth,
            requested_permission: input.requested_permission.clone(),
            policy_result,
            policy_reason: None,
            status: AgentActionStatus::Proposed,
            target_type: Some("project".to_owned()),
            target_id: Some(input.project_id.clone()),
            outcome_json: None,
            version: 0,
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        };
        self.execute_task_proposal_command_with_policy(
            &action,
            0,
            &input.executor_type,
            &input.executor_id,
            &input.idempotency_key,
            input.policy_revision.as_deref(),
            input.policy_digest.as_deref(),
            input.preflight_policy_result.as_deref(),
            input.preflight_policy_reason.as_deref(),
        )
        .await
    }

    /// Execute an admitted `task.propose` action through one command
    /// transaction.  The action row is the source authorization envelope;
    /// its successful execution is finalized with the Task domain rows.
    #[allow(clippy::too_many_arguments)]
    async fn execute_task_proposal_command_with_policy(
        &self,
        action: &AgentAction,
        expected_action_version: i64,
        executed_by_type: &str,
        executed_by_id: &str,
        idempotency_key: &str,
        policy_revision: Option<&str>,
        policy_digest: Option<&str>,
        preflight_policy_result: Option<&str>,
        preflight_policy_reason: Option<&str>,
    ) -> Result<TaskProposalCommandResult> {
        if action.operation != TASK_PROPOSE_COMMAND {
            return Err(ServiceError::invalid_operation(
                "action is not a task proposal",
            ));
        }
        let payload: TaskProposalPayload =
            serde_json::from_str(&action.payload_json).map_err(|error| {
                ServiceError::invalid_operation(format!(
                    "task proposal payload is invalid: {error}"
                ))
            })?;
        let project_id = action
            .target_id
            .clone()
            .filter(|_| action.target_type.as_deref() == Some("project"))
            .ok_or_else(|| {
                ServiceError::invalid_operation("task proposal must target a Project explicitly")
            })?;

        if !matches!(executed_by_type, "agent" | "user") || executed_by_id.trim().is_empty() {
            return Err(ServiceError::invalid_operation(
                "task proposal executor type and id are invalid",
            ));
        }

        // Build the canonical command identity from the source Action before
        // reading current Project/binding state.  In particular, the action
        // version and executor principal are part of the digest, so changing
        // either with the same key is an idempotency conflict.
        let context = task_proposal_context(
            action,
            expected_action_version,
            executed_by_type,
            executed_by_id,
            idempotency_key,
            &payload,
            &project_id,
            policy_revision,
            policy_digest,
        )?;

        // Replay is intentionally before current authorization, baseline, or
        // action-status checks.  The immutable receipt is the authority for a
        // response-loss retry; its frozen Task snapshot is returned exactly.
        if let Some(receipt) = self.replay_task_command(&context).await? {
            let task = frozen_task_from_receipt(&receipt)?;
            return Ok(TaskProposalCommandResult {
                task,
                receipt,
                replayed: true,
            });
        }

        if let Some(policy_result) = preflight_policy_result {
            if policy_result != "allowed" {
                return Err(ServiceError::AuthorizationDenied {
                    message: preflight_policy_reason
                        .unwrap_or("task.propose policy did not admit direct execution")
                        .to_owned(),
                });
            }
        }

        self.authorize_task_proposal_action(action, &project_id, executed_by_type, executed_by_id)
            .await?;
        if expected_action_version != action.version {
            return Err(ServiceError::Db(db::DbError::VersionConflict));
        }
        if action.status != AgentActionStatus::Proposed
            && action.status != AgentActionStatus::Approved
        {
            return Err(ServiceError::invalid_operation(
                "task proposal is not admitted for execution",
            ));
        }
        if action.policy_result == AgentActionPolicyResult::Denied
            || (action.policy_result == AgentActionPolicyResult::ApprovalRequired
                && action.status != AgentActionStatus::Approved)
        {
            return Err(ServiceError::invalid_operation(
                "task proposal policy has not admitted execution",
            ));
        }

        let project = ProjectRepo::get_by_id(&*self.db, &project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", project_id.clone()))?;
        let parent = if let Some(parent_task_id) = payload.parent_task_id.as_deref() {
            let parent = TaskRepo::get_by_id(&*self.db, parent_task_id, false)
                .await?
                .ok_or_else(|| ServiceError::not_found("task", parent_task_id.to_owned()))?;
            if parent.project_id != project_id || parent.parent_task_id.is_some() {
                return Err(ServiceError::invalid_operation(
                    "task proposal parent must be a root Task in the same Project",
                ));
            }
            Some(parent)
        } else {
            None
        };
        let repo_id = parent
            .as_ref()
            .map(|task| task.repo_id.clone())
            .unwrap_or_else(|| project.primary_repo_id.clone());
        let task_type = payload.task_type.clone().unwrap_or_else(|| {
            if parent.is_some() {
                "sub_task".to_owned()
            } else {
                "task".to_owned()
            }
        });
        if !matches!(
            task_type.as_str(),
            "task" | "planning_task" | "sub_task" | "discovery"
        ) {
            return Err(ServiceError::invalid_operation(
                "task_type must be task, planning_task, sub_task, or discovery",
            ));
        }
        let derived_governance = self
            .derive_active_baseline_governance(&project_id, &payload, &task_type)
            .await?;
        let governance = payload.clone().normalized_governance(derived_governance);
        // A proposal with a parent is an adaptive split even when the
        // transport omitted its governance envelope. Do not let the
        // task.propose path bypass the shared parent-boundary validation that
        // create_task_with_governance applies to ordinary split requests.
        if let Some(parent) = parent.as_ref() {
            if let Some(source_governance) = self.adaptive_task_governance(parent).await? {
                if source_governance.baseline_id.is_some() {
                    let requested = governance.as_ref().ok_or_else(|| {
                        ServiceError::Conflict(
                            "reconciliation_required: adaptive Task proposal must carry the exact parent governance envelope"
                                .to_owned(),
                        )
                    })?;
                    self.validate_adaptive_child_governance(parent, requested, "split")
                        .await?;
                }
            }
        }
        let prepared_governance = self
            .prepare_task_governance(&project, repo_id.as_ref(), &task_type, governance)
            .await?;

        let workflow = if parent.is_some() {
            crate::workflow::engine::WorkflowEngine::resolve_subtask_workflow()
        } else {
            crate::workflow::engine::WorkflowEngine::resolve_workflow(&project.workflow_definition)
        };
        let initial_status = if repo_id.is_none() {
            workflow
                .states
                .iter()
                .find(|state| state.kind == api_types::StateKind::Backlog)
                .map(|state| state.name.clone())
                .ok_or_else(|| ServiceError::invalid_operation("workflow has no backlog state"))?
        } else {
            workflow
                .states
                .iter()
                .find(|state| state.kind == api_types::StateKind::Initial)
                .map(|state| state.name.clone())
                .ok_or_else(|| ServiceError::invalid_operation("workflow has no initial state"))?
        };
        let now = now_rfc3339();
        let task_id = db::new_uuid_v4();
        // Preserve the existing Task workflow semantics: an omitted
        // per-task review configuration inherits the Project's default.
        let task_state_config = match payload.task_state_config.clone() {
            Some(config) => Some(config),
            None => {
                let settings: Value = serde_json::from_str(&project.settings).map_err(|error| {
                    ServiceError::invalid_operation(format!("invalid Project settings: {error}"))
                })?;
                settings
                    .get("default_review_config")
                    .cloned()
                    .map(|review| serde_json::json!({ "review": review }).to_string())
            }
        };
        let metadata_json = parent.as_ref().and_then(|_| {
            db::TaskMetadata {
                ..db::TaskMetadata::default()
            }
            .to_json()
        });
        let task = Task {
            id: task_id.clone(),
            project_id: project_id.clone(),
            repo_id: repo_id.clone(),
            parent_task_id: payload.parent_task_id.clone(),
            assignee_type: None,
            assignee_id: None,
            title: payload.title.clone(),
            description: payload.description.clone(),
            task_type: task_type.clone(),
            status: initial_status.clone(),
            is_automation: false,
            priority: payload.priority.unwrap_or(0),
            board_position: 0.0,
            subtask_order: None,
            task_state_config: task_state_config.clone(),
            merge_config: super::validation::serialize_config(payload.merge_config.clone())?,
            metadata_json: metadata_json.clone(),
            plan: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            entry_barrier_json: None,
            review_passed_at: None,
            archived_at: None,
            deleted_at: None,
            version: 1,
            created_at: now.clone(),
            updated_at: now.clone(),
        };
        let mut role_assignments =
            validate_role_assignments(&workflow, payload.role_assignments.clone(), &task_id, &now)?;
        let covered_roles = role_assignments
            .iter()
            .map(|assignment| assignment.role_name.clone())
            .collect();
        let defaults = if parent.is_none() {
            self.project_default_role_assignments(&task, covered_roles)
                .await?
        } else {
            Vec::new()
        };
        role_assignments.extend(defaults);

        let outcome_json = json!({
            "operation": TASK_PROPOSE_COMMAND,
            "project_id": project_id,
            "task_id": task_id,
            "domain_committed": true,
            "requires_user_authorization": false,
        })
        .to_string();
        let (receipt, action_execution) = command_bundle(&context, &outcome_json);
        let governance = prepared_governance.map(|governance| CreateProjectTaskGovernance {
            task_id: task_id.clone(),
            project_id: project_id.clone(),
            charter_revision_id: governance.charter_revision_id,
            baseline_id: governance.baseline_id,
            baseline_revision_id: governance.baseline_revision_id,
            plan_item_id: governance.plan_item_id,
            milestone_id: governance.milestone_id,
            document_revisions_json: governance.document_revisions_json,
            capability_class: governance.capability_class,
            risk_class: governance.risk_class,
            runnable: governance.runnable,
            replacement_of_task_id: governance.replacement_of_task_id,
            provenance_json: governance.provenance_json,
            created_at: now.clone(),
            updated_at: now.clone(),
        });
        let committed = ProjectOrchestrationRepo::create_task_proposal_command(
            &*self.db,
            CreateTaskProposalCommand {
                task: CreateTask {
                    id: task.id.clone(),
                    project_id: task.project_id.clone(),
                    repo_id: task.repo_id.clone(),
                    parent_task_id: task.parent_task_id.clone(),
                    subtask_order: None,
                    assignee_type: None,
                    assignee_id: None,
                    title: task.title.clone(),
                    description: task.description.clone(),
                    task_type: task.task_type.clone(),
                    status: task.status.clone(),
                    is_automation: false,
                    priority: task.priority,
                    task_state_config: task.task_state_config.clone(),
                    merge_config: task.merge_config.clone(),
                    plan: None,
                    created_at: task.created_at.clone(),
                    updated_at: task.updated_at.clone(),
                },
                governance,
                role_assignments,
                metadata_json,
                source_action_id: (!action.id.trim().is_empty()).then(|| action.id.clone()),
                expected_action_version: (!action.id.trim().is_empty())
                    .then_some(expected_action_version),
                source_actor_identity_id: action.actor_identity_id.clone(),
                source_scope_type: action.scope_type.clone(),
                source_scope_id: action.scope_id.clone(),
                source_target_type: action.target_type.clone(),
                source_target_id: action.target_id.clone(),
                source_operation: action.operation.clone(),
                source_requested_permission: action.requested_permission.clone(),
                source_policy_result: action.policy_result.to_string(),
                source_policy_revision: policy_revision.map(str::to_owned),
                source_policy_digest: policy_digest.map(str::to_owned),
                source_payload_hash: action.payload_hash.clone(),
                executor_type: executed_by_type.to_owned(),
                executor_id: executed_by_id.to_owned(),
                command_receipt: Some(receipt),
                action_execution,
            },
        )
        .await
        .map_err(ServiceError::from)?;
        self.publish(ForgeEvent {
            event_type: "task.created".to_owned(),
            entity_id: committed.task.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskCreated {
                project_id: committed.task.project_id.clone(),
                title: committed.task.title.clone(),
            },
        });
        let receipt = CommandReceiptRepo::get_command_receipt(
            &*self.db,
            context.principal().principal_type(),
            context.principal().principal_id(),
            context.canonical_scope().scope_type().as_str(),
            context.canonical_scope().scope_id(),
            context.operation(),
            context.idempotency_key(),
            context.input_digest(),
        )
        .await?
        .ok_or_else(|| ServiceError::Db(db::DbError::IdempotencyConflict))?;
        Ok(TaskProposalCommandResult {
            task: committed.task,
            receipt,
            replayed: false,
        })
    }

    /// Resolve whether this exact proposal already committed before a
    /// transport performs its mutable user/scope authorization.  Adapters
    /// use this only as a replay gate; the command repeats the receipt lookup
    /// inside its writer transaction before applying any domain mutation.
    pub async fn task_proposal_replay_exists(
        &self,
        action: &AgentAction,
        expected_action_version: i64,
        executed_by_type: &str,
        executed_by_id: &str,
        idempotency_key: &str,
    ) -> Result<bool> {
        if action.operation != TASK_PROPOSE_COMMAND {
            return Err(ServiceError::invalid_operation(
                "action is not a task proposal",
            ));
        }
        let payload: TaskProposalPayload =
            serde_json::from_str(&action.payload_json).map_err(|error| {
                ServiceError::invalid_operation(format!(
                    "task proposal payload is invalid: {error}"
                ))
            })?;
        let project_id = action
            .target_id
            .clone()
            .filter(|_| action.target_type.as_deref() == Some("project"))
            .ok_or_else(|| {
                ServiceError::invalid_operation("task proposal must target a Project explicitly")
            })?;
        let context = task_proposal_context(
            action,
            expected_action_version,
            executed_by_type,
            executed_by_id,
            idempotency_key,
            &payload,
            &project_id,
            None,
            None,
        )?;
        Ok(self.replay_task_command(&context).await?.is_some())
    }

    async fn replay_task_command(
        &self,
        context: &CommandContext,
    ) -> Result<Option<CommandReceipt>> {
        let existing = CommandReceiptRepo::get_command_receipt(
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
        .map_err(ServiceError::from)?;
        if existing.is_some() {
            return Ok(existing);
        }
        let existing_digest: Option<String> = sqlx::query_scalar(
            "SELECT input_digest FROM command_receipt
             WHERE principal_type = ? AND principal_id = ? AND scope_type = ?
               AND scope_id = ? AND operation = ? AND idempotency_key = ? LIMIT 1",
        )
        .bind(context.principal().principal_type())
        .bind(context.principal().principal_id())
        .bind(context.canonical_scope().scope_type().as_str())
        .bind(context.canonical_scope().scope_id())
        .bind(context.operation())
        .bind(context.idempotency_key())
        .fetch_optional(self.db.pool())
        .await?;
        if existing_digest.is_some() {
            return Err(ServiceError::Db(db::DbError::IdempotencyConflict));
        }
        Ok(None)
    }

    async fn authorize_task_proposal_action(
        &self,
        action: &AgentAction,
        project_id: &str,
        executed_by_type: &str,
        executed_by_id: &str,
    ) -> Result<()> {
        if action.target_type.as_deref() != Some("project")
            || action.target_id.as_deref() != Some(project_id)
        {
            return Err(ServiceError::invalid_operation(
                "task proposal target must match its Project scope",
            ));
        }
        match action.scope_type.as_str() {
            "project" if action.scope_id == project_id => {}
            "agent_chat" => {
                let row =
                    sqlx::query("SELECT kind, project_id FROM agent_chat WHERE id = ? LIMIT 1")
                        .bind(&action.scope_id)
                        .fetch_optional(self.db.pool())
                        .await?
                        .ok_or_else(|| {
                            ServiceError::not_found("agent_chat", action.scope_id.clone())
                        })?;
                let kind: String = row.try_get("kind")?;
                let chat_project_id: Option<String> = row.try_get("project_id")?;
                if kind != "project" || chat_project_id.as_deref() != Some(project_id) {
                    return Err(ServiceError::invalid_operation(
                        "task proposal scope must match its owning Project Agent Chat",
                    ));
                }
            }
            _ => {
                return Err(ServiceError::invalid_operation(
                    "task proposal scope must match its target Project",
                ));
            }
        }
        let permissions: Option<String> = sqlx::query_scalar(
            "SELECT permission_ceiling_json FROM project_agent_binding
             WHERE project_id = ? AND identity_id = ? AND state = 'active' LIMIT 1",
        )
        .bind(project_id)
        .bind(&action.actor_identity_id)
        .fetch_optional(self.db.pool())
        .await?;
        if !permissions.as_deref().is_some_and(|permissions| {
            serde_json::from_str::<Value>(permissions)
                .ok()
                .and_then(|value| {
                    value
                        .get("allowed")
                        .or_else(|| value.get("permissions"))
                        .cloned()
                })
                .and_then(|value| value.as_array().cloned())
                .is_some_and(|allowed| {
                    allowed
                        .iter()
                        .any(|permission| permission.as_str() == Some("propose_task"))
                })
        }) {
            return Err(ServiceError::invalid_operation(
                "task proposal actor is not an active Project binding",
            ));
        }
        match executed_by_type {
            "agent" => {
                if executed_by_id != action.actor_identity_id {
                    return Err(ServiceError::AuthorizationDenied {
                        message: "agent executor must be the source Project Agent".to_owned(),
                    });
                }
                let exists: Option<i64> =
                    sqlx::query_scalar("SELECT 1 FROM agent_identity WHERE id = ? LIMIT 1")
                        .bind(executed_by_id)
                        .fetch_optional(self.db.pool())
                        .await?;
                if exists.is_none() {
                    return Err(ServiceError::AuthorizationDenied {
                        message: "agent executor identity does not exist".to_owned(),
                    });
                }
            }
            "user" => {
                let owner: Option<String> =
                    sqlx::query_scalar("SELECT owner_id FROM project WHERE id = ? LIMIT 1")
                        .bind(project_id)
                        .fetch_optional(self.db.pool())
                        .await?;
                let member: Option<i64> = sqlx::query_scalar(
                    "SELECT 1 FROM project_member
                     WHERE project_id = ? AND user_id = ? LIMIT 1",
                )
                .bind(project_id)
                .bind(executed_by_id)
                .fetch_optional(self.db.pool())
                .await?;
                if owner.as_deref() != Some(executed_by_id) && member.is_none() {
                    return Err(ServiceError::AuthorizationDenied {
                        message: "user executor is not a Project owner or member".to_owned(),
                    });
                }
            }
            _ => {
                return Err(ServiceError::invalid_operation(
                    "task proposal executor type is unsupported",
                ));
            }
        }
        Ok(())
    }

    async fn derive_active_baseline_governance(
        &self,
        project_id: &str,
        payload: &TaskProposalPayload,
        task_type: &str,
    ) -> Result<Option<api_types::TaskGovernanceRequest>> {
        // Planning/discovery Tasks remain valid pre-existing read-only plans
        // even while a baseline is active. Implementation intent, however,
        // must resolve the active baseline before governance validation so a
        // missing plan_item_id cannot silently fall through as an ungoverned
        // Task. Explicit read-only capability likewise remains outside this
        // implementation-only derivation path.
        if !classify_task_execution(task_type, payload.capability_class.as_deref())?
            .requires_baseline()
        {
            return Ok(None);
        }
        let charter_revision_id: Option<String> = sqlx::query_scalar(
            "SELECT current_charter_revision_id FROM project
             WHERE id = ? AND charter_status = 'charter_backed'
               AND charter_setup_required = 0 LIMIT 1",
        )
        .bind(project_id)
        .fetch_optional(self.db.pool())
        .await?
        .flatten();
        let Some(charter_revision_id) = charter_revision_id else {
            return Ok(None);
        };
        let row = sqlx::query(
            "SELECT b.id, b.current_revision_id, r.primary_milestone_id
             FROM project_execution_baseline b
             JOIN project_execution_baseline_revision r
               ON r.id = b.current_revision_id AND r.baseline_id = b.id
             WHERE b.project_id = ? AND b.lifecycle = 'active'
               AND r.lifecycle = 'approved'
             ORDER BY b.updated_at DESC, b.id DESC LIMIT 1",
        )
        .bind(project_id)
        .fetch_optional(self.db.pool())
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let baseline_id: String = row.try_get("id")?;
        let baseline_revision_id: Option<String> = row.try_get("current_revision_id")?;
        let primary_milestone_id: Option<String> = row.try_get("primary_milestone_id")?;
        let Some(baseline_revision_id) = baseline_revision_id else {
            return Ok(None);
        };
        Ok(Some(api_types::TaskGovernanceRequest {
            charter_revision_id: Some(charter_revision_id),
            baseline_id: Some(baseline_id),
            baseline_revision_id: Some(baseline_revision_id),
            plan_item_id: payload.plan_item_id.clone(),
            milestone_id: payload.milestone_id.clone().or(primary_milestone_id),
            document_revision_ids: Vec::new(),
            capability_class: payload.capability_class.clone(),
            risk_class: payload.risk_class.clone(),
            provenance: None,
        }))
    }
}

#[derive(Serialize)]
struct TaskProposalDigestInput<'a> {
    payload: &'a TaskProposalPayload,
    source_action_id: &'a str,
    source_actor_identity_id: &'a str,
    source_scope_type: &'a str,
    source_scope_id: &'a str,
    source_target_type: &'a Option<String>,
    source_target_id: &'a Option<String>,
    source_operation: &'a str,
    source_requested_permission: &'a str,
    source_policy_result: String,
    source_payload_hash: &'a str,
}

#[allow(clippy::too_many_arguments)]
fn task_proposal_context(
    action: &AgentAction,
    expected_action_version: i64,
    executed_by_type: &str,
    executed_by_id: &str,
    idempotency_key: &str,
    payload: &TaskProposalPayload,
    project_id: &str,
    policy_revision: Option<&str>,
    policy_digest: Option<&str>,
) -> Result<CommandContext> {
    let mut expected_state = ExpectedCommandState::default();
    if !action.id.trim().is_empty() {
        expected_state
            .versions
            .insert("agent_action".to_owned(), expected_action_version);
    }
    CommandContext::from_authorized_input(
        NewCommandContext {
            principal: CommandPrincipal {
                principal_type: executed_by_type.to_owned(),
                principal_id: executed_by_id.to_owned(),
            },
            canonical_scope: CommandScope {
                scope_type: CommandScopeType::Project,
                scope_id: project_id.to_owned(),
            },
            operation: TASK_PROPOSE_COMMAND.to_owned(),
            idempotency_key: idempotency_key.to_owned(),
            expected_state,
            authorization_provenance: Some(AuthorizationProvenance {
                policy_result: action.policy_result.to_string(),
                policy_revision: policy_revision.map(str::to_owned),
                policy_digest: policy_digest.map(str::to_owned),
                requested_permission: Some(action.requested_permission.clone()),
            }),
            action_provenance: (!action.id.trim().is_empty()).then(|| {
                AgentActionProvenance::new(
                    action.id.clone(),
                    expected_action_version,
                    1,
                    idempotency_key.to_owned(),
                    executed_by_type.to_owned(),
                    executed_by_id.to_owned(),
                )
            }),
            correlation_id: action.correlation_id.clone(),
            causation_id: action.causation_id.clone(),
            causation_depth: action.causation_depth,
        },
        &TaskProposalDigestInput {
            payload,
            source_action_id: &action.id,
            source_actor_identity_id: &action.actor_identity_id,
            source_scope_type: &action.scope_type,
            source_scope_id: &action.scope_id,
            source_target_type: &action.target_type,
            source_target_id: &action.target_id,
            source_operation: &action.operation,
            source_requested_permission: &action.requested_permission,
            source_policy_result: action.policy_result.to_string(),
            source_payload_hash: &action.payload_hash,
        },
    )
    .map_err(|error| {
        ServiceError::invalid_operation(format!("task proposal digest failed: {error}"))
    })
}

fn validate_role_assignments(
    workflow: &api_types::WorkflowDefinition,
    assignments: Option<Vec<api_types::InitialRoleAssignment>>,
    task_id: &str,
    now: &str,
) -> Result<Vec<CreateTaskRoleAssignment>> {
    let Some(assignments) = assignments else {
        return Ok(Vec::new());
    };
    let workflow_roles = workflow
        .roles
        .iter()
        .map(|role| role.name.as_str())
        .collect::<std::collections::HashSet<_>>();
    assignments
        .into_iter()
        .map(|assignment| {
            if !workflow_roles.contains(assignment.role_name.as_str()) {
                return Err(ServiceError::invalid_operation(format!(
                    "unknown role: {}",
                    assignment.role_name
                )));
            }
            let assignee_type = match assignment.assignee_type {
                api_types::assignee::AssigneeKind::Agent => db::AssigneeKind::Agent,
                api_types::assignee::AssigneeKind::User => db::AssigneeKind::User,
            };
            let assignee_id = assignment
                .assignee_id
                .filter(|id| !id.trim().is_empty())
                .ok_or_else(|| {
                    ServiceError::invalid_operation(format!(
                        "role assignment for '{}' requires assignee_id",
                        assignment.role_name
                    ))
                })?;
            Ok(CreateTaskRoleAssignment {
                id: db::new_uuid_v4(),
                task_id: task_id.to_owned(),
                role_name: assignment.role_name,
                assignee_type: Some(assignee_type),
                assignee_id: Some(assignee_id),
                created_at: now.to_owned(),
                updated_at: now.to_owned(),
            })
        })
        .collect()
}

fn command_bundle(
    context: &CommandContext,
    outcome_json: &str,
) -> (CreateCommandReceipt, Option<CreateAgentActionExecution>) {
    let mut receipt = CreateCommandReceipt {
        id: db::new_uuid_v4(),
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
        agent_action_execution_id: None,
        outcome_json: outcome_json.to_owned(),
        committed_at: now_rfc3339(),
    };
    let execution = context.action_provenance.as_ref().map(|provenance| {
        let committed_at = now_rfc3339();
        CreateAgentActionExecution {
            id: db::new_uuid_v4(),
            action_id: provenance.action_id.clone(),
            expected_action_version: provenance.expected_action_version,
            attempt: provenance.attempt,
            status: AgentActionExecutionStatus::Succeeded,
            result_json: Some(receipt.outcome_json.clone()),
            error: None,
            executed_by_type: provenance.executed_by_type.clone(),
            executed_by_id: provenance.executed_by_id.clone(),
            idempotency_key: provenance.execution_idempotency_key.clone(),
            action_status: AgentActionStatus::Executed,
            action_outcome_json: Some(receipt.outcome_json.clone()),
            created_at: committed_at.clone(),
            completed_at: Some(committed_at.clone()),
            updated_at: committed_at,
        }
    });
    if let Some(execution) = execution.as_ref() {
        receipt.agent_action_execution_id = Some(execution.id.clone());
    }
    (receipt, execution)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    hex::encode(digest.finalize())
}

fn frozen_task_from_receipt(receipt: &CommandReceipt) -> Result<Task> {
    let outcome: Value = serde_json::from_str(&receipt.outcome_json)
        .map_err(|_| ServiceError::Db(db::DbError::IdempotencyConflict))?;
    serde_json::from_value(
        outcome
            .get("task")
            .cloned()
            .ok_or_else(|| ServiceError::Db(db::DbError::IdempotencyConflict))?,
    )
    .map_err(|_| ServiceError::Db(db::DbError::IdempotencyConflict))
}
