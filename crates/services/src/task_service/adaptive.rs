//! Shared TaskService boundary for bounded split/sequence/replace changes.
//!
//! The public methods that historically implemented these operations remain
//! as compatibility wrappers, but all of them now construct this one typed
//! command.  The DB composite repeats the policy/baseline/CAS checks under
//! `BEGIN IMMEDIATE` and commits the Task rows, governance, domain event, and
//! command receipt together.

use super::*;
use crate::command_boundary::{
    AuthorizationProvenance, CommandContext, CommandPrincipal, CommandScope, CommandScopeType,
    ExpectedCommandState, NewCommandContext,
};
use db::{
    ApplyAdaptiveTaskCommand, CommandReceipt, CommandReceiptRepo, CreateCommandReceipt,
    ProjectOrchestrationRepo, Task,
};
use serde::Serialize;
use serde_json::json;

pub use db::{AdaptiveTaskChild, AdaptiveTaskOperation};

pub const TASK_ADAPTIVE_COMMAND: &str = "task.adaptive";

/// Transport-neutral input for one bounded Task reshape.  `operation` is a
/// closed enum so an adapter cannot smuggle a fourth mutation path around the
/// shared governance gate.  The actor/rationale fields are persisted in both
/// the event and inherited child provenance.
#[derive(Debug, Clone, Serialize)]
pub struct AdaptiveTaskCommand {
    pub project_id: String,
    pub source_task_id: String,
    pub expected_task_version: i64,
    pub expected_board_revision: i64,
    pub operation: AdaptiveTaskOperation,
    pub rationale: String,
    pub actor_type: String,
    pub actor_id: String,
    pub policy_result: String,
    pub policy_revision: Option<String>,
    pub policy_digest: Option<String>,
    pub requested_permission: Option<String>,
    pub idempotency_key: String,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub causation_depth: i64,
}

impl AdaptiveTaskCommand {
    /// Build a server-owned command for legacy service wrappers. Callers that
    /// carry an authenticated policy should fill the policy/provenance fields
    /// directly before execution.
    pub fn system(
        project_id: impl Into<String>,
        source_task_id: impl Into<String>,
        expected_task_version: i64,
        expected_board_revision: i64,
        operation: AdaptiveTaskOperation,
        rationale: impl Into<String>,
    ) -> Self {
        Self {
            project_id: project_id.into(),
            source_task_id: source_task_id.into(),
            expected_task_version,
            expected_board_revision,
            operation,
            rationale: rationale.into(),
            actor_type: "system".to_owned(),
            actor_id: "task-service".to_owned(),
            policy_result: "allowed".to_owned(),
            policy_revision: None,
            policy_digest: None,
            requested_permission: None,
            idempotency_key: db::new_uuid_v4(),
            correlation_id: db::new_uuid_v4(),
            causation_id: None,
            causation_depth: 0,
        }
    }

    pub fn operation_name(&self) -> &'static str {
        self.operation.name()
    }
}

/// Frozen result returned by the adaptive command. A replay returns the
/// receipt's original snapshots and `replayed = true`, never a live row that
/// may have changed after the original commit.
#[derive(Debug, Clone)]
pub struct AdaptiveTaskCommandResult {
    pub source_task: Task,
    pub tasks: Vec<Task>,
    pub board_revision: i64,
    pub receipt: CommandReceipt,
    pub replayed: bool,
}

#[derive(Debug, Clone, Serialize)]
struct AdaptiveTaskDigestInput<'a> {
    project_id: &'a str,
    source_task_id: &'a str,
    expected_task_version: i64,
    expected_board_revision: i64,
    operation: &'a AdaptiveTaskOperation,
    rationale: &'a str,
}

impl TaskService {
    /// Execute split, sequence, or replace through the single adaptive Task
    /// command boundary.
    pub async fn execute_adaptive_task_command(
        &self,
        command: AdaptiveTaskCommand,
    ) -> Result<AdaptiveTaskCommandResult> {
        validate_adaptive_command(&command)?;
        let mut expected_state = ExpectedCommandState::default();
        expected_state
            .versions
            .insert("task".to_owned(), command.expected_task_version);
        expected_state
            .versions
            .insert("board".to_owned(), command.expected_board_revision);
        let context = CommandContext::from_authorized_input(
            NewCommandContext {
                principal: CommandPrincipal {
                    principal_type: command.actor_type.clone(),
                    principal_id: command.actor_id.clone(),
                },
                canonical_scope: CommandScope {
                    scope_type: CommandScopeType::Project,
                    scope_id: command.project_id.clone(),
                },
                operation: TASK_ADAPTIVE_COMMAND.to_owned(),
                idempotency_key: command.idempotency_key.clone(),
                expected_state,
                authorization_provenance: Some(AuthorizationProvenance {
                    policy_result: command.policy_result.clone(),
                    policy_revision: command.policy_revision.clone(),
                    policy_digest: command.policy_digest.clone(),
                    requested_permission: command.requested_permission.clone(),
                }),
                action_provenance: None,
                correlation_id: command.correlation_id.clone(),
                causation_id: command.causation_id.clone(),
                causation_depth: command.causation_depth,
            },
            &AdaptiveTaskDigestInput {
                project_id: &command.project_id,
                source_task_id: &command.source_task_id,
                expected_task_version: command.expected_task_version,
                expected_board_revision: command.expected_board_revision,
                operation: &command.operation,
                rationale: &command.rationale,
            },
        )
        .map_err(|error| {
            ServiceError::invalid_operation(format!("adaptive Task digest failed: {error}"))
        })?;
        let receipt = CreateCommandReceipt {
            id: db::new_uuid_v4(),
            principal_type: context.principal().principal_type().to_owned(),
            principal_id: context.principal().principal_id().to_owned(),
            scope_type: context.canonical_scope().scope_type().as_str().to_owned(),
            scope_id: context.canonical_scope().scope_id().to_owned(),
            operation: context.operation().to_owned(),
            idempotency_key: context.idempotency_key().to_owned(),
            input_digest: context.input_digest().to_owned(),
            policy_result: command.policy_result.clone(),
            correlation_id: context.correlation_id().to_owned(),
            causation_id: command.causation_id.clone(),
            causation_depth: command.causation_depth,
            event_id: String::new(),
            agent_action_execution_id: None,
            outcome_json: json!({
                "operation": command.operation_name(),
                "project_id": command.project_id,
                "source_task_id": command.source_task_id,
                "rationale": command.rationale,
            })
            .to_string(),
            committed_at: now_rfc3339(),
        };
        // Check the durable receipt before any source/baseline preflight. An
        // exact retry must replay its frozen result even when the mutable
        // source Task or active baseline has since changed. The DB composite
        // repeats this lookup under its own BEGIN IMMEDIATE transaction for
        // the final race-safe authority boundary.
        let existing = CommandReceiptRepo::get_command_receipt(
            &*self.db,
            &receipt.principal_type,
            &receipt.principal_id,
            &receipt.scope_type,
            &receipt.scope_id,
            &receipt.operation,
            &receipt.idempotency_key,
            &receipt.input_digest,
        )
        .await?;
        if existing.is_none() {
            if command.policy_result != "allowed" {
                return Err(ServiceError::AuthorizationDenied {
                    message: "adaptive Task command policy did not admit execution".to_owned(),
                });
            }
            let source = TaskRepo::get_by_id(&*self.db, &command.source_task_id, false)
                .await?
                .ok_or_else(|| ServiceError::not_found("task", command.source_task_id.clone()))?;
            if source.project_id != command.project_id {
                return Err(ServiceError::NotFound {
                    entity: "task",
                    id: command.source_task_id.clone(),
                });
            }
            // Friendly preflight (the DB repeats it while holding the writer
            // transaction). This preserves the existing reconciliation
            // projection for stale/out-of-envelope requests while the command
            // itself remains authoritative against races.
            self.authorize_adaptive_task_operation(&source, command.operation_name())
                .await?;
        }
        let committed = ProjectOrchestrationRepo::apply_adaptive_task_command(
            &*self.db,
            ApplyAdaptiveTaskCommand {
                project_id: command.project_id.clone(),
                source_task_id: command.source_task_id.clone(),
                expected_task_version: command.expected_task_version,
                expected_board_revision: command.expected_board_revision,
                operation: command.operation.clone(),
                rationale: command.rationale.clone(),
                command_receipt: Some(receipt),
                action_execution: None,
            },
        )
        .await
        .map_err(ServiceError::from)?;

        if !committed.replayed {
            match command.operation {
                AdaptiveTaskOperation::Split { .. } | AdaptiveTaskOperation::Replace { .. } => {
                    for task in &committed.tasks {
                        self.publish(ForgeEvent {
                            event_type: "task.created".to_owned(),
                            entity_id: task.id.clone(),
                            timestamp: event_timestamp(),
                            context: EventContext::TaskCreated {
                                project_id: task.project_id.clone(),
                                title: task.title.clone(),
                            },
                        });
                    }
                }
                AdaptiveTaskOperation::Sequence { .. } => {
                    self.publish(ForgeEvent {
                        event_type: "task.updated".to_owned(),
                        entity_id: committed.source_task.id.clone(),
                        timestamp: event_timestamp(),
                        context: EventContext::TaskUpdated {
                            project_id: committed.source_task.project_id.clone(),
                        },
                    });
                }
            }
        }
        Ok(AdaptiveTaskCommandResult {
            source_task: committed.source_task,
            tasks: committed.tasks,
            board_revision: committed.board_revision,
            receipt: committed.receipt,
            replayed: committed.replayed,
        })
    }
}

fn validate_adaptive_command(command: &AdaptiveTaskCommand) -> Result<()> {
    for (field, value) in [
        ("project_id", command.project_id.as_str()),
        ("source_task_id", command.source_task_id.as_str()),
        ("rationale", command.rationale.as_str()),
        ("actor_type", command.actor_type.as_str()),
        ("actor_id", command.actor_id.as_str()),
        ("idempotency_key", command.idempotency_key.as_str()),
        ("correlation_id", command.correlation_id.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(ServiceError::invalid_operation(format!(
                "adaptive Task {field} is required"
            )));
        }
    }
    if command.expected_task_version < 1 || command.expected_board_revision < 0 {
        return Err(ServiceError::invalid_operation(
            "adaptive Task expected versions are invalid",
        ));
    }
    if !(0..=16).contains(&command.causation_depth) {
        return Err(ServiceError::invalid_operation(
            "adaptive Task causation depth is outside the allowed range",
        ));
    }
    match &command.operation {
        AdaptiveTaskOperation::Split { items } if items.is_empty() => Err(
            ServiceError::invalid_operation("adaptive Task split requires at least one child"),
        ),
        AdaptiveTaskOperation::Sequence { ordered_task_ids } => {
            if ordered_task_ids.is_empty() {
                return Err(ServiceError::invalid_operation(
                    "adaptive Task sequence requires at least one subtask",
                ));
            }
            let unique = ordered_task_ids
                .iter()
                .collect::<std::collections::HashSet<_>>();
            if unique.len() != ordered_task_ids.len() {
                return Err(ServiceError::invalid_operation(
                    "adaptive Task sequence ids must be unique",
                ));
            }
            Ok(())
        }
        AdaptiveTaskOperation::Replace { title, .. } if title.trim().is_empty() => Err(
            ServiceError::invalid_operation("adaptive Task replacement title is required"),
        ),
        _ => Ok(()),
    }
}
