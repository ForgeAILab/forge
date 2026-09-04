use api_types::{
    StateKind, TaskWorkflowStateSnapshot, TaskWorkflowTransitionSnapshot, WorkflowDefinition,
};
use sha2::{Digest, Sha256};

use crate::{Result, ServiceError};

/// Freeze the workflow actually used by transition guards and hooks. The Task
/// version is checked again by the atomic transition/board-move writer.
pub fn transition_workflow_snapshot(
    task: &db::Task,
    workflow: &WorkflowDefinition,
    from_state: &str,
    to_state: &str,
) -> Result<serde_json::Value> {
    let state_snapshot = |name: &str| -> Result<TaskWorkflowStateSnapshot> {
        let state = workflow
            .states
            .iter()
            .find(|state| state.name == name)
            .ok_or_else(|| {
                ServiceError::invalid_operation(
                    super::engine::WorkflowEngine::undefined_state_message(name, workflow),
                )
            })?;
        Ok(TaskWorkflowStateSnapshot {
            name: state.name.clone(),
            kind: state.kind,
            canonical_phase: workflow.canonical_phase_for_state(name),
            requires_user_approval: state
                .gate_config
                .as_ref()
                .is_some_and(|gate| gate.requires_user_approval()),
            is_cancellation: state.kind == StateKind::Terminal
                && workflow
                    .cancellation_state
                    .as_deref()
                    .unwrap_or("cancelled")
                    == name,
        })
    };
    let definition = serde_json::to_vec(workflow)
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
    let snapshot = TaskWorkflowTransitionSnapshot {
        definition_digest: format!("sha256:{}", hex::encode(Sha256::digest(&definition))),
        parent_task_id: task.parent_task_id.clone(),
        source_task_version: task.version,
        from_state: state_snapshot(from_state)?,
        to_state: state_snapshot(to_state)?,
    };
    serde_json::to_value(snapshot)
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))
}
