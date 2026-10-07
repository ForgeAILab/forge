//! The visible half of an explicit owner park.
//!
//! A Task that nothing will ever continue (its state is not in the workflow,
//! its merge entry cannot be replayed safely, its plan publication marker
//! cannot be read) used to sit with no step, no owner and no annotation.
//! Until public readers switch to the condition, the park is also written as
//! an annotation they already show: an existing kind, a reason naming the
//! owner and the action, and no effect on dispatch or slot classification.
//! The dispatcher that wrote it removes it as soon as the Task resolves to
//! anything else, so a repaired Task continues on the pass that sees the
//! repair.
use api_types::{Actor, FailureKind, SystemComponent, TaskBlockingAnnotation};
use db::{Task, TaskRepo, UpdateTaskStatus};

use super::{
    next_step::{Action, Owner, Park, Reason},
    TaskDispatcher,
};
use crate::{Result, ServiceError};

const WORKFLOW_INVALID: &str = "workflow_invalid";
const UNKNOWN_CONDITION: &str = "unknown_condition";

fn dispatcher() -> String {
    Actor::system(SystemComponent::TaskDispatcher).display()
}

/// Whether the Task's annotation is a park this dispatcher wrote.
pub(super) fn owns(task: &Task) -> bool {
    stored(task).is_some()
}

fn stored(task: &Task) -> Option<TaskBlockingAnnotation> {
    let annotation: TaskBlockingAnnotation =
        serde_json::from_str(task.error_annotation.as_deref()?).ok()?;
    (annotation.annotation_type == FailureKind::WorkflowGuardRejected
        && annotation.blocked_by.as_deref() == Some(dispatcher().as_str())
        && matches!(
            annotation.blocking_reason.as_str(),
            WORKFLOW_INVALID | UNKNOWN_CONDITION
        ))
    .then_some(annotation)
}

fn owner(owner: &Owner) -> &'static str {
    match owner {
        Owner::User => "the Task owner",
        Owner::ProjectAgent => "the Project Agent",
        Owner::Worker | Owner::Workflow | Owner::Scheduler => "the Project owner",
        Owner::Machine => "the machine owner",
    }
}

fn action(action: &Action) -> &'static str {
    match action {
        Action::EditWorkflow => "edit the Project workflow or move the Task to a state it defines",
        Action::ReconcileEntry => {
            "move the Task back to the previous state and forward again, or cancel it"
        }
        Action::AssignRole => "assign the role",
        _ => "move or cancel the Task",
    }
}

/// The annotation a park is shown as, for the parks that name a Task nothing
/// else owns. Every other park leaves the legacy fields as they are.
fn visible(park: &Park) -> Option<(&'static str, String)> {
    let (reason, what) = match &park.reason {
        Reason::WorkflowInvalid { state, cause } => (
            WORKFLOW_INVALID,
            format!("Nothing can continue this Task from `{state}`: {cause}"),
        ),
        // Only the two owners whose absence strands a Task. A hold the
        // stored condition merely fails to label is already visible in the
        // legacy fields that cause it.
        Reason::UnknownCondition { owner }
            if owner == super::next_step::PUBLICATION_OWNER
                || owner == super::next_step::ENTRY_HOOKS_OWNER =>
        {
            (
                UNKNOWN_CONDITION,
                format!("Nothing owns this Task: no recorded owner for {owner}"),
            )
        }
        _ => return None,
    };
    Some((
        reason,
        format!(
            "{what}. Owner: {}. Action: {}.",
            owner(&park.owner),
            action(&park.recovery)
        ),
    ))
}

impl TaskDispatcher {
    /// Bring the visible annotation in line with the park the Task resolved
    /// to. Returns whether the Task row changed. Writes only on a change, and
    /// never over an annotation something else wrote.
    pub(super) async fn sync_legacy_park(&self, task: &Task, park: Option<&Park>) -> Result<bool> {
        let wanted = park.and_then(visible);
        let current = stored(task);
        let annotation = match (&wanted, &current) {
            (None, None) => return Ok(false),
            (Some(_), None) if task.error_annotation.is_some() => return Ok(false),
            (Some((reason, message)), Some(current))
                if current.blocking_reason == *reason
                    && current.message.as_deref() == Some(message.as_str()) =>
            {
                return Ok(false)
            }
            (Some((reason, message)), _) => Some(
                serde_json::to_string(&TaskBlockingAnnotation {
                    annotation_type: FailureKind::WorkflowGuardRejected,
                    blocking_reason: (*reason).to_owned(),
                    blocked_by: Some(dispatcher()),
                    blocked_at: Some(db::now_rfc3339()),
                    blocked_execution_id: None,
                    artifact: None,
                    message: Some(message.clone()),
                    hook: None,
                })
                .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
            ),
            (None, Some(_)) => None,
        };
        match TaskRepo::update_status(
            &*self.db,
            UpdateTaskStatus {
                id: task.id.clone(),
                expected_version: task.version,
                status: task.status.clone(),
                assignee_id: None,
                error_annotation: Some(annotation),
                blocked_json: None,
                failed_json: None,
                updated_at: db::now_rfc3339(),
            },
        )
        .await
        {
            Ok(_) => {
                if let Some((reason, message)) = &wanted {
                    tracing::warn!(task_id = %task.id, reason, %message, "Task parked for its owner");
                }
                Ok(true)
            }
            // Something else wrote the Task first: the next pass decides.
            Err(db::DbError::VersionConflict | db::DbError::TaskVersionConflict { .. }) => {
                Ok(false)
            }
            Err(error) => Err(error.into()),
        }
    }
}
