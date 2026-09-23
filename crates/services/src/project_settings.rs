//! One validator for a Project's settings document.
//!
//! REST and MCP each grew their own copy of this, and the copies drifted
//! apart in both directions: REST checked that an assigned agent is usable in
//! the Project and that an assigned user is a member, while MCP checked
//! neither; MCP checked that a script lifecycle hook declares a usable
//! timeout, while REST did not. A settings document accepted through one
//! surface could therefore be one the other would have refused — on a
//! Project the two surfaces share.
//!
//! This is the union of both, in one place. Surfaces map the error into
//! whatever shape they return.

use std::collections::HashSet;

use api_types::{LifecycleEvent, LifecycleHookDef, ProjectSettings, WorkflowDefinition};
use db::{ProjectMemberRepo, SqliteDb};
use serde_json::Value;

use crate::{Result, ServiceError};

/// The assignee id a legacy default assignment carries to mean "a person
/// picks this up". It names no account, so membership cannot be checked
/// against it, and Projects created before real membership records still
/// carry it.
const LEGACY_MANUAL_ASSIGNEE: &str = "human";

/// Validates a Project settings document against its workflow.
///
/// `project_id` and `user_id` unlock the checks that need the database: an
/// agent must be usable in this Project, and a user assignee must be a member
/// of it. A caller that has neither — validating a document before the
/// Project exists — still gets every structural check.
pub async fn validate_project_settings(
    db: &SqliteDb,
    settings: &Value,
    workflow: &WorkflowDefinition,
    project_id: Option<&str>,
    user_id: Option<&str>,
) -> Result<()> {
    let settings: ProjectSettings = serde_json::from_value(settings.clone())
        .map_err(|error| ServiceError::invalid_operation(format!("invalid settings: {error}")))?;
    let role_names: HashSet<&str> = workflow
        .roles
        .iter()
        .map(|role| role.name.as_str())
        .collect();

    for assignment in &settings.default_role_assignments {
        if !role_names.contains(assignment.role_name.as_str()) {
            return Err(ServiceError::invalid_operation(format!(
                "unknown role: {}",
                assignment.role_name
            )));
        }
        let assignee_id = assignment
            .assignee_id
            .as_ref()
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                ServiceError::invalid_operation(format!(
                    "default role assignment for role '{}' requires assignee_id",
                    assignment.role_name
                ))
            })?;

        match assignment.assignee_type.as_str() {
            "agent" => {
                if let (Some(project_id), Some(user_id)) = (project_id, user_id) {
                    let usable = db
                        .list_agents_usable_in_project(project_id, user_id)
                        .await?
                        .into_iter()
                        .any(|agent| agent.id == assignee_id);
                    if !usable {
                        return Err(ServiceError::invalid_operation(
                            "agent not usable in this project",
                        ));
                    }
                }
            }
            "user" => {
                if assignee_id == LEGACY_MANUAL_ASSIGNEE {
                    continue;
                }
                if let Some(project_id) = project_id {
                    if ProjectMemberRepo::get_member(db, project_id, assignee_id)
                        .await?
                        .is_none()
                    {
                        return Err(ServiceError::invalid_operation(
                            "assignee must be a project member",
                        ));
                    }
                }
            }
            other => {
                return Err(ServiceError::invalid_operation(format!(
                    "default role assignment for role '{}' must use assignee_type 'agent' or \
                     'user', not '{other}'",
                    assignment.role_name
                )));
            }
        }
    }

    for (name, value) in [
        ("review", settings.retry_budgets.review),
        ("merge_fix", settings.retry_budgets.merge_fix),
    ] {
        if value.is_some_and(|value| value < 0) {
            return Err(ServiceError::invalid_operation(format!(
                "retry_budgets.{name} must be 0 or greater"
            )));
        }
    }

    for (event, hooks) in &settings.lifecycle_hooks {
        for hook in hooks {
            let LifecycleHookDef::Script {
                blocking,
                timeout_seconds,
                ..
            } = hook
            else {
                continue;
            };
            if *blocking && *event != LifecycleEvent::BeforeWork {
                return Err(ServiceError::invalid_operation(
                    "blocking lifecycle hooks are only supported for before_work",
                ));
            }
            if *timeout_seconds < 1 {
                return Err(ServiceError::invalid_operation(
                    "script lifecycle hooks require timeout_seconds to be at least 1",
                ));
            }
        }
    }

    Ok(())
}
