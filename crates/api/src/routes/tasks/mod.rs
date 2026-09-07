use api_types::{
    AddDependencyRequest, ApproveGateRequest, AssignRoleRequest, AuthorType, CanonicalPhase,
    ClaimTaskRequest, CommentResponse, CreateCommentRequest, CreateTaskRequest, DiffEnvelope,
    HookResultEntry, LaunchExecutionRequest, LaunchExecutionResponse, MoveTaskRequest,
    MoveTaskResponse, PaginatedResponse, PromptPreviewResponse, RecoverTaskRequest,
    RejectGateRequest, RejectReviewRequest, ReorderSubtasksRequest, ReviewConfig,
    ReviewDecisionResponse, StateKind, TaskAction, TaskActionRequest, TaskDependency,
    TaskMediaResponse, TaskResponse, TaskRoleAssignmentResponse, TasksResponse, TransitionLogEntry,
    TransitionSource, TransitionTaskRequest, TransitionTaskResponse, UpdateTaskRequest,
    WorkflowDefinition, WorkflowTrigger, WorkspaceResponse,
};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use db::{
    now_rfc3339, CommentAuthorType, CreateTaskRoleAssignment, ExecutionRepo, ExecutionStatus,
    PageRequest, ProjectRepo, ReviewRepo, ReviewStatus, SharedMediaRepo, SortBy, SortOrder,
    TaskBoardRepo, TaskCommentRepo, TaskDependencyRepo, TaskListQuery, TaskMediaRepo, TaskRepo,
    TaskRoleAssignmentRepo, TransitionLogRepo, WorkspaceRepo,
};
use executors::ExecutionOverrides;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use services::{
    task_service::TransitionOptions,
    workflow::{default_states, engine::WorkflowEngine},
    Assignee, DiffService, ServiceError,
};
use uuid::Uuid;

use crate::{
    errors::{ApiError, ApiResult},
    routes::{
        execution_response, paginated, parse_csv, review_response, serialize_json,
        task_page_request, task_response, task_response_light_with_latest,
        task_response_with_awaiting_human, task_role_assignment_response, workspace_response,
        ListParams,
    },
    state::AppState,
};

mod actions;
mod comments;
mod crud;
mod dependencies;
mod execution;
mod gates;
mod media;
mod prompt_preview;
mod reviews;
mod roles;
mod transitions;
mod workspace;

pub use actions::{
    approve_task, cancel_task, list_task_actions, pause_task, request_changes_task, resume_task,
    start_task, submit_task,
};
pub use comments::{create_comment, delete_comment, list_comments};
pub use crud::{
    advance_task, archive_task, create_task, delete_task, duplicate_task, get_task, list_tasks,
    move_task, recover_task, reorder_subtasks, update_task,
};
pub use dependencies::{add_dependency, list_dependencies, list_dependents, remove_dependency};
pub use execution::{claim_task, launch_task};
pub use gates::{approve_gate, reject_gate};
pub use media::{delete_media, get_media, list_media, upload_media};
pub use prompt_preview::prompt_preview;
pub use reviews::{approve_review, list_reviews, reject_review, trigger_review};
pub use roles::{
    assign_task_role, list_task_roles, remove_task_role, RoleResetRequest,
    TaskRoleAssignmentListResponse,
};
pub use transitions::{list_transitions, transition_task, TransitionLogListResponse};
pub use workspace::{get_task_diff, get_task_workspace, reset_task_workspace};

async fn project_default_review_config(
    db: &db::SqliteDb,
    project_id: &str,
) -> ApiResult<Option<ReviewConfig>> {
    let project = ProjectRepo::get_by_id(db, project_id)
        .await?
        .ok_or_else(|| ApiError::not_found("project", project_id.to_owned()))?;
    let settings: serde_json::Value = serde_json::from_str(&project.settings)
        .map_err(|error| ApiError::bad_request(format!("invalid settings: {error}")))?;
    Ok(settings
        .get("default_review_config")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok()))
}

fn map_launch_error(error: ServiceError) -> ApiError {
    match error {
        ServiceError::InvalidOperation { message } => {
            if message.contains("terminal status") {
                ApiError::conflict_with_code("task.terminal", message)
            } else if message.contains("interactive execution already running") {
                ApiError::conflict_with_code("execution.already_running", message)
            } else {
                ApiError::invalid_operation_conflict(message)
            }
        }
        other => ApiError::from(other),
    }
}

fn map_diff_error(error: ServiceError) -> ApiError {
    match error {
        ServiceError::NotFound { entity, id } if entity == "workspace" => {
            ApiError::not_found_with_code("workspace.not_found", entity, id)
        }
        ServiceError::InvalidOperation { message } if message.contains("error state") => {
            ApiError::conflict_with_code("workspace.error_state", message)
        }
        other => ApiError::from(other),
    }
}

fn map_manual_review_error(error: ServiceError) -> ApiError {
    match error {
        ServiceError::InvalidOperation { message } => ApiError::invalid_operation_conflict(message),
        other => ApiError::from(other),
    }
}

fn comment_response(comment: db::TaskComment) -> CommentResponse {
    CommentResponse {
        id: comment.id,
        task_id: comment.task_id,
        author_type: match comment.author_type {
            CommentAuthorType::User => AuthorType::User,
            CommentAuthorType::Agent => AuthorType::Agent,
            CommentAuthorType::System => AuthorType::System,
        },
        author_id: comment.author_id,
        author_name: comment.author_name,
        content: comment.content,
        created_at: comment.created_at,
        updated_at: comment.updated_at,
    }
}

fn media_response(media: db::TaskMedia) -> TaskMediaResponse {
    TaskMediaResponse {
        url: format!("/api/v1/media/{}", media.id),
        id: media.id,
        task_id: media.task_id,
        filename: media.display_filename,
        content_type: media.content_type,
        byte_size: media.byte_size,
        author_type: match media.author_type {
            CommentAuthorType::User => AuthorType::User,
            CommentAuthorType::Agent => AuthorType::Agent,
            CommentAuthorType::System => AuthorType::System,
        },
        author_id: media.author_id,
        author_name: media.author_name,
        created_at: media.created_at,
    }
}

fn transition_log_entry(entry: db::TransitionLog) -> ApiResult<TransitionLogEntry> {
    let hook_results_json = entry
        .hook_results_json
        .as_deref()
        .filter(|json| !json.trim().is_empty())
        .map(serde_json::from_str::<Vec<HookResultEntry>>)
        .transpose()
        .map_err(|error| ApiError::internal(format!("invalid transition hook results: {error}")))?
        .unwrap_or_default();

    Ok(TransitionLogEntry {
        id: entry.id,
        task_id: entry.task_id,
        from_state: entry.from_state,
        to_state: entry.to_state,
        triggered_by: entry.triggered_by,
        trigger_reason: entry.trigger_reason,
        hook_results_json,
        rejection: entry.rejection,
        created_at: entry.created_at,
    })
}
