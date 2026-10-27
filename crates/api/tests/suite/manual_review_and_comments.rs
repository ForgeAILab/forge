#![allow(dead_code, clippy::assertions_on_constants)]
use crate::common;

use std::{
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use api::{build_router, AppState};
use api_types::{
    AuthorType, CommentResponse, PaginatedResponse, ProjectResponse, RepoResponse, TaskResponse,
};
use axum::{
    body::{to_bytes, Body},
    http::{Method, Request, StatusCode},
    Router,
};
use db::{
    new_uuid_v4, now_rfc3339, AgentRepo, AgentStatus, AssigneeKind, CommentAuthorType, CreateAgent,
    CreateExecution, CreateProject, CreateRepo, CreateReview, CreateTask, CreateTaskComment,
    CreateTaskRoleAssignment, CreateTransitionLog, CreateWorkspace, DaemonRepo, DaemonStatus,
    ExecutionRepo, ExecutionStatus, ProjectRepo, RepoRepo, ReviewRepo, ReviewStatus,
    TaskCommentRepo, TaskRepo, TaskRoleAssignmentRepo, TransitionLogRepo, UpdateProject,
    UpdateTask, UpsertDaemon, WorkspaceStatus,
};
use events::EventBus;
use executors::{
    AvailabilityInfo, AvailabilityStatus, CodingExecutorAdapter, DiscoverContext,
    DiscoveredOptions, ExecutionContext, ExecutionOutcome, ExecutionResult, ExecutorError,
    ExecutorKind,
};
use serde::de::DeserializeOwned;
use serde_json::json;
use std::process::Stdio;
use tower::ServiceExt;

use helpers::*;

#[path = "manual_review_and_comments/comments.rs"]
mod comments;
#[path = "manual_review_and_comments/helpers.rs"]
mod helpers;
#[path = "manual_review_and_comments/human_submission.rs"]
mod human_submission;
#[path = "manual_review_and_comments/owner_review.rs"]
mod owner_review;
#[path = "manual_review_and_comments/review_retry.rs"]
mod review_retry;
