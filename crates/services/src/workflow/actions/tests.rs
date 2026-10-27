//! Workflow action review, dispatch, and merge tests.
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, AgentRepo, AgentStatus,
    CreateAgent, CreateExecution, CreateProject, CreateRepo, CreateReview, CreateTask,
    CreateTaskRoleAssignment, CreateWorkspace, DaemonRepo, DaemonStatus, ExecutionRepo,
    ExecutionStatus, PageRequest, ProjectRepo, RepoRepo, ReviewConformanceRepo, ReviewRepo,
    ReviewStatus, SortBy, SortOrder, SqliteDb, TaskRepo, TaskRoleAssignmentRepo,
    UpdateDaemonReport, UpdateProject, UpdateTaskStatus, UpsertDaemon, WorkspaceRepo,
    WorkspaceStatus,
};
use events::{EventBus, EventContext};
use executors::{ExecutionContext, ExecutionResult, ExecutorError, TaskExecutor};
use serde_json::json;
use tempfile::TempDir;
use tokio::sync::mpsc;
use workspace::RepoCacheLockManager;

use super::merge::{merge_failure_result, RunMerge};
use super::{
    AutoCascadeOnReviewPass, CheckRetryBudget, DependencyGate, DispatchRoleAgent, NotifyRoleHolder,
    RequireUpstreamRolesCompleted,
};
use crate::workflow::{
    default_roles, default_states, default_workflow, HookAction, HookContext, HookResult,
};

use super::{carry, common, merge, AutoCascadeOnUnconfiguredReview, CarryReviewAuthority};

mod helpers;
use helpers::*;

mod admission_guards;
mod merge_conflicts;
mod review_authority_carry;
mod review_checks;
mod reviewer_dispatch;
mod role_dispatch;
