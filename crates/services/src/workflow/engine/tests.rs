//! Workflow engine lifecycle, entry hooks, and cascade tests.
use std::{path::PathBuf, sync::Arc};

use api_types::{
    FailurePolicy, GateConfig, HookAudience, HookResultEntry, HookSpec, StateDefinition,
    StateHooks, StateKind, WorkflowDefinition, WorkflowTrigger, WorkflowTriggerDefinition,
};
use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, CreateProject, CreateRepo,
    CreateTask, CreateTaskRoleAssignment, ProjectRepo, RepoRepo, SqliteDb, TaskRepo,
    TaskRoleAssignmentRepo, TaskStepRepo, TransitionLogRepo, UpdateProject, WorkspaceRepo,
};
use events::{EventBus, ForgeEvent};
use serde_json::json;
use tempfile::TempDir;
use tokio::sync::broadcast;

use super::WorkflowEngine;
use crate::{
    workflow::{default_roles, default_states, default_workflow},
    ServiceError,
};

use super::{
    annotate_dispatch_failure, annotate_upgrade_dispatch_refusal, clear_upgrade_dispatch_refusal,
    wake_upgraded_daemon_tasks, BoardMoveRequest, TransitionResult, WorkflowAuthority,
};

mod helpers;
use helpers::*;

mod sql_mutations;

#[path = "tests/audit_23a_worker.rs"]
mod audit_worker;

#[path = "tests/durable_23b.rs"]
mod durable_23b;
mod rerun_review;
mod root_replan_entry;

mod ci_triage;

mod no_change;
mod review_verify;

mod cascades;
mod custom_gates;
mod dispatch_failures;
mod lifecycle;
mod review_entry_budgets;
mod review_infrastructure;
mod user_overrides;
