//! Gate A / task 2.11 acceptance coverage for the native composition boundary.
//!
//! These tests intentionally invoke the public `ScopeToolComposition` tools,
//! rather than calling `CoordinationToolProvider` directly.  The provider is
//! still real and backed by SQLite; the composition is what proves that the
//! Main, Project, and Task operation registries deliver the same structured
//! outcome contract to the runtime.

use crate::test_services as services;
use std::{collections::BTreeSet, sync::Arc};

use agent_runtime::core::{
    cancel::Cancellation,
    clock::{Deadline, SystemClock},
    ids::{RequestId, SessionId, ToolCallId},
    prelude::{InvocationContext, PreparationContext, RuntimeError, ToolOutcome},
    workspace::DenyAllWorkspace,
};
use agent_runtime::harness::{FetchTransport, FETCH_TOOL_NAME};
use api_types::WorkflowTrigger;
use db::{
    create_sqlite_pool, run_migrations, AgentRepo, AgentStatus, CreateAgentIdentity,
    CreateAgentProfile, CreateProject, CreateRepo, ProjectRepo, RepoRepo, SqliteDb, UpdateProject,
    User, UserRepo,
};
use events::EventBus;
use forge_agent_host::{
    CanonicalScope, CanonicalScopeType, ForgeFetchTransport, ProjectChatToolContext,
    ScopeToolComposition, ScopeToolRuntime, WorkspaceAccess, FORGE_MAIN_ORCHESTRATION_PROPOSE_TOOL,
    FORGE_MAIN_ORCHESTRATION_READ_TOOL, FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL,
    FORGE_PROJECT_ORCHESTRATION_READ_TOOL, MAIN_CHARTER_APPROVAL_TARGET_OPERATION,
    MAIN_CHARTER_DIFF_OPERATION, MAIN_CHARTER_DRAFT_OPERATION, MAIN_CHARTER_READINESS_OPERATION,
    MAIN_CHARTER_READ_OPERATION, MAIN_GENESIS_PROJECT_AGENTS_READ_OPERATION,
    MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION, MAIN_GENESIS_START_OPERATION,
    MAIN_INQUIRY_RUN_OPERATION, MAIN_PROJECT_CREATE_OPERATION, MIGRATED_OPERATION_CONTRACTS,
    PROJECT_CHARTER_ADOPTION_OPERATION, PROJECT_CHARTER_READ_OPERATION,
    PROJECT_CURRENT_STATE_OPERATION, PROJECT_DECISION_OPERATION, PROJECT_DOCUMENT_OPERATION,
    PROJECT_EVIDENCE_OPERATION, PROJECT_MILESTONE_OPERATION, PROJECT_OBSERVATIONS_OPERATION,
    PROJECT_READINESS_OPERATION, PROJECT_RELEASE_OPERATION, PROJECT_REVIEW_CONFIG_OPERATION,
    PROJECT_SKILL_SECTION_OPERATION, PROJECT_VALIDATION_OPERATION, TASK_ACTION_OPERATION,
    TASK_ADAPTIVE_OPERATION, TASK_DEPENDENCY_OPERATION, TASK_EVIDENCE_OPERATION,
    TASK_PLAN_OPERATION, TASK_PROPOSE_OPERATION, TASK_WORKLOG_OPERATION,
};
use serde_json::{json, Value};
use services::{CoordinationToolProvider, TaskService};

#[path = "../../../services/tests/common/pre_change_main_runtime.rs"]
mod pre_change_main_runtime;

#[path = "native_scope_composition/helpers.rs"]
mod helpers;
use helpers::*;
#[path = "native_scope_composition/main_proposals.rs"]
mod main_proposals;
#[path = "native_scope_composition/operation_matrix.rs"]
mod operation_matrix;
#[path = "native_scope_composition/outcomes.rs"]
mod outcomes;
#[path = "native_scope_composition/receipt_replay.rs"]
mod receipt_replay;
#[path = "native_scope_composition/registry_reads.rs"]
mod registry_reads;
