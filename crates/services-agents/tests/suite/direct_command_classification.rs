//! Gate A / task 2.10 acceptance coverage for operation classification.
//!
//! These tests enter the same public provider boundary used by native Agent
//! Runtime tools.  They intentionally do not call an AgentAction materializer
//! directly: query operations must stay read-only, safe Project coordination
//! writes and `task.propose` must use a direct command receipt, and
//! consequential/denied operations must stop at the policy boundary.
use crate::test_services as services;
use std::sync::Arc;

use db::{
    create_sqlite_pool, run_migrations, AgentRepo, AgentStatus, CreateAgentIdentity,
    CreateAgentProfile, CreateProject, CreateRepo, ProjectRepo, RepoRepo, SqliteDb, UpdateProject,
    User, UserRepo,
};
use events::EventBus;
use forge_agent_host::{
    classify_operation, operation_descriptor, AgentHostError, CanonicalScope, CanonicalScopeType,
    ForgeToolProvider, OperationClassification, WorkspaceAccess, PROJECT_CURRENT_STATE_OPERATION,
    PROJECT_DOCUMENT_OPERATION, PROJECT_MILESTONE_OPERATION,
};
use serde_json::{json, Value};
use services::{CoordinationToolProvider, OrchestrationAuthorizationService, TaskService};
use sqlx::Row;

#[path = "direct_command_classification/helpers.rs"]
mod helpers;
use helpers::*;

#[path = "direct_command_classification/command_receipts.rs"]
mod command_receipts;
#[path = "direct_command_classification/native_denials.rs"]
mod native_denials;
