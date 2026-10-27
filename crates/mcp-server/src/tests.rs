use std::{future::Future, sync::Arc};

use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, Agent, AgentChatMessageListQuery,
    AgentChatMessageRepo, AgentChatRepo, AgentChatTurnJobRepo, AgentHandoffRepo, AgentRepo,
    AgentStatus, AssigneeKind, CostCoverageReasonCode, CreateAgent, CreateAgentChatMessage,
    CreateAgentIdentity, CreateAgentProfile, CreateExecution, CreatePricingSelection,
    CreateProject, CreateProjectMember, CreateRepo, CreateTask, CreateTaskRoleAssignment,
    CreateUsageEvent, CreateUsageInvocation, CreateWorkspace, DaemonRepo, DaemonStatus,
    ExecutionRepo, ExecutionStatus, PageRequest, PricingAdmissionProvenanceKind, PricingDomainKind,
    PricingSelectionStatus, ProjectAgentBindingRepo, ProjectMemberRepo, ProjectRepo, RepoRepo,
    SortBy, SortOrder, SqliteDb, StartUsageInvocation, Task, TaskDependencyRepo, TaskRepo,
    TaskRoleAssignmentRepo, UpdateDaemonReport, UpdateProject, UpsertDaemon, UsageCostKind,
    UsageEventProvenanceKind, UsageEventReportMode, UsageLedgerRepo, UsageLedgerSettlement,
    UsageSurface, UsageTelemetryState, UserRepo, WorkspaceRepo, WorkspaceStatus,
};
use events::EventBus;
use serde_json::{json, Value};
use services::ServiceError;
use services_agents::{SetMainAgentBindingInput, SetProjectAgentBindingInput};

use crate::{
    error::McpToolError,
    protocol::McpContext,
    rpc::{dispatch, dispatch_with_context},
    AppState,
};

#[path = "tests/project_contract.rs"]
mod project_contract;

#[path = "tests/registry_contract.rs"]
mod registry_contract;

use helpers::*;

#[path = "tests/agents.rs"]
mod agents;
#[path = "tests/catalog.rs"]
mod catalog;
#[path = "tests/chat.rs"]
mod chat;
#[path = "tests/helpers.rs"]
mod helpers;
#[path = "tests/projects.rs"]
mod projects;
#[path = "tests/rpc_errors.rs"]
mod rpc_errors;
#[path = "tests/scope.rs"]
mod scope;
#[path = "tests/task_mutations.rs"]
mod task_mutations;
#[path = "tests/task_reads_and_actions.rs"]
mod task_reads_and_actions;

#[path = "tests/authorization.rs"]
mod authorization;
