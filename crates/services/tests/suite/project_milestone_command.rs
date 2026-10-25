//! Gate A acceptance coverage for the Project milestone/readiness/release
//! command composites.
//!
//! The Project Agent materializer is still a legacy adapter for these three
//! operation families.  These tests therefore enter at the shared repository
//! command boundary, where the command contracts are already available, and
//! keep the transport gap explicit rather than duplicating the adapter's raw
//! SQL in another test.

use std::sync::Arc;

use api_types::{
    AcceptanceCheckSourceKind, AcceptanceEvidenceRequirement, AuthorizationProvenance,
    MilestoneAcceptanceCheck, MilestoneDefinitionContent, MilestoneDefinitionLifecycle,
    PrincipalKind, PrincipalRef, RevisionProvenance,
};
use db::{
    create_sqlite_pool, run_migrations, AgentActionExecutionStatus, AgentActionPolicyResult,
    AgentActionRepo, AgentActionStatus, AgentRepo, CommandReceiptRepo, CreateAgentAction,
    CreateAgentActionExecution, CreateAgentIdentity, CreateAgentProfile, CreateCommandReceipt,
    CreateDomainEvent, CreateProject, CreateProjectMilestone, CreateProjectMilestoneCommand,
    CreateProjectMilestoneRevision, CreateProjectReadinessSnapshot,
    CreateProjectReadinessSnapshotCommand, CreateProjectReleaseRequest,
    CreateProjectReleaseRequestCommand, DbError, DomainEventRepo, ProjectOrchestrationRepo,
    ProjectReadinessSnapshotRecord, ProjectRepo, SqliteDb, User, UserRepo,
};
use forge_agent_host::{
    PROJECT_MILESTONE_OPERATION, PROJECT_READINESS_OPERATION, PROJECT_RELEASE_OPERATION,
};
use serde_json::{json, Value};
use services::{
    MilestoneRuntime, ProjectCommandAuthorization, ProjectMilestoneCommandService,
    ProjectMilestoneDefinitionCommand, ProjectPrimaryMilestoneCommand,
    ProjectReadinessRequestCommand, ProjectReleaseRequestCommand, ServiceError,
};

#[path = "project_milestone_command/definitions.rs"]
mod definitions;
#[path = "project_milestone_command/helpers.rs"]
mod helpers;
#[path = "project_milestone_command/readiness.rs"]
mod readiness;
#[path = "project_milestone_command/rollback.rs"]
mod rollback;

use helpers::*;
