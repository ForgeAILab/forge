use crate::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, run_migrations_from,
    validate_uuid_v4, AgentContextScopeRepo, AgentListQuery, AgentProfileRepo, AgentRepo,
    AgentSessionRepo, AgentStatus, AgentTaskListQuery, ArchiveTask, ClaimExecutionLease, ClaimTask,
    CompareAndMoveTask, CreateAgent, CreateAgentContextScope, CreateAgentIdentity,
    CreateAgentProfile, CreateAgentSession, CreateDomainEvent, CreateExecution, CreateProject,
    CreateProjectAgentBinding, CreateProjectCharter, CreateProjectCharterRevision,
    CreateProjectCharterRevisionAtomically, CreateProjectMember,
    CreateProviderAuthorizationOperation, CreateRepo, CreateReview, CreateSkill, CreateTask,
    CreateTaskRoleAssignment, CreateTerminalSession, CreateTransitionLog, CreateWorkspace,
    CreateWorkspaceLease, CredentialHandleRepo, DaemonRepo, DaemonStatus, DbError, DomainEventRepo,
    ExecutionAdmission, ExecutionLeaseDisposition, ExecutionLeaseMutation,
    ExecutionProgressWarningOutcome, ExecutionRepo, ExecutionStatus, MemoryAccessQuery,
    MemoryConfidence, MemoryGetQuery, MemoryItem, MemoryKind, MemoryRepository, MemoryScopeGrant,
    MemorySourceType, MoveTaskIdentity, MoveTaskPersistence, NotificationListQuery,
    NotificationRepo, PageRequest, ProjectAgentBindingRepo, ProjectMemberRepo,
    ProjectOrchestrationRepo, ProjectRepo, ProviderAuthorizationRepo, RecordExecutionProgress,
    RenewExecutionLease, RepoRepo, ReviewConformanceRepo, ReviewRepo, ReviewStatus,
    RotateAgentSession, ScopedMemoryRepository, SelectAgentProfile, SkillRepo, SortBy, SortOrder,
    SqliteDb, Task, TaskBoardRepo, TaskDependencyRepo, TaskListQuery, TaskRepo,
    TaskRoleAssignmentRepo, TerminalSessionRepo, TerminalSessionStatus, TerminalizeExecution,
    TransitionLogRepo, UpdateAgent, UpdateExecution, UpdateProject,
    UpdateProviderAuthorizationOperation, UpdateRepo, UpdateSkill, UpdateTask, UpdateTaskStatus,
    UpdateTerminalSessionStatus, UpsertDaemon, WorkspaceLeaseRepo, WorkspaceRepo, WorkspaceStatus,
};
use crate::{RefreshToken, RefreshTokenRepo, User, UserRepo};
use api_types::{CanonicalPhase, StateDefinition, StateHooks, StateKind, WorkflowDefinition};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};

mod helpers;
use helpers::*;

mod agents;
mod claim_capacity;
mod dependencies;
mod domain_events;
mod execution_admission;
mod execution_leases;
mod memory;
mod migrations;
mod notifications;
mod project_lifecycle;
mod repositories;
mod repository_lifecycle;
mod review_admission;
mod review_contracts;
mod task_board;
mod task_conditions;
mod task_lists;
mod terminal_sessions;
mod usage;
mod users;
mod workflow_admission;
mod workspaces;
