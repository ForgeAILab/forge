use std::{future::pending, path::Path, sync::Arc};

use api_types::{Actor, SystemComponent};
use async_trait::async_trait;
use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, AgentRepo, AgentStatus,
    CreateAgent, CreateProject, CreateRepo, CreateTask, CreateTaskRoleAssignment, CreateWorkspace,
    DaemonRepo, DaemonStatus, ExecutionRepo, ExecutionStatus, PageRequest, RepoRepo, ResumePolicy,
    ReviewRepo, ReviewStatus, SortBy, SortOrder, StopReason, TaskRepo, TaskRoleAssignmentRepo,
    TransitionLogRepo, UpdateDaemonReport, UpdateProject, UpdateTask, UpsertDaemon,
    WorkspaceLeaseRepo, WorkspaceRepo, WorkspaceStatus,
};
use executors::{ExecutionContext, ExecutionResult, ExecutorError, TaskExecutor};
use tempfile::TempDir;
use tokio::sync::mpsc;
use workspace::RepoCacheLockManager;

use crate::{deferred_dispatch, ServiceError};
use db::TaskStepRepo;

use super::*;
#[path = "tests/environment_placement.rs"]
mod environment_placement;
mod project_wait_tests;

#[path = "tests/environment_parity.rs"]
mod environment_parity;

#[path = "tests/audit_23a.rs"]
mod audit_23a;

#[path = "tests/reaudit_23a.rs"]
mod reaudit_23a;

#[path = "tests/equivalence.rs"]
mod equivalence;
#[path = "tests/lead_check.rs"]
mod lead_check;
#[path = "tests/lead_operations.rs"]
mod lead_operations;
#[cfg(test)]
#[path = "tests/refresh_recovery.rs"]
mod refresh_recovery;
#[path = "tests/rerun_hardening.rs"]
mod rerun_hardening;
#[path = "tests/review_candidate.rs"]
mod review_candidate;
#[path = "tests/root_lead.rs"]
mod root_lead;
#[path = "tests/root_replan.rs"]
mod root_replan;
#[path = "tests/scheduler.rs"]
mod scheduler;

#[path = "tests/support.rs"]
mod support;
pub(super) use support::FixtureWorker;
use support::*;
#[path = "tests/capacity_recovery.rs"]
mod capacity_recovery;
#[path = "tests/charter_dispatch.rs"]
mod charter_dispatch;
#[path = "tests/dispatch_admission.rs"]
mod dispatch_admission;
#[path = "tests/environment_recheck.rs"]
mod environment_recheck;
#[path = "tests/execution_recovery.rs"]
mod execution_recovery;
#[path = "tests/execution_resume.rs"]
mod execution_resume;
#[path = "tests/failed_review_recovery.rs"]
mod failed_review_recovery;
#[path = "tests/hook_restart.rs"]
mod hook_restart;
#[path = "tests/machine_admission.rs"]
mod machine_admission;
#[path = "tests/machine_recovery.rs"]
mod machine_recovery;
#[path = "tests/machine_waits.rs"]
mod machine_waits;
#[path = "tests/merge_recovery.rs"]
mod merge_recovery;
#[path = "tests/planning_gates.rs"]
mod planning_gates;
#[path = "tests/project_capacity.rs"]
mod project_capacity;
#[path = "tests/repository_readiness.rs"]
mod repository_readiness;
#[path = "tests/review_recovery.rs"]
mod review_recovery;
#[path = "tests/role_defaults.rs"]
mod role_defaults;
#[path = "tests/scheduler_bookkeeping.rs"]
mod scheduler_bookkeeping;
