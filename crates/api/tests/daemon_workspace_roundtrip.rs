mod common;

use std::{
    collections::{BTreeMap, HashSet},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use api_types::*;
use axum::http::{Method, StatusCode};
use db::{
    AssigneeKind, CreateExecution, CreateProject, CreateRepo, CreateRepoLocation, CreateReview,
    CreateTask, CreateTaskRoleAssignment, CreateWorkspace, CreateWorkspacePlacement, ExecutionRepo,
    ExecutionStatus, PlacementOwnerKind, PlacementSelectedBy, PlacementState, ProjectRepo,
    RepoLocationKind, RepoLocationOwnerKind, RepoLocationRepo, RepoLocationStatus, RepoRepo,
    ReviewRepo, ReviewStatus, RuntimeRepo, TaskRepo, TaskRoleAssignmentRepo,
    UpdateWorkspacePlacement, WorkspacePlacementRepo, WorkspaceRepo, WorkspaceStatus,
};
use forge_client::{
    daemon_persistence::{JournalEntry, JournalOperation},
    daemon_runtime::{ActiveExecutionTracker, DaemonRuntime},
};
use futures_util::{SinkExt, StreamExt};
use review::{CommandLimits, ReviewWorkspace};
use serde_json::{json, Value};
use services::workspace_backend::{PrepareSpec, ResolvedWorkspace, RunSpec, WorkspaceBackendError};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, http::HeaderValue, Message},
};

use common::{fake_daemon::TestServer, Harness, TestDir};

use fixtures::*;
use helpers::*;

#[path = "daemon_workspace_roundtrip/cancellation_and_reconnect.rs"]
mod cancellation_and_reconnect;
#[path = "daemon_workspace_roundtrip/cleanup_and_runs.rs"]
mod cleanup_and_runs;
#[path = "daemon_workspace_roundtrip/cross_owner_recovery.rs"]
mod cross_owner_recovery;
#[path = "daemon_workspace_roundtrip/fixtures.rs"]
mod fixtures;
#[path = "daemon_workspace_roundtrip/helpers.rs"]
mod helpers;
#[path = "daemon_workspace_roundtrip/integration_protocol.rs"]
mod integration_protocol;
#[path = "daemon_workspace_roundtrip/merge_cancellation.rs"]
mod merge_cancellation;
#[path = "daemon_workspace_roundtrip/planning_and_outages.rs"]
mod planning_and_outages;
#[path = "daemon_workspace_roundtrip/queue_owners.rs"]
mod queue_owners;
#[path = "daemon_workspace_roundtrip/queue_upgrade.rs"]
mod queue_upgrade;
#[path = "daemon_workspace_roundtrip/queue_worker.rs"]
mod queue_worker;
#[path = "daemon_workspace_roundtrip/review_and_rebase.rs"]
mod review_and_rebase;
