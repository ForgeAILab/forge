use serde::{Deserialize, Serialize};
use ts_rs::TS;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum PlacementFilterCode {
    OwnerUnreachable,
    DaemonUpgradeRequired,
    WorkspaceProtocolMissing,
    LocationNotReady,
    ExecutorUnavailable,
    CapabilityMissing,
    PinMismatch,
    AgentCapacity,
    MachineCapacity,
    /// The machine's workspace filesystem is under its free-space floor and
    /// this Task has no worktree there yet.
    DiskPressure,
    NativeBackendUnsupported,
    RunPurposeDenied,
    NotVisible,
    EnvironmentNotReady,
    EnvironmentProbePending,
    EnvironmentUnverified,
    ProvisionFailed,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum RepoLocationOwnerKind {
    Server,
    Daemon,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum RepoLocationKind {
    PrimaryCheckout,
    ManagedClone,
    SharedMount,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum RepoLocationStatus {
    Unverified,
    Ready,
    Unavailable,
    Invalid,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct RepoLocationResponse {
    pub id: String,
    pub repo_id: String,
    pub owner_kind: RepoLocationOwnerKind,
    pub daemon_id: Option<String>,
    pub runtime_id: Option<String>,
    pub path: String,
    pub kind: RepoLocationKind,
    pub is_default: bool,
    pub status: RepoLocationStatus,
    pub last_verified_at: Option<String>,
    pub last_error: Option<String>,
    #[ts(type = "number")]
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct CreateRepoLocationRequest {
    pub owner_kind: RepoLocationOwnerKind,
    #[ts(optional = nullable)]
    pub daemon_id: Option<String>,
    #[ts(optional = nullable)]
    pub runtime_id: Option<String>,
    pub path: String,
    pub kind: RepoLocationKind,
    #[serde(default)]
    #[ts(optional = nullable)]
    pub is_default: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct UpdateRepoLocationRequest {
    #[ts(type = "number")]
    pub version: i64,
    pub is_default: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct VerifyRepoLocationRequest {
    #[ts(type = "number")]
    pub version: i64,
}

#[test]
#[ignore = "manual repository location type export for the web client"]
fn export_repo_location_typescript() {
    let out_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../web/src/types/generated/bindings");
    CreateRepoLocationRequest::export_all_to(&out_dir).expect("export CreateRepoLocationRequest");
    RepoLocationResponse::export_all_to(&out_dir).expect("export RepoLocationResponse");
    UpdateRepoLocationRequest::export_all_to(&out_dir).expect("export UpdateRepoLocationRequest");
    VerifyRepoLocationRequest::export_all_to(&out_dir).expect("export VerifyRepoLocationRequest");
}
