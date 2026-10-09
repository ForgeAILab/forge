//! Owner execution evidence, independent of check storage and consumer authority.
use crate::CheckEnvironmentIdentity;
use serde::{Deserialize, Serialize};
use ts_rs::TS;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct CheckOwnerIdentity {
    pub owner_kind: String,
    pub machine_id: Option<String>,
    pub runtime_id: String,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum CheckExecutionOutcome {
    Passed,
    Failed,
    TimedOut,
    Cancelled,
    Infrastructure,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum CheckCleanupOutcome {
    NotPerformed,
    Success,
    Failed,
    TimedOut,
    Uncertain,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct CheckCommandReceipt {
    pub id: String,
    pub command: String,
    pub exit_code: Option<i32>,
    pub outcome: CheckExecutionOutcome,
    pub duration_ms: u64,
    pub stdout_tail: String,
    pub stderr_tail: String,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
    /// The group was stopped before this receipt was returned.
    pub process_tree_stopped: bool,
    pub started_at: String,
    pub finished_at: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct CheckCleanupReceipt {
    pub outcome: CheckCleanupOutcome,
    pub commands: Vec<CheckCommandReceipt>,
    pub checkout_removed: bool,
    pub message: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct CheckReceipt {
    pub operation_id: String,
    pub owner: CheckOwnerIdentity,
    pub execution_inputs: CheckEnvironmentIdentity,
    pub commands: Vec<CheckCommandReceipt>,
    pub outcome: CheckExecutionOutcome,
    pub cleanup: CheckCleanupReceipt,
    pub prepared_head: Option<String>,
    pub finished_head: Option<String>,
    pub tracked_changes: Option<bool>,
    pub started_at: String,
    pub finished_at: String,
    pub infrastructure_message: Option<String>,
}
