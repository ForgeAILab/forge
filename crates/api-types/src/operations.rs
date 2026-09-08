use std::cmp::Ordering;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::{ActivityCounts, CostSummary, TokenCounters};

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum OperatorSeverity {
    Healthy,
    Attention,
    Blocked,
    Error,
}

impl OperatorSeverity {
    fn rank(&self) -> u8 {
        match self {
            Self::Healthy => 0,
            Self::Attention => 1,
            Self::Blocked => 2,
            Self::Error => 3,
        }
    }
}

impl fmt::Display for OperatorSeverity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Healthy => "healthy",
            Self::Attention => "attention",
            Self::Blocked => "blocked",
            Self::Error => "error",
        })
    }
}

impl FromStr for OperatorSeverity {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "healthy" => Ok(Self::Healthy),
            "attention" => Ok(Self::Attention),
            "blocked" => Ok(Self::Blocked),
            "error" => Ok(Self::Error),
            other => Err(format!("unknown operator severity: {other}")),
        }
    }
}

impl PartialOrd for OperatorSeverity {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OperatorSeverity {
    fn cmp(&self, other: &Self) -> Ordering {
        self.rank().cmp(&other.rank())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct OperatorStatusResponse {
    pub overall_severity: OperatorSeverity,
    pub active_executions: Vec<ActiveExecutionSummary>,
    pub blocked_tasks: Vec<BlockedTaskSummary>,
    pub daemon_issues: Vec<DaemonIssueSummary>,
    pub daemon_pressure: Vec<DaemonPressureSummary>,
    pub agent_pressure: Vec<AgentPressureSummary>,
    pub workspace_cleanup: Vec<WorkspaceCleanupSummary>,
    pub retry_pressure: Vec<RetryPressureSummary>,
    pub usage_summary: Option<UsageSummary>,
    pub recent_errors: Vec<RecentErrorSummary>,
    pub computed_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ActiveExecutionSummary {
    pub execution_id: String,
    pub task_id: String,
    pub task_title: Option<String>,
    pub role: String,
    pub agent_id: Option<String>,
    pub agent_name: Option<String>,
    pub daemon_id: Option<String>,
    pub workspace_id: Option<String>,
    pub workspace_path: Option<String>,
    pub session_id: Option<String>,
    pub started_at: String,
    pub runtime_seconds: f64,
    pub elapsed_seconds: f64,
    pub latest_event: Option<String>,
    pub last_event: Option<String>,
    pub last_event_time: Option<String>,
    pub turn_count: u32,
    pub token_totals: Option<TokenTotalsSummary>,
    #[ts(type = "Record<string, unknown> | null")]
    pub rate_limit_snapshot: Option<serde_json::Value>,
    pub effective_policy: Option<EffectiveExecutionPolicy>,
    pub plan_progress: Option<PlanProgressSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct BlockedTaskSummary {
    pub task_id: String,
    pub title: String,
    pub blocked_reason: Option<String>,
    pub blocked_since: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct DaemonIssueSummary {
    pub daemon_id: String,
    pub hostname: Option<String>,
    pub issue: String,
    pub severity: OperatorSeverity,
    pub detected_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct DaemonPressureSummary {
    pub daemon_id: String,
    pub hostname: Option<String>,
    pub active_sessions: u32,
    pub max_sessions: Option<u32>,
    pub at_capacity: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct AgentPressureSummary {
    pub agent_id: String,
    pub agent_name: String,
    pub daemon_id: Option<String>,
    pub active_sessions: u32,
    pub max_sessions: u32,
    pub at_capacity: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct WorkspaceCleanupSummary {
    pub workspace_id: String,
    pub task_id: String,
    pub worktree_path: Option<String>,
    pub cleanup_after: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct RetryPressureSummary {
    pub task_id: String,
    pub title: String,
    pub attempt_count: u32,
    pub max_attempts: Option<u32>,
    pub current_state: String,
    pub retry_reason: Option<String>,
    pub due_time: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct TokenTotalsSummary {
    pub tokens: TokenCounters,
    pub cost: CostSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct OperationsRefreshResponse {
    pub dispatched_tasks: u64,
    pub refreshed_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct UsageSummary {
    pub counts: ActivityCounts,
    pub tokens: TokenCounters,
    pub cost: CostSummary,
    pub active_execution_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct RecentErrorSummary {
    pub entity_type: String,
    pub entity_id: String,
    pub error: String,
    pub occurred_at: String,
    pub severity: OperatorSeverity,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct PlanArtifactDetail {
    pub items: Vec<PlanChecklistItem>,
    pub warnings: Vec<String>,
    pub source_path: Option<String>,
    pub last_modified: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct PlanChecklistItem {
    pub checked: bool,
    pub label: String,
    pub nesting_level: u32,
    pub line_number: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct EffectiveExecutionPolicy {
    pub executor_kind: String,
    pub permission_policy: String,
    pub isolation_posture: String,
    pub is_high_risk: bool,
    pub effective_cwd: Option<String>,
    pub workspace_root: Option<String>,
    pub environment_posture: String,
    pub scoped_tools: Vec<String>,
    pub mcp_servers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct PlanProgressSummary {
    pub total: u32,
    pub completed: u32,
    pub remaining: u32,
    pub available: bool,
    pub warnings: Vec<String>,
}
