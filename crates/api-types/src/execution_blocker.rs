//! Canonical execution-blocker projection (design D17).
//!
//! Project execution setup, Task detail/banner, chat context, phase
//! controls, dispatcher logs, and activity history all explain why
//! repository execution cannot proceed. Before this type existed, every one
//! of those surfaces derived its own copy from raw enums, and the same
//! blocked Task was shown as "Waiting for plan reconciliation," "waiting for
//! plan approval," and "TASK NOT STARTED" at once (F12) even though the Task
//! already had executions and a commit. `ExecutionBlockerProjection` is the
//! one server-owned answer: surfaces may adapt layout, but they render this
//! projection's copy and evidence rather than reinterpreting the blocker.

use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::RetryAction;

/// The Define -> Plan -> Build -> Review -> Release stage a blocker belongs
/// to. This is navigation/explanation only; it is never a second workflow or
/// authority store (D17).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ExecutionBlockerStage {
    Define,
    Plan,
    Build,
    Review,
    Release,
}

/// Whether a blocker widens the whole Project's execution gate or attaches
/// only to the named affected record(s) (D16). Only a Project-wide
/// governing-truth conflict may take the `Project` scope; a Task-,
/// plan-item-, or milestone-scoped conflict must never widen the Project
/// gate for unrelated work.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ExecutionBlockerScope {
    Project,
    Task,
    PlanItem,
    Milestone,
}

/// The principal class expected to clear this blocker.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ExecutionBlockerPrincipal {
    User,
    Worker,
    IndependentReviewer,
    ProjectAgent,
    System,
}

/// The closed set of blocker classifications. Every surface renders the
/// server-authored `headline`/`safe_explanation` that accompanies a code; it
/// must not synthesize its own copy from the code alone.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ExecutionBlockerCode {
    CoordinationSetupRequired,
    RepositorySetupRequired,
    WorkerAssignmentRequired,
    IndependentReviewerAssignmentRequired,
    ProvisioningInProgress,
    ProvisioningFailed,
    PreBaselineReadOnly,
    BaselineApprovalRequired,
    ReconciliationRequired,
    InvalidActiveBaseline,
    ProjectionUnavailable,
}

/// An exact record a blocker refers to (the affected Task/plan item/
/// milestone, or the governing baseline/reconciliation record).
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ExecutionBlockerRecordRef {
    pub record_type: String,
    pub record_id: String,
    pub label: Option<String>,
}

/// Progress derived only from canonical attempt/execution/commit history
/// (D17, F12). `not_started` can never be reported once at least one
/// execution has been attempted or a commit exists, and this value is never
/// derived from — or overridden by — an unrelated gate/blocker state.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ExecutionProgress {
    NotStarted,
    ImplementationAttempted,
    ImplementationCommitted,
}

/// Canonical execution evidence for one Task, shared by every progress-
/// language surface so a committed result can never regress to "not
/// started" copy on a different screen (F12).
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ExecutionEvidenceSummary {
    pub attempt_count: i64,
    pub execution_count: i64,
    pub has_commit: bool,
    pub latest_commit_sha: Option<String>,
    pub progress: ExecutionProgress,
    /// The exact canonical phrase for `progress`. Surfaces render this
    /// string directly instead of re-deriving their own from `progress` or
    /// from unrelated Task/gate state.
    pub progress_label: String,
}

impl ExecutionEvidenceSummary {
    #[must_use]
    pub fn from_counts(
        attempt_count: i64,
        execution_count: i64,
        has_commit: bool,
        latest_commit_sha: Option<String>,
    ) -> Self {
        let progress = if has_commit {
            ExecutionProgress::ImplementationCommitted
        } else if attempt_count > 0 || execution_count > 0 {
            ExecutionProgress::ImplementationAttempted
        } else {
            ExecutionProgress::NotStarted
        };
        let progress_label = match progress {
            ExecutionProgress::NotStarted => "Not started",
            ExecutionProgress::ImplementationAttempted => "Implementation attempted",
            ExecutionProgress::ImplementationCommitted => "Implementation committed",
        }
        .to_owned();
        Self {
            attempt_count,
            execution_count,
            has_commit,
            latest_commit_sha,
            progress,
            progress_label,
        }
    }
}

/// One server-owned execution blocker (D17). Every surface that explains why
/// execution cannot proceed — Project execution setup, Task detail/banner,
/// chat context, phase controls, dispatcher logs, and activity history —
/// renders exactly this projection. Surfaces may adapt layout but must not
/// reinterpret the blocker: exactly one `next_action` is ever offered for
/// it, and Cancel (when applicable) is a separate, always-available Task
/// control rather than a second gate-recovery option.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ExecutionBlockerProjection {
    pub code: ExecutionBlockerCode,
    pub stage: ExecutionBlockerStage,
    pub scope: ExecutionBlockerScope,
    /// The exact record(s) this blocker attaches to. For `scope: project`
    /// this is empty or names the Project itself; for a scoped blocker it
    /// names exactly the affected Task/plan item/milestone.
    pub affected_refs: Vec<ExecutionBlockerRecordRef>,
    /// The authoritative record this blocker is measured against (for
    /// example the governing execution-baseline revision).
    pub governing_ref: Option<ExecutionBlockerRecordRef>,
    pub headline: String,
    pub safe_explanation: String,
    /// Present only when the blocker is scoped to (or evaluated against) a
    /// specific Task's own attempt/commit history.
    pub evidence: Option<ExecutionEvidenceSummary>,
    pub required_principal: ExecutionBlockerPrincipal,
    /// Exactly one permitted next action. A phase control must never offer a
    /// second option that only re-enters this same blocker.
    pub next_action: RetryAction,
    pub blocker_digest: String,
    pub observed_version: i64,
}
