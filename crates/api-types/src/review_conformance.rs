//! Server-issued review inputs and the reviewer response contract.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use ts_rs::TS;

pub const REVIEW_CONFORMANCE_POLICY: &str = "forge.review-conformance/3";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, TS)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ReviewRequirement {
    pub id: String,
    pub source: String,
    pub text: String,
    pub universal: bool,
    pub allocated_task_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, TS)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ConformanceCheck {
    pub id: String,
    pub command: String,
    pub requirement_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, TS)]
#[ts(export)]
pub struct ReviewGoverningContext {
    pub project_id: String,
    pub task_id: String,
    pub repo_id: Option<String>,
    pub charter_revision_id: Option<String>,
    pub charter_digest: Option<String>,
    #[ts(type = "unknown")]
    pub charter: Option<Value>,
    #[ts(type = "unknown")]
    pub task_scope: Value,
    #[ts(type = "unknown[]")]
    pub linked_documents: Vec<Value>,
    /// Requirements this Task review must disposition. Project-wide
    /// requirements that are not assigned to this Task are tracked by the
    /// deferred count/digest and remain milestone-readiness obligations.
    pub requirements: Vec<ReviewRequirement>,
    #[serde(default)]
    pub deferred_requirement_count: usize,
    #[serde(default)]
    pub deferred_requirements_digest: Option<String>,
    /// Commands used to prepare the Task worktree before required
    /// conformance checks execute.
    #[serde(default)]
    pub setup_steps: Vec<String>,
    pub required_checks: Vec<ConformanceCheck>,
    /// Per-command limit for the setup steps and required checks. Absent
    /// (the default) keeps contracts frozen before the field existed equal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub check_timeout_seconds: Option<u32>,
    pub source_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, TS)]
#[ts(export)]
pub struct ReviewContract {
    pub execution_id: String,
    pub policy: String,
    pub commit_sha: String,
    pub base_sha: String,
    /// Exact repository-relative paths changed by `base_sha..commit_sha`.
    /// Blocking file evidence must be attributable to this candidate delta.
    #[serde(default)]
    pub candidate_changed_paths: Vec<String>,
    pub context: ReviewGoverningContext,
    /// Results Forge recorded before dispatching the reviewer. These are
    /// immutable reviewer inputs; conformance admission reruns required checks
    /// before accepting the assessment.
    #[serde(default)]
    pub check_results: Vec<ConformanceCheckResult>,
    pub digest: String,
}

/// The reviewer's one-word outcome.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, TS)]
#[ts(export)]
#[serde(rename_all = "snake_case")]
pub enum ReviewResult {
    Pass,
    Fail,
    /// The review environment, not the candidate, prevented a verdict (for
    /// example the toolchain or dependencies are missing). Routed to the
    /// owner instead of the coder.
    Blocked,
}

/// What Forge keeps from a reviewer's reply: the result block it ended with
/// and the Markdown review written before it.
///
/// The reply is free Markdown ending in one small JSON object
/// `{"result": "...", "reason": "..."}`. Unknown keys in that object are
/// ignored so any model can produce an acceptable review; the hard guarantee
/// comes from the required checks Forge runs itself, not from the reviewer's
/// citations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, TS)]
#[ts(export)]
pub struct ReviewAssessment {
    pub result: ReviewResult,
    pub reason: String,
    /// The reviewer's Markdown review, without the result block.
    #[serde(default)]
    pub report: String,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq, TS)]
#[ts(export)]
#[serde(rename_all = "snake_case")]
pub enum ConformanceStatus {
    #[default]
    NotAssessed,
    Passed,
    Failed,
    /// The reviewer reported that the review environment prevented a
    /// verdict. The owner resolves it; the coder is not asked to remediate.
    Blocked,
    Unverified,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, TS)]
#[ts(export)]
pub struct ConformanceCheckResult {
    pub check_id: String,
    pub command: String,
    pub exit_code: i32,
    pub output: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, TS)]
#[ts(export)]
pub struct ReviewConformance {
    pub status: ConformanceStatus,
    pub contract: Option<ReviewContract>,
    pub assessment: Option<ReviewAssessment>,
    pub checks: Vec<ConformanceCheckResult>,
    pub reason: Option<String>,
}
