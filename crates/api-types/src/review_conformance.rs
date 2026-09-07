//! Server-issued review inputs and the reviewer response contract.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use ts_rs::TS;

pub const REVIEW_CONFORMANCE_POLICY: &str = "forge.review-conformance/2";

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
    /// Commands used to prepare the detached clean checkout before required
    /// conformance checks execute.
    #[serde(default)]
    pub setup_steps: Vec<String>,
    pub required_checks: Vec<ConformanceCheck>,
    pub source_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, TS)]
#[ts(export)]
pub struct ReviewContract {
    pub execution_id: String,
    pub policy: String,
    pub commit_sha: String,
    pub base_sha: String,
    pub context: ReviewGoverningContext,
    /// Results Forge recorded before dispatching the reviewer. These are
    /// immutable reviewer inputs; conformance admission reruns required checks
    /// before accepting the assessment.
    #[serde(default)]
    pub check_results: Vec<ConformanceCheckResult>,
    pub digest: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, TS)]
#[ts(export)]
#[serde(rename_all = "snake_case")]
pub enum ConformanceVerdict {
    Pass,
    Fail,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, TS)]
#[ts(export)]
#[serde(rename_all = "snake_case")]
pub enum RequirementDisposition {
    Satisfied,
    Violated,
    Unverified,
    OutsideTaskScope,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, TS)]
#[ts(export)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[ts(tag = "kind", rename_all = "snake_case")]
pub enum ReviewEvidenceRef {
    File {
        path: String,
        commit_sha: String,
        start_line: usize,
        end_line: usize,
    },
    Check {
        check_id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, TS)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct RequirementAssessment {
    pub requirement_id: String,
    pub disposition: RequirementDisposition,
    pub rationale: String,
    pub evidence: Vec<ReviewEvidenceRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, TS)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ConformanceFinding {
    pub blocking: bool,
    pub expected: String,
    pub actual: String,
    pub evidence: Vec<ReviewEvidenceRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, TS)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ReviewAssessment {
    pub contract_digest: String,
    pub verdict: ConformanceVerdict,
    pub requirements: Vec<RequirementAssessment>,
    pub findings: Vec<ConformanceFinding>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq, TS)]
#[ts(export)]
#[serde(rename_all = "snake_case")]
pub enum ConformanceStatus {
    #[default]
    NotAssessed,
    Passed,
    Failed,
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
