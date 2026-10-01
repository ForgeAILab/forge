//! Server-issued review inputs and the reviewer response contract.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use ts_rs::TS;

pub const REVIEW_CONFORMANCE_POLICY: &str = "forge.review-conformance/3";
pub const REVIEW_SOURCE_DIGEST_VERSION: u32 = 2;

/// Verify existing v1 contracts against precisely the source they originally
/// fingerprinted. This algorithm is retained only for immutable data at rest;
/// new contracts use the scoped review-authority fingerprint.
fn legacy_v1_source_digest(source: &Value) -> Result<String, String> {
    crate::canonical_digest(source).map_err(|error| error.to_string())
}

/// Fingerprint review authority independently of prompt context and audit trail.
pub fn review_source_digest(source: &Value, version: u32) -> Result<String, String> {
    match version {
        1 => return legacy_v1_source_digest(source),
        REVIEW_SOURCE_DIGEST_VERSION => {}
        _ => {
            return Err(format!(
                "unsupported review source digest version: {version}"
            ))
        }
    }
    let config = effective_review_config(source)?;
    let state = review_state(source);
    let state_name = state
        .and_then(|state| state["name"].as_str())
        .unwrap_or("review");
    let mut environment_names: Vec<_> = source
        .pointer("/project_settings/environment/env")
        .and_then(Value::as_object)
        .into_iter()
        .flat_map(|env| env.keys())
        .collect();
    environment_names.sort_unstable();
    let documents: Vec<_> = source["documents"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|document| {
            serde_json::json!({
                "revision_id": document["id"],
                "content_digest": document["content_digest"],
            })
        })
        .collect();
    let array = |key: &str| {
        config
            .get(key)
            .cloned()
            .unwrap_or_else(|| serde_json::json!([]))
    };
    let timeout = config
        .get("check_timeout_seconds")
        .filter(|value| !value.is_null())
        .cloned()
        // Match the review runner's default of 30 minutes.
        .unwrap_or_else(|| serde_json::json!(30 * 60));
    let authority = serde_json::json!({
        "charter": {
            "revision_id": source["charter"]["id"],
            "content_digest": source["charter"]["content_digest"],
            "task_revision_id": source["task_charter_revision_id"],
        },
        "task": {
            "title": source["task_scope"]["title"],
            "description": source["task_scope"]["description"],
            // The resolver explicitly includes the plan in task:acceptance.
            "plan": source["task_scope"]["plan"],
            "requirement_ids": array("requirement_ids"),
            "allocations": source["task_scope"]["allocations"],
            "read_only": task_scope_is_read_only(source),
        },
        "documents": documents,
        "review_config": {
            "setup_steps": array("setup_steps"),
            "ci_steps": array("ci_steps"),
            "check_timeout_seconds": timeout,
            "conformance_checks": array("conformance_checks"),
        },
        "review_state": {
            "config": state.and_then(|state| state.get("config")),
            "task_config": source["task_scope"]["config"].get(state_name),
        },
        "project_environment_names": environment_names,
    });
    crate::canonical_digest_with_schema("forge.review-source/2", &authority)
        .map_err(|error| error.to_string())
}

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
    /// Source fingerprint algorithm. Missing means v1 for contracts at rest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub source_digest_version: Option<u32>,
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

fn review_state(source: &Value) -> Option<&Value> {
    source
        .pointer("/workflow/states")
        .and_then(Value::as_array)
        .and_then(|states| {
            states
                .iter()
                .find(|state| state["role"] == "reviewer" || state["name"] == "review")
        })
}

/// Resolve workflow defaults, Project defaults, then Task review overrides.
/// Shared by check execution and source fingerprinting so their policies agree.
pub fn effective_review_config(source: &Value) -> Result<Value, String> {
    let state = review_state(source);
    let state_name = state
        .and_then(|state| state["name"].as_str())
        .unwrap_or("review");
    let mut merged = serde_json::Map::new();
    if let Some(config) = state.and_then(|state| state.get("config")) {
        if !config.is_null() {
            let config = config
                .as_object()
                .ok_or("review workflow state config must be an object")?;
            merged.extend(config.clone());
        }
    }
    if let Some(defaults) = source.pointer("/project_settings/default_review_config") {
        if !defaults.is_null() {
            let defaults = defaults
                .as_object()
                .ok_or("project default review config must be an object")?;
            for (key, value) in defaults {
                merged.entry(key.clone()).or_insert_with(|| value.clone());
            }
        }
    }
    if let Some(task_config) = source.pointer("/task_scope/config") {
        if !task_config.is_null() {
            let task_config = task_config
                .as_object()
                .ok_or("Task review config must be an object")?;
            if let Some(overrides) = task_config.get(state_name) {
                if !overrides.is_null() {
                    let overrides = overrides
                        .as_object()
                        .ok_or("Task review state config must be an object")?;
                    for (key, value) in overrides {
                        merged.insert(key.clone(), value.clone());
                    }
                }
            }
        }
    }
    if task_scope_is_read_only(source) {
        merged.remove("ci_steps");
        merged.remove("setup_steps");
    }
    Ok(Value::Object(merged))
}

/// Whether the server-owned Task kind or capability forbids repository writes.
/// Review contracts use the same persisted inputs as execution admission so a
/// Project's implementation CI defaults do not become requirements for a
/// discovery or planning Task.
#[must_use]
pub fn task_scope_is_read_only(source: &Value) -> bool {
    matches!(
        source
            .pointer("/task_scope/task_type")
            .and_then(Value::as_str),
        Some("planning_task" | "discovery")
    ) || matches!(
        source
            .pointer("/task_scope/capability_class")
            .and_then(Value::as_str),
        Some("repository_read" | "read_only" | "discovery_read" | "planning_read")
    )
}
