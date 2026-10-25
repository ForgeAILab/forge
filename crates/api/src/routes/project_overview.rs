//! Read-only Project Overview projection.
//!
//! The Overview is deliberately assembled from the canonical V076 records at
//! request time.  It is not a second source of truth and it never derives
//! authority from chat text, Task prose, or a client supplied Project id.  The
//! first database reads are the Project visibility checks; only after those
//! checks do we touch Charter, milestone, Task, evidence, or release rows.

use api_types::{DocumentFreshnessStatus, ProjectNextAction, ProjectOverview};
use axum::{
    extract::{Path, State},
    Json,
};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use sqlx::Row;
use std::collections::HashMap;

use crate::{
    errors::{ApiError, ApiResult},
    routes::{
        auth::AuthenticatedUser,
        authorization::{require_project_access, ProjectAccess},
    },
    state::AppState,
};

#[cfg(test)]
mod classify_status_tests;
mod next_action;

/// Parity between `next_action()` and a real service/route/UI target
/// (task 8.1.8, finding F10).
///
/// `NextActionCard` in the web app renders every `route_or_operation` as
/// plain informational text and otherwise sends the user to Project Agent
/// chat, except for `action_kind == "release"` (which links to the
/// readiness section instead). An operation therefore only has a genuine
/// executable target when either:
///
/// - `required_principal` is `project_agent`/`worker`, in which case chat
///   (or ordinary Task/Worker execution) IS the real target because that
///   principal already has its own typed command for the domain action, or
/// - `required_principal` is `user`, in which case the operation must name
///   a real REST-backed command -- reachable through some dedicated control
///   elsewhere in the product, not necessarily this banner -- because no
///   chat agent may act on the user's behalf here.
///
/// `project.reconciliation.resolve` is the literal F10 regression: before
/// this task it named no handler anywhere. `resolve_project_reconciliation`
/// is referenced by path below so renaming or deleting that handler fails
/// this module to compile rather than silently reintroducing a dead action.
#[cfg(test)]
mod next_action_parity_tests;
mod projections;

use next_action::next_action;
use projections::{
    load_active_milestones, load_check_summary, load_current_acceptance_checks,
    load_current_charter, load_decisions, load_document_freshness, load_evidence,
    load_failed_task_count, load_latest_readiness, load_pending_decisions,
    load_reconciliation_required, load_releases, load_task_counts, load_watermark,
};

/// Return the current, authorization-bound Project Overview.
pub async fn get_project_overview(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(project_id): Path<String>,
) -> ApiResult<Json<ProjectOverview>> {
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
    // Authorization is intentionally before every orchestration-table query.
    // A non-member receives the same not-found response as an unknown Project
    // and cannot probe Charter, milestone, media, or release identifiers.
    let project =
        require_project_access(&state, &project_id, &user.user_id, ProjectAccess::Read).await?;

    let mut stale = false;
    let (current_charter, vision, charter_stale) = load_current_charter(&state, &project).await?;
    stale |= charter_stale;

    let (active_milestones, milestone_stale) = load_active_milestones(&state, &project.id).await?;
    stale |= milestone_stale;

    let task_counts = load_task_counts(&state, &project, None).await?;
    let check_summary = load_check_summary(&state, &project.id, None).await?;
    let (milestone_evidence, evidence_stale) = load_evidence(&state, &project.id).await?;
    stale |= evidence_stale;
    let (document_freshness, document_projection_stale) =
        load_document_freshness(&state, &project.id).await?;
    stale |= document_projection_stale;
    let pending_decisions = load_pending_decisions(&state, &project.id).await?;
    let pending_decision_ids: Vec<String> = pending_decisions
        .iter()
        .filter_map(|value| value.get("id").and_then(Value::as_str))
        .map(str::to_owned)
        .collect();
    let decisions = load_decisions(&state, &project.id).await?;
    let (releases, releases_stale) = load_releases(&state, &project.id).await?;
    stale |= releases_stale;
    let watermark = load_watermark(&state, &project.id, project.project_work_epoch).await?;

    // Charter setup is a starting state, not a projection fault.  A Project
    // that has simply never adopted a Charter reports that through
    // `charter_state` and the `charter_adoption` next action; raising the
    // generic stale banner on top of those would say the Overview cannot be
    // trusted when in fact it is exactly current -- there is nothing yet to
    // govern.  A missing Charter only makes the projection unprovable in the
    // two cases below.
    //
    // 1. The Project record claims a Charter that cannot be loaded.
    let charter_pointer_broken =
        project.charter_status == "charter_backed" && current_charter.is_none();
    let charter_state = if project.charter_setup_required || charter_pointer_broken {
        stale |= charter_pointer_broken;
        "charter_setup_required"
    } else {
        match project.charter_status.as_str() {
            "charter_backed" => "approved",
            "legacy_unverified" => "legacy_unverified",
            _ => {
                // An uninterpretable Charter status is a real record fault.
                stale = true;
                "charter_setup_required"
            }
        }
    };
    // 2. The Project already carries governed records -- milestones, evidence,
    //    or releases -- that only an approved Charter could have authorized.
    //    Those cannot be shown as current release truth without it.
    if current_charter.is_none()
        && (!active_milestones.is_empty() || !milestone_evidence.is_empty() || !releases.is_empty())
    {
        stale = true;
    }

    let execution_setup = services::load_project_execution_setup(&state.db, &project.id)
        .await
        .map_err(|_| ApiError::internal("Project execution setup projection is unavailable"))?;
    let milestone_runtime = services::MilestoneRuntime::new(state.db.clone());
    let failed_task_count = load_failed_task_count(&state, &project.id).await?;
    let reconciliation_required = load_reconciliation_required(&state, &project.id).await?;

    let mut milestone_overviews = Vec::with_capacity(active_milestones.len());
    for (milestone, definition) in active_milestones {
        let milestone_id = milestone
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let counts = load_task_counts(&state, &project, Some(&milestone_id)).await?;
        let checks = load_check_summary(&state, &project.id, Some(&milestone_id)).await?;
        let definition_revision_id = milestone
            .get("definition_revision_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let (latest_readiness, mut readiness_stale) = load_latest_readiness(
            &state,
            &project.id,
            &milestone_id,
            definition_revision_id,
            &milestone_evidence,
        )
        .await?;
        let readiness_freshness = if let Some(snapshot) = latest_readiness.as_ref() {
            let snapshot_id = snapshot
                .get("id")
                .and_then(Value::as_str)
                .ok_or_else(|| ApiError::internal("Readiness snapshot id is missing"))?;
            let freshness = milestone_runtime
                .readiness_freshness(&project.id, &milestone_id, snapshot_id)
                .await
                .map_err(|_| {
                    ApiError::internal("Readiness freshness is temporarily unavailable")
                })?;
            readiness_stale |= freshness.status != api_types::ReadinessFreshnessStatus::Current;
            Some(serde_json::to_value(freshness).map_err(|_| {
                ApiError::internal("Readiness freshness projection is temporarily unavailable")
            })?)
        } else {
            None
        };
        stale |= readiness_stale;
        let evidence = milestone_evidence
            .iter()
            .filter(|item| {
                item.get("milestone_id")
                    .and_then(Value::as_str)
                    .is_some_and(|id| id == milestone_id)
            })
            .cloned()
            .collect::<Vec<_>>();

        milestone_overviews.push(json!({
            "milestone": milestone,
            "definition": definition,
            "task_counts": counts,
            "check_summary": checks,
            "current_checks": load_current_acceptance_checks(
                &state,
                &project.id,
                &milestone_id,
                definition_revision_id,
            ).await?,
            "latest_readiness": latest_readiness,
            "readiness_freshness": readiness_freshness,
            "evidence": evidence,
        }));
    }

    let projection_state = if stale { "stale" } else { "current" };
    let next_action = next_action(NextActionContext {
        project_id: &project.id,
        project_version: project.version,
        charter_setup_required: project.charter_setup_required,
        no_milestones: milestone_overviews.is_empty(),
        execution_setup: &execution_setup,
        milestones: &milestone_overviews,
        documents: &document_freshness,
        releases: &releases,
        pending_decision_ids: &pending_decision_ids,
        task_counts: &task_counts,
        failed_task_count,
        checks: &check_summary,
        reconciliation_required,
        stale,
    });
    let generated_at = db::now_rfc3339();

    let overview = serde_json::from_value::<ProjectOverview>(json!({
        "project_id": project.id,
        "project_name": project.name,
        "vision": vision,
        "charter_state": charter_state,
        "current_charter": current_charter,
        "primary_milestone_id": project.primary_milestone_id,
        "active_milestones": milestone_overviews,
        "task_counts": task_counts,
        "check_summary": check_summary,
        "pending_decisions": pending_decisions,
        "decisions": decisions,
        "risks": current_charter
            .as_ref()
            .and_then(|value| value.pointer("/content/constraints_and_risks/risks"))
            .cloned()
            .unwrap_or_else(|| json!([])),
        "document_freshness": document_freshness,
        "evidence": milestone_evidence,
        "releases": releases,
        "next_action": next_action,
        "projection_state": projection_state,
        "source_event_watermark": watermark,
        "generated_at": generated_at,
        "execution_setup": execution_setup,
    }))
    .map_err(|error| {
        tracing::error!(error = %error, "invalid Project Overview projection");
        ApiError::internal("Project Overview projection is temporarily unavailable")
    })?;

    Ok(Json(overview))
}

struct NextActionContext<'a> {
    project_id: &'a str,
    project_version: i64,
    charter_setup_required: bool,
    no_milestones: bool,
    execution_setup: &'a api_types::ProjectExecutionSetupResponse,
    milestones: &'a [Value],
    documents: &'a [Value],
    releases: &'a [Value],
    pending_decision_ids: &'a [String],
    task_counts: &'a Value,
    failed_task_count: i64,
    checks: &'a Value,
    reconciliation_required: bool,
    stale: bool,
}

#[derive(Default)]
struct Counts {
    total: i64,
    backlog: i64,
    active: i64,
    review: i64,
    terminal: i64,
    blocked: i64,
}

#[derive(Default)]
struct CheckSummary {
    required_total: i64,
    passed: i64,
    failed: i64,
    missing: i64,
    stale: i64,
    waived: i64,
    unavailable: i64,
}

enum TaskBucket {
    Backlog,
    Active,
    Review,
    Terminal,
    Blocked,
}

fn classify_status(status: &str, workflow: &api_types::WorkflowDefinition) -> TaskBucket {
    if services::workflow::is_review_state(workflow, status) {
        return TaskBucket::Review;
    }
    let status = status.to_ascii_lowercase();
    if matches!(
        status.as_str(),
        "done" | "completed" | "cancelled" | "canceled" | "archived"
    ) {
        TaskBucket::Terminal
    } else if status == "blocked" || status.contains("blocked") {
        TaskBucket::Blocked
    } else if status.contains("review") || status.contains("merge") {
        TaskBucket::Review
    } else if matches!(
        status.as_str(),
        "todo" | "backlog" | "ready" | "pending" | "queued"
    ) {
        TaskBucket::Backlog
    } else {
        TaskBucket::Active
    }
}

fn document_kind(value: String) -> Option<&'static str> {
    match value.as_str() {
        "research" => Some("research"),
        "delivery_brief" => Some("delivery_brief"),
        "product_spec" => Some("product_spec"),
        "design" => Some("design"),
        "architecture" => Some("architecture"),
        "execution_plan" => Some("execution_plan"),
        _ => None,
    }
}

fn evidence_kind(value: Option<&str>) -> Option<&'static str> {
    match value? {
        "screenshot" => Some("screenshot"),
        "walkthrough_video" => Some("walkthrough_video"),
        "log" => Some("log"),
        "report" => Some("report"),
        "other" => Some("other"),
        _ => None,
    }
}

fn evidence_availability(value: &str) -> Option<&'static str> {
    match value {
        "available" => Some("available"),
        "quarantined" => Some("quarantined"),
        "redacted" => Some("redacted"),
        "purged" => Some("purged"),
        _ => None,
    }
}

fn principal_value(kind: &str, id: Option<&str>) -> Option<Value> {
    let kind = match kind {
        "user" => "user",
        // `task_lead`: a coordination root's lead, an Agent acting from its run.
        "agent" | "main_agent" | "project_agent" | "task_lead" => "agent",
        "worker" => "worker",
        "reviewer" => "reviewer",
        "service" => "service",
        "system" => "system",
        _ => return None,
    };
    let principal_id = match (kind, id.filter(|value| !value.is_empty())) {
        ("system", None) => "system",
        (_, Some(id)) => id,
        _ => return None,
    };
    Some(json!({
        "kind": kind,
        "id": principal_id,
        "display_name": Value::Null,
    }))
}

fn parse_release_refs<T: DeserializeOwned>(value: &str) -> Option<Vec<T>> {
    let parsed: Value = serde_json::from_str(value).ok()?;
    if !parsed.is_array() {
        return None;
    }
    serde_json::from_value(parsed).ok()
}

fn typed_json_array<T: DeserializeOwned>(
    row: &sqlx::sqlite::SqliteRow,
    column: &str,
) -> ApiResult<Vec<T>> {
    let value = row_json_array(row, column)?;
    serde_json::from_value(value).map_err(|_| invalid_persisted_field(column))
}

fn readiness_evidence_ids(value: &Value) -> ApiResult<Value> {
    if let Value::Array(items) = value {
        if items.iter().all(Value::is_string) {
            return Ok(value.clone());
        }
        return Err(invalid_persisted_field("evidence_manifest_json"));
    }
    let Value::Object(manifest) = value else {
        return Err(invalid_persisted_field("evidence_manifest_json"));
    };
    let attachment_ids = manifest
        .get("attachment_ids")
        .or_else(|| manifest.get("evidence_attachment_ids"))
        .or_else(|| manifest.get("ids"))
        .ok_or_else(|| invalid_persisted_field("evidence_manifest_json"))?;
    if !attachment_ids.is_array()
        || !attachment_ids
            .as_array()
            .is_some_and(|items| items.iter().all(Value::is_string))
    {
        return Err(invalid_persisted_field("evidence_manifest_json"));
    }
    Ok(attachment_ids.clone())
}

fn concat_json_arrays(left: Value, middle: Value, right: Value) -> Value {
    let mut output = Vec::new();
    for value in [left, middle, right] {
        if let Value::Array(items) = value {
            output.extend(items);
        }
    }
    Value::Array(output)
}

fn append_projection_reason(mut reasons: Value, reason: Value) -> Value {
    if let Value::Array(items) = &mut reasons {
        items.push(reason);
    }
    reasons
}

fn row_json(row: &sqlx::sqlite::SqliteRow, column: &str) -> ApiResult<Value> {
    let value = row.try_get::<String, _>(column).map_err(sql_error)?;
    serde_json::from_str(&value).map_err(|_| invalid_persisted_field(column))
}

fn row_json_array(row: &sqlx::sqlite::SqliteRow, column: &str) -> ApiResult<Value> {
    let value = row_json(row, column)?;
    if value.is_array() {
        Ok(value)
    } else {
        Err(invalid_persisted_field(column))
    }
}

fn row_json_array_from(row: &sqlx::sqlite::SqliteRow, column: &str) -> ApiResult<Value> {
    row_json_array(row, column)
}

fn string_array_from(row: &sqlx::sqlite::SqliteRow, column: &str) -> ApiResult<Value> {
    let value = row_json_array(row, column)?;
    if value
        .as_array()
        .is_some_and(|items| items.iter().all(Value::is_string))
    {
        Ok(value)
    } else {
        Err(invalid_persisted_field(column))
    }
}

fn invalid_persisted_field(column: &str) -> ApiError {
    tracing::error!(column, "invalid Project Overview persisted field");
    ApiError::internal("Project Overview contains invalid persisted data")
}

fn sql_error(error: sqlx::Error) -> ApiError {
    tracing::error!(error = %error, "Project Overview query failed");
    ApiError::internal("Project Overview is temporarily unavailable")
}
