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
use db::{ProjectMemberRepo, ProjectRepo};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use sqlx::Row;
use std::collections::HashMap;

use crate::{
    errors::{ApiError, ApiResult},
    routes::auth::AuthenticatedUser,
    state::AppState,
};

const NO_APPROVED_CHARTER_VISION: &str = "No approved Charter vision recorded.";

macro_rules! try_get {
    ($row:expr, $ty:ty, $column:expr) => {
        $row.try_get::<$ty, _>($column).map_err(sql_error)?
    };
}

/// Return the current, authorization-bound Project Overview.
pub async fn get_project_overview(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(project_id): Path<String>,
) -> ApiResult<Json<ProjectOverview>> {
    // Authorization is intentionally before every orchestration-table query.
    // A non-member receives the same not-found response as an unknown Project
    // and cannot probe Charter, milestone, media, or release identifiers.
    let project = ProjectRepo::get_by_id(&*state.db, &project_id)
        .await
        .map_err(db_error)?
        .ok_or_else(|| ApiError::not_found("project", project_id.clone()))?;
    let is_owner = project.owner_id.as_deref() == Some(user.user_id.as_str());
    if project.owner_id.is_some()
        && !is_owner
        && ProjectMemberRepo::get_member(&*state.db, &project.id, &user.user_id)
            .await
            .map_err(db_error)?
            .is_none()
    {
        return Err(ApiError::not_found("project", project_id));
    }

    let mut stale = false;
    let (current_charter, vision, charter_stale) = load_current_charter(&state, &project).await?;
    stale |= charter_stale;

    let (active_milestones, milestone_stale) = load_active_milestones(&state, &project.id).await?;
    stale |= milestone_stale;

    let task_counts = load_task_counts(&state, &project.id, None).await?;
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
        let counts = load_task_counts(&state, &project.id, Some(&milestone_id)).await?;
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

async fn load_current_charter(
    state: &AppState,
    project: &db::Project,
) -> ApiResult<(Option<Value>, String, bool)> {
    let Some(charter_id) = project.current_charter_id.as_deref() else {
        return Ok((None, NO_APPROVED_CHARTER_VISION.to_owned(), false));
    };
    let Some(revision_id) = project.current_charter_revision_id.as_deref() else {
        return Ok((None, NO_APPROVED_CHARTER_VISION.to_owned(), true));
    };

    let row = sqlx::query(
        "SELECT c.id AS charter_id, c.project_id, c.project_mode, c.maturity,
                r.id AS revision_id,
                r.revision, r.base_revision, r.base_revision_id,
                r.lifecycle, r.schema_version,
                r.render_version, r.content_json, r.rendered_view,
                r.change_summary, r.author_type, r.author_id,
                r.source_refs_json, r.content_digest, r.rendered_digest,
                r.created_at,
                (SELECT ca.created_at
                   FROM project_charter_approval ca
                  WHERE ca.charter_id = c.id
                    AND ca.revision_id = r.id
                    AND ca.lifecycle IN ('active', 'consumed')
                  ORDER BY ca.created_at DESC, ca.id DESC
                  LIMIT 1) AS approved_at
         FROM project_charter c
         JOIN project_charter_revision r ON r.id = ? AND r.charter_id = c.id
         WHERE c.id = ? AND c.project_id = ?
           AND c.current_approved_revision_id = r.id
           AND c.lifecycle = 'attached'
           AND r.lifecycle = 'approved'",
    )
    .bind(revision_id)
    .bind(charter_id)
    .bind(&project.id)
    .fetch_optional(state.db.pool())
    .await
    .map_err(sql_error)?;

    let Some(row) = row else {
        return Ok((None, NO_APPROVED_CHARTER_VISION.to_owned(), true));
    };

    let content: Value = row_json(&row, "content_json")?;
    let vision = content
        .pointer("/identity/one_line_vision")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(NO_APPROVED_CHARTER_VISION)
        .to_owned();
    let principal = principal_value(
        try_get!(row, String, "author_type").as_str(),
        try_get!(row, Option<String>, "author_id").as_deref(),
    )
    .ok_or_else(|| ApiError::internal("invalid Charter author principal"))?;
    let source_refs = row_json_array(&row, "source_refs_json")?;
    let content_digest = try_get!(row, String, "content_digest");
    let rendered_view = try_get!(row, String, "rendered_view");
    let render_version = try_get!(row, String, "render_version");
    let rendered_digest = try_get!(row, String, "rendered_digest");
    let approved_at = try_get!(row, Option<String>, "approved_at");
    let charter_stale = content_digest.trim().is_empty()
        || rendered_view.trim().is_empty()
        || render_version.trim().is_empty()
        || rendered_digest.trim().is_empty()
        || approved_at.is_none();
    let charter = json!({
        "id": try_get!(row, String, "revision_id"),
        "charter_id": try_get!(row, String, "charter_id"),
        "revision_number": try_get!(row, i64, "revision"),
        "base_revision_id": try_get!(row, Option<String>, "base_revision_id"),
        "lifecycle": "approved",
        "project_mode": try_get!(row, String, "project_mode"),
        "maturity": try_get!(row, String, "maturity"),
        "schema_version": try_get!(row, String, "schema_version"),
        "content": content,
        "rendered_view": rendered_view,
        "render_version": render_version,
        "content_digest": content_digest,
        "render_digest": rendered_digest,
        "provenance": {
            "author": principal,
            "profile_revision": Value::Null,
            "operating_skill_revision": Value::Null,
            "source_refs": source_refs,
            "change_summary": try_get!(row, String, "change_summary"),
            "material_diff": Value::Null,
        },
        "readiness": Value::Null,
        "approved_at": approved_at,
        "superseded_by_revision_id": Value::Null,
        "created_at": try_get!(row, String, "created_at"),
    });
    Ok((Some(charter), vision, charter_stale))
}

async fn load_active_milestones(
    state: &AppState,
    project_id: &str,
) -> ApiResult<(Vec<(Value, Value)>, bool)> {
    let rows = sqlx::query(
        "SELECT id, project_id, milestone_sequence, milestone_key,
                display_label, current_definition_revision_id, lifecycle,
                blocker_reason_json, stale_reason_json,
                reconciliation_reason_json, version, created_at, updated_at
         FROM project_milestone
         WHERE project_id = ?
           AND (
                lifecycle IN ('active', 'ready_for_release')
                OR id = (SELECT primary_milestone_id FROM project WHERE id = ?)
           )
         ORDER BY milestone_sequence ASC, id ASC",
    )
    .bind(project_id)
    .bind(project_id)
    .fetch_all(state.db.pool())
    .await
    .map_err(sql_error)?;

    let mut stale = false;
    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        let milestone_row_id = row.try_get::<String, _>("id").map_err(sql_error)?;
        let Some(revision_id) = row
            .try_get::<Option<String>, _>("current_definition_revision_id")
            .map_err(sql_error)?
        else {
            stale = true;
            continue;
        };
        let Some(revision) = sqlx::query(
            "SELECT mr.id, mr.milestone_id, mr.revision, mr.base_revision, mr.lifecycle,
                    mr.display_label, mr.outcome, mr.included_scope_json,
                    mr.excluded_scope_json, mr.charter_revision_id,
                    mr.document_revisions_json, mr.task_selection_json,
                    mr.dependencies_json, mr.risks_json, mr.acceptance_checks_json,
                    mr.evidence_requirements_json, mr.known_issues_json,
                    mr.change_summary, mr.schema_version, mr.render_version, mr.rendered_view,
                    mr.content_digest, mr.rendered_digest, mr.author_type, mr.author_id,
                    mr.source_refs_json, mr.created_at,
                    mr.base_revision_id AS base_revision_id,
                    cr.charter_id AS charter_id,
                    cr.content_digest AS charter_content_digest,
                    cr.render_version AS charter_render_version,
                    cr.rendered_digest AS charter_render_digest
             FROM project_milestone_revision mr
             LEFT JOIN project_charter_revision cr
               ON cr.id = mr.charter_revision_id
             WHERE mr.id = ? AND mr.milestone_id = ?",
        )
        .bind(&revision_id)
        .bind(&milestone_row_id)
        .fetch_optional(state.db.pool())
        .await
        .map_err(sql_error)?
        else {
            stale = true;
            continue;
        };

        let principal = match principal_value(
            try_get!(revision, String, "author_type").as_str(),
            try_get!(revision, Option<String>, "author_id").as_deref(),
        ) {
            Some(value) => value,
            None => {
                stale = true;
                continue;
            }
        };
        let charter_revision_id = try_get!(revision, Option<String>, "charter_revision_id");
        let charter_id = try_get!(revision, Option<String>, "charter_id");
        let charter_content_digest = try_get!(revision, Option<String>, "charter_content_digest");
        let charter_render_version = try_get!(revision, Option<String>, "charter_render_version");
        let charter_render_digest = try_get!(revision, Option<String>, "charter_render_digest");
        let charter_revision = match (
            charter_revision_id.as_deref(),
            charter_id.as_deref(),
            charter_content_digest.as_deref(),
            charter_render_version.as_deref(),
            charter_render_digest.as_deref(),
        ) {
            (None, None, None, None, None) => Value::Null,
            (
                Some(revision_id),
                Some(charter_id),
                Some(content_digest),
                Some(render_version),
                Some(render_digest),
            ) if !revision_id.trim().is_empty()
                && !charter_id.trim().is_empty()
                && !content_digest.trim().is_empty()
                && !render_version.trim().is_empty()
                && !render_digest.trim().is_empty() =>
            {
                json!({
                    "artifact_id": charter_id,
                    "revision_id": revision_id,
                    "content_digest": content_digest,
                    "render_version": render_version,
                    "render_digest": render_digest,
                })
            }
            _ => {
                stale = true;
                Value::Null
            }
        };
        let rendered_view = try_get!(revision, String, "rendered_view");
        if rendered_view.trim().is_empty() {
            stale = true;
        }
        if try_get!(revision, String, "content_digest")
            .trim()
            .is_empty()
            || try_get!(revision, String, "rendered_digest")
                .trim()
                .is_empty()
            || try_get!(revision, String, "render_version")
                .trim()
                .is_empty()
        {
            stale = true;
        }
        if try_get!(revision, String, "lifecycle") != "approved" {
            stale = true;
        }
        let definition_name = try_get!(revision, Option<String>, "display_label")
            .unwrap_or(try_get!(row, String, "milestone_key"));
        let definition_content = json!({
            "name": definition_name,
            "outcome": try_get!(revision, String, "outcome"),
            "included_scope": row_json_array_from(&revision, "included_scope_json")?,
            "excluded_scope": row_json_array_from(&revision, "excluded_scope_json")?,
            "charter_revision": charter_revision,
            "document_revisions": row_json_array_from(&revision, "document_revisions_json")?,
            "task_ids": row_json_array_from(&revision, "task_selection_json")?,
            "dependencies": row_json_array_from(&revision, "dependencies_json")?,
            "risks": row_json_array_from(&revision, "risks_json")?,
            "acceptance_checks": row_json_array_from(&revision, "acceptance_checks_json")?,
            "evidence_requirements": row_json_array_from(&revision, "evidence_requirements_json")?,
            "known_issues": row_json_array_from(&revision, "known_issues_json")?,
            "target_date": Value::Null,
        });
        let reasons = concat_json_arrays(
            row_json_array_from(&row, "blocker_reason_json")?,
            row_json_array_from(&row, "stale_reason_json")?,
            row_json_array_from(&row, "reconciliation_reason_json")?,
        );
        let reasons = if rendered_view.trim().is_empty() {
            append_projection_reason(
                reasons,
                json!({
                    "kind": "stale",
                    "code": "rendered_view_unavailable",
                    "message": "The milestone definition has no persisted rendered view.",
                    "source_ids": [revision_id.clone()],
                }),
            )
        } else {
            reasons
        };
        let milestone = json!({
            "id": try_get!(row, String, "id"),
            "project_id": try_get!(row, String, "project_id"),
            "milestone_sequence": try_get!(row, i64, "milestone_sequence"),
            "canonical_id": try_get!(row, String, "milestone_key"),
            "display_label": try_get!(row, Option<String>, "display_label"),
            "definition_revision_id": revision_id,
            "lifecycle": try_get!(row, String, "lifecycle"),
            "projection_reasons": reasons,
            "version": try_get!(row, i64, "version"),
            "created_at": try_get!(row, String, "created_at"),
            "updated_at": try_get!(row, String, "updated_at"),
        });
        let definition = json!({
            "id": try_get!(revision, String, "id"),
            "milestone_id": try_get!(revision, String, "milestone_id"),
            "project_id": project_id,
            "revision_number": try_get!(revision, i64, "revision"),
            "base_revision_id": try_get!(revision, Option<String>, "base_revision_id"),
            "lifecycle": try_get!(revision, String, "lifecycle"),
            "schema_version": try_get!(revision, String, "schema_version"),
            "content": definition_content,
            "rendered_view": rendered_view,
            "render_version": try_get!(revision, String, "render_version"),
            "content_digest": try_get!(revision, String, "content_digest"),
            "render_digest": try_get!(revision, String, "rendered_digest"),
            "provenance": {
                "author": principal,
                "profile_revision": Value::Null,
                "operating_skill_revision": Value::Null,
                "source_refs": row_json_array_from(&revision, "source_refs_json")?,
                "change_summary": try_get!(revision, String, "change_summary"),
                "material_diff": Value::Null,
            },
            "created_at": try_get!(revision, String, "created_at"),
        });
        result.push((milestone, definition));
    }
    Ok((result, stale))
}

async fn load_current_acceptance_checks(
    state: &AppState,
    project_id: &str,
    milestone_id: &str,
    definition_revision_id: &str,
) -> ApiResult<Vec<Value>> {
    let rows = sqlx::query(
        "SELECT c.id, c.description, c.required, c.source_kind, c.expected_result,
                c.version, r.id AS result_id, r.outcome, r.input_digest
         FROM project_milestone_check c
         LEFT JOIN project_milestone_check_result r ON r.id = c.current_result_id
         WHERE c.project_id = ? AND c.milestone_id = ? AND c.definition_revision_id = ?
         ORDER BY c.check_key ASC, c.id ASC",
    )
    .bind(project_id)
    .bind(milestone_id)
    .bind(definition_revision_id)
    .fetch_all(state.db.pool())
    .await
    .map_err(sql_error)?;

    rows.into_iter()
        .map(|row| {
            let outcome = row
                .try_get::<Option<String>, _>("outcome")
                .map_err(sql_error)?;
            let latest_result = match outcome.as_deref() {
                Some("passed") => Some("pass"),
                Some("failed") => Some("fail"),
                Some("missing") => Some("blocked"),
                Some("stale") => Some("stale"),
                Some("waived") => Some("waived"),
                Some(_) => Some("unavailable"),
                None => None,
            };
            Ok(json!({
                "id": try_get!(row, String, "id"),
                "description": try_get!(row, String, "description"),
                "required": try_get!(row, i64, "required") != 0,
                "source_kind": try_get!(row, String, "source_kind"),
                "expected_result": try_get!(row, String, "expected_result"),
                "version": try_get!(row, i64, "version"),
                "latest_result": latest_result,
                "latest_result_id": try_get!(row, Option<String>, "result_id"),
                "latest_result_digest": try_get!(row, Option<String>, "input_digest"),
            }))
        })
        .collect()
}

async fn load_task_counts(
    state: &AppState,
    project_id: &str,
    milestone_id: Option<&str>,
) -> ApiResult<Value> {
    let rows = if let Some(milestone_id) = milestone_id {
        sqlx::query(
            "SELECT t.status, t.blocked_json
             FROM task t
             JOIN project_task_governance g ON g.task_id = t.id
             WHERE t.project_id = ? AND g.project_id = ? AND g.milestone_id = ?
               AND t.deleted_at IS NULL",
        )
        .bind(project_id)
        .bind(project_id)
        .bind(milestone_id)
        .fetch_all(state.db.pool())
        .await
        .map_err(sql_error)?
    } else {
        sqlx::query(
            "SELECT status, blocked_json FROM task
             WHERE project_id = ? AND deleted_at IS NULL",
        )
        .bind(project_id)
        .fetch_all(state.db.pool())
        .await
        .map_err(sql_error)?
    };

    let mut counts = Counts::default();
    for row in rows {
        counts.total += 1;
        let status = row.try_get::<String, _>("status").map_err(sql_error)?;
        // A Task keeps its workflow status when an execution fails, so status
        // alone reports stalled work as active. The blocked record is what the
        // user is actually waiting on, and a terminal Task has moved past it.
        let blocked = row
            .try_get::<Option<String>, _>("blocked_json")
            .unwrap_or_default()
            .is_some_and(|value| !value.trim().is_empty());
        match classify_status(&status) {
            TaskBucket::Terminal => counts.terminal += 1,
            _ if blocked => counts.blocked += 1,
            TaskBucket::Backlog => counts.backlog += 1,
            TaskBucket::Active => counts.active += 1,
            TaskBucket::Review => counts.review += 1,
            TaskBucket::Blocked => counts.blocked += 1,
        }
    }
    Ok(json!({
        "total": counts.total,
        "backlog": counts.backlog,
        "active": counts.active,
        "review": counts.review,
        "terminal": counts.terminal,
        "blocked": counts.blocked,
    }))
}

async fn load_check_summary(
    state: &AppState,
    project_id: &str,
    milestone_id: Option<&str>,
) -> ApiResult<Value> {
    let rows = if let Some(milestone_id) = milestone_id {
        sqlx::query(
            "SELECT c.required, r.outcome
             FROM project_milestone_check c
             JOIN project_milestone m
               ON m.id = c.milestone_id AND m.project_id = c.project_id
              AND m.current_definition_revision_id = c.definition_revision_id
             LEFT JOIN project_milestone_check_result r
               ON r.id = COALESCE(
                    c.current_result_id,
                    (SELECT r2.id FROM project_milestone_check_result r2
                     WHERE r2.check_id = c.id
                       AND r2.definition_revision_id = c.definition_revision_id
                     ORDER BY r2.created_at DESC, r2.id DESC LIMIT 1)
                  )
              AND r.definition_revision_id = c.definition_revision_id
             WHERE c.project_id = ? AND c.milestone_id = ?",
        )
        .bind(project_id)
        .bind(milestone_id)
        .fetch_all(state.db.pool())
        .await
        .map_err(sql_error)?
    } else {
        sqlx::query(
            "SELECT c.required, r.outcome
             FROM project_milestone_check c
             JOIN project_milestone m
               ON m.id = c.milestone_id AND m.project_id = c.project_id
              AND m.current_definition_revision_id = c.definition_revision_id
             LEFT JOIN project_milestone_check_result r
               ON r.id = COALESCE(
                    c.current_result_id,
                    (SELECT r2.id FROM project_milestone_check_result r2
                     WHERE r2.check_id = c.id
                       AND r2.definition_revision_id = c.definition_revision_id
                     ORDER BY r2.created_at DESC, r2.id DESC LIMIT 1)
                  )
              AND r.definition_revision_id = c.definition_revision_id
             WHERE c.project_id = ?",
        )
        .bind(project_id)
        .fetch_all(state.db.pool())
        .await
        .map_err(sql_error)?
    };

    let mut summary = CheckSummary::default();
    for row in rows {
        let required = row.try_get::<i64, _>("required").map_err(sql_error)? != 0;
        if !required {
            continue;
        }
        summary.required_total += 1;
        match row
            .try_get::<Option<String>, _>("outcome")
            .map_err(sql_error)?
            .as_deref()
        {
            Some("passed") => summary.passed += 1,
            Some("failed") => summary.failed += 1,
            Some("stale") => summary.stale += 1,
            Some("waived") => summary.waived += 1,
            Some("missing") | None => summary.missing += 1,
            Some(_) => summary.unavailable += 1,
        }
    }
    Ok(json!({
        "required_total": summary.required_total,
        "passed": summary.passed,
        "failed": summary.failed,
        "missing": summary.missing,
        "stale": summary.stale,
        "waived": summary.waived,
        "unavailable": summary.unavailable,
    }))
}

async fn load_document_freshness(
    state: &AppState,
    project_id: &str,
) -> ApiResult<(Vec<Value>, bool)> {
    let rows = sqlx::query(
        "SELECT d.id, d.kind, d.lifecycle AS document_lifecycle,
                d.current_draft_revision_id,
                d.current_approved_revision_id,
                approved.content_digest AS approved_digest,
                approved.lifecycle AS approved_lifecycle,
                working.content_digest AS working_digest,
                working.lifecycle AS working_lifecycle
         FROM project_document d
         LEFT JOIN project_document_revision approved
           ON approved.id = d.current_approved_revision_id
          AND approved.document_id = d.id
         LEFT JOIN project_document_revision working
           ON working.id = d.current_draft_revision_id
          AND working.document_id = d.id
         WHERE d.project_id = ?
         ORDER BY d.updated_at DESC, d.id ASC",
    )
    .bind(project_id)
    .fetch_all(state.db.pool())
    .await
    .map_err(sql_error)?;
    let mut stale = false;
    let mut documents = Vec::new();
    for row in rows {
        let approved = row
            .try_get::<Option<String>, _>("current_approved_revision_id")
            .map_err(sql_error)?;
        let Some(kind) = document_kind(row.try_get::<String, _>("kind").map_err(sql_error)?) else {
            stale = true;
            continue;
        };
        let document_lifecycle = row
            .try_get::<String, _>("document_lifecycle")
            .map_err(sql_error)?;
        let draft = row
            .try_get::<Option<String>, _>("current_draft_revision_id")
            .map_err(sql_error)?;
        let approved_digest = row
            .try_get::<Option<String>, _>("approved_digest")
            .map_err(sql_error)?;
        let approved_lifecycle = row
            .try_get::<Option<String>, _>("approved_lifecycle")
            .map_err(sql_error)?;
        let working_digest = row
            .try_get::<Option<String>, _>("working_digest")
            .map_err(sql_error)?;
        let working_lifecycle = row
            .try_get::<Option<String>, _>("working_lifecycle")
            .map_err(sql_error)?;
        let approved_is_valid = approved.as_deref().is_some_and(|id| !id.trim().is_empty())
            && approved_digest
                .as_deref()
                .is_some_and(|digest| !digest.trim().is_empty())
            && approved_lifecycle.as_deref() == Some("approved");
        let working_is_valid = draft.as_deref().is_none_or(|id| {
            !id.trim().is_empty()
                && working_digest
                    .as_deref()
                    .is_some_and(|digest| !digest.trim().is_empty())
                && working_lifecycle.is_some()
        });
        let has_unapproved_working = draft
            .as_deref()
            .is_some_and(|id| approved.as_deref() != Some(id));
        let status = if !working_is_valid || document_lifecycle == "corrupt" {
            stale = true;
            DocumentFreshnessStatus::ReconciliationRequired
        } else if approved_is_valid && document_lifecycle == "approved" {
            if has_unapproved_working {
                DocumentFreshnessStatus::ChangesPending
            } else {
                DocumentFreshnessStatus::Current
            }
        } else if draft.is_some() && working_is_valid {
            // A draft/proposed revision is useful Project work even before the
            // first approved revision exists. It must be shown as pending,
            // never silently dropped as if the document did not exist.
            DocumentFreshnessStatus::ChangesPending
        } else {
            stale = true;
            DocumentFreshnessStatus::Stale
        };
        let reason = match status {
            DocumentFreshnessStatus::ChangesPending => Some(
                "A working revision is newer than the approved Project truth and awaits approval.",
            ),
            DocumentFreshnessStatus::Stale => Some(
                "The document has no complete approved revision that can be used as Project truth.",
            ),
            DocumentFreshnessStatus::ReconciliationRequired => Some(
                "The document pointers or revision metadata disagree and require reconciliation.",
            ),
            DocumentFreshnessStatus::Unavailable => {
                Some("The document revision projection is unavailable.")
            }
            DocumentFreshnessStatus::Current => None,
        };
        documents.push(json!({
            "document_id": try_get!(row, String, "id"),
            "kind": kind,
            "approved_revision_id": approved,
            "approved_digest": approved_digest,
            "working_revision_id": draft,
            "working_digest": working_digest,
            "working_lifecycle": working_lifecycle,
            "status": status,
            "reason": reason,
        }));
    }
    Ok((documents, stale))
}

/// The exact D19/F15 candidate-shape invariant, mirrored from the
/// enforcement point in `services::project_decision_commands` (a non-empty
/// question, at least two distinct non-empty options, a rationale, and a
/// recommendation that names one of those options). This projection cannot
/// import that service-crate-private check, so it re-derives the same rule
/// read-only: a row already rejected by the service boundary can only be
/// seen here as a historical row written before the invariant existed.
fn pending_candidate_shape_violation(
    options: &[String],
    selected_outcome: Option<&str>,
) -> Option<&'static str> {
    if options.iter().any(|option| option.trim().is_empty()) {
        return Some("an option is empty");
    }
    let distinct: std::collections::BTreeSet<&str> = options.iter().map(String::as_str).collect();
    if distinct.len() < 2 {
        return Some("it does not have at least two distinct options");
    }
    if let Some(outcome) = selected_outcome {
        if !options.iter().any(|option| option == outcome) {
            return Some("its recommendation does not name one of its options");
        }
    }
    None
}

fn valid_decision_class_name(value: &str) -> Option<&'static str> {
    match value {
        "user_scope" => Some("user_scope"),
        "project_implementation" => Some("project_implementation"),
        "policy" => Some("policy"),
        "waiver" => Some("waiver"),
        _ => None,
    }
}

/// Load bounded, typed pending Decision candidate summaries (design D19,
/// finding F15). This replaced the bare `unresolved_decision_ids` identifier
/// list: Project Overview previously exposed only opaque candidate UUIDs
/// with no question, alternatives, or approve/reject action. A historical
/// row that predates the D19 candidate-shape invariant is preserved exactly
/// as persisted -- never rewritten or dropped -- but is projected with
/// `validity: malformed`, an exact `invalid_reason`, and no `approve_target`
/// so no surface can promote its malformed shape into a permanent effective
/// Decision. `reject_target` remains available for every pending candidate,
/// malformed or not, because rejection never propagates that shape anywhere
/// consequential.
async fn load_pending_decisions(state: &AppState, project_id: &str) -> ApiResult<Vec<Value>> {
    let rows = sqlx::query(
        "SELECT id, lifecycle, question, context_json, options_json,
                selected_outcome, rationale, principal_type, principal_id,
                version, created_at, updated_at
         FROM project_decision_candidate
         WHERE project_id = ? AND lifecycle IN ('draft', 'proposed')
         ORDER BY created_at ASC, id ASC",
    )
    .bind(project_id)
    .fetch_all(state.db.pool())
    .await
    .map_err(sql_error)?;

    let mut pending = Vec::with_capacity(rows.len());
    for row in rows {
        let id = try_get!(row, String, "id");
        let mut reasons: Vec<String> = Vec::new();

        let question = try_get!(row, String, "question");
        if question.trim().is_empty() {
            reasons.push("question is empty".to_owned());
        }
        let options: Vec<String> =
            serde_json::from_str(&try_get!(row, String, "options_json")).unwrap_or_default();
        let selected_outcome = try_get!(row, Option<String>, "selected_outcome");
        let rationale = try_get!(row, Option<String>, "rationale");
        if rationale
            .as_deref()
            .map(str::trim)
            .unwrap_or_default()
            .is_empty()
        {
            reasons.push("rationale is missing".to_owned());
        }
        if let Some(reason) =
            pending_candidate_shape_violation(&options, selected_outcome.as_deref())
        {
            reasons.push(reason.to_owned());
        }

        let context_value: Value = serde_json::from_str(&try_get!(row, String, "context_json"))
            .unwrap_or_else(|_| json!({}));
        let decision_class = context_value
            .get("decision_class")
            .and_then(Value::as_str)
            .and_then(valid_decision_class_name)
            .map(str::to_owned)
            .unwrap_or_else(|| {
                reasons.push("decision class is invalid".to_owned());
                "project_implementation".to_owned()
            });
        let affected_records = json!({
            "affected_artifact_refs": context_value.get("affected_artifact_refs").cloned().unwrap_or_else(|| json!([])),
            "affected_task_ids": context_value.get("affected_task_ids").cloned().unwrap_or_else(|| json!([])),
            "affected_milestone_ids": context_value.get("affected_milestone_ids").cloned().unwrap_or_else(|| json!([])),
        });

        let principal_type = try_get!(row, Option<String>, "principal_type");
        let principal_id = try_get!(row, Option<String>, "principal_id");
        let proposed_by = principal_type
            .as_deref()
            .and_then(|kind| principal_value(kind, principal_id.as_deref()))
            .unwrap_or_else(|| {
                reasons.push("proposing principal is missing".to_owned());
                json!({ "kind": "system", "id": "system", "display_name": Value::Null })
            });

        let valid = reasons.is_empty();
        let approve_target = valid.then(|| {
            json!({
                "method": "POST",
                "path": format!(
                    "/api/v1/projects/{project_id}/decisions/candidates/{id}/approve"
                ),
            })
        });
        let reject_target = json!({
            "method": "POST",
            "path": format!("/api/v1/projects/{project_id}/decisions/candidates/{id}/reject"),
        });

        let summary = json!({
            "id": id,
            "project_id": project_id,
            "lifecycle": try_get!(row, String, "lifecycle"),
            "version": try_get!(row, i64, "version"),
            "question": question,
            "options": options,
            "recommendation": selected_outcome,
            "rationale": rationale,
            "decision_class": decision_class,
            "affected_records": affected_records,
            "proposed_by": proposed_by,
            "required_principal": "user",
            "validity": if valid { "valid" } else { "malformed" },
            "invalid_reason": if valid { None } else { Some(reasons.join("; ")) },
            "approve_target": approve_target,
            "reject_target": reject_target,
            "created_at": try_get!(row, String, "created_at"),
            "updated_at": try_get!(row, String, "updated_at"),
        });
        // Validate the closed public contract at the projection boundary so
        // a malformed persisted row can only ever surface through the
        // typed, marked-non-approvable shape above -- never as a permissive
        // JSON blob or a bare opaque identifier (F15).
        let _: api_types::PendingDecisionSummary = serde_json::from_value(summary.clone())
            .map_err(|_| ApiError::internal("invalid pending Decision candidate projection"))?;
        pending.push(summary);
    }
    Ok(pending)
}

/// Load a bounded effective Decision Log projection. Candidate rows are not
/// included here; they remain in `pending_decisions` so a draft cannot
/// be mistaken for an authoritative decision.
async fn load_decisions(state: &AppState, project_id: &str) -> ApiResult<Vec<Value>> {
    let rows = sqlx::query(
        "SELECT id, project_id, state, decision_class, question, context_json,
                options_json, selected_outcome, rationale, principal_type,
                principal_id, authority_basis, source_refs_json,
                affected_records_json, supersedes_decision_id, created_at
         FROM project_decision
         WHERE project_id = ?
         ORDER BY created_at DESC, id DESC
         LIMIT 64",
    )
    .bind(project_id)
    .fetch_all(state.db.pool())
    .await
    .map_err(sql_error)?;

    let replacement_rows = sqlx::query(
        "SELECT supersedes_decision_id, state FROM project_decision
         WHERE project_id = ? AND supersedes_decision_id IS NOT NULL
         ORDER BY created_at DESC, id DESC",
    )
    .bind(project_id)
    .fetch_all(state.db.pool())
    .await
    .map_err(sql_error)?;
    let mut replacement_state = HashMap::<String, String>::new();
    for row in replacement_rows {
        let Some(superseded_id) = row
            .try_get::<Option<String>, _>("supersedes_decision_id")
            .map_err(sql_error)?
        else {
            continue;
        };
        replacement_state.insert(superseded_id, try_get!(row, String, "state"));
    }

    let mut decisions = Vec::with_capacity(rows.len());
    for row in rows {
        let id = try_get!(row, String, "id");
        let state = effective_decision_state(
            &try_get!(row, String, "state"),
            replacement_state.get(&id).map(String::as_str),
        );
        let principal = principal_value(
            try_get!(row, String, "principal_type").as_str(),
            Some(try_get!(row, String, "principal_id").as_str()),
        )
        .ok_or_else(|| ApiError::internal("invalid persisted Decision principal"))?;
        let affected = row_json(&row, "affected_records_json")?;
        let affected_artifact_refs = affected
            .get("artifact_refs")
            .cloned()
            .unwrap_or_else(|| json!([]));
        let affected_task_ids = affected
            .get("task_ids")
            .cloned()
            .unwrap_or_else(|| json!([]));
        let affected_milestone_ids = affected
            .get("milestone_ids")
            .cloned()
            .unwrap_or_else(|| json!([]));
        let decision = json!({
            "id": id,
            "project_id": try_get!(row, String, "project_id"),
            "state": state,
            "question": try_get!(row, String, "question"),
            "context": Some(try_get!(row, String, "context_json")),
            "options": row_json_array(&row, "options_json")?,
            "selected_outcome": try_get!(row, String, "selected_outcome"),
            "rationale": try_get!(row, String, "rationale"),
            "decision_maker": principal,
            "decision_class": try_get!(row, String, "decision_class"),
            "authority_basis": Some(try_get!(row, String, "authority_basis")),
            "affected_artifact_refs": affected_artifact_refs,
            "affected_task_ids": affected_task_ids,
            "affected_milestone_ids": affected_milestone_ids,
            "supersedes_id": try_get!(row, Option<String>, "supersedes_decision_id"),
            "provenance": row_json_array(&row, "source_refs_json")?,
            "created_at": try_get!(row, String, "created_at"),
            "effective_at": try_get!(row, String, "created_at"),
        });
        // Validate the closed public contract at the projection boundary so a
        // malformed persisted row cannot become a permissive JSON blob.
        let _: api_types::DecisionRecord = serde_json::from_value(decision.clone())
            .map_err(|_| ApiError::internal("invalid persisted Decision record"))?;
        decisions.push(decision);
    }
    Ok(decisions)
}

fn effective_decision_state(current: &str, replacement_state: Option<&str>) -> String {
    if current != "active" {
        return current.to_owned();
    }
    match replacement_state {
        Some("invalidated") => "invalidated".to_owned(),
        Some(_) => "superseded".to_owned(),
        None => "active".to_owned(),
    }
}

async fn load_evidence(state: &AppState, project_id: &str) -> ApiResult<(Vec<Value>, bool)> {
    let rows = sqlx::query(
        "SELECT a.id, a.project_id, a.asset_id, a.task_id,
                a.source_task_id, a.source_execution_id, a.source_validation_id,
                a.source_task_version, a.source_context_digest,
                a.source_definition_revision_id,
                a.milestone_id, a.acceptance_check_ids_json, a.caption,
                a.evidence_kind, COALESCE(a.checksum, m.checksum) AS checksum,
                CASE WHEN m.availability != 'available' THEN m.availability
                     ELSE a.availability END AS availability,
                a.author_type, a.author_id, a.created_at,
                a.updated_at, a.deleted_at, a.version
         FROM project_media_attachment a
         JOIN media_asset m ON m.id = a.asset_id AND m.project_id = a.project_id
         WHERE a.project_id = ? AND a.attachment_kind = 'evidence'
         ORDER BY a.created_at ASC, a.id ASC",
    )
    .bind(project_id)
    .fetch_all(state.db.pool())
    .await
    .map_err(sql_error)?;
    let mut evidence = Vec::new();
    let mut stale = false;
    for row in rows {
        let Some(kind) = evidence_kind(
            row.try_get::<Option<String>, _>("evidence_kind")
                .map_err(sql_error)?
                .as_deref(),
        ) else {
            stale = true;
            continue;
        };
        let Some(checksum) = row
            .try_get::<Option<String>, _>("checksum")
            .map_err(sql_error)?
        else {
            stale = true;
            continue;
        };
        if checksum.trim().is_empty() {
            stale = true;
            continue;
        }
        let availability_value = try_get!(row, String, "availability");
        let Some(availability) = evidence_availability(availability_value.as_str()) else {
            stale = true;
            continue;
        };
        let Some(author) = principal_value(
            row.try_get::<String, _>("author_type")
                .map_err(sql_error)?
                .as_str(),
            row.try_get::<Option<String>, _>("author_id")
                .map_err(sql_error)?
                .as_deref(),
        ) else {
            stale = true;
            continue;
        };
        evidence.push(json!({
            "id": try_get!(row, String, "id"),
            "project_id": try_get!(row, String, "project_id"),
            "asset_id": try_get!(row, String, "asset_id"),
            "task_id": try_get!(row, Option<String>, "task_id"),
        "source_task_id": try_get!(row, Option<String>, "source_task_id"),
        "source_run_id": try_get!(row, Option<String>, "source_execution_id"),
        "source_validation_id": try_get!(row, Option<String>, "source_validation_id"),
        "source_task_version": try_get!(row, Option<i64>, "source_task_version"),
        "source_context_digest": try_get!(row, Option<String>, "source_context_digest"),
        "source_definition_revision_id": try_get!(
            row,
            Option<String>,
            "source_definition_revision_id"
        ),
        "milestone_id": try_get!(row, Option<String>, "milestone_id"),
            "acceptance_check_ids": row_json_array_from(&row, "acceptance_check_ids_json")?,
            "caption": try_get!(row, Option<String>, "caption").unwrap_or_default(),
            "kind": kind,
            "checksum": checksum,
            "availability": availability,
            "author": author,
            "captured_at": try_get!(row, String, "created_at"),
            "version": try_get!(row, i64, "version"),
            "created_at": try_get!(row, String, "created_at"),
            "removed_at": try_get!(row, Option<String>, "deleted_at"),
        }));
    }
    Ok((evidence, stale))
}

async fn load_latest_readiness(
    state: &AppState,
    project_id: &str,
    milestone_id: &str,
    current_definition_revision_id: &str,
    project_evidence: &[Value],
) -> ApiResult<(Option<Value>, bool)> {
    let row = sqlx::query(
        "SELECT id, project_id, milestone_id, definition_revision_id,
                input_manifest_json, event_watermark, outcome,
                blocking_reasons_json, check_results_json, waiver_manifest_json,
                evidence_manifest_json, commit_context_json,
                computing_policy_revision, readiness_digest,
                principal_type, principal_id, authorization_basis,
                authorization_action, authorization_occurred_at,
                expected_milestone_version, explicit_event, created_at
         FROM project_readiness_snapshot
         WHERE project_id = ? AND milestone_id = ?
         ORDER BY created_at DESC, id DESC LIMIT 1",
    )
    .bind(project_id)
    .bind(milestone_id)
    .fetch_optional(state.db.pool())
    .await
    .map_err(sql_error)?;
    let Some(row) = row else {
        return Ok((None, false));
    };
    let definition_revision_id = try_get!(row, String, "definition_revision_id");
    let source_event_watermark = try_get!(row, String, "event_watermark");
    let stale = definition_revision_id != current_definition_revision_id
        || source_event_watermark.trim().is_empty();
    let input_manifest =
        typed_json_array::<api_types::ReadinessInput>(&row, "input_manifest_json")?;
    let reasons = typed_json_array::<api_types::ReadinessReason>(&row, "blocking_reasons_json")?;
    let check_results =
        typed_json_array::<api_types::ValidationResult>(&row, "check_results_json")?;
    let waiver_ids = string_array_from(&row, "waiver_manifest_json")?;
    let evidence_attachment_ids =
        readiness_evidence_ids(&row_json(&row, "evidence_manifest_json")?)?;
    let persisted_evidence_manifest = row_json(&row, "evidence_manifest_json")?;
    let persisted_evidence_digests = persisted_evidence_manifest
        .get("digests")
        .or_else(|| persisted_evidence_manifest.get("evidence_digests"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let persisted_evidence_availability = persisted_evidence_manifest
        .get("availability")
        .or_else(|| persisted_evidence_manifest.get("evidence_availability"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut evidence_digests = persisted_evidence_digests;
    let mut evidence_availability = persisted_evidence_availability;
    let mut evidence_stale = false;
    for attachment_id in evidence_attachment_ids
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
    {
        let Some(attachment) = project_evidence
            .iter()
            .find(|item| item.get("id").and_then(Value::as_str) == Some(attachment_id))
        else {
            evidence_stale = true;
            continue;
        };
        let Some(checksum) = attachment.get("checksum").and_then(Value::as_str) else {
            evidence_stale = true;
            continue;
        };
        let Some(availability) = attachment.get("availability").and_then(Value::as_str) else {
            evidence_stale = true;
            continue;
        };
        if availability != "available" {
            evidence_stale = true;
        }
        // Keep the exact persisted manifest as the immutable snapshot body;
        // only the separate freshness overlay reflects current attachments.
        if evidence_digests.is_empty() {
            evidence_digests.push(Value::String(checksum.to_owned()));
        }
        if evidence_availability.is_empty() {
            evidence_availability.push(Value::String(availability.to_owned()));
        }
    }
    let commit_build_check_context = string_array_from(&row, "commit_context_json")?;
    let result = try_get!(row, String, "outcome");
    let computing_policy_revision = try_get!(row, String, "computing_policy_revision");
    let readiness_digest = try_get!(row, String, "readiness_digest");
    let expected_milestone_version = try_get!(row, i64, "expected_milestone_version");
    let Some(requesting_principal) = principal_value(
        try_get!(row, String, "principal_type").as_str(),
        Some(try_get!(row, String, "principal_id").as_str()),
    ) else {
        return Ok((None, true));
    };
    let authorization_basis = try_get!(row, String, "authorization_basis");
    let authorization_action = try_get!(row, String, "authorization_action");
    let authorization_event = try_get!(row, String, "explicit_event");
    let authorization_occurred_at = try_get!(row, String, "authorization_occurred_at");
    let stale = stale
        || result == "stale"
        || evidence_stale
        || expected_milestone_version <= 0
        || computing_policy_revision.trim().is_empty()
        || readiness_digest.trim().is_empty()
        || authorization_basis.trim().is_empty()
        || authorization_action.trim().is_empty()
        || authorization_event.trim().is_empty()
        || authorization_occurred_at.trim().is_empty();

    Ok((
        Some(json!({
            "id": try_get!(row, String, "id"),
            "project_id": try_get!(row, String, "project_id"),
            "milestone_id": try_get!(row, String, "milestone_id"),
            "expected_milestone_version": expected_milestone_version,
            "milestone_definition_revision_id": definition_revision_id,
            "input_manifest": input_manifest,
            "source_event_watermark": source_event_watermark,
            "result": result,
            "reasons": reasons,
            "check_results": check_results,
            "waiver_ids": waiver_ids,
            "evidence_attachment_ids": evidence_attachment_ids,
            "evidence_digests": evidence_digests,
            "evidence_availability": evidence_availability,
            "commit_build_check_context": commit_build_check_context,
            "computing_policy_revision": computing_policy_revision,
            "readiness_digest": readiness_digest,
            "computed_at": try_get!(row, String, "created_at"),
            "requesting_principal": requesting_principal,
            "authorization": {
                "principal": requesting_principal,
                "authorization_basis": authorization_basis,
                "action": authorization_action,
                "event_id": authorization_event,
                "occurred_at": authorization_occurred_at,
            },
        })),
        stale,
    ))
}

async fn load_releases(state: &AppState, project_id: &str) -> ApiResult<(Vec<Value>, bool)> {
    let rows = sqlx::query(
        "SELECT r.id, r.project_id, r.milestone_id, r.release_sequence,
                r.release_revision, r.release_identifier,
                r.milestone_revision_id, r.readiness_snapshot_id,
                r.readiness_digest,
                r.summary, r.changelog, r.known_issues_json,
                r.charter_revision_id, r.document_revisions_json,
                r.decision_ids_json, r.task_references_json,
                r.validation_references_json, r.git_references_json,
                r.evidence_references_json, r.waivers_json,
                r.releasing_principal_type, r.releasing_principal_id,
                r.authorization_basis, r.authorization_action,
                r.explicit_event, r.authorization_occurred_at,
                r.schema_version, r.snapshot_digest, r.idempotency_key,
                r.created_at, m.milestone_key, m.display_label,
                mr.content_digest AS milestone_definition_digest,
                rs.expected_milestone_version,
                rs.event_watermark AS source_event_watermark,
                cr.charter_id AS historic_charter_id,
                cr.content_digest AS charter_content_digest,
                cr.render_version AS charter_render_version,
                cr.rendered_digest AS charter_render_digest
         FROM project_release r
         JOIN project_milestone m ON m.id = r.milestone_id AND m.project_id = r.project_id
         JOIN project_milestone_revision mr
           ON mr.id = r.milestone_revision_id AND mr.milestone_id = r.milestone_id
         JOIN project_readiness_snapshot rs
           ON rs.id = r.readiness_snapshot_id
          AND rs.project_id = r.project_id
          AND rs.milestone_id = r.milestone_id
         LEFT JOIN project_charter_revision cr ON cr.id = r.charter_revision_id
         WHERE r.project_id = ?
         ORDER BY r.created_at DESC, r.id DESC",
    )
    .bind(project_id)
    .fetch_all(state.db.pool())
    .await
    .map_err(sql_error)?;
    let mut releases = Vec::new();
    let mut stale = false;
    for row in rows {
        // The release wire contract intentionally contains full immutable
        // references.  A malformed/incomplete persisted row is not silently
        // filled with guessed digests; it is omitted from the projection.
        let Some(charter_revision_id) = try_get!(row, Option<String>, "charter_revision_id") else {
            stale = true;
            continue;
        };
        let Some(charter_id) = try_get!(row, Option<String>, "historic_charter_id") else {
            stale = true;
            continue;
        };
        let Some(charter_digest) = try_get!(row, Option<String>, "charter_content_digest") else {
            stale = true;
            continue;
        };
        let Some(charter_render_version) = try_get!(row, Option<String>, "charter_render_version")
        else {
            stale = true;
            continue;
        };
        let Some(charter_render_digest) = try_get!(row, Option<String>, "charter_render_digest")
        else {
            stale = true;
            continue;
        };
        if charter_revision_id.trim().is_empty()
            || charter_id.trim().is_empty()
            || charter_digest.trim().is_empty()
            || charter_render_version.trim().is_empty()
            || charter_render_digest.trim().is_empty()
        {
            stale = true;
            continue;
        }
        let charter_ref = json!({
            "artifact_id": charter_id,
            "revision_id": charter_revision_id,
            "content_digest": charter_digest,
            "render_version": charter_render_version,
            "render_digest": charter_render_digest,
        });
        let decision_refs = parse_release_refs::<api_types::ReleaseDecisionReference>(&try_get!(
            row,
            String,
            "decision_ids_json"
        ));
        let task_refs = parse_release_refs::<api_types::ReleaseTaskReference>(&try_get!(
            row,
            String,
            "task_references_json"
        ));
        let validation_refs = parse_release_refs::<api_types::ReleaseValidationReference>(
            &try_get!(row, String, "validation_references_json"),
        );
        let evidence_pins = parse_release_refs::<api_types::EvidencePin>(&try_get!(
            row,
            String,
            "evidence_references_json"
        ));
        if decision_refs.is_none()
            || task_refs.is_none()
            || validation_refs.is_none()
            || evidence_pins.is_none()
        {
            stale = true;
            continue;
        }
        let Some(released_by) = principal_value(
            try_get!(row, String, "releasing_principal_type").as_str(),
            Some(try_get!(row, String, "releasing_principal_id").as_str()),
        ) else {
            stale = true;
            continue;
        };
        let milestone_definition_revision_id = try_get!(row, String, "milestone_revision_id");
        let milestone_definition_digest = try_get!(row, String, "milestone_definition_digest");
        let readiness_snapshot_id = try_get!(row, String, "readiness_snapshot_id");
        let readiness_digest = try_get!(row, String, "readiness_digest");
        let snapshot_digest = try_get!(row, String, "snapshot_digest");
        let release_identifier = try_get!(row, String, "release_identifier");
        let expected_milestone_version = try_get!(row, i64, "expected_milestone_version");
        let source_event_watermark = try_get!(row, String, "source_event_watermark");
        let authorization_basis = try_get!(row, String, "authorization_basis");
        let authorization_action = try_get!(row, String, "authorization_action");
        let authorization_event = try_get!(row, String, "explicit_event");
        let authorization_occurred_at = try_get!(row, String, "authorization_occurred_at");
        if milestone_definition_revision_id.trim().is_empty()
            || milestone_definition_digest.trim().is_empty()
            || readiness_snapshot_id.trim().is_empty()
            || readiness_digest.trim().is_empty()
            || snapshot_digest.trim().is_empty()
            || release_identifier.trim().is_empty()
            || expected_milestone_version <= 0
            || source_event_watermark.trim().is_empty()
            || authorization_basis.trim().is_empty()
            || authorization_action.trim().is_empty()
            || authorization_event.trim().is_empty()
            || authorization_occurred_at.trim().is_empty()
        {
            stale = true;
            continue;
        }
        let snapshot = json!({
            "schema_version": try_get!(row, String, "schema_version"),
            "project_id": project_id,
            "milestone_id": try_get!(row, String, "milestone_id"),
            "milestone_canonical_id": try_get!(row, String, "milestone_key"),
            "release_revision": try_get!(row, i64, "release_revision"),
            "release_identity": release_identifier,
            "milestone_definition_revision_id": milestone_definition_revision_id,
            "milestone_definition_digest": milestone_definition_digest,
            "expected_milestone_version": expected_milestone_version,
            "display_label": try_get!(row, Option<String>, "display_label"),
            "summary": try_get!(row, String, "summary"),
            "changelog": string_array_from(&row, "changelog")?,
            "known_issues": row_json_array_from(&row, "known_issues_json")?,
            "readiness_snapshot_id": readiness_snapshot_id,
            "readiness_digest": readiness_digest,
            "source_event_watermark": source_event_watermark,
            "charter_revision": charter_ref,
            "document_revisions": row_json_array_from(&row, "document_revisions_json")?,
            "included_decisions": decision_refs,
            "included_tasks": task_refs,
            "validation_results": validation_refs,
            "repository_references": string_array_from(&row, "git_references_json")?,
            "evidence_pins": evidence_pins,
            "waived_check_ids": string_array_from(&row, "waivers_json")?,
            "released_by": released_by,
            "authorization": {
                "principal": released_by,
                "authorization_basis": authorization_basis,
                "action": authorization_action,
                "event_id": authorization_event,
                "occurred_at": authorization_occurred_at,
            },
            "released_at": try_get!(row, String, "created_at"),
            "idempotency_key": try_get!(row, String, "idempotency_key"),
            "snapshot_digest": snapshot_digest,
        });
        releases.push(json!({
            "id": try_get!(row, String, "id"),
            "project_id": project_id,
            "milestone_id": try_get!(row, String, "milestone_id"),
            "release_sequence": try_get!(row, i64, "release_sequence"),
            "release_identity": release_identifier,
            "snapshot": snapshot,
            "version": try_get!(row, i64, "release_revision"),
            "created_at": try_get!(row, String, "created_at"),
        }));
    }
    Ok((releases, stale))
}

async fn load_watermark(
    state: &AppState,
    project_id: &str,
    project_work_epoch: i64,
) -> ApiResult<String> {
    let row = sqlx::query(
        "SELECT id FROM domain_event
         WHERE scope_type = 'project' AND scope_id = ?
         ORDER BY sequence DESC LIMIT 1",
    )
    .bind(project_id)
    .fetch_optional(state.db.pool())
    .await
    .map_err(sql_error)?;
    Ok(match row {
        Some(row) => try_get!(row, String, "id"),
        None => format!("project-work-epoch:{project_work_epoch}"),
    })
}

async fn load_failed_task_count(state: &AppState, project_id: &str) -> ApiResult<i64> {
    sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM task
         WHERE project_id = ? AND deleted_at IS NULL
           AND (lower(status) LIKE '%failed%'
                OR lower(status) LIKE '%error%'
                OR failed_json IS NOT NULL)",
    )
    .bind(project_id)
    .fetch_one(state.db.pool())
    .await
    .map_err(sql_error)
}

async fn load_reconciliation_required(state: &AppState, project_id: &str) -> ApiResult<bool> {
    sqlx::query_scalar::<_, i64>(
        "SELECT EXISTS (
             SELECT 1 FROM project_reconciliation_record
             WHERE project_id = ? AND state = 'required'
         )",
    )
    .bind(project_id)
    .fetch_one(state.db.pool())
    .await
    .map(|value| value != 0)
    .map_err(sql_error)
}

macro_rules! project_action {
    (
        $code:expr,
        $required_principal:expr,
        $target_type:expr,
        $target_id:expr,
        $title:expr,
        $explanation:expr,
        $action_kind:expr,
        $route_or_operation:expr,
        $blocking:expr,
        $expected_version:expr $(,)?
    ) => {
        ProjectNextAction {
            code: $code.to_owned(),
            required_principal: $required_principal.to_owned(),
            target_type: $target_type.to_owned(),
            target_id: ($target_id).into(),
            title: $title.to_owned(),
            explanation: $explanation.to_owned(),
            action_kind: $action_kind.to_owned(),
            route_or_operation: $route_or_operation.to_owned(),
            blocking: $blocking,
            expected_version: $expected_version,
        }
    };
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

fn next_action(context: NextActionContext<'_>) -> Option<ProjectNextAction> {
    let NextActionContext {
        project_id,
        project_version,
        charter_setup_required,
        no_milestones,
        execution_setup,
        milestones,
        documents,
        releases,
        pending_decision_ids,
        task_counts,
        failed_task_count,
        checks,
        reconciliation_required,
        stale,
    } = context;
    // This order is part of the public projection contract. More specific
    // blockers must never be hidden behind a generic stale banner.
    if charter_setup_required {
        return Some(project_action!(
            "charter_adoption",
            "user",
            "project",
            project_id,
            "Adopt the Project Charter",
            "An approved Charter is required before Project work can be governed.",
            "approval",
            "project.charter.adoption",
            true,
            Some(project_version),
        ));
    }
    if reconciliation_required {
        return Some(project_action!(
            "reconciliation_required",
            "user",
            "project",
            project_id,
            "Resolve the Project reconciliation",
            "A canonical conflict is waiting for an explicit user resolution.",
            "reconciliation",
            "project.reconciliation.resolve",
            true,
            Some(project_version),
        ));
    }
    if let Some(milestone) = milestones.iter().find(|milestone| {
        milestone
            .get("milestone")
            .and_then(|value| value.get("projection_reasons"))
            .and_then(Value::as_array)
            .is_some_and(|reasons| {
                reasons.iter().any(|reason| {
                    matches!(
                        reason.get("kind").and_then(Value::as_str),
                        Some("reconciliation") | Some("conflict")
                    )
                })
            })
    }) {
        let milestone = milestone.get("milestone")?;
        let milestone_id = milestone.get("id")?.as_str()?;
        let version = milestone.get("version").and_then(Value::as_i64);
        return Some(project_action!(
            "milestone_reconciliation",
            "user",
            "milestone",
            milestone_id,
            "Reconcile the milestone",
            "The current milestone projection contains an unresolved canonical conflict.",
            "reconciliation",
            "project.milestone.revision.save",
            true,
            version,
        ));
    }
    if documents.iter().any(|document| {
        matches!(
            document.get("status").and_then(Value::as_str),
            Some("stale") | Some("reconciliation_required") | Some("unavailable")
        )
    }) {
        return Some(project_action!(
            "document_reconciliation",
            "project_agent",
            "project",
            project_id,
            "Reconcile Project documents",
            "A governing Document pointer or revision is incomplete and cannot be used as Project truth.",
            "reconciliation",
            "project.document.reconcile",
            true,
            Some(project_version),
        ));
    }

    if execution_setup_requires_action(execution_setup) {
        let (code, title, explanation, operation) = match execution_setup.execution_setup_state {
            api_types::ExecutionSetupState::Provisioning => (
                "execution_setup_provisioning",
                "Finish execution setup",
                "Repository and execution principals are still being provisioned.",
                "project.execution_setup.retry_provisioning",
            ),
            api_types::ExecutionSetupState::Failed => (
                "execution_setup_failed",
                "Repair execution setup",
                "Execution setup failed and needs explicit configuration or retry.",
                "project.execution_setup.retry_provisioning",
            ),
            api_types::ExecutionSetupState::Unavailable => (
                "execution_setup_unavailable",
                "Refresh execution setup",
                "The authoritative execution setup projection is unavailable.",
                "project.execution_setup.refresh",
            ),
            _ => (
                "execution_setup_required",
                "Complete execution setup",
                "Select the execution roles and attach a repository before running Tasks.",
                "project.execution_setup",
            ),
        };
        return Some(project_action!(
            code,
            "user",
            "project",
            project_id,
            title,
            explanation,
            "setup",
            operation,
            true,
            Some(execution_setup.project_version),
        ));
    }

    match execution_setup.execution_gate {
        api_types::ExecutionGate::BaselineApprovalRequired
        | api_types::ExecutionGate::PreBaselineReadOnly
        | api_types::ExecutionGate::Active => {}
        api_types::ExecutionGate::ReconciliationRequired
        | api_types::ExecutionGate::Unavailable => {
            // Same underlying `project_reconciliation_record` source as the
            // Project-wide `reconciliation_required` branch above -- this
            // branch is reached only when that projection disagrees with the
            // execution-setup projection, so it points at the same real,
            // registered resolve operation rather than a second invented one.
            return Some(project_action!(
                "execution_gate_reconciliation",
                "user",
                "project",
                project_id,
                "Reconcile the execution gate",
                "Forge could not verify the current Charter-backed execution state.",
                "reconciliation",
                "project.reconciliation.resolve",
                true,
                Some(execution_setup.project_version),
            ));
        }
    }

    // A milestone whose current definition revision is still `draft` or
    // `proposed` has no approved contract; validation, evidence, and
    // readiness would all be measured against a definition nobody approved.
    // The user's approval is the blocker there, not the Worker's evidence, and
    // the stale banner alone gave the user nothing to act on.
    if let Some(milestone) = milestones.iter().find(|milestone| {
        milestone
            .get("definition")
            .and_then(|definition| definition.get("lifecycle"))
            .and_then(Value::as_str)
            .is_some_and(|lifecycle| lifecycle != "approved")
    }) {
        let revision_id = milestone.get("definition")?.get("id")?.as_str()?;
        let version = milestone
            .get("milestone")?
            .get("version")
            .and_then(Value::as_i64);
        return Some(project_action!(
            "milestone_definition_approval",
            "user",
            "milestone_revision",
            revision_id,
            "Approve the milestone definition revision",
            "The milestone's current definition revision is not approved. Validation, evidence, and readiness are measured against an unapproved contract until a user approves or supersedes it.",
            "approval",
            "project.milestone.revision.transition",
            true,
            version,
        ));
    }

    if let Some(milestone) = milestones
        .iter()
        .find(|milestone| readiness_requires_contract_reconciliation(milestone))
    {
        let milestone = milestone.get("milestone")?;
        let milestone_id = milestone.get("id")?.as_str()?;
        let version = milestone.get("version").and_then(Value::as_i64);
        return Some(project_action!(
            "acceptance_contract_reconciliation",
            "project_agent",
            "milestone",
            milestone_id,
            "Reconcile the acceptance and evidence contract",
            "The active baseline and current milestone use different stable check or evidence identities. Revise the milestone and propose one exact replacement baseline before collecting release inputs.",
            "reconciliation",
            "project.milestone.revision.save",
            true,
            version,
        ));
    }

    if value_i64(task_counts, "blocked") > 0 || failed_task_count > 0 {
        return Some(project_action!(
            if value_i64(task_counts, "blocked") > 0 {
                "task_blocked_remediation"
            } else {
                "task_failure_remediation"
            },
            "project_agent",
            "project",
            project_id,
            if value_i64(task_counts, "blocked") > 0 {
                "Unblock blocked Tasks"
            } else {
                "Remediate failed Tasks"
            },
            if value_i64(task_counts, "blocked") > 0 {
                "One or more Tasks are blocked and need an explicit dependency or scope remediation."
            } else {
                "One or more Tasks have failed and must be repaired or rerun before validation."
            },
            "remediation",
            "task.remediate",
            true,
            None,
        ));
    }
    if let Some((_, check)) = manual_check_requiring_user(milestones) {
        let check_id = check.get("id")?.as_str()?;
        let version = check.get("version").and_then(Value::as_i64);
        let latest = check.get("latest_result").and_then(Value::as_str);
        return Some(project_action!(
            "manual_attestation_required",
            "user",
            "milestone_check",
            check_id,
            if latest.is_some() {
                "Review the manual acceptance result"
            } else {
                "Record a manual acceptance result"
            },
            "This check requires an explicit user Pass or Fail result. Attestation does not replace its separate evidence requirement.",
            "attestation",
            "project.milestone.check.record",
            true,
            version,
        ));
    }
    if value_i64(checks, "failed") > 0 {
        return Some(project_action!(
            "validation_failure_remediation",
            "worker",
            "project",
            project_id,
            "Resolve failed validation",
            "A required acceptance check failed and needs a new authoritative result.",
            "validation",
            "project.milestone.check.evaluate",
            true,
            None,
        ));
    }
    if value_i64(checks, "stale") > 0
        || value_i64(checks, "missing") > 0
        || value_i64(checks, "unavailable") > 0
    {
        return Some(project_action!(
            "validation_required",
            "worker",
            "project",
            project_id,
            "Complete required validation",
            "Required acceptance checks are missing, stale, or unavailable.",
            "validation",
            "project.milestone.check.evaluate",
            true,
            None,
        ));
    }
    if !pending_decision_ids.is_empty() {
        return Some(project_action!(
            "decision_resolution",
            "user",
            "decision",
            pending_decision_ids[0].clone(),
            "Resolve the open decision",
            "A draft or proposed decision is still awaiting explicit resolution.",
            "reconciliation",
            "project.decision.resolve",
            true,
            None,
        ));
    }
    if let Some(milestone) = milestones
        .iter()
        .find(|milestone| evidence_requires_attention(milestone))
    {
        let milestone = milestone.get("milestone")?;
        let milestone_id = milestone.get("id")?.as_str()?;
        let version = milestone.get("version").and_then(Value::as_i64);
        return Some(project_action!(
            "evidence_required",
            "worker",
            "milestone",
            milestone_id,
            "Attach required evidence",
            "The milestone's evidence contract is not satisfied by available authoritative attachments.",
            "evidence",
            "project.milestone.evidence.attach",
            true,
            version,
        ));
    }

    if let Some(milestone) = milestones.iter().find(|milestone| {
        milestone.get("latest_readiness").is_none_or(Value::is_null)
            || milestone
                .get("readiness_freshness")
                .and_then(|freshness| freshness.get("status"))
                .and_then(Value::as_str)
                .is_some_and(|status| status != "current")
    }) {
        let milestone = milestone.get("milestone")?;
        let milestone_id = milestone.get("id")?.as_str()?;
        let version = milestone.get("version").and_then(Value::as_i64);
        return Some(project_action!(
            "readiness_request",
            "project_agent",
            "milestone",
            milestone_id,
            "Request a readiness snapshot",
            "Validation and evidence are present; compute a fresh immutable readiness snapshot.",
            "readiness",
            "project.milestone.readiness",
            true,
            version,
        ));
    }

    if let Some(milestone) = milestones.iter().find(|milestone| {
        milestone
            .get("latest_readiness")
            .and_then(|snapshot| snapshot.get("result"))
            .and_then(Value::as_str)
            .is_some_and(|result| result != "ready")
            && milestone
                .get("readiness_freshness")
                .and_then(|freshness| freshness.get("status"))
                .and_then(Value::as_str)
                == Some("current")
    }) {
        let milestone = milestone.get("milestone")?;
        let milestone_id = milestone.get("id")?.as_str()?;
        let version = milestone.get("version").and_then(Value::as_i64);
        return Some(project_action!(
            "readiness_blocked",
            "project_agent",
            "milestone",
            milestone_id,
            "Address readiness blockers",
            "The latest authoritative readiness snapshot is not ready for release; resolve its typed reasons and evaluate again.",
            "readiness",
            "project.milestone.readiness",
            true,
            version,
        ));
    }

    if let Some(milestone) = milestones.iter().find(|milestone| {
        let Some(snapshot) = milestone.get("latest_readiness") else {
            return false;
        };
        snapshot.get("result").and_then(Value::as_str) == Some("ready")
            && milestone
                .get("readiness_freshness")
                .and_then(|freshness| freshness.get("status"))
                .and_then(Value::as_str)
                == Some("current")
            && milestone_release_missing(milestone, releases)
    }) {
        let milestone = milestone.get("milestone")?;
        let milestone_id = milestone.get("id")?.as_str()?;
        // Release CAS is against the current mutable milestone row. The
        // immutable readiness candidate's expected version is one step behind
        // after readiness evaluation advances the lifecycle/version.
        let expected_version = milestone.get("version").and_then(Value::as_i64);
        return Some(project_action!(
            "user_release",
            "user",
            "milestone",
            milestone_id,
            "Release the ready milestone",
            "The latest readiness snapshot is ready for the exact user-authorized release operation.",
            "release",
            "project.milestone.release",
            true,
            expected_version,
        ));
    }

    if no_milestones {
        return Some(project_action!(
            "milestone_definition",
            "project_agent",
            "project",
            project_id,
            "Define the next bounded milestone",
            "No active milestone is available; define the next bounded outcome and acceptance contract.",
            "planning",
            "project.milestone.create",
            true,
            Some(project_version),
        ));
    }
    if stale {
        return Some(project_action!(
            "projection_reconciliation",
            "project_agent",
            "project",
            project_id,
            "Reconcile stale Project records",
            "The Overview contains records that no longer prove a current authoritative projection.",
            "reconciliation",
            "project.projection.reconcile",
            true,
            Some(project_version),
        ));
    }
    None
}

fn manual_check_requiring_user(milestones: &[Value]) -> Option<(&Value, &Value)> {
    milestones.iter().find_map(|milestone| {
        milestone
            .get("current_checks")?
            .as_array()?
            .iter()
            .find(|check| {
                check.get("required").and_then(Value::as_bool) == Some(true)
                    && check.get("source_kind").and_then(Value::as_str) == Some("manual")
                    && !matches!(
                        check.get("latest_result").and_then(Value::as_str),
                        Some("pass") | Some("waived")
                    )
            })
            .map(|check| (milestone, check))
    })
}

fn readiness_requires_contract_reconciliation(milestone: &Value) -> bool {
    milestone
        .pointer("/latest_readiness/reasons")
        .and_then(Value::as_array)
        .is_some_and(|reasons| {
            reasons.iter().any(|reason| {
                reason
                    .get("code")
                    .and_then(Value::as_str)
                    .is_some_and(|code| code.contains("reconciliation_required"))
            })
        })
}

fn value_i64(value: &Value, key: &str) -> i64 {
    value.get(key).and_then(Value::as_i64).unwrap_or_default()
}

fn execution_setup_requires_action(setup: &api_types::ProjectExecutionSetupResponse) -> bool {
    setup.coordination_state != api_types::CoordinationState::Ready
        || !matches!(
            setup.execution_setup_state,
            api_types::ExecutionSetupState::Ready
        )
        || !setup.setup_requirements.is_empty()
}

fn evidence_requires_attention(milestone: &Value) -> bool {
    let Some(definition) = milestone.get("definition") else {
        return false;
    };
    let requirements = definition
        .pointer("/content/evidence_requirements")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    if requirements.is_empty() {
        return false;
    }
    let evidence = milestone
        .get("evidence")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    if evidence.is_empty() {
        return true;
    }
    let available = evidence
        .iter()
        .filter(|item| item.get("availability").and_then(Value::as_str) == Some("available"));
    let available = available.count();
    let required_count = requirements
        .iter()
        .filter(|requirement| {
            requirement
                .get("required")
                .and_then(Value::as_bool)
                .unwrap_or(true)
        })
        .count();
    // The definition and attachment schemas can evolve independently; when a
    // requirement has no explicit id, count-based availability is the only
    // safe projection and still fails closed.
    available < required_count.max(1)
}

fn milestone_release_missing(milestone: &Value, releases: &[Value]) -> bool {
    let milestone_id = milestone
        .get("milestone")
        .and_then(|value| value.get("id"))
        .and_then(Value::as_str);
    milestone_id.is_some_and(|id| {
        !releases
            .iter()
            .any(|release| release.get("milestone_id").and_then(Value::as_str) == Some(id))
    })
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

fn classify_status(status: &str) -> TaskBucket {
    let status = status.to_ascii_lowercase();
    if matches!(
        status.as_str(),
        "done" | "completed" | "cancelled" | "canceled" | "archived"
    ) {
        TaskBucket::Terminal
    } else if status == "blocked" || status.contains("blocked") {
        TaskBucket::Blocked
    } else if status == "review" || status.contains("review") || status.contains("merge") {
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
        "agent" | "main_agent" | "project_agent" => "agent",
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

fn db_error(error: db::DbError) -> ApiError {
    tracing::error!(error = ?error, "Project Overview repository query failed");
    ApiError::internal("Project Overview is temporarily unavailable")
}

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
mod next_action_parity_tests {
    use super::*;

    #[derive(Debug, Clone, Copy)]
    enum NextActionTarget {
        /// A real REST route registered in `crates/api/src/lib.rs`. The
        /// backing handler(s) are referenced by path in
        /// `rest_backed_operations_reference_real_handlers` below.
        Rest,
        /// `required_principal` is `project_agent`/`worker`: the
        /// "Continue with Project Agent" chat link (or ordinary Task
        /// execution) is the real target because that principal already
        /// has its own typed command for the domain action.
        AutomatedByResponsiblePrincipal,
        /// A plain re-fetch of the same query. Only valid for a stale or
        /// unavailable projection -- never for a current canonical
        /// conflict, per design invariant D15/8.1.8.
        Refresh,
    }

    /// Every operation `next_action()` can currently produce, and its real
    /// target. `registered_target` panics on an unregistered operation, so
    /// this table is the enforcement point: a new `next_action()` branch
    /// that advertises an unregistered operation fails the coverage test
    /// below before it can ship as another dead action.
    const REGISTRY: &[(&str, NextActionTarget)] = &[
        ("project.charter.adoption", NextActionTarget::Rest),
        ("project.reconciliation.resolve", NextActionTarget::Rest),
        ("project.milestone.revision.save", NextActionTarget::Rest),
        (
            "project.document.reconcile",
            NextActionTarget::AutomatedByResponsiblePrincipal,
        ),
        (
            "project.execution_setup.retry_provisioning",
            NextActionTarget::Rest,
        ),
        ("project.execution_setup.refresh", NextActionTarget::Refresh),
        ("project.execution_setup", NextActionTarget::Rest),
        (
            "task.remediate",
            NextActionTarget::AutomatedByResponsiblePrincipal,
        ),
        (
            "project.milestone.revision.transition",
            NextActionTarget::Rest,
        ),
        ("project.milestone.check.record", NextActionTarget::Rest),
        (
            "project.milestone.check.evaluate",
            NextActionTarget::AutomatedByResponsiblePrincipal,
        ),
        // F15 is closed: pending Decision candidates are now typed
        // `pending_decisions` summaries with a real approve/reject target
        // (`crate::routes::project_documents::approve_decision_candidate` /
        // `reject_decision_candidate`), not an opaque UUID with no handler.
        ("project.decision.resolve", NextActionTarget::Rest),
        ("project.milestone.evidence.attach", NextActionTarget::Rest),
        ("project.milestone.readiness", NextActionTarget::Rest),
        ("project.milestone.release", NextActionTarget::Rest),
        ("project.milestone.create", NextActionTarget::Rest),
        ("project.projection.reconcile", NextActionTarget::Refresh),
    ];

    fn registered_target(operation: &str) -> NextActionTarget {
        REGISTRY
            .iter()
            .find(|(name, _)| *name == operation)
            .unwrap_or_else(|| {
                panic!(
                    "next_action() produced operation '{operation}' with no registered \
                     service/route/UI target -- add it to REGISTRY in project_overview.rs \
                     and give it a real target before shipping"
                )
            })
            .1
    }

    /// Compile-time existence proof for every `NextActionTarget::Rest`
    /// entry above: referencing a handler by path fails this module to
    /// compile if the handler is ever renamed or removed.
    #[test]
    fn rest_backed_operations_reference_real_handlers() {
        let _ = crate::routes::project_charters::approve_project_charter_revision;
        let _ = crate::routes::reconciliations::resolve_project_reconciliation;
        let _ = crate::routes::milestones::save_milestone_revision;
        let _ = crate::routes::project_execution_setup::retry_provisioning;
        let _ = crate::routes::project_execution_setup::select_worker;
        let _ = crate::routes::project_execution_setup::attach_primary_repository;
        let _ = crate::routes::milestones::record_milestone_check;
        let _ = crate::routes::project_media::attach_evidence;
        let _ = crate::routes::milestones::evaluate_readiness;
        let _ = crate::routes::milestones::release_milestone;
        let _ = crate::routes::milestones::create_milestone;
        let _ = crate::routes::project_documents::approve_decision_candidate;
        let _ = crate::routes::project_documents::reject_decision_candidate;
    }

    fn base_execution_setup(
        coordination_state: api_types::CoordinationState,
        execution_setup_state: api_types::ExecutionSetupState,
        execution_gate: api_types::ExecutionGate,
    ) -> api_types::ProjectExecutionSetupResponse {
        api_types::ProjectExecutionSetupResponse {
            project_id: "project-1".to_owned(),
            project_version: 7,
            coordination_state,
            execution_setup_state,
            execution_gate,
            availability: api_types::ProjectExecutionSetupAvailability {
                coordination: api_types::ProjectionStatus::current(),
                execution_setup: api_types::ProjectionStatus::current(),
                execution_gate: api_types::ProjectionStatus::current(),
            },
            primary_repo: None,
            worker: None,
            independent_reviewer: None,
            eligible_workers: Vec::new(),
            eligible_reviewers: Vec::new(),
            setup_requirements: Vec::new(),
            next_action: None,
            provisioning: None,
            execution_blocker: None,
        }
    }

    fn ready_execution_setup() -> api_types::ProjectExecutionSetupResponse {
        base_execution_setup(
            api_types::CoordinationState::Ready,
            api_types::ExecutionSetupState::Ready,
            api_types::ExecutionGate::Active,
        )
    }

    struct Fixture {
        charter_setup_required: bool,
        no_milestones: bool,
        execution_setup: api_types::ProjectExecutionSetupResponse,
        milestones: Vec<Value>,
        documents: Vec<Value>,
        releases: Vec<Value>,
        pending_decision_ids: Vec<String>,
        task_counts: Value,
        failed_task_count: i64,
        checks: Value,
        reconciliation_required: bool,
        stale: bool,
    }

    impl Fixture {
        fn base() -> Self {
            Self {
                charter_setup_required: false,
                no_milestones: false,
                execution_setup: ready_execution_setup(),
                milestones: Vec::new(),
                documents: Vec::new(),
                releases: Vec::new(),
                pending_decision_ids: Vec::new(),
                task_counts: json!({}),
                failed_task_count: 0,
                checks: json!({}),
                reconciliation_required: false,
                stale: false,
            }
        }

        fn action(&self) -> ProjectNextAction {
            next_action(NextActionContext {
                project_id: "project-1",
                project_version: 7,
                charter_setup_required: self.charter_setup_required,
                no_milestones: self.no_milestones,
                execution_setup: &self.execution_setup,
                milestones: &self.milestones,
                documents: &self.documents,
                releases: &self.releases,
                pending_decision_ids: &self.pending_decision_ids,
                task_counts: &self.task_counts,
                failed_task_count: self.failed_task_count,
                checks: &self.checks,
                reconciliation_required: self.reconciliation_required,
                stale: self.stale,
            })
            .expect("fixture is constructed to always trigger exactly one next action")
        }
    }

    fn default_milestone(id: &str, version: i64) -> Value {
        json!({
            "milestone": {"id": id, "version": version, "projection_reasons": []},
            "definition": {"content": {"evidence_requirements": []}},
            "evidence": [],
            "current_checks": [],
            "latest_readiness": null,
            "readiness_freshness": {"status": "current"},
        })
    }

    /// Exercise every branch of `next_action()`, and assert its operation
    /// has a registered target. This is the parity guarantee: a future
    /// branch that advertises an unregistered operation fails here instead
    /// of shipping as another dead action.
    #[test]
    fn every_next_action_operation_has_a_registered_target() {
        let mut scenarios: Vec<(&str, Fixture)> = Vec::new();

        let mut fixture = Fixture::base();
        fixture.charter_setup_required = true;
        scenarios.push(("charter_setup_required", fixture));

        let mut fixture = Fixture::base();
        fixture.reconciliation_required = true;
        scenarios.push(("reconciliation_required", fixture));

        let mut fixture = Fixture::base();
        let mut milestone = default_milestone("milestone-1", 3);
        milestone["milestone"]["projection_reasons"] = json!([{"kind": "reconciliation"}]);
        fixture.milestones = vec![milestone];
        scenarios.push(("milestone_reconciliation", fixture));

        let mut fixture = Fixture::base();
        fixture.documents = vec![json!({"status": "stale"})];
        scenarios.push(("document_reconciliation", fixture));

        let mut fixture = Fixture::base();
        fixture.execution_setup = base_execution_setup(
            api_types::CoordinationState::Ready,
            api_types::ExecutionSetupState::Provisioning,
            api_types::ExecutionGate::PreBaselineReadOnly,
        );
        scenarios.push(("execution_setup_provisioning", fixture));

        let mut fixture = Fixture::base();
        fixture.execution_setup = base_execution_setup(
            api_types::CoordinationState::Ready,
            api_types::ExecutionSetupState::Failed,
            api_types::ExecutionGate::PreBaselineReadOnly,
        );
        scenarios.push(("execution_setup_failed", fixture));

        let mut fixture = Fixture::base();
        fixture.execution_setup = base_execution_setup(
            api_types::CoordinationState::Ready,
            api_types::ExecutionSetupState::Unavailable,
            api_types::ExecutionGate::PreBaselineReadOnly,
        );
        scenarios.push(("execution_setup_unavailable", fixture));

        let mut fixture = Fixture::base();
        fixture.execution_setup = base_execution_setup(
            api_types::CoordinationState::SetupRequired,
            api_types::ExecutionSetupState::SetupRequired,
            api_types::ExecutionGate::PreBaselineReadOnly,
        );
        scenarios.push(("execution_setup_required", fixture));

        let mut fixture = Fixture::base();
        fixture.execution_setup = base_execution_setup(
            api_types::CoordinationState::Ready,
            api_types::ExecutionSetupState::Ready,
            api_types::ExecutionGate::ReconciliationRequired,
        );
        scenarios.push(("execution_gate_reconciliation", fixture));

        let mut fixture = Fixture::base();
        let mut milestone = default_milestone("milestone-2", 1);
        milestone["latest_readiness"] = json!({
            "result": "not_ready",
            "reasons": [{"code": "baseline_check_definition_reconciliation_required"}],
        });
        fixture.milestones = vec![milestone];
        scenarios.push(("acceptance_contract_reconciliation", fixture));

        let mut fixture = Fixture::base();
        let mut milestone = default_milestone("milestone-8", 4);
        milestone["definition"]["id"] = json!("revision-8");
        milestone["definition"]["lifecycle"] = json!("proposed");
        fixture.milestones = vec![milestone];
        scenarios.push(("milestone_definition_approval", fixture));

        let mut fixture = Fixture::base();
        fixture.task_counts = json!({"blocked": 1});
        scenarios.push(("task_blocked_remediation", fixture));

        let mut fixture = Fixture::base();
        fixture.failed_task_count = 1;
        scenarios.push(("task_failure_remediation", fixture));

        let mut fixture = Fixture::base();
        let mut milestone = default_milestone("milestone-3", 1);
        milestone["current_checks"] = json!([{
            "id": "check-1",
            "version": 1,
            "required": true,
            "source_kind": "manual",
            "latest_result": null,
        }]);
        fixture.milestones = vec![milestone];
        scenarios.push(("manual_attestation_required", fixture));

        let mut fixture = Fixture::base();
        fixture.checks = json!({"failed": 1});
        scenarios.push(("validation_failure_remediation", fixture));

        let mut fixture = Fixture::base();
        fixture.checks = json!({"stale": 1});
        scenarios.push(("validation_required", fixture));

        let mut fixture = Fixture::base();
        fixture.pending_decision_ids = vec!["decision-1".to_owned()];
        scenarios.push(("decision_resolution", fixture));

        let mut fixture = Fixture::base();
        let mut milestone = default_milestone("milestone-4", 1);
        milestone["definition"]["content"]["evidence_requirements"] =
            json!([{"id": "req-1", "required": true}]);
        fixture.milestones = vec![milestone];
        scenarios.push(("evidence_required", fixture));

        let mut fixture = Fixture::base();
        fixture.milestones = vec![default_milestone("milestone-5", 1)];
        scenarios.push(("readiness_request", fixture));

        let mut fixture = Fixture::base();
        let mut milestone = default_milestone("milestone-6", 1);
        milestone["latest_readiness"] = json!({"result": "not_ready", "reasons": []});
        fixture.milestones = vec![milestone];
        scenarios.push(("readiness_blocked", fixture));

        let mut fixture = Fixture::base();
        let mut milestone = default_milestone("milestone-7", 1);
        milestone["latest_readiness"] = json!({"result": "ready", "reasons": []});
        fixture.milestones = vec![milestone];
        scenarios.push(("user_release", fixture));

        let mut fixture = Fixture::base();
        fixture.no_milestones = true;
        scenarios.push(("milestone_definition", fixture));

        let mut fixture = Fixture::base();
        fixture.stale = true;
        scenarios.push(("projection_reconciliation", fixture));

        let mut produced: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for (name, fixture) in &scenarios {
            let action = fixture.action();
            assert!(
                !action.route_or_operation.trim().is_empty(),
                "{name} produced an empty operation"
            );
            // registered_target panics with a clear message on a miss, so a
            // plain lookup is the assertion that this operation has a
            // service/route/UI target.
            let _ = registered_target(&action.route_or_operation);
            produced.insert(action.route_or_operation);
        }

        // Reverse direction: every registered operation must actually be
        // reachable from `next_action()` (several scenarios intentionally
        // share one operation under a different `code`, so this is a set
        // comparison, not a 1:1 scenario-to-registry count). A registry
        // entry that no branch can ever produce is stale and must be
        // deleted, not left to imply coverage that does not exist.
        for (operation, _) in REGISTRY {
            assert!(
                produced.contains(*operation),
                "REGISTRY names '{operation}' but no next_action() scenario produces it \
                 -- delete the stale entry or add the scenario that reaches it"
            );
        }

        let reconciliation_action = Fixture {
            reconciliation_required: true,
            ..Fixture::base()
        }
        .action();
        assert_eq!(
            reconciliation_action.route_or_operation,
            "project.reconciliation.resolve"
        );
        assert!(matches!(
            registered_target(&reconciliation_action.route_or_operation),
            NextActionTarget::Rest
        ));
    }

    #[test]
    fn unapproved_definition_revision_asks_the_user_before_worker_evidence() {
        // InkDrop: the Agent proposed definition revision 2, nobody approved
        // it, and the Overview answered with "Attach required evidence" for
        // the Worker plus a stale banner whose only button re-fetched the
        // same projection.
        let mut fixture = Fixture::base();
        let mut milestone = default_milestone("milestone-1", 5);
        milestone["definition"]["id"] = json!("revision-2");
        milestone["definition"]["lifecycle"] = json!("proposed");
        milestone["definition"]["content"]["evidence_requirements"] =
            json!([{"id": "req-1", "required": true}]);
        fixture.milestones = vec![milestone];
        fixture.checks = json!({"missing": 1});
        fixture.stale = true;

        let action = fixture.action();
        assert_eq!(action.code, "milestone_definition_approval");
        assert_eq!(action.required_principal, "user");
        assert_eq!(action.target_type, "milestone_revision");
        assert_eq!(action.target_id, "revision-2");
        assert_eq!(
            action.route_or_operation,
            "project.milestone.revision.transition"
        );
        assert_eq!(action.expected_version, Some(5));
        assert!(action.blocking);

        let mut approved = Fixture::base();
        let mut milestone = default_milestone("milestone-1", 5);
        milestone["definition"]["id"] = json!("revision-2");
        milestone["definition"]["lifecycle"] = json!("approved");
        approved.milestones = vec![milestone];
        approved.checks = json!({"missing": 1});
        assert_eq!(approved.action().code, "validation_required");
    }

    #[test]
    fn legacy_baseline_states_do_not_create_project_next_actions() {
        for gate in [
            api_types::ExecutionGate::BaselineApprovalRequired,
            api_types::ExecutionGate::PreBaselineReadOnly,
        ] {
            let mut fixture = Fixture::base();
            fixture.execution_setup = base_execution_setup(
                api_types::CoordinationState::Ready,
                api_types::ExecutionSetupState::Ready,
                gate,
            );
            assert!(next_action(NextActionContext {
                project_id: "project-1",
                project_version: 7,
                charter_setup_required: fixture.charter_setup_required,
                no_milestones: fixture.no_milestones,
                execution_setup: &fixture.execution_setup,
                milestones: &fixture.milestones,
                documents: &fixture.documents,
                releases: &fixture.releases,
                pending_decision_ids: &fixture.pending_decision_ids,
                task_counts: &fixture.task_counts,
                failed_task_count: fixture.failed_task_count,
                checks: &fixture.checks,
                reconciliation_required: fixture.reconciliation_required,
                stale: fixture.stale,
            })
            .is_none());
        }
    }
}
