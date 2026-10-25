//! Load canonical records for the Project Overview projection.

use super::*;

const NO_APPROVED_CHARTER_VISION: &str = "No approved Charter vision recorded.";

macro_rules! try_get {
    ($row:expr, $ty:ty, $column:expr) => {
        $row.try_get::<$ty, _>($column).map_err(sql_error)?
    };
}

pub(super) async fn load_current_charter(
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

pub(super) async fn load_active_milestones(
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

pub(super) async fn load_current_acceptance_checks(
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

pub(super) async fn load_task_counts(
    state: &AppState,
    project: &db::Project,
    milestone_id: Option<&str>,
) -> ApiResult<Value> {
    let project_id = &project.id;
    let workflow =
        services::workflow::engine::WorkflowEngine::resolve_workflow(&project.workflow_definition);
    let rows = if let Some(milestone_id) = milestone_id {
        sqlx::query(
            "SELECT t.status, t.condition_json
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
            "SELECT status, condition_json FROM task
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
        // An unreadable condition is the same typed unknown park every row
        // reader shows; one such Task never fails the whole overview.
        let condition = db::task_condition::decode_or_unknown(
            &row.try_get::<String, _>("condition_json")
                .map_err(sql_error)?,
        );
        // A blank blocked record was never counted as blocked.
        let blocked = condition.read().interruption_present
            && !condition
                .evidence()
                .blocked_json
                .as_deref()
                .is_some_and(|value| value.trim().is_empty());
        match classify_status(&status, &workflow) {
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

pub(super) async fn load_check_summary(
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

pub(super) async fn load_document_freshness(
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
pub(super) async fn load_pending_decisions(
    state: &AppState,
    project_id: &str,
) -> ApiResult<Vec<Value>> {
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
pub(super) async fn load_decisions(state: &AppState, project_id: &str) -> ApiResult<Vec<Value>> {
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

pub(super) async fn load_evidence(
    state: &AppState,
    project_id: &str,
) -> ApiResult<(Vec<Value>, bool)> {
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

pub(super) async fn load_latest_readiness(
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

pub(super) async fn load_releases(
    state: &AppState,
    project_id: &str,
) -> ApiResult<(Vec<Value>, bool)> {
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

pub(super) async fn load_watermark(
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

pub(super) async fn load_failed_task_count(state: &AppState, project_id: &str) -> ApiResult<i64> {
    sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM task
         WHERE project_id = ? AND deleted_at IS NULL
           AND (lower(status) LIKE '%failed%'
                OR lower(status) LIKE '%error%'
                OR json_extract(condition_json, '$.evidence.presentation.hard_failure') = 1)",
    )
    .bind(project_id)
    .fetch_one(state.db.pool())
    .await
    .map_err(sql_error)
}

pub(super) async fn load_reconciliation_required(
    state: &AppState,
    project_id: &str,
) -> ApiResult<bool> {
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
