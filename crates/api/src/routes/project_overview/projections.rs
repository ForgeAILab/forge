//! Load canonical records for the Project Overview projection.

use super::*;

const NO_APPROVED_CHARTER_VISION: &str = "No approved Charter vision recorded.";

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

    let row = state
        .db
        .overview_current_charter(revision_id, charter_id, &project.id)
        .await
        .map_err(sql_error)?;

    let Some(row) = row else {
        return Ok((None, NO_APPROVED_CHARTER_VISION.to_owned(), true));
    };

    let content: Value = row_json(row.content_json(), "content_json")?;
    let vision = content
        .pointer("/identity/one_line_vision")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(NO_APPROVED_CHARTER_VISION)
        .to_owned();
    let principal = principal_value(
        row.author_type().map_err(sql_error)?.as_str(),
        row.author_id().map_err(sql_error)?.as_deref(),
    )
    .ok_or_else(|| ApiError::internal("invalid Charter author principal"))?;
    let source_refs = row_json_array(row.source_refs_json(), "source_refs_json")?;
    let content_digest = row.content_digest().map_err(sql_error)?;
    let rendered_view = row.rendered_view().map_err(sql_error)?;
    let render_version = row.render_version().map_err(sql_error)?;
    let rendered_digest = row.rendered_digest().map_err(sql_error)?;
    let approved_at = row.approved_at().map_err(sql_error)?;
    let charter_stale = content_digest.trim().is_empty()
        || rendered_view.trim().is_empty()
        || render_version.trim().is_empty()
        || rendered_digest.trim().is_empty()
        || approved_at.is_none();
    let charter = json!({
        "id": row.revision_id().map_err(sql_error)?,
        "charter_id": row.charter_id().map_err(sql_error)?,
        "revision_number": row.revision().map_err(sql_error)?,
        "base_revision_id": row.base_revision_id().map_err(sql_error)?,
        "lifecycle": "approved",
        "project_mode": row.project_mode().map_err(sql_error)?,
        "maturity": row.maturity().map_err(sql_error)?,
        "schema_version": row.schema_version().map_err(sql_error)?,
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
            "change_summary": row.change_summary().map_err(sql_error)?,
            "material_diff": Value::Null,
        },
        "readiness": Value::Null,
        "approved_at": approved_at,
        "superseded_by_revision_id": Value::Null,
        "created_at": row.created_at().map_err(sql_error)?,
    });
    Ok((Some(charter), vision, charter_stale))
}

pub(super) async fn load_active_milestones(
    state: &AppState,
    project_id: &str,
) -> ApiResult<(Vec<(Value, Value)>, bool)> {
    let rows = state
        .db
        .overview_active_milestones(project_id)
        .await
        .map_err(sql_error)?;

    let mut stale = false;
    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        let milestone_row_id = row.id().map_err(sql_error)?;
        let Some(revision_id) = row.current_definition_revision_id().map_err(sql_error)? else {
            stale = true;
            continue;
        };
        let Some(revision) = state
            .db
            .overview_milestone_revision(&revision_id, &milestone_row_id)
            .await
            .map_err(sql_error)?
        else {
            stale = true;
            continue;
        };

        let principal = match principal_value(
            revision.author_type().map_err(sql_error)?.as_str(),
            revision.author_id().map_err(sql_error)?.as_deref(),
        ) {
            Some(value) => value,
            None => {
                stale = true;
                continue;
            }
        };
        let charter_revision_id = revision.charter_revision_id().map_err(sql_error)?;
        let charter_id = revision.charter_id().map_err(sql_error)?;
        let charter_content_digest = revision.charter_content_digest().map_err(sql_error)?;
        let charter_render_version = revision.charter_render_version().map_err(sql_error)?;
        let charter_render_digest = revision.charter_render_digest().map_err(sql_error)?;
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
        let rendered_view = revision.rendered_view().map_err(sql_error)?;
        if rendered_view.trim().is_empty() {
            stale = true;
        }
        if revision
            .content_digest()
            .map_err(sql_error)?
            .trim()
            .is_empty()
            || revision
                .rendered_digest()
                .map_err(sql_error)?
                .trim()
                .is_empty()
            || revision
                .render_version()
                .map_err(sql_error)?
                .trim()
                .is_empty()
        {
            stale = true;
        }
        if revision.lifecycle().map_err(sql_error)? != "approved" {
            stale = true;
        }
        let definition_name = revision
            .display_label()
            .map_err(sql_error)?
            .unwrap_or(row.milestone_key().map_err(sql_error)?);
        let definition_content = json!({
            "name": definition_name,
            "outcome": revision.outcome().map_err(sql_error)?,
            "included_scope": row_json_array_from(revision.included_scope_json(), "included_scope_json")?,
            "excluded_scope": row_json_array_from(revision.excluded_scope_json(), "excluded_scope_json")?,
            "charter_revision": charter_revision,
            "document_revisions": row_json_array_from(revision.document_revisions_json(), "document_revisions_json")?,
            "task_ids": row_json_array_from(revision.task_selection_json(), "task_selection_json")?,
            "dependencies": row_json_array_from(revision.dependencies_json(), "dependencies_json")?,
            "risks": row_json_array_from(revision.risks_json(), "risks_json")?,
            "acceptance_checks": row_json_array_from(revision.acceptance_checks_json(), "acceptance_checks_json")?,
            "evidence_requirements": row_json_array_from(revision.evidence_requirements_json(), "evidence_requirements_json")?,
            "known_issues": row_json_array_from(revision.known_issues_json(), "known_issues_json")?,
            "target_date": Value::Null,
        });
        let reasons = concat_json_arrays(
            row_json_array_from(row.blocker_reason_json(), "blocker_reason_json")?,
            row_json_array_from(row.stale_reason_json(), "stale_reason_json")?,
            row_json_array_from(
                row.reconciliation_reason_json(),
                "reconciliation_reason_json",
            )?,
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
            "id": row.id().map_err(sql_error)?,
            "project_id": row.project_id().map_err(sql_error)?,
            "milestone_sequence": row.milestone_sequence().map_err(sql_error)?,
            "canonical_id": row.milestone_key().map_err(sql_error)?,
            "display_label": row.display_label().map_err(sql_error)?,
            "definition_revision_id": revision_id,
            "lifecycle": row.lifecycle().map_err(sql_error)?,
            "projection_reasons": reasons,
            "version": row.version().map_err(sql_error)?,
            "created_at": row.created_at().map_err(sql_error)?,
            "updated_at": row.updated_at().map_err(sql_error)?,
        });
        let definition = json!({
            "id": revision.id().map_err(sql_error)?,
            "milestone_id": revision.milestone_id().map_err(sql_error)?,
            "project_id": project_id,
            "revision_number": revision.revision().map_err(sql_error)?,
            "base_revision_id": revision.base_revision_id().map_err(sql_error)?,
            "lifecycle": revision.lifecycle().map_err(sql_error)?,
            "schema_version": revision.schema_version().map_err(sql_error)?,
            "content": definition_content,
            "rendered_view": rendered_view,
            "render_version": revision.render_version().map_err(sql_error)?,
            "content_digest": revision.content_digest().map_err(sql_error)?,
            "render_digest": revision.rendered_digest().map_err(sql_error)?,
            "provenance": {
                "author": principal,
                "profile_revision": Value::Null,
                "operating_skill_revision": Value::Null,
                "source_refs": row_json_array_from(revision.source_refs_json(), "source_refs_json")?,
                "change_summary": revision.change_summary().map_err(sql_error)?,
                "material_diff": Value::Null,
            },
            "created_at": revision.created_at().map_err(sql_error)?,
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
    let rows = state
        .db
        .overview_acceptance_checks(project_id, milestone_id, definition_revision_id)
        .await
        .map_err(sql_error)?;

    rows.into_iter()
        .map(|row| {
            let outcome = row.outcome;
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
                "id": row.id,
                "description": row.description,
                "required": row.required != 0,
                "source_kind": row.source_kind,
                "expected_result": row.expected_result,
                "version": row.version,
                "latest_result": latest_result,
                "latest_result_id": row.result_id,
                "latest_result_digest": row.input_digest,
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
        state
            .db
            .overview_milestone_tasks(project_id, milestone_id)
            .await
            .map_err(sql_error)?
    } else {
        state
            .db
            .overview_project_tasks(project_id)
            .await
            .map_err(sql_error)?
    };

    let mut counts = Counts::default();
    for row in rows {
        counts.total += 1;
        let status = row.status;
        // A Task keeps its workflow status when an execution fails, so status
        // alone reports stalled work as active. The blocked record is what the
        // user is actually waiting on, and a terminal Task has moved past it.
        // An unreadable condition is the same typed unknown park every row
        // reader shows; one such Task never fails the whole overview.
        let condition = db::task_condition::decode_or_unknown(&row.condition_json);
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
        state
            .db
            .overview_milestone_checks(project_id, milestone_id)
            .await
            .map_err(sql_error)?
    } else {
        state
            .db
            .overview_project_checks(project_id)
            .await
            .map_err(sql_error)?
    };

    let mut summary = CheckSummary::default();
    for row in rows {
        let required = row.required().map_err(sql_error)? != 0;
        if !required {
            continue;
        }
        summary.required_total += 1;
        match row.outcome().map_err(sql_error)?.as_deref() {
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
    let rows = state
        .db
        .overview_document_freshness(project_id)
        .await
        .map_err(sql_error)?;
    let mut stale = false;
    let mut documents = Vec::new();
    for row in rows {
        let approved = row.current_approved_revision_id().map_err(sql_error)?;
        let Some(kind) = document_kind(row.kind().map_err(sql_error)?) else {
            stale = true;
            continue;
        };
        let document_lifecycle = row.document_lifecycle().map_err(sql_error)?;
        let draft = row.current_draft_revision_id().map_err(sql_error)?;
        let approved_digest = row.approved_digest().map_err(sql_error)?;
        let approved_lifecycle = row.approved_lifecycle().map_err(sql_error)?;
        let working_digest = row.working_digest().map_err(sql_error)?;
        let working_lifecycle = row.working_lifecycle().map_err(sql_error)?;
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
            "document_id": row.id().map_err(sql_error)?,
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
    let rows = state
        .db
        .overview_pending_decisions(project_id)
        .await
        .map_err(sql_error)?;

    let mut pending = Vec::with_capacity(rows.len());
    for row in rows {
        let id = row.id().map_err(sql_error)?;
        let mut reasons: Vec<String> = Vec::new();

        let question = row.question().map_err(sql_error)?;
        if question.trim().is_empty() {
            reasons.push("question is empty".to_owned());
        }
        let options: Vec<String> =
            serde_json::from_str(&row.options_json().map_err(sql_error)?).unwrap_or_default();
        let selected_outcome = row.selected_outcome().map_err(sql_error)?;
        let rationale = row.rationale().map_err(sql_error)?;
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

        let context_value: Value = serde_json::from_str(&row.context_json().map_err(sql_error)?)
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

        let principal_type = row.principal_type().map_err(sql_error)?;
        let principal_id = row.principal_id().map_err(sql_error)?;
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
            "lifecycle": row.lifecycle().map_err(sql_error)?,
            "version": row.version().map_err(sql_error)?,
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
            "created_at": row.created_at().map_err(sql_error)?,
            "updated_at": row.updated_at().map_err(sql_error)?,
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
    let rows = state
        .db
        .overview_decisions(project_id)
        .await
        .map_err(sql_error)?;

    let replacement_rows = state
        .db
        .overview_decision_replacements(project_id)
        .await
        .map_err(sql_error)?;
    let mut replacement_state = HashMap::<String, String>::new();
    for row in replacement_rows {
        let Some(superseded_id) = row.supersedes_decision_id().map_err(sql_error)? else {
            continue;
        };
        replacement_state.insert(superseded_id, row.state().map_err(sql_error)?);
    }

    let mut decisions = Vec::with_capacity(rows.len());
    for row in rows {
        let id = row.id().map_err(sql_error)?;
        let state = effective_decision_state(
            &row.state().map_err(sql_error)?,
            replacement_state.get(&id).map(String::as_str),
        );
        let principal = principal_value(
            row.principal_type().map_err(sql_error)?.as_str(),
            Some(row.principal_id().map_err(sql_error)?.as_str()),
        )
        .ok_or_else(|| ApiError::internal("invalid persisted Decision principal"))?;
        let affected = row_json(row.affected_records_json(), "affected_records_json")?;
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
            "project_id": row.project_id().map_err(sql_error)?,
            "state": state,
            "question": row.question().map_err(sql_error)?,
            "context": Some(row.context_json().map_err(sql_error)?),
            "options": row_json_array(row.options_json(), "options_json")?,
            "selected_outcome": row.selected_outcome().map_err(sql_error)?,
            "rationale": row.rationale().map_err(sql_error)?,
            "decision_maker": principal,
            "decision_class": row.decision_class().map_err(sql_error)?,
            "authority_basis": Some(row.authority_basis().map_err(sql_error)?),
            "affected_artifact_refs": affected_artifact_refs,
            "affected_task_ids": affected_task_ids,
            "affected_milestone_ids": affected_milestone_ids,
            "supersedes_id": row.supersedes_decision_id().map_err(sql_error)?,
            "provenance": row_json_array(row.source_refs_json(), "source_refs_json")?,
            "created_at": row.created_at().map_err(sql_error)?,
            "effective_at": row.created_at().map_err(sql_error)?,
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
    let rows = state
        .db
        .overview_evidence(project_id)
        .await
        .map_err(sql_error)?;
    let mut evidence = Vec::new();
    let mut stale = false;
    for row in rows {
        let Some(kind) = evidence_kind(row.evidence_kind().map_err(sql_error)?.as_deref()) else {
            stale = true;
            continue;
        };
        let Some(checksum) = row.checksum().map_err(sql_error)? else {
            stale = true;
            continue;
        };
        if checksum.trim().is_empty() {
            stale = true;
            continue;
        }
        let availability_value = row.availability().map_err(sql_error)?;
        let Some(availability) = evidence_availability(availability_value.as_str()) else {
            stale = true;
            continue;
        };
        let Some(author) = principal_value(
            row.author_type().map_err(sql_error)?.as_str(),
            row.author_id().map_err(sql_error)?.as_deref(),
        ) else {
            stale = true;
            continue;
        };
        evidence.push(json!({
            "id": row.id().map_err(sql_error)?,
            "project_id": row.project_id().map_err(sql_error)?,
            "asset_id": row.asset_id().map_err(sql_error)?,
            "task_id": row.task_id().map_err(sql_error)?,
        "source_task_id": row.source_task_id().map_err(sql_error)?,
        "source_run_id": row.source_execution_id().map_err(sql_error)?,
        "source_validation_id": row.source_validation_id().map_err(sql_error)?,
        "source_task_version": row.source_task_version().map_err(sql_error)?,
        "source_context_digest": row.source_context_digest().map_err(sql_error)?,
        "source_definition_revision_id": row.source_definition_revision_id().map_err(sql_error)?,
        "milestone_id": row.milestone_id().map_err(sql_error)?,
            "acceptance_check_ids": row_json_array_from(row.acceptance_check_ids_json(), "acceptance_check_ids_json")?,
            "caption": row.caption().map_err(sql_error)?.unwrap_or_default(),
            "kind": kind,
            "checksum": checksum,
            "availability": availability,
            "author": author,
            "captured_at": row.created_at().map_err(sql_error)?,
            "version": row.version().map_err(sql_error)?,
            "created_at": row.created_at().map_err(sql_error)?,
            "removed_at": row.deleted_at().map_err(sql_error)?,
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
    let row = state
        .db
        .overview_latest_readiness(project_id, milestone_id)
        .await
        .map_err(sql_error)?;
    let Some(row) = row else {
        return Ok((None, false));
    };
    let definition_revision_id = row.definition_revision_id().map_err(sql_error)?;
    let source_event_watermark = row.event_watermark().map_err(sql_error)?;
    let stale = definition_revision_id != current_definition_revision_id
        || source_event_watermark.trim().is_empty();
    let input_manifest = typed_json_array::<api_types::ReadinessInput>(
        row.input_manifest_json(),
        "input_manifest_json",
    )?;
    let reasons = typed_json_array::<api_types::ReadinessReason>(
        row.blocking_reasons_json(),
        "blocking_reasons_json",
    )?;
    let check_results = typed_json_array::<api_types::ValidationResult>(
        row.check_results_json(),
        "check_results_json",
    )?;
    let waiver_ids = string_array_from(row.waiver_manifest_json(), "waiver_manifest_json")?;
    let evidence_attachment_ids = readiness_evidence_ids(&row_json(
        row.evidence_manifest_json(),
        "evidence_manifest_json",
    )?)?;
    let persisted_evidence_manifest =
        row_json(row.evidence_manifest_json(), "evidence_manifest_json")?;
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
    let commit_build_check_context =
        string_array_from(row.commit_context_json(), "commit_context_json")?;
    let result = row.outcome().map_err(sql_error)?;
    let computing_policy_revision = row.computing_policy_revision().map_err(sql_error)?;
    let readiness_digest = row.readiness_digest().map_err(sql_error)?;
    let expected_milestone_version = row.expected_milestone_version().map_err(sql_error)?;
    let Some(requesting_principal) = principal_value(
        row.principal_type().map_err(sql_error)?.as_str(),
        Some(row.principal_id().map_err(sql_error)?.as_str()),
    ) else {
        return Ok((None, true));
    };
    let authorization_basis = row.authorization_basis().map_err(sql_error)?;
    let authorization_action = row.authorization_action().map_err(sql_error)?;
    let authorization_event = row.explicit_event().map_err(sql_error)?;
    let authorization_occurred_at = row.authorization_occurred_at().map_err(sql_error)?;
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
            "id": row.id().map_err(sql_error)?,
            "project_id": row.project_id().map_err(sql_error)?,
            "milestone_id": row.milestone_id().map_err(sql_error)?,
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
            "computed_at": row.created_at().map_err(sql_error)?,
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
    let rows = state
        .db
        .overview_releases(project_id)
        .await
        .map_err(sql_error)?;
    let mut releases = Vec::new();
    let mut stale = false;
    for row in rows {
        // The release wire contract intentionally contains full immutable
        // references.  A malformed/incomplete persisted row is not silently
        // filled with guessed digests; it is omitted from the projection.
        let Some(charter_revision_id) = row.charter_revision_id().map_err(sql_error)? else {
            stale = true;
            continue;
        };
        let Some(charter_id) = row.historic_charter_id().map_err(sql_error)? else {
            stale = true;
            continue;
        };
        let Some(charter_digest) = row.charter_content_digest().map_err(sql_error)? else {
            stale = true;
            continue;
        };
        let Some(charter_render_version) = row.charter_render_version().map_err(sql_error)? else {
            stale = true;
            continue;
        };
        let Some(charter_render_digest) = row.charter_render_digest().map_err(sql_error)? else {
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
        let decision_refs = parse_release_refs::<api_types::ReleaseDecisionReference>(
            &row.decision_ids_json().map_err(sql_error)?,
        );
        let task_refs = parse_release_refs::<api_types::ReleaseTaskReference>(
            &row.task_references_json().map_err(sql_error)?,
        );
        let validation_refs = parse_release_refs::<api_types::ReleaseValidationReference>(
            &row.validation_references_json().map_err(sql_error)?,
        );
        let evidence_pins = parse_release_refs::<api_types::EvidencePin>(
            &row.evidence_references_json().map_err(sql_error)?,
        );
        if decision_refs.is_none()
            || task_refs.is_none()
            || validation_refs.is_none()
            || evidence_pins.is_none()
        {
            stale = true;
            continue;
        }
        let Some(released_by) = principal_value(
            row.releasing_principal_type().map_err(sql_error)?.as_str(),
            Some(row.releasing_principal_id().map_err(sql_error)?.as_str()),
        ) else {
            stale = true;
            continue;
        };
        let milestone_definition_revision_id = row.milestone_revision_id().map_err(sql_error)?;
        let milestone_definition_digest = row.milestone_definition_digest().map_err(sql_error)?;
        let readiness_snapshot_id = row.readiness_snapshot_id().map_err(sql_error)?;
        let readiness_digest = row.readiness_digest().map_err(sql_error)?;
        let snapshot_digest = row.snapshot_digest().map_err(sql_error)?;
        let release_identifier = row.release_identifier().map_err(sql_error)?;
        let expected_milestone_version = row.expected_milestone_version().map_err(sql_error)?;
        let source_event_watermark = row.source_event_watermark().map_err(sql_error)?;
        let authorization_basis = row.authorization_basis().map_err(sql_error)?;
        let authorization_action = row.authorization_action().map_err(sql_error)?;
        let authorization_event = row.explicit_event().map_err(sql_error)?;
        let authorization_occurred_at = row.authorization_occurred_at().map_err(sql_error)?;
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
            "schema_version": row.schema_version().map_err(sql_error)?,
            "project_id": project_id,
            "milestone_id": row.milestone_id().map_err(sql_error)?,
            "milestone_canonical_id": row.milestone_key().map_err(sql_error)?,
            "release_revision": row.release_revision().map_err(sql_error)?,
            "release_identity": release_identifier,
            "milestone_definition_revision_id": milestone_definition_revision_id,
            "milestone_definition_digest": milestone_definition_digest,
            "expected_milestone_version": expected_milestone_version,
            "display_label": row.display_label().map_err(sql_error)?,
            "summary": row.summary().map_err(sql_error)?,
            "changelog": string_array_from(row.changelog(), "changelog")?,
            "known_issues": row_json_array_from(row.known_issues_json(), "known_issues_json")?,
            "readiness_snapshot_id": readiness_snapshot_id,
            "readiness_digest": readiness_digest,
            "source_event_watermark": source_event_watermark,
            "charter_revision": charter_ref,
            "document_revisions": row_json_array_from(row.document_revisions_json(), "document_revisions_json")?,
            "included_decisions": decision_refs,
            "included_tasks": task_refs,
            "validation_results": validation_refs,
            "repository_references": string_array_from(row.git_references_json(), "git_references_json")?,
            "evidence_pins": evidence_pins,
            "waived_check_ids": string_array_from(row.waivers_json(), "waivers_json")?,
            "released_by": released_by,
            "authorization": {
                "principal": released_by,
                "authorization_basis": authorization_basis,
                "action": authorization_action,
                "event_id": authorization_event,
                "occurred_at": authorization_occurred_at,
            },
            "released_at": row.created_at().map_err(sql_error)?,
            "idempotency_key": row.idempotency_key().map_err(sql_error)?,
            "snapshot_digest": snapshot_digest,
        });
        releases.push(json!({
            "id": row.id().map_err(sql_error)?,
            "project_id": project_id,
            "milestone_id": row.milestone_id().map_err(sql_error)?,
            "release_sequence": row.release_sequence().map_err(sql_error)?,
            "release_identity": release_identifier,
            "snapshot": snapshot,
            "version": row.release_revision().map_err(sql_error)?,
            "created_at": row.created_at().map_err(sql_error)?,
        }));
    }
    Ok((releases, stale))
}

pub(super) async fn load_watermark(
    state: &AppState,
    project_id: &str,
    project_work_epoch: i64,
) -> ApiResult<String> {
    let row = state
        .db
        .overview_event_watermark(project_id)
        .await
        .map_err(sql_error)?;
    Ok(match row {
        Some(id) => id,
        None => format!("project-work-epoch:{project_work_epoch}"),
    })
}

pub(super) async fn load_failed_task_count(state: &AppState, project_id: &str) -> ApiResult<i64> {
    state
        .db
        .overview_failed_task_count(project_id)
        .await
        .map_err(sql_error)
}

pub(super) async fn load_reconciliation_required(
    state: &AppState,
    project_id: &str,
) -> ApiResult<bool> {
    state
        .db
        .overview_reconciliation_required(project_id)
        .await
        .map_err(sql_error)
}
