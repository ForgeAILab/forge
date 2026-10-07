//! Reader parity over stored rows. Each row is written as the pre-condition
//! server stored it (raw legacy columns, real Review, execution and role
//! rows), its condition is rederived by the invariant check, and the Task is
//! read back through the real snapshot loader. The pre-condition readers (the
//! two legacy files, and ports of `777b5f1f`'s awaiting-human rule and slot
//! SQL) must then agree with the condition readers on every row, except for
//! the differences named in `EXPECTED`.
use super::helpers::*;
use super::*;
use serde_json::json;

#[allow(dead_code, unused_imports)]
mod legacy_diag {
    include!("../../task_diagnostics_legacy.rs");
}

fn gate_decided(entries: &[db::TransitionLog], state: &str, entered_at: &str) -> bool {
    entries.iter().any(|entry| {
        entry.from_state == state
            && entry.created_at.as_str() >= entered_at
            && matches!(
                entry.bridge.bridge_kind,
                Some(
                    api_types::TransitionBridgeKind::GateApproved
                        | api_types::TransitionBridgeKind::GateRejected
                )
            )
    })
}

/// Verbatim port of 777b5f1f `TaskService::is_task_awaiting_human`.
async fn base_awaiting_human(db: &SqliteDb, task: &Task) -> std::result::Result<bool, String> {
    if task.blocked_json.is_some() {
        return Ok(true);
    }
    if db::TaskStepRepo::entry_hooks_pending(db, &task.id)
        .await
        .map_err(|e| e.to_string())?
    {
        return Ok(false);
    }
    let metadata = db::TaskMetadata::parse(task.metadata_json.as_deref())
        .map_err(|error| format!("ERR invalid task metadata: {error}"))?;
    if metadata
        .extra
        .get("awaiting_human")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        return Ok(true);
    }
    if task.status == "review" {
        let latest_review = ReviewRepo::list_by_task(db, &task.id)
            .await
            .unwrap()
            .into_iter()
            .max_by_key(|review| review.attempt_number);
        if latest_review
            .as_ref()
            .is_some_and(|review| review.status == ReviewStatus::AwaitingHuman)
        {
            return Ok(true);
        }
        if latest_review
            .as_ref()
            .is_some_and(|review| review.status == ReviewStatus::Failed)
            && sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM execution WHERE task_id = ? AND status = 'running'",
            )
            .bind(&task.id)
            .fetch_one(db.pool())
            .await
            .unwrap()
                == 0
        {
            return Ok(true);
        }
    }
    let project = ProjectRepo::get_by_id(db, &task.project_id)
        .await
        .unwrap()
        .unwrap();
    let workflow = WorkflowEngine::resolve_workflow_for_task(
        task,
        &project.workflow_definition,
        &Actor::system(api_types::SystemComponent::General),
    );
    let Some(state) = workflow
        .states
        .iter()
        .find(|state| state.name == task.status)
    else {
        return Ok(false);
    };
    if task.status == "planning" {
        let Some(role_name) = state.role.as_deref() else {
            return Ok(false);
        };
        let assignment = TaskRoleAssignmentRepo::get_by_task_and_role(db, &task.id, role_name)
            .await
            .unwrap();
        return Ok(assignment.as_ref().is_some_and(|assignment| {
            assignment.assignee_type == Some(db::AssigneeKind::User)
                && assignment.assignee_id.is_some()
        }));
    }
    if state.kind != api_types::StateKind::Gate {
        return Ok(false);
    }
    let transition_log = TransitionLogRepo::list_by_task(db, &task.id).await.unwrap();
    let entered_at = transition_log
        .iter()
        .rev()
        .find(|entry| entry.to_state == task.status)
        .map(|entry| entry.created_at.as_str())
        .unwrap_or(task.created_at.as_str());
    let decided = gate_decided(&transition_log, &task.status, entered_at);
    if let Some(gate_config) = state
        .gate_config
        .as_ref()
        .filter(|gate_config| gate_config.requires_user_approval())
    {
        if gate_config.optional_when_unassigned() {
            let Some(role_name) = state.role.as_deref() else {
                return Ok(false);
            };
            let assignment = TaskRoleAssignmentRepo::get_by_task_and_role(db, &task.id, role_name)
                .await
                .unwrap();
            let assigned = assignment.as_ref().is_some_and(|assignment| {
                assignment.assignee_type.is_some() && assignment.assignee_id.is_some()
            });
            if !assigned {
                return Ok(false);
            }
        }
        return Ok(!decided);
    }
    let Some(role_name) = state.role.as_deref() else {
        return Ok(false);
    };
    let role_assignments = TaskRoleAssignmentRepo::list_by_task(db, &task.id)
        .await
        .unwrap();
    let Some(assignment) = role_assignments
        .iter()
        .find(|assignment| assignment.role_name == role_name)
    else {
        return Ok(false);
    };
    if assignment.assignee_type != Some(db::AssigneeKind::User) {
        return Ok(false);
    }
    Ok(!decided)
}

const BASE_PARKED: &str = "SELECT COALESCE((t.blocked_json IS NOT NULL OR t.failed_json IS NOT NULL
 OR CASE WHEN json_valid(t.metadata_json) THEN json_extract(t.metadata_json, '$.dispatch_disposition.capability') IN ('machine_capacity', 'project_capacity') AND NOT EXISTS (SELECT 1 FROM execution e WHERE e.task_id = t.id AND e.status = 'running') ELSE 0 END
 OR CASE WHEN json_valid(t.metadata_json) THEN json_type(t.metadata_json, '$.environment_wait') IS NOT NULL ELSE 0 END
 OR CASE WHEN json_valid(t.error_annotation) THEN COALESCE(json_extract(t.error_annotation, '$.type') IN (SELECT value FROM json_each(?)), 0) ELSE 0 END
 OR COALESCE((SELECT r.status = 'awaiting_human' FROM review r WHERE r.task_id = t.id ORDER BY r.attempt_number DESC, r.created_at DESC, r.id DESC LIMIT 1), 0)),0) FROM task t WHERE t.id=?";
const HEAD_PARKED: &str = "SELECT COALESCE((json_extract(t.condition_json,'$.evidence.presentation.interruption_present') = 1
 OR json_extract(t.condition_json,'$.evidence.presentation.hard_failure') = 1
 OR (json_extract(t.condition_json,'$.evidence.presentation.refusal.capability') IN ('machine_capacity', 'project_capacity') AND NOT EXISTS (SELECT 1 FROM execution e WHERE e.task_id = t.id AND e.status = 'running'))
 OR json_extract(t.condition_json,'$.evidence.presentation.environment_recorded')=1
 OR COALESCE(json_extract(t.condition_json,'$.evidence.presentation.slot_blocker'),0)
 OR COALESCE(json_extract(t.condition_json,'$.evidence.presentation.review_wait'),0)),0) FROM task t WHERE t.id=?";

struct Shape {
    label: &'static str,
    status: &'static str,
    annotation: Option<String>,
    blocked: Option<String>,
    failed: Option<String>,
    barrier: Option<String>,
    metadata: Option<String>,
    extra: &'static str,
}

fn shape(label: &'static str, status: &'static str) -> Shape {
    Shape {
        label,
        status,
        annotation: None,
        blocked: None,
        failed: None,
        barrier: None,
        metadata: None,
        extra: "",
    }
}
fn ann(kind: &str) -> Option<String> {
    Some(json!({"type":kind,"blocking_reason":"fixture reason","blocked_by":"system:test","blocked_at":"2026-10-01T00:00:00Z","blocked_execution_id":null,"artifact":null,"message":format!("{kind} happened")}).to_string())
}

fn shapes() -> Vec<Shape> {
    let mut v = Vec::new();
    v.push(shape("clear_in_progress", "in_progress"));
    v.push(shape("clear_todo", "todo"));
    v.push(shape("clear_review", "review"));
    v.push(shape("clear_planning", "planning"));
    v.push(shape("clear_merging", "merging"));
    v.push(shape("done", "done"));
    v.push(Shape { annotation: ann("manual_stop"), blocked: Some(json!({"kind":"manual_stop","reason":"held by user","created_at":"2026-10-01T00:00:00Z"}).to_string()), ..shape("blocked_manual_stop", "in_progress") });
    v.push(Shape { blocked: Some(json!({"kind":"retry_exhausted","reason":"retries exhausted","created_at":"2026-10-01T00:00:00Z"}).to_string()), ..shape("blocked_retry_exhausted", "in_progress") });
    v.push(Shape { blocked: Some(json!({"kind":"review_gate_failed","reason":"review budget","created_at":"2026-10-01T00:00:00Z"}).to_string()), annotation: ann("review_budget_exhausted"), ..shape("blocked_review_gate", "review") });
    v.push(Shape { blocked: Some(json!({"kind":"dependency_cancelled","reason":"dep cancelled","created_at":"2026-10-01T00:00:00Z"}).to_string()), ..shape("blocked_dep_cancelled", "todo") });
    v.push(Shape { failed: Some(json!({"kind":"executor_failed","reason":"executor crashed","created_at":"2026-10-01T00:00:00Z","execution_id":"e-1"}).to_string()), ..shape("failed_executor", "in_progress") });
    v.push(Shape { failed: Some(json!({"kind":"merge_conflict","reason":"conflict","created_at":"2026-10-01T00:00:00Z"}).to_string()), ..shape("failed_merge_conflict", "merge_failed") });
    v.push(Shape {
        failed: Some(
            json!({"kind":"executor_failed","reason":"f","created_at":"2026-10-01T00:00:00Z"})
                .to_string(),
        ),
        blocked: Some(
            json!({"kind":"manual_stop","reason":"b","created_at":"2026-10-01T00:00:00Z"})
                .to_string(),
        ),
        ..shape("failed_and_blocked", "in_progress")
    });
    v.push(Shape {
        failed: Some(
            json!({"kind":"executor_failed","reason":"f","created_at":"2026-10-01T00:00:00Z"})
                .to_string(),
        ),
        annotation: ann("workspace_error"),
        ..shape("failed_and_annotation", "in_progress")
    });
    for kind in db::LEGACY_BLOCKING_ANNOTATION_KINDS {
        let label: &'static str = Box::leak(format!("ann_{kind}").into_boxed_str());
        v.push(Shape {
            annotation: ann(kind),
            ..shape(
                label,
                if *kind == "review_needs_owner" {
                    "review"
                } else {
                    "in_progress"
                },
            )
        });
    }
    for kind in [
        "merge_conflict",
        "ci_failed",
        "executor_failed",
        "workflow_guard_rejected",
        "environment_not_ready",
        "executor_unavailable",
        "internal_command_failed",
        "dirty_worktree",
        "review_blocked",
        "unknown",
        "dependency_cancelled",
        "review_gate_failed",
        "retry_exhausted",
        "review_budget_exhausted",
        "merge_fix_budget_exhausted",
        "workspace_failed",
        "target_repo_dirty",
    ] {
        let label: &'static str = Box::leak(format!("ann_info_{kind}").into_boxed_str());
        v.push(Shape {
            annotation: ann(kind),
            ..shape(label, "in_progress")
        });
    }
    v.push(Shape {
        annotation: Some(json!({"message":"old free-form annotation"}).to_string()),
        ..shape("ann_typeless", "in_progress")
    });
    v.push(Shape {
        annotation: Some(json!({"type":"crash","message":"boom"}).to_string()),
        ..shape("ann_unknown_type_crash", "in_progress")
    });
    v.push(Shape {
        annotation: Some("not json at all".into()),
        ..shape("ann_malformed", "in_progress")
    });
    v.push(Shape {
        blocked: Some("{".into()),
        ..shape("blocked_malformed", "in_progress")
    });
    v.push(Shape {
        failed: Some("[1,2]".into()),
        ..shape("failed_non_object", "in_progress")
    });
    v.push(Shape {
        blocked: Some("".into()),
        ..shape("blocked_empty_string", "in_progress")
    });
    v.push(Shape {
        metadata: Some("nope".into()),
        ..shape("metadata_malformed", "in_progress")
    });
    v.push(Shape {
        metadata: Some("nope".into()),
        ..shape("metadata_malformed_review", "review")
    });
    v.push(Shape { barrier: Some(json!({"state":"review","status":"blocked","blocking_reason":"review retry budget exhausted"}).to_string()), ..shape("barrier_review", "review") });
    v.push(Shape {
        barrier: Some(json!({"state":"in_progress","status":"blocked"}).to_string()),
        ..shape("barrier_in_progress", "in_progress")
    });
    v.push(Shape { metadata: Some(json!({"awaiting_human":true,"awaiting_human_reason":"plan_review","awaiting_human_marker_id":"m1"}).to_string()), ..shape("human_plan_review", "planning") });
    v.push(Shape {
        metadata: Some(
            json!({"awaiting_human":true,"awaiting_human_reason":"pull_request_merge"}).to_string(),
        ),
        ..shape("human_pr_merge", "merging")
    });
    v.push(Shape {
        metadata: Some(json!({"awaiting_human":"yes"}).to_string()),
        ..shape("human_flag_non_bool", "in_progress")
    });
    v.push(Shape {
        extra: "review_awaiting_human",
        ..shape("review_awaiting_human", "review")
    });
    v.push(Shape {
        extra: "review_failed_idle",
        ..shape("review_failed_idle", "review")
    });
    v.push(Shape {
        extra: "review_failed_running",
        ..shape("review_failed_running", "review")
    });
    v.push(Shape {
        extra: "review_failed_idle",
        ..shape("review_failed_then_in_progress", "in_progress")
    });
    v.push(Shape {
        extra: "review_awaiting_human",
        ..shape("review_awaiting_human_in_progress", "in_progress")
    });
    v.push(Shape {
        extra: "user_reviewer",
        ..shape("gate_user_reviewer", "review")
    });
    v.push(Shape {
        extra: "user_planner",
        ..shape("planning_user_planner", "planning")
    });
    v.push(Shape {
        annotation: ann("review_needs_owner"),
        extra: "review_awaiting_human",
        ..shape("review_needs_owner_with_review", "review")
    });
    v.push(Shape {
        metadata: Some(
            json!({"owner_wait":{"daemon_id":"d","started_at":"2026-10-06T00:00:00Z"}}).to_string(),
        ),
        ..shape("owner_wait", "in_progress")
    });
    v.push(Shape { metadata: Some(json!({"environment_wait":{"kind":"environment_unverified","machine":{"daemon_id":"d"}}}).to_string()), ..shape("environment_wait", "in_progress") });
    v.push(Shape {
        metadata: Some(json!({"environment_wait":null}).to_string()),
        ..shape("environment_wait_null", "in_progress")
    });
    v.push(Shape { metadata: Some(json!({"placement_refusal":{"eligibility_key":"k","annotation":serde_json::from_str::<serde_json::Value>(&ann("workspace_error").unwrap()).unwrap()}}).to_string()), annotation: ann("workspace_error"), ..shape("placement_refusal", "in_progress") });
    v.push(Shape { metadata: Some(json!({"deferred_dispatch":{"not_before":"2099-01-01T00:00:00Z","reason":"retry later","target_state":"in_progress"}}).to_string()), ..shape("deferred_future", "in_progress") });
    v.push(Shape { metadata: Some(json!({"deferred_dispatch":{"not_before":"2020-01-01T00:00:00Z","reason":"retry later","target_state":"in_progress"}}).to_string()), ..shape("deferred_past", "in_progress") });
    v.push(Shape { metadata: Some(json!({"deferred_dispatch":{"not_before":"2099-01-01T00:00:00Z","reason":"environment_not_ready: x","target_state":"in_progress","kind":"environment_not_ready"}}).to_string()), ..shape("deferred_environment", "in_progress") });
    v.push(Shape {
        metadata: Some(json!({"deferred_dispatch":"garbage"}).to_string()),
        ..shape("deferred_garbage", "in_progress")
    });
    v.push(Shape { metadata: Some(json!({"dispatch_disposition":{"task_version":1,"capability":"machine_capacity","blocker_digest":"x","recorded_at":"2026-10-02T00:00:00Z","safe_message":"machine_capacity: waiting"}}).to_string()), ..shape("capacity_machine_full", "in_progress") });
    v.push(Shape { metadata: Some(json!({"dispatch_disposition":{"task_version":99,"capability":"project_capacity","blocker_digest":"x","recorded_at":"2026-10-02T00:00:00Z","safe_message":"project_capacity: waiting"}}).to_string()), ..shape("capacity_project_stale_version", "todo") });
    v.push(Shape {
        metadata: Some(
            json!({"dispatch_disposition":{"task_version":1,"capability":"machine_capacity"}})
                .to_string(),
        ),
        ..shape("capacity_minimal_marker", "in_progress")
    });
    v.push(Shape { metadata: Some(json!({"dispatch_disposition":{"task_version":1,"capability":"dependency_gate","blocker_digest":"x","recorded_at":"2026-10-02T00:00:00Z","safe_message":"dependency_gate: waiting"}}).to_string()), ..shape("refusal_dependency_gate", "todo") });
    v.push(Shape {
        metadata: Some(
            json!({"paused_integration":{"state":"merging"},"paused_integration_generation":7})
                .to_string(),
        ),
        ..shape("paused_integration", "merging")
    });
    v.push(Shape {
        metadata: Some(
            json!({"queued_recovery":{"id":"intent","request":{"guidance":"private"}}}).to_string(),
        ),
        ..shape("queued_recovery", "in_progress")
    });
    let long = "x".repeat(5000);
    v.push(Shape { annotation: Some(json!({"type":"workspace_error","blocking_reason":"r","blocked_by":null,"blocked_at":null,"blocked_execution_id":null,"artifact":null,"message":long}).to_string()), ..shape("long_annotation_message", "in_progress") });
    v.push(Shape { blocked: Some(json!({"kind":"manual_stop","reason":"y".repeat(5000),"created_at":"2026-10-01T00:00:00Z"}).to_string()), ..shape("long_blocked_reason", "in_progress") });
    v.push(Shape { failed: Some(json!({"kind":"executor_failed","reason":"z".repeat(5000),"created_at":"2026-10-01T00:00:00Z"}).to_string()), ..shape("long_failed_reason", "in_progress") });
    let blk = || {
        Some(json!({"kind":"retry_exhausted","reason":"retries exhausted","created_at":"2026-10-01T00:00:00Z"}).to_string())
    };
    let fl = || {
        Some(json!({"kind":"executor_failed","reason":"executor crashed","created_at":"2026-10-01T00:00:00Z"}).to_string())
    };
    v.push(Shape {
        extra: "running_coder",
        ..shape("running_clear", "in_progress")
    });
    v.push(Shape {
        extra: "running_coder",
        blocked: blk(),
        ..shape("running_blocked", "in_progress")
    });
    v.push(Shape {
        extra: "running_coder",
        failed: fl(),
        ..shape("running_failed", "in_progress")
    });
    v.push(Shape {
        extra: "running_coder",
        annotation: ann("workspace_error"),
        ..shape("running_ann_blocking", "in_progress")
    });
    v.push(Shape {
        extra: "running_coder",
        annotation: ann("ci_failed"),
        ..shape("running_ann_info", "in_progress")
    });
    v.push(Shape {
        extra: "running_coder",
        metadata: Some(json!({"awaiting_human":true}).to_string()),
        ..shape("running_human_flag", "in_progress")
    });
    v.push(Shape { extra: "running_coder", metadata: Some(json!({"dispatch_disposition":{"task_version":1,"capability":"machine_capacity","blocker_digest":"x","recorded_at":"2026-10-02T00:00:00Z","safe_message":"machine_capacity: waiting"}}).to_string()), ..shape("running_capacity", "in_progress") });
    v.push(Shape { extra: "running_coder", metadata: Some(json!({"environment_wait":{"kind":"environment_unverified","machine":{"daemon_id":"d"}}}).to_string()), ..shape("running_environment_wait", "in_progress") });
    v.push(Shape {
        extra: "running_coder",
        barrier: Some(json!({"state":"in_progress","status":"blocked"}).to_string()),
        ..shape("running_barrier", "in_progress")
    });
    v.push(Shape {
        extra: "review_awaiting_human+running_reviewer",
        ..shape("review_awaiting_human_running", "review")
    });
    v.push(Shape {
        failed: fl(),
        ..shape("done_failed", "done")
    });
    v.push(Shape {
        blocked: blk(),
        ..shape("done_blocked", "done")
    });
    v.push(Shape {
        annotation: ann("workspace_error"),
        ..shape("cancelled_ann", "cancelled")
    });
    v.push(Shape {
        annotation: ann("dependency_cancelled"),
        ..shape("review_ann_dependency_cancelled", "review")
    });
    v.push(Shape {
        annotation: ann("dependency_cancelled"),
        ..shape("merging_ann_dependency_cancelled", "merging")
    });
    v.push(Shape {
        annotation: ann("dependency_cancelled"),
        extra: "review_awaiting_human",
        ..shape("review_ann_dep_cancelled_with_review", "review")
    });
    v.push(Shape {
        blocked: blk(),
        ..shape("backlog_blocked", "backlog")
    });
    v.push(Shape {
        blocked: blk(),
        ..shape("todo_blocked", "todo")
    });
    v.push(Shape {
        failed: fl(),
        ..shape("review_failed_json", "review")
    });
    v.push(Shape {
        blocked: blk(),
        extra: "review_awaiting_human",
        ..shape("review_blocked_with_review", "review")
    });
    v
}

/// The only rows on which the two generations of readers differ, with what
/// differs. Each is a named, deliberate stage-four difference:
/// - `metadata_malformed*`: the old awaiting-human rule returned an error for
///   the Task (`malformed_metadata_detail`); the condition says "not waiting".
/// - `capacity_minimal_marker`: a two-field capacity marker no writer has ever
///   produced is not a capacity wait.
/// - `long_*`: presented text is bounded to 1,024 bytes with a truncation
///   marker (`bounded_diagnostic_presentation`).
const EXPECTED: &[(&str, &[&str])] = &[
    ("metadata_malformed", &["AWAITING"]),
    ("metadata_malformed_review", &["AWAITING"]),
    ("capacity_minimal_marker", &["SLOT"]),
    ("long_annotation_message", &["EXCEPTION"]),
    ("long_blocked_reason", &["EXCEPTION", "HEALTH"]),
    ("long_failed_reason", &["EXCEPTION", "HEALTH"]),
];

#[tokio::test]
async fn stored_rows_read_the_same_through_legacy_and_condition_readers() {
    let db = Arc::new(sqlite_db().await);
    let (project, _, _repo_dir) = seed_project_repo(&db).await;
    let agent = seed_agent(&db).await;
    let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(1024)));
    let kinds = serde_json::to_string(db::LEGACY_BLOCKING_ANNOTATION_KINDS).unwrap();
    let mut found: Vec<(&'static str, Vec<String>)> = Vec::new();
    let all = shapes();
    assert!(all.len() > 90, "the stored-row table: {}", all.len());
    for s in all {
        let task = seed_task_with_status(&db, &project, s.status).await;
        seed_role_assignment(&db, &task.id, "coder", Some(&agent)).await;
        if s.extra.contains("running_coder") {
            seed_execution(
                &db,
                &task.id,
                Some(&agent),
                "coder",
                ExecutionStatus::Running,
                None,
                &now_rfc3339(),
            )
            .await;
        }
        let extra0 = s.extra.split('+').next().unwrap_or("");
        match extra0 {
            "review_awaiting_human" | "review_failed_idle" | "review_failed_running" => {
                let e = seed_execution(
                    &db,
                    &task.id,
                    Some(&agent),
                    "coder",
                    ExecutionStatus::Completed,
                    None,
                    &now_rfc3339(),
                )
                .await;
                let status = if extra0 == "review_awaiting_human" {
                    ReviewStatus::AwaitingHuman
                } else {
                    ReviewStatus::Failed
                };
                let now = now_rfc3339();
                ReviewRepo::create(
                    &*db,
                    db::CreateReview {
                        id: new_uuid_v4(),
                        task_id: task.id.clone(),
                        execution_id: e.id.clone(),
                        attempt_number: 1,
                        status,
                        step_results_json: "[]".into(),
                        started_at: now.clone(),
                        created_at: now.clone(),
                        updated_at: now,
                    },
                )
                .await
                .unwrap();
                if extra0 == "review_failed_running" {
                    seed_execution(
                        &db,
                        &task.id,
                        Some(&agent),
                        "reviewer",
                        ExecutionStatus::Running,
                        None,
                        &now_rfc3339(),
                    )
                    .await;
                }
            }
            "user_reviewer" | "user_planner" => {
                let now = now_rfc3339();
                TaskRoleAssignmentRepo::assign(
                    &*db,
                    CreateTaskRoleAssignment {
                        id: new_uuid_v4(),
                        task_id: task.id.clone(),
                        role_name: if extra0 == "user_reviewer" {
                            "reviewer".into()
                        } else {
                            "planner".into()
                        },
                        assignee_type: Some(db::AssigneeKind::User),
                        assignee_id: Some("user-1".into()),
                        created_at: now.clone(),
                        updated_at: now,
                    },
                )
                .await
                .unwrap();
            }
            _ => {}
        }
        if s.extra.contains("running_reviewer") {
            seed_execution(
                &db,
                &task.id,
                Some(&agent),
                "reviewer",
                ExecutionStatus::Running,
                None,
                &now_rfc3339(),
            )
            .await;
        }
        // The old server's stored row: legacy columns only, written raw.
        sqlx::query("UPDATE task SET error_annotation=?, blocked_json=?, failed_json=?, entry_barrier_json=?, metadata_json=? WHERE id=?")
            .bind(&s.annotation).bind(&s.blocked).bind(&s.failed).bind(&s.barrier).bind(&s.metadata).bind(&task.id)
            .execute(db.pool()).await.unwrap();
        // The upgrade: rederive the condition from the stored row.
        db.check_task_conditions_of(std::slice::from_ref(&task.id))
            .await
            .unwrap();
        let task = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        let mut diffs: Vec<String> = Vec::new();

        let base_parked: i64 = sqlx::query_scalar(BASE_PARKED)
            .bind(&kinds)
            .bind(&task.id)
            .fetch_one(db.pool())
            .await
            .unwrap();
        let head_parked: i64 = sqlx::query_scalar(HEAD_PARKED)
            .bind(&task.id)
            .fetch_one(db.pool())
            .await
            .unwrap();
        if base_parked != head_parked {
            diffs.push(format!("SLOT parked base={base_parked} head={head_parked}"));
        }
        let read = task.condition.read();
        // The Overview's rule: a blank blocked record was never a block.
        let base_overview_blocked = task
            .blocked_json
            .as_deref()
            .is_some_and(|v| !v.trim().is_empty());
        let head_overview_blocked = read.interruption_present
            && !task
                .condition
                .evidence()
                .blocked_json
                .as_deref()
                .is_some_and(|v| v.trim().is_empty());
        if base_overview_blocked != head_overview_blocked {
            diffs.push(format!(
                "OVERVIEW blocked base={base_overview_blocked} head={head_overview_blocked}"
            ));
        }
        let head_failed_sql: i64 = sqlx::query_scalar("SELECT COALESCE(json_extract(condition_json, '$.evidence.presentation.hard_failure') = 1,0) FROM task WHERE id=?").bind(&task.id).fetch_one(db.pool()).await.unwrap();
        if task.failed_json.is_some() != (head_failed_sql == 1) {
            diffs.push(format!(
                "OVERVIEW failed base={} head={head_failed_sql}",
                task.failed_json.is_some()
            ));
        }
        let public = task.condition.public();

        let base_await = base_awaiting_human(&db, &task).await;
        let head_await = read.human_wait;
        match &base_await {
            Ok(b) if *b != head_await => {
                diffs.push(format!("AWAITING detail base={b} head={head_await}"))
            }
            Err(e) => diffs.push(format!("AWAITING base errored ({e}) head={head_await}")),
            _ => {}
        }
        let base_await = base_await.unwrap_or(false);

        let actor = Actor::user(UserActionSource::Test);
        let snap = service
            .task_action_snapshot_for_task(task.clone(), &actor)
            .await
            .unwrap();
        let new_offers = crate::task_actions::available_actions(&snap);
        let old_offers = crate::task_actions::legacy::available_actions(
            &crate::task_actions::legacy::TaskSnapshot::from(&snap),
        );
        let (no, oo) = (
            serde_json::to_value(&new_offers).unwrap(),
            serde_json::to_value(&old_offers).unwrap(),
        );
        if no != oo {
            diffs.push(format!("OFFERS\n      base={oo}\n      head={no}"));
        }
        let old_exc = legacy_diag::task_exception_projection(
            &task,
            &snap.workflow,
            &snap.executions,
            snap.latest_review.as_ref(),
            Vec::new(),
        );
        let new_exc = crate::task_diagnostics::task_exception_projection(
            &task,
            &snap.workflow,
            &snap.executions,
            snap.latest_review.as_ref(),
            Vec::new(),
        );
        let (ne, oe) = (
            serde_json::to_value(&new_exc).unwrap(),
            serde_json::to_value(&old_exc).unwrap(),
        );
        if ne != oe {
            let cut = |v: &serde_json::Value| {
                let t = v.to_string();
                if t.len() > 400 {
                    format!("{}...[{} bytes]", &t[..400], t.len())
                } else {
                    t
                }
            };
            diffs.push(format!(
                "EXCEPTION\n      base={}\n      head={}",
                cut(&oe),
                cut(&ne)
            ));
        }
        let latest_exec = snap
            .executions
            .iter()
            .max_by_key(|e| (&e.created_at, &e.id));
        let old_h = legacy_diag::derive_workflow_health(
            &task,
            &snap.workflow,
            &snap.role_assignments,
            snap.latest_review.as_ref(),
            latest_exec,
            base_await,
            old_exc.as_ref(),
        );
        let new_h = crate::task_diagnostics::derive_workflow_health(
            &task,
            &snap.workflow,
            &snap.role_assignments,
            snap.latest_review.as_ref(),
            latest_exec,
            head_await,
            new_exc.as_ref(),
        );
        let (mut nh, mut oh) = (
            serde_json::to_value(&new_h).unwrap(),
            serde_json::to_value(&old_h).unwrap(),
        );
        if nh != oh {
            // show only differing keys
            let mut d = String::new();
            for (k, v) in oh.as_object().unwrap() {
                if nh.get(k) != Some(v) {
                    let t1 = v.to_string();
                    let t2 = nh.get(k).cloned().unwrap_or_default().to_string();
                    d.push_str(&format!(
                        "\n      {k}: base[{}]={:.200} head[{}]={:.200}",
                        t1.len(),
                        t1,
                        t2.len(),
                        t2
                    ));
                }
            }
            diffs.push(format!("HEALTH{d}"));
        }
        let _ = (&mut nh, &mut oh);
        sqlx::query("UPDATE execution SET status = 'cancelled' WHERE status = 'running'")
            .execute(db.pool())
            .await
            .unwrap();
        // The presented text is bounded and says so.
        if s.label.starts_with("long_") {
            let message = new_exc
                .as_ref()
                .map(|e| e.message.clone())
                .unwrap_or_default();
            assert!(
                message.len() <= db::task_condition::PRESENTATION_TEXT_LIMIT
                    && message.ends_with(db::task_condition::PRESENTATION_TRUNCATION_MARKER),
                "{}: {} bytes",
                s.label,
                message.len()
            );
            let reason = read.operator_reason.clone().unwrap_or_default();
            if s.annotation.is_some() {
                assert!(
                    serde_json::from_str::<serde_json::Value>(&reason).is_ok(),
                    "{}: operator reason stays JSON",
                    s.label
                );
            }
        }
        // Which stored record the one public interruption is.
        let flags = (public.details().failed, public.details().blocked);
        match s.label {
            "failed_executor" | "failed_merge_conflict" | "long_failed_reason" => {
                assert_eq!(flags, (true, false), "{}", s.label)
            }
            "blocked_retry_exhausted" | "blocked_manual_stop" | "long_blocked_reason" => {
                assert_eq!(flags, (false, true), "{}", s.label)
            }
            "failed_and_blocked" => assert_eq!(flags, (true, true), "{}", s.label),
            "clear_in_progress" | "ann_ci_failed" | "blocked_empty_string" => {
                assert_eq!(flags, (false, false), "{}", s.label)
            }
            _ => {}
        }
        if s.label == "ann_typeless" {
            let diagnostic = public
                .details()
                .diagnostic
                .clone()
                .expect("an untyped annotation stays visible");
            assert_eq!(
                diagnostic.message.as_deref(),
                Some("old free-form annotation")
            );
        }
        if !diffs.is_empty() {
            found.push((
                s.label,
                diffs
                    .iter()
                    .map(|d| d.split_whitespace().next().unwrap_or("").to_owned())
                    .collect(),
            ));
            for d in &diffs {
                println!("[{}] DIFF {d}", s.label);
            }
        }
    }
    let expected: Vec<(&'static str, Vec<String>)> = EXPECTED
        .iter()
        .map(|(label, kinds)| (*label, kinds.iter().map(|k| (*k).to_owned()).collect()))
        .collect();
    assert_eq!(found, expected, "rows whose readers differ, in table order");
}
