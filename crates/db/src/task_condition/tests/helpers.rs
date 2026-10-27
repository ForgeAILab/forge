//! Shared fixtures and helpers for task condition tests.

use super::*;
pub(in crate::task_condition) async fn db() -> SqliteDb {
    let pool = crate::create_sqlite_pool("sqlite::memory:").await.unwrap();
    crate::run_migrations(&pool).await.unwrap();
    let db = SqliteDb::new(pool);
    ProjectRepo::create(
        &db,
        CreateProject {
            id: "p".into(),
            owner_id: None,
            name: "Conditions".into(),
            primary_repo_id: None,
            updated_at: crate::now_rfc3339(),
            settings: "{}".into(),
            workflow_definition: "{}".into(),
            created_at: crate::now_rfc3339(),
        },
    )
    .await
    .unwrap();
    db
}
pub(in crate::task_condition) async fn task(db: &SqliteDb, id: &str) -> Task {
    TaskRepo::create(
        db,
        CreateTask {
            id: id.into(),
            project_id: "p".into(),
            parent_task_id: None,
            assignee_type: None,
            assignee_id: None,
            title: id.into(),
            description: None,
            task_type: "task".into(),
            status: "todo".into(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            subtask_order: None,
            plan: None,
            updated_at: crate::now_rfc3339(),
            created_at: crate::now_rfc3339(),
        },
    )
    .await
    .unwrap()
}
pub(in crate::task_condition) async fn claim(db: &SqliteDb, id: &str) -> crate::TaskStep {
    let t = TaskRepo::get_by_id(db, id, false).await.unwrap().unwrap();
    let step_id = crate::new_uuid_v4();
    db.enqueue_step(&EnqueueTaskStep {
        id: step_id.clone(),
        task_id: id.into(),
        kind: "command".into(),
        payload_json: "{}".into(),
        causation_step_id: None,
        causation_key: step_id.clone(),
        chain_id: step_id,
        chain_position: 1,
        expected_status: t.status,
        expected_version: t.version,
        expected_epoch: None,
        lane: "fast".into(),
        available_at: crate::now_rfc3339(),
    })
    .await
    .unwrap();
    db.claim_step(
        "condition-tests",
        Some(id),
        &crate::task_writer::lease_deadline(),
    )
    .await
    .unwrap()
    .unwrap()
}
pub(in crate::task_condition) fn input(
    annotation: Option<Value>,
    blocked: Option<Value>,
    failed: Option<Value>,
    barrier: Option<Value>,
    metadata: Option<Value>,
) -> ConditionInput {
    ConditionInput {
        error_annotation: annotation.map(|v| v.to_string()),
        blocked_json: blocked.map(|v| v.to_string()),
        failed_json: failed.map(|v| v.to_string()),
        entry_barrier_json: barrier.map(|v| v.to_string()),
        metadata_json: metadata.map(|v| v.to_string()),
        ..Default::default()
    }
}
pub(super) fn reasons(condition: &TaskCondition) -> Vec<&ParkReason> {
    match condition {
        TaskCondition::Parked {
            primary,
            additional,
            ..
        } => std::iter::once(primary).chain(additional).collect(),
        TaskCondition::Failed {
            failure,
            additional,
            ..
        } => std::iter::once(failure).chain(additional).collect(),
        _ => condition.evidence().observations.iter().collect(),
    }
}

pub(super) const MIGRATION: &str =
    include_str!("../../../migrations/V202610060030__task_condition.sql");

/// The migration as the runner applies it: SQL, then the Rust backfill.
pub(super) async fn migrate(connection: &mut SqliteConnection) {
    sqlx::raw_sql(MIGRATION)
        .execute(&mut *connection)
        .await
        .unwrap();
    backfill(connection).await.unwrap();
}

pub(super) fn update(t: &Task) -> UpdateTask {
    UpdateTask {
        id: t.id.clone(),
        expected_version: t.version,
        title: None,
        description: None,
        priority: None,
        merge_config: None,
        plan: None,
        error_annotation: None,
        blocked_json: None,
        failed_json: None,
        task_state_config: None,
        parent_task_id: None,
        updated_at: crate::now_rfc3339(),
    }
}

// ---------------------------------------------------------------------------
// Ported audit probes (audit-31-s1): each asserts the fixed behaviour.
// ---------------------------------------------------------------------------

/// Hostile legacy values for the four condition columns.
pub(super) fn hostile() -> Vec<(String, Option<String>)> {
    let big = "x".repeat(1_000_000);
    let deep_ok = format!("{}1{}", "[".repeat(900), "]".repeat(900));
    let deep_bad = format!("{}1{}", "[".repeat(5000), "]".repeat(5000));
    let mut v: Vec<(String, Option<String>)> = [
        ("null", None),
        ("empty", Some("".to_owned())),
        ("space", Some(" ".to_owned())),
        ("bad", Some("{bad".to_owned())),
        ("jnull", Some("null".to_owned())),
        ("arr", Some("[]".to_owned())),
        ("str", Some("\"s\"".to_owned())),
        ("num", Some("123".to_owned())),
        ("true", Some("true".to_owned())),
        ("json5", Some("{kind:'manual_stop'}".to_owned())),
        ("nan", Some("NaN".to_owned())),
        ("dupkey", Some(r#"{"kind":"a","kind":"manual_stop","type":"x","type":"ci_failed"}"#.to_owned())),
        ("surrogate", Some(r#"{"kind":"\ud800","type":"\ud800","blocked_by":"\udfff","blocking_reason":"\ud800","state":"\ud800","status":"\ud800"}"#.to_owned())),
        ("nul", Some("{\"kind\":\"a\\u0000b\",\"type\":\"a\\u0000b\"}".to_owned())),
        ("rawnul", Some("{\"kind\":\"a\0b\"}".to_owned())),
        ("types", Some(r#"{"kind":1,"type":[1],"blocked_by":{},"blocking_reason":false,"state":1.5,"status":null}"#.to_owned())),
        ("bignum", Some(r#"{"kind":1e999,"type":123456789012345678901234567890,"n":-0.0}"#.to_owned())),
        ("deep_ok", Some(deep_ok)),
        ("deep_bad", Some(deep_bad)),
        ("big_str", Some(format!("{{\"kind\":\"{big}\",\"type\":\"{big}\",\"message\":\"{big}\"}}"))),
        ("big_arab", Some(format!("{{\"kind\":\"{0}\",\"type\":\"{0}\"}}", "\u{06A9}".repeat(40_000)))),
        ("cjk", Some(r#"{"kind":"中文한국어😀","type":"中文한국어😀","message":"失败"}"#.to_owned())),
        ("bom", Some("\u{feff}{\"kind\":\"manual_stop\"}".to_owned())),
    ]
    .into_iter()
    .map(|(name, value)| (name.to_owned(), value))
    .collect();
    for k in [
        "pull_request_merge",
        "merge_fix_ci_failed",
        "agent_timeout",
        "dispatch_failed",
        "workflow_loop",
        "cascade_failed",
        "dependency_cancelled",
        "merge_conflict",
        "target_repo_dirty",
        "dirty_worktree",
        "ci_failed",
        "review_gate_failed",
        "review_budget_exhausted",
        "review_blocked",
        "review_needs_owner",
        "environment_not_ready",
        "retry_exhausted",
        "merge_fix_budget_exhausted",
        "workflow_guard_rejected",
        "internal_command_failed",
        "executor_failed",
        "workspace_failed",
        "workspace_reset_required",
        "workspace_error",
        "before_work_hook_timeout",
        "before_work_hook_failed",
        "max_turns_exceeded",
        "manual_stop",
        "recovery_required",
        "executor_unavailable",
        "unknown",
        "Unknown",
        "",
    ] {
        v.push((format!("k:{k}"), Some(json!({"kind":k,"type":k,"blocking_reason":k,"status":"blocked","state":"review","source":"workflow_loop","blocked_by":"user:1"}).to_string())));
    }
    for r in [
        "review_ci_infrastructure_exhausted",
        "pending_remote_cancel",
        "dependency_cancelled",
        "review_ci_infrastructure",
        "review_ci_unavailable",
        "review retry budget exhausted",
    ] {
        v.push((format!("r:{r}"), Some(json!({"type":"before_work_hook_failed","kind":"review_gate_failed","blocking_reason":r,"status":"blocked","state":"review"}).to_string())));
    }
    for s in ["running", "blocked", "passed", ""] {
        v.push((
            format!("s:{s}"),
            Some(json!({"status":s,"state":"x"}).to_string()),
        ));
    }
    v
}
pub(super) fn hostile_metadata() -> Vec<Option<String>> {
    let mut v = vec![
        None,
        Some("".into()),
        Some("{bad".into()),
        Some("null".into()),
        Some("[1]".into()),
        Some("\"s\"".into()),
        Some("{}".into()),
        Some(format!("{{\"custom\":\"{}\"}}", "y".repeat(1_000_000))),
        Some(
            r#"{"a\"b":1,"$":2,"x.y":3,"[0]":4,"":5,"awaiting_human":true,"awaiting_human":false}"#
                .into(),
        ),
    ];
    let keys = [
        "awaiting_human",
        "awaiting_human_marker_id",
        "awaiting_human_reason",
        "coordination_review_pending",
        "coordination_review_pending_id",
        "daemon_upgrade_refusal",
        "deferred_dispatch",
        "dispatch_disposition",
        "environment_wait",
        "executor_unavailable_execution_id",
        "last_execution_failure_at",
        "last_execution_failure_execution_id",
        "last_workflow_guard_execution_id",
        "last_workflow_guard_name",
        "last_workflow_guard_reason",
        "last_workflow_guard_rejection_at",
        "owner_wait",
        "paused_integration",
        "paused_integration_generation",
        "placement_refusal",
        "plan_settlement_wait",
        "planning_completed_at",
        "planning_execution_id",
        "planning_state_entry_token",
        "queued_recovery",
        "plan_publication_claim",
        "plan_publication_cleanup",
        "terminal_execution_settlement",
    ];
    let vals = [
        json!(null),
        json!(true),
        json!(false),
        json!(1),
        json!(1e300),
        json!("s"),
        json!("\u{06A9}x"),
        json!([1,{"a":[]}]),
        json!({}),
        json!({"kind":"environment_probe_pending","reason":"r","not_before":"later","target_state":7,"capability":"project_capacity","state":"review","daemon_id":1,"execution_id":["e"],"started_at":{},"blocker_digest":3}),
        json!({"not_before":"2026-10-05T12:00:00.123456789+00:00","reason":"r","target_state":"todo","kind":"x"}),
        json!({"not_before":"9999-99-99T99:99:99Z","reason":"r","target_state":"todo"}),
        json!({"not_before":"2026-10-05 12:00:00","reason":"r","target_state":"todo"}),
        json!({"capability":"machine_capacity"}),
        json!({"capability":"\u{06A9}"}),
        json!({"kind":"provision_failed"}),
    ];
    for k in keys {
        for val in &vals {
            v.push(Some(json!({k: val}).to_string()));
        }
    }
    // Raw escapes that serde_json::json! would normalise.
    v.push(Some(r#"{"awaiting_human":true,"awaiting_human_reason":"\ud800","deferred_dispatch":{"not_before":"\ud800","reason":"\udc00","target_state":"\ud800"},"owner_wait":{"daemon_id":"\ud800"},"dispatch_disposition":{"capability":"\ud800"},"environment_wait":{"kind":"\ud800"},"paused_integration":{"state":"\ud800"},"plan_settlement_wait":{"execution_id":"\ud800"}}"#.into()));
    v.push(Some(r#"{"deferred_dispatch":{"not_before":1e999,"reason":123456789012345678901234567890,"target_state":"\u0000"}}"#.into()));
    let all: serde_json::Map<String, Value> = keys
        .iter()
        .map(|k| (k.to_string(), vals[9].clone()))
        .collect();
    v.push(Some(Value::Object(all).to_string()));
    v
}
pub(super) const LEGACY_COLUMNS: [&str; 5] = [
    "error_annotation",
    "blocked_json",
    "failed_json",
    "entry_barrier_json",
    "metadata_json",
];
pub(super) async fn clone_tasks(db: &SqliteDb, template: &str, n: usize, prefix: &str) {
    let columns: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM pragma_table_info('task') WHERE name NOT IN ('id','condition_json')",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    let columns = columns.join(",");
    let mut tx = db.pool().begin().await.unwrap();
    for i in 0..n {
        sqlx::query(&format!(
            "INSERT INTO task(id,{columns}) SELECT ?,{columns} FROM task WHERE id=?"
        ))
        .bind(format!("{prefix}{i}"))
        .bind(template)
        .execute(&mut *tx)
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
}
/// Back to the pre-stage-one schema, keeping every other table and trigger.
pub(super) async fn unmigrate(db: &SqliteDb) {
    sqlx::raw_sql(
        "DROP TRIGGER task_schedule_update; DROP TRIGGER task_schedule_relationship; DROP INDEX IF EXISTS idx_task_schedule_open; DROP INDEX IF EXISTS idx_task_condition_kind; DROP INDEX IF EXISTS idx_task_condition_retry_project; DROP TRIGGER IF EXISTS task_list_revision_task_update; ALTER TABLE task DROP COLUMN condition_json;",
    )
    .execute(db.pool())
    .await
    .unwrap();
}
pub(super) async fn legacy_snapshot(db: &SqliteDb) -> Vec<(String, String)> {
    sqlx::query_as("SELECT id, hex(COALESCE(error_annotation,'~N'))||'|'||hex(COALESCE(blocked_json,'~N'))||'|'||hex(COALESCE(failed_json,'~N'))||'|'||hex(COALESCE(entry_barrier_json,'~N'))||'|'||hex(COALESCE(metadata_json,'~N'))||'|'||version||'|'||updated_at||'|'||typeof(error_annotation)||typeof(blocked_json)||typeof(failed_json)||typeof(entry_barrier_json)||typeof(metadata_json) FROM task ORDER BY id").fetch_all(db.pool()).await.unwrap()
}
