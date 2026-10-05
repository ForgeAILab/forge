//! Budget limit resolution keeps the pre-ledger resolver's order on every path,
//! and the upgrade preserves allowances without rewriting user configuration.
//! `base_*` reimplement the pre-ledger resolver (`runtime_retry_budget` and
//! `merged_state_config`) as the reference.
use db::{
    budget::{self, Kind},
    create_sqlite_pool, run_migrations, run_migrations_from, TaskRepo,
};
use serde_json::{json, Value};

/// Pre-ledger `runtime_retry_budget`: Task-wide first, then the supplied state
/// config, then gate max (review/merge_fix), then 3/1/3.
fn base_runtime_retry_budget(
    task_state_config: Option<&str>,
    key: &str,
    state_config: Option<&Value>,
    gate_max: Option<i32>,
) -> i32 {
    let pick = |v: &Value| {
        v.get("retry_budgets")
            .and_then(|b| b.get(key))
            .and_then(Value::as_i64)
            .and_then(|n| i32::try_from(n).ok())
            .filter(|n| *n >= 0)
    };
    let task = task_state_config.and_then(|raw| serde_json::from_str::<Value>(raw).ok());
    if let Some(n) = task.as_ref().and_then(pick) {
        return n;
    }
    if let Some(n) = state_config.and_then(pick) {
        return n;
    }
    if matches!(key, "review" | "merge_fix") {
        if let Some(n) = gate_max {
            return n;
        }
    }
    match key {
        "merge_fix" => 1,
        _ => 3,
    }
}

/// Pre-ledger `hooks::merged_state_config` (HookContext paths): workflow state
/// config overlaid with `task_state_config[state]`.
fn base_merged(state_config: &Value, task_state_config: Option<&str>, state: &str) -> Value {
    let mut merged = state_config.clone();
    if let Some(Value::Object(overrides)) = task_state_config
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
        .and_then(|v| v.get(state).cloned())
    {
        if let Value::Object(m) = &mut merged {
            for (k, v) in overrides {
                m.insert(k, v);
            }
        } else {
            merged = Value::Object(overrides);
        }
    }
    merged
}

async fn pre_budget_pool() -> (tempfile::TempDir, sqlx::SqlitePool) {
    let temporary = tempfile::tempdir().unwrap();
    let historical = temporary.path().join("migrations");
    std::fs::create_dir(&historical).unwrap();
    for entry in std::fs::read_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/migrations")).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name();
        if name.to_string_lossy().ends_with(".sql")
            && !name.to_string_lossy().ends_with("__task_budgets.sql")
        {
            std::fs::copy(entry.path(), historical.join(name)).unwrap();
        }
    }
    let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
    run_migrations_from(&pool, &historical).await.unwrap();
    (temporary, pool)
}

fn gate(max: i32) -> api_types::GateConfig {
    api_types::GateConfig {
        reject_target: None,
        max_rejections: Some(max),
        approve_label: None,
        reject_label: None,
        requires_user_approval: None,
        optional_when_unassigned: None,
    }
}

/// The upgrade leaves a shadowed per-state value as it was, so a later
/// web-editor save (top-level `retry_budgets` only) still wins.
#[tokio::test]
async fn upgrade_keeps_config_so_later_editor_save_wins() {
    let (_dir, pool) = pre_budget_pool().await;
    let workflow = json!({"roles":[],"states":[{"name":"review","kind":"gate","column":"Review","display_name":"Review","role":null,"hooks":{},"config":{},"gate_config":{"max_rejections":2}}]});
    sqlx::query("INSERT INTO project(id,name,workflow_definition,settings,created_at,updated_at) VALUES('p','P',?,'{}','now','now')")
        .bind(workflow.to_string()).execute(&pool).await.unwrap();
    let original = json!({"retry_budgets":{"review":5},"review":{"retry_budgets":{"review":2}}});
    sqlx::query("INSERT INTO task(id,project_id,title,status,task_state_config,created_at,updated_at) VALUES('t','p','T','review',?,'now','now')")
        .bind(original.to_string()).execute(&pool).await.unwrap();
    run_migrations(&pool).await.unwrap();
    let stored: String = sqlx::query_scalar("SELECT task_state_config FROM task WHERE id='t'")
        .fetch_one(&pool)
        .await
        .unwrap();
    let mut stored: Value = serde_json::from_str(&stored).unwrap();
    assert_eq!(
        stored, original,
        "the upgrade does not rewrite Task configuration"
    );
    // TaskDetailPage.onSaveRetryBudgets: keep every other key, replace retry_budgets.
    stored["retry_budgets"] = json!({"review": 1});
    sqlx::query("UPDATE task SET task_state_config=? WHERE id='t'")
        .bind(stored.to_string())
        .execute(&pool)
        .await
        .unwrap();
    let db = db::SqliteDb::new(pool.clone());
    let task = TaskRepo::get_by_id(&db, "t", false).await.unwrap().unwrap();
    let head = budget::limit(&task, Kind::Review, Some(&json!({})), Some(&gate(2))).unwrap();
    let base = base_runtime_retry_budget(
        task.task_state_config.as_deref(),
        "review",
        Some(&base_merged(
            &json!({}),
            task.task_state_config.as_deref(),
            "review",
        )),
        Some(2),
    );
    assert_eq!(
        head, base,
        "owner's top-level edit is shadowed after upgrade"
    );
    assert_eq!(head, 1);
}

/// Per-state-only Task overrides stay ignored on the paths that never read them
/// (reviewer completion, Execution retry, workflow guard).
#[test]
fn per_state_only_override_matches_pre_ledger_on_raw_state_paths() {
    let cases = [
        // (status, task_state_config, kind, key, raw workflow state config, gate max)
        (
            "review",
            json!({"review":{"retry_budgets":{"review":5}}}),
            Kind::Review,
            "review",
            json!({}),
            Some(2),
        ),
        (
            "in_progress",
            json!({"in_progress":{"retry_budgets":{"execution":1}}}),
            Kind::Execution,
            "execution",
            json!({}),
            None,
        ),
        (
            "in_progress",
            json!({"in_progress":{"retry_budgets":{"execution":1}}}),
            Kind::WorkflowGuard,
            "execution",
            json!({}),
            None,
        ),
    ];
    let mut diffs = Vec::new();
    for (status, config, kind, key, raw, gate_max) in cases {
        let task = db::Task {
            status: status.into(),
            task_state_config: Some(config.to_string()),
            ..test_task()
        };
        let g = gate_max.map(gate);
        let head = budget::limit(&task, kind, Some(&raw), g.as_ref()).unwrap();
        // Completion/execution/guard call sites pass the raw `state.config`.
        let base =
            base_runtime_retry_budget(task.task_state_config.as_deref(), key, Some(&raw), gate_max);
        if head != base {
            diffs.push(format!("{kind:?}: base {base} -> HEAD {head}"));
        }
    }
    assert!(diffs.is_empty(), "allowance changes: {diffs:?}");
}

fn test_task() -> db::Task {
    serde_json::from_value(json!({
        "id":"t","project_id":"p","title":"T","task_type":"task","status":"review",
        "is_automation":false,"priority":0,"board_position":0.0,"created_at":"now","updated_at":"now","version":1
    }))
    .unwrap_or_else(|_| panic!("construct Task"))
}

/// A Project stored with the DB default `'{}'` workflow (resolved to the default
/// workflow at runtime) keeps its planning-gate spending across the upgrade.
#[tokio::test]
async fn upgrade_preserves_planning_gate_for_default_workflow_project() {
    let (_dir, pool) = pre_budget_pool().await;
    sqlx::query("INSERT INTO project(id,name,workflow_definition,settings,created_at,updated_at) VALUES('p','P','{}','{}','now','now')")
        .execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES('t','p','T','planning','now','now')")
        .execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO transition_log(id,task_id,from_state,to_state,triggered_by,trigger_reason,rejection,created_at) VALUES('r1','t','planning','planning','agent:planner','rejected',1,'now')")
        .execute(&pool).await.unwrap();
    // resolve_workflow('{}') = default workflow, planning max_rejections 2, one spent.
    let base_remaining = 2 - 1;
    run_migrations(&pool).await.unwrap();
    let spent = budget::spent(&pool, "t", "gate:planning").await.unwrap();
    assert_eq!(budget::remaining(2, spent), base_remaining);
}

/// A pre-upgrade queued execution increment replays exactly once.
#[tokio::test]
async fn queued_increment_replays_once_after_upgrade() {
    let (_dir, pool) = pre_budget_pool().await;
    sqlx::query("INSERT INTO project(id,name,workflow_definition,settings,created_at,updated_at) VALUES('p','P','{}','{}','now','now')")
        .execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO task(id,project_id,title,status,metadata_json,created_at,updated_at) VALUES('t','p','T','in_progress',?,'now','now')")
        .bind(json!({"execution_retry_count":1}).to_string()).execute(&pool).await.unwrap();
    let queued = json!({"TaskMutateMetadata":{"id":"t","expected_version":null,"mutations":[
        {"Increment":{"key":"execution_retry_count","by":1}}],"updated_at":"later"}});
    sqlx::query("INSERT INTO task_step(id,task_id,seq,kind,payload_json,causation_key,chain_id,chain_position,expected_status,expected_version,status,available_at,created_at,updated_at) VALUES('q','t',1,'mutation',?,'effect','chain',1,'in_progress',1,'pending','now','now','now')")
        .bind(queued.to_string()).execute(&pool).await.unwrap();
    run_migrations(&pool).await.unwrap();
    let payload: String = sqlx::query_scalar("SELECT payload_json FROM task_step WHERE id='q'")
        .fetch_one(&pool)
        .await
        .unwrap();
    let payload: Value = serde_json::from_str(&payload).unwrap();
    let mutation: db::TaskMetadataMutation =
        serde_json::from_value(payload["TaskMutateMetadata"]["mutations"][0].clone()).unwrap();
    let db::TaskMetadataMutation::Budget(effect) = mutation else {
        panic!("converted to a budget effect: {payload}");
    };
    for _ in 0..2 {
        let mut tx = db::begin_immediate(&pool).await.unwrap();
        budget::apply(&mut tx, "t", effect.clone()).await.unwrap();
        tx.commit().await.unwrap();
    }
    let spent = budget::spent(&pool, "t", "execution").await.unwrap();
    assert_eq!(spent, 2);
}
