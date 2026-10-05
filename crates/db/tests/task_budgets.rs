use db::{
    budget::{self, Kind},
    create_sqlite_pool, run_migrations, run_migrations_from,
};
use serde_json::json;
use sqlx::Row;

/// The upgrade snapshots retained allowance, not erased historical spending.
#[tokio::test]
async fn upgrade_preserves_remaining_for_every_persisted_kind() {
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
    let workflow = json!({"states":[
        {"name":"planning","kind":"gate","gate_config":{"max_rejections":2}},
        {"name":"review","kind":"gate","gate_config":{"max_rejections":2}},
        {"name":"merging","kind":"gate","gate_config":{"max_rejections":2}}
    ]});
    sqlx::query("INSERT INTO project(id,name,workflow_definition,settings,created_at,updated_at) VALUES('p','Project',?,?,'now','now')")
        .bind(workflow.to_string()).bind(json!({"automatic_recovery":{"enabled":true,"max_attempts":2}}).to_string()).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO task(id,project_id,title,status,metadata_json,entry_barrier_json,created_at,updated_at) VALUES('t','p','Task','review',?,?,'now','now')")
        .bind(json!({"execution_retry_count":2,"workflow_guard_retry_count":1,"custom":"retained"}).to_string())
        .bind(json!({"state":"review","started_at":"episode","infrastructure_attempts":2,"status":"blocked"}).to_string())
        .execute(&pool).await.unwrap();
    for (id, state, bridge, rejection) in [
        ("planning", "planning", None, true),
        ("review", "review", None, true),
        ("merge", "merging", None, true),
        ("rebase", "merging", Some("target_moved_rebase"), false),
        ("handoff", "merging", Some("conflict_handoff"), false),
    ] {
        sqlx::query("INSERT INTO transition_log(id,task_id,from_state,to_state,triggered_by,trigger_reason,rejection,bridge_kind,created_at) VALUES(?,'t',?,'in_progress','system:workflow','historical evidence',?,?,'now')")
            .bind(id).bind(state).bind(rejection).bind(bridge).execute(&pool).await.unwrap();
    }
    for (id, purpose, status) in [
        ("candidate", None, "completed"),
        ("recovery", Some("automatic_review_recovery"), "running"),
    ] {
        sqlx::query("INSERT INTO execution(id,task_id,role,status,purpose,created_at,updated_at) VALUES(?,'t','coder',?,?, 'now','now')")
            .bind(id).bind(status).bind(purpose).execute(&pool).await.unwrap();
    }
    sqlx::query("INSERT INTO review(id,task_id,execution_id,attempt_number,status,step_results_json,started_at,finished_at,created_at,updated_at) VALUES('passed','t','candidate',1,'passed',?,'now','now','now','now')")
        .bind(json!({"conformance":{"contract":{"execution_id":"contract"}}}).to_string()).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO review(id,task_id,execution_id,attempt_number,status,step_results_json,started_at,finished_at,created_at,updated_at) VALUES('failed','t','candidate',2,'failed','{}','now','now','now','now')")
        .execute(&pool).await.unwrap();
    for id in ["carry1", "carry2"] {
        sqlx::query("INSERT INTO review_authority_carry(id,task_id,contract_execution_id,commit_sha,base_sha,kind,changed_paths_json,created_at) VALUES(?,'t','contract','commit','base','clean_rebase','[]','now')")
            .bind(id).execute(&pool).await.unwrap();
    }
    let literal = json!({"Increment":{"key":"execution_retry_count","by":1}});
    let queued = json!({"TaskMutateMetadata":{"id":"t","expected_version":null,"mutations":[
        {"Increment":{"key":"execution_retry_count","by":1}},
        {"Set":{"key":"example","value":literal.clone()}}
    ],"updated_at":"later"}});
    let guard = json!({"TaskMutateMetadata":{"id":"t","expected_version":null,"mutations":[
        {"CompareAndMutate":{"key":"workflow_guard_retry_count","expected":1,"mutations":[{"Remove":{"key":"workflow_guard_retry_count"}}]}}
    ],"updated_at":"later"}});
    for (index, payload) in [queued, guard].into_iter().enumerate() {
        sqlx::query("INSERT INTO task_step(id,task_id,seq,kind,payload_json,causation_key,chain_id,chain_position,expected_status,expected_version,status,available_at,created_at,updated_at) VALUES(?,'t',?,'mutation',?,?,'chain',?,'review',1,'pending','now','now','now')")
            .bind(format!("queued-{index}")).bind(index as i64+1).bind(payload.to_string()).bind(format!("effect-{index}")).bind(index as i64+1).execute(&pool).await.unwrap();
    }
    // These are the old source counts, independently collected before upgrading.
    let mut before = std::collections::HashMap::new();
    for (kind, state) in [
        ("review", "review"),
        ("merge_fix", "merging"),
        ("gate:planning", "planning"),
        ("gate:merging", "merging"),
    ] {
        let spent:i64=sqlx::query_scalar("SELECT COUNT(*) FROM transition_log WHERE task_id='t' AND from_state=? AND rejection=1")
            .bind(state).fetch_one(&pool).await.unwrap();
        before.insert(kind, budget::remaining(2, spent));
    }
    for (kind,source,limit) in [
        ("execution","SELECT json_extract(metadata_json,'$.execution_retry_count') FROM task WHERE id='t'",3),
        ("workflow_guard","SELECT json_extract(metadata_json,'$.workflow_guard_retry_count') FROM task WHERE id='t'",3),
        ("target_moved_rebase","SELECT COUNT(*) FROM transition_log WHERE bridge_kind='target_moved_rebase'",5),
        ("conflict_handoff","SELECT COUNT(*) FROM transition_log WHERE bridge_kind='conflict_handoff'",5),
        ("review_carry","SELECT COUNT(*) FROM review_authority_carry WHERE contract_execution_id='contract'",5),
        ("automatic_review_recovery","SELECT COUNT(*) FROM execution WHERE purpose='automatic_review_recovery' AND status='running'",2),
        ("review_ci_infrastructure","SELECT json_extract(entry_barrier_json,'$.infrastructure_attempts') FROM task WHERE id='t'",5),
    ] {
        let spent:i64=sqlx::query_scalar(source).fetch_one(&pool).await.unwrap();
        before.insert(kind,budget::remaining(limit,spent));
    }
    before.insert("gate:review", 0);
    let original_policy =
        json!({"retry_budgets":{"execution":3},"review":{"retry_budgets":{"execution":2}}})
            .to_string();
    sqlx::query("UPDATE task SET task_state_config=? WHERE id='t'")
        .bind(&original_policy)
        .execute(&pool)
        .await
        .unwrap();
    run_migrations(&pool).await.unwrap();
    for (kind, remaining) in before {
        let limit = match kind {
            "gate:review" => 1,
            "review"
            | "merge_fix"
            | "gate:planning"
            | "gate:merging"
            | "automatic_review_recovery" => 2,
            "execution" | "workflow_guard" => 3,
            _ => 5,
        };
        assert_eq!(
            budget::remaining(limit, budget::spent(&pool, "t", kind).await.unwrap()),
            remaining,
            "{kind}"
        );
    }
    // Resolution keeps its old order, so no Task configuration is rewritten.
    let policy: String = sqlx::query_scalar("SELECT task_state_config FROM task WHERE id='t'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(policy, original_policy);
    let queued: String =
        sqlx::query_scalar("SELECT payload_json FROM task_step WHERE id='queued-0'")
            .fetch_one(&pool)
            .await
            .unwrap();
    let queued: serde_json::Value = serde_json::from_str(&queued).unwrap();
    assert_eq!(
        queued
            .pointer("/TaskMutateMetadata/mutations/0/Budget/Charge/key")
            .unwrap(),
        "execution"
    );
    assert_eq!(
        queued
            .pointer("/TaskMutateMetadata/mutations/1/Set/value")
            .unwrap(),
        &literal
    );
    let guard: String =
        sqlx::query_scalar("SELECT payload_json FROM task_step WHERE id='queued-1'")
            .fetch_one(&pool)
            .await
            .unwrap();
    let guard: serde_json::Value = serde_json::from_str(&guard).unwrap();
    assert_eq!(
        guard
            .pointer("/TaskMutateMetadata/mutations/0/BudgetIfSpent/key")
            .unwrap(),
        "workflow_guard"
    );
    assert!(guard
        .pointer("/TaskMutateMetadata/mutations/0/BudgetIfSpent/mutations/1/Budget/Reset")
        .is_some());
    let row = sqlx::query("SELECT metadata_json,entry_barrier_json FROM task WHERE id='t'")
        .fetch_one(&pool)
        .await
        .unwrap();
    let metadata: serde_json::Value = serde_json::from_str(row.get("metadata_json")).unwrap();
    assert_eq!(metadata, json!({"custom":"retained"}));
    let barrier: serde_json::Value = serde_json::from_str(row.get("entry_barrier_json")).unwrap();
    assert_eq!(barrier["started_at"], "episode");
    assert!(barrier.get("infrastructure_attempts").is_none());
    assert_eq!(
        budget::load(&pool, "t").await.unwrap().len(),
        Kind::PERSISTED.len() + 3
    );
    let history: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM transition_log")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(history, 5);
}
