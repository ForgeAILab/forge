use super::*;
use crate::{EnqueueTaskStep, TaskRepo, TaskStepRepo};

// Main's json!-built placement refusal, copied as bytes before any typed writer
// runs. The marker is independently decoded/re-rendered by the clearing service.
const MAIN_PLACEMENT: &str = r#"{"code":"placement_unavailable","machines":["worker"],"message":"upgrade daemon","missing_capabilities":["workspaces"],"rejected_candidates":[],"state":"in_progress","type":"dispatch_failed"}"#;

#[tokio::test]
async fn pre_upgrade_exact_clear_matches_and_preserves_a_superseding_annotation() {
    let pool = crate::create_sqlite_pool("sqlite::memory:").await.unwrap();
    crate::run_migrations(&pool).await.unwrap();
    let db = SqliteDb::new(pool);
    let at = crate::now_rfc3339();
    sqlx::query("INSERT INTO project(id,name,settings,workflow_definition,created_at,updated_at) VALUES ('p','p','{}','{}',?,?)")
        .bind(&at).bind(&at).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO task(id,project_id,title,status,error_annotation,metadata_json,created_at,updated_at) VALUES ('t','p','t','in_progress',?,'{}',?,?)")
        .bind(MAIN_PLACEMENT).bind(&at).bind(&at).execute(db.pool()).await.unwrap();
    db.enqueue_step(&EnqueueTaskStep {
        id: "step".into(),
        task_id: "t".into(),
        kind: "command".into(),
        payload_json: "{}".into(),
        causation_step_id: None,
        causation_key: "step".into(),
        chain_id: "step".into(),
        chain_position: 1,
        expected_status: "in_progress".into(),
        expected_version: 1,
        expected_epoch: None,
        lane: "fast".into(),
        available_at: at.clone(),
    })
    .await
    .unwrap();
    let step = db
        .claim_step(
            "fence-test",
            Some("t"),
            &crate::task_writer::lease_deadline(),
        )
        .await
        .unwrap()
        .unwrap();
    crate::task_writer::in_task_step(step, async {
        let rendered = serde_json::from_str::<serde_json::Value>(MAIN_PLACEMENT).unwrap().to_string();
        assert_eq!(rendered.as_bytes(), MAIN_PLACEMENT.as_bytes());
        for superseded in [false, true] {
            let annotation = if superseded { r#"{"blocked_at":"at","blocked_by":"system:heartbeat_monitor","blocking_reason":"owner_workspace_reset_required","message":"reset","type":"workspace_reset_required"}"# } else { MAIN_PLACEMENT };
            sqlx::query("UPDATE task SET error_annotation=?,metadata_json='{}' WHERE id='t'")
                .bind(annotation).execute(db.pool()).await.unwrap();
            let row = TaskRepo::get_by_id(&db, "t", false).await.unwrap().unwrap();
            assert_eq!(db.clear_placement_dispatch_refusal(ClearPlacementDispatchRefusalInput {
                annotation:rendered.clone(), updated_at:at.clone(), task_id:"t", version:row.version,
                metadata_json:&row.metadata_json,
            }).await.unwrap(), 1);
            let after = TaskRepo::get_by_id(&db, "t", false).await.unwrap().unwrap();
            assert_eq!(after.error_annotation.as_deref(), superseded.then_some(annotation));
        }
    }).await;
}
