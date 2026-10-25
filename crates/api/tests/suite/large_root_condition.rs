use crate::common;
use api_types::{ProjectResponse, TaskResponse};
use axum::http::{Method, StatusCode};
use db::TaskRepo;
use serde_json::json;

#[tokio::test]
async fn large_root_detail_and_list_restore_all_child_ids() {
    let root_dir = common::TestDir::new("large-root-condition");
    let harness = common::test_app(root_dir.path(), "large-root-condition").await;
    let project: ProjectResponse = common::json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({"name":"Large root"}),
        StatusCode::OK,
    )
    .await;
    let root: TaskResponse = common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{}/tasks", project.id),
        json!({"title":"Large coordination root", "task_type":"task"}),
        StatusCode::OK,
    )
    .await;
    let db = &*harness.state.db;
    let mut tx = db.pool().begin().await.unwrap();
    let mut ids = Vec::new();
    for i in 0..1000 {
        let id = db::new_uuid_v4();
        sqlx::query("INSERT INTO task(id,project_id,parent_task_id,title,status,subtask_order,created_at,updated_at) VALUES (?,?,?,'child','todo',?,'2026-10-25','2026-10-25')")
            .bind(&id).bind(&project.id).bind(&root.id).bind(i).execute(&mut *tx).await.unwrap();
        ids.push(id);
    }
    sqlx::query("UPDATE task SET status='in_progress',metadata_json='{\"coordination_review_pending\":true}' WHERE id=?")
        .bind(&root.id).execute(&mut *tx).await.unwrap();
    db.produce_condition_in_tx(&mut tx, &root.id, db::ConditionChange::Full)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let raw: String = sqlx::query_scalar("SELECT condition_json FROM task WHERE id=?")
        .bind(&root.id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert!(raw.len() <= db::CONDITION_JSON_LIMIT);
    let stored = db::task_condition::decode(&raw).unwrap();
    let counts = &stored
        .evidence()
        .overflow
        .as_ref()
        .expect("crossed cap")
        .collection_counts;
    assert!(counts.contains_key("primary") || counts.contains_key("resume"));
    let row = TaskRepo::get_by_id(db, &root.id, false)
        .await
        .unwrap()
        .unwrap();
    let mut c = db.pool().acquire().await.unwrap();
    let facts = db::ConditionFacts::load(&mut c, &root.id).await.unwrap();
    drop(c);
    let mut legacy = db::LegacyConditionInput::from(&row);
    legacy.facts = Some(facts);
    let expected = db::map_legacy_condition(&legacy).public();
    assert!(serde_json::to_string(&expected).unwrap().len() > db::CONDITION_JSON_LIMIT);
    let detail: TaskResponse = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}", root.id),
        StatusCode::OK,
    )
    .await;
    assert_eq!(detail.condition, expected);
    let page: serde_json::Value = common::empty_request(
        &harness.app,
        Method::GET,
        &format!(
            "/api/v1/projects/{}/tasks?status=in_progress&limit=100",
            project.id
        ),
        StatusCode::OK,
    )
    .await;
    let listed = page["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["id"] == root.id)
        .unwrap();
    assert_eq!(
        listed["condition"],
        serde_json::to_value(&expected).unwrap()
    );
    let public = serde_json::to_value(expected).unwrap();
    assert_eq!(public["primary"]["remaining"], json!(ids));
    assert_eq!(public["resume"]["kind"], "reconcile");
    // Read restoration changes no storage, queue or Attention inputs.
    let after: String = sqlx::query_scalar("SELECT condition_json FROM task WHERE id=?")
        .bind(&root.id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(raw, after);
    let updated: TaskResponse = common::json_request(
        &harness.app,
        Method::PATCH,
        &format!("/api/v1/tasks/{}", root.id),
        json!({"title":"Root renamed", "version":detail.version}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        updated.condition, detail.condition,
        "an ordinary write's public response keeps the complete sets"
    );
    let mut version = updated.version;
    for annotation in [
        Some(json!({"type":"workspace_error", "blocking_reason":"event probe"}).to_string()),
        None,
    ] {
        let changed = TaskRepo::update_status(
            db,
            db::UpdateTaskStatus {
                id: root.id.clone(),
                expected_version: version,
                status: "in_progress".into(),
                assignee_id: None,
                error_annotation: Some(annotation),
                blocked_json: None,
                failed_json: None,
                updated_at: db::now_rfc3339(),
            },
        )
        .await
        .unwrap();
        version = changed.version;
        let captured = harness
            .state
            .task_service
            .task_action_snapshot_for_task(
                row.clone(),
                &api_types::Actor::user(api_types::UserActionSource::Api),
            )
            .await
            .unwrap();
        assert_eq!(
            captured.task.condition.public(),
            row.condition.public(),
            "an already restored committed snapshot is not rehydrated from a newer row"
        );
        let payload: String = sqlx::query_scalar("SELECT payload_json FROM domain_event WHERE entity_id=? AND event_type='task.interruption_changed' ORDER BY rowid DESC LIMIT 1")
            .bind(&root.id).fetch_one(db.pool()).await.unwrap();
        let event: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(
            event["condition"],
            serde_json::to_value(changed.condition.public()).unwrap()
        );
        let response: TaskResponse = common::empty_request(
            &harness.app,
            Method::GET,
            &format!("/api/v1/tasks/{}", root.id),
            StatusCode::OK,
        )
        .await;
        assert_eq!(
            event["condition"],
            serde_json::to_value(response.condition).unwrap(),
            "event projection retains the same complete sets as REST"
        );
    }
    let mut tx = db.pool().begin().await.unwrap();
    sqlx::query("UPDATE task SET status='done' WHERE parent_task_id=?")
        .bind(&root.id)
        .execute(&mut *tx)
        .await
        .unwrap();
    db.produce_condition_in_tx(&mut tx, &root.id, db::ConditionChange::Children)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let raw: String = sqlx::query_scalar("SELECT condition_json FROM task WHERE id=?")
        .bind(&root.id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    let stored = db::task_condition::decode(&raw).unwrap();
    assert!(raw.len() <= db::CONDITION_JSON_LIMIT);
    assert!(stored
        .evidence()
        .overflow
        .as_ref()
        .unwrap()
        .collection_counts
        .contains_key("resume"));
    let ready: TaskResponse = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}", root.id),
        StatusCode::OK,
    )
    .await;
    let ready = serde_json::to_value(ready.condition).unwrap();
    assert_eq!(ready["resume"]["child_ids"], json!(ids));
}
