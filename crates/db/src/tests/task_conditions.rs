//! Task conditions repository tests.
use super::*;

#[tokio::test]
async fn task_interruption_changes_are_atomic_and_bounded() {
    let db = sqlite_db().await;
    let (project_id, _repo_id, _) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        None,
        "todo".to_owned(),
        "interruption task",
    )
    .await;
    let task = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task lookup succeeds")
        .expect("task exists");
    let annotation = serde_json::json!({
        "type": "manual_stop",
        "recovery_actions": ["resume_session", "retry_execution"],
    })
    .to_string();
    let failed_reason = "x".repeat(2048);
    let failed = serde_json::json!({
        "kind": "executor_failed",
        "reason": failed_reason,
        "execution_id": "execution-1",
        "message": "unbounded details must not be copied",
    })
    .to_string();
    let updated = TaskRepo::update(
        &db,
        UpdateTask {
            id: task_id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(annotation)),
            blocked_json: None,
            failed_json: Some(Some(failed)),
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("task interruption updates");
    assert_eq!(updated.version, task.version + 1);

    let cleared = TaskRepo::update_status(
        &db,
        UpdateTaskStatus {
            id: task_id.clone(),
            expected_version: updated.version,
            status: updated.status.clone(),
            assignee_id: None,
            error_annotation: Some(None),
            blocked_json: Some(None),
            failed_json: Some(None),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("task interruption clears");
    assert_eq!(cleared.version, updated.version + 1);

    let events = DomainEventRepo::list_events_after(&db, 0, 100)
        .await
        .expect("domain events list");
    let interruption_events = events
        .iter()
        .filter(|event| {
            event.event_type == "task.interruption_changed" && event.entity_id == task_id
        })
        .collect::<Vec<_>>();
    assert_eq!(interruption_events.len(), 2);

    let first: serde_json::Value = serde_json::from_str(&interruption_events[0].payload_json)
        .expect("interruption payload parses");
    assert_eq!(first["task_id"], task_id);
    assert_eq!(first["task_version"], updated.version);
    assert_eq!(first["material_blocker"]["requires_intervention"], true);
    assert_eq!(
        first["material_blocker"]["interruption"]["source"],
        "failed"
    );
    assert_eq!(
        first["material_blocker"]["interruption"]["kind"],
        "executor_failed"
    );
    assert_eq!(
        first["material_blocker"]["interruption"]["reason"]
            .as_str()
            .expect("bounded reason is a string")
            .chars()
            .count(),
        512
    );
    assert_eq!(first["condition"]["details"]["execution_id"], "execution-1");
    assert!(first.get("requires_intervention").is_none());
    assert!(first.get("interruption").is_none());
    assert_eq!(first["condition"]["kind"], "failed");
    assert!(
        first["material_blocker"]["interruption"]
            .get("recovery_actions")
            .is_none(),
        "interruption evidence must not persist an action allowlist"
    );
    let expected_dedupe = format!("task-interruption-update:{task_id}:{}", updated.version);
    assert!(interruption_events[0]
        .dedupe_key
        .as_deref()
        .unwrap()
        .starts_with(&expected_dedupe));

    let second: serde_json::Value = serde_json::from_str(&interruption_events[1].payload_json)
        .expect("cleared interruption payload parses");
    assert_eq!(second["task_version"], cleared.version);
    assert_eq!(second["material_blocker"]["requires_intervention"], false);
    assert!(second["material_blocker"]["interruption"].is_null());
}

#[tokio::test]
async fn task_soft_delete_refuses_builtin_and_renamed_review_gates() {
    let db = sqlite_db().await;
    let (project_id, _, _) = seed_project_repo_agent(&db).await;
    let review = serde_json::json!({
        "name": "qa_review", "kind": "gate", "column": "Review",
        "display_name": "QA Review", "role": "reviewer", "canonical_phase": "review",
        "hooks": {}, "config": {}
    });
    for change in [
        None,
        Some(("kind", "active")),
        Some(("role", "planner")),
        Some(("canonical_phase", "working")),
    ] {
        let mut state = review.clone();
        if let Some((key, value)) = change {
            state[key] = value.into();
        }
        let refuses = change.is_none();
        let workflow = WorkflowDefinition {
            states: vec![serde_json::from_value(state).unwrap()],
            roles: Vec::new(),
            configuration: Vec::new(),
            cancellation_state: None,
        };
        sqlx::query("UPDATE project SET workflow_definition = ? WHERE id = ?")
            .bind(serde_json::to_string(&workflow).unwrap())
            .bind(&project_id)
            .execute(db.pool())
            .await
            .unwrap();
        let id = seed_task(
            &db,
            &project_id,
            None,
            "qa_review".into(),
            "review deletion",
        )
        .await;
        let task = TaskRepo::get_by_id(&db, &id, false).await.unwrap().unwrap();
        let now = now_rfc3339();
        let result = TaskRepo::soft_delete(
            &db,
            crate::SoftDeleteTask {
                id: id.clone(),
                expected_version: task.version,
                deleted_at: now.clone(),
                updated_at: now,
            },
        )
        .await;
        if refuses {
            assert!(matches!(result, Err(DbError::InvalidSoftDelete)));
            let unchanged = TaskRepo::get_by_id(&db, &id, true).await.unwrap().unwrap();
            assert!(unchanged.deleted_at.is_none());
            assert_eq!(unchanged.version, task.version);
        } else {
            assert!(result.unwrap().deleted_at.is_some());
        }
    }
    sqlx::query("UPDATE project SET workflow_definition = '{}' WHERE id = ?")
        .bind(&project_id)
        .execute(db.pool())
        .await
        .unwrap();
    for status in ["review", "in_progress", "merging"] {
        let id = seed_task(&db, &project_id, None, status.into(), "active deletion").await;
        let task = TaskRepo::get_by_id(&db, &id, false).await.unwrap().unwrap();
        let now = now_rfc3339();
        let result = TaskRepo::soft_delete(
            &db,
            crate::SoftDeleteTask {
                id,
                expected_version: task.version,
                deleted_at: now.clone(),
                updated_at: now,
            },
        )
        .await;
        assert!(
            matches!(result, Err(DbError::InvalidSoftDelete)),
            "{status}"
        );
    }
}

#[tokio::test]
async fn archived_interruption_shape_decodes_at_the_event_boundary() {
    let db = sqlite_db().await;
    let stored=DomainEventRepo::append_event(&db,crate::CreateDomainEvent{id:new_uuid_v4(),event_type:"task.interruption_changed".into(),entity_type:"task".into(),entity_id:"archived".into(),actor_type:"system".into(),actor_id:None,scope_type:"task".into(),scope_id:"archived".into(),correlation_id:new_uuid_v4(),causation_id:None,causation_depth:0,dedupe_key:None,payload_json:serde_json::json!({"task_id":"archived","task_version":7,"task_status":"review","requires_intervention":true,"interruption":{"source":"blocked","kind":"review_needs_owner","reason":"finding","execution_id":"old-reporter"}}).to_string(),created_at:now_rfc3339()}).await.unwrap();
    let raw = stored.payload_json.clone();
    let material = stored.task_material_blocker().unwrap();
    assert!(material.requires_intervention);
    assert_eq!(
        material.interruption.unwrap(),
        serde_json::json!({"source":"blocked","kind":"review_needs_owner","reason":"finding"})
    );
    let reloaded = DomainEventRepo::list_events_after(&db, 0, 100)
        .await
        .unwrap()
        .into_iter()
        .find(|e| e.id == stored.id)
        .unwrap();
    assert_eq!(
        raw, reloaded.payload_json,
        "decoding does not rewrite stored history"
    );
}
