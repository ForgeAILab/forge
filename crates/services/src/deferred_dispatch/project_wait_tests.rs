use super::*;
use db::{CreateProject, CreateTask, ProjectRepo};

async fn fixture() -> (db::SqliteDb, Task) {
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let db = db::SqliteDb::new(pool);
    let at = "2026-10-02T00:00:00Z".to_owned();
    ProjectRepo::create(
        &db,
        CreateProject {
            id: "project".into(),
            owner_id: None,
            name: "Project waits".into(),
            primary_repo_id: None,
            settings: "{}".into(),
            workflow_definition: "{}".into(),
            created_at: at.clone(),
            updated_at: at.clone(),
        },
    )
    .await
    .unwrap();
    let task = TaskRepo::create(
        &db,
        CreateTask {
            id: "task".into(),
            project_id: "project".into(),
            parent_task_id: None,
            assignee_type: None,
            assignee_id: None,
            title: "Project wait".into(),
            description: None,
            task_type: "task".into(),
            status: "todo".into(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            subtask_order: None,
            plan: None,
            created_at: at.clone(),
            updated_at: at,
        },
    )
    .await
    .unwrap();
    (db, task)
}

#[tokio::test]
async fn project_wait_writer_persists_each_subcause_and_changes_it_without_changing_text() {
    let (db, mut task) = fixture().await;
    let message = "project_waiting_on_owner: unchanged message";
    for wait in [ProjectWait::AtCapacity, ProjectWait::WaitingOnOwner] {
        assert!(record_project_capacity_wait(&db, &task, wait, message)
            .await
            .unwrap());
        task = TaskRepo::get_by_id(&db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        let refusal = current_dispatch_disposition(&task).unwrap();
        assert_eq!(refusal.project_wait, Some(wait));
        assert_eq!(refusal.safe_message, message);
        assert_eq!(refusal.blocker_digest, dispatch_blocker_digest(message));
        assert_eq!(
            refusal.capacity_reason(),
            (wait.reason(), "unchanged message")
        );
        assert_eq!(task.condition.read().refusal.unwrap(), refusal);
        let health = crate::task_diagnostics::derive_workflow_health(
            &task,
            &crate::workflow::default_workflow::default_workflow(),
            &[],
            None,
            None,
            false,
            None,
        );
        assert_eq!(health.stale_reason.as_deref(), Some(wait.reason()));
        let before = task.clone();
        assert!(!record_project_capacity_wait(&db, &task, wait, message)
            .await
            .unwrap());
        task = TaskRepo::get_by_id(&db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task, before, "same observation never refreshes the row");
        assert!(
            record_project_capacity_wait(&db, &task, wait, "Reworded message")
                .await
                .unwrap()
        );
        task = TaskRepo::get_by_id(&db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            task.condition.read().refusal.unwrap().capacity_reason(),
            (wait.reason(), "Reworded message")
        );
        // Restore identical text before the other subcause is stated.
        assert!(record_project_capacity_wait(&db, &task, wait, message)
            .await
            .unwrap());
        task = TaskRepo::get_by_id(&db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn literal_pre_upgrade_waits_keep_offers_health_operations_and_attention_material() {
    let rows: Vec<serde_json::Value> = serde_json::from_str(include_str!(
        "../../../db/src/task_condition/readers/fixtures/pre_upgrade_project_waits.json"
    ))
    .unwrap();
    for row in rows {
        let (db, task) = fixture().await;
        sqlx::query("UPDATE task SET condition_json=?, metadata_json=?, version=1 WHERE id=?")
            .bind(row["condition_json"].as_str().unwrap())
            .bind(row["metadata_json"].as_str().unwrap())
            .bind(&task.id)
            .execute(db.pool())
            .await
            .unwrap();
        let before = TaskRepo::get_by_id(&db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        let mut snapshot = crate::task_actions::test_support::snapshot("todo", None);
        snapshot.task = before.clone();
        let offers = crate::available_actions(&snapshot);
        assert_eq!(
            offers,
            crate::task_actions::legacy::available_actions(&(&snapshot).into())
        );
        assert!(offers.iter().any(|offer| offer.reason == "dispatch_wait"));
        let health = crate::task_diagnostics::derive_workflow_health(
            &before,
            &snapshot.workflow,
            &[],
            None,
            None,
            false,
            None,
        );
        assert_eq!(health.label, row["label"].as_str().unwrap());
        assert_eq!(health.stale_reason.as_deref(), row["reason"].as_str());
        assert_eq!(health.message.as_deref(), row["message"].as_str());
        assert_eq!(before.condition.read().operator_reason, None);
        assert!(db.blocked_task_operator_rows().await.unwrap().is_empty());
        assert!(db.task_operator_retry_rows().await.unwrap().is_empty());

        let wait: ProjectWait = serde_json::from_value(row["project_wait"].clone()).unwrap();
        assert!(!record_project_capacity_wait(
            &db,
            &before,
            wait,
            row["safe_message"].as_str().unwrap()
        )
        .await
        .unwrap());
        let unchanged = TaskRepo::get_by_id(&db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            unchanged, before,
            "upgrade plus an unchanged scan preserves the old row"
        );

        let mut typed = before.clone();
        let mut metadata: serde_json::Value =
            serde_json::from_str(typed.metadata_json.as_deref().unwrap()).unwrap();
        metadata["dispatch_disposition"]["project_wait"] = row["project_wait"].clone();
        typed.metadata_json = Some(metadata.to_string());
        typed.condition = db::map_legacy_condition(&db::LegacyConditionInput::from(&typed));
        snapshot.task = typed.clone();
        assert_eq!(crate::available_actions(&snapshot), offers);
        assert_eq!(
            crate::task_diagnostics::derive_workflow_health(
                &typed,
                &snapshot.workflow,
                &[],
                None,
                None,
                false,
                None,
            ),
            health
        );
        assert_eq!(typed.condition.public(), before.condition.public());
        assert_eq!(
            typed.condition.read().operator_reason,
            before.condition.read().operator_reason
        );
        let old_event = db::CreateDomainEvent::task_interruption_changed(&before);
        let typed_event = db::CreateDomainEvent::task_interruption_changed(&typed);
        assert_eq!(old_event.dedupe_key, typed_event.dedupe_key);
        assert_eq!(old_event.payload_json, typed_event.payload_json);
        assert!(!db::material_blocker(&typed.condition).requires_intervention);
    }
}

#[tokio::test]
async fn finishing_a_project_wait_preserves_its_typed_subcause() {
    let (db, mut task) = fixture().await;
    record_project_capacity_wait(&db, &task, ProjectWait::WaitingOnOwner, "Reworded message")
        .await
        .unwrap();
    // A transition's final barrier version differs from the refusal's version.
    sqlx::query("UPDATE task SET version=2 WHERE id=?")
        .bind(&task.id)
        .execute(db.pool())
        .await
        .unwrap();
    finish_machine_wait(&db, &mut task, -1).await.unwrap();
    let refusal = task.condition.read().refusal.unwrap();
    assert_eq!(refusal.task_version, 2);
    assert_eq!(refusal.project_wait, Some(ProjectWait::WaitingOnOwner));
    assert_eq!(
        refusal.capacity_reason(),
        ("project_waiting_on_owner", "Reworded message")
    );
}
