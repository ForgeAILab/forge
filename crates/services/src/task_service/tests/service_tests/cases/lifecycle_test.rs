use super::super::*;

#[tokio::test]
async fn hook_test_does_not_transition_or_create_execution() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let now = now_rfc3339();
    let task_id = new_uuid_v4();
    let task = TaskRepo::create(
        &*db,
        CreateTask {
            id: task_id.clone(),
            project_id: project_id.clone(),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "Lifecycle hook test".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "todo".to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("task creates");

    let settings = serde_json::json!({
      "lifecycle_hooks": {
        "before_work": [{
          "type": "script",
          "command": "echo ok && echo err >&2",
          "timeout_seconds": 5,
          "blocking": false
        }]
      }
    });
    sqlx::query("UPDATE project SET settings = ?, updated_at = ? WHERE id = ?")
        .bind(settings.to_string())
        .bind(now_rfc3339())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project settings update");

    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus));
    let result = service
        .test_lifecycle_hook(
            &project_id,
            &task_id,
            api_types::LifecycleEvent::BeforeWork,
            0,
        )
        .await
        .expect("hook test succeeds");
    assert_eq!(result.status, "success");
    assert_eq!(result.exit_code, Some(0));
    assert!(result.stdout.contains("ok"));
    assert!(result.stderr.contains("err"));

    let refreshed = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reload")
        .expect("task exists");
    assert_eq!(refreshed.status, "todo");

    let executions = ExecutionRepo::list_by_task(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 10,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("list executions");
    assert!(executions.items.is_empty());
}
