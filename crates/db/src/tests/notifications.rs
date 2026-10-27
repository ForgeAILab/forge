//! Notifications repository tests.
use super::*;

#[tokio::test]
async fn notification_repo_crud_and_cascade_delete() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let (project_id, _repo_id, _agent_id) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        None,
        "todo".to_owned(),
        "Notification target",
    )
    .await;

    let notification = NotificationRepo::create(
        &db,
        crate::CreateNotification {
            id: new_uuid_v4(),
            project_id: project_id.clone(),
            task_id: Some(task_id.clone()),
            event_type: "task.blocked".to_owned(),
            title: "Task blocked".to_owned(),
            body: Some("Need input".to_owned()),
            read: false,
            created_at: now.clone(),
        },
    )
    .await
    .expect("notification creates");
    assert!(!notification.read);

    let page = NotificationRepo::list(
        &db,
        NotificationListQuery {
            project_id: Some(project_id.clone()),
            read: Some(false),
            page: page(20),
        },
    )
    .await
    .expect("notifications list");
    assert_eq!(page.items.len(), 1);

    assert_eq!(
        NotificationRepo::unread_count(&db, Some(&project_id))
            .await
            .expect("unread count"),
        1
    );

    let marked = NotificationRepo::mark_read(&db, &notification.id)
        .await
        .expect("mark read");
    assert!(marked.read);
    assert_eq!(
        NotificationRepo::unread_count(&db, Some(&project_id))
            .await
            .expect("unread count after mark read"),
        0
    );

    NotificationRepo::create(
        &db,
        crate::CreateNotification {
            id: new_uuid_v4(),
            project_id: project_id.clone(),
            task_id: Some(task_id.clone()),
            event_type: "review.failed".to_owned(),
            title: "Review failed".to_owned(),
            body: None,
            read: false,
            created_at: now.clone(),
        },
    )
    .await
    .expect("second notification creates");
    NotificationRepo::create(
        &db,
        crate::CreateNotification {
            id: new_uuid_v4(),
            project_id: project_id.clone(),
            task_id: Some(task_id.clone()),
            event_type: "merge.failed".to_owned(),
            title: "Merge failed".to_owned(),
            body: Some("conflict".to_owned()),
            read: false,
            created_at: now,
        },
    )
    .await
    .expect("third notification creates");

    assert_eq!(
        NotificationRepo::mark_all_read(&db, Some(&project_id))
            .await
            .expect("mark all read"),
        2
    );
    assert_eq!(
        NotificationRepo::unread_count(&db, Some(&project_id))
            .await
            .expect("unread count after mark all"),
        0
    );

    NotificationRepo::delete(&db, &notification.id)
        .await
        .expect("notification delete");

    let remaining_before_cascade: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM notification WHERE project_id = ?")
            .bind(&project_id)
            .fetch_one(db.pool())
            .await
            .expect("count notifications before cascade");
    assert_eq!(remaining_before_cascade, 2);

    sqlx::query("DELETE FROM task WHERE id = ?")
        .bind(&task_id)
        .execute(db.pool())
        .await
        .expect("hard delete task");
    let remaining_after_task_delete: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM notification WHERE project_id = ?")
            .bind(&project_id)
            .fetch_one(db.pool())
            .await
            .expect("count notifications after task delete");
    assert_eq!(remaining_after_task_delete, 0);

    let task_id = seed_task(
        &db,
        &project_id,
        None,
        "todo".to_owned(),
        "Notification target 2",
    )
    .await;
    NotificationRepo::create(
        &db,
        crate::CreateNotification {
            id: new_uuid_v4(),
            project_id: project_id.clone(),
            task_id: Some(task_id),
            event_type: "task.done".to_owned(),
            title: "Done".to_owned(),
            body: None,
            read: false,
            created_at: now_rfc3339(),
        },
    )
    .await
    .expect("notification for project cascade");

    ProjectRepo::delete(&db, &project_id)
        .await
        .expect("project delete cascade");
    let remaining_after_project_delete: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM notification WHERE project_id = ?")
            .bind(&project_id)
            .fetch_one(db.pool())
            .await
            .expect("count notifications after project delete");
    assert_eq!(remaining_after_project_delete, 0);
}
