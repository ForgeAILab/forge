//! Task lists repository tests.
use super::*;

#[tokio::test]
async fn task_page_projection_batches_roles_and_retry_history() {
    let db = sqlite_db().await;
    let (project_id, _, agent_id) = seed_project_repo_agent(&db).await;
    let mut task_ids = Vec::new();
    for index in 0..3 {
        let task_id = seed_task(
            &db,
            &project_id,
            Some(&agent_id),
            "todo".to_owned(),
            &format!("Task {index}"),
        )
        .await;
        for (role, to_state, rejection) in
            [("reviewer", "review", true), ("auditor", "todo", false)]
        {
            TaskRoleAssignmentRepo::assign(
                &db,
                CreateTaskRoleAssignment {
                    id: new_uuid_v4(),
                    task_id: task_id.clone(),
                    role_name: role.to_owned(),
                    assignee_type: None,
                    assignee_id: None,
                    created_at: now_rfc3339(),
                    updated_at: now_rfc3339(),
                },
            )
            .await
            .unwrap();
            TransitionLogRepo::insert(
                &db,
                CreateTransitionLog {
                    id: new_uuid_v4(),
                    task_id: task_id.clone(),
                    from_state: "review".to_owned(),
                    to_state: to_state.to_owned(),
                    trigger_name: Some("reject".to_owned()),
                    triggered_by: "system".to_owned(),
                    bridge: Default::default(),
                    trigger_reason: "test".to_owned(),
                    hook_results_json: Some("{}".to_owned()),
                    rejection,
                    created_at: "2026-09-30T00:00:00Z".to_owned(),
                },
            )
            .await
            .unwrap();
        }
        task_ids.push(task_id);
    }
    let ids = task_ids[..2].iter().map(String::as_str).collect::<Vec<_>>();
    let roles = TaskRoleAssignmentRepo::list_by_tasks(&db, &ids)
        .await
        .unwrap();
    let transitions = TransitionLogRepo::list_by_tasks(&db, &ids).await.unwrap();
    assert_eq!(roles.len(), 6);
    assert_eq!(transitions.len(), 4);
    for task_id in ids {
        let actual_roles = roles
            .iter()
            .filter(|row| row.task_id == task_id)
            .collect::<Vec<_>>();
        let expected_roles = TaskRoleAssignmentRepo::list_by_task(&db, task_id)
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(actual_roles).unwrap(),
            serde_json::to_value(expected_roles).unwrap()
        );
        let actual_transitions = transitions
            .iter()
            .filter(|row| row.task_id == task_id)
            .collect::<Vec<_>>();
        let expected_transitions = TransitionLogRepo::list_by_task(&db, task_id).await.unwrap();
        assert_eq!(
            serde_json::to_value(actual_transitions).unwrap(),
            serde_json::to_value(expected_transitions).unwrap()
        );
    }
    assert!(TaskRoleAssignmentRepo::list_by_tasks(&db, &[])
        .await
        .unwrap()
        .is_empty());
    assert!(TransitionLogRepo::list_by_tasks(&db, &[])
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn task_page_projection_executions_preserve_authority_with_bounded_history() {
    let db = sqlite_db().await;
    let (project_id, _, agent_id) = seed_project_repo_agent(&db).await;
    let first = seed_task(&db, &project_id, None, "todo".to_owned(), "First").await;
    let second = seed_task(&db, &project_id, None, "todo".to_owned(), "Second").await;
    let hidden = seed_task(&db, &project_id, None, "todo".to_owned(), "Not on page").await;
    for task_id in [&first, &second, &hidden] {
        // The older coder alias session and explicit blocked row must survive
        // more than 100 newer unrelated terminal rows.
        for (id, role, status, session, agent, created_at) in [
            (
                format!("{task_id}-blocked"),
                "auditor",
                "completed",
                None,
                None,
                "2026-09-01T00:00:00Z",
            ),
            (
                format!("{task_id}-resume"),
                "executor",
                "completed",
                Some("coder-session"),
                Some(agent_id.as_str()),
                "2026-09-02T00:00:00Z",
            ),
            (
                format!("{task_id}-interactive"),
                "interactive",
                "running",
                Some("live-session"),
                None,
                "2026-09-03T00:00:00Z",
            ),
            (
                format!("{task_id}-lease"),
                "interactive",
                "running",
                None,
                None,
                "2026-09-04T00:00:00Z",
            ),
        ] {
            sqlx::query("INSERT INTO execution (id, task_id, role, status, agent_session_id, agent_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
                .bind(id).bind(task_id).bind(role).bind(status).bind(session).bind(agent)
                .bind(created_at).bind(created_at).execute(db.pool()).await.unwrap();
        }
        for index in 0..105 {
            sqlx::query("INSERT INTO execution (id, task_id, role, status, created_at, updated_at) VALUES (?, ?, 'reviewer', 'completed', ?, ?)")
                .bind(format!("{task_id}-history-{index:03}")).bind(task_id)
                .bind("2026-09-05T00:00:00Z").bind("2026-09-05T00:00:00Z")
                .execute(db.pool()).await.unwrap();
        }
    }
    let queries = [&first, &second]
        .into_iter()
        .map(|task_id| crate::TaskExecutionProjectionQuery {
            task_id: task_id.clone(),
            current_role: Some("coder".to_owned()),
            blocked_execution_id: Some(format!("{task_id}-blocked")),
        })
        .collect::<Vec<_>>();
    let rows = ExecutionRepo::list_task_projection_executions(&db, &queries)
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        10,
        "five representatives per Task, not all history"
    );
    for task_id in [&first, &second] {
        let ids = rows
            .iter()
            .filter(|row| &row.task_id == task_id)
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>();
        for suffix in ["blocked", "resume", "interactive", "lease", "history-104"] {
            assert!(ids.contains(&format!("{task_id}-{suffix}").as_str()));
        }
        assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
    }
    assert!(rows.iter().all(|row| row.task_id != hidden));
    assert!(ExecutionRepo::list_task_projection_executions(&db, &[])
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn task_page_projection_links_select_latest_per_task() {
    use crate::{
        CreateProjectIntegration, CreateTaskExternalLink, ExternalLinkRepo, IntegrationPlatform,
        IntegrationRepo,
    };
    let db = sqlite_db().await;
    let project_id = seed_project(&db, "Links", None).await;
    let integration_id = new_uuid_v4();
    let now = now_rfc3339();
    IntegrationRepo::create_integration(
        &db,
        CreateProjectIntegration {
            id: integration_id.clone(),
            project_id: project_id.clone(),
            platform: IntegrationPlatform::Github,
            base_url: "https://api.github.com".to_owned(),
            owner: "owner".to_owned(),
            repo: "repo".to_owned(),
            token_secret_ref: "test".to_owned(),
            poll_interval_secs: 60,
            sync_filter: "{}".to_owned(),
            default_task_state: None,
            default_assignee_type: None,
            default_assignee_id: None,
            enabled: false,
            last_polled_at: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    let mut task_ids = Vec::new();
    for index in 0..3 {
        let task_id = seed_task(&db, &project_id, None, "todo".to_owned(), "Links").await;
        for issue in 1..=2 {
            ExternalLinkRepo::create_link(
                &db,
                CreateTaskExternalLink {
                    id: format!("{task_id}-link-{issue}"),
                    task_id: task_id.clone(),
                    integration_id: integration_id.clone(),
                    platform: "github".to_owned(),
                    remote_owner: "owner".to_owned(),
                    remote_repo: "repo".to_owned(),
                    remote_issue_number: index * 2 + issue,
                    remote_url: format!("https://example.com/{index}/{issue}"),
                    global_id: format!("link-{index}-{issue}"),
                    synced_at: now.clone(),
                    created_at: now.clone(),
                    updated_at: now.clone(),
                },
            )
            .await
            .unwrap();
        }
        task_ids.push(task_id);
    }
    let ids = task_ids[..2].iter().map(String::as_str).collect::<Vec<_>>();
    let links = ExternalLinkRepo::list_latest_links_for_tasks(&db, &ids)
        .await
        .unwrap();
    assert_eq!(links.len(), 2);
    for task_id in ids {
        let latest = ExternalLinkRepo::get_by_task_id(&db, task_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            links.iter().find(|link| link.task_id == task_id),
            Some(&latest)
        );
    }
    assert!(ExternalLinkRepo::list_latest_links_for_tasks(&db, &[])
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn task_list_hides_cancelled_and_archived_by_default() {
    let db = sqlite_db().await;
    let (project_id, _repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let visible_id = seed_task(
        &db,
        &project_id,
        Some(&agent_id),
        "todo".to_owned(),
        "Visible",
    )
    .await;
    let cancelled_id = seed_task(
        &db,
        &project_id,
        Some(&agent_id),
        "cancelled".to_owned(),
        "Cancelled",
    )
    .await;
    let archived_id = seed_task(
        &db,
        &project_id,
        Some(&agent_id),
        "done".to_owned(),
        "Archived",
    )
    .await;
    let archived = TaskRepo::get_by_id(&db, &archived_id, false)
        .await
        .unwrap()
        .unwrap();
    TaskRepo::archive(
        &db,
        ArchiveTask {
            id: archived_id.clone(),
            expected_version: archived.version,
            archived_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();

    let default_page = TaskRepo::list(
        &db,
        TaskListQuery {
            project_id: project_id.clone(),
            q: None,
            statuses: Vec::new(),
            agent_ids: Vec::new(),
            assignee_types: Vec::new(),
            assignee_ids: Vec::new(),
            priority: None,
            include_archived: false,
            include_cancelled: false,
            include_deleted: false,
            page: page(10),
        },
    )
    .await
    .unwrap();
    let default_ids = default_page
        .items
        .iter()
        .map(|task| task.id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(default_ids, vec![visible_id.as_str()]);

    let included_page = TaskRepo::list(
        &db,
        TaskListQuery {
            project_id,
            q: None,
            statuses: Vec::new(),
            agent_ids: vec![agent_id],
            assignee_types: Vec::new(),
            assignee_ids: Vec::new(),
            priority: None,
            include_archived: true,
            include_cancelled: true,
            include_deleted: false,
            page: page(10),
        },
    )
    .await
    .unwrap();
    let included_ids = included_page
        .items
        .iter()
        .map(|task| task.id.as_str())
        .collect::<Vec<_>>();
    assert!(included_ids.contains(&visible_id.as_str()));
    assert!(included_ids.contains(&cancelled_id.as_str()));
    assert!(included_ids.contains(&archived_id.as_str()));
}

#[tokio::test]
async fn task_list_filters_by_user_assignee() {
    let db = sqlite_db().await;
    let (project_id, _repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let human_task_id = seed_task(&db, &project_id, None, "todo".to_owned(), "Human task").await;
    let agent_task_id = seed_task(
        &db,
        &project_id,
        Some(&agent_id),
        "todo".to_owned(),
        "Agent task",
    )
    .await;
    TaskRoleAssignmentRepo::assign(
        &db,
        CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: human_task_id.clone(),
            role_name: "coder".to_owned(),
            assignee_type: Some(crate::AssigneeKind::User),
            assignee_id: Some("human".to_owned()),
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();

    let user_page = TaskRepo::list(
        &db,
        TaskListQuery {
            project_id: project_id.clone(),
            q: None,
            statuses: Vec::new(),
            agent_ids: Vec::new(),
            assignee_types: vec!["user".to_owned()],
            assignee_ids: Vec::new(),
            priority: None,
            include_archived: false,
            include_cancelled: false,
            include_deleted: false,
            page: page(10),
        },
    )
    .await
    .unwrap();
    assert_eq!(user_page.items.len(), 1);
    assert_eq!(user_page.items[0].id, human_task_id);

    let human_page = TaskRepo::list(
        &db,
        TaskListQuery {
            project_id,
            q: None,
            statuses: Vec::new(),
            agent_ids: Vec::new(),
            assignee_types: vec!["user".to_owned()],
            assignee_ids: vec!["human".to_owned()],
            priority: None,
            include_archived: false,
            include_cancelled: false,
            include_deleted: false,
            page: page(10),
        },
    )
    .await
    .unwrap();
    assert_eq!(human_page.items.len(), 1);
    assert_eq!(human_page.items[0].id, human_task_id);
    assert_ne!(human_page.items[0].id, agent_task_id);
}

#[tokio::test]
async fn task_list_filters_by_search_query() {
    let db = sqlite_db().await;
    let (project_id, _repo_id, _agent_id) = seed_project_repo_agent(&db).await;
    let alpha_id = seed_task(&db, &project_id, None, "todo".to_owned(), "Alpha release").await;
    let description_id = seed_task(
        &db,
        &project_id,
        None,
        "todo".to_owned(),
        "Description only",
    )
    .await;
    let percent_id = seed_task(&db, &project_id, None, "todo".to_owned(), "100% literal").await;
    let wildcard_id = seed_task(&db, &project_id, None, "todo".to_owned(), "100x wildcard").await;
    seed_task(&db, &project_id, None, "todo".to_owned(), "Beta rollout").await;

    let description_task = TaskRepo::get_by_id(&db, &description_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    TaskRepo::update(
        &db,
        UpdateTask {
            id: description_id.clone(),
            expected_version: description_task.version,
            title: None,
            description: Some(Some("Needle lives in this description".to_owned())),
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("description updates");

    let title_page = TaskRepo::list(
        &db,
        TaskListQuery {
            project_id: project_id.clone(),
            q: Some("release".to_owned()),
            statuses: Vec::new(),
            agent_ids: Vec::new(),
            assignee_types: Vec::new(),
            assignee_ids: Vec::new(),
            priority: None,
            include_archived: false,
            include_cancelled: false,
            include_deleted: false,
            page: page(10),
        },
    )
    .await
    .expect("search by title");
    assert_eq!(title_page.items.len(), 1);
    assert_eq!(title_page.items[0].id, alpha_id);

    let description_page = TaskRepo::list(
        &db,
        TaskListQuery {
            project_id: project_id.clone(),
            q: Some("needle".to_owned()),
            statuses: Vec::new(),
            agent_ids: Vec::new(),
            assignee_types: Vec::new(),
            assignee_ids: Vec::new(),
            priority: None,
            include_archived: false,
            include_cancelled: false,
            include_deleted: false,
            page: page(10),
        },
    )
    .await
    .expect("search by description");
    assert_eq!(description_page.items.len(), 1);
    assert_eq!(description_page.items[0].id, description_id);

    let literal_page = TaskRepo::list(
        &db,
        TaskListQuery {
            project_id,
            q: Some("100%".to_owned()),
            statuses: Vec::new(),
            agent_ids: Vec::new(),
            assignee_types: Vec::new(),
            assignee_ids: Vec::new(),
            priority: None,
            include_archived: false,
            include_cancelled: false,
            include_deleted: false,
            page: page(10),
        },
    )
    .await
    .expect("search escapes wildcards");
    assert_eq!(literal_page.items.len(), 1);
    assert_eq!(literal_page.items[0].id, percent_id);
    assert_ne!(literal_page.items[0].id, wildcard_id);
}

#[tokio::test]
async fn next_subtask_order_appends_after_existing_siblings() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let (project_id, _repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let parent_task_id = seed_task(
        &db,
        &project_id,
        Some(&agent_id),
        "todo".to_owned(),
        "Parent",
    )
    .await;

    assert_eq!(
        TaskRepo::next_subtask_order(&db, &parent_task_id)
            .await
            .expect("next order loads"),
        0
    );
    let first_order = TaskRepo::next_subtask_order(&db, &parent_task_id)
        .await
        .expect("first order loads");
    seed_ordered_task(
        &db,
        &project_id,
        Some(&parent_task_id),
        Some(first_order),
        "First",
        &now,
    )
    .await;
    assert_eq!(
        TaskRepo::next_subtask_order(&db, &parent_task_id)
            .await
            .expect("next order loads"),
        1
    );
    let second_order = TaskRepo::next_subtask_order(&db, &parent_task_id)
        .await
        .expect("second order loads");
    seed_ordered_task(
        &db,
        &project_id,
        Some(&parent_task_id),
        Some(second_order),
        "Second",
        &now,
    )
    .await;

    assert_eq!(
        TaskRepo::next_subtask_order(&db, &parent_task_id)
            .await
            .expect("next order loads"),
        2
    );
}

#[tokio::test]
async fn list_subtasks_ordered_uses_subtask_order_before_tiebreakers() {
    let db = sqlite_db().await;
    let (project_id, _repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let parent_task_id = seed_task(
        &db,
        &project_id,
        Some(&agent_id),
        "todo".to_owned(),
        "Parent",
    )
    .await;

    seed_ordered_task(
        &db,
        &project_id,
        Some(&parent_task_id),
        Some(2),
        "Third",
        "2026-04-18T00:00:00Z",
    )
    .await;
    seed_ordered_task(
        &db,
        &project_id,
        Some(&parent_task_id),
        Some(0),
        "First",
        "2026-04-18T00:02:00Z",
    )
    .await;
    seed_ordered_task(
        &db,
        &project_id,
        Some(&parent_task_id),
        Some(1),
        "Second",
        "2026-04-18T00:01:00Z",
    )
    .await;

    let titles = TaskRepo::list_subtasks_ordered(&db, &parent_task_id)
        .await
        .expect("subtasks list")
        .into_iter()
        .map(|task| task.title)
        .collect::<Vec<_>>();

    assert_eq!(titles, vec!["First", "Second", "Third"]);
}

#[tokio::test]
async fn reorder_subtasks_persists_and_rejects_invalid_orders() {
    let db = sqlite_db().await;
    let (project_id, _repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let parent_task_id = seed_task(
        &db,
        &project_id,
        Some(&agent_id),
        "todo".to_owned(),
        "Parent",
    )
    .await;
    let first = seed_ordered_task(
        &db,
        &project_id,
        Some(&parent_task_id),
        Some(0),
        "First",
        "2026-04-18T00:00:00Z",
    )
    .await;
    let second = seed_ordered_task(
        &db,
        &project_id,
        Some(&parent_task_id),
        Some(1),
        "Second",
        "2026-04-18T00:01:00Z",
    )
    .await;

    let reordered_at = "2026-04-18T00:02:00Z";
    TaskRepo::reorder_subtasks(
        &db,
        &parent_task_id,
        &[second.id.clone(), first.id.clone()],
        reordered_at,
    )
    .await
    .expect("subtasks reorder");

    let ordered = TaskRepo::list_subtasks_ordered(&db, &parent_task_id)
        .await
        .expect("subtasks list");
    assert_eq!(ordered[0].id, second.id);
    assert_eq!(ordered[0].subtask_order, Some(0));
    assert_eq!(ordered[0].updated_at, reordered_at);
    assert_eq!(ordered[1].id, first.id);
    assert_eq!(ordered[1].subtask_order, Some(1));

    let unknown = TaskRepo::reorder_subtasks(
        &db,
        &parent_task_id,
        &[ordered[0].id.clone(), new_uuid_v4()],
        "2026-04-18T00:03:00Z",
    )
    .await;
    assert!(matches!(unknown, Err(DbError::NotFound)));

    let mismatched_length = TaskRepo::reorder_subtasks(
        &db,
        &parent_task_id,
        &[ordered[0].id.clone()],
        "2026-04-18T00:04:00Z",
    )
    .await;
    assert!(matches!(mismatched_length, Err(DbError::InvalidTransition)));
}

#[tokio::test]
async fn task_list_orders_equal_board_positions_by_created_at() {
    let db = sqlite_db().await;
    let (project_id, _repo_id, _agent_id) = seed_project_repo_agent(&db).await;
    let later = seed_ordered_task(
        &db,
        &project_id,
        None,
        None,
        "Later",
        "2026-04-18T00:01:00Z",
    )
    .await;
    let earlier = seed_ordered_task(
        &db,
        &project_id,
        None,
        None,
        "Earlier",
        "2026-04-18T00:00:00Z",
    )
    .await;
    for task_id in [&later.id, &earlier.id] {
        sqlx::query("UPDATE task SET board_position = 10.0 WHERE id = ?")
            .bind(task_id)
            .execute(db.pool())
            .await
            .expect("board position updates");
    }

    let page = TaskRepo::list(
        &db,
        TaskListQuery {
            project_id,
            q: None,
            statuses: Vec::new(),
            agent_ids: Vec::new(),
            assignee_types: Vec::new(),
            assignee_ids: Vec::new(),
            priority: None,
            include_archived: false,
            include_cancelled: false,
            include_deleted: false,
            page: PageRequest {
                cursor: None,
                limit: 10,
                include_total: false,
                sort_by: SortBy::BoardPosition,
                sort_order: SortOrder::Asc,
            },
        },
    )
    .await
    .expect("tasks list");

    assert_eq!(page.items[0].id, earlier.id);
    assert_eq!(page.items[1].id, later.id);
}
