//! Terminal sessions repository tests.
use super::*;

#[tokio::test]
async fn terminal_session_create_get_and_list_filters_running_and_ended() {
    let db = sqlite_db().await;
    let (project_id, repo_id, _agent_id) = seed_project_repo_agent(&db).await;
    let user_id = seed_user(&db).await;
    let task_id = seed_task(&db, &project_id, None, "todo".to_owned(), "Terminal task").await;
    let workspace_id = seed_workspace_for_task(&db, &task_id, &repo_id).await;

    let running = seed_terminal_session(
        &db,
        &task_id,
        &workspace_id,
        &user_id,
        "2026-05-20T00:00:00Z",
    )
    .await;
    let ended = seed_terminal_session(
        &db,
        &task_id,
        &workspace_id,
        &user_id,
        "2026-05-20T00:01:00Z",
    )
    .await;

    let running = TerminalSessionRepo::update_terminal_session_status(
        &db,
        &running.id,
        running.version,
        UpdateTerminalSessionStatus {
            status: TerminalSessionStatus::Running,
            started_at: Some("2026-05-20T00:00:01Z".to_owned()),
            last_activity_at: Some("2026-05-20T00:00:01Z".to_owned()),
            ended_at: None,
            pid: Some(100),
            exit_code: None,
            exit_signal: None,
            exit_reason: None,
        },
    )
    .await
    .expect("session starts running");
    let ended = TerminalSessionRepo::update_terminal_session_status(
        &db,
        &ended.id,
        ended.version,
        UpdateTerminalSessionStatus {
            status: TerminalSessionStatus::Exited,
            started_at: Some("2026-05-20T00:01:01Z".to_owned()),
            last_activity_at: Some("2026-05-20T00:01:05Z".to_owned()),
            ended_at: Some("2026-05-20T00:01:05Z".to_owned()),
            pid: Some(101),
            exit_code: Some(0),
            exit_signal: None,
            exit_reason: Some("process exited".to_owned()),
        },
    )
    .await
    .expect("session exits");

    let loaded = TerminalSessionRepo::get_terminal_session(&db, &running.id)
        .await
        .expect("session loads")
        .expect("session exists");
    assert_eq!(loaded.status, TerminalSessionStatus::Running);
    assert_eq!(loaded.pid, Some(100));

    let active = TerminalSessionRepo::list_terminal_sessions_for_task(&db, &task_id, false)
        .await
        .expect("active sessions list");
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].id, running.id);

    let all = TerminalSessionRepo::list_terminal_sessions_for_task(&db, &task_id, true)
        .await
        .expect("all sessions list");
    assert_eq!(
        all.iter().map(|session| &session.id).collect::<Vec<_>>(),
        vec![&running.id, &ended.id,]
    );

    assert_eq!(
        TerminalSessionRepo::list_running_terminal_sessions_for_task(&db, &task_id)
            .await
            .expect("task running sessions list")
            .len(),
        1
    );
    assert_eq!(
        TerminalSessionRepo::list_running_terminal_sessions_for_user(&db, &user_id)
            .await
            .expect("user running sessions list")
            .len(),
        1
    );
    assert_eq!(
        TerminalSessionRepo::list_running_terminal_sessions_for_workspace(&db, &workspace_id)
            .await
            .expect("workspace running sessions list")
            .len(),
        1
    );
    assert_eq!(
        TerminalSessionRepo::list_all_running_terminal_sessions(&db)
            .await
            .expect("all running sessions list")
            .len(),
        1
    );
}

#[tokio::test]
async fn terminal_session_status_updates_increment_version() {
    let db = sqlite_db().await;
    let (project_id, repo_id, _agent_id) = seed_project_repo_agent(&db).await;
    let user_id = seed_user(&db).await;
    let task_id = seed_task(&db, &project_id, None, "todo".to_owned(), "Terminal status").await;
    let workspace_id = seed_workspace_for_task(&db, &task_id, &repo_id).await;
    let session = seed_terminal_session(
        &db,
        &task_id,
        &workspace_id,
        &user_id,
        "2026-05-20T01:00:00Z",
    )
    .await;

    assert_eq!(session.status, TerminalSessionStatus::Starting);
    assert_eq!(session.version, 1);

    let running = TerminalSessionRepo::update_terminal_session_status(
        &db,
        &session.id,
        session.version,
        UpdateTerminalSessionStatus {
            status: TerminalSessionStatus::Running,
            started_at: Some("2026-05-20T01:00:01Z".to_owned()),
            last_activity_at: Some("2026-05-20T01:00:02Z".to_owned()),
            ended_at: None,
            pid: Some(4242),
            exit_code: None,
            exit_signal: None,
            exit_reason: None,
        },
    )
    .await
    .expect("session transitions to running");
    assert_eq!(running.status, TerminalSessionStatus::Running);
    assert_eq!(running.version, 2);
    assert_eq!(running.pid, Some(4242));

    let exited = TerminalSessionRepo::update_terminal_session_status(
        &db,
        &running.id,
        running.version,
        UpdateTerminalSessionStatus {
            status: TerminalSessionStatus::Exited,
            started_at: running.started_at.clone(),
            last_activity_at: Some("2026-05-20T01:01:00Z".to_owned()),
            ended_at: Some("2026-05-20T01:01:00Z".to_owned()),
            pid: running.pid,
            exit_code: Some(0),
            exit_signal: None,
            exit_reason: Some("process exited".to_owned()),
        },
    )
    .await
    .expect("session transitions to exited");
    assert_eq!(exited.status, TerminalSessionStatus::Exited);
    assert_eq!(exited.version, 3);
    assert_eq!(exited.exit_code, Some(0));
    assert_eq!(exited.ended_at.as_deref(), Some("2026-05-20T01:01:00Z"));
}

#[tokio::test]
async fn terminal_session_size_update_touches_activity() {
    let db = sqlite_db().await;
    let (project_id, repo_id, _agent_id) = seed_project_repo_agent(&db).await;
    let user_id = seed_user(&db).await;
    let task_id = seed_task(&db, &project_id, None, "todo".to_owned(), "Terminal resize").await;
    let workspace_id = seed_workspace_for_task(&db, &task_id, &repo_id).await;
    let session = seed_terminal_session(
        &db,
        &task_id,
        &workspace_id,
        &user_id,
        "2026-05-20T02:00:00Z",
    )
    .await;

    let resized = TerminalSessionRepo::update_terminal_session_size(
        &db,
        &session.id,
        40,
        120,
        "2026-05-20T02:00:10Z",
    )
    .await
    .expect("session resizes");
    assert_eq!(resized.rows, 40);
    assert_eq!(resized.cols, 120);
    assert_eq!(
        resized.last_activity_at.as_deref(),
        Some("2026-05-20T02:00:10Z")
    );

    TerminalSessionRepo::touch_terminal_session_activity(&db, &session.id, "2026-05-20T02:00:20Z")
        .await
        .expect("session activity touches");
    let touched = TerminalSessionRepo::get_terminal_session(&db, &session.id)
        .await
        .expect("session loads")
        .expect("session exists");
    assert_eq!(
        touched.last_activity_at.as_deref(),
        Some("2026-05-20T02:00:20Z")
    );
}

#[tokio::test]
async fn terminal_session_status_update_detects_version_conflict() {
    let db = sqlite_db().await;
    let (project_id, repo_id, _agent_id) = seed_project_repo_agent(&db).await;
    let user_id = seed_user(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        None,
        "todo".to_owned(),
        "Terminal conflict",
    )
    .await;
    let workspace_id = seed_workspace_for_task(&db, &task_id, &repo_id).await;
    let session = seed_terminal_session(
        &db,
        &task_id,
        &workspace_id,
        &user_id,
        "2026-05-20T03:00:00Z",
    )
    .await;

    TerminalSessionRepo::update_terminal_session_status(
        &db,
        &session.id,
        session.version,
        UpdateTerminalSessionStatus {
            status: TerminalSessionStatus::Running,
            started_at: Some("2026-05-20T03:00:01Z".to_owned()),
            last_activity_at: Some("2026-05-20T03:00:01Z".to_owned()),
            ended_at: None,
            pid: Some(500),
            exit_code: None,
            exit_signal: None,
            exit_reason: None,
        },
    )
    .await
    .expect("session starts running");

    let stale = TerminalSessionRepo::update_terminal_session_status(
        &db,
        &session.id,
        session.version,
        UpdateTerminalSessionStatus {
            status: TerminalSessionStatus::Exited,
            started_at: Some("2026-05-20T03:00:01Z".to_owned()),
            last_activity_at: Some("2026-05-20T03:00:02Z".to_owned()),
            ended_at: Some("2026-05-20T03:00:02Z".to_owned()),
            pid: Some(500),
            exit_code: Some(0),
            exit_signal: None,
            exit_reason: None,
        },
    )
    .await;
    assert!(matches!(stale, Err(DbError::VersionConflict)));
}

#[tokio::test]
async fn terminal_sessions_cascade_when_workspace_is_deleted() {
    let db = sqlite_db().await;
    let (project_id, repo_id, _agent_id) = seed_project_repo_agent(&db).await;
    let user_id = seed_user(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        None,
        "todo".to_owned(),
        "Terminal cascade",
    )
    .await;
    let workspace_id = seed_workspace_for_task(&db, &task_id, &repo_id).await;
    let session = seed_terminal_session(
        &db,
        &task_id,
        &workspace_id,
        &user_id,
        "2026-05-20T04:00:00Z",
    )
    .await;

    WorkspaceRepo::delete(&db, &workspace_id)
        .await
        .expect("workspace deletes");

    assert!(TerminalSessionRepo::get_terminal_session(&db, &session.id)
        .await
        .expect("session lookup succeeds")
        .is_none());
    assert!(
        TerminalSessionRepo::list_terminal_sessions_for_task(&db, &task_id, true)
            .await
            .expect("sessions list")
            .is_empty()
    );
}

async fn seed_terminal_session(
    db: &SqliteDb,
    task_id: &str,
    workspace_id: &str,
    user_id: &str,
    created_at: &str,
) -> crate::TerminalSession {
    TerminalSessionRepo::create_terminal_session(
        db,
        CreateTerminalSession {
            id: new_uuid_v4(),
            task_id: task_id.to_owned(),
            workspace_id: workspace_id.to_owned(),
            daemon_id: None,
            created_by_user_id: user_id.to_owned(),
            rows: 24,
            cols: 80,
            created_at: created_at.to_owned(),
        },
    )
    .await
    .expect("terminal session creates")
}
