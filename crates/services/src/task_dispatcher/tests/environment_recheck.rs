//! Tests for environment recheck in the task dispatcher.

use super::*;
#[tokio::test]
async fn environment_probe_three_admissions_defer_once_then_launch() {
    use db::ProjectMachineReadinessRepo;
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let root = TempDir::new().unwrap();
    let signals = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo.path()).await;
    let environment = serde_json::json!({"env":{"COUNT":signals.path().join("starts"),"RELEASE":signals.path().join("release")},
        "checks":[{"name":"toolchain","command":"printf 'start\\n' >> \"$COUNT\"; while ! test -f \"$RELEASE\"; do sleep 0.05; done", "timeout_seconds":10}]});
    sqlx::query("UPDATE project SET settings = ? WHERE id = ?")
        .bind(serde_json::json!({"environment":environment}).to_string())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .unwrap();
    let agent = seed_agent(&db, 3, DaemonStatus::Online, AgentStatus::Idle).await;
    let mut tasks = Vec::new();
    for index in 0..3 {
        let task = seed_task(&db, &project_id, &format!("Task {index}"), "todo", index).await;
        assign_role(&db, &task.id, "coder", &agent).await;
        tasks.push(task);
    }
    let (dispatcher, mut launches) = build_dispatcher(db.clone(), root.path()).await;
    let claimant = AgentRepo::get_by_id(&*db, &agent).await.unwrap().unwrap();
    let admissions = tokio::join!(
        dispatcher
            .task_service
            .defer_initial_environment_probe(&tasks[0], &claimant, "coder"),
        dispatcher
            .task_service
            .defer_initial_environment_probe(&tasks[1], &claimant, "coder"),
        dispatcher
            .task_service
            .defer_initial_environment_probe(&tasks[2], &claimant, "coder"),
    );
    for result in [admissions.0, admissions.1, admissions.2] {
        assert!(result.unwrap());
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        while !signals.path().join("starts").exists() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let initial_versions = task_versions(&db, &tasks).await;
    assert_eq!(dispatcher.check_once_and_drain().await.unwrap(), 0);
    let before: Vec<_> = task_versions(&db, &tasks).await;
    assert_eq!(dispatcher.check_once_and_drain().await.unwrap(), 0);
    assert_eq!(
        task_versions(&db, &tasks).await,
        before,
        "repeated deferral does not change versions"
    );
    for (task, initial_version) in tasks.iter().zip(initial_versions) {
        let current = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.status, "todo");
        assert!(deferred_dispatch::pending_until(&current)
            .unwrap()
            .reason
            .starts_with("environment_probe_pending:"));
        assert!(current.error_annotation.is_none());
        assert!(ExecutionRepo::list_running_by_task(&*db, &task.id)
            .await
            .unwrap()
            .is_empty());
        assert!(
            current.version <= initial_version + 1,
            "first deferral spends at most one version: {} -> {}",
            initial_version,
            current.version
        );
    }
    assert_eq!(
        std::fs::read_to_string(signals.path().join("starts"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    std::fs::write(signals.path().join("release"), "go").unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if db
                .get_readiness(&project_id, &db::EnvironmentMachine::Server)
                .await
                .unwrap()
                .is_some_and(|row| row.status == db::EnvironmentReadinessStatus::Ready)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        task_versions(&db, &tasks).await,
        before,
        "probe completion wakes without another version change"
    );
    assert_eq!(dispatcher.check_once_and_drain().await.unwrap(), 3);
    for _ in 0..3 {
        tokio::time::timeout(Duration::from_secs(5), launches.recv())
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn environment_settings_edit_starts_probe_without_a_task() {
    use db::ProjectMachineReadinessRepo;
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let root = TempDir::new().unwrap();
    let signals = TempDir::new().unwrap();
    let (project_id, repo_id) = seed_project_repo(&db, repo.path()).await;
    let environment: api_types::ProjectEnvironment =
        serde_json::from_value(serde_json::json!({"checks":[{"name":"old","command":"true"}]}))
            .unwrap();
    sqlx::query("UPDATE project SET settings = ? WHERE id = ?")
        .bind(serde_json::json!({"environment":environment}).to_string())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .unwrap();
    let now = now_rfc3339();
    db::RepoLocationRepo::create(
        &*db,
        db::CreateRepoLocation {
            id: new_uuid_v4(),
            repo_id,
            owner_kind: db::RepoLocationOwnerKind::Server,
            daemon_id: None,
            runtime_id: None,
            path: repo.path().to_string_lossy().into_owned(),
            kind: db::RepoLocationKind::PrimaryCheckout,
            is_default: true,
            status: db::RepoLocationStatus::Ready,
            last_verified_at: Some(now.clone()),
            last_error: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
    let mut row = crate::placement::environment::unknown_record(
        &project_id,
        db::EnvironmentMachine::Server,
        &environment,
    );
    row.status = db::EnvironmentReadinessStatus::NotReady;
    row.next_check_at = Some("2099-01-01T00:00:00Z".into());
    row.failing_checks = vec![db::ReadinessCheckFailure {
        name: "old".into(),
        output_tail: "old failure".into(),
    }];
    db.put_readiness(row, None).await.unwrap();
    let snapshot = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    ProjectRepo::set_environment_pause_if_unchanged(&*db,&project_id,snapshot.version,&now_rfc3339(),
        &serde_json::json!({"checks":["old"],"role":"coder","output":"old failure","paused_at":now_rfc3339(),"last_checked_at":now_rfc3339(),"next_check_at":"2099-01-01T00:00:00Z"}).to_string()).await.unwrap();
    let (dispatcher, _) = build_dispatcher(db.clone(), root.path()).await;
    dispatcher.check_once_and_drain().await.unwrap();
    let current = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    let new_environment = serde_json::json!({"env":{"COUNT":signals.path().join("starts"),"RELEASE":signals.path().join("release")}, "checks":[{"name":"old","command":"true"},{"name":"new","command":"echo started > \"$COUNT\"; while ! test -f \"$RELEASE\"; do sleep 0.05; done", "timeout_seconds":10}]});
    ProjectRepo::update_at_version(
        &*db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: Some(serde_json::json!({"environment":new_environment}).to_string()),
            primary_repo_id: None,
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        current.version,
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        db.get_readiness(&project_id, &db::EnvironmentMachine::Server)
            .await
            .unwrap()
            .unwrap()
            .status,
        db::EnvironmentReadinessStatus::Unknown
    );
    // Settings edits also probe an existing host row without a location row.
    sqlx::query(
        "DELETE FROM repo_location WHERE repo_id IN (SELECT id FROM repo WHERE project_id = ?)",
    )
    .bind(&project_id)
    .execute(db.pool())
    .await
    .unwrap();
    dispatcher.event_bus.publish(events::ForgeEvent {
        event_type: "project.updated".into(),
        entity_id: project_id.clone(),
        timestamp: events::event_timestamp(),
        context: events::EventContext::ProjectUpdated {},
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !signals.path().join("starts").exists() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let renamed = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    ProjectRepo::update_at_version(
        &*db,
        UpdateProject {
            id: project_id.clone(),
            name: Some("name changed while probing".into()),
            settings: None,
            primary_repo_id: None,
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        renamed.version,
        None,
    )
    .await
    .unwrap();
    std::fs::write(signals.path().join("release"), "go").unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if db
                .get_readiness(&project_id, &db::EnvironmentMachine::Server)
                .await
                .unwrap()
                .is_some_and(|row| row.status == db::EnvironmentReadinessStatus::Ready)
                && ProjectRepo::get_by_id(&*db, &project_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .paused_at
                    .is_none()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        ProjectRepo::get_by_id(&*db, &project_id)
            .await
            .unwrap()
            .unwrap()
            .paused_at
            .is_none(),
        "a passing settings probe resumes the matching pause without a Task"
    );
}

#[tokio::test]
async fn environment_recheck_is_single_flight_off_tick_and_stale_result_preserves_user_pause() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let other_repo = TempDir::new().unwrap();
    let workspace = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    seed_environment_pause(&db, &project_id, serde_json::json!({"checks":[{
        "name":"disk", "command":"echo started >> starts; while ! test -f release-check; do sleep 0.05; done", "timeout_seconds":30
    }]}), &["disk"], "2026-01-01T00:00:00Z").await;
    let (other, _) = seed_project_repo(&db, other_repo.path()).await;
    let agent = seed_agent(&db, 2, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &other, "unrelated admission", "todo", 0).await;
    assign_role(&db, &task.id, "coder", &agent).await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace.path()).await;
    // Off the tick: the pass admits the unrelated Task and returns while the
    // check it started is still blocked. A pass that waited for the check
    // would return only after the check's timeout, with the job finished.
    assert_eq!(dispatcher.check_once_and_drain().await.unwrap(), 1);
    assert!(
        environment_recheck_runs(&dispatcher, &project_id),
        "the pass returned before its environment check ended"
    );
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();
    while !repo_dir.path().join("starts").exists() {
        assert!(
            environment_recheck_runs(&dispatcher, &project_id),
            "the environment check ended without starting its command"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // Single flight: a second pass starts no second check and does not wait
    // for the first.
    assert_eq!(dispatcher.check_once_and_drain().await.unwrap(), 0);
    assert!(
        environment_recheck_runs(&dispatcher, &project_id),
        "the second pass returned before the environment check ended"
    );
    assert!(matches!(
        dispatcher
            .task_service
            .recheck_project_environment(&project_id)
            .await,
        Err(crate::ServiceError::Conflict(_))
    ));
    assert_eq!(
        std::fs::read_to_string(repo_dir.path().join("starts"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    ProjectRepo::set_paused_at(&*db, &project_id, Some(now_rfc3339()))
        .await
        .unwrap();
    let user_pause = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    std::fs::write(repo_dir.path().join("release-check"), "yes").unwrap();
    finish_environment_recheck(&dispatcher, &project_id).await;
    let current = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.version, user_pause.version);
    assert_eq!(current.paused_at, user_pause.paused_at);
    assert!(current.system_pause_reason.is_none());
}

#[tokio::test]
async fn environment_recheck_without_rerunnable_checks_stays_paused() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    seed_environment_pause(
        &db,
        &project_id,
        serde_json::json!({}),
        &[],
        "2026-01-01T00:00:00Z",
    )
    .await;
    let (dispatcher, _) = build_dispatcher(Arc::clone(&db), workspace.path()).await;
    dispatcher.check_once_and_drain().await.unwrap();
    let current = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    assert!(current.paused_at.is_some());
    assert!(crate::project_environment::pause_detail(&current)
        .unwrap()
        .unwrap()
        .output
        .contains("No re-runnable"));
    dispatcher.check_once_and_drain().await.unwrap();
    assert_eq!(
        ProjectRepo::get_by_id(&*db, &project_id)
            .await
            .unwrap()
            .unwrap()
            .version,
        current.version
    );
    let (results, current) = dispatcher
        .task_service
        .recheck_project_environment(&project_id)
        .await
        .unwrap();
    assert!(results.is_empty());
    assert!(current.paused_at.is_some());
    ProjectRepo::update_at_version(
        &*db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: Some(
                r#"{"environment":{"checks":[{"name":"fixed","command":"true"}]}}"#.into(),
            ),
            primary_repo_id: None,
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        current.version,
        None,
    )
    .await
    .unwrap();
    // The manual empty check rescheduled the detail; force it due again.
    sqlx::query("UPDATE project SET environment_pause_json = json_set(environment_pause_json, '$.next_check_at', '2026-01-01T00:00:00Z') WHERE id = ?")
        .bind(&project_id).execute(db.pool()).await.unwrap();
    // A configured check that passes says nothing about the failure that
    // caused this pause, so the dispatcher must not resume on it.
    dispatcher.check_once_and_drain().await.unwrap();
    assert!(dispatcher
        .environment_rechecks
        .lock()
        .unwrap()
        .get(&project_id)
        .is_none());
    assert!(ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap()
        .paused_at
        .is_some());
    // The owner's own "Check now" is an explicit decision and does resume.
    let (results, current) = dispatcher
        .task_service
        .recheck_project_environment(&project_id)
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert!(current.paused_at.is_none());
}

#[tokio::test]
async fn environment_and_project_limit_errors_do_not_abort_later_projects() {
    let db = Arc::new(sqlite_db().await);
    let workspace = TempDir::new().unwrap();
    let agent = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let mut repos = Vec::new();
    for (index, corruption) in [
        "pause",
        "environment-settings",
        "slot-settings",
        "readiness",
    ]
    .into_iter()
    .enumerate()
    {
        let repo = TempDir::new().unwrap();
        let (project_id, _) = seed_project_repo(&db, repo.path()).await;
        if corruption != "slot-settings" {
            seed_environment_pause(
                &db,
                &project_id,
                serde_json::json!({"checks":[{"name":"disk","command":if corruption=="pause" {"false"} else {"true"}}]}),
                &["disk"],
                "2026-01-01T00:00:00Z",
            )
            .await;
        } else {
            let task = seed_task(&db, &project_id, "waiting for admission", "todo", 0).await;
            assign_role(&db, &task.id, "coder", &agent).await;
        }
        if corruption == "pause" {
            sqlx::query("UPDATE project SET environment_pause_json = 'invalid' WHERE id = ?")
                .bind(&project_id)
                .execute(db.pool())
                .await
                .unwrap();
        } else if corruption == "readiness" {
            sqlx::query("UPDATE project_machine_readiness SET failing_checks_json='[{\"name\":null}]' WHERE project_id=?").bind(&project_id).execute(db.pool()).await.unwrap();
        } else {
            sqlx::query("UPDATE project SET settings = 'invalid' WHERE id = ?")
                .bind(&project_id)
                .execute(db.pool())
                .await
                .unwrap();
        }
        sqlx::query("UPDATE project SET created_at = ? WHERE id = ?")
            .bind(format!("2000-01-0{}T00:00:00Z", index + 1))
            .bind(&project_id)
            .execute(db.pool())
            .await
            .unwrap();
        repos.push(repo);
    }
    let repo = TempDir::new().unwrap();
    let (healthy, _) = seed_project_repo(&db, repo.path()).await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace.path()).await;
    assert_eq!(dispatcher.check_once_and_drain().await.unwrap(), 0);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if dispatcher
                .environment_rechecks
                .lock()
                .unwrap()
                .values()
                .all(|job| job.is_finished())
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let task = seed_task(
        &db,
        &healthy,
        "healthy admission after failed job",
        "todo",
        0,
    )
    .await;
    assign_role(&db, &task.id, "coder", &agent).await;
    assert_eq!(dispatcher.check_once_and_drain().await.unwrap(), 1);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap()
            .task_id,
        task.id
    );
    assert_eq!(
        dispatcher.check_once_and_drain().await.unwrap(),
        0,
        "subsequent ticks also isolate bad rows"
    );
    let overdue_bad:i64=sqlx::query_scalar("SELECT count(*) FROM project_machine_readiness WHERE failing_checks_json='[{\"name\":null}]' AND julianday(next_check_at)<=julianday(?)").bind(now_rfc3339()).fetch_one(db.pool()).await.unwrap();
    assert_eq!(
        overdue_bad, 0,
        "the undecodable row is rescheduled instead of retried each tick"
    );
}

#[tokio::test]
async fn environment_recheck_resumes_and_redispatches_current_task_states() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_dir = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 2, DaemonStatus::Online, AgentStatus::Idle).await;
    let active = seed_task(
        &db,
        &project_id,
        "active before environment failure",
        "in_progress",
        1,
    )
    .await;
    let initial = seed_task(&db, &project_id, "waiting for environment", "todo", 0).await;
    for task in [&active, &initial] {
        assign_role(&db, &task.id, "coder", &agent_id).await;
        let failed = seed_cancelled_execution(
            &db,
            &task.id,
            &agent_id,
            "coder",
            Some(StopReason::ExecutorFailed),
            Some(ResumePolicy::Manual),
        )
        .await;
        sqlx::query("UPDATE execution SET status = 'failed', error = 'environment not ready: disk exited 1' WHERE id = ?")
            .bind(failed.id).execute(db.pool()).await.unwrap();
        assert!(
            !helpers::latest_stopped_execution_blocks_dispatch(&db, &task.id, "coder")
                .await
                .unwrap()
        );
        assert!(
            !helpers::latest_execution_awaits_completion_cascade(&db, &task.id, "coder")
                .await
                .unwrap()
        );
    }
    let paused = seed_environment_pause(
        &db,
        &project_id,
        serde_json::json!({
            "env": {"READY": "yes"},
            "checks": [
                {"name": "disk", "command": "test \"$READY\" = yes", "roles": ["reviewer"]},
                {"name": "unrelated", "command": "touch unrelated-check; exit 1"}
            ]
        }),
        &["disk"],
        "2026-01-01T00:00:00Z",
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    let mut events = dispatcher.event_bus.subscribe();
    assert_eq!(
        dispatcher.check_once_and_drain().await.unwrap(),
        0,
        "resume skips the stale snapshot"
    );
    finish_environment_recheck(&dispatcher, &project_id).await;
    let resumed = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    assert!(resumed.paused_at.is_none());
    assert!(resumed.system_pause_reason.is_none());
    assert!(resumed.environment_pause_json.is_none());
    assert!(resumed.version > paused.version);
    assert!(
        !repo_dir.path().join("unrelated-check").exists(),
        "only recorded checks run"
    );
    let event = events.try_recv().unwrap();
    assert_eq!(event.event_type, "project.resumed");
    assert_eq!(event.entity_id, project_id);
    assert_eq!(
        TaskRepo::get_by_id(&*db, &active.id, false)
            .await
            .unwrap()
            .unwrap()
            .status,
        "in_progress"
    );
    assert_eq!(
        TaskRepo::get_by_id(&*db, &initial.id, false)
            .await
            .unwrap()
            .unwrap()
            .status,
        "todo"
    );
    // Remove the intentionally failing unrelated probe before the two launches,
    // which apply all checks for their own role at the normal launch boundary.
    sqlx::query("UPDATE project SET settings = json_set(settings, '$.environment.checks', json('[]')), version = version + 1 WHERE id = ?")
        .bind(&project_id)
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(dispatcher.check_once_and_drain().await.unwrap(), 2);
    let first = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();
    let ids = std::collections::HashSet::from([first.task_id, second.task_id]);
    assert_eq!(
        ids,
        std::collections::HashSet::from([active.id.clone(), initial.id.clone()])
    );
    for task in [&active, &initial] {
        let current = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.status, "in_progress");
        assert!(current.error_annotation.is_none());
        assert!(current.blocked_json.is_none());
    }
}

#[tokio::test]
async fn environment_recheck_still_failing_reschedules_without_dispatch() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_dir = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 2, DaemonStatus::Online, AgentStatus::Idle).await;
    let waiting = seed_task(&db, &project_id, "waiting while paused", "todo", 0).await;
    let running = seed_task(&db, &project_id, "already running", "in_progress", 1).await;
    for task in [&waiting, &running] {
        assign_role(&db, &task.id, "coder", &agent_id).await;
    }
    seed_running_execution(&db, &running.id, &agent_id, "coder").await;
    let paused = seed_environment_pause(&db, &project_id, serde_json::json!({
        "recheck_interval_seconds": 60,
        "checks": [{"name": "disk", "command": "printf 'root free: 7G'; exit 1", "roles": ["reviewer"]}]
    }), &["disk"], "2026-01-01T00:00:00Z").await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    let mut events = dispatcher.event_bus.subscribe();
    let before = chrono::Utc::now();
    assert_eq!(dispatcher.check_once_and_drain().await.unwrap(), 0);
    finish_environment_recheck(&dispatcher, &project_id).await;
    let current = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.paused_at, paused.paused_at);
    assert_eq!(
        current.system_pause_reason.as_deref(),
        Some("environment_not_ready")
    );
    let detail = crate::project_environment::pause_detail(&current)
        .unwrap()
        .unwrap();
    assert_eq!(detail.checks, vec!["disk"]);
    assert!(detail.output.contains("root free: 7G"));
    let last = chrono::DateTime::parse_from_rfc3339(&detail.last_checked_at).unwrap();
    let next = chrono::DateTime::parse_from_rfc3339(&detail.next_check_at).unwrap();
    assert!(last >= before);
    assert_eq!((next - last).num_seconds(), 60);
    assert_eq!(
        ExecutionRepo::list_running_by_task(&*db, &running.id)
            .await
            .unwrap()
            .len(),
        1,
        "existing executions keep running"
    );
    let waiting_now = TaskRepo::get_by_id(&*db, &waiting.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(waiting_now.status, "todo");
    assert!(waiting_now.error_annotation.is_none());
    assert!(rx.try_recv().is_err());
    assert_eq!(events.try_recv().unwrap().event_type, "project.updated");
    assert_eq!(dispatcher.check_once_and_drain().await.unwrap(), 0);
    assert_eq!(
        ProjectRepo::get_by_id(&*db, &project_id)
            .await
            .unwrap()
            .unwrap()
            .version,
        current.version,
        "not due again yet"
    );
}

#[tokio::test]
async fn environment_recheck_preserves_user_pause_and_stale_resume_loses() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_dir = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let paused = seed_environment_pause(
        &db,
        &project_id,
        serde_json::json!({
            "checks": [{"name": "disk", "command": "touch checked"}]
        }),
        &["disk"],
        "2026-01-01T00:00:00Z",
    )
    .await;
    let (dispatcher, _) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    ProjectRepo::set_paused_at(&*db, &project_id, Some(now_rfc3339()))
        .await
        .unwrap();
    let user_paused = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    assert!(user_paused.environment_pause_json.is_none());
    assert!(user_paused.system_pause_reason.is_none());
    assert!(!dispatcher
        .task_service
        .clear_environment_pause(&paused)
        .await
        .unwrap());
    assert_eq!(dispatcher.check_once_and_drain().await.unwrap(), 0);
    assert!(!repo_dir.path().join("checked").exists());
    let (checks, current) = dispatcher
        .task_service
        .recheck_project_environment(&project_id)
        .await
        .unwrap();
    assert!(checks[0].passed);
    assert_eq!(current.paused_at, user_paused.paused_at);
    assert_eq!(current.version, user_paused.version);
    assert!(!ProjectRepo::set_environment_pause_if_unchanged(
        &*db,
        &project_id,
        current.version,
        &now_rfc3339(),
        "{}"
    )
    .await
    .unwrap());
}

#[tokio::test]
async fn environment_recheck_missing_checkout_reschedules_without_immediate_respawn() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let workspace = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo.path()).await;
    let paused = seed_environment_pause(
        &db,
        &project_id,
        serde_json::json!({"checks":[{"name":"disk","command":"true"}], "recheck_interval_seconds": 60}),
        &["disk"],
        "2026-01-01T00:00:00Z",
    )
    .await;
    // Repository sync leaves an environment-owned pause alone, but the
    // re-check cannot resolve a usable primary checkout.
    std::fs::remove_dir_all(repo.path().join(".git")).unwrap();
    let (dispatcher, _) = build_dispatcher(Arc::clone(&db), workspace.path()).await;
    let started_at = chrono::Utc::now();
    assert_eq!(dispatcher.check_once_and_drain().await.unwrap(), 0);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if dispatcher.environment_rechecks.lock().unwrap()[&project_id].is_finished() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let current = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    let detail = crate::project_environment::pause_detail(&current)
        .unwrap()
        .unwrap();
    assert_eq!(current.paused_at, paused.paused_at);
    assert_eq!(detail.checks, vec!["disk"]);
    assert!(detail.output.contains("primary checkout is unavailable"));
    assert!(chrono::DateTime::parse_from_rfc3339(&detail.last_checked_at).unwrap() >= started_at);
    assert!(
        chrono::DateTime::parse_from_rfc3339(&detail.next_check_at).unwrap()
            >= started_at + chrono::Duration::seconds(60)
    );
    // Observe the finished job, then scan once more against the new due time.
    assert_eq!(dispatcher.check_once_and_drain().await.unwrap(), 0);
    assert_eq!(dispatcher.check_once_and_drain().await.unwrap(), 0);
    assert!(!dispatcher
        .environment_rechecks
        .lock()
        .unwrap()
        .contains_key(&project_id));
    assert_eq!(
        ProjectRepo::get_by_id(&*db, &project_id)
            .await
            .unwrap()
            .unwrap()
            .version,
        current.version
    );
}
