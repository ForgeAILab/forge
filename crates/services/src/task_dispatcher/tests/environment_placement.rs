//! Regressions ported from the independent base/HEAD audit. No audit-dir inputs.
use super::*;
use db::ProjectMachineReadinessRepo;

pub(super) async fn configure(db: &db::SqliteDb, id: &str, environment: serde_json::Value) {
    let project = ProjectRepo::get_by_id(db, id).await.unwrap().unwrap();
    let mut settings: serde_json::Value = serde_json::from_str(&project.settings).unwrap();
    settings["environment"] = environment;
    ProjectRepo::update_at_version(
        db,
        UpdateProject {
            id: id.into(),
            name: None,
            settings: Some(settings.to_string()),
            primary_repo_id: None,
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        project.version,
        None,
    )
    .await
    .unwrap();
}
pub(super) async fn drive<F, Fut>(dispatcher: &TaskDispatcher, mut done: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            dispatcher.check_once().await.unwrap();
            if done().await {
                break;
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    })
    .await
    .expect("automatic dispatcher makes progress");
}
async fn assert_clean_wait(db: &db::SqliteDb, task: &Task, status: &str) {
    let current = TaskRepo::get_by_id(db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.status, status);
    assert!(current.error_annotation.is_none());
    assert!(current.blocked_json.is_none());
    let attention: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM attention_projection WHERE dedupe_key = ? AND status <> 'resolved'",
    )
    .bind(format!("task-environment-wait:{}", task.id))
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(
        attention, 0,
        "single-machine Project pause is the sole signal"
    );
}

#[tokio::test]
async fn single_machine_probe_failure_pauses_then_recovers_without_annotation() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let root = TempDir::new().unwrap();
    let signals = TempDir::new().unwrap();
    let (project, _) = seed_project_repo(&db, repo.path()).await;
    let agent = seed_agent(&db, 2, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project, "first dispatch", "todo", 0).await;
    assign_role(&db, &task.id, "coder", &agent).await;
    let flag = signals.path().join("fixed");
    configure(&db,&project,serde_json::json!({"env":{"FIXED":flag},"recheck_interval_seconds":60,"checks":[{"name":"disk","command":"test -f \"$FIXED\" || { echo 'root free: 7G'; exit 1; }"}]})).await;
    let (dispatcher, mut launches) = build_dispatcher(db.clone(), root.path()).await;
    drive(&dispatcher, || async {
        ProjectRepo::get_by_id(&*db, &project)
            .await
            .unwrap()
            .unwrap()
            .paused_at
            .is_some()
    })
    .await;
    assert_clean_wait(&db, &task, "todo").await;
    assert!(ExecutionRepo::list_running_by_task(&*db, &task.id)
        .await
        .unwrap()
        .is_empty());
    assert!(launches.try_recv().is_err());
    std::fs::write(flag, "fixed").unwrap();
    sqlx::query("UPDATE project_machine_readiness SET next_check_at = '2000-01-01T00:00:00Z' WHERE project_id = ?").bind(&project).execute(db.pool()).await.unwrap();
    drive(&dispatcher, || async {
        !ExecutionRepo::list_running_by_task(&*db, &task.id)
            .await
            .unwrap()
            .is_empty()
    })
    .await;
    assert!(ProjectRepo::get_by_id(&*db, &project)
        .await
        .unwrap()
        .unwrap()
        .paused_at
        .is_none());
    tokio::time::timeout(Duration::from_secs(5), launches.recv())
        .await
        .unwrap()
        .unwrap();
    assert_clean_wait(&db, &task, "in_progress").await;
}

async fn launch_failure_resume(fix: bool) {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let root = TempDir::new().unwrap();
    let signals = TempDir::new().unwrap();
    let (project, _) = seed_project_repo(&db, repo.path()).await;
    let agent = seed_agent(&db, 2, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project, "launch failure", "todo", 0).await;
    assign_role(&db, &task.id, "coder", &agent).await;
    let flag = signals.path().join("fixed");
    std::fs::write(&flag, "yes").unwrap();
    configure(&db,&project,serde_json::json!({"env":{"FIXED":flag},"recheck_interval_seconds":86400,"checks":[{"name":"disk","command":"test -f \"$FIXED\" || { echo broken; exit 1; }"}]})).await;
    let (dispatcher, mut launches) = build_dispatcher(db.clone(), root.path()).await;
    dispatcher.check_once().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if db
                .get_readiness(&project, &db::EnvironmentMachine::Server)
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
    std::fs::remove_file(&flag).unwrap();
    drive(&dispatcher, || async {
        ProjectRepo::get_by_id(&*db, &project)
            .await
            .unwrap()
            .unwrap()
            .paused_at
            .is_some()
    })
    .await;
    assert_clean_wait(&db, &task, "in_progress").await;
    assert!(launches.try_recv().is_err());
    if fix {
        std::fs::write(&flag, "yes").unwrap();
    }
    ProjectRepo::set_paused_at(&*db, &project, None)
        .await
        .unwrap();
    assert_eq!(
        db.get_readiness(&project, &db::EnvironmentMachine::Server)
            .await
            .unwrap()
            .unwrap()
            .status,
        db::EnvironmentReadinessStatus::Unknown
    );
    if fix {
        drive(&dispatcher, || async {
            !ExecutionRepo::list_running_by_task(&*db, &task.id)
                .await
                .unwrap()
                .is_empty()
        })
        .await;
        tokio::time::timeout(Duration::from_secs(5), launches.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(ProjectRepo::get_by_id(&*db, &project)
            .await
            .unwrap()
            .unwrap()
            .paused_at
            .is_none());
    } else {
        drive(&dispatcher, || async {
            ProjectRepo::get_by_id(&*db, &project)
                .await
                .unwrap()
                .unwrap()
                .paused_at
                .is_some()
        })
        .await;
        assert!(launches.try_recv().is_err());
    }
    assert_clean_wait(&db, &task, "in_progress").await;
}
#[tokio::test]
async fn single_machine_manual_resume_fixed_launches_without_old_due_time() {
    launch_failure_resume(true).await;
}
#[tokio::test]
async fn single_machine_manual_resume_unfixed_pauses_again() {
    launch_failure_resume(false).await;
}

#[tokio::test]
async fn single_machine_unnamed_asset_failure_retries_after_resume() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let root = TempDir::new().unwrap();
    let assets = TempDir::new().unwrap();
    let (project, _) = seed_project_repo(&db, repo.path()).await;
    let agent = seed_agent(&db, 2, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project, "asset failure", "todo", 0).await;
    assign_role(&db, &task.id, "coder", &agent).await;
    let source = assets.path().join("missing");
    configure(&db,&project,serde_json::json!({"assets":[{"source":source,"target":"vendor"}],"checks":[{"name":"tool","command":"true"}],"recheck_interval_seconds":86400})).await;
    let (dispatcher, mut launches) = build_dispatcher(db.clone(), root.path()).await;
    drive(&dispatcher, || async {
        ProjectRepo::get_by_id(&*db, &project)
            .await
            .unwrap()
            .unwrap()
            .paused_at
            .is_some()
    })
    .await;
    assert_clean_wait(&db, &task, "in_progress").await;
    assert!(db
        .get_readiness(&project, &db::EnvironmentMachine::Server)
        .await
        .unwrap()
        .unwrap()
        .failing_checks
        .is_empty());
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("art"), "art").unwrap();
    ProjectRepo::set_paused_at(&*db, &project, None)
        .await
        .unwrap();
    drive(&dispatcher, || async {
        !ExecutionRepo::list_running_by_task(&*db, &task.id)
            .await
            .unwrap()
            .is_empty()
    })
    .await;
    tokio::time::timeout(Duration::from_secs(5), launches.recv())
        .await
        .unwrap()
        .unwrap();
    assert_clean_wait(&db, &task, "in_progress").await;
}
#[tokio::test]
async fn single_machine_reviewer_check_does_not_stop_coder() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let root = TempDir::new().unwrap();
    let (project, _) = seed_project_repo(&db, repo.path()).await;
    let agent = seed_agent(&db, 2, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project, "coder", "todo", 0).await;
    assign_role(&db, &task.id, "coder", &agent).await;
    configure(&db,&project,serde_json::json!({"checks":[{"name":"browser","command":"echo missing; exit 4","roles":["reviewer"]},{"name":"tool","command":"true","roles":["coder"]}]})).await;
    let (dispatcher, mut launches) = build_dispatcher(db.clone(), root.path()).await;
    drive(&dispatcher, || async {
        !ExecutionRepo::list_running_by_task(&*db, &task.id)
            .await
            .unwrap()
            .is_empty()
    })
    .await;
    assert!(ProjectRepo::get_by_id(&*db, &project)
        .await
        .unwrap()
        .unwrap()
        .paused_at
        .is_none());
    tokio::time::timeout(Duration::from_secs(5), launches.recv())
        .await
        .unwrap()
        .unwrap();
    let row = db
        .get_readiness(&project, &db::EnvironmentMachine::Server)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.status, db::EnvironmentReadinessStatus::NotReady);
    assert_eq!(row.check_results.len(), 2);
}

#[tokio::test]
async fn probe_completion_kicks_dispatch_before_long_interval() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let root = TempDir::new().unwrap();
    let (project, _) = seed_project_repo(&db, repo.path()).await;
    let agent = seed_agent(&db, 2, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project, "kick", "todo", 0).await;
    assign_role(&db, &task.id, "coder", &agent).await;
    configure(
        &db,
        &project,
        serde_json::json!({"checks":[{"name":"tool","command":"sleep 0.05; true"}]}),
    )
    .await;
    let (built, mut launches) = build_dispatcher(db.clone(), root.path()).await;
    let dispatcher = Arc::new(TaskDispatcher::with_check_interval(
        db,
        built.event_bus.clone(),
        built.task_service.clone(),
        Duration::from_secs(300),
    ));
    let job = dispatcher.clone().start();
    tokio::time::timeout(Duration::from_secs(5), launches.recv())
        .await
        .expect("probe completion wakes the sleeping dispatcher")
        .unwrap();
    dispatcher.stop();
    job.await.unwrap();
}

#[tokio::test]
async fn environment_five_parked_waiters_do_not_block_sixth_on_healthy_server() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let root = TempDir::new().unwrap();
    let (project, _) = seed_project_repo(&db, repo.path()).await;
    let agent = seed_agent(&db, 2, DaemonStatus::Online, AgentStatus::Idle).await;
    let current = ProjectRepo::get_by_id(&*db, &project)
        .await
        .unwrap()
        .unwrap();
    let mut settings: serde_json::Value = serde_json::from_str(&current.settings).unwrap();
    settings["max_active_tasks"] = serde_json::json!(5);
    sqlx::query("UPDATE project SET settings=? WHERE id=?")
        .bind(settings.to_string())
        .bind(&project)
        .execute(db.pool())
        .await
        .unwrap();
    configure(
        &db,
        &project,
        serde_json::json!({"checks":[{"name":"tool","command":"true"}]}),
    )
    .await;
    for index in 0..5 {
        let waiter = seed_task(&db, &project, &format!("parked {index}"), "in_progress", 0).await;
        sqlx::query("UPDATE task SET metadata_json=? WHERE id=?")
            .bind(serde_json::json!({"environment_wait":{"machine":{"owner_kind":"daemon","daemon_id":"failed","runtime_id":"one"},"checks":["tool"]},"deferred_dispatch":{"kind":"environment_not_ready","reason":"failed owner","target_state":"in_progress","not_before":"2099-01-01T00:00:00Z"}}).to_string())
            .bind(&waiter.id).execute(db.pool()).await.unwrap();
    }
    let sixth = seed_task(&db, &project, "sixth healthy", "todo", 0).await;
    assign_role(&db, &sixth.id, "coder", &agent).await;
    let (dispatcher, mut launches) = build_dispatcher(db.clone(), root.path()).await;
    drive(&dispatcher, || async {
        !ExecutionRepo::list_running_by_task(&*db, &sixth.id)
            .await
            .unwrap()
            .is_empty()
    })
    .await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), launches.recv())
            .await
            .unwrap()
            .unwrap()
            .task_id,
        sixth.id
    );
    assert!(ProjectRepo::get_by_id(&*db, &project)
        .await
        .unwrap()
        .unwrap()
        .paused_at
        .is_none());
}

#[tokio::test]
async fn environment_recheck_name_edit_cas_loss_retries_the_same_pause() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let root = TempDir::new().unwrap();
    let signals = TempDir::new().unwrap();
    let (project, _) = seed_project_repo(&db, repo.path()).await;
    seed_environment_pause(&db,&project,serde_json::json!({"env":{"START":signals.path().join("start"),"RELEASE":signals.path().join("release")},"checks":[{"name":"disk","command":"echo started > \"$START\"; while ! test -f \"$RELEASE\"; do sleep 0.05; done","timeout_seconds":10}]}),&["disk"],"2000-01-01T00:00:00Z").await;
    let (dispatcher, _) = build_dispatcher(db.clone(), root.path()).await;
    dispatcher.check_once().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !signals.path().join("start").exists() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let snapshot = ProjectRepo::get_by_id(&*db, &project)
        .await
        .unwrap()
        .unwrap();
    ProjectRepo::update_at_version(
        &*db,
        UpdateProject {
            id: project.clone(),
            name: Some("renamed while checking".into()),
            settings: None,
            primary_repo_id: None,
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        snapshot.version,
        None,
    )
    .await
    .unwrap();
    std::fs::write(signals.path().join("release"), "go").unwrap();
    drive(&dispatcher, || async {
        ProjectRepo::get_by_id(&*db, &project)
            .await
            .unwrap()
            .unwrap()
            .paused_at
            .is_none()
    })
    .await;
    assert_eq!(
        db.get_readiness(&project, &db::EnvironmentMachine::Server)
            .await
            .unwrap()
            .unwrap()
            .status,
        db::EnvironmentReadinessStatus::Ready
    );
}

#[tokio::test]
async fn machine_capacity_wait_with_ready_environment_keeps_todo_and_no_environment_wait() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let root = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo.path()).await;
    let agent = seed_agent(&db, 8, DaemonStatus::Online, AgentStatus::Idle).await;
    let busy = seed_task(&db, &project_id, "busy", "in_progress", 0).await;
    seed_running_execution(&db, &busy.id, &agent, "coder").await;
    let task = seed_task(&db, &project_id, "capacity waiter", "todo", 0).await;
    assign_role(&db, &task.id, "coder", &agent).await;
    configure(
        &db,
        &project_id,
        serde_json::json!({"checks":[{"name":"disk","command":"true"}]}),
    )
    .await;
    let project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    let settings: api_types::ProjectSettings = serde_json::from_str(&project.settings).unwrap();
    let mut ready = crate::placement::environment::unknown_record(
        &project_id,
        db::EnvironmentMachine::Server,
        &settings.environment,
    );
    ready.status = db::EnvironmentReadinessStatus::Ready;
    ready.checked_at = Some(now_rfc3339());
    ready.next_check_at = Some("2099-01-01T00:00:00Z".into());
    db.put_readiness(ready, None).await.unwrap();
    sqlx::query("UPDATE task SET metadata_json = json_object('environment_wait',json_object('machine',json_object('owner_kind','server')), 'deferred_dispatch',json_object('kind','environment_not_ready','not_before','2099-01-01T00:00:00Z','target_state','todo','reason','old environment failure')) WHERE id = ?").bind(&task.id).execute(db.pool()).await.unwrap();
    db.server_run_cap
        .set(Some(1), 1, &config::embedded_machine_id());
    let (dispatcher, mut launches) = build_dispatcher(db.clone(), root.path()).await;
    dispatcher.check_once().await.unwrap();
    let current = machine_capacity_task(&db, &task.id).await;
    assert_eq!(current.status, "todo");
    assert_eq!(current.version, task.version);
    assert_eq!(
        deferred_dispatch::current_dispatch_disposition(&current)
            .unwrap()
            .capability,
        "machine_capacity"
    );
    let metadata = db::TaskMetadata::parse(current.metadata_json.as_deref()).unwrap();
    assert!(!metadata.extra.contains_key("environment_wait"));
    assert!(!metadata.extra.contains_key("deferred_dispatch"));
    assert!(ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap()
        .paused_at
        .is_none());
    assert!(launches.try_recv().is_err());
}

#[tokio::test]
async fn environment_probe_pending_on_full_machine_does_not_leave_todo() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let root = TempDir::new().unwrap();
    let (project, _) = seed_project_repo(&db, repo.path()).await;
    let agent = seed_agent(&db, 8, DaemonStatus::Online, AgentStatus::Idle).await;
    let busy = seed_task(&db, &project, "busy", "in_progress", 0).await;
    seed_running_execution(&db, &busy.id, &agent, "coder").await;
    let task = seed_task(&db, &project, "probe waiter", "todo", 0).await;
    assign_role(&db, &task.id, "coder", &agent).await;
    configure(
        &db,
        &project,
        serde_json::json!({"checks":[{"name":"slow","command":"sleep 1; true"}]}),
    )
    .await;
    db.server_run_cap
        .set(Some(1), 1, &config::embedded_machine_id());
    let (dispatcher, mut launches) = build_dispatcher(db.clone(), root.path()).await;
    dispatcher.check_once().await.unwrap();
    let current = machine_capacity_task(&db, &task.id).await;
    assert_eq!(current.status, "todo");
    assert_eq!(current.version, task.version);
    assert!(current.error_annotation.is_none());
    assert!(deferred_dispatch::current_dispatch_disposition(&current).is_none());
    assert!(ProjectRepo::get_by_id(&*db, &project)
        .await
        .unwrap()
        .unwrap()
        .paused_at
        .is_none());
    assert!(launches.try_recv().is_err());
}
