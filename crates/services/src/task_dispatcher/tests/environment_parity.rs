//! Asserted reproductions of second-round audit s5, s6c, s10, s13, s15 and s17.
use super::environment_placement::{configure, drive};
use super::*;
use db::ProjectMachineReadinessRepo;

async fn wait_paused(dispatcher: &TaskDispatcher, project: &str) {
    drive(dispatcher, || async {
        ProjectRepo::get_by_id(&*dispatcher.db, project)
            .await
            .unwrap()
            .unwrap()
            .paused_at
            .is_some()
    })
    .await;
}
async fn wait_running(dispatcher: &TaskDispatcher, task: &Task) {
    drive(dispatcher, || async {
        !ExecutionRepo::list_running_by_task(&*dispatcher.db, &task.id)
            .await
            .unwrap()
            .is_empty()
    })
    .await;
}

#[tokio::test]
async fn environment_asset_check_launches_after_staging_without_probe() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let root = TempDir::new().unwrap();
    let assets = TempDir::new().unwrap();
    std::fs::write(assets.path().join("weights.bin"), "weights").unwrap();
    let (project, _) = seed_project_repo(&db, repo.path()).await;
    let agent = seed_agent(&db, 2, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project, "asset-backed check", "todo", 0).await;
    assign_role(&db, &task.id, "coder", &agent).await;
    configure(&db,&project,serde_json::json!({"assets":[{"source":assets.path(),"target":"vendor/model"}],"checks":[{"name":"model","command":"test -f vendor/model/weights.bin"}]})).await;
    let (dispatcher, mut launches) = build_dispatcher(db.clone(), root.path()).await;
    assert_eq!(dispatcher.check_once_and_drain().await.unwrap(), 1);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), launches.recv())
            .await
            .unwrap()
            .unwrap()
            .task_id,
        task.id
    );
    assert!(ProjectRepo::get_by_id(&*db, &project)
        .await
        .unwrap()
        .unwrap()
        .paused_at
        .is_none());
    assert!(
        db.list_readiness(&project).await.unwrap().is_empty(),
        "an asset-backed probe must not manufacture a failure"
    );
}

#[tokio::test]
async fn environment_unnamed_failure_stays_paused_for_three_intervals() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let root = TempDir::new().unwrap();
    let assets = TempDir::new().unwrap();
    let (project, _) = seed_project_repo(&db, repo.path()).await;
    let agent = seed_agent(&db, 2, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project, "missing asset", "todo", 0).await;
    assign_role(&db, &task.id, "coder", &agent).await;
    configure(&db,&project,serde_json::json!({"assets":[{"source":assets.path().join("missing"),"target":"vendor"}],"checks":[{"name":"tool","command":"true"}],"recheck_interval_seconds":60})).await;
    let (dispatcher, mut launches) = build_dispatcher(db.clone(), root.path()).await;
    wait_paused(&dispatcher, &project).await;
    for _ in 0..3 {
        sqlx::query("UPDATE project_machine_readiness SET next_check_at='2000-01-01T00:00:00Z' WHERE project_id=?").bind(&project).execute(db.pool()).await.unwrap();
        assert_eq!(dispatcher.check_once().await.unwrap(), 0);
        assert!(dispatcher.environment_rechecks.lock().unwrap().is_empty());
        assert!(ProjectRepo::get_by_id(&*db, &project)
            .await
            .unwrap()
            .unwrap()
            .paused_at
            .is_some());
    }
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM execution WHERE task_id=?")
        .bind(&task.id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(count, 1);
    assert!(launches.try_recv().is_err());
    assert!(TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap()
        .error_annotation
        .is_none());
}

#[tokio::test]
async fn environment_reviewer_failure_does_not_gate_coder_with_separate_reviewer() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let root = TempDir::new().unwrap();
    let (project, _) = seed_project_repo(&db, repo.path()).await;
    let coder = seed_agent(&db, 2, DaemonStatus::Online, AgentStatus::Idle).await;
    let reviewer = seed_agent(&db, 2, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project, "coder needs tool", "todo", 0).await;
    assign_role(&db, &task.id, "reviewer", &reviewer).await;
    assign_role(&db, &task.id, "coder", &coder).await;
    configure(&db,&project,serde_json::json!({"checks":[{"name":"browser","command":"false","roles":["reviewer"]},{"name":"tool","command":"true","roles":["coder"]}]})).await;
    let (dispatcher, mut launches) = build_dispatcher(db.clone(), root.path()).await;
    wait_running(&dispatcher, &task).await;
    let launch = tokio::time::timeout(Duration::from_secs(5), launches.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        ExecutionRepo::get_by_id(&*db, &launch.execution_id)
            .await
            .unwrap()
            .unwrap()
            .role,
        "coder"
    );
    assert!(ProjectRepo::get_by_id(&*db, &project)
        .await
        .unwrap()
        .unwrap()
        .paused_at
        .is_none());
    let reviewer = AgentRepo::get_by_id(&*db, &reviewer)
        .await
        .unwrap()
        .unwrap();
    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(
        dispatcher
            .task_service
            .defer_initial_environment_probe(&task, &reviewer, "reviewer")
            .await
            .unwrap(),
        "the reviewer is gated on its own launch"
    );
}

#[tokio::test]
async fn environment_due_recheck_clears_pause_created_later_in_same_tick() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let root = TempDir::new().unwrap();
    let (project, _) = seed_project_repo(&db, repo.path()).await;
    let agent = seed_agent(&db, 2, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project, "same tick", "todo", 0).await;
    assign_role(&db, &task.id, "coder", &agent).await;
    let gate = TempDir::new().unwrap();
    let release = gate.path().join("release");
    let quote = |p: &std::path::Path| format!("'{}'", p.to_string_lossy().replace('\'', "'\"'\"'"));
    let command = format!(
        "while [ ! -f {} ] && [ -d {} ]; do sleep 0.01; done; true",
        quote(&release),
        quote(gate.path())
    );
    let environment:api_types::ProjectEnvironment=serde_json::from_value(serde_json::json!({"checks":[{"name":"disk","command":command}],"recheck_interval_seconds":86400})).unwrap();
    configure(&db, &project, serde_json::to_value(&environment).unwrap()).await;
    let mut row = crate::placement::environment::unknown_record(
        &project,
        db::EnvironmentMachine::Server,
        &environment,
    );
    row.status = db::EnvironmentReadinessStatus::NotReady;
    row.failing_checks = vec![db::ReadinessCheckFailure {
        name: "disk".into(),
        output_tail: "old failure".into(),
    }];
    row.next_check_at = Some("2000-01-01T00:00:00Z".into());
    db.put_readiness(row, None).await.unwrap();
    let (dispatcher, mut launches) = build_dispatcher(db.clone(), root.path()).await;
    assert_eq!(dispatcher.check_once().await.unwrap(), 0);
    assert!(ProjectRepo::get_by_id(&*db, &project)
        .await
        .unwrap()
        .unwrap()
        .paused_at
        .is_some());
    std::fs::write(&release, "continue").unwrap();
    wait_running(&dispatcher, &task).await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), launches.recv())
            .await
            .unwrap()
            .unwrap()
            .task_id,
        task.id
    );
    assert!(ProjectRepo::get_by_id(&*db, &project)
        .await
        .unwrap()
        .unwrap()
        .paused_at
        .is_none());
}

#[tokio::test]
async fn environment_direct_claim_succeeds_first_attempt_without_probe() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let root = TempDir::new().unwrap();
    let (project, _) = seed_project_repo(&db, repo.path()).await;
    let agent = seed_agent(&db, 2, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project, "direct claim", "todo", 0).await;
    assign_role(&db, &task.id, "coder", &agent).await;
    configure(
        &db,
        &project,
        serde_json::json!({"checks":[{"name":"tool","command":"true"}]}),
    )
    .await;
    let (dispatcher, _) = build_dispatcher(db.clone(), root.path()).await;
    let claimed = dispatcher
        .task_service
        .claim_task(task.id.clone(), crate::Assignee::Agent(agent), None)
        .await
        .unwrap();
    assert_eq!(claimed.task.status, "in_progress");
    assert!(db.list_readiness(&project).await.unwrap().is_empty());
}

#[tokio::test]
async fn environment_pause_resolves_an_earlier_capacity_wait_attention() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let root = TempDir::new().unwrap();
    let (project, _) = seed_project_repo(&db, repo.path()).await;
    let agent = seed_agent(&db, 2, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project, "capacity then pause", "todo", 0).await;
    assign_role(&db, &task.id, "coder", &agent).await;
    let environment: api_types::ProjectEnvironment =
        serde_json::from_value(serde_json::json!({"checks":[{"name":"disk","command":"false"}]}))
            .unwrap();
    configure(&db, &project, serde_json::to_value(&environment).unwrap()).await;
    let mut row = crate::placement::environment::unknown_record(
        &project,
        db::EnvironmentMachine::Server,
        &environment,
    );
    row.status = db::EnvironmentReadinessStatus::NotReady;
    row.failing_checks = vec![db::ReadinessCheckFailure {
        name: "disk".into(),
        output_tail: "low disk".into(),
    }];
    row.next_check_at = Some("2099-01-01T00:00:00Z".into());
    db.put_readiness(row, None).await.unwrap();
    let claimant = AgentRepo::get_by_id(&*db, &agent).await.unwrap().unwrap();
    for _ in 0..claimant.max_concurrent_tasks {
        let busy = seed_task(&db, &project, "busy", "in_progress", 0).await;
        seed_running_execution(&db, &busy.id, &agent, "coder").await;
    }
    let (dispatcher, _) = build_dispatcher(db.clone(), root.path()).await;
    assert!(dispatcher
        .task_service
        .defer_initial_environment_probe(&task, &claimant, "coder")
        .await
        .unwrap());
    let open: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM attention_projection WHERE dedupe_key=? AND status='open'",
    )
    .bind(format!("task-environment-wait:{}", task.id))
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(open, 1);
    sqlx::query("UPDATE execution SET status='completed' WHERE agent_id=?")
        .bind(&agent)
        .execute(db.pool())
        .await
        .unwrap();
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(dispatcher
        .task_service
        .defer_initial_environment_probe(&current, &claimant, "coder")
        .await
        .unwrap());
    assert!(ProjectRepo::get_by_id(&*db, &project)
        .await
        .unwrap()
        .unwrap()
        .paused_at
        .is_some());
    let open: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM attention_projection WHERE dedupe_key=? AND status <> 'resolved'",
    )
    .bind(format!("task-environment-wait:{}", task.id))
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(open, 0);
    assert!(!db::TaskMetadata::parse(
        TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap()
            .metadata_json
            .as_deref()
    )
    .unwrap()
    .extra
    .contains_key("environment_wait"));
}
