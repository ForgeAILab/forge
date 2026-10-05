use super::*;
use crate::worker_runtime::queue::TaskStepWorker;
use db::TaskStepRepo;
use std::time::Duration;

async fn queued_ci_for_task(
    fixture: &FailedCiFixture,
    selected: &db::Task,
    gate: &std::path::Path,
) -> (String, PathBuf, PathBuf) {
    let started = gate.join("started");
    let release = gate.join("release");
    let quote = |p: &std::path::Path| format!("\"{}\"", p.to_str().unwrap());
    let script = format!(
        "touch {}; while [ ! -f {} ] && [ -d {} ]; do sleep 0.01; done",
        quote(&started),
        quote(&release),
        quote(gate)
    );
    sqlx::query("UPDATE task SET task_state_config=? WHERE id=?")
        .bind(json!({"retry_budgets":{"review":3},"review":{"ci_steps":[script]}}).to_string())
        .bind(&selected.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let task = TaskRepo::get_by_id(&*fixture.db, &selected.id, false)
        .await
        .unwrap()
        .unwrap();
    let input = fixture
        .engine
        .workflow_execution()
        .cascade_step_input(
            &task,
            &fixture.workflow,
            "review".into(),
            "queued CI".into(),
            Default::default(),
            false,
            false,
            Some(fixture.workflow_authority().await),
            None,
            format!("audit:ci:{}", selected.id),
            None,
        )
        .await
        .unwrap();
    fixture.db.enqueue_step(&input).await.unwrap();
    fixture.db.ready_step(&input.id).await.unwrap();
    (input.id, started, release)
}
#[tokio::test]
async fn audit_23a_renewal_and_claim_errors_do_not_cancel_started_ci_and_shutdown_drains() {
    let fixture = failed_ci_fixture(3, FailurePolicy::Block).await;
    let gate = TempDir::new().unwrap();
    let (id, started, release) = queued_ci_for_task(&fixture, &fixture.task, gate.path()).await;
    let mut worker = TaskStepWorker::new(fixture.engine.clone());
    worker.renew_interval = Duration::from_millis(10);
    let (stop, rx) = tokio::sync::watch::channel(false);
    let handle = Arc::new(worker).start(rx);
    let entered = tokio::time::timeout(Duration::from_secs(10), async {
        while !started.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        entered.is_ok(),
        "steps: {:?} task:{:?}",
        fixture.db.task_steps(&fixture.task.id).await.unwrap(),
        TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
            .await
            .unwrap()
    );
    sqlx::query("CREATE TRIGGER reject_renew BEFORE UPDATE OF lease_until ON task_step WHEN NEW.lease_until IS NOT NULL BEGIN SELECT RAISE(ABORT,'injected renewal failure'); END").execute(fixture.db.pool()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!handle.is_finished());
    assert!(fixture.db.task_step_is_running(&fixture.task.id));
    sqlx::query("DROP TRIGGER reject_renew")
        .execute(fixture.db.pool())
        .await
        .unwrap();
    sqlx::query("ALTER TABLE task_step RENAME TO unavailable_step")
        .execute(fixture.db.pool())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert!(!handle.is_finished());
    assert!(fixture.db.task_step_is_running(&fixture.task.id));
    sqlx::query("ALTER TABLE unavailable_step RENAME TO task_step")
        .execute(fixture.db.pool())
        .await
        .unwrap();
    stop.send(true).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!handle.is_finished());
    std::fs::write(release, "release").unwrap();
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .unwrap()
        .unwrap();
    let task = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(task.entry_barrier_json.is_none());
    let row = fixture
        .db
        .task_steps(&task.id)
        .await
        .unwrap()
        .into_iter()
        .find(|s| s.id == id)
        .unwrap();
    assert_eq!(row.status, "done");
    assert!(row.last_error.is_none());
}
#[tokio::test]
async fn audit_23a_real_target_moved_rebases_twice_without_ci_do_not_park() {
    let fixture = failed_ci_fixture(3, FailurePolicy::Block).await;
    sqlx::query("UPDATE task SET status='in_progress',task_state_config=NULL WHERE id=?")
        .bind(&fixture.task.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let task = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    fixture
        .engine
        .workflow_execution()
        .transition(
            &task.id,
            "review",
            task.version,
            &fixture.workflow,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "completed",
            false,
            Default::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        drain(fixture.engine.clone(), &task.id).await.status,
        "merging"
    );
    for round in 0..2 {
        std::fs::write(
            fixture
                ._repo_dir
                .path()
                .join(format!("sibling-{round}.txt")),
            "sibling",
        )
        .unwrap();
        for args in [vec!["add", "."], vec!["commit", "-m", "sibling"]] {
            assert!(std::process::Command::new("git")
                .args(args)
                .current_dir(fixture._repo_dir.path())
                .output()
                .unwrap()
                .status
                .success());
        }
        let task = TaskRepo::get_by_id(&*fixture.db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        let project = ProjectRepo::get_by_id(&*fixture.db, &task.project_id)
            .await
            .unwrap()
            .unwrap();
        let ctx = crate::workflow::HookContext {
            task_id: task.id.clone(),
            project_id: task.project_id.clone(),
            from_state: "review".into(),
            to_state: "merging".into(),
            db: fixture.db.clone(),
            event_bus: fixture.engine.event_bus.clone(),
            gate_config: None,
            workflow: Arc::new(fixture.workflow.clone()),
            project_version: Some(project.version),
            project_workflow_definition: Some(project.workflow_definition),
            triggered_by: api_types::Actor::system(api_types::SystemComponent::Workflow),
            review_runner: None,
            merge_service: None,
            cleanup_scheduler: None,
            task_service: fixture.engine.clone(),
            daemon_connections: None,
            workspace_exec_locks: None,
            terminal_activity: None,
            workspace_root: fixture.engine.workspace_root.clone(),
            repo_cache_locks: None,
            workspace_backend_router: fixture.engine.workspace_backend_router.clone(),
            workspace_id: Some(fixture.workspace.id.clone()),
            agent_id: None,
            execution_id: None,
            state_config: json!({}),
        };
        let crate::workflow::HookResult::Cascade { to, reason, bridge } =
            crate::workflow::actions::target_moved_result(&ctx, &task, "target advanced", "main")
                .await
        else {
            panic!("real rebase must cascade")
        };
        let parent = fixture
            .db
            .task_steps(&task.id)
            .await
            .unwrap()
            .last()
            .cloned()
            .unwrap();
        let input = fixture
            .engine
            .workflow_execution()
            .cascade_step_input(
                &task,
                &fixture.workflow,
                to,
                reason,
                bridge,
                false,
                false,
                None,
                Some(&parent),
                format!("audit:real:{round}"),
                None,
            )
            .await
            .unwrap();
        fixture.db.enqueue_step(&input).await.unwrap();
        fixture.db.ready_step(&input.id).await.unwrap();
        let settled = drain(fixture.engine.clone(), &task.id).await;
        assert_eq!(settled.status, "merging");
        assert!(settled.blocked_json.is_none());
        assert!(fixture
            .db
            .task_steps(&task.id)
            .await
            .unwrap()
            .iter()
            .all(|s| s.status == "done"));
        assert!(db::ReviewRepo::list_by_task(&*fixture.db, &task.id)
            .await
            .unwrap()
            .is_empty());
    }
}

#[tokio::test]
async fn audit_23a_before_exit_cascade_does_not_suppress_target_hooks() {
    let db = Arc::new(sqlite_db().await);
    let bus = Arc::new(EventBus::new(32));
    let id = "before-exit-cascade";
    seed_project_repo_and_task(&db, id, "start").await;
    let start = with_trigger(
        state(
            "start",
            StateKind::Initial,
            None,
            StateHooks {
                before_exit: vec![hook("auto_cascade_on_completion", FailurePolicy::Log)],
                ..StateHooks::default()
            },
        ),
        WorkflowTrigger::Accept,
        "target",
    );
    let target = state(
        "target",
        StateKind::Terminal,
        None,
        StateHooks {
            on_enter: vec![hook("undefined_action", FailurePolicy::Log)],
            ..StateHooks::default()
        },
    );
    let workflow = WorkflowDefinition {
        states: vec![start, target],
        roles: vec![],
        configuration: vec![],
        cancellation_state: None,
    };
    let result = engine(db.clone(), bus)
        .workflow_execution()
        .transition(
            id,
            "target",
            1,
            &workflow,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "advance",
            false,
            Default::default(),
        )
        .await;
    assert!(result.is_ok(), "the CAS returns before target hooks");
    let settled = drain(engine(db.clone(), Arc::new(EventBus::new(32))), id).await;
    assert_eq!(settled.status, "target");
    let rows: Vec<_> = db
        .task_steps(id)
        .await
        .unwrap()
        .into_iter()
        .filter(|step| step.kind == "hooks")
        .collect();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].status, "failed",
        "undefined target hook ran despite before_exit Cascade"
    );
}

/// Four CI steps on one repository fill the long lane: the first holds the
/// checkout lock inside its blocked command, the rest wait on that lock.
async fn hold_long_lane(
    fixture: &FailedCiFixture,
) -> (
    Vec<TempDir>,
    Vec<db::Task>,
    tokio::sync::watch::Sender<bool>,
    tokio::task::JoinHandle<()>,
) {
    let gates: Vec<_> = (0..4).map(|_| TempDir::new().unwrap()).collect();
    let mut tasks = vec![fixture.task.clone()];
    for n in 1..4 {
        let id = new_uuid_v4();
        let now = now_rfc3339();
        sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES (?,?,?,'merge_failed',?,?)").bind(&id).bind(&fixture.task.project_id).bind(format!("long {n}")).bind(&now).bind(&now).execute(fixture.db.pool()).await.unwrap();
        let task = TaskRepo::get_by_id(&*fixture.db, &id, false)
            .await
            .unwrap()
            .unwrap();
        let workspace = crate::task_service::workspace::prepare_workspace(
            &fixture.db,
            fixture._workspace_root.path(),
            &task,
            &task.id,
            None,
            &crate::lifecycle::context::embedded_workspace_router_for_test(
                fixture.db.clone(),
                fixture._workspace_root.path().to_path_buf(),
                None,
            ),
        )
        .await
        .unwrap();
        let sha = git::get_current_sha(std::path::Path::new(
            &workspace.embedded_worktree_path_for_backend(),
        ))
        .await
        .unwrap();
        sqlx::query("INSERT INTO execution(id,task_id,role,status,workspace_id,before_sha,after_sha,created_at,updated_at) VALUES (?,?,'coder','completed',?,?,?,?,?)")
            .bind(new_uuid_v4()).bind(&task.id).bind(&workspace.id).bind(&sha).bind(&sha).bind(&now).bind(&now).execute(fixture.db.pool()).await.unwrap();
        tasks.push(task);
    }
    for (task, gate) in tasks.iter().zip(&gates) {
        queued_ci_for_task(fixture, task, gate.path()).await;
    }
    for task in &tasks {
        assert_eq!(
            fixture.db.task_steps(&task.id).await.unwrap()[0].lane,
            "long"
        );
    }
    let (stop, rx) = tokio::sync::watch::channel(false);
    let handle = Arc::new(TaskStepWorker::new(fixture.engine.clone())).start(rx);
    tokio::time::timeout(Duration::from_secs(5), async {
        while fixture.db.active_step_count() < 4 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !gates
            .iter()
            .any(|gate| gate.path().join("started").exists())
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    (gates, tasks, stop, handle)
}

async fn release_long_lane(
    fixture: &FailedCiFixture,
    gates: Vec<TempDir>,
    tasks: Vec<db::Task>,
    stop: tokio::sync::watch::Sender<bool>,
    handle: tokio::task::JoinHandle<()>,
) {
    for task in &tasks {
        assert!(fixture.db.task_step_is_running(&task.id));
    }
    stop.send(true).unwrap();
    for gate in &gates {
        std::fs::write(gate.path().join("release"), "release").unwrap();
    }
    tokio::time::timeout(Duration::from_secs(7), handle)
        .await
        .unwrap()
        .unwrap();
}

async fn wait_for_status(db: &SqliteDb, id: &str, status: &str) {
    let reached = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if TaskRepo::get_by_id(db, id, false)
                .await
                .unwrap()
                .unwrap()
                .status
                == status
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        reached.is_ok(),
        "never reached {status}: {:?}",
        db.task_steps(id).await.unwrap()
    );
}

#[tokio::test]
async fn audit_23a_fast_lane_progresses_while_four_real_ci_hooks_hold_long_lane() {
    let fixture = failed_ci_fixture(3, FailurePolicy::Block).await;
    let (gates, tasks, stop, handle) = hold_long_lane(&fixture).await;
    let id = new_uuid_v4();
    let now = now_rfc3339();
    sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES (?,?,'fast','start',?,?)").bind(&id).bind(&fixture.task.project_id).bind(&now).bind(&now).execute(fixture.db.pool()).await.unwrap();
    let start = with_trigger(
        state("start", StateKind::Initial, None, StateHooks::default()),
        WorkflowTrigger::Accept,
        "finished",
    );
    let workflow = WorkflowDefinition {
        states: vec![
            start,
            state("finished", StateKind::Terminal, None, StateHooks::default()),
        ],
        roles: vec![],
        configuration: vec![],
        cancellation_state: None,
    };
    let task = TaskRepo::get_by_id(&*fixture.db, &id, false)
        .await
        .unwrap()
        .unwrap();
    let input = fixture
        .engine
        .workflow_execution()
        .cascade_step_input(
            &task,
            &workflow,
            "finished".into(),
            "fast cascade".into(),
            Default::default(),
            false,
            false,
            None,
            None,
            "audit:fast".into(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(input.lane, "fast");
    fixture.db.enqueue_step(&input).await.unwrap();
    fixture.db.ready_step(&input.id).await.unwrap();
    wait_for_status(&fixture.db, &id, "finished").await;
    release_long_lane(&fixture, gates, tasks, stop, handle).await;
}

/// R4: with the long lane full on one repository's lock, default-workflow
/// cascades in another Project still run. planning -> in_progress (provision,
/// dispatch) and review -> in_progress (reject) were long before; only merge
/// and CI review checks are long now.
#[tokio::test]
async fn reaudit_23a_default_cascades_in_other_projects_run_while_long_lane_is_full() {
    let fixture = failed_ci_fixture(3, FailurePolicy::Block).await;
    let (gates, tasks, stop, handle) = hold_long_lane(&fixture).await;
    let workflow = crate::workflow::default_workflow::default_workflow();
    assert_eq!(
        crate::worker_runtime::queue::cascade_lane(&workflow, "merging"),
        "long"
    );
    for (from, to) in [("planning", "in_progress"), ("review", "in_progress")] {
        let id = new_uuid_v4();
        seed_project_repo_and_task(&fixture.db, &id, from).await;
        let other = TaskRepo::get_by_id(&*fixture.db, &id, false)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(other.project_id, fixture.task.project_id);
        let input = fixture
            .engine
            .workflow_execution()
            .cascade_step_input(
                &other,
                &workflow,
                to.into(),
                "gate skipped: other Project".into(),
                Default::default(),
                from == "review",
                false,
                None,
                None,
                format!("reaudit:other:{from}"),
                None,
            )
            .await
            .unwrap();
        assert_eq!(input.lane, "fast", "{from} -> {to}");
        fixture.db.enqueue_step(&input).await.unwrap();
        fixture.db.ready_step(&input.id).await.unwrap();
        wait_for_status(&fixture.db, &id, to).await;
    }
    release_long_lane(&fixture, gates, tasks, stop, handle).await;
}
