mod common;

use api_types::*;
use axum::http::{Method, StatusCode};
use common::{fake_daemon::*, json_request, TestDir};
use db::{ProjectMachineReadinessRepo, ProjectRepo, RepoRepo, TaskRepo, WorkspacePlacementRepo};
use forge_client::daemon_runtime::{ActiveExecutionTracker, DaemonRuntime};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_tungstenite::tungstenite::Message;

struct Owner {
    requests: Arc<Mutex<Vec<String>>>,
    jobs: Vec<JoinHandle<()>>,
    drop_provision_reply: Arc<std::sync::atomic::AtomicBool>,
}
impl Drop for Owner {
    fn drop(&mut self) {
        for job in &self.jobs {
            job.abort();
        }
    }
}
impl Owner {
    async fn connect(
        server: &TestServer,
        registration: &DaemonRegisterResponse,
        runtime: Arc<DaemonRuntime>,
    ) -> Self {
        let socket = connect_daemon(
            server,
            &registration.daemon_id,
            Some(&registration.registration_token),
        )
        .await
        .unwrap();
        let (mut writer, mut reader) = socket.split();
        let (send, mut receive) = mpsc::unbounded_channel();
        runtime.attach(send.clone());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = requests.clone();
        let drop_provision_reply = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let dropped = drop_provision_reply.clone();
        let write = tokio::spawn(async move {
            while let Some(frame) = receive.recv().await {
                if writer
                    .send(Message::Text(serde_json::to_string(&frame).unwrap().into()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        let read = tokio::spawn(async move {
            while let Some(Ok(Message::Text(text))) = reader.next().await {
                if let Ok(frame @ DaemonFrame::Request { .. }) =
                    serde_json::from_str::<DaemonFrame>(&text)
                {
                    if let DaemonFrame::Request { method, .. } = &frame {
                        seen.lock().unwrap().push(method.clone());
                    }
                    let provision = matches!(&frame, DaemonFrame::Request { method, .. } if method == METHOD_REPO_LOCATION_PROVISION);
                    let method = if let DaemonFrame::Request { method, .. } = &frame {
                        method.clone()
                    } else {
                        unreachable!()
                    };
                    let reply = runtime.handle_request(frame).await;
                    seen.lock().unwrap().push(format!("completed: {method}"));
                    if !(provision && dropped.load(std::sync::atomic::Ordering::SeqCst)) {
                        let _ = send.send(reply);
                    }
                }
            }
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if server
                    .state
                    .daemon_connections
                    .connection_snapshots()
                    .get(&registration.daemon_id)
                    .is_some_and(|facts| {
                        facts
                            .handshake
                            .capabilities
                            .contains(&DAEMON_CAPABILITY_MACHINE_PROBE.into())
                    })
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        Self {
            requests,
            jobs: vec![write, read],
            drop_provision_reply,
        }
    }
}

struct Harness {
    app: axum::Router,
    state: Arc<api::AppState>,
}

async fn state_for_db(db: Arc<db::SqliteDb>, root: &std::path::Path) -> Arc<api::AppState> {
    let adapters = Arc::new(cli_adapters::test_support::test_registry());
    let events = Arc::new(events::EventBus::new(64));
    let merge = Arc::new(services::MergeService::new_for_test(
        db.clone(),
        events.clone(),
        root.to_owned(),
    ));
    let cleanup = Arc::new(services::WorkspaceCleanupScheduler::new(
        db.clone(),
        events.clone(),
        root.to_owned(),
    ));
    let review = Arc::new(review::ReviewRunner::new(
        db.clone(),
        events.clone(),
        adapters.clone(),
    ));
    Arc::new(api::AppState::with_adapter_registry_services_and_shutdown(
        db,
        events,
        true,
        adapters,
        merge,
        cleanup,
        review,
        api::state::ShutdownSignal::new(),
        api::state::test_workflows_dir(),
        api::state::test_jwt_secret(),
        api::state::test_bcrypt_cost(),
    ))
}

async fn harness(root: &std::path::Path) -> Harness {
    let pool = db::create_sqlite_pool(&format!("sqlite:{}", root.join("forge.sqlite").display()))
        .await
        .unwrap();
    db::run_migrations(&pool).await.unwrap();
    let db = Arc::new(db::SqliteDb::new(pool));
    common::fake_daemon::seed_test_user(&db).await;
    services::ensure_default_agents(&db, &cli_adapters::test_support::test_registry())
        .await
        .unwrap();
    let state = state_for_db(db, &root.join("workspaces")).await;
    let web = root.join("web");
    std::fs::create_dir_all(&web).unwrap();
    std::fs::write(web.join("index.html"), "<html></html>").unwrap();
    let app = api::build_router((*state).clone(), web);
    Harness { app, state }
}

struct Fixture {
    harness: Harness,
    service: services::TaskService,
    server: TestServer,
    owner: Option<Owner>,
    runtime: Arc<DaemonRuntime>,
    registration: DaemonRegisterResponse,
    project: String,
    repo: String,
    task: String,
    agent: String,
    root: TestDir,
    _server_root: TestDir,
}
impl Fixture {
    async fn new(checks: Value, role: &str) -> Self {
        let server_root = TestDir::new("environment-provision-server");
        let root = TestDir::new("environment-provision-daemon");
        let source = common::setup_git_repo(server_root.path());
        let harness = harness(server_root.path()).await;
        let registration =
            register_daemon(&harness.app, &db::new_uuid_v4(), "environment-provision").await;
        report_remote_daemon_shell(
            &harness.app,
            &registration.daemon_id,
            &registration.registration_token,
            root.path(),
            "environment-provision",
        )
        .await;
        let server = TestServer::start(harness.state.clone()).await;
        let (outbound, _) = mpsc::unbounded_channel();
        let runtime = DaemonRuntime::new_owned(
            outbound,
            root.path().to_owned(),
            ActiveExecutionTracker::default(),
            registration.daemon_id.clone(),
            WorkspaceRunPolicy {
                allowed_purposes: vec![
                    WorkspaceRunPurpose::EnvironmentProbe,
                    WorkspaceRunPurpose::RepoProvision,
                    WorkspaceRunPurpose::EnvironmentSetup,
                    WorkspaceRunPurpose::CiStep,
                ],
            },
        )
        .unwrap();
        let owner = Owner::connect(&server, &registration, runtime.clone()).await;
        let agent: AgentResponse = json_request(
            &harness.app,
            Method::POST,
            "/api/v1/agents",
            json!({"name":"daemon executor","executor_type":"shell","config":{"command": if role == "planner" { r#"printf '%s\n' '- [ ] Provisioned owner plan' > "$FORGE_PLAN_PATH""# } else { r#"printf '%s\n' '- [x] Implement the task' > "$FORGE_PLAN_PATH""# }}}),
            StatusCode::OK,
        )
        .await;
        let now = db::now_rfc3339();
        let mut workflow = services::workflow::default_workflow::default_workflow();
        workflow
            .states
            .iter_mut()
            .find(|state| state.name == "in_progress")
            .unwrap()
            .role = Some(role.into());
        let project = ProjectRepo::create(
            &*harness.state.db,
            db::CreateProject {
                id: db::new_uuid_v4(),
                name: "Probe before code".into(),
                settings: json!({"environment":{"checks":checks}}).to_string(),
                workflow_definition: serde_json::to_string(&workflow).unwrap(),
                primary_repo_id: None,
                owner_id: Some("test-user-id".into()),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .unwrap();
        let repo = RepoRepo::create(
            &*harness.state.db,
            db::CreateRepo {
                id: db::new_uuid_v4(),
                project_id: project.id.clone(),
                name: "repo".into(),
                remote_url: Some(source.to_string_lossy().into_owned()),
                local_path: Some(source.to_string_lossy().into_owned()),
                default_branch: "main".into(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .unwrap();
        db::RepoLocationRepo::create(
            &*harness.state.db,
            db::CreateRepoLocation {
                id: db::new_uuid_v4(),
                repo_id: repo.id.clone(),
                owner_kind: db::RepoLocationOwnerKind::Server,
                daemon_id: None,
                runtime_id: None,
                path: source.to_string_lossy().into_owned(),
                kind: db::RepoLocationKind::PrimaryCheckout,
                is_default: true,
                status: db::RepoLocationStatus::Ready,
                last_verified_at: Some(now.clone()),
                last_error: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .unwrap();
        ProjectRepo::update_at_version(
            &*harness.state.db,
            db::UpdateProject {
                id: project.id.clone(),
                name: None,
                settings: None,
                primary_repo_id: Some(Some(repo.id.clone())),
                paused_at: None,
                updated_at: now.clone(),
            },
            project.version,
            None,
        )
        .await
        .unwrap();
        let task = TaskRepo::create(
            &*harness.state.db,
            db::CreateTask {
                id: db::new_uuid_v4(),
                project_id: project.id.clone(),
                parent_task_id: None,
                subtask_order: None,
                assignee_type: None,
                assignee_id: None,
                title: "Probe before clone".into(),
                description: None,
                task_type: "task".into(),
                status: "todo".into(),
                is_automation: false,
                priority: 0,
                task_state_config: None,
                merge_config: None,
                plan: Some("- [ ] Implement the task\n".into()),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .unwrap();
        // The host deliberately has no executor. The daemon advertises Shell,
        // workspace.v1, and plan transport through its real runtime handshake.
        let service = services::TaskService::new_for_test(
            harness.state.db.clone(),
            harness.state.event_bus.clone(),
        )
        .with_workspace_root(server_root.path().join("workspaces"))
        .with_workspace_backend_router(harness.state.workspace_backend_router.clone())
        .with_daemon_connections(harness.state.daemon_connections.clone())
        .with_placement_adapter_registry(Arc::new(executors::AdapterRegistry::new()));
        Self {
            harness,
            service,
            server,
            owner: Some(owner),
            runtime,
            registration,
            project: project.id,
            repo: repo.id,
            task: task.id,
            agent: agent.id,
            root,
            _server_root: server_root,
        }
    }
    async fn claim(&self) -> services::Result<db::ClaimedTask> {
        self.service
            .claim_task(
                &self.task,
                services::Assignee::Agent(self.agent.clone()),
                None,
            )
            .await
    }
    async fn claim_until_ready(&self) -> db::ClaimedTask {
        let mut last = String::new();
        let result = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                match self.claim().await {
                    Ok(claimed) => break claimed,
                    Err(services::ServiceError::PlacementUnavailable(refusal)) => {
                        last = format!("{refusal:?}");
                        tokio::time::sleep(Duration::from_millis(25)).await;
                    }
                    Err(error) => panic!("claim failed: {error:?}"),
                }
            }
        })
        .await;
        match result {
            Ok(claimed) => claimed,
            Err(_) => {
                let locations: Vec<(String, String, Option<String>)> = sqlx::query_as(
                    "SELECT kind, status, last_error FROM repo_location WHERE repo_id = ?",
                )
                .bind(&self.repo)
                .fetch_all(self.harness.state.db.pool())
                .await
                .unwrap();
                let retry: Option<(i64, String, Option<String>)> = sqlx::query_as("SELECT attempts, next_attempt_at, last_error FROM repo_provision_retry WHERE repo_id = ?").bind(&self.repo).fetch_optional(self.harness.state.db.pool()).await.unwrap();
                let readiness = self
                    .harness
                    .state
                    .db
                    .list_readiness(&self.project)
                    .await
                    .unwrap();
                let requests = self
                    .owner
                    .as_ref()
                    .unwrap()
                    .requests
                    .lock()
                    .unwrap()
                    .clone();
                panic!("placement did not wake: {last}; locations={locations:?}; retry={retry:?}; readiness={readiness:?}; requests={requests:?}");
            }
        }
    }
    async fn wait_for_fact(&self) {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if self
                    .harness
                    .state
                    .db
                    .list_readiness(&self.project)
                    .await
                    .unwrap()
                    .iter()
                    .any(|row| {
                        row.machine != db::EnvironmentMachine::Server && row.checked_at.is_some()
                    })
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn two_machine_probe_provisions_and_places_coder_worker_and_planner() {
    for role in ["coder", "worker", "planner"] {
        let fixture = Fixture::new(json!([{"name":"toolchain","command":"true","scope":"machine","timeout_seconds":5},{"name":"checkout","command":"test -f README.md","timeout_seconds":5}]), role).await;
        let first = fixture.claim().await.unwrap_err();
        assert!(matches!(
            first,
            services::ServiceError::PlacementUnavailable(_)
        ));
        let claimed = fixture.claim_until_ready().await;
        assert_eq!(claimed.execution.role, role);
        tokio::time::timeout(
            Duration::from_secs(15),
            fixture.service.start_execution(&claimed.execution.id),
        )
        .await
        .unwrap()
        .unwrap();
        let placement =
            WorkspacePlacementRepo::get_for_task(&*fixture.harness.state.db, &fixture.task)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(
            placement.daemon_id.as_deref(),
            Some(fixture.registration.daemon_id.as_str())
        );
        assert!(fixture
            .root
            .path()
            .join("repos")
            .join(&fixture.repo)
            .join("README.md")
            .exists());
        let requests = fixture
            .owner
            .as_ref()
            .unwrap()
            .requests
            .lock()
            .unwrap()
            .clone();
        assert!(
            requests.contains(&METHOD_EXECUTION_START.to_owned()),
            "the provisioned owner launches {role}"
        );
        let probe = requests
            .iter()
            .position(|method| method == METHOD_MACHINE_PROBE)
            .unwrap();
        let provision = requests
            .iter()
            .position(|method| method == METHOD_REPO_LOCATION_PROVISION)
            .unwrap();
        let verify = requests
            .iter()
            .position(|method| method == METHOD_REPO_LOCATION_VERIFY)
            .unwrap();
        let prepare = requests
            .iter()
            .position(|method| method == METHOD_WORKSPACE_PREPARE)
            .unwrap();
        assert!(probe < provision && provision < verify && verify < prepare);
        assert!(requests[verify + 1..prepare].contains(&METHOD_MACHINE_PROBE.to_owned()));
    }
}

#[tokio::test]
async fn failed_machine_check_or_no_machine_checks_never_copies_code() {
    for checks in [
        json!([{"name":"cargo","command":"echo missing; exit 127","scope":"machine","timeout_seconds":5}]),
        json!([{"name":"workspace","command":"true"}]),
        json!([]),
    ] {
        let machine = checks
            .as_array()
            .unwrap()
            .iter()
            .any(|check| check["scope"] == "machine");
        let fixture = Fixture::new(checks, "coder").await;
        let mut error = fixture.claim().await.unwrap_err();
        if machine {
            fixture.wait_for_fact().await;
            error = fixture.claim().await.unwrap_err();
        }
        let services::ServiceError::PlacementUnavailable(refusal) = error else {
            panic!("expected placement refusal");
        };
        assert!(refusal
            .rejected_candidates
            .iter()
            .any(|candidate| candidate.owner_kind == "server"
                && candidate
                    .filter_codes
                    .contains(&services::placement::PlacementFilterCode::ExecutorUnavailable)));
        let daemon = refusal
            .rejected_candidates
            .iter()
            .find(|candidate| {
                candidate.daemon_id.as_deref() == Some(&fixture.registration.daemon_id)
            })
            .unwrap();
        assert!(daemon.filter_codes.contains(&if machine {
            services::placement::PlacementFilterCode::EnvironmentNotReady
        } else {
            services::placement::PlacementFilterCode::EnvironmentUnverified
        }));
        if machine {
            assert_eq!(daemon.failing_checks, ["cargo"]);
        }
        assert!(!fixture
            .root
            .path()
            .join("repos")
            .join(&fixture.repo)
            .exists());
        assert!(!fixture
            .owner
            .as_ref()
            .unwrap()
            .requests
            .lock()
            .unwrap()
            .contains(&METHOD_REPO_LOCATION_PROVISION.into()));
        let task = TaskRepo::get_by_id(&*fixture.harness.state.db, &fixture.task, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.status, "todo");
        assert!(task.error_annotation.is_none());
        let project = ProjectRepo::get_by_id(&*fixture.harness.state.db, &fixture.project)
            .await
            .unwrap()
            .unwrap();
        if project.paused_at.is_some() {
            assert_eq!(
                project.system_pause_reason.as_deref(),
                Some("environment_not_ready")
            );
        } else {
            assert!(
                task.metadata_json
                    .as_deref()
                    .unwrap_or_default()
                    .contains("environment_wait"),
                "metadata={:?}; refusal={refusal:?}",
                task.metadata_json
            );
        }
    }
}

#[tokio::test]
async fn daemon_offline_mid_probe_retains_facts_and_retry_can_restart() {
    let mut fixture = Fixture::new(
        json!([{"name":"toolchain","command":"sleep 1; true","scope":"machine","timeout_seconds":5}]),
        "coder",
    )
    .await;
    fixture.claim().await.unwrap_err();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !fixture
            .owner
            .as_ref()
            .unwrap()
            .requests
            .lock()
            .unwrap()
            .contains(&METHOD_MACHINE_PROBE.into())
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    fixture.owner.take();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        fixture
            .harness
            .state
            .db
            .list_readiness(&fixture.project)
            .await
            .unwrap()
            .is_empty(),
        "transport failure cannot invent not-ready facts"
    );
    assert!(!fixture
        .root
        .path()
        .join("repos")
        .join(&fixture.repo)
        .exists());
    fixture.owner = Some(
        Owner::connect(
            &fixture.server,
            &fixture.registration,
            fixture.runtime.clone(),
        )
        .await,
    );
    sqlx::query("UPDATE repo_provision_retry SET next_attempt_at = '2000-01-01T00:00:00Z'")
        .execute(fixture.harness.state.db.pool())
        .await
        .unwrap();
    fixture.claim_until_ready().await;
}

#[tokio::test]
async fn provisioning_disabled_or_repository_without_remote_sends_no_probe_or_clone() {
    for disable in [true, false] {
        let fixture = Fixture::new(
            json!([{"name":"cargo","command":"true","scope":"machine","timeout_seconds":5}]),
            "coder",
        )
        .await;
        if disable {
            let project = ProjectRepo::get_by_id(&*fixture.harness.state.db, &fixture.project)
                .await
                .unwrap()
                .unwrap();
            let mut settings: Value = serde_json::from_str(&project.settings).unwrap();
            settings["placement"] = json!({"provision":"never"});
            ProjectRepo::update_at_version(
                &*fixture.harness.state.db,
                db::UpdateProject {
                    id: project.id,
                    name: None,
                    settings: Some(settings.to_string()),
                    primary_repo_id: None,
                    paused_at: None,
                    updated_at: db::now_rfc3339(),
                },
                project.version,
                None,
            )
            .await
            .unwrap();
        } else {
            sqlx::query("UPDATE repo SET remote_url = NULL WHERE id = ?")
                .bind(&fixture.repo)
                .execute(fixture.harness.state.db.pool())
                .await
                .unwrap();
        }
        fixture.claim().await.unwrap_err();
        assert!(fixture
            .owner
            .as_ref()
            .unwrap()
            .requests
            .lock()
            .unwrap()
            .is_empty());
        assert!(!fixture
            .root
            .path()
            .join("repos")
            .join(&fixture.repo)
            .exists());
    }
}

#[tokio::test]
async fn server_restart_after_clone_lost_reply_reuses_one_location_and_wakes_placement() {
    let mut fixture = Fixture::new(json!([{"name":"toolchain","command":"true","scope":"machine","timeout_seconds":5},{"name":"checkout","command":"test -f README.md","timeout_seconds":5}]), "coder").await;
    fixture
        .owner
        .as_ref()
        .unwrap()
        .drop_provision_reply
        .store(true, std::sync::atomic::Ordering::SeqCst);
    fixture.claim().await.unwrap_err();
    let clone = fixture.root.path().join("repos").join(&fixture.repo);
    tokio::time::timeout(Duration::from_secs(15), async {
        while !clone.join(".git").exists() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // The daemon has cloned, but the server has not received the provisioning
    // result. Replace the listening server and reconstruct its admission service.
    fixture.owner.take();
    // Reopen the durable SQLite file through a fresh service graph and command registry.
    let pool = db::create_sqlite_pool(&format!(
        "sqlite:{}",
        fixture._server_root.path().join("forge.sqlite").display()
    ))
    .await
    .unwrap();
    db::run_migrations(&pool).await.unwrap();
    let state = state_for_db(
        Arc::new(db::SqliteDb::new(pool)),
        &fixture._server_root.path().join("workspaces"),
    )
    .await;
    fixture.server = TestServer::start(state.clone()).await;
    fixture.harness.state = state;
    tokio::time::timeout(Duration::from_secs(10), async { loop {
        let failed: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM repo_provision_retry WHERE repo_id = ? AND last_error IS NOT NULL)").bind(&fixture.repo).fetch_one(fixture.harness.state.db.pool()).await.unwrap();
        if failed { break; } tokio::task::yield_now().await;
    } }).await.unwrap();
    fixture.service = services::TaskService::new_for_test(
        fixture.harness.state.db.clone(),
        fixture.harness.state.event_bus.clone(),
    )
    .with_workspace_root(fixture._server_root.path().join("workspaces"))
    .with_workspace_backend_router(fixture.harness.state.workspace_backend_router.clone())
    .with_daemon_connections(fixture.harness.state.daemon_connections.clone())
    .with_placement_adapter_registry(Arc::new(executors::AdapterRegistry::new()));
    fixture.owner = Some(
        Owner::connect(
            &fixture.server,
            &fixture.registration,
            fixture.runtime.clone(),
        )
        .await,
    );
    sqlx::query("UPDATE repo_provision_retry SET next_attempt_at = '2000-01-01T00:00:00Z' WHERE repo_id = ?").bind(&fixture.repo).execute(fixture.harness.state.db.pool()).await.unwrap();
    fixture.claim_until_ready().await;
    let locations: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM repo_location WHERE repo_id = ? AND owner_kind = 'daemon'",
    )
    .bind(&fixture.repo)
    .fetch_one(fixture.harness.state.db.pool())
    .await
    .unwrap();
    assert_eq!(locations, 1);
    assert_eq!(
        std::fs::read_dir(fixture.root.path().join("repos"))
            .unwrap()
            .count(),
        1
    );
    let retries: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM repo_provision_retry WHERE repo_id = ?")
            .bind(&fixture.repo)
            .fetch_one(fixture.harness.state.db.pool())
            .await
            .unwrap();
    assert_eq!(retries, 0);
}
