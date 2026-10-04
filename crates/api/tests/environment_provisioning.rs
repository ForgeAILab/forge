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
use tokio::{
    sync::{mpsc, oneshot, Notify},
    task::JoinHandle,
};
use tokio_tungstenite::tungstenite::Message;

struct Owner {
    requests: Arc<Mutex<Vec<String>>>,
    jobs: Vec<JoinHandle<()>>,
    drop_provision_reply: Arc<std::sync::atomic::AtomicBool>,
    reject_verify: Arc<std::sync::atomic::AtomicBool>,
    full_probe_release: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
    full_probe_started: Arc<Notify>,
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
        let reject_verify = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let rejected = reject_verify.clone();
        let full_probe_release: Arc<Mutex<Option<oneshot::Receiver<()>>>> =
            Arc::new(Mutex::new(None));
        let probe_release = full_probe_release.clone();
        let full_probe_started = Arc::new(Notify::new());
        let probe_started = full_probe_started.clone();
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
                    let full_probe = method == METHOD_MACHINE_PROBE
                        && seen
                            .lock()
                            .unwrap()
                            .iter()
                            .filter(|method| method.as_str() == METHOD_MACHINE_PROBE)
                            .count()
                            == 2;
                    let release = if full_probe {
                        probe_release.lock().unwrap().take()
                    } else {
                        None
                    };
                    if let Some(release) = release {
                        probe_started.notify_one();
                        release.await.expect("test releases full checks");
                    }
                    let mut reply = match common::fake_daemon::handle_environment_request(
                        &runtime,
                        frame.clone(),
                    )
                    .await
                    {
                        Some(reply) => reply,
                        None => runtime.handle_request(frame.clone()).await,
                    };
                    if method == METHOD_REPO_LOCATION_VERIFY
                        && rejected.load(std::sync::atomic::Ordering::SeqCst)
                    {
                        if let DaemonFrame::Request { id, .. } = &frame {
                            reply = DaemonFrame::Error {
                                id: Some(id.clone()),
                                error: DaemonErrorPayload {
                                    code: "verify_failed".into(),
                                    message: "verification deliberately failed".into(),
                                    details: None,
                                },
                            };
                        }
                    }
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
            reject_verify,
            full_probe_release,
            full_probe_started,
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
    service: Arc<services::TaskService>,
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
        Self::new_rooted(checks, role, false).await
    }
    async fn new_rooted(checks: Value, role: &str, symlink: bool) -> Self {
        let server_root = TestDir::new("environment-provision-server");
        let root = TestDir::new("environment-provision-daemon");
        let daemon_root = if symlink {
            let path = server_root.path().join("daemon-link");
            #[cfg(unix)]
            std::os::unix::fs::symlink(root.path(), &path).unwrap();
            path
        } else {
            root.path().to_owned()
        };
        let source = common::setup_git_repo(server_root.path());
        let harness = harness(server_root.path()).await;
        let registration =
            register_daemon(&harness.app, &db::new_uuid_v4(), "environment-provision").await;
        report_remote_daemon_shell(
            &harness.app,
            &registration.daemon_id,
            &registration.registration_token,
            &daemon_root,
            "environment-provision",
        )
        .await;
        let server = TestServer::start(harness.state.clone()).await;
        let (outbound, _) = mpsc::unbounded_channel();
        let runtime = DaemonRuntime::new_owned(
            outbound,
            daemon_root,
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
        let service = Arc::new(
            services::TaskService::new_for_test(
                harness.state.db.clone(),
                harness.state.event_bus.clone(),
            )
            .with_workspace_root(server_root.path().join("workspaces"))
            .with_workspace_backend_router(harness.state.workspace_backend_router.clone())
            .with_daemon_connections(harness.state.daemon_connections.clone())
            .with_placement_adapter_registry(Arc::new(executors::AdapterRegistry::new())),
        );
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
        if machine {
            assert!(task.error_annotation.is_none());
        }
        let project = ProjectRepo::get_by_id(&*fixture.harness.state.db, &fixture.project)
            .await
            .unwrap()
            .unwrap();
        assert!(
            project.paused_at.is_none(),
            "a codeless machine cannot pause the Project"
        );
        if machine {
            assert!(task
                .metadata_json
                .as_deref()
                .unwrap_or_default()
                .contains("environment_wait"));
            let attention: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM attention_projection WHERE dedupe_key=? AND status='open'",
            )
            .bind(format!("task-environment-wait:{}", fixture.task))
            .fetch_one(fixture.harness.state.db.pool())
            .await
            .unwrap();
            assert_eq!(attention, 1);
        } else {
            assert!(task
                .error_annotation
                .as_deref()
                .unwrap_or_default()
                .contains("Environment unverified"));
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
    fixture.service = Arc::new(
        services::TaskService::new_for_test(
            fixture.harness.state.db.clone(),
            fixture.harness.state.event_bus.clone(),
        )
        .with_workspace_root(fixture._server_root.path().join("workspaces"))
        .with_workspace_backend_router(fixture.harness.state.workspace_backend_router.clone())
        .with_daemon_connections(fixture.harness.state.daemon_connections.clone())
        .with_placement_adapter_registry(Arc::new(executors::AdapterRegistry::new())),
    );
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

async fn set_environment(fixture: &Fixture, checks: Value) -> ProjectEnvironment {
    let project = ProjectRepo::get_by_id(&*fixture.harness.state.db, &fixture.project)
        .await
        .unwrap()
        .unwrap();
    let environment: ProjectEnvironment =
        serde_json::from_value(json!({"checks": checks})).unwrap();
    ProjectRepo::update_at_version(
        &*fixture.harness.state.db,
        db::UpdateProject {
            id: project.id,
            name: None,
            settings: Some(json!({"environment":environment}).to_string()),
            primary_repo_id: None,
            paused_at: None,
            updated_at: db::now_rfc3339(),
        },
        project.version,
        None,
    )
    .await
    .unwrap();
    environment
}

#[cfg(unix)]
#[tokio::test]
async fn symlinked_daemon_root_records_authoritative_clone_path() {
    let fixture = Fixture::new_rooted(
        json!([{"name":"toolchain","command":"true","scope":"machine"}]),
        "coder",
        true,
    )
    .await;
    fixture.claim_until_ready().await;
    let path: String = sqlx::query_scalar(
        "SELECT path FROM repo_location WHERE repo_id=? AND owner_kind='daemon' AND status='ready'",
    )
    .bind(&fixture.repo)
    .fetch_one(fixture.harness.state.db.pool())
    .await
    .unwrap();
    assert_eq!(
        std::path::Path::new(&path),
        fixture
            .root
            .path()
            .canonicalize()
            .unwrap()
            .join("repos")
            .join(&fixture.repo)
    );
}

#[tokio::test]
async fn settings_edit_during_full_checks_discards_old_results() {
    let fixture = Fixture::new(
        json!([
            {"name":"toolchain","command":"true","scope":"machine"},
            {"name":"slowcheck","command":"sleep 3; true","timeout_seconds":10}
        ]),
        "coder",
    )
    .await;
    fixture.claim().await.unwrap_err();
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let probes = fixture
                .owner
                .as_ref()
                .unwrap()
                .requests
                .lock()
                .unwrap()
                .iter()
                .filter(|method| method.as_str() == METHOD_MACHINE_PROBE)
                .count();
            if probes >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let environment = set_environment(
        &fixture,
        json!([
            {"name":"toolchain","command":"true","scope":"machine"},
            {"name":"newfail","command":"echo newly failing check; exit 1"}
        ]),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let _ = fixture.claim().await;
            let rows = fixture
                .harness
                .state
                .db
                .list_readiness(&fixture.project)
                .await
                .unwrap();
            for row in rows
                .iter()
                .filter(|row| row.machine != db::EnvironmentMachine::Server)
            {
                if row.checks_digest == db::environment_checks_digest(&environment) {
                    assert!(!row
                        .check_results
                        .iter()
                        .any(|result| result.name == "slowcheck"));
                    if row.scope_covered == "full"
                        && row
                            .failing_checks
                            .iter()
                            .any(|check| check.name == "newfail")
                    {
                        return;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    let executions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM execution WHERE task_id=?")
        .bind(&fixture.task)
        .fetch_one(fixture.harness.state.db.pool())
        .await
        .unwrap();
    assert_eq!(executions, 0);
}

#[tokio::test]
async fn unverified_refusal_is_recorded_once_and_wakes_on_settings_change() {
    let fixture = Fixture::new(json!([{ "name":"checkout", "command":"true" }]), "coder").await;
    let mut first = None;
    for _ in 0..3 {
        let services::ServiceError::PlacementUnavailable(refusal) =
            fixture.claim().await.unwrap_err()
        else {
            panic!("expected placement refusal");
        };
        assert!(refusal.is_deterministic());
        let snapshot: (i64, i64, String) =
            sqlx::query_as("SELECT t.version,p.list_revision,t.metadata_json FROM task t JOIN project p ON p.id=t.project_id WHERE t.id=?")
                .bind(&fixture.task)
                .fetch_one(fixture.harness.state.db.pool())
                .await
                .unwrap();
        if let Some(first) = &first {
            assert_eq!(&snapshot, first);
        } else {
            first = Some(snapshot);
        }
    }
    let task = TaskRepo::get_by_id(&*fixture.harness.state.db, &fixture.task, false)
        .await
        .unwrap()
        .unwrap();
    let annotation = task.error_annotation.unwrap();
    assert!(annotation.contains("remote-test-host"), "{annotation}");
    assert!(annotation.contains("mark a Project check"));
    let attention: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM attention_projection WHERE dedupe_key=? AND status='open'",
    )
    .bind(format!("task-environment-unverified:{}", fixture.task))
    .fetch_one(fixture.harness.state.db.pool())
    .await
    .unwrap();
    assert_eq!(attention, 1);
    let visible: AttentionListResponse = json_request(
        &fixture.harness.app,
        Method::GET,
        &format!(
            "/api/v1/mission-control/attention?project_id={}",
            fixture.project
        ),
        Value::Null,
        StatusCode::OK,
    )
    .await;
    assert!(visible
        .items
        .iter()
        .any(|item| item.details["cause"] == "environment_unverified"
            && item.category == AttentionCategory::HumanInputRequired));

    assert!(!fixture
        .root
        .path()
        .join("repos")
        .join(&fixture.repo)
        .exists());
    set_environment(
        &fixture,
        json!([{ "name":"toolchain", "command":"true", "scope":"machine" }]),
    )
    .await;
    fixture.claim_until_ready().await;
}

#[tokio::test]
async fn incompatible_provisioning_candidate_keeps_deterministic_refusal() {
    for checks in [
        json!([]),
        json!([{ "name":"toolchain", "command":"true", "scope":"machine" }]),
    ] {
        let fixture = Fixture::new(checks, "coder").await;
        let nowhere: AgentResponse = json_request(
            &fixture.harness.app,
            Method::POST,
            "/api/v1/agents",
            json!({"name":"executor nowhere","executor_type":"codex"}),
            StatusCode::OK,
        )
        .await;
        // The normal claim rejects this unavailable identity at its earlier
        // governance gate. Exercise the same public admission context loader
        // directly to reproduce the audit's placement-classification defect.
        let task = TaskRepo::get_by_id(&*fixture.harness.state.db, &fixture.task, false)
            .await
            .unwrap()
            .unwrap();
        let repo = RepoRepo::get_by_id(&*fixture.harness.state.db, &fixture.repo)
            .await
            .unwrap()
            .unwrap();
        let project = ProjectRepo::get_by_id(&*fixture.harness.state.db, &fixture.project)
            .await
            .unwrap()
            .unwrap();
        let agent = db::AgentRepo::get_by_id(&*fixture.harness.state.db, &nowhere.id)
            .await
            .unwrap()
            .unwrap();
        let claiming = services::placement::WorktreeAgent {
            role: "coder".into(),
            agent,
            required_capabilities: Default::default(),
        };
        let settings: ProjectSettings = serde_json::from_str(&project.settings).unwrap();
        let server = services::placement::ServerFacts {
            execution_daemon_id: None,
            executors: Default::default(),
        };
        let handshakes = fixture
            .harness
            .state
            .daemon_connections
            .connection_snapshots()
            .into_iter()
            .map(|(id, facts)| {
                (
                    id,
                    services::placement::ConnectionHandshake {
                        connection_id: facts.connection_id,
                        handshake: facts.handshake,
                    },
                )
            })
            .collect();
        let mut tx = fixture.harness.state.db.pool().begin().await.unwrap();
        let context = services::placement::load_selection_context(
            &fixture.harness.state.db,
            &mut tx,
            &fixture.harness.state.daemon_connections,
            services::placement::SelectionLoadInput {
                task: &task,
                repo: &repo,
                claiming_agent: &claiming,
                worktree_agents: &[],
                task_owner_id: Some("test-user-id"),
                workspace_id: None,
                inherited_root_workspace_id: None,
                review_config: &ReviewConfig::default(),
                project_settings: &settings,
                server: &server,
                handshakes: &handshakes,
                fallback_server_location: None,
            },
        )
        .await
        .unwrap();
        tx.rollback().await.unwrap();
        let refusal = services::placement::select_placement(&context)
            .into_result()
            .unwrap_err();
        assert!(refusal.is_deterministic(), "{refusal:?}");
        assert!(
            !refusal.rejected_candidates.iter().any(|candidate| candidate
                .filter_codes
                .contains(&services::placement::PlacementFilterCode::EnvironmentProbePending))
        );
        let retries: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM repo_provision_retry WHERE repo_id=?")
                .bind(&fixture.repo)
                .fetch_one(fixture.harness.state.db.pool())
                .await
                .unwrap();
        assert_eq!(retries, 0);
    }
}

#[tokio::test]
async fn dispatcher_provisions_without_manual_retry_deadlines_and_keeps_server_location() {
    let fixture = Fixture::new(
        json!([{ "name":"toolchain", "command":"true", "scope":"machine" }]),
        "coder",
    )
    .await;
    sqlx::query("DELETE FROM repo_location WHERE repo_id=? AND owner_kind='server'")
        .bind(&fixture.repo)
        .execute(fixture.harness.state.db.pool())
        .await
        .unwrap();
    let now = db::now_rfc3339();
    db::TaskRoleAssignmentRepo::assign(
        &*fixture.harness.state.db,
        db::CreateTaskRoleAssignment {
            id: db::new_uuid_v4(),
            task_id: fixture.task.clone(),
            role_name: "coder".into(),
            assignee_type: Some(db::AssigneeKind::Agent),
            assignee_id: Some(fixture.agent.clone()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
    let dispatcher = services::TaskDispatcher::new(
        fixture.harness.state.db.clone(),
        fixture.harness.state.event_bus.clone(),
        fixture.service.clone(),
    );
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            dispatcher.check_once().await.unwrap();
            if let Some(placement) =
                WorkspacePlacementRepo::get_for_task(&*fixture.harness.state.db, &fixture.task)
                    .await
                    .unwrap()
            {
                assert_eq!(
                    placement.daemon_id.as_deref(),
                    Some(fixture.registration.daemon_id.as_str())
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    // Once the daemon clone exists, a missing lazy server row must still be
    // recreated for the next host-capable admission.
    sqlx::query("DELETE FROM repo_location WHERE repo_id=? AND owner_kind='server'")
        .bind(&fixture.repo)
        .execute(fixture.harness.state.db.pool())
        .await
        .unwrap();
    let task = TaskRepo::get_by_id(&*fixture.harness.state.db, &fixture.task, false)
        .await
        .unwrap()
        .unwrap();
    let now = db::now_rfc3339();
    let second = TaskRepo::create(
        &*fixture.harness.state.db,
        db::CreateTask {
            id: db::new_uuid_v4(),
            project_id: fixture.project.clone(),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "Server placement".into(),
            description: None,
            task_type: "task".into(),
            status: "todo".into(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: task.plan,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
    let host_agent: AgentResponse = json_request(
        &fixture.harness.app,
        Method::POST,
        "/api/v1/agents",
        json!({"name":"server executor","executor_type":"shell","config":{"command":"true"}}),
        StatusCode::OK,
    )
    .await;
    let host = services::TaskService::new_for_test(
        fixture.harness.state.db.clone(),
        fixture.harness.state.event_bus.clone(),
    )
    .with_workspace_root(fixture._server_root.path().join("workspaces"))
    .with_workspace_backend_router(fixture.harness.state.workspace_backend_router.clone())
    .with_daemon_connections(fixture.harness.state.daemon_connections.clone());
    host.claim_task(&second.id, services::Assignee::Agent(host_agent.id), None)
        .await
        .unwrap();
    let host_placement =
        WorkspacePlacementRepo::get_for_task(&*fixture.harness.state.db, &second.id)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(host_placement.owner_kind, db::PlacementOwnerKind::Server);
    let locations: Vec<String> = sqlx::query_scalar(
        "SELECT owner_kind FROM repo_location WHERE repo_id=? ORDER BY owner_kind",
    )
    .bind(&fixture.repo)
    .fetch_all(fixture.harness.state.db.pool())
    .await
    .unwrap();
    assert_eq!(locations, vec!["daemon", "server"]);
    assert!(fixture
        .root
        .path()
        .join("repos")
        .join(&fixture.repo)
        .join(".git")
        .exists());
}

#[tokio::test]
async fn clone_failure_wait_exposes_redacted_error_and_elapsed_time() {
    let fixture = Fixture::new(
        json!([{ "name":"toolchain", "command":"true", "scope":"machine" }]),
        "coder",
    )
    .await;
    sqlx::query("UPDATE repo SET remote_url='https://probe-user:USERSECRET@127.0.0.1:9/repo.git?token=QUERYSECRET' WHERE id=?").bind(&fixture.repo).execute(fixture.harness.state.db.pool()).await.unwrap();
    fixture.claim().await.unwrap_err();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let task = TaskRepo::get_by_id(&*fixture.harness.state.db, &fixture.task, false)
                .await
                .unwrap()
                .unwrap();
            let metadata = task.metadata_json.unwrap_or_default();
            if metadata.contains("last failure") {
                assert!(metadata.contains("clone_failed"), "{metadata}");
                assert!(metadata.contains("elapsed"), "{metadata}");
                assert!(
                    !metadata.contains("USERSECRET") && !metadata.contains("QUERYSECRET"),
                    "{metadata}"
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    let error: String = sqlx::query_scalar(
        "SELECT last_error FROM repo_location WHERE repo_id=? AND owner_kind='daemon'",
    )
    .bind(&fixture.repo)
    .fetch_one(fixture.harness.state.db.pool())
    .await
    .unwrap();
    assert!(
        !error.contains("USERSECRET") && !error.contains("QUERYSECRET"),
        "{error}"
    );
    RepoRepo::delete(&*fixture.harness.state.db, &fixture.repo)
        .await
        .unwrap();
    let retries: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM repo_provision_retry WHERE repo_id=?")
            .bind(&fixture.repo)
            .fetch_one(fixture.harness.state.db.pool())
            .await
            .unwrap();
    assert_eq!(
        retries, 0,
        "repository deletion cascades the provisioning retry row"
    );
}

#[tokio::test]
async fn provision_requested_branch_differs_from_remote_head() {
    let fixture = Fixture::new(
        json!([{ "name":"toolchain", "command":"true", "scope":"machine" }]),
        "coder",
    )
    .await;
    let source: String = sqlx::query_scalar("SELECT local_path FROM repo WHERE id=?")
        .bind(&fixture.repo)
        .fetch_one(fixture.harness.state.db.pool())
        .await
        .unwrap();
    assert!(tokio::process::Command::new("git")
        .args(["switch", "-c", "develop"])
        .current_dir(source)
        .output()
        .await
        .unwrap()
        .status
        .success());
    // Project default remains main, while the remote's HEAD is develop.
    // An ordinary clone lacks a local main branch, so verification needs the
    // explicit provision branch rather than the remote's HEAD choice.
    fixture.claim_until_ready().await;
    let location: (String, String) = sqlx::query_as(
        "SELECT path,status FROM repo_location WHERE repo_id=? AND owner_kind='daemon'",
    )
    .bind(&fixture.repo)
    .fetch_one(fixture.harness.state.db.pool())
    .await
    .unwrap();
    assert_eq!(location.1, "ready");
    let branch = tokio::process::Command::new("git")
        .args(["branch", "--show-current"])
        .current_dir(location.0)
        .output()
        .await
        .unwrap();
    assert_eq!(String::from_utf8(branch.stdout).unwrap().trim(), "main");
}

#[tokio::test]
async fn initial_unverified_dispatch_is_stable_until_eligibility_changes() {
    let fixture = Fixture::new(json!([]), "coder").await;
    let now = db::now_rfc3339();
    db::TaskRoleAssignmentRepo::assign(
        &*fixture.harness.state.db,
        db::CreateTaskRoleAssignment {
            id: db::new_uuid_v4(),
            task_id: fixture.task.clone(),
            role_name: "coder".into(),
            assignee_type: Some(db::AssigneeKind::Agent),
            assignee_id: Some(fixture.agent.clone()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
    let dispatcher = services::TaskDispatcher::new(
        fixture.harness.state.db.clone(),
        fixture.harness.state.event_bus.clone(),
        fixture.service.clone(),
    );
    let mut first = None;
    for _ in 0..3 {
        dispatcher.check_once().await.unwrap();
        fixture.service.drain(&fixture.task).await.unwrap();
        let snapshot:(i64,i64,Option<String>,Option<String>)=sqlx::query_as("SELECT t.version,p.list_revision,t.error_annotation,t.metadata_json FROM task t JOIN project p ON p.id=t.project_id WHERE t.id=?").bind(&fixture.task).fetch_one(fixture.harness.state.db.pool()).await.unwrap();
        assert!(
            snapshot
                .2
                .as_deref()
                .unwrap_or_default()
                .contains("environment_unverified"),
            "{snapshot:?}"
        );
        if let Some(first) = &first {
            assert_eq!(&snapshot, first);
        } else {
            first = Some(snapshot);
        }
    }
    assert!(fixture
        .harness
        .state
        .db
        .list_readiness(&fixture.project)
        .await
        .unwrap()
        .is_empty());
    assert!(fixture
        .owner
        .as_ref()
        .unwrap()
        .requests
        .lock()
        .unwrap()
        .is_empty());
    sqlx::query("UPDATE task SET status='in_progress' WHERE id=?")
        .bind(&fixture.task)
        .execute(fixture.harness.state.db.pool())
        .await
        .unwrap();
    let states =
        json!({"in_progress":{"kind":"active","owns_work":true},"todo":{"kind":"initial"}});
    // Exclude annotation blockers: the environment_wait marker itself parks
    // active Tasks in both slot queries, without another scan rewriting it.
    let slots = TaskRepo::count_project_slots(
        &*fixture.harness.state.db,
        &fixture.project,
        &states.to_string(),
        "{}",
        "[]",
    )
    .await
    .unwrap();
    assert_eq!(slots, (0, 1, 0));
    let grouped = TaskRepo::count_projects_slots(
        &*fixture.harness.state.db,
        &json!({fixture.project.clone():states}).to_string(),
        "{}",
        "[]",
    )
    .await
    .unwrap();
    assert_eq!(grouped[0].active, 0);
    assert_eq!(grouped[0].parked, 1);
}

#[tokio::test]
async fn verification_failure_is_visible_and_exhausted_retry_waits_for_reconnect() {
    let mut fixture = Fixture::new(
        json!([{ "name":"toolchain", "command":"true", "scope":"machine" }]),
        "coder",
    )
    .await;
    fixture
        .owner
        .as_ref()
        .unwrap()
        .reject_verify
        .store(true, std::sync::atomic::Ordering::SeqCst);
    fixture.claim().await.unwrap_err();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let task = TaskRepo::get_by_id(&*fixture.harness.state.db, &fixture.task, false)
                .await
                .unwrap()
                .unwrap();
            if task
                .metadata_json
                .as_deref()
                .unwrap_or_default()
                .contains("verification deliberately failed")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    // Model the same durable state left by five failed/never-answering attempts.
    sqlx::query("UPDATE repo_provision_retry SET attempts=5 WHERE repo_id=?")
        .bind(&fixture.repo)
        .execute(fixture.harness.state.db.pool())
        .await
        .unwrap();
    let before = fixture
        .owner
        .as_ref()
        .unwrap()
        .requests
        .lock()
        .unwrap()
        .len();
    let mut first = None;
    for _ in 0..3 {
        let services::ServiceError::PlacementUnavailable(refusal) =
            fixture.claim().await.unwrap_err()
        else {
            panic!("placement refusal");
        };
        assert!(refusal.is_deterministic());
        assert!(refusal.rejected_candidates.iter().any(|candidate| candidate
            .filter_codes
            .contains(&services::placement::PlacementFilterCode::ProvisionFailed)));
        let snapshot:(i64,i64,Option<String>,Option<String>)=sqlx::query_as("SELECT t.version,p.list_revision,t.error_annotation,t.metadata_json FROM task t JOIN project p ON p.id=t.project_id WHERE t.id=?").bind(&fixture.task).fetch_one(fixture.harness.state.db.pool()).await.unwrap();
        assert!(snapshot.2.as_deref().unwrap().contains("remote-test-host"));
        assert!(snapshot
            .2
            .as_deref()
            .unwrap()
            .contains("verification deliberately failed"));
        assert!(snapshot.3.as_deref().unwrap().contains("provision_failed"));
        if let Some(first) = &first {
            assert_eq!(&snapshot, first);
        } else {
            first = Some(snapshot);
        }
    }
    assert_eq!(
        fixture
            .owner
            .as_ref()
            .unwrap()
            .requests
            .lock()
            .unwrap()
            .len(),
        before
    );
    let attention: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM attention_projection WHERE dedupe_key=? AND status='open'",
    )
    .bind(format!("task-provision-failed:{}", fixture.task))
    .fetch_one(fixture.harness.state.db.pool())
    .await
    .unwrap();
    assert_eq!(attention, 1);
    let now = db::now_rfc3339();
    db::TaskRoleAssignmentRepo::assign(
        &*fixture.harness.state.db,
        db::CreateTaskRoleAssignment {
            id: db::new_uuid_v4(),
            task_id: fixture.task.clone(),
            role_name: "coder".into(),
            assignee_type: Some(db::AssigneeKind::Agent),
            assignee_id: Some(fixture.agent.clone()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
    sqlx::query("UPDATE task SET status='in_progress',error_annotation=NULL,metadata_json=NULL,version=version+1 WHERE id=?").bind(&fixture.task).execute(fixture.harness.state.db.pool()).await.unwrap();
    let dispatcher = services::TaskDispatcher::new(
        fixture.harness.state.db.clone(),
        fixture.harness.state.event_bus.clone(),
        fixture.service.clone(),
    );
    let mut active = None;
    for _ in 0..3 {
        dispatcher.check_once().await.unwrap();
        fixture.service.drain(&fixture.task).await.unwrap();
        let snapshot:(i64,i64,Option<String>,Option<String>)=sqlx::query_as("SELECT t.version,p.list_revision,t.error_annotation,t.metadata_json FROM task t JOIN project p ON p.id=t.project_id WHERE t.id=?").bind(&fixture.task).fetch_one(fixture.harness.state.db.pool()).await.unwrap();
        assert!(
            snapshot
                .2
                .as_deref()
                .unwrap_or_default()
                .contains("verification deliberately failed"),
            "{snapshot:?}"
        );
        if let Some(active) = &active {
            assert_eq!(&snapshot, active);
        } else {
            active = Some(snapshot);
        }
    }
    sqlx::query("UPDATE task SET status='todo',version=version+1 WHERE id=?")
        .bind(&fixture.task)
        .execute(fixture.harness.state.db.pool())
        .await
        .unwrap();
    fixture.owner.take();
    fixture.owner = Some(
        Owner::connect(
            &fixture.server,
            &fixture.registration,
            fixture.runtime.clone(),
        )
        .await,
    );
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            dispatcher.check_once().await.unwrap();
            fixture.service.drain(&fixture.task).await.unwrap();
            if WorkspacePlacementRepo::get_for_task(&*fixture.harness.state.db, &fixture.task)
                .await
                .unwrap()
                .is_some()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn unrelated_project_edit_during_full_checks_keeps_the_same_provision_attempt() {
    let fixture = Fixture::new(
        json!([
            {"name":"toolchain","command":"true","scope":"machine"},
            {"name":"full","command":"true","timeout_seconds":10}
        ]),
        "coder",
    )
    .await;
    let owner = fixture.owner.as_ref().unwrap();
    let (release, wait) = oneshot::channel();
    *owner.full_probe_release.lock().unwrap() = Some(wait);
    fixture.claim().await.unwrap_err();
    tokio::time::timeout(Duration::from_secs(20), owner.full_probe_started.notified())
        .await
        .unwrap();
    let project = ProjectRepo::get_by_id(&*fixture.harness.state.db, &fixture.project)
        .await
        .unwrap()
        .unwrap();
    let mut settings: Value = serde_json::from_str(&project.settings).unwrap();
    settings["max_active_tasks"] = json!(17);
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
    release.send(()).unwrap();
    // Observe this provisioning job's settlement before asking admission to
    // verify the same still-unverified location concurrently. The barrier
    // above proves the unrelated edit happened during its full checks.
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let ready: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM repo_location WHERE repo_id=? AND owner_kind='daemon' AND status='ready')")
                .bind(&fixture.repo).fetch_one(fixture.harness.state.db.pool()).await.unwrap();
            if ready { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap();
    fixture.claim_until_ready().await;
    assert_eq!(
        fixture
            .owner
            .as_ref()
            .unwrap()
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|method| method.as_str() == METHOD_REPO_LOCATION_PROVISION)
            .count(),
        1
    );
}
