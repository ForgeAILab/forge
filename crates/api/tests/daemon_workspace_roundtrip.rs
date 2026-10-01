mod common;

use std::{
    collections::{BTreeMap, HashSet},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use api_types::*;
use db::{
    AssigneeKind, CreateExecution, CreateProject, CreateRepo, CreateRepoLocation, CreateReview,
    CreateTask, CreateTaskRoleAssignment, CreateWorkspace, CreateWorkspacePlacement, ExecutionRepo,
    ExecutionStatus, PlacementOwnerKind, PlacementSelectedBy, PlacementState, ProjectRepo,
    RepoLocationKind, RepoLocationOwnerKind, RepoLocationRepo, RepoLocationStatus, RepoRepo,
    ReviewRepo, ReviewStatus, RuntimeRepo, TaskRepo, TaskRoleAssignmentRepo,
    UpdateWorkspacePlacement, WorkMode, WorkspacePlacementRepo, WorkspaceRepo, WorkspaceStatus,
};
use forge_client::{
    daemon_persistence::{JournalEntry, JournalOperation},
    daemon_runtime::{ActiveExecutionTracker, DaemonRuntime},
};
use futures_util::{SinkExt, StreamExt};
use review::{CommandLimits, ReviewWorkspace};
use serde_json::{json, Value};
use services::workspace_backend::{
    MergeOutcome, MergeSpec, PrepareSpec, ResolvedWorkspace, RunSpec, WorkspaceBackendError,
};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, http::HeaderValue, Message},
};

use common::{fake_daemon::TestServer, Harness, TestDir};

struct DaemonLink {
    outbound: mpsc::UnboundedSender<DaemonFrame>,
    requests: Arc<Mutex<Vec<(String, Value)>>>,
    interrupt: Arc<Mutex<HashSet<String>>>,
    drop_replies: Arc<Mutex<HashSet<String>>>,
    tasks: Vec<JoinHandle<()>>,
}

impl DaemonLink {
    async fn connect(
        server: &TestServer,
        daemon_id: &str,
        token: &str,
        runtime: Arc<DaemonRuntime>,
    ) -> Self {
        let mut request = format!("ws://{}/api/v1/daemons/{daemon_id}/connect", server.addr)
            .into_client_request()
            .expect("websocket request");
        request.headers_mut().insert(
            "Authorization",
            HeaderValue::from_str(&format!("Bearer {token}")).expect("bearer header"),
        );
        let (socket, _) = connect_async(request).await.expect("real daemon connects");
        let (mut writer, mut reader) = socket.split();
        let (outbound, mut incoming) = mpsc::unbounded_channel();
        runtime.attach(outbound.clone());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let interrupt = Arc::new(Mutex::new(HashSet::new()));
        let drop_replies = Arc::new(Mutex::new(HashSet::new()));
        let write_task = tokio::spawn(async move {
            while let Some(frame) = incoming.recv().await {
                let text = serde_json::to_string(&frame).expect("daemon frame serializes");
                if writer.send(Message::Text(text.into())).await.is_err() {
                    break;
                }
            }
        });
        let observed = Arc::clone(&requests);
        let interrupted = Arc::clone(&interrupt);
        let dropped = Arc::clone(&drop_replies);
        let replies = outbound.clone();
        let read_task = tokio::spawn(async move {
            while let Some(Ok(message)) = reader.next().await {
                let Message::Text(text) = message else {
                    continue;
                };
                let frame: DaemonFrame = serde_json::from_str(text.as_ref()).expect("server frame");
                let DaemonFrame::Request { method, params, .. } = &frame else {
                    continue;
                };
                observed
                    .lock()
                    .unwrap()
                    .push((method.clone(), params.clone()));
                // Install a real durable intent to reproduce a daemon crash
                // between journal retention and recording the operation outcome.
                if interrupted.lock().unwrap().remove(method) {
                    let fence: WorkspaceMutationFence =
                        serde_json::from_value(params.clone()).expect("mutation fence");
                    runtime
                        .journal()
                        .retain_entry(&JournalEntry::Operation {
                            operation: JournalOperation {
                                entry_id: format!("forge:operation:{}", fence.operation_id),
                                fence,
                                workspace_handle: params["workspace_handle"]
                                    .as_str()
                                    .map(str::to_owned),
                                method: method.clone(),
                                request: params.clone(),
                                outcome: None,
                                acknowledged: false,
                            },
                        })
                        .expect("interrupted intent retained");
                }
                let runtime = Arc::clone(&runtime);
                let replies = replies.clone();
                let dropped = Arc::clone(&dropped);
                let method = method.clone();
                tokio::spawn(async move {
                    let response = runtime.handle_request(frame).await;
                    if !dropped.lock().unwrap().contains(&method) {
                        let _ = replies.send(response);
                    }
                });
            }
        });
        let heartbeats = outbound.clone();
        let heartbeat = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(200));
            let mut seq = 0;
            loop {
                ticker.tick().await;
                seq += 1;
                if heartbeats.send(DaemonFrame::Heartbeat { seq }).is_err() {
                    break;
                }
            }
        });
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            if server
                .state
                .daemon_connections
                .get(daemon_id)
                .is_some_and(|connection| {
                    connection.protocol_allows_dispatch()
                        && connection
                            .snapshot()
                            .is_some_and(|facts| !facts.workspace_incapable)
                })
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "workspace handshake arrives"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Self {
            outbound,
            requests,
            interrupt,
            drop_replies,
            tasks: vec![write_task, read_task, heartbeat],
        }
    }

    fn requests(&self, method: &str) -> Vec<Value> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(name, _)| name == method)
            .map(|(_, params)| params.clone())
            .collect()
    }

    fn stop_heartbeats(&self) {
        self.tasks[2].abort();
    }
}

impl Drop for DaemonLink {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort()
        }
    }
}

struct Fixture {
    harness: Harness,
    server: TestServer,
    link: Option<DaemonLink>,
    runtime: Arc<DaemonRuntime>,
    daemon_id: String,
    token: String,
    resolved: ResolvedWorkspace,
    execution_id: String,
    base_sha: String,
    checkout: PathBuf,
    inaccessible_server_path: PathBuf,
    server_root: TestDir,
    _daemon_root: TestDir,
}

impl Fixture {
    async fn new(suite: &str) -> Self {
        Self::with_policy(
            suite,
            WorkspaceRunPolicy {
                allowed_purposes: vec![
                    WorkspaceRunPurpose::CiStep,
                    WorkspaceRunPurpose::Hook,
                    WorkspaceRunPurpose::EnvironmentSetup,
                ],
            },
        )
        .await
    }

    async fn with_policy(suite: &str, run_policy: WorkspaceRunPolicy) -> Self {
        let server_root = TestDir::new(&format!("{suite}-server"));
        let daemon_root = TestDir::new(&format!("{suite}-owner"));
        let checkout = common::setup_git_repo(daemon_root.path());
        let harness = common::test_app(&server_root.path().join("workspaces"), suite).await;
        let registration =
            common::fake_daemon::register_daemon(&harness.app, &db::new_uuid_v4(), suite).await;
        common::fake_daemon::report_remote_daemon_shell(
            &harness.app,
            &registration.daemon_id,
            &registration.registration_token,
            daemon_root.path(),
            suite,
        )
        .await;
        let runtime_id = RuntimeRepo::get_by_daemon_id(&*harness.state.db, &registration.daemon_id)
            .await
            .expect("runtime loads")
            .expect("runtime exists")
            .id;
        let server = TestServer::start(Arc::clone(&harness.state)).await;
        let (outbound, _discarded) = mpsc::unbounded_channel();
        let runtime = DaemonRuntime::new_owned(
            outbound,
            daemon_root.path().to_path_buf(),
            ActiveExecutionTracker::default(),
            registration.daemon_id.clone(),
            run_policy,
        )
        .expect("owner runtime starts");
        let link = DaemonLink::connect(
            &server,
            &registration.daemon_id,
            &registration.registration_token,
            Arc::clone(&runtime),
        )
        .await;
        let now = db::now_rfc3339();
        let project = ProjectRepo::create(
            &*harness.state.db,
            CreateProject {
                id: db::new_uuid_v4(),
                name: suite.into(),
                settings: "{}".into(),
                workflow_definition: "{}".into(),
                primary_repo_id: None,
                owner_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("project creates");
        let repo = RepoRepo::create(
            &*harness.state.db,
            CreateRepo {
                id: db::new_uuid_v4(),
                project_id: project.id.clone(),
                name: "repo".into(),
                remote_url: Some("https://example.test/owner-only.git".into()),
                local_path: None,
                work_mode: WorkMode::DirectMerge,
                default_branch: "main".into(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("repo creates without a server path");
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
        .expect("owner repository is the Project primary repository");
        let location = RepoLocationRepo::create(
            &*harness.state.db,
            CreateRepoLocation {
                id: db::new_uuid_v4(),
                repo_id: repo.id.clone(),
                owner_kind: RepoLocationOwnerKind::Daemon,
                daemon_id: Some(registration.daemon_id.clone()),
                runtime_id: Some(runtime_id.clone()),
                path: checkout.to_string_lossy().into_owned(),
                kind: RepoLocationKind::PrimaryCheckout,
                is_default: true,
                status: RepoLocationStatus::Ready,
                last_verified_at: Some(now.clone()),
                last_error: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("owner location creates");
        let task = TaskRepo::create(
            &*harness.state.db,
            CreateTask {
                id: db::new_uuid_v4(),
                project_id: project.id,
                parent_task_id: None,
                subtask_order: None,
                assignee_type: None,
                assignee_id: None,
                title: "Daemon workspace review".into(),
                description: None,
                task_type: "task".into(),
                status: "review".into(),
                is_automation: false,
                priority: 0,
                task_state_config: None,
                merge_config: None,
                plan: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("task creates");
        let inaccessible_server_path = server_root.path().join("missing-checkout/repo");
        let workspace = WorkspaceRepo::create(
            &*harness.state.db,
            CreateWorkspace {
                id: db::new_uuid_v4(),
                task_id: task.id.clone(),
                repo_id: repo.id,
                worktree_path: inaccessible_server_path.to_string_lossy().into_owned(),
                branch: workspace::task_branch_name(&task.id),
                status: WorkspaceStatus::Creating,
                before_sha: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("workspace creates");
        let placement = WorkspacePlacementRepo::create(
            &*harness.state.db,
            CreateWorkspacePlacement {
                id: db::new_uuid_v4(),
                workspace_id: workspace.id.clone(),
                task_id: task.id.clone(),
                agent_id: None,
                owner_kind: PlacementOwnerKind::Daemon,
                daemon_id: Some(registration.daemon_id.clone()),
                runtime_id: Some(runtime_id.clone()),
                repo_location_id: location.id.clone(),
                execution_daemon_id: Some(registration.daemon_id.clone()),
                workspace_handle: None,
                generation: 1,
                state: PlacementState::Preparing,
                selected_by: PlacementSelectedBy::Scheduler,
                selection_reason: "{}".into(),
                reserved_until: None,
                disconnected_at: None,
                failure_cause: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("placement creates");
        let backend = harness
            .state
            .workspace_backend_router
            .for_placement(&placement)
            .expect("daemon backend is installed");
        let verified = backend
            .daemon_client()
            .expect("owner client")
            .verify_location(
                &registration.daemon_id,
                RepoLocationVerifyParams {
                    repo_location_id: location.id,
                    daemon_id: registration.daemon_id.clone(),
                    runtime_id,
                    path: checkout.to_string_lossy().into_owned(),
                    kind: DaemonRepoLocationKind::PrimaryCheckout,
                    default_branch: "main".into(),
                    remote_url: None,
                    expected_version: location.version,
                    probe: None,
                },
            )
            .await
            .expect("owner verifies its checkout");
        let prepared = backend
            .prepare(
                &placement,
                &PrepareSpec {
                    base_ref: "main".into(),
                },
            )
            .await
            .expect("owner prepares a worktree");
        assert_eq!(prepared.base_sha, verified.default_branch_sha);
        let placement = WorkspacePlacementRepo::update(
            &*harness.state.db,
            UpdateWorkspacePlacement {
                id: placement.id,
                expected_version: placement.version,
                agent_id: None,
                owner_kind: None,
                daemon_id: None,
                runtime_id: None,
                repo_location_id: None,
                execution_daemon_id: None,
                workspace_handle: Some(Some(prepared.handle)),
                generation: None,
                state: Some(PlacementState::Ready),
                selected_by: None,
                selection_reason: None,
                reserved_until: None,
                disconnected_at: None,
                failure_cause: None,
                updated_at: db::now_rfc3339(),
            },
        )
        .await
        .expect("prepared handle persists with CAS");
        WorkspaceRepo::update_status(
            &*harness.state.db,
            &workspace.id,
            WorkspaceStatus::Ready,
            None,
            &now,
        )
        .await
        .expect("workspace is ready");
        let execution_id = db::new_uuid_v4();
        ExecutionRepo::create(
            &*harness.state.db,
            execution_input(
                &execution_id,
                &task.id,
                &workspace.id,
                "executor",
                ExecutionStatus::Completed,
            ),
        )
        .await
        .expect("implementation execution creates");
        Self {
            harness,
            server,
            link: Some(link),
            runtime,
            daemon_id: registration.daemon_id,
            token: registration.registration_token,
            resolved: ResolvedWorkspace { placement, backend },
            execution_id,
            base_sha: prepared.base_sha,
            checkout,
            inaccessible_server_path,
            server_root,
            _daemon_root: daemon_root,
        }
    }

    async fn run(&self, command: &str) -> services::workspace_backend::RunResult {
        self.resolved
            .backend
            .run(
                &self.resolved.placement,
                &RunSpec {
                    purpose: WorkspaceRunPurpose::CiStep,
                    command: command.into(),
                    env: BTreeMap::new(),
                    timeout_secs: 0,
                    max_output_bytes: usize::MAX,
                },
            )
            .await
            .expect("owner command succeeds")
    }

    async fn candidate(&self) -> String {
        let result = self
            .run("printf 'candidate\n' > feature.txt; git add feature.txt; git commit -m candidate")
            .await;
        assert_eq!(result.exit_code, 0);
        self.resolved
            .git_query(WorkspaceGitQuery::Head, false)
            .await
            .unwrap()
            .unwrap()
            .trim()
            .into()
    }

    async fn reviewer_attempt(&self) -> (db::Review, db::Execution) {
        let agent_id: String = sqlx::query_scalar(
            "SELECT id FROM agent_current WHERE backend_kind = 'cli' ORDER BY id LIMIT 1",
        )
        .fetch_one(self.harness.state.db.pool())
        .await
        .expect("default reviewer identity");
        let now = db::now_rfc3339();
        TaskRoleAssignmentRepo::assign(
            &*self.harness.state.db,
            CreateTaskRoleAssignment {
                id: db::new_uuid_v4(),
                task_id: self.resolved.placement.task_id.clone(),
                role_name: "reviewer".into(),
                assignee_type: Some(AssigneeKind::Agent),
                assignee_id: Some(agent_id.clone()),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("reviewer assigned");
        let reviewer_id = db::new_uuid_v4();
        let mut input = execution_input(
            &reviewer_id,
            &self.resolved.placement.task_id,
            &self.resolved.placement.workspace_id,
            "reviewer",
            ExecutionStatus::Running,
        );
        input.agent_id = Some(agent_id);
        input.parent_execution_id = Some(self.execution_id.clone());
        let connection = self
            .harness
            .state
            .daemon_connections
            .get(&self.daemon_id)
            .unwrap();
        ReviewRepo::create_attempt_with_execution_and_lease(
            &*self.harness.state.db,
            CreateReview {
                id: db::new_uuid_v4(),
                task_id: self.resolved.placement.task_id.clone(),
                execution_id: self.execution_id.clone(),
                attempt_number: 1,
                status: ReviewStatus::Running,
                step_results_json: "{}".into(),
                started_at: now.clone(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
            input,
            db::ClaimExecutionLease {
                execution_id: reviewer_id,
                expected_version: 1,
                owner: services::daemon_transport::execution_lease_owner(
                    &self.daemon_id,
                    connection.id(),
                ),
                lease_expires_at: (chrono::Utc::now() + chrono::Duration::seconds(60)).to_rfc3339(),
                hard_deadline_at: Some(
                    (chrono::Utc::now() + chrono::Duration::minutes(30)).to_rfc3339(),
                ),
                now,
            },
            None,
        )
        .await
        .expect("review attempt and owner lease admit together")
    }

    async fn approve_candidate(&self) -> ReviewContract {
        let (review, auditor) = self.reviewer_attempt().await;
        let auditor_id = auditor.id.clone();
        let now = db::now_rfc3339();
        let contract = review::contract::admit(
            &self.harness.state.db,
            &auditor_id,
            &self.resolved.placement.task_id,
            &self.resolved,
        )
        .await
        .expect("owner review admits");
        let conformance = review::contract::evaluate(
            &self.harness.state.db,
            &auditor_id,
            &self.resolved,
            "The candidate is implemented.\n\n{\"result\":\"pass\",\"reason\":\"verified\"}",
        )
        .await
        .expect("owner conformance evaluates");
        assert_eq!(conformance.status, ConformanceStatus::Passed);
        ReviewRepo::update_status(
            &*self.harness.state.db,
            &review.id,
            ReviewStatus::Passed,
            json!({"ci_steps":[], "conformance":conformance}).to_string(),
            Some(now.clone()),
            &now,
        )
        .await
        .expect("review passes");
        let mut transaction = db::begin_immediate(self.harness.state.db.pool())
            .await
            .unwrap();
        let version: i64 =
            sqlx::query_scalar("SELECT execution_version FROM execution WHERE id = ?")
                .bind(&auditor_id)
                .fetch_one(&mut *transaction)
                .await
                .unwrap();
        let updated = sqlx::query("UPDATE execution SET status = 'completed', execution_version = execution_version + 1 WHERE id = ? AND execution_version = ? AND status = 'running'")
            .bind(&auditor_id).bind(version)
            .execute(&mut *transaction).await.expect("reviewer completes");
        assert_eq!(updated.rows_affected(), 1);
        transaction.commit().await.unwrap();
        let task = TaskRepo::get_by_id(
            &*self.harness.state.db,
            &self.resolved.placement.task_id,
            false,
        )
        .await
        .unwrap()
        .unwrap();
        TaskRepo::set_review_passed_at_cas(
            &*self.harness.state.db,
            &task.id,
            task.version,
            Some(now.clone()),
            &now,
        )
        .await
        .expect("review authority projects");
        contract
    }

    async fn receipt(&self, method: &str) -> Value {
        let raw: String = sqlx::query_scalar(
            "SELECT outcome_json FROM command_receipt WHERE scope_id = ? AND operation = ? ORDER BY committed_at DESC, id DESC LIMIT 1",
        ).bind(&self.resolved.placement.task_id).bind(format!("daemon.{method}"))
            .fetch_one(self.harness.state.db.pool()).await.expect("owner receipt exists");
        serde_json::from_str(&raw).unwrap()
    }

    async fn reconnect(&mut self) {
        self.link.take();
        common::fake_daemon::wait_until_disconnected(&self.harness.state, &self.daemon_id).await;
        self.link = Some(
            DaemonLink::connect(
                &self.server,
                &self.daemon_id,
                &self.token,
                Arc::clone(&self.runtime),
            )
            .await,
        );
    }

    fn assert_server_cannot_resolve_workspace(&self) {
        assert!(!self.inaccessible_server_path.exists());
        assert!(self.resolved.embedded_path().is_err());
        assert!(!std::path::Path::new(self.resolved.handle().unwrap()).is_absolute());
        assert!(!self.checkout.starts_with(self.server_root.path()));
    }
}

fn execution_input(
    id: &str,
    task_id: &str,
    workspace_id: &str,
    role: &str,
    status: ExecutionStatus,
) -> CreateExecution {
    let now = db::now_rfc3339();
    CreateExecution {
        id: id.into(),
        task_id: task_id.into(),
        agent_id: None,
        role: role.into(),
        status,
        stop_reason: None,
        stopped_by: None,
        resume_policy: None,
        stopped_at: None,
        parent_execution_id: None,
        agent_session_id: None,
        agent_message_id: None,
        last_activity_at: None,
        summary: None,
        logs_path: None,
        before_sha: None,
        after_sha: None,
        error: None,
        executor_config_snapshot_json: None,
        workspace_id: Some(workspace_id.into()),
        created_at: now.clone(),
        updated_at: now,
    }
}

fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

#[tokio::test]
async fn remote_lease_expiry_before_socket_disconnect_accepts_journaled_commit_once() {
    assert_outage_order(false).await;
}

#[tokio::test]
async fn remote_socket_disconnect_before_lease_expiry_accepts_journaled_commit_once() {
    assert_outage_order(true).await;
}

async fn assert_outage_order(socket_first: bool) {
    use db::AgentRepo;
    let mut fixture = Fixture::new(if socket_first {
        "forge-workspace-disconnect-first"
    } else {
        "forge-workspace-lease-first"
    })
    .await;
    let database = Arc::clone(&fixture.harness.state.db);
    let placement = fixture.resolved.placement.clone();
    let now = db::now_rfc3339();
    let agent = AgentRepo::create(
        &*database,
        db::CreateAgent {
            id: db::new_uuid_v4(),
            name: "outage-shell".into(),
            description: None,
            executor_type: "shell".into(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            capabilities_json: "[]".into(),
            config_json: "{}".into(),
            credential_ref: None,
            daemon_id: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: db::AgentStatus::Idle,
            last_heartbeat_at: Some(now.clone()),
            is_default: false,
            paused: false,
            owner_id: None,
            visibility: "global".into(),
            prompt_template: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    TaskRoleAssignmentRepo::assign(
        &*database,
        CreateTaskRoleAssignment {
            id: db::new_uuid_v4(),
            task_id: placement.task_id.clone(),
            role_name: "coder".into(),
            assignee_type: Some(AssigneeKind::Agent),
            assignee_id: Some(agent.id.clone()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    sqlx::query(
        "UPDATE task SET status = 'in_progress', assignee_type = 'agent', assignee_id = ?,
        version = version + 1, updated_at = ? WHERE id = ?",
    )
    .bind(&agent.id)
    .bind(&now)
    .bind(&placement.task_id)
    .execute(database.pool())
    .await
    .unwrap();
    sqlx::query("UPDATE workspace SET before_sha = ? WHERE id = ?")
        .bind(&fixture.base_sha)
        .bind(&placement.workspace_id)
        .execute(database.pool())
        .await
        .unwrap();
    // Stop at the Review gate so this test can inspect reconciliation before
    // another Agent or cleanup starts. The coder completion cascade is real.
    let mut workflow = services::workflow::default_workflow::default_workflow();
    workflow
        .states
        .iter_mut()
        .find(|state| state.name == "review")
        .unwrap()
        .hooks = StateHooks::default();
    sqlx::query(
        "UPDATE project SET workflow_definition = ?, version = version + 1 WHERE id =
        (SELECT project_id FROM task WHERE id = ?)",
    )
    .bind(serde_json::to_string(&workflow).unwrap())
    .bind(&placement.task_id)
    .execute(database.pool())
    .await
    .unwrap();
    let owner_connection = fixture
        .harness
        .state
        .daemon_connections
        .get(&fixture.daemon_id)
        .unwrap()
        .id();
    let execution_id = db::new_uuid_v4();
    let mut input = execution_input(
        &execution_id,
        &placement.task_id,
        &placement.workspace_id,
        "coder",
        ExecutionStatus::Running,
    );
    input.agent_id = Some(agent.id);
    input.before_sha = Some(fixture.base_sha.clone());
    input.executor_config_snapshot_json = Some(
        json!({"executor_type": "shell", "config": {},
        "placement_id": placement.id})
        .to_string(),
    );
    ExecutionRepo::create_with_lease(
        &*database,
        input,
        db::ClaimExecutionLease {
            execution_id: execution_id.clone(),
            expected_version: 1,
            owner: services::daemon_transport::execution_lease_owner(
                &fixture.daemon_id,
                owner_connection,
            ),
            lease_expires_at: (chrono::Utc::now() + chrono::Duration::minutes(1)).to_rfc3339(),
            hard_deadline_at: Some((chrono::Utc::now() + chrono::Duration::hours(1)).to_rfc3339()),
            now,
        },
    )
    .await
    .unwrap();
    let (workspace_path, _) = fixture.resolved.owner_paths().await.unwrap();
    fixture.runtime.start(ExecutionStartParams {
        task_id: placement.task_id.clone(), execution_id: execution_id.clone(),
            workspace_path,
        executor_type: "shell".into(), executor_config: json!({"executor_type": "shell", "config": {}}),
        prompt: json!({"description": format!(
            "sleep 1; printf 'outage line\\n' >> feature.txt; git add feature.txt; git commit -m 'outage line'; mkdir -p ../.forge-outbox/{execution_id}; printf '%s\\n' '{{\"kind\":\"validation\",\"summary\":\"Outage work committed\"}}' > ../.forge-outbox/{execution_id}/worklog.jsonl"
        )}), max_turns: None,
    }).await.unwrap();
    fixture.link.as_ref().unwrap().stop_heartbeats();
    // Freeze daemon frames while the CLI child keeps running. The server's
    // socket remains open for the lease-first ordering.
    let (frozen_outbound, _frozen_frames) = mpsc::unbounded_channel();
    fixture.runtime.attach(frozen_outbound);
    tokio::time::sleep(Duration::from_millis(250)).await;
    if socket_first {
        fixture.link.take();
        common::fake_daemon::wait_until_disconnected(&fixture.harness.state, &fixture.daemon_id)
            .await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while WorkspacePlacementRepo::get_by_id(&*database, &placement.id)
            .await
            .unwrap()
            .unwrap()
            .state
            != PlacementState::Disconnected
        {
            assert!(
                tokio::time::Instant::now() < deadline,
                "socket disconnect suspends before lease expiry"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    sqlx::query(
        "UPDATE execution SET lease_expires_at = ?, last_heartbeat_at = ?,
        execution_version = execution_version + 1 WHERE id = ?",
    )
    .bind((chrono::Utc::now() - chrono::Duration::minutes(9)).to_rfc3339())
    .bind((chrono::Utc::now() - chrono::Duration::minutes(10)).to_rfc3339())
    .bind(&execution_id)
    .execute(database.pool())
    .await
    .unwrap();
    let monitor =
        services::HeartbeatMonitor::new(database.clone(), fixture.harness.state.event_bus.clone())
            .with_daemon_connections(fixture.harness.state.daemon_connections.clone())
            .with_task_service(fixture.harness.state.task_service.clone());
    tokio::time::timeout(Duration::from_secs(5), monitor.check_once())
        .await
        .unwrap()
        .unwrap();
    let disconnected = WorkspacePlacementRepo::get_by_id(&*database, &placement.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(disconnected.state, PlacementState::Disconnected);
    assert_eq!(
        disconnected.failure_cause,
        Some(db::PlacementFailureCause::OwnerDisconnected)
    );
    assert!(disconnected.disconnected_at.is_some());
    assert_eq!(disconnected.version, placement.version + 1);
    assert_eq!(
        ExecutionRepo::get_by_id(&*database, &execution_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        ExecutionStatus::Running
    );
    assert_eq!(
        TaskRepo::get_by_id(&*database, &placement.task_id, false)
            .await
            .unwrap()
            .unwrap()
            .status,
        "in_progress"
    );
    if !socket_first {
        assert!(fixture
            .harness
            .state
            .daemon_connections
            .is_connected(&fixture.daemon_id));
        fixture.link.take();
        common::fake_daemon::wait_until_disconnected(&fixture.harness.state, &fixture.daemon_id)
            .await;
    }
    assert_eq!(
        WorkspacePlacementRepo::get_by_id(&*database, &placement.id)
            .await
            .unwrap()
            .unwrap(),
        disconnected
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let report = loop {
        if let Some(report) = fixture
            .runtime
            .journal()
            .pending()
            .unwrap()
            .into_iter()
            .find_map(|entry| match entry {
                JournalEntry::Terminal { report } if report.execution_id == execution_id => {
                    Some(report)
                }
                _ => None,
            })
        {
            break report;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "CLI finishes and journals its committed head while frozen"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(report.exit_code, Some(0));
    assert!(report
        .after_sha
        .as_deref()
        .is_some_and(|head| head != fixture.base_sha));
    fixture.reconnect().await;
    loop {
        monitor.check_once().await.unwrap();
        if WorkspacePlacementRepo::get_by_id(&*database, &placement.id)
            .await
            .unwrap()
            .unwrap()
            .state
            == PlacementState::Ready
            && TaskRepo::get_by_id(&*database, &placement.task_id, false)
                .await
                .unwrap()
                .unwrap()
                .status
                == "review"
            && fixture.runtime.journal().pending().unwrap().is_empty()
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "retained terminal reconciles and advances the Task to Review"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let finished = ExecutionRepo::get_by_id(&*database, &execution_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(finished.status, ExecutionStatus::Completed);
    assert_eq!(finished.after_sha, report.after_sha);
    let link = fixture.link.as_ref().unwrap();
    let acknowledgements = link.requests(METHOD_JOURNAL_ACK).len();
    link.outbound
        .send(DaemonFrame::Notification {
            method: METHOD_EXECUTION_TERMINAL.into(),
            params: serde_json::to_value(&report).unwrap(),
        })
        .unwrap();
    while link.requests(METHOD_JOURNAL_ACK).len() <= acknowledgements {
        assert!(
            tokio::time::Instant::now() < deadline,
            "duplicate terminal is acknowledged"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let terminals: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM domain_event WHERE event_type = 'execution.completed'
        AND json_extract(payload_json, '$.execution_id') = ?",
    )
    .bind(&execution_id)
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert_eq!(terminals, 1);
    let outbox_entries: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM task_comment WHERE task_id = ? AND idempotency_key = ?",
    )
    .bind(&placement.task_id)
    .bind(format!("outbox:{execution_id}:worklog:1"))
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert_eq!(outbox_entries, 1);
    let executions: i64 =
        sqlx::query_scalar("SELECT count(*) FROM execution WHERE workspace_id = ?")
            .bind(&placement.workspace_id)
            .fetch_one(database.pool())
            .await
            .unwrap();
    assert_eq!(
        executions, 2,
        "only the fixture's historical execution and this attempt exist"
    );
    let placements: i64 =
        sqlx::query_scalar("SELECT count(*) FROM workspace_placement WHERE workspace_id = ?")
            .bind(&placement.workspace_id)
            .fetch_one(database.pool())
            .await
            .unwrap();
    assert_eq!(placements, 1);
    assert!(!link.requests(METHOD_WORKSPACE_DESCRIBE).is_empty());
    fixture.assert_server_cannot_resolve_workspace();
}

#[tokio::test]
async fn remote_plan_checklist_gate_matches_server_placement() {
    use services::workflow::{
        actions::RequirePlanChecklistComplete, default_workflow, HookAction, HookContext,
        HookResult,
    };

    let fixture = Fixture::new("forge-workspace-plan-gate").await;
    let state = &fixture.harness.state;
    let owner_task = TaskRepo::get_by_id(&*state.db, &fixture.resolved.placement.task_id, false)
        .await
        .unwrap()
        .unwrap();
    let now = db::now_rfc3339();
    let server_task = TaskRepo::create(
        &*state.db,
        CreateTask {
            id: db::new_uuid_v4(),
            project_id: owner_task.project_id.clone(),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "Server workspace plan gate".into(),
            description: None,
            task_type: "task".into(),
            status: "in_progress".into(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    let server_path = fixture.server_root.path().join("plan-gate/repo");
    std::fs::create_dir_all(&server_path).unwrap();
    let server_workspace = WorkspaceRepo::create(
        &*state.db,
        CreateWorkspace {
            id: db::new_uuid_v4(),
            task_id: server_task.id.clone(),
            repo_id: WorkspaceRepo::get_by_id(&*state.db, &fixture.resolved.placement.workspace_id)
                .await
                .unwrap()
                .unwrap()
                .repo_id,
            worktree_path: server_path.to_string_lossy().into_owned(),
            branch: workspace::task_branch_name(&server_task.id),
            status: WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
    let context = |task_id: &str, workspace_id: &str| HookContext {
        task_id: task_id.into(),
        project_id: owner_task.project_id.clone(),
        from_state: "in_progress".into(),
        to_state: "review".into(),
        db: Arc::clone(&state.db),
        event_bus: Arc::clone(&state.event_bus),
        gate_config: None,
        workflow: Arc::new(default_workflow::default_workflow()),
        project_version: None,
        project_workflow_definition: None,
        triggered_by: Actor::system(SystemComponent::Test),
        review_runner: None,
        merge_service: None,
        cleanup_scheduler: None,
        task_service: fixture.harness.state.task_service.as_ref().clone(),
        daemon_connections: None,
        workspace_exec_locks: None,
        terminal_activity: None,
        workspace_root: fixture.server_root.path().join("workspaces"),
        repo_cache_locks: None,
        workspace_backend_router: Arc::clone(&state.workspace_backend_router),
        workspace_id: Some(workspace_id.into()),
        agent_id: None,
        execution_id: None,
        state_config: json!({}),
    };
    let daemon_context = context(&owner_task.id, &fixture.resolved.placement.workspace_id);
    let server_context = context(&server_task.id, &server_workspace.id);
    for content in [
        None,
        Some("- [x] complete\n"),
        Some("No checklist\n"),
        Some("- [ ] unfinished\n"),
    ] {
        if let Some(content) = content {
            std::fs::write(server_path.parent().unwrap().join("plan.md"), content).unwrap();
            fixture
                .run(&format!("printf '%s' {} > ../plan.md", quote(content)))
                .await;
        }
        let daemon_result = RequirePlanChecklistComplete.execute(&daemon_context).await;
        let server_result = RequirePlanChecklistComplete.execute(&server_context).await;
        assert_eq!(format!("{daemon_result:?}"), format!("{server_result:?}"));
        match content {
            None => {
                assert!(
                    matches!(daemon_result, HookResult::Skipped { reason } if reason == "no plan checklist")
                );
                assert!(services::plan_artifact::read_plan_with_router(
                    &state.db,
                    &state.workspace_backend_router,
                    &fixture.resolved.placement.workspace_id,
                )
                .await
                .unwrap()
                .is_none());
            }
            Some("- [ ] unfinished\n") => {
                assert!(
                    matches!(daemon_result, HookResult::Failed { reason } if reason.contains("1 unchecked item(s)"))
                );
            }
            Some(_) => assert!(matches!(daemon_result, HookResult::Ok)),
        }
    }
    assert!(!fixture.inaccessible_server_path.exists());
}

#[tokio::test]
async fn remote_review_uses_owner_evidence_checkouts_and_unbounded_ci_output() {
    let fixture = Fixture::new("forge-workspace-review").await;
    let candidate = fixture.candidate().await;
    let env = BTreeMap::from([("FORGE_TEST_SECRET".into(), "secret-value".into())]);
    let output = ReviewWorkspace::run(&fixture.resolved,
        "head -c 1100000 /dev/zero | tr '\\000' x; printf '%s' \"$FORGE_TEST_SECRET\"; printf 'stderr' >&2", &env, None)
        .await.expect("CI collects the full stream");
    assert_eq!(output.exit_code, Some(0));
    assert!(output.stdout.len() >= 1_100_000);
    assert!(!output.stdout.contains("secret-value"));
    assert_eq!(output.stderr, "stderr");
    let run = fixture
        .link
        .as_ref()
        .unwrap()
        .requests(METHOD_WORKSPACE_RUN)
        .pop()
        .unwrap();
    assert_eq!(run["timeout_secs"], 0);
    assert_eq!(run["max_output_bytes"], json!(u64::MAX));
    let receipt = fixture.receipt(METHOD_WORKSPACE_RUN).await;
    assert!(!receipt["owner_result"]["stdout"]
        .as_str()
        .unwrap()
        .contains("secret-value"));
    let diff = ReviewWorkspace::diff(&fixture.resolved, "main")
        .await
        .unwrap();
    assert!(diff.contains("+candidate"));
    let paths = fixture
        .resolved
        .git_query(
            WorkspaceGitQuery::CandidatePaths {
                base_sha: fixture.base_sha.clone(),
                commit_sha: candidate.clone(),
            },
            false,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(paths, "feature.txt\0");
    assert_eq!(
        fixture
            .resolved
            .backend
            .read(&fixture.resolved.placement, "feature.txt", 1024)
            .await
            .unwrap(),
        b"candidate\n"
    );
    let asset = fixture._daemon_root.path().join("fixture.asset");
    std::fs::write(&asset, "owner asset").unwrap();
    let environment = ProjectEnvironment {
        env: BTreeMap::new(),
        assets: vec![EnvironmentAsset {
            source: asset.to_string_lossy().into_owned(),
            target: "assets/seed".into(),
        }],
        checks: vec![],
    };
    fixture
        .resolved
        .materialize_assets(&environment)
        .await
        .unwrap();
    assert_eq!(
        fixture
            .resolved
            .backend
            .read(&fixture.resolved.placement, "assets/seed", 1024)
            .await
            .unwrap(),
        b"owner asset"
    );
    assert_eq!(
        fixture
            .run("printf 'dirty\n' > feature.txt; printf 'artifact' > untracked-build")
            .await
            .exit_code,
        0
    );
    let checkout = fixture
        .resolved
        .clean_checkout(&candidate, &environment, true)
        .await
        .unwrap();
    let clean = checkout.run("test ! -e untracked-build && test \"$(cat feature.txt)\" = candidate && test \"$(cat assets/seed)\" = 'owner asset'",
        &BTreeMap::new(), Some(CommandLimits { timeout_secs: 2, max_output_bytes: 1024 })).await.unwrap();
    assert_eq!(clean.exit_code, Some(0));
    checkout.close().await.unwrap();
    fixture.resolved.restore(&candidate).await.unwrap();
    assert_eq!(
        fixture
            .resolved
            .git_query(WorkspaceGitQuery::TrackedChanges, false)
            .await
            .unwrap()
            .unwrap()
            .trim(),
        ""
    );
    let bounded = fixture
        .resolved
        .run(
            "printf 'abcdefghijklmnopqrstuvwxyz'",
            &BTreeMap::new(),
            Some(CommandLimits {
                timeout_secs: 2,
                max_output_bytes: 16,
            }),
        )
        .await;
    assert!(bounded
        .err()
        .unwrap()
        .to_string()
        .contains("output exceeds"));
    for purpose in [
        WorkspaceRunPurpose::Hook,
        WorkspaceRunPurpose::EnvironmentSetup,
    ] {
        let output = fixture
            .resolved
            .backend
            .run(
                &fixture.resolved.placement,
                &RunSpec {
                    purpose,
                    command: "test -f feature.txt && printf owner".into(),
                    env: BTreeMap::new(),
                    timeout_secs: 2,
                    max_output_bytes: 1024,
                },
            )
            .await
            .unwrap();
        assert_eq!(output.exit_code, 0);
        assert_eq!(output.stdout_tail, "owner");
    }
    fixture.assert_server_cannot_resolve_workspace();
    assert!(fixture.runtime.journal().pending().unwrap().is_empty());
}

#[tokio::test]
async fn remote_merge_binds_approved_evidence_and_replays_durable_success() {
    let fixture = Fixture::new("forge-workspace-merge").await;
    let candidate = fixture.candidate().await;
    let contract = fixture.approve_candidate().await;
    assert_eq!(contract.commit_sha, candidate);
    let spec = MergeSpec {
        target_branch: "main".into(),
        expected_target_sha: fixture.base_sha.clone(),
        handed_off_paths: vec![],
    };
    let outcome = fixture
        .resolved
        .backend
        .merge(&fixture.resolved.placement, &spec)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        MergeOutcome::Done {
            before_sha: fixture.base_sha.clone(),
            after_sha: candidate.clone(),
            branch: "main".into()
        }
    );
    let receipt = fixture.receipt(METHOD_WORKSPACE_MERGE).await;
    assert_eq!(receipt["owner_result"]["diffstat"]["files_changed"], 1);
    assert_eq!(receipt["owner_result"]["outcome"]["after_sha"], candidate);
    let execution = ExecutionRepo::get_by_id(&*fixture.harness.state.db, &fixture.execution_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(execution.before_sha.as_deref(), Some(candidate.as_str()));
    assert_eq!(execution.after_sha.as_deref(), Some(candidate.as_str()));
    let requests = fixture
        .link
        .as_ref()
        .unwrap()
        .requests(METHOD_WORKSPACE_MERGE);
    assert_eq!(requests[0]["expected"]["sha"], candidate);
    assert_eq!(requests[0]["expected_target_sha"], contract.base_sha);
    assert_eq!(requests[0]["reviewed_commit_sha"], candidate);
    let replay = fixture
        .resolved
        .backend
        .merge(&fixture.resolved.placement, &spec)
        .await
        .unwrap();
    assert_eq!(replay, outcome);
    assert_eq!(
        fixture
            .link
            .as_ref()
            .unwrap()
            .requests(METHOD_WORKSPACE_MERGE)
            .len(),
        1
    );
    assert!(fixture.runtime.journal().pending().unwrap().is_empty());
    fixture.assert_server_cannot_resolve_workspace();
}

#[tokio::test]
async fn remote_merge_refuses_changed_candidate_and_target() {
    let fixture = Fixture::new("forge-workspace-merge-fences").await;
    fixture.candidate().await;
    fixture.approve_candidate().await;
    let spec = MergeSpec {
        target_branch: "main".into(),
        expected_target_sha: fixture.base_sha.clone(),
        handed_off_paths: vec![],
    };
    assert_eq!(
        fixture
            .run(&format!(
                "git -C {} commit --allow-empty -m target-moved",
                quote(&fixture.checkout.to_string_lossy())
            ))
            .await
            .exit_code,
        0
    );
    assert!(matches!(
        fixture
            .resolved
            .backend
            .merge(&fixture.resolved.placement, &spec)
            .await
            .unwrap(),
        MergeOutcome::TargetMoved { .. }
    ));
    let candidate_changed = fixture
        .run("git commit --allow-empty -m changed-candidate")
        .await;
    assert_eq!(candidate_changed.exit_code, 0);
    let before = fixture
        .link
        .as_ref()
        .unwrap()
        .requests(METHOD_WORKSPACE_MERGE)
        .len();
    assert!(matches!(
        fixture
            .resolved
            .backend
            .merge(&fixture.resolved.placement, &spec)
            .await
            .unwrap(),
        MergeOutcome::ReviewRequired { .. }
    ));
    assert_eq!(
        fixture
            .link
            .as_ref()
            .unwrap()
            .requests(METHOD_WORKSPACE_MERGE)
            .len(),
        before
    );
    fixture.assert_server_cannot_resolve_workspace();
}

#[tokio::test]
async fn remote_merge_without_a_reviewer_preserves_merge_commit_and_conflict_evidence() {
    let fixture = Fixture::new("forge-workspace-unreviewed-merge").await;
    let candidate = fixture.candidate().await;
    assert_eq!(
        fixture
            .run(&format!(
                "git -C {} commit --allow-empty -m target",
                quote(&fixture.checkout.to_string_lossy())
            ))
            .await
            .exit_code,
        0
    );
    let outcome = fixture
        .harness
        .state
        .merge_service
        .merge(&fixture.resolved.placement.task_id)
        .await
        .unwrap();
    let MergeOutcome::Done { after_sha, .. } = outcome else {
        panic!("divergent histories merge")
    };
    assert_ne!(after_sha, candidate);
    let requests = fixture
        .link
        .as_ref()
        .unwrap()
        .requests(METHOD_WORKSPACE_MERGE);
    assert!(requests[0]["reviewed_commit_sha"].is_null());
    let receipt = fixture.receipt(METHOD_WORKSPACE_MERGE).await;
    assert_eq!(receipt["owner_result"]["outcome"]["after_sha"], after_sha);
    assert_eq!(receipt["owner_result"]["diffstat"]["files_changed"], 1);
    fixture.assert_server_cannot_resolve_workspace();

    let fixture = Fixture::new("forge-workspace-conflict").await;
    fixture.candidate().await;
    let target = quote(&fixture.checkout.to_string_lossy());
    assert_eq!(fixture.run(&format!("printf 'target\\n' > {target}/feature.txt; git -C {target} add feature.txt; git -C {target} commit -m target")).await.exit_code, 0);
    let outcome = fixture
        .harness
        .state
        .merge_service
        .merge(&fixture.resolved.placement.task_id)
        .await
        .unwrap();
    let MergeOutcome::Conflict { conflict_paths, .. } = outcome else {
        panic!("conflicting additions fail the merge")
    };
    assert_eq!(conflict_paths, vec![PathBuf::from("feature.txt")]);
    assert_eq!(
        fixture.receipt(METHOD_WORKSPACE_MERGE).await["owner_result"]["outcome"]["conflict_paths"],
        json!(["feature.txt"])
    );
    assert!(fixture.runtime.journal().pending().unwrap().is_empty());
    fixture.assert_server_cannot_resolve_workspace();
}

#[tokio::test]
async fn remote_knowledge_plugins_read_and_write_through_the_owner() {
    use services::lifecycle::{
        knowledge_capture::KnowledgeCapturePlugin, knowledge_inject::KnowledgeInjectPlugin,
        LifecycleHookContext, LifecyclePlugin, PluginResult,
    };
    let fixture = Fixture::new("forge-workspace-knowledge").await;
    let logs = fixture.server_root.path().join("logs");
    std::fs::create_dir_all(&logs).unwrap();
    std::fs::write(logs.join("execution.jsonl"), format!("{}\n", json!({
        "kind":"assistant", "payload":"Architecture: daemon workspace review operations use their owner handle."
    }))).unwrap();
    let ctx = LifecycleHookContext {
        event: LifecycleEvent::OnTaskDone,
        task_id: fixture.resolved.placement.task_id.clone(),
        task_title: "Daemon workspace review".into(),
        task_status: "done".into(),
        previous_status: "merging".into(),
        project_id: "fixture-project".into(),
        project_name: "fixture".into(),
        repo_path: fixture
            .inaccessible_server_path
            .to_string_lossy()
            .into_owned(),
        worktree_path: Some(fixture.resolved.handle().unwrap().into()),
        agent_id: None,
        execution_id: Some(fixture.execution_id.clone()),
        log_dir: Some(logs),
        env: BTreeMap::new(),
    };
    let capture = KnowledgeCapturePlugin
        .execute_in_workspace(&ctx, &fixture.resolved)
        .await
        .unwrap();
    assert!(matches!(capture, PluginResult::Success));
    let index = fixture
        .resolved
        .backend
        .read(
            &fixture.resolved.placement,
            "docs/knowledge/KNOWLEDGE.md",
            8192,
        )
        .await
        .unwrap();
    assert!(String::from_utf8(index).unwrap().contains("daemon"));
    let mut ctx = ctx;
    ctx.event = LifecycleEvent::BeforeWork;
    assert!(matches!(
        KnowledgeInjectPlugin
            .execute_in_workspace(&ctx, &fixture.resolved)
            .await
            .unwrap(),
        PluginResult::Success
    ));
    let injected = fixture
        .resolved
        .backend
        .read(
            &fixture.resolved.placement,
            ".forge/knowledge-context.md",
            8192,
        )
        .await
        .unwrap();
    assert!(String::from_utf8(injected)
        .unwrap()
        .contains("owner handle"));
    assert_eq!(
        fixture
            .run("git diff --name-only HEAD")
            .await
            .stdout_tail
            .trim(),
        ""
    );
    assert!(fixture.runtime.journal().pending().unwrap().is_empty());
    fixture.assert_server_cannot_resolve_workspace();
}

#[tokio::test]
async fn remote_cleanup_stays_cleaning_offline_until_owner_acknowledges() {
    let mut fixture = Fixture::new("forge-workspace-cleanup").await;
    fixture.link.take();
    common::fake_daemon::wait_until_disconnected(&fixture.harness.state, &fixture.daemon_id).await;
    fixture
        .harness
        .state
        .cleanup_scheduler
        .cleanup_now(&fixture.resolved.placement.workspace_id)
        .await
        .unwrap();
    let placement = WorkspacePlacementRepo::get_by_id(
        &*fixture.harness.state.db,
        &fixture.resolved.placement.id,
    )
    .await
    .unwrap()
    .unwrap();
    let workspace = WorkspaceRepo::get_by_id(&*fixture.harness.state.db, &placement.workspace_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(placement.state, PlacementState::Cleaning);
    assert_eq!(workspace.status, WorkspaceStatus::Cleaning);
    fixture.reconnect().await;
    fixture
        .harness
        .state
        .cleanup_scheduler
        .cleanup_now(&placement.workspace_id)
        .await
        .unwrap();
    let placement = WorkspacePlacementRepo::get_by_id(&*fixture.harness.state.db, &placement.id)
        .await
        .unwrap()
        .unwrap();
    let workspace = WorkspaceRepo::get_by_id(&*fixture.harness.state.db, &placement.workspace_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(placement.state, PlacementState::Cleaned);
    assert_eq!(workspace.status, WorkspaceStatus::Cleaned);
    assert_eq!(
        fixture.receipt(METHOD_WORKSPACE_CLEANUP).await["owner_result"]["cleaned"],
        true
    );
    assert!(fixture.runtime.journal().pending().unwrap().is_empty());
    fixture.assert_server_cannot_resolve_workspace();
}

#[tokio::test]
async fn remote_cleanup_retry_acknowledges_after_the_cleaned_state_commits() {
    let fixture = Fixture::new("forge-workspace-cleanup-retained-ack").await;
    // Lose the unsolicited cleanup notification while retaining RPC replies,
    // so the test can stop between the state commit and journal acknowledgement.
    let (outbound, _notifications) = mpsc::unbounded_channel();
    fixture.runtime.attach(outbound);
    let mut transaction = db::begin_immediate(fixture.harness.state.db.pool())
        .await
        .unwrap();
    let version: i64 = sqlx::query_scalar("SELECT version FROM workspace_placement WHERE id = ?")
        .bind(&fixture.resolved.placement.id)
        .fetch_one(&mut *transaction)
        .await
        .unwrap();
    let updated = sqlx::query("UPDATE workspace_placement SET state = 'cleaning', version = version + 1 WHERE id = ? AND version = ? AND state = 'ready'")
        .bind(&fixture.resolved.placement.id)
        .bind(version)
        .execute(&mut *transaction)
        .await
        .unwrap();
    assert_eq!(updated.rows_affected(), 1);
    transaction.commit().await.unwrap();
    let placement = WorkspacePlacementRepo::get_by_id(
        &*fixture.harness.state.db,
        &fixture.resolved.placement.id,
    )
    .await
    .unwrap()
    .unwrap();
    // The backend retains removal without acknowledging it until the server
    // commits the cleaned state. Stop at that boundary to model interruption.
    fixture.resolved.backend.cleanup(&placement).await.unwrap();
    assert!(!fixture.runtime.journal().pending().unwrap().is_empty());
    let updated = sqlx::query("UPDATE workspace_placement SET state = 'cleaned', version = version + 1 WHERE id = ? AND version = ? AND state = 'cleaning'")
        .bind(&placement.id)
        .bind(placement.version)
        .execute(fixture.harness.state.db.pool())
        .await
        .unwrap();
    assert_eq!(updated.rows_affected(), 1);
    WorkspaceRepo::update_status(
        &*fixture.harness.state.db,
        &placement.workspace_id,
        WorkspaceStatus::Cleaned,
        None,
        &db::now_rfc3339(),
    )
    .await
    .unwrap();
    fixture
        .harness
        .state
        .cleanup_scheduler
        .cleanup_now(&placement.workspace_id)
        .await
        .unwrap();
    assert!(fixture.runtime.journal().pending().unwrap().is_empty());
    assert_eq!(
        fixture
            .link
            .as_ref()
            .unwrap()
            .requests(METHOD_WORKSPACE_CLEANUP)
            .len(),
        1,
        "acknowledgement retry does not remove the workspace again"
    );
}

#[tokio::test]
async fn remote_interrupted_run_is_settled_and_error_acknowledged_without_rerun() {
    let fixture = Fixture::new("forge-workspace-interrupted-run").await;
    fixture
        .link
        .as_ref()
        .unwrap()
        .interrupt
        .lock()
        .unwrap()
        .insert(METHOD_WORKSPACE_RUN.into());
    let result = fixture
        .resolved
        .backend
        .run(
            &fixture.resolved.placement,
            &RunSpec {
                purpose: WorkspaceRunPurpose::CiStep,
                command: "printf duplicate >> never-run".into(),
                env: BTreeMap::new(),
                timeout_secs: 0,
                max_output_bytes: usize::MAX,
            },
        )
        .await;
    assert!(matches!(
        result,
        Err(WorkspaceBackendError::OwnerUnreachable { .. })
    ));
    let receipt = fixture.receipt(METHOD_WORKSPACE_RUN).await;
    assert_eq!(receipt["metadata"]["status"], "error");
    assert_eq!(receipt["owner_result"]["error"]["code"], DAEMON_UNAVAILABLE);
    assert!(fixture.runtime.journal().pending().unwrap().is_empty());
    assert_eq!(fixture.run("test ! -e never-run").await.exit_code, 0);
    let requests = fixture
        .link
        .as_ref()
        .unwrap()
        .requests(METHOD_WORKSPACE_RUN);
    assert_eq!(
        requests
            .iter()
            .filter(|request| request["command"] == "printf duplicate >> never-run")
            .count(),
        1
    );
    fixture.assert_server_cannot_resolve_workspace();
}

#[tokio::test]
async fn remote_run_policy_error_is_durably_recorded_and_acknowledged() {
    let fixture = Fixture::with_policy(
        "forge-workspace-run-policy",
        WorkspaceRunPolicy {
            allowed_purposes: vec![WorkspaceRunPurpose::CiStep],
        },
    )
    .await;
    let result = fixture
        .resolved
        .backend
        .run(
            &fixture.resolved.placement,
            &RunSpec {
                purpose: WorkspaceRunPurpose::Hook,
                command: "printf should-not-run".into(),
                env: BTreeMap::new(),
                timeout_secs: 2,
                max_output_bytes: 1024,
            },
        )
        .await;
    assert!(matches!(
        result,
        Err(WorkspaceBackendError::PurposeDenied {
            purpose: WorkspaceRunPurpose::Hook
        })
    ));
    let receipt = fixture.receipt(METHOD_WORKSPACE_RUN).await;
    assert_eq!(receipt["metadata"]["status"], "error");
    assert_eq!(receipt["owner_result"]["error"]["code"], PURPOSE_DENIED);
    assert_eq!(
        fixture
            .link
            .as_ref()
            .unwrap()
            .requests(METHOD_WORKSPACE_RUN)
            .len(),
        1
    );
    assert!(fixture.runtime.journal().pending().unwrap().is_empty());
}

#[tokio::test]
async fn remote_review_disconnect_does_not_freeze_a_failed_assessment() {
    use db::ReviewConformanceRepo;
    let mut fixture = Fixture::new("forge-workspace-review-disconnect").await;
    fixture.candidate().await;
    let (_review, auditor) = fixture.reviewer_attempt().await;
    let auditor_id = auditor.id;
    review::contract::admit(
        &fixture.harness.state.db,
        &auditor_id,
        &fixture.resolved.placement.task_id,
        &fixture.resolved,
    )
    .await
    .unwrap();
    fixture.link.take();
    common::fake_daemon::wait_until_disconnected(&fixture.harness.state, &fixture.daemon_id).await;
    let result = review::contract::evaluate(
        &fixture.harness.state.db,
        &auditor_id,
        &fixture.resolved,
        "The candidate is implemented.\n\n{\"result\":\"pass\",\"reason\":\"verified\"}",
    )
    .await;
    let reason = result.err().unwrap();
    assert!(matches!(
        fixture.resolved.infrastructure_error(&reason),
        Some(review::ReviewError::OwnerUnavailable { .. })
    ));
    assert!(fixture
        .harness
        .state
        .db
        .review_conformance(&auditor_id)
        .await
        .unwrap()
        .is_none());
    fixture.assert_server_cannot_resolve_workspace();
}

#[tokio::test]
async fn remote_interrupted_merge_recovers_only_the_exact_candidate() {
    let fixture = Fixture::new("forge-workspace-interrupted-merge").await;
    let candidate = fixture.candidate().await;
    let contract = fixture.approve_candidate().await;
    assert_eq!(
        fixture
            .run(&format!(
                "git -C {} merge --ff-only {}",
                quote(&fixture.checkout.to_string_lossy()),
                quote(&candidate)
            ))
            .await
            .exit_code,
        0
    );
    fixture
        .link
        .as_ref()
        .unwrap()
        .interrupt
        .lock()
        .unwrap()
        .insert(METHOD_WORKSPACE_MERGE.into());
    let outcome = fixture
        .resolved
        .backend
        .merge(
            &fixture.resolved.placement,
            &MergeSpec {
                target_branch: "main".into(),
                expected_target_sha: contract.base_sha.clone(),
                handed_off_paths: vec![],
            },
        )
        .await
        .unwrap();
    assert_eq!(
        outcome,
        MergeOutcome::Done {
            before_sha: contract.base_sha,
            after_sha: candidate.clone(),
            branch: "main".into()
        }
    );
    assert_eq!(
        fixture.receipt(METHOD_WORKSPACE_MERGE).await["owner_result"]["diffstat"]["files_changed"],
        1
    );
    assert!(fixture.runtime.journal().pending().unwrap().is_empty());
    assert_eq!(
        fixture
            .link
            .as_ref()
            .unwrap()
            .requests(METHOD_WORKSPACE_MERGE)
            .len(),
        1
    );
    fixture.assert_server_cannot_resolve_workspace();
}

#[tokio::test]
async fn remote_lost_run_result_reconciles_from_the_real_daemon_journal() {
    let mut fixture = Fixture::new("forge-workspace-lost-reply").await;
    fixture
        .link
        .as_ref()
        .unwrap()
        .drop_replies
        .lock()
        .unwrap()
        .insert(METHOD_WORKSPACE_RUN.into());
    let resolved = fixture.resolved.clone();
    let running = tokio::spawn(async move {
        resolved
            .backend
            .run(
                &resolved.placement,
                &RunSpec {
                    purpose: WorkspaceRunPurpose::CiStep,
                    command: "printf once >> run-count; printf retained".into(),
                    env: BTreeMap::new(),
                    timeout_secs: 0,
                    max_output_bytes: usize::MAX,
                },
            )
            .await
    });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if fixture.runtime.journal().pending().unwrap().iter().any(|entry| matches!(entry,
            JournalEntry::Operation { operation } if operation.method == METHOD_WORKSPACE_RUN && operation.outcome.is_some())) { break }
        assert!(
            tokio::time::Instant::now() < deadline,
            "run result reaches the owner journal"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    fixture.link.take();
    common::fake_daemon::wait_until_disconnected(&fixture.harness.state, &fixture.daemon_id).await;
    assert!(matches!(
        running.await.unwrap(),
        Err(WorkspaceBackendError::OwnerUnreachable { .. })
    ));
    fixture.reconnect().await;
    let client = fixture.resolved.backend.daemon_client().unwrap();
    client
        .reconcile_pending_operations(&fixture.daemon_id)
        .await
        .unwrap();
    client
        .retry_acknowledgements(&fixture.daemon_id)
        .await
        .unwrap();
    assert_eq!(
        fixture.receipt(METHOD_WORKSPACE_RUN).await["owner_result"]["stdout"],
        "retained"
    );
    assert!(fixture.runtime.journal().pending().unwrap().is_empty());
    assert_eq!(fixture.run("cat run-count").await.stdout_tail, "once");
    fixture.assert_server_cannot_resolve_workspace();
}
