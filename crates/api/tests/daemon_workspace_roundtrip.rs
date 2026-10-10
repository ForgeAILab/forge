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
    UpdateWorkspacePlacement, WorkspacePlacementRepo, WorkspaceRepo, WorkspaceStatus,
};
use forge_client::{
    daemon_persistence::{JournalEntry, JournalOperation, MAX_CI_LOG_BYTES},
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
                                effect_started: true,
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

    fn last_request(&self) -> Option<(String, Value)> {
        self.requests.lock().unwrap().last().cloned()
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

    async fn mark_task_done(&self) {
        let task = TaskRepo::get_by_id(
            &*self.harness.state.db,
            &self.resolved.placement.task_id,
            false,
        )
        .await
        .expect("cleanup Task loads")
        .expect("cleanup Task exists");
        let task = TaskRepo::update_status(
            &*self.harness.state.db,
            db::UpdateTaskStatus {
                id: task.id,
                expected_version: task.version,
                status: "done".into(),
                assignee_id: None,
                error_annotation: None,
                blocked_json: None,
                failed_json: None,
                updated_at: db::now_rfc3339(),
            },
        )
        .await
        .expect("cleanup Task becomes terminal");
        assert_eq!(task.status, "done");
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

    async fn wait_until_journal_empty(&self) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let pending = self.runtime.journal().pending().unwrap();
            if pending.is_empty() {
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "owner journal acknowledgement arrives: pending_count={}, pending_entry_ids={:?}, last_server_frame={:?}",
                    pending.len(),
                    pending.iter().map(JournalEntry::entry_id).collect::<Vec<_>>(),
                    self.link.as_ref().and_then(DaemonLink::last_request),
                );
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
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
            owner_id: Some(common::fake_daemon::FAKE_DAEMON_USER_ID.into()),
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
    // Dispatch stamps the Project revision it ran under; terminal effects from
    // a superseded revision are ignored.
    let project_version: i64 = sqlx::query_scalar(
        "SELECT version FROM project WHERE id = (SELECT project_id FROM task WHERE id = ?)",
    )
    .bind(&placement.task_id)
    .fetch_one(database.pool())
    .await
    .unwrap();
    input.executor_config_snapshot_json = Some(
        json!({"executor_type": "shell", "config": {},
        "placement_id": placement.id, "project_version": project_version})
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
    // Dispatch freezes the candidate pricing selections before it sends
    // `execution.start`; the owner's terminal usage report settles against them.
    assert!(fixture
        .harness
        .state
        .task_service
        .admit_execution_for_test(&execution_id)
        .await
        .unwrap());
    let (workspace_path, _) = fixture.resolved.owner_paths().await.unwrap();
    fixture.runtime.start(ExecutionStartParams {
            plan_text: None,
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
        let observed_placement = WorkspacePlacementRepo::get_by_id(&*database, &placement.id)
            .await
            .unwrap()
            .unwrap();
        let observed_task = TaskRepo::get_by_id(&*database, &placement.task_id, false)
            .await
            .unwrap()
            .unwrap();
        let observed_execution = ExecutionRepo::get_by_id(&*database, &execution_id)
            .await
            .unwrap()
            .unwrap();
        let pending = fixture.runtime.journal().pending().unwrap();
        if observed_placement.state == PlacementState::Ready
            && observed_task.status == "review"
            && pending.is_empty()
        {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "retained terminal reconciles and advances the Task to Review: placement_state={:?}, placement_version={}, task_status={}, task_version={}, execution_status={:?}, execution_version={}, pending_journal_count={}, pending_entry_ids={:?}, last_server_frame={:?}",
                observed_placement.state,
                observed_placement.version,
                observed_task.status,
                observed_task.version,
                observed_execution.status,
                observed_execution.execution_version,
                pending.len(),
                pending.iter().map(JournalEntry::entry_id).collect::<Vec<_>>(),
                fixture.link.as_ref().and_then(DaemonLink::last_request),
            );
        }
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
    assert_eq!(output.stdout.len(), MAX_CI_LOG_BYTES);
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
        recheck_interval_seconds: 600,
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
    fixture.wait_until_journal_empty().await;
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
    fixture.wait_until_journal_empty().await;
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
    fixture.wait_until_journal_empty().await;
    fixture.assert_server_cannot_resolve_workspace();
}

// Snapshot every table, including rows the effect must leave alone. SQLite's
// quote() preserves NULL/blob/text distinctions; sorting removes row order.
async fn table_digests(db: &db::SqliteDb) -> BTreeMap<String, String> {
    use sha2::{Digest, Sha256};
    let tables: Vec<String> =
        sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .fetch_all(db.pool())
            .await
            .unwrap();
    let mut digests = BTreeMap::new();
    for table in tables {
        let columns: Vec<String> = sqlx::query_scalar("SELECT name FROM pragma_table_info(?)")
            .bind(&table)
            .fetch_all(db.pool())
            .await
            .unwrap();
        let expression = columns
            .iter()
            .map(|column| format!("quote(\"{}\")", column.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(" || '|' || ");
        async fn digest(db: &db::SqliteDb, table: &str, expression: &str) -> String {
            let mut rows: Vec<String> = sqlx::query_scalar(&format!(
                "SELECT {expression} FROM \"{}\"",
                table.replace('"', "\"\""),
            ))
            .fetch_all(db.pool())
            .await
            .unwrap();
            rows.sort();
            hex::encode(Sha256::digest(serde_json::to_vec(&rows).unwrap()))
        }
        digests.insert(table.clone(), digest(db, &table, &expression).await);
        let volatile: &[&str] = match table.as_str() {
            "execution" => &["before_sha", "after_sha", "updated_at"],
            "project" => &["list_revision"],
            "usage_ledger_revision" => &["execution_revision"],
            _ => &[],
        };
        if !volatile.is_empty() {
            let stable = columns
                .iter()
                .filter(|column| !volatile.contains(&column.as_str()))
                .map(|column| format!("quote(\"{}\")", column.replace('"', "\"\"")))
                .collect::<Vec<_>>()
                .join(" || '|' || ");
            digests.insert(
                format!("{table}::stable"),
                digest(db, &table, &stable).await,
            );
        }
        if let Some(column) = match table.as_str() {
            "project" => Some("list_revision"),
            "usage_ledger_revision" => Some("execution_revision"),
            _ => None,
        } {
            let revision: i64 = sqlx::query_scalar(&format!("SELECT {column} FROM {table}"))
                .fetch_one(db.pool())
                .await
                .unwrap();
            digests.insert(format!("{table}::{column}"), revision.to_string());
        }
    }
    digests
}

// These backend effects leave Task-step projections to their caller.
async fn task_projection(fixture: &Fixture) -> (String, Vec<(String, String)>, i64) {
    let db = &fixture.harness.state.db;
    let id = &fixture.resolved.placement.task_id;
    let task: String = sqlx::query_scalar("SELECT status || '|' || version || '|' || status_epoch || '|' || COALESCE(review_passed_at,'') || '|' || condition_json FROM task WHERE id=?")
        .bind(id).fetch_one(db.pool()).await.unwrap();
    let reviews =
        sqlx::query_as("SELECT status, step_results_json FROM review WHERE task_id=? ORDER BY id")
            .bind(id)
            .fetch_all(db.pool())
            .await
            .unwrap();
    let comments = sqlx::query_scalar("SELECT COUNT(*) FROM task_comment WHERE task_id=?")
        .bind(id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    (task, reviews, comments)
}

#[tokio::test]
async fn remote_ci_pass_and_failure_record_reviews_and_events() {
    use services::workflow::{default_workflow, HookContext, HookResult};
    // Review-entry CI of a daemon-placed Task is a check the durable runner
    // executes on that daemon (legacy policy: always runs, never reused). A
    // run the daemon cannot execute is no verdict and parks the Task on the
    // typed check condition; that path is covered by the check-runner tests.
    for case in ["pass", "fail"] {
        let fixture = Fixture::new("forge-ci-characterization").await;
        let state = &fixture.harness.state;
        let task_id = &fixture.resolved.placement.task_id;
        let task = TaskRepo::get_by_id(&*state.db, task_id, false)
            .await
            .unwrap()
            .unwrap();
        let project = ProjectRepo::get_by_id(&*state.db, &task.project_id)
            .await
            .unwrap()
            .unwrap();
        let mut events = state.event_bus.subscribe();
        let command = if case == "fail" {
            "printf failure; printf diagnostic >&2; exit 7"
        } else {
            "printf first"
        };
        let ctx = HookContext {
            task_id: task_id.clone(),
            project_id: task.project_id.clone(),
            from_state: "in_progress".into(),
            to_state: "review".into(),
            db: Arc::clone(&state.db),
            event_bus: Arc::clone(&state.event_bus),
            gate_config: None,
            workflow: Arc::new(default_workflow::default_workflow()),
            project_version: Some(project.version),
            project_workflow_definition: Some(project.workflow_definition),
            triggered_by: Actor::system(SystemComponent::Test),
            review_runner: None,
            merge_service: None,
            cleanup_scheduler: None,
            task_service: state.task_service.as_ref().clone(),
            daemon_connections: None,
            workspace_exec_locks: None,
            terminal_activity: None,
            workspace_root: fixture.server_root.path().join("workspaces"),
            repo_cache_locks: None,
            workspace_backend_router: Arc::clone(&state.workspace_backend_router),
            workspace_id: Some(fixture.resolved.placement.workspace_id.clone()),
            agent_id: None,
            execution_id: Some(fixture.execution_id.clone()),
            state_config: json!({"ci_steps": [command, "printf second"]}),
        };
        let worker = state
            .task_service
            .check_worker()
            .expect("the runtime composes the check worker");
        let result = services::workflow::actions::run_ci_steps_in_step(&ctx, &worker).await;
        let reviews = ReviewRepo::list_by_task(&*state.db, task_id).await.unwrap();
        assert_eq!(reviews.len(), 1, "{case}: {result:?}");
        let details: Value = serde_json::from_str(&reviews[0].step_results_json).unwrap();
        let task = TaskRepo::get_by_id(&*state.db, task_id, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.status, "review");
        match case {
            "pass" => {
                assert!(matches!(result, HookResult::Ok));
                assert_eq!(reviews[0].status, ReviewStatus::Passed);
                assert_eq!(details["ci_steps"].as_array().unwrap().len(), 2);
                assert!(task.review_passed_at.is_some());
                assert_eq!(details["ci_steps"][1]["output_tail"], "second");
            }
            "fail" => {
                assert!(matches!(result, HookResult::Failed { .. }));
                assert_eq!(reviews[0].status, ReviewStatus::Failed);
                assert_eq!(details["ci_steps"].as_array().unwrap().len(), 1);
                assert_eq!(details["ci_steps"][0]["exit_code"], 7);
                assert_eq!(details["ci_steps"][0]["output_tail"], "failure\ndiagnostic");
                assert!(task.review_passed_at.is_none());
            }
            _ => unreachable!(),
        }
        // The whole entry is ONE `check.run` on the owning daemon under the
        // daemon's frozen policy (always runs, never reused), bounded by an
        // absolute deadline; no command travels as `workspace.run` any more.
        let link = fixture.link.as_ref().unwrap();
        let runs = link.requests(api_types::METHOD_CHECK_RUN);
        assert_eq!(runs.len(), 1, "{case}: one check.run per entry");
        assert_eq!(
            runs[0]["purpose"],
            serde_json::to_value(api_types::WorkspaceRunPurpose::CiStep).unwrap()
        );
        assert_eq!(runs[0]["spec"]["execution_policy"], "legacy-daemon/1");
        assert_eq!(
            runs[0]["spec"]["commands"].as_array().unwrap().len(),
            2,
            "{case}: every configured step is in the spec"
        );
        assert!(runs[0]["deadline"].is_string());
        assert!(
            runs[0]["target"]
                .to_string()
                .contains(&fixture.resolved.placement.id),
            "{case}: the run targets the Task's own placement"
        );
        assert!(link.requests(api_types::METHOD_WORKSPACE_RUN).is_empty());
        let comments: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_comment WHERE task_id=?")
            .bind(task_id)
            .fetch_one(state.db.pool())
            .await
            .unwrap();
        assert_eq!(comments, 0);
        let mut review_events = Vec::new();
        while let Ok(event) = events.try_recv() {
            if event.event_type.starts_with("review.") {
                review_events.push(event.event_type);
            }
        }
        assert_eq!(
            review_events,
            match case {
                "pass" => vec!["review.passed"],
                _ => vec!["review.failed"],
            }
        );
        assert_eq!(
            fixture
                .resolved
                .git_query(
                    WorkspaceGitQuery::TargetHead {
                        branch: "main".into()
                    },
                    false
                )
                .await
                .unwrap()
                .unwrap()
                .trim(),
            fixture.base_sha
        );
        fixture.wait_until_journal_empty().await;
    }
}

#[tokio::test]
async fn remote_exact_object_mismatch_retains_refusal_without_execution_success() {
    let fixture = Fixture::new("forge-workspace-exact-object-mismatch").await;
    let candidate = fixture.candidate().await;
    let contract = fixture.approve_candidate().await;
    let hooks = fixture.checkout.join(".git").join("effect-hooks");
    let hook = hooks.join("post-merge");
    let script = "#!/bin/sh\ngit -c core.hooksPath=/dev/null commit --allow-empty -m hook-moved\n";
    assert_eq!(
        fixture
            .run(&format!(
                "mkdir -p {}; printf %s {} > {}; chmod +x {}; git -C {} config core.hooksPath {}",
                quote(&hooks.to_string_lossy()),
                quote(script),
                quote(&hook.to_string_lossy()),
                quote(&hook.to_string_lossy()),
                quote(&fixture.checkout.to_string_lossy()),
                quote(&hooks.to_string_lossy())
            ))
            .await
            .exit_code,
        0
    );
    let before = task_projection(&fixture).await;
    let outcome = fixture
        .resolved
        .backend
        .merge(
            &fixture.resolved.placement,
            &MergeSpec {
                target_branch: "main".into(),
                expected_target_sha: contract.base_sha,
                handed_off_paths: vec![],
            },
        )
        .await
        .unwrap();
    assert_eq!(
        outcome,
        MergeOutcome::TargetMoved {
            reason: "integration target changed during merge; reviewed content was not integrated"
                .into(),
            target_branch: "main".into(),
        }
    );
    assert_eq!(before, task_projection(&fixture).await);
    let execution = ExecutionRepo::get_by_id(&*fixture.harness.state.db, &fixture.execution_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(execution.after_sha, None);
    assert_ne!(
        fixture
            .resolved
            .git_query(
                WorkspaceGitQuery::TargetHead {
                    branch: "main".into()
                },
                false
            )
            .await
            .unwrap()
            .unwrap()
            .trim(),
        candidate
    );
    assert_eq!(
        fixture.receipt(METHOD_WORKSPACE_MERGE).await["owner_result"]["outcome"]["kind"],
        "target_moved"
    );
    fixture.wait_until_journal_empty().await;
}

#[tokio::test]
async fn remote_target_moves_after_rebase_and_review_before_fast_forward() {
    let fixture = Fixture::new("forge-workspace-target-moves-after-rebase").await;
    fixture.candidate().await;
    let target = quote(&fixture.checkout.to_string_lossy());
    assert_eq!(
        fixture
            .run(&format!(
                "git -C {target} commit --allow-empty -m first-target-move"
            ))
            .await
            .exit_code,
        0
    );
    assert!(matches!(
        fixture.resolved.rebase_target("main", true).await.unwrap(),
        WorkspaceOwnerOperationOutcome::Rebased
    ));
    let contract = fixture.approve_candidate().await;
    assert_eq!(
        fixture
            .run(&format!(
                "git -C {target} commit --allow-empty -m second-target-move"
            ))
            .await
            .exit_code,
        0
    );
    let before = task_projection(&fixture).await;
    let outcome = fixture
        .resolved
        .backend
        .merge(
            &fixture.resolved.placement,
            &MergeSpec {
                target_branch: "main".into(),
                expected_target_sha: contract.base_sha,
                handed_off_paths: vec![],
            },
        )
        .await
        .unwrap();
    assert!(matches!(outcome, MergeOutcome::TargetMoved { .. }));
    assert_eq!(before, task_projection(&fixture).await);
    let execution = ExecutionRepo::get_by_id(&*fixture.harness.state.db, &fixture.execution_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(execution.after_sha, None);
    assert_eq!(
        fixture.receipt(METHOD_WORKSPACE_MERGE).await["owner_result"]["outcome"]["kind"],
        "target_moved"
    );
    fixture.wait_until_journal_empty().await;
}

#[tokio::test]
async fn remote_dirty_task_and_target_retain_receipts_without_task_projection() {
    for target_dirty in [false, true] {
        let fixture = Fixture::new("forge-workspace-dirty-characterization").await;
        let candidate = fixture.candidate().await;
        let command = if target_dirty {
            format!(
                "printf dirty > {}/README.md",
                quote(&fixture.checkout.to_string_lossy())
            )
        } else {
            "printf dirty > feature.txt".into()
        };
        assert_eq!(fixture.run(&command).await.exit_code, 0);
        let before = task_projection(&fixture).await;
        let outcome = fixture
            .resolved
            .backend
            .merge(
                &fixture.resolved.placement,
                &MergeSpec {
                    target_branch: "main".into(),
                    expected_target_sha: fixture.base_sha.clone(),
                    handed_off_paths: vec![],
                },
            )
            .await
            .unwrap();
        if target_dirty {
            assert!(matches!(outcome, MergeOutcome::TargetDirty { .. }));
        } else {
            assert_eq!(
                outcome,
                MergeOutcome::Dirty {
                    files: vec!["feature.txt".into()]
                }
            );
        }
        let receipt = fixture.receipt(METHOD_WORKSPACE_MERGE).await;
        assert_eq!(receipt["metadata"]["status"], "result");
        assert_eq!(before, task_projection(&fixture).await);
        let execution = ExecutionRepo::get_by_id(&*fixture.harness.state.db, &fixture.execution_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(execution.after_sha, None);
        assert_eq!(
            fixture
                .resolved
                .git_query(WorkspaceGitQuery::Head, false)
                .await
                .unwrap()
                .unwrap()
                .trim(),
            candidate
        );
        assert_eq!(
            fixture
                .resolved
                .git_query(
                    WorkspaceGitQuery::TargetHead {
                        branch: "main".into()
                    },
                    false
                )
                .await
                .unwrap()
                .unwrap()
                .trim(),
            fixture.base_sha
        );
        fixture.wait_until_journal_empty().await;
    }
}

#[tokio::test]
async fn remote_rebase_clean_conflict_and_interrupted_recovery_preserve_projection() {
    for case in ["clean", "conflict", "restart", "abort"] {
        let mut fixture = Fixture::new("forge-workspace-rebase-characterization").await;
        fixture.candidate().await;
        let target = quote(&fixture.checkout.to_string_lossy());
        let file = if case == "clean" {
            "sibling.txt"
        } else {
            "feature.txt"
        };
        assert_eq!(fixture.run(&format!("printf 'target\\n' > {target}/{file}; git -C {target} add {file}; git -C {target} commit -m target")).await.exit_code, 0);
        if matches!(case, "restart" | "abort") {
            assert_ne!(fixture.run("git rebase main").await.exit_code, 0);
        }
        if case == "restart" {
            fixture.wait_until_journal_empty().await;
            fixture.link.take();
            common::fake_daemon::wait_until_disconnected(
                &fixture.harness.state,
                &fixture.daemon_id,
            )
            .await;
            let (outbound, _discarded) = mpsc::unbounded_channel();
            fixture.runtime = DaemonRuntime::new_owned(
                outbound,
                fixture._daemon_root.path().to_path_buf(),
                ActiveExecutionTracker::default(),
                fixture.daemon_id.clone(),
                WorkspaceRunPolicy {
                    allowed_purposes: vec![
                        WorkspaceRunPurpose::CiStep,
                        WorkspaceRunPurpose::Hook,
                        WorkspaceRunPurpose::EnvironmentSetup,
                    ],
                },
            )
            .unwrap();
            fixture.reconnect().await;
        }
        let before = task_projection(&fixture).await;
        let result = fixture
            .resolved
            .rebase_target("main", case != "abort")
            .await
            .unwrap();
        match case {
            "clean" => assert!(matches!(result, WorkspaceOwnerOperationOutcome::Rebased)),
            "abort" => assert!(
                matches!(result, api_types::WorkspaceOwnerOperationOutcome::Conflict { details, conflict_paths } if details == "aborted interrupted rebase" && conflict_paths.is_empty())
            ),
            _ => assert!(
                matches!(result, WorkspaceOwnerOperationOutcome::Conflict { conflict_paths, .. } if conflict_paths == vec!["feature.txt"])
            ),
        }
        assert_eq!(before, task_projection(&fixture).await);
        assert_eq!(
            fixture
                .resolved
                .git_query(WorkspaceGitQuery::RebaseInProgress, false)
                .await
                .unwrap()
                .unwrap()
                .trim(),
            "false"
        );
        fixture.wait_until_journal_empty().await;
        if case == "clean" {
            assert!(matches!(
                fixture
                    .harness
                    .state
                    .merge_service
                    .merge(&fixture.resolved.placement.task_id)
                    .await
                    .unwrap(),
                MergeOutcome::Done { .. }
            ));
        }
    }
}

#[tokio::test]
async fn remote_lost_merge_reply_reconnect_commits_evidence_before_ack_without_rerun() {
    let mut fixture = Fixture::new("forge-workspace-lost-merge-reply").await;
    let candidate = fixture.candidate().await;
    fixture.approve_candidate().await;
    let before = task_projection(&fixture).await;
    fixture
        .link
        .as_ref()
        .unwrap()
        .drop_replies
        .lock()
        .unwrap()
        .insert(METHOD_WORKSPACE_MERGE.into());
    let resolved = fixture.resolved.clone();
    let target = fixture.base_sha.clone();
    let running = tokio::spawn(async move {
        resolved
            .backend
            .merge(
                &resolved.placement,
                &MergeSpec {
                    target_branch: "main".into(),
                    expected_target_sha: target,
                    handed_off_paths: vec![],
                },
            )
            .await
    });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if fixture.runtime.journal().pending().unwrap().iter().any(|entry| matches!(entry,
            JournalEntry::Operation { operation } if operation.method == METHOD_WORKSPACE_MERGE && operation.outcome.is_some())) { break }
        assert!(
            tokio::time::Instant::now() < deadline,
            "merge reaches the owner journal"
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
    let execution = ExecutionRepo::get_by_id(&*fixture.harness.state.db, &fixture.execution_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(execution.before_sha.as_deref(), Some(candidate.as_str()));
    assert_eq!(execution.after_sha.as_deref(), Some(candidate.as_str()));
    assert_eq!(
        fixture.receipt(METHOD_WORKSPACE_MERGE).await["owner_result"]["outcome"]["after_sha"],
        candidate
    );
    assert_eq!(before, task_projection(&fixture).await);
    fixture.wait_until_journal_empty().await;
    assert!(fixture
        .link
        .as_ref()
        .unwrap()
        .requests(METHOD_WORKSPACE_MERGE)
        .is_empty());
}

#[tokio::test]
async fn remote_cleanup_stays_cleaning_offline_until_owner_acknowledges() {
    let mut fixture = Fixture::new("forge-workspace-cleanup").await;
    fixture.mark_task_done().await;
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
    let offline_diagnostic = format!(
        "placement_state={:?}, workspace_status={:?}, pending_journal_count={}, last_server_frame={:?}",
        placement.state,
        workspace.status,
        fixture.runtime.journal().pending().unwrap().len(),
        fixture.link.as_ref().and_then(DaemonLink::last_request),
    );
    assert_eq!(
        placement.state,
        PlacementState::Cleaning,
        "offline cleanup remains pending: {offline_diagnostic}"
    );
    assert_eq!(
        workspace.status,
        WorkspaceStatus::Cleaning,
        "offline cleanup preserves workspace state: {offline_diagnostic}"
    );
    fixture.reconnect().await;
    // The offline attempt left the scheduler's one-tick retry deadline on the
    // workspace, and cleanup retains that backoff. Elapse it as the next
    // scheduler tick would see it, so this retry reaches the returned owner.
    assert!(workspace
        .cleanup_after
        .as_deref()
        .and_then(|deadline| chrono::DateTime::parse_from_rfc3339(deadline).ok())
        .is_some_and(|deadline| deadline > chrono::Utc::now()));
    fixture
        .harness
        .state
        .cleanup_scheduler
        .schedule(&placement.workspace_id, Duration::ZERO)
        .await
        .unwrap();
    let cleanup_result = fixture
        .harness
        .state
        .cleanup_scheduler
        .cleanup_now(&placement.workspace_id)
        .await;
    if let Err(error) = cleanup_result {
        let observed_placement =
            WorkspacePlacementRepo::get_by_id(&*fixture.harness.state.db, &placement.id)
                .await
                .unwrap();
        let observed_workspace =
            WorkspaceRepo::get_by_id(&*fixture.harness.state.db, &placement.workspace_id)
                .await
                .unwrap();
        panic!(
            "owner cleanup acknowledgement commits: error={error:?}, placement={observed_placement:?}, workspace={observed_workspace:?}, pending_journal_count={}, last_server_frame={:?}",
            fixture.runtime.journal().pending().unwrap().len(),
            fixture.link.as_ref().and_then(DaemonLink::last_request),
        );
    }
    let placement = WorkspacePlacementRepo::get_by_id(&*fixture.harness.state.db, &placement.id)
        .await
        .unwrap()
        .unwrap();
    let workspace = WorkspaceRepo::get_by_id(&*fixture.harness.state.db, &placement.workspace_id)
        .await
        .unwrap()
        .unwrap();
    let settled_diagnostic = format!(
        "placement_state={:?}, workspace_status={:?}, pending_journal_count={}, last_server_frame={:?}",
        placement.state,
        workspace.status,
        fixture.runtime.journal().pending().unwrap().len(),
        fixture.link.as_ref().and_then(DaemonLink::last_request),
    );
    assert_eq!(
        placement.state,
        PlacementState::Cleaned,
        "owner acknowledgement cleans placement: {settled_diagnostic}"
    );
    assert_eq!(
        workspace.status,
        WorkspaceStatus::Cleaned,
        "owner acknowledgement cleans workspace: {settled_diagnostic}"
    );
    assert_eq!(
        fixture.receipt(METHOD_WORKSPACE_CLEANUP).await["owner_result"]["cleaned"],
        true
    );
    fixture.wait_until_journal_empty().await;
    fixture.assert_server_cannot_resolve_workspace();
}

#[tokio::test]
async fn remote_cleanup_retry_acknowledges_after_the_cleaned_state_commits() {
    let fixture = Fixture::new("forge-workspace-cleanup-retained-ack").await;
    fixture.mark_task_done().await;
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
    fixture.wait_until_journal_empty().await;
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
    fixture.wait_until_journal_empty().await;
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
    fixture.wait_until_journal_empty().await;
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
    fixture.wait_until_journal_empty().await;
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
    fixture.wait_until_journal_empty().await;
    assert_eq!(fixture.run("cat run-count").await.stdout_tail, "once");
    fixture.assert_server_cannot_resolve_workspace();
}

#[tokio::test]
async fn remote_plan_owner_publication_restore_and_discard_are_fenced() {
    let fixture = Fixture::new("forge-owner-plan-publication").await;
    let placement = &fixture.resolved.placement;
    let client = fixture.resolved.backend.daemon_client().unwrap();
    let daemon = placement.daemon_id.as_deref().unwrap();
    let params = |operation| WorkspaceOwnerOperationParams {
        fence: WorkspaceMutationFence {
            integration: api_types::WorkspaceIntegrationBinding::TaskStep,
            daemon_id: daemon.into(),
            runtime_id: placement.runtime_id.clone().unwrap(),
            placement_id: placement.id.clone(),
            operation_id: db::new_uuid_v4(),
            generation: placement.generation as u64,
            expected: WorkspaceOperationExpected::BaseSha {
                sha: fixture.base_sha.clone(),
            },
        },
        workspace_handle: placement.workspace_handle.clone().unwrap(),
        operation,
    };
    let initial = params(WorkspaceOwnerOperation::PublishPlan {
        execution_id: "plan-initial".into(),
        content: "- [ ] prior plan\n".into(),
    });
    client.owner_operation(daemon, initial).await.unwrap();
    let publication = params(WorkspaceOwnerOperation::PublishPlan {
        execution_id: "plan-revision".into(),
        content: "- [x] revised plan\n".into(),
    });
    client
        .owner_operation(daemon, publication.clone())
        .await
        .unwrap();
    client
        .owner_operation(daemon, publication.clone())
        .await
        .unwrap();
    assert_eq!(
        fixture
            .resolved
            .backend
            .read(placement, "../plan.md", 1024)
            .await
            .unwrap(),
        b"- [x] revised plan\n"
    );
    let mut stale = publication;
    stale.fence.operation_id = db::new_uuid_v4();
    stale.fence.generation = 0;
    assert!(client.owner_operation(daemon, stale).await.is_err());
    let restore = params(WorkspaceOwnerOperation::RestorePlan {
        execution_id: "plan-revision".into(),
    });
    client
        .owner_operation(daemon, restore.clone())
        .await
        .unwrap();
    client.owner_operation(daemon, restore).await.unwrap();
    assert_eq!(
        fixture
            .resolved
            .backend
            .read(placement, "../plan.md", 1024)
            .await
            .unwrap(),
        b"- [ ] prior plan\n"
    );
    // The next execution can already be active when the previous terminal
    // settlement removes its private plan snapshot and outbox.
    let (path, _) = fixture.resolved.owner_paths().await.unwrap();
    fixture
        .runtime
        .start(ExecutionStartParams {
            task_id: placement.task_id.clone(),
            execution_id: "active-next-turn".into(),
            workspace_path: path.clone(),
            executor_type: "shell".into(),
            executor_config: json!({"executor_type":"shell", "config":{}}),
            prompt: json!({"description":"while [ ! -f finish-next-turn ]; do sleep 0.01; done"}),
            max_turns: None,
            plan_text: None,
        })
        .await
        .unwrap();
    assert!(fixture
        .runtime
        .active_execution_ids()
        .contains(&"active-next-turn".into()));
    client
        .owner_operation(
            daemon,
            params(WorkspaceOwnerOperation::DiscardPlan {
                execution_id: "plan-revision".into(),
            }),
        )
        .await
        .unwrap();
    assert!(fixture
        .resolved
        .backend
        .describe(placement)
        .await
        .unwrap()
        .active_execution_ids
        .contains(&"active-next-turn".into()));
    // End the owner-side fixture turn without depending on cancellation races.
    std::fs::write(PathBuf::from(path).join("finish-next-turn"), "done").unwrap();
    for _ in 0..500 {
        if !fixture
            .resolved
            .backend
            .describe(placement)
            .await
            .unwrap()
            .active_execution_ids
            .contains(&"active-next-turn".into())
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!fixture
        .resolved
        .backend
        .describe(placement)
        .await
        .unwrap()
        .active_execution_ids
        .contains(&"active-next-turn".into()));
    fixture.assert_server_cannot_resolve_workspace();
}

#[tokio::test]
async fn remote_plan_operations_wait_for_workspace_run_and_discard_cleaned_state() {
    let fixture = Fixture::new("forge-plan-command-queue").await;
    let placement = &fixture.resolved.placement;
    let daemon = placement.daemon_id.as_deref().unwrap();
    let client = fixture
        .resolved
        .backend
        .daemon_client()
        .unwrap()
        .clone()
        .with_timeout(Duration::from_millis(10));
    let params = |operation| WorkspaceOwnerOperationParams {
        fence: WorkspaceMutationFence {
            integration: api_types::WorkspaceIntegrationBinding::TaskStep,
            daemon_id: daemon.into(),
            runtime_id: placement.runtime_id.clone().unwrap(),
            placement_id: placement.id.clone(),
            operation_id: db::new_uuid_v4(),
            generation: placement.generation as u64,
            expected: WorkspaceOperationExpected::BaseSha {
                sha: fixture.base_sha.clone(),
            },
        },
        workspace_handle: placement.workspace_handle.clone().unwrap(),
        operation,
    };
    let (path, _) = fixture.resolved.owner_paths().await.unwrap();
    let worktree = PathBuf::from(path);
    for (index, operation) in [
        WorkspaceOwnerOperation::PublishPlan {
            execution_id: "queued-plan".into(),
            content: "- [ ] queued\n".into(),
        },
        WorkspaceOwnerOperation::RestorePlan {
            execution_id: "queued-plan".into(),
        },
    ]
    .into_iter()
    .enumerate()
    {
        let command = format!("touch queue-ready-{index}; while [ ! -e queue-release-{index} ]; do sleep 0.01; done; printf 'owner command finished'");
        let run = fixture.run(&command);
        let settlement = async {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            while !worktree.join(format!("queue-ready-{index}")).exists() {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "owner command acquired the lock"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let request = client.owner_operation(daemon, params(operation));
            tokio::pin!(request);
            let waiting = tokio::time::timeout(Duration::from_millis(50), &mut request).await;
            let blocked = waiting.is_err();
            std::fs::write(worktree.join(format!("queue-release-{index}")), "release").unwrap();
            let result = match waiting {
                Ok(result) => result,
                Err(_) => request.await,
            }
            .unwrap();
            assert!(blocked, "plan RPC waited beyond the ordinary 10ms timeout");
            assert!(matches!(
                result.outcome,
                WorkspaceOwnerOperationOutcome::Applied
            ));
        };
        let (run, ()) = tokio::join!(run, settlement);
        assert_eq!(run.stdout_tail, "owner command finished");
    }
    let cleanup = params(WorkspaceOwnerOperation::DiscardPlan {
        execution_id: "queued-plan".into(),
    });
    let normal = fixture.resolved.backend.daemon_client().unwrap();
    let clean: WorkspaceCleanupResult = normal
        .cleanup(
            daemon,
            WorkspaceCleanupParams {
                fence: cleanup.fence.clone(),
                workspace_handle: cleanup.workspace_handle.clone(),
            },
        )
        .await
        .unwrap();
    assert!(clean.cleaned);
    let discard = || {
        params(WorkspaceOwnerOperation::DiscardPlan {
            execution_id: "queued-plan".into(),
        })
    };
    assert!(matches!(
        client
            .owner_operation(daemon, discard())
            .await
            .unwrap()
            .outcome,
        WorkspaceOwnerOperationOutcome::Applied
    ));
    normal
        .acknowledge(daemon, clean.entry_id.clone())
        .await
        .unwrap();
    normal.retry_acknowledgements(daemon).await.unwrap();
    fixture.wait_until_journal_empty().await;
    assert!(matches!(
        client
            .owner_operation(daemon, discard())
            .await
            .unwrap()
            .outcome,
        WorkspaceOwnerOperationOutcome::Applied
    ));
}

/// What a Cancel or Hold during remote review-entry CI must leave behind:
/// no consumer still waits (a late result is stale), the review attempt the
/// superseded step opened is closed, and the run is settled so it holds no
/// machine slot.
async fn assert_remote_ci_abandoned(db: &db::SqliteDb, task_id: &str) {
    let waiting: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM check_consumer WHERE task_id=? AND cancelled_at IS NULL",
    )
    .bind(task_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(waiting, 0, "no consumer waits for the abandoned run");
    let review: String = sqlx::query_scalar(
        "SELECT status FROM review WHERE task_id=? ORDER BY attempt_number DESC LIMIT 1",
    )
    .bind(task_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(review, "cancelled");
    tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            let live: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM check_run r WHERE r.id IN (SELECT run_id FROM check_consumer WHERE task_id=?) AND r.state NOT IN ('succeeded','failed','cancelled')")
                .bind(task_id)
                .fetch_one(db.pool())
                .await
                .unwrap();
            if live == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the abandoned remote run settles and frees its slot");
    let applied: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM check_consumer WHERE task_id=? AND applied_at IS NOT NULL",
    )
    .bind(task_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(applied, 0, "the late result is applied to nothing");
}

#[tokio::test]
async fn connected_cancel_kills_remote_ci_and_supersedes_its_hook_step() {
    use db::TaskStepRepo;
    let fixture = Fixture::new("forge-remote-ci-cancel").await;
    let db = &fixture.harness.state.db;
    let task_id = fixture.resolved.placement.task_id.clone();
    let started = fixture._daemon_root.path().join("cancel-ci-started");
    let completed = fixture._daemon_root.path().join("cancel-ci-completed");
    sqlx::query("UPDATE task SET task_state_config=?,status='merge_failed' WHERE id=?").bind(json!({"review":{"ci_steps":[format!("touch {}; sleep 120; touch {}",quote(&started.to_string_lossy()),quote(&completed.to_string_lossy()))]}}).to_string()).bind(&task_id).execute(db.pool()).await.unwrap();
    let task = TaskRepo::get_by_id(&**db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    let service = fixture.harness.state.task_service.clone();
    let entry = service
        .transition(
            &task_id,
            "review".to_owned(),
            services::task_service::TransitionOptions {
                bridge: Default::default(),
                version: task.version,
                triggered_by: Actor::system(SystemComponent::Workflow),
                reason: Some("remote CI entry".into()),
                rejection: false,
                defer_dispatch_seconds: None,
            },
        )
        .await
        .unwrap();
    assert!(entry.pending_steps > 0);
    let running = {
        let service = service.clone();
        let id = task_id.clone();
        tokio::spawn(async move { service.drain(&id).await })
    };
    tokio::time::timeout(Duration::from_secs(15), async {
        while !started.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let task = TaskRepo::get_by_id(&**db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    let cancelled = service
        .perform_task_action(
            &task_id,
            TaskAction::Cancel {
                reason: Some("owner cancelled remote CI".into()),
            },
            task.version,
        )
        .await
        .unwrap();
    assert_eq!(cancelled.task.status, "cancelled");
    assert!(!completed.exists());
    assert!(db
        .pending_remote_cancels(None, None)
        .await
        .unwrap()
        .is_empty());
    assert!(db
        .task_steps(&task_id)
        .await
        .unwrap()
        .iter()
        .any(|s| s.kind == "hooks" && s.expected_status == "review" && s.status == "superseded"));
    assert_eq!(
        fixture
            .link
            .as_ref()
            .unwrap()
            .requests(METHOD_WORKSPACE_CANCEL)
            .len(),
        1
    );
    running.await.unwrap().unwrap();
    assert_remote_ci_abandoned(db, &task_id).await;
    assert!(!completed.exists());
    assert_eq!(
        TaskRepo::get_by_id(&**db, &task_id, false)
            .await
            .unwrap()
            .unwrap()
            .status,
        "cancelled"
    );
}

#[tokio::test]
async fn disconnected_cancel_fences_workspace_until_real_owner_reconnect_cleanup() {
    let mut fixture = Fixture::new("forge-disconnected-ci-cancel").await;
    let db = fixture.harness.state.db.clone();
    let task_id = fixture.resolved.placement.task_id.clone();
    // Normal admission records this preparation base before publishing ready.
    sqlx::query("UPDATE workspace SET before_sha=? WHERE id=?")
        .bind(&fixture.base_sha)
        .bind(&fixture.resolved.placement.workspace_id)
        .execute(db.pool())
        .await
        .unwrap();

    let started = fixture._daemon_root.path().join("disconnected-ci-started");
    let completed = fixture
        ._daemon_root
        .path()
        .join("disconnected-ci-completed");
    sqlx::query("UPDATE task SET task_state_config=?,status='merge_failed' WHERE id=?").bind(json!({"review":{"ci_steps":[format!("touch {}; sleep 120; touch {}",quote(&started.to_string_lossy()),quote(&completed.to_string_lossy()))]}}).to_string()).bind(&task_id).execute(db.pool()).await.unwrap();
    let service = fixture.harness.state.task_service.clone();
    let task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    service
        .transition(
            &task_id,
            "review".to_owned(),
            services::task_service::TransitionOptions {
                bridge: Default::default(),
                version: task.version,
                triggered_by: Actor::system(SystemComponent::Workflow),
                reason: Some("remote CI".into()),
                rejection: false,
                defer_dispatch_seconds: None,
            },
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        while !started.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    fixture.link.take();
    fixture
        .harness
        .state
        .daemon_connections
        .unregister(&fixture.daemon_id);
    let task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    let cancelled = service
        .perform_task_action(&task_id, TaskAction::Cancel { reason: None }, task.version)
        .await
        .unwrap();
    assert_eq!(cancelled.task.status, "cancelled");
    assert!(!completed.exists());
    assert!(db.task_has_pending_remote_cancel(&task_id).await.unwrap());
    assert!(!db
        .workspace_remote_cancels(&fixture.resolved.placement.workspace_id, &task_id)
        .await
        .unwrap()
        .is_empty());
    service.drain(&task_id).await.unwrap();
    // A cancelled Task offers nothing; its workspace stays fenced for reuse.
    let task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(service
        .task_action_offers(&task_id, &Actor::user(UserActionSource::Api))
        .await
        .unwrap()
        .available_actions
        .is_empty());
    assert_eq!(task.status, "cancelled");
    fixture.link = Some(
        DaemonLink::connect(
            &fixture.server,
            &fixture.daemon_id,
            &fixture.token,
            fixture.runtime.clone(),
        )
        .await,
    );
    tokio::time::timeout(Duration::from_secs(15), async {
        while db.task_has_pending_remote_cancel(&task_id).await.unwrap() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    service.drain(&task_id).await.unwrap();
    assert!(!completed.exists());
    assert!(!fixture
        .link
        .as_ref()
        .unwrap()
        .requests(METHOD_WORKSPACE_CANCEL)
        .is_empty());
    assert!(db
        .workspace_remote_cancels(&fixture.resolved.placement.workspace_id, &task_id)
        .await
        .unwrap()
        .is_empty());
    // The fence held while the owner was away; once it confirms, nothing of
    // the abandoned run is left: no waiting consumer, no open review
    // attempt, no run holding a slot, and the Task did not move.
    assert_remote_ci_abandoned(&db, &task_id).await;
    assert!(!completed.exists());
    assert_eq!(
        TaskRepo::get_by_id(&*db, &task_id, false)
            .await
            .unwrap()
            .unwrap()
            .status,
        "cancelled"
    );
}

/// Hold with the owner disconnected parks the follow-up action on the
/// unconfirmed remote cleanup, naming the machine, until the owner reconnects.
#[tokio::test]
async fn disconnected_hold_parks_follow_up_on_the_named_machine_until_reconnect() {
    let mut fixture = Fixture::new("forge-disconnected-ci-hold").await;
    let db = fixture.harness.state.db.clone();
    let task_id = fixture.resolved.placement.task_id.clone();
    sqlx::query("UPDATE workspace SET before_sha=? WHERE id=?")
        .bind(&fixture.base_sha)
        .bind(&fixture.resolved.placement.workspace_id)
        .execute(db.pool())
        .await
        .unwrap();
    let started = fixture
        ._daemon_root
        .path()
        .join("disconnected-hold-started");
    let completed = fixture
        ._daemon_root
        .path()
        .join("disconnected-hold-completed");
    sqlx::query("UPDATE task SET task_state_config=?,status='merge_failed' WHERE id=?").bind(json!({"review":{"ci_steps":[format!("touch {}; sleep 120; touch {}",quote(&started.to_string_lossy()),quote(&completed.to_string_lossy()))]}}).to_string()).bind(&task_id).execute(db.pool()).await.unwrap();
    let service = fixture.harness.state.task_service.clone();
    let task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    service
        .transition(
            &task_id,
            "review".to_owned(),
            services::task_service::TransitionOptions {
                bridge: Default::default(),
                version: task.version,
                triggered_by: Actor::system(SystemComponent::Workflow),
                reason: Some("remote CI".into()),
                rejection: false,
                defer_dispatch_seconds: None,
            },
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        while !started.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    fixture.link.take();
    fixture
        .harness
        .state
        .daemon_connections
        .unregister(&fixture.daemon_id);
    let task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    let held = service
        .perform_task_action(&task_id, TaskAction::Hold { reason: None }, task.version)
        .await
        .unwrap();
    assert_ne!(held.task.status, "cancelled");
    assert!(db.task_has_pending_remote_cancel(&task_id).await.unwrap());
    service.drain(&task_id).await.unwrap();
    let now = db::now_rfc3339();
    TaskRoleAssignmentRepo::assign(
        &*db,
        CreateTaskRoleAssignment {
            id: db::new_uuid_v4(),
            task_id: task_id.clone(),
            role_name: "coder".into(),
            assignee_type: Some(AssigneeKind::User),
            assignee_id: Some(common::fake_daemon::FAKE_DAEMON_USER_ID.into()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
    let follow_up = |offers: &TaskActionsResponse| {
        offers
            .available_actions
            .iter()
            .find(|offer| matches!(offer.action.verb(), "release" | "restart" | "retry"))
            .map(|offer| offer.action.clone())
            .unwrap_or_else(|| panic!("held Task offers a follow-up: {offers:?}"))
    };
    let offers = service
        .task_action_offers(&task_id, &Actor::user(UserActionSource::Api))
        .await
        .unwrap();
    let parked = service
        .perform_task_action(&task_id, follow_up(&offers), offers.version)
        .await
        .unwrap();
    let annotation: Value =
        serde_json::from_str(parked.task.error_annotation.as_deref().unwrap()).unwrap();
    assert_eq!(annotation["blocking_reason"], "pending_remote_cancel");
    let hostname = db::DaemonRepo::get_by_id(&*db, &fixture.daemon_id)
        .await
        .unwrap()
        .unwrap()
        .hostname;
    assert!(
        annotation["message"]
            .as_str()
            .unwrap()
            .contains(&format!("machine {hostname}")),
        "parked annotation names the machine: {annotation}"
    );
    fixture.link = Some(
        DaemonLink::connect(
            &fixture.server,
            &fixture.daemon_id,
            &fixture.token,
            fixture.runtime.clone(),
        )
        .await,
    );
    tokio::time::timeout(Duration::from_secs(15), async {
        while db.task_has_pending_remote_cancel(&task_id).await.unwrap() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    service.drain(&task_id).await.unwrap();
    assert!(!completed.exists());
    let task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(!task
        .error_annotation
        .as_deref()
        .unwrap_or_default()
        .contains("pending_remote_cancel"));
    // With cleanup confirmed, the follow-up proceeds instead of parking.
    let offers = service
        .task_action_offers(&task_id, &Actor::user(UserActionSource::Api))
        .await
        .unwrap();
    let proceeded = service
        .perform_task_action(&task_id, follow_up(&offers), offers.version)
        .await
        .unwrap();
    assert!(!proceeded
        .task
        .error_annotation
        .as_deref()
        .unwrap_or_default()
        .contains("pending_remote_cancel"));
    assert!(!completed.exists());
}

/// The owning daemon drops off while review-entry CI runs there. That is no
/// verdict: the Task stays in `review` on its typed check wait with the
/// asking step suspended, and when the daemon is back the result it kept is
/// read once and the same review attempt settles.
#[tokio::test]
async fn disconnected_remote_ci_waits_on_the_typed_check_and_resumes_on_reconnect() {
    let mut fixture = Fixture::new("forge-disconnected-ci-resume").await;
    let db = fixture.harness.state.db.clone();
    let task_id = fixture.resolved.placement.task_id.clone();
    let started = fixture._daemon_root.path().join("resume-ci-started");
    let completed = fixture._daemon_root.path().join("resume-ci-completed");
    sqlx::query("UPDATE task SET task_state_config=?,status='merge_failed' WHERE id=?").bind(json!({"review":{"ci_steps":[format!("touch {}; sleep 2; touch {}; printf resumed",quote(&started.to_string_lossy()),quote(&completed.to_string_lossy()))]}}).to_string()).bind(&task_id).execute(db.pool()).await.unwrap();
    let service = fixture.harness.state.task_service.clone();
    let task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    service
        .transition(
            &task_id,
            "review".to_owned(),
            services::task_service::TransitionOptions {
                bridge: Default::default(),
                version: task.version,
                triggered_by: Actor::system(SystemComponent::Workflow),
                reason: Some("remote CI".into()),
                rejection: false,
                defer_dispatch_seconds: None,
            },
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        while !started.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    fixture.link.take();
    fixture
        .harness
        .state
        .daemon_connections
        .unregister(&fixture.daemon_id);
    // The command finishes on the daemon while the server cannot hear it.
    tokio::time::timeout(Duration::from_secs(15), async {
        while !completed.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    let waiting = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(waiting.status, "review");
    assert!(
        waiting.condition.check_witness().is_some(),
        "the wait is a typed check condition: {:?}",
        waiting.condition
    );
    let suspended: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_step WHERE task_id=? AND kind='hooks' AND status='suspended'",
    )
    .bind(&task_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(suspended, 1);
    assert!(!db.task_has_pending_remote_cancel(&task_id).await.unwrap());
    fixture.link = Some(
        DaemonLink::connect(
            &fixture.server,
            &fixture.daemon_id,
            &fixture.token,
            fixture.runtime.clone(),
        )
        .await,
    );
    tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            let reviews = ReviewRepo::list_by_task(&*db, &task_id).await.unwrap();
            let details: Value =
                serde_json::from_str(&reviews[0].step_results_json).unwrap_or(Value::Null);
            if details["ci_steps"]
                .as_array()
                .is_some_and(|s| !s.is_empty())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the reconnected daemon's result settles the review attempt");
    let reviews = ReviewRepo::list_by_task(&*db, &task_id).await.unwrap();
    assert_eq!(reviews.len(), 1, "the same attempt resumes");
    let details: Value = serde_json::from_str(&reviews[0].step_results_json).unwrap();
    assert_eq!(details["ci_steps"].as_array().unwrap().len(), 1);
    assert_eq!(details["ci_steps"][0]["exit_code"], 0);
    assert_eq!(details["ci_steps"][0]["output_tail"], "resumed");
    // The command ran once: the reconnected link is only asked what happened.
    assert!(fixture
        .link
        .as_ref()
        .unwrap()
        .requests(api_types::METHOD_CHECK_RUN)
        .is_empty());
}

#[cfg(unix)]
#[tokio::test]
async fn cancel_during_remote_merge_push_finishes_integration_and_reports_done() {
    use db::TaskStepRepo;
    use std::os::unix::fs::PermissionsExt;
    let fixture = Fixture::new("forge-merge-push-cancel").await;
    let candidate = fixture.candidate().await;
    fixture.approve_candidate().await;
    let root = fixture._daemon_root.path();
    let remote = root.join("push-target.git");
    let started = root.join("push-started");
    let release = root.join("push-release");
    let output = tokio::process::Command::new("git")
        .args(["init", "--bare"])
        .arg(&remote)
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
    let hook = remote.join("hooks/pre-receive");
    std::fs::write(
        &hook,
        format!(
            "#!/bin/sh\ntouch {}\nwhile [ ! -f {} ]; do sleep 0.02; done\n",
            quote(&started.to_string_lossy()),
            quote(&release.to_string_lossy())
        ),
    )
    .unwrap();
    std::fs::set_permissions(&hook, std::fs::Permissions::from_mode(0o755)).unwrap();
    let post_merge = fixture.checkout.join(".git/hooks/post-merge");
    std::fs::write(
        &post_merge,
        format!(
            "#!/bin/sh\ngit push {} HEAD:refs/heads/main\n",
            quote(&remote.to_string_lossy())
        ),
    )
    .unwrap();
    std::fs::set_permissions(&post_merge, std::fs::Permissions::from_mode(0o755)).unwrap();
    let db = fixture.harness.state.db.clone();
    let service = fixture.harness.state.task_service.clone();
    let task_id = fixture.resolved.placement.task_id.clone();
    let task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    service
        .transition(
            &task_id,
            "merging".into(),
            services::task_service::TransitionOptions {
                bridge: Default::default(),
                version: task.version,
                triggered_by: Actor::system(SystemComponent::Workflow),
                reason: Some("approved remote merge".into()),
                rejection: false,
                defer_dispatch_seconds: None,
            },
        )
        .await
        .unwrap();
    let drain = {
        let service = service.clone();
        let task_id = task_id.clone();
        tokio::spawn(async move { service.drain(&task_id).await })
    };
    tokio::time::timeout(Duration::from_secs(15), async {
        while !started.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("merge reaches the push process");
    let task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    let mut cancel = {
        let service = service.clone();
        let task_id = task_id.clone();
        tokio::spawn(async move {
            service
                .perform_task_action(&task_id, TaskAction::Cancel { reason: None }, task.version)
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        while !db.task_steps(&task_id).await.unwrap().iter().any(|s| {
            s.kind == "command"
                && s.status == "pending"
                && serde_json::from_str::<Value>(&s.payload_json).unwrap()["preempt"] == true
        }) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut cancel)
            .await
            .is_err(),
        "Cancel waits for the protected push"
    );
    std::fs::write(&release, "release").unwrap();
    let cancelled = tokio::time::timeout(Duration::from_secs(15), cancel)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(cancelled.task.status, "done");
    // The queued Cancel ran after the merge landed and records that it was moot.
    let moot: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_comment WHERE task_id=? AND content LIKE 'Cancel had no effect%'",
    )
    .bind(&task_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(moot, 1);
    drain.await.unwrap().unwrap();
    let output = tokio::process::Command::new("git")
        .arg("--git-dir")
        .arg(&remote)
        .args(["rev-parse", "refs/heads/main"])
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
    assert_eq!(String::from_utf8(output.stdout).unwrap().trim(), candidate);
    assert!(db
        .pending_remote_cancels(None, None)
        .await
        .unwrap()
        .is_empty());
}

#[cfg(unix)]
#[tokio::test]
async fn cancel_during_slow_remote_merge_push_reports_done() {
    use db::TaskStepRepo;
    use std::os::unix::fs::PermissionsExt;
    let fixture = Fixture::new("forge-merge-push-cancel-slow").await;
    let candidate = fixture.candidate().await;
    fixture.approve_candidate().await;
    let root = fixture._daemon_root.path();
    let remote = root.join("push-target.git");
    let started = root.join("push-started");
    let release = root.join("push-release");
    let output = tokio::process::Command::new("git")
        .args(["init", "--bare"])
        .arg(&remote)
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
    let hook = remote.join("hooks/pre-receive");
    std::fs::write(
        &hook,
        format!(
            "#!/bin/sh\ntouch {}\nwhile [ ! -f {} ]; do sleep 0.02; done\n",
            quote(&started.to_string_lossy()),
            quote(&release.to_string_lossy())
        ),
    )
    .unwrap();
    std::fs::set_permissions(&hook, std::fs::Permissions::from_mode(0o755)).unwrap();
    let post_merge = fixture.checkout.join(".git/hooks/post-merge");
    std::fs::write(
        &post_merge,
        format!(
            "#!/bin/sh\ngit push {} HEAD:refs/heads/main\n",
            quote(&remote.to_string_lossy())
        ),
    )
    .unwrap();
    std::fs::set_permissions(&post_merge, std::fs::Permissions::from_mode(0o755)).unwrap();
    let db = fixture.harness.state.db.clone();
    let service = fixture.harness.state.task_service.clone();
    let task_id = fixture.resolved.placement.task_id.clone();
    let task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    service
        .transition(
            &task_id,
            "merging".into(),
            services::task_service::TransitionOptions {
                bridge: Default::default(),
                version: task.version,
                triggered_by: Actor::system(SystemComponent::Workflow),
                reason: Some("approved remote merge".into()),
                rejection: false,
                defer_dispatch_seconds: None,
            },
        )
        .await
        .unwrap();
    let drain = {
        let service = service.clone();
        let task_id = task_id.clone();
        tokio::spawn(async move { service.drain(&task_id).await })
    };
    tokio::time::timeout(Duration::from_secs(15), async {
        while !started.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("merge reaches the push process");
    let task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    let mut cancel = {
        let service = service.clone();
        let task_id = task_id.clone();
        tokio::spawn(async move {
            service
                .perform_task_action(&task_id, TaskAction::Cancel { reason: None }, task.version)
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        while !db.task_steps(&task_id).await.unwrap().iter().any(|s| {
            s.kind == "command"
                && s.status == "pending"
                && serde_json::from_str::<Value>(&s.payload_json).unwrap()["preempt"] == true
        }) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut cancel)
            .await
            .is_err(),
        "Cancel waits for the protected push"
    );
    // The push is still running after the 10 s acknowledgement window.
    tokio::time::sleep(Duration::from_secs(11)).await;
    std::fs::write(&release, "release").unwrap();
    let cancelled = tokio::time::timeout(Duration::from_secs(15), cancel)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let pushed = {
        let mut sha = String::new();
        for _ in 0..200 {
            let output = tokio::process::Command::new("git")
                .arg("--git-dir")
                .arg(&remote)
                .args(["rev-parse", "refs/heads/main"])
                .output()
                .await
                .unwrap();
            sha = String::from_utf8(output.stdout).unwrap().trim().to_owned();
            if sha == candidate {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        sha
    };
    let now = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (cancelled.task.status.as_str(), now.status.as_str()),
        ("done", "done"),
        "integration landed on the target (pushed={}, candidate={candidate}) but Cancel reported {} and the Task is now {}",
        pushed, cancelled.task.status, now.status
    );
    assert_eq!(cancelled.task.status, "done");
    drain.await.unwrap().unwrap();
    let output = tokio::process::Command::new("git")
        .arg("--git-dir")
        .arg(&remote)
        .args(["rev-parse", "refs/heads/main"])
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
    assert_eq!(String::from_utf8(output.stdout).unwrap().trim(), candidate);
    assert!(db
        .pending_remote_cancels(None, None)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn daemon_merge_rebase_and_check_primitives_write_no_server_tables_or_events() {
    use services::integration_effects::rpc::{decode_reply, validate_merge_result, RpcExchange};
    for case in ["merge", "rebase", "check"] {
        let fixture = Fixture::new("forge-owner-effect-without-recorder").await;
        let candidate = fixture.candidate().await;
        if case == "merge" {
            fixture.approve_candidate().await;
        }
        if case == "rebase" {
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
        }
        // Stop the fixture heartbeat so a background writer cannot invalidate
        // the all-table assertion. A describe reply drains earlier socket frames.
        fixture.link.as_ref().unwrap().tasks[2].abort();
        tokio::task::yield_now().await;
        let placement = &fixture.resolved.placement;
        let reference = WorkspaceHandleReference {
            daemon_id: fixture.daemon_id.clone(),
            runtime_id: placement.runtime_id.clone().unwrap(),
            placement_id: placement.id.clone(),
            workspace_handle: placement.workspace_handle.clone().unwrap(),
            generation: placement.generation as u64,
        };
        fixture
            .resolved
            .backend
            .daemon_client()
            .unwrap()
            .describe(
                &fixture.daemon_id,
                WorkspaceDescribeParams {
                    workspace: reference,
                },
            )
            .await
            .unwrap();
        let operation_id = db::new_uuid_v4();
        let fence = WorkspaceMutationFence {
            integration: api_types::WorkspaceIntegrationBinding::TaskStep,
            daemon_id: fixture.daemon_id.clone(),
            runtime_id: placement.runtime_id.clone().unwrap(),
            placement_id: placement.id.clone(),
            operation_id: operation_id.clone(),
            generation: placement.generation as u64,
            expected: WorkspaceOperationExpected::BaseSha {
                sha: candidate.clone(),
            },
        };
        let handle = placement.workspace_handle.clone().unwrap();
        let (method, request) = match case {
            "merge" => (
                METHOD_WORKSPACE_MERGE,
                serde_json::to_value(WorkspaceReviewedMergeParams {
                    merge: WorkspaceMergeParams {
                        fence,
                        workspace_handle: handle,
                        repo_location_id: placement.repo_location_id.clone(),
                        target_branch: "main".into(),
                        expected_target_sha: fixture.base_sha.clone(),
                        handed_off_paths: vec![],
                    },
                    reviewed_commit_sha: Some(candidate.clone()),
                })
                .unwrap(),
            ),
            "rebase" => (
                METHOD_WORKSPACE_RESET,
                serde_json::to_value(WorkspaceOwnerOperationParams {
                    fence,
                    workspace_handle: handle,
                    operation: WorkspaceOwnerOperation::RebaseTarget {
                        target_branch: "main".into(),
                        handoff_conflicts: true,
                    },
                })
                .unwrap(),
            ),
            _ => (
                METHOD_WORKSPACE_RUN,
                serde_json::to_value(WorkspaceRunParams {
                    fence,
                    workspace_handle: handle,
                    purpose: WorkspaceRunPurpose::CiStep,
                    command: "printf '%s' \"$TEST_SECRET\"".into(),
                    env: vec![("TEST_SECRET".into(), "hidden-value".into())],
                    timeout_secs: 0,
                    max_output_bytes: u64::MAX,
                })
                .unwrap(),
            ),
        };
        let before = table_digests(&fixture.harness.state.db).await;
        let mut events = fixture.harness.state.event_bus.subscribe();
        let mut exchange = RpcExchange::prepare(
            fixture
                .harness
                .state
                .daemon_connections
                .get(&fixture.daemon_id)
                .unwrap(),
            method,
            request.clone(),
        )
        .unwrap();
        let value = exchange
            .execute(if case == "check" {
                None
            } else {
                Some(Duration::from_secs(5))
            })
            .await
            .unwrap();
        match case {
            "merge" => {
                let result: WorkspaceMergeResult = decode_reply(method, &request, &value).unwrap();
                validate_merge_result(&request, &result).unwrap();
                assert!(
                    matches!(result.outcome, WorkspaceMergeOutcome::Done { after_sha, .. } if after_sha == candidate)
                );
            }
            "rebase" => {
                let result: WorkspaceOwnerOperationResult =
                    decode_reply(method, &request, &value).unwrap();
                assert!(matches!(
                    result.outcome,
                    WorkspaceOwnerOperationOutcome::Rebased
                ));
            }
            _ => {
                let result: WorkspaceRunResult = decode_reply(method, &request, &value).unwrap();
                assert_eq!(result.exit_code, Some(0));
                assert!(!result.stdout.contains("hidden-value"));
                assert!(!result.timed_out);
            }
        }
        assert_eq!(
            before,
            table_digests(&fixture.harness.state.db).await,
            "{case}"
        );
        assert!(events.try_recv().is_err());
        let receipts: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM command_receipt WHERE correlation_id=?")
                .bind(&operation_id)
                .fetch_one(fixture.harness.state.db.pool())
                .await
                .unwrap();
        assert_eq!(receipts, 0);
        assert!(fixture.runtime.journal().pending().unwrap().iter().any(|entry| matches!(entry,
            JournalEntry::Operation { operation } if operation.fence.operation_id == operation_id && operation.outcome.is_some() && !operation.acknowledged)));
    }
}

#[tokio::test]
async fn integration_attempt_lost_reply_across_real_reconnect_settles_once() {
    use db::IntegrationQueueRepo;
    let mut fixture = Fixture::new("forge-integration-attempt-reconnect").await;
    let candidate = fixture.candidate().await;
    let database = fixture.harness.state.db.clone();
    let placement = fixture.resolved.placement.clone();
    let workspace = WorkspaceRepo::get_by_id(&*database, &placement.workspace_id)
        .await
        .unwrap()
        .unwrap();
    let task = TaskRepo::get_by_id(&*database, &placement.task_id, false)
        .await
        .unwrap()
        .unwrap();
    let epoch: i64 = sqlx::query_scalar("SELECT status_epoch FROM task WHERE id=?")
        .bind(&task.id)
        .fetch_one(database.pool())
        .await
        .unwrap();
    let queue = database
        .create_or_get_integration_queue(&workspace.repo_id, "main")
        .await
        .unwrap();
    let attempt = database
        .admit_integration_attempt(db::IntegrationAttempt::new(
            Some(queue.id.clone()),
            task.id,
            task.project_id,
            "reconnect-attempt".into(),
            task.status,
            epoch,
            task.version,
        ))
        .await
        .unwrap();
    let queue = database
        .integration_queue(&queue.id)
        .await
        .unwrap()
        .unwrap();
    database
        .claim_integration_queue(
            &queue.id,
            queue.revision,
            "reconnect-worker",
            &db::now_rfc3339(),
            "2099-01-01T00:00:00Z",
        )
        .await
        .unwrap();
    let fence = database
        .integration_owner_fence(&attempt.id)
        .await
        .unwrap()
        .unwrap();
    let witness = json!({"workspace":{"workspace_id":placement.workspace_id,"placement_id":placement.id,"generation":placement.generation,"handle":placement.workspace_handle,"owner":{"kind":"daemon","daemon_id":placement.daemon_id,"runtime_id":placement.runtime_id}},"target_branch":"main","task_branch":workspace.branch,"expected_head_sha":candidate,"expected_target_sha":fixture.base_sha,"reviewed":{"commit_sha":candidate,"base_sha":fixture.base_sha}});
    let request = db::IntegrationEffectRequest {
        fence,
        kind: db::IntegrationOperationKind::FastForward,
        witness,
    };
    let params = serde_json::to_value(WorkspaceReviewedMergeParams {
        merge: WorkspaceMergeParams {
            fence: WorkspaceMutationFence {
                integration: WorkspaceIntegrationBinding::TaskStep,
                daemon_id: fixture.daemon_id.clone(),
                runtime_id: placement.runtime_id.clone().unwrap(),
                placement_id: placement.id.clone(),
                operation_id: "assigned-by-attempt-sink".into(),
                generation: placement.generation as u64,
                expected: WorkspaceOperationExpected::BaseSha {
                    sha: candidate.clone(),
                },
            },
            workspace_handle: placement.workspace_handle.clone().unwrap(),
            repo_location_id: placement.repo_location_id.clone(),
            target_branch: "main".into(),
            expected_target_sha: fixture.base_sha.clone(),
            handed_off_paths: Vec::new(),
        },
        reviewed_commit_sha: Some(candidate.clone()),
    })
    .unwrap();
    let client = services::daemon_transport::workspace_client::DaemonWorkspaceClient::new(
        fixture.harness.state.daemon_connections.clone(),
    )
    .with_receipts(database.clone());
    fixture
        .link
        .as_ref()
        .unwrap()
        .drop_replies
        .lock()
        .unwrap()
        .insert(METHOD_WORKSPACE_MERGE.into());
    let effect = client.integration_effect(
        &fixture.daemon_id,
        request.clone(),
        METHOD_WORKSPACE_MERGE,
        params.clone(),
        Duration::from_secs(10),
    );
    let owner_finished = async {
        tokio::time::timeout(Duration::from_secs(8), async {
            while git::get_current_sha(&fixture.checkout).await.unwrap() != candidate {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        // This closes the actual WebSocket, failing the server's pending reply.
        fixture.link.take();
        common::fake_daemon::wait_until_disconnected(&fixture.harness.state, &fixture.daemon_id)
            .await;
    };
    let (lost_reply, _) = tokio::join!(effect, owner_finished);
    assert!(lost_reply.is_err());
    assert!(database
        .integration_attempt(&attempt.id)
        .await
        .unwrap()
        .unwrap()
        .effect_intent_json
        .is_some());
    let reflog = git::command_output(&fixture.checkout, &["reflog", "--all"])
        .await
        .unwrap()
        .stdout;
    let objects = git::command_output(&fixture.checkout, &["count-objects", "-v"])
        .await
        .unwrap()
        .stdout;
    fixture.reconnect().await;
    client
        .reconcile_integration_attempts(&fixture.daemon_id)
        .await
        .unwrap();
    let receipt = client
        .integration_effect(
            &fixture.daemon_id,
            request.clone(),
            METHOD_WORKSPACE_MERGE,
            params,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(
        receipt.operation_state,
        db::IntegrationOperationState::Succeeded
    );
    assert_eq!(receipt.result["outcome"]["Done"]["after_sha"], candidate);
    assert_eq!(
        git::command_output(&fixture.checkout, &["reflog", "--all"])
            .await
            .unwrap()
            .stdout,
        reflog
    );
    assert_eq!(
        git::command_output(&fixture.checkout, &["count-objects", "-v"])
            .await
            .unwrap()
            .stdout,
        objects
    );
    assert_eq!(
        fixture
            .link
            .as_ref()
            .unwrap()
            .requests(METHOD_WORKSPACE_MERGE)
            .len(),
        0
    );
    let settled = database
        .integration_attempt(&attempt.id)
        .await
        .unwrap()
        .unwrap();
    assert!(settled.effect_intent_json.is_none());
    assert_eq!(
        settled.current_operation_state,
        Some(db::IntegrationOperationState::Succeeded)
    );
    let count: i64 = sqlx::query_scalar(
        "SELECT json_array_length(effect_receipts_json) FROM integration_attempt WHERE id=?",
    )
    .bind(&attempt.id)
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn check_spec_real_roundtrip_reconnect_and_duplicate_return_owner_receipt() {
    let mut fixture = Fixture::new("check-owner-roundtrip").await;
    let client = services::daemon_transport::workspace_client::DaemonWorkspaceClient::new(
        fixture.harness.state.daemon_connections.clone(),
    );
    let daemon_id = fixture.daemon_id.clone();
    let placement = &fixture.resolved.placement;
    let operation_id = db::new_uuid_v4();
    let params = DaemonCheckRunParams {
        operation_id: operation_id.clone(),
        target: DaemonCheckTarget::Workspace {
            workspace: WorkspaceHandleReference {
                daemon_id: daemon_id.clone(),
                runtime_id: placement.runtime_id.clone().unwrap(),
                placement_id: placement.id.clone(),
                workspace_handle: placement.workspace_handle.clone().unwrap(),
                generation: placement.generation as u64,
            },
        },
        purpose: WorkspaceRunPurpose::CiStep,
        spec: CheckSpec {
            schema_revision: CHECK_SPEC_REVISION,
            scope: CheckScope::Commit,
            commands: vec![CheckCommandSpec {
                id: "ci:0".into(),
                shell_text: "printf run >> check-runs; sleep 3; printf owner-result".into(),
                shell: "bash -lc".into(),
                working_directory: CheckWorkingDirectory::TaskRoot,
                environment_keys: Default::default(),
                timeout_seconds: Some(5),
                failure_policy: CheckFailurePolicy::StopBundle,
                cacheability: CheckCacheability::Uncacheable,
                requirement_ids: Default::default(),
            }],
            declares_cleanup: false,
            configured_commands: 1,
            blank_commands: vec![],
            execution_policy: "legacy-daemon/1".into(),
        },
        env: vec![],
        cleanup_commands: vec![],
        cleanup_timeout_ms: 1000,
        deadline: (chrono::Utc::now() + chrono::Duration::seconds(15)).to_rfc3339(),
    };
    let effect = client.run_check(&daemon_id, params.clone());
    let disconnect = async {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if matches!(
                    client
                        .lookup_check(&daemon_id, &operation_id)
                        .await
                        .unwrap(),
                    DaemonCheckResult::Running { .. }
                ) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        // A duplicate on the actual command stream starts nothing.
        assert!(matches!(
            client.run_check(&daemon_id, params.clone()).await.unwrap(),
            DaemonCheckResult::Running { .. }
        ));
        fixture.link.take();
        common::fake_daemon::wait_until_disconnected(&fixture.harness.state, &daemon_id).await;
    };
    let (lost, ()) = tokio::join!(effect, disconnect);
    assert!(lost.is_err());
    fixture.reconnect().await;
    let receipt = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match client
                .lookup_check(&daemon_id, &operation_id)
                .await
                .unwrap()
            {
                DaemonCheckResult::Completed { receipt } => break receipt,
                DaemonCheckResult::Running { .. } => {
                    tokio::time::sleep(Duration::from_millis(20)).await
                }
                result => panic!("unexpected lookup {result:?}"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(receipt.outcome, CheckExecutionOutcome::Passed);
    assert_eq!(
        receipt.execution_inputs,
        CheckEnvironmentIdentity::NotAttested
    );
    assert_eq!(receipt.commands[0].stdout_tail, "owner-result");
    assert_eq!(
        client.run_check(&daemon_id, params).await.unwrap(),
        DaemonCheckResult::Completed { receipt }
    );
    let runs = fixture
        .resolved
        .run("cat check-runs", &BTreeMap::new(), None)
        .await
        .unwrap();
    assert_eq!(runs.stdout, "run");
}

/// A claimed queue attempt for the fixture's Task: its fence and attempt id.
async fn claimed_attempt(fixture: &Fixture, key: &str) -> (IntegrationOwnerFence, String) {
    use db::IntegrationQueueRepo;
    let database = fixture.harness.state.db.clone();
    let placement = &fixture.resolved.placement;
    let workspace = WorkspaceRepo::get_by_id(&*database, &placement.workspace_id)
        .await
        .unwrap()
        .unwrap();
    let task = TaskRepo::get_by_id(&*database, &placement.task_id, false)
        .await
        .unwrap()
        .unwrap();
    let epoch: i64 = sqlx::query_scalar("SELECT status_epoch FROM task WHERE id=?")
        .bind(&task.id)
        .fetch_one(database.pool())
        .await
        .unwrap();
    let queue = database
        .create_or_get_integration_queue(&workspace.repo_id, "main")
        .await
        .unwrap();
    let attempt = database
        .admit_integration_attempt(db::IntegrationAttempt::new(
            Some(queue.id.clone()),
            task.id,
            task.project_id,
            key.into(),
            task.status,
            epoch,
            task.version,
        ))
        .await
        .unwrap();
    let queue = database
        .integration_queue(&queue.id)
        .await
        .unwrap()
        .unwrap();
    database
        .claim_integration_queue(
            &queue.id,
            queue.revision,
            "wire-worker",
            &db::now_rfc3339(),
            "2099-01-01T00:00:00Z",
        )
        .await
        .unwrap();
    let fence = database
        .integration_owner_fence(&attempt.id)
        .await
        .unwrap()
        .unwrap();
    let fence = serde_json::from_value(serde_json::to_value(fence).unwrap()).unwrap();
    (fence, attempt.id)
}

fn git_sync(path: &std::path::Path, args: &[&str]) -> String {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(path)
        .env("GIT_AUTHOR_NAME", "Forge")
        .env("GIT_AUTHOR_EMAIL", "forge@example.invalid")
        .env("GIT_COMMITTER_NAME", "Forge")
        .env("GIT_COMMITTER_EMAIL", "forge@example.invalid")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

fn transfer_staging(root: &std::path::Path) -> Vec<String> {
    std::fs::read_dir(root.join(".forge/transfer"))
        .map(|entries| {
            entries
                .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default()
}

/// Stage a started attempt effect the owner never received.
async fn lost_fast_forward(
    fixture: &Fixture,
    fence: &IntegrationOwnerFence,
    candidate: &str,
) -> db::IntegrationEffectRequest {
    let database = &fixture.harness.state.db;
    let placement = &fixture.resolved.placement;
    let witness = json!({"workspace":{"workspace_id":placement.workspace_id,"placement_id":placement.id,"generation":placement.generation,"handle":placement.workspace_handle,"owner":{"kind":"daemon","daemon_id":placement.daemon_id,"runtime_id":placement.runtime_id}},"target_branch":"main","expected_head_sha":candidate,"expected_target_sha":fixture.base_sha,"reviewed":{"commit_sha":candidate,"base_sha":fixture.base_sha}});
    let request = db::IntegrationEffectRequest {
        fence: serde_json::from_value(serde_json::to_value(fence).unwrap()).unwrap(),
        kind: db::IntegrationOperationKind::FastForward,
        witness,
    };
    let db::IntegrationEffectAdmission::Started(mut guard) = database
        .begin_integration_effect(request.clone())
        .await
        .unwrap()
    else {
        panic!("effect admitted")
    };
    assert!(guard.start().await.unwrap().is_none());
    drop(guard);
    request
}

#[tokio::test]
async fn integration_objects_move_between_daemon_and_server_owners_by_key() {
    use db::IntegrationQueueRepo;
    use services::integration_owner::{
        ObjectTransferOutcome, ServerIntegrationOwner, ServerObjectExport, ServerObjectImport,
    };
    let fixture = Fixture::new("forge-integration-object-transfer").await;
    let candidate = fixture.candidate().await;
    let database = fixture.harness.state.db.clone();
    let placement = fixture.resolved.placement.clone();
    let (fence, attempt_id) = claimed_attempt(&fixture, "object-transfer").await;
    let repo_id = WorkspaceRepo::get_by_id(&*database, &placement.workspace_id)
        .await
        .unwrap()
        .unwrap()
        .repo_id;
    // A server-owned clone of the same repository, without the candidate.
    let server_clone = fixture.server_root.path().join("server-clone");
    git_sync(
        fixture.server_root.path(),
        &[
            "clone",
            "-q",
            "--single-branch",
            "--branch",
            "main",
            fixture.checkout.to_str().unwrap(),
            "server-clone",
        ],
    );
    let now = db::now_rfc3339();
    sqlx::query("INSERT INTO repo_location(id,repo_id,owner_kind,path,kind,is_default,status,created_at,updated_at) VALUES('server-clone',?,'server',?,'managed_clone',0,'ready',?,?)")
        .bind(&repo_id).bind(server_clone.to_str()).bind(&now).bind(&now)
        .execute(database.pool()).await.unwrap();
    let owner = ServerIntegrationOwner::new(database.clone());
    let client = services::daemon_transport::workspace_client::DaemonWorkspaceClient::new(
        fixture.harness.state.daemon_connections.clone(),
    )
    .with_receipts(database.clone());
    let runtime_id = placement.runtime_id.clone().unwrap();
    let daemon = services::daemon_transport::workspace_client::DaemonObjectEndpoint {
        daemon_id: &fixture.daemon_id,
        runtime_id: &runtime_id,
        repo_location_id: &placement.repo_location_id,
    };
    let cancel = tokio_util::sync::CancellationToken::new();
    let daemon_root = fixture._daemon_root.path().canonicalize().unwrap();

    // Daemon -> server: the candidate goes to the server-owned checkout.
    let outbound = object_transfer_key(
        &attempt_id,
        fence.generation,
        ObjectTransferDirection::Outbound,
    );
    assert_eq!(
        owner
            .imported_objects(&fence, "server-clone", &outbound, &candidate)
            .await
            .unwrap(),
        ObjectTransferOutcome::Done(None)
    );
    let bundle = fixture.server_root.path().join("outbound.bundle");
    let have = vec![fixture.base_sha.clone()];
    let ObjectTransferOutcome::Done(export) = client
        .export_objects(
            daemon, &fence, &outbound, &have, &candidate, &bundle, &cancel,
        )
        .await
        .unwrap()
    else {
        panic!("daemon export refused")
    };
    assert_eq!(export.tip_sha, candidate);
    assert_eq!(
        std::fs::metadata(&bundle).unwrap().len(),
        export.total_bytes
    );
    let server_head = git_sync(&server_clone, &["rev-parse", "HEAD"]);
    let imported = owner
        .import_objects(ServerObjectImport {
            fence: &fence,
            export: &export,
            repo_location_id: "server-clone",
            bundle: &bundle,
            cancel: &cancel,
        })
        .await
        .unwrap();
    assert!(
        matches!(&imported, ObjectTransferOutcome::Done(receipt) if !receipt.replayed && receipt.tip_sha == candidate)
    );
    assert_eq!(
        git_sync(
            &server_clone,
            &["rev-parse", &format!("refs/forge/integration/{outbound}")]
        ),
        candidate
    );
    assert_eq!(git_sync(&server_clone, &["rev-parse", "HEAD"]), server_head);
    assert_eq!(
        git_sync(&server_clone, &["rev-parse", "refs/heads/main"]),
        server_head
    );
    client
        .release_objects(&fixture.daemon_id, &runtime_id, &outbound)
        .await;
    assert!(transfer_staging(&daemon_root).is_empty());
    assert!(matches!(
        owner.imported_objects(&fence, "server-clone", &outbound, &candidate).await.unwrap(),
        ObjectTransferOutcome::Done(Some(receipt)) if receipt.replayed
    ));

    // Server -> daemon: a commit made on the server-owned checkout.
    std::fs::write(server_clone.join("server.txt"), "server\n").unwrap();
    git_sync(&server_clone, &["add", "."]);
    git_sync(&server_clone, &["commit", "-q", "-m", "server"]);
    let server_tip = git_sync(&server_clone, &["rev-parse", "HEAD"]);
    let inbound = object_transfer_key(
        &attempt_id,
        fence.generation,
        ObjectTransferDirection::Inbound,
    );
    let bundle = fixture.server_root.path().join("inbound.bundle");
    let ObjectTransferOutcome::Done(export) = owner
        .export_objects(ServerObjectExport {
            fence: &fence,
            key: &inbound,
            repo_location_id: "server-clone",
            have: &have,
            want: &server_tip,
            dest: &bundle,
            cancel: &cancel,
        })
        .await
        .unwrap()
    else {
        panic!("server export refused")
    };
    assert_eq!(
        client
            .imported_objects(daemon, &fence, &inbound, &server_tip)
            .await
            .unwrap(),
        ObjectTransferOutcome::Done(None)
    );
    let daemon_refs = git_sync(&fixture.checkout, &["for-each-ref"]);
    let daemon_head = git_sync(&fixture.checkout, &["rev-parse", "HEAD"]);
    // A cancelled push, a mislabelled bundle and an oversized one change
    // nothing on the owner and leave no staging there.
    let cancelled = tokio_util::sync::CancellationToken::new();
    cancelled.cancel();
    assert_eq!(
        client
            .import_objects(daemon, &fence, &export, &bundle, &cancelled)
            .await
            .unwrap(),
        ObjectTransferOutcome::Cancelled
    );
    let mut mislabelled = export.clone();
    mislabelled.sha256 = "0".repeat(64);
    assert!(matches!(
        client
            .import_objects(daemon, &fence, &mislabelled, &bundle, &cancel)
            .await
            .unwrap(),
        ObjectTransferOutcome::Refused(ObjectTransferRefusal::Invalid { .. })
    ));
    let mut oversized = export.clone();
    oversized.total_bytes = MAX_OBJECT_TRANSFER_BYTES + 1;
    assert!(matches!(
        client
            .import_objects(daemon, &fence, &oversized, &bundle, &cancel)
            .await
            .unwrap(),
        ObjectTransferOutcome::Refused(ObjectTransferRefusal::TooLarge { .. })
    ));
    assert!(transfer_staging(&daemon_root).is_empty());
    assert_eq!(git_sync(&fixture.checkout, &["for-each-ref"]), daemon_refs);
    let received = client
        .import_objects(daemon, &fence, &export, &bundle, &cancel)
        .await
        .unwrap();
    assert!(
        matches!(&received, ObjectTransferOutcome::Done(receipt) if !receipt.replayed && receipt.tip_sha == server_tip)
    );
    assert_eq!(
        git_sync(
            &fixture.checkout,
            &["rev-parse", &format!("refs/forge/integration/{inbound}")]
        ),
        server_tip
    );
    assert_eq!(
        git_sync(&fixture.checkout, &["rev-parse", "HEAD"]),
        daemon_head
    );
    assert!(transfer_staging(&daemon_root).is_empty());
    assert!(matches!(
        client.imported_objects(daemon, &fence, &inbound, &server_tip).await.unwrap(),
        ObjectTransferOutcome::Done(Some(receipt)) if receipt.replayed
    ));

    // The owner knows this claim generation (the transfers carried its
    // fence; an announcement repeats it): a started intent it never received
    // is settled as not performed.
    let announced = client
        .announce_integration_fence(&fixture.daemon_id, &runtime_id, &fence, None)
        .await
        .unwrap();
    assert_eq!(announced.previous.as_ref(), Some(&fence));
    lost_fast_forward(&fixture, &fence, &candidate).await;
    client
        .reconcile_integration_attempts(&fixture.daemon_id)
        .await
        .unwrap();
    let settled = database
        .integration_attempt(&attempt_id)
        .await
        .unwrap()
        .unwrap();
    assert!(settled.effect_intent_json.is_none());
    assert_eq!(
        settled.current_operation_state,
        Some(db::IntegrationOperationState::Failed)
    );
    assert_eq!(
        git_sync(&fixture.checkout, &["rev-parse", "HEAD"]),
        daemon_head
    );
}

#[tokio::test]
async fn integration_lookup_on_an_owner_without_the_claim_fence_stays_unknown() {
    use db::IntegrationQueueRepo;
    let fixture = Fixture::new("forge-integration-unknown-lookup").await;
    let candidate = fixture.candidate().await;
    let database = fixture.harness.state.db.clone();
    let (fence, attempt_id) = claimed_attempt(&fixture, "unknown-lookup").await;
    let client = services::daemon_transport::workspace_client::DaemonWorkspaceClient::new(
        fixture.harness.state.daemon_connections.clone(),
    )
    .with_receipts(database.clone());
    // The claim was never announced to this owner and no effect of it was
    // admitted there: its empty journal cannot prove "not performed".
    lost_fast_forward(&fixture, &fence, &candidate).await;
    let head = git_sync(&fixture.checkout, &["rev-parse", "HEAD"]);
    for _ in 0..2 {
        client
            .reconcile_integration_attempts(&fixture.daemon_id)
            .await
            .unwrap();
        let attempt = database
            .integration_attempt(&attempt_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            attempt.current_operation_state,
            Some(db::IntegrationOperationState::Uncertain)
        );
    }
    assert_eq!(git_sync(&fixture.checkout, &["rev-parse", "HEAD"]), head);
}
