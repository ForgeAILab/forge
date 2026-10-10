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

/// Bounds a wait by progress instead of by wall time: it is over only when
/// the watched value has not changed for the whole limit.
struct Stall {
    seen: Option<String>,
    since: tokio::time::Instant,
    limit: Duration,
}

impl Stall {
    fn new(limit: Duration) -> Self {
        Self {
            seen: None,
            since: tokio::time::Instant::now(),
            limit,
        }
    }

    /// `true` once `observed` has stayed the same for the whole limit.
    fn stalled(&mut self, observed: String) -> bool {
        if self.seen.as_ref() != Some(&observed) {
            self.seen = Some(observed);
            self.since = tokio::time::Instant::now();
        }
        self.since.elapsed() >= self.limit
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

    /// A fixture over a file-backed database, for tests that run the queue
    /// worker, the check worker and the step worker on several threads.
    async fn file_backed(suite: &str) -> Self {
        Self::build(
            suite,
            WorkspaceRunPolicy {
                allowed_purposes: vec![
                    WorkspaceRunPurpose::CiStep,
                    WorkspaceRunPurpose::Hook,
                    WorkspaceRunPurpose::EnvironmentSetup,
                ],
            },
            true,
        )
        .await
    }

    async fn with_policy(suite: &str, run_policy: WorkspaceRunPolicy) -> Self {
        Self::build(suite, run_policy, false).await
    }

    async fn build(suite: &str, run_policy: WorkspaceRunPolicy, file_backed: bool) -> Self {
        let server_root = TestDir::new(&format!("{suite}-server"));
        let daemon_root = TestDir::new(&format!("{suite}-owner"));
        let checkout = common::setup_git_repo(daemon_root.path());
        let database_url = if file_backed {
            format!(
                "sqlite://{}?mode=rwc",
                server_root.path().join("forge.sqlite").display()
            )
        } else {
            "sqlite::memory:".to_owned()
        };
        let harness =
            common::test_app_on(&server_root.path().join("workspaces"), suite, &database_url).await;
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

    /// What the server has done so far, as one comparable value: versions of
    /// the rows a reconciliation writes, settled Task steps and recorded
    /// events. A wait that watches it tells a busy server from a stuck one.
    async fn server_activity(&self) -> String {
        sqlx::query_scalar(
            "SELECT (SELECT COALESCE(SUM(version),0) FROM task) || '|' || \
             (SELECT COALESCE(SUM(execution_version),0) FROM execution) || '|' || \
             (SELECT COALESCE(SUM(version),0) FROM workspace_placement) || '|' || \
             (SELECT COUNT(*) || ':' || COALESCE(SUM(attempts),0) FROM task_step WHERE status<>'pending') || '|' || \
             (SELECT COUNT(*) FROM domain_event)",
        )
        .fetch_one(self.harness.state.db.pool())
        .await
        .unwrap()
    }

    /// Waits until the owner's journal is acknowledged empty. It fails only
    /// when nothing has moved for 5 s (the journal, the frames the server
    /// sent, the server's own rows), not after a fixed time: a loaded
    /// machine is slow, not stuck.
    /// What the server runtime's heartbeat monitor does after an owner
    /// reconnects: its placements go from `disconnected` back to `ready`.
    /// The harness runs no monitor, so a test that needs it calls this.
    async fn reconcile_reconnected_placement(&self, placement_id: &str) {
        let monitor = services::HeartbeatMonitor::new(
            self.harness.state.db.clone(),
            self.harness.state.event_bus.clone(),
        )
        .with_daemon_connections(self.harness.state.daemon_connections.clone())
        .with_task_service(self.harness.state.task_service.clone());
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                monitor.check_once().await.unwrap();
                let placement =
                    WorkspacePlacementRepo::get_by_id(&*self.harness.state.db, placement_id)
                        .await
                        .unwrap()
                        .unwrap();
                if placement.state == PlacementState::Ready {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the reconnected owner's placement is reconciled");
    }

    async fn wait_until_journal_empty(&self) {
        let mut stall = Stall::new(Duration::from_secs(5));
        loop {
            let pending = self.runtime.journal().pending().unwrap();
            if pending.is_empty() {
                return;
            }
            let observed = format!(
                "{:?}|{}|{}",
                pending
                    .iter()
                    .map(JournalEntry::entry_id)
                    .collect::<Vec<_>>(),
                self.link
                    .as_ref()
                    .map_or(0, |link| link.requests.lock().unwrap().len()),
                self.server_activity().await
            );
            if stall.stalled(observed) {
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
    // Bounded by progress, with its own budget: the wait above may have used
    // most of the shared deadline on a loaded machine.
    let mut stall = Stall::new(Duration::from_secs(10));
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
        let observed = format!(
            "{:?}|{}|{}|{}|{:?}|{}|{:?}|{}|{}",
            observed_placement.state,
            observed_placement.version,
            observed_task.status,
            observed_task.version,
            observed_execution.status,
            observed_execution.execution_version,
            pending
                .iter()
                .map(JournalEntry::entry_id)
                .collect::<Vec<_>>(),
            fixture
                .link
                .as_ref()
                .map_or(0, |link| link.requests.lock().unwrap().len()),
            fixture.server_activity().await
        );
        if stall.stalled(observed) {
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
    // The push is still running 10 s after the Cancel was asked for. The
    // Cancel waits at most 15 s from its own enqueue, so the hold is measured
    // on the command's own clock (`created_at`), not from when this test saw
    // it: a slow machine then spends none of the remaining margin on the
    // detection above.
    let enqueued = db
        .task_steps(&task_id)
        .await
        .unwrap()
        .into_iter()
        .find(|s| {
            s.kind == "command"
                && serde_json::from_str::<Value>(&s.payload_json).unwrap()["preempt"] == true
        })
        .map(|s| {
            chrono::DateTime::parse_from_rfc3339(&s.created_at)
                .unwrap()
                .with_timezone(&chrono::Utc)
        })
        .expect("the preempting Cancel is enqueued");
    let hold_until = enqueued + chrono::Duration::milliseconds(10_200);
    while chrono::Utc::now() < hold_until {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(!cancel.is_finished(), "Cancel still waits for the push");
    assert!(
        chrono::Utc::now() < enqueued + chrono::Duration::seconds(14),
        "this machine was too slow to release the push inside the Cancel's own 15 s wait"
    );
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
                    candidate_transfer_key: None,
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
        candidate_transfer_key: None,
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
    let mut attempt = db::IntegrationAttempt::new(
        Some(queue.id.clone()),
        task.id,
        task.project_id,
        key.into(),
        task.status,
        epoch,
        task.version,
    );
    // What admission records about the owner of the Task's checkout. An
    // effect performed off the target's owner is admitted only against it.
    attempt.repo_location_id = Some(placement.repo_location_id.clone());
    attempt.owner_kind = Some(if placement.daemon_id.is_some() {
        db::IntegrationOwnerKind::Daemon
    } else {
        db::IntegrationOwnerKind::Server
    });
    attempt.daemon_id = placement.daemon_id.clone();
    attempt.runtime_id = placement.runtime_id.clone();
    let attempt = database.admit_integration_attempt(attempt).await.unwrap();
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

    // The server stopped between a transfer and its release: the attempt is
    // off the slot and its ref is still on the daemon-owned checkout. The
    // next start releases it there, by the release wire, per ended attempt.
    let has_ref = |reference: String| {
        std::process::Command::new("git")
            .arg("-C")
            .arg(&fixture.checkout)
            .args(["rev-parse", "--verify", "--quiet", &reference])
            .output()
            .unwrap()
            .status
            .success()
    };
    let inbound_ref = format!("refs/forge/integration/{inbound}");
    assert!(has_ref(inbound_ref.clone()));
    sqlx::query("UPDATE integration_attempt SET repo_location_id='server-clone',started_at=?,updated_at=?,revision=revision+1 WHERE id=?")
        .bind(&now).bind(db::now_rfc3339()).bind(&attempt_id)
        .execute(database.pool()).await.unwrap();
    sqlx::query("UPDATE integration_queue SET head_attempt_id=NULL,revision=revision+1 WHERE head_attempt_id=?")
        .bind(&attempt_id)
        .execute(database.pool()).await.unwrap();
    let transfer = services::integration_ports::OwnerObjectTransfer::new(
        database.clone(),
        Arc::new(ServerIntegrationOwner::new(database.clone())),
        client.clone(),
        Arc::new(services::integration_ports::DaemonFences::new(
            database.clone(),
            client.clone(),
        )),
        &fixture.server_root.path().join("sweep-staging"),
    );
    services::integration_worker::ObjectTransferPort::sweep_at_start(&transfer)
        .await
        .unwrap();
    assert!(
        !has_ref(inbound_ref),
        "the ended attempt's ref is gone from the daemon-owned checkout"
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

// ----- the integration queue worker on a daemon-owned target (3.2 D2a) -----

struct QueueHead {
    attempt_id: String,
    queue_id: String,
    task_id: String,
    candidate: String,
}

/// The fixture's Task, with a candidate commit, in `merging` with a passed
/// review and admitted to its repository's `main` queue.
async fn admit_queue_head(fixture: &Fixture, ci_steps: &[&str]) -> QueueHead {
    let candidate = fixture.candidate().await;
    admit_placed_head(fixture, &fixture.resolved.placement, candidate, ci_steps).await
}

/// The Task of `placement`, whose checkout holds `candidate`, in `merging`
/// with a passed review and admitted to its repository's `main` queue.
async fn admit_placed_head(
    fixture: &Fixture,
    placement: &db::WorkspacePlacement,
    candidate: String,
    ci_steps: &[&str],
) -> QueueHead {
    use db::IntegrationQueueRepo;
    let database = fixture.harness.state.db.clone();
    let placement = placement.clone();
    let workspace = WorkspaceRepo::get_by_id(&*database, &placement.workspace_id)
        .await
        .unwrap()
        .unwrap();
    let now = db::now_rfc3339();
    sqlx::query("UPDATE task SET status='merging',review_passed_at=?,task_state_config=?,updated_at=? WHERE id=?")
        .bind(&now)
        .bind(json!({"review":{"ci_steps":ci_steps}}).to_string())
        .bind(&now)
        .bind(&placement.task_id)
        .execute(database.pool())
        .await
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
    let mut attempt = db::IntegrationAttempt::new(
        Some(queue.id.clone()),
        task.id.clone(),
        task.project_id,
        format!("admit-{}", task.id),
        task.status,
        epoch,
        task.version,
    );
    attempt.original_candidate_sha = Some(candidate.clone());
    // What admission records about where the candidate lives.
    attempt.workspace_id = Some(workspace.id.clone());
    attempt.workspace_ref = Some(workspace.id.clone());
    attempt.placement_id = Some(placement.id.clone());
    attempt.placement_ref = Some(placement.id.clone());
    attempt.repo_location_id = Some(placement.repo_location_id.clone());
    attempt.repo_location_ref = Some(placement.repo_location_id.clone());
    attempt.owner_kind = Some(if placement.daemon_id.is_some() {
        db::IntegrationOwnerKind::Daemon
    } else {
        db::IntegrationOwnerKind::Server
    });
    attempt.daemon_id = placement.daemon_id.clone();
    attempt.runtime_id = placement.runtime_id.clone();
    attempt.placement_generation = Some(placement.generation);
    let attempt = database.admit_integration_attempt(attempt).await.unwrap();
    QueueHead {
        attempt_id: attempt.id,
        queue_id: queue.id,
        task_id: task.id,
        candidate,
    }
}

/// `build_integration_worker` over the harness's router and daemon registry,
/// running, with fast timers.
fn run_queue_worker(
    fixture: &Fixture,
    lease: Duration,
) -> (
    tokio::sync::watch::Sender<bool>,
    JoinHandle<services::Result<()>>,
) {
    use services::integration_worker::IntegrationWorkerConfig;
    let state = &fixture.harness.state;
    let worker = services::integration_ports::build_integration_worker(
        state.db.clone(),
        state.workspace_backend_router.clone(),
        state.daemon_connections.clone(),
        state.repo_location_service.clone(),
        IntegrationWorkerConfig {
            poll: Duration::from_millis(20),
            sweep_interval: Duration::from_millis(100),
            conflict_backoff: Duration::from_millis(5),
            reconcile_interval: Duration::from_millis(300),
            // A head left with an unknown result is taken up again when its
            // lease runs out.
            lease,
            renew_every: lease / 4,
            ..Default::default()
        },
    );
    let (stop, stopped) = tokio::sync::watch::channel(false);
    (stop, tokio::spawn(worker.run(stopped)))
}

async fn queue_attempt(fixture: &Fixture, head: &QueueHead) -> db::IntegrationAttempt {
    use db::IntegrationQueueRepo;
    fixture
        .harness
        .state
        .db
        .integration_attempt(&head.attempt_id)
        .await
        .unwrap()
        .unwrap()
}

async fn task_status(fixture: &Fixture, task_id: &str) -> String {
    sqlx::query_scalar("SELECT status FROM task WHERE id=?")
        .bind(task_id)
        .fetch_one(fixture.harness.state.db.pool())
        .await
        .unwrap()
}

/// Polls `done` for up to a minute, naming the attempt's state on a timeout.
macro_rules! queue_eventually {
    ($fixture:expr, $head:expr, $what:literal, $done:expr) => {{
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        while !$done {
            if tokio::time::Instant::now() >= deadline {
                let attempt = queue_attempt($fixture, $head).await;
                let receipts: Vec<String> = serde_json::from_value::<
                    Vec<db::IntegrationEffectReceipt>,
                >(attempt.effect_receipts_json.clone())
                .unwrap_or_default()
                .iter()
                .map(|receipt| {
                    format!(
                        "{:?}@{} {:?} {}",
                        receipt.request.kind,
                        receipt.request.fence.generation,
                        receipt.operation_state,
                        receipt.result
                    )
                })
                .collect();
                panic!(
                    "timed out waiting for {}: attempt {} ({:?}: {:?}), Task {}, receipts {:#?}",
                    $what,
                    attempt.state,
                    attempt.failure_kind,
                    attempt.failure_message,
                    task_status($fixture, &$head.task_id).await,
                    receipts
                );
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }};
}

/// Requests of `method` this link saw that were bound to a queue attempt.
fn attempt_requests(link: &DaemonLink, method: &str) -> Vec<Value> {
    link.requests(method)
        .into_iter()
        .filter(|params| params["integration"]["kind"] == "attempt")
        .collect()
}

fn fast_forward_receipts(attempt: &db::IntegrationAttempt) -> usize {
    serde_json::from_value::<Vec<db::IntegrationEffectReceipt>>(
        attempt.effect_receipts_json.clone(),
    )
    .unwrap()
    .into_iter()
    .filter(|receipt| {
        receipt.request.kind == db::IntegrationOperationKind::FastForward
            && receipt.operation_state == db::IntegrationOperationState::Succeeded
    })
    .count()
}

/// A head on a daemon-owned target no longer parks: the queue worker drives
/// the rebase, the check hand-off and the fast-forward through the daemon
/// that holds the default checkout, with a real `ci_steps` command run by the
/// real check runner on that daemon.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn queue_worker_rebases_checks_and_fast_forwards_on_a_daemon_owned_target() {
    let fixture = Fixture::file_backed("forge-queue-daemon-owned").await;
    // Passes only on the rebased commit: `outside.txt` arrives with the target.
    let head = admit_queue_head(&fixture, &["test -f feature.txt", "test -f outside.txt"]).await;
    std::fs::write(fixture.checkout.join("outside.txt"), "outside\n").unwrap();
    git::commit_all(&fixture.checkout, "outside").await.unwrap();
    let moved_tip = git::get_current_sha(&fixture.checkout).await.unwrap();
    // The check worker, as the server runtime starts it.
    let state = fixture.harness.state.clone();
    let periodic = services::worker_runtime::PeriodicWorkers::new(state.db.clone());
    let (stop_checks, checks_stopped) = tokio::sync::watch::channel(false);
    let _checks = state
        .task_service
        .compose_checks(state.daemon_connections.clone(), Duration::from_secs(60))
        .worker
        .start(&periodic, checks_stopped);

    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(60));
    queue_eventually!(&fixture, &head, "the Task to merge", {
        task_status(&fixture, &head.task_id).await == "done"
            && queue_attempt(&fixture, &head).await.state == db::IntegrationAttemptState::Completed
    });
    let _ = stop.send(true);
    let _ = stop_checks.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;

    let attempt = queue_attempt(&fixture, &head).await;
    let tip = git_sync(&fixture.checkout, &["rev-parse", "refs/heads/main"]);
    assert_eq!(attempt.integrated_sha.as_deref(), Some(tip.as_str()));
    assert_ne!(tip, head.candidate, "the queue rebased the candidate");
    assert_eq!(
        attempt.integrated_before_sha.as_deref(),
        Some(moved_tip.as_str())
    );
    for file in ["feature.txt", "outside.txt"] {
        git_sync(
            &fixture.checkout,
            &["cat-file", "-e", &format!("{tip}:{file}")],
        );
    }
    assert!(
        attempt.failure_message.is_none(),
        "never parked: {:?}",
        attempt.failure_message
    );
    // One rebase and one fast-forward, each an attempt effect on the owner.
    let link = fixture.link.as_ref().unwrap();
    let rebases = attempt_requests(link, METHOD_WORKSPACE_RESET);
    assert_eq!(rebases.len(), 1, "{rebases:?}");
    assert_eq!(rebases[0]["operation"]["kind"], "rebase_target");
    assert_eq!(attempt_requests(link, METHOD_WORKSPACE_MERGE).len(), 1);
    assert_eq!(fast_forward_receipts(&attempt), 1);
    // The claim generation was announced with the live queues before its
    // first effect.
    let announced = link.requests(METHOD_INTEGRATION_ANNOUNCE);
    assert!(
        announced.iter().any(|params| {
            params["live_queue_ids"]
                .as_array()
                .is_some_and(|live| live.iter().any(|id| id == head.queue_id.as_str()))
        }),
        "{announced:?}"
    );
    // The check ran on the rebased commit, on the daemon, and passed.
    let checks: Vec<(String, String)> = sqlx::query_as(
        "SELECT r.commit_sha, s.outcome FROM check_run r JOIN check_result s ON s.run_id=r.id",
    )
    .fetch_all(state.db.pool())
    .await
    .unwrap();
    assert_eq!(checks, vec![(tip.clone(), "pass".to_owned())]);
}

/// The red twin of the test above: the rebased commit's real `ci_steps`
/// command fails on the daemon. The head is ejected with the command's own
/// failure, the default checkout is not moved, and the Task is sent back.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn queue_worker_ejects_a_red_check_on_a_daemon_owned_target() {
    let fixture = Fixture::file_backed("forge-queue-daemon-red").await;
    let head = admit_queue_head(&fixture, &["test -f outside.txt", "exit 3"]).await;
    std::fs::write(fixture.checkout.join("outside.txt"), "outside\n").unwrap();
    git::commit_all(&fixture.checkout, "outside").await.unwrap();
    let moved_tip = git::get_current_sha(&fixture.checkout).await.unwrap();
    let state = fixture.harness.state.clone();
    let periodic = services::worker_runtime::PeriodicWorkers::new(state.db.clone());
    let (stop_checks, checks_stopped) = tokio::sync::watch::channel(false);
    let _checks = state
        .task_service
        .compose_checks(state.daemon_connections.clone(), Duration::from_secs(60))
        .worker
        .start(&periodic, checks_stopped);

    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(60));
    queue_eventually!(&fixture, &head, "the head to be ejected", {
        queue_attempt(&fixture, &head).await.state == db::IntegrationAttemptState::Ejected
            && task_status(&fixture, &head.task_id).await != "merging"
    });
    let _ = stop.send(true);
    let _ = stop_checks.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;

    let attempt = queue_attempt(&fixture, &head).await;
    assert_eq!(
        attempt.failure_kind,
        Some(db::IntegrationFailureKind::CandidateCheckFailed)
    );
    assert!(
        attempt
            .failure_message
            .as_deref()
            .is_some_and(|message| message.contains("exited 3")),
        "{:?}",
        attempt.failure_message
    );
    // Rebased on the daemon, checked there, never fast-forwarded.
    let link = fixture.link.as_ref().unwrap();
    assert_eq!(attempt_requests(link, METHOD_WORKSPACE_RESET).len(), 1);
    assert!(attempt_requests(link, METHOD_WORKSPACE_MERGE).is_empty());
    assert_eq!(fast_forward_receipts(&attempt), 0);
    assert_eq!(
        git_sync(&fixture.checkout, &["rev-parse", "refs/heads/main"]),
        moved_tip
    );
    assert_ne!(task_status(&fixture, &head.task_id).await, "done");
    let checks: Vec<(String, String)> = sqlx::query_as(
        "SELECT r.commit_sha, s.outcome FROM check_run r JOIN check_result s ON s.run_id=r.id",
    )
    .fetch_all(state.db.pool())
    .await
    .unwrap();
    assert_eq!(
        checks,
        vec![(attempt.candidate_sha.clone().unwrap(), "fail".to_owned())]
    );
    {
        use db::IntegrationQueueRepo;
        let queue = state
            .db
            .integration_queue(&head.queue_id)
            .await
            .unwrap()
            .unwrap();
        assert!(queue.head_attempt_id.is_none(), "the slot is free");
    }
}

/// A daemon-owned default checkout with uncommitted changes: the worker
/// reads that on the daemon and parks the head `target_dirty` at once, before
/// any effect, instead of spending rounds on the owner's refusals.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn queue_worker_parks_target_dirty_at_once_on_a_daemon_owned_target() {
    let fixture = Fixture::file_backed("forge-queue-daemon-dirty").await;
    let head = admit_queue_head(&fixture, &[]).await;
    std::fs::write(fixture.checkout.join("uncommitted.txt"), "dirty\n").unwrap();
    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(60));
    queue_eventually!(&fixture, &head, "the head to park", {
        queue_attempt(&fixture, &head).await.state == db::IntegrationAttemptState::Parked
    });
    let _ = stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    let attempt = queue_attempt(&fixture, &head).await;
    assert!(
        attempt
            .failure_message
            .as_deref()
            .is_some_and(|message| message.starts_with("target_dirty")),
        "{:?}",
        attempt.failure_message
    );
    assert_eq!(attempt.available_at, None, "the owner decides, no retry");
    assert_eq!(
        attempt.phase_timings.as_ref().map(|timings| timings.rounds),
        Some(1)
    );
    let link = fixture.link.as_ref().unwrap();
    assert!(attempt_requests(link, METHOD_WORKSPACE_MERGE).is_empty());
    assert!(attempt_requests(link, METHOD_WORKSPACE_RESET).is_empty());
    assert_eq!(attempt.effect_receipts_json, json!([]));
    assert_eq!(task_status(&fixture, &head.task_id).await, "merging");
}

/// The daemon finishes the queue's rebase but the reply is lost with the
/// connection. After the reconnect the worker reads the owner's receipt and
/// continues from it: the rebase is never sent again.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn daemon_disconnect_in_rebasing_reconciles_by_receipt_without_a_second_rebase() {
    let mut fixture = Fixture::file_backed("forge-queue-daemon-rebasing").await;
    let head = admit_queue_head(&fixture, &[]).await;
    std::fs::write(fixture.checkout.join("outside.txt"), "outside\n").unwrap();
    git::commit_all(&fixture.checkout, "outside").await.unwrap();
    fixture
        .link
        .as_ref()
        .unwrap()
        .drop_replies
        .lock()
        .unwrap()
        .insert(METHOD_WORKSPACE_RESET.into());
    // A lease ten times longer than any wait of this test (each is bounded
    // at a minute): a head that merges here was not retried by its lease
    // running out. No wall-clock bound is asserted; the machine may be busy.
    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(600));
    // The owner rebased; its reply never arrives.
    let rebased = async {
        loop {
            let sha = fixture
                .resolved
                .git_query(WorkspaceGitQuery::Head, false)
                .await
                .ok()
                .flatten()
                .unwrap_or_default();
            if !sha.trim().is_empty() && sha.trim() != head.candidate {
                return sha.trim().to_owned();
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    let rebased = tokio::time::timeout(Duration::from_secs(30), rebased)
        .await
        .expect("the owner rebases");
    let sent_before =
        attempt_requests(fixture.link.as_ref().unwrap(), METHOD_WORKSPACE_RESET).len();
    assert_eq!(sent_before, 1);
    fixture.link.take();
    common::fake_daemon::wait_until_disconnected(&fixture.harness.state, &fixture.daemon_id).await;
    // Unknown result: the attempt keeps its intent and nothing merges.
    queue_eventually!(&fixture, &head, "the head to hold an unknown rebase", {
        let attempt = queue_attempt(&fixture, &head).await;
        attempt.effect_intent_json.is_some()
            && matches!(
                attempt.state,
                db::IntegrationAttemptState::Reconciling | db::IntegrationAttemptState::Quarantined
            )
    });
    assert_eq!(task_status(&fixture, &head.task_id).await, "merging");

    fixture.reconnect().await;
    // The head goes on once the Task's placement is `ready` again (the
    // server runtime's owner reconciliation after a reconnect).
    fixture
        .reconcile_reconnected_placement(&fixture.resolved.placement.id)
        .await;
    // The owner's receipt is read once and the head continues from it: the
    // rebased commit becomes the candidate without another rebase.
    let rebase_receipts = |attempt: &db::IntegrationAttempt| {
        serde_json::from_value::<Vec<db::IntegrationEffectReceipt>>(
            attempt.effect_receipts_json.clone(),
        )
        .unwrap()
        .into_iter()
        .filter(|receipt| receipt.request.kind == db::IntegrationOperationKind::Rebase)
        .collect::<Vec<_>>()
    };
    queue_eventually!(
        &fixture,
        &head,
        "the head to continue from the owner's receipt",
        {
            let attempt = queue_attempt(&fixture, &head).await;
            attempt.candidate_sha.as_deref() == Some(rebased.as_str())
                && !matches!(
                    attempt.state,
                    db::IntegrationAttemptState::Reconciling
                        | db::IntegrationAttemptState::Quarantined
                        | db::IntegrationAttemptState::Rebasing
                )
        }
    );
    // And the Task merges: the fast-forward that follows the reconnect is
    // admitted by the owner (its record of the default checkout is the one
    // the server stored with the reconnect verification).
    queue_eventually!(&fixture, &head, "the Task to merge after the reconnect", {
        task_status(&fixture, &head.task_id).await == "done"
            && queue_attempt(&fixture, &head).await.state == db::IntegrationAttemptState::Completed
    });
    let _ = stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    let attempt = queue_attempt(&fixture, &head).await;
    let receipts = rebase_receipts(&attempt);
    assert_eq!(receipts.len(), 1, "one rebase receipt: {receipts:?}");
    assert_eq!(
        receipts[0].operation_state,
        db::IntegrationOperationState::Succeeded
    );
    assert_eq!(receipts[0].result["outcome"]["kind"], "rebased");
    let link = fixture.link.as_ref().unwrap();
    assert_eq!(
        attempt_requests(link, METHOD_WORKSPACE_RESET).len(),
        0,
        "the rebase was not repeated after the reconnect"
    );
    // Every effect ran once: one rebase receipt, one fast-forward receipt,
    // and no refusal in between.
    let all: Vec<db::IntegrationEffectReceipt> =
        serde_json::from_value(attempt.effect_receipts_json.clone()).unwrap();
    assert_eq!(all.len(), 2, "{all:?}");
    assert_eq!(fast_forward_receipts(&attempt), 1);
    assert_eq!(attempt_requests(link, METHOD_WORKSPACE_MERGE).len(), 1);
    assert_eq!(
        attempt.candidate_sha.as_deref(),
        Some(rebased.as_str()),
        "the one rebase is what merged"
    );
    assert_eq!(attempt.integrated_sha.as_deref(), Some(rebased.as_str()));
    assert_eq!(
        git_sync(&fixture.checkout, &["rev-parse", "refs/heads/main"]),
        rebased
    );
    assert!(
        attempt.failure_message.is_none(),
        "never parked: {:?}",
        attempt.failure_message
    );
}

/// The server changed its record of the default checkout after the owner last
/// verified it (a default toggle bumps the location version; the owner is not
/// told). The owner refuses the claim `foreign_owner`. The worker asks for the
/// location to be verified again and the head merges in a later round: it
/// does not park.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn queue_worker_reverifies_a_default_checkout_its_owner_does_not_recognise() {
    let fixture = Fixture::file_backed("forge-queue-daemon-location").await;
    let head = admit_queue_head(&fixture, &[]).await;
    let location_id = fixture.resolved.placement.repo_location_id.clone();
    sqlx::query("UPDATE repo_location SET version=version+1 WHERE id=?")
        .bind(&location_id)
        .execute(fixture.harness.state.db.pool())
        .await
        .unwrap();
    let verified_before = fixture
        .link
        .as_ref()
        .unwrap()
        .requests(METHOD_REPO_LOCATION_VERIFY)
        .len();
    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(60));
    queue_eventually!(&fixture, &head, "the Task to merge", {
        task_status(&fixture, &head.task_id).await == "done"
            && queue_attempt(&fixture, &head).await.state == db::IntegrationAttemptState::Completed
    });
    let _ = stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    let attempt = queue_attempt(&fixture, &head).await;
    let receipts: Vec<db::IntegrationEffectReceipt> =
        serde_json::from_value(attempt.effect_receipts_json.clone()).unwrap();
    let refused: Vec<_> = receipts
        .iter()
        .filter(|receipt| receipt.result["reason"] == "foreign_owner")
        .collect();
    assert_eq!(
        refused.len(),
        1,
        "one refusal, then a verification: {receipts:?}"
    );
    assert_eq!(fast_forward_receipts(&attempt), 1);
    let link = fixture.link.as_ref().unwrap();
    assert_eq!(
        link.requests(METHOD_REPO_LOCATION_VERIFY).len(),
        verified_before + 1,
        "the worker asked for one verification"
    );
    assert!(
        attempt.failure_message.is_none(),
        "never parked: {:?}",
        attempt.failure_message
    );
    assert_eq!(
        git_sync(&fixture.checkout, &["rev-parse", "refs/heads/main"]),
        head.candidate
    );
    // The owner's record and the server's agree again.
    let server: i64 = sqlx::query_scalar("SELECT version FROM repo_location WHERE id=?")
        .bind(&location_id)
        .fetch_one(fixture.harness.state.db.pool())
        .await
        .unwrap();
    let claimed: Vec<i64> = receipts
        .iter()
        .filter_map(|receipt| receipt.request.fence.target_owner["generation"].as_i64())
        .collect();
    assert_eq!(claimed.last(), Some(&server), "{claimed:?}");
}

/// The daemon fast-forwards the default checkout but the reply is lost with
/// the connection. The merge has landed; after the reconnect the worker
/// learns that from the owner's receipt and never fast-forwards again.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn daemon_disconnect_in_ff_inflight_reconciles_by_receipt_without_a_second_merge() {
    let mut fixture = Fixture::file_backed("forge-queue-daemon-ff").await;
    let head = admit_queue_head(&fixture, &[]).await;
    fixture
        .link
        .as_ref()
        .unwrap()
        .drop_replies
        .lock()
        .unwrap()
        .insert(METHOD_WORKSPACE_MERGE.into());
    // A lease ten times longer than any wait of this test (each is bounded
    // at a minute): a head that merges here was not retried by its lease
    // running out. No wall-clock bound is asserted; the machine may be busy.
    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(600));
    tokio::time::timeout(Duration::from_secs(30), async {
        while git::get_current_sha(&fixture.checkout).await.unwrap() != head.candidate {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the owner fast-forwards");
    assert_eq!(
        attempt_requests(fixture.link.as_ref().unwrap(), METHOD_WORKSPACE_MERGE).len(),
        1
    );
    fixture.link.take();
    common::fake_daemon::wait_until_disconnected(&fixture.harness.state, &fixture.daemon_id).await;
    queue_eventually!(
        &fixture,
        &head,
        "the head to hold an unknown fast-forward",
        {
            let attempt = queue_attempt(&fixture, &head).await;
            attempt.effect_intent_json.is_some()
                && matches!(
                    attempt.state,
                    db::IntegrationAttemptState::Reconciling
                        | db::IntegrationAttemptState::Quarantined
                )
        }
    );
    // The Task is not advanced on a guess, and not failed either.
    assert_eq!(task_status(&fixture, &head.task_id).await, "merging");
    let reflog = git_sync(&fixture.checkout, &["reflog", "--all"]);

    // While its result is unknown the head keeps the slot under one claim.
    let held = {
        use db::IntegrationQueueRepo;
        let database = &fixture.harness.state.db;
        let queue = database
            .integration_queue(&head.queue_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            queue.head_attempt_id.as_deref(),
            Some(head.attempt_id.as_str())
        );
        queue.fence_generation
    };
    fixture.reconnect().await;
    queue_eventually!(
        &fixture,
        &head,
        "the Task to be done after the reconnect",
        {
            task_status(&fixture, &head.task_id).await == "done"
                && queue_attempt(&fixture, &head).await.state
                    == db::IntegrationAttemptState::Completed
        }
    );
    let _ = stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    let attempt = queue_attempt(&fixture, &head).await;
    assert_eq!(
        attempt.integrated_sha.as_deref(),
        Some(head.candidate.as_str())
    );
    assert_eq!(fast_forward_receipts(&attempt), 1);
    // The same claim finished the head: no takeover was needed.
    let receipts: Vec<db::IntegrationEffectReceipt> =
        serde_json::from_value(attempt.effect_receipts_json.clone()).unwrap();
    assert_eq!(receipts.len(), 1, "{receipts:?}");
    assert_eq!(receipts[0].request.fence.generation, held);
    assert_eq!(
        attempt_requests(fixture.link.as_ref().unwrap(), METHOD_WORKSPACE_MERGE).len(),
        0,
        "the fast-forward was not repeated after the reconnect"
    );
    assert_eq!(git_sync(&fixture.checkout, &["reflog", "--all"]), reflog);
    {
        use db::IntegrationQueueRepo;
        let queue = fixture
            .harness
            .state
            .db
            .integration_queue(&head.queue_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(queue.state, db::IntegrationQueueState::Open);
        assert!(queue.head_attempt_id.is_none());
    }
}

// ----- cross-owner queue heads (3.2 D2a part 2) -----
//
// The Task's checkout and the repository's default checkout are held by
// different owners. The rebase and the head check run on the Task's owner,
// the fast-forward on the owner of the default checkout; each effect is
// requested from, journaled by and reconciled with the owner that ran it.

/// The check worker, as the server runtime starts it. `$stop` names the
/// sender that stops it.
macro_rules! start_check_worker {
    ($fixture:expr, $stop:ident) => {
        let check_state = $fixture.harness.state.clone();
        let check_periodic = services::worker_runtime::PeriodicWorkers::new(check_state.db.clone());
        let ($stop, checks_stopped) = tokio::sync::watch::channel(false);
        let _checks = check_state
            .task_service
            .compose_checks(
                check_state.daemon_connections.clone(),
                Duration::from_secs(60),
            )
            .worker
            .start(&check_periodic, checks_stopped);
    };
}

/// Makes a server-owned clone of the fixture's repository its default
/// checkout. The daemon-owned location the Task is placed in stays, no
/// longer the default. Call before the candidate is committed: the clone
/// then holds `main` only.
async fn server_owned_default(fixture: &Fixture) -> PathBuf {
    let database = fixture.harness.state.db.clone();
    let clone = fixture.server_root.path().join("default-checkout");
    git_sync(
        fixture.server_root.path(),
        &[
            "clone",
            "-q",
            "--single-branch",
            "--branch",
            "main",
            fixture.checkout.to_str().unwrap(),
            "default-checkout",
        ],
    );
    for (key, value) in [
        ("user.name", "Forge"),
        ("user.email", "forge@example.invalid"),
    ] {
        git_sync(&clone, &["config", key, value]);
    }
    let placement = &fixture.resolved.placement;
    let repo_id = WorkspaceRepo::get_by_id(&*database, &placement.workspace_id)
        .await
        .unwrap()
        .unwrap()
        .repo_id;
    sqlx::query("UPDATE repo_location SET is_default=0 WHERE id=?")
        .bind(&placement.repo_location_id)
        .execute(database.pool())
        .await
        .unwrap();
    let now = db::now_rfc3339();
    RepoLocationRepo::create(
        &*database,
        CreateRepoLocation {
            id: db::new_uuid_v4(),
            repo_id,
            owner_kind: RepoLocationOwnerKind::Server,
            daemon_id: None,
            runtime_id: None,
            path: clone.to_string_lossy().into_owned(),
            kind: RepoLocationKind::PrimaryCheckout,
            is_default: true,
            status: RepoLocationStatus::Ready,
            last_verified_at: Some(now.clone()),
            last_error: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("server-owned default location creates");
    clone
}

fn integration_refs(path: &std::path::Path) -> String {
    git_sync(
        path,
        &["for-each-ref", "--format=%(refname)", "refs/forge/"],
    )
}

fn attempt_receipts(attempt: &db::IntegrationAttempt) -> Vec<db::IntegrationEffectReceipt> {
    serde_json::from_value(attempt.effect_receipts_json.clone()).unwrap()
}

/// Receipts of `kind` and the daemon (or `None` for the server) that
/// performed each.
fn receipt_owners(
    attempt: &db::IntegrationAttempt,
    kind: db::IntegrationOperationKind,
) -> Vec<(db::IntegrationOperationState, Option<String>)> {
    attempt_receipts(attempt)
        .into_iter()
        .filter(|receipt| receipt.request.kind == kind)
        .map(|receipt| {
            (
                receipt.operation_state,
                receipt.request.effect_owner()["daemon_id"]
                    .as_str()
                    .map(str::to_owned),
            )
        })
        .collect()
}

/// Task on a daemon, default checkout on the server. The target moved, so
/// the head needs the whole path: the target tip goes to the daemon, the
/// rebase and the real head check run there, the exact rebased commit goes
/// to the server and is fast-forwarded there.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn queue_worker_merges_a_daemon_placed_task_into_a_server_owned_default_checkout() {
    use db::{IntegrationOperationKind as Kind, IntegrationOperationState as State};
    let fixture = Fixture::file_backed("forge-queue-cross-daemon-server").await;
    let default_checkout = server_owned_default(&fixture).await;
    let head = admit_queue_head(&fixture, &["test -f feature.txt", "test -f outside.txt"]).await;
    std::fs::write(default_checkout.join("outside.txt"), "outside\n").unwrap();
    git::commit_all(&default_checkout, "outside").await.unwrap();
    let moved_tip = git::get_current_sha(&default_checkout).await.unwrap();
    let task_location_tip = git_sync(&fixture.checkout, &["rev-parse", "refs/heads/main"]);
    start_check_worker!(fixture, stop_checks);

    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(60));
    queue_eventually!(&fixture, &head, "the Task to merge", {
        task_status(&fixture, &head.task_id).await == "done"
            && queue_attempt(&fixture, &head).await.state == db::IntegrationAttemptState::Completed
    });
    // The attempt left the slot: its transfer refs go on both ends.
    queue_eventually!(&fixture, &head, "the transfer refs to be released", {
        integration_refs(&default_checkout).is_empty()
            && integration_refs(&fixture.checkout).is_empty()
    });
    let _ = stop.send(true);
    let _ = stop_checks.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;

    let attempt = queue_attempt(&fixture, &head).await;
    assert!(
        attempt.failure_message.is_none(),
        "never parked: {:?}",
        attempt.failure_message
    );
    // The default checkout holds exactly the checked candidate.
    let tip = git_sync(&default_checkout, &["rev-parse", "refs/heads/main"]);
    assert_eq!(attempt.candidate_sha.as_deref(), Some(tip.as_str()));
    assert_eq!(attempt.integrated_sha.as_deref(), Some(tip.as_str()));
    assert_eq!(
        attempt.integrated_before_sha.as_deref(),
        Some(moved_tip.as_str())
    );
    assert_ne!(tip, head.candidate, "the queue rebased the candidate");
    assert_eq!(git_sync(&default_checkout, &["rev-parse", "HEAD"]), tip);
    for file in ["feature.txt", "outside.txt"] {
        assert!(default_checkout.join(file).exists(), "{file}");
    }
    // The Task's own location was neither merged nor advanced.
    assert_eq!(
        git_sync(&fixture.checkout, &["rev-parse", "refs/heads/main"]),
        task_location_tip
    );
    // One rebase, by the daemon; one fast-forward, by the server.
    assert_eq!(
        receipt_owners(&attempt, Kind::Rebase),
        vec![(State::Succeeded, Some(fixture.daemon_id.clone()))]
    );
    assert_eq!(
        receipt_owners(&attempt, Kind::FastForward),
        vec![(State::Succeeded, None)]
    );
    let link = fixture.link.as_ref().unwrap();
    assert_eq!(attempt_requests(link, METHOD_WORKSPACE_RESET).len(), 1);
    assert!(attempt_requests(link, METHOD_WORKSPACE_MERGE).is_empty());
    assert!(
        !link.requests(METHOD_INTEGRATION_ANNOUNCE).is_empty(),
        "the Task's owner was told the claim generation"
    );
    // The check ran on the rebased commit, on the daemon, and passed.
    let checks: Vec<(String, String)> = sqlx::query_as(
        "SELECT r.commit_sha, s.outcome FROM check_run r JOIN check_result s ON s.run_id=r.id",
    )
    .fetch_all(fixture.harness.state.db.pool())
    .await
    .unwrap();
    assert_eq!(checks, vec![(tip, "pass".to_owned())]);
}

/// A second Task of the fixture's Project, in `review`, with its workspace
/// row. The worktree path is filled in by the caller's placement.
async fn second_task(fixture: &Fixture, worktree_path: &str) -> (db::Task, db::Workspace) {
    let database = fixture.harness.state.db.clone();
    let first = TaskRepo::get_by_id(&*database, &fixture.resolved.placement.task_id, false)
        .await
        .unwrap()
        .unwrap();
    let repo_id = WorkspaceRepo::get_by_id(&*database, &fixture.resolved.placement.workspace_id)
        .await
        .unwrap()
        .unwrap()
        .repo_id;
    let now = db::now_rfc3339();
    let task = TaskRepo::create(
        &*database,
        CreateTask {
            id: db::new_uuid_v4(),
            project_id: first.project_id,
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "Task in another checkout".into(),
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
    .expect("second task creates");
    let workspace = WorkspaceRepo::create(
        &*database,
        CreateWorkspace {
            id: db::new_uuid_v4(),
            task_id: task.id.clone(),
            repo_id,
            worktree_path: worktree_path.to_owned(),
            branch: workspace::task_branch_name(&task.id),
            status: WorkspaceStatus::Creating,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("second workspace creates");
    (task, workspace)
}

fn clone_main(from: &std::path::Path, parent: &std::path::Path, name: &str) -> PathBuf {
    git_sync(
        parent,
        &[
            "clone",
            "-q",
            "--single-branch",
            "--branch",
            "main",
            from.to_str().unwrap(),
            name,
        ],
    );
    let clone = parent.join(name);
    for (key, value) in [
        ("user.name", "Forge"),
        ("user.email", "forge@example.invalid"),
    ] {
        git_sync(&clone, &["config", key, value]);
    }
    clone
}

/// A Task placed on the server, in a worktree of a server-owned clone of the
/// fixture's repository (not the default checkout), with a candidate commit.
/// Returns its placement, the clone and the candidate.
async fn server_placed_task(fixture: &Fixture) -> (db::WorkspacePlacement, PathBuf, String) {
    let database = fixture.harness.state.db.clone();
    let clone = clone_main(&fixture.checkout, fixture.server_root.path(), "task-clone");
    let worktree = fixture
        .server_root
        .path()
        .join("workspaces")
        .join("cross-owner-task");
    std::fs::create_dir_all(worktree.parent().unwrap()).unwrap();
    let (task, workspace) = second_task(fixture, worktree.to_str().unwrap()).await;
    git_sync(
        &clone,
        &[
            "worktree",
            "add",
            "-q",
            "-b",
            &workspace.branch,
            worktree.to_str().unwrap(),
            "main",
        ],
    );
    let now = db::now_rfc3339();
    let location = RepoLocationRepo::create(
        &*database,
        CreateRepoLocation {
            id: db::new_uuid_v4(),
            repo_id: workspace.repo_id.clone(),
            owner_kind: RepoLocationOwnerKind::Server,
            daemon_id: None,
            runtime_id: None,
            path: clone.to_string_lossy().into_owned(),
            kind: RepoLocationKind::PrimaryCheckout,
            is_default: false,
            status: RepoLocationStatus::Ready,
            last_verified_at: Some(now.clone()),
            last_error: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("server-owned Task location creates");
    let placement = WorkspacePlacementRepo::create(
        &*database,
        CreateWorkspacePlacement {
            id: db::new_uuid_v4(),
            workspace_id: workspace.id.clone(),
            task_id: task.id.clone(),
            agent_id: None,
            owner_kind: PlacementOwnerKind::Server,
            daemon_id: None,
            runtime_id: None,
            repo_location_id: location.id,
            execution_daemon_id: None,
            workspace_handle: Some(worktree.to_string_lossy().into_owned()),
            generation: 1,
            state: PlacementState::Ready,
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
    .expect("server placement creates");
    WorkspaceRepo::update_status(
        &*database,
        &workspace.id,
        WorkspaceStatus::Ready,
        None,
        &now,
    )
    .await
    .expect("workspace is ready");
    std::fs::write(worktree.join("feature.txt"), "candidate\n").unwrap();
    git_sync(&worktree, &["add", "feature.txt"]);
    git_sync(&worktree, &["commit", "-q", "-m", "candidate"]);
    let candidate = git_sync(&worktree, &["rev-parse", "HEAD"]);
    (placement, clone, candidate)
}

/// A second connected daemon with its own workspace root.
struct ExtraDaemon {
    link: Option<DaemonLink>,
    runtime: Arc<DaemonRuntime>,
    daemon_id: String,
    runtime_id: String,
    token: String,
    root: TestDir,
}

async fn extra_daemon(fixture: &Fixture, suite: &str) -> ExtraDaemon {
    let harness = &fixture.harness;
    let root = TestDir::new(&format!("{suite}-second-owner"));
    let registration =
        common::fake_daemon::register_daemon(&harness.app, &db::new_uuid_v4(), suite).await;
    common::fake_daemon::report_remote_daemon_shell(
        &harness.app,
        &registration.daemon_id,
        &registration.registration_token,
        root.path(),
        suite,
    )
    .await;
    let runtime_id = RuntimeRepo::get_by_daemon_id(&*harness.state.db, &registration.daemon_id)
        .await
        .expect("runtime loads")
        .expect("runtime exists")
        .id;
    let (outbound, _discarded) = mpsc::unbounded_channel();
    let runtime = DaemonRuntime::new_owned(
        outbound,
        root.path().to_path_buf(),
        ActiveExecutionTracker::default(),
        registration.daemon_id.clone(),
        WorkspaceRunPolicy {
            allowed_purposes: vec![
                WorkspaceRunPurpose::CiStep,
                WorkspaceRunPurpose::Hook,
                WorkspaceRunPurpose::EnvironmentSetup,
            ],
        },
    )
    .expect("second owner runtime starts");
    let link = DaemonLink::connect(
        &fixture.server,
        &registration.daemon_id,
        &registration.registration_token,
        Arc::clone(&runtime),
    )
    .await;
    ExtraDaemon {
        link: Some(link),
        runtime,
        daemon_id: registration.daemon_id,
        runtime_id,
        token: registration.registration_token,
        root,
    }
}

/// A Task placed on a daemon, in a worktree of a clone of the fixture's
/// repository under `root` (a location of that daemon, not the default
/// checkout), prepared by the daemon, with a candidate commit.
async fn daemon_clone_task(
    fixture: &Fixture,
    daemon_id: &str,
    runtime_id: &str,
    root: &std::path::Path,
) -> (ResolvedWorkspace, PathBuf, String) {
    daemon_task(fixture, daemon_id, runtime_id, Some(root), "feature.txt").await
}

/// A second Task placed on a daemon with a candidate commit that adds
/// `file`: in a worktree of a new clone under `clone_root` (a location of
/// that daemon, not the default checkout), or, without one, in another
/// worktree of the fixture's own location.
async fn daemon_task(
    fixture: &Fixture,
    daemon_id: &str,
    runtime_id: &str,
    clone_root: Option<&std::path::Path>,
    file: &str,
) -> (ResolvedWorkspace, PathBuf, String) {
    let database = fixture.harness.state.db.clone();
    let clone = match clone_root {
        Some(root) => clone_main(&fixture.checkout, root, "task-clone"),
        None => fixture.checkout.clone(),
    };
    let (task, workspace) = second_task(
        fixture,
        &fixture
            .server_root
            .path()
            .join("missing-checkout/second")
            .to_string_lossy(),
    )
    .await;
    let now = db::now_rfc3339();
    let location_id = match clone_root {
        None => fixture.resolved.placement.repo_location_id.clone(),
        Some(_) => {
            RepoLocationRepo::create(
                &*database,
                CreateRepoLocation {
                    id: db::new_uuid_v4(),
                    repo_id: workspace.repo_id.clone(),
                    owner_kind: RepoLocationOwnerKind::Daemon,
                    daemon_id: Some(daemon_id.to_owned()),
                    runtime_id: Some(runtime_id.to_owned()),
                    path: clone.to_string_lossy().into_owned(),
                    kind: RepoLocationKind::PrimaryCheckout,
                    is_default: false,
                    status: RepoLocationStatus::Ready,
                    last_verified_at: Some(now.clone()),
                    last_error: None,
                    created_at: now.clone(),
                    updated_at: now.clone(),
                },
            )
            .await
            .expect("second daemon location creates")
            .id
        }
    };
    let location = RepoLocationRepo::get_by_id(&*database, &location_id)
        .await
        .unwrap()
        .unwrap();
    let placement = WorkspacePlacementRepo::create(
        &*database,
        CreateWorkspacePlacement {
            id: db::new_uuid_v4(),
            workspace_id: workspace.id.clone(),
            task_id: task.id.clone(),
            agent_id: None,
            owner_kind: PlacementOwnerKind::Daemon,
            daemon_id: Some(daemon_id.to_owned()),
            runtime_id: Some(runtime_id.to_owned()),
            repo_location_id: location.id.clone(),
            execution_daemon_id: Some(daemon_id.to_owned()),
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
    .expect("second daemon placement creates");
    let backend = fixture
        .harness
        .state
        .workspace_backend_router
        .for_placement(&placement)
        .expect("daemon backend is installed");
    if clone_root.is_some() {
        backend
            .daemon_client()
            .expect("owner client")
            .verify_location(
                daemon_id,
                RepoLocationVerifyParams {
                    repo_location_id: location.id,
                    daemon_id: daemon_id.to_owned(),
                    runtime_id: runtime_id.to_owned(),
                    path: clone.to_string_lossy().into_owned(),
                    kind: DaemonRepoLocationKind::PrimaryCheckout,
                    default_branch: "main".into(),
                    remote_url: None,
                    expected_version: location.version,
                    probe: None,
                },
            )
            .await
            .expect("owner verifies the Task's clone");
    }
    let prepared = backend
        .prepare(
            &placement,
            &PrepareSpec {
                base_ref: "main".into(),
            },
        )
        .await
        .expect("owner prepares a worktree in the clone");
    let placement = WorkspacePlacementRepo::update(
        &*database,
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
    .expect("prepared handle persists");
    WorkspaceRepo::update_status(
        &*database,
        &workspace.id,
        WorkspaceStatus::Ready,
        None,
        &now,
    )
    .await
    .expect("workspace is ready");
    let resolved = ResolvedWorkspace { placement, backend };
    let committed = resolved
        .backend
        .run(
            &resolved.placement,
            &RunSpec {
                purpose: WorkspaceRunPurpose::CiStep,
                command: format!(
                    "printf 'candidate\\n' > {file}; git add {file}; git commit -m candidate"
                ),
                env: BTreeMap::new(),
                timeout_secs: 0,
                max_output_bytes: usize::MAX,
            },
        )
        .await
        .expect("owner command succeeds");
    assert_eq!(committed.exit_code, 0);
    let candidate = resolved
        .git_query(WorkspaceGitQuery::Head, false)
        .await
        .unwrap()
        .unwrap()
        .trim()
        .to_owned();
    (resolved, clone, candidate)
}

/// What every merged cross-checkout head must show: the default checkout
/// holds exactly the checked candidate, and the transfer refs are gone.
async fn assert_landed_exactly(
    fixture: &Fixture,
    head: &QueueHead,
    default_checkout: &std::path::Path,
    moved_tip: &str,
) -> db::IntegrationAttempt {
    let attempt = queue_attempt(fixture, head).await;
    assert!(
        attempt.failure_message.is_none(),
        "never parked: {:?}",
        attempt.failure_message
    );
    let tip = git_sync(default_checkout, &["rev-parse", "refs/heads/main"]);
    assert_eq!(attempt.candidate_sha.as_deref(), Some(tip.as_str()));
    assert_eq!(attempt.integrated_sha.as_deref(), Some(tip.as_str()));
    assert_eq!(attempt.integrated_before_sha.as_deref(), Some(moved_tip));
    assert_ne!(tip, head.candidate, "the queue rebased the candidate");
    assert_eq!(git_sync(default_checkout, &["rev-parse", "HEAD"]), tip);
    for file in ["feature.txt", "outside.txt"] {
        assert!(default_checkout.join(file).exists(), "{file}");
    }
    assert!(integration_refs(default_checkout).is_empty());
    attempt
}

/// The reverse: Task on the server, default checkout on a daemon. The
/// rebase runs on the server against the transferred target tip; the daemon
/// fast-forwards its checkout from the transferred candidate, without a
/// worktree of the Task.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn queue_worker_merges_a_server_placed_task_into_a_daemon_owned_default_checkout() {
    use db::{IntegrationOperationKind as Kind, IntegrationOperationState as State};
    let fixture = Fixture::file_backed("forge-queue-cross-server-daemon").await;
    let (placement, task_clone, candidate) = server_placed_task(&fixture).await;
    let head = admit_placed_head(
        &fixture,
        &placement,
        candidate,
        &["test -f feature.txt", "test -f outside.txt"],
    )
    .await;
    std::fs::write(fixture.checkout.join("outside.txt"), "outside\n").unwrap();
    git::commit_all(&fixture.checkout, "outside").await.unwrap();
    let moved_tip = git::get_current_sha(&fixture.checkout).await.unwrap();
    let task_location_tip = git_sync(&task_clone, &["rev-parse", "refs/heads/main"]);
    start_check_worker!(fixture, stop_checks);

    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(60));
    queue_eventually!(&fixture, &head, "the Task to merge", {
        task_status(&fixture, &head.task_id).await == "done"
            && queue_attempt(&fixture, &head).await.state == db::IntegrationAttemptState::Completed
    });
    queue_eventually!(&fixture, &head, "the transfer refs to be released", {
        integration_refs(&fixture.checkout).is_empty() && integration_refs(&task_clone).is_empty()
    });
    let _ = stop.send(true);
    let _ = stop_checks.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;

    let attempt = assert_landed_exactly(&fixture, &head, &fixture.checkout, &moved_tip).await;
    assert_eq!(
        git_sync(&task_clone, &["rev-parse", "refs/heads/main"]),
        task_location_tip,
        "the Task's own location was not advanced"
    );
    // One rebase, by the server; one fast-forward, by the daemon.
    assert_eq!(
        receipt_owners(&attempt, Kind::Rebase),
        vec![(State::Succeeded, None)]
    );
    assert_eq!(
        receipt_owners(&attempt, Kind::FastForward),
        vec![(State::Succeeded, Some(fixture.daemon_id.clone()))]
    );
    let link = fixture.link.as_ref().unwrap();
    assert!(attempt_requests(link, METHOD_WORKSPACE_RESET).is_empty());
    let merges = attempt_requests(link, METHOD_WORKSPACE_MERGE);
    assert_eq!(merges.len(), 1, "{merges:?}");
    assert!(merges[0]["candidate_transfer_key"].is_string());
    assert_eq!(merges[0]["workspace_handle"], "");
}

/// Two daemons: the Task on one, the default checkout on the other.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn queue_worker_merges_across_two_daemons() {
    use db::{IntegrationOperationKind as Kind, IntegrationOperationState as State};
    let fixture = Fixture::file_backed("forge-queue-cross-two-daemons").await;
    let second = extra_daemon(&fixture, "forge-queue-cross-two-daemons").await;
    let (resolved, task_clone, candidate) = daemon_clone_task(
        &fixture,
        &second.daemon_id,
        &second.runtime_id,
        second.root.path(),
    )
    .await;
    let head = admit_placed_head(
        &fixture,
        &resolved.placement,
        candidate,
        &["test -f feature.txt", "test -f outside.txt"],
    )
    .await;
    std::fs::write(fixture.checkout.join("outside.txt"), "outside\n").unwrap();
    git::commit_all(&fixture.checkout, "outside").await.unwrap();
    let moved_tip = git::get_current_sha(&fixture.checkout).await.unwrap();
    let task_location_tip = git_sync(&task_clone, &["rev-parse", "refs/heads/main"]);
    start_check_worker!(fixture, stop_checks);

    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(60));
    queue_eventually!(&fixture, &head, "the Task to merge", {
        task_status(&fixture, &head.task_id).await == "done"
            && queue_attempt(&fixture, &head).await.state == db::IntegrationAttemptState::Completed
    });
    queue_eventually!(&fixture, &head, "the transfer refs to be released", {
        integration_refs(&fixture.checkout).is_empty() && integration_refs(&task_clone).is_empty()
    });
    let _ = stop.send(true);
    let _ = stop_checks.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;

    let attempt = assert_landed_exactly(&fixture, &head, &fixture.checkout, &moved_tip).await;
    assert_eq!(
        git_sync(&task_clone, &["rev-parse", "refs/heads/main"]),
        task_location_tip,
        "the Task's own location was not advanced"
    );
    assert_eq!(
        receipt_owners(&attempt, Kind::Rebase),
        vec![(State::Succeeded, Some(second.daemon_id.clone()))]
    );
    assert_eq!(
        receipt_owners(&attempt, Kind::FastForward),
        vec![(State::Succeeded, Some(fixture.daemon_id.clone()))]
    );
    // Each owner was asked for its own effect only, after it was told the
    // claim generation.
    let target = fixture.link.as_ref().unwrap();
    let tasks = second.link.as_ref().unwrap();
    assert!(attempt_requests(target, METHOD_WORKSPACE_RESET).is_empty());
    assert_eq!(attempt_requests(target, METHOD_WORKSPACE_MERGE).len(), 1);
    assert_eq!(attempt_requests(tasks, METHOD_WORKSPACE_RESET).len(), 1);
    assert!(attempt_requests(tasks, METHOD_WORKSPACE_MERGE).is_empty());
    for link in [target, tasks] {
        assert!(!link.requests(METHOD_INTEGRATION_ANNOUNCE).is_empty());
    }
    // The check ran where the Task is.
    assert!(!tasks.requests(METHOD_CHECK_RUN).is_empty());
    assert!(target.requests(METHOD_CHECK_RUN).is_empty());
    let _ = (&second.runtime, &second.token);
}

/// Two daemons, red head check: the Task's daemon rebases and runs the
/// check, which fails. The Task is sent back; the other daemon's default
/// checkout is never asked to fast-forward and holds no transfer ref.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn two_daemons_red_head_check_sends_the_task_back_without_a_merge() {
    use db::IntegrationOperationKind as Kind;
    let fixture = Fixture::file_backed("forge-queue-two-daemons-red").await;
    let second = extra_daemon(&fixture, "forge-queue-two-daemons-red").await;
    let (resolved, task_clone, candidate) = daemon_clone_task(
        &fixture,
        &second.daemon_id,
        &second.runtime_id,
        second.root.path(),
    )
    .await;
    let head = admit_placed_head(
        &fixture,
        &resolved.placement,
        candidate,
        &["test -f outside.txt", "exit 3"],
    )
    .await;
    std::fs::write(fixture.checkout.join("outside.txt"), "outside\n").unwrap();
    git::commit_all(&fixture.checkout, "outside").await.unwrap();
    let moved_tip = git::get_current_sha(&fixture.checkout).await.unwrap();
    start_check_worker!(fixture, stop_checks);

    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(60));
    queue_eventually!(&fixture, &head, "the head to be sent back", {
        queue_attempt(&fixture, &head).await.state == db::IntegrationAttemptState::Ejected
            && task_status(&fixture, &head.task_id).await != "merging"
    });
    queue_eventually!(&fixture, &head, "the transfer refs to be released", {
        integration_refs(&fixture.checkout).is_empty() && integration_refs(&task_clone).is_empty()
    });
    let _ = stop.send(true);
    let _ = stop_checks.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;

    let attempt = queue_attempt(&fixture, &head).await;
    assert_eq!(
        attempt.failure_kind,
        Some(db::IntegrationFailureKind::CandidateCheckFailed)
    );
    assert_ne!(task_status(&fixture, &head.task_id).await, "done");
    assert_eq!(
        succeeded(&attempt, Kind::Rebase),
        vec![Some(second.daemon_id.clone())]
    );
    assert!(receipt_owners(&attempt, Kind::FastForward).is_empty());
    let target = fixture.link.as_ref().unwrap();
    let tasks = second.link.as_ref().unwrap();
    assert!(attempt_requests(target, METHOD_WORKSPACE_MERGE).is_empty());
    assert!(attempt_requests(target, METHOD_WORKSPACE_RESET).is_empty());
    assert!(!tasks.requests(METHOD_CHECK_RUN).is_empty());
    assert!(target.requests(METHOD_CHECK_RUN).is_empty());
    assert_eq!(
        git_sync(&fixture.checkout, &["rev-parse", "refs/heads/main"]),
        moved_tip
    );
    assert!(
        git_sync(&fixture.checkout, &["status", "--porcelain"]).is_empty(),
        "the default checkout is untouched"
    );
    let _ = (&second.runtime, &second.token);
}

/// Two daemons, conflict on the rebase: the Task's daemon rebases onto the
/// tip the other daemon's default checkout sent, commits the conflict for
/// hand-off, and the Task goes back to its coder with the conflicted path.
/// The default checkout's daemon performs no effect.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn two_daemons_rebase_conflict_sends_the_task_back_with_the_conflict_handoff() {
    use db::IntegrationOperationKind as Kind;
    let fixture = Fixture::file_backed("forge-queue-two-daemons-conflict").await;
    let second = extra_daemon(&fixture, "forge-queue-two-daemons-conflict").await;
    let (resolved, _task_clone, candidate) = daemon_clone_task(
        &fixture,
        &second.daemon_id,
        &second.runtime_id,
        second.root.path(),
    )
    .await;
    let head = admit_placed_head(&fixture, &resolved.placement, candidate, &[]).await;
    // The same file the candidate adds, with other content.
    std::fs::write(fixture.checkout.join("feature.txt"), "theirs\n").unwrap();
    git::commit_all(&fixture.checkout, "outside").await.unwrap();
    let moved_tip = git::get_current_sha(&fixture.checkout).await.unwrap();

    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(60));
    queue_eventually!(&fixture, &head, "the conflict to be handed off", {
        queue_attempt(&fixture, &head).await.state == db::IntegrationAttemptState::Ejected
            && task_status(&fixture, &head.task_id).await == "merge_failed"
    });
    let _ = stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;

    let attempt = queue_attempt(&fixture, &head).await;
    assert_eq!(attempt.conflict_paths_json, Some(json!(["feature.txt"])));
    assert_eq!(
        succeeded(&attempt, Kind::Rebase),
        vec![Some(second.daemon_id.clone())]
    );
    assert!(receipt_owners(&attempt, Kind::FastForward).is_empty());
    let target = fixture.link.as_ref().unwrap();
    assert!(attempt_requests(target, METHOD_WORKSPACE_MERGE).is_empty());
    assert!(attempt_requests(target, METHOD_WORKSPACE_RESET).is_empty());
    assert_eq!(
        git_sync(&fixture.checkout, &["rev-parse", "refs/heads/main"]),
        moved_tip
    );
    assert_eq!(
        std::fs::read_to_string(fixture.checkout.join("feature.txt")).unwrap(),
        "theirs\n"
    );
    let _ = (&second.runtime, &second.token);
}

/// Two daemons, lost fast-forward reply: the Task is on one daemon, the
/// default checkout on the other, whose reply is lost with its connection.
/// The Task's daemon stays connected and is never asked about that effect;
/// the receipt comes from the daemon that performed it when it is back, and
/// the branch is moved once.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn two_daemons_lost_fast_forward_reply_is_settled_by_the_target_daemon_alone() {
    use db::IntegrationOperationKind as Kind;
    let mut fixture = Fixture::file_backed("forge-queue-two-daemons-ff-lost").await;
    let second = extra_daemon(&fixture, "forge-queue-two-daemons-ff-lost").await;
    let (resolved, task_clone, candidate) = daemon_clone_task(
        &fixture,
        &second.daemon_id,
        &second.runtime_id,
        second.root.path(),
    )
    .await;
    let head = admit_placed_head(&fixture, &resolved.placement, candidate, &[]).await;
    fixture
        .link
        .as_ref()
        .unwrap()
        .drop_replies
        .lock()
        .unwrap()
        .insert(METHOD_WORKSPACE_MERGE.into());
    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(600));
    tokio::time::timeout(Duration::from_secs(30), async {
        while git::get_current_sha(&fixture.checkout).await.unwrap() != head.candidate {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the target's daemon fast-forwards the transferred candidate");
    assert_eq!(
        attempt_requests(fixture.link.as_ref().unwrap(), METHOD_WORKSPACE_MERGE).len(),
        1
    );
    fixture.link.take();
    common::fake_daemon::wait_until_disconnected(&fixture.harness.state, &fixture.daemon_id).await;
    queue_eventually!(
        &fixture,
        &head,
        "the head to hold an unknown fast-forward",
        {
            let attempt = queue_attempt(&fixture, &head).await;
            attempt.effect_intent_json.is_some()
                && matches!(
                    attempt.state,
                    db::IntegrationAttemptState::Reconciling
                        | db::IntegrationAttemptState::Quarantined
                )
        }
    );
    {
        // The unknown effect belongs to the default checkout's daemon. The
        // Task's daemon is connected and has nothing to say about it: the
        // head stays unknown until the performing owner is back.
        let attempt = queue_attempt(&fixture, &head).await;
        let intent = attempt.effect_intent_json.clone().unwrap();
        assert!(intent["request"]["witness"].get("effect_owner").is_none());
        assert_eq!(
            intent["request"]["fence"]["target_owner"]["daemon_id"],
            json!(fixture.daemon_id)
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(queue_attempt(&fixture, &head)
            .await
            .effect_intent_json
            .is_some());
        assert_eq!(task_status(&fixture, &head.task_id).await, "merging");
    }
    let reflog = git_sync(&fixture.checkout, &["reflog", "--all"]);
    fixture.reconnect().await;
    queue_eventually!(
        &fixture,
        &head,
        "the Task to be done after the reconnect",
        {
            task_status(&fixture, &head.task_id).await == "done"
                && queue_attempt(&fixture, &head).await.state
                    == db::IntegrationAttemptState::Completed
        }
    );
    let _ = stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    let attempt = queue_attempt(&fixture, &head).await;
    assert_eq!(
        attempt.integrated_sha.as_deref(),
        Some(head.candidate.as_str())
    );
    assert_eq!(
        receipt_owners(&attempt, Kind::FastForward),
        vec![(
            db::IntegrationOperationState::Succeeded,
            Some(fixture.daemon_id.clone())
        )]
    );
    assert!(
        attempt_requests(fixture.link.as_ref().unwrap(), METHOD_WORKSPACE_MERGE).is_empty(),
        "the fast-forward was not repeated after the reconnect"
    );
    assert!(
        attempt_requests(second.link.as_ref().unwrap(), METHOD_WORKSPACE_MERGE).is_empty(),
        "the Task's daemon was never asked to fast-forward"
    );
    assert_eq!(git_sync(&fixture.checkout, &["reflog", "--all"]), reflog);
    queue_eventually!(&fixture, &head, "the transfer refs to be released", {
        integration_refs(&fixture.checkout).is_empty() && integration_refs(&task_clone).is_empty()
    });
    let _ = (&second.runtime, &second.token);
}

/// Two clones on ONE daemon: the Task in one, the default checkout in the
/// other. One owner performs both effects; the fast-forward takes the
/// candidate from the transferred ref (the clones share no Git common dir).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn queue_worker_merges_between_two_clones_on_one_daemon() {
    use db::{IntegrationOperationKind as Kind, IntegrationOperationState as State};
    let fixture = Fixture::file_backed("forge-queue-cross-clone-daemon").await;
    let runtime_id = fixture.resolved.placement.runtime_id.clone().unwrap();
    let (resolved, task_clone, candidate) = daemon_clone_task(
        &fixture,
        &fixture.daemon_id,
        &runtime_id,
        fixture._daemon_root.path(),
    )
    .await;
    let head = admit_placed_head(
        &fixture,
        &resolved.placement,
        candidate,
        &["test -f feature.txt", "test -f outside.txt"],
    )
    .await;
    std::fs::write(fixture.checkout.join("outside.txt"), "outside\n").unwrap();
    git::commit_all(&fixture.checkout, "outside").await.unwrap();
    let moved_tip = git::get_current_sha(&fixture.checkout).await.unwrap();
    let task_location_tip = git_sync(&task_clone, &["rev-parse", "refs/heads/main"]);
    start_check_worker!(fixture, stop_checks);

    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(60));
    queue_eventually!(&fixture, &head, "the Task to merge", {
        task_status(&fixture, &head.task_id).await == "done"
            && queue_attempt(&fixture, &head).await.state == db::IntegrationAttemptState::Completed
    });
    queue_eventually!(&fixture, &head, "the transfer refs to be released", {
        integration_refs(&fixture.checkout).is_empty() && integration_refs(&task_clone).is_empty()
    });
    let _ = stop.send(true);
    let _ = stop_checks.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;

    let attempt = assert_landed_exactly(&fixture, &head, &fixture.checkout, &moved_tip).await;
    assert_eq!(
        git_sync(&task_clone, &["rev-parse", "refs/heads/main"]),
        task_location_tip,
        "the Task's own clone was not advanced"
    );
    let owner = Some(fixture.daemon_id.clone());
    assert_eq!(
        receipt_owners(&attempt, Kind::Rebase),
        vec![(State::Succeeded, owner.clone())]
    );
    assert_eq!(
        receipt_owners(&attempt, Kind::FastForward),
        vec![(State::Succeeded, owner)]
    );
    let link = fixture.link.as_ref().unwrap();
    assert_eq!(attempt_requests(link, METHOD_WORKSPACE_RESET).len(), 1);
    let merges = attempt_requests(link, METHOD_WORKSPACE_MERGE);
    assert_eq!(merges.len(), 1, "{merges:?}");
    assert!(merges[0]["candidate_transfer_key"].is_string());
}

fn succeeded(
    attempt: &db::IntegrationAttempt,
    kind: db::IntegrationOperationKind,
) -> Vec<Option<String>> {
    receipt_owners(attempt, kind)
        .into_iter()
        .filter(|(state, _)| *state == db::IntegrationOperationState::Succeeded)
        .map(|(_, owner)| owner)
        .collect()
}

/// A red head check across owners: the rebased commit fails its real
/// `ci_steps` command on the Task's daemon. The head is sent back; the
/// server's default checkout is never touched and nothing is transferred
/// to it.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cross_owner_red_head_check_sends_the_task_back_without_a_merge() {
    use db::IntegrationOperationKind as Kind;
    let fixture = Fixture::file_backed("forge-queue-cross-red").await;
    let default_checkout = server_owned_default(&fixture).await;
    let head = admit_queue_head(&fixture, &["test -f outside.txt", "exit 3"]).await;
    std::fs::write(default_checkout.join("outside.txt"), "outside\n").unwrap();
    git::commit_all(&default_checkout, "outside").await.unwrap();
    let moved_tip = git::get_current_sha(&default_checkout).await.unwrap();
    start_check_worker!(fixture, stop_checks);

    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(60));
    queue_eventually!(&fixture, &head, "the head to be sent back", {
        queue_attempt(&fixture, &head).await.state == db::IntegrationAttemptState::Ejected
            && task_status(&fixture, &head.task_id).await != "merging"
    });
    queue_eventually!(&fixture, &head, "the transfer refs to be released", {
        integration_refs(&default_checkout).is_empty()
            && integration_refs(&fixture.checkout).is_empty()
    });
    let _ = stop.send(true);
    let _ = stop_checks.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;

    let attempt = queue_attempt(&fixture, &head).await;
    assert_eq!(
        attempt.failure_kind,
        Some(db::IntegrationFailureKind::CandidateCheckFailed)
    );
    assert!(
        attempt
            .failure_message
            .as_deref()
            .is_some_and(|message| message.contains("exited 3")),
        "{:?}",
        attempt.failure_message
    );
    assert_ne!(task_status(&fixture, &head.task_id).await, "done");
    assert_eq!(
        succeeded(&attempt, Kind::Rebase),
        vec![Some(fixture.daemon_id.clone())]
    );
    assert!(receipt_owners(&attempt, Kind::FastForward).is_empty());
    assert_eq!(
        git_sync(&default_checkout, &["rev-parse", "refs/heads/main"]),
        moved_tip
    );
    assert!(
        git_sync(&default_checkout, &["status", "--porcelain"]).is_empty(),
        "the default checkout is untouched"
    );
}

/// A conflict on the cross-owner rebase: the Task's daemon rebases onto the
/// transferred tip, commits the conflict for hand-off, and the Task is sent
/// back to its coder with the conflicted paths. Nothing reaches the server's
/// default checkout.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cross_owner_rebase_conflict_sends_the_task_back_with_the_conflict_handoff() {
    use db::IntegrationOperationKind as Kind;
    let fixture = Fixture::file_backed("forge-queue-cross-conflict").await;
    let default_checkout = server_owned_default(&fixture).await;
    let head = admit_queue_head(&fixture, &[]).await;
    // The same file the candidate adds, with other content.
    std::fs::write(default_checkout.join("feature.txt"), "theirs\n").unwrap();
    git::commit_all(&default_checkout, "outside").await.unwrap();
    let moved_tip = git::get_current_sha(&default_checkout).await.unwrap();

    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(60));
    queue_eventually!(&fixture, &head, "the conflict to be handed off", {
        queue_attempt(&fixture, &head).await.state == db::IntegrationAttemptState::Ejected
            && task_status(&fixture, &head.task_id).await == "merge_failed"
    });
    let _ = stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;

    let attempt = queue_attempt(&fixture, &head).await;
    assert_eq!(attempt.conflict_paths_json, Some(json!(["feature.txt"])));
    let bridges: Vec<(String, String)> = sqlx::query_as(
        "SELECT bridge_kind, bridge_payload FROM transition_log WHERE task_id=? AND bridge_kind IS NOT NULL",
    )
    .bind(&head.task_id)
    .fetch_all(fixture.harness.state.db.pool())
    .await
    .unwrap();
    assert_eq!(bridges.len(), 1, "{bridges:?}");
    assert_eq!(bridges[0].0, "conflict_handoff");
    assert!(bridges[0].1.contains("feature.txt"), "{bridges:?}");
    // The rebase ran on the Task's daemon; no fast-forward was asked for.
    assert_eq!(
        succeeded(&attempt, Kind::Rebase),
        vec![Some(fixture.daemon_id.clone())]
    );
    assert!(receipt_owners(&attempt, Kind::FastForward).is_empty());
    assert_eq!(
        git_sync(&default_checkout, &["rev-parse", "refs/heads/main"]),
        moved_tip
    );
    assert_eq!(
        std::fs::read_to_string(default_checkout.join("feature.txt")).unwrap(),
        "theirs\n"
    );
}

/// The Task's daemon rebases, and its reply is lost with the connection.
/// The default checkout's owner (the server) has nothing to say about that
/// effect: the receipt is asked of the Task's daemon when it is back, the
/// rebase is not repeated, and the head merges.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cross_owner_task_daemon_disconnect_in_rebasing_is_settled_by_that_daemon() {
    use db::IntegrationOperationKind as Kind;
    let mut fixture = Fixture::file_backed("forge-queue-cross-rebase-lost").await;
    let default_checkout = server_owned_default(&fixture).await;
    let head = admit_queue_head(&fixture, &["test -f outside.txt"]).await;
    std::fs::write(default_checkout.join("outside.txt"), "outside\n").unwrap();
    git::commit_all(&default_checkout, "outside").await.unwrap();
    let moved_tip = git::get_current_sha(&default_checkout).await.unwrap();
    fixture
        .link
        .as_ref()
        .unwrap()
        .drop_replies
        .lock()
        .unwrap()
        .insert(METHOD_WORKSPACE_RESET.into());
    start_check_worker!(fixture, stop_checks);
    // A lease far longer than any wait here: nothing below is a lease retry.
    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(600));
    // The owner rebases: the Task's HEAD leaves the admitted candidate.
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let moved = fixture
                .resolved
                .git_query(WorkspaceGitQuery::Head, false)
                .await
                .ok()
                .flatten()
                .is_some_and(|now| now.trim() != head.candidate);
            if moved {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the Task's daemon rebases");
    assert_eq!(
        attempt_requests(fixture.link.as_ref().unwrap(), METHOD_WORKSPACE_RESET).len(),
        1
    );
    fixture.link.take();
    common::fake_daemon::wait_until_disconnected(&fixture.harness.state, &fixture.daemon_id).await;
    queue_eventually!(&fixture, &head, "the head to hold an unknown rebase", {
        let attempt = queue_attempt(&fixture, &head).await;
        attempt.effect_intent_json.is_some()
            && matches!(
                attempt.state,
                db::IntegrationAttemptState::Reconciling | db::IntegrationAttemptState::Quarantined
            )
    });
    {
        // The intent names the Task's daemon as the owner to ask.
        let attempt = queue_attempt(&fixture, &head).await;
        let intent = attempt.effect_intent_json.clone().unwrap();
        assert_eq!(
            intent["request"]["witness"]["effect_owner"]["daemon_id"],
            json!(fixture.daemon_id)
        );
        assert_eq!(
            intent["request"]["fence"]["target_owner"]["owner_kind"],
            "server"
        );
        assert_eq!(task_status(&fixture, &head.task_id).await, "merging");
        assert_eq!(
            git_sync(&default_checkout, &["rev-parse", "refs/heads/main"]),
            moved_tip
        );
    }
    fixture.reconnect().await;
    // The receipt is back with the connection, but the placement is still
    // `disconnected`: the head does not ask for its check yet.
    queue_eventually!(&fixture, &head, "the rebase receipt after the reconnect", {
        succeeded(&queue_attempt(&fixture, &head).await, Kind::Rebase).len() == 1
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        queue_attempt(&fixture, &head).await.state,
        db::IntegrationAttemptState::Rebasing
    );
    assert!(fixture
        .link
        .as_ref()
        .unwrap()
        .requests(METHOD_CHECK_RUN)
        .is_empty());
    // The owner reconciliation of the server runtime makes it ready again.
    fixture
        .reconcile_reconnected_placement(&fixture.resolved.placement.id)
        .await;
    queue_eventually!(&fixture, &head, "the Task to merge after the reconnect", {
        task_status(&fixture, &head.task_id).await == "done"
            && queue_attempt(&fixture, &head).await.state == db::IntegrationAttemptState::Completed
    });
    let _ = stop.send(true);
    let _ = stop_checks.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;

    let attempt = assert_landed_exactly(&fixture, &head, &default_checkout, &moved_tip).await;
    // One rebase, by the daemon, never asked for again; one fast-forward.
    assert_eq!(
        receipt_owners(&attempt, Kind::Rebase)
            .into_iter()
            .map(|(_, owner)| owner)
            .collect::<Vec<_>>(),
        vec![Some(fixture.daemon_id.clone())]
    );
    assert_eq!(succeeded(&attempt, Kind::Rebase).len(), 1);
    assert!(
        attempt_requests(fixture.link.as_ref().unwrap(), METHOD_WORKSPACE_RESET).is_empty(),
        "the rebase was not repeated after the reconnect"
    );
    assert_eq!(succeeded(&attempt, Kind::FastForward), vec![None]);
}

/// The target's daemon fast-forwards a transferred candidate (the Task is
/// on the server) and its reply is lost with the connection. The receipt is
/// asked of that daemon when it is back; the fast-forward is not repeated.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cross_owner_target_daemon_disconnect_in_ff_inflight_is_settled_by_that_daemon() {
    use db::IntegrationOperationKind as Kind;
    let mut fixture = Fixture::file_backed("forge-queue-cross-ff-lost").await;
    let (placement, _task_clone, candidate) = server_placed_task(&fixture).await;
    let head = admit_placed_head(&fixture, &placement, candidate, &[]).await;
    fixture
        .link
        .as_ref()
        .unwrap()
        .drop_replies
        .lock()
        .unwrap()
        .insert(METHOD_WORKSPACE_MERGE.into());
    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(600));
    tokio::time::timeout(Duration::from_secs(30), async {
        while git::get_current_sha(&fixture.checkout).await.unwrap() != head.candidate {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the target's daemon fast-forwards the transferred candidate");
    assert_eq!(
        attempt_requests(fixture.link.as_ref().unwrap(), METHOD_WORKSPACE_MERGE).len(),
        1
    );
    fixture.link.take();
    common::fake_daemon::wait_until_disconnected(&fixture.harness.state, &fixture.daemon_id).await;
    queue_eventually!(
        &fixture,
        &head,
        "the head to hold an unknown fast-forward",
        {
            let attempt = queue_attempt(&fixture, &head).await;
            attempt.effect_intent_json.is_some()
                && matches!(
                    attempt.state,
                    db::IntegrationAttemptState::Reconciling
                        | db::IntegrationAttemptState::Quarantined
                )
        }
    );
    assert_eq!(task_status(&fixture, &head.task_id).await, "merging");
    let reflog = git_sync(&fixture.checkout, &["reflog", "--all"]);
    fixture.reconnect().await;
    queue_eventually!(
        &fixture,
        &head,
        "the Task to be done after the reconnect",
        {
            task_status(&fixture, &head.task_id).await == "done"
                && queue_attempt(&fixture, &head).await.state
                    == db::IntegrationAttemptState::Completed
        }
    );
    let _ = stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    let attempt = queue_attempt(&fixture, &head).await;
    assert_eq!(
        attempt.integrated_sha.as_deref(),
        Some(head.candidate.as_str())
    );
    assert_eq!(
        receipt_owners(&attempt, Kind::FastForward),
        vec![(
            db::IntegrationOperationState::Succeeded,
            Some(fixture.daemon_id.clone())
        )]
    );
    assert!(
        attempt_requests(fixture.link.as_ref().unwrap(), METHOD_WORKSPACE_MERGE).is_empty(),
        "the fast-forward was not repeated after the reconnect"
    );
    assert_eq!(git_sync(&fixture.checkout, &["reflog", "--all"]), reflog);
}

/// The default checkout moves after the cross-owner rebase and before the
/// fast-forward (a writer outside Forge, while the head check runs). The
/// server refuses to fast-forward onto a tip the candidate was not rebased
/// on; the head goes through a new round and lands on top of the new tip.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cross_owner_target_moved_between_rebase_and_fast_forward_starts_a_new_round() {
    use db::IntegrationOperationKind as Kind;
    let fixture = Fixture::file_backed("forge-queue-cross-moved").await;
    let default_checkout = server_owned_default(&fixture).await;
    let head = admit_queue_head(&fixture, &["test -f outside.txt && sleep 2"]).await;
    std::fs::write(default_checkout.join("outside.txt"), "outside\n").unwrap();
    git::commit_all(&default_checkout, "outside").await.unwrap();
    start_check_worker!(fixture, stop_checks);
    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(60));
    // The first round's check is running on the rebased commit.
    queue_eventually!(&fixture, &head, "the first head check", {
        let attempt = queue_attempt(&fixture, &head).await;
        attempt.state == db::IntegrationAttemptState::Checking
            && !fixture
                .link
                .as_ref()
                .unwrap()
                .requests(METHOD_CHECK_RUN)
                .is_empty()
    });
    let first_candidate = queue_attempt(&fixture, &head).await.candidate_sha.unwrap();
    std::fs::write(default_checkout.join("late.txt"), "late\n").unwrap();
    git::commit_all(&default_checkout, "late").await.unwrap();
    let late_tip = git::get_current_sha(&default_checkout).await.unwrap();

    queue_eventually!(&fixture, &head, "the Task to merge", {
        task_status(&fixture, &head.task_id).await == "done"
            && queue_attempt(&fixture, &head).await.state == db::IntegrationAttemptState::Completed
    });
    let _ = stop.send(true);
    let _ = stop_checks.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;

    let attempt = queue_attempt(&fixture, &head).await;
    let tip = git_sync(&default_checkout, &["rev-parse", "refs/heads/main"]);
    assert_eq!(attempt.integrated_sha.as_deref(), Some(tip.as_str()));
    assert_eq!(
        attempt.integrated_before_sha.as_deref(),
        Some(late_tip.as_str())
    );
    assert_ne!(
        tip, first_candidate,
        "the first round's commit never landed"
    );
    // Linear on top of the late commit: not a merge of a stale candidate.
    assert_eq!(
        git_sync(&default_checkout, &["rev-parse", &format!("{tip}^")]),
        late_tip
    );
    for file in ["feature.txt", "outside.txt", "late.txt"] {
        assert!(default_checkout.join(file).exists(), "{file}");
    }
    // Two rebases on the Task's daemon, one landed fast-forward on the server.
    assert_eq!(succeeded(&attempt, Kind::Rebase).len(), 2);
    assert_eq!(succeeded(&attempt, Kind::FastForward), vec![None]);
    assert!(
        attempt
            .phase_timings
            .as_ref()
            .is_some_and(|timings| timings.rounds >= 2),
        "{:?}",
        attempt.phase_timings
    );
}

/// The worker's cross-owner capabilities (facts, both transfers, the rebase
/// on the Task's daemon, the fast-forward on the server, the release) write
/// nothing outside the queue, the attempt and the Task-step queue. They are
/// driven directly, so no Task step runs in the window.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cross_owner_ports_write_only_queue_attempt_and_task_step() {
    use db::IntegrationQueueRepo;
    use services::{
        daemon_transport::workspace_client::DaemonWorkspaceClient,
        integration_owner::{OwnerMergeReceipt, OwnerRebaseReceipt, ServerIntegrationOwner},
        integration_ports::{
            DaemonFences, OwnerObjectTransfer, RoutingIntegrationOwner, WorkspaceHeadFacts,
        },
        integration_worker::{
            IntegrationFactsPort, IntegrationOwnerPort, ObjectTransferEndpoint,
            ObjectTransferOutcome, ObjectTransferPort, ObjectTransferRelease,
            ObjectTransferRequest, OwnerFastForwardRequest, OwnerRebaseRequest,
        },
    };
    let fixture = Fixture::file_backed("forge-queue-cross-writes").await;
    let default_checkout = server_owned_default(&fixture).await;
    let candidate = fixture.candidate().await;
    std::fs::write(default_checkout.join("outside.txt"), "outside\n").unwrap();
    git::commit_all(&default_checkout, "outside").await.unwrap();
    let (fence, attempt_id) = claimed_attempt(&fixture, "cross-owner-writes").await;
    let state = fixture.harness.state.clone();
    let database = state.db.clone();
    let server = Arc::new(ServerIntegrationOwner::new(database.clone()));
    let client = DaemonWorkspaceClient::new(state.daemon_connections.clone())
        .with_receipts(database.clone());
    let fences = Arc::new(DaemonFences::new(database.clone(), client.clone()));
    let owner = RoutingIntegrationOwner::new(server.clone(), client.clone(), fences.clone());
    let transfer = OwnerObjectTransfer::new(
        database.clone(),
        server,
        client.clone(),
        fences,
        &fixture.server_root.path().join("transfer-staging"),
    );
    let facts_port =
        WorkspaceHeadFacts::new(database.clone(), state.workspace_backend_router.clone())
            .with_daemons(client);
    let attempt = database
        .integration_attempt(&attempt_id)
        .await
        .unwrap()
        .unwrap();
    let queue = database
        .integration_queue(&fence.queue_id)
        .await
        .unwrap()
        .unwrap();
    async fn foreign(database: &db::SqliteDb) -> BTreeMap<String, String> {
        let mut digests = table_digests(database).await;
        // `daemon` carries the connected link's heartbeat (`last_report_at`,
        // `updated_at`, `version`), written by the connection, not by a port.
        digests.retain(|table, _| {
            ![
                "integration_queue",
                "integration_attempt",
                "task_step",
                "task_schedule_dirty",
                "daemon",
            ]
            .iter()
            .any(|own| table == own || table.starts_with(&format!("{own}::")))
        });
        let daemons: Vec<String> = sqlx::query_scalar(
            "SELECT id || '|' || machine_id || '|' || status || '|' || labels_json FROM daemon ORDER BY id",
        )
        .fetch_all(database.pool())
        .await
        .unwrap();
        digests.insert("daemon::stable".into(), daemons.join("\n"));
        digests
    }
    let before = foreign(&database).await;

    let facts = facts_port.head_facts(&attempt, &queue).await.unwrap();
    assert_eq!(facts.candidate_head, candidate);
    assert!(!facts.shared_object_store);
    let target = ObjectTransferEndpoint {
        repo_location_id: queue.target_location_id.clone().unwrap(),
        owner: services::integration_effects::EffectOwner::Server,
    };
    let request = |direction, have: &str, want: &str| ObjectTransferRequest {
        fence: fence.clone(),
        direction,
        task: facts.task_location.clone(),
        target: target.clone(),
        target_branch: "main".into(),
        have: vec![have.to_owned()],
        want: want.to_owned(),
        max_bytes: 256 * 1024 * 1024,
    };
    assert!(matches!(
        transfer
            .transfer(request(
                ObjectTransferDirection::Inbound,
                &candidate,
                &facts.target_tip
            ))
            .await
            .unwrap(),
        ObjectTransferOutcome::Transferred { .. }
    ));
    let rebased = owner
        .rebase(OwnerRebaseRequest {
            fence: fence.clone(),
            workspace: facts.workspace.clone(),
            target_branch: "main".into(),
            expected_head_sha: candidate.clone(),
            expected_target_sha: facts.target_tip.clone(),
            handoff_conflicts: true,
            deadline: Duration::from_secs(60),
            cancel: tokio_util::sync::CancellationToken::new(),
        })
        .await
        .unwrap();
    assert!(
        matches!(
            rebased,
            OwnerRebaseReceipt::Completed {
                outcome: WorkspaceOwnerOperationOutcome::Rebased
            }
        ),
        "{rebased:?}"
    );
    let attempt = database
        .integration_attempt(&attempt_id)
        .await
        .unwrap()
        .unwrap();
    let after_rebase = facts_port.head_facts(&attempt, &queue).await.unwrap();
    assert_ne!(after_rebase.candidate_head, candidate);
    assert!(after_rebase.target_in_candidate);
    assert!(matches!(
        transfer
            .transfer(request(
                ObjectTransferDirection::Outbound,
                &facts.target_tip,
                &after_rebase.candidate_head
            ))
            .await
            .unwrap(),
        ObjectTransferOutcome::Transferred { .. }
    ));
    let merged = owner
        .fast_forward(OwnerFastForwardRequest {
            fence: fence.clone(),
            workspace: after_rebase.workspace.clone(),
            target_branch: "main".into(),
            task_branch: after_rebase.task_branch.clone(),
            candidate_sha: after_rebase.candidate_head.clone(),
            target_sha: facts.target_tip.clone(),
            deadline: Duration::from_secs(30),
        })
        .await
        .unwrap();
    assert!(
        matches!(merged, OwnerMergeReceipt::Completed { .. }),
        "{merged:?}"
    );
    assert_eq!(
        git_sync(&default_checkout, &["rev-parse", "refs/heads/main"]),
        after_rebase.candidate_head
    );
    transfer
        .release(ObjectTransferRelease {
            attempt_id: attempt_id.clone(),
            task: facts.task_location.clone(),
            target,
        })
        .await
        .unwrap();
    assert!(integration_refs(&default_checkout).is_empty());
    assert!(integration_refs(&fixture.checkout).is_empty());

    let after = foreign(&database).await;
    for (table, digest) in &before {
        assert_eq!(after.get(table), Some(digest), "table `{table}` changed");
    }
    assert_eq!(after.len(), before.len());
    assert!(before.len() > 50, "the digest covers the schema");
    // Both effects left their receipt on the attempt, and nowhere else.
    let attempt = database
        .integration_attempt(&attempt_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(attempt_receipts(&attempt).len(), 2);
}

/// Two Tasks in ONE queue on a daemon-owned default checkout, both in
/// worktrees of it. They are taken in order: the first is rebased, checked
/// green and merged; the second is then rebased onto the first one's merge,
/// its check is red, and it is sent back. The default checkout holds the
/// first Task only.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn two_tasks_in_one_queue_on_a_daemon_owned_target_merge_green_then_eject_red_in_order() {
    use db::IntegrationOperationKind as Kind;
    let fixture = Fixture::file_backed("forge-queue-daemon-two-tasks").await;
    let runtime_id = fixture.resolved.placement.runtime_id.clone().unwrap();
    let first = admit_queue_head(&fixture, &["test -f feature.txt", "test -f outside.txt"]).await;
    let (second_workspace, _, second_candidate) = daemon_task(
        &fixture,
        &fixture.daemon_id,
        &runtime_id,
        None,
        "second.txt",
    )
    .await;
    // Red only once it sits on top of the first Task's merge.
    let second = admit_placed_head(
        &fixture,
        &second_workspace.placement,
        second_candidate,
        &["test -f second.txt", "! test -f feature.txt"],
    )
    .await;
    assert_eq!(first.queue_id, second.queue_id, "one queue");
    std::fs::write(fixture.checkout.join("outside.txt"), "outside\n").unwrap();
    git::commit_all(&fixture.checkout, "outside").await.unwrap();
    start_check_worker!(fixture, stop_checks);

    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(60));
    queue_eventually!(&fixture, &first, "the first Task to merge", {
        task_status(&fixture, &first.task_id).await == "done"
            && queue_attempt(&fixture, &first).await.state == db::IntegrationAttemptState::Completed
    });
    queue_eventually!(&fixture, &second, "the second Task to be sent back", {
        queue_attempt(&fixture, &second).await.state == db::IntegrationAttemptState::Ejected
            && task_status(&fixture, &second.task_id).await != "merging"
    });
    let _ = stop.send(true);
    let _ = stop_checks.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;

    let merged = queue_attempt(&fixture, &first).await;
    let ejected = queue_attempt(&fixture, &second).await;
    assert!(merged.queue_seq < ejected.queue_seq);
    let tip = git_sync(&fixture.checkout, &["rev-parse", "refs/heads/main"]);
    assert_eq!(merged.integrated_sha.as_deref(), Some(tip.as_str()));
    assert!(fixture.checkout.join("feature.txt").exists());
    assert!(!fixture.checkout.join("second.txt").exists());
    // The second head was rebased onto the first one's merge, then checked.
    assert_eq!(ejected.target_tip_sha.as_deref(), Some(tip.as_str()));
    assert_eq!(
        ejected.failure_kind,
        Some(db::IntegrationFailureKind::CandidateCheckFailed)
    );
    assert_ne!(task_status(&fixture, &second.task_id).await, "done");
    assert_eq!(succeeded(&merged, Kind::Rebase).len(), 1);
    assert_eq!(succeeded(&merged, Kind::FastForward).len(), 1);
    assert_eq!(succeeded(&ejected, Kind::Rebase).len(), 1);
    assert!(receipt_owners(&ejected, Kind::FastForward).is_empty());
    // Green first, red second, each on its own rebased commit.
    let checks: Vec<(String, String)> = sqlx::query_as(
        "SELECT r.commit_sha, s.outcome FROM check_run r JOIN check_result s ON s.run_id=r.id ORDER BY r.created_at, r.id",
    )
    .fetch_all(fixture.harness.state.db.pool())
    .await
    .unwrap();
    assert_eq!(
        checks,
        vec![
            (tip, "pass".to_owned()),
            (ejected.candidate_sha.clone().unwrap(), "fail".to_owned())
        ]
    );
    {
        use db::IntegrationQueueRepo;
        let queue = fixture
            .harness
            .state
            .db
            .integration_queue(&first.queue_id)
            .await
            .unwrap()
            .unwrap();
        assert!(queue.head_attempt_id.is_none(), "the slot is free");
    }
}

/// The head's claim, taken by this test instead of a running worker, and the
/// production owner and facts ports to drive it by hand.
async fn claimed_head(
    fixture: &Fixture,
    head: &QueueHead,
) -> (
    IntegrationOwnerFence,
    services::integration_ports::RoutingIntegrationOwner,
    services::integration_worker::HeadFacts,
) {
    use db::IntegrationQueueRepo;
    use services::{
        daemon_transport::workspace_client::DaemonWorkspaceClient,
        integration_owner::ServerIntegrationOwner,
        integration_ports::{DaemonFences, RoutingIntegrationOwner, WorkspaceHeadFacts},
        integration_worker::IntegrationFactsPort,
    };
    let state = fixture.harness.state.clone();
    let database = state.db.clone();
    let queue = database
        .integration_queue(&head.queue_id)
        .await
        .unwrap()
        .unwrap();
    let queue = database
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
        .integration_owner_fence(&head.attempt_id)
        .await
        .unwrap()
        .unwrap();
    let client = DaemonWorkspaceClient::new(state.daemon_connections.clone())
        .with_receipts(database.clone());
    let owner = RoutingIntegrationOwner::new(
        Arc::new(ServerIntegrationOwner::new(database.clone())),
        client.clone(),
        Arc::new(DaemonFences::new(database.clone(), client.clone())),
    );
    let facts = WorkspaceHeadFacts::new(database.clone(), state.workspace_backend_router.clone())
        .with_daemons(client)
        .head_facts(&queue_attempt(fixture, head).await, &queue)
        .await
        .unwrap();
    (fence, owner, facts)
}

/// R5, daemon side: a daemon that holds the default checkout fast-forwards a
/// candidate from another machine only from the ref this claim generation's
/// outbound transfer bound. Asked without that transfer, it refuses before
/// any Git effect, with a receipt, and its checkout does not move.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_daemon_refuses_to_fast_forward_a_candidate_it_was_not_sent() {
    use services::{
        integration_owner::{OwnerEffectRefusal, OwnerMergeReceipt},
        integration_worker::{IntegrationOwnerPort, OwnerFastForwardRequest},
    };
    let fixture = Fixture::file_backed("forge-queue-cross-ff-unsent").await;
    let (placement, _task_clone, candidate) = server_placed_task(&fixture).await;
    let head = admit_placed_head(&fixture, &placement, candidate.clone(), &[]).await;
    let (fence, owner, facts) = claimed_head(&fixture, &head).await;
    assert_eq!(facts.candidate_head, candidate);
    assert!(!facts.candidate_in_target, "read on the target's daemon");
    let before = git_sync(&fixture.checkout, &["rev-parse", "refs/heads/main"]);
    assert_eq!(facts.target_tip, before);
    let reflog = git_sync(&fixture.checkout, &["reflog", "--all"]);

    let refused = owner
        .fast_forward(OwnerFastForwardRequest {
            fence,
            workspace: facts.workspace.clone(),
            target_branch: "main".into(),
            task_branch: facts.task_branch.clone(),
            candidate_sha: candidate.clone(),
            target_sha: facts.target_tip.clone(),
            deadline: Duration::from_secs(30),
        })
        .await
        .unwrap();
    // The owner's gate refused before the effect started: its receipt says
    // the fast-forward was not performed.
    assert!(
        matches!(
            refused,
            OwnerMergeReceipt::NotPerformed {}
                | OwnerMergeReceipt::Refused {
                    reason: OwnerEffectRefusal::WitnessMismatch
                }
        ),
        "{refused:?}"
    );
    assert_eq!(
        git_sync(&fixture.checkout, &["rev-parse", "refs/heads/main"]),
        before
    );
    assert_eq!(git_sync(&fixture.checkout, &["reflog", "--all"]), reflog);
    // It was asked as a transferred fast-forward, and answered by receipt.
    let merges = attempt_requests(fixture.link.as_ref().unwrap(), METHOD_WORKSPACE_MERGE);
    assert_eq!(merges.len(), 1);
    assert!(merges[0]["candidate_transfer_key"].is_string());
    let attempt = queue_attempt(&fixture, &head).await;
    assert_eq!(
        receipt_owners(&attempt, db::IntegrationOperationKind::FastForward),
        vec![(
            db::IntegrationOperationState::Failed,
            Some(fixture.daemon_id.clone())
        )]
    );
    assert!(attempt.effect_intent_json.is_none(), "settled, not unknown");
}

/// R5, server side: the server fast-forwards a daemon-placed Task's
/// candidate only from the outbound transfer ref of the claim generation.
/// Without it nothing is merged, and the refusal is a settled receipt.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn the_server_refuses_to_fast_forward_a_candidate_it_was_not_sent() {
    use services::{
        integration_owner::{OwnerEffectRefusal, OwnerMergeReceipt},
        integration_worker::{IntegrationOwnerPort, OwnerFastForwardRequest},
    };
    let fixture = Fixture::file_backed("forge-queue-cross-ff-unsent-server").await;
    let default_checkout = server_owned_default(&fixture).await;
    let head = admit_queue_head(&fixture, &[]).await;
    let (fence, owner, facts) = claimed_head(&fixture, &head).await;
    let before = git_sync(&default_checkout, &["rev-parse", "refs/heads/main"]);
    assert_eq!(facts.target_tip, before);

    let refused = owner
        .fast_forward(OwnerFastForwardRequest {
            fence,
            workspace: facts.workspace.clone(),
            target_branch: "main".into(),
            task_branch: facts.task_branch.clone(),
            candidate_sha: head.candidate.clone(),
            target_sha: facts.target_tip.clone(),
            deadline: Duration::from_secs(30),
        })
        .await
        .unwrap();
    assert!(
        matches!(
            refused,
            OwnerMergeReceipt::Refused {
                reason: OwnerEffectRefusal::WitnessMismatch
            }
        ),
        "{refused:?}"
    );
    assert_eq!(
        git_sync(&default_checkout, &["rev-parse", "refs/heads/main"]),
        before
    );
    assert!(git_sync(&default_checkout, &["status", "--porcelain"]).is_empty());
    // The Task's daemon was not asked to merge anything.
    assert!(attempt_requests(fixture.link.as_ref().unwrap(), METHOD_WORKSPACE_MERGE).is_empty());
    let attempt = queue_attempt(&fixture, &head).await;
    assert_eq!(
        receipt_owners(&attempt, db::IntegrationOperationKind::FastForward),
        vec![(db::IntegrationOperationState::Failed, None)]
    );
    assert!(attempt.effect_intent_json.is_none(), "settled, not unknown");
}

/// The same race with the owners reversed: Task on the server, default
/// checkout on a daemon, and the daemon's checkout moves while the head
/// check runs on the server. The daemon refuses the transferred candidate
/// (its target witness no longer holds) before any effect; the head goes
/// through a new round and lands on top of the new tip.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cross_owner_daemon_target_moved_between_rebase_and_fast_forward_starts_a_new_round() {
    use db::IntegrationOperationKind as Kind;
    let fixture = Fixture::file_backed("forge-queue-cross-moved-daemon").await;
    let (placement, _task_clone, candidate) = server_placed_task(&fixture).await;
    let head = admit_placed_head(
        &fixture,
        &placement,
        candidate,
        &["test -f outside.txt && sleep 2"],
    )
    .await;
    std::fs::write(fixture.checkout.join("outside.txt"), "outside\n").unwrap();
    git::commit_all(&fixture.checkout, "outside").await.unwrap();
    start_check_worker!(fixture, stop_checks);
    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(60));
    queue_eventually!(&fixture, &head, "the first head check", {
        let runs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM check_run")
            .fetch_one(fixture.harness.state.db.pool())
            .await
            .unwrap();
        queue_attempt(&fixture, &head).await.state == db::IntegrationAttemptState::Checking
            && runs > 0
    });
    let first_candidate = queue_attempt(&fixture, &head).await.candidate_sha.unwrap();
    std::fs::write(fixture.checkout.join("late.txt"), "late\n").unwrap();
    git::commit_all(&fixture.checkout, "late").await.unwrap();
    let late_tip = git::get_current_sha(&fixture.checkout).await.unwrap();

    queue_eventually!(&fixture, &head, "the Task to merge", {
        task_status(&fixture, &head.task_id).await == "done"
            && queue_attempt(&fixture, &head).await.state == db::IntegrationAttemptState::Completed
    });
    let _ = stop.send(true);
    let _ = stop_checks.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;

    let attempt = queue_attempt(&fixture, &head).await;
    let tip = git_sync(&fixture.checkout, &["rev-parse", "refs/heads/main"]);
    assert_eq!(attempt.integrated_sha.as_deref(), Some(tip.as_str()));
    assert_eq!(
        attempt.integrated_before_sha.as_deref(),
        Some(late_tip.as_str())
    );
    assert_ne!(
        tip, first_candidate,
        "the first round's commit never landed"
    );
    assert_eq!(
        git_sync(&fixture.checkout, &["rev-parse", &format!("{tip}^")]),
        late_tip
    );
    for file in ["feature.txt", "outside.txt", "late.txt"] {
        assert!(fixture.checkout.join(file).exists(), "{file}");
    }
    // Two rebases on the server; the daemon refused once and landed once.
    assert_eq!(succeeded(&attempt, Kind::Rebase), vec![None, None]);
    let fast_forwards = receipt_owners(&attempt, Kind::FastForward);
    assert_eq!(
        fast_forwards,
        vec![
            (
                db::IntegrationOperationState::Failed,
                Some(fixture.daemon_id.clone())
            ),
            (
                db::IntegrationOperationState::Succeeded,
                Some(fixture.daemon_id.clone())
            )
        ],
        "{:?}",
        attempt_receipts(&attempt)
    );
}
