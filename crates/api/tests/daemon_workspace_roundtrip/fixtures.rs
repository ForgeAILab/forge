//! Shared daemon links, workspace fixtures and execution inputs.

use super::*;

pub(super) struct DaemonLink {
    pub(super) outbound: mpsc::UnboundedSender<DaemonFrame>,
    pub(super) requests: Arc<Mutex<Vec<(String, Value)>>>,
    pub(super) interrupt: Arc<Mutex<HashSet<String>>>,
    pub(super) drop_replies: Arc<Mutex<HashSet<String>>>,
    pub(super) tasks: Vec<JoinHandle<()>>,
}

impl DaemonLink {
    pub(super) async fn connect(
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

    pub(super) fn requests(&self, method: &str) -> Vec<Value> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(name, _)| name == method)
            .map(|(_, params)| params.clone())
            .collect()
    }

    pub(super) fn last_request(&self) -> Option<(String, Value)> {
        self.requests.lock().unwrap().last().cloned()
    }

    pub(super) fn stop_heartbeats(&self) {
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
pub(super) struct Stall {
    pub(super) seen: Option<String>,
    pub(super) since: tokio::time::Instant,
    pub(super) limit: Duration,
}

impl Stall {
    pub(super) fn new(limit: Duration) -> Self {
        Self {
            seen: None,
            since: tokio::time::Instant::now(),
            limit,
        }
    }

    /// `true` once `observed` has stayed the same for the whole limit.
    pub(super) fn stalled(&mut self, observed: String) -> bool {
        if self.seen.as_ref() != Some(&observed) {
            self.seen = Some(observed);
            self.since = tokio::time::Instant::now();
        }
        self.since.elapsed() >= self.limit
    }
}

pub(super) struct Fixture {
    pub(super) harness: Harness,
    pub(super) server: TestServer,
    pub(super) link: Option<DaemonLink>,
    pub(super) runtime: Arc<DaemonRuntime>,
    pub(super) daemon_id: String,
    pub(super) token: String,
    pub(super) resolved: ResolvedWorkspace,
    pub(super) execution_id: String,
    pub(super) base_sha: String,
    pub(super) checkout: PathBuf,
    pub(super) inaccessible_server_path: PathBuf,
    pub(super) server_root: TestDir,
    pub(super) _daemon_root: TestDir,
}

impl Fixture {
    pub(super) async fn new(suite: &str) -> Self {
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
    pub(super) async fn file_backed(suite: &str) -> Self {
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

    pub(super) async fn with_policy(suite: &str, run_policy: WorkspaceRunPolicy) -> Self {
        Self::build(suite, run_policy, false).await
    }

    pub(super) async fn build(
        suite: &str,
        run_policy: WorkspaceRunPolicy,
        file_backed: bool,
    ) -> Self {
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

    pub(super) async fn mark_task_done(&self) {
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

    pub(super) async fn run(&self, command: &str) -> services::workspace_backend::RunResult {
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

    pub(super) async fn candidate(&self) -> String {
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

    pub(super) async fn reviewer_attempt(&self) -> (db::Review, db::Execution) {
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
        // The fixture states the rows a dispatched reviewer leaves (a running
        // reviewer execution leased to this daemon connection, and the
        // attempt bound to it); dispatch admission is not its subject.
        sqlx::query(
            "INSERT INTO execution (id, task_id, agent_id, role, status, parent_execution_id, workspace_id, created_at, updated_at)
             VALUES (?, ?, ?, 'reviewer', 'running', ?, ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.task_id)
        .bind(input.agent_id.as_deref())
        .bind(input.parent_execution_id.as_deref())
        .bind(input.workspace_id.as_deref())
        .bind(&now)
        .bind(&now)
        .execute(self.harness.state.db.pool())
        .await
        .expect("the reviewer execution row inserts");
        let created = ExecutionRepo::get_by_id(&*self.harness.state.db, &reviewer_id)
            .await
            .expect("reviewer reads")
            .expect("reviewer exists");
        let leased = ExecutionRepo::claim_lease(
            &*self.harness.state.db,
            db::ClaimExecutionLease {
                execution_id: reviewer_id.clone(),
                expected_version: created.execution_version,
                owner: services::daemon_transport::execution_lease_owner(
                    &self.daemon_id,
                    connection.id(),
                ),
                lease_expires_at: (chrono::Utc::now() + chrono::Duration::seconds(60)).to_rfc3339(),
                hard_deadline_at: Some(
                    (chrono::Utc::now() + chrono::Duration::minutes(30)).to_rfc3339(),
                ),
                now: now.clone(),
            },
        )
        .await
        .expect("the owner lease is claimed");
        assert!(matches!(leased, db::ExecutionLeaseMutation::Updated(_)));
        let review = ReviewRepo::create(
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
                updated_at: now,
            },
        )
        .await
        .expect("review attempt opens");

        sqlx::query("UPDATE review SET reviewer_execution_id = ? WHERE id = ?")
            .bind(&reviewer_id)
            .bind(&review.id)
            .execute(self.harness.state.db.pool())
            .await
            .expect("the attempt binds its reviewer");
        let review = ReviewRepo::get_by_id(&*self.harness.state.db, &review.id)
            .await
            .expect("review reads")
            .expect("review exists");
        let reviewer = ExecutionRepo::get_by_id(&*self.harness.state.db, &reviewer_id)
            .await
            .expect("reviewer reads")
            .expect("reviewer exists");
        (review, reviewer)
    }

    pub(super) async fn approve_candidate(&self) -> ReviewContract {
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

    pub(super) async fn receipt(&self, method: &str) -> Value {
        let raw: String = sqlx::query_scalar(
            "SELECT outcome_json FROM command_receipt WHERE scope_id = ? AND operation = ? ORDER BY committed_at DESC, id DESC LIMIT 1",
        ).bind(&self.resolved.placement.task_id).bind(format!("daemon.{method}"))
            .fetch_one(self.harness.state.db.pool()).await.expect("owner receipt exists");
        serde_json::from_str(&raw).unwrap()
    }

    pub(super) async fn reconnect(&mut self) {
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
    pub(super) async fn server_activity(&self) -> String {
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
    pub(super) async fn reconcile_reconnected_placement(&self, placement_id: &str) {
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

    pub(super) async fn wait_until_journal_empty(&self) {
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

    pub(super) fn assert_server_cannot_resolve_workspace(&self) {
        assert!(!self.inaccessible_server_path.exists());
        assert!(self.resolved.embedded_path().is_err());
        assert!(!std::path::Path::new(self.resolved.handle().unwrap()).is_absolute());
        assert!(!self.checkout.starts_with(self.server_root.path()));
    }
}

pub(super) fn execution_input(
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

pub(super) fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}
