mod common;
use api_types::*;
use axum::http::{Method, StatusCode};
use common::{fake_daemon::*, json_request, json_request_with_bearer, TestDir};
use db::{ExecutionRepo, TaskRepo, WorkspaceRepo};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_tungstenite::tungstenite::Message;

struct Owner {
    outbound: mpsc::UnboundedSender<DaemonFrame>,
    requests: Arc<Mutex<Vec<(String, Value)>>>,
    canonical: Arc<Mutex<Option<String>>>,
    jobs: Vec<JoinHandle<()>>,
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
        root: PathBuf,
        plan: Arc<Mutex<Option<String>>>,
        capable: bool,
    ) -> Self {
        let socket = connect_daemon(
            server,
            &registration.daemon_id,
            Some(&registration.registration_token),
        )
        .await
        .unwrap();
        let (mut writer, mut reader) = socket.split();
        let (outbound, mut rx) = mpsc::unbounded_channel();
        let write = tokio::spawn(async move {
            while let Some(frame) = rx.recv().await {
                writer
                    .send(Message::Text(serde_json::to_string(&frame).unwrap().into()))
                    .await
                    .unwrap();
            }
        });
        let mut capabilities: Vec<String> = vec![
            DAEMON_CAPABILITY_USAGE_REPORTS.into(),
            DAEMON_CAPABILITY_JOURNAL_ACK.into(),
            DAEMON_CAPABILITY_WORKSPACE.into(),
        ];
        if capable {
            capabilities.push(DAEMON_CAPABILITY_PLAN_TRANSPORT.into());
        }
        outbound.send(DaemonFrame::Notification { method: METHOD_DAEMON_HANDSHAKE.into(), params: json!({"protocol_revision":DAEMON_PROTOCOL_REVISION, "capabilities":capabilities,
            "executor_capabilities":{"shell":{"cancel_ack":true,"terminal_observed":true,"resume":true}},
            "workspace_run_policy":{"allowed_purposes":["hook","ci_step","environment_setup"]}}) }).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let handles = Arc::new(Mutex::new(BTreeMap::<String, Value>::new()));
        let logged = requests.clone();
        let canonical = plan.clone();
        let read_handles = handles.clone();
        let send = outbound.clone();
        let read = tokio::spawn(async move {
            let mut publications = BTreeMap::<String, (String, Option<String>)>::new();
            while let Some(Ok(message)) = reader.next().await {
                let Message::Text(text) = message else {
                    continue;
                };
                let Ok(DaemonFrame::Request { id, method, params }) = serde_json::from_str(&text)
                else {
                    continue;
                };
                logged
                    .lock()
                    .unwrap()
                    .push((method.clone(), params.clone()));
                let result = match method.as_str() {
                    METHOD_REPO_LOCATION_VERIFY => {
                        json!({"repo_location_id":params["repo_location_id"],"path":params["path"],"default_branch_sha":"0123456789012345678901234567890123456789","origin_url":null})
                    }
                    METHOD_EXECUTION_CANCEL => {
                        json!({"execution_id":params["execution_id"],"cancelled":true})
                    }
                    METHOD_EXECUTION_START => {
                        json!({"execution_id":params["execution_id"],"accepted":true})
                    }
                    METHOD_JOURNAL_ACK => {
                        json!({"entry_id":params["entry_id"],"acknowledged":true})
                    }
                    METHOD_WORKSPACE_PREPARE => {
                        let handle = format!("owner-{}", params["workspace_id"].as_str().unwrap());
                        let state = json!({"workspace_handle":handle,"workspace_path":root.join(params["task_id"].as_str().unwrap()).join("repo"),"base_sha":"0123456789012345678901234567890123456789","branch":params["branch"],"generation":params["generation"]});
                        read_handles.lock().unwrap().insert(handle, state.clone());
                        let mut result = state;
                        result["entry_id"] = json!(format!(
                            "journal-{}",
                            params["operation_id"].as_str().unwrap()
                        ));
                        result["operation_id"] = params["operation_id"].clone();
                        result
                    }
                    METHOD_WORKSPACE_DESCRIBE => {
                        json!({"workspace_handle":params["workspace_handle"],"generation":params["generation"],"exists":true,"head_sha":"0123456789012345678901234567890123456789","dirty":false,"branch":"main","locked":false,"active_execution_ids":[],"journaled_execution_ids":[]})
                    }
                    METHOD_WORKSPACE_READ if params["operation"] == "paths" => {
                        let state = read_handles.lock().unwrap();
                        let path =
                            &state[params["workspace_handle"].as_str().unwrap()]["workspace_path"];
                        json!({"kind":"paths","workspace_path":path,"repo_path":root.join("checkout")})
                    }
                    METHOD_WORKSPACE_READ if params["operation"] == "git" => {
                        let kind = params["query"]["kind"].as_str().unwrap_or("");
                        json!({"kind":"git","output": if kind == "head" || kind == "resolve_commit" {"0123456789012345678901234567890123456789"} else {""}})
                    }
                    METHOD_WORKSPACE_READ if params["operation"] == "files" => {
                        json!({"kind":"files","files":[]})
                    }
                    METHOD_WORKSPACE_READ => {
                        if let Some(content) = canonical.lock().unwrap().as_ref() {
                            json!({"path":params["path"],"bytes":content.as_bytes(),"truncated":false})
                        } else {
                            send.send(DaemonFrame::Error {
                                id: Some(id),
                                error: DaemonErrorPayload {
                                    code: "workspace_file_not_found".into(),
                                    message: "missing plan".into(),
                                    details: None,
                                },
                            })
                            .unwrap();
                            continue;
                        }
                    }
                    METHOD_WORKSPACE_RESET => {
                        let exec = params["operation"]["execution_id"].as_str().unwrap_or("");
                        match params["operation"]["kind"].as_str().unwrap_or("") {
                            "publish_plan" => {
                                let content =
                                    params["operation"]["content"].as_str().unwrap().to_owned();
                                let mut current = canonical.lock().unwrap();
                                publications
                                    .entry(exec.into())
                                    .or_insert((content.clone(), current.clone()));
                                *current = Some(content);
                            }
                            "restore_plan" => {
                                if let Some((_, previous)) = publications.get(exec) {
                                    *canonical.lock().unwrap() = previous.clone();
                                }
                            }
                            "discard_plan" => {
                                publications.remove(exec);
                            }
                            _ => (),
                        }
                        json!({"entry_id":format!("journal-{}",params["operation_id"].as_str().unwrap()),"operation_id":params["operation_id"],"outcome":{"kind":"applied"}})
                    }
                    METHOD_WORKSPACE_DIFF if params["operation"] == "review" => json!({"diff":""}),
                    METHOD_WORKSPACE_DIFF => {
                        json!({"base_ref":"main","head_ref":"HEAD","base_sha":"0123456789012345678901234567890123456789","head_sha":"0123456789012345678901234567890123456789","files":[],"stats":{"files_changed":0,"total_additions":0,"total_deletions":0},"diff":"","truncated":false})
                    }
                    METHOD_WORKSPACE_RUN => {
                        json!({"entry_id":format!("journal-{}",params["operation_id"].as_str().unwrap()),"operation_id":params["operation_id"],"exit_code":0,"stdout":"","stderr":"","duration_ms":1,"timed_out":false,"stdout_truncated":false,"stderr_truncated":false})
                    }
                    _ => panic!("unhandled owner request {method}: {params}"),
                };
                send.send(DaemonFrame::Response { id, result }).unwrap();
            }
        });
        let heartbeats = outbound.clone();
        let heartbeat = tokio::spawn(async move {
            let mut seq = 0;
            loop {
                tokio::time::sleep(Duration::from_millis(200)).await;
                seq += 1;
                if heartbeats.send(DaemonFrame::Heartbeat { seq }).is_err() {
                    break;
                }
            }
        });
        wait_until_connected(&server.state, &registration.daemon_id).await;
        // Wait for the replacement handshake, rather than relying on socket order.
        for _ in 0..200 {
            if server
                .state
                .daemon_connections
                .get(&registration.daemon_id)
                .and_then(|c| c.snapshot())
                .is_some_and(|f| {
                    f.handshake
                        .capabilities
                        .iter()
                        .any(|c| c == DAEMON_CAPABILITY_WORKSPACE)
                })
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Self {
            outbound,
            requests,
            canonical: plan,
            jobs: vec![write, read, heartbeat],
        }
    }
    async fn started(&self, task_id: &str) -> Value {
        self.started_after(task_id, None).await
    }
    async fn started_after(&self, task_id: &str, previous: Option<&str>) -> Value {
        for _ in 0..300 {
            if let Some((_, params)) =
                self.requests
                    .lock()
                    .unwrap()
                    .iter()
                    .rev()
                    .find(|(method, params)| {
                        method == METHOD_EXECUTION_START
                            && params["task_id"] == task_id
                            && params["execution_id"].as_str() != previous
                    })
            {
                return params.clone();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!(
            "no execution.start for {task_id}; requests={:?}",
            self.requests.lock().unwrap()
        );
    }
    async fn acknowledged(&self, report: &Value) {
        for _ in 0..300 {
            if self
                .requests
                .lock()
                .unwrap()
                .iter()
                .any(|(method, params)| {
                    method == METHOD_JOURNAL_ACK
                        && params["entry_id"] == report["terminal_report_id"]
                })
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!(
            "terminal report was not acknowledged: {}",
            report["terminal_report_id"]
        );
    }
    fn terminal(&self, start: &Value, content: &str) -> Value {
        let params = json!({"terminal_report_id":format!("terminal-{}",start["execution_id"].as_str().unwrap()),"execution_id":start["execution_id"],"exit_code":0,"signal":null,"error":null,"ts":db::now_rfc3339(),"status":"completed","agent_session_id":"fake-session","after_sha":"0123456789012345678901234567890123456789","usage_reports":[],"outbox_entries":[],"plan_text":content});
        self.outbound
            .send(DaemonFrame::Notification {
                method: METHOD_EXECUTION_TERMINAL.into(),
                params: params.clone(),
            })
            .unwrap();
        params
    }
}

struct Fixture {
    harness: common::Harness,
    server: TestServer,
    registration: DaemonRegisterResponse,
    owner: Owner,
    project: String,
    agent: String,
    root: PathBuf,
    _dir: TestDir,
}
impl Fixture {
    async fn new(prefix: &str, capable: bool) -> Self {
        let dir = TestDir::new(prefix);
        let repo = common::setup_git_repo(dir.path());
        let harness = common::test_app(&dir.path().join("server"), prefix).await;
        let registration = register_daemon(&harness.app, &db::new_uuid_v4(), prefix).await;
        let root = dir.path().join("daemon-only");
        report_remote_daemon_shell(
            &harness.app,
            &registration.daemon_id,
            &registration.registration_token,
            &root,
            prefix,
        )
        .await;
        let server = TestServer::start(harness.state.clone()).await;
        let owner = Owner::connect(
            &server,
            &registration,
            root.clone(),
            Arc::new(Mutex::new(None)),
            capable,
        )
        .await;
        let (project, repo_id) = common::create_project_and_repo(&harness.app, prefix, &repo).await;
        let runtime =
            db::RuntimeRepo::get_by_daemon_id(&*harness.state.db, &registration.daemon_id)
                .await
                .unwrap()
                .unwrap();
        let now = db::now_rfc3339();
        db::RepoLocationRepo::create(
            &*harness.state.db,
            db::CreateRepoLocation {
                id: db::new_uuid_v4(),
                repo_id: repo_id.clone(),
                owner_kind: db::RepoLocationOwnerKind::Daemon,
                daemon_id: Some(registration.daemon_id.clone()),
                runtime_id: Some(runtime.id),
                path: root.join("checkout").to_string_lossy().into(),
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
        let mut agents = Vec::new();
        for role in ["worker", "reviewer"] {
            let agent:AgentResponse=json_request_with_bearer(&harness.app,Method::POST,"/api/v1/agents",&common::admin_jwt(),json!({"name":format!("{prefix}-{role}"),"executor_type":"shell","daemon_id":registration.daemon_id}),StatusCode::OK).await;
            agents.push(agent.id);
        }
        common::configure_execution_test_setup(
            &harness.state.db,
            &project,
            &repo_id,
            &agents[0],
            &agents[1],
        )
        .await;
        Self {
            harness,
            server,
            registration,
            owner,
            project,
            agent: agents.remove(0),
            root,
            _dir: dir,
        }
    }
    async fn task(&self, role: &str, parent: Option<&str>) -> String {
        let task:TaskResponse=json_request(&self.harness.app,Method::POST,&format!("/api/v1/projects/{}/tasks",self.project),json!({"title":format!("Remote {role}"),"description":"complete remote plan","parent_task_id":parent,"task_type":if parent.is_some() {"sub_task"} else {"task"}}),StatusCode::OK).await;
        sqlx::query("UPDATE task SET status = ?, plan = ? WHERE id = ?")
            .bind(if role == "planner" {
                "planning"
            } else {
                "in_progress"
            })
            .bind("- [ ] database fallback\n")
            .bind(&task.id)
            .execute(self.harness.state.db.pool())
            .await
            .unwrap();
        // Use real assignments and the ordinary dispatcher, with planner/coder
        // determined from the state rather than a hand-created execution row.
        task.id
    }
    async fn dispatch(&self) -> Result<u64, services::ServiceError> {
        let result = services::TaskDispatcher::new(
            self.harness.state.db.clone(),
            self.harness.state.event_bus.clone(),
            self.harness.state.task_service.clone(),
        )
        .check_once()
        .await;
        result
    }
    async fn settled(&self, task_id: &str, expected: &str) {
        for _ in 0..300 {
            let task = TaskRepo::get_by_id(&*self.harness.state.db, task_id, false)
                .await
                .unwrap()
                .unwrap();
            if task.status == expected {
                assert!(!task
                    .metadata_json
                    .unwrap_or_default()
                    .contains("owner_unsupported"));
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!(
            "task did not reach {expected}: {:?}",
            TaskRepo::get_by_id(&*self.harness.state.db, task_id, false)
                .await
                .unwrap()
        );
    }
}

#[tokio::test]
async fn daemon_plan_dispatch_coder_first_existing_completion_and_subtask() {
    let fixture = Fixture::new("daemon-plan-coder", true).await;
    let task = fixture.task("coder", None).await;
    fixture.dispatch().await.unwrap();
    let start = fixture.owner.started(&task).await;
    assert_eq!(start["plan_text"], "- [ ] database fallback\n");
    assert!(!fixture.root.exists());
    let report = fixture.owner.terminal(&start, "- [x] database fallback\n");
    fixture.settled(&task, "review").await;
    fixture.owner.acknowledged(&report).await;
    assert_eq!(
        fixture.owner.canonical.lock().unwrap().as_deref(),
        Some("- [x] database fallback\n")
    );
    let row = ExecutionRepo::get_by_id(
        &*fixture.harness.state.db,
        start["execution_id"].as_str().unwrap(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(row.status, db::ExecutionStatus::Completed);
    // Sending the Task back creates a fresh state-entry authority while
    // retaining the owner's canonical plan and workspace.
    let running = ExecutionRepo::list_running_by_task(&*fixture.harness.state.db, &task)
        .await
        .unwrap();
    for execution in running {
        fixture
            .harness
            .state
            .task_service
            .cancel_execution(&execution.id, "send back".into())
            .await
            .unwrap();
    }
    let current = TaskRepo::get_by_id(&*fixture.harness.state.db, &task, false)
        .await
        .unwrap()
        .unwrap();
    fixture
        .harness
        .state
        .task_service
        .transition(
            &task,
            "in_progress".into(),
            (current.version, Some("send back".into())),
        )
        .await
        .unwrap();
    fixture.dispatch().await.unwrap();
    let restarted = fixture
        .owner
        .started_after(&task, start["execution_id"].as_str())
        .await;
    assert_ne!(restarted["execution_id"], start["execution_id"]);
    assert_eq!(restarted["plan_text"], "- [x] database fallback\n");
    fixture
        .harness
        .state
        .task_service
        .cancel_execution(
            restarted["execution_id"].as_str().unwrap(),
            "prepare child".into(),
        )
        .await
        .unwrap();
    // The cancellation above only releases the fixture's in-flight run;
    // keep the coordination root runnable for its ordered child.
    sqlx::query("UPDATE task SET error_annotation=NULL, blocked_json=NULL WHERE id=?")
        .bind(&task)
        .execute(fixture.harness.state.db.pool())
        .await
        .unwrap();
    let child = fixture.task("coder", Some(&task)).await;
    fixture.dispatch().await.unwrap();
    let child_start = fixture.owner.started(&child).await;
    assert_eq!(child_start["workspace_path"], start["workspace_path"]);
    assert!(!fixture.root.exists());
}

#[tokio::test]
async fn daemon_plan_dispatch_planner_publication_and_terminal_reconnect_replay() {
    let mut fixture = Fixture::new("daemon-plan-planner", true).await;
    let task = fixture.task("planner", None).await;
    fixture.dispatch().await.unwrap();
    let start = fixture.owner.started(&task).await;
    assert_eq!(start["plan_text"], "- [ ] database fallback\n");
    let report = fixture.owner.terminal(&start, "- [ ] owner revision\n");
    fixture.owner.acknowledged(&report).await;
    // The default planner advances to implementation as on a server owner.
    fixture.settled(&task, "in_progress").await;
    for _ in 0..300 {
        if fixture.owner.canonical.lock().unwrap().as_deref() == Some("- [ ] owner revision\n") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        fixture.owner.canonical.lock().unwrap().as_deref(),
        Some("- [ ] owner revision\n")
    );
    let before: i64 =
        sqlx::query_scalar("SELECT count(*) FROM execution_terminal_receipt WHERE execution_id=?")
            .bind(start["execution_id"].as_str().unwrap())
            .fetch_one(fixture.harness.state.db.pool())
            .await
            .unwrap();
    assert_eq!(before, 1);
    let plan = fixture.owner.canonical.clone();
    fixture.owner.jobs.iter().for_each(|job| job.abort());
    wait_until_disconnected(&fixture.harness.state, &fixture.registration.daemon_id).await;
    fixture.owner = Owner::connect(
        &fixture.server,
        &fixture.registration,
        fixture.root.clone(),
        plan,
        true,
    )
    .await;
    fixture
        .owner
        .outbound
        .send(DaemonFrame::Notification {
            method: METHOD_EXECUTION_TERMINAL.into(),
            params: report,
        })
        .unwrap();
    let replay = json!({"terminal_report_id": format!("terminal-{}", start["execution_id"].as_str().unwrap())});
    fixture.owner.acknowledged(&replay).await;
    let after: i64 =
        sqlx::query_scalar("SELECT count(*) FROM execution_terminal_receipt WHERE execution_id=?")
            .bind(start["execution_id"].as_str().unwrap())
            .fetch_one(fixture.harness.state.db.pool())
            .await
            .unwrap();
    assert_eq!(after, 1);
    assert!(!fixture.root.exists());
    assert!(fixture
        .owner
        .requests
        .lock()
        .unwrap()
        .iter()
        .all(|(_, p)| p["operation"]["kind"] != "publish_plan"));
}

#[tokio::test]
async fn daemon_plan_dispatch_missing_capability_is_structured_placement_refusal() {
    let fixture = Fixture::new("daemon-plan-incapable", false).await;
    let task = fixture.task("planner", None).await;
    let error = fixture
        .harness
        .state
        .task_service
        .claim_task(
            &task,
            services::Assignee::Agent(fixture.agent.clone()),
            None,
        )
        .await
        .unwrap_err();
    let services::ServiceError::PlacementUnavailable(refusal) = error else {
        panic!("unexpected placement error: {error:?}")
    };
    assert!(refusal.rejected_candidates.iter().any(|candidate| candidate
        .filter_codes
        .contains(&services::placement::selection::PlacementFilterCode::CapabilityMissing)));
    assert!(fixture
        .owner
        .requests
        .lock()
        .unwrap()
        .iter()
        .all(|(m, _)| m != METHOD_EXECUTION_START));
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM execution WHERE task_id=?")
        .bind(&task)
        .fetch_one(fixture.harness.state.db.pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
    assert!(
        WorkspaceRepo::get_by_task_id(&*fixture.harness.state.db, &task)
            .await
            .unwrap()
            .is_none()
    );
    assert!(!fixture.root.exists());
}

#[tokio::test]
async fn daemon_plan_dispatch_worker_existing_owner_workspace() {
    let fixture = Fixture::new("daemon-plan-worker", true).await;
    let mut workflow = services::workflow::default_workflow::default_workflow();
    workflow
        .states
        .iter_mut()
        .find(|state| state.name == "in_progress")
        .unwrap()
        .role = Some("worker".into());
    let mut worker = workflow
        .roles
        .iter()
        .find(|role| role.name == "coder")
        .unwrap()
        .clone();
    worker.name = "worker".into();
    workflow.roles.push(worker);
    sqlx::query("UPDATE project SET workflow_definition=? WHERE id=?")
        .bind(serde_json::to_string(&workflow).unwrap())
        .bind(&fixture.project)
        .execute(fixture.harness.state.db.pool())
        .await
        .unwrap();
    let task = fixture.task("planner", None).await;
    fixture.dispatch().await.unwrap();
    let planner = fixture.owner.started(&task).await;
    let report = fixture
        .owner
        .terminal(&planner, "- [ ] existing owner plan\n");
    fixture.owner.acknowledged(&report).await;
    fixture.settled(&task, "in_progress").await;
    let start = fixture
        .owner
        .started_after(&task, planner["execution_id"].as_str())
        .await;
    assert_eq!(start["workspace_path"], planner["workspace_path"]);
    assert_eq!(start["plan_text"], "- [ ] existing owner plan\n");
    let execution = ExecutionRepo::get_by_id(
        &*fixture.harness.state.db,
        start["execution_id"].as_str().unwrap(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(execution.role, "worker");
    assert!(!fixture.root.exists());
}

#[tokio::test]
async fn daemon_plan_dispatch_guard_send_back_restores_owner_plan() {
    let fixture = Fixture::new("daemon-plan-rollback", true).await;
    *fixture.owner.canonical.lock().unwrap() = Some("- [x] prior plan\n".into());
    let task = fixture.task("coder", None).await;
    fixture.dispatch().await.unwrap();
    let start = fixture.owner.started(&task).await;
    assert_eq!(start["plan_text"], "- [x] prior plan\n");
    fixture
        .owner
        .terminal(&start, "- [ ] incomplete candidate\n");
    for _ in 0..300 {
        if fixture
            .owner
            .requests
            .lock()
            .unwrap()
            .iter()
            .any(|(_, p)| p["operation"]["kind"] == "restore_plan")
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(fixture
        .owner
        .requests
        .lock()
        .unwrap()
        .iter()
        .any(|(_, p)| p["operation"]["kind"] == "restore_plan"));
    assert_eq!(
        fixture.owner.canonical.lock().unwrap().as_deref(),
        Some("- [x] prior plan\n")
    );
    fixture.dispatch().await.unwrap();
    let retry = fixture
        .owner
        .started_after(&task, start["execution_id"].as_str())
        .await;
    assert_eq!(retry["plan_text"], "- [x] prior plan\n");
    assert!(!fixture.root.exists());
}

#[tokio::test]
async fn daemon_plan_dispatch_owner_path_collision_never_seeds_server_outbox() {
    for role in ["coder", "planner"] {
        let fixture = Fixture::new(&format!("daemon-plan-path-collision-{role}"), true).await;
        let task = fixture.task(role, None).await;
        let server_worktree = fixture.root.join(&task).join("repo");
        std::fs::create_dir_all(&server_worktree).unwrap();
        std::fs::write(
            server_worktree.parent().unwrap().join("plan.md"),
            "- [ ] unrelated server plan\n",
        )
        .unwrap();
        fixture.dispatch().await.unwrap();
        let start = fixture.owner.started(&task).await;
        assert_eq!(start["plan_text"], "- [ ] database fallback\n");
        assert!(!server_worktree
            .parent()
            .unwrap()
            .join(".forge-outbox")
            .exists());
        assert_eq!(
            std::fs::read_to_string(server_worktree.parent().unwrap().join("plan.md")).unwrap(),
            "- [ ] unrelated server plan\n"
        );
    }
}
