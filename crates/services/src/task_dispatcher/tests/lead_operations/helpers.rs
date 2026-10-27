//! Shared fixtures and helpers for lead operations tests.

use super::*;
pub(super) const PLANNER: &str = crate::workflow::default_roles::PLANNER;
pub(super) const CODER: &str = crate::workflow::default_roles::CODER;

/// One operation of a scripted lead run.
#[derive(Clone)]
pub(super) enum Call {
    Fail,
    Verdict(&'static str),
    Raw(&'static str, Value),
    /// The lead reads the subtask first and sends the version it was shown.
    AtVersion(&'static str, String, Value),
}

pub(super) type Results = Arc<Mutex<Vec<(String, std::result::Result<Value, String>)>>>;
pub(super) type Script = Arc<Mutex<VecDeque<Vec<Call>>>>;

/// A lead whose every planner run issues the next entry of the script and
/// then completes. Every other role is left running.
pub(super) struct ScriptedLead {
    pub(super) db: Arc<db::SqliteDb>,
    pub(super) sender: mpsc::UnboundedSender<ExecutionContext>,
    pub(super) service: Arc<OnceLock<Arc<TaskService>>>,
    pub(super) script: Script,
    pub(super) results: Results,
}

#[async_trait]
impl TaskExecutor for ScriptedLead {
    async fn execute(
        &self,
        ctx: ExecutionContext,
    ) -> std::result::Result<ExecutionResult, ExecutorError> {
        let execution = ExecutionRepo::get_by_id(&*self.db, &ctx.execution_id)
            .await
            .ok()
            .flatten();
        let role = execution
            .as_ref()
            .map(|execution| execution.role.clone())
            .unwrap_or_default();
        let (task_id, execution_id) = (ctx.task_id.clone(), ctx.execution_id.clone());
        let _ = self.sender.send(ctx);
        if role != PLANNER {
            pending::<()>().await;
            unreachable!()
        }
        let calls = self.script.lock().unwrap().pop_front().unwrap_or_default();
        let lead = execution.and_then(|execution| execution.agent_id).unwrap();
        let service = Arc::clone(self.service.get().expect("the service is wired"));
        for (index, call) in calls.into_iter().enumerate() {
            let key = format!("{execution_id}:{index}");
            let (operation, payload) = match call {
                Call::Verdict(verdict) => {
                    return Ok(ExecutionResult {
                        status: executors::ExecutionOutcome::Completed,
                        summary: Some(verdict.to_owned()),
                        ..Default::default()
                    });
                }
                Call::Fail => {
                    return Ok(ExecutionResult {
                        status: executors::ExecutionOutcome::Failed,
                        error: Some("asking run failed".to_owned()),
                        ..Default::default()
                    })
                }
                Call::Raw(operation, payload) => (operation, payload),
                Call::AtVersion(operation, subtask, mut payload) => {
                    let read = lead_call(
                        &self.db,
                        &service,
                        &lead,
                        &task_id,
                        "lead.read",
                        json!({"task_id": subtask}),
                        &format!("{key}:read"),
                    )
                    .await
                    .expect("the lead reads its subtask");
                    payload["version"] = read["subtask"]["version"].clone();
                    (operation, payload)
                }
            };
            let result = lead_call(
                &self.db, &service, &lead, &task_id, operation, payload, &key,
            )
            .await
            .map_err(|error| format!("{error:?}"));
            self.results
                .lock()
                .unwrap()
                .push((operation.to_owned(), result));
        }
        Ok(ExecutionResult {
            status: executors::ExecutionOutcome::Completed,
            ..Default::default()
        })
    }

    async fn cancel(&self, _execution_id: &str) -> std::result::Result<(), ExecutorError> {
        Ok(())
    }
}

/// The native Task session of `agent` on `task_id`, created once.
pub(super) async fn session(db: &db::SqliteDb, agent: &str, task_id: &str) -> String {
    let runtime = format!("runtime:{agent}:{task_id}");
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_session WHERE runtime_session_id=?)")
            .bind(&runtime)
            .fetch_one(db.pool())
            .await
            .unwrap();
    if exists {
        return runtime;
    }
    let (profile, project): (String, String) = sqlx::query_as(
        "SELECT i.selected_profile_id, t.project_id FROM agent_identity i, task t WHERE i.id=? AND t.id=?",
    )
    .bind(agent)
    .bind(task_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    let now = now_rfc3339();
    let scope = new_uuid_v4();
    db::AgentContextScopeRepo::create_context_scope(
        db,
        db::CreateAgentContextScope {
            id: scope.clone(),
            identity_id: agent.to_owned(),
            scope_type: "task".to_owned(),
            scope_id: task_id.to_owned(),
            project_id: Some(project),
            task_id: Some(task_id.to_owned()),
            task_role: Some(PLANNER.to_owned()),
            workspace_access: "task_read".to_owned(),
            authority_json: "{}".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
            workspace_path: None,
        },
    )
    .await
    .expect("context scope creates");
    db::AgentSessionRepo::create_agent_session(
        db,
        db::CreateAgentSession {
            id: new_uuid_v4(),
            identity_id: agent.to_owned(),
            profile_id: profile,
            context_scope_id: scope,
            backend_kind: "native".to_owned(),
            runtime_session_id: Some(runtime.clone()),
            status: "running".to_owned(),
            capabilities_json: "{}".to_owned(),
            connection_status: "healthy".to_owned(),
            predecessor_session_id: None,
            last_activity_at: Some(now.clone()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("agent session creates");
    runtime
}

/// One lead operation, as the native host issues it for the run of `agent`
/// on `task_id`.
pub(super) fn decode_lead_input<T: serde::de::DeserializeOwned>(
    payload: Value,
) -> std::result::Result<T, AgentHostError> {
    serde_json::from_value(payload)
        .map_err(|error| AgentHostError::Runtime(format!("invalid lead operation: {error}")))
}

pub(super) async fn lead_call(
    db: &Arc<db::SqliteDb>,
    service: &Arc<TaskService>,
    agent: &str,
    task_id: &str,
    operation: &str,
    payload: Value,
    key: &str,
) -> std::result::Result<Value, forge_agent_host::AgentHostError> {
    let runtime = session(db, agent, task_id).await;
    let authority = db
        .resolve_task_effective_authority(agent, None, task_id, "task_read", &runtime)
        .await
        .map_err(|_| AgentHostError::ProtectedPersistence)?;
    let operation_registry::authority::Principal::TaskAgent {
        execution_id,
        lead_root_id: Some(root_id),
        ..
    } = authority.principal
    else {
        return Err(AgentHostError::Authority(
            "OperationNotInScope: this run does not lead a coordination root".to_owned(),
        ));
    };
    let run = crate::LeadRun {
        identity_id: agent.to_owned(),
        execution_id,
        task_id: task_id.to_owned(),
        root_id,
    };
    let result = match operation {
        "lead.read" => {
            let input: operation_registry::task_lead::Read = decode_lead_input(payload)?;
            service.lead_read(&run, input.task_id.as_deref()).await
        }
        "lead.subtask" => {
            let input: operation_registry::task_lead::Subtask = decode_lead_input(payload)?;
            match input {
                operation_registry::task_lead::Subtask::Create {
                    title,
                    description,
                    assignee_id,
                } => {
                    service
                        .lead_create_subtask(&run, title, description, assignee_id, key)
                        .await
                }
                operation_registry::task_lead::Subtask::Update {
                    task_id,
                    version,
                    title,
                    description,
                } => {
                    service
                        .lead_update_subtask(&run, &task_id, version, title, description, key)
                        .await
                }
                operation_registry::task_lead::Subtask::Cancel {
                    task_id,
                    version,
                    reason,
                } => {
                    service
                        .lead_cancel_subtask(&run, &task_id, version, reason)
                        .await
                }
                operation_registry::task_lead::Subtask::Reorder { ordered_task_ids } => {
                    service
                        .lead_reorder_subtasks(&run, ordered_task_ids, key)
                        .await
                }
                operation_registry::task_lead::Subtask::Assign { task_id, agent_id } => {
                    service
                        .lead_assign_worker(&run, task_id.as_deref(), &agent_id)
                        .await
                }
            }
        }
        "lead.recover" => {
            let input: operation_registry::task_lead::Recover = decode_lead_input(payload)?;
            service
                .lead_recover_subtask(&run, &input.task_id, input.action, input.version, key)
                .await
        }
        "lead.handoff" => {
            let input: operation_registry::task_lead::Handoff = decode_lead_input(payload)?;
            service
                .lead_handoff(&run, &input.task_id, &input.note, key)
                .await
        }
        "lead.request_input" => {
            let input: operation_registry::task_lead::RequestInput = decode_lead_input(payload)?;
            service.lead_request_input(&run, &input.question, key).await
        }
        _ => {
            return Err(AgentHostError::Unsupported(operation.to_owned()));
        }
    };
    result.map_err(|error| match error {
        crate::ServiceError::LeadOperationRefused { refusal, message } => {
            let mut outcome = api_types::OrchestrationOutcome::failed(
                api_types::OutcomeCode::ValidationError,
                operation,
                api_types::CanonicalScopeRef::new(api_types::OutcomeScopeType::Account, ""),
                "",
                message,
            );
            outcome.details = Some(json!({ "refusal": refusal.code() }));
            AgentHostError::StructuredOutcome(Box::new(outcome))
        }
        _error => {
            AgentHostError::StructuredOutcome(Box::new(api_types::OrchestrationOutcome::failed(
                api_types::OutcomeCode::InternalFailure,
                operation,
                api_types::CanonicalScopeRef::new(api_types::OutcomeScopeType::Account, ""),
                "",
                "the Forge operation could not complete",
            )))
        }
    })
}

/// Cancelling a recording run must end its future and release the shared
/// workspace lock, so the answered planning run really reaches the executor.
pub(super) struct CancellableRecording {
    pub(super) sender: mpsc::UnboundedSender<ExecutionContext>,
    pub(super) waiting: Mutex<HashMap<String, tokio::sync::oneshot::Sender<()>>>,
}

#[async_trait]
impl TaskExecutor for CancellableRecording {
    async fn execute(
        &self,
        ctx: ExecutionContext,
    ) -> std::result::Result<ExecutionResult, ExecutorError> {
        let (cancel, cancelled) = tokio::sync::oneshot::channel();
        self.waiting
            .lock()
            .unwrap()
            .insert(ctx.execution_id.clone(), cancel);
        let _ = self.sender.send(ctx);
        let _ = cancelled.await;
        Ok(ExecutionResult {
            status: executors::ExecutionOutcome::Cancelled,
            ..Default::default()
        })
    }
    async fn cancel(&self, execution_id: &str) -> std::result::Result<(), ExecutorError> {
        if let Some(cancel) = self.waiting.lock().unwrap().remove(execution_id) {
            let _ = cancel.send(());
        }
        Ok(())
    }
}

pub(super) struct Ops {
    pub(super) lead: Lead,
    pub(super) service: Arc<TaskService>,
    pub(super) script: Script,
    pub(super) results: Results,
}

impl Ops {
    /// A call the test issues as the lead running on `task_id`.
    pub(super) async fn call(
        &self,
        task_id: &str,
        operation: &str,
        payload: Value,
        key: &str,
    ) -> std::result::Result<Value, String> {
        lead_call(
            &self.lead.db,
            &self.service,
            &self.lead.planner,
            task_id,
            operation,
            payload,
            key,
        )
        .await
        .map_err(|error| format!("{error:?}"))
    }
    pub(super) fn results(&self) -> Vec<(String, std::result::Result<Value, String>)> {
        self.results.lock().unwrap().clone()
    }
    pub(super) fn script(&self, runs: Vec<Vec<Call>>) {
        *self.script.lock().unwrap() = runs.into();
    }
}

/// `scripted`: the lead's runs follow the script and complete. Otherwise
/// every run is left running and the test issues the lead's calls itself.
pub(super) async fn ops_fixture(scripted: bool) -> Ops {
    let cell = Arc::new(OnceLock::new());
    let script: Script = Arc::default();
    let results: Results = Arc::default();
    let (executor_cell, executor_script, executor_results) =
        (Arc::clone(&cell), Arc::clone(&script), Arc::clone(&results));
    let lead = fixture_with(move |db, tx| {
        if scripted {
            Arc::new(ScriptedLead {
                db,
                sender: tx,
                service: executor_cell,
                script: executor_script,
                results: executor_results,
            })
        } else {
            Arc::new(CancellableRecording {
                sender: tx,
                waiting: Mutex::default(),
            })
        }
    })
    .await;
    let service = Arc::clone(&lead.dispatcher.task_service);
    let _ = cell.set(Arc::clone(&service));
    // The Task read ceiling every native Task session has. Profiles are
    // immutable, so the lead gets a profile that carries it.
    let ceiling = r#"["read_task","read_memory","task_read"]"#;
    let now = now_rfc3339();
    let profile = new_uuid_v4();
    db::AgentProfileRepo::create_profile(
        &*lead.db,
        db::CreateAgentProfile {
            id: profile.clone(),
            identity_id: lead.planner.clone(),
            backend_kind: "native".to_owned(),
            executor_type: "embedded".to_owned(),
            provider: Some("fixture".to_owned()),
            model: Some("fixture-model".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: ceiling.to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("the lead's profile creates");
    db::AgentConnectionHealthRepo::upsert_connection_health(
        &*lead.db,
        db::UpsertAgentConnectionHealth {
            profile_id: profile.clone(),
            status: "healthy".to_owned(),
            capability_status_json: "{}".to_owned(),
            checked_at: Some(now.clone()),
            error_code: None,
            updated_at: now,
        },
    )
    .await
    .expect("connection health records");
    sqlx::query(
        "UPDATE agent_identity SET account_permission_ceiling=?, selected_profile_id=? WHERE id=?",
    )
    .bind(ceiling)
    .bind(&profile)
    .bind(&lead.planner)
    .execute(lead.db.pool())
    .await
    .unwrap();
    Ops {
        lead,
        service,
        script,
        results,
    }
}

/// Wait for the scripted lead's first `count` calls and require them to
/// have succeeded: a refused call is the failure to report, not a timeout.
pub(super) async fn calls_succeeded(ops: &Ops, count: usize) {
    until(&ops.lead, "the lead issued its operations", async || {
        ops.results().len() >= count
    })
    .await;
    let results = ops.results();
    assert!(
        results.iter().all(|(_, result)| result.is_ok()),
        "{results:#?}"
    );
}

pub(super) async fn children(lead: &Lead, root: &str) -> Vec<Task> {
    TaskRepo::list_subtasks_ordered(&*lead.db, root)
        .await
        .unwrap()
}

pub(super) async fn running(lead: &Lead, task: &str, role: &str) -> bool {
    executions(&lead.db, task)
        .await
        .contains(&(role.to_owned(), "running".to_owned()))
}

/// Wait until the launch of the planner run on `task` is decided: its
/// embedded runner holds the run's lease (the executor is next), or the run
/// is no longer running.
pub(super) async fn launched(lead: &Lead, task: &str) {
    let decided = async {
        loop {
            let (status, owner): (String, Option<String>) = sqlx::query_as(
                "SELECT status, lease_owner FROM execution WHERE task_id=? AND role=?
                 ORDER BY created_at DESC, id DESC LIMIT 1",
            )
            .bind(task)
            .bind(PLANNER)
            .fetch_one(lead.db.pool())
            .await
            .unwrap();
            if status != "running"
                || owner.is_some_and(|owner| owner.starts_with("embedded-execution:"))
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(30), decided)
        .await
        .expect("the planner run's launch is decided");
}

pub(super) async fn comments(lead: &Lead, task: &str) -> Vec<db::TaskComment> {
    db::TaskCommentRepo::list_comments(
        &*lead.db,
        task,
        PageRequest {
            cursor: None,
            limit: 100,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Asc,
        },
    )
    .await
    .unwrap()
    .items
}

/// A second planning root with its own subtask, led by the same Agent.
pub(super) async fn other_root(lead: &Lead) -> (Task, Task) {
    let root = seed_task(&lead.db, &lead.project, "other root", "todo", 1).await;
    let child = seed_subtask(&lead.db, &root, "other child", "todo", 0).await;
    assign_role(&lead.db, &root.id, CODER, &lead.coder).await;
    assign_role(&lead.db, &root.id, PLANNER, &lead.planner).await;
    (reload(&lead.db, &root.id).await, child)
}

pub(super) async fn started_subtask_and_bounded_creation(slow_launch: bool) {
    let mut ops = ops_fixture(false).await;
    let (root, started, later) = root_with_parked_child(&ops.lead, true).await;
    let held = slow_launch.then(|| {
        let (db, id) = (Arc::clone(&ops.lead.db), root.id.clone());
        crate::task_service::execution::launch_gate::hold(
            &root.id,
            Arc::new(move || {
                let (db, id) = (Arc::clone(&db), id.clone());
                Box::pin(async move {
                    sqlx::query(
                        "UPDATE task SET version = version + 1, updated_at = ? WHERE id = ?",
                    )
                    .bind(now_rfc3339())
                    .bind(&id)
                    .execute(db.pool())
                    .await
                    .unwrap();
                })
            }),
        )
    });
    seed_completed_coder_execution(&ops.lead.db, &started.id).await;
    until(&ops.lead, "the root re-plans", async || {
        running(&ops.lead, &root.id, PLANNER).await
    })
    .await;
    if let Some(held) = &held {
        tokio::time::timeout(Duration::from_secs(30), held.reached())
            .await
            .expect("the lead's run reaches its final launch check");
        held.release();
    }
    // The lead's operations come from its run, so they start once the
    // executor has it. A `running` row is not that yet: the launch still
    // checks the run's authority, and the lead's writes to the root inside
    // that window could make the check fail and end the run.
    let run = tokio::time::timeout(Duration::from_secs(30), ops.lead.rx.recv())
        .await
        .expect("the lead's run reaches its executor")
        .expect("the executor channel is open");
    assert_eq!(run.task_id, root.id);
    let lead = &ops.lead;
    let read = ops
        .call(&root.id, "lead.read", json!({}), "read")
        .await
        .unwrap();
    assert_eq!(read["subtasks"].as_array().unwrap().len(), 2, "{read}");
    assert_eq!(read["subtasks"][0]["started"], true, "{read}");
    assert_eq!(read["subtasks"][0]["lead_decides_park"], true, "{read}");
    assert_eq!(read["subtasks"][1]["started"], false, "{read}");

    let current = reload(&lead.db, &started.id).await;
    for (key, payload) in [
        (
            "update",
            json!({"action": "update", "task_id": current.id, "version": current.version, "title": "rewritten"}),
        ),
        (
            "cancel",
            json!({"action": "cancel", "task_id": current.id, "version": current.version, "reason": "drop it"}),
        ),
    ] {
        let refused = ops
            .call(&root.id, "lead.subtask", payload, key)
            .await
            .unwrap_err();
        assert!(refused.contains("subtask_started"), "{key}: {refused}");
    }
    // The sequence command keeps the started subtask where it is.
    assert!(ops
        .call(
            &root.id,
            "lead.subtask",
            json!({"action": "reorder", "ordered_task_ids": [later.id, current.id]}),
            "reorder-started"
        )
        .await
        .is_err());
    let unchanged = reload(&lead.db, &started.id).await;
    assert_eq!(
        (
            unchanged.title.as_str(),
            unchanged.status.as_str(),
            unchanged.subtask_order
        ),
        ("first step", "in_progress", Some(0))
    );

    // Assign (the subtask's worker, and the root's default worker) and a
    // hand-off note are accepted on the started subtask.
    let assigned = ops
        .call(
            &root.id,
            "lead.subtask",
            json!({"action": "assign", "task_id": current.id, "agent_id": lead.coder}),
            "assign",
        )
        .await
        .unwrap();
    assert_eq!(assigned["default_worker"], false, "{assigned}");
    let default_worker = ops
        .call(
            &root.id,
            "lead.subtask",
            json!({"action": "assign", "agent_id": lead.coder}),
            "assign-default",
        )
        .await
        .unwrap();
    assert_eq!(default_worker["default_worker"], true, "{default_worker}");
    let note = ops
        .call(
            &root.id,
            "lead.handoff",
            json!({"task_id": current.id, "note": "the linker needs the static feature"}),
            "note",
        )
        .await
        .unwrap();
    assert_eq!(note["role"], PLANNER, "{note}");
    let notes = comments(lead, &started.id).await;
    assert!(notes
        .iter()
        .any(|comment| comment.worklog_kind.as_deref() == Some("handoff")
            && comment.content == "the linker needs the static feature"
            && comment.author_id.as_deref() == Some(lead.planner.as_str())));

    // Create: the server fills the parent; the same key creates one subtask.
    let created = ops
        .call(
            &root.id,
            "lead.subtask",
            json!({"action": "create", "title": "third step", "description": "added by the lead"}),
            "create",
        )
        .await
        .unwrap();
    let again = ops
        .call(
            &root.id,
            "lead.subtask",
            json!({"action": "create", "title": "third step", "description": "added by the lead"}),
            "create",
        )
        .await
        .unwrap();
    assert_eq!(created["task_id"], again["task_id"], "{created} {again}");
    assert_eq!(again["replayed"], true, "{again}");
    let all = children(lead, &root.id).await;
    assert_eq!(all.len(), 3);
    assert_eq!(all[2].title, "third step");
    assert_eq!(all[2].parent_task_id.as_deref(), Some(root.id.as_str()));
    assert_eq!(
        (all[2].status.as_str(), all[2].subtask_order),
        ("todo", Some(2))
    );
    // A payload cannot name another parent: the contract has no such field.
    assert!(ops
        .call(
            &root.id,
            "lead.subtask",
            json!({"action": "create", "title": "elsewhere", "parent_task_id": later.id}),
            "parent"
        )
        .await
        .is_err());

    // A subtask that has not started: update, reorder within the untouched
    // suffix, cancel.
    let waiting = reload(&lead.db, &later.id).await;
    ops.call(&root.id, "lead.subtask", json!({"action": "update", "task_id": waiting.id, "version": waiting.version, "title": "second step, narrowed"}), "update-later")
        .await
        .unwrap();
    ops.call(
        &root.id,
        "lead.subtask",
        json!({"action": "reorder", "ordered_task_ids": [current.id, all[2].id, waiting.id]}),
        "reorder",
    )
    .await
    .unwrap();
    let order = children(lead, &root.id).await;
    assert_eq!(
        order
            .iter()
            .map(|task| task.title.as_str())
            .collect::<Vec<_>>(),
        vec!["first step", "third step", "second step, narrowed"]
    );
    let waiting = reload(&lead.db, &later.id).await;
    // A stale version is refused, typed, before anything is written.
    assert!(ops
        .call(&root.id, "lead.subtask", json!({"action": "cancel", "task_id": waiting.id, "version": waiting.version + 5, "reason": "superseded"}), "cancel-stale")
        .await
        .is_err());
    let cancelled = ops
        .call(&root.id, "lead.subtask", json!({"action": "cancel", "task_id": waiting.id, "version": waiting.version, "reason": "superseded"}), "cancel-later")
        .await
        .unwrap();
    assert_eq!(cancelled["status"], "cancelled", "{cancelled}");

    // The per-run bound: one was created above, seven more fit, the next is
    // refused, typed, and creates nothing.
    for index in 0..7 {
        if let Some(held) = held.as_ref().filter(|_| index == 5) {
            held.release();
            launched(lead, &root.id).await;
        }
        let result = ops
            .call(
                &root.id,
                "lead.subtask",
                json!({"action": "create", "title": format!("extra {index}")}),
                &format!("extra-{index}"),
            )
            .await;
        if let Err(error) = &result {
            let task = reload(&lead.db, &root.id).await;
            let runs = ExecutionRepo::list_running_by_task(&*lead.db, &root.id)
                .await
                .unwrap();
            let all = ExecutionRepo::list_by_task_and_role(
                &*lead.db,
                &root.id,
                PLANNER,
                PageRequest {
                    cursor: None,
                    limit: 20,
                    include_total: false,
                    sort_by: SortBy::CreatedAt,
                    sort_order: SortOrder::Desc,
                },
            )
            .await
            .unwrap();
            let agent = AgentRepo::get_by_id(&*lead.db, &lead.planner)
                .await
                .unwrap();
            panic!("extra-{index}: {error}\nroot: {task:#?}\nrunning: {runs:#?}\nplanner history: {all:#?}\nAgent: {agent:#?}");
        }
        result.unwrap();
    }
    let before = children(lead, &root.id).await.len();
    let refused = ops
        .call(
            &root.id,
            "lead.subtask",
            json!({"action": "create", "title": "one too many"}),
            "too-many",
        )
        .await
        .unwrap_err();
    assert!(refused.contains("lead_limit_reached"), "{refused}");
    assert_eq!(children(lead, &root.id).await.len(), before);
    assert_eq!(
        spent(lead, &root.id, db::budget::LEAD_SUBTASKS_KEY).await,
        db::budget::LEAD_SUBTASKS_PER_RUN_LIMIT
    );
}

/// The lead's Attention item of a question, by status.
pub(super) async fn question_items(lead: &Lead, status: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM attention_projection WHERE recommended_action='answer_lead_input' AND status=?",
    )
    .bind(status)
    .fetch_one(lead.db.pool())
    .await
    .unwrap()
}

pub(super) async fn waiting_question(ops: &Ops, root: &str) -> (String, i64) {
    until(
        &ops.lead,
        "the asking run ended and the root waits",
        async || {
            !running(&ops.lead, root, PLANNER).await
                && park(&ops.lead.db, root).await.contains("HumanWork")
        },
    )
    .await;
    sqlx::query_as("SELECT id, version FROM attention_projection WHERE json_extract(details_json, '$.task_id')=? AND recommended_action='answer_lead_input' AND status='open'")
        .bind(root).fetch_one(ops.lead.db.pool()).await.unwrap()
}

pub(super) async fn assert_answer_runs_once(
    ops: &Ops,
    root: &str,
    child: &str,
    item: &str,
    version: i64,
) {
    let before = executions(&ops.lead.db, root).await.len();
    crate::attention_service::AttentionService::new(
        Arc::clone(&ops.lead.db),
        crate::test_support::noop_wake_turns(),
    )
    .resolve_with_answer("owner", item, version, Some("B"))
    .await
    .expect("the preserved question is answerable");
    until(
        &ops.lead,
        "one lead run after the answer releases the child",
        async || {
            reload(&ops.lead.db, root).await.status == "in_progress"
                && running(&ops.lead, child, CODER).await
        },
    )
    .await;
    passes(&ops.lead, 5).await;
    let runs = executions(&ops.lead.db, root).await;
    assert_eq!(runs.len(), before + 1, "one run for the answer: {runs:?}");
    assert_eq!(
        runs.last().unwrap(),
        &(PLANNER.to_owned(), "completed".to_owned())
    );
    assert_eq!(spent(&ops.lead, root, "replan").await, 0);
    assert_eq!(question_items(&ops.lead, "open").await, 0);
}

/// Reconstruct the service and dispatcher over the same durable database.
/// The old executor stays pending and cannot perform a late operation.
pub(super) fn restart_operations(ops: &mut Ops) {
    let db = Arc::new(db::SqliteDb::new(ops.lead.db.pool().clone()));
    let bus = Arc::new(EventBus::new(64));
    let (sender, rx) = mpsc::unbounded_channel();
    let service = Arc::new(
        TaskService::new(Arc::clone(&db), Arc::clone(&bus))
            .with_task_executor(Arc::new(RecordingExecutor { sender }))
            .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default())),
    );
    ops.lead.dispatcher = TaskDispatcher::with_check_interval(
        Arc::clone(&db),
        bus,
        Arc::clone(&service),
        Duration::from_millis(10),
    );
    ops.lead.db = db;
    ops.lead.rx = rx;
    ops.service = service;
}
