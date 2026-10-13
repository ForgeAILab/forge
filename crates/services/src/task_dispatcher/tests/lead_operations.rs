//! Stage 4 of the root lead: the lead's operations. Every call here goes
//! through the native tool boundary (`ForgeToolProvider::propose`), so the
//! registry's principal rule, the database's lead fact and the one command
//! boundary are all in the path. In the end-to-end tests the lead's run is
//! scripted in the test executor: the run issues its operations and
//! completes, and the dispatcher does the rest.
use super::root_lead::{
    executions, fixture_with, park, passes, reload, root_with_child, until, Lead,
};
use super::root_replan::{log, root_with_failed_review, root_with_parked_child, spent};
use super::*;
use forge_agent_host::{CanonicalScope, CanonicalScopeType, ForgeToolProvider, WorkspaceAccess};
use serde_json::{json, Value};
use std::collections::VecDeque;
use std::sync::{Mutex, OnceLock};

const PLANNER: &str = crate::workflow::default_roles::PLANNER;
const CODER: &str = crate::workflow::default_roles::CODER;

/// One operation of a scripted lead run.
#[derive(Clone)]
enum Call {
    Raw(&'static str, Value),
    /// The lead reads the subtask first and sends the version it was shown.
    AtVersion(&'static str, String, Value),
}

type Results = Arc<Mutex<Vec<(String, std::result::Result<Value, String>)>>>;
type Script = Arc<Mutex<VecDeque<Vec<Call>>>>;

/// A lead whose every planner run issues the next entry of the script and
/// then completes. Every other role is left running.
struct ScriptedLead {
    db: Arc<db::SqliteDb>,
    sender: mpsc::UnboundedSender<ExecutionContext>,
    service: Arc<OnceLock<Arc<TaskService>>>,
    script: Script,
    results: Results,
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
async fn session(db: &db::SqliteDb, agent: &str, task_id: &str) -> String {
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

fn task_scope(task_id: &str) -> CanonicalScope {
    CanonicalScope {
        scope_type: CanonicalScopeType::Task,
        scope_id: task_id.to_owned(),
        workspace_access: WorkspaceAccess::TaskRead,
    }
}

/// One lead operation, as the native host issues it for the run of `agent`
/// on `task_id`.
async fn lead_call(
    db: &Arc<db::SqliteDb>,
    service: &Arc<TaskService>,
    agent: &str,
    task_id: &str,
    operation: &str,
    payload: Value,
    key: &str,
) -> std::result::Result<Value, forge_agent_host::AgentHostError> {
    let runtime = session(db, agent, task_id).await;
    let provider = crate::CoordinationToolProvider::new(Arc::clone(db));
    provider.set_task_service(Arc::clone(service));
    ForgeToolProvider::propose(
        &provider,
        agent,
        &task_scope(task_id),
        &runtime,
        operation,
        json!({"operation": operation, "payload": payload, "dedupe_key": key, "correlation_id": key}),
    )
    .await
    // The host wraps a command's value in its outcome envelope.
    .map(|value| match value.get("result") {
        Some(result) if result.is_object() => result.clone(),
        _ => value,
    })
}

struct Ops {
    lead: Lead,
    service: Arc<TaskService>,
    script: Script,
    results: Results,
}

impl Ops {
    /// A call the test issues as the lead running on `task_id`.
    async fn call(
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
    fn results(&self) -> Vec<(String, std::result::Result<Value, String>)> {
        self.results.lock().unwrap().clone()
    }
    fn script(&self, runs: Vec<Vec<Call>>) {
        *self.script.lock().unwrap() = runs.into();
    }
}

/// `scripted`: the lead's runs follow the script and complete. Otherwise
/// every run is left running and the test issues the lead's calls itself.
async fn ops_fixture(scripted: bool) -> Ops {
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
            Arc::new(RecordingExecutor { sender: tx })
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
async fn calls_succeeded(ops: &Ops, count: usize) {
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

async fn children(lead: &Lead, root: &str) -> Vec<Task> {
    TaskRepo::list_subtasks_ordered(&*lead.db, root)
        .await
        .unwrap()
}

async fn running(lead: &Lead, task: &str, role: &str) -> bool {
    executions(&lead.db, task)
        .await
        .contains(&(role.to_owned(), "running".to_owned()))
}

async fn comments(lead: &Lead, task: &str) -> Vec<db::TaskComment> {
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
async fn other_root(lead: &Lead) -> (Task, Task) {
    let root = seed_task(&lead.db, &lead.project, "other root", "todo", 1).await;
    let child = seed_subtask(&lead.db, &root, "other child", "todo", 0).await;
    assign_role(&lead.db, &root.id, CODER, &lead.coder).await;
    assign_role(&lead.db, &root.id, PLANNER, &lead.planner).await;
    (reload(&lead.db, &root.id).await, child)
}

/// The failing-first test of stage 4. The planning run on root A may not
/// touch a subtask of root B: a typed scope refusal and no write. The same
/// call on its own subtask that has not started succeeds and the record
/// names the lead and its execution.
#[tokio::test]
async fn a_lead_updates_its_own_subtask_and_never_another_roots() {
    let ops = ops_fixture(false).await;
    let lead = &ops.lead;
    let (root_a, a1) = root_with_child(lead, true).await;
    let (root_b, b1) = other_root(lead).await;
    until(lead, "both roots plan", async || {
        running(lead, &root_a.id, PLANNER).await && running(lead, &root_b.id, PLANNER).await
    })
    .await;
    let update = |task: &Task| json!({"action": "update", "task_id": task.id, "version": task.version, "description": "narrowed by the lead"});

    let foreign = reload(&lead.db, &b1.id).await;
    let refused = ops
        .call(&root_a.id, "lead.subtask", update(&foreign), "escape")
        .await
        .unwrap_err();
    assert!(refused.contains("outside_lead_subtree"), "{refused}");
    let after = reload(&lead.db, &b1.id).await;
    assert_eq!(
        (after.description.clone(), after.version),
        (foreign.description.clone(), foreign.version),
        "nothing was written to the other root's subtask"
    );
    assert!(comments(lead, &b1.id).await.is_empty());

    let own = reload(&lead.db, &a1.id).await;
    let updated = ops
        .call(&root_a.id, "lead.subtask", update(&own), "own")
        .await
        .unwrap();
    assert_eq!(updated["task_id"], own.id.as_str(), "{updated}");
    let now = reload(&lead.db, &a1.id).await;
    assert_eq!(now.description.as_deref(), Some("narrowed by the lead"));
    assert!(now.version > own.version);
    let execution: String = sqlx::query_scalar(
        "SELECT id FROM execution WHERE task_id=? AND role='planner' AND status='running'",
    )
    .bind(&root_a.id)
    .fetch_one(lead.db.pool())
    .await
    .unwrap();
    let record = comments(lead, &a1.id).await;
    assert_eq!(record.len(), 1, "{record:?}");
    assert_eq!(record[0].author_type, db::CommentAuthorType::Agent);
    assert_eq!(record[0].author_id.as_deref(), Some(lead.planner.as_str()));
    assert_eq!(record[0].execution_id.as_deref(), Some(execution.as_str()));
    assert_eq!(record[0].role.as_deref(), Some(PLANNER));

    // The same refusal for every operation that names a target, whether the
    // target is the other root's subtask, the other root, or no Task at all.
    let foreign = reload(&lead.db, &b1.id).await;
    let escapes = [
        ("lead.read", json!({"task_id": foreign.id})),
        (
            "lead.subtask",
            json!({"action": "cancel", "task_id": foreign.id, "version": foreign.version, "reason": "not mine"}),
        ),
        (
            "lead.subtask",
            json!({"action": "assign", "task_id": foreign.id, "agent_id": lead.coder}),
        ),
        (
            "lead.subtask",
            json!({"action": "reorder", "ordered_task_ids": [foreign.id]}),
        ),
        (
            "lead.subtask",
            json!({"action": "update", "task_id": root_b.id, "version": 1, "title": "mine now"}),
        ),
        (
            "lead.recover",
            json!({"task_id": foreign.id, "action": {"verb": "retry"}, "version": foreign.version}),
        ),
        (
            "lead.recover",
            json!({"task_id": "no-such-task", "action": {"verb": "cancel"}, "version": 1}),
        ),
        (
            "lead.handoff",
            json!({"task_id": foreign.id, "note": "a note for someone else's worker"}),
        ),
        (
            "lead.handoff",
            json!({"task_id": root_b.id, "note": "a note on someone else's root"}),
        ),
    ];
    for (index, (operation, payload)) in escapes.into_iter().enumerate() {
        let refused = ops
            .call(
                &root_a.id,
                operation,
                payload.clone(),
                &format!("escape-{index}"),
            )
            .await
            .unwrap_err();
        assert!(
            refused.contains("outside_lead_subtree"),
            "{operation} {payload}: {refused}"
        );
    }
    let after = reload(&lead.db, &b1.id).await;
    assert_eq!(
        (after.status.as_str(), after.version),
        ("todo", foreign.version)
    );
    assert!(comments(lead, &b1.id).await.is_empty());
    assert!(comments(lead, &root_b.id).await.is_empty());
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_role_assignment WHERE task_id=?")
        .bind(&b1.id)
        .fetch_one(lead.db.pool())
        .await
        .unwrap();
    assert_eq!(rows, 0);
    // The root itself is never a subtask target of its own lead.
    let own_root = reload(&lead.db, &root_a.id).await;
    for (operation, payload) in [
        (
            "lead.subtask",
            json!({"action": "cancel", "task_id": own_root.id, "version": own_root.version, "reason": "give up"}),
        ),
        (
            "lead.recover",
            json!({"task_id": own_root.id, "action": {"verb": "cancel"}, "version": own_root.version}),
        ),
        (
            "lead.handoff",
            json!({"task_id": own_root.id, "note": "to myself"}),
        ),
    ] {
        let refused = ops
            .call(&root_a.id, operation, payload, &format!("root-{operation}"))
            .await
            .unwrap_err();
        assert!(refused.contains("wrong_target"), "{operation}: {refused}");
    }
    assert_eq!(reload(&lead.db, &root_a.id).await.status, "planning");
}

/// A planner run that leads nothing (an ordinary Task in its planning state)
/// is not admitted to any lead operation, and the server's principal says so.
#[tokio::test]
async fn an_ordinary_planner_run_has_no_lead_operation() {
    let ops = ops_fixture(false).await;
    let lead = &ops.lead;
    let plain = seed_task(&lead.db, &lead.project, "plain", "todo", 1).await;
    assign_role(&lead.db, &plain.id, CODER, &lead.coder).await;
    assign_role(&lead.db, &plain.id, PLANNER, &lead.planner).await;
    let (root, _) = root_with_child(lead, true).await;
    until(lead, "both planners run", async || {
        running(lead, &plain.id, PLANNER).await && running(lead, &root.id, PLANNER).await
    })
    .await;
    for (operation, payload) in [
        ("lead.read", json!({})),
        (
            "lead.subtask",
            json!({"action": "create", "title": "smuggled"}),
        ),
        (
            "lead.recover",
            json!({"task_id": root.id, "action": {"verb": "cancel"}, "version": 1}),
        ),
        ("lead.handoff", json!({"task_id": root.id, "note": "note"})),
        ("lead.request_input", json!({"question": "may I?"})),
    ] {
        let denied = ops
            .call(&plain.id, operation, payload, operation)
            .await
            .unwrap_err();
        assert!(
            denied.contains("OperationNotInScope") || denied.contains("operation_not_in_scope"),
            "{operation}: {denied}"
        );
    }
    assert_eq!(children(lead, &plain.id).await.len(), 0);
    let principal = |task: String| async move {
        let runtime = session(&lead.db, &lead.planner, &task).await;
        lead.db
            .resolve_task_effective_authority(&lead.planner, None, &task, "task_read", &runtime)
            .await
            .unwrap()
            .principal
    };
    assert!(matches!(
        principal(plain.id.clone()).await,
        operation_registry::authority::Principal::TaskAgent {
            lead_root_id: None,
            ..
        }
    ));
    assert!(matches!(
        principal(root.id.clone()).await,
        operation_registry::authority::Principal::TaskAgent { lead_root_id: Some(id), .. } if id == root.id
    ));
    // The lead's own run keeps the lead operations; removing the lead takes
    // them away from the run that is still going.
    assert!(ops
        .call(&root.id, "lead.read", json!({}), "read")
        .await
        .is_ok());
    sqlx::query("DELETE FROM task_role_assignment WHERE task_id=? AND role_name='planner'")
        .bind(&root.id)
        .execute(lead.db.pool())
        .await
        .unwrap();
    assert!(ops
        .call(&root.id, "lead.read", json!({}), "read-2")
        .await
        .is_err());
}

/// What a lead may do to a subtask depends on whether it started. A started
/// one is recovered, reassigned or given a note; one that has not started is
/// updated, reordered or cancelled. Creating appends under the root, once per
/// key, and is bounded per run.
#[tokio::test]
async fn a_started_subtask_is_recovered_not_rewritten_and_creation_is_bounded() {
    let ops = ops_fixture(false).await;
    let lead = &ops.lead;
    let (root, started, later) = root_with_parked_child(lead, true).await;
    seed_completed_coder_execution(&lead.db, &started.id).await;
    until(lead, "the root re-plans", async || {
        running(lead, &root.id, PLANNER).await
    })
    .await;
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
        ops.call(
            &root.id,
            "lead.subtask",
            json!({"action": "create", "title": format!("extra {index}")}),
            &format!("extra-{index}"),
        )
        .await
        .unwrap();
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

/// End to end, the park ends by the lead's own operation: a subtask out of
/// retries sends the root to planning, the lead's run retries it, the root
/// leaves planning and the recovered subtask runs. Recovery is bounded per
/// subtask on the root's ledger.
#[tokio::test]
async fn a_lead_recovers_a_parked_subtask_and_it_runs_again() {
    let ops = ops_fixture(true).await;
    let lead = &ops.lead;
    let (root, child, later) = root_with_parked_child(lead, true).await;
    let retry = Call::AtVersion(
        "lead.recover",
        child.id.clone(),
        json!({"task_id": child.id, "action": {"verb": "retry"}}),
    );
    ops.script(vec![vec![retry.clone()]]);
    calls_succeeded(&ops, 1).await;
    until(lead, "the recovered subtask runs", async || {
        reload(&lead.db, &root.id).await.status == "in_progress"
            && running(lead, &child.id, CODER).await
    })
    .await;
    let results = ops.results();
    assert_eq!(results.len(), 1, "{results:?}");
    assert!(results[0].1.is_ok(), "{results:?}");
    assert_eq!(
        log(lead, &root.id).await,
        vec![
            (
                "in_progress".to_owned(),
                "planning".to_owned(),
                Some("replan".to_owned())
            ),
            ("planning".to_owned(), "in_progress".to_owned(), None),
        ]
    );
    assert_eq!(spent(lead, &root.id, "replan").await, 1);
    assert_eq!(
        spent(lead, &root.id, &db::budget::lead_recover_key(&child.id)).await,
        1
    );
    let recovered = reload(&lead.db, &child.id).await;
    assert!(
        crate::task_hierarchy::subtask_decision_park(&recovered).is_none(),
        "the park ended: {:?}",
        recovered.condition
    );
    assert!(
        !running(lead, &later.id, CODER).await,
        "one subtask at a time"
    );
    // The root waits on its subtasks; no further lead run is started.
    passes(lead, 4).await;
    assert_eq!(
        executions(&lead.db, &root.id).await,
        vec![(PLANNER.to_owned(), "completed".to_owned())]
    );
}

/// The recover bound. Two recoveries of one subtask fit in a root's budget
/// window; the third is refused, typed, the subtask stays parked, the root
/// returns to implementation and waits on its subtasks (the Project Agent's
/// from there).
#[tokio::test]
async fn a_third_recovery_of_one_subtask_is_refused() {
    let ops = ops_fixture(true).await;
    let lead = &ops.lead;
    let (root, child, _) = root_with_parked_child(lead, true).await;
    let mut tx = lead.db.pool().begin().await.unwrap();
    for step in ["earlier-1", "earlier-2"] {
        db::budget::charge(
            &mut tx,
            &root.id,
            &db::budget::lead_recover_key(&child.id),
            db::budget::LEAD_RECOVER_LIMIT,
            step,
        )
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
    ops.script(vec![vec![Call::AtVersion(
        "lead.recover",
        child.id.clone(),
        json!({"task_id": child.id, "action": {"verb": "retry"}}),
    )]]);
    until(
        lead,
        "the lead ran and the root implements again",
        async || {
            !ops.results().is_empty() && reload(&lead.db, &root.id).await.status == "in_progress"
        },
    )
    .await;
    passes(lead, 3).await;
    let results = ops.results();
    let refused = results[0].1.clone().unwrap_err();
    assert!(refused.contains("lead_limit_reached"), "{refused}");
    let parked = reload(&lead.db, &child.id).await;
    assert!(crate::task_hierarchy::subtask_decision_park(&parked).is_some());
    assert!(!running(lead, &child.id, CODER).await);
    assert!(
        park(&lead.db, &root.id).await.contains("Children"),
        "{}",
        park(&lead.db, &root.id).await
    );
    assert_eq!(
        spent(lead, &root.id, "replan").await,
        1,
        "one re-plan per park"
    );
    assert!(
        !crate::task_hierarchy::lead_handles_subtask_park(&lead.db, &child.id)
            .await
            .unwrap(),
        "the Project Agent is woken for the park the lead could not end"
    );
}

/// End to end, the lead changes the breakdown: it cancels the parked subtask
/// and adds one that replaces it. The root leaves planning and the sequence
/// goes on with the next subtask.
#[tokio::test]
async fn a_lead_drops_a_parked_subtask_and_adds_its_replacement() {
    let ops = ops_fixture(true).await;
    let lead = &ops.lead;
    let (root, child, later) = root_with_parked_child(lead, true).await;
    ops.script(vec![vec![
        Call::AtVersion(
            "lead.recover",
            child.id.clone(),
            json!({"task_id": child.id, "action": {"verb": "cancel", "reason": "replaced by a narrower step"}}),
        ),
        Call::Raw(
            "lead.subtask",
            json!({"action": "create", "title": "first step, narrower", "description": "link statically"}),
        ),
    ]]);
    calls_succeeded(&ops, 2).await;
    until(lead, "the sequence goes on", async || {
        reload(&lead.db, &root.id).await.status == "in_progress"
            && running(lead, &later.id, CODER).await
    })
    .await;
    let results = ops.results();
    assert!(
        results.iter().all(|(_, result)| result.is_ok()),
        "{results:?}"
    );
    assert_eq!(reload(&lead.db, &child.id).await.status, "cancelled");
    let all = children(lead, &root.id).await;
    assert_eq!(
        all.iter()
            .map(|task| (task.title.as_str(), task.status.as_str()))
            .collect::<Vec<_>>(),
        vec![
            ("first step", "cancelled"),
            ("second step", "in_progress"),
            ("first step, narrower", "todo"),
        ]
    );
    assert_eq!(all[2].parent_task_id.as_deref(), Some(root.id.as_str()));
    // The added subtask is the root's work now: the root does not go on to
    // review while it has not run.
    passes(lead, 4).await;
    assert_eq!(reload(&lead.db, &root.id).await.status, "in_progress");
    assert_eq!(
        spent(lead, &root.id, db::budget::LEAD_SUBTASKS_KEY).await,
        1
    );
}

/// End to end, a rejected aggregate review becomes fix work: the lead adds a
/// subtask, the root leaves planning for implementation (not review), and
/// the fix subtask runs.
#[tokio::test]
async fn a_lead_turns_a_rejected_review_into_a_fix_subtask_that_runs() {
    let ops = ops_fixture(true).await;
    let lead = &ops.lead;
    let root = root_with_failed_review(lead, true).await;
    ops.script(vec![vec![Call::Raw(
        "lead.subtask",
        json!({"action": "create", "title": "fix the build", "description": "make verify fails on the combined result"}),
    )]]);
    calls_succeeded(&ops, 1).await;
    until(lead, "the fix subtask runs", async || {
        let all = children(lead, &root.id).await;
        all.len() == 2 && running(lead, &all[1].id, CODER).await
    })
    .await;
    let results = ops.results();
    assert!(
        results.iter().all(|(_, result)| result.is_ok()),
        "{results:?}"
    );
    assert_eq!(
        log(lead, &root.id).await[1..],
        [
            (
                "review".to_owned(),
                "planning".to_owned(),
                Some("replan".to_owned())
            ),
            ("planning".to_owned(), "in_progress".to_owned(), None),
        ]
    );
    passes(lead, 4).await;
    let implementing = reload(&lead.db, &root.id).await;
    assert_eq!(
        implementing.status, "in_progress",
        "the root waits for the fix"
    );
    assert_eq!(spent(lead, &root.id, "replan").await, 1);
    assert_eq!(spent(lead, &root.id, "review").await, 1);
    let fix = &children(lead, &root.id).await[1];
    assert_eq!(
        (fix.title.as_str(), fix.status.as_str()),
        ("fix the build", "in_progress")
    );
}

/// A hand-off note is the first thing the subtask's next implementation run
/// reads, attributed to the lead, ahead of its description and its ordinary
/// comments.
#[tokio::test]
async fn a_handoff_note_is_first_in_the_subtasks_next_implementation_prompt() {
    let ops = ops_fixture(true).await;
    let (root, child) = root_with_child(&ops.lead, true).await;
    ops.service
        .add_user_comment(
            &child.id,
            "owner".to_owned(),
            "an ordinary comment".to_owned(),
        )
        .await
        .unwrap();
    ops.script(vec![vec![Call::Raw(
        "lead.handoff",
        json!({"task_id": child.id, "note": "start from the parser; the lexer is done"}),
    )]]);
    calls_succeeded(&ops, 1).await;
    {
        let lead = &ops.lead;
        until(lead, "the subtask starts", async || {
            reload(&lead.db, &root.id).await.status == "in_progress"
                && running(lead, &child.id, CODER).await
        })
        .await;
    }
    assert!(
        ops.results().iter().all(|(_, result)| result.is_ok()),
        "{:?}",
        ops.results()
    );
    let prompt: Option<String> =
        sqlx::query_scalar("SELECT prompt FROM execution WHERE task_id=? AND role='coder'")
            .bind(&child.id)
            .fetch_one(ops.lead.db.pool())
            .await
            .unwrap();
    let prompt = prompt.expect("the worker's prompt");
    let note = prompt
        .find("Hand-off notes from the lead")
        .unwrap_or_else(|| panic!("the note is in the prompt: {prompt}"));
    assert!(
        prompt.contains("- lead: start from the parser; the lexer is done"),
        "{prompt}"
    );
    let description = prompt.find("echo subtask").expect("the description");
    let ordinary = prompt
        .find("an ordinary comment")
        .expect("the ordinary comment");
    assert!(note < description && note < ordinary, "{prompt}");
    assert_eq!(
        prompt.matches("start from the parser").count(),
        1,
        "the note is not repeated among the ordinary comments: {prompt}"
    );
}

/// The request-input wait. The lead asks, its run ends, and the root waits
/// in planning under a typed park with one Attention item: no relaunch
/// across ten passes and a restart, subtasks held. The owner's answer ends
/// the wait, the lead runs again with the answer in its input, and no
/// re-plan is charged.
#[tokio::test]
async fn a_lead_question_parks_the_root_until_the_owner_answers() {
    let mut ops = ops_fixture(true).await;
    let (root, child) = root_with_child(&ops.lead, true).await;
    ops.script(vec![
        vec![Call::Raw(
            "lead.request_input",
            json!({"question": "Which database should the service use?"}),
        )],
        vec![],
    ]);
    {
        let lead = &ops.lead;
        until(lead, "the root waits for the answer", async || {
            executions(&lead.db, &root.id).await
                == vec![(PLANNER.to_owned(), "completed".to_owned())]
                && park(&lead.db, &root.id).await.contains("HumanWork")
        })
        .await;
    }
    let lead = &ops.lead;
    let results = ops.results();
    assert!(results[0].1.is_ok(), "{results:?}");
    let waiting = reload(&lead.db, &root.id).await;
    assert_eq!(waiting.status, "planning");
    let condition = serde_json::to_string(&waiting.condition.public()).unwrap();
    assert!(
        condition.contains("human_decision") && condition.contains("lead_input"),
        "{condition}"
    );
    let asked = comments(lead, &root.id).await;
    let question = asked
        .iter()
        .find(|comment| comment.worklog_kind.as_deref() == Some("request_input"))
        .expect("the question is a comment on the root");
    assert_eq!(question.content, "Which database should the service use?");
    assert_eq!(question.author_id.as_deref(), Some(lead.planner.as_str()));
    assert!(question.execution_id.is_some());
    let (item, action, status, version): (String, String, String, i64) = sqlx::query_as(
        "SELECT id, recommended_action, status, version FROM attention_projection WHERE dedupe_key=?",
    )
    .bind(format!("lead-input:{}:{}", root.id, question.id))
    .fetch_one(lead.db.pool())
    .await
    .unwrap();
    assert_eq!(
        (action.as_str(), status.as_str()),
        ("answer_lead_input", "open")
    );
    let held = park(&lead.db, &child.id).await;
    assert!(
        held.contains("ParentWait") && held.contains("planning"),
        "{held}"
    );

    // Ten passes and a restart: the lead is not run again and nothing moves.
    passes(lead, 10).await;
    let restarted = TaskDispatcher::with_check_interval(
        Arc::clone(&lead.db),
        Arc::new(EventBus::new(64)),
        Arc::clone(&ops.service),
        Duration::from_millis(10),
    );
    for _ in 0..3 {
        restarted.check_once_and_drain().await.unwrap();
    }
    assert_eq!(
        executions(&lead.db, &root.id).await,
        vec![(PLANNER.to_owned(), "completed".to_owned())]
    );
    assert_eq!(reload(&lead.db, &root.id).await.status, "planning");
    assert!(executions(&lead.db, &child.id).await.is_empty());

    // The item is a question: it is resolved with an answer and not without.
    let attention = crate::attention_service::AttentionService::new(Arc::clone(&lead.db));
    assert!(attention.resolve("owner", &item, version).await.is_err());
    assert_eq!(reload(&lead.db, &root.id).await.status, "planning");
    let resolved = attention
        .resolve_with_answer("owner", &item, version, Some("Postgres, version 16"))
        .await
        .unwrap();
    assert_eq!(resolved.status, "resolved");
    until(
        lead,
        "the lead ran again and the root implements",
        async || {
            executions(&lead.db, &root.id).await
                == vec![
                    (PLANNER.to_owned(), "completed".to_owned()),
                    (PLANNER.to_owned(), "completed".to_owned()),
                ]
                && reload(&lead.db, &root.id).await.status == "in_progress"
                && running(lead, &child.id, CODER).await
        },
    )
    .await;
    assert_eq!(
        spent(lead, &root.id, "replan").await,
        0,
        "an answer is not a re-plan"
    );
    let condition =
        serde_json::to_string(&reload(&lead.db, &root.id).await.condition.public()).unwrap();
    assert!(!condition.contains("lead_input"), "{condition}");
    let mut inputs = Vec::new();
    while let Ok(ctx) = ops.lead.rx.try_recv() {
        if ctx.task_id == root.id {
            inputs.push(ctx.description);
        }
    }
    assert_eq!(inputs.len(), 2);
    assert!(!inputs[0].contains("Postgres"));
    assert!(
        inputs[1].contains("Which database should the service use?")
            && inputs[1].contains("Answer to the lead's question: Postgres, version 16"),
        "the second run reads the answer: {}",
        inputs[1]
    );
}

/// A question is for the planning run on the root: a second one while the
/// root already waits is refused, typed.
#[tokio::test]
async fn a_second_question_while_the_root_waits_is_refused() {
    let ops = ops_fixture(false).await;
    let lead = &ops.lead;
    let (root, _) = root_with_child(lead, true).await;
    until(lead, "the root plans", async || {
        running(lead, &root.id, PLANNER).await
    })
    .await;
    let first = ops
        .call(
            &root.id,
            "lead.request_input",
            json!({"question": "A or B?"}),
            "q1",
        )
        .await
        .unwrap();
    // The same request again answers the same question.
    let replay = ops
        .call(
            &root.id,
            "lead.request_input",
            json!({"question": "A or B?"}),
            "q1",
        )
        .await
        .unwrap();
    assert_eq!(first["comment_id"], replay["comment_id"]);
    let refused = ops
        .call(
            &root.id,
            "lead.request_input",
            json!({"question": "and C?"}),
            "q2",
        )
        .await
        .unwrap_err();
    assert!(refused.contains("lead_input_pending"), "{refused}");
    let items: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM attention_projection WHERE recommended_action='answer_lead_input'",
    )
    .fetch_one(lead.db.pool())
    .await
    .unwrap();
    assert_eq!(items, 1);
}

/// A step check is a verdict on one subtask and nothing else. Its run is the
/// lead's Agent in the lead's role, but it leads nothing: the server derives
/// no root for it, no `lead.*` operation is in its scope, and the breakdown
/// is not changed by it. (The planning run on the root is the one place the
/// lead changes the breakdown; a step it cannot pass ends there by itself.)
#[tokio::test]
async fn a_step_check_run_has_no_lead_operation() {
    let ops = ops_fixture(false).await;
    let lead = &ops.lead;
    let root = seed_task(&lead.db, &lead.project, "root", "in_progress", 1).await;
    let checked = seed_subtask(
        &lead.db,
        &root,
        "first step",
        db::inherited_subtask::STEP_CHECK_STATE,
        0,
    )
    .await;
    let next = seed_subtask(&lead.db, &root, "second step", "todo", 1).await;
    seed_completed_coder_execution(&lead.db, &checked.id).await;
    assign_role(&lead.db, &root.id, CODER, &lead.coder).await;
    assign_role(&lead.db, &root.id, PLANNER, &lead.planner).await;
    until(lead, "the step check runs", async || {
        running(lead, &checked.id, PLANNER).await
    })
    .await;
    let runtime = session(&lead.db, &lead.planner, &checked.id).await;
    let principal = lead
        .db
        .resolve_task_effective_authority(&lead.planner, None, &checked.id, "task_read", &runtime)
        .await
        .unwrap()
        .principal;
    assert!(
        matches!(
            principal,
            operation_registry::authority::Principal::TaskAgent {
                lead_root_id: None,
                ..
            }
        ),
        "{principal:?}"
    );
    let waiting = reload(&lead.db, &next.id).await;
    for (operation, payload) in [
        ("lead.read", json!({})),
        (
            "lead.subtask",
            json!({"action": "update", "task_id": waiting.id, "version": waiting.version, "description": "rewritten by a step check"}),
        ),
        (
            "lead.subtask",
            json!({"action": "create", "title": "third step"}),
        ),
        (
            "lead.subtask",
            json!({"action": "cancel", "task_id": waiting.id, "version": waiting.version, "reason": "no"}),
        ),
        (
            "lead.subtask",
            json!({"action": "assign", "task_id": waiting.id, "agent_id": lead.planner}),
        ),
        (
            "lead.recover",
            json!({"task_id": waiting.id, "action": {"verb": "cancel"}, "version": waiting.version}),
        ),
        (
            "lead.handoff",
            json!({"task_id": waiting.id, "note": "note"}),
        ),
        (
            "lead.request_input",
            json!({"question": "is this good enough?"}),
        ),
    ] {
        let denied = ops
            .call(&checked.id, operation, payload.clone(), &format!("{operation}:{payload}"))
            .await
            .unwrap_err();
        assert!(
            denied.contains("OperationNotInScope") || denied.contains("operation_not_in_scope"),
            "{operation}: {denied}"
        );
    }
    // The command boundary says the same to a caller that got past the
    // registry (a principal resolved before the rule, a direct service call).
    let execution: String = sqlx::query_scalar(
        "SELECT id FROM execution WHERE task_id=? AND role='planner' AND status='running'",
    )
    .bind(&checked.id)
    .fetch_one(lead.db.pool())
    .await
    .unwrap();
    let refused = ops
        .service
        .lead_create_subtask(
            &crate::LeadRun {
                identity_id: lead.planner.clone(),
                execution_id: execution,
                task_id: checked.id.clone(),
                root_id: root.id.clone(),
            },
            "third step".to_owned(),
            None,
            None,
            "direct",
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            refused,
            crate::ServiceError::LeadOperationRefused {
                refusal: crate::LeadRefusal::NotLeading,
                ..
            }
        ),
        "{refused:?}"
    );
    assert_eq!(children(lead, &root.id).await.len(), 2);
    let untouched = reload(&lead.db, &next.id).await;
    assert_eq!(untouched.version, waiting.version);
    assert_eq!(untouched.status, "todo");
    assert_eq!(reload(&lead.db, &root.id).await.status, "in_progress");
}

/// Two questions sent at once by one run: one is asked, the other is
/// refused, and there is one comment, one Attention item and one wait. (The
/// comment, the item and the wait are one transaction.)
#[tokio::test]
async fn two_questions_at_once_ask_one() {
    let ops = ops_fixture(false).await;
    let lead = &ops.lead;
    let (root, _) = root_with_child(lead, true).await;
    until(lead, "the root plans", async || {
        running(lead, &root.id, PLANNER).await
    })
    .await;
    // The session exists before the two calls race.
    session(&lead.db, &lead.planner, &root.id).await;
    let (first, second) = tokio::join!(
        ops.call(
            &root.id,
            "lead.request_input",
            json!({"question": "A or B?"}),
            "q1",
        ),
        ops.call(
            &root.id,
            "lead.request_input",
            json!({"question": "C or D?"}),
            "q2",
        ),
    );
    let (asked, refused) = match (first, second) {
        (Ok(asked), Err(refused)) | (Err(refused), Ok(asked)) => (asked, refused),
        other => panic!("exactly one question is asked: {other:?}"),
    };
    assert!(refused.contains("lead_input_pending"), "{refused}");
    let questions = comments(lead, &root.id)
        .await
        .into_iter()
        .filter(|comment| comment.worklog_kind.as_deref() == Some("request_input"))
        .collect::<Vec<_>>();
    assert_eq!(questions.len(), 1, "the refused question left no comment");
    assert_eq!(asked["comment_id"], questions[0].id.as_str());
    let items: Vec<String> = sqlx::query_scalar(
        "SELECT dedupe_key FROM attention_projection WHERE recommended_action='answer_lead_input' AND status='open'",
    )
    .fetch_all(lead.db.pool())
    .await
    .unwrap();
    assert_eq!(
        items,
        vec![format!("lead-input:{}:{}", root.id, questions[0].id)]
    );
    let waiting = reload(&lead.db, &root.id).await;
    let metadata: Value = serde_json::from_str(waiting.metadata_json.as_deref().unwrap()).unwrap();
    assert_eq!(metadata["awaiting_human_marker_id"], questions[0].id.as_str());
}

/// A lead cannot make itself a worker (its runs read the repository; a
/// worker's run writes it), on a subtask, as the root's default worker or on
/// a subtask it creates.
#[tokio::test]
async fn a_lead_never_assigns_itself_as_a_worker() {
    let ops = ops_fixture(false).await;
    let lead = &ops.lead;
    let (root, child) = root_with_child(lead, true).await;
    until(lead, "the root plans", async || {
        running(lead, &root.id, PLANNER).await
    })
    .await;
    for (payload, key) in [
        (
            json!({"action": "assign", "task_id": child.id, "agent_id": lead.planner}),
            "self-child",
        ),
        (
            json!({"action": "assign", "agent_id": lead.planner}),
            "self-default",
        ),
        (
            json!({"action": "create", "title": "mine", "assignee_id": lead.planner}),
            "self-create",
        ),
    ] {
        let refused = ops
            .call(&root.id, "lead.subtask", payload, key)
            .await
            .unwrap_err();
        assert!(refused.contains("lead_self_assignment"), "{key}: {refused}");
    }
    let coders: Vec<String> = sqlx::query_scalar(
        "SELECT a.assignee_id FROM task_role_assignment a JOIN task t ON t.id=a.task_id
         WHERE a.role_name='coder' AND (t.id=? OR t.parent_task_id=?)",
    )
    .bind(&root.id)
    .bind(&root.id)
    .fetch_all(lead.db.pool())
    .await
    .unwrap();
    assert!(!coders.contains(&lead.planner), "{coders:?}");
    assert_eq!(children(lead, &root.id).await.len(), 1);
}

/// "Started" is any run on the subtask that is not the step check, whatever
/// the Project's workflow calls its implementation role.
#[tokio::test]
async fn a_subtask_started_under_any_role_name_is_not_rewritten() {
    let ops = ops_fixture(false).await;
    let lead = &ops.lead;
    let (root, child) = root_with_child(lead, true).await;
    seed_completed_coder_execution(&lead.db, &child.id).await;
    sqlx::query("UPDATE execution SET role='builder' WHERE task_id=?")
        .bind(&child.id)
        .execute(lead.db.pool())
        .await
        .unwrap();
    until(lead, "the root plans", async || {
        running(lead, &root.id, PLANNER).await
    })
    .await;
    let child = reload(&lead.db, &child.id).await;
    for (payload, key) in [
        (
            json!({"action": "update", "task_id": child.id, "version": child.version, "title": "rewritten"}),
            "update",
        ),
        (
            json!({"action": "cancel", "task_id": child.id, "version": child.version, "reason": "drop"}),
            "cancel",
        ),
    ] {
        let refused = ops
            .call(&root.id, "lead.subtask", payload, key)
            .await
            .unwrap_err();
        assert!(
            refused.contains("subtask_started") || refused.contains("unavailable"),
            "{key}: {refused}"
        );
    }
    let after = reload(&lead.db, &child.id).await;
    assert_eq!((after.title, after.status), (child.title, child.status));
}

/// A question whose root was deleted can still be closed by its owner: the
/// item is resolved and nothing else is written.
#[tokio::test]
async fn a_question_of_a_deleted_root_is_resolved_without_an_answer_target() {
    let ops = ops_fixture(false).await;
    let lead = &ops.lead;
    let (root, _) = root_with_child(lead, true).await;
    until(lead, "the root plans", async || {
        running(lead, &root.id, PLANNER).await
    })
    .await;
    ops.call(
        &root.id,
        "lead.request_input",
        json!({"question": "A or B?"}),
        "q1",
    )
    .await
    .unwrap();
    let (item, version): (String, i64) = sqlx::query_as(
        "SELECT id, version FROM attention_projection WHERE recommended_action='answer_lead_input'",
    )
    .fetch_one(lead.db.pool())
    .await
    .unwrap();
    sqlx::query("UPDATE task SET deleted_at=? WHERE id=?")
        .bind(now_rfc3339())
        .bind(&root.id)
        .execute(lead.db.pool())
        .await
        .unwrap();
    let owner: String = sqlx::query_scalar("SELECT owner_id FROM project WHERE id=?")
        .bind(&lead.project)
        .fetch_one(lead.db.pool())
        .await
        .unwrap();
    sqlx::query("INSERT OR IGNORE INTO user (id, email, password_hash, created_at, updated_at) VALUES (?, 'owner@example.test', 'unused', ?, ?)")
        .bind(&owner)
        .bind(now_rfc3339())
        .bind(now_rfc3339())
        .execute(lead.db.pool())
        .await
        .unwrap();
    let attention = crate::attention_service::AttentionService::new(Arc::clone(&lead.db));
    let resolved = attention
        .resolve_with_answer(&owner, &item, version, Some("B"))
        .await
        .unwrap();
    assert_eq!(resolved.status, "resolved");
}
