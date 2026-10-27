//! Shared lead-agent setup and coordination-root model helpers.

use super::*;

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
    let now = db::now_rfc3339();
    let scope = db::new_uuid_v4();
    db::AgentContextScopeRepo::create_context_scope(
        db,
        db::CreateAgentContextScope {
            id: scope.clone(),
            identity_id: agent.to_owned(),
            scope_type: "task".to_owned(),
            scope_id: task_id.to_owned(),
            project_id: Some(project),
            task_id: Some(task_id.to_owned()),
            task_role: Some("planner".to_owned()),
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
            id: db::new_uuid_v4(),
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

pub(super) fn task_scope(task_id: &str) -> CanonicalScope {
    CanonicalScope {
        scope_type: CanonicalScopeType::Task,
        scope_id: task_id.to_owned(),
        workspace_access: WorkspaceAccess::TaskRead,
    }
}

/// One lead operation, as the native host issues it for the run of `agent`
/// on `task_id`.
pub(super) async fn lead_call(
    db: &Arc<db::SqliteDb>,
    service: &Arc<services::TaskService>,
    agent: &str,
    task_id: &str,
    operation: &str,
    payload: Value,
    key: &str,
) -> std::result::Result<Value, forge_agent_host::AgentHostError> {
    let runtime = session(db, agent, task_id).await;
    let provider = services_agents::CoordinationToolProvider::new(Arc::clone(db));
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

impl World {
    /// The scripted lead, created on first use.
    pub(super) async fn lead_agent(&self) -> Result<String, String> {
        let db = &*self.live().state.db;
        let err = |error: db::DbError| error.to_string();
        if db::AgentRepo::get_by_id(db, LEAD_AGENT_ID)
            .await
            .map_err(err)?
            .is_some()
        {
            return Ok(LEAD_AGENT_ID.to_owned());
        }
        let daemon_id: Option<String> = sqlx::query_scalar(
            "SELECT p.daemon_id FROM agent_profile p JOIN agent_identity i ON i.selected_profile_id = p.id WHERE i.id = ?",
        )
        .bind(&self.coder_id)
        .fetch_one(&self.live().pool)
        .await
        .map_err(|error| error.to_string())?;
        let now = db::now_rfc3339();
        let agent = db::AgentRepo::create_identity_with_profile(
            db,
            db::CreateAgentIdentity {
                id: LEAD_AGENT_ID.to_owned(),
                name: "model-lead".to_owned(),
                description: None,
                max_concurrent_tasks: 2,
                heartbeat_interval_seconds: 30,
                max_missed_heartbeats: 3,
                status: db::AgentStatus::Idle,
                last_heartbeat_at: Some(now.clone()),
                is_default: false,
                paused: false,
                owner_id: None,
                visibility: "global".to_owned(),
                account_permission_ceiling: r#"["read_task","read_memory","task_read"]"#.to_owned(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
            db::CreateAgentProfile {
                id: db::new_uuid_v4(),
                identity_id: LEAD_AGENT_ID.to_owned(),
                backend_kind: "native".to_owned(),
                executor_type: "codex".to_owned(),
                provider: None,
                model: None,
                reasoning_effort: None,
                permission_policy: None,
                prompt_template: None,
                capabilities_json: "{}".to_owned(),
                tool_policy_json: r#"["read_task","read_memory","task_read"]"#.to_owned(),
                config_json: "{}".to_owned(),
                credential_ref: None,
                daemon_id,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .map_err(err)?;
        db::AgentConnectionHealthRepo::upsert_connection_health(
            db,
            db::UpsertAgentConnectionHealth {
                profile_id: agent.profile_id.clone(),
                status: "healthy".to_owned(),
                capability_status_json: "{}".to_owned(),
                checked_at: Some(now.clone()),
                error_code: None,
                updated_at: now,
            },
        )
        .await
        .map_err(err)?;
        Ok(LEAD_AGENT_ID.to_owned())
    }

    /// `Action::AssignLead`: the lead becomes the planner of the Task. A
    /// refusal (the Task is a subtask, or cannot have a lead) is an answer.
    pub(super) async fn assign_lead(&mut self, index: usize) -> Result<String, String> {
        let lead = self.lead_agent().await?;
        let (status, body) = self
            .request(
                Method::PUT,
                &format!("/api/v1/tasks/{}/roles/planner", self.tasks[index]),
                Some(json!({ "assignee_type": "agent", "assignee_id": lead })),
            )
            .await?;
        Ok(if status.is_success() {
            "lead assigned".to_owned()
        } else {
            format!("refused {status} {}", body["code"])
        })
    }

    /// One model step outside `run`: the action, then every invariant of a
    /// committed and of a quiescent state.
    pub(super) async fn lead_step(&mut self, action: Action) -> Result<String, String> {
        let result: Result<String, String> = async {
            let applied = self.apply(&action).await?;
            self.check_committed().await?;
            self.quiesce().await?;
            let state = self.check_quiescent().await?;
            self.trace.push(format!("{action:?} -> {applied}: {state}"));
            Ok(applied)
        }
        .await;
        result.map_err(|error| {
            format!(
                "{error}\nfailing action: {action:?}\ntrace: {:#?}",
                self.trace
            )
        })
    }

    pub(super) async fn lead_count(&self, sql: &str) -> Result<i64, String> {
        sqlx::query_scalar(sql)
            .fetch_one(&self.live().pool)
            .await
            .map_err(|error| error.to_string())
    }
}

/// A coordination root with a lead, end to end on the real stack: the lead
/// plans (the root rests in `planning` with its run live, its subtasks parked
/// on it), then checks every finished step; one step is returned once and
/// then accepted. Every invariant runs after every action, so (i), (j) of
/// stage 1 and (m), (n) of stage 3 are checked on states that have a lead, a
/// planning root, running step checks and a charged `gate:lead_check`.
pub(super) async fn root_with_a_lead() -> Result<(), String> {
    let mut world = World::new().await;
    world.create(None).await?;
    for _ in 0..2 {
        let child = world.create(Some(0)).await?;
        if child.starts_with("refused") {
            return Err(format!("the subtask was {child}"));
        }
    }
    let assigned = world.lead_step(Action::AssignLead(0)).await?;
    if assigned != "lead assigned" {
        return Err(format!("the lead was {assigned}"));
    }
    // The lead plans: its run is live on the root, nothing else runs.
    let root = world.task(0).await?;
    let planning = root["status"] == "planning";
    if planning {
        if !world.waiting(0) {
            return Err(format!(
                "the root plans and its lead does not run: {}",
                root["condition"]
            ));
        }
        let parked = world
            .lead_count(
                "SELECT COUNT(*) FROM task c WHERE c.parent_task_id IS NOT NULL
                   AND json_extract(c.condition_json, '$.primary.kind') = 'parent'
                   AND json_extract(c.condition_json, '$.primary.cause') = 'planning'",
            )
            .await?;
        if parked != 2 {
            return Err(format!(
                "(j) {parked} of 2 subtasks are parked on the planning root"
            ));
        }
        world.lead_step(Action::Finish(0, Outcome::Success)).await?;
    }
    // The first step is returned once, then accepted.
    world.lead_step(Action::Verdict(1, false)).await?;
    if !world.waiting(1) {
        return Err(format!(
            "the first subtask does not run: {:#?}",
            world.trace
        ));
    }
    world.lead_step(Action::Finish(1, Outcome::Success)).await?;
    // The step rests in the gate with the lead's check live: (m), (h).
    if world.task(1).await?["status"] != "lead_check" || !world.waiting(1) {
        return Err(format!(
            "(m) the finished step is not in the gate with its check running: {}; trace {:#?}",
            world.task(1).await?,
            world.trace
        ));
    }
    let live = world
        .lead_count(
            "SELECT COUNT(*) FROM execution e JOIN task c ON c.id = e.task_id
             WHERE c.status = 'lead_check' AND e.role = 'planner' AND e.status = 'running'
               AND e.agent_id = 'model-lead'",
        )
        .await?;
    if live != 1 {
        return Err(format!(
            "(m) {live} step checks run under the lead, 1 expected"
        ));
    }
    world.lead_step(Action::Finish(1, Outcome::Success)).await?;
    let returned = world
        .lead_count(
            "SELECT COALESCE(MAX(spent), 0) FROM task_budget WHERE kind = 'gate:lead_check'",
        )
        .await?;
    if returned != 1 || world.task(1).await?["status"] != "in_progress" {
        return Err(format!(
            "(n) the lead's `fail` did not return the step once: spent {returned}, task {}; trace {:#?}",
            world.task(1).await?,
            world.trace
        ));
    }
    world.lead_step(Action::Verdict(1, true)).await?;
    world.lead_step(Action::Finish(1, Outcome::Success)).await?;
    world.lead_step(Action::Finish(1, Outcome::Success)).await?;
    if world.task(1).await?["status"] != "done" {
        return Err(format!(
            "the lead's `pass` did not finish the step: {}; trace {:#?}",
            world.task(1).await?,
            world.trace
        ));
    }
    // The second step and the root's review and merge: every waiting run
    // (the worker, the lead's check) is finished by the settlement drive.
    world.drive_to_settlement().await?;
    for index in 0..world.tasks.len() {
        let task = world.task(index).await?;
        if task["status"] != "done" {
            return Err(format!(
                "task {index} ended {}: {}; trace {:#?}",
                task["status"], task["condition"], world.trace
            ));
        }
    }
    let checks = world
        .lead_count(
            "SELECT COUNT(*) FROM execution e JOIN task c ON c.id = e.task_id
             WHERE c.parent_task_id IS NOT NULL AND e.role = 'planner' AND e.status = 'completed'",
        )
        .await?;
    if checks != 3 {
        return Err(format!(
            "(m) 3 step checks were expected (fail, pass, pass), {checks} ran"
        ));
    }
    let skipped = world
        .lead_count("SELECT COUNT(*) FROM transition_log WHERE from_state = 'lead_check' AND bridge_kind = 'gate_skipped'")
        .await?;
    if skipped != 0 {
        return Err(format!(
            "{skipped} step(s) passed the gate unchecked under a lead"
        ));
    }
    if !planning {
        return Err(format!(
            "(i), (j) the root never rested in planning under its lead; trace {:#?}",
            world.trace
        ));
    }
    Ok(())
}
