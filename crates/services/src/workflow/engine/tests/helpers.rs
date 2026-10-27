//! Shared fixtures and helpers for workflow engine tests.

use super::*;

pub(super) async fn sqlite_db() -> SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    SqliteDb::new(pool)
}

pub(super) async fn seed_project_repo_and_task(db: &SqliteDb, task_id: &str, status: &str) {
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();

    ProjectRepo::create(
        db,
        CreateProject {
            id: project_id.clone(),
            name: "Forge".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");

    RepoRepo::create(
        db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "forge".to_owned(),
            remote_url: Some("https://example.com/forge.git".to_owned()),
            local_path: None,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo creates");
    ProjectRepo::update_at_version(
        db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        ProjectRepo::get_by_id(db, &project_id)
            .await
            .expect("fixture Project lookup")
            .expect("fixture Project exists")
            .version,
        None,
    )
    .await
    .expect("project primary repo updates");

    TaskRepo::create(
        db,
        CreateTask {
            id: task_id.to_owned(),
            project_id,
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "test task".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: status.to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("task creates");
}

pub(super) async fn assign_agent_role_without_agent(db: &SqliteDb, task_id: &str, role: &str) {
    let now = now_rfc3339();
    TaskRoleAssignmentRepo::assign(
        db,
        CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: task_id.to_owned(),
            role_name: role.to_owned(),
            assignee_type: Some(db::AssigneeKind::Agent),
            assignee_id: Some("deleted-agent".to_owned()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("role assignment creates");
    sqlx::query(
        "UPDATE task_role_assignment SET assignee_id = NULL WHERE task_id = ? AND role_name = ?",
    )
    .bind(task_id)
    .bind(role)
    .execute(db.pool())
    .await
    .expect("role assignment marks deleted agent");
}

pub(super) async fn assign_user_role(db: &SqliteDb, task_id: &str, role: &str) {
    let now = now_rfc3339();
    TaskRoleAssignmentRepo::assign(
        db,
        CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: task_id.to_owned(),
            role_name: role.to_owned(),
            assignee_type: Some(db::AssigneeKind::User),
            assignee_id: Some("human".to_owned()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("role assignment creates");
}

pub(super) async fn assign_agent_role(db: &SqliteDb, task_id: &str, role: &str, agent_id: &str) {
    let now = now_rfc3339();
    TaskRoleAssignmentRepo::assign(
        db,
        CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: task_id.to_owned(),
            role_name: role.to_owned(),
            assignee_type: Some(db::AssigneeKind::Agent),
            assignee_id: Some(agent_id.to_owned()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("agent role assignment creates");
}

pub(super) fn run_git(path: &std::path::Path, args: &[&str]) {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(path)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .output()
        .expect("git runs");
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

pub(super) fn initialize_git_repo(path: &std::path::Path) {
    run_git(path, &["init", "--initial-branch=main"]);
    run_git(path, &["config", "user.email", "test@forge.dev"]);
    run_git(path, &["config", "user.name", "Forge Test"]);
    std::fs::write(path.join("README.md"), "# Forge\n").expect("README writes");
    run_git(path, &["add", "-A"]);
    run_git(path, &["commit", "-m", "initial commit"]);
}

pub(super) fn engine(db: Arc<SqliteDb>, event_bus: Arc<EventBus>) -> crate::TaskService {
    crate::TaskService::new_for_test(db, event_bus)
}

pub(super) fn hook(action: &str, on_failure: FailurePolicy) -> HookSpec {
    HookSpec {
        action: action.to_owned(),
        params: json!({}),
        applies_to: HookAudience::All,
        on_failure,
    }
}

pub(super) fn state(
    name: &str,
    kind: StateKind,
    role: Option<&str>,
    hooks: StateHooks,
) -> StateDefinition {
    StateDefinition {
        name: name.to_owned(),
        kind,
        column: name.to_owned(),
        display_name: name.to_owned(),
        role: role.map(str::to_owned),
        hooks,
        cleanup: None,
        canonical_phase: Some(match kind {
            StateKind::Backlog => api_types::CanonicalPhase::Backlog,
            StateKind::Initial => api_types::CanonicalPhase::Ready,
            StateKind::Active => api_types::CanonicalPhase::Working,
            StateKind::Gate => api_types::CanonicalPhase::Working,
            StateKind::Terminal => api_types::CanonicalPhase::Done,
            StateKind::Custom => api_types::CanonicalPhase::Working,
        }),
        gate_config: None,
        dispatch: None,
        triggers: std::collections::BTreeMap::new(),
        config: json!({}),
    }
}

pub(super) fn with_trigger(
    mut state: StateDefinition,
    trigger: WorkflowTrigger,
    to: &str,
) -> StateDefinition {
    state.triggers.insert(
        trigger,
        WorkflowTriggerDefinition {
            to: to.to_owned(),
            dispatch: None,
        },
    );
    state
}

pub(super) fn user_approval_review_workflow(
    before_enter_hook: HookSpec,
    after_enter_hooks: Vec<HookSpec>,
) -> WorkflowDefinition {
    let working = with_trigger(
        state("working", StateKind::Active, None, StateHooks::default()),
        WorkflowTrigger::Accept,
        "review",
    );
    let mut review = state(
        "review",
        StateKind::Gate,
        None,
        StateHooks {
            before_enter: vec![before_enter_hook],
            after_enter: after_enter_hooks,
            ..StateHooks::default()
        },
    );
    review.gate_config = Some(GateConfig {
        reject_target: Some("working".to_owned()),
        max_rejections: Some(3),
        approve_label: Some("Approve".to_owned()),
        reject_label: Some("Reject".to_owned()),
        requires_user_approval: Some(true),
        optional_when_unassigned: Some(false),
    });
    review.triggers.insert(
        WorkflowTrigger::Accept,
        WorkflowTriggerDefinition {
            to: "done".to_owned(),
            dispatch: None,
        },
    );
    review.triggers.insert(
        WorkflowTrigger::Reject,
        WorkflowTriggerDefinition {
            to: "working".to_owned(),
            dispatch: None,
        },
    );

    WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            working,
            review,
            state("done", StateKind::Terminal, None, StateHooks::default()),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    }
}

pub(super) fn phases(results: &[HookResultEntry]) -> Vec<&str> {
    results.iter().map(|entry| entry.phase.as_str()).collect()
}

pub(super) fn assert_phases_are_ordered(results: &[HookResultEntry]) {
    let order = [
        "before_exit",
        "on_exit",
        "before_enter",
        "on_enter",
        "after_enter",
    ];
    let mut previous_index = 0;

    for phase in phases(results) {
        let current_index = order
            .iter()
            .position(|candidate| *candidate == phase)
            .unwrap_or_else(|| panic!("unexpected phase: {phase}"));
        assert!(
            current_index >= previous_index,
            "hook phases are out of order: {:?}",
            phases(results)
        );
        previous_index = current_index;
    }
}

pub(super) async fn hook_results(db: &SqliteDb, task_id: &str) -> Vec<HookResultEntry> {
    let logs = TransitionLogRepo::list_by_task(db, task_id)
        .await
        .expect("transition logs list");
    assert!(!logs.is_empty(), "expected at least one transition log row");
    let payload = logs[0]
        .hook_results_json
        .as_deref()
        .expect("hook results are written");
    serde_json::from_str(payload).expect("hook results deserialize")
}

pub(super) fn drain_events(rx: &mut broadcast::Receiver<ForgeEvent>) -> Vec<ForgeEvent> {
    let mut events = Vec::new();
    loop {
        match rx.try_recv() {
            Ok(event) => events.push(event),
            Err(broadcast::error::TryRecvError::Empty) => break,
            Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
            Err(broadcast::error::TryRecvError::Closed) => break,
        }
    }
    events
}

pub(super) fn cascade_chain_workflow(steps: usize) -> WorkflowDefinition {
    let mut states = vec![with_trigger(
        state("start", StateKind::Initial, None, StateHooks::default()),
        WorkflowTrigger::Accept,
        "step_0",
    )];
    for index in 0..steps {
        let last = index + 1 == steps;
        let mut step = with_trigger(
            state(
                &format!("step_{index}"),
                StateKind::Active,
                Some(default_roles::CODER),
                StateHooks {
                    on_enter: vec![hook(
                        if last {
                            "auto_cascade_on_completion"
                        } else {
                            "auto_cascade_on_unassigned_role"
                        },
                        FailurePolicy::Log,
                    )],
                    ..StateHooks::default()
                },
            ),
            WorkflowTrigger::Accept,
            &if last {
                "done".to_owned()
            } else {
                format!("step_{}", index + 1)
            },
        );
        step.gate_config = Some(GateConfig {
            reject_target: None,
            max_rejections: None,
            approve_label: None,
            reject_label: None,
            requires_user_approval: Some(false),
            optional_when_unassigned: Some(true),
        });
        states.push(step);
    }
    states.push(state(
        "done",
        StateKind::Terminal,
        None,
        StateHooks::default(),
    ));
    WorkflowDefinition {
        roles: Vec::new(),
        states,
        configuration: Vec::new(),
        cancellation_state: None,
    }
}

pub(super) async fn drain(engine: crate::TaskService, task_id: &str) -> db::Task {
    crate::worker_runtime::queue::TaskStepWorker::new(engine)
        .drain(task_id)
        .await
        .expect("cascade queue drains")
}

pub(super) async fn drain_result(
    driver: crate::TaskService,
    mut result: super::TransitionResult,
) -> super::TransitionResult {
    result.task = drain(driver.clone(), &result.task.id).await;
    result.review = db::ReviewRepo::list_by_task(&*driver.db, &result.task.id)
        .await
        .unwrap()
        .into_iter()
        .max_by_key(|r| r.attempt_number);
    result.pending_steps = 0;
    result
}

pub(super) async fn seed_custom_workflow_task(
    db: &SqliteDb,
    task_id: &str,
    status: &str,
    workflow: &WorkflowDefinition,
) -> String {
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();
    ProjectRepo::create(
        db,
        CreateProject {
            id: project_id.clone(),
            name: "Custom".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: serde_json::to_string(workflow).unwrap(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");
    RepoRepo::create(
        db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "repo".to_owned(),
            remote_url: Some("https://example.com/repo.git".to_owned()),
            local_path: None,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo creates");
    ProjectRepo::update_at_version(
        db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        ProjectRepo::get_by_id(db, &project_id)
            .await
            .expect("fixture Project lookup")
            .expect("fixture Project exists")
            .version,
        None,
    )
    .await
    .expect("project primary repo updates");
    TaskRepo::create(
        db,
        CreateTask {
            id: task_id.to_owned(),
            project_id: project_id.clone(),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "custom workflow task".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: status.to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("task creates");
    project_id
}

pub(super) struct FailedCiFixture {
    pub(super) db: Arc<SqliteDb>,
    pub(super) engine: crate::TaskService,
    pub(super) workflow: WorkflowDefinition,
    pub(super) task: db::Task,
    pub(super) workspace: db::Workspace,
    pub(super) _repo_dir: TempDir,
    pub(super) _workspace_root: TempDir,
}

impl FailedCiFixture {
    pub(super) async fn workflow_authority(&self) -> super::WorkflowAuthority {
        // Match TaskService's transition entry: review hooks require the
        // current Project snapshot before they can create a Review attempt.
        // Read it here because a test may change the workflow after setup.
        let project = ProjectRepo::get_by_id(&*self.db, &self.task.project_id)
            .await
            .unwrap()
            .unwrap();
        super::WorkflowAuthority {
            project_version: project.version,
            workflow_definition: project.workflow_definition,
            clear_review_passed_at_on_commit: false,
        }
    }
}

pub(super) async fn failed_ci_fixture(budget: i32, policy: FailurePolicy) -> FailedCiFixture {
    let db = Arc::new(sqlite_db().await);
    let task_id = new_uuid_v4();
    seed_project_repo_and_task(&db, &task_id, "merge_failed").await;
    let repo_dir = TempDir::new().unwrap();
    let workspace_root = TempDir::new().unwrap();
    initialize_git_repo(repo_dir.path());
    let mut workflow = default_workflow::default_workflow();
    let review_state = workflow
        .states
        .iter_mut()
        .find(|state| state.name == "review")
        .unwrap();
    review_state
        .hooks
        .before_enter
        .iter_mut()
        .find(|hook| hook.action == "run_ci_steps")
        .unwrap()
        .on_failure = policy;
    // Task-level budget must win over the gate's default.
    review_state.gate_config.as_mut().unwrap().max_rejections = Some(99);
    let task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    sqlx::query("UPDATE repo SET local_path = ?, remote_url = ? WHERE project_id = ?")
        .bind(repo_dir.path().to_string_lossy().as_ref())
        .bind(repo_dir.path().to_string_lossy().as_ref())
        .bind(&task.project_id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE project SET workflow_definition = ?,settings=json_set(settings,'$.review_ci_triage','off') WHERE id = ?")
        .bind(serde_json::to_string(&workflow).unwrap())
        .bind(&task.project_id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE task SET task_state_config = ? WHERE id = ?")
        .bind(json!({"retry_budgets":{"review":budget}, "review":{"ci_steps":["echo 'entry-ci-failed [conflict-handoff]'; exit 101"]}}).to_string())
        .bind(&task.id).execute(db.pool()).await.unwrap();
    let task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    let workspace = crate::task_service::workspace::prepare_workspace(
        &db,
        workspace_root.path(),
        &task,
        &task.id,
        None,
        &crate::lifecycle::context::embedded_workspace_router_for_test(
            Arc::clone(&db),
            workspace_root.path().to_path_buf(),
            None,
        ),
    )
    .await
    .unwrap();
    let sha = git::get_current_sha(std::path::Path::new(
        &workspace.embedded_worktree_path_for_backend(),
    ))
    .await
    .unwrap();
    let now = now_rfc3339();
    db::ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: None,
            role: default_roles::CODER.to_owned(),
            status: db::ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: Some(now.clone()),
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: Some(sha.clone()),
            after_sha: Some(sha),
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: Some(workspace.id.clone()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
    let mut eng = engine(Arc::clone(&db), Arc::new(EventBus::new(32)));
    eng.workspace_root = workspace_root.path().to_path_buf();
    eng = eng.with_workspace_root(workspace_root.path().to_path_buf());
    FailedCiFixture {
        db,
        engine: eng,
        workflow,
        task,
        workspace,
        _repo_dir: repo_dir,
        _workspace_root: workspace_root,
    }
}

pub(super) fn missing_edge_workflow() -> WorkflowDefinition {
    WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            with_trigger(
                state("working", StateKind::Active, None, StateHooks::default()),
                WorkflowTrigger::Accept,
                "paused",
            ),
            state("paused", StateKind::Custom, None, StateHooks::default()),
            state("done", StateKind::Terminal, None, StateHooks::default()),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    }
}

pub(super) fn system_only_fail_edge_workflow() -> WorkflowDefinition {
    WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            with_trigger(
                state("working", StateKind::Active, None, StateHooks::default()),
                WorkflowTrigger::Fail,
                "failed",
            ),
            state("failed", StateKind::Custom, None, StateHooks::default()),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    }
}

pub(super) fn workflow_without_review_state() -> WorkflowDefinition {
    WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            with_trigger(
                state("pending", StateKind::Initial, None, StateHooks::default()),
                WorkflowTrigger::Accept,
                "working",
            ),
            with_trigger(
                state("working", StateKind::Active, None, StateHooks::default()),
                WorkflowTrigger::Accept,
                "shipped",
            ),
            state("shipped", StateKind::Terminal, None, StateHooks::default()),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    }
}

pub(super) async fn seed_parent_and_subtask(
    db: &SqliteDb,
    parent_id: &str,
    subtask_id: &str,
    subtask_status: &str,
    task_state_config: Option<String>,
) {
    seed_project_repo_and_task(db, parent_id, default_states::IN_PROGRESS).await;
    let parent = TaskRepo::get_by_id(db, parent_id, false)
        .await
        .expect("parent loads")
        .expect("parent exists");
    let now = now_rfc3339();
    TaskRepo::create(
        db,
        CreateTask {
            id: subtask_id.to_owned(),
            project_id: parent.project_id.clone(),
            parent_task_id: Some(parent_id.to_owned()),
            subtask_order: Some(0),
            assignee_type: None,
            assignee_id: None,
            title: "subtask".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: subtask_status.to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("subtask creates");
}

pub(super) async fn transition_memory_count(db: &SqliteDb, transition_log_id: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM memory_item WHERE source_type = 'transition'
         AND json_extract(metadata_json, '$.source_ref') = ?",
    )
    .bind(transition_log_id)
    .fetch_one(db.pool())
    .await
    .expect("memory items count")
}

// A review entry hook that fails with no failed Review is charged once per
// entry: the gate cap `max_rejections` allows `max - 1` bounces, then parks.
pub(super) async fn entry_failure_outcomes(
    max_rejections: i32,
    review_override: Option<i32>,
    actor: api_types::Actor,
) -> Vec<String> {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(64));
    let task_id = new_uuid_v4();
    let mut workflow =
        user_approval_review_workflow(hook("run_ci_steps", FailurePolicy::Block), Vec::new());
    workflow
        .states
        .iter_mut()
        .find(|s| s.name == "review")
        .unwrap()
        .gate_config
        .as_mut()
        .unwrap()
        .max_rejections = Some(max_rejections);
    seed_custom_workflow_task(&db, &task_id, "working", &workflow).await;
    let mut task_config = serde_json::json!({"review":{"ci_steps":"not-an-array"}});
    if let Some(review) = review_override {
        task_config["retry_budgets"] = serde_json::json!({ "review": review });
    }
    sqlx::query("UPDATE task SET task_state_config = ? WHERE id = ?")
        .bind(task_config.to_string())
        .bind(&task_id)
        .execute(db.pool())
        .await
        .unwrap();
    let mut outcomes = Vec::new();
    for _ in 0..(review_override.unwrap_or(max_rejections) + 1) {
        let task = TaskRepo::get_by_id(&*db, &task_id, false)
            .await
            .unwrap()
            .unwrap();
        if task.status != "working" {
            break;
        }
        let result = engine(Arc::clone(&db), Arc::clone(&event_bus))
            .workflow_execution()
            .transition(
                &task_id,
                "review",
                task.version,
                &workflow,
                &actor,
                "submit for validation",
                false,
                Default::default(),
            )
            .await
            .unwrap();
        let result = drain_result(engine(Arc::clone(&db), Arc::clone(&event_bus)), result).await;
        let blocked = result
            .task
            .entry_barrier_json
            .as_deref()
            .is_some_and(|b| b.contains("review retry budget exhausted"));
        // One failed entry, one Review charge: the bounce cascade shares it.
        assert_eq!(
            db::budget::spent(db.pool(), &task_id, db::budget::Kind::Review.key())
                .await
                .unwrap(),
            outcomes.len() as i64 + 1,
            "max_rejections={max_rejections}"
        );
        outcomes.push(if result.task.status == "working" {
            "bounce".to_owned()
        } else if blocked {
            "block".to_owned()
        } else {
            format!("other:{}", result.task.status)
        });
    }
    outcomes
}
