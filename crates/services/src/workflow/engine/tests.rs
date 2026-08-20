use std::{path::PathBuf, sync::Arc};

use api_types::{
    FailurePolicy, GateConfig, HookAudience, HookResultEntry, HookSpec, StateDefinition,
    StateHooks, StateKind, WorkflowDefinition, WorkflowTrigger, WorkflowTriggerDefinition,
};
use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, CreateProject, CreateRepo,
    CreateTask, CreateTaskRoleAssignment, ProjectRepo, RepoRepo, SqliteDb, TaskRepo,
    TaskRoleAssignmentRepo, TransitionLogRepo, UpdateProject,
};
use events::{EventBus, ForgeEvent};
use serde_json::json;
use tokio::sync::broadcast;

use super::WorkflowEngine;
use crate::{
    workflow::{default_roles, default_states, default_workflow},
    ServiceError,
};

async fn sqlite_db() -> SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    SqliteDb::new(pool)
}

async fn seed_project_repo_and_task(db: &SqliteDb, task_id: &str, status: &str) {
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
            remote_url: "https://example.com/forge.git".to_owned(),
            local_path: None,
            work_mode: db::WorkMode::DirectMerge,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo creates");
    ProjectRepo::update(
        db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("project primary repo updates");

    TaskRepo::create(
        db,
        CreateTask {
            id: task_id.to_owned(),
            project_id,
            repo_id: Some(repo_id),
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

async fn assign_agent_role_without_agent(db: &SqliteDb, task_id: &str, role: &str) {
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

async fn assign_user_role(db: &SqliteDb, task_id: &str, role: &str) {
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

fn engine(db: Arc<SqliteDb>, event_bus: Arc<EventBus>) -> WorkflowEngine {
    WorkflowEngine {
        db,
        event_bus,
        review_runner: None,
        merge_service: None,
        cleanup_scheduler: None,
        task_executor: None,
        daemon_connections: None,
        workspace_exec_locks: None,
        terminal_activity: None,
        workspace_root: PathBuf::new(),
        repo_cache_locks: None,
    }
}

fn hook(action: &str, on_failure: FailurePolicy) -> HookSpec {
    HookSpec {
        action: action.to_owned(),
        params: json!({}),
        applies_to: HookAudience::All,
        on_failure,
    }
}

fn state(name: &str, kind: StateKind, role: Option<&str>, hooks: StateHooks) -> StateDefinition {
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

fn with_trigger(mut state: StateDefinition, trigger: WorkflowTrigger, to: &str) -> StateDefinition {
    state.triggers.insert(
        trigger,
        WorkflowTriggerDefinition {
            to: to.to_owned(),
            dispatch: None,
        },
    );
    state
}

fn user_approval_review_workflow(
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

fn phases(results: &[HookResultEntry]) -> Vec<&str> {
    results.iter().map(|entry| entry.phase.as_str()).collect()
}

fn assert_phases_are_ordered(results: &[HookResultEntry]) {
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

async fn hook_results(db: &SqliteDb, task_id: &str) -> Vec<HookResultEntry> {
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

fn drain_events(rx: &mut broadcast::Receiver<ForgeEvent>) -> Vec<ForgeEvent> {
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

#[tokio::test]
async fn lifecycle_ordering() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-lifecycle-ordering";
    seed_project_repo_and_task(&db, task_id, default_states::TODO).await;
    let workflow = default_workflow::default_workflow();
    let current_task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let result = engine(Arc::clone(&db), event_bus)
        .transition(
            task_id,
            default_states::PLANNING,
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "start work",
            false,
        )
        .await
        .expect("transition succeeds");

    assert_eq!(result.task.status.to_string(), default_states::IN_PROGRESS);
    let results = hook_results(&db, task_id).await;
    assert_phases_are_ordered(&results);
    assert!(
        results
            .iter()
            .any(|entry| entry.action == "auto_cascade_on_unassigned_role"
                && entry.phase == "after_enter"),
        "default todo -> planning should cascade to in_progress when no planner is assigned"
    );
}

#[tokio::test]
async fn default_workflow_allows_user_to_leave_planning() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-planning-manual-exit";
    seed_project_repo_and_task(&db, task_id, default_states::PLANNING).await;
    let workflow = default_workflow::default_workflow();
    let current_task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let result = engine(Arc::clone(&db), event_bus)
        .transition(
            task_id,
            default_states::IN_PROGRESS,
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "start work after planning",
            false,
        )
        .await
        .expect("planning can be advanced by a user");

    assert_eq!(result.task.status.to_string(), default_states::IN_PROGRESS);
}

#[tokio::test]
async fn default_workflow_allows_user_to_start_work_from_todo() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-human-start-work";
    seed_project_repo_and_task(&db, task_id, default_states::TODO).await;
    let workflow = default_workflow::default_workflow();
    let current_task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let result = engine(Arc::clone(&db), event_bus)
        .transition(
            task_id,
            default_states::IN_PROGRESS,
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "start human work",
            false,
        )
        .await
        .expect("todo can be moved directly into active work by a user");

    assert_eq!(result.task.status.to_string(), default_states::IN_PROGRESS);
    let transition_logs = TransitionLogRepo::list_by_task(&*db, task_id)
        .await
        .expect("transition logs load");
    assert!(transition_logs.iter().any(|log| {
        log.from_state == default_states::TODO
            && log.to_state == default_states::IN_PROGRESS
            && log.triggered_by == "user:test"
    }));
    assert!(
        !transition_logs
            .iter()
            .any(|log| log.to_state == default_states::PLANNING),
        "dragging to the main In Progress column should not implicitly enter planning"
    );
}

#[tokio::test]
async fn default_workflow_skips_planning_when_no_planner_is_assigned() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-planning-no-planner";
    seed_project_repo_and_task(&db, task_id, default_states::TODO).await;
    let workflow = default_workflow::default_workflow();
    let current_task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let result = engine(Arc::clone(&db), event_bus)
        .transition(
            task_id,
            default_states::PLANNING,
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "enter planning",
            false,
        )
        .await
        .expect("planning gate is skipped");

    assert_eq!(result.task.status.to_string(), default_states::IN_PROGRESS);
    let transition_logs = TransitionLogRepo::list_by_task(&*db, task_id)
        .await
        .expect("transition logs load");
    assert!(transition_logs.iter().any(|log| {
        log.from_state == default_states::PLANNING
            && log.to_state == default_states::IN_PROGRESS
            && log.trigger_reason == "gate skipped: no planner role assigned"
            && !log.rejection
    }));
}

#[tokio::test]
async fn default_workflow_keeps_planning_when_planner_is_human() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-planning-human-planner";
    seed_project_repo_and_task(&db, task_id, default_states::TODO).await;
    assign_user_role(&db, task_id, default_roles::PLANNER).await;
    let workflow = default_workflow::default_workflow();
    let current_task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let result = engine(Arc::clone(&db), event_bus)
        .transition(
            task_id,
            default_states::PLANNING,
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "enter planning",
            false,
        )
        .await
        .expect("human planning gate is entered");

    assert_eq!(result.task.status.to_string(), default_states::PLANNING);
}

#[tokio::test]
async fn default_workflow_skips_system_review_when_no_checks_or_reviewer_are_assigned() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-review-no-checks";
    seed_project_repo_and_task(&db, task_id, default_states::IN_PROGRESS).await;
    let workflow = default_workflow::default_workflow();
    let current_task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let result = engine(Arc::clone(&db), event_bus)
        .transition(
            task_id,
            default_states::REVIEW,
            current_task.version,
            &workflow,
            &api_types::Actor::system(api_types::SystemComponent::General),
            "request review after agent completion",
            false,
        )
        .await
        .expect("system-entered unconfigured review skips to merge gate");

    assert_eq!(result.task.status.to_string(), default_states::MERGING);
    let transition_logs = TransitionLogRepo::list_by_task(&*db, task_id)
        .await
        .expect("transition logs load");
    assert!(transition_logs.iter().any(|log| {
        log.from_state == default_states::REVIEW
            && log.to_state == default_states::MERGING
            && log.trigger_reason == "review skipped: no checks or reviewer assigned"
    }));
}

#[tokio::test]
async fn default_workflow_keeps_user_requested_review_when_unassigned() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-review-human-requested";
    seed_project_repo_and_task(&db, task_id, default_states::IN_PROGRESS).await;
    let workflow = default_workflow::default_workflow();
    let current_task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let result = engine(Arc::clone(&db), event_bus)
        .transition(
            task_id,
            default_states::REVIEW,
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "request human review",
            false,
        )
        .await
        .expect("user-requested review gate is entered");

    assert_eq!(result.task.status.to_string(), default_states::REVIEW);
    let stored = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let metadata = stored.metadata().expect("metadata parses");
    assert_eq!(metadata.extra.get("awaiting_human"), Some(&json!(true)));
    assert_eq!(
        metadata.extra.get("awaiting_human_reason"),
        Some(&json!("manual_review"))
    );
    let transition_logs = TransitionLogRepo::list_by_task(&*db, task_id)
        .await
        .expect("transition logs load");
    assert!(
        !transition_logs
            .iter()
            .any(|log| log.from_state == default_states::REVIEW
                && log.to_state == default_states::MERGING),
        "user-requested review should not immediately cascade into merge"
    );
}

#[tokio::test]
async fn default_workflow_keeps_review_when_reviewer_is_human() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-review-human-reviewer";
    seed_project_repo_and_task(&db, task_id, default_states::IN_PROGRESS).await;
    assign_user_role(&db, task_id, default_roles::REVIEWER).await;
    let workflow = default_workflow::default_workflow();
    let current_task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let result = engine(Arc::clone(&db), event_bus)
        .transition(
            task_id,
            default_states::REVIEW,
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "request review",
            false,
        )
        .await
        .expect("human review gate is entered");

    assert_eq!(result.task.status.to_string(), default_states::REVIEW);
}

#[tokio::test]
async fn guard_rejection_returns_412_error() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let mut rx = event_bus.subscribe();
    let task_id = "task-guard-rejection";
    seed_project_repo_and_task(&db, task_id, default_states::TODO).await;
    assign_agent_role_without_agent(&db, task_id, default_roles::PLANNER).await;

    let workflow = WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            with_trigger(
                state(
                    default_states::TODO,
                    StateKind::Initial,
                    None,
                    StateHooks {
                        before_exit: vec![hook(
                            "require_upstream_roles_completed",
                            FailurePolicy::Block,
                        )],
                        ..StateHooks::default()
                    },
                ),
                WorkflowTrigger::Accept,
                default_states::IN_PROGRESS,
            ),
            with_trigger(
                state(
                    default_states::PLANNING,
                    StateKind::Gate,
                    Some(default_roles::PLANNER),
                    StateHooks::default(),
                ),
                WorkflowTrigger::Accept,
                default_states::IN_PROGRESS,
            ),
            state(
                default_states::IN_PROGRESS,
                StateKind::Active,
                None,
                StateHooks::default(),
            ),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    };
    let current_task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let result = engine(Arc::clone(&db), Arc::clone(&event_bus))
        .transition(
            task_id,
            default_states::IN_PROGRESS,
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "start work",
            false,
        )
        .await;

    match result {
        Err(ServiceError::GuardRejection { guard, reason }) => {
            assert_eq!(guard, "require_upstream_roles_completed");
            assert!(reason.contains(default_roles::PLANNER));
        }
        Err(error) => panic!("expected guard rejection, got {error:?}"),
        Ok(_) => panic!("expected guard rejection, got successful transition"),
    }

    let task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(task.status.to_string(), default_states::TODO);

    let events = drain_events(&mut rx);
    assert!(
        events
            .iter()
            .all(|event| event.event_type != "task.status_changed"),
        "guard rejection must not publish task.status_changed: {events:?}"
    );
    assert!(
        events
            .iter()
            .any(|event| event.event_type == "transition.guard_rejected"),
        "guard rejection event should be published"
    );
}

#[tokio::test]
async fn effect_failure_log_policy_continues() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-effect-failure-log";
    seed_project_repo_and_task(&db, task_id, default_states::TODO).await;
    assign_agent_role_without_agent(&db, task_id, default_roles::CODER).await;

    let workflow = WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            with_trigger(
                state(
                    default_states::TODO,
                    StateKind::Initial,
                    None,
                    StateHooks::default(),
                ),
                WorkflowTrigger::Accept,
                default_states::IN_PROGRESS,
            ),
            state(
                default_states::IN_PROGRESS,
                StateKind::Active,
                Some(default_roles::CODER),
                StateHooks {
                    // A non-dispatch effect: Log-policy failures never move
                    // the task. (A failed *dispatch* entering an active state
                    // rolls the task back — covered separately below.)
                    on_enter: vec![hook("notify_role_holder", FailurePolicy::Log)],
                    ..StateHooks::default()
                },
            ),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    };
    let current_task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let result = engine(Arc::clone(&db), event_bus)
        .transition(
            task_id,
            default_states::IN_PROGRESS,
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "start work",
            false,
        )
        .await
        .expect("transition succeeds despite logged effect failure");

    assert_eq!(result.task.status.to_string(), default_states::IN_PROGRESS);
    let results = hook_results(&db, task_id).await;
    assert!(results.iter().any(|entry| {
        entry.action == "notify_role_holder"
            && entry.phase == "on_enter"
            && entry.outcome == "failed"
    }));
}

#[tokio::test]
async fn dispatch_failure_entering_active_state_rolls_task_back_to_initial() {
    // Incident repro: a task without an approved execution baseline was
    // scheduled by the dispatcher, planning was skipped (no planner), and the
    // coder dispatch failed on entering in_progress — yet the task stayed in
    // in_progress with zero executions, looking in-flight forever.
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-dispatch-failure-rollback";
    seed_project_repo_and_task(&db, task_id, default_states::TODO).await;
    // A coder assignment whose agent is gone makes dispatch_role_agent FAIL
    // (not skip) when the task enters in_progress.
    assign_agent_role_without_agent(&db, task_id, default_roles::CODER).await;
    let workflow = default_workflow::default_workflow();
    let current_task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let result = engine(Arc::clone(&db), event_bus)
        .transition(
            task_id,
            default_states::PLANNING,
            current_task.version,
            &workflow,
            &api_types::Actor::system(api_types::SystemComponent::TaskDispatcher),
            "scheduled by task dispatcher",
            false,
        )
        .await
        .expect("transition succeeds");

    assert!(result.cascaded);
    assert_eq!(
        result.task.status.to_string(),
        default_states::TODO,
        "a failed dispatch must not leave the task in the working state"
    );

    let task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(task.status.to_string(), default_states::TODO);
    let annotation = task
        .error_annotation
        .expect("dispatch failure is annotated on the task");
    let annotation: serde_json::Value =
        serde_json::from_str(&annotation).expect("annotation is json");
    assert_eq!(annotation["type"], "dispatch_failed");
    assert!(
        annotation["message"]
            .as_str()
            .expect("annotation message is a string")
            .contains("invalid coder role assignment"),
        "annotation carries the dispatch error: {annotation}"
    );

    let logs = TransitionLogRepo::list_by_task(&*db, task_id)
        .await
        .expect("transition logs list");
    let enter = logs
        .iter()
        .find(|entry| entry.to_state == default_states::IN_PROGRESS)
        .expect("planning -> in_progress transition is logged");
    let entries: Vec<HookResultEntry> = serde_json::from_str(
        enter
            .hook_results_json
            .as_deref()
            .expect("hook results are written"),
    )
    .expect("hook results deserialize");
    assert!(
        entries.iter().any(|entry| {
            entry.action == "dispatch_role_agent"
                && entry.phase == "on_enter"
                && entry.outcome == "failed"
        }),
        "failed dispatch hook is recorded: {entries:?}"
    );
    let rollback = logs
        .iter()
        .find(|entry| {
            entry.from_state == default_states::IN_PROGRESS
                && entry.to_state == default_states::TODO
        })
        .expect("in_progress -> todo rollback transition is logged");
    assert!(
        rollback
            .trigger_reason
            .starts_with("dispatch failed entering in_progress"),
        "rollback records the dispatch failure: {}",
        rollback.trigger_reason
    );
    assert_eq!(
        rollback.triggered_by,
        api_types::Actor::system(api_types::SystemComponent::Workflow).display()
    );
}

#[tokio::test]
async fn successful_dispatch_clears_stale_dispatch_failure_annotation() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-dispatch-annotation-clears";
    seed_project_repo_and_task(&db, task_id, default_states::TODO).await;
    // A user-held coder role makes dispatch_role_agent return Ok
    // (task.awaiting_human) without needing a task executor.
    assign_user_role(&db, task_id, default_roles::CODER).await;
    sqlx::query("UPDATE task SET error_annotation = ? WHERE id = ?")
        .bind(
            json!({
                "type": "dispatch_failed",
                "message": "repository Task is not runnable",
            })
            .to_string(),
        )
        .bind(task_id)
        .execute(db.pool())
        .await
        .expect("stale dispatch failure annotation seeds");
    let workflow = default_workflow::default_workflow();
    let current_task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let result = engine(Arc::clone(&db), event_bus)
        .transition(
            task_id,
            default_states::IN_PROGRESS,
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "restart work after baseline approval",
            false,
        )
        .await
        .expect("transition succeeds");

    assert_eq!(result.task.status.to_string(), default_states::IN_PROGRESS);
    let task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(task.status.to_string(), default_states::IN_PROGRESS);
    assert_eq!(
        task.error_annotation, None,
        "successful dispatch clears the stale dispatch_failed annotation"
    );
}

#[tokio::test]
#[ignore = "Blocked: engine tests cannot register a configurable test-only Cascade action without changing the production registry; built-in cascade actions target fixed default states, so they cannot drive s1 -> s2 -> s3 -> s4."]
async fn cascade_depth_limiting() {
    let workflow = WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            with_trigger(
                state("s0", StateKind::Initial, None, StateHooks::default()),
                WorkflowTrigger::Accept,
                "s1",
            ),
            with_trigger(
                state(
                    "s1",
                    StateKind::Active,
                    None,
                    StateHooks {
                        after_enter: vec![hook("test_cascade_to_s2", FailurePolicy::Log)],
                        ..StateHooks::default()
                    },
                ),
                WorkflowTrigger::Accept,
                "s2",
            ),
            with_trigger(
                state(
                    "s2",
                    StateKind::Active,
                    None,
                    StateHooks {
                        after_enter: vec![hook("test_cascade_to_s3", FailurePolicy::Log)],
                        ..StateHooks::default()
                    },
                ),
                WorkflowTrigger::Accept,
                "s3",
            ),
            with_trigger(
                state(
                    "s3",
                    StateKind::Active,
                    None,
                    StateHooks {
                        after_enter: vec![hook("test_cascade_to_s4", FailurePolicy::Log)],
                        ..StateHooks::default()
                    },
                ),
                WorkflowTrigger::Accept,
                "s4",
            ),
            state("s4", StateKind::Active, None, StateHooks::default()),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    };

    assert_eq!(workflow.states.len(), 5);
}

#[test]
fn validate_claimable_backlog_rejection() {
    let workflow = default_workflow::default_workflow();

    let result = WorkflowEngine::validate_claimable(&workflow, default_states::BACKLOG);
    match result {
        Err(ServiceError::InvalidOperation { message }) => {
            assert!(
                message.contains(default_states::BACKLOG) || message.contains("not claimable"),
                "unexpected validation message: {message}"
            );
        }
        other => panic!("expected invalid operation, got {other:?}"),
    }

    WorkflowEngine::validate_claimable(&workflow, default_states::TODO)
        .expect("todo should be claimable");
}

#[tokio::test]
async fn cancellation_implicit_edge() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-cancellation-implicit-edge";
    seed_project_repo_and_task(&db, task_id, default_states::IN_PROGRESS).await;

    let workflow = WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            state(
                default_states::IN_PROGRESS,
                StateKind::Active,
                None,
                StateHooks {
                    before_exit: vec![hook("dispatch_fix_agent", FailurePolicy::Block)],
                    on_exit: vec![hook("dispatch_fix_agent", FailurePolicy::Log)],
                    ..StateHooks::default()
                },
            ),
            state(
                default_states::CANCELLED,
                StateKind::Terminal,
                None,
                StateHooks::default(),
            ),
        ],
        configuration: Vec::new(),
        cancellation_state: Some(default_states::CANCELLED.to_owned()),
    };
    let current_task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let result = engine(Arc::clone(&db), event_bus)
        .transition(
            task_id,
            default_states::CANCELLED,
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "cancel",
            false,
        )
        .await
        .expect("implicit cancellation transition succeeds");

    assert_eq!(result.task.status.to_string(), default_states::CANCELLED);
    let results = hook_results(&db, task_id).await;
    assert!(
        results
            .iter()
            .filter(|entry| entry.phase == "before_exit")
            .all(|entry| entry.outcome == "skipped"),
        "implicit cancellation should skip before_exit hooks: {results:?}"
    );
    assert!(
        results
            .iter()
            .any(|entry| entry.action == "dispatch_fix_agent" && entry.phase == "on_exit"),
        "implicit cancellation should still run on_exit hooks"
    );
}

async fn seed_custom_workflow_task(
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
            remote_url: "https://example.com/repo.git".to_owned(),
            local_path: None,
            work_mode: db::WorkMode::DirectMerge,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo creates");
    ProjectRepo::update(
        db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("project primary repo updates");
    TaskRepo::create(
        db,
        CreateTask {
            id: task_id.to_owned(),
            project_id: project_id.clone(),
            repo_id: Some(repo_id),
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

#[tokio::test]
async fn custom_workflow_renamed_states_lifecycle() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow = WorkflowDefinition {
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
                "checking",
            ),
            with_trigger(
                state("checking", StateKind::Gate, None, StateHooks::default()),
                WorkflowTrigger::Accept,
                "shipped",
            ),
            state("shipped", StateKind::Terminal, None, StateHooks::default()),
            state(
                "abandoned",
                StateKind::Terminal,
                None,
                StateHooks::default(),
            ),
        ],
        configuration: Vec::new(),
        cancellation_state: Some("abandoned".to_owned()),
    };
    seed_custom_workflow_task(&db, &task_id, "pending", &workflow).await;
    let eng = engine(Arc::clone(&db), Arc::clone(&event_bus));

    let r1 = eng
        .transition(
            &task_id,
            "working",
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "claim",
            false,
        )
        .await
        .expect("pending → working");
    assert_eq!(r1.task.status, "working");

    let r2 = eng
        .transition(
            &task_id,
            "checking",
            r1.task.version,
            &workflow,
            &api_types::Actor::system(api_types::SystemComponent::General),
            "completed",
            false,
        )
        .await
        .expect("working → checking");
    assert_eq!(r2.task.status, "checking");

    let r3 = eng
        .transition(
            &task_id,
            "shipped",
            r2.task.version,
            &workflow,
            &api_types::Actor::system(api_types::SystemComponent::General),
            "approved",
            false,
        )
        .await
        .expect("checking → shipped");
    assert_eq!(r3.task.status, "shipped");
}

#[tokio::test]
async fn implicit_accept_transition_moves_to_next_declared_state() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow = WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            state("pending", StateKind::Initial, None, StateHooks::default()),
            state("working", StateKind::Active, None, StateHooks::default()),
            state("shipped", StateKind::Terminal, None, StateHooks::default()),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    };
    seed_custom_workflow_task(&db, &task_id, "pending", &workflow).await;

    let result = engine(Arc::clone(&db), event_bus)
        .transition(
            &task_id,
            "working",
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "accept",
            false,
        )
        .await
        .expect("pending implicitly accepts to working");

    assert_eq!(result.task.status, "working");
}

#[tokio::test]
async fn gate_approve_reject_on_custom_gate_states() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let mut start_state = state("start", StateKind::Initial, None, StateHooks::default());
    start_state.triggers.insert(
        WorkflowTrigger::Accept,
        WorkflowTriggerDefinition {
            to: "coding".to_owned(),
            dispatch: None,
        },
    );
    let mut coding_state = state("coding", StateKind::Active, None, StateHooks::default());
    coding_state.triggers.insert(
        WorkflowTrigger::Accept,
        WorkflowTriggerDefinition {
            to: "qa".to_owned(),
            dispatch: None,
        },
    );
    let mut qa_state = state("qa", StateKind::Gate, None, StateHooks::default());
    qa_state.gate_config = Some(api_types::GateConfig {
        reject_target: Some("coding".to_owned()),
        max_rejections: Some(2),
        approve_label: None,
        reject_label: None,
        requires_user_approval: Some(false),
        optional_when_unassigned: Some(false),
    });
    qa_state.triggers.insert(
        WorkflowTrigger::Accept,
        WorkflowTriggerDefinition {
            to: "released".to_owned(),
            dispatch: None,
        },
    );
    qa_state.triggers.insert(
        WorkflowTrigger::Reject,
        WorkflowTriggerDefinition {
            to: "coding".to_owned(),
            dispatch: None,
        },
    );
    let workflow = WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            start_state,
            coding_state,
            qa_state,
            state("released", StateKind::Terminal, None, StateHooks::default()),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    };
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();
    let task_a_id = new_uuid_v4();
    let task_b_id = new_uuid_v4();
    ProjectRepo::create(
        &*db,
        CreateProject {
            id: project_id.clone(),
            name: "Custom".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: serde_json::to_string(&workflow).unwrap(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");
    RepoRepo::create(
        &*db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "repo".to_owned(),
            remote_url: "https://example.com/repo.git".to_owned(),
            local_path: None,
            work_mode: db::WorkMode::DirectMerge,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo creates");
    ProjectRepo::update(
        &*db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("project primary repo updates");
    for (task_id, title) in [
        (task_a_id.as_str(), "custom gate approve"),
        (task_b_id.as_str(), "custom gate reject"),
    ] {
        TaskRepo::create(
            &*db,
            CreateTask {
                id: task_id.to_owned(),
                project_id: project_id.clone(),
                repo_id: Some(repo_id.clone()),
                parent_task_id: None,
                subtask_order: None,
                assignee_type: None,
                assignee_id: None,
                title: title.to_owned(),
                description: None,
                task_type: "task".to_owned(),
                status: "start".to_owned(),
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
    }
    let eng = engine(Arc::clone(&db), Arc::clone(&event_bus));

    let current_task = TaskRepo::get_by_id(&*db, &task_a_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let result = eng
        .transition(
            &task_a_id,
            "coding",
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "start work",
            false,
        )
        .await
        .expect("start → coding");
    assert_eq!(result.task.status, "coding");

    let current_task = TaskRepo::get_by_id(&*db, &task_a_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let result = eng
        .transition(
            &task_a_id,
            "qa",
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "request qa",
            false,
        )
        .await
        .expect("coding → qa");
    assert_eq!(result.task.status, "qa");

    let current_task = TaskRepo::get_by_id(&*db, &task_a_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let result = eng
        .transition(
            &task_a_id,
            "released",
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "gate approved",
            false,
        )
        .await
        .expect("qa → released");
    assert_eq!(result.task.status, "released");

    let current_task = TaskRepo::get_by_id(&*db, &task_b_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let result = eng
        .transition(
            &task_b_id,
            "coding",
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "start work",
            false,
        )
        .await
        .expect("start → coding");
    assert_eq!(result.task.status, "coding");

    let current_task = TaskRepo::get_by_id(&*db, &task_b_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let result = eng
        .transition(
            &task_b_id,
            "qa",
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "request qa",
            false,
        )
        .await
        .expect("coding → qa");
    assert_eq!(result.task.status, "qa");

    let current_task = TaskRepo::get_by_id(&*db, &task_b_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let result = eng
        .transition(
            &task_b_id,
            "coding",
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "gate rejected",
            true,
        )
        .await
        .expect("qa → coding (reject)");
    assert_eq!(result.task.status, "coding");
}

#[tokio::test]
async fn user_approval_gate_failed_blocking_before_enter_cascades_to_reject_target() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow =
        user_approval_review_workflow(hook("run_ci_steps", FailurePolicy::Block), Vec::new());
    seed_custom_workflow_task(&db, &task_id, "working", &workflow).await;
    sqlx::query("UPDATE task SET task_state_config = ? WHERE id = ?")
        .bind(r#"{"review":{"ci_steps":"not-an-array"}}"#)
        .bind(&task_id)
        .execute(db.pool())
        .await
        .expect("task review config updates");

    let result = engine(Arc::clone(&db), event_bus)
        .transition(
            &task_id,
            "review",
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "submit for validation",
            false,
        )
        .await
        .expect("failed validation cascades back to working");

    assert_eq!(result.task.status, "working");
    assert!(result.task.entry_barrier_json.is_none());
    let logs = TransitionLogRepo::list_by_task(&*db, &task_id)
        .await
        .expect("transition logs load");
    assert!(
        logs.iter().any(|entry| {
            entry.from_state == "review" && entry.to_state == "working" && entry.rejection
        }),
        "validation rejection should be recorded on the automatic reject cascade"
    );
}

#[tokio::test]
async fn user_approval_gate_passing_hooks_pauses_forward_cascade_for_human() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow = user_approval_review_workflow(
        hook("check_retry_budget", FailurePolicy::Block),
        vec![hook("auto_cascade_on_completion", FailurePolicy::Log)],
    );
    seed_custom_workflow_task(&db, &task_id, "working", &workflow).await;

    let result = engine(Arc::clone(&db), event_bus)
        .transition(
            &task_id,
            "review",
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "submit for validation",
            false,
        )
        .await
        .expect("passing validation pauses for human approval");

    assert_eq!(result.task.status, "review");
    assert!(result.task.entry_barrier_json.is_none());
    assert!(!result.cascaded);
    let logs = TransitionLogRepo::list_by_task(&*db, &task_id)
        .await
        .expect("transition logs load");
    assert!(!logs.iter().any(|entry| entry.rejection));
}

#[tokio::test]
async fn review_gate_exhausted_budget_defers_blocking_until_review_failure() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let mut review = state("review", StateKind::Gate, None, StateHooks::default());
    review.gate_config = Some(api_types::GateConfig {
        reject_target: Some("coding".to_owned()),
        max_rejections: Some(1),
        approve_label: None,
        reject_label: None,
        requires_user_approval: Some(false),
        optional_when_unassigned: Some(false),
    });
    review.triggers.insert(
        WorkflowTrigger::Accept,
        WorkflowTriggerDefinition {
            to: "shipped".to_owned(),
            dispatch: None,
        },
    );
    review.triggers.insert(
        WorkflowTrigger::Reject,
        WorkflowTriggerDefinition {
            to: "coding".to_owned(),
            dispatch: None,
        },
    );
    review.triggers.insert(
        WorkflowTrigger::Fail,
        WorkflowTriggerDefinition {
            to: "needs_help".to_owned(),
            dispatch: None,
        },
    );
    let workflow = WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            with_trigger(
                state("start", StateKind::Initial, None, StateHooks::default()),
                WorkflowTrigger::Accept,
                "coding",
            ),
            with_trigger(
                state("coding", StateKind::Active, None, StateHooks::default()),
                WorkflowTrigger::Accept,
                "review",
            ),
            review,
            state("needs_help", StateKind::Custom, None, StateHooks::default()),
            state("shipped", StateKind::Terminal, None, StateHooks::default()),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    };
    seed_custom_workflow_task(&db, &task_id, "start", &workflow).await;
    let eng = engine(Arc::clone(&db), Arc::clone(&event_bus));

    let current_task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let result = eng
        .transition(
            &task_id,
            "coding",
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "start work",
            false,
        )
        .await
        .expect("start → coding");
    assert_eq!(result.task.status, "coding");

    let current_task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let result = eng
        .transition(
            &task_id,
            "review",
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "request review",
            false,
        )
        .await
        .expect("coding → review");
    assert_eq!(result.task.status, "review");

    let transition_logs = TransitionLogRepo::list_by_task(&*db, &task_id)
        .await
        .expect("transition logs load");
    let payload = transition_logs
        .last()
        .and_then(|entry| entry.hook_results_json.as_deref())
        .expect("hook results are written");
    let results: Vec<HookResultEntry> =
        serde_json::from_str(payload).expect("hook results deserialize");
    assert!(results
        .iter()
        .any(|entry| { entry.action == "check_retry_budget" && entry.phase == "after_enter" }));

    let current_task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let result = eng
        .transition(
            &task_id,
            "coding",
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "gate rejected",
            true,
        )
        .await
        .expect("review → coding (reject)");
    assert_eq!(result.task.status, "coding");

    let current_task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    eng.transition(
        &task_id,
        "review",
        current_task.version,
        &workflow,
        &api_types::Actor::user(api_types::UserActionSource::Test),
        "request review again",
        false,
    )
    .await
    .expect("review budget resolves on re-entry");

    let current_task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(current_task.status, "review");
    assert!(
        current_task.blocked_json.is_none(),
        "review entry defers retry exhaustion enforcement until a review failure is recorded"
    );

    let transition_logs = TransitionLogRepo::list_by_task(&*db, &task_id)
        .await
        .expect("transition logs load");
    assert!(!transition_logs
        .iter()
        .any(|entry| { entry.from_state == "review" && entry.to_state == "needs_help" }));
}

#[tokio::test]
async fn planning_gate_reject_back_to_itself() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow = default_workflow::default_workflow();
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();

    ProjectRepo::create(
        &*db,
        CreateProject {
            id: project_id.clone(),
            name: "Default".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: serde_json::to_string(&workflow).unwrap(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");
    RepoRepo::create(
        &*db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "repo".to_owned(),
            remote_url: "https://example.com/repo.git".to_owned(),
            local_path: None,
            work_mode: db::WorkMode::DirectMerge,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo creates");
    ProjectRepo::update(
        &*db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("project primary repo updates");
    TaskRepo::create(
        &*db,
        CreateTask {
            id: task_id.clone(),
            project_id: project_id.clone(),
            repo_id: Some(repo_id),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "planning reject".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: default_states::TODO.to_owned(),
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
    assign_user_role(&db, &task_id, default_roles::PLANNER).await;
    let eng = engine(Arc::clone(&db), Arc::clone(&event_bus));

    let current_task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let result = eng
        .transition(
            &task_id,
            default_states::PLANNING,
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "enter planning",
            false,
        )
        .await
        .expect("todo → planning");
    assert_eq!(result.task.status, default_states::PLANNING);

    let current_task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let result = eng
        .transition(
            &task_id,
            default_states::PLANNING,
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "gate rejected",
            true,
        )
        .await
        .expect("planning rejects back to itself");
    assert_eq!(result.task.status, default_states::PLANNING);
}

#[tokio::test]
async fn gate_reject_back_to_itself() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let mut planning = state("planning", StateKind::Gate, None, StateHooks::default());
    planning.gate_config = Some(api_types::GateConfig {
        reject_target: Some("planning".to_owned()),
        max_rejections: Some(3),
        approve_label: None,
        reject_label: None,
        requires_user_approval: Some(false),
        optional_when_unassigned: Some(false),
    });
    planning.triggers.insert(
        WorkflowTrigger::Reject,
        WorkflowTriggerDefinition {
            to: "planning".to_owned(),
            dispatch: None,
        },
    );
    planning.triggers.insert(
        WorkflowTrigger::Accept,
        WorkflowTriggerDefinition {
            to: "working".to_owned(),
            dispatch: None,
        },
    );
    let workflow = WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            with_trigger(
                state("todo", StateKind::Initial, None, StateHooks::default()),
                WorkflowTrigger::Accept,
                "planning",
            ),
            planning,
            with_trigger(
                state("working", StateKind::Active, None, StateHooks::default()),
                WorkflowTrigger::Accept,
                "done",
            ),
            state("done", StateKind::Terminal, None, StateHooks::default()),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    };
    seed_custom_workflow_task(&db, &task_id, "planning", &workflow).await;
    let eng = engine(Arc::clone(&db), Arc::clone(&event_bus));

    let reject_target = workflow
        .gate_reject_target("planning")
        .expect("planning has reject_target");
    assert_eq!(reject_target, "planning");

    let result = eng
        .transition(
            &task_id,
            reject_target,
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "gate rejected: needs more detail",
            true,
        )
        .await
        .expect("planning → planning (reject)");
    assert_eq!(result.task.status, "planning");

    let logs = TransitionLogRepo::list_by_task(&*db, &task_id)
        .await
        .expect("transition log loads");
    assert!(
        logs.iter().any(|entry| entry.from_state == "planning"
            && entry.to_state == "planning"
            && entry.rejection),
        "transition log should record the rejection"
    );
}

fn missing_edge_workflow() -> WorkflowDefinition {
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

fn system_only_fail_edge_workflow() -> WorkflowDefinition {
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

fn workflow_without_review_state() -> WorkflowDefinition {
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

async fn seed_parent_and_subtask(
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
            repo_id: parent.repo_id.clone(),
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

#[tokio::test]
async fn user_override_succeeds_across_missing_edge() {
    // Delta: User moves a task across a missing edge
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow = missing_edge_workflow();
    seed_custom_workflow_task(&db, &task_id, "working", &workflow).await;
    let eng = engine(Arc::clone(&db), event_bus);

    let result = eng
        .transition(
            &task_id,
            "done",
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "override",
            false,
        )
        .await
        .expect("user override across missing edge succeeds");

    assert_eq!(result.task.status, "done");
}

#[tokio::test]
async fn user_override_does_not_reopen_terminal_state() {
    // Terminal reopen must not use user routing override (missing edge from terminal -> non-terminal).
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow = missing_edge_workflow();
    seed_custom_workflow_task(&db, &task_id, "done", &workflow).await;
    let eng = engine(Arc::clone(&db), event_bus);

    let result = eng
        .transition(
            &task_id,
            "working",
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "override",
            false,
        )
        .await;

    assert!(
        matches!(
            result,
            Err(ServiceError::Db(db::DbError::InvalidTransition))
        ),
        "user override must not reopen a terminal state"
    );
}

#[tokio::test]
async fn user_override_succeeds_along_system_only_edge() {
    // Delta: User moves a task along a system-only edge
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow = system_only_fail_edge_workflow();
    seed_custom_workflow_task(&db, &task_id, "working", &workflow).await;
    let eng = engine(Arc::clone(&db), Arc::clone(&event_bus));

    let user_result = eng
        .transition(
            &task_id,
            "failed",
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "user override along fail edge",
            false,
        )
        .await
        .expect("user override along system-only edge succeeds");
    assert_eq!(user_result.task.status, "failed");

    let task_id_system = new_uuid_v4();
    seed_custom_workflow_task(&db, &task_id_system, "working", &workflow).await;
    let system_result = eng
        .transition(
            &task_id_system,
            "failed",
            1,
            &workflow,
            &api_types::Actor::system(api_types::SystemComponent::General),
            "system fail transition",
            false,
        )
        .await
        .expect("system actor may use system-only edge");
    assert_eq!(system_result.task.status, "failed");

    let task_id_agent = new_uuid_v4();
    seed_custom_workflow_task(&db, &task_id_agent, "working", &workflow).await;
    let agent_result = eng
        .transition(
            &task_id_agent,
            "failed",
            1,
            &workflow,
            &api_types::Actor::agent("unit"),
            "agent attempt",
            false,
        )
        .await;
    match agent_result {
        Err(ServiceError::InvalidOperation { message }) => {
            assert!(
                message.contains("system-only"),
                "expected system-only rejection, got: {message}"
            );
        }
        Ok(_) => panic!("expected system-only rejection for agent, got Ok"),
        Err(other) => panic!("expected system-only rejection for agent, got {other:?}"),
    }
}

#[tokio::test]
async fn override_not_granted_to_agents_or_system() {
    // Delta: Override authority is not granted to agents or system
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workflow = missing_edge_workflow();
    let eng = engine(Arc::clone(&db), event_bus);

    for (actor, label) in [
        (
            api_types::Actor::system(api_types::SystemComponent::General),
            "system",
        ),
        (api_types::Actor::agent("unit"), "agent"),
    ] {
        let task_id = new_uuid_v4();
        seed_custom_workflow_task(&db, &task_id, "working", &workflow).await;
        let result = eng
            .transition(
                &task_id,
                "done",
                1,
                &workflow,
                &actor,
                "should not override",
                false,
            )
            .await;
        assert!(
            matches!(
                result,
                Err(ServiceError::Db(db::DbError::InvalidTransition))
            ),
            "{label} actor should be rejected on missing edge without override"
        );
    }
}

#[tokio::test]
async fn override_to_undefined_target_rejected_with_enumerated_states() {
    // Delta: Target state not in workflow
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow = missing_edge_workflow();
    seed_custom_workflow_task(&db, &task_id, "working", &workflow).await;
    let eng = engine(Arc::clone(&db), event_bus);

    let result = eng
        .transition(
            &task_id,
            "nonexistent",
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "invalid target",
            false,
        )
        .await;

    match result {
        Err(ServiceError::InvalidOperation { message }) => {
            assert!(
                message.contains("not defined in workflow"),
                "expected undefined-state message, got: {message}"
            );
            assert!(
                message.contains("working"),
                "expected defined state 'working' in message, got: {message}"
            );
            assert!(
                message.contains("done"),
                "expected defined state 'done' in message, got: {message}"
            );
        }
        Ok(_) => panic!("expected undefined target rejection, got Ok"),
        Err(other) => panic!("expected undefined target rejection, got {other:?}"),
    }
}

#[tokio::test]
async fn custom_workflow_lacking_review_rejects_with_enumerated_states() {
    // Delta: Custom project workflow lacks the requested target
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow = workflow_without_review_state();
    seed_custom_workflow_task(&db, &task_id, "working", &workflow).await;
    let eng = engine(Arc::clone(&db), event_bus);

    let result = eng
        .transition(
            &task_id,
            "review",
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "request review",
            false,
        )
        .await;

    match result {
        Err(ServiceError::InvalidOperation { message }) => {
            assert!(
                message.contains("not defined in workflow"),
                "expected undefined-state message, got: {message}"
            );
            for state_name in ["pending", "working", "shipped"] {
                assert!(
                    message.contains(state_name),
                    "expected defined state '{state_name}' in message, got: {message}"
                );
            }
            assert!(
                !message
                    .split("defined states are: ")
                    .nth(1)
                    .unwrap_or("")
                    .contains("review"),
                "review must not appear among defined states, got: {message}"
            );
        }
        Ok(_) => panic!("expected review target rejection, got Ok"),
        Err(other) => panic!("expected review target rejection, got {other:?}"),
    }
}

#[tokio::test]
async fn content_guard_blocks_user_override() {
    // Delta: A content guard may still block a user override
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow = WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            with_trigger(
                state(
                    "working",
                    StateKind::Active,
                    None,
                    StateHooks {
                        before_exit: vec![hook(
                            "require_upstream_roles_completed",
                            FailurePolicy::Block,
                        )],
                        ..StateHooks::default()
                    },
                ),
                WorkflowTrigger::Accept,
                "paused",
            ),
            state("paused", StateKind::Custom, None, StateHooks::default()),
            with_trigger(
                state(
                    default_states::PLANNING,
                    StateKind::Gate,
                    Some(default_roles::PLANNER),
                    StateHooks::default(),
                ),
                WorkflowTrigger::Accept,
                "done",
            ),
            state("done", StateKind::Terminal, None, StateHooks::default()),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    };
    seed_custom_workflow_task(&db, &task_id, "working", &workflow).await;
    assign_agent_role_without_agent(&db, &task_id, default_roles::PLANNER).await;
    let eng = engine(Arc::clone(&db), event_bus);

    let result = eng
        .transition(
            &task_id,
            "done",
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "override blocked by guard",
            false,
        )
        .await;

    match result {
        Err(ServiceError::GuardRejection { guard, reason: _ }) => {
            assert_eq!(guard, "require_upstream_roles_completed");
        }
        Ok(_) => panic!("expected guard rejection, got Ok"),
        Err(other) => panic!("expected guard rejection, got {other:?}"),
    }

    let task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(task.status, "working");
}

#[tokio::test]
async fn version_conflict_still_applies_to_override() {
    // Delta: Version conflict still applies to override
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow = missing_edge_workflow();
    seed_custom_workflow_task(&db, &task_id, "working", &workflow).await;
    let eng = engine(Arc::clone(&db), event_bus);

    let result = eng
        .transition(
            &task_id,
            "done",
            2,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "stale version",
            false,
        )
        .await;

    assert!(
        matches!(result, Err(ServiceError::Db(db::DbError::VersionConflict))),
        "expected version conflict"
    );
}

#[tokio::test]
async fn override_is_auditable() {
    // Delta: Override is auditable
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow = missing_edge_workflow();
    seed_custom_workflow_task(&db, &task_id, "working", &workflow).await;
    let reason = "audit this override";
    let eng = engine(Arc::clone(&db), event_bus);

    eng.transition(
        &task_id,
        "done",
        1,
        &workflow,
        &api_types::Actor::user(api_types::UserActionSource::Test),
        reason,
        false,
    )
    .await
    .expect("override transition succeeds");

    let logs = TransitionLogRepo::list_by_task(&*db, &task_id)
        .await
        .expect("transition logs load");
    let latest = logs
        .iter()
        .find(|entry| entry.to_state == "done" && !entry.rejection)
        .expect("successful override transition log exists");
    assert_eq!(latest.triggered_by, "user:override:test");
    assert_eq!(latest.trigger_reason, reason);
}

#[tokio::test]
async fn subtask_user_override_into_review_no_workspace_no_reviewer_completes() {
    // Task 6.1: no workspace, no reviewer — subtask user override into review must not panic
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let parent_id = new_uuid_v4();
    let subtask_id = new_uuid_v4();
    seed_parent_and_subtask(
        &db,
        &parent_id,
        &subtask_id,
        default_states::IN_PROGRESS,
        None,
    )
    .await;
    let workflow = default_workflow::default_workflow();
    let eng = engine(Arc::clone(&db), event_bus);

    let result = eng
        .transition(
            &subtask_id,
            default_states::REVIEW,
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "subtask into review",
            false,
        )
        .await
        .expect("subtask user override into review completes without panic");

    assert_eq!(result.task.status, default_states::REVIEW);
}

#[tokio::test]
async fn subtask_user_override_into_review_with_ci_steps_completes_or_fails_gracefully() {
    // Task 6.1: CI step configured — must not panic; may fail with ServiceError only
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let parent_id = new_uuid_v4();
    let subtask_id = new_uuid_v4();
    seed_parent_and_subtask(
        &db,
        &parent_id,
        &subtask_id,
        default_states::IN_PROGRESS,
        Some(r#"{"review":{"ci_steps":["test -d ."]}}"#.to_owned()),
    )
    .await;
    let workflow = default_workflow::default_workflow();
    let eng = engine(Arc::clone(&db), event_bus);

    let result = eng
        .transition(
            &subtask_id,
            default_states::REVIEW,
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "subtask review with ci",
            false,
        )
        .await;

    match result {
        Ok(transition) => assert_eq!(transition.task.status, default_states::REVIEW),
        Err(error) => assert!(
            matches!(
                error,
                ServiceError::GuardRejection { .. }
                    | ServiceError::InvalidOperation { .. }
                    | ServiceError::Db(_)
            ),
            "CI-configured subtask review must fail via ServiceError, not panic: {error:?}"
        ),
    }
}

#[tokio::test]
async fn subtask_user_override_into_review_empty_ci_auto_passes() {
    // Task 6.1: empty CI auto-passes — system unconfigured review skips; user stays in review
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let parent_id = new_uuid_v4();
    let subtask_id = new_uuid_v4();
    seed_parent_and_subtask(
        &db,
        &parent_id,
        &subtask_id,
        default_states::IN_PROGRESS,
        None,
    )
    .await;
    let workflow = default_workflow::default_workflow();
    let eng = engine(Arc::clone(&db), event_bus);

    let result = eng
        .transition(
            &subtask_id,
            default_states::REVIEW,
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "subtask review no ci",
            false,
        )
        .await
        .expect("subtask user override into review with empty CI completes");

    assert_eq!(result.task.status, default_states::REVIEW);
}

#[tokio::test]
async fn subtask_user_override_into_merging_without_merge_service_completes() {
    // Task 6.1: merge service absent — subtask user override into merging must not panic
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let parent_id = new_uuid_v4();
    let subtask_id = new_uuid_v4();
    seed_parent_and_subtask(
        &db,
        &parent_id,
        &subtask_id,
        default_states::IN_PROGRESS,
        None,
    )
    .await;
    let workflow = default_workflow::default_workflow();
    let eng = engine(Arc::clone(&db), event_bus);

    let result = eng
        .transition(
            &subtask_id,
            default_states::MERGING,
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "subtask into merging",
            false,
        )
        .await;

    match result {
        Ok(transition) => assert!(
            transition.task.status == default_states::MERGING
                || transition.task.status == default_states::DONE,
            "merge hook may cascade when merge service is absent, got {}",
            transition.task.status
        ),
        Err(error) => assert!(
            matches!(
                error,
                ServiceError::GuardRejection { .. }
                    | ServiceError::InvalidOperation { .. }
                    | ServiceError::Db(_)
            ),
            "subtask merge override must fail via ServiceError, not panic: {error:?}"
        ),
    }
}
