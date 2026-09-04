#![allow(dead_code, clippy::assertions_on_constants)]
use std::sync::Arc;

use api_types::{
    Actor, CanonicalPhase, FailurePolicy, HookAudience, HookSpec, RoleDefinition, StateDefinition,
    StateHooks, StateKind, UserActionSource, WorkflowDefinition, WorkflowTrigger,
    WorkflowTriggerDefinition,
};
use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, CreateProject, CreateRepo,
    CreateTask, CreateTaskRoleAssignment, ProjectRepo, RepoRepo, SqliteDb, TaskRepo,
    TaskRoleAssignmentRepo, UpdateProject,
};
use events::EventBus;
use serde_json::json;
use services::{
    workflow::{default_roles, default_states, engine::WorkflowEngine},
    ServiceError,
};

#[tokio::test]
async fn agent_only_before_exit_guard_blocks_agents_but_not_users() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    seed_project_repo_and_task(&db, &task_id, default_states::TODO).await;
    assign_role(&db, &task_id, default_roles::PLANNER).await;

    let workflow = workflow_with_agent_only_upstream_guard();
    let current_task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let agent_result = engine(Arc::clone(&db), Arc::clone(&event_bus))
        .transition(
            &task_id,
            default_states::IN_PROGRESS,
            current_task.version,
            &workflow,
            &Actor::agent("coder"),
            "claim",
            false,
        )
        .await;

    match agent_result {
        Err(ServiceError::GuardRejection { guard, reason }) => {
            assert_eq!(guard, "require_upstream_roles_completed");
            assert!(
                reason.contains(default_roles::PLANNER),
                "guard reason should mention planner role: {reason}"
            );
        }
        Err(error) => panic!("expected guard rejection, got {error:?}"),
        Ok(_) => panic!("expected agent-triggered transition to be blocked"),
    }

    let after_agent_attempt = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(after_agent_attempt.status, default_states::TODO);

    let user_result = engine(db, event_bus)
        .transition(
            &task_id,
            default_states::IN_PROGRESS,
            after_agent_attempt.version,
            &workflow,
            &Actor::user(UserActionSource::Test),
            "manual move",
            false,
        )
        .await
        .expect("user-triggered transition skips agent-only guard");

    assert_eq!(user_result.task.status, default_states::IN_PROGRESS);
}

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
            name: "repo".to_owned(),
            local_path: None,
            work_mode: db::WorkMode::DirectMerge,
            remote_url: "https://example.com/repo.git".to_owned(),
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
            repo_id: Some(repo_id),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "hook audience task".to_owned(),
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

async fn assign_role(db: &SqliteDb, task_id: &str, role_name: &str) {
    let now = now_rfc3339();
    TaskRoleAssignmentRepo::assign(
        db,
        CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: task_id.to_owned(),
            role_name: role_name.to_owned(),
            assignee_type: Some(db::AssigneeKind::User),
            assignee_id: Some("planner@example.com".to_owned()),
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
        workspace_root: std::path::PathBuf::new(),
        repo_cache_locks: None,
    }
}

fn workflow_with_agent_only_upstream_guard() -> WorkflowDefinition {
    let mut todo = state(
        default_states::TODO,
        StateKind::Initial,
        None,
        StateHooks {
            before_exit: vec![HookSpec {
                action: "require_upstream_roles_completed".to_owned(),
                params: json!({}),
                applies_to: HookAudience::AgentOnly,
                on_failure: FailurePolicy::Block,
            }],
            ..StateHooks::default()
        },
    );
    todo.triggers.insert(
        WorkflowTrigger::Accept,
        WorkflowTriggerDefinition {
            to: default_states::IN_PROGRESS.to_owned(),
            dispatch: None,
        },
    );
    let mut planning = state(
        default_states::PLANNING,
        StateKind::Gate,
        Some(default_roles::PLANNER),
        StateHooks::default(),
    );
    planning.triggers.insert(
        WorkflowTrigger::Accept,
        WorkflowTriggerDefinition {
            to: default_states::IN_PROGRESS.to_owned(),
            dispatch: None,
        },
    );
    WorkflowDefinition {
        roles: vec![RoleDefinition {
            name: default_roles::PLANNER.to_owned(),
            display_name: "Planner".to_owned(),
            description: "Plans work before implementation".to_owned(),
        }],
        states: vec![
            todo,
            planning,
            state(
                default_states::IN_PROGRESS,
                StateKind::Active,
                None,
                StateHooks::default(),
            ),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
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
            StateKind::Backlog => CanonicalPhase::Backlog,
            StateKind::Initial => CanonicalPhase::Ready,
            StateKind::Active => CanonicalPhase::Working,
            StateKind::Gate => CanonicalPhase::Working,
            StateKind::Terminal => CanonicalPhase::Done,
            StateKind::Custom => CanonicalPhase::Working,
        }),
        gate_config: None,
        dispatch: None,
        triggers: std::collections::BTreeMap::new(),
        config: json!({}),
    }
}
