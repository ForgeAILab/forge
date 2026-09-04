#![allow(dead_code, clippy::assertions_on_constants)]
use std::sync::Arc;

use api_types::{
    Actor, CanonicalPhase, FailurePolicy, HookAudience, HookResultEntry, HookSpec, StateDefinition,
    StateHooks, StateKind, UserActionSource, WorkflowDefinition, WorkflowTrigger,
    WorkflowTriggerDefinition,
};
use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, CreateProject, CreateRepo,
    CreateTask, ProjectRepo, RepoRepo, SqliteDb, TaskRepo, TransitionLogRepo, UpdateProject,
};
use events::EventBus;
use serde_json::json;
use services::workflow::engine::WorkflowEngine;

#[tokio::test]
async fn explicit_cancellation_state_uses_implicit_edge_and_skips_before_exit_guards() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    seed_project_repo_and_task(&db, &task_id, "todo").await;
    let workflow = cancellation_workflow();

    let todo = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let qa = engine(Arc::clone(&db), Arc::clone(&event_bus))
        .transition(
            &task_id,
            "qa",
            todo.version,
            &workflow,
            &Actor::user(UserActionSource::Test),
            "ready for qa",
            false,
        )
        .await
        .expect("todo -> qa succeeds")
        .task;
    assert_eq!(qa.status, "qa");

    let cancelled = engine(Arc::clone(&db), event_bus)
        .transition(
            &task_id,
            "cancelled",
            qa.version,
            &workflow,
            &Actor::user(UserActionSource::Test),
            "cancel explicitly",
            false,
        )
        .await
        .expect("implicit qa -> cancelled cancellation transition succeeds")
        .task;

    assert_eq!(cancelled.status, "cancelled");

    let logs = TransitionLogRepo::list_by_task(&*db, &task_id)
        .await
        .expect("transition logs load");
    let cancellation_log = logs
        .iter()
        .find(|entry| entry.from_state == "qa" && entry.to_state == "cancelled")
        .expect("qa -> cancelled transition is logged");
    let hook_results: Vec<HookResultEntry> = serde_json::from_str(
        cancellation_log
            .hook_results_json
            .as_deref()
            .expect("hook results are recorded"),
    )
    .expect("hook results deserialize");

    assert!(
        hook_results
            .iter()
            .all(|entry| entry.action != "require_clean_worktree" && entry.phase != "before_exit"),
        "implicit cancellation must not run qa.before_exit guards: {hook_results:?}"
    );
    // on_exit behavior is intentionally not asserted here; this test is scoped to cancellation
    // bypassing before_exit guards, which is the part needed for explicit cancellation semantics.
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
            title: "explicit cancellation task".to_owned(),
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

fn cancellation_workflow() -> WorkflowDefinition {
    let mut todo = state("todo", StateKind::Initial, StateHooks::default());
    todo.triggers.insert(
        WorkflowTrigger::Accept,
        WorkflowTriggerDefinition {
            to: "qa".to_owned(),
            dispatch: None,
        },
    );
    let mut qa = state(
        "qa",
        StateKind::Gate,
        StateHooks {
            before_exit: vec![HookSpec {
                action: "require_clean_worktree".to_owned(),
                params: json!({}),
                applies_to: HookAudience::All,
                on_failure: FailurePolicy::Block,
            }],
            ..StateHooks::default()
        },
    );
    qa.triggers.insert(
        WorkflowTrigger::Accept,
        WorkflowTriggerDefinition {
            to: "done".to_owned(),
            dispatch: None,
        },
    );
    WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            todo,
            qa,
            state("done", StateKind::Terminal, StateHooks::default()),
            state("cancelled", StateKind::Terminal, StateHooks::default()),
        ],
        configuration: Vec::new(),
        cancellation_state: Some("cancelled".to_owned()),
    }
}

fn state(name: &str, kind: StateKind, hooks: StateHooks) -> StateDefinition {
    StateDefinition {
        name: name.to_owned(),
        kind,
        column: name.to_owned(),
        display_name: name.to_owned(),
        role: None,
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
