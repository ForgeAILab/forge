use api_types::{
    CanonicalPhase, FailurePolicy, HookAudience, HookSpec, RoleDefinition, StateDefinition,
    StateHooks, StateKind, WorkflowDefinition, WorkflowTrigger, WorkflowTriggerDefinition,
};
use serde_json::json;

use crate::workflow::{default_roles, default_states};

fn hook(action: &str) -> HookSpec {
    HookSpec {
        action: action.to_owned(),
        params: json!({}),
        applies_to: HookAudience::All,
        on_failure: FailurePolicy::Log,
    }
}

fn state(
    name: &str,
    kind: StateKind,
    column: &str,
    display_name: &str,
    role: Option<&str>,
    canonical_phase: CanonicalPhase,
    hooks: StateHooks,
) -> StateDefinition {
    StateDefinition {
        name: name.to_owned(),
        kind,
        column: column.to_owned(),
        display_name: display_name.to_owned(),
        role: role.map(str::to_owned),
        hooks,
        cleanup: None,
        canonical_phase: Some(canonical_phase),
        gate_config: None,
        dispatch: None,
        triggers: std::collections::BTreeMap::new(),
        config: json!({}),
    }
}

pub fn inherited_subtask_workflow() -> WorkflowDefinition {
    let mut todo = state(
        default_states::TODO,
        StateKind::Initial,
        "Todo",
        "Todo",
        None,
        CanonicalPhase::Ready,
        StateHooks::default(),
    );
    todo.triggers.insert(
        WorkflowTrigger::Accept,
        WorkflowTriggerDefinition {
            to: default_states::IN_PROGRESS.to_owned(),
            dispatch: None,
        },
    );
    todo.triggers.insert(
        WorkflowTrigger::Fail,
        WorkflowTriggerDefinition {
            to: default_states::CANCELLED.to_owned(),
            dispatch: None,
        },
    );
    let mut in_progress = state(
        default_states::IN_PROGRESS,
        StateKind::Active,
        "In Progress",
        "In Progress",
        Some(default_roles::CODER),
        CanonicalPhase::Working,
        StateHooks {
            on_enter: vec![hook("dispatch_role_agent")],
            ..StateHooks::default()
        },
    );
    in_progress.triggers.insert(
        WorkflowTrigger::Accept,
        WorkflowTriggerDefinition {
            to: default_states::DONE.to_owned(),
            dispatch: None,
        },
    );
    in_progress.triggers.insert(
        WorkflowTrigger::Fail,
        WorkflowTriggerDefinition {
            to: default_states::CANCELLED.to_owned(),
            dispatch: None,
        },
    );
    WorkflowDefinition {
        roles: vec![RoleDefinition {
            name: default_roles::CODER.to_owned(),
            display_name: "Coder".to_owned(),
            description: "Implements the work".to_owned(),
        }],
        states: vec![
            todo,
            in_progress,
            state(
                default_states::DONE,
                StateKind::Terminal,
                "Done",
                "Done",
                None,
                CanonicalPhase::Done,
                StateHooks::default(),
            ),
            state(
                default_states::CANCELLED,
                StateKind::Terminal,
                "Done",
                "Cancelled",
                None,
                CanonicalPhase::Done,
                StateHooks::default(),
            ),
        ],
        configuration: Vec::new(),
        cancellation_state: Some(default_states::CANCELLED.to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use db::{
        create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, CreateProject, CreateRepo,
        CreateTask, ProjectRepo, RepoRepo, SqliteDb, TaskRepo, UpdateProject,
    };
    use events::EventBus;

    use super::inherited_subtask_workflow;
    use crate::workflow::{default_states, engine::WorkflowEngine};

    #[test]
    fn inherited_subtask_workflow_has_no_review_state() {
        assert!(!inherited_subtask_workflow()
            .states
            .iter()
            .any(|state| state.name == "review"));
    }

    #[test]
    fn inherited_subtask_workflow_has_no_merging_state() {
        assert!(!inherited_subtask_workflow()
            .states
            .iter()
            .any(|state| state.name == "merging"));
    }

    async fn sqlite_db() -> SqliteDb {
        let pool = create_sqlite_pool("sqlite::memory:")
            .await
            .expect("pool creates");
        run_migrations(&pool).await.expect("migrations run");
        SqliteDb::new(pool)
    }

    async fn seed_root_and_subtask(db: &SqliteDb, subtask_status: &str) -> db::Task {
        let now = now_rfc3339();
        let project_id = new_uuid_v4();
        let repo_id = new_uuid_v4();
        let root_id = new_uuid_v4();

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
                id: root_id.clone(),
                project_id: project_id.clone(),
                repo_id: Some(repo_id.clone()),
                parent_task_id: None,
                subtask_order: None,
                assignee_type: None,
                assignee_id: None,
                title: "root".to_owned(),
                description: None,
                task_type: "task".to_owned(),
                status: default_states::IN_PROGRESS.to_owned(),
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
        .expect("root task creates");

        TaskRepo::create(
            db,
            CreateTask {
                id: new_uuid_v4(),
                project_id,
                repo_id: Some(repo_id),
                parent_task_id: Some(root_id),
                subtask_order: Some(0),
                assignee_type: None,
                assignee_id: None,
                title: "subtask".to_owned(),
                description: None,
                task_type: "task".to_owned(),
                status: subtask_status.to_owned(),
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
        .expect("subtask creates")
    }

    #[tokio::test]
    async fn in_progress_to_review_transition_fails() {
        let db = Arc::new(sqlite_db().await);
        let subtask = seed_root_and_subtask(&db, default_states::IN_PROGRESS).await;
        let workflow = inherited_subtask_workflow();
        let engine = WorkflowEngine {
            db,
            event_bus: Arc::new(EventBus::new(16)),
            review_runner: None,
            merge_service: None,
            cleanup_scheduler: None,
            task_executor: None,
            daemon_connections: None,
            workspace_exec_locks: None,
            terminal_activity: None,
            workspace_root: PathBuf::new(),
            repo_cache_locks: None,
        };

        let result = engine
            .transition(
                &subtask.id,
                default_states::REVIEW,
                subtask.version,
                &workflow,
                &api_types::Actor::user(api_types::UserActionSource::Test),
                "attempt review",
                false,
            )
            .await;

        assert!(result.is_err());
    }
}
