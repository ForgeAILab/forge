use api_types::{
    CanonicalPhase, FailurePolicy, GateConfig, HookAudience, HookSpec, RoleDefinition,
    StateDefinition, StateHooks, StateKind, WorkflowDefinition, WorkflowDispatch,
    WorkflowExecutionPolicy, WorkflowTrigger, WorkflowTriggerDefinition,
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

/// The role of the step-check gate. It is never assigned on a subtask: it
/// resolves to the root's lead (`task_hierarchy::effective_role_assignment`).
pub const STEP_CHECK_ROLE: &str = db::inherited_subtask::STEP_CHECK_ROLE;

/// Whether `state` belongs to the inherited subtask workflow. The names live
/// in `db::inherited_subtask` (the database decides role holders and workflow
/// governance from them without loading a workflow); the test
/// `the_database_names_are_this_workflow` fails when they and the definition
/// below diverge.
pub fn is_inherited_state(state: &str) -> bool {
    db::inherited_subtask::is_state(state)
}

/// The step check: an optional gate in the working phase between
/// implementation and `done`. With no lead on the root it is skipped on entry
/// (`auto_cascade_on_unassigned_role`); with one, the lead runs a read-only
/// execution on the subtask and its result block accepts the step or returns
/// it to the same worker thread. `max_rejections` bounds the returns: the
/// entry after the last allowed return parks the subtask (`check_retry_budget`
/// runs before the dispatch, so no check is started on a spent budget).
fn lead_check() -> StateDefinition {
    let mut lead_check = state(
        default_states::LEAD_CHECK,
        StateKind::Gate,
        "In Progress",
        "Lead Check",
        Some(STEP_CHECK_ROLE),
        CanonicalPhase::Working,
        StateHooks {
            on_enter: vec![hook("check_retry_budget"), hook("dispatch_role_agent")],
            after_enter: vec![hook("auto_cascade_on_unassigned_role")],
            ..StateHooks::default()
        },
    );
    lead_check.dispatch = Some(WorkflowDispatch {
        builder: Some(crate::workflow::dispatch::BUILDER_ID_PLANNER_STEP_CHECK_V1.to_owned()),
        execution_policy: None,
        prompt: None,
    });
    lead_check.gate_config = Some(GateConfig {
        reject_target: Some(default_states::IN_PROGRESS.to_owned()),
        max_rejections: Some(2),
        approve_label: Some("Accept step".to_owned()),
        reject_label: Some("Return step".to_owned()),
        requires_user_approval: Some(false),
        optional_when_unassigned: Some(true),
    });
    lead_check.triggers.insert(
        WorkflowTrigger::Accept,
        WorkflowTriggerDefinition {
            to: default_states::DONE.to_owned(),
            dispatch: None,
        },
    );
    lead_check.triggers.insert(
        WorkflowTrigger::Reject,
        WorkflowTriggerDefinition {
            to: default_states::IN_PROGRESS.to_owned(),
            dispatch: Some(WorkflowDispatch {
                builder: None,
                execution_policy: Some(WorkflowExecutionPolicy::ResumeLatestTargetRoleThread),
                prompt: None,
            }),
        },
    );
    lead_check.triggers.insert(
        WorkflowTrigger::Fail,
        WorkflowTriggerDefinition {
            to: default_states::CANCELLED.to_owned(),
            dispatch: None,
        },
    );
    lead_check
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
            to: default_states::LEAD_CHECK.to_owned(),
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
        roles: vec![
            RoleDefinition {
                name: default_roles::CODER.to_owned(),
                display_name: "Coder".to_owned(),
                description: "Implements the work".to_owned(),
            },
            RoleDefinition {
                name: STEP_CHECK_ROLE.to_owned(),
                display_name: "Lead".to_owned(),
                description: "The root's lead checks each finished step".to_owned(),
            },
        ],
        states: vec![
            todo,
            in_progress,
            lead_check(),
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
    use std::sync::Arc;

    use db::{
        create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, CreateProject, CreateRepo,
        CreateTask, ProjectRepo, RepoRepo, SqliteDb, TaskRepo, UpdateProject,
    };
    use events::EventBus;

    use super::inherited_subtask_workflow;
    use crate::workflow::default_states;

    #[test]
    fn inherited_subtask_workflow_has_no_review_state() {
        assert!(!inherited_subtask_workflow()
            .states
            .iter()
            .any(|state| state.name == "review"));
    }

    /// The step check is a working-phase gate, never a review: a review-phase
    /// gate on a subtask would change subtask creation, the aggregate review
    /// role and merge admission.
    #[test]
    fn the_step_check_is_an_optional_working_gate_between_implementation_and_done() {
        let workflow = inherited_subtask_workflow();
        let gate = workflow
            .states
            .iter()
            .find(|state| state.name == default_states::LEAD_CHECK)
            .expect("lead_check exists");
        assert_eq!(gate.kind, api_types::StateKind::Gate);
        assert_eq!(
            gate.canonical_phase,
            Some(api_types::CanonicalPhase::Working)
        );
        assert_eq!(gate.role.as_deref(), Some(super::STEP_CHECK_ROLE));
        let config = gate.gate_config.as_ref().expect("gate config");
        assert!(config.optional_when_unassigned());
        assert!(!config.requires_user_approval());
        assert_eq!(config.max_rejections, Some(2));
        assert_eq!(
            config.reject_target.as_deref(),
            Some(default_states::IN_PROGRESS)
        );
        assert_eq!(
            workflow.auto_transition_target(default_states::IN_PROGRESS),
            Some(default_states::LEAD_CHECK)
        );
        assert_eq!(
            workflow.auto_transition_target(default_states::LEAD_CHECK),
            Some(default_states::DONE)
        );
        // The budget check runs before the dispatch.
        let on_enter: Vec<&str> = gate
            .hooks
            .on_enter
            .iter()
            .map(|hook| hook.action.as_str())
            .collect();
        assert_eq!(on_enter, ["check_retry_budget", "dispatch_role_agent"]);
        for state in &workflow.states {
            assert!(super::is_inherited_state(&state.name), "{}", state.name);
        }
        assert!(!super::is_inherited_state(default_states::PLANNING));
        assert!(!super::is_inherited_state(default_states::REVIEW));
    }

    /// One source for the rule "who holds a subtask's step check": the
    /// database reads state and role names from `db::inherited_subtask`, the
    /// services derive them from this workflow. They must be the same set,
    /// in both directions.
    #[test]
    fn the_database_names_are_this_workflow() {
        let workflow = inherited_subtask_workflow();
        let defined: Vec<&str> = workflow.states.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(defined, db::inherited_subtask::STATES);
        for state in &workflow.states {
            assert_eq!(
                crate::workflow::effective_role(state),
                db::inherited_subtask::role(&state.name),
                "{}",
                state.name
            );
        }
        let gates: Vec<&str> = workflow
            .states
            .iter()
            .filter(|s| s.kind == api_types::StateKind::Gate)
            .map(|s| s.name.as_str())
            .collect();
        assert_eq!(gates, [db::inherited_subtask::STEP_CHECK_STATE]);
        assert_eq!(
            default_states::LEAD_CHECK,
            db::inherited_subtask::STEP_CHECK_STATE
        );
        assert_eq!(
            crate::workflow::default_roles::PLANNER,
            db::inherited_subtask::STEP_CHECK_ROLE
        );
        assert!(workflow
            .roles
            .iter()
            .any(|r| r.name == db::inherited_subtask::STEP_CHECK_ROLE));
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
                id: root_id.clone(),
                project_id: project_id.clone(),
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
        let event_bus = Arc::new(EventBus::new(16));
        let task_service = crate::TaskService::new(Arc::clone(&db), Arc::clone(&event_bus));
        let engine = task_service;

        let result = engine
            .workflow_execution()
            .transition(
                &subtask.id,
                default_states::REVIEW,
                subtask.version,
                &workflow,
                &api_types::Actor::user(api_types::UserActionSource::Test),
                "attempt review",
                false,
                Default::default(),
            )
            .await;

        assert!(result.is_err());
    }
}
