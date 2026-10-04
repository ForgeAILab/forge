use api_types::{
    CanonicalPhase, CleanupPolicy, FailurePolicy, GateConfig, HookAudience, HookSpec,
    RoleDefinition, StateDefinition, StateHooks, StateKind, WorkflowConfigBinding,
    WorkflowConfigField, WorkflowConfigValueType, WorkflowDefinition, WorkflowDispatch,
    WorkflowExecutionPolicy, WorkflowPromptConfig, WorkflowTrigger, WorkflowTriggerDefinition,
};
use serde_json::json;

use crate::workflow::{default_roles, dispatch};

pub const BACKLOG: &str = "backlog";
pub const READY: &str = "ready";
pub const WORKING: &str = "working";
pub const REVIEW: &str = "review";
pub const MERGING: &str = "merging";
pub const MERGE_FAILED: &str = "merge_failed";
pub const DONE: &str = "done";
pub const CANCELLED: &str = "cancelled";

fn hook(action: &str) -> HookSpec {
    HookSpec {
        action: action.to_owned(),
        params: json!({}),
        applies_to: HookAudience::All,
        on_failure: FailurePolicy::Log,
    }
}

fn blocking_hook(action: &str) -> HookSpec {
    HookSpec {
        action: action.to_owned(),
        params: json!({}),
        applies_to: HookAudience::All,
        on_failure: FailurePolicy::Block,
    }
}

fn agent_blocking_hook(action: &str) -> HookSpec {
    HookSpec {
        action: action.to_owned(),
        params: json!({}),
        applies_to: HookAudience::AgentOnly,
        on_failure: FailurePolicy::Block,
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

pub fn default_autonomous_workflow() -> WorkflowDefinition {
    let mut states = vec![
        state(
            BACKLOG,
            StateKind::Backlog,
            "Backlog",
            "Backlog",
            None,
            CanonicalPhase::Backlog,
            StateHooks::default(),
        ),
        state(
            READY,
            StateKind::Initial,
            "Ready",
            "Ready",
            None,
            CanonicalPhase::Ready,
            StateHooks {
                before_exit: vec![agent_blocking_hook("dependency_gate")],
                ..StateHooks::default()
            },
        ),
        state(
            WORKING,
            StateKind::Active,
            "Working",
            "Working",
            Some(default_roles::WORKER),
            CanonicalPhase::Working,
            StateHooks {
                before_enter: vec![blocking_hook("run_before_work_hooks")],
                on_enter: vec![hook("dispatch_role_agent")],
                ..StateHooks::default()
            },
        ),
        state(
            REVIEW,
            StateKind::Gate,
            "Review",
            "Review",
            None,
            CanonicalPhase::Review,
            StateHooks {
                before_enter: vec![
                    blocking_hook("run_before_work_hooks"),
                    blocking_hook("run_ci_steps"),
                ],
                after_enter: vec![
                    hook("auto_cascade_on_review_pass"),
                    hook("auto_cascade_on_unconfigured_review"),
                    hook("check_retry_budget"),
                ],
                ..StateHooks::default()
            },
        ),
        state(
            MERGING,
            StateKind::Gate,
            "Review",
            "Merging",
            None,
            CanonicalPhase::Review,
            StateHooks {
                on_enter: vec![hook("run_merge")],
                after_enter: vec![hook("auto_cascade_on_merge_result")],
                ..StateHooks::default()
            },
        ),
        state(
            MERGE_FAILED,
            StateKind::Active,
            "Review",
            "Merge repair",
            Some(default_roles::WORKER),
            CanonicalPhase::Review,
            StateHooks {
                before_exit: vec![blocking_hook("require_conflict_markers_resolved")],
                before_enter: vec![blocking_hook("run_before_work_hooks")],
                on_enter: vec![hook("notify_role_holder"), hook("dispatch_role_agent")],
                ..StateHooks::default()
            },
        ),
        state(
            DONE,
            StateKind::Terminal,
            "Done",
            "Done",
            None,
            CanonicalPhase::Done,
            StateHooks {
                on_enter: vec![hook("cleanup_workspace_now")],
                ..StateHooks::default()
            },
        ),
        state(
            CANCELLED,
            StateKind::Terminal,
            "Done",
            "Cancelled",
            None,
            CanonicalPhase::Done,
            StateHooks {
                on_enter: vec![hook("schedule_workspace_cleanup")],
                ..StateHooks::default()
            },
        ),
    ];

    for state in &mut states {
        if state.name == DONE {
            state.cleanup = Some(CleanupPolicy::Immediate);
        }
        if state.name == CANCELLED {
            state.cleanup = Some(CleanupPolicy::Delayed { seconds: 86_400 });
        }
        if state.name == WORKING {
            // A normal first entry has no terminal worker execution to resume, so the
            // dispatch loader falls back to a new run. Returning from failed validation
            // does have one, which makes this state-level policy the same-worker repair path.
            state.dispatch = Some(WorkflowDispatch {
                builder: Some(dispatch::BUILDER_ID_WORKER_AUTONOMOUS_V1.to_owned()),
                execution_policy: Some(WorkflowExecutionPolicy::ResumeLatestTargetRoleThread),
                prompt: None,
            });
            state.config = json!({
                "prompt": {
                    "user_append": ""
                }
            });
            state.triggers.insert(
                WorkflowTrigger::Accept,
                WorkflowTriggerDefinition {
                    to: REVIEW.to_owned(),
                    dispatch: None,
                },
            );
        }
        if state.name == REVIEW {
            state.gate_config = Some(GateConfig {
                reject_target: Some(WORKING.to_owned()),
                max_rejections: Some(3),
                approve_label: Some("Approve delivery".to_owned()),
                reject_label: Some("Request changes".to_owned()),
                requires_user_approval: Some(true),
                optional_when_unassigned: Some(false),
            });
            state.config = json!({});
            state.triggers.insert(
                WorkflowTrigger::Accept,
                WorkflowTriggerDefinition {
                    to: MERGING.to_owned(),
                    dispatch: None,
                },
            );
            state.triggers.insert(
                WorkflowTrigger::Reject,
                WorkflowTriggerDefinition {
                    to: WORKING.to_owned(),
                    dispatch: Some(WorkflowDispatch {
                        builder: Some(dispatch::BUILDER_ID_WORKER_REVIEW_FIX_V1.to_owned()),
                        execution_policy: Some(
                            WorkflowExecutionPolicy::ResumeLatestTargetRoleThread,
                        ),
                        prompt: Some(WorkflowPromptConfig {
                            user_prefix: None,
                            user_append: Some(
                                "Address all review feedback before resubmitting with updated validation evidence."
                                    .to_owned(),
                            ),
                            system_prefix: None,
                            system_append: None,
                        }),
                    }),
                },
            );
        }
        if state.name == MERGING {
            state.config = json!({
                "retry_budgets": {
                    "merge_fix": 1
                }
            });
            state.gate_config = Some(GateConfig {
                reject_target: Some(MERGE_FAILED.to_owned()),
                max_rejections: Some(1),
                approve_label: Some("Merge".to_owned()),
                reject_label: Some("Repair merge".to_owned()),
                requires_user_approval: Some(false),
                optional_when_unassigned: Some(false),
            });
            state.triggers.insert(
                WorkflowTrigger::Accept,
                WorkflowTriggerDefinition {
                    to: DONE.to_owned(),
                    dispatch: None,
                },
            );
            state.triggers.insert(
                WorkflowTrigger::Retry,
                WorkflowTriggerDefinition {
                    to: MERGE_FAILED.to_owned(),
                    dispatch: None,
                },
            );
            state.triggers.insert(
                WorkflowTrigger::Fail,
                WorkflowTriggerDefinition {
                    to: CANCELLED.to_owned(),
                    dispatch: None,
                },
            );
        }
        if state.name == MERGE_FAILED {
            state.config = json!({
                "retry_budgets": {
                    "merge_fix": 1
                },
                "prompt": {
                    "user_append": ""
                }
            });
            state.dispatch = Some(WorkflowDispatch {
                builder: Some(dispatch::BUILDER_ID_WORKER_MERGE_FIX_V1.to_owned()),
                execution_policy: Some(WorkflowExecutionPolicy::ResumeLatestTargetRoleThread),
                prompt: None,
            });
            state.triggers.insert(
                WorkflowTrigger::Accept,
                WorkflowTriggerDefinition {
                    to: REVIEW.to_owned(),
                    dispatch: None,
                },
            );
            state.triggers.insert(
                WorkflowTrigger::Retry,
                WorkflowTriggerDefinition {
                    to: WORKING.to_owned(),
                    dispatch: Some(WorkflowDispatch {
                        builder: Some(dispatch::BUILDER_ID_WORKER_MERGE_FIX_V1.to_owned()),
                        execution_policy: Some(
                            WorkflowExecutionPolicy::ResumeLatestTargetRoleThread,
                        ),
                        prompt: None,
                    }),
                },
            );
            state.triggers.insert(
                WorkflowTrigger::Fail,
                WorkflowTriggerDefinition {
                    to: CANCELLED.to_owned(),
                    dispatch: None,
                },
            );
        }
        if state.name == BACKLOG {
            state.triggers.insert(
                WorkflowTrigger::Accept,
                WorkflowTriggerDefinition {
                    to: READY.to_owned(),
                    dispatch: None,
                },
            );
            state.triggers.insert(
                WorkflowTrigger::Reject,
                WorkflowTriggerDefinition {
                    to: CANCELLED.to_owned(),
                    dispatch: None,
                },
            );
        }
        if state.name == READY {
            state.triggers.insert(
                WorkflowTrigger::Accept,
                WorkflowTriggerDefinition {
                    to: WORKING.to_owned(),
                    dispatch: None,
                },
            );
            state.triggers.insert(
                WorkflowTrigger::Reject,
                WorkflowTriggerDefinition {
                    to: BACKLOG.to_owned(),
                    dispatch: None,
                },
            );
            state.triggers.insert(
                WorkflowTrigger::Fail,
                WorkflowTriggerDefinition {
                    to: CANCELLED.to_owned(),
                    dispatch: None,
                },
            );
        }
    }

    WorkflowDefinition {
        roles: vec![RoleDefinition {
            name: default_roles::WORKER.to_owned(),
            display_name: "Worker".to_owned(),
            description:
                "Owns planning, implementation, self-validation, and routine recovery for the task."
                    .to_owned(),
        }],
        states,
        configuration: vec![
            WorkflowConfigField {
                id: "review_retries".to_owned(),
                label: "Review change cycles".to_owned(),
                description: Some(
                    "Requested-change cycles allowed before the task requires explicit recovery."
                        .to_owned(),
                ),
                value_type: WorkflowConfigValueType::Integer,
                min: Some(0),
                default_value: Some(json!(3)),
                binding: WorkflowConfigBinding::GateConfig {
                    state: REVIEW.to_owned(),
                    field: "max_rejections".to_owned(),
                },
            },
            WorkflowConfigField {
                id: "worker_prompt_instructions".to_owned(),
                label: "Worker instructions".to_owned(),
                description: Some(
                    "Extra project instructions appended to worker task prompts.".to_owned(),
                ),
                value_type: WorkflowConfigValueType::Text,
                min: None,
                default_value: Some(json!("")),
                binding: WorkflowConfigBinding::StateConfig {
                    state: WORKING.to_owned(),
                    path: vec!["prompt".to_owned(), "user_append".to_owned()],
                },
            },
            WorkflowConfigField {
                id: "merge_fix_prompt_instructions".to_owned(),
                label: "Merge repair instructions".to_owned(),
                description: Some(
                    "Extra project instructions appended during merge-conflict recovery."
                        .to_owned(),
                ),
                value_type: WorkflowConfigValueType::Text,
                min: None,
                default_value: Some(json!("")),
                binding: WorkflowConfigBinding::StateConfig {
                    state: MERGE_FAILED.to_owned(),
                    path: vec!["prompt".to_owned(), "user_append".to_owned()],
                },
            },
        ],
        cancellation_state: Some(CANCELLED.to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use api_types::{
        CanonicalPhase, CleanupPolicy, HookAudience, StateKind, WorkflowExecutionPolicy,
        WorkflowTrigger,
    };

    use super::{default_autonomous_workflow, CANCELLED, MERGE_FAILED, MERGING, REVIEW, WORKING};
    use crate::workflow::{default_roles, validation::validate_workflow};

    #[test]
    fn autonomous_v1_has_the_worker_only_state_graph() {
        let workflow = default_autonomous_workflow();
        validate_workflow(&workflow).expect("autonomous workflow validates");

        assert_eq!(workflow.roles.len(), 1);
        assert_eq!(workflow.roles[0].name, default_roles::WORKER);
        assert!(!workflow.states.iter().any(|state| state.name == "planning"));
        assert!(workflow.states.iter().all(|state| {
            state.canonical_phase.is_some()
                && !state
                    .hooks
                    .before_exit
                    .iter()
                    .any(|hook| hook.action == "require_plan_checklist_complete")
        }));

        let expected = [
            ("backlog", StateKind::Backlog, CanonicalPhase::Backlog, None),
            ("ready", StateKind::Initial, CanonicalPhase::Ready, None),
            (
                WORKING,
                StateKind::Active,
                CanonicalPhase::Working,
                Some(default_roles::WORKER),
            ),
            (REVIEW, StateKind::Gate, CanonicalPhase::Review, None),
            (MERGING, StateKind::Gate, CanonicalPhase::Review, None),
            (
                MERGE_FAILED,
                StateKind::Active,
                CanonicalPhase::Review,
                Some(default_roles::WORKER),
            ),
            ("done", StateKind::Terminal, CanonicalPhase::Done, None),
            (CANCELLED, StateKind::Terminal, CanonicalPhase::Done, None),
        ];
        for (name, kind, phase, role) in expected {
            let state = workflow
                .states
                .iter()
                .find(|state| state.name == name)
                .expect("state exists");
            assert_eq!(state.kind, kind);
            assert_eq!(state.canonical_phase, Some(phase));
            assert_eq!(state.role.as_deref(), role);
        }

        let ready = workflow
            .states
            .iter()
            .find(|state| state.name == super::READY)
            .expect("ready state");
        assert_eq!(
            ready.hooks.before_exit[0].applies_to,
            HookAudience::AgentOnly
        );

        let done = workflow
            .states
            .iter()
            .find(|state| state.name == super::DONE)
            .expect("done state");
        assert_eq!(done.cleanup, Some(CleanupPolicy::Immediate));
        assert_eq!(
            done.hooks
                .on_enter
                .iter()
                .map(|hook| hook.action.as_str())
                .collect::<Vec<_>>(),
            vec!["cleanup_workspace_now"]
        );

        let cancelled = workflow
            .states
            .iter()
            .find(|state| state.name == CANCELLED)
            .expect("cancelled state");
        assert_eq!(
            cancelled.cleanup,
            Some(CleanupPolicy::Delayed { seconds: 86_400 })
        );
        assert_eq!(
            cancelled
                .hooks
                .on_enter
                .iter()
                .map(|hook| hook.action.as_str())
                .collect::<Vec<_>>(),
            vec!["schedule_workspace_cleanup"]
        );
    }

    #[test]
    fn validation_and_review_rejection_resume_the_worker() {
        let workflow = default_autonomous_workflow();
        let working = workflow
            .states
            .iter()
            .find(|state| state.name == WORKING)
            .expect("working state");
        assert_eq!(
            working
                .dispatch
                .as_ref()
                .and_then(|dispatch| dispatch.execution_policy),
            Some(WorkflowExecutionPolicy::ResumeLatestTargetRoleThread)
        );

        let review = workflow
            .states
            .iter()
            .find(|state| state.name == REVIEW)
            .expect("review state");
        assert_eq!(
            review
                .hooks
                .before_enter
                .iter()
                .map(|hook| (hook.action.as_str(), &hook.on_failure))
                .collect::<Vec<_>>(),
            vec![
                ("run_before_work_hooks", &api_types::FailurePolicy::Block),
                ("run_ci_steps", &api_types::FailurePolicy::Block),
            ]
        );
        assert!(review
            .gate_config
            .as_ref()
            .is_some_and(|gate| gate.requires_user_approval()));
        assert_eq!(review.role, None);
        let reject = review
            .triggers
            .get(&WorkflowTrigger::Reject)
            .expect("review rejection");
        assert_eq!(reject.to, WORKING);
        assert_eq!(
            reject
                .dispatch
                .as_ref()
                .and_then(|dispatch| dispatch.execution_policy),
            Some(WorkflowExecutionPolicy::ResumeLatestTargetRoleThread)
        );
    }
}
