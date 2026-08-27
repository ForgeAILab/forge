use api_types::{
    CanonicalPhase, CleanupPolicy, FailurePolicy, GateConfig, HookAudience, HookSpec,
    RoleDefinition, StateDefinition, StateHooks, StateKind, WorkflowConfigBinding,
    WorkflowConfigField, WorkflowConfigValueType, WorkflowDefinition, WorkflowDispatch,
    WorkflowExecutionPolicy, WorkflowPromptConfig, WorkflowTrigger, WorkflowTriggerDefinition,
};
use serde_json::json;

use crate::workflow::{default_roles, default_states};

fn hook(action: &str) -> HookSpec {
    HookSpec {
        action: action.to_string(),
        params: json!({}),
        applies_to: HookAudience::All,
        on_failure: FailurePolicy::Log,
    }
}

fn blocking_hook(action: &str) -> HookSpec {
    HookSpec {
        action: action.to_string(),
        params: json!({}),
        applies_to: HookAudience::All,
        on_failure: FailurePolicy::Block,
    }
}

fn agent_blocking_hook(action: &str) -> HookSpec {
    HookSpec {
        action: action.to_string(),
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
        name: name.to_string(),
        kind,
        column: column.to_string(),
        display_name: display_name.to_string(),
        role: role.map(str::to_string),
        hooks,
        cleanup: None,
        canonical_phase: Some(canonical_phase),
        gate_config: None,
        dispatch: None,
        triggers: std::collections::BTreeMap::new(),
        config: json!({}),
    }
}

pub fn default_workflow() -> WorkflowDefinition {
    let mut states = vec![
        state(
            default_states::BACKLOG,
            StateKind::Backlog,
            "Backlog",
            "Backlog",
            None,
            CanonicalPhase::Backlog,
            StateHooks::default(),
        ),
        state(
            default_states::TODO,
            StateKind::Initial,
            "Todo",
            "Todo",
            None,
            CanonicalPhase::Ready,
            StateHooks {
                before_exit: vec![agent_blocking_hook("dependency_gate")],
                ..StateHooks::default()
            },
        ),
        state(
            default_states::PLANNING,
            StateKind::Gate,
            "In Progress",
            "Planning",
            Some(default_roles::PLANNER),
            CanonicalPhase::Working,
            StateHooks {
                before_enter: vec![blocking_hook("run_before_work_hooks")],
                on_enter: vec![hook("dispatch_role_agent")],
                after_enter: vec![hook("auto_cascade_on_unassigned_role")],
                ..StateHooks::default()
            },
        ),
        state(
            default_states::IN_PROGRESS,
            StateKind::Active,
            "In Progress",
            "In Progress",
            Some(default_roles::CODER),
            CanonicalPhase::Working,
            StateHooks {
                before_exit: vec![
                    blocking_hook("subtask_sequence_complete"),
                    blocking_hook("require_plan_checklist_complete"),
                ],
                before_enter: vec![blocking_hook("run_before_work_hooks")],
                on_enter: vec![hook("dispatch_role_agent")],
                ..StateHooks::default()
            },
        ),
        state(
            default_states::REVIEW,
            StateKind::Gate,
            "Review",
            "Review",
            Some(default_roles::REVIEWER),
            CanonicalPhase::Review,
            StateHooks {
                before_enter: vec![
                    blocking_hook("run_before_work_hooks"),
                    blocking_hook("run_ci_steps"),
                ],
                on_enter: vec![hook("dispatch_role_agent")],
                after_enter: vec![
                    hook("auto_cascade_on_review_pass"),
                    hook("auto_cascade_on_unconfigured_review"),
                    hook("check_retry_budget"),
                ],
                ..StateHooks::default()
            },
        ),
        state(
            default_states::MERGING,
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
            default_states::MERGE_FAILED,
            StateKind::Active,
            "Review",
            "Merge Failed",
            Some(default_roles::CODER),
            CanonicalPhase::Review,
            StateHooks {
                before_enter: vec![blocking_hook("run_before_work_hooks")],
                on_enter: vec![hook("notify_role_holder"), hook("dispatch_role_agent")],
                ..StateHooks::default()
            },
        ),
        state(
            default_states::DONE,
            StateKind::Terminal,
            "Done",
            "Done",
            None,
            CanonicalPhase::Done,
            StateHooks {
                on_enter: vec![
                    hook("cleanup_workspace_now"),
                    hook("satisfy_dependents"),
                    hook("propagate_done_to_subtasks"),
                ],
                ..StateHooks::default()
            },
        ),
        state(
            default_states::CANCELLED,
            StateKind::Terminal,
            "Done",
            "Cancelled",
            None,
            CanonicalPhase::Done,
            StateHooks {
                on_enter: vec![
                    hook("schedule_workspace_cleanup"),
                    hook("cancel_pending_subtasks"),
                ],
                ..StateHooks::default()
            },
        ),
    ];

    for state in &mut states {
        if state.name == default_states::DONE {
            state.cleanup = Some(CleanupPolicy::Immediate);
        }
        if state.name == default_states::CANCELLED {
            state.cleanup = Some(CleanupPolicy::Delayed { seconds: 86_400 });
        }
        if state.name == default_states::PLANNING {
            state.dispatch = Some(WorkflowDispatch {
                builder: Some("planner.default.v2".to_string()),
                execution_policy: None,
                prompt: None,
            });
            state.config = json!({
                "prompt": {
                    "user_append": ""
                }
            });
            state.gate_config = Some(GateConfig {
                reject_target: Some(default_states::PLANNING.to_string()),
                max_rejections: Some(2),
                approve_label: Some("Continue to implementation".to_string()),
                reject_label: None,
                // Charter approval authorizes implementation. Planning may
                // still run as an Agent role, but it is not another human
                // approval boundary.
                requires_user_approval: Some(false),
                optional_when_unassigned: Some(true),
            });
            state.triggers.insert(
                WorkflowTrigger::Accept,
                WorkflowTriggerDefinition {
                    to: default_states::IN_PROGRESS.to_string(),
                    dispatch: None,
                },
            );
            state.triggers.insert(
                WorkflowTrigger::Reject,
                WorkflowTriggerDefinition {
                    to: default_states::PLANNING.to_string(),
                    dispatch: None,
                },
            );
        }
        if state.name == default_states::IN_PROGRESS {
            state.config = json!({
                "prompt": {
                    "user_append": ""
                }
            });
        }
        if state.name == default_states::REVIEW {
            state.dispatch = Some(WorkflowDispatch {
                builder: Some("reviewer.default.v2".to_string()),
                execution_policy: None,
                prompt: None,
            });
            state.gate_config = Some(GateConfig {
                reject_target: Some(default_states::IN_PROGRESS.to_string()),
                max_rejections: Some(2),
                approve_label: Some("Approve review".to_string()),
                reject_label: Some("Request changes".to_string()),
                requires_user_approval: Some(false),
                optional_when_unassigned: Some(false),
            });
            state.triggers.insert(
                WorkflowTrigger::Accept,
                WorkflowTriggerDefinition {
                    to: default_states::MERGING.to_string(),
                    dispatch: None,
                },
            );
            state.triggers.insert(
                WorkflowTrigger::Reject,
                WorkflowTriggerDefinition {
                    to: default_states::IN_PROGRESS.to_string(),
                    dispatch: Some(WorkflowDispatch {
                        builder: Some("coder.review_fix.v2".to_string()),
                        execution_policy: Some(
                            WorkflowExecutionPolicy::ResumeLatestTargetRoleThread,
                        ),
                        prompt: Some(WorkflowPromptConfig {
                            user_prefix: None,
                            user_append: Some(
                                "Address all review feedback before resubmitting.".to_string(),
                            ),
                            system_prefix: None,
                            system_append: None,
                        }),
                    }),
                },
            );
        }
        if state.name == default_states::MERGE_FAILED {
            state.config = json!({
                "retry_budgets": {
                    "merge_fix": 1
                },
                "prompt": {
                    "user_append": ""
                }
            });
            state.dispatch = Some(WorkflowDispatch {
                builder: Some("coder.merge_fix.v2".to_string()),
                execution_policy: Some(WorkflowExecutionPolicy::ResumeLatestTargetRoleThread),
                prompt: None,
            });
            state.triggers.insert(
                WorkflowTrigger::Accept,
                WorkflowTriggerDefinition {
                    to: default_states::REVIEW.to_string(),
                    dispatch: None,
                },
            );
            state.triggers.insert(
                WorkflowTrigger::Retry,
                WorkflowTriggerDefinition {
                    to: default_states::IN_PROGRESS.to_string(),
                    dispatch: Some(WorkflowDispatch {
                        builder: Some("coder.merge_fix.v2".to_string()),
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
                    to: default_states::CANCELLED.to_string(),
                    dispatch: None,
                },
            );
        }
        if state.name == default_states::MERGING {
            state.config = json!({
                "retry_budgets": {
                    "merge_fix": 1
                }
            });
            state.gate_config = Some(GateConfig {
                reject_target: Some(default_states::MERGE_FAILED.to_string()),
                max_rejections: Some(1),
                approve_label: Some("Merge".to_string()),
                reject_label: Some("Fix merge".to_string()),
                requires_user_approval: Some(false),
                optional_when_unassigned: Some(false),
            });
        }
        if state.name == default_states::IN_PROGRESS {
            state.dispatch = Some(WorkflowDispatch {
                builder: Some("coder.implementation.v2".to_string()),
                execution_policy: None,
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
                    to: default_states::REVIEW.to_string(),
                    dispatch: None,
                },
            );
        }
        if state.name == default_states::REVIEW {
            state.config = json!({
                "prompt": {
                    "user_append": ""
                }
            });
        }
        if state.name == default_states::BACKLOG {
            state.triggers.insert(
                WorkflowTrigger::Accept,
                WorkflowTriggerDefinition {
                    to: default_states::TODO.to_string(),
                    dispatch: None,
                },
            );
            state.triggers.insert(
                WorkflowTrigger::Reject,
                WorkflowTriggerDefinition {
                    to: default_states::CANCELLED.to_string(),
                    dispatch: None,
                },
            );
        }
        if state.name == default_states::TODO {
            state.triggers.insert(
                WorkflowTrigger::Accept,
                WorkflowTriggerDefinition {
                    to: default_states::PLANNING.to_string(),
                    dispatch: None,
                },
            );
            state.triggers.insert(
                WorkflowTrigger::Retry,
                WorkflowTriggerDefinition {
                    to: default_states::IN_PROGRESS.to_string(),
                    dispatch: None,
                },
            );
            state.triggers.insert(
                WorkflowTrigger::Reject,
                WorkflowTriggerDefinition {
                    to: default_states::BACKLOG.to_string(),
                    dispatch: None,
                },
            );
            state.triggers.insert(
                WorkflowTrigger::Fail,
                WorkflowTriggerDefinition {
                    to: default_states::CANCELLED.to_string(),
                    dispatch: None,
                },
            );
        }
        if state.name == default_states::MERGING {
            state.triggers.insert(
                WorkflowTrigger::Accept,
                WorkflowTriggerDefinition {
                    to: default_states::DONE.to_string(),
                    dispatch: None,
                },
            );
            state.triggers.insert(
                WorkflowTrigger::Retry,
                WorkflowTriggerDefinition {
                    to: default_states::MERGE_FAILED.to_string(),
                    dispatch: None,
                },
            );
            state.triggers.insert(
                WorkflowTrigger::Fail,
                WorkflowTriggerDefinition {
                    to: default_states::CANCELLED.to_string(),
                    dispatch: None,
                },
            );
        }
    }

    let roles = vec![
        RoleDefinition {
            name: default_roles::PLANNER.to_string(),
            display_name: "Planner".to_string(),
            description: "Plans the work".to_string(),
        },
        RoleDefinition {
            name: default_roles::CODER.to_string(),
            display_name: "Coder".to_string(),
            description: "Implements the work".to_string(),
        },
        RoleDefinition {
            name: default_roles::REVIEWER.to_string(),
            display_name: "Reviewer".to_string(),
            description: "Validates the work".to_string(),
        },
    ];

    WorkflowDefinition {
        roles,
        states,
        configuration: vec![
            WorkflowConfigField {
                id: "review_retries".to_string(),
                label: "Review retries".to_string(),
                description: Some(
                    "Rejected review cycles allowed before the task is blocked.".to_string(),
                ),
                value_type: WorkflowConfigValueType::Integer,
                min: Some(0),
                default_value: Some(json!(2)),
                binding: WorkflowConfigBinding::GateConfig {
                    state: default_states::REVIEW.to_string(),
                    field: "max_rejections".to_string(),
                },
            },
            WorkflowConfigField {
                id: "merge_fix_retries".to_string(),
                label: "Merge-fix retries".to_string(),
                description: Some(
                    "Merge-conflict fix attempts allowed before the task is blocked.".to_string(),
                ),
                value_type: WorkflowConfigValueType::Integer,
                min: Some(0),
                default_value: Some(json!(1)),
                binding: WorkflowConfigBinding::GateConfig {
                    state: default_states::MERGING.to_string(),
                    field: "max_rejections".to_string(),
                },
            },
            WorkflowConfigField {
                id: "planner_prompt_instructions".to_string(),
                label: "Planner prompt instructions".to_string(),
                description: Some(
                    "Extra instructions appended to planner dispatch prompts.".to_string(),
                ),
                value_type: WorkflowConfigValueType::Text,
                min: None,
                default_value: Some(json!("")),
                binding: WorkflowConfigBinding::StateConfig {
                    state: default_states::PLANNING.to_string(),
                    path: vec!["prompt".to_string(), "user_append".to_string()],
                },
            },
            WorkflowConfigField {
                id: "coder_prompt_instructions".to_string(),
                label: "Coder prompt instructions".to_string(),
                description: Some(
                    "Extra instructions appended to coder dispatch prompts.".to_string(),
                ),
                value_type: WorkflowConfigValueType::Text,
                min: None,
                default_value: Some(json!("")),
                binding: WorkflowConfigBinding::StateConfig {
                    state: default_states::IN_PROGRESS.to_string(),
                    path: vec!["prompt".to_string(), "user_append".to_string()],
                },
            },
            WorkflowConfigField {
                id: "reviewer_prompt_instructions".to_string(),
                label: "Reviewer prompt instructions".to_string(),
                description: Some(
                    "Extra instructions appended to reviewer dispatch prompts.".to_string(),
                ),
                value_type: WorkflowConfigValueType::Text,
                min: None,
                default_value: Some(json!("")),
                binding: WorkflowConfigBinding::StateConfig {
                    state: default_states::REVIEW.to_string(),
                    path: vec!["prompt".to_string(), "user_append".to_string()],
                },
            },
            WorkflowConfigField {
                id: "merge_fix_prompt_instructions".to_string(),
                label: "Merge-fix prompt instructions".to_string(),
                description: Some(
                    "Extra instructions appended to merge-conflict fix prompts.".to_string(),
                ),
                value_type: WorkflowConfigValueType::Text,
                min: None,
                default_value: Some(json!("")),
                binding: WorkflowConfigBinding::StateConfig {
                    state: default_states::MERGE_FAILED.to_string(),
                    path: vec!["prompt".to_string(), "user_append".to_string()],
                },
            },
        ],
        cancellation_state: Some(default_states::CANCELLED.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_failed_completion_re_reviews_before_merge() {
        let workflow = default_workflow();

        assert_eq!(
            workflow.auto_transition_target(default_states::MERGE_FAILED),
            Some(default_states::REVIEW)
        );
        assert_eq!(
            workflow.gate_reject_target(default_states::MERGING),
            Some(default_states::MERGE_FAILED)
        );
        assert_eq!(
            workflow
                .outgoing_trigger_targets(default_states::MERGE_FAILED)
                .find(|(trigger, _)| *trigger == WorkflowTrigger::Retry)
                .map(|(_, target)| target),
            Some(default_states::IN_PROGRESS.to_string())
        );
    }

    #[test]
    fn every_default_state_has_its_decided_canonical_phase() {
        let workflow = default_workflow();
        let expected = [
            (default_states::BACKLOG, CanonicalPhase::Backlog),
            (default_states::TODO, CanonicalPhase::Ready),
            (default_states::PLANNING, CanonicalPhase::Working),
            (default_states::IN_PROGRESS, CanonicalPhase::Working),
            (default_states::REVIEW, CanonicalPhase::Review),
            (default_states::MERGING, CanonicalPhase::Review),
            (default_states::MERGE_FAILED, CanonicalPhase::Review),
            (default_states::DONE, CanonicalPhase::Done),
            (default_states::CANCELLED, CanonicalPhase::Done),
        ];

        for (state, phase) in expected {
            assert_eq!(workflow.canonical_phase_for_state(state), phase);
            assert_eq!(
                workflow
                    .states
                    .iter()
                    .find(|definition| definition.name == state)
                    .and_then(|definition| definition.canonical_phase),
                Some(phase),
                "default state {state} must set canonical_phase explicitly"
            );
        }
    }
}
