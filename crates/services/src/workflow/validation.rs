use std::collections::{HashMap, HashSet, VecDeque};

use api_types::{
    StateHooks, StateKind, WorkflowConfigBinding, WorkflowDefinition, WorkflowExecutionPolicy,
    WorkflowTrigger,
};

use crate::{
    workflow::{default_roles, registry},
    ServiceError,
};

pub fn validate_workflow(def: &WorkflowDefinition) -> Result<(), ServiceError> {
    let initial_states = def
        .states
        .iter()
        .filter(|state| state.kind == StateKind::Initial)
        .count();
    if initial_states != 1 {
        return Err(ServiceError::InvalidOperation {
            message: "workflow must have exactly one initial state".to_string(),
        });
    }

    let has_terminal = def
        .states
        .iter()
        .any(|state| state.kind == StateKind::Terminal || state.name == "cancelled");
    if !has_terminal {
        return Err(ServiceError::InvalidOperation {
            message: "workflow must have at least one terminal state".to_string(),
        });
    }
    let mut states = HashMap::new();
    for state in &def.states {
        if states.insert(state.name.clone(), state.kind).is_some() {
            return Err(ServiceError::InvalidOperation {
                message: format!("duplicate workflow state '{}'", state.name),
            });
        }
    }

    if let Some(cancellation_state) = def.cancellation_state.as_ref() {
        match states.get(cancellation_state) {
            Some(StateKind::Terminal) => {}
            Some(_) => {
                return Err(ServiceError::InvalidOperation {
                    message: format!(
                        "cancellation_state '{}' must be a terminal state",
                        cancellation_state
                    ),
                });
            }
            None => {
                return Err(ServiceError::InvalidOperation {
                    message: format!("cancellation_state '{}' is not defined", cancellation_state),
                });
            }
        }
    }

    for state in &def.states {
        if state.canonical_phase.is_none() {
            return Err(ServiceError::InvalidOperation {
                message: format!(
                    "state '{}' must define canonical_phase when saving a workflow",
                    state.name
                ),
            });
        }
        validate_hooks(&state.name, &state.hooks)?;
        // "assignee" is reserved as the implicit role for active states; no role-declaration check is enforced (engine is role-name-agnostic by design).
        if state.role.as_deref() == Some(default_roles::ASSIGNEE) && state.kind != StateKind::Active
        {
            return Err(ServiceError::InvalidOperation {
                message: format!(
                    "state '{}' uses reserved role 'assignee' but is not active",
                    state.name
                ),
            });
        }
        if state.kind == StateKind::Terminal {
            let has_outbound = !state.triggers.is_empty();
            if has_outbound {
                return Err(ServiceError::InvalidOperation {
                    message: format!(
                        "terminal state '{}' must not have outbound transitions",
                        state.name
                    ),
                });
            }
        }
        if state.kind == StateKind::Gate {
            if let Some(reject_target) = state
                .gate_config
                .as_ref()
                .and_then(|config| config.reject_target.as_ref())
            {
                if !states.contains_key(reject_target) {
                    return Err(ServiceError::InvalidOperation {
                        message: format!(
                            "gate state '{}' has invalid reject_target '{}'",
                            state.name, reject_target
                        ),
                    });
                }
            }
        }
        for (trigger, trigger_def) in &state.triggers {
            if !matches!(
                trigger,
                WorkflowTrigger::Accept
                    | WorkflowTrigger::Reject
                    | WorkflowTrigger::Fail
                    | WorkflowTrigger::Retry
            ) {
                return Err(ServiceError::InvalidOperation {
                    message: format!(
                        "state '{}' has unsupported trigger '{trigger:?}'",
                        state.name
                    ),
                });
            }
            if !states.contains_key(&trigger_def.to) {
                return Err(ServiceError::InvalidOperation {
                    message: format!(
                        "state '{}' trigger '{trigger:?}' has unknown target '{}'",
                        state.name, trigger_def.to
                    ),
                });
            }
            if let Some(dispatch) = trigger_def.dispatch.as_ref() {
                validate_dispatch(
                    dispatch.builder.as_deref(),
                    dispatch.execution_policy,
                    format!("state '{}' trigger '{trigger:?}'", state.name),
                )?;
            }
        }
        if let Some(dispatch) = state.dispatch.as_ref() {
            validate_dispatch(
                dispatch.builder.as_deref(),
                dispatch.execution_policy,
                format!("state '{}'", state.name),
            )?;
        }
    }

    let mut config_ids = HashSet::new();
    for field in &def.configuration {
        if field.id.trim().is_empty() {
            return Err(ServiceError::InvalidOperation {
                message: "workflow configuration field id must not be empty".to_string(),
            });
        }
        if !config_ids.insert(field.id.as_str()) {
            return Err(ServiceError::InvalidOperation {
                message: format!("duplicate workflow configuration field '{}'", field.id),
            });
        }
        if field.label.trim().is_empty() {
            return Err(ServiceError::InvalidOperation {
                message: format!(
                    "workflow configuration field '{}' must have a label",
                    field.id
                ),
            });
        }
        if field.min.is_some_and(|min| min < 0) {
            return Err(ServiceError::InvalidOperation {
                message: format!(
                    "workflow configuration field '{}' min must be 0 or greater",
                    field.id
                ),
            });
        }
        match field.value_type {
            api_types::WorkflowConfigValueType::Integer => {
                if let Some(default_value) = &field.default_value {
                    let Some(default_number) = default_value.as_i64() else {
                        return Err(ServiceError::InvalidOperation {
                            message: format!(
                                "workflow configuration field '{}' default_value must be an integer",
                                field.id
                            ),
                        });
                    };
                    if let Some(min) = field.min {
                        if default_number < i64::from(min) {
                            return Err(ServiceError::InvalidOperation {
                                message: format!(
                                    "workflow configuration field '{}' default_value must be greater than or equal to min",
                                    field.id
                                ),
                            });
                        }
                    }
                }
            }
            api_types::WorkflowConfigValueType::Text => {
                if field.min.is_some() {
                    return Err(ServiceError::InvalidOperation {
                        message: format!(
                            "workflow configuration field '{}' min is only supported for integer fields",
                            field.id
                        ),
                    });
                }
                if field
                    .default_value
                    .as_ref()
                    .is_some_and(|default_value| !default_value.is_string())
                {
                    return Err(ServiceError::InvalidOperation {
                        message: format!(
                            "workflow configuration field '{}' default_value must be text",
                            field.id
                        ),
                    });
                }
            }
        }
        match &field.binding {
            WorkflowConfigBinding::GateConfig {
                state,
                field: binding_field,
            } => {
                let Some(kind) = states.get(state) else {
                    return Err(ServiceError::InvalidOperation {
                        message: format!(
                            "workflow configuration field '{}' references unknown state '{}'",
                            field.id, state
                        ),
                    });
                };
                if *kind != StateKind::Gate {
                    return Err(ServiceError::InvalidOperation {
                        message: format!(
                            "workflow configuration field '{}' gate_config binding references non-gate state '{}'",
                            field.id, state
                        ),
                    });
                }
                if binding_field != "max_rejections" {
                    return Err(ServiceError::InvalidOperation {
                        message: format!(
                            "workflow configuration field '{}' references unsupported gate_config field '{}'",
                            field.id, binding_field
                        ),
                    });
                }
            }
            WorkflowConfigBinding::StateConfig { state, path } => {
                if !states.contains_key(state) {
                    return Err(ServiceError::InvalidOperation {
                        message: format!(
                            "workflow configuration field '{}' references unknown state '{}'",
                            field.id, state
                        ),
                    });
                }
                if path.is_empty() || path.iter().any(|segment| segment.trim().is_empty()) {
                    return Err(ServiceError::InvalidOperation {
                        message: format!(
                            "workflow configuration field '{}' state_config path must not be empty",
                            field.id
                        ),
                    });
                }
            }
        }
    }

    let has_active = def
        .states
        .iter()
        .any(|state| state.kind == StateKind::Active);
    if !has_active {
        return Err(ServiceError::InvalidOperation {
            message: "workflow must have at least one active executor state".to_string(),
        });
    }

    let initial_state = def
        .states
        .iter()
        .find(|state| state.kind == StateKind::Initial)
        .ok_or_else(|| ServiceError::InvalidOperation {
            message: "workflow must have exactly one initial state".to_string(),
        })?;

    let mut adjacency: HashMap<String, Vec<String>> = HashMap::new();
    for state in &def.states {
        for (_, to) in def.outgoing_trigger_targets(&state.name) {
            adjacency.entry(state.name.clone()).or_default().push(to);
        }
    }

    let mut reachable: HashSet<String> = HashSet::new();
    let mut queue = VecDeque::new();
    queue.push_back(initial_state.name.clone());

    while let Some(state) = queue.pop_front() {
        if !reachable.insert(state.clone()) {
            continue;
        }
        if let Some(next_states) = adjacency.get(&state) {
            for next in next_states {
                if !reachable.contains(next) {
                    queue.push_back(next.clone());
                }
            }
        }
    }

    let orphans: Vec<String> = def
        .states
        .iter()
        .filter(|state| state.kind != StateKind::Initial)
        .map(|state| state.name.clone())
        .filter(|name| !reachable.contains(name))
        .collect();

    if !orphans.is_empty() {
        return Err(ServiceError::InvalidOperation {
            message: format!("workflow contains orphan states: {}", orphans.join(", ")),
        });
    }

    Ok(())
}

fn validate_dispatch(
    builder: Option<&str>,
    execution_policy: Option<WorkflowExecutionPolicy>,
    context: String,
) -> Result<(), ServiceError> {
    if let Some(policy) = execution_policy {
        match policy {
            WorkflowExecutionPolicy::NewExecution
            | WorkflowExecutionPolicy::ResumeLatestTargetRoleThread => {}
        }
    }
    if let Some(builder_id) = builder {
        if !is_known_prompt_builder_id(builder_id) {
            return Err(ServiceError::InvalidOperation {
                message: format!("{context} references unknown prompt builder '{builder_id}'"),
            });
        }
    }
    Ok(())
}

fn is_known_prompt_builder_id(builder_id: &str) -> bool {
    matches!(
        builder_id,
        "generic.default.v2"
            | "planner.default.v2"
            | "coder.implementation.v2"
            | "coder.review_fix.v2"
            | "coder.merge_fix.v2"
            | "worker.autonomous.v1"
            | "worker.review_fix.v1"
            | "worker.merge_fix.v1"
            | "reviewer.conformance.v1"
    )
}

fn validate_hooks(state_name: &str, hooks: &StateHooks) -> Result<(), ServiceError> {
    for (phase, hook) in hooks
        .before_exit
        .iter()
        .map(|hook| ("before_exit", hook))
        .chain(hooks.on_exit.iter().map(|hook| ("on_exit", hook)))
        .chain(hooks.on_enter.iter().map(|hook| ("on_enter", hook)))
        .chain(hooks.after_enter.iter().map(|hook| ("after_enter", hook)))
    {
        if !registry::is_known_action(&hook.action) {
            return Err(ServiceError::InvalidOperation {
                message: format!(
                    "state '{state_name}' has unknown {phase} hook action '{}'",
                    hook.action
                ),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use api_types::{
        CanonicalPhase, GateConfig, HookSpec, StateDefinition, StateHooks, StateKind,
        WorkflowConfigBinding, WorkflowConfigField, WorkflowConfigValueType, WorkflowDefinition,
        WorkflowDispatch, WorkflowExecutionPolicy, WorkflowTrigger, WorkflowTriggerDefinition,
    };
    use serde_json::json;

    use super::validate_workflow;
    use crate::{
        workflow::{default_roles, default_workflow::default_workflow},
        ServiceError,
    };

    fn state(name: &str, kind: StateKind, role: Option<&str>) -> StateDefinition {
        StateDefinition {
            name: name.to_owned(),
            kind,
            column: name.to_owned(),
            display_name: name.to_owned(),
            role: role.map(str::to_owned),
            hooks: StateHooks::default(),
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

    #[test]
    fn validation_rejects_unknown_trigger_target() {
        let mut initial = state("todo", StateKind::Initial, None);
        initial.triggers.insert(
            WorkflowTrigger::Accept,
            WorkflowTriggerDefinition {
                to: "missing".to_string(),
                dispatch: None,
            },
        );
        let workflow = WorkflowDefinition {
            roles: Vec::new(),
            states: vec![
                initial,
                state(
                    "in_progress",
                    StateKind::Active,
                    Some(default_roles::ASSIGNEE),
                ),
                state("done", StateKind::Terminal, None),
            ],
            configuration: Vec::new(),
            cancellation_state: None,
        };

        match validate_workflow(&workflow) {
            Err(ServiceError::InvalidOperation { message }) => {
                assert!(message.contains("unknown target"));
                assert!(message.contains("missing"));
            }
            other => panic!("expected invalid operation, got {other:?}"),
        }
    }

    #[test]
    fn validation_rejects_unknown_prompt_builder() {
        let mut review = state("review", StateKind::Gate, Some(default_roles::REVIEWER));
        review.triggers.insert(
            WorkflowTrigger::Reject,
            WorkflowTriggerDefinition {
                to: "in_progress".to_string(),
                dispatch: Some(WorkflowDispatch {
                    builder: Some("coder.unknown.v1".to_string()),
                    execution_policy: Some(WorkflowExecutionPolicy::NewExecution),
                    prompt: None,
                }),
            },
        );
        let workflow = WorkflowDefinition {
            roles: Vec::new(),
            states: vec![
                state("todo", StateKind::Initial, None),
                state(
                    "in_progress",
                    StateKind::Active,
                    Some(default_roles::ASSIGNEE),
                ),
                review,
                state("done", StateKind::Terminal, None),
            ],
            configuration: Vec::new(),
            cancellation_state: None,
        };

        match validate_workflow(&workflow) {
            Err(ServiceError::InvalidOperation { message }) => {
                assert!(message.contains("review"));
                assert!(message.contains("unknown prompt builder"));
            }
            other => panic!("expected invalid operation, got {other:?}"),
        }
    }

    #[test]
    fn validation_uses_implicit_accept_edges_for_reachability() {
        let workflow = WorkflowDefinition {
            roles: Vec::new(),
            states: vec![
                state("todo", StateKind::Initial, None),
                state(
                    "in_progress",
                    StateKind::Active,
                    Some(default_roles::ASSIGNEE),
                ),
                state("done", StateKind::Terminal, None),
            ],
            configuration: Vec::new(),
            cancellation_state: None,
        };

        validate_workflow(&workflow).expect("implicit accept edges make the sequence reachable");
    }

    #[test]
    fn validation_rejects_duplicate_state_names() {
        let workflow = WorkflowDefinition {
            roles: Vec::new(),
            states: vec![
                state("todo", StateKind::Initial, None),
                state("todo", StateKind::Active, Some(default_roles::ASSIGNEE)),
                state("done", StateKind::Terminal, None),
            ],
            configuration: Vec::new(),
            cancellation_state: None,
        };

        match validate_workflow(&workflow) {
            Err(ServiceError::InvalidOperation { message }) => {
                assert!(message.contains("duplicate workflow state"));
                assert!(message.contains("todo"));
            }
            other => panic!("expected invalid operation, got {other:?}"),
        }
    }

    #[test]
    fn validation_rejects_unknown_hook_action() {
        let mut active = state(
            "in_progress",
            StateKind::Active,
            Some(default_roles::ASSIGNEE),
        );
        active.hooks.on_enter.push(HookSpec {
            action: "missing_action".to_owned(),
            params: serde_json::json!({}),
            applies_to: api_types::HookAudience::All,
            on_failure: api_types::FailurePolicy::Log,
        });
        let workflow = WorkflowDefinition {
            roles: Vec::new(),
            states: vec![
                state("todo", StateKind::Initial, None),
                active,
                state("done", StateKind::Terminal, None),
            ],
            configuration: Vec::new(),
            cancellation_state: None,
        };

        match validate_workflow(&workflow) {
            Err(ServiceError::InvalidOperation { message }) => {
                assert!(message.contains("unknown on_enter hook action"));
                assert!(message.contains("missing_action"));
            }
            other => panic!("expected invalid operation, got {other:?}"),
        }
    }

    #[test]
    fn validation_rejects_workflow_without_active_state() {
        let workflow = WorkflowDefinition {
            roles: Vec::new(),
            states: vec![
                state("todo", StateKind::Initial, None),
                state("done", StateKind::Terminal, None),
            ],
            configuration: Vec::new(),
            cancellation_state: None,
        };

        match validate_workflow(&workflow) {
            Err(ServiceError::InvalidOperation { message }) => {
                assert!(message.contains("active executor state"));
            }
            other => panic!("expected invalid operation, got {other:?}"),
        }
    }

    #[test]
    fn validation_rejects_configuration_reference_to_unknown_state() {
        let workflow = WorkflowDefinition {
            roles: Vec::new(),
            states: vec![
                state("todo", StateKind::Initial, None),
                state(
                    "in_progress",
                    StateKind::Active,
                    Some(default_roles::ASSIGNEE),
                ),
                state("done", StateKind::Terminal, None),
            ],
            configuration: vec![WorkflowConfigField {
                id: "missing_state_knob".to_owned(),
                label: "Missing state knob".to_owned(),
                description: None,
                value_type: WorkflowConfigValueType::Integer,
                min: Some(0),
                default_value: Some(serde_json::json!(1)),
                binding: WorkflowConfigBinding::StateConfig {
                    state: "missing".to_owned(),
                    path: vec!["retry_budgets".to_owned(), "review".to_owned()],
                },
            }],
            cancellation_state: None,
        };

        match validate_workflow(&workflow) {
            Err(ServiceError::InvalidOperation { message }) => {
                assert!(message.contains("missing_state_knob"));
                assert!(message.contains("unknown state"));
            }
            other => panic!("expected invalid operation, got {other:?}"),
        }
    }

    #[test]
    fn validation_rejects_invalid_gate_reject_target() {
        let mut review = state("review", StateKind::Gate, None);
        review.gate_config = Some(GateConfig {
            reject_target: Some("missing".to_owned()),
            max_rejections: Some(2),
            approve_label: None,
            reject_label: None,
            requires_user_approval: Some(false),
            optional_when_unassigned: Some(false),
        });
        let workflow = WorkflowDefinition {
            roles: Vec::new(),
            states: vec![
                state("todo", StateKind::Initial, None),
                review,
                state("done", StateKind::Terminal, None),
            ],
            configuration: vec![WorkflowConfigField {
                id: "bad_gate_field".to_owned(),
                label: "Bad gate field".to_owned(),
                description: None,
                value_type: WorkflowConfigValueType::Integer,
                min: Some(0),
                default_value: Some(serde_json::json!(1)),
                binding: WorkflowConfigBinding::GateConfig {
                    state: "review".to_owned(),
                    field: "max_rejections".to_owned(),
                },
            }],
            cancellation_state: None,
        };

        match validate_workflow(&workflow) {
            Err(ServiceError::InvalidOperation { message }) => {
                assert!(message.contains("reject_target"));
                assert!(message.contains("missing"));
            }
            other => panic!("expected invalid operation, got {other:?}"),
        }
    }

    #[test]
    fn validation_accepts_default_workflow() {
        validate_workflow(&default_workflow()).expect("default workflow should validate");
    }

    #[test]
    fn validation_rejects_state_without_explicit_canonical_phase() {
        let mut workflow = WorkflowDefinition {
            roles: Vec::new(),
            states: vec![
                state("todo", StateKind::Initial, None),
                state(
                    "in_progress",
                    StateKind::Active,
                    Some(default_roles::ASSIGNEE),
                ),
                state("done", StateKind::Terminal, None),
            ],
            configuration: Vec::new(),
            cancellation_state: None,
        };
        workflow.states[1].canonical_phase = None;

        match validate_workflow(&workflow) {
            Err(ServiceError::InvalidOperation { message }) => {
                assert!(message.contains("canonical_phase"));
                assert!(message.contains("in_progress"));
            }
            other => panic!("expected missing canonical phase error, got {other:?}"),
        }
    }
}
