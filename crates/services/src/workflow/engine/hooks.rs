use std::time::Instant;

use api_types::{Actor, FailurePolicy, HookAudience, HookSpec, StateDefinition, StateKind};
use serde_json::json;
use serde_json::Value;

use crate::workflow::HookResult;

pub(super) fn merged_state_config(
    state: &StateDefinition,
    project: Option<&db::Project>,
    task_state_config_json: Option<&str>,
) -> Value {
    let mut merged = state.config.clone();
    if state.name == crate::workflow::default_states::REVIEW {
        merge_project_review_config(&mut merged, project);
    }
    if state.name == crate::workflow::default_states::MERGING {
        merge_project_merge_fix_budget(&mut merged, project);
    }

    let Some(task_state_config_json) = task_state_config_json else {
        return merged;
    };
    let Ok(Value::Object(task_config)) = serde_json::from_str::<Value>(task_state_config_json)
    else {
        return merged;
    };
    let Some(Value::Object(overrides)) = task_config.get(&state.name) else {
        return merged;
    };

    match &mut merged {
        Value::Object(defaults) => {
            for (key, value) in overrides {
                defaults.insert(key.clone(), value.clone());
            }
            merged
        }
        _ => Value::Object(overrides.clone()),
    }
}

fn merge_project_merge_fix_budget(merged: &mut Value, project: Option<&db::Project>) {
    let Some(project) = project else {
        return;
    };
    let Ok(settings) = serde_json::from_str::<Value>(&project.settings) else {
        return;
    };
    let Some(merge_fix) = settings
        .get("retry_budgets")
        .and_then(|b| b.get("merge_fix"))
        .cloned()
    else {
        return;
    };
    match merged {
        Value::Object(obj) => {
            let budgets = obj.entry("retry_budgets").or_insert_with(|| json!({}));
            if let Value::Object(b) = budgets {
                b.insert("merge_fix".to_string(), merge_fix);
            }
        }
        _ => {
            *merged = json!({ "retry_budgets": { "merge_fix": merge_fix } });
        }
    }
}

fn merge_project_review_config(merged: &mut Value, project: Option<&db::Project>) {
    let Some(project) = project else {
        return;
    };
    let Ok(settings) = serde_json::from_str::<Value>(&project.settings) else {
        return;
    };
    let Some(Value::Object(review_config)) = settings.get("default_review_config") else {
        return;
    };
    match merged {
        Value::Object(defaults) => {
            for (key, value) in review_config {
                defaults.entry(key.clone()).or_insert_with(|| value.clone());
            }
        }
        _ => {
            *merged = Value::Object(review_config.clone());
        }
    }
}

pub(super) fn hook_audience_matches(audience: HookAudience, actor: &Actor) -> bool {
    match audience {
        HookAudience::All => true,
        // System transitions run the autonomous workflow just like agent
        // transitions. This includes component-specific system actors.
        HookAudience::AgentOnly => actor.is_agent() || actor.is_system(),
        HookAudience::UserOnly => actor.is_user(),
    }
}

pub(super) fn effective_after_enter_hooks(to_state: &StateDefinition) -> Vec<HookSpec> {
    let should_attach_retry_budget = to_state.kind == StateKind::Gate
        && to_state
            .gate_config
            .as_ref()
            .and_then(|config| config.max_rejections)
            .is_some()
        && !to_state
            .hooks
            .after_enter
            .iter()
            .any(|hook| hook.action == "check_retry_budget");

    let mut hooks = Vec::with_capacity(
        to_state.hooks.after_enter.len() + usize::from(should_attach_retry_budget),
    );

    if should_attach_retry_budget {
        hooks.push(HookSpec {
            action: "check_retry_budget".to_string(),
            params: Value::Object(Default::default()),
            applies_to: HookAudience::All,
            on_failure: FailurePolicy::Log,
        });
    }

    hooks.extend(to_state.hooks.after_enter.iter().cloned());
    hooks
}

pub(super) fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().try_into().unwrap_or(u64::MAX)
}

pub(super) fn log_hook_skipped_by_audience(
    task_id: &str,
    from_state: &str,
    to_state: &str,
    phase: &str,
    hook: &HookSpec,
    actor: &Actor,
) {
    tracing::debug!(
        task_id = %task_id,
        from_state = %from_state,
        to_state = %to_state,
        phase = %phase,
        action = %hook.action,
        audience = ?hook.applies_to,
        actor = %actor,
        "workflow hook skipped because triggered_by does not match audience"
    );
}

pub(super) fn log_hook_start(
    task_id: &str,
    from_state: &str,
    to_state: &str,
    phase: &str,
    hook: &HookSpec,
    actor: &Actor,
) {
    tracing::debug!(
        task_id = %task_id,
        from_state = %from_state,
        to_state = %to_state,
        phase = %phase,
        action = %hook.action,
        audience = ?hook.applies_to,
        on_failure = ?hook.on_failure,
        actor = %actor,
        "workflow hook executing"
    );
}

pub(super) fn log_hook_result(
    task_id: &str,
    from_state: &str,
    to_state: &str,
    phase: &str,
    hook: &HookSpec,
    result: &HookResult,
    duration_ms: u64,
) {
    match result {
        HookResult::Ok => {
            tracing::debug!(
                task_id = %task_id,
                from_state = %from_state,
                to_state = %to_state,
                phase = %phase,
                action = %hook.action,
                duration_ms = duration_ms,
                "workflow hook completed"
            );
        }
        HookResult::Skipped { reason } => {
            tracing::debug!(
                task_id = %task_id,
                from_state = %from_state,
                to_state = %to_state,
                phase = %phase,
                action = %hook.action,
                duration_ms = duration_ms,
                reason = %reason,
                "workflow hook skipped"
            );
        }
        HookResult::Failed { reason } => {
            tracing::warn!(
                task_id = %task_id,
                from_state = %from_state,
                to_state = %to_state,
                phase = %phase,
                action = %hook.action,
                duration_ms = duration_ms,
                reason = %reason,
                on_failure = ?hook.on_failure,
                "workflow hook failed"
            );
        }
        HookResult::Cascade { to, reason } => {
            tracing::info!(
                task_id = %task_id,
                from_state = %from_state,
                to_state = %to_state,
                phase = %phase,
                action = %hook.action,
                duration_ms = duration_ms,
                cascade_to = %to,
                cascade_reason = %reason,
                "workflow hook completed with cascade"
            );
        }
    }
}

#[cfg(test)]
mod audience_tests {
    use super::hook_audience_matches;
    use api_types::{Actor, HookAudience, SystemComponent, UserActionSource};

    #[test]
    fn hook_audience_matrix_is_exhaustive_for_actor_kinds() {
        let actors = [
            Actor::user(UserActionSource::Test),
            Actor::agent("worker"),
            Actor::system(SystemComponent::TaskDispatcher),
        ];

        for actor in &actors {
            assert!(hook_audience_matches(HookAudience::All, actor));
        }
        assert!(!hook_audience_matches(HookAudience::AgentOnly, &actors[0]));
        assert!(hook_audience_matches(HookAudience::AgentOnly, &actors[1]));
        assert!(hook_audience_matches(HookAudience::AgentOnly, &actors[2]));
        assert!(hook_audience_matches(HookAudience::UserOnly, &actors[0]));
        assert!(!hook_audience_matches(HookAudience::UserOnly, &actors[1]));
        assert!(!hook_audience_matches(HookAudience::UserOnly, &actors[2]));
    }
}

pub(super) fn hook_result_entry(
    action: &str,
    phase: &str,
    result: &HookResult,
    duration_ms: u64,
) -> api_types::HookResultEntry {
    let (outcome, error) = match result {
        HookResult::Ok => ("ok".to_string(), None),
        HookResult::Skipped { reason } => ("skipped".to_string(), Some(reason.clone())),
        HookResult::Failed { reason } => ("failed".to_string(), Some(reason.clone())),
        HookResult::Cascade { to, reason } => {
            ("cascade".to_string(), Some(format!("{} -> {}", reason, to)))
        }
    };

    api_types::HookResultEntry {
        action: action.to_string(),
        phase: phase.to_string(),
        outcome,
        duration_ms: Some(duration_ms),
        error,
    }
}

#[cfg(test)]
mod tests {
    use api_types::{CanonicalPhase, StateDefinition, StateHooks, StateKind};
    use serde_json::json;

    use super::merged_state_config;

    fn review_state() -> StateDefinition {
        StateDefinition {
            name: crate::workflow::default_states::REVIEW.to_owned(),
            kind: StateKind::Gate,
            column: "Review".to_owned(),
            display_name: "Review".to_owned(),
            role: Some(crate::workflow::default_roles::REVIEWER.to_owned()),
            hooks: StateHooks::default(),
            cleanup: None,
            canonical_phase: Some(CanonicalPhase::Review),
            gate_config: None,
            dispatch: None,
            triggers: Default::default(),
            config: json!({ "prompt": { "user_append": "" } }),
        }
    }

    fn project(settings: serde_json::Value) -> db::Project {
        db::Project {
            id: "project".to_owned(),
            name: "Project".to_owned(),
            settings: settings.to_string(),
            workflow_definition: "{}".to_owned(),
            workflow_template_name: None,
            primary_repo_id: None,
            paused_at: None,
            system_pause_reason: None,
            owner_id: None,
            project_hooks_json: "[]".to_owned(),
            project_work_epoch: 0,
            charter_status: "legacy_unverified".to_owned(),
            charter_setup_required: true,
            current_charter_id: None,
            current_charter_revision_id: None,
            current_charter_version: 0,
            primary_milestone_id: None,
            version: 1,
            created_at: "now".to_owned(),
            updated_at: "now".to_owned(),
        }
    }

    #[test]
    fn review_state_inherits_project_default_review_config() {
        let state = review_state();
        let project = project(json!({
            "default_review_config": {
                "ci_steps": ["./scripts/ci.sh"],
                "review_prompt": null
            }
        }));

        let config = merged_state_config(&state, Some(&project), None);

        assert_eq!(config["ci_steps"], json!(["./scripts/ci.sh"]));
        assert_eq!(config["review_prompt"], json!(null));
        assert_eq!(config["prompt"]["user_append"], json!(""));
    }

    #[test]
    fn task_review_object_overrides_project_default_review_config() {
        let state = review_state();
        let project = project(json!({
            "default_review_config": {
                "ci_steps": ["./scripts/ci.sh"],
                "review_prompt": null
            }
        }));
        let task_config = json!({
            "review": {
                "ci_steps": [],
                "review_prompt": "task review only"
            }
        })
        .to_string();

        let config = merged_state_config(&state, Some(&project), Some(&task_config));

        assert_eq!(config["ci_steps"], json!([]));
        assert_eq!(config["review_prompt"], json!("task review only"));
    }

    #[test]
    fn null_task_review_config_does_not_disable_project_default_review_config() {
        let state = review_state();
        let project = project(json!({
            "default_review_config": {
                "ci_steps": ["./scripts/ci.sh"],
                "review_prompt": null
            }
        }));
        let task_config = json!({ "review": null }).to_string();

        let config = merged_state_config(&state, Some(&project), Some(&task_config));

        assert_eq!(config["ci_steps"], json!(["./scripts/ci.sh"]));
    }
}
