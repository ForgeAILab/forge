use std::sync::Arc;

use api_types::{StateDefinition, WorkflowDefinition, WorkflowTrigger, WorkflowTriggerDefinition};
use db::{ProjectRepo, TaskRepo};
use serde_json::{json, Value};

use crate::{
    workflow::{
        default_states,
        dispatch::{
            build_effective_prompt, dispatch_intent_from_workflow_dispatch,
            effective_prompt_selection, loader::load_agent_dispatch_context, AgentPrompt,
            DispatchIntent,
        },
        engine::WorkflowEngine,
    },
    Result, ServiceError,
};

pub async fn preview_effective_prompt(
    db: Arc<db::SqliteDb>,
    task_id: &str,
    role: &str,
    trigger: Option<WorkflowTrigger>,
) -> Result<AgentPrompt> {
    let task = TaskRepo::get_by_id(&*db, task_id, false)
        .await?
        .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()))?;
    let project = ProjectRepo::get_by_id(&*db, &task.project_id)
        .await?
        .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
    let workflow = WorkflowEngine::resolve_workflow_for_task(
        &task,
        &project.workflow_definition,
        &api_types::Actor::system(api_types::SystemComponent::General),
    );
    ensure_known_role(&workflow, role)?;

    let (preview_state, trigger_dispatch) =
        preview_state_and_trigger_dispatch(&workflow, &task.status, trigger)?;
    let state_config =
        merged_state_config(preview_state, &project, task.task_state_config.as_deref());
    let state_dispatch = dispatch_intent_from_workflow_dispatch(preview_state.dispatch.as_ref());
    let selection =
        effective_prompt_selection(role, trigger_dispatch.as_ref(), state_dispatch.as_ref());
    let dispatch_ctx = load_agent_dispatch_context(
        Arc::clone(&db),
        task_id,
        role,
        &preview_state.name,
        state_config,
        Some(selection.execution_policy.as_str()),
        &workflow,
    )
    .await?;
    let (mut prompt, selection) = build_effective_prompt(
        &dispatch_ctx,
        trigger_dispatch.as_ref(),
        state_dispatch.as_ref(),
    );
    let context = ::review::contract::load_context(&db, task_id)
        .await
        .map_err(ServiceError::invalid_operation)?;
    prompt
        .user
        .push_str(&::review::contract::governing_prompt(&context));
    if role == crate::workflow::default_roles::REVIEWER
        || selection.builder_id == crate::workflow::dispatch::BUILDER_ID_REVIEWER_CONFORMANCE_V1
    {
        prompt.system.push_str("\n\n");
        prompt
            .system
            .push_str(::review::contract::RESPONSE_INSTRUCTION);
        prompt.user.push_str(
            "\nPreview note: Forge freezes the candidate commit and contract digest when the reviewer execution starts.\n",
        );
    }
    Ok(prompt)
}

fn ensure_known_role(workflow: &WorkflowDefinition, role: &str) -> Result<()> {
    if workflow
        .roles
        .iter()
        .any(|definition| definition.name == role)
    {
        return Ok(());
    }
    Err(ServiceError::InvalidOperation {
        message: format!("unknown role: {role}"),
    })
}

fn preview_state_and_trigger_dispatch<'a>(
    workflow: &'a WorkflowDefinition,
    current_state_name: &str,
    trigger: Option<WorkflowTrigger>,
) -> Result<(&'a StateDefinition, Option<DispatchIntent>)> {
    let current_state = workflow
        .states
        .iter()
        .find(|state| state.name == current_state_name)
        .ok_or_else(|| ServiceError::InvalidOperation {
            message: WorkflowEngine::undefined_state_message(current_state_name, workflow),
        })?;

    let Some(trigger) = trigger else {
        return Ok((current_state, None));
    };

    if let Some(definition) = current_state.triggers.get(&trigger) {
        let target_state = workflow_state(workflow, &definition.to)?;
        let trigger_dispatch = dispatch_from_trigger(definition);
        return Ok((target_state, trigger_dispatch));
    }

    if trigger == WorkflowTrigger::Accept {
        if let Some(target) = workflow.auto_transition_target(current_state_name) {
            return Ok((workflow_state(workflow, target)?, None));
        }
    }

    Err(ServiceError::InvalidOperation {
        message: format!(
            "trigger '{}' is not available from state '{}'",
            trigger.as_str(),
            current_state_name
        ),
    })
}

fn workflow_state<'a>(
    workflow: &'a WorkflowDefinition,
    state_name: &str,
) -> Result<&'a StateDefinition> {
    workflow
        .states
        .iter()
        .find(|state| state.name == state_name)
        .ok_or_else(|| ServiceError::InvalidOperation {
            message: WorkflowEngine::undefined_state_message(state_name, workflow),
        })
}

fn dispatch_from_trigger(definition: &WorkflowTriggerDefinition) -> Option<DispatchIntent> {
    dispatch_intent_from_workflow_dispatch(definition.dispatch.as_ref())
}

fn merged_state_config(
    state: &StateDefinition,
    project: &db::Project,
    task_state_config_json: Option<&str>,
) -> Value {
    let mut merged = state.config.clone();
    if state.name == default_states::REVIEW {
        merge_project_review_config(&mut merged, project);
    }
    if state.name == default_states::MERGING {
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

fn merge_project_review_config(merged: &mut Value, project: &db::Project) {
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

fn merge_project_merge_fix_budget(merged: &mut Value, project: &db::Project) {
    let Ok(settings) = serde_json::from_str::<Value>(&project.settings) else {
        return;
    };
    let Some(merge_fix) = settings
        .get("retry_budgets")
        .and_then(|budgets| budgets.get("merge_fix"))
        .cloned()
    else {
        return;
    };
    match merged {
        Value::Object(obj) => {
            let budgets = obj.entry("retry_budgets").or_insert_with(|| json!({}));
            if let Value::Object(existing) = budgets {
                existing.insert("merge_fix".to_owned(), merge_fix);
            }
        }
        _ => {
            *merged = json!({ "retry_budgets": { "merge_fix": merge_fix } });
        }
    }
}
