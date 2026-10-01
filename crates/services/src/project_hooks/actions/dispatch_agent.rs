use db::{AgentRepo, ProjectAgentBindingRepo, ProjectHookRunStatus};
use serde_json::Value;

use crate::{
    agent_service::{compute_effective_status, EffectiveStatus},
    project_hooks::actions::{
        task_type_to_string, ActionContext, ActionOutcome, HookActionHandler,
    },
    Result, ServiceError,
};

pub struct DispatchAgentAction<'a> {
    pub agent_id: &'a str,
    pub prompt: Option<&'a str>,
    pub follow_up: Option<&'a Value>,
}

#[async_trait::async_trait]
impl HookActionHandler for DispatchAgentAction<'_> {
    async fn execute(&self, context: &ActionContext<'_>) -> Result<ActionOutcome> {
        if context.project.paused_at.is_some() {
            return Ok(ActionOutcome::skipped(format!(
                "project {} is paused",
                context.project.id
            )));
        }

        let agent = AgentRepo::get_by_id(&*context.service.db, self.agent_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent", self.agent_id.to_owned()))?;
        if !agent_usable_in_project(context, &agent).await? {
            return Ok(ActionOutcome::skipped(format!(
                "agent {} is not usable in project {}",
                agent.id, context.project.id
            )));
        }

        match compute_effective_status(&context.service.db, &agent, None).await? {
            EffectiveStatus::Active => {}
            status => {
                return Ok(ActionOutcome::skipped(format!(
                    "agent {} is not available: {}",
                    agent.id, status
                )));
            }
        }

        let task = context
            .service
            .task_service
            .create_automation_task(
                context.project.id.clone(),
                format!("Automation: {}", context.rule_id),
                Some(automation_task_description(context)),
                Some(task_type_to_string(api_types::TaskType::Task)),
                None,
                None,
            )
            .await?;

        let prompt = build_prompt(context, self.prompt, self.follow_up);
        let launch = context
            .service
            .task_service
            .launch_execution(task.id.clone(), agent.id.clone(), Some(prompt), None)
            .await;
        let execution = match launch {
            Ok(result) => result.execution,
            Err(error) => {
                return Ok(ActionOutcome {
                    status: ProjectHookRunStatus::Failed,
                    automation_task_id: Some(task.id),
                    execution_id: None,
                    agent_id: Some(agent.id),
                    reason: Some(format!(
                        "automation task created but execution launch failed: {error}"
                    )),
                });
            }
        };

        Ok(ActionOutcome {
            status: ProjectHookRunStatus::Dispatched,
            automation_task_id: Some(task.id),
            execution_id: Some(execution.id),
            agent_id: Some(agent.id),
            reason: Some("agent dispatched".to_owned()),
        })
    }
}

async fn agent_usable_in_project(context: &ActionContext<'_>, agent: &db::Agent) -> Result<bool> {
    if agent.visibility == "global" {
        return Ok(true);
    }
    if context.project.owner_id.as_deref().is_some()
        && context.project.owner_id.as_deref() == agent.owner_id.as_deref()
    {
        return Ok(true);
    }
    Ok(ProjectAgentBindingRepo::get_active_project_binding(
        &*context.service.db,
        &context.project.id,
    )
    .await?
    .is_some_and(|binding| binding.identity_id.as_deref() == Some(agent.id.as_str())))
}

fn automation_task_description(context: &ActionContext<'_>) -> String {
    format!(
        "Project hook automation task.\n\nHook run: {}\nRule: {}\nTrigger: {}\nDedupe key: {}",
        context.run.id,
        context.rule_id,
        context.trigger_match.trigger_type,
        context.trigger_match.dedupe_key
    )
}

fn build_prompt(
    context: &ActionContext<'_>,
    prompt: Option<&str>,
    follow_up: Option<&Value>,
) -> String {
    let mut parts = vec![
        format!("Project: {}", context.project.name),
        format!("Project ID: {}", context.project.id),
        format!("Hook run ID: {}", context.run.id),
        format!("Rule ID: {}", context.rule_id),
        format!("Trigger: {}", context.trigger_match.trigger_type),
        format!("Dedupe key: {}", context.trigger_match.dedupe_key),
    ];
    if let Some(source_task_id) = context.trigger_match.source_task_id.as_deref() {
        parts.push(format!("Source task ID: {source_task_id}"));
    }
    if let Some(reason) = context.trigger_match.reason.as_deref() {
        parts.push(format!("Reason: {reason}"));
    }
    if let Some(follow_up) = follow_up {
        parts.push(format!("Follow-up policy: {follow_up}"));
    }
    if let Some(prompt) = prompt.map(str::trim).filter(|prompt| !prompt.is_empty()) {
        parts.push(format!("Instructions:\n{prompt}"));
    }
    parts.join("\n\n")
}
