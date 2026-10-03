use api_types::{parse_project_hooks_json, ProjectHookRule, ProjectHookTrigger};
use db::{Project, ProjectRepo};

use crate::{project_hooks::triggers::HookTrigger, Result, ServiceError};

use super::{
    engine::ProjectHookEngine,
    triggers::{all_work_completed::AllWorkCompletedTrigger, TriggerContext},
    ProjectHookService,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvaluationCause {
    TaskCreated { task_id: String },
    TaskTransitioned { task_id: String },
    TaskArchived { task_id: String },
    ScheduledScan,
}

impl EvaluationCause {
    pub(crate) fn source_task_id(&self) -> Option<&str> {
        match self {
            Self::TaskCreated { task_id }
            | Self::TaskTransitioned { task_id }
            | Self::TaskArchived { task_id } => Some(task_id),
            Self::ScheduledScan => None,
        }
    }
}

pub async fn evaluate_for_project(
    service: &ProjectHookService,
    project_id: String,
    cause: EvaluationCause,
) -> Result<()> {
    let Some(project) = ProjectRepo::get_by_id(&*service.db, &project_id).await? else {
        return Ok(());
    };
    let rules = parse_project_hooks_json(&project.project_hooks_json).map_err(|error| {
        ServiceError::invalid_operation(format!("invalid project hooks: {error}"))
    })?;
    if rules.is_empty() {
        return Ok(());
    }

    let engine = ProjectHookEngine::new(service);
    for rule in rules.into_iter().filter(|rule| rule.enabled) {
        let rule_id = rule.id.clone();
        if let Err(error) = evaluate_rule(&engine, service, &project, rule, &cause).await {
            tracing::warn!(
                project_id = %project.id,
                rule_id = %rule_id,
                %error,
                "project hook rule evaluation failed"
            );
        }
    }
    Ok(())
}

async fn evaluate_rule(
    engine: &ProjectHookEngine<'_>,
    service: &ProjectHookService,
    project: &Project,
    rule: ProjectHookRule,
    cause: &EvaluationCause,
) -> Result<()> {
    let trigger_context = TriggerContext {
        db: service.db.as_ref(),
        project,
        cause,
    };
    let trigger_match = match &rule.trigger {
        ProjectHookTrigger::AllWorkCompleted => {
            AllWorkCompletedTrigger.evaluate(&trigger_context).await?
        }
    };

    if let Some(trigger_match) = trigger_match {
        engine.run(project, rule, trigger_match).await?;
    }
    Ok(())
}

/// Prepare every enabled rule outside the runtime's writer transaction.
pub(crate) async fn prepare_for_project(
    service: &ProjectHookService,
    project: &Project,
    cause: &EvaluationCause,
) -> Result<Vec<super::engine::PreparedHook>> {
    let rules = parse_project_hooks_json(&project.project_hooks_json).map_err(|error| {
        ServiceError::invalid_operation(format!("invalid project hooks: {error}"))
    })?;
    let engine = ProjectHookEngine::new(service);
    let mut prepared = Vec::new();
    for rule in rules.into_iter().filter(|rule| rule.enabled) {
        let context = TriggerContext {
            db: &service.db,
            project,
            cause,
        };
        let matched = match rule.trigger {
            ProjectHookTrigger::AllWorkCompleted => {
                AllWorkCompletedTrigger.evaluate(&context).await?
            }
        };
        if let Some(matched) = matched {
            prepared.push(engine.prepare(project, rule, matched).await?);
        }
    }
    Ok(prepared)
}
