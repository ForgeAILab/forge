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

    let prepared = prepare_rules(
        service,
        &project,
        cause,
        &format!("manual:{}", db::new_uuid_v4()),
        rules,
        &AllWorkCompletedTrigger,
    )
    .await?;
    let engine = ProjectHookEngine::new(service);
    for hook in prepared {
        let mut tx = db::begin_immediate(service.db.pool()).await?;
        if engine.still_matches(&mut tx, &hook).await? {
            let committed = engine.commit(&mut tx, &hook).await?;
            tx.commit().await?;
            if let Some(committed) = committed {
                if let Err(error) = engine.after_commit(&hook, &committed).await {
                    tracing::warn!(%error, "project hook rule action failed");
                }
            }
        } else {
            tx.rollback().await?;
        }
    }
    Ok(())
}

/// Prepare every enabled rule outside the runtime's writer transaction.
pub(crate) async fn prepare_for_project(
    service: &ProjectHookService,
    project: &Project,
    cause: &EvaluationCause,
    source_key: &str,
) -> Result<Vec<super::engine::PreparedHook>> {
    let rules = parse_project_hooks_json(&project.project_hooks_json).map_err(|error| {
        ServiceError::invalid_operation(format!("invalid project hooks: {error}"))
    })?;
    prepare_rules(
        service,
        project,
        cause.clone(),
        source_key,
        rules,
        &AllWorkCompletedTrigger,
    )
    .await
}

pub(super) async fn prepare_rules(
    service: &ProjectHookService,
    project: &Project,
    cause: EvaluationCause,
    source_key: &str,
    rules: Vec<ProjectHookRule>,
    trigger: &impl HookTrigger,
) -> Result<Vec<super::engine::PreparedHook>> {
    let engine = ProjectHookEngine::new(service);
    let mut prepared = Vec::new();
    for rule in rules.into_iter().filter(|rule| rule.enabled) {
        let context = TriggerContext {
            db: &service.db,
            project,
            cause: &cause,
        };
        let result = async {
            let matched = match rule.trigger {
                ProjectHookTrigger::AllWorkCompleted => trigger.evaluate(&context).await?,
            };
            match matched {
                Some(matched) => engine
                    .prepare(project, rule.clone(), matched)
                    .await
                    .map(Some),
                None => Ok(None),
            }
        }
        .await;
        match result {
            Ok(Some(hook)) => prepared.push(hook),
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(project_id = %project.id, rule_id = %rule.id, %error, "project hook rule preparation failed");
                prepared.push(engine.prepare_failed(
                    project,
                    rule,
                    source_key,
                    &cause,
                    error.to_string(),
                ));
            }
        }
    }
    Ok(prepared)
}
