use api_types::StateKind;
use async_trait::async_trait;

use crate::{
    project_hooks::triggers::{HookTrigger, TriggerContext, TriggerMatch},
    workflow::engine::WorkflowEngine,
    Result,
};

pub const ALL_WORK_COMPLETED_TRIGGER_TYPE: &str = "project.all_work_completed";

pub struct AllWorkCompletedTrigger;

#[async_trait]
impl HookTrigger for AllWorkCompletedTrigger {
    async fn evaluate(&self, context: &TriggerContext<'_>) -> Result<Option<TriggerMatch>> {
        let rows = context
            .db
            .project_work_completion_sources(&context.project.id)
            .await?;

        if !all_work_completed(context.project, rows)? {
            return Ok(None);
        }

        let epoch = context.project.project_work_epoch;
        Ok(Some(TriggerMatch {
            trigger_type: ALL_WORK_COMPLETED_TRIGGER_TYPE.to_owned(),
            dedupe_key: format!("{ALL_WORK_COMPLETED_TRIGGER_TYPE}:{epoch}"),
            source_task_id: context.cause.source_task_id().map(str::to_owned),
            source_execution_id: None,
            reason: Some(format!(
                "all visible project work completed at epoch {epoch}"
            )),
        }))
    }
}

pub(crate) async fn all_work_completed_in_tx(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    project: &db::Project,
) -> Result<bool> {
    let rows = db::project_work_completion_sources_in_tx(transaction, &project.id).await?;
    all_work_completed(project, rows)
}

fn all_work_completed(
    project: &db::Project,
    rows: Vec<db::ProjectWorkCompletionSourceRow>,
) -> Result<bool> {
    if rows.is_empty() {
        return Ok(false);
    }

    let project_workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
    let subtask_workflow = WorkflowEngine::resolve_subtask_workflow();
    for row in rows {
        let status: String = row.status()?;
        let parent_task_id: Option<String> = row.parent_task_id()?;
        let workflow = if parent_task_id.is_some() {
            &subtask_workflow
        } else {
            &project_workflow
        };
        if workflow.state_kind(&status) != Some(StateKind::Terminal) {
            return Ok(false);
        }
    }

    Ok(true)
}
