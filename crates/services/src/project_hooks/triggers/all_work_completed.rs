use api_types::StateKind;
use async_trait::async_trait;
use sqlx::Row;

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
        let rows = sqlx::query(
            "SELECT id, status, parent_task_id \
             FROM task \
             WHERE project_id = ? \
               AND is_automation = 0 \
               AND archived_at IS NULL \
               AND deleted_at IS NULL",
        )
        .bind(&context.project.id)
        .fetch_all(context.db.pool())
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
    let rows = sqlx::query("SELECT id, status, parent_task_id FROM task WHERE project_id = ? AND is_automation = 0 AND archived_at IS NULL AND deleted_at IS NULL").bind(&project.id).fetch_all(&mut **transaction).await?;
    all_work_completed(project, rows)
}

fn all_work_completed(project: &db::Project, rows: Vec<sqlx::sqlite::SqliteRow>) -> Result<bool> {
    if rows.is_empty() {
        return Ok(false);
    }

    let project_workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
    let subtask_workflow = WorkflowEngine::resolve_subtask_workflow();
    for row in rows {
        let status: String = row.try_get("status")?;
        let parent_task_id: Option<String> = row.try_get("parent_task_id")?;
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
