use super::*;

impl TaskService {
    pub async fn reorder_subtasks(
        &self,
        parent_task_id: String,
        ordered_ids: Vec<String>,
    ) -> Result<()> {
        validate_required("parent_task_id", &parent_task_id)?;
        let parent = TaskRepo::get_by_id(&*self.db, &parent_task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", parent_task_id.clone()))?;
        if !subtask::is_root_task(&self.db, &parent_task_id).await? {
            return Err(ServiceError::invalid_operation(format!(
                "task {parent_task_id} is not a root task"
            )));
        }
        let board_revision = TaskBoardRepo::board_revision(&*self.db, &parent.project_id).await?;
        self.execute_adaptive_task_command(AdaptiveTaskCommand::system(
            parent.project_id.clone(),
            parent.id.clone(),
            parent.version,
            board_revision,
            AdaptiveTaskOperation::Sequence {
                ordered_task_ids: ordered_ids,
            },
            "Sequence Task subtasks",
        ))
        .await?;
        Ok(())
    }
}
