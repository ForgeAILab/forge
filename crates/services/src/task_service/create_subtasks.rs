use super::*;

pub struct NewSubtaskInput {
    pub title: String,
    pub description: Option<String>,
    pub assignee_id: Option<String>,
}

impl TaskService {
    pub async fn create_subtasks(
        &self,
        parent_task_id: String,
        items: Vec<NewSubtaskInput>,
    ) -> Result<Vec<Task>> {
        validate_required("parent_task_id", &parent_task_id)?;
        let parent = TaskRepo::get_by_id(&*self.db, &parent_task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", parent_task_id.clone()))?;
        if parent.parent_task_id.is_some() {
            return Err(ServiceError::nested_subtask_unsupported());
        }
        for item in &items {
            validate_required("title", &item.title)?;
        }
        let board_revision = TaskBoardRepo::board_revision(&*self.db, &parent.project_id).await?;
        let result = self
            .execute_adaptive_task_command(AdaptiveTaskCommand::system(
                parent.project_id.clone(),
                parent.id.clone(),
                parent.version,
                board_revision,
                AdaptiveTaskOperation::Split {
                    items: items
                        .into_iter()
                        .map(|item| AdaptiveTaskChild {
                            title: item.title,
                            description: item.description,
                            assignee_id: item.assignee_id,
                        })
                        .collect(),
                },
                "Split Task into bounded subtasks",
            ))
            .await?;
        Ok(result.tasks)
    }
}
