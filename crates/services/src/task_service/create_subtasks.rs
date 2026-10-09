use super::*;

#[derive(serde::Serialize, serde::Deserialize)]
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
        if !db::task_writer::owns_task(&parent_task_id) {
            return self
                .request_task_command(
                    &parent_task_id,
                    "create_subtasks",
                    serde_json::json!([parent_task_id, items]),
                    false,
                )
                .await;
        }
        validate_required("parent_task_id", &parent_task_id)?;
        let parent = TaskRepo::get_by_id(&*self.db, &parent_task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", parent_task_id.clone()))?;
        if parent.parent_task_id.is_some() {
            return Err(ServiceError::nested_subtask_unsupported());
        }
        let (_, parent_workflow) =
            crate::task_hierarchy::coordination_root_context(&self.db, &parent.id).await?;
        crate::task_hierarchy::ensure_parent_accepts_subtasks(&parent, &parent_workflow)?;
        if !ExecutionRepo::list_running_by_task(&*self.db, &parent.id)
            .await?
            .is_empty()
        {
            return Err(ServiceError::invalid_operation(
                "cannot convert a running task into a coordination root; stop its implementation execution first",
            ));
        }
        for item in &items {
            validate_required("title", &item.title)?;
            if let Some(agent_id) = item.assignee_id.as_deref() {
                validate_required("assignee_id", agent_id)?;
                crate::ensure_execution_role_principal(
                    &self.db,
                    &parent.project_id,
                    crate::workflow::default_roles::CODER,
                    agent_id,
                )
                .await?;
            }
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
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &parent,
            &ProjectRepo::get_by_id(&*self.db, &parent.project_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("project", parent.project_id.clone()))?
                .workflow_definition,
            &api_types::Actor::system(api_types::SystemComponent::TaskDispatcher),
        );
        let root_role_policy = crate::task_hierarchy::RootRolePolicy::for_workflow(&workflow);
        let mut current_parent = result.source_task.clone();
        for assignment in TaskRoleAssignmentRepo::list_by_task(&*self.db, &parent.id).await? {
            if !root_role_policy.allows_assignment(&assignment.role_name) {
                current_parent = TaskRoleAssignmentRepo::remove_and_clear_review_authority(
                    &*self.db,
                    &assignment,
                    current_parent.version,
                    &now_rfc3339(),
                )
                .await?;
                let current_parent_id = current_parent.id.clone();
                crate::wake_task_dispatch(
                    &self.db,
                    &current_parent_id,
                    "coordination root role assignment removed",
                )
                .await?;
                current_parent = TaskRepo::get_by_id(&*self.db, &current_parent_id, false)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("task", current_parent_id.clone()))?;
            }
        }
        Ok(result.tasks)
    }
}
