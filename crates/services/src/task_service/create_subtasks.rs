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
        let executions = ExecutionRepo::list_by_task(
            &*self.db,
            &parent.id,
            PageRequest {
                cursor: None,
                limit: 100,
                include_total: false,
                sort_by: SortBy::CreatedAt,
                sort_order: SortOrder::Desc,
            },
        )
        .await?;
        if executions
            .items
            .iter()
            .any(|execution| execution.status == ExecutionStatus::Running)
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
        let implementation_role = workflow
            .states
            .iter()
            .find(|state| state.name == crate::workflow::default_states::IN_PROGRESS)
            .and_then(crate::workflow::effective_role)
            .or_else(|| {
                workflow
                    .states
                    .iter()
                    .find(|state| state.kind == api_types::StateKind::Active)
                    .and_then(crate::workflow::effective_role)
            });
        let aggregate_review_roles = workflow
            .states
            .iter()
            .filter(|state| {
                state.kind == api_types::StateKind::Gate
                    && state.canonical_phase == Some(api_types::CanonicalPhase::Review)
            })
            .filter_map(|state| state.role.as_deref())
            .filter(|role| Some(*role) != implementation_role)
            .collect::<std::collections::HashSet<_>>();
        for assignment in TaskRoleAssignmentRepo::list_by_task(&*self.db, &parent.id).await? {
            if !aggregate_review_roles.contains(assignment.role_name.as_str()) {
                TaskRoleAssignmentRepo::remove(&*self.db, &parent.id, &assignment.role_name)
                    .await?;
            }
        }
        Ok(result.tasks)
    }
}
