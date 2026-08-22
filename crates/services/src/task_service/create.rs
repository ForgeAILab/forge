use super::*;

impl TaskService {
    #[allow(clippy::too_many_arguments)]
    pub async fn create_task(
        &self,
        project_id: impl Into<String>,
        title: impl Into<String>,
        description: Option<String>,
        parent_task_id: Option<String>,
        priority: Option<i64>,
        task_type: Option<String>,
        task_state_config: Option<String>,
        merge_config: Option<Value>,
        role_assignments: Option<Vec<api_types::InitialRoleAssignment>>,
    ) -> Result<Task> {
        self.create_task_with_governance(
            project_id,
            title,
            description,
            parent_task_id,
            priority,
            task_type,
            task_state_config,
            merge_config,
            role_assignments,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_task_with_governance(
        &self,
        project_id: impl Into<String>,
        title: impl Into<String>,
        description: Option<String>,
        parent_task_id: Option<String>,
        priority: Option<i64>,
        task_type: Option<String>,
        task_state_config: Option<String>,
        merge_config: Option<Value>,
        role_assignments: Option<Vec<api_types::InitialRoleAssignment>>,
        governance: Option<api_types::TaskGovernanceRequest>,
    ) -> Result<Task> {
        let project_id = project_id.into();
        let title = title.into();
        validate_required("project_id", &project_id)?;
        validate_required("title", &title)?;

        let project = ProjectRepo::get_by_id(&*self.db, &project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", project_id.clone()))?;
        let (repo_id, subtask_order) = if let Some(parent_id) = parent_task_id.as_deref() {
            validate_required("parent_task_id", parent_id)?;
            let parent = TaskRepo::get_by_id(&*self.db, parent_id, false)
                .await?
                .ok_or_else(|| ServiceError::not_found("task", parent_id.to_owned()))?;
            if parent.parent_task_id.is_some() {
                return Err(ServiceError::nested_subtask_unsupported());
            }
            (
                parent.repo_id,
                Some(TaskRepo::next_subtask_order(&*self.db, parent_id).await?),
            )
        } else {
            (project.primary_repo_id.clone(), None)
        };

        let now = now_rfc3339();
        let is_subtask = parent_task_id.is_some();
        let is_root = !is_subtask;
        let workflow = if is_subtask {
            WorkflowEngine::resolve_subtask_workflow()
        } else {
            WorkflowEngine::resolve_workflow(&project.workflow_definition)
        };
        let no_repo = repo_id.is_none();
        let initial_status = if no_repo {
            workflow
                .states
                .iter()
                .find(|state| state.kind == api_types::StateKind::Backlog)
                .map(|state| state.name.clone())
                .ok_or_else(|| ServiceError::invalid_operation("workflow has no backlog state"))?
        } else {
            workflow
                .states
                .iter()
                .find(|state| state.kind == api_types::StateKind::Initial)
                .map(|state| state.name.clone())
                .ok_or_else(|| ServiceError::invalid_operation("workflow has no initial state"))?
        };
        let effective_task_type = task_type.unwrap_or_else(|| {
            if is_subtask {
                "sub_task".to_owned()
            } else {
                "task".to_owned()
            }
        });
        if !matches!(
            effective_task_type.as_str(),
            "task" | "planning_task" | "sub_task" | "discovery"
        ) {
            return Err(ServiceError::invalid_operation(
                "task_type must be task, planning_task, sub_task, or discovery",
            ));
        }
        if let (Some(parent_task_id), Some(requested_governance)) =
            (parent_task_id.as_deref(), governance.as_ref())
        {
            let parent = TaskRepo::get_by_id(&*self.db, parent_task_id, false)
                .await?
                .ok_or_else(|| ServiceError::not_found("task", parent_task_id.to_owned()))?;
            self.validate_adaptive_child_governance(&parent, requested_governance, "split")
                .await?;
        }
        let prepared_governance = self
            .prepare_task_governance(&project, repo_id.as_ref(), &effective_task_type, governance)
            .await?;
        let validated_assignments = if let Some(ref assignments) = role_assignments {
            let workflow_roles: std::collections::HashSet<&str> =
                workflow.roles.iter().map(|r| r.name.as_str()).collect();
            let mut validated = Vec::with_capacity(assignments.len());
            for assignment in assignments {
                if !workflow_roles.contains(assignment.role_name.as_str()) {
                    return Err(ServiceError::invalid_operation(format!(
                        "unknown role: {}",
                        assignment.role_name
                    )));
                }
                let assignee_type: AssigneeKind = match assignment.assignee_type {
                    api_types::assignee::AssigneeKind::Agent => AssigneeKind::Agent,
                    api_types::assignee::AssigneeKind::User => AssigneeKind::User,
                };
                let assignee_id = assignment
                    .assignee_id
                    .clone()
                    .filter(|id| !id.trim().is_empty())
                    .ok_or_else(|| {
                        ServiceError::invalid_operation(format!(
                            "role assignment for '{}' requires assignee_id",
                            assignment.role_name
                        ))
                    })?;
                validated.push((assignment.role_name.clone(), assignee_type, assignee_id));
            }
            Some(validated)
        } else {
            None
        };

        let metadata_json = if is_subtask {
            let metadata = TaskMetadata {
                ..TaskMetadata::default()
            };
            metadata.to_json()
        } else {
            None
        };
        let create_task = CreateTask {
            id: new_uuid_v4(),
            project_id,
            repo_id,
            parent_task_id,
            subtask_order,
            assignee_type: None,
            assignee_id: None,
            title,
            description,
            task_type: effective_task_type,
            status: initial_status,
            is_automation: false,
            priority: priority.unwrap_or(0),
            task_state_config,
            merge_config: serialize_config(merge_config)?,
            plan: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        };
        let mut transaction = db::begin_immediate(self.db.pool()).await?;
        let mut task = TaskRepo::create_in_tx(&*self.db, &mut transaction, create_task).await?;
        if !task.is_automation {
            ProjectRepo::increment_project_work_epoch(
                &*self.db,
                &mut transaction,
                &task.project_id,
                1,
            )
            .await?;
        }
        if let Some(governance) = prepared_governance {
            self.insert_task_governance(
                &mut transaction,
                &task.id,
                &task.project_id,
                governance,
                &now,
            )
            .await?;
        }
        transaction.commit().await?;
        if is_subtask {
            TaskRepo::set_metadata_json(&*self.db, &task.id, metadata_json.clone(), &now).await?;
            task.metadata_json = metadata_json;
        }

        if let Some(assignments) = validated_assignments {
            for (role_name, assignee_type, assignee_id) in assignments {
                TaskRoleAssignmentRepo::assign(
                    &*self.db,
                    CreateTaskRoleAssignment {
                        id: new_uuid_v4(),
                        task_id: task.id.clone(),
                        role_name,
                        assignee_type: Some(assignee_type),
                        assignee_id: Some(assignee_id),
                        created_at: now.clone(),
                        updated_at: now.clone(),
                    },
                )
                .await?;
            }
        }

        if is_root {
            self.assign_project_default_roles(&task).await?;
        }

        self.publish(ForgeEvent {
            event_type: "task.created".to_owned(),
            entity_id: task.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskCreated {
                project_id: task.project_id.clone(),
                title: task.title.clone(),
            },
        });

        Ok(task)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_automation_task(
        &self,
        project_id: impl Into<String>,
        title: impl Into<String>,
        description: Option<String>,
        task_type: Option<String>,
        task_state_config: Option<String>,
        merge_config: Option<Value>,
    ) -> Result<Task> {
        let project_id = project_id.into();
        let title = title.into();
        validate_required("project_id", &project_id)?;
        validate_required("title", &title)?;

        let project = ProjectRepo::get_by_id(&*self.db, &project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", project_id.clone()))?;
        let repo_id = project.primary_repo_id.clone();
        let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
        let initial_status = if repo_id.is_none() {
            workflow
                .states
                .iter()
                .find(|state| state.kind == api_types::StateKind::Backlog)
                .map(|state| state.name.clone())
                .ok_or_else(|| ServiceError::invalid_operation("workflow has no backlog state"))?
        } else {
            workflow
                .states
                .iter()
                .find(|state| state.kind == api_types::StateKind::Initial)
                .map(|state| state.name.clone())
                .ok_or_else(|| ServiceError::invalid_operation("workflow has no initial state"))?
        };

        let now = now_rfc3339();
        let effective_task_type = task_type.unwrap_or_else(|| "task".to_owned());
        let prepared_governance = self
            .prepare_task_governance(&project, repo_id.as_ref(), &effective_task_type, None)
            .await?;
        let create_task = CreateTask {
            id: new_uuid_v4(),
            project_id,
            repo_id,
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title,
            description,
            task_type: effective_task_type,
            status: initial_status,
            is_automation: true,
            priority: 0,
            task_state_config,
            merge_config: serialize_config(merge_config)?,
            plan: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        };
        let mut transaction = db::begin_immediate(self.db.pool()).await?;
        let task = TaskRepo::create_in_tx(&*self.db, &mut transaction, create_task).await?;
        if let Some(governance) = prepared_governance {
            self.insert_task_governance(
                &mut transaction,
                &task.id,
                &task.project_id,
                governance,
                &now,
            )
            .await?;
        }
        transaction.commit().await?;

        self.publish(ForgeEvent {
            event_type: "task.created".to_owned(),
            entity_id: task.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskCreated {
                project_id: task.project_id.clone(),
                title: task.title.clone(),
            },
        });

        Ok(task)
    }

    pub async fn duplicate_task(&self, source_task_id: &str) -> Result<Task> {
        let source = TaskRepo::get_by_id(&*self.db, source_task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", source_task_id.to_owned()))?;
        let task = self
            .create_task(
                source.project_id,
                source.title,
                source.description,
                None,
                Some(source.priority),
                Some(source.task_type),
                source.task_state_config,
                source
                    .merge_config
                    .as_deref()
                    .and_then(|s| serde_json::from_str(s).ok()),
                None,
            )
            .await?;
        TaskRepo::get_by_id(&*self.db, &task.id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task.id))
    }

    /// Create a replacement through the same Task/governance materializer as
    /// ordinary proposals. The replacement inherits the source's approved
    /// baseline boundaries and records both origin and replacement links;
    /// callers cannot supply a second risk, acceptance, or release contract.
    pub async fn replace_task(
        &self,
        source_task_id: impl Into<String>,
        title: impl Into<String>,
        description: Option<String>,
    ) -> Result<Task> {
        let source_task_id = source_task_id.into();
        let source = TaskRepo::get_by_id(&*self.db, &source_task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", source_task_id.clone()))?;
        let board_revision = TaskBoardRepo::board_revision(&*self.db, &source.project_id).await?;
        let result = self
            .execute_adaptive_task_command(AdaptiveTaskCommand::system(
                source.project_id.clone(),
                source.id.clone(),
                source.version,
                board_revision,
                AdaptiveTaskOperation::Replace {
                    title: title.into(),
                    description,
                },
                "Replace Task within the approved adaptive boundary",
            ))
            .await?;
        result
            .tasks
            .into_iter()
            .next()
            .ok_or_else(|| ServiceError::Db(db::DbError::IdempotencyConflict))
    }

    pub(super) async fn project_default_role_assignments(
        &self,
        task: &Task,
        mut covered_roles: HashSet<String>,
    ) -> Result<Vec<CreateTaskRoleAssignment>> {
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let settings =
            serde_json::from_str::<ProjectSettings>(&project.settings).map_err(|error| {
                ServiceError::invalid_operation(format!("invalid project settings: {error}"))
            })?;
        if settings.default_role_assignments.is_empty() {
            return Ok(Vec::new());
        }

        let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
        let workflow_roles = workflow
            .roles
            .iter()
            .map(|role| role.name.as_str())
            .collect::<HashSet<_>>();
        let mut assignments = Vec::new();
        for assignment in settings.default_role_assignments {
            let role_name = assignment.role_name;
            if !workflow_roles.contains(role_name.as_str()) || covered_roles.contains(&role_name) {
                continue;
            }

            let assignee_type = assignment.assignee_type;
            let assignee_id = match assignee_type.as_str() {
                "agent" => {
                    let Some(assignee_id) = assignment
                        .assignee_id
                        .filter(|assignee_id| !assignee_id.trim().is_empty())
                    else {
                        return Err(ServiceError::invalid_operation(format!(
                            "default role assignment for role '{role_name}' requires assignee_id"
                        )));
                    };
                    assignee_id
                }
                "user" => {
                    let Some(assignee_id) = assignment
                        .assignee_id
                        .filter(|assignee_id| !assignee_id.trim().is_empty())
                    else {
                        return Err(ServiceError::invalid_operation(format!(
                            "default role assignment for role '{role_name}' requires assignee_id"
                        )));
                    };
                    assignee_id
                }
                _ => {
                    return Err(ServiceError::invalid_operation(format!(
                        "default role assignment for role '{role_name}' must use assignee_type 'agent' or 'user'"
                    )));
                }
            };
            let assignee_type = assignee_type
                .parse::<AssigneeKind>()
                .map_err(ServiceError::invalid_operation)?;

            let now = now_rfc3339();
            assignments.push(CreateTaskRoleAssignment {
                id: new_uuid_v4(),
                task_id: task.id.clone(),
                role_name: role_name.clone(),
                assignee_type: Some(assignee_type),
                assignee_id: Some(assignee_id),
                created_at: now.clone(),
                updated_at: now,
            });
            covered_roles.insert(role_name);
        }

        Ok(assignments)
    }

    async fn assign_project_default_roles(&self, task: &Task) -> Result<()> {
        let covered_roles = TaskRoleAssignmentRepo::list_by_task(&*self.db, &task.id)
            .await?
            .into_iter()
            .map(|assignment| assignment.role_name)
            .collect::<HashSet<_>>();
        for assignment in self
            .project_default_role_assignments(task, covered_roles)
            .await?
        {
            TaskRoleAssignmentRepo::assign(&*self.db, assignment).await?;
        }
        Ok(())
    }
}
