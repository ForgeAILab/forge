use super::*;
use events::RoleAssignmentSnapshot;
use sqlx::{Row, Sqlite, Transaction};
use std::collections::HashSet;

impl TaskService {
    pub async fn on_agent_deleted(&self, agent_id: &str) -> Result<()> {
        validate_required("agent_id", agent_id)?;
        let mut transaction = db::begin_immediate(self.db.pool()).await?;
        let events = self
            .on_agent_deleted_in_tx(&mut transaction, agent_id)
            .await?;
        transaction.commit().await?;
        self.publish_role_sweep_events(events);
        Ok(())
    }

    pub(crate) async fn on_agent_deleted_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        agent_id: &str,
    ) -> Result<Vec<RoleSweepEvent>> {
        validate_required("agent_id", agent_id)?;
        let rows = sqlx::query(
            "SELECT id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at FROM task_role_assignment WHERE assignee_type = 'agent' AND assignee_id = ? ORDER BY task_id, role_name",
        )
        .bind(agent_id)
        .fetch_all(&mut **transaction)
        .await?;

        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let previous_assignment = TaskRoleAssignment {
                id: row.try_get("id")?,
                task_id: row.try_get("task_id")?,
                role_name: row.try_get("role_name")?,
                assignee_type: Some(AssigneeKind::Agent),
                assignee_id: row.try_get("assignee_id")?,
                created_at: row.try_get("created_at")?,
                updated_at: row.try_get("updated_at")?,
            };
            let mut new_assignment = previous_assignment.clone();
            new_assignment.assignee_id = None;
            events.push(RoleSweepEvent {
                task_id: previous_assignment.task_id.clone(),
                role_name: previous_assignment.role_name.clone(),
                previous_assignment,
                new_assignment,
            });
        }

        sqlx::query(
            "UPDATE task_role_assignment SET assignee_id = NULL, updated_at = ? WHERE assignee_type = 'agent' AND assignee_id = ?",
        )
        .bind(now_rfc3339())
        .bind(agent_id)
        .execute(&mut **transaction)
        .await?;
        sqlx::query(
            "UPDATE task SET assignee_id = NULL, updated_at = ? WHERE assignee_type = 'agent' AND assignee_id = ?",
        )
        .bind(now_rfc3339())
        .bind(agent_id)
        .execute(&mut **transaction)
        .await?;

        // Agent archival changes the effective authority of every affected
        // Task.  Keep that invalidation and dispatch wake in the same write
        // transaction as the assignment sweep so a crash cannot leave a
        // parked Task with either stale review authority or a stale denial.
        let affected_task_ids: HashSet<&str> =
            events.iter().map(|event| event.task_id.as_str()).collect();
        for task_id in affected_task_ids {
            sqlx::query(
                "UPDATE task
                 SET review_passed_at = NULL,
                     version = version + CASE WHEN review_passed_at IS NOT NULL THEN 1 ELSE 0 END,
                     metadata_json = CASE
                         WHEN json_valid(COALESCE(metadata_json, '{}'))
                         THEN NULLIF(json_remove(COALESCE(metadata_json, '{}'), '$.dispatch_disposition', '$.deferred_dispatch'), '{}')
                         ELSE metadata_json
                     END,
                     updated_at = CASE
                         WHEN review_passed_at IS NOT NULL
                              OR (json_valid(COALESCE(metadata_json, '{}'))
                                  AND (json_type(metadata_json, '$.dispatch_disposition') IS NOT NULL
                                       OR json_type(metadata_json, '$.deferred_dispatch') IS NOT NULL))
                         THEN ? ELSE updated_at END
                 WHERE id = ? AND deleted_at IS NULL
                   AND (review_passed_at IS NOT NULL
                        OR (json_valid(COALESCE(metadata_json, '{}'))
                            AND (json_type(metadata_json, '$.dispatch_disposition') IS NOT NULL
                                 OR json_type(metadata_json, '$.deferred_dispatch') IS NOT NULL)))",
            )
            .bind(now_rfc3339())
            .bind(task_id)
            .execute(&mut **transaction)
            .await?;
        }

        Ok(events)
    }

    pub(crate) fn publish_role_sweep_events(&self, events: Vec<RoleSweepEvent>) {
        for event in events {
            self.publish_role_reassigned(
                &event.task_id,
                &event.role_name,
                Some(&event.previous_assignment),
                Some(&event.new_assignment),
                RoleReassignmentEventFlags::default(),
            );
        }
    }

    pub async fn coder_assignment(&self, task_id: &str) -> Result<Option<TaskRoleAssignment>> {
        validate_required("task_id", task_id)?;
        let task = TaskRepo::get_by_id(&*self.db, task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()))?;
        let Some(role_name) = self.active_work_role(&task).await? else {
            return Ok(None);
        };
        TaskRoleAssignmentRepo::get_by_task_and_role(&*self.db, task_id, &role_name)
            .await
            .map_err(Into::into)
    }

    /// Assign an Agent to the Task's implementation role without claiming or
    /// starting the Task. Dispatch remains the scheduler's responsibility, so
    /// this operation is valid while the Project is paused.
    pub async fn assign_agent_to_task(
        &self,
        task_id: &str,
        agent_id: &str,
    ) -> Result<TaskRoleAssignment> {
        validate_required("task_id", task_id)?;
        validate_required("agent_id", agent_id)?;
        let task = TaskRepo::get_by_id(&*self.db, task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()))?;
        let role_name = self.active_work_role(&task).await?.ok_or_else(|| {
            ServiceError::invalid_operation(format!(
                "task {} has no implementation role in its effective workflow",
                task.id
            ))
        })?;
        let now = now_rfc3339();
        self.reassign_role_with_active_execution_policy(
            CreateTaskRoleAssignment {
                id: new_uuid_v4(),
                task_id: task.id,
                role_name,
                assignee_type: Some(AssigneeKind::Agent),
                assignee_id: Some(agent_id.to_owned()),
                created_at: now.clone(),
                updated_at: now,
            },
            false,
            false,
            false,
        )
        .await
    }

    pub async fn reassign_role(
        &self,
        input: CreateTaskRoleAssignment,
        reset_workspace: bool,
        reset_worktree: bool,
    ) -> Result<TaskRoleAssignment> {
        self.reassign_role_with_active_execution_policy(
            input,
            reset_workspace,
            reset_worktree,
            true,
        )
        .await
    }

    async fn reassign_role_with_active_execution_policy(
        &self,
        input: CreateTaskRoleAssignment,
        reset_workspace: bool,
        reset_worktree: bool,
        cancel_active_execution: bool,
    ) -> Result<TaskRoleAssignment> {
        let mut task = self.validate_reassignable_task(&input.task_id).await?;
        self.ensure_coordination_root_role_allowed(&task, &input.role_name)
            .await?;
        if input.assignee_type == Some(AssigneeKind::Agent) {
            if let Some(identity_id) = input.assignee_id.as_deref() {
                crate::ensure_execution_role_principal(
                    &self.db,
                    &task.project_id,
                    &input.role_name,
                    identity_id,
                )
                .await?;
            }
        }

        let is_coder_role =
            self.active_work_role(&task).await?.as_deref() == Some(input.role_name.as_str());
        let previous = TaskRoleAssignmentRepo::get_by_task_and_role(
            &*self.db,
            &input.task_id,
            &input.role_name,
        )
        .await?;
        if is_coder_role && !cancel_active_execution {
            let (previous, assignment, changed) = self
                .assign_coder_role_if_idle(input, previous.as_ref(), task.version)
                .await?;
            if changed {
                self.publish_role_reassigned(
                    &assignment.task_id,
                    &assignment.role_name,
                    previous.as_ref(),
                    Some(&assignment),
                    RoleReassignmentEventFlags::default(),
                );
            }
            crate::wake_task_dispatch(
                &self.db,
                &assignment.task_id,
                if changed {
                    "task role assignment changed"
                } else {
                    "task role assignment confirmed"
                },
            )
            .await?;
            return Ok(assignment);
        }

        if same_assignment(previous.as_ref(), Some(&input)) {
            let previous = previous.as_ref().ok_or_else(|| {
                ServiceError::conflict("role assignment changed while confirming it")
            })?;
            let assignment =
                TaskRoleAssignmentRepo::assign_if_unchanged(&*self.db, input, Some(previous))
                    .await?;
            crate::wake_task_dispatch(
                &self.db,
                &assignment.task_id,
                "task role assignment confirmed",
            )
            .await?;
            return Ok(assignment);
        }

        if !is_coder_role {
            let now = now_rfc3339();
            let (assignment, _updated_task) =
                TaskRoleAssignmentRepo::assign_and_clear_review_authority(
                    &*self.db,
                    input,
                    previous.as_ref(),
                    task.version,
                    &now,
                )
                .await?;
            self.publish_role_reassigned(
                &assignment.task_id,
                &assignment.role_name,
                previous.as_ref(),
                Some(&assignment),
                RoleReassignmentEventFlags::default(),
            );
            crate::wake_task_dispatch(
                &self.db,
                &assignment.task_id,
                "task role assignment changed",
            )
            .await?;
            return Ok(assignment);
        }

        let active_execution = self
            .active_execution_for_role(&task, &input.role_name)
            .await?;
        let Some(active_execution) = active_execution else {
            let now = now_rfc3339();
            let (assignment, _updated_task) =
                TaskRoleAssignmentRepo::assign_and_clear_review_authority(
                    &*self.db,
                    input,
                    previous.as_ref(),
                    task.version,
                    &now,
                )
                .await?;
            self.publish_role_reassigned(
                &assignment.task_id,
                &assignment.role_name,
                previous.as_ref(),
                Some(&assignment),
                RoleReassignmentEventFlags::default(),
            );
            crate::wake_task_dispatch(
                &self.db,
                &assignment.task_id,
                "task role assignment changed",
            )
            .await?;
            return Ok(assignment);
        };

        if !cancel_active_execution {
            return Err(ServiceError::invalid_operation(format!(
                "task {} has a running {} execution; stop it before changing its assignment",
                task.id, input.role_name
            )));
        }

        self.cancel_active_execution(
            &active_execution,
            "cancelled by role reassignment",
            db::StopReason::RoleReassigned,
            &api_types::Actor::user(api_types::UserActionSource::RoleReassignment),
            db::ResumePolicy::None,
        )
        .await?;

        let now = now_rfc3339();
        let (assignment, updated_task) = TaskRoleAssignmentRepo::assign_and_clear_review_authority(
            &*self.db,
            input,
            previous.as_ref(),
            task.version,
            &now,
        )
        .await?;
        task = updated_task;

        let (workflow, workflow_authority) = self.workflow_and_authority_for_task(&task).await?;
        let initial_state = workflow_initial_state(&workflow)?;
        self.workflow_engine()
            .reset_to_initial_with_authority(
                &task.id,
                &initial_state,
                task.version,
                &workflow,
                &api_types::Actor::user(api_types::UserActionSource::Reassignment),
                "coder reassigned",
                workflow_authority,
            )
            .await?;

        let (effective_reset_workspace, effective_reset_worktree) = self
            .apply_reassignment_reset(&task, &active_execution, reset_workspace, reset_worktree)
            .await?;

        self.publish_role_reassigned(
            &assignment.task_id,
            &assignment.role_name,
            previous.as_ref(),
            Some(&assignment),
            RoleReassignmentEventFlags {
                triggered_cancellation: true,
                reset_workspace: effective_reset_workspace,
                reset_worktree: effective_reset_worktree,
                transitioned_to_todo: true,
            },
        );
        crate::wake_task_dispatch(
            &self.db,
            &assignment.task_id,
            "task role assignment changed",
        )
        .await?;
        Ok(assignment)
    }

    pub async fn remove_role(
        &self,
        task_id: &str,
        role_name: &str,
        reset_workspace: bool,
        reset_worktree: bool,
    ) -> Result<()> {
        let mut task = self.validate_reassignable_task(task_id).await?;
        let previous =
            TaskRoleAssignmentRepo::get_by_task_and_role(&*self.db, task_id, role_name).await?;
        let Some(previous) = previous else {
            return Ok(());
        };

        let is_coder_role = self.active_work_role(&task).await?.as_deref() == Some(role_name);
        if !is_coder_role {
            let _updated_task = TaskRoleAssignmentRepo::remove_and_clear_review_authority(
                &*self.db,
                &previous,
                task.version,
                &now_rfc3339(),
            )
            .await?;
            self.publish_role_reassigned(
                task_id,
                role_name,
                Some(&previous),
                None,
                RoleReassignmentEventFlags::default(),
            );
            crate::wake_task_dispatch(&self.db, task_id, "task role assignment removed").await?;
            return Ok(());
        }

        let active_execution = self.active_execution_for_role(&task, role_name).await?;
        let Some(active_execution) = active_execution else {
            let _updated_task = TaskRoleAssignmentRepo::remove_and_clear_review_authority(
                &*self.db,
                &previous,
                task.version,
                &now_rfc3339(),
            )
            .await?;
            self.publish_role_reassigned(
                task_id,
                role_name,
                Some(&previous),
                None,
                RoleReassignmentEventFlags::default(),
            );
            crate::wake_task_dispatch(&self.db, task_id, "task role assignment removed").await?;
            return Ok(());
        };

        self.cancel_active_execution(
            &active_execution,
            "cancelled by role reassignment",
            db::StopReason::RoleReassigned,
            &api_types::Actor::user(api_types::UserActionSource::RoleReassignment),
            db::ResumePolicy::None,
        )
        .await?;
        task = TaskRoleAssignmentRepo::remove_and_clear_review_authority(
            &*self.db,
            &previous,
            task.version,
            &now_rfc3339(),
        )
        .await?;

        let (workflow, workflow_authority) = self.workflow_and_authority_for_task(&task).await?;
        let initial_state = workflow_initial_state(&workflow)?;
        self.workflow_engine()
            .reset_to_initial_with_authority(
                &task.id,
                &initial_state,
                task.version,
                &workflow,
                &api_types::Actor::user(api_types::UserActionSource::Reassignment),
                "coder reassigned",
                workflow_authority,
            )
            .await?;

        let (effective_reset_workspace, effective_reset_worktree) = self
            .apply_reassignment_reset(&task, &active_execution, reset_workspace, reset_worktree)
            .await?;

        self.publish_role_reassigned(
            task_id,
            role_name,
            Some(&previous),
            None,
            RoleReassignmentEventFlags {
                triggered_cancellation: true,
                reset_workspace: effective_reset_workspace,
                reset_worktree: effective_reset_worktree,
                transitioned_to_todo: true,
            },
        );
        crate::wake_task_dispatch(&self.db, task_id, "task role assignment removed").await?;
        Ok(())
    }

    async fn validate_reassignable_task(&self, task_id: &str) -> Result<Task> {
        validate_required("task_id", task_id)?;
        let task = TaskRepo::get_by_id(&*self.db, task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()))?;
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
        if workflow
            .states
            .iter()
            .find(|state| state.name == task.status)
            .is_some_and(|state| state.kind == api_types::StateKind::Terminal)
        {
            return Err(ServiceError::invalid_operation(format!(
                "task {} is in terminal state {}; cannot reassign role",
                task.id, task.status
            )));
        }
        Ok(task)
    }

    async fn workflow_for_task(&self, task: &Task) -> Result<api_types::WorkflowDefinition> {
        Ok(self.workflow_and_authority_for_task(task).await?.0)
    }

    async fn workflow_and_authority_for_task(
        &self,
        task: &Task,
    ) -> Result<(api_types::WorkflowDefinition, WorkflowAuthority)> {
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            task,
            &project.workflow_definition,
            &api_types::Actor::system(api_types::SystemComponent::TaskDispatcher),
        );
        Ok((
            workflow,
            WorkflowAuthority {
                project_version: project.version,
                workflow_definition: project.workflow_definition,
            },
        ))
    }

    async fn ensure_coordination_root_role_allowed(
        &self,
        task: &Task,
        role_name: &str,
    ) -> Result<()> {
        if !super::subtask::coordination_root_has_subtasks(&self.db, task).await? {
            return Ok(());
        }
        let workflow = self.workflow_for_task(task).await?;
        let implementation_role = workflow
            .states
            .iter()
            .find(|state| state.name == default_states::IN_PROGRESS)
            .and_then(crate::workflow::effective_role)
            .or_else(|| {
                workflow
                    .states
                    .iter()
                    .find(|state| state.kind == api_types::StateKind::Active)
                    .and_then(crate::workflow::effective_role)
            });
        if implementation_role == Some(role_name) {
            return Err(ServiceError::invalid_operation(
                "root tasks with subtasks are coordination containers; assign implementation agents to the subtasks",
            ));
        }
        let aggregate_review_role = workflow.states.iter().any(|state| {
            state.role.as_deref() == Some(role_name)
                && state.kind == api_types::StateKind::Gate
                && state.canonical_phase == Some(api_types::CanonicalPhase::Review)
        });
        if aggregate_review_role {
            return Ok(());
        }
        Err(ServiceError::invalid_operation(
            "root tasks with subtasks are coordination containers; assign implementation agents to the subtasks",
        ))
    }

    /// Assignment writes share SQLite's immediate transaction boundary with
    /// Running execution creation and review-authority invalidation. Whichever
    /// mutation wins first is authoritative: a changed assignment rejects an
    /// already-running worker, while execution insertion rechecks the
    /// resulting role binding.
    async fn assign_coder_role_if_idle(
        &self,
        input: CreateTaskRoleAssignment,
        expected_previous: Option<&TaskRoleAssignment>,
        expected_task_version: i64,
    ) -> Result<(Option<TaskRoleAssignment>, TaskRoleAssignment, bool)> {
        let mut transaction = db::begin_immediate(self.db.pool()).await?;
        let is_coordination_root = sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(
                 SELECT 1
                 FROM task AS root
                 JOIN task AS child ON child.parent_task_id = root.id
                 WHERE root.id = ?
                   AND root.parent_task_id IS NULL
                   AND root.deleted_at IS NULL
                   AND child.deleted_at IS NULL
             )",
        )
        .bind(&input.task_id)
        .fetch_one(&mut *transaction)
        .await?
            != 0;
        if is_coordination_root {
            return Err(ServiceError::invalid_operation(
                "root tasks with subtasks are coordination containers; assign implementation agents to the subtasks",
            ));
        }
        let previous_row = sqlx::query(
            "SELECT id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at
             FROM task_role_assignment WHERE task_id = ? AND role_name = ?",
        )
        .bind(&input.task_id)
        .bind(&input.role_name)
        .fetch_optional(&mut *transaction)
        .await?;
        let previous = previous_row
            .map(|row| {
                let assignee_type = row
                    .try_get::<Option<String>, _>("assignee_type")?
                    .map(|value| {
                        value.parse::<AssigneeKind>().map_err(|_| {
                            ServiceError::invalid_operation(format!(
                                "invalid stored assignee type '{value}'"
                            ))
                        })
                    })
                    .transpose()?;
                Ok::<_, ServiceError>(TaskRoleAssignment {
                    id: row.try_get("id")?,
                    task_id: row.try_get("task_id")?,
                    role_name: row.try_get("role_name")?,
                    assignee_type,
                    assignee_id: row.try_get("assignee_id")?,
                    created_at: row.try_get("created_at")?,
                    updated_at: row.try_get("updated_at")?,
                })
            })
            .transpose()?;

        let expected_matches = match expected_previous {
            Some(expected) => previous.as_ref().is_some_and(|current| {
                current.id == expected.id
                    && current.task_id == expected.task_id
                    && current.role_name == expected.role_name
                    && current.assignee_type == expected.assignee_type
                    && current.assignee_id == expected.assignee_id
                    && current.created_at == expected.created_at
                    && current.updated_at == expected.updated_at
            }),
            None => previous.is_none(),
        };
        if !expected_matches {
            return Err(db::DbError::VersionConflict.into());
        }

        if same_assignment(previous.as_ref(), Some(&input)) {
            // Confirming the already-assigned agent is still an explicit
            // recovery decision (see `latest_stopped_execution_blocks_dispatch`
            // in `task_dispatcher::helpers`): its whole purpose is to refresh
            // `updated_at` so that timestamp can authorize one fresh dispatch
            // attempt past a gated Execution. Returning the stale row here
            // silently broke that escape hatch, so this must be a real write.
            let mut assignment = previous
                .clone()
                .expect("same assignment requires an existing assignment");
            sqlx::query(
                "UPDATE task_role_assignment SET updated_at = ? WHERE task_id = ? AND role_name = ?",
            )
            .bind(&input.updated_at)
            .bind(&input.task_id)
            .bind(&input.role_name)
            .execute(&mut *transaction)
            .await?;
            assignment.updated_at = input.updated_at;
            transaction.commit().await?;
            return Ok((previous, assignment, false));
        }

        let running_execution_id = sqlx::query_scalar::<_, String>(
            "SELECT id FROM execution
             WHERE task_id = ? AND status = 'running' AND (role = ? OR role = 'executor')
             ORDER BY created_at DESC, id DESC LIMIT 1",
        )
        .bind(&input.task_id)
        .bind(&input.role_name)
        .fetch_optional(&mut *transaction)
        .await?;
        if running_execution_id.is_some() {
            return Err(ServiceError::invalid_operation(format!(
                "task {} has a running {} execution; stop it before changing its assignment",
                input.task_id, input.role_name
            )));
        }

        sqlx::query(
            "INSERT INTO task_role_assignment
                (id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(task_id, role_name) DO UPDATE SET
                assignee_type = excluded.assignee_type,
                assignee_id = excluded.assignee_id,
                updated_at = excluded.updated_at",
        )
        .bind(&input.id)
        .bind(&input.task_id)
        .bind(&input.role_name)
        .bind(input.assignee_type.as_ref().map(ToString::to_string))
        .bind(input.assignee_id.as_deref())
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&mut *transaction)
        .await?;

        let result = sqlx::query(
            "UPDATE task
             SET review_passed_at = NULL, updated_at = ?, version = version + 1
             WHERE id = ? AND deleted_at IS NULL AND version = ?",
        )
        .bind(&input.updated_at)
        .bind(&input.task_id)
        .bind(expected_task_version)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            return Err(db::DbError::VersionConflict.into());
        }

        let assignment = TaskRoleAssignment {
            id: previous
                .as_ref()
                .map(|assignment| assignment.id.clone())
                .unwrap_or(input.id),
            task_id: input.task_id,
            role_name: input.role_name,
            assignee_type: input.assignee_type,
            assignee_id: input.assignee_id,
            created_at: previous
                .as_ref()
                .map(|assignment| assignment.created_at.clone())
                .unwrap_or(input.created_at),
            updated_at: input.updated_at,
        };
        transaction.commit().await?;
        Ok((previous, assignment, true))
    }

    async fn active_work_role(&self, task: &Task) -> Result<Option<String>> {
        let workflow = self.workflow_for_task(task).await?;
        Ok(workflow
            .states
            .iter()
            .find(|state| state.name == default_states::IN_PROGRESS)
            .and_then(crate::workflow::effective_role)
            .map(str::to_owned)
            .or_else(|| {
                workflow
                    .states
                    .iter()
                    .find(|state| state.kind == api_types::StateKind::Active)
                    .and_then(crate::workflow::effective_role)
                    .map(str::to_owned)
            }))
    }

    async fn active_execution_for_role(
        &self,
        task: &Task,
        role_name: &str,
    ) -> Result<Option<Execution>> {
        if self.role_for_state(task, &task.status).await?.as_deref() != Some(role_name) {
            return Ok(None);
        }
        let executions = ExecutionRepo::list_running_by_task(&*self.db, &task.id).await?;
        Ok(executions
            .into_iter()
            .find(|execution| execution.role == role_name || execution.role == "executor"))
    }

    pub(super) async fn cancel_active_execution(
        &self,
        execution: &Execution,
        reason: &str,
        stop_reason: db::StopReason,
        actor: &api_types::Actor,
        resume_policy: db::ResumePolicy,
    ) -> Result<bool> {
        self.cancel_active_execution_with_provider_policy(
            execution,
            reason,
            stop_reason,
            actor,
            resume_policy,
            false,
        )
        .await
    }

    /// Cancel an execution as part of a forced Project teardown. Unlike the
    /// ordinary user/workflow cancellation path, a provider failure is
    /// returned to the caller so the Project's authoritative rows cannot be
    /// deleted while remote work may still be running.
    pub(super) async fn cancel_active_execution_for_project_deletion(
        &self,
        execution: &Execution,
    ) -> Result<bool> {
        let actor = api_types::Actor::system(api_types::SystemComponent::General);
        self.cancel_active_execution_with_provider_policy(
            execution,
            "Project deletion requested",
            db::StopReason::TaskCancelled,
            &actor,
            db::ResumePolicy::None,
            true,
        )
        .await
    }

    async fn cancel_active_execution_with_provider_policy(
        &self,
        execution: &Execution,
        reason: &str,
        stop_reason: db::StopReason,
        actor: &api_types::Actor,
        resume_policy: db::ResumePolicy,
        provider_before_terminalization: bool,
    ) -> Result<bool> {
        let reconciliation_reason = match &stop_reason {
            db::StopReason::RoleReassigned => "stopped because task moved".to_owned(),
            _ => reason.to_owned(),
        };
        let preserve_resume_context = resume_policy == db::ResumePolicy::Manual;

        // Ordinary cancellation claims the terminal state before touching the
        // provider so a stale role/task caller cannot perform a live/runtime
        // side effect after losing the execution CAS. Project deletion is the
        // deliberate exception: it must obtain provider acknowledgement first
        // so a failed stop cannot make the next deletion retry believe that
        // the provider has already stopped.
        let actor_type = if actor.is_user() {
            "user".to_owned()
        } else if actor.is_agent() {
            "agent".to_owned()
        } else {
            "system".to_owned()
        };
        let mut terminal_candidate = execution.clone();
        let mut outcome = None;
        for _ in 0..3 {
            if provider_before_terminalization {
                // This is intentionally before the durable CAS. If the
                // provider rejects the exact execution ID, the row remains
                // running and every later force attempt is forced to retry
                // the stop request instead of deleting the Project.
                self.cancel_execution_with_provider(&terminal_candidate, reason)
                    .await?;
            }
            let terminalized_at = now_rfc3339();
            let attempt = ExecutionRepo::terminalize_with_ledger(
                &*self.db,
                execution::ledger::terminal_with_ledger(
                    TerminalizeExecution {
                        execution_id: terminal_candidate.id.clone(),
                        expected_version: terminal_candidate.execution_version,
                        lease_owner: terminal_candidate.lease_owner.clone(),
                        status: ExecutionStatus::Cancelled,
                        stop_reason: Some(Some(stop_reason.clone())),
                        stopped_by: Some(Some(actor.display())),
                        stopped_at: Some(Some(terminalized_at.clone())),
                        resume_policy: Some(Some(resume_policy.clone())),
                        agent_session_id: (!preserve_resume_context).then_some(None),
                        agent_message_id: None,
                        last_activity_at: None,
                        last_progress_at: None,
                        summary: None,
                        logs_path: None,
                        before_sha: None,
                        after_sha: None,
                        error: Some(Some(reason.to_owned())),
                        executor_config_snapshot_json: (!preserve_resume_context).then_some(None),
                        updated_at: terminalized_at,
                        actor_type: actor_type.clone(),
                        actor_id: None,
                        correlation_id: Some(terminal_candidate.id.clone()),
                        causation_id: None,
                        causation_depth: 0,
                        lease_disposition: ExecutionLeaseDisposition::Revoke,
                    },
                    Vec::new(),
                    None,
                    None,
                ),
            )
            .await
            .map_err(|error| ServiceError::invalid_operation(format!("cancel failed: {error}")))?;
            match attempt {
                ExecutionTerminalOutcome::Concurrent {
                    current: Some(current),
                } if current.status == ExecutionStatus::Running => {
                    // Heartbeats/progress also advance execution_version.  A
                    // cancellation that loses only to that liveness CAS may
                    // retry against the fresh owner/version; terminal rows
                    // are returned as ordinary concurrent losers below.
                    terminal_candidate = current;
                }
                other => {
                    outcome = Some(other);
                    break;
                }
            }
        }
        let outcome = outcome.unwrap_or(ExecutionTerminalOutcome::Concurrent {
            current: Some(terminal_candidate),
        });

        let committed_execution = match outcome {
            ExecutionTerminalOutcome::Committed { execution, .. } => execution,
            ExecutionTerminalOutcome::Concurrent { current } => {
                tracing::debug!(
                    execution_id = %execution.id,
                    current_status = ?current.as_ref().map(|execution| &execution.status),
                    "skipping stale execution cancellation after terminal CAS loss"
                );
                return Ok(false);
            }
        };

        // Ordinary cancellation contacts the provider only after winning the
        // execution CAS. Provider failures there are recoverable because the
        // durable row is already terminal. Project deletion uses the branch
        // above, which requires acknowledgement before this row changes.
        if !provider_before_terminalization
            && (self.daemon_connections.is_some() || self.task_executor.is_some())
        {
            if let Err(error) = self
                .cancel_execution_with_provider(&committed_execution, reason)
                .await
            {
                tracing::warn!(
                    execution_id = %execution.id,
                    %error,
                    "executor cancellation failed after durable cancellation"
                );
            }
        }
        self.publish(ForgeEvent {
            event_type: "task.execution_cancelled".to_owned(),
            entity_id: committed_execution.task_id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskExecutionCancelled {
                task_id: committed_execution.task_id.clone(),
                execution_id: committed_execution.id.clone(),
                reason: reason.to_owned(),
            },
        });
        self.publish(ForgeEvent {
            event_type: "reconciliation.event".to_owned(),
            entity_id: committed_execution.task_id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::ReconciliationEvent {
                task_id: Some(committed_execution.task_id.clone()),
                execution_id: Some(committed_execution.id.clone()),
                reason: reconciliation_reason,
            },
        });
        Ok(true)
    }

    async fn apply_reassignment_reset(
        &self,
        task: &Task,
        execution: &Execution,
        reset_workspace: bool,
        reset_worktree: bool,
    ) -> Result<(bool, bool)> {
        if reset_workspace {
            if task.parent_task_id.is_some() {
                // Ordered subtasks do not own their execution workspace. The
                // execution points at the coordination root's shared row, so
                // cleaning it here would remove the branch needed by the
                // remaining siblings.
                tracing::info!(
                    task_id = %task.id,
                    execution_id = %execution.id,
                    "skipping shared workspace reset for subtask reassignment"
                );
                return Ok((false, false));
            }
            let mut workspace_id = execution.workspace_id.clone();
            if workspace_id.is_none() {
                workspace_id = WorkspaceRepo::get_by_task_id(&*self.db, &task.id)
                    .await?
                    .map(|workspace| workspace.id);
            }
            if let (Some(cleanup_scheduler), Some(workspace_id)) =
                (self.cleanup_scheduler.as_ref(), workspace_id)
            {
                cleanup_scheduler.cleanup_now(workspace_id).await?;
                return Ok((true, false));
            }
            return Ok((false, false));
        }

        if reset_worktree {
            let workspace = if let Some(workspace_id) = execution.workspace_id.as_deref() {
                WorkspaceRepo::get_by_id(&*self.db, workspace_id).await?
            } else {
                WorkspaceRepo::get_by_task_id(&*self.db, &task.id).await?
            }
            .ok_or_else(|| ServiceError::not_found("workspace", task.id.clone()))?;
            let repo = RepoRepo::get_by_id(&*self.db, &workspace.repo_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("repo", workspace.repo_id.clone()))?;
            let repo_url = repo
                .local_path
                .filter(|path| !path.trim().is_empty())
                .unwrap_or(repo.remote_url);
            let repo_name = reassignment_repo_name(&repo_url);
            let workspace_root = self
                .cleanup_scheduler
                .as_ref()
                .map(|scheduler| scheduler.workspace_root().to_path_buf())
                .unwrap_or_else(|| self.workspace_root.clone());
            let manager = WorkspaceManager::new(workspace_root);
            manager
                .reset_worktree(&task.id, &repo_name)
                .await
                .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
            return Ok((false, true));
        }

        Ok((false, false))
    }

    fn workflow_engine(&self) -> WorkflowEngine {
        WorkflowEngine {
            db: Arc::clone(&self.db),
            event_bus: Arc::clone(&self.event_bus),
            review_runner: self.review_runner.clone(),
            merge_service: self.merge_service.clone(),
            cleanup_scheduler: self.cleanup_scheduler.clone(),
            task_executor: self.task_executor.clone(),
            daemon_connections: self.daemon_connections.clone(),
            workspace_exec_locks: self.workspace_exec_locks.clone(),
            terminal_activity: self.terminal_activity.clone(),
            workspace_root: self.workspace_root.clone(),
            repo_cache_locks: self.repo_cache_locks.clone(),
        }
    }

    fn publish_role_reassigned(
        &self,
        task_id: &str,
        role_name: &str,
        previous_assignment: Option<&TaskRoleAssignment>,
        new_assignment: Option<&TaskRoleAssignment>,
        flags: RoleReassignmentEventFlags,
    ) {
        self.publish(ForgeEvent {
            event_type: "task.role_reassigned".to_owned(),
            entity_id: task_id.to_owned(),
            timestamp: event_timestamp(),
            context: EventContext::TaskRoleReassigned {
                task_id: task_id.to_owned(),
                role_name: role_name.to_owned(),
                previous_assignment: previous_assignment.map(snapshot),
                new_assignment: new_assignment.map(snapshot),
                triggered_cancellation: flags.triggered_cancellation,
                reset_workspace: flags.reset_workspace,
                reset_worktree: flags.reset_worktree,
                transitioned_to_todo: flags.transitioned_to_todo,
            },
        });
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct RoleReassignmentEventFlags {
    triggered_cancellation: bool,
    reset_workspace: bool,
    reset_worktree: bool,
    transitioned_to_todo: bool,
}

pub(crate) struct RoleSweepEvent {
    pub(crate) task_id: String,
    pub(crate) role_name: String,
    pub(crate) previous_assignment: TaskRoleAssignment,
    pub(crate) new_assignment: TaskRoleAssignment,
}

fn same_assignment(
    previous: Option<&TaskRoleAssignment>,
    next: Option<&CreateTaskRoleAssignment>,
) -> bool {
    match (previous, next) {
        (Some(previous), Some(next)) => {
            previous.assignee_type == next.assignee_type && previous.assignee_id == next.assignee_id
        }
        (None, None) => true,
        _ => false,
    }
}

fn snapshot(assignment: &TaskRoleAssignment) -> RoleAssignmentSnapshot {
    RoleAssignmentSnapshot {
        assignee_type: assignment.assignee_type.as_ref().map(ToString::to_string),
        assignee_id: assignment.assignee_id.clone(),
    }
}

fn workflow_initial_state(workflow: &api_types::WorkflowDefinition) -> Result<String> {
    workflow
        .states
        .iter()
        .find(|state| state.kind == api_types::StateKind::Initial)
        .map(|state| state.name.clone())
        .ok_or_else(|| ServiceError::invalid_operation("workflow has no initial state"))
}

fn reassignment_repo_name(repo_url: &str) -> String {
    let trimmed = repo_url.trim_end_matches(['/', '\\']);
    let last_component = trimmed
        .rsplit(['/', '\\'])
        .next()
        .filter(|component| !component.is_empty())
        .unwrap_or("repo");

    last_component
        .strip_suffix(".git")
        .unwrap_or(last_component)
        .to_owned()
}
