use super::*;

const COORDINATION_REVIEW_PENDING_KEY: &str = "coordination_review_pending";

pub(crate) fn subtask_is_terminal(
    task: &Task,
    project_workflow: &api_types::WorkflowDefinition,
) -> bool {
    let inherited = WorkflowEngine::resolve_subtask_workflow();
    inherited.state_kind(&task.status) == Some(api_types::StateKind::Terminal)
        || project_workflow.state_kind(&task.status) == Some(api_types::StateKind::Terminal)
}

pub(crate) async fn subtask_dispatch_ready(db: &SqliteDb, task: &Task) -> Result<bool> {
    let Some(parent_task_id) = task.parent_task_id.as_deref() else {
        return Ok(true);
    };
    let (parent, workflow) = coordination_root_context(db, parent_task_id).await?;
    if !coordination_root_allows_child_dispatch(&parent, &workflow) {
        return Ok(false);
    }
    let subtasks = TaskRepo::list_subtasks_ordered(db, parent_task_id).await?;
    Ok(subtasks
        .iter()
        .find(|candidate| !subtask_is_terminal(candidate, &workflow))
        .is_some_and(|candidate| candidate.id == task.id))
}

pub(crate) async fn ensure_subtask_dispatch_order(db: &SqliteDb, task: &Task) -> Result<()> {
    let Some(parent_task_id) = task.parent_task_id.as_deref() else {
        return Ok(());
    };
    let (parent, workflow) = coordination_root_context(db, parent_task_id).await?;
    if !coordination_root_allows_child_dispatch(&parent, &workflow) {
        return Err(ServiceError::invalid_operation(format!(
            "subtask {} cannot start while coordination root {} is blocked, terminal, or in aggregate review",
            task.id, parent_task_id
        )));
    }
    let subtasks = TaskRepo::list_subtasks_ordered(db, parent_task_id).await?;
    let next = subtasks
        .iter()
        .find(|candidate| !subtask_is_terminal(candidate, &workflow));
    if next.is_some_and(|candidate| candidate.id == task.id) {
        return Ok(());
    }
    let waiting_on = next
        .map(|candidate| candidate.id.as_str())
        .unwrap_or("the completed sequence");
    Err(ServiceError::invalid_operation(format!(
        "subtask {} cannot start yet; ordered execution is waiting on {}",
        task.id, waiting_on
    )))
}

async fn coordination_root_context(
    db: &SqliteDb,
    parent_task_id: &str,
) -> Result<(Task, api_types::WorkflowDefinition)> {
    let parent = TaskRepo::get_by_id(db, parent_task_id, false)
        .await?
        .ok_or_else(|| ServiceError::not_found("task", parent_task_id.to_owned()))?;
    let project = ProjectRepo::get_by_id(db, &parent.project_id)
        .await?
        .ok_or_else(|| ServiceError::not_found("project", parent.project_id.clone()))?;
    let workflow = WorkflowEngine::resolve_workflow_for_task(
        &parent,
        &project.workflow_definition,
        &api_types::Actor::system(api_types::SystemComponent::TaskDispatcher),
    );
    Ok((parent, workflow))
}

fn coordination_root_allows_child_dispatch(
    parent: &Task,
    workflow: &api_types::WorkflowDefinition,
) -> bool {
    if parent.blocked_json.is_some()
        || parent.failed_json.is_some()
        || parent.error_annotation.is_some()
        || parent.entry_barrier_json.is_some()
    {
        return false;
    }
    let Some(state) = workflow
        .states
        .iter()
        .find(|state| state.name == parent.status)
    else {
        return false;
    };
    if matches!(
        state.kind,
        api_types::StateKind::Backlog | api_types::StateKind::Terminal
    ) {
        return false;
    }
    !(state.kind == api_types::StateKind::Gate
        && state.canonical_phase == Some(api_types::CanonicalPhase::Review))
}

pub(crate) async fn coordination_root_has_subtasks(db: &SqliteDb, task: &Task) -> Result<bool> {
    if task.parent_task_id.is_some() {
        return Ok(false);
    }
    Ok(!TaskRepo::list_subtasks_ordered(db, &task.id)
        .await?
        .is_empty())
}

pub(crate) async fn coordination_root_sequence_complete(
    db: &SqliteDb,
    task: &Task,
    workflow: &api_types::WorkflowDefinition,
) -> Result<bool> {
    if task.parent_task_id.is_some() {
        return Ok(false);
    }
    let subtasks = TaskRepo::list_subtasks_ordered(db, &task.id).await?;
    Ok(!subtasks.is_empty()
        && subtasks
            .iter()
            .all(|subtask| subtask_is_terminal(subtask, workflow)))
}

/// A coordination root may move through ready/working states while its
/// children run, but review, merge, and terminal states represent the
/// aggregate result and therefore require the ordered sequence to be settled.
pub(crate) async fn ensure_coordination_root_target_ready(
    db: &SqliteDb,
    task: &Task,
    workflow: &api_types::WorkflowDefinition,
    target: &str,
) -> Result<()> {
    if task.parent_task_id.is_some()
        || workflow.cancellation_state.as_deref() == Some(target)
        || (!matches!(
            workflow.state_kind(target),
            Some(api_types::StateKind::Terminal)
        ) && workflow.canonical_phase_for_state(target) != api_types::CanonicalPhase::Review)
    {
        return Ok(());
    }
    let subtasks = TaskRepo::list_subtasks_ordered(db, &task.id).await?;
    if let Some(incomplete) = subtasks
        .iter()
        .find(|child| !subtask_is_terminal(child, workflow))
    {
        return Err(ServiceError::invalid_operation(format!(
            "coordination root {} cannot enter aggregate review or a terminal state while subtask {} is incomplete",
            task.id, incomplete.id
        )));
    }
    Ok(())
}

pub(crate) fn coordination_review_pending(task: &Task) -> bool {
    TaskMetadata::parse(task.metadata_json.as_deref())
        .ok()
        .and_then(|metadata| {
            metadata
                .extra
                .get(COORDINATION_REVIEW_PENDING_KEY)
                .and_then(Value::as_bool)
        })
        .unwrap_or(false)
}

impl TaskService {
    pub(crate) async fn reconcile_terminal_subtask(&self, task: &Task) {
        if task.parent_task_id.is_none() {
            return;
        }
        let project = match ProjectRepo::get_by_id(&*self.db, &task.project_id).await {
            Ok(Some(project)) => project,
            Ok(None) => return,
            Err(error) => {
                tracing::warn!(task_id = %task.id, %error, "could not resolve terminal subtask workflow");
                return;
            }
        };
        let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
        if !subtask_is_terminal(task, &workflow) {
            return;
        }
        if let Err(error) = self.advance_subtask_sequence(task).await {
            tracing::warn!(
                task_id = %task.id,
                %error,
                "terminal subtask committed but the ordered sequence could not advance"
            );
        }
    }

    pub(crate) async fn wake_next_ordered_subtask(
        &self,
        parent_task_id: &str,
        reason: &str,
    ) -> Result<bool> {
        let (_, workflow) = coordination_root_context(&self.db, parent_task_id).await?;
        let subtasks = TaskRepo::list_subtasks_ordered(&*self.db, parent_task_id).await?;
        let Some(next) = subtasks
            .iter()
            .find(|task| !subtask_is_terminal(task, &workflow))
        else {
            return Ok(false);
        };
        crate::wake_task_dispatch(&self.db, &next.id, reason).await?;
        Ok(true)
    }

    /// Wake the next ordered child, or advance the coordination root into its
    /// aggregate review once every child has reached a terminal state.
    pub(crate) async fn advance_subtask_sequence(&self, completed: &Task) -> Result<()> {
        let Some(parent_task_id) = completed.parent_task_id.as_deref() else {
            return Ok(());
        };
        if self
            .wake_next_ordered_subtask(parent_task_id, "previous ordered subtask completed")
            .await?
        {
            return Ok(());
        }
        self.set_coordination_review_pending(parent_task_id, true)
            .await?;
        crate::wake_task_dispatch(
            &self.db,
            parent_task_id,
            "ordered subtask sequence ready for aggregate review",
        )
        .await?;
        self.advance_coordination_root(parent_task_id).await
    }

    pub(crate) async fn advance_coordination_root(&self, parent_task_id: &str) -> Result<()> {
        let mut parent = TaskRepo::get_by_id(&*self.db, parent_task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", parent_task_id.to_owned()))?;
        let project = ProjectRepo::get_by_id(&*self.db, &parent.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", parent.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
        let subtasks = TaskRepo::list_subtasks_ordered(&*self.db, parent_task_id).await?;
        if subtasks.is_empty()
            || subtasks
                .iter()
                .any(|task| !subtask_is_terminal(task, &workflow))
        {
            return Err(ServiceError::invalid_operation(format!(
                "coordination root {parent_task_id} cannot enter aggregate review before every subtask is terminal"
            )));
        }

        if workflow.state_kind(&parent.status) == Some(api_types::StateKind::Terminal) {
            self.set_coordination_review_pending(parent_task_id, false)
                .await?;
            return Ok(());
        }

        ensure_coordination_root_unblocked(&parent)?;

        // Traverse ready/non-review gate hops until the workflow reaches its
        // active aggregation state. Root non-review dispatch hooks are
        // deliberately skipped, so this advances coordination state without
        // launching a root planner/coder. The bound also makes malformed
        // cyclic workflow definitions fail deterministically.
        for _ in 0..workflow.states.len() {
            ensure_coordination_root_unblocked(&parent)?;
            let Some(parent_state) = workflow
                .states
                .iter()
                .find(|state| state.name == parent.status)
            else {
                break;
            };
            if parent_state.kind == api_types::StateKind::Active
                || parent_state.canonical_phase == Some(api_types::CanonicalPhase::Review)
            {
                break;
            }
            let next_target = workflow
                .outgoing_trigger_targets(&parent.status)
                .filter_map(|(_, target)| {
                    workflow
                        .states
                        .iter()
                        .find(|state| state.name == target)
                        .filter(|state| {
                            !matches!(
                                state.kind,
                                api_types::StateKind::Backlog | api_types::StateKind::Terminal
                            ) && state.canonical_phase != Some(api_types::CanonicalPhase::Review)
                        })
                        .map(|state| {
                            (
                                state.kind == api_types::StateKind::Active,
                                state.name.clone(),
                            )
                        })
                })
                .max_by_key(|(is_active, _)| *is_active)
                .map(|(_, target)| target)
                .ok_or_else(|| {
                    ServiceError::invalid_operation(format!(
                        "coordination root {} has no route from {} to an active aggregation state",
                        parent.id, parent.status
                    ))
                })?;
            let previous_status = parent.status.clone();
            parent = Box::pin(self.transition(
                parent.id.clone(),
                next_target,
                TransitionOptions {
                    version: parent.version,
                    reason: Some("ordered subtasks completed".to_owned()),
                    triggered_by: api_types::Actor::system(
                        api_types::SystemComponent::TaskDispatcher,
                    ),
                    rejection: false,
                    defer_dispatch_seconds: None,
                },
            ))
            .await?
            .task;
            if parent.status == previous_status {
                return Err(ServiceError::invalid_operation(format!(
                    "coordination root {} did not advance from {}",
                    parent.id, parent.status
                )));
            }
        }

        ensure_coordination_root_unblocked(&parent)?;
        let parent_kind = workflow.state_kind(&parent.status);
        let parent_phase = workflow.canonical_phase_for_state(&parent.status);
        if parent_kind != Some(api_types::StateKind::Active)
            && parent_phase != api_types::CanonicalPhase::Review
        {
            return Err(ServiceError::invalid_operation(format!(
                "coordination root {} could not reach an active aggregation or review state from {}",
                parent.id, parent.status
            )));
        }

        if parent_kind == Some(api_types::StateKind::Active) {
            let review_target = workflow
                .outgoing_trigger_targets(&parent.status)
                .find_map(|(_, target)| {
                    workflow.states.iter().find_map(|state| {
                        (state.name == target
                            && state.canonical_phase == Some(api_types::CanonicalPhase::Review))
                        .then(|| state.name.clone())
                    })
                })
                .or_else(|| {
                    workflow
                        .auto_transition_target(&parent.status)
                        .map(str::to_owned)
                });
            if let Some(target) = review_target {
                parent = Box::pin(self.transition(
                    parent.id.clone(),
                    target,
                    TransitionOptions {
                        version: parent.version,
                        reason: Some("all ordered subtasks completed".to_owned()),
                        triggered_by: api_types::Actor::system(
                            api_types::SystemComponent::Workflow,
                        ),
                        rejection: false,
                        defer_dispatch_seconds: None,
                    },
                ))
                .await?
                .task;
                ensure_coordination_root_unblocked(&parent)?;
                if workflow.canonical_phase_for_state(&parent.status)
                    != api_types::CanonicalPhase::Review
                {
                    return Err(ServiceError::invalid_operation(format!(
                        "coordination root {} did not enter aggregate review from {}",
                        parent.id, parent.status
                    )));
                }
            } else {
                return Err(ServiceError::invalid_operation(format!(
                    "coordination root {} has no aggregate review transition from {}",
                    parent.id, parent.status
                )));
            }
        }

        self.set_coordination_review_pending(parent_task_id, false)
            .await?;

        crate::wake_task_dispatch(
            &self.db,
            parent_task_id,
            "ordered subtask sequence completed",
        )
        .await?;
        Ok(())
    }

    async fn set_coordination_review_pending(
        &self,
        parent_task_id: &str,
        pending: bool,
    ) -> Result<()> {
        let parent = TaskRepo::get_by_id(&*self.db, parent_task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", parent_task_id.to_owned()))?;
        let mut metadata =
            TaskMetadata::parse(parent.metadata_json.as_deref()).map_err(|error| {
                ServiceError::invalid_operation(format!(
                    "invalid task metadata for {}: {error}",
                    parent.id
                ))
            })?;
        if pending {
            metadata.extra.insert(
                COORDINATION_REVIEW_PENDING_KEY.to_owned(),
                Value::Bool(true),
            );
        } else {
            metadata.extra.remove(COORDINATION_REVIEW_PENDING_KEY);
        }
        TaskRepo::set_metadata_json(
            &*self.db,
            parent_task_id,
            metadata.to_json(),
            &now_rfc3339(),
        )
        .await?;
        Ok(())
    }
}

fn ensure_coordination_root_unblocked(parent: &Task) -> Result<()> {
    if parent.blocked_json.is_some()
        || parent.failed_json.is_some()
        || parent.error_annotation.is_some()
        || parent.entry_barrier_json.is_some()
    {
        return Err(ServiceError::invalid_operation(format!(
            "coordination root {} cannot enter aggregate review while blocked or awaiting recovery",
            parent.id
        )));
    }
    Ok(())
}

pub async fn is_root_task(db: &SqliteDb, task_id: &str) -> Result<bool> {
    let task = TaskRepo::get_by_id(db, task_id, false)
        .await?
        .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()))?;
    Ok(task.parent_task_id.is_none())
}

pub async fn is_subtask(db: &SqliteDb, task_id: &str) -> Result<bool> {
    Ok(!is_root_task(db, task_id).await?)
}

pub async fn root_for(db: &SqliteDb, task_id: &str) -> Result<Task> {
    let task = TaskRepo::get_by_id(db, task_id, false)
        .await?
        .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()))?;
    let Some(parent_task_id) = task.parent_task_id.as_deref() else {
        return Ok(task);
    };
    TaskRepo::get_by_id(db, parent_task_id, false)
        .await?
        .ok_or_else(|| ServiceError::not_found("task", parent_task_id.to_owned()))
}
