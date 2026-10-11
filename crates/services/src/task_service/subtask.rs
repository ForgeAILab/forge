use super::*;

const COORDINATION_REVIEW_PENDING_KEY: &str = "coordination_review_pending";

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
        if !crate::task_hierarchy::subtask_is_terminal(task, &workflow) {
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
        let (_, workflow) =
            crate::task_hierarchy::coordination_root_context(&self.db, parent_task_id).await?;
        let subtasks = crate::task_hierarchy::ordered_children(&self.db, parent_task_id).await?;
        let Some(next) = crate::task_hierarchy::next_incomplete_child(&subtasks, &workflow) else {
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
        if db::task_writer::current_task_step().is_some_and(|step| step.task_id != parent_task_id) {
            // Identity-fenced (TaskCommand::fence): a root wake re-derives
            // its work from the children when it runs and is never dropped.
            self.enqueue_task_command(
                parent_task_id,
                "advance_coordination_root",
                serde_json::json!([parent_task_id]),
                false,
            )
            .await?;
            return Ok(());
        }
        if !db::task_writer::owns_task(parent_task_id) {
            return self
                .request_task_command(
                    parent_task_id,
                    "advance_coordination_root",
                    serde_json::json!([parent_task_id]),
                    false,
                )
                .await;
        }

        let mut parent = TaskRepo::get_by_id(&*self.db, parent_task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", parent_task_id.to_owned()))?;
        let project = ProjectRepo::get_by_id(&*self.db, &parent.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", parent.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
        let subtasks = crate::task_hierarchy::ordered_children(&self.db, parent_task_id).await?;
        if !crate::task_hierarchy::child_sequence_complete(&subtasks, &workflow) {
            return Err(ServiceError::invalid_operation(format!(
                "coordination root {parent_task_id} cannot enter aggregate review before every subtask is terminal"
            )));
        }

        if workflow.state_kind(&parent.status) == Some(api_types::StateKind::Terminal) {
            self.set_coordination_review_pending(parent_task_id, false)
                .await?;
            return Ok(());
        }

        crate::task_hierarchy::ensure_coordination_root_unblocked(&parent)?;

        // Traverse ready/non-review gate hops until the workflow reaches its
        // active aggregation state. Root non-review dispatch hooks are
        // deliberately skipped, so this advances coordination state without
        // launching a root planner/coder. The bound also makes malformed
        // cyclic workflow definitions fail deterministically.
        for _ in 0..workflow.states.len() {
            crate::task_hierarchy::ensure_coordination_root_unblocked(&parent)?;
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
                    bridge: Default::default(),
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

        crate::task_hierarchy::ensure_coordination_root_unblocked(&parent)?;
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
                        bridge: Default::default(),
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
                crate::task_hierarchy::ensure_coordination_root_unblocked(&parent)?;
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
        update_coordination_review_pending(&self.db, parent_task_id, pending).await
    }
}

async fn update_coordination_review_pending(
    db: &SqliteDb,
    parent_task_id: &str,
    pending: bool,
) -> Result<()> {
    let parent = TaskRepo::get_by_id(db, parent_task_id, false)
        .await?
        .ok_or_else(|| ServiceError::not_found("task", parent_task_id.to_owned()))?;
    let metadata = TaskMetadata::parse(parent.metadata_json.as_deref()).map_err(|error| {
        ServiceError::invalid_operation(format!("invalid task metadata for {}: {error}", parent.id))
    })?;
    if pending {
        if !db::task_writer::owns_task(parent_task_id) {
            db.enqueue_fenced_task_mutation(
                parent_task_id,
                db::TaskMutation::TaskMutateMetadata {
                    id: parent_task_id.to_owned(),
                    expected_version: None,
                    updated_at: now_rfc3339(),
                    mutations: vec![
                        db::TaskMetadataMutation::Set {
                            key: COORDINATION_REVIEW_PENDING_KEY.into(),
                            value: Value::Bool(true),
                        },
                        db::TaskMetadataMutation::Set {
                            key: "coordination_review_pending_id".into(),
                            value: Value::String(new_uuid_v4()),
                        },
                    ],
                },
                // A root wake flag; advance_coordination_root clears it on a
                // terminal root, so it is never dropped by a status change.
                db::task_writer::EffectFence::Identity,
            )
            .await?;
            return Ok(());
        }
        TaskRepo::mutate_metadata(
            db,
            parent_task_id,
            Some(parent.version),
            vec![
                db::TaskMetadataMutation::Set {
                    key: COORDINATION_REVIEW_PENDING_KEY.to_owned(),
                    value: Value::Bool(true),
                },
                db::TaskMetadataMutation::Set {
                    key: "coordination_review_pending_id".to_owned(),
                    value: Value::String(new_uuid_v4()),
                },
            ],
            &now_rfc3339(),
        )
        .await?;
        return Ok(());
    }
    let Some(expected) = metadata
        .extra
        .get("coordination_review_pending_id")
        .cloned()
    else {
        // Legacy markers have no identity that can distinguish this clear
        // from a newer ordered-subtask sequence.
        return Ok(());
    };
    TaskRepo::mutate_metadata(
        db,
        parent_task_id,
        None,
        vec![db::TaskMetadataMutation::CompareAndMutate {
            key: "coordination_review_pending_id".to_owned(),
            expected,
            mutations: vec![
                db::TaskMetadataMutation::Remove {
                    key: COORDINATION_REVIEW_PENDING_KEY.to_owned(),
                },
                db::TaskMetadataMutation::Remove {
                    key: "coordination_review_pending_id".to_owned(),
                },
            ],
        }],
        &now_rfc3339(),
    )
    .await?;
    Ok(())
}
