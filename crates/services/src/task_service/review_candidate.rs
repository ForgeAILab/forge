//! A review candidate may be an Agent delivery or a human branch submission.
use super::*;
use ::review::ReviewWorkspace;
use db::{Project, UpdateTask};

pub(crate) const MISSING_REVIEW_CANDIDATE: &str = "review_candidate_missing";

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct MissingReviewCandidate {
    pub task: Task,
    pub target: Option<String>,
    pub bridge: api_types::TransitionBridge,
    pub reason: String,
}

impl TaskService {
    /// Recovery of an entry that never obtained delivery evidence. This also
    /// replaces an older cached dispatch refusal with the actionable park.
    pub(crate) async fn reconcile_review_candidate(&self, task: &Task) -> Result<Task> {
        if !db::task_writer::owns_task(&task.id) {
            return self
                .request_task_command(
                    &task.id,
                    "reconcile_review_candidate",
                    json!([task.id, task.version, task.status]),
                    false,
                )
                .await;
        }
        crate::deferred_dispatch::clear_dispatch_disposition(&self.db, task).await?;
        let task = TaskRepo::get_by_id(&*self.db, &task.id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?;
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        if self
            .prepare_review_candidate(&task, &project)
            .await?
            .is_some()
        {
            return Box::pin(
                self.rerun_review(
                    Uuid::parse_str(&task.id)
                        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
                    Actor::system(api_types::SystemComponent::Workflow),
                ),
            )
            .await;
        }
        let missing = self.missing_review_candidate(&task).await?;
        match missing.target {
            Some(target) => Ok(self
                .transition(
                    task.id.clone(),
                    target,
                    TransitionOptions {
                        bridge: missing.bridge,
                        version: missing.task.version,
                        reason: Some(missing.reason),
                        triggered_by: Actor::system(api_types::SystemComponent::Workflow),
                        rejection: false,
                        defer_dispatch_seconds: None,
                    },
                )
                .await?
                .task),
            None => Ok(missing.task),
        }
    }

    /// The unconfigured gate may skip, but a cancelled empty sequence has
    /// nothing to integrate. Prove a registered branch is unchanged and clean
    /// before completing that no-op; unavailable or changed work is retained.
    pub(crate) async fn empty_cancelled_root_has_no_commits(&self, task: &Task) -> Result<bool> {
        let children = crate::task_hierarchy::children_if_root(&self.db, task).await?;
        if children.is_empty() || children.iter().any(|child| child.status != "cancelled") {
            return Ok(false);
        }
        let Some(workspace) = WorkspaceRepo::get_by_task_id(&*self.db, &task.id).await? else {
            return Ok(true);
        };
        let repo = RepoRepo::get_by_id(&*self.db, &workspace.repo_id)
            .await?
            .filter(|repo| repo.project_id == task.project_id)
            .ok_or_else(|| ServiceError::not_found("repo", workspace.repo_id.clone()))?;
        let io = self.resolve_task_workspace(&workspace).await?;
        let branch = format!("refs/heads/{}", workspace.branch);
        let Some(head) = io
            .git_read(&["rev-parse", "--verify", &branch], true)
            .await
            .map_err(ServiceError::invalid_operation)?
        else {
            return Ok(false);
        };
        let base = match workspace.before_sha {
            Some(base) => Some(base),
            None => {
                let target =
                    crate::merge_service::target_branch(&task.merge_config, &repo.default_branch)?;
                io.git_read(&["merge-base", &target, head.trim()], true)
                    .await
                    .map_err(ServiceError::invalid_operation)?
            }
        };
        if !base.is_some_and(|base| base.trim() == head.trim()) {
            return Ok(false);
        }
        Ok(io
            .git_read(&["status", "--porcelain"], false)
            .await
            .map_err(ServiceError::invalid_operation)?
            .is_some_and(|status| status.trim().is_empty()))
    }

    /// Import human delivery evidence only when no implementation run owns
    /// this Task's candidate. No Agent, process, lease or completion cascade
    /// is manufactured: the completed row records the human submission's
    /// base/head for the existing Review execution foreign key.
    pub(crate) async fn prepare_review_candidate(
        &self,
        task: &Task,
        project: &Project,
    ) -> Result<Option<Execution>> {
        let children = crate::task_hierarchy::children_if_root(&self.db, task).await?;
        if !children.is_empty() && children.iter().all(|child| child.status == "cancelled") {
            return Ok(None);
        }
        if let Some(candidate) = latest_executor_execution_for_task(&self.db, task).await? {
            return Ok(Some(candidate));
        }
        // A coordination root reviews delivered children, never a fabricated
        // root implementation. Human child submissions use their own entry.
        if !children.is_empty() {
            return Ok(None);
        }
        let Some(workspace) = WorkspaceRepo::get_by_task_id(
            &*self.db,
            task.parent_task_id.as_deref().unwrap_or(&task.id),
        )
        .await?
        else {
            return Ok(None);
        };
        if workspace.status != db::WorkspaceStatus::Ready {
            return Ok(None);
        }
        let repo = RepoRepo::get_by_id(&*self.db, &workspace.repo_id)
            .await?
            .filter(|repo| repo.project_id == task.project_id)
            .ok_or_else(|| ServiceError::not_found("repo", workspace.repo_id.clone()))?;
        let io = self.resolve_task_workspace(&workspace).await?;
        let branch_ref = format!("refs/heads/{}", workspace.branch);
        let Some(head) = io
            .git_read(&["rev-parse", "--verify", &branch_ref], true)
            .await
            .map_err(ServiceError::invalid_operation)?
        else {
            return Ok(None);
        };
        let base = match workspace.before_sha.as_deref() {
            Some(base) => base.to_owned(),
            None => {
                let target =
                    crate::merge_service::target_branch(&task.merge_config, &repo.default_branch)?;
                let Some(base) = io
                    .git_read(&["merge-base", &target, &head], true)
                    .await
                    .map_err(ServiceError::invalid_operation)?
                else {
                    return Ok(None);
                };
                base
            }
        };
        let (base, head) = (base.trim(), head.trim());
        if base == head {
            return Ok(None);
        }
        self.db
            .create_branch_review_candidate(task, project, &workspace, base, head)
            .await
            .map(Some)
            .map_err(Into::into)
    }

    /// Missing work is an owner decision, or a bounded re-plan for a lead.
    /// An entry is the trigger identity: redelivery charges it only once.
    pub(crate) async fn missing_review_candidate(
        &self,
        task: &Task,
    ) -> Result<MissingReviewCandidate> {
        if !db::task_writer::owns_task(&task.id) {
            return self
                .request_task_command(
                    &task.id,
                    "missing_review_candidate",
                    json!([task.id, task.version, task.status]),
                    false,
                )
                .await;
        }
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            task,
            &project.workflow_definition,
            &Actor::system(api_types::SystemComponent::Workflow),
        );
        let children = crate::task_hierarchy::children_if_root(&self.db, task).await?;
        let reason =
            if !children.is_empty() && children.iter().all(|child| child.status == "cancelled") {
                "nothing to review: every subtask was cancelled"
            } else {
                "nothing to review: no implementation run or branch commits were delivered"
            }
            .to_owned();
        if crate::task_hierarchy::coordination_root_lead(&self.db, task, &workflow)
            .await?
            .is_some()
        {
            if let Some(target) =
                crate::task_hierarchy::RootRolePolicy::for_workflow(&workflow).planning_state()
            {
                // Missing work belongs to this entry, never to an older
                // failed Review. Reusing that Review's receipt would let
                // every later empty entry re-plan without spending budget.
                let epoch: i64 = sqlx::query_scalar(
                    "SELECT status_epoch FROM task WHERE id=? AND version=? AND status=?",
                )
                .bind(&task.id)
                .bind(task.version)
                .bind(&task.status)
                .fetch_optional(self.db.pool())
                .await?
                .ok_or(DbError::VersionConflict)?;
                let subject = format!("entry:{}:{epoch}", task.status);
                let receipt = crate::task_hierarchy::replan_receipt_for_review(&subject);
                if self.charge_replan(task, &receipt).await?
                    == super::replan::ReplanCharge::Exhausted
                {
                    let parked = self.block_replan_exhausted(task, &reason).await?;
                    return Ok(MissingReviewCandidate {
                        task: parked,
                        target: None,
                        bridge: Default::default(),
                        reason,
                    });
                }
                return Ok(MissingReviewCandidate {
                    task: task.clone(),
                    target: Some(target.to_owned()),
                    bridge: crate::task_hierarchy::replan_bridge(
                        "aggregate_review",
                        &subject,
                        &receipt,
                        &reason,
                    ),
                    reason,
                });
            }
        }
        // Leave an unchanged park byte-for-byte. It has no Review verdict or
        // retry run: only adding work or cancellation can resolve it.
        if task
            .condition
            .read()
            .diagnostic
            .as_ref()
            .is_some_and(|annotation| annotation.blocking_reason == MISSING_REVIEW_CANDIDATE)
        {
            return Ok(MissingReviewCandidate {
                task: task.clone(),
                target: None,
                bridge: Default::default(),
                reason,
            });
        }
        let now = now_rfc3339();
        let message = format!("{reason}; send back to add work, or cancel the Task");
        let parked = TaskRepo::update(
            &*self.db,
            UpdateTask {
                id: task.id.clone(),
                expected_version: task.version,
                title: None,
                description: None,
                priority: None,
                merge_config: None,
                plan: None,
                error_annotation: Some(Some(
                    json!({"type":api_types::FailureKind::ReviewBlocked,
                "blocking_reason":MISSING_REVIEW_CANDIDATE,"message":message,"blocked_at":now})
                    .to_string(),
                )),
                blocked_json: Some(Some(
                    json!({"kind":api_types::FailureKind::ReviewBlocked,
                "reason":reason,"created_at":now,"execution_id":null})
                    .to_string(),
                )),
                failed_json: Some(None),
                task_state_config: None,
                parent_task_id: None,
                updated_at: now,
            },
        )
        .await?;
        Ok(MissingReviewCandidate {
            task: parked,
            target: None,
            bridge: Default::default(),
            reason,
        })
    }
}
