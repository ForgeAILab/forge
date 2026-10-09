use crate::{
    workspace_backend::{EmbeddedWorkspaceBackend, MergeSpec, WorkspaceBackendRouter},
    Result, ServiceError,
};
use db::{
    now_rfc3339, ExecutionRepo, RepoRepo, ReviewConformanceRepo, SqliteDb, TaskRepo,
    WorkspacePlacementRepo, WorkspaceRepo,
};
use events::EventBus;
use serde_json::Value;
use std::{
    path::{Path, PathBuf},
    sync::{Arc, RwLock, Weak},
};
use tokio::process::Command;

#[derive(Clone)]
pub struct MergeService {
    db: Arc<SqliteDb>,
    event_bus: Arc<EventBus>,
    workspace_root: PathBuf,
    integration_locks: Arc<workspace::RepoCacheLockManager>,
    workspace_backend_router: Arc<RwLock<Option<Weak<WorkspaceBackendRouter>>>>,
    test_workspace_backend: bool,
}

pub(crate) struct WorkspaceMergeInput<'a> {
    pub workspace: &'a crate::integration_effects::EffectWorkspace,
    pub worktree_path: &'a Path,
    pub repo_path: &'a Path,
    pub spec: &'a crate::workspace_backend::MergeSpec,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum MergeOutcome {
    ReviewRequired {
        reason: String,
    },
    /// The integration target moved while this Task was being reviewed.
    ///
    /// Distinct from [`MergeOutcome::ReviewRequired`] because it is not a
    /// fault: under any concurrency it is *guaranteed* whenever another Task
    /// merges first. Treating it as a merge failure spent the Task's single
    /// merge-fix retry on ordinary contention and blocked it dead on the
    /// second occurrence. The caller rebases onto the new target and asks for
    /// a fresh review instead of charging the budget.
    TargetMoved {
        reason: String,
        target_branch: String,
    },
    Done {
        before_sha: String,
        after_sha: String,
        branch: String,
    },
    /// Only an unreviewed-by-agent candidate (no review contract) takes the
    /// plain merge path that can produce this; the caller rebases it onto
    /// `target_branch` and hands any conflict back to the Worker.
    Conflict {
        details: String,
        conflict_paths: Vec<PathBuf>,
        target_branch: String,
    },
    Dirty {
        files: Vec<String>,
    },
    TargetDirty {
        files: Vec<String>,
    },
    /// A file handed to the Worker still adds Git conflict marker lines.
    UnresolvedConflictMarkers {
        paths: Vec<String>,
    },
}

/// The immutable object and delivery identity selected before a hook attempts
/// integration. On restart, target ancestry can prove completion independently
/// of later policy edits or sibling commits.
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct MergeIntent {
    pub execution_id: String,
    pub workspace_id: String,
    pub candidate_sha: String,
    pub target_branch: String,
}

/// Git facts a review-authority carry needs about the Task's current HEAD.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReviewCarryFacts {
    Ready {
        /// Worktree HEAD, a descendant of `base_sha`.
        commit_sha: String,
        /// Tip of the integration target the HEAD is built on.
        base_sha: String,
        /// Every path HEAD changes relative to `base_sha`, sorted.
        changed_paths: Vec<String>,
    },
    /// The candidate cannot be integrated without a fresh review.
    Unavailable { reason: String },
}

impl MergeService {
    pub fn new(db: Arc<SqliteDb>, event_bus: Arc<EventBus>, workspace_root: PathBuf) -> Self {
        Self {
            db,
            event_bus,
            workspace_root,
            integration_locks: Arc::new(workspace::RepoCacheLockManager::new()),
            workspace_backend_router: Arc::new(RwLock::new(None)),
            test_workspace_backend: false,
        }
    }

    /// Handed-off conflict files whose current `HEAD` still adds Git conflict
    /// markers relative to the target branch. Empty when the Task was never
    /// handed a conflict or has no workspace.
    /// Lets the workflow reject an unresolved repair before it is re-reviewed;
    /// [`MergeService::merge`] repeats the same check as the final guard.
    pub async fn unresolved_handoff_markers(&self, task_id: &str) -> Result<Vec<String>> {
        let task = TaskRepo::get_by_id(&*self.db, task_id, false)
            .await?
            .ok_or_else(|| ServiceError::NotFound {
                entity: "task",
                id: task_id.to_owned(),
            })?;
        let Some(execution) =
            crate::task_service::latest_executor_execution_for_task(&self.db, &task).await?
        else {
            return Ok(Vec::new());
        };
        let workspace = resolve_delivery_workspace(&self.db, &execution).await?;
        let Some(repo) = RepoRepo::get_by_id(&*self.db, &workspace.repo_id)
            .await?
            .filter(|repo| repo.project_id == task.project_id)
        else {
            return Ok(Vec::new());
        };
        let target_branch = target_branch(&task.merge_config, &repo.default_branch)?;
        let resolved = EmbeddedWorkspaceBackend::resolve_workspace(
            self.workspace_backend_router()?.as_ref(),
            &self.db,
            &workspace,
            &self.workspace_root,
        )
        .await?;
        if resolved.placement.owner_kind == db::PlacementOwnerKind::Server {
            let worktree_path = resolved.embedded_path()?;
            if !matches!(
                crate::task_service::workspace::worktree_readiness(&worktree_path).await?,
                crate::task_service::workspace::WorktreeReadiness::Ready
            ) {
                return Ok(Vec::new());
            }
        }
        let transitions = db::TransitionLogRepo::list_by_task(&*self.db, task_id).await?;
        let handed_off = crate::workflow::handed_off_conflict_paths(&transitions);
        let markers = resolved
            .git_query(
                api_types::WorkspaceGitQuery::MarkerPaths {
                    base: target_branch,
                    head: "HEAD".to_owned(),
                },
                false,
            )
            .await?
            .unwrap_or_default();
        Ok(markers
            .lines()
            .filter(|path| handed_off.iter().any(|handed| handed == *path))
            .map(str::to_owned)
            .collect())
    }

    /// Read-only Git facts for carrying a passed review across a mechanical
    /// rebase or conflict repair. Anything that is not a clean, marker-free
    /// descendant of the current target tip is `Unavailable`, so the caller
    /// falls back to a full review.
    pub async fn review_carry_facts(&self, task_id: &str) -> Result<ReviewCarryFacts> {
        let unavailable = |reason: &str| ReviewCarryFacts::Unavailable {
            reason: reason.to_owned(),
        };
        let task = TaskRepo::get_by_id(&*self.db, task_id, false)
            .await?
            .ok_or_else(|| ServiceError::NotFound {
                entity: "task",
                id: task_id.to_owned(),
            })?;
        if task.parent_task_id.is_some() {
            return Ok(unavailable("subtasks do not integrate"));
        }
        let Some(execution) =
            crate::task_service::latest_executor_execution_for_task(&self.db, &task).await?
        else {
            return Ok(unavailable("no executor execution"));
        };
        let workspace = resolve_delivery_workspace(&self.db, &execution).await?;
        let Some(repo) = RepoRepo::get_by_id(&*self.db, &workspace.repo_id)
            .await?
            .filter(|repo| repo.project_id == task.project_id)
        else {
            return Ok(unavailable("repository not found"));
        };
        let target_branch = target_branch(&task.merge_config, &repo.default_branch)?;
        let resolved = EmbeddedWorkspaceBackend::resolve_workspace(
            self.workspace_backend_router()?.as_ref(),
            &self.db,
            &workspace,
            &self.workspace_root,
        )
        .await?;
        if resolved.placement.owner_kind == db::PlacementOwnerKind::Server {
            let worktree_path = resolved.embedded_path()?;
            if !matches!(
                crate::task_service::workspace::worktree_readiness(&worktree_path).await?,
                crate::task_service::workspace::WorktreeReadiness::Ready
            ) {
                return Ok(unavailable("workspace is unavailable"));
            }
        }
        let worktree_path = &resolved;
        if resolved.backend.describe(&resolved.placement).await?.dirty {
            return Ok(unavailable("worktree has uncommitted changes"));
        }
        let commit_sha = ::review::contract::git_read(worktree_path, &["rev-parse", "HEAD"])
            .await
            .map_err(ServiceError::invalid_operation)?
            .trim()
            .to_owned();
        let Ok(base_sha) = ::review::contract::git_read(
            worktree_path,
            &[
                "rev-parse",
                "--verify",
                &format!("refs/heads/{target_branch}"),
            ],
        )
        .await
        else {
            return Ok(unavailable("integration target is unreadable"));
        };
        let base_sha = base_sha.trim().to_owned();
        // The recorded base must be an ancestor of HEAD, or the later
        // fast-forward would integrate something other than what was checked.
        if ::review::contract::git_read(
            worktree_path,
            &["merge-base", "--is-ancestor", &base_sha, &commit_sha],
        )
        .await
        .is_err()
        {
            return Ok(unavailable("the candidate is not based on the target tip"));
        }
        if !self.unresolved_handoff_markers(task_id).await?.is_empty() {
            return Ok(unavailable("conflict markers remain in handed-off files"));
        }
        let Ok(changed_paths) =
            ::review::contract::candidate_changed_paths(worktree_path, &base_sha, &commit_sha)
                .await
        else {
            return Ok(unavailable("changed paths are unreadable"));
        };
        Ok(ReviewCarryFacts::Ready {
            commit_sha,
            base_sha,
            changed_paths,
        })
    }

    pub fn new_for_test(
        db: Arc<SqliteDb>,
        event_bus: Arc<EventBus>,
        workspace_root: PathBuf,
    ) -> Self {
        Self {
            test_workspace_backend: true,
            ..Self::new(db, event_bus, workspace_root)
        }
    }

    pub fn set_workspace_backend_router(&self, router: Arc<WorkspaceBackendRouter>) {
        match self.workspace_backend_router.write() {
            Ok(mut current) => *current = Some(Arc::downgrade(&router)),
            Err(error) => {
                tracing::warn!(%error, "merge workspace backend router lock poisoned");
            }
        }
    }

    pub(crate) fn workspace_backend_router(&self) -> Result<Arc<WorkspaceBackendRouter>> {
        let router = self.workspace_backend_router.read().map_err(|error| {
            ServiceError::invalid_operation(format!(
                "merge workspace backend router lock poisoned: {error}"
            ))
        })?;
        if let Some(router) = router.as_ref().and_then(Weak::upgrade) {
            return Ok(router);
        }
        if !self.test_workspace_backend {
            return Err(ServiceError::invalid_operation(
                "merge workspace backend router is not configured",
            ));
        }
        Ok(Arc::new(WorkspaceBackendRouter::new(Arc::new(
            EmbeddedWorkspaceBackend::new(
                Arc::clone(&self.db),
                Arc::new(self.clone()),
                self.workspace_root.clone(),
            ),
        ))))
    }

    async fn ensure_delivery_workspace(
        &self,
        task: &db::Task,
        execution: &db::Execution,
    ) -> Result<(db::Workspace, crate::workspace_backend::ResolvedWorkspace)> {
        let expected = resolve_delivery_workspace(&self.db, execution).await?;
        let router = self.workspace_backend_router()?;
        let daemon_owned = WorkspacePlacementRepo::get_by_workspace_id(&*self.db, &expected.id)
            .await?
            .is_some_and(|placement| placement.owner_kind == db::PlacementOwnerKind::Daemon);
        let workspace = if daemon_owned {
            expected.clone()
        } else {
            crate::task_service::workspace::ensure_valid(
                &self.db,
                &self.workspace_root,
                task,
                expected.clone(),
                Some(Arc::clone(&self.integration_locks)),
                false,
                &router,
            )
            .await?
        };
        if workspace.id != expected.id {
            return Err(ServiceError::conflict(
                "validated workspace differs from the implementation execution workspace",
            ));
        }
        let resolved = EmbeddedWorkspaceBackend::resolve_workspace(
            &router,
            &self.db,
            &workspace,
            &self.workspace_root,
        )
        .await?;
        Ok((workspace, resolved))
    }

    pub async fn merge(&self, task_id: impl Into<String>) -> Result<MergeOutcome> {
        let task_id = task_id.into();
        let task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
        if task.parent_task_id.is_some() {
            return Err(ServiceError::invalid_operation(
                "subtasks do not merge; only root tasks merge to the default branch",
            ));
        }
        let execution = crate::task_service::latest_executor_execution_for_task(&self.db, &task)
            .await?
            .ok_or_else(|| {
                ServiceError::invalid_operation(format!("task {task_id} has no executor execution"))
            })?;
        let (workspace, resolved) = self.ensure_delivery_workspace(&task, &execution).await?;
        let repo = RepoRepo::get_by_id(&*self.db, &workspace.repo_id)
            .await?
            .filter(|repo| repo.project_id == task.project_id)
            .ok_or_else(|| ServiceError::not_found("repo", workspace.repo_id.clone()))?;
        let transitions = db::TransitionLogRepo::list_by_task(&*self.db, &task_id).await?;
        let target_branch = target_branch(&task.merge_config, &repo.default_branch)?;
        let expected_target_sha = if resolved.placement.owner_kind == db::PlacementOwnerKind::Server
        {
            String::new()
        } else {
            resolved
                .git_query(
                    api_types::WorkspaceGitQuery::TargetHead {
                        branch: target_branch.clone(),
                    },
                    false,
                )
                .await?
                .ok_or_else(|| ServiceError::invalid_operation("merge target has no HEAD"))?
                .trim()
                .to_owned()
        };
        if !expected_target_sha.is_empty() {
            if let Some(hook) = crate::workflow::engine::durable::current_hook(&task_id) {
                db::note_integration_target(&hook.step.id, hook.index, None, &expected_target_sha);
            }
        }
        let spec = MergeSpec {
            target_branch,
            // Embedded integration retains its locked review-contract precondition.
            // Daemon integration also fences the recorded base on its owner.
            expected_target_sha,
            handed_off_paths: crate::workflow::handed_off_conflict_paths(&transitions)
                .into_iter()
                .collect(),
        };
        resolved
            .backend
            .merge(&resolved.placement, &spec)
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn hook_merge_intent(&self, task_id: &str) -> Result<MergeIntent> {
        let task = TaskRepo::get_by_id(&*self.db, task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()))?;
        let execution = crate::task_service::latest_executor_execution_for_task(&self.db, &task)
            .await?
            .ok_or_else(|| {
                ServiceError::invalid_operation("merge has no implementation execution")
            })?;
        let (workspace, resolved) = self.ensure_delivery_workspace(&task, &execution).await?;
        let repo = RepoRepo::get_by_id(&*self.db, &workspace.repo_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("repo", workspace.repo_id.clone()))?;
        let candidate_sha = resolved
            .git_query(api_types::WorkspaceGitQuery::Head, false)
            .await?
            .ok_or_else(|| ServiceError::invalid_operation("merge candidate has no HEAD"))?
            .trim()
            .to_owned();
        Ok(MergeIntent {
            execution_id: execution.id,
            workspace_id: workspace.id,
            candidate_sha,
            target_branch: target_branch(&task.merge_config, &repo.default_branch)?,
        })
    }

    pub(crate) async fn completed_hook_merge(
        &self,
        intent: &MergeIntent,
    ) -> Result<Option<MergeOutcome>> {
        let execution = ExecutionRepo::get_by_id(&*self.db, &intent.execution_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("execution", intent.execution_id.clone()))?;
        let workspace = WorkspaceRepo::get_by_id(&*self.db, &intent.workspace_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("workspace", intent.workspace_id.clone()))?;
        let resolved = EmbeddedWorkspaceBackend::resolve_workspace(
            self.workspace_backend_router()?.as_ref(),
            &self.db,
            &workspace,
            &self.workspace_root,
        )
        .await?;
        let target = resolved
            .git_query(
                api_types::WorkspaceGitQuery::ResolveRef {
                    reference: intent.target_branch.clone(),
                },
                false,
            )
            .await?
            .ok_or_else(|| ServiceError::invalid_operation("merge target has no HEAD"))?
            .trim()
            .to_owned();
        if resolved
            .git_query(
                api_types::WorkspaceGitQuery::IsAncestor {
                    base: intent.candidate_sha.clone(),
                    head: target.clone(),
                },
                true,
            )
            .await?
            .is_none()
        {
            return Ok(None);
        }
        // Reviewed integration fast-forwards the target to this exact
        // object, so the candidate is the merge result. The target's tip may
        // since be a sibling's commit and is never recorded here.
        let merged_sha = intent.candidate_sha.clone();
        self.record_merge_execution_evidence(
            &execution.id,
            crate::integration_effects::MergeExecutionEvidence {
                before_sha: None,
                after_sha: Some(merged_sha.clone()),
            },
        )
        .await?;
        Ok(Some(MergeOutcome::Done {
            before_sha: intent.candidate_sha.clone(),
            after_sha: merged_sha,
            branch: intent.target_branch.clone(),
        }))
    }

    pub(crate) async fn merge_workspace(
        &self,
        task_id: &str,
        input: WorkspaceMergeInput<'_>,
    ) -> Result<MergeOutcome> {
        self.merge_inner(task_id.to_owned(), input).await
    }

    async fn merge_inner(
        &self,
        task_id: String,
        input: WorkspaceMergeInput<'_>,
    ) -> Result<MergeOutcome> {
        let _ = self.event_bus.receiver_count();
        let task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .ok_or_else(|| ServiceError::NotFound {
                entity: "task",
                id: task_id.clone(),
            })?;
        if task.parent_task_id.is_some() {
            return Err(ServiceError::invalid_operation(
                "subtasks do not merge; only root tasks merge to the default branch",
            ));
        }
        let execution = crate::task_service::latest_executor_execution_for_task(&self.db, &task)
            .await?
            .ok_or_else(|| ServiceError::InvalidOperation {
                message: format!("task {task_id} has no executor execution"),
            })?;
        let workspace = resolve_delivery_workspace(&self.db, &execution).await?;
        if input.workspace.workspace_id != workspace.id {
            return Err(ServiceError::conflict(
                "merge placement differs from the implementation execution workspace",
            ));
        }
        let repo_id = workspace.repo_id.as_str();
        RepoRepo::get_by_id(&*self.db, repo_id)
            .await?
            .filter(|repo| repo.project_id == task.project_id)
            .ok_or_else(|| ServiceError::NotFound {
                entity: "repo",
                id: repo_id.to_owned(),
            })?;
        let target_branch = input.spec.target_branch.clone();
        let repo_path = input.repo_path;
        let worktree_path = input.worktree_path;

        let _integration_lock = self.integration_locks.acquire(repo_id).await;

        if let Some(outcome) =
            crate::integration_effects::merge::merge_cleanliness(worktree_path, repo_path).await?
        {
            return Ok(outcome);
        }
        let unresolved =
            unresolved_handed_off_markers(&self.db, &task_id, worktree_path, &target_branch)
                .await?;
        if !unresolved.is_empty() {
            return Ok(MergeOutcome::UnresolvedConflictMarkers { paths: unresolved });
        }

        if let Some(outcome) = crate::integration_effects::merge::expected_target(
            repo_path,
            &target_branch,
            &input.spec.expected_target_sha,
        )
        .await?
        {
            return Ok(outcome);
        }

        let (before_sha, worktree_sha) =
            crate::integration_effects::merge::merge_heads(repo_path, worktree_path).await?;
        self.record_merge_execution_evidence(
            &execution.id,
            crate::integration_effects::MergeExecutionEvidence {
                before_sha: Some(worktree_sha.clone()),
                after_sha: None,
            },
        )
        .await?;

        let observed_target =
            crate::integration_effects::merge::target_tip(repo_path, &target_branch).await?;
        // Critical before the intent exists: a preempt between the started
        // intent and its receipt would orphan it for the next admission.
        self.db.protect_step_integration().await?;
        let owner = crate::integration_owner::ServerIntegrationOwner::new(self.db.clone());
        let receipt_guard = match owner.admit_task_step(input.workspace, db::IntegrationOperationKind::Merge,
            serde_json::json!({"workspace":input.workspace,"target_branch":target_branch,"expected_head_sha":worktree_sha,"expected_target_sha":observed_target})).await? {
            None => None,
            Some(db::IntegrationEffectAdmission::Started(guard)) => Some(guard),
            Some(db::IntegrationEffectAdmission::Replay(receipt)) => {
                let receipt: crate::integration_owner::OwnerMergeReceipt = serde_json::from_value(receipt.result).map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
                return match receipt {
                    crate::integration_owner::OwnerMergeReceipt::Completed { outcome } => Ok(outcome),
                    _ => Err(ServiceError::invalid_operation("prior merge effect did not complete")),
                };
            }
            Some(db::IntegrationEffectAdmission::Refused(reason)) => return Err(ServiceError::invalid_operation(format!("integration owner refused {reason:?}"))),
        };
        let result = async {
            self.db.protect_step_integration().await?;
            let review_guard = match self.db.lock_review_integration(&task_id).await {
                Ok(guard) => guard,
                Err(db::DbError::Check(reason)) => {
                    return Ok(MergeOutcome::ReviewRequired { reason });
                }
                Err(error) => return Err(error.into()),
            };
            let target_sha =
                crate::integration_effects::merge::target_tip(repo_path, &target_branch).await?;
            if let Some(hook) = crate::workflow::engine::durable::current_hook(&task_id) {
                db::note_integration_target(
                    &hook.step.id,
                    hook.index,
                    Some(&worktree_sha),
                    &target_sha,
                );
            }
            let task_branch = workspace::task_branch_name(&task_id);
            let effect = crate::integration_effects::merge::MergeEffectInput {
                workspace: input.workspace,
                worktree_path,
                repo_path,
                target_branch: &target_branch,
                task_branch: &task_branch,
                diagnostic_entity_id: &task_id,
                before_sha: &before_sha,
                expected_head_sha: &worktree_sha,
                observed_target_sha: &target_sha,
                reviewed: review_guard.candidate.as_ref().map(|candidate| {
                    crate::integration_effects::merge::ReviewedMergeObject {
                        commit_sha: candidate.commit_sha.clone(),
                        base_sha: candidate.base_sha.clone(),
                    }
                }),
            };
            let already_merged =
                match crate::integration_effects::merge::validate_merge_candidate(&effect).await? {
                    crate::integration_effects::merge::MergeCandidateOutcome::Ready {
                        already_merged,
                    } => already_merged,
                    crate::integration_effects::merge::MergeCandidateOutcome::Refused(outcome) => {
                        return Ok(outcome)
                    }
                };
            let applied =
                crate::integration_effects::merge::apply_merge(&effect, already_merged).await?;
            // Preserve the original guard lifetime on the exact-object refusal.
            if matches!(
                applied,
                crate::integration_effects::merge::MergeApplyOutcome::ExactObjectMismatch
            ) {
                return crate::integration_effects::merge::merge_result(
                    &effect,
                    already_merged,
                    applied,
                )
                .await;
            }
            review_guard.release().await?;
            crate::integration_effects::merge::merge_result(&effect, already_merged, applied).await
        }
        .await;
        if let Some(guard) = receipt_guard {
            let (receipt, state) = match &result {
                Ok(outcome) => (
                    crate::integration_owner::OwnerMergeReceipt::Completed {
                        outcome: outcome.clone(),
                    },
                    db::IntegrationOperationState::Succeeded,
                ),
                Err(error) => (
                    crate::integration_owner::OwnerMergeReceipt::Infrastructure {
                        message: crate::integration_effects::check::tail_bytes(
                            &error.to_string(),
                            4096,
                        ),
                    },
                    // Terminal: the step's own recovery re-reads Git and
                    // decides whether to repeat. An uncertain receipt would
                    // keep the intent and refuse every later merge here.
                    db::IntegrationOperationState::Failed,
                ),
            };
            // The receipt is a record of the effect, never its result.
            if let Err(error) = guard.record(serde_json::json!(receipt), state).await {
                tracing::warn!(target: "services::merge_service", task_id = %task_id, %error, "merge attempt receipt was not recorded");
            }
        }
        let outcome = result?;
        if let MergeOutcome::Done { after_sha, .. } = &outcome {
            self.record_merge_execution_evidence(
                &execution.id,
                crate::integration_effects::MergeExecutionEvidence {
                    before_sha: None,
                    after_sha: Some(after_sha.clone()),
                },
            )
            .await?;
        }
        Ok(outcome)
    }

    /// Today's Task-step projection, including the original repository
    /// transaction and its Project/usage trigger effects.
    async fn record_merge_execution_evidence(
        &self,
        execution_id: &str,
        evidence: crate::integration_effects::MergeExecutionEvidence,
    ) -> Result<()> {
        ExecutionRepo::update(
            &*self.db,
            db::UpdateExecution {
                id: execution_id.to_owned(),
                status: None,
                stop_reason: None,
                stopped_by: None,
                resume_policy: None,
                stopped_at: None,
                agent_session_id: None,
                agent_message_id: None,
                last_activity_at: None,
                summary: None,
                logs_path: None,
                before_sha: evidence.before_sha.map(Some),
                after_sha: evidence.after_sha.map(Some),
                error: None,
                executor_config_snapshot_json: None,
                updated_at: now_rfc3339(),
            },
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn resolve_repo_source(&self, repo: &db::Repo) -> Result<String> {
        if let Some(local_path) = repo
            .local_path
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            if Path::new(local_path).exists() {
                return Ok(local_path.to_owned());
            }
        }
        let managed = self.managed_repo_path(&repo.id);
        if managed.exists() {
            return Ok(managed.to_string_lossy().into_owned());
        }
        let remote_url = repo
            .remote_url
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                ServiceError::invalid_operation(
                    "repository has no available local path or remote URL",
                )
            })?;
        ensure_managed_clone(remote_url, &managed).await
    }

    fn managed_repo_path(&self, repo_id: &str) -> PathBuf {
        self.workspace_root.join(".repos").join(repo_id)
    }
}

/// Handed-off conflict files where `HEAD` still adds conflict markers.
async fn unresolved_handed_off_markers(
    db: &SqliteDb,
    task_id: &str,
    worktree_path: &Path,
    target_branch: &str,
) -> Result<Vec<String>> {
    let transitions = db::TransitionLogRepo::list_by_task(db, task_id).await?;
    let handed_off_paths = crate::workflow::handed_off_conflict_paths(&transitions);
    crate::integration_effects::merge::unresolved_marker_paths(
        worktree_path,
        target_branch,
        &handed_off_paths,
    )
    .await
}

/// Resolve the exact worktree pinned to the selected implementation attempt.
/// Child executions may point at the coordination root's shared Workspace,
/// but the execution reference remains the immutable repository provenance.
async fn resolve_delivery_workspace(
    db: &SqliteDb,
    execution: &db::Execution,
) -> Result<db::Workspace> {
    let workspace_id = execution.workspace_id.as_deref().ok_or_else(|| {
        ServiceError::invalid_operation("executor execution missing workspace_id")
    })?;
    WorkspaceRepo::get_by_id(db, workspace_id)
        .await?
        .ok_or_else(|| ServiceError::not_found("workspace", workspace_id.to_owned()))
}

async fn ensure_managed_clone(remote_url: &str, clone_path: &Path) -> Result<String> {
    if clone_path.exists() {
        return Ok(clone_path.to_string_lossy().into_owned());
    }
    if let Some(parent) = clone_path.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(|error| {
            ServiceError::invalid_operation(format!("failed to create repo cache: {error}"))
        })?;
    }
    let output = Command::new("git")
        .args(["clone", remote_url, &clone_path.to_string_lossy()])
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .output()
        .await
        .map_err(|error| {
            ServiceError::invalid_operation(format!("failed to clone repo: {error}"))
        })?;
    if !output.status.success() {
        return Err(ServiceError::invalid_operation(format!(
            "failed to clone repo from {remote_url}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    Ok(clone_path.to_string_lossy().into_owned())
}

fn target_branch(merge_config: &Option<String>, repo_default_branch: &str) -> Result<String> {
    if let Some(merge_config) = merge_config {
        let value: Value =
            serde_json::from_str(merge_config).map_err(|error| ServiceError::InvalidOperation {
                message: format!("invalid merge_config: {error}"),
            })?;
        if let Some(target_branch) = value
            .get("target_branch")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|target_branch| !target_branch.is_empty())
        {
            return Ok(target_branch.to_owned());
        }
    }
    let repo_default_branch = repo_default_branch.trim();
    if repo_default_branch.is_empty() {
        Ok("main".to_owned())
    } else {
        Ok(repo_default_branch.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use db::{
        create_sqlite_pool, new_uuid_v4, run_migrations, CreateExecution, CreateProject,
        CreateRepo, CreateTask, CreateWorkspace, ExecutionStatus, ProjectRepo, RepoRepo,
        UpdateProject, WorkspaceStatus,
    };
    use std::process::Stdio;
    use tempfile::TempDir;
    use tokio::process::Command;

    async fn sqlite_db() -> Arc<SqliteDb> {
        let pool = create_sqlite_pool("sqlite::memory:")
            .await
            .expect("pool creates");
        run_migrations(&pool).await.expect("migrations run");
        Arc::new(SqliteDb::new(pool))
    }

    async fn run_git(path: &Path, args: &[&str]) {
        let output = Command::new("git")
            .args(args)
            .current_dir(path)
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await
            .expect("git runs");
        assert!(
            output.status.success(),
            "git {} failed\nstdout: {}\nstderr: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    async fn setup_repo(temp: &TempDir) -> std::path::PathBuf {
        let repo_path = temp.path().join("repo");
        std::fs::create_dir_all(&repo_path).expect("repo dir creates");
        git::init(&repo_path).await.expect("repo initializes");
        run_git(&repo_path, &["checkout", "-B", "main"]).await;
        std::fs::write(repo_path.join("file.txt"), "base\n").expect("file writes");
        git::commit_all(&repo_path, "initial")
            .await
            .expect("initial commit creates");
        repo_path
    }

    async fn seed_merge_rows(
        db: &SqliteDb,
        repo_path: &Path,
        worktree_path: &Path,
        task_id: &str,
    ) -> String {
        let now = now_rfc3339();
        let project_id = new_uuid_v4();
        let repo_id = new_uuid_v4();
        let workspace_id = new_uuid_v4();
        ProjectRepo::create(
            db,
            CreateProject {
                id: project_id.clone(),
                name: "Forge".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("project creates");
        RepoRepo::create(
            db,
            CreateRepo {
                id: repo_id.clone(),
                project_id: project_id.clone(),
                name: "repo".to_owned(),
                remote_url: Some(repo_path.to_string_lossy().into_owned()),
                local_path: Some(repo_path.to_string_lossy().into_owned()),
                default_branch: "main".to_owned(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("repo creates");
        ProjectRepo::update_at_version(
            db,
            UpdateProject {
                id: project_id.clone(),
                name: None,
                settings: None,
                primary_repo_id: Some(Some(repo_id.clone())),
                paused_at: None,
                updated_at: now_rfc3339(),
            },
            ProjectRepo::get_by_id(db, &project_id)
                .await
                .expect("fixture Project lookup")
                .expect("fixture Project exists")
                .version,
            None,
        )
        .await
        .expect("project primary repo updates");
        TaskRepo::create(
            db,
            CreateTask {
                id: task_id.to_owned(),
                project_id: RepoRepo::get_by_id(db, &repo_id)
                    .await
                    .expect("repo loads")
                    .expect("repo exists")
                    .project_id,
                parent_task_id: None,
                subtask_order: None,
                assignee_type: None,
                assignee_id: None,
                title: "task".to_owned(),
                description: None,
                task_type: "task".to_owned(),
                status: "review".to_owned(),
                is_automation: false,
                priority: 0,
                task_state_config: None,
                merge_config: None,
                plan: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("task creates");
        WorkspaceRepo::create(
            db,
            CreateWorkspace {
                id: workspace_id.clone(),
                task_id: task_id.to_owned(),
                repo_id,
                worktree_path: worktree_path.to_string_lossy().into_owned(),
                branch: workspace::task_branch_name(task_id),
                status: WorkspaceStatus::Ready,
                before_sha: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("workspace creates");
        let execution_id = new_uuid_v4();
        ExecutionRepo::create(
            db,
            CreateExecution {
                id: execution_id.clone(),
                task_id: task_id.to_owned(),
                agent_id: None,
                role: "executor".to_owned(),
                status: ExecutionStatus::Completed,
                stop_reason: None,
                stopped_by: None,
                resume_policy: None,
                stopped_at: None,
                parent_execution_id: None,
                agent_session_id: None,
                agent_message_id: None,
                last_activity_at: None,
                summary: None,
                logs_path: None,
                before_sha: None,
                after_sha: None,
                error: None,
                executor_config_snapshot_json: None,
                workspace_id: Some(workspace_id),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("execution creates");
        execution_id
    }

    /// Today's server-local Task-step merge, bound to its shadow attempt: it
    /// writes one receipt, and an earlier Task-step effect that started and
    /// never wrote a receipt (crash or dropped future) is settled by the next
    /// claim's merge instead of refusing it with `ReconciliationRequired`.
    #[tokio::test]
    async fn task_step_merge_records_a_receipt_and_is_never_refused_by_an_orphaned_intent() {
        use db::{IntegrationQueueRepo, TaskStepRepo};
        let db = sqlite_db().await;
        let event_bus = Arc::new(EventBus::new(16));
        let temp = TempDir::new().expect("temp creates");
        let repo_path = setup_repo(&temp).await;
        let task_id = new_uuid_v4();
        let worktree_path = temp.path().join("worktrees").join(&task_id).join("repo");
        git::create_worktree(
            &repo_path,
            &workspace::task_branch_name(&task_id),
            &worktree_path,
        )
        .await
        .expect("worktree creates");
        std::fs::write(worktree_path.join("feature.txt"), "one\n").expect("feature writes");
        git::commit_all(&worktree_path, "feature")
            .await
            .expect("feature commits");
        seed_merge_rows(&db, &repo_path, &worktree_path, &task_id).await;
        let task = TaskRepo::get_by_id(&*db, &task_id, false)
            .await
            .expect("task loads")
            .expect("task exists");
        let repo_id: String = sqlx::query_scalar("SELECT repo_id FROM workspace WHERE task_id=?")
            .bind(&task_id)
            .fetch_one(db.pool())
            .await
            .expect("repo id reads");
        let queue = db
            .create_or_get_integration_queue(&repo_id, "main")
            .await
            .expect("queue creates");
        let attempt = db
            .admit_integration_attempt(db::IntegrationAttempt::new(
                Some(queue.id.clone()),
                task_id.clone(),
                task.project_id.clone(),
                format!("shadow:{task_id}:0"),
                task.status.clone(),
                0,
                task.version,
            ))
            .await
            .expect("shadow attempt admits");
        db.enqueue_step(&db::EnqueueTaskStep {
            id: "merge-step".into(),
            task_id: task_id.clone(),
            kind: "hooks".into(),
            payload_json: "{}".into(),
            causation_step_id: None,
            causation_key: "merge-step".into(),
            chain_id: "merge-chain".into(),
            chain_position: 1,
            expected_status: task.status.clone(),
            expected_version: task.version,
            expected_epoch: Some(0),
            lane: "long".into(),
            available_at: now_rfc3339(),
        })
        .await
        .expect("step enqueues");
        let service =
            MergeService::new_for_test(Arc::clone(&db), event_bus, temp.path().to_path_buf());
        let claim = || async {
            sqlx::query("UPDATE task_step SET lease_until='2000-01-01T00:00:00Z' WHERE id='merge-step' AND status='claimed'")
                .execute(db.pool())
                .await
                .expect("lease expires");
            db.claim_step("worker", Some(&task_id), "2099-01-01T00:00:00Z")
                .await
                .expect("claim runs")
                .expect("step claims")
        };
        let receipts = || async {
            let raw: String = sqlx::query_scalar(
                "SELECT effect_receipts_json FROM integration_attempt WHERE id=?",
            )
            .bind(&attempt.id)
            .fetch_one(db.pool())
            .await
            .expect("receipts read");
            serde_json::from_str::<Vec<serde_json::Value>>(&raw).expect("receipts parse")
        };

        let first = db::task_writer::in_task_step(claim().await, service.merge(task_id.clone()))
            .await
            .expect("first merge returns");
        assert!(matches!(first, MergeOutcome::Done { .. }), "{first:?}");
        let recorded = receipts().await;
        assert_eq!(
            recorded.len(),
            1,
            "the live merge was not bound: {recorded:?}"
        );
        assert_eq!(recorded[0]["operation_state"], "succeeded");

        // A later effect of this Task started under the old claim and died
        // before its receipt. Nothing can prove it either way.
        let mut orphan = recorded[0]["request"].clone();
        orphan["kind"] = serde_json::json!("rebase");
        orphan["witness"]["expected_target_sha"] = serde_json::json!("unprovable");
        sqlx::query("UPDATE integration_attempt SET effect_intent_json=?,current_operation_state='running' WHERE id=?")
            .bind(serde_json::json!({"request":orphan,"digest":"","started":true}).to_string())
            .bind(&attempt.id)
            .execute(db.pool())
            .await
            .expect("orphan plants");

        std::fs::write(worktree_path.join("feature.txt"), "two\n").expect("feature rewrites");
        let head = git::commit_all(&worktree_path, "feature again")
            .await
            .expect("feature commits again");
        let second = db::task_writer::in_task_step(claim().await, service.merge(task_id.clone()))
            .await
            .expect("an orphaned Task-step intent refused the next claim's merge");
        assert!(matches!(second, MergeOutcome::Done { .. }), "{second:?}");
        assert_eq!(
            git::get_current_sha(&repo_path).await.expect("head reads"),
            head
        );
        let recorded = receipts().await;
        assert_eq!(recorded.len(), 3, "{recorded:?}");
        assert!(recorded
            .iter()
            .any(|receipt| receipt["request"]["kind"] == "rebase"
                && receipt["operation_state"] == "failed"
                && receipt["result"]["kind"] == "infrastructure"));
        let intent: Option<String> =
            sqlx::query_scalar("SELECT effect_intent_json FROM integration_attempt WHERE id=?")
                .bind(&attempt.id)
                .fetch_one(db.pool())
                .await
                .expect("intent reads");
        assert!(
            intent.is_none(),
            "an intent outlived its effect: {intent:?}"
        );
    }

    #[tokio::test]
    async fn clean_merge_uses_execution_workspace_repository_after_project_reselection() {
        let db = sqlite_db().await;
        let event_bus = Arc::new(EventBus::new(16));
        let temp = TempDir::new().expect("temp creates");
        let repo_path = setup_repo(&temp).await;
        let task_id = new_uuid_v4();
        let worktree_path = temp.path().join("worktrees").join(&task_id).join("repo");
        git::create_worktree(
            &repo_path,
            &workspace::task_branch_name(&task_id),
            &worktree_path,
        )
        .await
        .expect("worktree creates");
        std::fs::write(worktree_path.join("feature.txt"), "hello\n").expect("feature writes");
        let worktree_sha = git::commit_all(&worktree_path, "feature")
            .await
            .expect("feature commits");
        let execution_id = seed_merge_rows(&db, &repo_path, &worktree_path, &task_id).await;
        let task = TaskRepo::get_by_id(&*db, &task_id, false)
            .await
            .expect("task loads")
            .expect("task exists");
        let replacement_repo_id = new_uuid_v4();
        let now = now_rfc3339();
        RepoRepo::create(
            &*db,
            CreateRepo {
                id: replacement_repo_id.clone(),
                project_id: task.project_id.clone(),
                name: "replacement".to_owned(),
                remote_url: Some(temp.path().join("replacement.git").display().to_string()),
                local_path: None,
                default_branch: "trunk".to_owned(),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("replacement repo creates");
        let project = ProjectRepo::get_by_id(&*db, &task.project_id)
            .await
            .expect("project loads")
            .expect("project exists");
        ProjectRepo::update_at_version(
            &*db,
            UpdateProject {
                id: project.id,
                name: None,
                settings: None,
                primary_repo_id: Some(Some(replacement_repo_id)),
                paused_at: None,
                updated_at: now_rfc3339(),
            },
            project.version,
            None,
        )
        .await
        .expect("project primary repo changes after execution");
        let before_sha = git::get_current_sha(&repo_path)
            .await
            .expect("before sha reads");

        let service =
            MergeService::new_for_test(Arc::clone(&db), event_bus, temp.path().to_path_buf());
        let outcome = service.merge(task_id).await.expect("merge succeeds");

        match outcome {
            MergeOutcome::Done {
                before_sha: actual_before,
                after_sha,
                branch: _,
            } => {
                assert_eq!(actual_before, before_sha);
                assert_eq!(after_sha, worktree_sha);
            }
            other => panic!("expected done, got {other:?}"),
        }
        let execution = ExecutionRepo::get_by_id(&*db, &execution_id)
            .await
            .expect("execution loads")
            .expect("execution exists");
        assert_eq!(execution.before_sha, Some(worktree_sha.clone()));
        assert_eq!(execution.after_sha, Some(worktree_sha));
    }

    #[tokio::test]
    async fn merge_recovers_deleted_ready_worktree_from_task_branch() {
        let db = sqlite_db().await;
        let event_bus = Arc::new(EventBus::new(16));
        let temp = TempDir::new().expect("temp creates");
        let repo_path = setup_repo(&temp).await;
        let task_id = new_uuid_v4();
        let worktree_path = temp.path().join(&task_id).join("repo");
        git::create_worktree(
            &repo_path,
            &workspace::task_branch_name(&task_id),
            &worktree_path,
        )
        .await
        .expect("worktree creates");
        std::fs::write(worktree_path.join("feature.txt"), "recovered merge\n")
            .expect("feature writes");
        let worktree_sha = git::commit_all(&worktree_path, "feature")
            .await
            .expect("feature commits");
        seed_merge_rows(&db, &repo_path, &worktree_path, &task_id).await;
        std::fs::remove_dir_all(&worktree_path).expect("worktree directory removes");
        run_git(&repo_path, &["worktree", "prune"]).await;

        let service =
            MergeService::new_for_test(Arc::clone(&db), event_bus, temp.path().to_path_buf());
        let outcome = service.merge(&task_id).await.expect("merge recovers");

        assert!(matches!(outcome, MergeOutcome::Done { .. }), "{outcome:?}");
        assert!(worktree_path.exists());
        assert_eq!(
            git::get_current_sha(&worktree_path)
                .await
                .expect("recovered worktree HEAD reads"),
            worktree_sha
        );
    }

    #[tokio::test]
    async fn read_only_checks_do_not_repair_unavailable_workspace() {
        let db = sqlite_db().await;
        let event_bus = Arc::new(EventBus::new(16));
        let temp = TempDir::new().expect("temp creates");
        let repo_path = setup_repo(&temp).await;
        let task_id = new_uuid_v4();
        let worktree_path = temp.path().join(&task_id).join("repo");
        git::create_worktree(
            &repo_path,
            &workspace::task_branch_name(&task_id),
            &worktree_path,
        )
        .await
        .expect("worktree creates");
        seed_merge_rows(&db, &repo_path, &worktree_path, &task_id).await;
        let workspace = WorkspaceRepo::get_by_task_id(&*db, &task_id)
            .await
            .expect("workspace loads")
            .expect("workspace exists");
        let task = TaskRepo::get_by_id(&*db, &task_id, false)
            .await
            .expect("task loads")
            .expect("task exists");
        std::fs::remove_dir_all(&worktree_path).expect("worktree directory removes");
        run_git(&repo_path, &["worktree", "prune"]).await;
        let service =
            MergeService::new_for_test(Arc::clone(&db), event_bus, temp.path().to_path_buf());

        assert!(service
            .unresolved_handoff_markers(&task_id)
            .await
            .expect("marker check treats a missing workspace as unavailable")
            .is_empty());
        assert!(matches!(
            service
                .review_carry_facts(&task_id)
                .await
                .expect("review carry treats a missing workspace as unavailable"),
            ReviewCarryFacts::Unavailable { .. }
        ));
        assert!(
            !worktree_path.exists(),
            "read-only checks must not recreate"
        );

        sqlx::query("UPDATE project SET primary_repo_id = NULL WHERE id = ?")
            .bind(&task.project_id)
            .execute(db.pool())
            .await
            .expect("project primary repo clears");
        sqlx::query("DELETE FROM repo WHERE id = ?")
            .bind(&workspace.repo_id)
            .execute(db.pool())
            .await
            .expect("repo deletes while workspace provenance remains");
        assert!(service
            .unresolved_handoff_markers(&task_id)
            .await
            .expect("missing repo keeps the empty marker result")
            .is_empty());
        assert_eq!(
            service
                .review_carry_facts(&task_id)
                .await
                .expect("missing repo keeps the unavailable review result"),
            ReviewCarryFacts::Unavailable {
                reason: "repository not found".to_owned()
            }
        );
    }

    #[tokio::test]
    async fn marker_gate_applies_only_to_handed_off_paths_and_resolved_files_integrate() {
        let db = sqlite_db().await;
        let temp = TempDir::new().expect("temp creates");
        let repo_path = setup_repo(&temp).await;
        let task_id = new_uuid_v4();
        let worktree_path = temp.path().join("marker_worktree");
        git::create_worktree(
            &repo_path,
            &workspace::task_branch_name(&task_id),
            &worktree_path,
        )
        .await
        .expect("worktree creates");
        std::fs::write(
            worktree_path.join("handoff.txt"),
            "<<<<<<< ours\n>>>>>>> theirs\n",
        )
        .expect("handed-off file writes");
        std::fs::write(worktree_path.join("example.txt"), "<<<<<<< example\n")
            .expect("legitimate example writes");
        git::commit_all(&worktree_path, "add marker-shaped lines")
            .await
            .expect("branch commits");
        seed_merge_rows(&db, &repo_path, &worktree_path, &task_id).await;
        let service = MergeService::new_for_test(
            Arc::clone(&db),
            Arc::new(EventBus::new(16)),
            temp.path().to_path_buf(),
        );

        // Without a handoff, the branch is allowed to document marker lines.
        assert!(matches!(
            service.merge(&task_id).await.expect("merge evaluates"),
            MergeOutcome::Done { .. }
        ));

        // A second Task is handed only handoff.txt. An unrelated example
        // remains legitimate even while the handoff gate is active.
        let second_id = new_uuid_v4();
        let second_path = temp.path().join("second_marker_worktree");
        git::create_worktree(
            &repo_path,
            &workspace::task_branch_name(&second_id),
            &second_path,
        )
        .await
        .expect("second worktree creates");
        std::fs::write(
            second_path.join("handoff.txt"),
            "<<<<<<< worker\n>>>>>>> target\n",
        )
        .expect("second handed-off file writes");
        std::fs::write(
            second_path.join("example2.txt"),
            "<<<<<<< documented example\n",
        )
        .expect("unrelated example writes");
        git::commit_all(&second_path, "unresolved handoff")
            .await
            .expect("second branch commits");
        seed_merge_rows(&db, &repo_path, &second_path, &second_id).await;
        db::TransitionLogRepo::insert(
            &*db,
            db::CreateTransitionLog {
                id: new_uuid_v4(),
                task_id: second_id.clone(),
                from_state: "merging".to_owned(),
                to_state: "merge_failed".to_owned(),
                trigger_name: None,
                triggered_by: api_types::Actor::system(api_types::SystemComponent::Workflow)
                    .display(),
                bridge: api_types::TransitionBridge::conflict_handoff(&["handoff.txt".into()]),
                trigger_reason:
                    "rebased onto main; conflicts were committed with markers in: handoff.txt"
                        .to_owned(),
                hook_results_json: None,
                rejection: false,
                created_at: now_rfc3339(),
            },
        )
        .await
        .expect("handoff records");
        assert_eq!(
            service
                .merge(&second_id)
                .await
                .expect("marker gate evaluates"),
            MergeOutcome::UnresolvedConflictMarkers {
                paths: vec!["handoff.txt".to_owned()]
            }
        );

        std::fs::write(second_path.join("handoff.txt"), "both sides resolved\n")
            .expect("repair writes");
        git::commit_all(&second_path, "resolve handoff")
            .await
            .expect("repair commits");
        assert!(matches!(
            service
                .merge(&second_id)
                .await
                .expect("resolved branch integrates"),
            MergeOutcome::Done { .. }
        ));
    }

    #[tokio::test]
    async fn paused_project_cannot_write_the_integration_branch() {
        let db = sqlite_db().await;
        let event_bus = Arc::new(EventBus::new(16));
        let temp = TempDir::new().expect("temp creates");
        let repo_path = setup_repo(&temp).await;
        let task_id = new_uuid_v4();
        let worktree_path = temp.path().join("worktrees").join(&task_id).join("repo");
        git::create_worktree(
            &repo_path,
            &workspace::task_branch_name(&task_id),
            &worktree_path,
        )
        .await
        .expect("worktree creates");
        std::fs::write(worktree_path.join("feature.txt"), "hello\n").expect("feature writes");
        git::commit_all(&worktree_path, "feature")
            .await
            .expect("feature commits");
        seed_merge_rows(&db, &repo_path, &worktree_path, &task_id).await;
        let task = TaskRepo::get_by_id(&*db, &task_id, false)
            .await
            .expect("task loads")
            .expect("task exists");
        ProjectRepo::set_paused_at(&*db, &task.project_id, Some(now_rfc3339()))
            .await
            .expect("project pauses");
        let before_sha = git::get_current_sha(&repo_path).await.expect("head reads");
        let service =
            MergeService::new_for_test(Arc::clone(&db), event_bus, temp.path().to_path_buf());

        let result = service.merge(task_id).await;

        assert!(matches!(
            result,
            Err(ServiceError::ProjectPaused { project_id }) if project_id == task.project_id
        ));
        assert_eq!(
            git::get_current_sha(&repo_path)
                .await
                .expect("head reloads"),
            before_sha
        );
    }

    #[tokio::test]
    async fn conformance_merge_refuses_changed_candidate_target_policy_and_legacy_pass() {
        for changed in [
            "none",
            "candidate",
            "target",
            "target_after_rebase",
            "exact_object",
            "policy",
            "legacy",
            "landed",
        ] {
            if changed == "exact_object" && !cfg!(unix) {
                continue;
            }
            let db = sqlite_db().await;
            let temp = TempDir::new().unwrap();
            let repo = setup_repo(&temp).await;
            let task_id = new_uuid_v4();
            let worktree = temp.path().join("worktree");
            git::create_worktree(&repo, &workspace::task_branch_name(&task_id), &worktree)
                .await
                .unwrap();
            std::fs::write(worktree.join("feature.txt"), "reviewed\n").unwrap();
            let mut accepted_sha = git::commit_all(&worktree, "candidate").await.unwrap();
            if changed == "target_after_rebase" {
                std::fs::write(repo.join("sibling.txt"), "sibling\n").unwrap();
                git::commit_all(&repo, "first target move").await.unwrap();
                git::rebase(&worktree, "main").await.unwrap();
                accepted_sha = git::get_current_sha(&worktree).await.unwrap();
            }
            let execution_id = seed_merge_rows(&db, &repo, &worktree, &task_id).await;
            sqlx::query("INSERT INTO task_role_assignment(id,task_id,role_name,assignee_type,assignee_id,created_at,updated_at) VALUES ('reviewer-role',?,'reviewer','agent','reviewer','now','now')")
                .bind(&task_id).execute(db.pool()).await.unwrap();
            let conformance = if changed == "legacy" {
                api_types::ReviewConformance::default()
            } else {
                sqlx::query("UPDATE execution SET status='running' WHERE id=?")
                    .bind(&execution_id)
                    .execute(db.pool())
                    .await
                    .unwrap();
                ::review::contract::admit(&db, &execution_id, &task_id, &worktree)
                    .await
                    .unwrap();
                let report = serde_json::json!({"result": "pass", "reason": "Feature exists"});
                let result = ::review::contract::evaluate(
                    &db,
                    &execution_id,
                    &worktree,
                    &report.to_string(),
                )
                .await
                .unwrap();
                assert_eq!(result.status, api_types::ConformanceStatus::Passed);
                sqlx::query("UPDATE execution SET status='completed' WHERE id=?")
                    .bind(&execution_id)
                    .execute(db.pool())
                    .await
                    .unwrap();
                result
            };
            let now = now_rfc3339();
            let review = db::ReviewRepo::create(
                &*db,
                db::CreateReview {
                    id: new_uuid_v4(),
                    task_id: task_id.clone(),
                    execution_id,
                    attempt_number: 1,
                    status: db::ReviewStatus::Running,
                    step_results_json: "{}".into(),
                    started_at: now.clone(),
                    created_at: now.clone(),
                    updated_at: now.clone(),
                },
            )
            .await
            .unwrap();
            db::ReviewRepo::update_status(
                &*db,
                &review.id,
                db::ReviewStatus::Passed,
                serde_json::json!({"ci_steps":[],"conformance":conformance}).to_string(),
                Some(now.clone()),
                &now,
            )
            .await
            .unwrap();
            if changed != "legacy" && changed != "none" {
                let now = now_rfc3339();
                TaskRepo::set_review_passed_at(&*db, &task_id, Some(now.clone()), &now)
                    .await
                    .unwrap();
            }
            match changed {
                "candidate" => {
                    std::fs::write(worktree.join("feature.txt"), "unreviewed\n").unwrap();
                    git::commit_all(&worktree, "repair").await.unwrap();
                }
                "target" | "target_after_rebase" => {
                    std::fs::write(repo.join("target.txt"), "new target\n").unwrap();
                    git::commit_all(&repo, "target moved").await.unwrap();
                }
                "landed" => {
                    run_git(&repo, &["merge", "--ff-only", &accepted_sha]).await;
                    std::fs::write(repo.join("sibling.txt"), "later merge\n").unwrap();
                    git::commit_all(&repo, "sibling landed").await.unwrap();
                }
                "exact_object" => {
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt;
                        let hooks = temp.path().join("hooks");
                        std::fs::create_dir(&hooks).unwrap();
                        let hook = hooks.join("post-merge");
                        std::fs::write(&hook, "#!/bin/sh\ngit -c core.hooksPath=/dev/null commit --allow-empty -m hook-moved\n").unwrap();
                        std::fs::set_permissions(&hook, std::fs::Permissions::from_mode(0o755))
                            .unwrap();
                        run_git(
                            &repo,
                            &["config", "core.hooksPath", hooks.to_str().unwrap()],
                        )
                        .await;
                    }
                }
                "policy" => {
                    sqlx::query("UPDATE task SET task_state_config=? WHERE id=?")
                        .bind(r#"{"review":{"ci_steps":["false"]}}"#)
                        .bind(&task_id)
                        .execute(db.pool())
                        .await
                        .unwrap();
                }
                _ => {}
            }
            let before = git::get_current_sha(&repo).await.unwrap();
            let service = MergeService::new_for_test(
                db.clone(),
                Arc::new(EventBus::new(16)),
                temp.path().into(),
            );
            if changed == "none" {
                let without_authority = service.merge(task_id.clone()).await.unwrap();
                assert!(
                    matches!(&without_authority, MergeOutcome::ReviewRequired { reason } if reason.contains("current review authority"))
                );
                let now = now_rfc3339();
                TaskRepo::set_review_passed_at(&*db, &task_id, Some(now.clone()), &now)
                    .await
                    .unwrap();
                let outcome = service.merge(task_id).await.unwrap();
                assert!(matches!(outcome, MergeOutcome::Done { .. }), "{outcome:?}");
                assert_eq!(git::get_current_sha(&repo).await.unwrap(), accepted_sha);
            } else if changed == "landed" {
                let outcome = service.merge(task_id).await.unwrap();
                assert!(matches!(outcome, MergeOutcome::Done { .. }), "{outcome:?}");
                assert_eq!(git::get_current_sha(&repo).await.unwrap(), before);
            } else if changed == "exact_object" {
                let outcome = service.merge(task_id).await.unwrap();
                assert_eq!(outcome, MergeOutcome::TargetMoved {
                    reason: "integration target changed during merge; reviewed content was not integrated".into(),
                    target_branch: "main".into(),
                });
                assert_ne!(git::get_current_sha(&repo).await.unwrap(), accepted_sha);
                assert!(git::is_worktree_clean(&repo).await.unwrap());
            } else if matches!(changed, "target" | "target_after_rebase") {
                let outcome = service.merge(task_id).await.unwrap();
                // A moved integration target is contention, not a fault, and
                // is reported separately so the caller can rebase instead of
                // charging the Task's single merge-fix retry.
                assert!(
                    matches!(outcome, MergeOutcome::TargetMoved { .. }),
                    "{changed}: {outcome:?}"
                );
                assert_eq!(git::get_current_sha(&repo).await.unwrap(), before);
            } else {
                let outcome = service.merge(task_id).await.unwrap();
                assert!(
                    matches!(outcome, MergeOutcome::ReviewRequired { .. }),
                    "{changed}: {outcome:?}"
                );
                assert_eq!(git::get_current_sha(&repo).await.unwrap(), before);
            }
        }
    }

    #[tokio::test]
    async fn conflicting_merge_returns_conflict_and_aborts() {
        let db = sqlite_db().await;
        let event_bus = Arc::new(EventBus::new(16));
        let temp = TempDir::new().expect("temp creates");
        let repo_path = setup_repo(&temp).await;
        let task_id = new_uuid_v4();
        let worktree_path = temp.path().join("worktrees").join(&task_id).join("repo");
        git::create_worktree(
            &repo_path,
            &workspace::task_branch_name(&task_id),
            &worktree_path,
        )
        .await
        .expect("worktree creates");
        std::fs::write(worktree_path.join("file.txt"), "feature\n").expect("feature writes");
        git::commit_all(&worktree_path, "feature")
            .await
            .expect("feature commits");
        std::fs::write(repo_path.join("file.txt"), "main\n").expect("main writes");
        let repo_head = git::commit_all(&repo_path, "main")
            .await
            .expect("main commits");
        seed_merge_rows(&db, &repo_path, &worktree_path, &task_id).await;

        let service =
            MergeService::new_for_test(Arc::clone(&db), event_bus, temp.path().to_path_buf());
        let outcome = service.merge(task_id).await.expect("merge returns outcome");

        match outcome {
            MergeOutcome::Conflict {
                details,
                conflict_paths,
                target_branch,
            } => {
                assert_eq!(target_branch, "main");
                assert!(details.contains("CONFLICT"));
                assert_eq!(conflict_paths, vec![PathBuf::from("file.txt")]);
            }
            other => panic!("expected conflict, got {other:?}"),
        }
        assert_eq!(
            git::get_current_sha(&repo_path).await.expect("head reads"),
            repo_head
        );
        assert!(!git::detect_interrupted_merge(&repo_path)
            .await
            .expect("merge state reads"));
    }

    #[tokio::test]
    async fn dirty_worktree_returns_dirty() {
        let db = sqlite_db().await;
        let event_bus = Arc::new(EventBus::new(16));
        let temp = TempDir::new().expect("temp creates");
        let repo_path = setup_repo(&temp).await;
        let task_id = new_uuid_v4();
        let worktree_path = temp.path().join("worktrees").join(&task_id).join("repo");
        git::create_worktree(
            &repo_path,
            &workspace::task_branch_name(&task_id),
            &worktree_path,
        )
        .await
        .expect("worktree creates");
        std::fs::write(worktree_path.join("dirty.txt"), "dirty\n").expect("dirty writes");
        seed_merge_rows(&db, &repo_path, &worktree_path, &task_id).await;

        let service =
            MergeService::new_for_test(Arc::clone(&db), event_bus, temp.path().to_path_buf());
        let outcome = service.merge(task_id).await.expect("merge returns outcome");

        match outcome {
            MergeOutcome::Dirty { files } => assert!(files.contains(&"dirty.txt".to_owned())),
            other => panic!("expected dirty, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn dirty_target_repo_returns_target_dirty() {
        let db = sqlite_db().await;
        let event_bus = Arc::new(EventBus::new(16));
        let temp = TempDir::new().expect("temp creates");
        let repo_path = setup_repo(&temp).await;
        let task_id = new_uuid_v4();
        let worktree_path = temp.path().join("worktrees").join(&task_id).join("repo");
        git::create_worktree(
            &repo_path,
            &workspace::task_branch_name(&task_id),
            &worktree_path,
        )
        .await
        .expect("worktree creates");
        std::fs::write(worktree_path.join("feature.txt"), "hello\n").expect("feature writes");
        git::commit_all(&worktree_path, "feature")
            .await
            .expect("feature commits");
        std::fs::write(repo_path.join("target-dirty.txt"), "dirty\n").expect("dirty writes");
        let before_sha = git::get_current_sha(&repo_path)
            .await
            .expect("before sha reads");
        seed_merge_rows(&db, &repo_path, &worktree_path, &task_id).await;

        let service =
            MergeService::new_for_test(Arc::clone(&db), event_bus, temp.path().to_path_buf());
        let outcome = service.merge(task_id).await.expect("merge returns outcome");

        match outcome {
            MergeOutcome::TargetDirty { files } => {
                assert!(
                    files.iter().any(|file| file.contains("target-dirty.txt")),
                    "{files:?}"
                )
            }
            other => panic!("expected target dirty, got {other:?}"),
        }
        assert_eq!(
            git::get_current_sha(&repo_path).await.expect("head reads"),
            before_sha
        );
    }
}
