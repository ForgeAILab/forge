//! The head's read-only facts: the Task gate from storage, the Git state
//! from the owner of the Task's checkout.
use crate::{
    integration_effects::{EffectOwner, EffectWorkspace},
    integration_worker::{HeadFacts, IntegrationFactsPort, ObjectTransferEndpoint, TaskGate},
    workspace_backend::{effect_workspace, WorkspaceBackendRouter},
    Result, ServiceError,
};
use api_types::WorkspaceGitQuery;
use async_trait::async_trait;
use db::{IntegrationAttempt, IntegrationQueue, SqliteDb, WorkspaceRepo};
use std::{path::PathBuf, sync::Arc};

/// The production [`IntegrationFactsPort`].
///
/// - Gate: the Task is still in the status entry the attempt was admitted in
///   (`db::STEP_FENCE`), and its Project is not paused.
/// - Git: read through the Task's workspace placement, so a daemon-placed
///   Task is read on its daemon. The Task's checkout is a worktree of its
///   repo location, so when that location is the queue's target the target
///   branch is read there too; otherwise the target is read from the
///   server-owned default checkout.
///
/// It writes nothing and holds no Task, Review or event capability.
pub struct WorkspaceHeadFacts {
    db: Arc<SqliteDb>,
    router: Arc<WorkspaceBackendRouter>,
}

impl WorkspaceHeadFacts {
    pub fn new(db: Arc<SqliteDb>, router: Arc<WorkspaceBackendRouter>) -> Self {
        Self { db, router }
    }

    /// A Task that left its entry has no checkout this worker may rely on
    /// (cleanup may already have removed it): nothing but the gate is read.
    fn left(queue: &IntegrationQueue) -> HeadFacts {
        HeadFacts {
            gate: TaskGate::Left,
            workspace: EffectWorkspace {
                workspace_id: String::new(),
                placement_id: String::new(),
                generation: 0,
                owner: EffectOwner::Server,
                handle: String::new(),
            },
            task_branch: String::new(),
            candidate_head: String::new(),
            target_tip: String::new(),
            target_in_candidate: false,
            candidate_in_target: false,
            worktree_dirty: false,
            target_dirty: false,
            rebase_in_progress: false,
            shared_object_store: true,
            task_target_tip: String::new(),
            task_location: ObjectTransferEndpoint {
                repo_location_id: queue.target_location_id.clone().unwrap_or_default(),
                owner: EffectOwner::Server,
            },
        }
    }
}

fn unreadable(what: &str) -> ServiceError {
    ServiceError::invalid_operation(format!("integration head facts: {what}"))
}

#[async_trait]
impl IntegrationFactsPort for WorkspaceHeadFacts {
    async fn head_facts(
        &self,
        attempt: &IntegrationAttempt,
        queue: &IntegrationQueue,
    ) -> Result<HeadFacts> {
        let live: bool = sqlx::query_scalar(db::STEP_FENCE)
            .bind(&attempt.task_ref)
            .bind(&attempt.expected_status)
            .bind(attempt.expected_epoch)
            .fetch_one(self.db.pool())
            .await
            .map_err(db::DbError::from)?;
        if !live {
            return Ok(Self::left(queue));
        }
        let paused: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM task t JOIN project p ON p.id=t.project_id WHERE t.id=? AND p.paused_at IS NOT NULL)",
        )
        .bind(&attempt.task_ref)
        .fetch_one(self.db.pool())
        .await
        .map_err(db::DbError::from)?;

        let workspace = match attempt.workspace_id.as_deref() {
            Some(id) => WorkspaceRepo::get_by_id(&*self.db, id).await?,
            None => WorkspaceRepo::get_by_task_id(&*self.db, &attempt.task_ref).await?,
        }
        .ok_or_else(|| unreadable("the Task has no workspace"))?;
        let resolved = self.router.resolve(&self.db, &workspace).await?;
        let placement = &resolved.placement;
        let shared = queue.target_location_id.as_deref() == Some(&placement.repo_location_id);

        let read = |query: WorkspaceGitQuery, what: &'static str| {
            let resolved = &resolved;
            async move {
                Ok::<_, ServiceError>(
                    resolved
                        .git_query(query, false)
                        .await?
                        .ok_or_else(|| unreadable(what))?
                        .trim()
                        .to_owned(),
                )
            }
        };
        let candidate_head = read(WorkspaceGitQuery::Head, "the Task checkout has no HEAD").await?;
        // The default checkout, when it is a path on this host.
        let target_path: Option<PathBuf> = match queue.target_location_id.as_deref() {
            Some(location_id) => sqlx::query_scalar::<_, String>(
                "SELECT path FROM repo_location WHERE id=? AND owner_kind='server'",
            )
            .bind(location_id)
            .fetch_optional(self.db.pool())
            .await
            .map_err(db::DbError::from)?
            .map(PathBuf::from),
            None => None,
        };
        let target_tip = if shared {
            read(
                WorkspaceGitQuery::TargetHead {
                    branch: queue.target_branch.clone(),
                },
                "the target branch has no tip",
            )
            .await?
        } else {
            let path = target_path.as_deref().ok_or_else(|| {
                unreadable(
                    "the default checkout is on another machine than the Task and cannot be read",
                )
            })?;
            crate::integration_effects::merge::target_tip(path, &queue.target_branch).await?
        };
        let ancestor = |base: String, head: String| {
            let resolved = &resolved;
            async move {
                Ok::<_, ServiceError>(
                    resolved
                        .git_query(WorkspaceGitQuery::IsAncestor { base, head }, true)
                        .await?
                        .is_some(),
                )
            }
        };
        // Across clones the Task's checkout may not hold the target tip yet
        // (the inbound transfer brings it): an unknown object is not an
        // ancestor, which sends the head to the rebase round that needs it.
        let target_in_candidate = ancestor(target_tip.clone(), candidate_head.clone()).await?;
        let candidate_in_target = if shared {
            ancestor(candidate_head.clone(), target_tip.clone()).await?
        } else {
            let path = target_path.as_deref().expect("read above");
            git::command_output(
                path,
                &["merge-base", "--is-ancestor", &candidate_head, &target_tip],
            )
            .await?
            .status
            .success()
        };
        let worktree_dirty = !resolved
            .git_query(WorkspaceGitQuery::StatusPorcelain, false)
            .await?
            .unwrap_or_default()
            .trim()
            .is_empty();
        let rebase_in_progress = resolved
            .git_query(WorkspaceGitQuery::RebaseInProgress, false)
            .await?
            .is_some_and(|value| value.trim() == "true");
        // A daemon-owned default checkout is read on its daemon, through the
        // Task's placement when the Task is placed in it. From another clone
        // of that daemon it is not readable here; the owner then refuses a
        // fast-forward into a dirty checkout itself.
        let target_dirty = match target_path.as_deref() {
            Some(path) => !git::is_worktree_clean(path).await?,
            None if shared => !resolved
                .git_query(WorkspaceGitQuery::TargetStatusPorcelain, false)
                .await?
                .unwrap_or_default()
                .trim()
                .is_empty(),
            None => false,
        };
        let task_target_tip = if shared {
            String::new()
        } else {
            resolved
                .git_query(
                    WorkspaceGitQuery::TargetHead {
                        branch: queue.target_branch.clone(),
                    },
                    true,
                )
                .await
                .ok()
                .flatten()
                .map(|tip| tip.trim().to_owned())
                .unwrap_or_default()
        };
        let workspace_witness = effect_workspace(placement);
        Ok(HeadFacts {
            gate: if paused {
                TaskGate::ProjectPaused
            } else {
                TaskGate::Live
            },
            task_location: ObjectTransferEndpoint {
                repo_location_id: placement.repo_location_id.clone(),
                owner: workspace_witness.owner.clone(),
            },
            workspace: workspace_witness,
            task_branch: workspace.branch.clone(),
            candidate_head,
            target_tip,
            target_in_candidate,
            candidate_in_target,
            worktree_dirty,
            target_dirty,
            rebase_in_progress,
            shared_object_store: shared,
            task_target_tip,
        })
    }
}
