//! The head's read-only facts: the Task gate from storage, the Git state
//! from the owner of the Task's checkout.
use super::owner::client_error;
use crate::{
    daemon_transport::workspace_client::{DaemonObjectEndpoint, DaemonWorkspaceClient},
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
///   (`db::STEP_FENCE`), and nothing stops it there: no Hold, block, failure
///   or wait for a person, and neither its Project nor its Agent is paused
///   (`integration_steps::gate`).
/// - Git: read through the Task's workspace placement, so a daemon-placed
///   Task is read on its daemon. The Task's checkout is a worktree of its
///   repo location, so when that location is the queue's target the target
///   branch is read there too. Otherwise the target is read where the
///   default checkout is: on this host for a server-owned location, by the
///   owner's read-only `integration.target_facts` for a daemon-owned one.
///
/// It writes nothing and holds no Task, Review or event capability.
pub struct WorkspaceHeadFacts {
    db: Arc<SqliteDb>,
    router: Arc<WorkspaceBackendRouter>,
    daemons: Option<DaemonWorkspaceClient>,
}

impl WorkspaceHeadFacts {
    pub fn new(db: Arc<SqliteDb>, router: Arc<WorkspaceBackendRouter>) -> Self {
        Self {
            db,
            router,
            daemons: None,
        }
    }

    /// Reads of a daemon-owned default checkout the Task is not placed in.
    pub fn with_daemons(mut self, client: DaemonWorkspaceClient) -> Self {
        self.daemons = Some(client);
        self
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
            placement_ready: true,
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

impl WorkspaceHeadFacts {
    /// Whether the Task is still in the entry the attempt was admitted in,
    /// and that entry is one the queue may merge from. An attempt the
    /// start-time import made for a Task that was already out of its merge
    /// state when the server was upgraded (with its coder for a conflict or
    /// a dirty checkout, or back in review) names that state as its entry:
    /// once its old merge is known not to have landed there is nothing for
    /// the queue to do, the Task's own flow continues and its next entry of
    /// the merge state is admitted. Only imported attempts are read twice.
    async fn entry_live(&self, attempt: &IntegrationAttempt) -> Result<bool> {
        let live: bool = sqlx::query_scalar(db::STEP_FENCE)
            .bind(&attempt.task_ref)
            .bind(&attempt.expected_status)
            .bind(attempt.expected_epoch)
            .fetch_one(self.db.pool())
            .await
            .map_err(db::DbError::from)?;
        if !live || attempt.import_source_json.is_none() {
            return Ok(live);
        }
        let Some(task) = db::TaskRepo::get_by_id(&*self.db, &attempt.task_ref, false).await? else {
            return Ok(false);
        };
        let Some(project) = db::ProjectRepo::get_by_id(&*self.db, &task.project_id).await? else {
            return Ok(false);
        };
        let workflow = crate::workflow::engine::WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &crate::worker_runtime::queue::cascade_actor(),
        );
        Ok(crate::integration_steps::reconcile::merge_state(
            &workflow,
            &attempt.expected_status,
        ))
    }
}

#[async_trait]
impl IntegrationFactsPort for WorkspaceHeadFacts {
    async fn task_gate(&self, attempt: &IntegrationAttempt) -> Result<TaskGate> {
        if !self.entry_live(attempt).await? {
            return Ok(TaskGate::Left);
        }
        Ok(
            match crate::integration_steps::gate::task_gate(&self.db, &attempt.task_ref).await? {
                Some(cause) => TaskGate::Gated(cause),
                None => TaskGate::Live,
            },
        )
    }

    async fn head_facts(
        &self,
        attempt: &IntegrationAttempt,
        queue: &IntegrationQueue,
    ) -> Result<HeadFacts> {
        if !self.entry_live(attempt).await? {
            return Ok(Self::left(queue));
        }
        let gate =
            match crate::integration_steps::gate::task_gate(&self.db, &attempt.task_ref).await? {
                Some(cause) => TaskGate::Gated(cause),
                None => TaskGate::Live,
            };

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
        // The default checkout: a path on this host, or a daemon's.
        let target_row: Option<(String, String, Option<String>, Option<String>)> =
            match queue.target_location_id.as_deref() {
                Some(location_id) => sqlx::query_as(
                    "SELECT path,owner_kind,daemon_id,runtime_id FROM repo_location WHERE id=?",
                )
                .bind(location_id)
                .fetch_optional(self.db.pool())
                .await
                .map_err(db::DbError::from)?,
                None => None,
            };
        let target_path: Option<PathBuf> = target_row
            .as_ref()
            .filter(|(_, owner_kind, _, _)| owner_kind == "server")
            .map(|(path, _, _, _)| PathBuf::from(path));
        // Another owner's default checkout is read by that owner, once.
        let remote = match (&target_row, &self.daemons) {
            (Some((_, owner_kind, Some(daemon_id), Some(runtime_id))), Some(client))
                if !shared && owner_kind == "daemon" =>
            {
                Some(
                    client
                        .target_facts(
                            DaemonObjectEndpoint {
                                daemon_id,
                                runtime_id,
                                repo_location_id: queue
                                    .target_location_id
                                    .as_deref()
                                    .unwrap_or_default(),
                            },
                            &queue.target_branch,
                            &candidate_head,
                        )
                        .await
                        .map_err(client_error)?,
                )
            }
            _ => None,
        };
        let target_tip = if shared {
            read(
                WorkspaceGitQuery::TargetHead {
                    branch: queue.target_branch.clone(),
                },
                "the target branch has no tip",
            )
            .await?
        } else if let Some(remote) = &remote {
            remote.target_tip.clone()
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
        } else if let Some(remote) = &remote {
            remote.candidate_in_target
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
        // The reads above are separate Git commands. A writer that moved the
        // checkout between them (a rebase passes through the target tip and
        // has no state directory before it starts or after it ends) would
        // leave facts of two different commits: HEAD read again after the
        // rebase flag must be the HEAD the ancestry was computed for.
        if read(WorkspaceGitQuery::Head, "the Task checkout has no HEAD").await? != candidate_head {
            return Err(unreadable("the Task checkout moved while it was read"));
        }
        // A daemon-owned default checkout is read on its daemon: through the
        // Task's placement when the Task is placed in it, by the owner's
        // target facts otherwise.
        let target_dirty = match target_path.as_deref() {
            Some(path) => !git::is_worktree_clean(path).await?,
            None if remote.is_some() => remote.as_ref().is_some_and(|remote| remote.dirty),
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
            gate,
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
            placement_ready: placement.state == db::PlacementState::Ready,
            task_target_tip,
        })
    }
}
