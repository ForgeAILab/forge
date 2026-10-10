//! The head's fenced Git owners. Each effect goes to the owner that holds
//! what it works on: the rebase to the owner of the Task's checkout, the
//! fast-forward to the owner of the default checkout. They are one owner in
//! the usual case and two when the Task was placed on another machine; an
//! effect is requested from, journaled by and reconciled with the owner that
//! performs it (`IntegrationEffectRequest::effect_owner`).
use crate::{
    daemon_transport::workspace_client::{DaemonWorkspaceClient, WorkspaceClientError},
    integration_effects::{EffectOwner, EffectWorkspace},
    integration_owner::{
        OwnerEffectRefusal, OwnerMergeReceipt, OwnerRebaseReceipt, ServerIntegrationOwner,
    },
    integration_worker::{IntegrationOwnerPort, OwnerFastForwardRequest, OwnerRebaseRequest},
    Result, ServiceError,
};
use api_types::{
    WorkspaceIntegrationBinding, WorkspaceMergeParams, WorkspaceMutationFence,
    WorkspaceOperationExpected, WorkspaceOwnerOperation, WorkspaceOwnerOperationParams,
    WorkspaceReviewedMergeParams, METHOD_WORKSPACE_MERGE, METHOD_WORKSPACE_RESET,
};
use async_trait::async_trait;
use db::{
    IntegrationEffectReceipt, IntegrationEffectRequest, IntegrationOperationKind,
    IntegrationOperationState, IntegrationOwnerFence, SqliteDb,
};
use serde_json::json;
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::Duration,
};

/// Transport slack on top of an effect's own deadline: the owner answers
/// with a receipt at its deadline, and the reply still has to travel.
const REPLY_SLACK: Duration = Duration::from_secs(30);
const ANNOUNCED_MAX: usize = 4096;

pub(crate) fn client_error(error: WorkspaceClientError) -> ServiceError {
    match error {
        WorkspaceClientError::Transport(error) => error,
        WorkspaceClientError::Daemon(error) => ServiceError::invalid_operation(format!(
            "integration owner answered {}: {}",
            error.code, error.message
        )),
    }
}

/// The daemon an owner value (a fence's or queue's `target_owner`, or an
/// effect's `effect_owner`) names.
pub(crate) fn daemon_owner(owner: &serde_json::Value) -> Option<(String, String)> {
    (owner["owner_kind"] == "daemon")
        .then(|| {
            Some((
                owner["daemon_id"].as_str()?.to_owned(),
                owner["runtime_id"].as_str()?.to_owned(),
            ))
        })
        .flatten()
}

/// Announces a claim generation to a daemon owner before its first effect.
///
/// The owner records the fence as the queue's high-water mark, so a delayed
/// frame of an older generation is refused there, and an owner that later
/// says it has no intent for this generation proves the effect never ran.
/// `live_queue_ids` lets the owner drop fences of queues that left it; the
/// list always covers every queue that still targets the daemon, every queue
/// whose head's Task is placed on it, and every queue with an outstanding
/// intent on it.
pub struct DaemonFences {
    db: Arc<SqliteDb>,
    client: DaemonWorkspaceClient,
    announced: Mutex<HashSet<(String, String, i64)>>,
}

impl DaemonFences {
    pub fn new(db: Arc<SqliteDb>, client: DaemonWorkspaceClient) -> Self {
        Self {
            db,
            client,
            announced: Mutex::new(HashSet::new()),
        }
    }

    pub(crate) fn db(&self) -> &Arc<SqliteDb> {
        &self.db
    }

    /// Every queue this daemon may still be asked about.
    pub async fn live_queue_ids(&self, daemon_id: &str, fence_queue: &str) -> Result<Vec<String>> {
        let mut live: Vec<String> = sqlx::query_scalar(
            "SELECT id FROM integration_queue WHERE state!='closed' AND json_extract(target_owner_json,'$.daemon_id')=?",
        )
        .bind(daemon_id)
        .fetch_all(self.db.pool())
        .await
        .map_err(db::DbError::from)?;
        // Queues whose head's Task is placed on this daemon: it performs
        // their rebase although their default checkout is elsewhere, and a
        // claim it was told of must not be dropped by another queue's
        // announcement before that rebase is asked for.
        let heads: Vec<String> = sqlx::query_scalar(
            "SELECT q.id FROM integration_queue q JOIN integration_attempt a ON a.id=q.head_attempt_id WHERE q.state!='closed' AND a.daemon_id=?",
        )
        .bind(daemon_id)
        .fetch_all(self.db.pool())
        .await
        .map_err(db::DbError::from)?;
        live.extend(heads);
        live.extend(
            self.db
                .outstanding_integration_effects("daemon", Some(daemon_id))
                .await?
                .into_iter()
                .map(|request| request.fence.queue_id),
        );
        live.push(fence_queue.to_owned());
        live.sort();
        live.dedup();
        Ok(live)
    }

    /// Idempotent per `(daemon, queue, generation)` for this process. A
    /// refused or undelivered announcement is an error and leaves no intent.
    pub async fn announce(
        &self,
        daemon_id: &str,
        runtime_id: &str,
        fence: &IntegrationOwnerFence,
    ) -> Result<()> {
        let key = (
            daemon_id.to_owned(),
            fence.queue_id.clone(),
            fence.generation,
        );
        if self.announced.lock().expect("announced").contains(&key) {
            return Ok(());
        }
        let live = self.live_queue_ids(daemon_id, &fence.queue_id).await?;
        self.client
            .announce_integration_fence(daemon_id, runtime_id, fence, Some(live))
            .await
            .map_err(client_error)?;
        let mut announced = self.announced.lock().expect("announced");
        if announced.len() >= ANNOUNCED_MAX {
            announced.clear();
        }
        announced.insert(key);
        Ok(())
    }
}

/// Dispatches each effect on the owner that performs it.
pub struct RoutingIntegrationOwner {
    server: Arc<ServerIntegrationOwner>,
    client: DaemonWorkspaceClient,
    fences: Arc<DaemonFences>,
}

impl RoutingIntegrationOwner {
    pub fn new(
        server: Arc<ServerIntegrationOwner>,
        client: DaemonWorkspaceClient,
        fences: Arc<DaemonFences>,
    ) -> Self {
        Self {
            server,
            client,
            fences,
        }
    }

    /// The transport fence of an effect on the daemon that holds `workspace`.
    fn workspace_fence(
        workspace: &EffectWorkspace,
        expected_head_sha: &str,
    ) -> Option<(String, String, WorkspaceMutationFence)> {
        let EffectOwner::Daemon {
            daemon_id,
            runtime_id,
        } = &workspace.owner
        else {
            return None;
        };
        Some((
            daemon_id.clone(),
            runtime_id.clone(),
            WorkspaceMutationFence {
                // The attempt sink binds the request and its operation id.
                integration: WorkspaceIntegrationBinding::TaskStep,
                daemon_id: daemon_id.clone(),
                runtime_id: runtime_id.clone(),
                placement_id: workspace.placement_id.clone(),
                operation_id: "assigned-by-attempt-sink".into(),
                generation: workspace.generation.max(0) as u64,
                expected: WorkspaceOperationExpected::BaseSha {
                    sha: expected_head_sha.to_owned(),
                },
            },
        ))
    }

    /// The owner of the Task's checkout when it is not the target's owner:
    /// the `witness.effect_owner` of a rebase performed there. `Ok(None)`:
    /// one owner holds both. `Err(())`: the placement names no such owner.
    async fn off_target_owner(
        &self,
        fence: &IntegrationOwnerFence,
        workspace: &EffectWorkspace,
    ) -> Result<std::result::Result<Option<serde_json::Value>, ()>> {
        let target = daemon_owner(&fence.target_owner);
        let same = match (&workspace.owner, &target) {
            (EffectOwner::Server, None) => true,
            (EffectOwner::Daemon { daemon_id, .. }, Some((target, _))) => daemon_id == target,
            _ => false,
        };
        if same {
            return Ok(Ok(None));
        }
        let owner = self
            .fences
            .db()
            .integration_placement_owner(&workspace.placement_id)
            .await?;
        Ok(match (owner, &workspace.owner) {
            (
                Some(owner),
                EffectOwner::Daemon {
                    daemon_id,
                    runtime_id,
                },
            ) if owner["owner_kind"] == "daemon"
                && owner["daemon_id"].as_str() == Some(daemon_id)
                && owner["runtime_id"].as_str() == Some(runtime_id) =>
            {
                Ok(Some(owner))
            }
            _ => Err(()),
        })
    }

    async fn effect(
        &self,
        daemon_id: &str,
        runtime_id: &str,
        request: IntegrationEffectRequest,
        method: &str,
        params: serde_json::Value,
        deadline: Duration,
    ) -> Result<IntegrationEffectReceipt> {
        self.fences
            .announce(daemon_id, runtime_id, &request.fence)
            .await?;
        self.client
            .integration_effect(daemon_id, request, method, params, deadline + REPLY_SLACK)
            .await
            .map_err(client_error)
    }
}

/// A daemon receipt's `result` has the server owner's receipt shape. One that
/// does not decode is read by its state: a settled failure ran nothing that
/// counts, anything else stays unknown.
fn undecodable<T>(receipt: &IntegrationEffectReceipt, not_performed: T) -> Result<T> {
    if receipt.operation_state == IntegrationOperationState::Failed {
        Ok(not_performed)
    } else {
        Err(ServiceError::invalid_operation(
            "integration owner returned a receipt this server cannot read",
        ))
    }
}

#[async_trait]
impl IntegrationOwnerPort for RoutingIntegrationOwner {
    async fn rebase(&self, request: OwnerRebaseRequest) -> Result<OwnerRebaseReceipt> {
        // By the owner of the Task's checkout, whoever owns the target.
        let Some((daemon_id, runtime_id, fence)) =
            Self::workspace_fence(&request.workspace, &request.expected_head_sha)
        else {
            return IntegrationOwnerPort::rebase(&*self.server, request).await;
        };
        let Ok(effect_owner) = self
            .off_target_owner(&request.fence, &request.workspace)
            .await?
        else {
            return Ok(OwnerRebaseReceipt::Refused {
                reason: OwnerEffectRefusal::ForeignOwner,
            });
        };
        let effect = IntegrationEffectRequest {
            fence: request.fence.clone(),
            kind: IntegrationOperationKind::Rebase,
            witness: {
                let mut witness = json!({"workspace":request.workspace,"target_branch":request.target_branch,"expected_head_sha":request.expected_head_sha,"expected_target_sha":request.expected_target_sha,"handoff_conflicts":request.handoff_conflicts,"deadline_nanos":request.deadline.as_nanos().to_string()});
                if let Some(owner) = effect_owner {
                    witness["effect_owner"] = owner;
                }
                witness
            },
        };
        let params = serde_json::to_value(WorkspaceOwnerOperationParams {
            fence,
            workspace_handle: request.workspace.handle.clone(),
            operation: WorkspaceOwnerOperation::RebaseTarget {
                target_branch: request.target_branch.clone(),
                handoff_conflicts: request.handoff_conflicts,
            },
        })
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        let receipt = tokio::select! {
            biased;
            // The owner cannot be stopped from here. Its result is unknown to
            // this caller: the intent stays and the receipt is asked for.
            _ = request.cancel.cancelled() => {
                return Err(ServiceError::invalid_operation(
                    "integration rebase reply abandoned; the owner's receipt decides",
                ))
            }
            receipt = self.effect(&daemon_id, &runtime_id, effect, METHOD_WORKSPACE_RESET, params, request.deadline) => receipt?,
        };
        match serde_json::from_value(receipt.result.clone()) {
            Ok(decoded) => Ok(decoded),
            Err(_) => undecodable(&receipt, OwnerRebaseReceipt::NotPerformed {}),
        }
    }

    async fn fast_forward(&self, request: OwnerFastForwardRequest) -> Result<OwnerMergeReceipt> {
        // By the owner of the default checkout. A Task in a worktree of that
        // checkout is fast-forwarded from the worktree; a Task anywhere else
        // (another machine, or another clone on the same one) from the ref
        // this claim generation's outbound transfer bound there.
        let Some((daemon_id, runtime_id)) = daemon_owner(&request.fence.target_owner) else {
            return IntegrationOwnerPort::fast_forward(&*self.server, request).await;
        };
        let location_id = request.fence.target_owner["location_id"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        let in_default_checkout = matches!(
            &request.workspace.owner,
            EffectOwner::Daemon { daemon_id: holder, .. } if *holder == daemon_id
        ) && self
            .fences
            .db()
            .integration_placement_owner(&request.workspace.placement_id)
            .await?
            .is_some_and(|owner| owner["location_id"].as_str() == Some(&location_id));
        let transfer_key = (!in_default_checkout).then(|| {
            api_types::object_transfer_key(
                &request.fence.attempt_id,
                request.fence.generation,
                api_types::ObjectTransferDirection::Outbound,
            )
        });
        let fence = match &transfer_key {
            None => {
                let Some((_, _, fence)) =
                    Self::workspace_fence(&request.workspace, &request.candidate_sha)
                else {
                    return Ok(OwnerMergeReceipt::Refused {
                        reason: OwnerEffectRefusal::ForeignOwner,
                    });
                };
                fence
            }
            Some(_) => WorkspaceMutationFence {
                integration: WorkspaceIntegrationBinding::TaskStep,
                daemon_id: daemon_id.clone(),
                runtime_id: runtime_id.clone(),
                placement_id: String::new(),
                operation_id: "assigned-by-attempt-sink".into(),
                generation: 0,
                expected: WorkspaceOperationExpected::BaseSha {
                    sha: request.candidate_sha.clone(),
                },
            },
        };
        let mut witness = json!({"workspace":request.workspace,"target_branch":request.target_branch,"task_branch":request.task_branch,"expected_head_sha":request.candidate_sha,"expected_target_sha":request.target_sha,"reviewed":{"commit_sha":request.candidate_sha,"base_sha":request.target_sha},"deadline_nanos":request.deadline.as_nanos().to_string()});
        if let Some(key) = &transfer_key {
            witness["candidate_transfer_key"] = json!(key);
        }
        let effect = IntegrationEffectRequest {
            fence: request.fence.clone(),
            kind: IntegrationOperationKind::FastForward,
            witness,
        };
        let params = serde_json::to_value(WorkspaceReviewedMergeParams {
            merge: WorkspaceMergeParams {
                fence,
                workspace_handle: if transfer_key.is_some() {
                    String::new()
                } else {
                    request.workspace.handle.clone()
                },
                repo_location_id: location_id,
                target_branch: request.target_branch.clone(),
                expected_target_sha: request.target_sha.clone(),
                handed_off_paths: Vec::new(),
            },
            reviewed_commit_sha: Some(request.candidate_sha.clone()),
            candidate_transfer_key: transfer_key,
        })
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        let receipt = self
            .effect(
                &daemon_id,
                &runtime_id,
                effect,
                METHOD_WORKSPACE_MERGE,
                params,
                request.deadline,
            )
            .await?;
        match serde_json::from_value(receipt.result.clone()) {
            Ok(decoded) => Ok(decoded),
            Err(_) => undecodable(&receipt, OwnerMergeReceipt::NotPerformed {}),
        }
    }

    async fn reconcile_effect(&self, request: &IntegrationEffectRequest) -> Result<()> {
        // The owner that performed the effect holds its receipt.
        match daemon_owner(request.effect_owner()) {
            None => self.server.reconcile_effect(request).await,
            // A lookup, never the effect: the owner answers from its journal
            // and the receipt is recorded before it is acknowledged.
            Some((daemon_id, _)) => self
                .client
                .reconcile_integration_attempts(&daemon_id)
                .await
                .map_err(client_error),
        }
    }

    async fn reconcile_outstanding(&self) -> Result<()> {
        // Daemon owners are asked on reconnect and by `reconcile_effect`.
        self.server.reconcile_outstanding().await
    }

    fn owner_connection(&self, effect_owner: &serde_json::Value) -> Option<u64> {
        match daemon_owner(effect_owner) {
            None => Some(0),
            Some((daemon_id, _)) => self.client.connection_id(&daemon_id),
        }
    }
}
