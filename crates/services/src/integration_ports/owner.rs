//! The head's fenced Git owner: the server for a server-owned target, the
//! daemon that holds the default checkout for a daemon-owned one.
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

/// The daemon a fence's (or a queue's) `target_owner` names.
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
/// list always covers every queue that still targets the daemon and every
/// queue with an outstanding intent on it.
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

    /// Every queue this daemon may still be asked about.
    pub async fn live_queue_ids(&self, daemon_id: &str, fence_queue: &str) -> Result<Vec<String>> {
        let mut live: Vec<String> = sqlx::query_scalar(
            "SELECT id FROM integration_queue WHERE state!='closed' AND json_extract(target_owner_json,'$.daemon_id')=?",
        )
        .bind(daemon_id)
        .fetch_all(self.db.pool())
        .await
        .map_err(db::DbError::from)?;
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

/// Dispatches each effect on the owner its fence names.
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

    /// `None`: the workspace is not held by the daemon the fence names.
    fn daemon_fence(
        fence: &IntegrationOwnerFence,
        workspace: &EffectWorkspace,
        expected_head_sha: &str,
    ) -> Option<(String, WorkspaceMutationFence)> {
        let (daemon_id, runtime_id) = daemon_owner(&fence.target_owner)?;
        if workspace.owner
            != (EffectOwner::Daemon {
                daemon_id: daemon_id.clone(),
                runtime_id: runtime_id.clone(),
            })
        {
            return None;
        }
        Some((
            daemon_id.clone(),
            WorkspaceMutationFence {
                // The attempt sink binds the request and its operation id.
                integration: WorkspaceIntegrationBinding::TaskStep,
                daemon_id,
                runtime_id,
                placement_id: workspace.placement_id.clone(),
                operation_id: "assigned-by-attempt-sink".into(),
                generation: workspace.generation.max(0) as u64,
                expected: WorkspaceOperationExpected::BaseSha {
                    sha: expected_head_sha.to_owned(),
                },
            },
        ))
    }

    async fn effect(
        &self,
        daemon_id: &str,
        request: IntegrationEffectRequest,
        method: &str,
        params: serde_json::Value,
        deadline: Duration,
    ) -> Result<IntegrationEffectReceipt> {
        let runtime_id = request.fence.target_owner["runtime_id"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        self.fences
            .announce(daemon_id, &runtime_id, &request.fence)
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
        if daemon_owner(&request.fence.target_owner).is_none() {
            return IntegrationOwnerPort::rebase(&*self.server, request).await;
        }
        let Some((daemon_id, fence)) = Self::daemon_fence(
            &request.fence,
            &request.workspace,
            &request.expected_head_sha,
        ) else {
            return Ok(OwnerRebaseReceipt::Refused {
                reason: OwnerEffectRefusal::ForeignOwner,
            });
        };
        let effect = IntegrationEffectRequest {
            fence: request.fence.clone(),
            kind: IntegrationOperationKind::Rebase,
            witness: json!({"workspace":request.workspace,"target_branch":request.target_branch,"expected_head_sha":request.expected_head_sha,"expected_target_sha":request.expected_target_sha,"handoff_conflicts":request.handoff_conflicts,"deadline_nanos":request.deadline.as_nanos().to_string()}),
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
            receipt = self.effect(&daemon_id, effect, METHOD_WORKSPACE_RESET, params, request.deadline) => receipt?,
        };
        match serde_json::from_value(receipt.result.clone()) {
            Ok(decoded) => Ok(decoded),
            Err(_) => undecodable(&receipt, OwnerRebaseReceipt::NotPerformed {}),
        }
    }

    async fn fast_forward(&self, request: OwnerFastForwardRequest) -> Result<OwnerMergeReceipt> {
        if daemon_owner(&request.fence.target_owner).is_none() {
            return IntegrationOwnerPort::fast_forward(&*self.server, request).await;
        }
        let Some((daemon_id, fence)) =
            Self::daemon_fence(&request.fence, &request.workspace, &request.candidate_sha)
        else {
            return Ok(OwnerMergeReceipt::Refused {
                reason: OwnerEffectRefusal::ForeignOwner,
            });
        };
        let location_id = request.fence.target_owner["location_id"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        let effect = IntegrationEffectRequest {
            fence: request.fence.clone(),
            kind: IntegrationOperationKind::FastForward,
            witness: json!({"workspace":request.workspace,"target_branch":request.target_branch,"task_branch":request.task_branch,"expected_head_sha":request.candidate_sha,"expected_target_sha":request.target_sha,"reviewed":{"commit_sha":request.candidate_sha,"base_sha":request.target_sha},"deadline_nanos":request.deadline.as_nanos().to_string()}),
        };
        let params = serde_json::to_value(WorkspaceReviewedMergeParams {
            merge: WorkspaceMergeParams {
                fence,
                workspace_handle: request.workspace.handle.clone(),
                repo_location_id: location_id,
                target_branch: request.target_branch.clone(),
                expected_target_sha: request.target_sha.clone(),
                handed_off_paths: Vec::new(),
            },
            reviewed_commit_sha: Some(request.candidate_sha.clone()),
        })
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        let receipt = self
            .effect(
                &daemon_id,
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
        match daemon_owner(&request.fence.target_owner) {
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
}
