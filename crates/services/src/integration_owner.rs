//! Server owner gate and attempt receipt sink. No worker or Task projection.
use crate::{
    integration_effects::{self, EffectOwner, EffectWorkspace},
    Result, ServiceError,
};
use api_types::WorkspaceOwnerOperationOutcome;
use db::{
    IntegrationEffectAdmission, IntegrationEffectRequest, IntegrationOperationKind,
    IntegrationOperationState, IntegrationOwnerFence, SqliteDb,
};
use serde_json::json;
use std::{path::Path, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OwnerEffectRefusal {
    StaleFence,
    ForeignOwner,
    WitnessMismatch,
    ReconciliationRequired,
    RequestConflict,
}
impl From<db::IntegrationEffectRefusal> for OwnerEffectRefusal {
    fn from(v: db::IntegrationEffectRefusal) -> Self {
        use db::IntegrationEffectRefusal as R;
        match v {
            R::StaleFence => Self::StaleFence,
            R::ForeignOwner => Self::ForeignOwner,
            R::ReconciliationRequired => Self::ReconciliationRequired,
            R::RequestConflict => Self::RequestConflict,
        }
    }
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OwnerRebaseReceipt {
    NotPerformed {},
    Completed {
        outcome: WorkspaceOwnerOperationOutcome,
    },
    Refused {
        reason: OwnerEffectRefusal,
    },
    Cancelled {
        head_sha: Option<String>,
        rebase_in_progress: bool,
    },
    TimedOut {
        head_sha: Option<String>,
        rebase_in_progress: bool,
    },
    Infrastructure {
        message: String,
        head_sha: Option<String>,
        rebase_in_progress: bool,
    },
}

pub struct ServerRebaseRequest<'a> {
    pub fence: &'a IntegrationOwnerFence,
    pub workspace: &'a EffectWorkspace,
    pub target_branch: &'a str,
    pub expected_head_sha: &'a str,
    pub expected_target_sha: &'a str,
    pub handoff_conflicts: bool,
    pub deadline: Duration,
    pub cancel: &'a CancellationToken,
}

pub struct ServerIntegrationOwner {
    db: Arc<SqliteDb>,
}
impl ServerIntegrationOwner {
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self { db }
    }
    /// Reconcile under the same physical-owner lock before admitting another
    /// effect. A restarted owner can prove an exact reviewed merge; an unknown
    /// run never becomes permission to repeat it.
    pub async fn reconcile_outstanding(&self) -> Result<()> {
        self.reconcile_checkout(None).await
    }

    /// Reconcile the outstanding effects that share an owner lock with
    /// `request` (its own included). The queue worker's reconcile timer calls
    /// this for its head, so one head never waits behind an unrelated
    /// checkout's running effect.
    pub async fn reconcile_effect(&self, request: &IntegrationEffectRequest) -> Result<()> {
        self.reconcile_checkout(Some(request)).await
    }

    async fn reconcile_checkout(&self, current: Option<&IntegrationEffectRequest>) -> Result<()> {
        for request in self
            .db
            .outstanding_integration_effects("server", None)
            .await?
        {
            // Only intents sharing an owner lock with the coming effect are
            // its business. A rebase or check locks its workspace alone, so it
            // must not queue behind another workspace's in-flight merge.
            let workspace_only = |request: &IntegrationEffectRequest| {
                matches!(
                    request.kind,
                    IntegrationOperationKind::Rebase | IntegrationOperationKind::Check
                )
            };
            if current.is_some_and(|current| {
                current.fence.target_owner["location_id"]
                    != request.fence.target_owner["location_id"]
                    || ((workspace_only(&request) || workspace_only(current))
                        && request.witness["workspace"]["handle"]
                            != current.witness["workspace"]["handle"])
            }) {
                continue;
            }
            let Some(guard) = self.db.lock_integration_reconciliation(&request).await? else {
                continue;
            };
            if !guard.started {
                guard
                    .record(
                        json!({"kind":"not_performed"}),
                        IntegrationOperationState::Failed,
                    )
                    .await?;
                continue;
            }
            let result = self.server_reconciliation_result(&request).await;
            // A queue claim's effect has no step recovery behind it, so this
            // owner settles what it can prove while it holds the checkout
            // lock: no effect of this process is running under it.
            if !matches!(result, Ok(Some(_))) && !db::is_task_step(&request) {
                if let Some((result, state)) = self.queue_effect_settlement(&request).await {
                    guard.record(result, state).await?;
                    continue;
                }
            }
            let (result, state) = match result {
                Ok(Some(result)) => (result, IntegrationOperationState::Succeeded),
                // This owner holds the checkout lock, so a Task-step guard is
                // gone. Its unproven effect settles terminally: today's step
                // recovery re-reads Git and decides whether to repeat it. An
                // uncertain receipt would refuse every later merge here.
                Ok(None) | Err(_) if db::is_task_step(&request) => {
                    (db::superseded_result(), IntegrationOperationState::Failed)
                }
                Ok(None) | Err(_) => (
                    json!({"kind":"infrastructure","message":"owner effect has no exact completion proof","head_sha":null,"rebase_in_progress":true,"since":self.uncertain_since(&request).await}),
                    IntegrationOperationState::Uncertain,
                ),
            };
            guard.record(result, state).await?;
        }
        Ok(())
    }

    /// When this effect was first found unproven (kept across lookups).
    async fn uncertain_since(&self, request: &IntegrationEffectRequest) -> String {
        self.db
            .integration_effect_receipt(request)
            .await
            .ok()
            .flatten()
            .filter(|receipt| receipt.operation_state == IntegrationOperationState::Uncertain)
            .and_then(|receipt| receipt.result["since"].as_str().map(str::to_owned))
            .unwrap_or_else(db::now_rfc3339)
    }

    /// Settlement of a started queue-claim effect that has no exact completion
    /// proof. Called under the effect's own owner lock, so the Git facts are
    /// read with no effect of this process running. `None`: still unknown.
    ///
    /// - fast-forward: the target ref still names the expected tip, so the
    ///   ref was never moved: not performed. (A Git child of a dead server
    ///   that lands later lands the exact candidate; the next validation
    ///   reads it as already merged.)
    /// - rebase: HEAD still names the expected commit and no rebase is
    ///   stopped: not performed. HEAD moved onto the expected target: done.
    ///   A rebase still stopped after the effect's own deadline is aborted
    ///   and settled as timed out.
    async fn queue_effect_settlement(
        &self,
        request: &IntegrationEffectRequest,
    ) -> Option<(serde_json::Value, IntegrationOperationState)> {
        let not_performed = || {
            Some((
                json!({"kind":"not_performed","reason":"ref_unmoved"}),
                IntegrationOperationState::Failed,
            ))
        };
        let witness = &request.witness;
        let text = |key: &str| witness[key].as_str().filter(|value| !value.is_empty());
        let head = text("expected_head_sha")?;
        let target = text("expected_target_sha")?;
        let since = self.uncertain_since(request).await;
        let facts = async {
            match request.kind {
                IntegrationOperationKind::Rebase => {
                    let handle = witness["workspace"]["handle"].as_str()?;
                    let path = Path::new(handle);
                    let now = git::get_current_sha(path).await.ok()?;
                    if git::detect_rebase_in_progress(path).await.ok()? {
                        let bound = witness["deadline_nanos"]
                            .as_str()
                            .and_then(|nanos| nanos.parse::<u64>().ok())
                            .map_or(Duration::from_secs(120), Duration::from_nanos);
                        let stopped = chrono::DateTime::parse_from_rfc3339(&since)
                            .ok()
                            .and_then(|since| {
                                (chrono::Utc::now() - since.with_timezone(&chrono::Utc))
                                    .to_std()
                                    .ok()
                            })
                            .is_some_and(|age| age > bound);
                        if !stopped {
                            return None;
                        }
                        // Nothing of this owner runs under the lock and the
                        // effect's own deadline has passed: put the branch
                        // back, so the next round starts from the candidate.
                        let aborted = git::abort_rebase(path).await.is_ok();
                        let outcome = OwnerRebaseReceipt::TimedOut {
                            head_sha: if aborted {
                                git::get_current_sha(path).await.ok()
                            } else {
                                Some(now)
                            },
                            rebase_in_progress: !aborted,
                        };
                        return Some((json!(outcome), IntegrationOperationState::Failed));
                    }
                    if now == head {
                        return not_performed();
                    }
                    if !git::command_output(path, &["merge-base", "--is-ancestor", target, "HEAD"])
                        .await
                        .ok()?
                        .status
                        .success()
                    {
                        return None;
                    }
                    let paths = if witness["handoff_conflicts"] == true {
                        git::paths_adding_conflict_markers(path, target, "HEAD")
                            .await
                            .ok()?
                    } else {
                        Vec::new()
                    };
                    let outcome = if paths.is_empty() {
                        WorkspaceOwnerOperationOutcome::Rebased
                    } else {
                        WorkspaceOwnerOperationOutcome::Conflict {
                            details: "resumed committed conflict handoff".into(),
                            conflict_paths: paths,
                        }
                    };
                    Some((
                        json!(OwnerRebaseReceipt::Completed { outcome }),
                        IntegrationOperationState::Succeeded,
                    ))
                }
                IntegrationOperationKind::FastForward | IntegrationOperationKind::Merge => {
                    let branch = text("target_branch")?;
                    let path: String = self
                        .db
                        .integration_owner_server_checkout_path(
                            request.fence.target_owner["location_id"].as_str(),
                        )
                        .await
                        .ok()??;
                    let tip = integration_effects::merge::target_tip(Path::new(&path), branch)
                        .await
                        .ok()?;
                    (tip == target && head != target)
                        .then(not_performed)
                        .flatten()
                }
                _ => None,
            }
        };
        tokio::time::timeout(Duration::from_secs(5), facts)
            .await
            .ok()
            .flatten()
    }

    async fn server_reconciliation_result(
        &self,
        request: &IntegrationEffectRequest,
    ) -> Result<Option<serde_json::Value>> {
        let legacy = request.fence.lease_owner.starts_with("task-step:");
        if legacy && request.kind == IntegrationOperationKind::Rebase {
            let path = request.witness["workspace"]["handle"]
                .as_str()
                .unwrap_or_default();
            let target = request.witness["expected_target_sha"]
                .as_str()
                .unwrap_or_default();
            if path.is_empty() || target.is_empty() {
                return Ok(None);
            }
            let proof = tokio::time::timeout(Duration::from_secs(2), async {
                let path = Path::new(path);
                if git::detect_rebase_in_progress(path).await?
                    || !git::command_output(path, &["merge-base", "--is-ancestor", target, "HEAD"])
                        .await?
                        .status
                        .success()
                {
                    return Ok::<_, ServiceError>(None);
                }
                let paths = if request.witness["handoff_conflicts"] == true {
                    git::paths_adding_conflict_markers(path, target, "HEAD").await?
                } else {
                    Vec::new()
                };
                let outcome = if paths.is_empty() {
                    WorkspaceOwnerOperationOutcome::Rebased
                } else {
                    WorkspaceOwnerOperationOutcome::Conflict {
                        details: "resumed committed conflict handoff".into(),
                        conflict_paths: paths,
                    }
                };
                Ok(Some(json!(OwnerRebaseReceipt::Completed { outcome })))
            })
            .await;
            return match proof {
                Ok(result) => result,
                Err(_) => Ok(None),
            };
        }
        let path: Option<String> = self
            .db
            .integration_owner_server_checkout_path(
                request.fence.target_owner["location_id"].as_str(),
            )
            .await?;
        let Some(path) = path else {
            return Ok(None);
        };
        let path = Path::new(&path);
        if !matches!(
            request.kind,
            IntegrationOperationKind::FastForward | IntegrationOperationKind::Merge
        ) || request.witness["reviewed"].is_null()
        {
            return Ok(None);
        }
        let candidate = request.witness["expected_head_sha"]
            .as_str()
            .unwrap_or_default();
        let branch = request.witness["target_branch"]
            .as_str()
            .unwrap_or_default();
        let before = request.witness["expected_target_sha"]
            .as_str()
            .unwrap_or_default();
        if candidate.is_empty() || before.is_empty() || branch.is_empty() {
            return Ok(None);
        }
        let proof = tokio::time::timeout(Duration::from_secs(2), async {
            if legacy {
                let target = integration_effects::merge::target_tip(path, branch).await?;
                return Ok::<_, ServiceError>(
                    git::command_output(path, &["merge-base", "--is-ancestor", candidate, &target])
                        .await?
                        .status
                        .success(),
                );
            }
            Ok::<_, ServiceError>(
                git::get_current_sha(path).await? == candidate
                    && integration_effects::merge::target_tip(path, branch).await? == candidate
                    && git::command_output(
                        path,
                        &["merge-base", "--is-ancestor", before, candidate],
                    )
                    .await?
                    .status
                    .success(),
            )
        })
        .await;
        if !matches!(proof, Ok(Ok(true))) {
            return Ok(None);
        }
        Ok(Some(json!(OwnerMergeReceipt::Completed {
            outcome: crate::MergeOutcome::Done {
                before_sha: before.to_owned(),
                after_sha: candidate.to_owned(),
                branch: branch.to_owned()
            }
        })))
    }

    pub async fn rebase(&self, input: ServerRebaseRequest<'_>) -> Result<OwnerRebaseReceipt> {
        // Owner mismatch is refused before any Git (including fact queries).
        // The rebase belongs to the owner of the Task's checkout. The default
        // checkout may be on another machine: the witness then is the ref the
        // inbound object transfer bound, and this owner is recorded as the
        // effect's owner so that its receipt is asked for here.
        if input.workspace.owner != EffectOwner::Server {
            return Ok(OwnerRebaseReceipt::Refused {
                reason: OwnerEffectRefusal::ForeignOwner,
            });
        }
        let cross_owner = input.fence.target_owner["owner_kind"] != "server";
        let mut witness = json!({"workspace":input.workspace,"target_branch":input.target_branch,"expected_head_sha":input.expected_head_sha,"expected_target_sha":input.expected_target_sha,"handoff_conflicts":input.handoff_conflicts,"deadline_nanos":input.deadline.as_nanos().to_string()});
        if cross_owner {
            match self
                .db
                .integration_placement_owner(&input.workspace.placement_id)
                .await?
            {
                Some(owner) if owner["owner_kind"] == "server" => witness["effect_owner"] = owner,
                _ => {
                    return Ok(OwnerRebaseReceipt::Refused {
                        reason: OwnerEffectRefusal::ForeignOwner,
                    })
                }
            }
        }
        let request = IntegrationEffectRequest {
            fence: input.fence.clone(),
            kind: IntegrationOperationKind::Rebase,
            witness,
        };
        self.reconcile_checkout(Some(&request)).await?;
        let mut guard = match self.db.begin_integration_effect(request).await? {
            IntegrationEffectAdmission::Replay(r) => {
                return serde_json::from_value(r.result).map_err(|e| {
                    ServiceError::invalid_operation(format!("invalid owner rebase receipt: {e}"))
                })
            }
            IntegrationEffectAdmission::Refused(r) => {
                return Ok(OwnerRebaseReceipt::Refused { reason: r.into() })
            }
            IntegrationEffectAdmission::Started(g) => g,
        };
        if let Some(refusal) = guard.start().await? {
            return Ok(OwnerRebaseReceipt::Refused {
                reason: refusal.into(),
            });
        }
        let paths = if cross_owner {
            guard.server_worktree_path().await?.map(|path| (path, None))
        } else {
            guard
                .server_workspace_paths()
                .await?
                .map(|(path, target)| (path, Some(target)))
        };
        let Some((path, target_path)) = paths else {
            let outcome = OwnerRebaseReceipt::Refused {
                reason: OwnerEffectRefusal::WitnessMismatch,
            };
            guard.record(json!(outcome), rebase_state(&outcome)).await?;
            return Ok(outcome);
        };
        let operation = async {
            match verify_objects(
                &path,
                target_path.as_deref(),
                input.target_branch,
                input.expected_head_sha,
                input.expected_target_sha,
                &api_types::object_transfer_key(
                    &input.fence.attempt_id,
                    input.fence.generation,
                    api_types::ObjectTransferDirection::Inbound,
                ),
            )
            .await
            {
                Err(error) => interrupted(&path, false, Some(error.to_string())).await,
                Ok(false) => OwnerRebaseReceipt::Refused {
                    reason: OwnerEffectRefusal::WitnessMismatch,
                },
                Ok(true) => {
                    let effect = integration_effects::rebase::RebaseEffectInput {
                        workspace: input.workspace,
                        worktree_path: &path,
                        target_branch: input.expected_target_sha,
                        handoff_conflicts: input.handoff_conflicts,
                        expected_head_sha: Some(input.expected_head_sha),
                        expected_target_sha: Some(input.expected_target_sha),
                        deadline: None,
                    };
                    match integration_effects::rebase::rebase(&effect).await {
                        Ok(outcome) => OwnerRebaseReceipt::Completed { outcome },
                        Err(error) => interrupted(&path, false, Some(error.to_string())).await,
                    }
                }
            }
        };
        let outcome = tokio::select! {
            biased;
            _ = input.cancel.cancelled() => interrupted(&path, false, None).await,
            result = tokio::time::timeout(input.deadline, operation) => match result {
                Ok(outcome) => outcome,
                Err(_) => interrupted(&path, true, None).await,
            }
        };
        guard.record(json!(outcome), rebase_state(&outcome)).await?;
        Ok(outcome)
    }
}
/// The rebase witness. The Task's checkout must hold the target tip the
/// claim read in the default checkout: as its own target branch when it
/// shares the default checkout's refs (a worktree of it), or, in another
/// clone, as the ref this claim generation's inbound object transfer bound
/// (`refs/forge/integration/<attempt>-<generation>-in`). The rebase is onto
/// that commit, never onto a branch name.
///
/// `target_path` is `None` when the default checkout is on another machine:
/// its tip cannot be read here, and only the transferred ref is a witness
/// (the fast-forward on the target's owner refuses a tip that moved since).
async fn verify_objects(
    path: &Path,
    target_path: Option<&Path>,
    target: &str,
    head: &str,
    target_sha: &str,
    inbound_key: &str,
) -> Result<bool> {
    if head.is_empty() || target_sha.is_empty() {
        return Ok(false);
    }
    if git::get_current_sha(path).await? != head {
        return Ok(false);
    }
    if let Some(target_path) = target_path {
        if integration_effects::merge::target_tip(target_path, target).await? != target_sha {
            return Ok(false);
        }
        if integration_effects::merge::target_tip(path, target)
            .await
            .is_ok_and(|own| own == target_sha)
        {
            return Ok(true);
        }
    }
    let Ok(transferred) = git::integration::transfer_ref(inbound_key) else {
        return Ok(false);
    };
    let bound = git::command_output(
        path,
        &[
            "rev-parse",
            "--verify",
            "--quiet",
            &format!("{transferred}^{{commit}}"),
        ],
    )
    .await?;
    Ok(bound.status.success() && String::from_utf8_lossy(&bound.stdout).trim() == target_sha)
}
async fn interrupted(path: &Path, timed_out: bool, message: Option<String>) -> OwnerRebaseReceipt {
    let (head_sha, rebase_in_progress) = tokio::time::timeout(Duration::from_secs(2), async {
        (
            git::get_current_sha(path).await.ok(),
            git::detect_rebase_in_progress(path).await.unwrap_or(true),
        )
    })
    .await
    .unwrap_or((None, true));
    match message {
        Some(message) => OwnerRebaseReceipt::Infrastructure {
            message: integration_effects::check::tail_bytes(&message, 4096),
            head_sha,
            rebase_in_progress,
        },
        None if timed_out => OwnerRebaseReceipt::TimedOut {
            head_sha,
            rebase_in_progress,
        },
        None => OwnerRebaseReceipt::Cancelled {
            head_sha,
            rebase_in_progress,
        },
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OwnerMergeReceipt {
    NotPerformed {},
    Cancelled {
        head_sha: Option<String>,
        rebase_in_progress: bool,
    },
    TimedOut {
        head_sha: Option<String>,
        rebase_in_progress: bool,
    },
    Completed {
        outcome: crate::MergeOutcome,
    },
    Refused {
        reason: OwnerEffectRefusal,
    },
    Infrastructure {
        message: String,
    },
}
pub struct ServerMergeRequest<'a> {
    pub fence: &'a IntegrationOwnerFence,
    pub workspace: &'a EffectWorkspace,
    pub target_branch: &'a str,
    pub task_branch: &'a str,
    pub expected_head_sha: &'a str,
    pub expected_target_sha: &'a str,
    pub reviewed: Option<integration_effects::merge::ReviewedMergeObject>,
    pub cancel: &'a CancellationToken,
    pub deadline: Option<Duration>,
}
impl ServerIntegrationOwner {
    /// The caller supplies its frozen review permit. The owner gate protects
    /// physical identity and objects, and grants no new review/Task authority.
    pub async fn merge(&self, input: ServerMergeRequest<'_>) -> Result<OwnerMergeReceipt> {
        // The fast-forward belongs to the owner of the default checkout. A
        // Task whose checkout is on another machine has no worktree here: its
        // exact candidate was brought by this claim generation's outbound
        // object transfer, and that ref is the candidate witness.
        let from_transfer = input.workspace.owner != EffectOwner::Server;
        if input.fence.target_owner["owner_kind"] != "server"
            || (from_transfer && input.reviewed.is_none())
        {
            return Ok(OwnerMergeReceipt::Refused {
                reason: OwnerEffectRefusal::ForeignOwner,
            });
        }
        let kind = if input.reviewed.is_some() {
            IntegrationOperationKind::FastForward
        } else {
            IntegrationOperationKind::Merge
        };
        let request = IntegrationEffectRequest {
            fence: input.fence.clone(),
            kind,
            witness: json!({"workspace":input.workspace,"target_branch":input.target_branch,"task_branch":input.task_branch,"expected_head_sha":input.expected_head_sha,"expected_target_sha":input.expected_target_sha,"reviewed":input.reviewed,"deadline_nanos":input.deadline.map(|duration| duration.as_nanos().to_string())}),
        };
        self.reconcile_checkout(Some(&request)).await?;
        let mut guard = match self.db.begin_integration_effect(request).await? {
            IntegrationEffectAdmission::Replay(r) => {
                return serde_json::from_value(r.result).map_err(|e| {
                    ServiceError::invalid_operation(format!("invalid owner merge receipt: {e}"))
                })
            }
            IntegrationEffectAdmission::Refused(r) => {
                return Ok(OwnerMergeReceipt::Refused { reason: r.into() })
            }
            IntegrationEffectAdmission::Started(g) => g,
        };
        if let Some(refusal) = guard.start().await? {
            return Ok(OwnerMergeReceipt::Refused {
                reason: refusal.into(),
            });
        }
        let paths = if from_transfer {
            guard
                .server_target_path()
                .await?
                .map(|target| (target.clone(), target))
        } else {
            guard.server_workspace_paths().await?
        };
        let outcome = match paths {
            None => OwnerMergeReceipt::Refused {
                reason: OwnerEffectRefusal::WitnessMismatch,
            },
            Some((path, target)) => {
                let effect = integration_effects::merge::MergeEffectInput {
                    workspace: input.workspace,
                    worktree_path: &path,
                    repo_path: &target,
                    target_branch: input.target_branch,
                    task_branch: input.task_branch,
                    diagnostic_entity_id: &input.fence.attempt_id,
                    before_sha: input.expected_target_sha,
                    expected_head_sha: input.expected_head_sha,
                    observed_target_sha: input.expected_target_sha,
                    reviewed: input.reviewed,
                };
                let deadline = async {
                    match input.deadline {
                        Some(duration) => tokio::time::sleep(duration).await,
                        None => std::future::pending().await,
                    }
                };
                tokio::select! {
                    biased;
                    _ = input.cancel.cancelled() => merge_interrupted(&target, false).await,
                    _ = deadline => merge_interrupted(&target, true).await,
                    result = async {
                        if from_transfer {
                            server_fast_forward_transferred(&effect, &api_types::object_transfer_key(
                                &input.fence.attempt_id,
                                input.fence.generation,
                                api_types::ObjectTransferDirection::Outbound,
                            )).await
                        } else {
                            server_merge(&effect).await
                        }
                    } => match result {
                        Ok(Some(outcome)) => OwnerMergeReceipt::Completed { outcome },
                        Ok(None) => OwnerMergeReceipt::Refused { reason: OwnerEffectRefusal::WitnessMismatch },
                        Err(e) => OwnerMergeReceipt::Infrastructure { message: integration_effects::check::tail_bytes(&e.to_string(), 4096) },
                    }
                }
            }
        };
        let state = match &outcome {
            OwnerMergeReceipt::Completed { .. } => IntegrationOperationState::Succeeded,
            OwnerMergeReceipt::Refused { .. }
            | OwnerMergeReceipt::NotPerformed { .. }
            | OwnerMergeReceipt::Cancelled { .. }
            | OwnerMergeReceipt::TimedOut { .. } => IntegrationOperationState::Failed,
            OwnerMergeReceipt::Infrastructure { .. } => IntegrationOperationState::Uncertain,
        };
        guard.record(json!(outcome), state).await?;
        Ok(outcome)
    }
}
async fn merge_interrupted(path: &Path, timed_out: bool) -> OwnerMergeReceipt {
    let (head_sha, rebase_in_progress) = tokio::time::timeout(Duration::from_secs(2), async {
        (
            git::get_current_sha(path).await.ok(),
            git::detect_rebase_in_progress(path).await.unwrap_or(true),
        )
    })
    .await
    .unwrap_or((None, true));
    if timed_out {
        OwnerMergeReceipt::TimedOut {
            head_sha,
            rebase_in_progress,
        }
    } else {
        OwnerMergeReceipt::Cancelled {
            head_sha,
            rebase_in_progress,
        }
    }
}

async fn server_merge(
    input: &integration_effects::merge::MergeEffectInput<'_>,
) -> Result<Option<crate::MergeOutcome>> {
    use integration_effects::merge::*;
    if input.expected_head_sha.is_empty()
        || input.observed_target_sha.is_empty()
        || git::get_current_sha(input.worktree_path).await? != input.expected_head_sha
        || target_tip(input.repo_path, input.target_branch).await? != input.observed_target_sha
    {
        return Ok(None);
    }
    if let Some(outcome) = merge_cleanliness(input.worktree_path, input.repo_path).await? {
        return Ok(Some(outcome));
    }
    let already = match validate_merge_candidate(input).await? {
        MergeCandidateOutcome::Ready { already_merged } => already_merged,
        MergeCandidateOutcome::Refused(outcome) => return Ok(Some(outcome)),
    };
    let applied = apply_merge(input, already).await?;
    Ok(Some(merge_result(input, already, applied).await?))
}

/// The fast-forward of a candidate held by no worktree on this host. The
/// candidate witness is the ref this claim generation's outbound transfer
/// bound in the default checkout; the target witness is its unchanged tip.
/// `None`: a witness does not hold (the driver reads the facts again).
async fn server_fast_forward_transferred(
    input: &integration_effects::merge::MergeEffectInput<'_>,
    outbound_key: &str,
) -> Result<Option<crate::MergeOutcome>> {
    use integration_effects::merge::*;
    let candidate = input.expected_head_sha;
    if candidate.is_empty() || input.observed_target_sha.is_empty() {
        return Ok(None);
    }
    let Ok(transferred) = git::integration::transfer_ref(outbound_key) else {
        return Ok(None);
    };
    let bound = git::command_output(
        input.repo_path,
        &[
            "rev-parse",
            "--verify",
            "--quiet",
            &format!("{transferred}^{{commit}}"),
        ],
    )
    .await?;
    if !bound.status.success()
        || String::from_utf8_lossy(&bound.stdout).trim() != candidate
        || target_tip(input.repo_path, input.target_branch).await? != input.observed_target_sha
    {
        return Ok(None);
    }
    if !git::is_worktree_clean(input.repo_path).await? {
        return Ok(Some(crate::MergeOutcome::TargetDirty {
            files: git::status_porcelain(input.repo_path).await?,
        }));
    }
    // A compare-and-swap of the branch from the witnessed tip to the
    // candidate: a writer that moved the target after the read above loses
    // nothing and gains nothing, the swap just does not happen.
    let applied = match git::integration::fast_forward_exact(
        input.repo_path,
        input.target_branch,
        candidate,
        input.observed_target_sha,
        git::integration::FastForwardLimits {
            deadline: Duration::from_secs(30),
            output_bytes: ::review::contract::MAX_EVIDENCE_BYTES,
        },
    )
    .await?
    {
        git::integration::MergeApplyOutcome::Applied => MergeApplyOutcome::Applied,
        git::integration::MergeApplyOutcome::ManualFailed(error) => {
            MergeApplyOutcome::ManualFailed(error)
        }
        git::integration::MergeApplyOutcome::ReviewRequired { reason } => {
            MergeApplyOutcome::ReviewRequired { reason }
        }
        git::integration::MergeApplyOutcome::ExactObjectMismatch
        | git::integration::MergeApplyOutcome::TargetMoved => {
            MergeApplyOutcome::ExactObjectMismatch
        }
    };
    Ok(Some(merge_result(input, false, applied).await?))
}

fn rebase_state(outcome: &OwnerRebaseReceipt) -> IntegrationOperationState {
    match outcome {
        OwnerRebaseReceipt::Completed { .. } => IntegrationOperationState::Succeeded,
        OwnerRebaseReceipt::Infrastructure { .. } => IntegrationOperationState::Uncertain,
        _ => IntegrationOperationState::Failed,
    }
}

/// How an object transfer step ended. A refusal or a cancellation leaves no
/// temporary file and no change in either repository.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectTransferOutcome<T> {
    Done(T),
    Refused(api_types::ObjectTransferRefusal),
    Cancelled,
}

impl From<git::integration::ObjectTransferError> for ServiceError {
    fn from(error: git::integration::ObjectTransferError) -> Self {
        match error {
            git::integration::ObjectTransferError::Git(error) => error.into(),
            other => ServiceError::invalid_operation(other.to_string()),
        }
    }
}

fn transfer_refusal(
    error: git::integration::ObjectTransferError,
) -> Result<api_types::ObjectTransferRefusal> {
    use api_types::ObjectTransferRefusal as R;
    use git::integration::ObjectTransferError as E;
    Ok(match error {
        E::TooLarge { bytes, max_bytes } => R::TooLarge { bytes, max_bytes },
        E::Invalid { reason } => R::Invalid { reason },
        E::MissingObject { sha } => R::MissingObject { sha },
        E::KeyConflict { existing_sha } => R::KeyConflict { existing_sha },
        E::Git(error) => return Err(error.into()),
    })
}

/// Export from a server-owned checkout: the same operation a daemon serves as
/// `integration.export_objects`.
pub struct ServerObjectExport<'a> {
    pub fence: &'a IntegrationOwnerFence,
    /// `api_types::object_transfer_key(attempt, generation, direction)`.
    pub key: &'a str,
    pub repo_location_id: &'a str,
    pub have: &'a [String],
    pub want: &'a str,
    /// Where the bundle is written; removed unless the export succeeds.
    pub dest: &'a Path,
    pub cancel: &'a CancellationToken,
}

/// Import into a server-owned checkout under `refs/forge/integration/<key>`.
pub struct ServerObjectImport<'a> {
    pub fence: &'a IntegrationOwnerFence,
    pub export: &'a api_types::ObjectExportReceipt,
    pub repo_location_id: &'a str,
    /// Not read for an export of zero bytes.
    pub bundle: &'a Path,
    pub cancel: &'a CancellationToken,
}

pub(crate) fn transfer_io(error: std::io::Error) -> ServiceError {
    git::GitError::Io(error).into()
}

async fn sha256_file(path: &Path) -> Result<String> {
    use sha2::Digest;
    use tokio::io::AsyncReadExt;
    let mut file = tokio::fs::File::open(path).await.map_err(transfer_io)?;
    let mut digest = sha2::Sha256::new();
    let mut buffer = vec![0; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).await.map_err(transfer_io)?;
        if read == 0 {
            return Ok(hex::encode(digest.finalize()));
        }
        digest.update(&buffer[..read]);
    }
}

impl ServerIntegrationOwner {
    /// The claim that asks must still be the attempt's current fence, and the
    /// checkout must be one this server owns.
    async fn admit_transfer(
        &self,
        fence: &IntegrationOwnerFence,
        repo_location_id: &str,
    ) -> Result<std::result::Result<std::path::PathBuf, api_types::ObjectTransferRefusal>> {
        if self.db.integration_owner_fence(&fence.attempt_id).await? != Some(fence.clone()) {
            return Ok(Err(api_types::ObjectTransferRefusal::StaleFence));
        }
        let path: Option<String> = self
            .db
            .integration_transfer_ready_server_location(repo_location_id)
            .await?;
        Ok(path.map(std::path::PathBuf::from).ok_or_else(|| {
            api_types::ObjectTransferRefusal::Invalid {
                reason: "checkout is not a ready server-owned repo location".into(),
            }
        }))
    }

    /// The receipt of an import this key already completed here, if any.
    pub async fn imported_objects(
        &self,
        fence: &IntegrationOwnerFence,
        repo_location_id: &str,
        key: &str,
        expected_tip_sha: &str,
    ) -> Result<ObjectTransferOutcome<Option<api_types::ObjectImportReceipt>>> {
        let repo = match self.admit_transfer(fence, repo_location_id).await? {
            Ok(repo) => repo,
            Err(refusal) => return Ok(ObjectTransferOutcome::Refused(refusal)),
        };
        match git::integration::imported_objects(&repo, key, expected_tip_sha).await {
            Ok(found) => Ok(ObjectTransferOutcome::Done(found.map(|found| {
                api_types::ObjectImportReceipt {
                    key: key.into(),
                    tip_sha: found.tip_sha,
                    ref_name: found.ref_name,
                    replayed: true,
                }
            }))),
            Err(error) => Ok(ObjectTransferOutcome::Refused(transfer_refusal(error)?)),
        }
    }

    /// Write the commits of `want` the receiver lacks to `dest`. Capped at
    /// `MAX_OBJECT_TRANSFER_BYTES`; nothing in the checkout changes.
    pub async fn export_objects(
        &self,
        input: ServerObjectExport<'_>,
    ) -> Result<ObjectTransferOutcome<api_types::ObjectExportReceipt>> {
        let repo = match self
            .admit_transfer(input.fence, input.repo_location_id)
            .await?
        {
            Ok(repo) => repo,
            Err(refusal) => return Ok(ObjectTransferOutcome::Refused(refusal)),
        };
        let export = async {
            let export = git::integration::export_objects(
                &repo,
                input.key,
                input.have,
                input.want,
                input.dest,
                api_types::MAX_OBJECT_TRANSFER_BYTES,
            )
            .await;
            let export = match export {
                Ok(export) => export,
                Err(error) => return Ok(ObjectTransferOutcome::Refused(transfer_refusal(error)?)),
            };
            let sha256 = if export.total_bytes == 0 {
                use sha2::Digest;
                hex::encode(sha2::Sha256::digest([]))
            } else {
                sha256_file(input.dest).await?
            };
            Ok::<_, ServiceError>(ObjectTransferOutcome::Done(
                api_types::ObjectExportReceipt {
                    key: input.key.into(),
                    tip_sha: export.tip_sha,
                    total_bytes: export.total_bytes,
                    sha256,
                },
            ))
        };
        let outcome = tokio::select! {
            biased;
            _ = input.cancel.cancelled() => Ok(ObjectTransferOutcome::Cancelled),
            outcome = export => outcome,
        };
        if !matches!(outcome, Ok(ObjectTransferOutcome::Done(_))) {
            let _ = tokio::fs::remove_file(input.dest).await;
        }
        outcome
    }

    /// Verify the bundle against its receipt and import it. The checkout's
    /// branches, HEAD and work tree do not change.
    pub async fn import_objects(
        &self,
        input: ServerObjectImport<'_>,
    ) -> Result<ObjectTransferOutcome<api_types::ObjectImportReceipt>> {
        use api_types::ObjectTransferRefusal as R;
        let repo = match self
            .admit_transfer(input.fence, input.repo_location_id)
            .await?
        {
            Ok(repo) => repo,
            Err(refusal) => return Ok(ObjectTransferOutcome::Refused(refusal)),
        };
        let export = input.export;
        let import = async {
            match git::integration::imported_objects(&repo, &export.key, &export.tip_sha).await {
                Ok(Some(found)) => return Ok(Ok(found)),
                Ok(None) => {}
                Err(error) => return Ok(Err(transfer_refusal(error)?)),
            }
            if export.total_bytes > api_types::MAX_OBJECT_TRANSFER_BYTES {
                return Ok(Err(R::TooLarge {
                    bytes: export.total_bytes,
                    max_bytes: api_types::MAX_OBJECT_TRANSFER_BYTES,
                }));
            }
            if export.total_bytes > 0 {
                let stored = tokio::fs::metadata(input.bundle).await.map(|m| m.len());
                if stored.ok() != Some(export.total_bytes)
                    || sha256_file(input.bundle).await? != export.sha256
                {
                    return Ok(Err(R::Invalid {
                        reason: "bundle does not match its export receipt".into(),
                    }));
                }
            }
            // Publishing objects and the ref is serialized with the merge on
            // this checkout by the caller's queue slot.
            match git::integration::import_objects(
                &repo,
                &export.key,
                (export.total_bytes > 0).then_some(input.bundle),
                &export.tip_sha,
                api_types::MAX_OBJECT_TRANSFER_BYTES,
            )
            .await
            {
                Ok(done) => Ok::<_, ServiceError>(Ok(done)),
                Err(error) => Ok(Err(transfer_refusal(error)?)),
            }
        };
        let outcome = tokio::select! {
            biased;
            _ = input.cancel.cancelled() => return Ok(ObjectTransferOutcome::Cancelled),
            outcome = import => outcome?,
        };
        Ok(match outcome {
            Ok(done) => ObjectTransferOutcome::Done(api_types::ObjectImportReceipt {
                key: export.key.clone(),
                tip_sha: done.tip_sha,
                ref_name: done.ref_name,
                replayed: done.replayed,
            }),
            Err(refusal) => ObjectTransferOutcome::Refused(refusal),
        })
    }
}

#[async_trait::async_trait]
impl crate::integration_worker::IntegrationOwnerPort for ServerIntegrationOwner {
    async fn rebase(
        &self,
        request: crate::integration_worker::OwnerRebaseRequest,
    ) -> Result<OwnerRebaseReceipt> {
        ServerIntegrationOwner::rebase(
            self,
            ServerRebaseRequest {
                fence: &request.fence,
                workspace: &request.workspace,
                target_branch: &request.target_branch,
                expected_head_sha: &request.expected_head_sha,
                expected_target_sha: &request.expected_target_sha,
                handoff_conflicts: request.handoff_conflicts,
                deadline: request.deadline,
                cancel: &request.cancel,
            },
        )
        .await
    }
    async fn fast_forward(
        &self,
        request: crate::integration_worker::OwnerFastForwardRequest,
    ) -> Result<OwnerMergeReceipt> {
        // Never cancelled: a started fast-forward runs to its receipt or to
        // the owner's own bound.
        let never = CancellationToken::new();
        self.merge(ServerMergeRequest {
            fence: &request.fence,
            workspace: &request.workspace,
            target_branch: &request.target_branch,
            task_branch: &request.task_branch,
            expected_head_sha: &request.candidate_sha,
            expected_target_sha: &request.target_sha,
            reviewed: Some(integration_effects::merge::ReviewedMergeObject {
                commit_sha: request.candidate_sha.clone(),
                base_sha: request.target_sha.clone(),
            }),
            cancel: &never,
            deadline: Some(request.deadline),
        })
        .await
    }
    async fn reconcile_effect(&self, request: &IntegrationEffectRequest) -> Result<()> {
        ServerIntegrationOwner::reconcile_effect(self, request).await
    }
    async fn reconcile_outstanding(&self) -> Result<()> {
        ServerIntegrationOwner::reconcile_outstanding(self).await
    }
}

/// Validate owner facts before persistence or journal acknowledgement.
pub(crate) fn validate_attempt_receipt(receipt: &db::IntegrationEffectReceipt) -> Result<()> {
    let invalid = || {
        ServiceError::invalid_operation(
            "owner attempt receipt contradicts its frozen request or state",
        )
    };
    let completed = receipt.result["kind"] == "completed";
    if (receipt.operation_state == IntegrationOperationState::Succeeded) != completed {
        return Err(invalid());
    }
    match receipt.request.kind {
        IntegrationOperationKind::Merge | IntegrationOperationKind::FastForward => {
            let outcome: OwnerMergeReceipt =
                serde_json::from_value(receipt.result.clone()).map_err(|_| invalid())?;
            if let OwnerMergeReceipt::Completed {
                outcome:
                    crate::MergeOutcome::Done {
                        after_sha, branch, ..
                    },
            } = outcome
            {
                let witness = &receipt.request.witness;
                if witness["target_branch"].as_str() != Some(&branch) {
                    return Err(invalid());
                }
                let reviewed = witness["reviewed"]["commit_sha"].as_str();
                let expected = if receipt.request.kind == IntegrationOperationKind::FastForward {
                    witness["expected_head_sha"].as_str()
                } else {
                    reviewed
                };
                if expected.is_some_and(|head| head != after_sha) {
                    return Err(invalid());
                }
            }
        }
        IntegrationOperationKind::Rebase => {
            let outcome: OwnerRebaseReceipt =
                serde_json::from_value(receipt.result.clone()).map_err(|_| invalid())?;
            if matches!(
                outcome,
                OwnerRebaseReceipt::Completed {
                    outcome: WorkspaceOwnerOperationOutcome::Applied
                        | WorkspaceOwnerOperationOutcome::ReviewCheckout { .. }
                }
            ) {
                return Err(invalid());
            }
        }
        IntegrationOperationKind::Check => {}
        IntegrationOperationKind::Reconcile => return Err(invalid()),
    }
    Ok(())
}

#[cfg(test)]
mod tests;
