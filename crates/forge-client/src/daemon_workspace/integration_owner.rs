//! Owner fence admission precedes every Git probe. Journal identity is the
//! attempt/effect/fence key, and completed entries replay before admission.
use super::*;

fn refusal(reason: IntegrationOwnerRefusal) -> DaemonErrorPayload {
    DaemonErrorPayload {
        code: "integration_owner_refused".into(),
        message: format!("integration owner refused {reason:?}"),
        details: Some(serde_json::json!({"refusal":reason})),
    }
}

impl DaemonWorkspaceBackend {
    pub(super) fn checkout_key(&self, params: &Value) -> String {
        if let Some(location) = params.get("repo_location_id").and_then(Value::as_str) {
            return location.to_owned();
        }
        if let Some(handle) = params.get("workspace_handle").and_then(Value::as_str) {
            return format!("workspace:{handle}");
        }
        params["placement_id"]
            .as_str()
            .unwrap_or("locations")
            .to_owned()
    }

    /// A Task-step effect left without an outcome by an owner restart would
    /// refuse every later effect on its checkout, and nothing looks it up once
    /// its step has moved on. The caller holds this checkout's lock, so that
    /// effect is not running: settle it as interrupted, with a receipt, and let
    /// the new request through. A queue claim's intent is left for its lookup.
    pub(super) async fn settle_orphaned_task_step_effect(
        &self,
        fence: &WorkspaceMutationFence,
        params: &Value,
    ) -> CommandResult<()> {
        if !matches!(
            fence.integration,
            WorkspaceIntegrationBinding::TaskStepEffect { .. }
        ) {
            return Ok(());
        }
        let checkout = self.checkout_key(params);
        let pending = self
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .integration_pending
            .get(&checkout)
            .cloned();
        let Some(pending) = pending.filter(|pending| pending != &fence.operation_id) else {
            return Ok(());
        };
        let Some(mut operation) = self.journal.operation(&pending).map_err(storage_error)? else {
            return Ok(());
        };
        if operation.outcome.is_some()
            || !matches!(
                operation.fence.integration,
                WorkspaceIntegrationBinding::TaskStepEffect { .. }
            )
        {
            return Ok(());
        }
        operation.outcome = Some(Err(interrupted_error(&pending)));
        let uncertain = operation.effect_started;
        self.attach_integration_receipt(&mut operation, None, uncertain)
            .await;
        self.journal
            .finish_operation(&operation)
            .map_err(storage_error)?;
        Ok(())
    }

    pub(super) fn admit_integration(
        &self,
        fence: &WorkspaceMutationFence,
        method: &str,
        params: &Value,
    ) -> CommandResult<()> {
        let (WorkspaceIntegrationBinding::Attempt { request }
        | WorkspaceIntegrationBinding::TaskStepEffect { request }) = &fence.integration
        else {
            return Ok(());
        };
        let owner = &request.fence.target_owner;
        if owner["owner_kind"] != "daemon"
            || owner["daemon_id"].as_str() != Some(&self.daemon_id)
            || owner["runtime_id"].as_str() != Some(&fence.runtime_id)
            || fence.daemon_id != self.daemon_id
        {
            return Err(refusal(IntegrationOwnerRefusal::ForeignOwner));
        }
        if (matches!(
            fence.integration,
            WorkspaceIntegrationBinding::Attempt { .. }
        ) && request.operation_id() != fence.operation_id)
            || request.fence.lease_owner.is_empty()
        {
            return Err(refusal(IntegrationOwnerRefusal::RequestConflict));
        }
        let allowed = match request.kind {
            WorkspaceIntegrationKind::Merge | WorkspaceIntegrationKind::FastForward => {
                method == METHOD_WORKSPACE_MERGE
            }
            WorkspaceIntegrationKind::Rebase => {
                method == METHOD_WORKSPACE_RESET
                    && params.pointer("/operation/kind").and_then(Value::as_str)
                        == Some("rebase_target")
            }
            WorkspaceIntegrationKind::Check => method == METHOD_WORKSPACE_RUN,
        };
        if !allowed {
            return Err(refusal(IntegrationOwnerRefusal::RequestConflict));
        }
        let checkout = self.checkout_key(params);
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(pending) = state.integration_pending.get(&checkout) {
            if pending != &fence.operation_id
                && self
                    .journal
                    .operation(pending)
                    .map_err(storage_error)?
                    .is_some_and(|operation| operation.outcome.is_none())
            {
                return Err(refusal(IntegrationOwnerRefusal::ReconciliationRequired));
            }
        }
        if matches!(
            fence.integration,
            WorkspaceIntegrationBinding::Attempt { .. }
        ) && (request.fence.generation < 1
            || state
                .integration_fences
                .get(&request.fence.queue_id)
                .is_some_and(|current| {
                    request.fence.generation < current.generation
                        || (request.fence.generation == current.generation
                            && request.fence != *current)
                }))
        {
            return Err(refusal(IntegrationOwnerRefusal::StaleFence));
        }
        let location_id = owner["location_id"]
            .as_str()
            .ok_or_else(|| refusal(IntegrationOwnerRefusal::ForeignOwner))?;
        let location = state
            .locations
            .get(location_id)
            .ok_or_else(|| refusal(IntegrationOwnerRefusal::ForeignOwner))?;
        // A queue claim freezes the location generation it resolved. A Task
        // step is bound to its step lease and placement instead: the server
        // bumps a location's version for reasons this owner is never told
        // (default toggles, provisioning notes), and the copy held here is
        // only refreshed by a verify, so comparing it would refuse a live
        // Task's merge until the daemon reconnects.
        let queue_claim = matches!(
            fence.integration,
            WorkspaceIntegrationBinding::Attempt { .. }
        );
        if location.daemon_id != fence.daemon_id
            || location.runtime_id != fence.runtime_id
            || (queue_claim
                && (owner["generation"].as_i64() != Some(location.version)
                    || location.kind != DaemonRepoLocationKind::PrimaryCheckout))
        {
            return Err(refusal(IntegrationOwnerRefusal::ForeignOwner));
        }
        let witness = &request.witness["workspace"];
        let handle = params["workspace_handle"]
            .as_str()
            .ok_or_else(|| refusal(IntegrationOwnerRefusal::WitnessMismatch))?;
        let owned = state
            .handles
            .get(handle)
            .ok_or_else(|| refusal(IntegrationOwnerRefusal::WitnessMismatch))?;
        if owned.cleaned
            || witness["placement_id"].as_str() != Some(&owned.placement_id)
            || witness["handle"].as_str() != Some(handle)
            || witness["generation"].as_u64() != Some(owned.generation)
            || owned.generation != fence.generation
            || owned.runtime_id != fence.runtime_id
            || witness["owner"]["kind"] != "daemon"
            || witness["owner"]["daemon_id"].as_str() != Some(&fence.daemon_id)
            || witness["owner"]["runtime_id"].as_str() != Some(&fence.runtime_id)
        {
            return Err(refusal(IntegrationOwnerRefusal::WitnessMismatch));
        }
        // The transport request must perform exactly the effect frozen by the
        // recorder, not merely reuse its attempt identity with different input.
        if !matches!(&fence.expected, WorkspaceOperationExpected::BaseSha { sha } if request.witness["expected_head_sha"].as_str() == Some(sha))
        {
            return Err(refusal(IntegrationOwnerRefusal::WitnessMismatch));
        }
        let target = request.witness["target_branch"].as_str();
        match request.kind {
            WorkspaceIntegrationKind::Merge | WorkspaceIntegrationKind::FastForward => {
                if matches!(
                    fence.integration,
                    WorkspaceIntegrationBinding::Attempt { .. }
                ) {
                    let reviewed = if request.kind == WorkspaceIntegrationKind::FastForward {
                        request.witness["expected_head_sha"].as_str()
                    } else {
                        request.witness["reviewed"]["commit_sha"].as_str()
                    };
                    if params["reviewed_commit_sha"].as_str() != reviewed
                        || request.witness["reviewed"]["base_sha"]
                            .as_str()
                            .is_some_and(|base| {
                                Some(base) != request.witness["expected_target_sha"].as_str()
                            })
                        || params["handed_off_paths"]
                            != request
                                .witness
                                .get("handed_off_paths")
                                .cloned()
                                .unwrap_or_else(|| serde_json::json!([]))
                    {
                        return Err(refusal(IntegrationOwnerRefusal::RequestConflict));
                    }
                }
                if params["repo_location_id"].as_str() != Some(location_id)
                    || params["target_branch"].as_str() != target
                    || params["expected_target_sha"] != request.witness["expected_target_sha"]
                {
                    return Err(refusal(IntegrationOwnerRefusal::WitnessMismatch));
                }
            }
            WorkspaceIntegrationKind::Rebase => {
                if params
                    .pointer("/operation/target_branch")
                    .and_then(Value::as_str)
                    != target
                    || params.pointer("/operation/handoff_conflicts")
                        != request.witness.get("handoff_conflicts")
                {
                    return Err(refusal(IntegrationOwnerRefusal::WitnessMismatch));
                }
            }
            WorkspaceIntegrationKind::Check => {
                use sha2::Digest;
                let env_digest = format!(
                    "{:x}",
                    sha2::Sha256::digest(params["env"].to_string().as_bytes())
                );
                if request.witness["environment_digest"].as_str() != Some(&env_digest) {
                    return Err(refusal(IntegrationOwnerRefusal::RequestConflict));
                }
                for field in ["command", "purpose", "timeout_secs", "max_output_bytes"] {
                    if params.get(field) != request.witness.get(field) {
                        return Err(refusal(IntegrationOwnerRefusal::RequestConflict));
                    }
                }
            }
        }
        let mut updated = state.clone();
        updated
            .integration_pending
            .insert(checkout, fence.operation_id.clone());
        if matches!(
            fence.integration,
            WorkspaceIntegrationBinding::Attempt { .. }
        ) {
            updated
                .advance_integration_fence(&request.fence, unix_now())
                .map_err(refusal)?;
        }
        self.journal
            .save_workspace_state(&updated)
            .map_err(storage_error)?;
        *state = updated;
        Ok(())
    }

    pub(super) async fn verify_integration_objects(
        &self,
        fence: &WorkspaceMutationFence,
    ) -> CommandResult<()> {
        let (WorkspaceIntegrationBinding::Attempt { request }
        | WorkspaceIntegrationBinding::TaskStepEffect { request }) = &fence.integration
        else {
            return Ok(());
        };
        // Task-step mode retains the effect's established classification
        // order (dirty, reviewed HEAD, ancestry, target moved). Its physical
        // placement/owner witness was checked above without Git.
        if matches!(
            fence.integration,
            WorkspaceIntegrationBinding::TaskStepEffect { .. }
        ) {
            return Ok(());
        }
        let witness = &request.witness;
        let handle = witness["workspace"]["handle"]
            .as_str()
            .ok_or_else(|| refusal(IntegrationOwnerRefusal::WitnessMismatch))?;
        let owned = self.workspace(&reference(fence, handle), false)?;
        let location = {
            let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            state
                .locations
                .get(
                    request.fence.target_owner["location_id"]
                        .as_str()
                        .unwrap_or_default(),
                )
                .cloned()
        }
        .ok_or_else(|| refusal(IntegrationOwnerRefusal::ForeignOwner))?;
        let head = witness["expected_head_sha"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| refusal(IntegrationOwnerRefusal::WitnessMismatch))?;
        if git::get_current_sha(&owned.path).await.map_err(git_error)? != head {
            return Err(refusal(IntegrationOwnerRefusal::WitnessMismatch));
        }
        if request.kind != WorkspaceIntegrationKind::Check {
            let branch = witness["target_branch"]
                .as_str()
                .ok_or_else(|| refusal(IntegrationOwnerRefusal::WitnessMismatch))?;
            let expected = witness["expected_target_sha"]
                .as_str()
                .filter(|s| !s.is_empty())
                .ok_or_else(|| refusal(IntegrationOwnerRefusal::WitnessMismatch))?;
            if resolve_commit(&location.path, &format!("refs/heads/{branch}")).await? != expected
                || (request.kind == WorkspaceIntegrationKind::Rebase
                    && resolve_commit(&owned.path, &format!("refs/heads/{branch}")).await?
                        != expected)
            {
                return Err(refusal(IntegrationOwnerRefusal::WitnessMismatch));
            }
        }
        Ok(())
    }
}

impl DaemonWorkspaceBackend {
    pub(super) async fn attach_integration_receipt(
        &self,
        operation: &mut JournalOperation,
        cancelled: Option<bool>,
        uncertain: bool,
    ) {
        let (WorkspaceIntegrationBinding::Attempt { request }
        | WorkspaceIntegrationBinding::TaskStepEffect { request }) = &operation.fence.integration
        else {
            return;
        };
        let result = if !operation.effect_started {
            serde_json::json!({"kind":"not_performed"})
        } else if cancelled.is_some() || uncertain {
            let handle = request.witness["workspace"]["handle"]
                .as_str()
                .unwrap_or_default();
            let path = self
                .state
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .handles
                .get(handle)
                .map(|w| w.path.clone());
            let (head_sha, rebase_in_progress) = if let Some(path) = path {
                tokio::time::timeout(Duration::from_secs(2), async {
                    (
                        git::get_current_sha(&path).await.ok(),
                        git::detect_rebase_in_progress(&path).await.unwrap_or(true),
                    )
                })
                .await
                .unwrap_or((None, true))
            } else {
                (None, true)
            };
            if uncertain {
                serde_json::json!({"kind":"infrastructure","message":"owner stopped before receipt retention","head_sha":head_sha,"rebase_in_progress":rebase_in_progress})
            } else {
                serde_json::json!({"kind":if cancelled == Some(true) { "timed_out" } else { "cancelled" },"head_sha":head_sha,"rebase_in_progress":rebase_in_progress})
            }
        } else {
            match operation.outcome.as_ref().expect("settled integration") {
                Ok(value) => {
                    let mut outcome = value["outcome"].clone();
                    if matches!(
                        request.kind,
                        WorkspaceIntegrationKind::Merge | WorkspaceIntegrationKind::FastForward
                    ) {
                        let variant = match outcome["kind"].as_str().unwrap_or_default() {
                            "done" => "Done",
                            "conflict" => "Conflict",
                            "dirty" => "Dirty",
                            "target_dirty" => "TargetDirty",
                            "target_moved" => "TargetMoved",
                            "review_required" => "ReviewRequired",
                            "unresolved_conflict_markers" => "UnresolvedConflictMarkers",
                            _ => "Invalid",
                        };
                        let mut fields = outcome.as_object().cloned().unwrap_or_default();
                        fields.remove("kind");
                        if variant == "Conflict" {
                            fields.insert(
                                "target_branch".into(),
                                request.witness["target_branch"].clone(),
                            );
                        }
                        outcome = serde_json::json!({variant:fields});
                    }
                    serde_json::json!({"kind":"completed","outcome":outcome,"owner_result":value})
                }
                Err(error) if error.code == "integration_owner_refused" => {
                    serde_json::json!({"kind":"refused","reason":error.details.as_ref().map(|d| &d["refusal"])})
                }
                Err(error) => {
                    serde_json::json!({"kind":"infrastructure","message":error.message.chars().take(4096).collect::<String>()})
                }
            }
        };
        let state = if uncertain || result["kind"] == "infrastructure" {
            "uncertain"
        } else if result["kind"] == "completed" {
            "succeeded"
        } else {
            "failed"
        };
        let receipt = serde_json::json!({"request":request,"result":result,"operation_state":state,"recorded_at":chrono::Utc::now().to_rfc3339()});
        match operation.outcome.as_mut().expect("settled integration") {
            Ok(value) => {
                value["integration_receipt"] = receipt;
            }
            Err(error) => {
                let details = error.details.get_or_insert_with(|| serde_json::json!({}));
                details["entry_id"] = serde_json::json!(operation.entry_id);
                details["operation_id"] = serde_json::json!(operation.fence.operation_id);
                details["integration_receipt"] = receipt;
            }
        }
    }

    pub(super) async fn settle_interrupted_integration(
        &self,
        operation_id: &str,
        timed_out: bool,
    ) -> CommandResult<()> {
        let Some(mut operation) = self
            .journal
            .operation(operation_id)
            .map_err(storage_error)?
        else {
            return Ok(());
        };
        if operation.outcome.is_some() {
            return Ok(());
        }
        operation.outcome = Some(Err(error(
            if timed_out {
                DAEMON_TIMEOUT
            } else {
                WORKSPACE_ERROR
            },
            if timed_out {
                "integration effect timed out"
            } else {
                "workspace operation was cancelled"
            },
        )));
        self.attach_integration_receipt(&mut operation, Some(timed_out), false)
            .await;
        self.journal
            .finish_operation(&operation)
            .map_err(storage_error)?;
        Ok(())
    }
}

/// One fence per queue that targets (or sources objects from) this owner.
/// Past this the least recently recorded fences are dropped.
pub(super) const MAX_INTEGRATION_FENCES: usize = 1024;
/// An acknowledged attempt receipt whose queue never claims again is kept
/// this long for a duplicate of its key, then replaced by a tombstone.
pub(super) const ACKNOWLEDGED_ATTEMPT_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

impl WorkspaceRegistry {
    /// Record `fence` as the queue's high-water mark and return what was held
    /// before. An older generation, or another claim of the same generation,
    /// is refused and changes nothing.
    pub(super) fn advance_integration_fence(
        &mut self,
        fence: &IntegrationOwnerFence,
        now: u64,
    ) -> Result<Option<IntegrationOwnerFence>, IntegrationOwnerRefusal> {
        let previous = self.integration_fences.get(&fence.queue_id).cloned();
        if fence.generation < 1
            || fence.queue_id.is_empty()
            || previous.as_ref().is_some_and(|current| {
                fence.generation < current.generation
                    || (fence.generation == current.generation && fence != current)
            })
        {
            return Err(IntegrationOwnerRefusal::StaleFence);
        }
        self.integration_fences
            .insert(fence.queue_id.clone(), fence.clone());
        self.integration_fence_seen
            .insert(fence.queue_id.clone(), now);
        Ok(previous)
    }
}

impl DaemonWorkspaceBackend {
    /// `integration.announce`: the first message of a claim generation.
    pub(super) fn announce_integration(
        &self,
        params: IntegrationAnnounceParams,
    ) -> CommandResult<IntegrationAnnounceResult> {
        self.check_owner(&params.daemon_id, &params.runtime_id)?;
        validate_id(&params.fence.queue_id)?;
        let previous = self.record_integration_fence(&params.fence)?;
        // The announced queue is live whatever the list says.
        let live = params.live_queue_ids.map(|mut live| {
            live.push(params.fence.queue_id.clone());
            live
        });
        let (pruned_fences, pruned_entries) =
            self.prune_integration_state(live.as_deref(), None)?;
        Ok(IntegrationAnnounceResult {
            queue_id: params.fence.queue_id,
            previous,
            generation: params.fence.generation,
            pruned_fences,
            pruned_entries,
        })
    }

    /// Durably advance the queue's high-water mark; returns the prior fence.
    pub(super) fn record_integration_fence(
        &self,
        fence: &IntegrationOwnerFence,
    ) -> CommandResult<Option<IntegrationOwnerFence>> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let mut updated = state.clone();
        let previous = updated
            .advance_integration_fence(fence, unix_now())
            .map_err(refusal)?;
        if previous.as_ref() != Some(fence) {
            self.journal
                .save_workspace_state(&updated)
                .map_err(storage_error)?;
            *state = updated;
        }
        Ok(previous)
    }

    /// Bounded retention for the owner's queue bookkeeping. Returns how many
    /// fences and journal receipts were dropped.
    ///
    /// * A fence is never dropped while its queue has work this owner still
    ///   answers for: a journal entry the server has not acknowledged (an
    ///   intent in flight or a result not yet stored by the server).
    ///   Dropping such a fence would let a delayed
    ///   frame of an older claim in, and would turn a lookup of the current
    ///   claim into "unknown".
    /// * Otherwise a fence goes when its queue is not in `live_queues`, or as
    ///   the least recently recorded past [`MAX_INTEGRATION_FENCES`]. The
    ///   protected fences are bounded by the journal's own entry bound.
    /// * An acknowledged attempt receipt goes once its queue's fence has moved
    ///   past its generation (a late duplicate is then refused `stale_fence`),
    ///   once the queue's fence is gone, or
    ///   [`ACKNOWLEDGED_ATTEMPT_RETENTION`] after the acknowledgement (in
    ///   both cases a late duplicate then meets a cancellation tombstone).
    /// * A checkout's pending marker goes when its operation was just
    ///   acknowledged, was pruned, or has settled.
    pub(super) fn prune_integration_state(
        &self,
        live_queues: Option<&[String]>,
        acknowledged_operation: Option<&str>,
    ) -> CommandResult<(u32, u32)> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let mut updated = state.clone();
        let before = updated.integration_fences.len();
        if live_queues.is_some() || before > MAX_INTEGRATION_FENCES {
            // Queues with an attempt entry the server has not acknowledged.
            let mut busy = std::collections::HashSet::new();
            for entry in self.journal.pending().map_err(storage_error)? {
                let JournalEntry::Operation { operation } = entry else {
                    continue;
                };
                if let WorkspaceIntegrationBinding::Attempt { request } =
                    &operation.fence.integration
                {
                    if !operation.acknowledged
                        && acknowledged_operation != Some(operation.fence.operation_id.as_str())
                    {
                        busy.insert(request.fence.queue_id.clone());
                    }
                }
            }
            if let Some(live) = live_queues {
                updated
                    .integration_fences
                    .retain(|queue, _| busy.contains(queue) || live.iter().any(|id| id == queue));
            }
            if updated.integration_fences.len() > MAX_INTEGRATION_FENCES {
                let mut by_age: Vec<(u64, String)> = updated
                    .integration_fences
                    .keys()
                    .filter(|queue| !busy.contains(*queue))
                    .map(|queue| {
                        (
                            updated
                                .integration_fence_seen
                                .get(queue)
                                .copied()
                                .unwrap_or_default(),
                            queue.clone(),
                        )
                    })
                    .collect();
                by_age.sort();
                let excess = updated.integration_fences.len() - MAX_INTEGRATION_FENCES;
                for (_, queue) in by_age.into_iter().take(excess) {
                    updated.integration_fences.remove(&queue);
                }
            }
        }
        let fences = &updated.integration_fences;
        updated
            .integration_fence_seen
            .retain(|queue, _| fences.contains_key(queue));
        let pruned_fences = before - updated.integration_fences.len();
        let mut unfenced = Vec::new();
        let removed = self
            .journal
            .prune_acknowledged_attempts(|operation, acknowledged_for| {
                let WorkspaceIntegrationBinding::Attempt { request } = &operation.fence.integration
                else {
                    return false;
                };
                match fences.get(&request.fence.queue_id) {
                    Some(current) if request.fence.generation < current.generation => true,
                    Some(_) if acknowledged_for < ACKNOWLEDGED_ATTEMPT_RETENTION => false,
                    _ => {
                        unfenced.push(operation.fence.operation_id.clone());
                        true
                    }
                }
            })
            .map_err(storage_error)?;
        let now = unix_now();
        for operation_id in &unfenced {
            updated.record_cancel_tombstone(operation_id, now);
        }
        let mut settled = Vec::new();
        for (checkout, operation_id) in &updated.integration_pending {
            if acknowledged_operation == Some(operation_id.as_str())
                || removed.contains(operation_id)
                || self
                    .journal
                    .operation(operation_id)
                    .map_err(storage_error)?
                    .is_some_and(|operation| operation.outcome.is_some())
            {
                settled.push(checkout.clone());
            }
        }
        for checkout in &settled {
            updated.integration_pending.remove(checkout);
        }
        if pruned_fences > 0 || !unfenced.is_empty() || !settled.is_empty() {
            self.journal
                .save_workspace_state(&updated)
                .map_err(storage_error)?;
            *state = updated;
        }
        Ok((pruned_fences as u32, removed.len() as u32))
    }
}
