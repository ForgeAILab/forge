use super::*;

pub const INTEGRATION_IMPORT_PAGE: u32 = 100;
const EVIDENCE_ROWS: i64 = 64;
const EVIDENCE_TEXT_LIMIT: usize = 65_536;

stored_enum!(IntegrationNeededFact {
    CandidateAncestry => "candidate_ancestry",
    ValidatedOwnerReceipt => "validated_owner_receipt",
    RemoteOperationResult => "original_remote_operation_result",
    NoPossibleEffect => "no_possible_effect",
    LegacyHookContinuation => "legacy_hook_continuation",
    RebaseInProgress => "git_rebase_in_progress_and_conflict_paths",
    RebasedCandidateChecks => "rebased_head_and_bound_checks",
    CurrentRetryConflictPaths => "current_retry_window_conflict_and_repair_paths",
    ReviewAuthority => "current_review_authority",
    CarryHeadBaseChecks => "carry_head_base_and_check_evidence",
    CandidateAvailability => "candidate_object_availability",
    CandidateHeadReviewAuthority => "candidate_head_and_review_authority",
    CompleteLegacyEvidence => "complete_legacy_evidence",
    ExistingAttemptIdentity => "existing_attempt_identity"
});

/// Raw, retained input to the priority tree. JSON columns remain strings so
/// malformed input can be quarantined without losing its original bytes.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IntegrationImportSnapshot {
    pub task: Value,
    pub project: Value,
    pub execution: Option<Value>,
    pub workspace: Option<Value>,
    pub placement: Option<Value>,
    pub steps: Vec<Value>,
    pub checkpoints: Vec<Value>,
    pub transitions: Vec<Value>,
    pub carries: Vec<Value>,
    pub contracts: Vec<Value>,
    pub reviews: Vec<Value>,
    pub remote_operations: Vec<Value>,
    pub pending_cancels: Vec<Value>,
    pub owner_receipts: Vec<Value>,
    pub incomplete: bool,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IntegrationImportDecision {
    pub priority: u8,
    pub disposition: IntegrationImportDisposition,
    pub state: IntegrationAttemptState,
    pub resume_state: Option<IntegrationAttemptState>,
    pub needed_facts: Vec<IntegrationNeededFact>,
    pub reason: String,
    pub candidate_sha: Option<String>,
    pub target_branch: Option<String>,
    pub integrated_sha: Option<String>,
    pub integrated_before_sha: Option<String>,
    pub target_tip_sha: Option<String>,
    pub conflict_paths: Option<Vec<String>>,
    pub guard_paths: Option<Vec<String>>,
}
fn decision(
    priority: u8,
    state: IntegrationAttemptState,
    reason: &str,
) -> IntegrationImportDecision {
    IntegrationImportDecision {
        priority,
        disposition: IntegrationImportDisposition::Classified,
        state,
        resume_state: None,
        needed_facts: Vec::new(),
        reason: reason.into(),
        candidate_sha: None,
        target_branch: None,
        integrated_sha: None,
        integrated_before_sha: None,
        target_tip_sha: None,
        conflict_paths: None,
        guard_paths: None,
    }
}
fn needs_fact(
    mut d: IntegrationImportDecision,
    fact: IntegrationNeededFact,
) -> IntegrationImportDecision {
    d.disposition = IntegrationImportDisposition::NeedsFact;
    d.needed_facts.push(fact);
    d.resume_state = Some(d.state);
    if !matches!(
        d.state,
        IntegrationAttemptState::Reconciling | IntegrationAttemptState::Ejected
    ) {
        d.state = IntegrationAttemptState::Parked;
    }
    d
}
fn quarantine(reason: &str) -> IntegrationImportDecision {
    let mut d = decision(20, IntegrationAttemptState::Quarantined, reason);
    d.disposition = IntegrationImportDisposition::Quarantined;
    d
}
fn json_column(object: &Value, key: &str) -> Result<Option<Value>> {
    match object.get(key).filter(|v| !v.is_null()) {
        None => Ok(None),
        Some(Value::String(s)) => parse_json(s.clone()).map(Some),
        Some(v) => Ok(Some(v.clone())),
    }
}
fn field<'a>(v: &'a Value, name: &str) -> Option<&'a str> {
    v.get(name).and_then(Value::as_str)
}
fn present(v: &Value, name: &str) -> bool {
    v.get(name).is_some_and(|v| !v.is_null())
}
fn paths(v: &Value) -> Result<Vec<String>> {
    validate_integration_paths(v)?;
    serde_json::from_value(v.clone())
        .map_err(|_| DbError::Check("unsupported path encoding".into()))
}

/// Total, conservative priority classifier. Only typed bridges and receipts
/// count as proof; marker-looking prose never grants approval or lineage.
pub fn classify_integration_import(
    snapshot: &IntegrationImportSnapshot,
    now: &str,
) -> IntegrationImportDecision {
    match classify_inner(snapshot, now) {
        Ok(mut d) => {
            if d.target_branch.is_none() && d.priority != 20 {
                let target = snapshot
                    .checkpoints
                    .iter()
                    .rev()
                    .find_map(|c| {
                        json_column(c, "effects_json")
                            .ok()
                            .flatten()
                            .and_then(|e| json_column(&e, "merge_intent").ok().flatten())
                            .and_then(|i| field(&i, "target_branch").map(str::to_owned))
                    })
                    .or_else(|| {
                        json_column(&snapshot.task, "merge_config")
                            .ok()
                            .flatten()
                            .and_then(|c| field(&c, "target_branch").map(str::to_owned))
                    })
                    .or_else(|| {
                        snapshot
                            .workspace
                            .as_ref()
                            .and_then(|w| field(w, "default_branch").map(str::to_owned))
                    });
                if let Some(target) = target {
                    match validate_branch(&target) {
                        Ok(t) => d.target_branch = Some(t),
                        Err(_) => return quarantine("invalid frozen integration target"),
                    }
                }
            }
            d
        }
        Err(error) => quarantine(&error.to_string()),
    }
}
fn classify_inner(s: &IntegrationImportSnapshot, now: &str) -> Result<IntegrationImportDecision> {
    use IntegrationAttemptState as State;
    if s.incomplete {
        return Ok(needs_fact(
            decision(
                20,
                State::Quarantined,
                "legacy evidence exceeds bounded snapshot",
            ),
            IntegrationNeededFact::CompleteLegacyEvidence,
        ));
    }
    let now_time = integration_time(now)?;
    let status =
        field(&s.task, "status").ok_or_else(|| DbError::Check("missing Task status".into()))?;
    let metadata = json_column(&s.task, "metadata")?.unwrap_or_else(|| serde_json::json!({}));
    if !metadata.is_object() {
        return Ok(quarantine("Task metadata is not an object"));
    }
    if let Some(deferred) = metadata.get("deferred_dispatch") {
        let deadline = field(deferred, "not_before")
            .ok_or_else(|| DbError::Check("retry deadline is missing".into()))?;
        integration_time(deadline)?;
    }
    let error = json_column(&s.task, "error_annotation")?;
    let blocked = json_column(&s.task, "blocked_json")?;
    let failed = json_column(&s.task, "failed_json")?;
    let barrier = json_column(&s.task, "entry_barrier_json")?;
    let merge_config = json_column(&s.task, "merge_config")?;
    for step in &s.steps {
        json_column(step, "payload_json")?;
    }
    for carry in &s.carries {
        if let Some(p) = json_column(carry, "changed_paths_json")? {
            paths(&p)?;
        }
    }
    for contract in &s.contracts {
        json_column(contract, "contract_json")?;
    }
    for review in &s.reviews {
        json_column(review, "step_results_json")?;
    }
    let mut merge_intents = Vec::new();
    let mut done_proofs: Vec<Value> = Vec::new();
    let mut rebase_target = None;
    let mut rebase_outcome = None;
    // Read oldest to newest so only the newest phase checkpoint resumes.
    for checkpoint in &s.checkpoints {
        json_column(checkpoint, "result_json")?;
        let effects =
            json_column(checkpoint, "effects_json")?.unwrap_or_else(|| serde_json::json!({}));
        if !effects.is_object() {
            return Ok(quarantine("hook effects are not an object"));
        }
        if let Some(intent) = json_column(&effects, "merge_intent")? {
            merge_intents.push(intent);
        }
        if let Some(outcome) = json_column(&effects, "merge_outcome")? {
            if let Some(done) = outcome.get("Done") {
                done_proofs.push(done.clone());
            } else if !outcome.as_object().is_some_and(|o| {
                o.keys().all(|k| {
                    matches!(
                        k.as_str(),
                        "ReviewRequired"
                            | "TargetMoved"
                            | "Conflict"
                            | "Dirty"
                            | "TargetDirty"
                            | "UnresolvedConflictMarkers"
                    )
                })
            }) {
                return Ok(quarantine("unknown merge outcome"));
            }
        }
        if let Some(target) = json_column(&effects, "rebase_target")? {
            rebase_target = Some(target);
        }
        if let Some(outcome) = json_column(&effects, "rebase_outcome")? {
            rebase_outcome = Some(outcome);
        }
    }
    let mut unresolved_owner_intent = false;
    let mut owner_done_unvalidated = false;
    for receipt in &s.owner_receipts {
        let outcome = json_column(receipt, "outcome_json")?
            .ok_or_else(|| DbError::Check("receipt has no outcome".into()))?;
        match field(receipt, "operation") {
            Some("daemon.workspace.merge.intent") => {
                let operation_id = outcome.pointer("/metadata/operation_id");
                if !s.owner_receipts.iter().any(|r| {
                    field(r, "operation") == Some("daemon.workspace.merge")
                        && json_column(r, "outcome_json")
                            .ok()
                            .flatten()
                            .as_ref()
                            .is_some_and(|v| v.pointer("/metadata/operation_id") == operation_id)
                }) {
                    unresolved_owner_intent = true;
                }
            }
            Some("daemon.workspace.merge")
                if outcome
                    .pointer("/owner_result/outcome/kind")
                    .and_then(Value::as_str)
                    == Some("done") =>
            {
                // Existing retained receipt is proof only with the frozen
                // intent, same operation/location/generation and candidate.
                let intent = s
                    .owner_receipts
                    .iter()
                    .filter(|r| field(r, "operation") == Some("daemon.workspace.merge.intent"))
                    .filter_map(|r| json_column(r, "outcome_json").ok().flatten())
                    .find(|v| {
                        v.pointer("/metadata/operation_id")
                            == outcome.pointer("/metadata/operation_id")
                    });
                if let Some(intent) = intent {
                    if intent.pointer("/metadata/generation")
                        != outcome.pointer("/metadata/generation")
                        || intent.pointer("/metadata/placement_id")
                            != outcome.pointer("/metadata/placement_id")
                        || intent.pointer("/metadata/daemon_id")
                            != outcome.pointer("/metadata/daemon_id")
                    {
                        return Ok(quarantine(
                            "owner proof sources disagree on placement or generation",
                        ));
                    }
                    // Today's redacted intent keeps expected SHA/target in
                    // owner_result; if missing, require the owner fact.
                    let candidate = intent
                        .pointer("/owner_result/request/expected/sha")
                        .and_then(Value::as_str);
                    let after = outcome
                        .pointer("/owner_result/outcome/after_sha")
                        .and_then(Value::as_str);
                    let request = &intent["owner_result"]["request"];
                    let branch = request["target_branch"].as_str();
                    let result_branch = outcome
                        .pointer("/owner_result/outcome/branch")
                        .and_then(Value::as_str);
                    let complete = ["operation_id", "placement_id", "generation", "daemon_id"]
                        .iter()
                        .all(|key| intent["metadata"].get(*key).is_some_and(|v| !v.is_null()));
                    if complete && candidate.is_some() && after.is_some() && branch.is_some() {
                        if branch != result_branch
                            || ["operation_id", "placement_id", "generation", "daemon_id"]
                                .iter()
                                .any(|key| request[*key] != intent["metadata"][*key])
                        {
                            return Ok(quarantine(
                                "owner intent and Done identity or target disagree",
                            ));
                        }
                        if request["reviewed_commit_sha"]
                            .as_str()
                            .is_some_and(|reviewed| Some(reviewed) != after)
                        {
                            return Ok(quarantine("reviewed exact object and owner Done disagree"));
                        }
                        done_proofs.push(serde_json::json!({"after_sha":after,"candidate_sha":candidate,"branch":branch,"generation":intent["metadata"]["generation"],"before_sha":outcome.pointer("/owner_result/outcome/before_sha")}));
                    } else {
                        owner_done_unvalidated = true;
                    }
                } else {
                    owner_done_unvalidated = true;
                }
            }
            _ => {}
        }
    }
    // Multiple certified successful proofs must agree before the priority
    // tree; a later safety marker cannot hide contradictory success evidence.
    if done_proofs.windows(2).any(|p| {
        p[0]["after_sha"] != p[1]["after_sha"]
            || p[0]["branch"] != p[1]["branch"]
            || (!p[0]["candidate_sha"].is_null()
                && !p[1]["candidate_sha"].is_null()
                && p[0]["candidate_sha"] != p[1]["candidate_sha"])
            || (!p[0]["generation"].is_null()
                && !p[1]["generation"].is_null()
                && p[0]["generation"] != p[1]["generation"])
    }) {
        return Ok(quarantine("two Done proof sources disagree"));
    }
    for proof in &done_proofs {
        if field(proof, "after_sha").is_none() || field(proof, "branch").is_none() {
            return Ok(quarantine("Done proof lacks exact candidate or target"));
        }
        if let Some(intent) = merge_intents.last() {
            if field(intent, "target_branch") != field(proof, "branch")
                || (field(proof, "candidate_sha").is_some()
                    && field(proof, "candidate_sha") != field(intent, "candidate_sha"))
            {
                return Ok(quarantine("merge intent and Done proof disagree"));
            }
        }
    }
    if let Some(proof) = done_proofs.last() {
        let mut d = decision(
            1,
            if status == "done" {
                State::Completed
            } else {
                State::Applied
            },
            "persisted exact Done proof",
        );
        d.integrated_sha = field(proof, "after_sha").map(str::to_owned);
        d.candidate_sha = merge_intents
            .last()
            .and_then(|v| field(v, "candidate_sha"))
            .map(str::to_owned)
            .or_else(|| field(proof, "candidate_sha").map(str::to_owned));
        d.target_branch = field(proof, "branch").map(validate_branch).transpose()?;
        d.integrated_before_sha = field(proof, "before_sha").map(str::to_owned);
        if status == "cancelled"
            || present(&s.task, "deleted_at")
            || present(&s.task, "archived_at")
        {
            d = needs_fact(d, IntegrationNeededFact::RemoteOperationResult);
            d.state = State::Reconciling;
        }
        return Ok(d);
    }
    if owner_done_unvalidated {
        return Ok(needs_fact(
            decision(
                1,
                State::Reconciling,
                "retained owner Done needs frozen candidate validation",
            ),
            IntegrationNeededFact::ValidatedOwnerReceipt,
        ));
    }
    if unresolved_owner_intent
        || s.remote_operations
            .iter()
            .any(|v| field(v, "state") == Some("running"))
    {
        return Ok(needs_fact(
            decision(
                2,
                State::Reconciling,
                "unresolved original remote operation",
            ),
            IntegrationNeededFact::RemoteOperationResult,
        ));
    }
    let terminal = matches!(status, "done" | "cancelled")
        || present(&s.task, "deleted_at")
        || present(&s.task, "archived_at");
    if terminal {
        if !merge_intents.is_empty() {
            return Ok(needs_fact(
                decision(
                    1,
                    State::Reconciling,
                    "terminal Task with unproved merge intent",
                ),
                IntegrationNeededFact::CandidateAncestry,
            ));
        }
        let mut d = decision(
            3,
            if status == "done" {
                State::Completed
            } else if status == "cancelled" {
                State::Cancelled
            } else {
                State::Superseded
            },
            "terminal history with no possible legacy effect",
        );
        d.disposition = IntegrationImportDisposition::History;
        return Ok(d);
    }
    if !merge_intents.is_empty() && rebase_target.is_none() && rebase_outcome.is_none() {
        let intent = merge_intents.last().expect("nonempty intents");
        let mut d = needs_fact(
            decision(
                1,
                State::Reconciling,
                "merge intent has no Done proof; candidate ancestry is unknown",
            ),
            IntegrationNeededFact::CandidateAncestry,
        );
        d.target_branch = field(intent, "target_branch")
            .map(validate_branch)
            .transpose()?;
        d.candidate_sha = field(intent, "candidate_sha").map(str::to_owned);
        return Ok(d);
    }
    if s.execution.is_none() || s.workspace.is_none() {
        return Ok(quarantine("missing execution or pinned workspace"));
    }
    if s.workspace
        .as_ref()
        .and_then(|w| field(w, "repo_project_id"))
        .is_some_and(|project| Some(project) != field(&s.task, "project_id"))
    {
        return Ok(quarantine("pinned repository belongs to another Project"));
    }
    if present(&s.task, "parent_task_id") {
        return Ok(quarantine(
            "subtask has no independent integration membership",
        ));
    }
    let target = merge_intents
        .last()
        .and_then(|v| field(v, "target_branch"))
        .or_else(|| {
            merge_config
                .as_ref()
                .and_then(|v| field(v, "target_branch"))
        })
        .or_else(|| {
            s.workspace
                .as_ref()
                .and_then(|v| field(v, "default_branch"))
        });
    let Some(target) = target else {
        return Ok(quarantine("unknown integration target"));
    };
    let target = match validate_branch(target) {
        Ok(t) => t,
        Err(_) => return Ok(quarantine("invalid integration target")),
    };
    for transition in &s.transitions {
        if let Some(kind) = field(transition, "bridge_kind") {
            if kind.parse::<api_types::TransitionBridgeKind>().is_err() {
                return Ok(quarantine("unknown typed transition bridge"));
            }
        }
        if let Some(payload) = json_column(transition, "bridge_payload")? {
            if let Some(p) = payload.get("paths") {
                paths(p)?;
            }
        }
    }
    let boundary = s
        .transitions
        .iter()
        .rposition(|v| {
            matches!(
                field(v, "bridge_kind"),
                Some("retry_window_reset" | "recovery")
            )
        })
        .map_or(0, |i| i + 1);
    let current_transitions = &s.transitions[boundary..];
    let bridge = current_transitions
        .last()
        .and_then(|v| field(v, "bridge_kind"));
    let bridge = if bridge == Some("review_refresh")
        && current_transitions.len() >= 2
        && field(
            &current_transitions[current_transitions.len() - 2],
            "bridge_kind",
        ) == Some("target_moved_rebase")
    {
        Some("target_moved_rebase")
    } else {
        bridge
    };
    if s.steps.iter().any(|v| {
        field(v, "kind") == Some("hooks")
            && matches!(field(v, "status"), Some("pending" | "claimed"))
            && v["expected_epoch"] == s.task["status_epoch"]
            && field(v, "expected_status").is_some_and(|expected| expected != status)
    }) {
        return Ok(quarantine("current hook entry and Task status disagree"));
    }
    let open_hook = s.steps.iter().any(|v| {
        field(v, "kind") == Some("hooks")
            && matches!(field(v, "status"), Some("pending" | "claimed"))
            && v["expected_epoch"] == s.task["status_epoch"]
    });
    let mut d = if rebase_target.is_some() && rebase_outcome.is_none() {
        needs_fact(
            decision(
                5,
                State::Rebasing,
                "rebase target without a durable outcome",
            ),
            IntegrationNeededFact::RebaseInProgress,
        )
    } else if rebase_outcome
        .as_ref()
        .is_some_and(|v| field(v, "kind") == Some("rebased"))
        || bridge == Some("target_moved_rebase")
    {
        needs_fact(
            decision(6, State::Checking, "clean mechanical rebase lineage"),
            IntegrationNeededFact::RebasedCandidateChecks,
        )
    } else if status == "merge_failed"
        && (rebase_outcome
            .as_ref()
            .is_some_and(|v| field(v, "kind") == Some("conflict"))
            || bridge == Some("conflict_handoff"))
    {
        let mut d = decision(7, State::Ejected, "typed conflict repair handoff");
        let mut union = std::collections::BTreeSet::new();
        if let Some(outcome) = &rebase_outcome {
            if let Some(p) = outcome.get("conflict_paths") {
                union.extend(paths(p)?);
            }
        }
        for t in current_transitions {
            if field(t, "bridge_kind") == Some("conflict_handoff") {
                if let Some(payload) = json_column(t, "bridge_payload")? {
                    if let Some(p) = payload.get("paths") {
                        union.extend(paths(p)?);
                    }
                }
            }
        }
        if union.is_empty() {
            return Ok(quarantine("conflict handoff has no typed paths"));
        }
        let p: Vec<_> = union.into_iter().collect();
        d.conflict_paths = Some(p.clone());
        d.guard_paths = Some(p);
        if blocked.is_some()
            || failed.is_some()
            || error.as_ref().is_some_and(|v| {
                matches!(
                    field(v, "type"),
                    Some("manual_stop" | "merge_fix_budget_exhausted")
                )
            })
        {
            d.state = State::Parked;
            d.resume_state = Some(State::Ejected);
        }
        // Stored bridges preserve actual known conflicts. Determining the
        // retry-window union and repair-touched set needs a separate fact.
        needs_fact(d, IntegrationNeededFact::CurrentRetryConflictPaths)
    } else if bridge == Some("review_refresh")
        || (s.task["review_passed_at"].is_null() && !s.contracts.is_empty())
    {
        decision(
            8,
            State::NeedsReview,
            "review refresh without mechanical proof",
        )
    } else if !s.carries.is_empty() {
        needs_fact(
            decision(
                9,
                State::Validating,
                "stored review carry awaits HEAD/base/check verification",
            ),
            IntegrationNeededFact::CarryHeadBaseChecks,
        )
    } else if let Some(paused) = metadata.get("paused_integration") {
        if !paused.is_object() || field(paused, "state").is_none() {
            return Ok(quarantine("malformed paused integration"));
        }
        if field(paused, "state") != Some(status) {
            let mut d = needs_fact(
                decision(
                    12,
                    State::Queued,
                    "obsolete paused-integration marker; current entry retained",
                ),
                IntegrationNeededFact::CandidateHeadReviewAuthority,
            );
            d.disposition = IntegrationImportDisposition::Obsolete;
            d
        } else if present(&s.project, "paused_at") {
            decision(
                10,
                State::Parked,
                "Project paused; exact continuation retained",
            )
        } else {
            needs_fact(
                decision(
                    11,
                    State::Queued,
                    "Project resumed; exact continuation retained",
                ),
                IntegrationNeededFact::CandidateHeadReviewAuthority,
            )
        }
    } else if blocked.is_some()
        || failed.is_some()
        || error
            .as_ref()
            .is_some_and(|v| field(v, "type") == Some("manual_stop"))
        || barrier
            .as_ref()
            .is_some_and(|v| field(v, "status") == Some("blocked"))
        || metadata.get("awaiting_human").and_then(Value::as_bool) == Some(true)
    {
        decision(
            13,
            State::Parked,
            "existing owner/human/entry/failure blocker retained",
        )
    } else if metadata.get("deferred_dispatch").is_some_and(|v| {
        field(v, "not_before")
            .and_then(|t| integration_time(t).ok())
            .is_some_and(|deadline| deadline > now_time)
    }) || error
        .as_ref()
        .is_some_and(|v| field(v, "type") == Some("review_ci_infrastructure"))
    {
        let compatible = metadata
            .get("deferred_dispatch")
            .and_then(|v| field(v, "target_state"))
            .is_none_or(|t| t == status);
        let mut d = decision(
            14,
            State::Parked,
            if compatible {
                "existing infrastructure retry deadline"
            } else {
                "obsolete incompatible retry target"
            },
        );
        if !compatible {
            d.disposition = IntegrationImportDisposition::Obsolete;
        }
        d
    } else if metadata.get("owner_wait").is_some()
        || metadata.get("daemon_upgrade_refusal").is_some()
        || s.placement
            .as_ref()
            .is_some_and(|v| field(v, "state") == Some("disconnected"))
        || !s.pending_cancels.is_empty()
    {
        decision(
            15,
            State::Parked,
            "owner reconnect/upgrade/cancellation exclusion retained",
        )
    } else if s
        .workspace
        .as_ref()
        .is_some_and(|v| field(v, "error") == Some("machine_removed"))
        || s.placement.as_ref().is_some_and(|v| {
            present(v, "removed_at")
                || (field(v, "owner_kind") == Some("daemon") && !present(v, "workspace_handle"))
        })
    {
        needs_fact(
            decision(16, State::Parked, "workspace owner/handle lost"),
            IntegrationNeededFact::CandidateAvailability,
        )
    } else if metadata.get("queued_recovery").is_some()
        || metadata.get("dispatch_disposition").is_some()
    {
        decision(
            17,
            State::Parked,
            "accepted owner command/sticky disposition ordering retained",
        )
    } else if open_hook {
        needs_fact(
            decision(
                4,
                State::Queued,
                "current legacy hook ownership retained; no redirect in shadow stage",
            ),
            IntegrationNeededFact::LegacyHookContinuation,
        )
    } else if status == "merging" && present(&s.task, "review_passed_at") {
        needs_fact(
            decision(
                18,
                State::Queued,
                "missing-hooks merge entry with pinned delivery",
            ),
            IntegrationNeededFact::CandidateHeadReviewAuthority,
        )
    } else if status == "merge_failed"
        && error.as_ref().is_some_and(|v| {
            matches!(
                field(v, "type"),
                Some("dirty_worktree" | "workspace_error" | "merge_conflict" | "target_repo_dirty")
            )
        })
    {
        decision(
            19,
            if error
                .as_ref()
                .is_some_and(|v| field(v, "type") == Some("dirty_worktree"))
            {
                State::Ejected
            } else {
                State::Parked
            },
            "ordinary repair without invented mechanical proof",
        )
    } else if !merge_intents.is_empty() {
        needs_fact(
            decision(
                1,
                State::Reconciling,
                "merge intent has no Done proof; ancestry is unknown",
            ),
            IntegrationNeededFact::CandidateAncestry,
        )
    } else {
        quarantine("unrecognized or contradictory legacy integration combination")
    };
    if open_hook && d.priority > 4 && d.priority < 20 {
        d.priority = 4;
        d = needs_fact(d, IntegrationNeededFact::LegacyHookContinuation);
    }
    d.target_branch = Some(target);
    d.candidate_sha = merge_intents
        .last()
        .and_then(|v| field(v, "candidate_sha"))
        .map(str::to_owned);
    if let Some(outcome) = &rebase_outcome {
        if !matches!(
            field(outcome, "kind"),
            Some("rebased" | "conflict" | "dirty" | "unsupported_conflict")
        ) {
            return Ok(quarantine("unknown rebase outcome"));
        }
    }
    Ok(d)
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IntegrationImportPass {
    pub examined: u32,
    pub imported: u32,
    pub quarantined: u32,
    pub needs_fact: u32,
    pub remaining: bool,
}
/// Progress is the unique `import:<Task>:<entry epoch>` admission identity.
/// Selection excludes committed rows. A crash rolls back a whole bounded
/// slice, and the next pass starts at its first uncommitted entry. No third
/// cursor table, Task mutation, step enqueue, condition statement or Git I/O.
impl SqliteDb {
    pub async fn import_integration_pass(&self, limit: u32) -> Result<IntegrationImportPass> {
        let mut tx = begin_immediate(self.pool()).await?;
        let limit = limit.clamp(1, INTEGRATION_IMPORT_PAGE);
        let mut tasks=sqlx::query_scalar::<_,String>("SELECT t.id FROM task t WHERE (t.status IN ('merging','merge_failed') OR (t.status IN ('done','cancelled','review') AND (EXISTS(SELECT 1 FROM transition_log l WHERE l.task_id=t.id AND (l.from_state IN ('merging','merge_failed') OR l.to_state IN ('merging','merge_failed'))) OR t.metadata_json LIKE '%paused_integration%' OR EXISTS(SELECT 1 FROM task_hook_checkpoint h JOIN task_step s ON s.id=h.step_id WHERE s.task_id=t.id AND h.effects_json LIKE '%merge_intent%')))) AND NOT EXISTS(SELECT 1 FROM integration_attempt a WHERE a.admission_key='import:'||t.id||':'||t.status_epoch) ORDER BY COALESCE((SELECT l.created_at FROM transition_log l WHERE l.task_id=t.id AND l.to_state=t.status ORDER BY l.created_at DESC,l.id DESC LIMIT 1),t.updated_at),COALESCE((SELECT l.id FROM transition_log l WHERE l.task_id=t.id AND l.to_state=t.status ORDER BY l.created_at DESC,l.id DESC LIMIT 1),''),t.id LIMIT ?")
            .bind(limit+1).fetch_all(&mut *tx).await?;
        let mut pass = IntegrationImportPass {
            remaining: tasks.len() > limit as usize,
            ..Default::default()
        };
        tasks.truncate(limit as usize);
        let now = now_rfc3339();
        for task_id in tasks {
            pass.examined += 1;
            let snapshot = load_snapshot(&mut tx, &task_id).await?;
            let mut d = classify_integration_import(&snapshot, &now);
            let epoch = snapshot.task["status_epoch"]
                .as_i64()
                .ok_or_else(|| DbError::Check("Task epoch missing".into()))?;
            let queue = match (
                snapshot
                    .workspace
                    .as_ref()
                    .and_then(|w| field(w, "repo_id")),
                d.target_branch.as_deref(),
            ) {
                (Some(repo), Some(branch)) => {
                    let exists: bool =
                        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM repo WHERE id=?)")
                            .bind(repo)
                            .fetch_one(&mut *tx)
                            .await?;
                    let same_project = snapshot
                        .workspace
                        .as_ref()
                        .and_then(|w| field(w, "repo_project_id"))
                        .is_none_or(|project| Some(project) == field(&snapshot.task, "project_id"));
                    if exists && same_project && !present(&snapshot.task, "parent_task_id") {
                        Some(create_queue_in_tx(&mut tx, repo, branch).await?)
                    } else {
                        None
                    }
                }
                _ => None,
            };
            if queue.is_none() && !d.state.terminal() && d.priority != 20 {
                let facts = d.needed_facts.clone();
                d = quarantine("repository/target unavailable; orphan evidence retained");
                d.needed_facts = facts;
            }
            let mut a = IntegrationAttempt::new(
                queue.as_ref().map(|q| q.id.clone()),
                task_id.clone(),
                field(&snapshot.task, "project_id")
                    .unwrap_or_default()
                    .to_owned(),
                format!("import:{task_id}:{epoch}"),
                field(&snapshot.task, "status")
                    .unwrap_or_default()
                    .to_owned(),
                epoch,
                snapshot.task["version"].as_i64().unwrap_or(0),
            );
            let existing: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM integration_attempt WHERE task_ref=? AND current=1)",
            )
            .bind(&task_id)
            .fetch_one(&mut *tx)
            .await?;
            a.current = queue.is_some() && !d.state.terminal() && !existing;
            if existing {
                d.needed_facts
                    .push(IntegrationNeededFact::ExistingAttemptIdentity);
            }
            a.state = d.state;
            a.resume_state = d.resume_state;
            a.candidate_sha = d.candidate_sha.clone();
            a.original_candidate_sha = d.candidate_sha.clone();
            a.integrated_sha = d.integrated_sha.clone();
            a.integrated_before_sha = d.integrated_before_sha.clone();
            a.target_tip_sha = d.target_tip_sha.clone();
            a.conflict_paths_json = d.conflict_paths.as_ref().map(|p| serde_json::json!(p));
            a.guard_paths_json = d.guard_paths.as_ref().map(|p| serde_json::json!(p));
            a.failure_kind = match d.disposition {
                IntegrationImportDisposition::Quarantined => {
                    Some(IntegrationFailureKind::CorruptImport)
                }
                IntegrationImportDisposition::NeedsFact => Some(IntegrationFailureKind::NeedsFact),
                _ => None,
            };
            a.failure_message = Some(d.reason.clone());
            if a.state.terminal() {
                a.completed_at = Some(now.clone());
            }
            if let Some(execution) = &snapshot.execution {
                a.execution_id = field(execution, "id").map(str::to_owned);
                a.execution_ref = a.execution_id.clone();
            }
            if let Some(workspace) = &snapshot.workspace {
                a.workspace_id = field(workspace, "id").map(str::to_owned);
                a.workspace_ref = a.workspace_id.clone();
            }
            if let Some(placement) = &snapshot.placement {
                a.placement_id = field(placement, "id").map(str::to_owned);
                a.placement_ref = a.placement_id.clone();
                a.repo_location_id = field(placement, "repo_location_id").map(str::to_owned);
                a.repo_location_ref = a.repo_location_id.clone();
                a.owner_kind = field(placement, "owner_kind").map(str::parse).transpose()?;
                a.daemon_id = field(placement, "daemon_id").map(str::to_owned);
                a.runtime_id = field(placement, "runtime_id").map(str::to_owned);
                a.placement_generation = placement["generation"].as_i64();
            }
            if let Some(operation) = snapshot
                .remote_operations
                .iter()
                .find(|o| field(o, "state") == Some("running"))
            {
                a.operation_id = field(operation, "operation_id").map(str::to_owned);
                a.current_operation_state = Some(IntegrationOperationState::Uncertain);
            }
            if a.operation_id.is_none() && a.state == IntegrationAttemptState::Reconciling {
                if let Some(intent) = snapshot
                    .owner_receipts
                    .iter()
                    .filter(|receipt| {
                        field(receipt, "operation") == Some("daemon.workspace.merge.intent")
                    })
                    .find_map(|receipt| json_column(receipt, "outcome_json").ok().flatten())
                {
                    a.operation_id = intent
                        .pointer("/metadata/operation_id")
                        .and_then(Value::as_str)
                        .map(str::to_owned);
                    a.operation_kind = Some(IntegrationOperationKind::Merge);
                    a.current_operation_state = Some(IntegrationOperationState::Uncertain);
                    a.daemon_id = intent
                        .pointer("/metadata/daemon_id")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                        .or(a.daemon_id);
                    a.placement_generation = intent
                        .pointer("/metadata/generation")
                        .and_then(Value::as_i64)
                        .or(a.placement_generation);
                }
            }
            if let Some(deadline) = json_column(&snapshot.task, "metadata")
                .ok()
                .flatten()
                .and_then(|m| m.get("deferred_dispatch").cloned())
                .and_then(|v| field(&v, "not_before").map(str::to_owned))
            {
                a.available_at = Some(deadline);
            }
            // The classified disposition and every evidence column used are
            // frozen together, so committed rows are the durable progress.
            if a.state == IntegrationAttemptState::Reconciling && a.current {
                if let Some(q) = &queue {
                    if q.head_attempt_id.is_some() {
                        d.disposition = IntegrationImportDisposition::Quarantined;
                        d.reason = "another uncertain operation already pins this queue".into();
                        a.state = IntegrationAttemptState::Quarantined;
                        a.current = false;
                        a.failure_kind = Some(IntegrationFailureKind::ContradictoryProof);
                        a.failure_message = Some(d.reason.clone());
                    }
                }
            }
            a.import_source_json = Some(
                serde_json::json!({"version":1,"priority":d.priority,"disposition":d.disposition,"needed_facts":d.needed_facts,"decision":d,"evidence":snapshot}),
            );
            let a = admit_in_tx(&mut tx, a).await?;
            if a.state == IntegrationAttemptState::Reconciling && a.current {
                sqlx::query("UPDATE integration_queue SET state='quarantined',head_attempt_id=?,revision=revision+1,updated_at=? WHERE id=? AND head_attempt_id IS NULL")
                    .bind(&a.id).bind(&now).bind(&a.queue_id).execute(&mut *tx).await?;
            }
            pass.imported += 1;
            if d.disposition == IntegrationImportDisposition::Quarantined {
                pass.quarantined += 1;
            }
            if d.disposition == IntegrationImportDisposition::NeedsFact {
                pass.needs_fact += 1;
            }
        }
        tx.commit().await?;
        Ok(pass)
    }
}

async fn json_rows(
    tx: &mut Transaction<'_, Sqlite>,
    query: &str,
    task_id: &str,
) -> Result<Vec<Value>> {
    // Bound each retained JSON/text source in SQL, before materialization.
    // Oversized evidence carries its original byte length and a marked
    // prefix; it is never interpreted as a complete frozen input.
    let mut bounded = query.to_owned();
    let mut clipped = Vec::new();
    for column in [
        "metadata_json",
        "error_annotation",
        "blocked_json",
        "failed_json",
        "entry_barrier_json",
        "merge_config",
        "condition_json",
        "p.workflow_definition",
        "payload_json",
        "h.effects_json",
        "h.result_json",
        "bridge_payload",
        "changed_paths_json",
        "contract_json",
        "step_results_json",
        "outcome_json",
    ] {
        let pattern = format!(",{column},");
        if bounded.contains(&pattern) {
            bounded = bounded.replace(
                &pattern,
                &format!(",substr({column},1,{EVIDENCE_TEXT_LIMIT}),"),
            );
            clipped.push(column);
        }
    }
    if !clipped.is_empty() {
        let condition = clipped
            .iter()
            .map(|column| format!("length(CAST({column} AS BLOB))>{EVIDENCE_TEXT_LIMIT}"))
            .collect::<Vec<_>>()
            .join(" OR ");
        let lengths = clipped
            .iter()
            .map(|column| {
                format!(
                    "'{}',length(CAST({column} AS BLOB))",
                    column.replace('.', "_")
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        bounded=bounded.replacen(") FROM",&format!(",'__text_truncated',CASE WHEN {condition} THEN 1 ELSE 0 END,'__source_bytes',json_object({lengths})) FROM"),1);
    }
    sqlx::query_scalar::<_, String>(&bounded)
        .bind(task_id)
        .fetch_all(&mut **tx)
        .await?
        .into_iter()
        .map(parse_json)
        .collect()
}
async fn load_snapshot(
    tx: &mut Transaction<'_, Sqlite>,
    task_id: &str,
) -> Result<IntegrationImportSnapshot> {
    let task=json_rows(tx,"SELECT json_object('id',id,'project_id',project_id,'parent_task_id',parent_task_id,'status',status,'status_epoch',status_epoch,'version',version,'review_passed_at',review_passed_at,'metadata',metadata_json,'error_annotation',error_annotation,'blocked_json',blocked_json,'failed_json',failed_json,'entry_barrier_json',entry_barrier_json,'merge_config',merge_config,'condition_json',condition_json,'archived_at',archived_at,'deleted_at',deleted_at,'updated_at',updated_at) FROM task WHERE id=?",task_id).await?.pop().ok_or(DbError::NotFound)?;
    let project=json_rows(tx,"SELECT json_object('id',p.id,'primary_repo_id',p.primary_repo_id,'paused_at',p.paused_at,'workflow_definition',p.workflow_definition,'version',p.version) FROM project p JOIN task t ON t.project_id=p.id WHERE t.id=?",task_id).await?.pop().unwrap_or(Value::Null);
    let execution=sqlx::query_scalar::<_,String>("SELECT json_object('id',e.id,'workspace_id',e.workspace_id,'role',e.role,'status',e.status,'before_sha',e.before_sha,'after_sha',e.after_sha,'created_at',e.created_at) FROM execution e WHERE e.task_id IN (SELECT child.id FROM task child WHERE child.parent_task_id=? AND child.deleted_at IS NULL UNION ALL SELECT root.id FROM task root WHERE root.id=? AND NOT EXISTS(SELECT 1 FROM task child WHERE child.parent_task_id=root.id AND child.deleted_at IS NULL)) AND e.status IN ('completed','running') AND e.role IN ('executor','coder','worker') ORDER BY e.created_at DESC,e.id DESC LIMIT 1").bind(task_id).bind(task_id).fetch_optional(&mut **tx).await?.map(parse_json).transpose()?;
    let workspace = if let Some(id) = execution.as_ref().and_then(|e| field(e, "workspace_id")) {
        json_rows(tx,"SELECT json_object('id',w.id,'repo_id',w.repo_id,'branch',w.branch,'before_sha',w.before_sha,'status',w.status,'error',w.error,'default_branch',r.default_branch,'repo_project_id',r.project_id) FROM workspace w LEFT JOIN repo r ON r.id=w.repo_id WHERE w.id=?",id).await?.pop()
    } else {
        None
    };
    let placement = if let Some(id) = workspace.as_ref().and_then(|w| field(w, "id")) {
        json_rows(tx,"SELECT json_object('id',p.id,'repo_location_id',p.repo_location_id,'owner_kind',p.owner_kind,'daemon_id',p.daemon_id,'runtime_id',p.runtime_id,'generation',p.generation,'state',p.state,'workspace_handle',p.workspace_handle,'removed_at',d.removed_at) FROM workspace_placement p LEFT JOIN daemon d ON d.id=p.daemon_id WHERE p.workspace_id=?",id).await?.pop()
    } else {
        None
    };
    let cap = EVIDENCE_ROWS + 1;
    let steps=json_rows(tx,&format!("SELECT json_object('id',id,'kind',kind,'status',status,'expected_epoch',expected_epoch,'expected_status',expected_status,'payload_json',payload_json,'workflow_ref_id',workflow_ref_id,'created_at',created_at) FROM task_step WHERE task_id=? ORDER BY seq DESC LIMIT {cap}"),task_id).await?;
    let mut checkpoints=json_rows(tx,&format!("SELECT json_object('step_id',h.step_id,'hook_index',h.hook_index,'effects_json',h.effects_json,'result_json',h.result_json,'expected_epoch',s.expected_epoch,'started_at',h.started_at) FROM task_hook_checkpoint h JOIN task_step s ON s.id=h.step_id WHERE s.task_id=? ORDER BY s.seq DESC,h.hook_index DESC LIMIT {cap}"),task_id).await?;
    checkpoints.reverse();
    let mut transitions=json_rows(tx,&format!("SELECT json_object('id',id,'from_state',from_state,'to_state',to_state,'bridge_kind',bridge_kind,'bridge_payload',bridge_payload,'created_at',created_at) FROM transition_log WHERE task_id=? ORDER BY created_at DESC,id DESC LIMIT {cap}"),task_id).await?;
    transitions.reverse();
    let carries=json_rows(tx,&format!("SELECT json_object('id',id,'contract_execution_id',contract_execution_id,'commit_sha',commit_sha,'base_sha',base_sha,'kind',kind,'changed_paths_json',changed_paths_json,'created_at',created_at) FROM review_authority_carry WHERE task_id=? ORDER BY created_at DESC,id DESC LIMIT {cap}"),task_id).await?;
    let contracts=json_rows(tx,&format!("SELECT json_object('execution_id',execution_id,'contract_digest',contract_digest,'source_digest',source_digest,'contract_json',contract_json,'created_at',created_at) FROM execution_review_contract WHERE task_id=? ORDER BY created_at DESC LIMIT {cap}"),task_id).await?;
    let reviews=json_rows(tx,&format!("SELECT json_object('id',id,'execution_id',execution_id,'attempt_number',attempt_number,'status',status,'step_results_json',step_results_json,'created_at',created_at) FROM review WHERE task_id=? ORDER BY attempt_number DESC,id DESC LIMIT {cap}"),task_id).await?;
    let remote_operations=json_rows(tx,&format!("SELECT json_object('operation_id',r.operation_id,'step_id',r.step_id,'workspace_id',r.workspace_id,'placement_id',r.placement_id,'daemon_id',r.daemon_id,'runtime_id',r.runtime_id,'generation',r.generation,'expected_epoch',r.expected_epoch,'state',r.state) FROM task_remote_operation r JOIN task_step s ON s.id=r.step_id WHERE s.task_id=? ORDER BY r.created_at DESC LIMIT {cap}"),task_id).await?;
    let pending_cancels=json_rows(tx,&format!("SELECT json_object('operation_id',r.operation_id,'step_id',r.step_id,'workspace_id',r.workspace_id,'placement_id',r.placement_id,'daemon_id',r.daemon_id,'runtime_id',r.runtime_id,'generation',r.generation,'expected_epoch',r.expected_epoch) FROM pending_remote_cancel r WHERE r.step_id IN(SELECT id FROM task_step WHERE task_id=?) ORDER BY r.created_at DESC LIMIT {cap}"),task_id).await?;
    let owner_receipts=json_rows(tx,&format!("SELECT json_object('id',id,'operation',operation,'outcome_json',outcome_json,'committed_at',committed_at) FROM command_receipt WHERE scope_type='task' AND scope_id=? AND operation IN ('daemon.workspace.merge.intent','daemon.workspace.merge') ORDER BY committed_at DESC,id DESC LIMIT {cap}"),task_id).await?;
    let incomplete = [
        &steps,
        &checkpoints,
        &transitions,
        &carries,
        &contracts,
        &reviews,
        &remote_operations,
        &pending_cancels,
        &owner_receipts,
    ]
    .iter()
    .any(|v| v.len() > EVIDENCE_ROWS as usize);
    let text_truncated = |value: &Value| value["__text_truncated"].as_i64() == Some(1);
    let incomplete = incomplete
        || text_truncated(&task)
        || text_truncated(&project)
        || [&execution, &workspace, &placement]
            .into_iter()
            .flatten()
            .any(text_truncated)
        || [
            &steps,
            &checkpoints,
            &transitions,
            &carries,
            &contracts,
            &reviews,
            &remote_operations,
            &pending_cancels,
            &owner_receipts,
        ]
        .into_iter()
        .any(|rows| rows.iter().any(text_truncated));
    Ok(IntegrationImportSnapshot {
        task,
        project,
        execution,
        workspace,
        placement,
        steps,
        checkpoints,
        transitions,
        carries,
        contracts,
        reviews,
        remote_operations,
        pending_cancels,
        owner_receipts,
        incomplete,
    })
}
