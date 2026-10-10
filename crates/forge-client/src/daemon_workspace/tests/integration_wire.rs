//! Daemon protocol 6: fence announcement, journal retention for queue
//! attempts and Git object transfer.
use super::protocol::{attempt_merge_params, attempt_request, retain_intent};
use super::*;
use base64::Engine as _;

async fn call<T: serde::de::DeserializeOwned>(
    backend: &DaemonWorkspaceBackend,
    method: &str,
    params: impl Serialize,
) -> CommandResult<T> {
    backend
        .handle(method, serde_json::to_value(params).unwrap(), Vec::new)
        .await
        .and_then(decode)
}

fn restart(fixture: &Fixture) -> DaemonWorkspaceBackend {
    DaemonWorkspaceBackend::new(
        fixture.dir.path().to_owned(),
        "daemon-1".into(),
        fixture.backend.policy.clone(),
        fixture.journal.clone(),
    )
    .unwrap()
}

async fn lookup(
    backend: &DaemonWorkspaceBackend,
    fixture: &Fixture,
    request: &WorkspaceIntegrationRequest,
) -> WorkspaceReconcileResult {
    call(
        backend,
        METHOD_WORKSPACE_DESCRIBE,
        WorkspaceReconcileParams {
            integration: WorkspaceIntegrationBinding::Attempt {
                request: request.clone(),
            },
            workspace: fixture.reference(),
            operation: WorkspaceReconcileOperation::Reconcile,
            operation_id: request.operation_id(),
        },
    )
    .await
    .unwrap()
}

fn receipt(lookup: &WorkspaceReconcileResult) -> Value {
    match &lookup.outcome {
        WorkspaceReconcileOutcome::Result { result } => result["integration_receipt"].clone(),
        WorkspaceReconcileOutcome::Error { error } => {
            error.details.as_ref().unwrap()["integration_receipt"].clone()
        }
    }
}

async fn announce(
    backend: &DaemonWorkspaceBackend,
    fence: &IntegrationOwnerFence,
    live_queue_ids: Option<Vec<String>>,
) -> CommandResult<IntegrationAnnounceResult> {
    call(
        backend,
        METHOD_INTEGRATION_ANNOUNCE,
        IntegrationAnnounceParams {
            daemon_id: "daemon-1".into(),
            runtime_id: "runtime-1".into(),
            fence: fence.clone(),
            live_queue_ids,
        },
    )
    .await
}

#[tokio::test]
async fn lookup_announces_the_fence_and_tells_not_performed_from_unknown() {
    // An owner that was told of the claim generation: an absent intent was
    // never performed, and stays fenced off.
    let fixture = Fixture::new().await;
    let head = git::get_current_sha(fixture.path()).await.unwrap();
    let request = attempt_request(
        &fixture,
        &head,
        &head,
        3,
        WorkspaceIntegrationKind::FastForward,
    );
    let announced = announce(&fixture.backend, &request.fence, None)
        .await
        .unwrap();
    assert_eq!(announced.previous, None);
    assert_eq!(announced.generation, 3);
    let found = lookup(&fixture.backend, &fixture, &request).await;
    assert_eq!(
        found.owner_fence,
        Some(IntegrationFenceAnnouncement {
            queue_id: "queue-1".into(),
            generation: Some(3),
            attempt_id: Some("attempt-1".into()),
            intent: IntegrationIntentRecord::NotPerformed,
        })
    );
    assert_eq!(receipt(&found)["result"]["kind"], "not_performed");
    assert_eq!(receipt(&found)["operation_state"], "failed");
    // An older claim cannot announce or act any more.
    let mut stale = request.clone();
    stale.fence.generation = 2;
    let refused = announce(&fixture.backend, &stale.fence, None)
        .await
        .unwrap_err();
    assert_eq!(refused.details.unwrap()["refusal"], "stale_fence");
    let refused = fixture
        .backend
        .handle(
            METHOD_WORKSPACE_MERGE,
            serde_json::to_value(attempt_merge_params(&fixture, stale)).unwrap(),
            Vec::new,
        )
        .await
        .unwrap_err();
    assert_eq!(refused.details.unwrap()["refusal"], "stale_fence");

    // An owner hearing of the generation for the first time (lost or replaced
    // state) cannot vouch for an empty journal: the result is unknown.
    let fixture = Fixture::new().await;
    let request = attempt_request(
        &fixture,
        &head,
        &head,
        3,
        WorkspaceIntegrationKind::FastForward,
    );
    let found = lookup(&fixture.backend, &fixture, &request).await;
    assert_eq!(
        found.owner_fence,
        Some(IntegrationFenceAnnouncement {
            queue_id: "queue-1".into(),
            generation: None,
            attempt_id: None,
            intent: IntegrationIntentRecord::Unknown,
        })
    );
    let unknown = receipt(&found);
    assert_eq!(unknown["operation_state"], "uncertain");
    assert_eq!(unknown["result"]["kind"], "infrastructure");
    assert!(unknown["result"]["message"]
        .as_str()
        .unwrap()
        .contains("unknown"));
    // Asking again (also after a restart) never turns it into "not performed".
    let again = lookup(&restart(&fixture), &fixture, &request).await;
    assert_eq!(receipt(&again), unknown);
    assert_eq!(
        again.owner_fence.unwrap().intent,
        IntegrationIntentRecord::Retained
    );
    // The delayed frame of that operation cannot start now.
    let target = git::get_current_sha(&fixture.repo).await.unwrap();
    let late = fixture
        .backend
        .handle(
            METHOD_WORKSPACE_MERGE,
            serde_json::to_value(attempt_merge_params(&fixture, request)).unwrap(),
            Vec::new,
        )
        .await
        .unwrap_err();
    assert_eq!(late.message, "workspace operation was cancelled");
    assert_eq!(git::get_current_sha(&fixture.repo).await.unwrap(), target);
}

#[tokio::test]
async fn journal_ack_keeps_the_receipt_until_a_newer_fence_prunes_it() {
    let fixture = Fixture::new().await;
    std::fs::write(fixture.path().join("candidate"), "candidate\n").unwrap();
    let head = git::commit_all(fixture.path(), "candidate").await.unwrap();
    let target = git::get_current_sha(&fixture.repo).await.unwrap();
    let request = attempt_request(
        &fixture,
        &head,
        &target,
        1,
        WorkspaceIntegrationKind::FastForward,
    );
    let operation_id = request.operation_id();
    let params = serde_json::to_value(attempt_merge_params(&fixture, request.clone())).unwrap();
    let first = fixture
        .backend
        .handle(METHOD_WORKSPACE_MERGE, params.clone(), Vec::new)
        .await
        .unwrap();
    assert_eq!(
        fixture.backend.state.lock().unwrap().integration_pending["location-1"],
        operation_id
    );
    let ack = fixture
        .backend
        .acknowledge_journal(&JournalAckParams {
            entry_id: first["entry_id"].as_str().unwrap().into(),
        })
        .await
        .unwrap();
    assert!(ack.acknowledged);
    // Acknowledged: no longer replayed to the server, the pending marker is
    // gone, and a duplicate of the same key still gets the stored receipt,
    // also after a restart.
    assert!(fixture
        .journal
        .pending()
        .unwrap()
        .iter()
        .all(|entry| entry.entry_id() != first["entry_id"].as_str().unwrap()));
    assert!(fixture
        .backend
        .state
        .lock()
        .unwrap()
        .integration_pending
        .is_empty());
    fixture.journal.initialize().unwrap();
    let restarted = restart(&fixture);
    assert!(restarted
        .state
        .lock()
        .unwrap()
        .integration_pending
        .is_empty());
    let duplicate = restarted
        .handle(METHOD_WORKSPACE_MERGE, params.clone(), Vec::new)
        .await
        .unwrap();
    assert_eq!(duplicate, first);
    // A repeated acknowledgement is harmless.
    restarted
        .acknowledge_journal(&JournalAckParams {
            entry_id: first["entry_id"].as_str().unwrap().into(),
        })
        .await
        .unwrap();
    assert!(fixture.journal.operation(&operation_id).unwrap().is_some());
    // The next claim generation makes the old key unreachable: pruned.
    let mut next = request.fence.clone();
    next.generation = 2;
    let announced = announce(&restarted, &next, None).await.unwrap();
    assert_eq!(announced.previous, Some(request.fence.clone()));
    assert_eq!(announced.pruned_entries, 1);
    assert!(fixture.journal.operation(&operation_id).unwrap().is_none());
    let refused = restarted
        .handle(METHOD_WORKSPACE_MERGE, params, Vec::new)
        .await
        .unwrap_err();
    assert_eq!(refused.details.unwrap()["refusal"], "stale_fence");
    // An unacknowledged receipt is never pruned.
    let unacknowledged = attempt_request(
        &fixture,
        &head,
        &head,
        2,
        WorkspaceIntegrationKind::FastForward,
    );
    let mut intent = attempt_merge_params(&fixture, unacknowledged.clone());
    intent.merge.fence.operation_id = unacknowledged.operation_id();
    retain_intent(
        &fixture,
        METHOD_WORKSPACE_MERGE,
        serde_json::to_value(intent).unwrap(),
    );
    next.generation = 3;
    assert_eq!(
        announce(&restarted, &next, None)
            .await
            .unwrap()
            .pruned_entries,
        0
    );
    assert!(fixture
        .journal
        .operation(&unacknowledged.operation_id())
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn fences_are_pruned_by_the_live_queue_set_and_a_fixed_bound() {
    let fixture = Fixture::new().await;
    let head = git::get_current_sha(fixture.path()).await.unwrap();
    let request = attempt_request(
        &fixture,
        &head,
        &head,
        1,
        WorkspaceIntegrationKind::FastForward,
    );
    announce(&fixture.backend, &request.fence, None)
        .await
        .unwrap();
    let mut other = request.fence.clone();
    other.queue_id = "queue-2".into();
    let result = announce(
        &fixture.backend,
        &other,
        Some(vec!["queue-2".into(), "queue-9".into()]),
    )
    .await
    .unwrap();
    assert_eq!(result.pruned_fences, 1);
    assert_eq!(
        fixture
            .backend
            .state
            .lock()
            .unwrap()
            .integration_fences
            .keys()
            .collect::<Vec<_>>(),
        vec!["queue-2"]
    );
    {
        let mut state = fixture.backend.state.lock().unwrap();
        for index in 0..integration_owner::MAX_INTEGRATION_FENCES + 5 {
            let mut fence = request.fence.clone();
            fence.queue_id = format!("bulk-{index:05}");
            state
                .advance_integration_fence(&fence, 1_000 + index as u64)
                .unwrap();
        }
    }
    let mut newest = request.fence.clone();
    newest.queue_id = "queue-3".into();
    let result = announce(&fixture.backend, &newest, None).await.unwrap();
    assert_eq!(result.pruned_fences, 7);
    let restarted = restart(&fixture);
    let state = restarted.state.lock().unwrap();
    assert_eq!(
        state.integration_fences.len(),
        integration_owner::MAX_INTEGRATION_FENCES
    );
    assert_eq!(
        state.integration_fence_seen.len(),
        state.integration_fences.len()
    );
    // The least recently recorded went first; the newest stayed.
    assert!(!state.integration_fences.contains_key("bulk-00000"));
    assert!(!state.integration_fences.contains_key("bulk-00006"));
    assert!(state.integration_fences.contains_key("bulk-00007"));
    assert!(state.integration_fences.contains_key("queue-2"));
    assert!(state.integration_fences.contains_key("queue-3"));
}

#[tokio::test]
async fn a_revision_five_journal_with_an_in_flight_attempt_upgrades_without_loss() {
    let fixture = Fixture::new().await;
    let head = git::get_current_sha(fixture.path()).await.unwrap();
    let request = attempt_request(
        &fixture,
        &head,
        &head,
        4,
        WorkspaceIntegrationKind::FastForward,
    );
    // Revision 5 state: a fence and a pending marker, no recorded-at map,
    // and an admitted attempt effect without an outcome.
    let mut params = attempt_merge_params(&fixture, request.clone());
    params.merge.fence.operation_id = request.operation_id();
    retain_intent(
        &fixture,
        METHOD_WORKSPACE_MERGE,
        serde_json::to_value(&params).unwrap(),
    );
    let state_path = fixture.journal.directory().join("workspace-state.json");
    let mut state: Value = serde_json::from_slice(&std::fs::read(&state_path).unwrap()).unwrap();
    state["integration_fences"] = serde_json::json!({"queue-1": request.fence});
    state["integration_pending"] = serde_json::json!({"location-1": request.operation_id()});
    state
        .as_object_mut()
        .unwrap()
        .remove("integration_fence_seen")
        .unwrap();
    std::fs::write(&state_path, serde_json::to_vec(&state).unwrap()).unwrap();

    fixture.journal.initialize().unwrap();
    let upgraded = restart(&fixture);
    {
        let state = upgraded.state.lock().unwrap();
        assert_eq!(state.integration_fences["queue-1"], request.fence);
        assert_eq!(
            state.integration_pending["location-1"],
            request.operation_id()
        );
        assert!(state
            .handles
            .contains_key(&fixture.prepared.workspace.workspace_handle));
    }
    // The in-flight intent still blocks another effect on its checkout...
    let mut other = attempt_request(&fixture, &head, &head, 4, WorkspaceIntegrationKind::Merge);
    other.fence = request.fence.clone();
    let mut blocked = attempt_merge_params(&fixture, other);
    blocked.reviewed_commit_sha = None;
    let refused = upgraded
        .handle(
            METHOD_WORKSPACE_MERGE,
            serde_json::to_value(blocked).unwrap(),
            Vec::new,
        )
        .await
        .unwrap_err();
    assert_eq!(
        refused.details.unwrap()["refusal"],
        "reconciliation_required"
    );
    // ...pruning leaves it alone, and its lookup is answered from the journal.
    let mut next = request.fence.clone();
    next.generation = 5;
    assert_eq!(
        announce(&upgraded, &next, None)
            .await
            .unwrap()
            .pruned_entries,
        0
    );
    let found = lookup(&upgraded, &fixture, &request).await;
    let announcement = found.owner_fence.clone().unwrap();
    assert_eq!(announcement.intent, IntegrationIntentRecord::Retained);
    assert_eq!(announcement.generation, Some(5));
    // HEAD already is the frozen candidate: the merge is proven, not rerun.
    assert_eq!(receipt(&found)["operation_state"], "succeeded");
}

struct Transfer {
    fixture: Fixture,
    fence: IntegrationOwnerFence,
    base: String,
    tip: String,
}

/// `location-1` is one commit ahead of a second clone, `location-2`.
async fn transfer() -> Transfer {
    let fixture = Fixture::new().await;
    let base = git::get_current_sha(&fixture.repo).await.unwrap();
    let clone = fixture.dir.path().join("secondary");
    local_git(
        fixture.dir.path(),
        &["clone", "-q", fixture.repo.to_str().unwrap(), "secondary"],
    )
    .await
    .unwrap();
    local_git(&clone, &["remote", "remove", "origin"])
        .await
        .unwrap();
    fixture
        .backend
        .handle(
            METHOD_REPO_LOCATION_VERIFY,
            serde_json::to_value(RepoLocationVerifyParams {
                repo_location_id: "location-2".into(),
                daemon_id: "daemon-1".into(),
                runtime_id: "runtime-1".into(),
                path: clone.to_string_lossy().into_owned(),
                kind: DaemonRepoLocationKind::PrimaryCheckout,
                default_branch: "main".into(),
                remote_url: None,
                expected_version: 0,
                probe: None,
            })
            .unwrap(),
            Vec::new,
        )
        .await
        .unwrap();
    std::fs::write(fixture.repo.join("work.bin"), vec![7u8; 3 * 1024]).unwrap();
    let tip = git::commit_all(&fixture.repo, "work").await.unwrap();
    let fence = attempt_request(&fixture, &tip, &base, 1, WorkspaceIntegrationKind::Rebase).fence;
    Transfer {
        fixture,
        fence,
        base,
        tip,
    }
}

impl Transfer {
    fn export(&self, key: &str, max_bytes: u64, offset: u64) -> ExportObjectsParams {
        ExportObjectsParams {
            daemon_id: "daemon-1".into(),
            runtime_id: "runtime-1".into(),
            fence: self.fence.clone(),
            key: key.into(),
            repo_location_id: "location-1".into(),
            have: vec![self.base.clone()],
            want: self.tip.clone(),
            max_bytes,
            offset,
        }
    }
    fn import(&self, key: &str, chunk: Option<ObjectChunk>) -> ImportObjectsParams {
        ImportObjectsParams {
            daemon_id: "daemon-1".into(),
            runtime_id: "runtime-1".into(),
            fence: self.fence.clone(),
            key: key.into(),
            repo_location_id: "location-2".into(),
            expected_tip_sha: self.tip.clone(),
            max_bytes: MAX_OBJECT_TRANSFER_BYTES,
            chunk,
        }
    }
    fn staging(&self) -> Vec<String> {
        let mut names: Vec<String> =
            std::fs::read_dir(self.fixture.dir.path().join(".forge/transfer"))
                .map(|entries| {
                    entries
                        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
                        .collect()
                })
                .unwrap_or_default();
        names.sort();
        names
    }
    async fn target(&self) -> String {
        let clone = self.fixture.dir.path().join("secondary");
        format!(
            "{}\n{}\n{}",
            local_git(&clone, &["for-each-ref"]).await.unwrap(),
            local_git(&clone, &["rev-parse", "HEAD"]).await.unwrap(),
            local_git(&clone, &["count-objects", "-v"]).await.unwrap()
        )
    }
}

fn refusal_of(error: DaemonErrorPayload) -> ObjectTransferRefusal {
    assert_eq!(error.code, OBJECT_TRANSFER_REFUSED, "{error:?}");
    serde_json::from_value(error.details.unwrap()["refusal"].clone()).unwrap()
}

#[tokio::test]
async fn objects_round_trip_between_two_checkouts_in_chunks_and_replay_by_key() {
    let transfer = transfer().await;
    let backend = &transfer.fixture.backend;
    assert_eq!(
        call::<ImportObjectsResult>(
            backend,
            METHOD_INTEGRATION_IMPORT_OBJECTS,
            transfer.import("attempt-1-1-out", None)
        )
        .await
        .unwrap(),
        ImportObjectsResult::Absent
    );
    let first: ExportObjectsResult = call(
        backend,
        METHOD_INTEGRATION_EXPORT_OBJECTS,
        transfer.export("attempt-1-1-out", MAX_OBJECT_TRANSFER_BYTES, 0),
    )
    .await
    .unwrap();
    assert!(first.eof);
    assert_eq!(first.receipt.tip_sha, transfer.tip);
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(&first.data)
        .unwrap();
    assert_eq!(bytes.len() as u64, first.receipt.total_bytes);
    // A retry reads the same staged bundle; a later offset continues it.
    let tail: ExportObjectsResult = call(
        backend,
        METHOD_INTEGRATION_EXPORT_OBJECTS,
        transfer.export("attempt-1-1-out", MAX_OBJECT_TRANSFER_BYTES, 10),
    )
    .await
    .unwrap();
    assert_eq!(tail.receipt, first.receipt);
    assert_eq!(
        base64::engine::general_purpose::STANDARD
            .decode(&tail.data)
            .unwrap(),
        bytes[10..]
    );
    let clone = transfer.fixture.dir.path().join("secondary");
    let head = git::get_current_sha(&clone).await.unwrap();
    // Two chunks; the first is also retried.
    let middle = bytes.len() / 2;
    let chunk = |range: std::ops::Range<usize>, last| ObjectChunk {
        total_bytes: first.receipt.total_bytes,
        sha256: first.receipt.sha256.clone(),
        offset: range.start as u64,
        data: base64::engine::general_purpose::STANDARD.encode(&bytes[range]),
        last,
    };
    for _ in 0..2 {
        assert_eq!(
            call::<ImportObjectsResult>(
                backend,
                METHOD_INTEGRATION_IMPORT_OBJECTS,
                transfer.import("attempt-1-1-out", Some(chunk(0..middle, false)))
            )
            .await
            .unwrap(),
            ImportObjectsResult::Receiving {
                received_bytes: middle as u64
            }
        );
    }
    let imported: ImportObjectsResult = call(
        backend,
        METHOD_INTEGRATION_IMPORT_OBJECTS,
        transfer.import("attempt-1-1-out", Some(chunk(middle..bytes.len(), true))),
    )
    .await
    .unwrap();
    assert_eq!(
        imported,
        ImportObjectsResult::Imported {
            receipt: ObjectImportReceipt {
                key: "attempt-1-1-out".into(),
                tip_sha: transfer.tip.clone(),
                ref_name: "refs/forge/integration/attempt-1-1-out".into(),
                replayed: false,
            }
        }
    );
    assert_eq!(git::get_current_sha(&clone).await.unwrap(), head);
    assert_eq!(
        local_git(
            &clone,
            &["rev-parse", "refs/forge/integration/attempt-1-1-out"]
        )
        .await
        .unwrap(),
        transfer.tip
    );
    assert!(!clone.join("work.bin").exists(), "nothing is checked out");
    // The import consumed its staging; the export's goes on release.
    assert_eq!(
        transfer.staging(),
        vec![
            "export-attempt-1-1-out.bundle",
            "export-attempt-1-1-out.json"
        ]
    );
    let released: ReleaseObjectsResult = call(
        backend,
        METHOD_INTEGRATION_RELEASE_OBJECTS,
        ReleaseObjectsParams {
            daemon_id: "daemon-1".into(),
            runtime_id: "runtime-1".into(),
            key: "attempt-1-1-out".into(),
            attempt: None,
        },
    )
    .await
    .unwrap();
    assert!(released.removed);
    assert!(transfer.staging().is_empty());
    // The duplicate key replays the receipt: no chunk is needed or stored.
    let before = transfer.target().await;
    let replay: ImportObjectsResult = call(
        &restart(&transfer.fixture),
        METHOD_INTEGRATION_IMPORT_OBJECTS,
        transfer.import("attempt-1-1-out", None),
    )
    .await
    .unwrap();
    assert!(
        matches!(replay, ImportObjectsResult::Imported { receipt } if receipt.replayed && receipt.tip_sha == transfer.tip)
    );
    assert_eq!(transfer.target().await, before);
    assert!(transfer.staging().is_empty());
    // The attempt left its queue slot: one release deletes every ref it
    // imported in this checkout (protocol revision 7), and nothing else.
    local_git(
        &clone,
        &[
            "update-ref",
            "refs/forge/integration/attempt-10-1-out",
            &transfer.tip,
        ],
    )
    .await
    .unwrap();
    let release = |attempt_id: &str| ReleaseObjectsParams {
        daemon_id: "daemon-1".into(),
        runtime_id: "runtime-1".into(),
        key: format!("{attempt_id}-0-in"),
        attempt: Some(ReleaseAttemptRefs {
            attempt_id: attempt_id.into(),
            repo_location_id: transfer.import("k", None).repo_location_id,
        }),
    };
    let released: ReleaseObjectsResult = call(
        backend,
        METHOD_INTEGRATION_RELEASE_OBJECTS,
        release("attempt-1"),
    )
    .await
    .unwrap();
    assert_eq!(released.removed_refs, 1);
    assert!(local_git(
        &clone,
        &["rev-parse", "refs/forge/integration/attempt-1-1-out"]
    )
    .await
    .is_err());
    assert_eq!(
        local_git(
            &clone,
            &["rev-parse", "refs/forge/integration/attempt-10-1-out"]
        )
        .await
        .unwrap(),
        transfer.tip,
        "another attempt's ref is not touched"
    );
    assert_eq!(git::get_current_sha(&clone).await.unwrap(), head);
    let again: ReleaseObjectsResult = call(
        backend,
        METHOD_INTEGRATION_RELEASE_OBJECTS,
        release("attempt-1"),
    )
    .await
    .unwrap();
    assert_eq!(again.removed_refs, 0, "idempotent");
}

#[tokio::test]
async fn object_transfer_refusals_leave_the_target_and_staging_untouched() {
    let transfer = transfer().await;
    let backend = &transfer.fixture.backend;
    let before = transfer.target().await;
    // Over the cap: the export keeps no file, the import stores no byte.
    let refused = call::<ExportObjectsResult>(
        backend,
        METHOD_INTEGRATION_EXPORT_OBJECTS,
        transfer.export("big", 16, 0),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        refusal_of(refused),
        ObjectTransferRefusal::TooLarge { max_bytes: 16, .. }
    ));
    let refused = call::<ImportObjectsResult>(
        backend,
        METHOD_INTEGRATION_IMPORT_OBJECTS,
        transfer.import(
            "big",
            Some(ObjectChunk {
                total_bytes: MAX_OBJECT_TRANSFER_BYTES + 1,
                sha256: String::new(),
                offset: 0,
                data: base64::engine::general_purpose::STANDARD.encode(b"bytes"),
                last: false,
            }),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(
        refusal_of(refused),
        ObjectTransferRefusal::TooLarge {
            bytes: MAX_OBJECT_TRANSFER_BYTES + 1,
            max_bytes: MAX_OBJECT_TRANSFER_BYTES
        }
    );
    assert!(transfer.staging().is_empty());
    // Corrupt, truncated and mislabelled payloads.
    let export: ExportObjectsResult = call(
        backend,
        METHOD_INTEGRATION_EXPORT_OBJECTS,
        transfer.export("k", MAX_OBJECT_TRANSFER_BYTES, 0),
    )
    .await
    .unwrap();
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(&export.data)
        .unwrap();
    let digest = |bytes: &[u8]| {
        use sha2::Digest;
        format!("{:x}", sha2::Sha256::digest(bytes))
    };
    let mut corrupt = bytes.clone();
    let at = corrupt.len() - 30;
    corrupt[at] ^= 0xff;
    let truncated = bytes[..bytes.len() - 40].to_vec();
    let cases = [
        (
            "digest mismatch",
            corrupt.clone(),
            export.receipt.sha256.clone(),
        ),
        ("corrupt pack", corrupt.clone(), digest(&corrupt)),
        ("truncated pack", truncated.clone(), digest(&truncated)),
    ];
    for (name, payload, sha256) in cases {
        let refused = call::<ImportObjectsResult>(
            backend,
            METHOD_INTEGRATION_IMPORT_OBJECTS,
            transfer.import(
                "k",
                Some(ObjectChunk {
                    total_bytes: payload.len() as u64,
                    sha256,
                    offset: 0,
                    data: base64::engine::general_purpose::STANDARD.encode(&payload),
                    last: true,
                }),
            ),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(refusal_of(refused), ObjectTransferRefusal::Invalid { .. }),
            "{name}"
        );
        assert_eq!(transfer.target().await, before, "{name}");
        assert_eq!(
            transfer.staging(),
            vec!["export-k.bundle", "export-k.json"],
            "{name}"
        );
    }
    // A shorter-than-declared transfer and an out-of-order chunk.
    for (offset, last) in [(0, true), (5, false)] {
        let refused = call::<ImportObjectsResult>(
            backend,
            METHOD_INTEGRATION_IMPORT_OBJECTS,
            transfer.import(
                "k",
                Some(ObjectChunk {
                    total_bytes: bytes.len() as u64,
                    sha256: export.receipt.sha256.clone(),
                    offset,
                    data: base64::engine::general_purpose::STANDARD.encode(&bytes[..4]),
                    last,
                }),
            ),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            refusal_of(refused),
            ObjectTransferRefusal::Invalid { .. }
        ));
        assert_eq!(transfer.staging(), vec!["export-k.bundle", "export-k.json"]);
    }
    // A cancelled transfer is released: nothing stays on the owner.
    call::<ImportObjectsResult>(
        backend,
        METHOD_INTEGRATION_IMPORT_OBJECTS,
        transfer.import(
            "k",
            Some(ObjectChunk {
                total_bytes: bytes.len() as u64,
                sha256: export.receipt.sha256.clone(),
                offset: 0,
                data: base64::engine::general_purpose::STANDARD.encode(&bytes[..4]),
                last: false,
            }),
        ),
    )
    .await
    .unwrap();
    assert_eq!(transfer.staging().len(), 3);
    call::<ReleaseObjectsResult>(
        backend,
        METHOD_INTEGRATION_RELEASE_OBJECTS,
        ReleaseObjectsParams {
            daemon_id: "daemon-1".into(),
            runtime_id: "runtime-1".into(),
            key: "k".into(),
            attempt: None,
        },
    )
    .await
    .unwrap();
    assert!(transfer.staging().is_empty());
    assert_eq!(transfer.target().await, before);
    // Keys that are not one ref component, a foreign owner and a stale claim.
    let refused = call::<ExportObjectsResult>(
        backend,
        METHOD_INTEGRATION_EXPORT_OBJECTS,
        transfer.export("../heads/main", MAX_OBJECT_TRANSFER_BYTES, 0),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        refusal_of(refused),
        ObjectTransferRefusal::Invalid { .. }
    ));
    let mut foreign = transfer.export("k", MAX_OBJECT_TRANSFER_BYTES, 0);
    foreign.daemon_id = "daemon-2".into();
    let refused = call::<ExportObjectsResult>(backend, METHOD_INTEGRATION_EXPORT_OBJECTS, foreign)
        .await
        .unwrap_err();
    assert_eq!(refused.code, WRONG_OWNER);
    let mut newer = transfer.fence.clone();
    newer.generation = 2;
    announce(backend, &newer, None).await.unwrap();
    let refused = call::<ImportObjectsResult>(
        backend,
        METHOD_INTEGRATION_IMPORT_OBJECTS,
        transfer.import("k", None),
    )
    .await
    .unwrap_err();
    assert_eq!(refusal_of(refused), ObjectTransferRefusal::StaleFence);
    assert!(transfer.staging().is_empty());
    assert_eq!(transfer.target().await, before);
}

/// Make every journal entry look as if it was last written `age` ago.
fn age_journal(fixture: &Fixture, age: Duration) {
    for entry in std::fs::read_dir(fixture.journal.directory()).unwrap() {
        let path = entry.unwrap().path();
        if path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().starts_with("entry-"))
        {
            std::fs::File::options()
                .write(true)
                .open(&path)
                .unwrap()
                .set_modified(std::time::SystemTime::now() - age)
                .unwrap();
        }
    }
}

/// A queue whose effect the server has not acknowledged keeps its fence at
/// the 1025th queue and when the server's live list leaves it out: its old
/// claim stays refused and its lookup never turns "unknown".
#[tokio::test]
async fn a_queue_with_unacknowledged_work_keeps_its_fence_past_every_bound() {
    let fixture = Fixture::new().await;
    let head = git::get_current_sha(fixture.path()).await.unwrap();
    let request = attempt_request(
        &fixture,
        &head,
        &head,
        2,
        WorkspaceIntegrationKind::FastForward,
    );
    let queue = request.fence.queue_id.clone();
    announce(&fixture.backend, &request.fence, None)
        .await
        .unwrap();
    let mut intent = attempt_merge_params(&fixture, request.clone());
    intent.merge.fence.operation_id = request.operation_id();
    retain_intent(
        &fixture,
        METHOD_WORKSPACE_MERGE,
        serde_json::to_value(intent).unwrap(),
    );
    {
        let mut state = fixture.backend.state.lock().unwrap();
        // The busy queue is the least recently recorded of all.
        state.integration_fence_seen.insert(queue.clone(), 0);
        for index in 0..integration_owner::MAX_INTEGRATION_FENCES + 5 {
            let mut fence = request.fence.clone();
            fence.queue_id = format!("bulk-{index:05}");
            state
                .advance_integration_fence(&fence, 1_000 + index as u64)
                .unwrap();
        }
    }
    let mut newest = request.fence.clone();
    newest.queue_id = "queue-new".into();
    let result = announce(&fixture.backend, &newest, None).await.unwrap();
    assert_eq!(result.pruned_fences, 7);
    {
        let state = fixture.backend.state.lock().unwrap();
        assert_eq!(
            state.integration_fences.len(),
            integration_owner::MAX_INTEGRATION_FENCES
        );
        assert_eq!(state.integration_fences[&queue], request.fence);
        assert!(!state.integration_fences.contains_key("bulk-00006"));
        assert!(state.integration_fences.contains_key("bulk-00007"));
    }
    // A live list that omits the busy queue (and the announced one).
    let result = announce(&fixture.backend, &newest, Some(Vec::new()))
        .await
        .unwrap();
    assert_eq!(
        result.pruned_fences as usize,
        integration_owner::MAX_INTEGRATION_FENCES - 2
    );
    let restarted = restart(&fixture);
    {
        let state = restarted.state.lock().unwrap();
        let mut kept: Vec<_> = state.integration_fences.keys().cloned().collect();
        kept.sort();
        assert_eq!(kept, vec![queue.clone(), "queue-new".to_owned()]);
    }
    // The older claim of the busy queue is still fenced off.
    let mut older = request.fence.clone();
    older.generation = 1;
    let refused = announce(&restarted, &older, None).await.unwrap_err();
    assert_eq!(refused.details.unwrap()["refusal"], "stale_fence");
    let found = lookup(&restarted, &fixture, &request).await;
    let announcement = found.owner_fence.unwrap();
    assert_eq!(announcement.generation, Some(2));
    assert_ne!(announcement.intent, IntegrationIntentRecord::Unknown);
}

/// A queue that never claims again does not keep its acknowledged receipt
/// for ever: after the retention it is replaced by a tombstone, so the
/// journal of a long-lived daemon with many idle queues stays bounded.
#[tokio::test]
async fn an_idle_queue_receipt_expires_into_a_tombstone() {
    let fixture = Fixture::new().await;
    std::fs::write(fixture.path().join("candidate"), "candidate\n").unwrap();
    let head = git::commit_all(fixture.path(), "candidate").await.unwrap();
    let target = git::get_current_sha(&fixture.repo).await.unwrap();
    let request = attempt_request(
        &fixture,
        &head,
        &target,
        1,
        WorkspaceIntegrationKind::FastForward,
    );
    let operation_id = request.operation_id();
    let params = serde_json::to_value(attempt_merge_params(&fixture, request.clone())).unwrap();
    let first = fixture
        .backend
        .handle(METHOD_WORKSPACE_MERGE, params.clone(), Vec::new)
        .await
        .unwrap();
    let ack = JournalAckParams {
        entry_id: first["entry_id"].as_str().unwrap().into(),
    };
    fixture.backend.acknowledge_journal(&ack).await.unwrap();
    // Inside the retention a repeated acknowledgement prunes nothing.
    age_journal(&fixture, Duration::from_secs(6 * 24 * 60 * 60));
    fixture.backend.acknowledge_journal(&ack).await.unwrap();
    assert!(fixture.journal.operation(&operation_id).unwrap().is_some());
    age_journal(&fixture, Duration::from_secs(8 * 24 * 60 * 60));
    let mut other = request.fence.clone();
    other.queue_id = "queue-other".into();
    let announced = announce(&fixture.backend, &other, None).await.unwrap();
    assert_eq!(announced.pruned_entries, 1);
    assert!(fixture.journal.operation(&operation_id).unwrap().is_none());
    // The queue's fence stays; a late duplicate of the key is not run again.
    assert!(fixture
        .backend
        .state
        .lock()
        .unwrap()
        .integration_fences
        .contains_key(&request.fence.queue_id));
    let reflog = local_git(&fixture.repo, &["reflog", "--all"])
        .await
        .unwrap();
    assert!(fixture
        .backend
        .handle(METHOD_WORKSPACE_MERGE, params, Vec::new)
        .await
        .is_err());
    assert_eq!(
        local_git(&fixture.repo, &["reflog", "--all"])
            .await
            .unwrap(),
        reflog
    );
}

async fn send_chunk(
    transfer: &Transfer,
    backend: &DaemonWorkspaceBackend,
    key: &str,
    chunk: ObjectChunk,
) -> CommandResult<ImportObjectsResult> {
    call(
        backend,
        METHOD_INTEGRATION_IMPORT_OBJECTS,
        transfer.import(key, Some(chunk)),
    )
    .await
}

/// The connection drops after the first chunk; then the daemon restarts.
/// Nothing staged survives the restart, crash leftovers in the checkout are
/// swept, a tail without its head is refused, and the sender's restart from
/// the first chunk imports the exact object.
#[tokio::test]
async fn an_interrupted_transfer_restarts_from_its_first_chunk() {
    let transfer = transfer().await;
    let backend = &transfer.fixture.backend;
    let key = "attempt-1-1-out";
    let first: ExportObjectsResult = call(
        backend,
        METHOD_INTEGRATION_EXPORT_OBJECTS,
        transfer.export(key, MAX_OBJECT_TRANSFER_BYTES, 0),
    )
    .await
    .unwrap();
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(&first.data)
        .unwrap();
    let middle = bytes.len() / 2;
    let chunk = |range: std::ops::Range<usize>, last| ObjectChunk {
        total_bytes: first.receipt.total_bytes,
        sha256: first.receipt.sha256.clone(),
        offset: range.start as u64,
        data: base64::engine::general_purpose::STANDARD.encode(&bytes[range]),
        last,
    };
    let clean = transfer.target().await;
    assert_eq!(
        send_chunk(&transfer, backend, key, chunk(0..middle, false))
            .await
            .unwrap(),
        ImportObjectsResult::Receiving {
            received_bytes: middle as u64
        }
    );
    // Same daemon, new connection: the sender starts over and the chunk
    // that was already stored is replaced, not appended.
    assert_eq!(
        send_chunk(&transfer, backend, key, chunk(0..middle, false))
            .await
            .unwrap(),
        ImportObjectsResult::Receiving {
            received_bytes: middle as u64
        }
    );
    assert!(transfer
        .staging()
        .iter()
        .any(|name| name.ends_with(".part")));
    // A daemon killed mid-export and mid-import leaves these in a checkout.
    let clone = transfer.fixture.dir.path().join("secondary");
    local_git(
        &clone,
        &["update-ref", "refs/forge/export/dead", &transfer.base],
    )
    .await
    .unwrap();
    let quarantine = clone.join(".git/forge-incoming-dead-1-2");
    std::fs::create_dir_all(quarantine.join("pack")).unwrap();
    let restarted = restart(&transfer.fixture);
    assert!(transfer.staging().is_empty(), "{:?}", transfer.staging());
    assert!(!quarantine.exists());
    assert_eq!(transfer.target().await, clean);
    // A tail without its head, a chunk past the declared size and an
    // offset that overflows are refused and stage nothing.
    let mut long = chunk(0..middle, false);
    long.total_bytes = middle as u64 - 1;
    let mut wrapped = chunk(0..middle, false);
    wrapped.offset = u64::MAX;
    for bad in [chunk(middle..bytes.len(), true), long, wrapped] {
        let refused = send_chunk(&transfer, &restarted, key, bad)
            .await
            .unwrap_err();
        assert!(matches!(
            refusal_of(refused),
            ObjectTransferRefusal::Invalid { .. }
        ));
        assert!(transfer.staging().is_empty(), "{:?}", transfer.staging());
    }
    assert_eq!(transfer.target().await, clean);
    assert_eq!(
        send_chunk(&transfer, &restarted, key, chunk(0..middle, false))
            .await
            .unwrap(),
        ImportObjectsResult::Receiving {
            received_bytes: middle as u64
        }
    );
    let ImportObjectsResult::Imported { receipt } =
        send_chunk(&transfer, &restarted, key, chunk(middle..bytes.len(), true))
            .await
            .unwrap()
    else {
        panic!("not imported")
    };
    assert!(!receipt.replayed);
    assert_eq!(receipt.tip_sha, transfer.tip);
    assert_eq!(
        local_git(&clone, &["rev-parse", &receipt.ref_name])
            .await
            .unwrap(),
        transfer.tip
    );
    assert!(transfer
        .staging()
        .iter()
        .all(|name| !name.ends_with(".part")));
}

// ----- the fast-forward of a transferred candidate ---------------------------
//
// `location-2` is the queue's default checkout on this owner; the candidate
// was made in another checkout and reaches it only as the ref the claim
// generation's outbound transfer binds (`<attempt>-<generation>-out`).

struct TransferredHead {
    transfer: Transfer,
    clone: PathBuf,
}

impl TransferredHead {
    async fn new() -> Self {
        let transfer = transfer().await;
        let clone = transfer.fixture.dir.path().join("secondary");
        Self { transfer, clone }
    }
    fn key(generation: i64) -> String {
        object_transfer_key("attempt-1", generation, ObjectTransferDirection::Outbound)
    }
    /// Bind `sha` (a commit of `location-1`) under `key` in the default
    /// checkout, as the outbound import does.
    async fn bind(&self, key: &str, sha: &str) {
        local_git(
            &self.clone,
            &[
                "fetch",
                "-q",
                self.transfer.fixture.repo.to_str().unwrap(),
                &format!("{sha}:refs/forge/integration/{key}"),
            ],
        )
        .await
        .unwrap();
    }
    fn request(&self, generation: i64, candidate: &str, target: &str) -> WorkspaceIntegrationRequest {
        let version = self.transfer.fixture.backend.state.lock().unwrap().locations["location-2"].version;
        let mut fence = self.transfer.fence.clone();
        fence.generation = generation;
        fence.target_owner = serde_json::json!({"location_id":"location-2","owner_kind":"daemon","daemon_id":"daemon-1","runtime_id":"runtime-1","generation":version});
        WorkspaceIntegrationRequest {
            fence,
            kind: WorkspaceIntegrationKind::FastForward,
            witness: serde_json::json!({"workspace":{"workspace_id":"workspace-9","placement_id":"placement-9","generation":1,"handle":"elsewhere","owner":{"kind":"server"}},"target_branch":"main","task_branch":"task","expected_head_sha":candidate,"expected_target_sha":target,"reviewed":{"commit_sha":candidate,"base_sha":target},"candidate_transfer_key":Self::key(generation)}),
        }
    }
    fn params(&self, request: &WorkspaceIntegrationRequest) -> Value {
        let candidate = request.witness["expected_head_sha"].as_str().unwrap().to_owned();
        serde_json::to_value(WorkspaceReviewedMergeParams {
            merge: WorkspaceMergeParams {
                fence: WorkspaceMutationFence {
                    integration: WorkspaceIntegrationBinding::Attempt {
                        request: request.clone(),
                    },
                    daemon_id: "daemon-1".into(),
                    runtime_id: "runtime-1".into(),
                    placement_id: String::new(),
                    operation_id: request.operation_id(),
                    generation: 0,
                    expected: WorkspaceOperationExpected::BaseSha {
                        sha: candidate.clone(),
                    },
                },
                workspace_handle: String::new(),
                repo_location_id: "location-2".into(),
                target_branch: "main".into(),
                expected_target_sha: request.witness["expected_target_sha"]
                    .as_str()
                    .unwrap()
                    .into(),
                handed_off_paths: Vec::new(),
            },
            reviewed_commit_sha: Some(candidate),
            candidate_transfer_key: request.witness["candidate_transfer_key"]
                .as_str()
                .map(str::to_owned),
        })
        .unwrap()
    }
    async fn merge(&self, backend: &DaemonWorkspaceBackend, params: Value) -> CommandResult<Value> {
        backend.handle(METHOD_WORKSPACE_MERGE, params, Vec::new).await
    }
    async fn tip(&self) -> String {
        local_git(&self.clone, &["rev-parse", "refs/heads/main"])
            .await
            .unwrap()
    }
    /// How often Forge moved `main` of the default checkout.
    async fn moves(&self) -> usize {
        local_git(&self.clone, &["reflog", "show", "--format=%gs", "main"])
            .await
            .unwrap()
            .lines()
            .filter(|line| line.contains("forge: fast-forward"))
            .count()
    }
    async fn lookup(
        &self,
        backend: &DaemonWorkspaceBackend,
        request: &WorkspaceIntegrationRequest,
    ) -> WorkspaceReconcileResult {
        // As the server asks for it: a transferred fast-forward names no
        // worktree of this owner.
        call(
            backend,
            METHOD_WORKSPACE_DESCRIBE,
            WorkspaceReconcileParams {
                integration: WorkspaceIntegrationBinding::Attempt {
                    request: request.clone(),
                },
                workspace: WorkspaceHandleReference {
                    daemon_id: "daemon-1".into(),
                    runtime_id: "runtime-1".into(),
                    placement_id: String::new(),
                    workspace_handle: String::new(),
                    generation: 0,
                },
                operation: WorkspaceReconcileOperation::Reconcile,
                operation_id: request.operation_id(),
            },
        )
        .await
        .unwrap()
    }
}

fn refusal_kind(result: CommandResult<Value>) -> Value {
    let error = result.unwrap_err();
    assert_eq!(error.code, "integration_owner_refused", "{error:?}");
    error.details.unwrap()["refusal"].clone()
}

#[tokio::test]
async fn a_transferred_candidate_is_fast_forwarded_once_from_its_own_generations_ref() {
    let head = TransferredHead::new().await;
    let (base, tip) = (head.transfer.base.clone(), head.transfer.tip.clone());
    let backend = &head.transfer.fixture.backend;
    head.bind(&TransferredHead::key(1), &tip).await;
    let request = head.request(1, &tip, &base);
    let params = head.params(&request);
    let done = head.merge(backend, params.clone()).await.unwrap();
    assert_eq!(done["outcome"]["kind"], "done", "{done}");
    assert_eq!(done["outcome"]["after_sha"], tip);
    assert_eq!(done["integration_receipt"]["operation_state"], "succeeded");
    assert_eq!(head.tip().await, tip);
    assert_eq!(
        local_git(&head.clone, &["status", "--porcelain"]).await.unwrap(),
        ""
    );
    assert_eq!(head.moves().await, 1);
    // The same frame again (a lost reply) is answered from the journal,
    // also by a restarted owner: no Git, no second move.
    assert_eq!(head.merge(backend, params.clone()).await.unwrap(), done);
    let restarted = restart(&head.transfer.fixture);
    assert_eq!(head.merge(&restarted, params).await.unwrap(), done);
    assert_eq!(head.moves().await, 1);
    // The lookup the server sends for it returns the same receipt.
    let found = head.lookup(&restarted, &request).await;
    assert_eq!(receipt(&found), done["integration_receipt"]);
}

#[tokio::test]
async fn the_transferred_fast_forward_gate_refuses_everything_but_the_permitted_commit() {
    // Wrong commit: the right key holds another commit than the request's.
    let head = TransferredHead::new().await;
    let (base, tip) = (head.transfer.base.clone(), head.transfer.tip.clone());
    let backend = &head.transfer.fixture.backend;
    std::fs::write(head.transfer.fixture.repo.join("other"), "other\n").unwrap();
    let other = git::commit_all(&head.transfer.fixture.repo, "other")
        .await
        .unwrap();
    head.bind(&TransferredHead::key(1), &other).await;
    let request = head.request(1, &tip, &base);
    assert_eq!(
        refusal_kind(head.merge(backend, head.params(&request)).await),
        "witness_mismatch"
    );
    assert_eq!(head.tip().await, base);

    // Missing ref: nothing was transferred under this generation's key.
    let head = TransferredHead::new().await;
    let backend = &head.transfer.fixture.backend;
    let request = head.request(1, &tip_of(&head), &head.transfer.base);
    assert_eq!(
        refusal_kind(head.merge(backend, head.params(&request)).await),
        "witness_mismatch"
    );
    assert_eq!(head.tip().await, head.transfer.base);

    // Wrong generation: the candidate sits under an older generation's key.
    // The request of generation 2 may name only its own key, and that key
    // holds nothing.
    let head = TransferredHead::new().await;
    let (base, tip) = (head.transfer.base.clone(), head.transfer.tip.clone());
    let backend = &head.transfer.fixture.backend;
    head.bind(&TransferredHead::key(1), &tip).await;
    let request = head.request(2, &tip, &base);
    assert_eq!(
        refusal_kind(head.merge(backend, head.params(&request)).await),
        "witness_mismatch"
    );
    let mut replayed = head.request(3, &tip, &base);
    replayed.witness["candidate_transfer_key"] = serde_json::json!(TransferredHead::key(1));
    assert_eq!(
        refusal_kind(head.merge(backend, head.params(&replayed)).await),
        "witness_mismatch"
    );
    // A key of another attempt, bound to the very same commit.
    head.bind("attempt-9-4-out", &tip).await;
    let mut foreign = head.request(4, &tip, &base);
    foreign.witness["candidate_transfer_key"] = serde_json::json!("attempt-9-4-out");
    assert_eq!(
        refusal_kind(head.merge(backend, head.params(&foreign)).await),
        "witness_mismatch"
    );
    // A fast-forward that names another performing owner is nobody's.
    let mut forged = head.request(5, &tip, &base);
    forged.witness["effect_owner"] = serde_json::json!({"location_id":"location-2","owner_kind":"daemon","daemon_id":"daemon-1","runtime_id":"runtime-1"});
    head.bind(&TransferredHead::key(5), &tip).await;
    assert_eq!(
        refusal_kind(head.merge(backend, head.params(&forged)).await),
        "foreign_owner"
    );
    // Only a queue claim may fast-forward from a transfer ref.
    let mut plain = head.params(&head.request(6, &tip, &base));
    plain["integration"] = serde_json::json!({"kind":"task_step"});
    let refused = head.merge(backend, plain).await.unwrap_err();
    assert_eq!(refused.code, INVALID_INPUT, "{refused:?}");
    assert_eq!(head.tip().await, base);
    assert_eq!(head.moves().await, 0);

    // Moved target: the default branch is no longer the tip the rebase was
    // onto. Nothing is written and the moved tip stays.
    let head = TransferredHead::new().await;
    let (base, tip) = (head.transfer.base.clone(), head.transfer.tip.clone());
    let backend = &head.transfer.fixture.backend;
    head.bind(&TransferredHead::key(1), &tip).await;
    std::fs::write(head.clone.join("outside"), "outside\n").unwrap();
    let moved = git::commit_all(&head.clone, "outside").await.unwrap();
    let request = head.request(1, &tip, &base);
    assert_eq!(
        refusal_kind(head.merge(backend, head.params(&request)).await),
        "witness_mismatch"
    );
    assert_eq!(head.tip().await, moved);
    assert_eq!(head.moves().await, 0);

    // Dirty target: a typed outcome, the branch does not move.
    let head = TransferredHead::new().await;
    let (base, tip) = (head.transfer.base.clone(), head.transfer.tip.clone());
    let backend = &head.transfer.fixture.backend;
    head.bind(&TransferredHead::key(1), &tip).await;
    std::fs::write(head.clone.join("README.md"), "edited by hand\n").unwrap();
    let request = head.request(1, &tip, &base);
    let dirty = head.merge(backend, head.params(&request)).await.unwrap();
    assert_eq!(dirty["outcome"]["kind"], "target_dirty", "{dirty}");
    assert_eq!(head.tip().await, base);
    assert_eq!(
        std::fs::read_to_string(head.clone.join("README.md")).unwrap(),
        "edited by hand\n"
    );
    assert_eq!(head.moves().await, 0);
}

fn tip_of(head: &TransferredHead) -> String {
    head.transfer.tip.clone()
}

/// The owner restarts in the middle of a transferred fast-forward: its
/// journal holds a started intent without an outcome. The lookup settles it
/// from the default checkout alone, in both directions, and the delayed
/// frame of the operation can never run afterwards.
#[tokio::test]
async fn an_owner_restart_during_a_transferred_fast_forward_is_settled_by_the_lookup() {
    // Stopped before the branch moved: provably not landed.
    let head = TransferredHead::new().await;
    let (base, tip) = (head.transfer.base.clone(), head.transfer.tip.clone());
    head.bind(&TransferredHead::key(1), &tip).await;
    let request = head.request(1, &tip, &base);
    let params = head.params(&request);
    announce(&head.transfer.fixture.backend, &request.fence, None)
        .await
        .unwrap();
    retain_intent(&head.transfer.fixture, METHOD_WORKSPACE_MERGE, params.clone());
    let restarted = restart(&head.transfer.fixture);
    let found = head.lookup(&restarted, &request).await;
    assert_eq!(receipt(&found)["result"]["kind"], "not_performed", "{found:?}");
    assert_eq!(receipt(&found)["operation_state"], "failed");
    assert_eq!(head.tip().await, base);
    // Settled for good: asked again, and the late frame, get the same
    // answer and move nothing.
    assert_eq!(receipt(&head.lookup(&restarted, &request).await), receipt(&found));
    let late = head.merge(&restarted, params).await.unwrap_err();
    assert_eq!(
        late.details.unwrap()["integration_receipt"],
        receipt(&found)
    );
    assert_eq!(head.tip().await, base);
    assert_eq!(head.moves().await, 0);

    // Stopped after the branch moved: proven landed, never repeated.
    let head = TransferredHead::new().await;
    let (base, tip) = (head.transfer.base.clone(), head.transfer.tip.clone());
    head.bind(&TransferredHead::key(1), &tip).await;
    let request = head.request(1, &tip, &base);
    let params = head.params(&request);
    announce(&head.transfer.fixture.backend, &request.fence, None)
        .await
        .unwrap();
    retain_intent(&head.transfer.fixture, METHOD_WORKSPACE_MERGE, params.clone());
    let landed = git::integration::fast_forward_exact(
        &head.clone,
        "main",
        &tip,
        &base,
        git::integration::FastForwardLimits {
            deadline: Duration::from_secs(30),
            output_bytes: 64 * 1024,
        },
    )
    .await
    .unwrap();
    assert!(matches!(landed, git::integration::MergeApplyOutcome::Applied));
    let restarted = restart(&head.transfer.fixture);
    let found = head.lookup(&restarted, &request).await;
    assert_eq!(receipt(&found)["operation_state"], "succeeded", "{found:?}");
    assert_eq!(receipt(&found)["result"]["outcome"]["Done"]["after_sha"], tip);
    let late = head.merge(&restarted, params).await.unwrap();
    assert_eq!(late["integration_receipt"], receipt(&found));
    assert_eq!(head.tip().await, tip);
    assert_eq!(head.moves().await, 1);
}
