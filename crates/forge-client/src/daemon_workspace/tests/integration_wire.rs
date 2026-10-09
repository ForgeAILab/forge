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
