//! Daemon integration primitives, receipts and object transfers.

use super::*;

#[tokio::test]
async fn daemon_merge_rebase_and_check_primitives_write_no_server_tables_or_events() {
    use services::integration_effects::rpc::{decode_reply, validate_merge_result, RpcExchange};
    for case in ["merge", "rebase", "check"] {
        let fixture = Fixture::new("forge-owner-effect-without-recorder").await;
        let candidate = fixture.candidate().await;
        if case == "merge" {
            fixture.approve_candidate().await;
        }
        if case == "rebase" {
            assert_eq!(
                fixture
                    .run(&format!(
                        "git -C {} commit --allow-empty -m target",
                        quote(&fixture.checkout.to_string_lossy())
                    ))
                    .await
                    .exit_code,
                0
            );
        }
        // Stop the fixture heartbeat so a background writer cannot invalidate
        // the all-table assertion. A describe reply drains earlier socket frames.
        fixture.link.as_ref().unwrap().tasks[2].abort();
        tokio::task::yield_now().await;
        let placement = &fixture.resolved.placement;
        let reference = WorkspaceHandleReference {
            daemon_id: fixture.daemon_id.clone(),
            runtime_id: placement.runtime_id.clone().unwrap(),
            placement_id: placement.id.clone(),
            workspace_handle: placement.workspace_handle.clone().unwrap(),
            generation: placement.generation as u64,
        };
        fixture
            .resolved
            .backend
            .daemon_client()
            .unwrap()
            .describe(
                &fixture.daemon_id,
                WorkspaceDescribeParams {
                    workspace: reference,
                },
            )
            .await
            .unwrap();
        let operation_id = db::new_uuid_v4();
        let fence = WorkspaceMutationFence {
            integration: api_types::WorkspaceIntegrationBinding::TaskStep,
            daemon_id: fixture.daemon_id.clone(),
            runtime_id: placement.runtime_id.clone().unwrap(),
            placement_id: placement.id.clone(),
            operation_id: operation_id.clone(),
            generation: placement.generation as u64,
            expected: WorkspaceOperationExpected::BaseSha {
                sha: candidate.clone(),
            },
        };
        let handle = placement.workspace_handle.clone().unwrap();
        let (method, request) = match case {
            "merge" => (
                METHOD_WORKSPACE_MERGE,
                serde_json::to_value(WorkspaceReviewedMergeParams {
                    merge: WorkspaceMergeParams {
                        fence,
                        workspace_handle: handle,
                        repo_location_id: placement.repo_location_id.clone(),
                        target_branch: "main".into(),
                        expected_target_sha: fixture.base_sha.clone(),
                        handed_off_paths: vec![],
                    },
                    reviewed_commit_sha: Some(candidate.clone()),
                    candidate_transfer_key: None,
                })
                .unwrap(),
            ),
            "rebase" => (
                METHOD_WORKSPACE_RESET,
                serde_json::to_value(WorkspaceOwnerOperationParams {
                    fence,
                    workspace_handle: handle,
                    operation: WorkspaceOwnerOperation::RebaseTarget {
                        target_branch: "main".into(),
                        handoff_conflicts: true,
                    },
                })
                .unwrap(),
            ),
            _ => (
                METHOD_WORKSPACE_RUN,
                serde_json::to_value(WorkspaceRunParams {
                    fence,
                    workspace_handle: handle,
                    purpose: WorkspaceRunPurpose::CiStep,
                    command: "printf '%s' \"$TEST_SECRET\"".into(),
                    env: vec![("TEST_SECRET".into(), "hidden-value".into())],
                    timeout_secs: 0,
                    max_output_bytes: u64::MAX,
                })
                .unwrap(),
            ),
        };
        let before = table_digests(&fixture.harness.state.db).await;
        let mut events = fixture.harness.state.event_bus.subscribe();
        let mut exchange = RpcExchange::prepare(
            fixture
                .harness
                .state
                .daemon_connections
                .get(&fixture.daemon_id)
                .unwrap(),
            method,
            request.clone(),
        )
        .unwrap();
        let value = exchange
            .execute(if case == "check" {
                None
            } else {
                Some(Duration::from_secs(5))
            })
            .await
            .unwrap();
        match case {
            "merge" => {
                let result: WorkspaceMergeResult = decode_reply(method, &request, &value).unwrap();
                validate_merge_result(&request, &result).unwrap();
                assert!(
                    matches!(result.outcome, WorkspaceMergeOutcome::Done { after_sha, .. } if after_sha == candidate)
                );
            }
            "rebase" => {
                let result: WorkspaceOwnerOperationResult =
                    decode_reply(method, &request, &value).unwrap();
                assert!(matches!(
                    result.outcome,
                    WorkspaceOwnerOperationOutcome::Rebased
                ));
            }
            _ => {
                let result: WorkspaceRunResult = decode_reply(method, &request, &value).unwrap();
                assert_eq!(result.exit_code, Some(0));
                assert!(!result.stdout.contains("hidden-value"));
                assert!(!result.timed_out);
            }
        }
        assert_eq!(
            before,
            table_digests(&fixture.harness.state.db).await,
            "{case}"
        );
        assert!(events.try_recv().is_err());
        let receipts: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM command_receipt WHERE correlation_id=?")
                .bind(&operation_id)
                .fetch_one(fixture.harness.state.db.pool())
                .await
                .unwrap();
        assert_eq!(receipts, 0);
        assert!(fixture.runtime.journal().pending().unwrap().iter().any(|entry| matches!(entry,
            JournalEntry::Operation { operation } if operation.fence.operation_id == operation_id && operation.outcome.is_some() && !operation.acknowledged)));
    }
}

#[tokio::test]
async fn integration_attempt_lost_reply_across_real_reconnect_settles_once() {
    use db::IntegrationQueueRepo;
    let mut fixture = Fixture::new("forge-integration-attempt-reconnect").await;
    let candidate = fixture.candidate().await;
    let database = fixture.harness.state.db.clone();
    let placement = fixture.resolved.placement.clone();
    let workspace = WorkspaceRepo::get_by_id(&*database, &placement.workspace_id)
        .await
        .unwrap()
        .unwrap();
    let task = TaskRepo::get_by_id(&*database, &placement.task_id, false)
        .await
        .unwrap()
        .unwrap();
    let epoch: i64 = sqlx::query_scalar("SELECT status_epoch FROM task WHERE id=?")
        .bind(&task.id)
        .fetch_one(database.pool())
        .await
        .unwrap();
    let queue = database
        .create_or_get_integration_queue(&workspace.repo_id, "main")
        .await
        .unwrap();
    let attempt = database
        .admit_integration_attempt(db::IntegrationAttempt::new(
            Some(queue.id.clone()),
            task.id,
            task.project_id,
            "reconnect-attempt".into(),
            task.status,
            epoch,
            task.version,
        ))
        .await
        .unwrap();
    let queue = database
        .integration_queue(&queue.id)
        .await
        .unwrap()
        .unwrap();
    database
        .claim_integration_queue(
            &queue.id,
            queue.revision,
            "reconnect-worker",
            &db::now_rfc3339(),
            "2099-01-01T00:00:00Z",
        )
        .await
        .unwrap();
    let fence = database
        .integration_owner_fence(&attempt.id)
        .await
        .unwrap()
        .unwrap();
    let witness = json!({"workspace":{"workspace_id":placement.workspace_id,"placement_id":placement.id,"generation":placement.generation,"handle":placement.workspace_handle,"owner":{"kind":"daemon","daemon_id":placement.daemon_id,"runtime_id":placement.runtime_id}},"target_branch":"main","task_branch":workspace.branch,"expected_head_sha":candidate,"expected_target_sha":fixture.base_sha,"reviewed":{"commit_sha":candidate,"base_sha":fixture.base_sha}});
    let request = db::IntegrationEffectRequest {
        fence,
        kind: db::IntegrationOperationKind::FastForward,
        witness,
    };
    let params = serde_json::to_value(WorkspaceReviewedMergeParams {
        merge: WorkspaceMergeParams {
            fence: WorkspaceMutationFence {
                integration: WorkspaceIntegrationBinding::TaskStep,
                daemon_id: fixture.daemon_id.clone(),
                runtime_id: placement.runtime_id.clone().unwrap(),
                placement_id: placement.id.clone(),
                operation_id: "assigned-by-attempt-sink".into(),
                generation: placement.generation as u64,
                expected: WorkspaceOperationExpected::BaseSha {
                    sha: candidate.clone(),
                },
            },
            workspace_handle: placement.workspace_handle.clone().unwrap(),
            repo_location_id: placement.repo_location_id.clone(),
            target_branch: "main".into(),
            expected_target_sha: fixture.base_sha.clone(),
            handed_off_paths: Vec::new(),
        },
        reviewed_commit_sha: Some(candidate.clone()),
        candidate_transfer_key: None,
    })
    .unwrap();
    let client = services::daemon_transport::workspace_client::DaemonWorkspaceClient::new(
        fixture.harness.state.daemon_connections.clone(),
    )
    .with_receipts(database.clone());
    fixture
        .link
        .as_ref()
        .unwrap()
        .drop_replies
        .lock()
        .unwrap()
        .insert(METHOD_WORKSPACE_MERGE.into());
    let effect = client.integration_effect(
        &fixture.daemon_id,
        request.clone(),
        METHOD_WORKSPACE_MERGE,
        params.clone(),
        Duration::from_secs(10),
    );
    let owner_finished = async {
        tokio::time::timeout(Duration::from_secs(8), async {
            while git::get_current_sha(&fixture.checkout).await.unwrap() != candidate {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        // This closes the actual WebSocket, failing the server's pending reply.
        fixture.link.take();
        common::fake_daemon::wait_until_disconnected(&fixture.harness.state, &fixture.daemon_id)
            .await;
    };
    let (lost_reply, _) = tokio::join!(effect, owner_finished);
    assert!(lost_reply.is_err());
    assert!(database
        .integration_attempt(&attempt.id)
        .await
        .unwrap()
        .unwrap()
        .effect_intent_json
        .is_some());
    let reflog = git::command_output(&fixture.checkout, &["reflog", "--all"])
        .await
        .unwrap()
        .stdout;
    let objects = git::command_output(&fixture.checkout, &["count-objects", "-v"])
        .await
        .unwrap()
        .stdout;
    fixture.reconnect().await;
    client
        .reconcile_integration_attempts(&fixture.daemon_id)
        .await
        .unwrap();
    let receipt = client
        .integration_effect(
            &fixture.daemon_id,
            request.clone(),
            METHOD_WORKSPACE_MERGE,
            params,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(
        receipt.operation_state,
        db::IntegrationOperationState::Succeeded
    );
    assert_eq!(receipt.result["outcome"]["Done"]["after_sha"], candidate);
    assert_eq!(
        git::command_output(&fixture.checkout, &["reflog", "--all"])
            .await
            .unwrap()
            .stdout,
        reflog
    );
    assert_eq!(
        git::command_output(&fixture.checkout, &["count-objects", "-v"])
            .await
            .unwrap()
            .stdout,
        objects
    );
    assert_eq!(
        fixture
            .link
            .as_ref()
            .unwrap()
            .requests(METHOD_WORKSPACE_MERGE)
            .len(),
        0
    );
    let settled = database
        .integration_attempt(&attempt.id)
        .await
        .unwrap()
        .unwrap();
    assert!(settled.effect_intent_json.is_none());
    assert_eq!(
        settled.current_operation_state,
        Some(db::IntegrationOperationState::Succeeded)
    );
    let count: i64 = sqlx::query_scalar(
        "SELECT json_array_length(effect_receipts_json) FROM integration_attempt WHERE id=?",
    )
    .bind(&attempt.id)
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn check_spec_real_roundtrip_reconnect_and_duplicate_return_owner_receipt() {
    let mut fixture = Fixture::new("check-owner-roundtrip").await;
    let client = services::daemon_transport::workspace_client::DaemonWorkspaceClient::new(
        fixture.harness.state.daemon_connections.clone(),
    );
    let daemon_id = fixture.daemon_id.clone();
    let placement = &fixture.resolved.placement;
    let operation_id = db::new_uuid_v4();
    let params = DaemonCheckRunParams {
        operation_id: operation_id.clone(),
        target: DaemonCheckTarget::Workspace {
            workspace: WorkspaceHandleReference {
                daemon_id: daemon_id.clone(),
                runtime_id: placement.runtime_id.clone().unwrap(),
                placement_id: placement.id.clone(),
                workspace_handle: placement.workspace_handle.clone().unwrap(),
                generation: placement.generation as u64,
            },
        },
        purpose: WorkspaceRunPurpose::CiStep,
        spec: CheckSpec {
            schema_revision: CHECK_SPEC_REVISION,
            scope: CheckScope::Commit,
            commands: vec![CheckCommandSpec {
                id: "ci:0".into(),
                shell_text: "printf run >> check-runs; sleep 3; printf owner-result".into(),
                shell: "bash -lc".into(),
                working_directory: CheckWorkingDirectory::TaskRoot,
                environment_keys: Default::default(),
                // Generous: the command sleeps 3 s and a loaded runner starts it late.
                timeout_seconds: Some(30),
                failure_policy: CheckFailurePolicy::StopBundle,
                cacheability: CheckCacheability::Uncacheable,
                requirement_ids: Default::default(),
            }],
            declares_cleanup: false,
            configured_commands: 1,
            blank_commands: vec![],
            execution_policy: "legacy-daemon/1".into(),
        },
        env: vec![],
        cleanup_commands: vec![],
        cleanup_timeout_ms: 1000,
        deadline: (chrono::Utc::now() + chrono::Duration::seconds(60)).to_rfc3339(),
    };
    let effect = client.run_check(&daemon_id, params.clone());
    let disconnect = async {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if matches!(
                    client
                        .lookup_check(&daemon_id, &operation_id)
                        .await
                        .unwrap(),
                    DaemonCheckResult::Running { .. }
                ) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        // A duplicate on the actual command stream starts nothing.
        assert!(matches!(
            client.run_check(&daemon_id, params.clone()).await.unwrap(),
            DaemonCheckResult::Running { .. }
        ));
        fixture.link.take();
        common::fake_daemon::wait_until_disconnected(&fixture.harness.state, &daemon_id).await;
    };
    let (lost, ()) = tokio::join!(effect, disconnect);
    assert!(lost.is_err());
    fixture.reconnect().await;
    let receipt = tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            match client
                .lookup_check(&daemon_id, &operation_id)
                .await
                .unwrap()
            {
                DaemonCheckResult::Completed { receipt } => break receipt,
                DaemonCheckResult::Running { .. } => {
                    tokio::time::sleep(Duration::from_millis(20)).await
                }
                result => panic!("unexpected lookup {result:?}"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(receipt.outcome, CheckExecutionOutcome::Passed);
    assert_eq!(
        receipt.execution_inputs,
        CheckEnvironmentIdentity::NotAttested
    );
    assert_eq!(receipt.commands[0].stdout_tail, "owner-result");
    assert_eq!(
        client.run_check(&daemon_id, params).await.unwrap(),
        DaemonCheckResult::Completed { receipt }
    );
    let runs = fixture
        .resolved
        .run("cat check-runs", &BTreeMap::new(), None)
        .await
        .unwrap();
    assert_eq!(runs.stdout, "run");
}

#[tokio::test]
async fn integration_objects_move_between_daemon_and_server_owners_by_key() {
    use db::IntegrationQueueRepo;
    use services::integration_owner::{
        ObjectTransferOutcome, ServerIntegrationOwner, ServerObjectExport, ServerObjectImport,
    };
    let fixture = Fixture::new("forge-integration-object-transfer").await;
    let candidate = fixture.candidate().await;
    let database = fixture.harness.state.db.clone();
    let placement = fixture.resolved.placement.clone();
    let (fence, attempt_id) = claimed_attempt(&fixture, "object-transfer").await;
    let repo_id = WorkspaceRepo::get_by_id(&*database, &placement.workspace_id)
        .await
        .unwrap()
        .unwrap()
        .repo_id;
    // A server-owned clone of the same repository, without the candidate.
    let server_clone = fixture.server_root.path().join("server-clone");
    git_sync(
        fixture.server_root.path(),
        &[
            "clone",
            "-q",
            "--single-branch",
            "--branch",
            "main",
            fixture.checkout.to_str().unwrap(),
            "server-clone",
        ],
    );
    // A clone does not copy the source's local identity.
    for (key, value) in [
        ("user.name", "Forge"),
        ("user.email", "forge@example.invalid"),
    ] {
        git_sync(&server_clone, &["config", key, value]);
    }
    let now = db::now_rfc3339();
    sqlx::query("INSERT INTO repo_location(id,repo_id,owner_kind,path,kind,is_default,status,created_at,updated_at) VALUES('server-clone',?,'server',?,'managed_clone',0,'ready',?,?)")
        .bind(&repo_id).bind(server_clone.to_str()).bind(&now).bind(&now)
        .execute(database.pool()).await.unwrap();
    let owner = ServerIntegrationOwner::new(database.clone());
    let client = services::daemon_transport::workspace_client::DaemonWorkspaceClient::new(
        fixture.harness.state.daemon_connections.clone(),
    )
    .with_receipts(database.clone());
    let runtime_id = placement.runtime_id.clone().unwrap();
    let daemon = services::daemon_transport::workspace_client::DaemonObjectEndpoint {
        daemon_id: &fixture.daemon_id,
        runtime_id: &runtime_id,
        repo_location_id: &placement.repo_location_id,
    };
    let cancel = tokio_util::sync::CancellationToken::new();
    let daemon_root = fixture._daemon_root.path().canonicalize().unwrap();

    // Daemon -> server: the candidate goes to the server-owned checkout.
    let outbound = object_transfer_key(
        &attempt_id,
        fence.generation,
        ObjectTransferDirection::Outbound,
    );
    assert_eq!(
        owner
            .imported_objects(&fence, "server-clone", &outbound, &candidate)
            .await
            .unwrap(),
        ObjectTransferOutcome::Done(None)
    );
    let bundle = fixture.server_root.path().join("outbound.bundle");
    let have = vec![fixture.base_sha.clone()];
    let ObjectTransferOutcome::Done(export) = client
        .export_objects(
            daemon, &fence, &outbound, &have, &candidate, &bundle, &cancel,
        )
        .await
        .unwrap()
    else {
        panic!("daemon export refused")
    };
    assert_eq!(export.tip_sha, candidate);
    assert_eq!(
        std::fs::metadata(&bundle).unwrap().len(),
        export.total_bytes
    );
    let server_head = git_sync(&server_clone, &["rev-parse", "HEAD"]);
    let imported = owner
        .import_objects(ServerObjectImport {
            fence: &fence,
            export: &export,
            repo_location_id: "server-clone",
            bundle: &bundle,
            cancel: &cancel,
        })
        .await
        .unwrap();
    assert!(
        matches!(&imported, ObjectTransferOutcome::Done(receipt) if !receipt.replayed && receipt.tip_sha == candidate)
    );
    assert_eq!(
        git_sync(
            &server_clone,
            &["rev-parse", &format!("refs/forge/integration/{outbound}")]
        ),
        candidate
    );
    assert_eq!(git_sync(&server_clone, &["rev-parse", "HEAD"]), server_head);
    assert_eq!(
        git_sync(&server_clone, &["rev-parse", "refs/heads/main"]),
        server_head
    );
    client
        .release_objects(&fixture.daemon_id, &runtime_id, &outbound)
        .await;
    assert!(transfer_staging(&daemon_root).is_empty());
    assert!(matches!(
        owner.imported_objects(&fence, "server-clone", &outbound, &candidate).await.unwrap(),
        ObjectTransferOutcome::Done(Some(receipt)) if receipt.replayed
    ));

    // Server -> daemon: a commit made on the server-owned checkout.
    std::fs::write(server_clone.join("server.txt"), "server\n").unwrap();
    git_sync(&server_clone, &["add", "."]);
    git_sync(&server_clone, &["commit", "-q", "-m", "server"]);
    let server_tip = git_sync(&server_clone, &["rev-parse", "HEAD"]);
    let inbound = object_transfer_key(
        &attempt_id,
        fence.generation,
        ObjectTransferDirection::Inbound,
    );
    let bundle = fixture.server_root.path().join("inbound.bundle");
    let ObjectTransferOutcome::Done(export) = owner
        .export_objects(ServerObjectExport {
            fence: &fence,
            key: &inbound,
            repo_location_id: "server-clone",
            have: &have,
            want: &server_tip,
            dest: &bundle,
            cancel: &cancel,
        })
        .await
        .unwrap()
    else {
        panic!("server export refused")
    };
    assert_eq!(
        client
            .imported_objects(daemon, &fence, &inbound, &server_tip)
            .await
            .unwrap(),
        ObjectTransferOutcome::Done(None)
    );
    let daemon_refs = git_sync(&fixture.checkout, &["for-each-ref"]);
    let daemon_head = git_sync(&fixture.checkout, &["rev-parse", "HEAD"]);
    // A cancelled push, a mislabelled bundle and an oversized one change
    // nothing on the owner and leave no staging there.
    let cancelled = tokio_util::sync::CancellationToken::new();
    cancelled.cancel();
    assert_eq!(
        client
            .import_objects(daemon, &fence, &export, &bundle, &cancelled)
            .await
            .unwrap(),
        ObjectTransferOutcome::Cancelled
    );
    let mut mislabelled = export.clone();
    mislabelled.sha256 = "0".repeat(64);
    assert!(matches!(
        client
            .import_objects(daemon, &fence, &mislabelled, &bundle, &cancel)
            .await
            .unwrap(),
        ObjectTransferOutcome::Refused(ObjectTransferRefusal::Invalid { .. })
    ));
    let mut oversized = export.clone();
    oversized.total_bytes = MAX_OBJECT_TRANSFER_BYTES + 1;
    assert!(matches!(
        client
            .import_objects(daemon, &fence, &oversized, &bundle, &cancel)
            .await
            .unwrap(),
        ObjectTransferOutcome::Refused(ObjectTransferRefusal::TooLarge { .. })
    ));
    assert!(transfer_staging(&daemon_root).is_empty());
    assert_eq!(git_sync(&fixture.checkout, &["for-each-ref"]), daemon_refs);
    let received = client
        .import_objects(daemon, &fence, &export, &bundle, &cancel)
        .await
        .unwrap();
    assert!(
        matches!(&received, ObjectTransferOutcome::Done(receipt) if !receipt.replayed && receipt.tip_sha == server_tip)
    );
    assert_eq!(
        git_sync(
            &fixture.checkout,
            &["rev-parse", &format!("refs/forge/integration/{inbound}")]
        ),
        server_tip
    );
    assert_eq!(
        git_sync(&fixture.checkout, &["rev-parse", "HEAD"]),
        daemon_head
    );
    assert!(transfer_staging(&daemon_root).is_empty());
    assert!(matches!(
        client.imported_objects(daemon, &fence, &inbound, &server_tip).await.unwrap(),
        ObjectTransferOutcome::Done(Some(receipt)) if receipt.replayed
    ));

    // The owner knows this claim generation (the transfers carried its
    // fence; an announcement repeats it): a started intent it never received
    // is settled as not performed.
    let announced = client
        .announce_integration_fence(&fixture.daemon_id, &runtime_id, &fence, None)
        .await
        .unwrap();
    assert_eq!(announced.previous.as_ref(), Some(&fence));
    lost_fast_forward(&fixture, &fence, &candidate).await;
    client
        .reconcile_integration_attempts(&fixture.daemon_id)
        .await
        .unwrap();
    let settled = database
        .integration_attempt(&attempt_id)
        .await
        .unwrap()
        .unwrap();
    assert!(settled.effect_intent_json.is_none());
    assert_eq!(
        settled.current_operation_state,
        Some(db::IntegrationOperationState::Failed)
    );
    assert_eq!(
        git_sync(&fixture.checkout, &["rev-parse", "HEAD"]),
        daemon_head
    );

    // The server stopped between a transfer and its release: the attempt is
    // off the slot and its ref is still on the daemon-owned checkout. The
    // next start releases it there, by the release wire, per ended attempt.
    let has_ref = |reference: String| {
        std::process::Command::new("git")
            .arg("-C")
            .arg(&fixture.checkout)
            .args(["rev-parse", "--verify", "--quiet", &reference])
            .output()
            .unwrap()
            .status
            .success()
    };
    let inbound_ref = format!("refs/forge/integration/{inbound}");
    assert!(has_ref(inbound_ref.clone()));
    sqlx::query("UPDATE integration_attempt SET repo_location_id='server-clone',started_at=?,updated_at=?,revision=revision+1 WHERE id=?")
        .bind(&now).bind(db::now_rfc3339()).bind(&attempt_id)
        .execute(database.pool()).await.unwrap();
    sqlx::query("UPDATE integration_queue SET head_attempt_id=NULL,revision=revision+1 WHERE head_attempt_id=?")
        .bind(&attempt_id)
        .execute(database.pool()).await.unwrap();
    let transfer = services::integration_ports::OwnerObjectTransfer::new(
        database.clone(),
        Arc::new(ServerIntegrationOwner::new(database.clone())),
        client.clone(),
        Arc::new(services::integration_ports::DaemonFences::new(
            database.clone(),
            client.clone(),
        )),
        &fixture.server_root.path().join("sweep-staging"),
    );
    services::integration_worker::ObjectTransferPort::sweep_at_start(&transfer)
        .await
        .unwrap();
    assert!(
        !has_ref(inbound_ref),
        "the ended attempt's ref is gone from the daemon-owned checkout"
    );
    assert_eq!(
        git_sync(&fixture.checkout, &["rev-parse", "HEAD"]),
        daemon_head
    );
}

#[tokio::test]
async fn integration_lookup_on_an_owner_without_the_claim_fence_stays_unknown() {
    use db::IntegrationQueueRepo;
    let fixture = Fixture::new("forge-integration-unknown-lookup").await;
    let candidate = fixture.candidate().await;
    let database = fixture.harness.state.db.clone();
    let (fence, attempt_id) = claimed_attempt(&fixture, "unknown-lookup").await;
    let client = services::daemon_transport::workspace_client::DaemonWorkspaceClient::new(
        fixture.harness.state.daemon_connections.clone(),
    )
    .with_receipts(database.clone());
    // The claim was never announced to this owner and no effect of it was
    // admitted there: its empty journal cannot prove "not performed".
    lost_fast_forward(&fixture, &fence, &candidate).await;
    let head = git_sync(&fixture.checkout, &["rev-parse", "HEAD"]);
    for _ in 0..2 {
        client
            .reconcile_integration_attempts(&fixture.daemon_id)
            .await
            .unwrap();
        let attempt = database
            .integration_attempt(&attempt_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            attempt.current_operation_state,
            Some(db::IntegrationOperationState::Uncertain)
        );
    }
    assert_eq!(git_sync(&fixture.checkout, &["rev-parse", "HEAD"]), head);
}
