//! Integration attempt fences, durable receipts, and merge recovery.

use super::*;

#[tokio::test]
async fn integration_attempt_duplicate_replays_durable_receipt_without_git() {
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
    let params = serde_json::to_value(attempt_merge_params(&fixture, request)).unwrap();
    let first = fixture
        .backend
        .handle(METHOD_WORKSPACE_MERGE, params.clone(), Vec::new)
        .await
        .unwrap();
    assert_eq!(first["integration_receipt"]["operation_state"], "succeeded");
    let reflog = local_git(&fixture.repo, &["reflog", "--all"])
        .await
        .unwrap();
    let objects = local_git(&fixture.repo, &["count-objects", "-v"])
        .await
        .unwrap();
    let restart = DaemonWorkspaceBackend::new(
        fixture.dir.path().to_owned(),
        "daemon-1".into(),
        fixture.backend.policy.clone(),
        fixture.journal.clone(),
    )
    .unwrap();
    // Hide Git entirely: replay must not even perform a probe.
    std::fs::rename(fixture.repo.join(".git"), fixture.repo.join("hidden-git")).unwrap();
    let duplicate = restart
        .handle(METHOD_WORKSPACE_MERGE, params, Vec::new)
        .await
        .unwrap();
    assert_eq!(first, duplicate);
    std::fs::rename(fixture.repo.join("hidden-git"), fixture.repo.join(".git")).unwrap();
    assert_eq!(
        local_git(&fixture.repo, &["reflog", "--all"])
            .await
            .unwrap(),
        reflog
    );
    assert_eq!(
        local_git(&fixture.repo, &["count-objects", "-v"])
            .await
            .unwrap(),
        objects
    );
}

#[tokio::test]
async fn integration_owner_refuses_stale_foreign_and_placement_before_git() {
    for case in ["stale", "foreign", "placement"] {
        let fixture = Fixture::new().await;
        let head = git::get_current_sha(fixture.path()).await.unwrap();
        let mut request = attempt_request(
            &fixture,
            &head,
            &head,
            1,
            WorkspaceIntegrationKind::FastForward,
        );
        match case {
            "stale" => {
                fixture
                    .backend
                    .state
                    .lock()
                    .unwrap()
                    .integration_fences
                    .insert(
                        "queue-1".into(),
                        IntegrationOwnerFence {
                            generation: 2,
                            ..request.fence.clone()
                        },
                    );
            }
            "foreign" => request.fence.target_owner["daemon_id"] = serde_json::json!("foreign"),
            "placement" => request.witness["workspace"]["generation"] = serde_json::json!(99),
            _ => unreachable!(),
        }
        let params = serde_json::to_value(attempt_merge_params(&fixture, request)).unwrap();
        std::fs::rename(fixture.repo.join(".git"), fixture.repo.join("hidden-git")).unwrap();
        let error = fixture
            .backend
            .handle(METHOD_WORKSPACE_MERGE, params, Vec::new)
            .await
            .unwrap_err();
        assert_eq!(error.code, "integration_owner_refused", "{case}: {error:?}");
        assert_eq!(
            error.details.unwrap()["refusal"],
            match case {
                "stale" => "stale_fence",
                "foreign" => "foreign_owner",
                _ => "witness_mismatch",
            }
        );
        std::fs::rename(fixture.repo.join("hidden-git"), fixture.repo.join(".git")).unwrap();
    }
}

#[tokio::test]
async fn integration_object_mismatch_retains_not_performed_receipt() {
    let fixture = Fixture::new().await;
    let head = git::get_current_sha(fixture.path()).await.unwrap();
    let request = attempt_request(
        &fixture,
        "changed",
        &head,
        1,
        WorkspaceIntegrationKind::FastForward,
    );
    let params = serde_json::to_value(attempt_merge_params(&fixture, request.clone())).unwrap();
    let before = local_git(&fixture.repo, &["reflog", "--all"])
        .await
        .unwrap();
    let error = fixture
        .backend
        .handle(METHOD_WORKSPACE_MERGE, params, Vec::new)
        .await
        .unwrap_err();
    assert_eq!(
        error.details.unwrap()["integration_receipt"]["result"]["kind"],
        "not_performed"
    );
    assert_eq!(
        local_git(&fixture.repo, &["reflog", "--all"])
            .await
            .unwrap(),
        before
    );
    let entry = fixture
        .journal
        .operation(&request.operation_id())
        .unwrap()
        .unwrap();
    assert!(!entry.effect_started);
    assert!(entry.outcome.is_some());
}

#[cfg(unix)]
#[tokio::test]
async fn integration_cancel_mid_pick_stops_driver_and_records_rebase_state() {
    let fixture = Fixture::new().await;
    std::fs::write(
        fixture.repo.join(".gitattributes"),
        "README.md merge=slow\n",
    )
    .unwrap();
    git::commit_all(&fixture.repo, "attributes").await.unwrap();
    git::rebase(fixture.path(), "main").await.unwrap();
    std::fs::write(fixture.path().join("README.md"), "candidate\n").unwrap();
    let head = git::commit_all(fixture.path(), "candidate").await.unwrap();
    std::fs::write(fixture.repo.join("README.md"), "target\n").unwrap();
    let target = git::commit_all(&fixture.repo, "target").await.unwrap();
    let driver_pid = fixture.dir.path().join("driver.pid");
    let git_pid = fixture.dir.path().join("pick.pid");
    let driver = format!(
        "sh -c 'echo $$ > \"$1\"; echo $PPID > \"$2\"; exec sleep 60' sh '{}' '{}'",
        driver_pid.display(),
        git_pid.display()
    );
    local_git(&fixture.repo, &["config", "merge.slow.driver", &driver])
        .await
        .unwrap();
    let request = attempt_request(
        &fixture,
        &head,
        &target,
        1,
        WorkspaceIntegrationKind::Rebase,
    );
    let operation_id = request.operation_id();
    let mut fence = fence(&operation_id, 1, &head);
    fence.integration = WorkspaceIntegrationBinding::Attempt { request };
    let params = serde_json::to_value(WorkspaceOwnerOperationParams {
        fence,
        workspace_handle: fixture.prepared.workspace.workspace_handle.clone(),
        operation: WorkspaceOwnerOperation::RebaseTarget {
            target_branch: "main".into(),
            handoff_conflicts: false,
        },
    })
    .unwrap();
    let operation = fixture
        .backend
        .handle(METHOD_WORKSPACE_RESET, params, Vec::new);
    let cancel = async {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !driver_pid.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        fixture
            .backend
            .cancel_command(WorkspaceCancelParams {
                operation_id: operation_id.clone(),
            })
            .await
            .unwrap()
    };
    let (result, cancel) = tokio::join!(operation, cancel);
    assert!(result.is_err());
    assert_eq!(cancel.state, WorkspaceCancelState::Killed);
    for path in [driver_pid, git_pid] {
        let pid = std::fs::read_to_string(path).unwrap();
        let status = std::process::Command::new("kill")
            .args(["-0", pid.trim()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        if status.success() {
            let zombie =
                std::fs::read_to_string(format!("/proc/{}/stat", pid.trim())).is_ok_and(|stat| {
                    stat.rsplit_once(") ")
                        .is_some_and(|(_, rest)| rest.starts_with('Z'))
                });
            assert!(zombie, "cancelled pick left process {pid} alive");
        }
    }
    let lock = local_git(fixture.path(), &["rev-parse", "--git-path", "index.lock"])
        .await
        .unwrap();
    assert!(!fixture.path().join(lock).exists());
    assert!(git::detect_rebase_in_progress(fixture.path())
        .await
        .unwrap());
    let receipt = fixture
        .journal
        .operation(&operation_id)
        .unwrap()
        .unwrap()
        .outcome
        .unwrap()
        .unwrap_err()
        .details
        .unwrap()["integration_receipt"]
        .clone();
    assert_eq!(receipt["result"]["kind"], "cancelled");
    assert_eq!(receipt["result"]["rebase_in_progress"], true);
    assert_eq!(receipt["operation_state"], "failed");
    git::abort_rebase(fixture.path()).await.unwrap();
    assert_eq!(git::get_current_sha(fixture.path()).await.unwrap(), head);
}

#[tokio::test]
async fn integration_receipt_lookup_survives_workspace_retirement() {
    let fixture = Fixture::new().await;
    let head = git::get_current_sha(fixture.path()).await.unwrap();
    let request = attempt_request(
        &fixture,
        &head,
        &head,
        1,
        WorkspaceIntegrationKind::FastForward,
    );
    let operation_id = request.operation_id();
    let params = serde_json::to_value(attempt_merge_params(&fixture, request)).unwrap();
    let first = fixture
        .backend
        .handle(METHOD_WORKSPACE_MERGE, params, Vec::new)
        .await
        .unwrap();
    fixture.backend.state.lock().unwrap().handles.clear();
    let lookup = reconcile(&fixture, &operation_id).await;
    let WorkspaceReconcileOutcome::Result { result } = lookup.outcome else {
        panic!("receipt disappeared after cleanup");
    };
    assert_eq!(result, first);
}

/// The live daemon path. A normal Task merge must not be refused because the
/// server's location version moved on without a verify (a default toggle), nor
/// because an earlier Task-step effect on this checkout was left without an
/// outcome by an owner restart and its step has since been replaced.
#[tokio::test]
async fn task_step_merge_survives_a_location_version_bump_and_an_orphaned_effect() {
    let fixture = Fixture::new().await;
    std::fs::write(fixture.path().join("candidate"), "candidate\n").unwrap();
    let head = git::commit_all(fixture.path(), "candidate").await.unwrap();
    let target = git::get_current_sha(&fixture.repo).await.unwrap();
    let known = fixture.backend.state.lock().unwrap().locations["location-1"].version;

    // An earlier claim's merge was journaled and started, then the owner died.
    let orphan = task_step_merge_params(&fixture, "orphaned-merge", &head, &target, 1, known);
    retain_intent(
        &fixture,
        METHOD_WORKSPACE_MERGE,
        journal_request(&serde_json::to_value(&orphan).unwrap()),
    );
    {
        let mut state = fixture.backend.state.lock().unwrap();
        state
            .integration_pending
            .insert("location-1".into(), "orphaned-merge".into());
    }

    let params = task_step_merge_params(&fixture, "next-merge", &head, &target, 2, known + 7);
    let merged = fixture
        .backend
        .handle(
            METHOD_WORKSPACE_MERGE,
            serde_json::to_value(params).unwrap(),
            Vec::new,
        )
        .await
        .expect("a live Task-step merge was refused");
    assert_eq!(merged["outcome"]["kind"], "done", "{merged}");
    assert_eq!(
        merged["integration_receipt"]["operation_state"],
        "succeeded"
    );
    assert_eq!(git::get_current_sha(&fixture.repo).await.unwrap(), head);

    // The orphan is settled with a receipt, not left to refuse later effects.
    let settled = fixture
        .journal
        .operation("orphaned-merge")
        .unwrap()
        .unwrap()
        .outcome
        .expect("orphaned effect was left unsettled")
        .unwrap_err();
    assert_eq!(
        settled.details.unwrap()["integration_receipt"]["result"]["kind"],
        "infrastructure"
    );

    // A queue claim still freezes the location generation.
    let mut stale = attempt_request(
        &fixture,
        &head,
        &head,
        1,
        WorkspaceIntegrationKind::FastForward,
    );
    stale.fence.target_owner["generation"] = serde_json::json!(known + 7);
    let error = fixture
        .backend
        .handle(
            METHOD_WORKSPACE_MERGE,
            serde_json::to_value(attempt_merge_params(&fixture, stale)).unwrap(),
            Vec::new,
        )
        .await
        .unwrap_err();
    assert_eq!(error.details.unwrap()["refusal"], "foreign_owner");
}
