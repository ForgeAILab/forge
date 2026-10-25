//! Interrupted workspace operations and retained result reconciliation.

use super::*;

#[tokio::test]
async fn reviewed_merge_refuses_changed_objects_and_unreviewed_merge_preserves_conflicts() {
    let fixture = Fixture::new().await;
    std::fs::write(fixture.path().join("feature.txt"), "candidate\n").unwrap();
    let candidate = git::commit_all(fixture.path(), "candidate").await.unwrap();
    let base = fixture.prepared.workspace.base_sha.clone();
    let mut params = merge_params(&fixture, "wrong-reviewed-object", &candidate, &base, true);
    params.reviewed_commit_sha = Some(base.clone());
    let result: WorkspaceMergeResult = decode(
        fixture
            .backend
            .handle(
                METHOD_WORKSPACE_MERGE,
                serde_json::to_value(params).unwrap(),
                Vec::new,
            )
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(matches!(
        result.outcome,
        WorkspaceMergeOutcome::ReviewRequired { .. }
    ));
    std::fs::write(fixture.repo.join("target.txt"), "target\n").unwrap();
    let target = git::commit_all(&fixture.repo, "target").await.unwrap();
    let result: WorkspaceMergeResult = decode(
        fixture
            .backend
            .handle(
                METHOD_WORKSPACE_MERGE,
                serde_json::to_value(merge_params(
                    &fixture,
                    "target-moved",
                    &candidate,
                    &base,
                    true,
                ))
                .unwrap(),
                Vec::new,
            )
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(matches!(
        result.outcome,
        WorkspaceMergeOutcome::TargetMoved { .. }
    ));
    let result: WorkspaceMergeResult = decode(
        fixture
            .backend
            .handle(
                METHOD_WORKSPACE_MERGE,
                serde_json::to_value(merge_params(
                    &fixture,
                    "reviewed-divergence",
                    &candidate,
                    &target,
                    true,
                ))
                .unwrap(),
                Vec::new,
            )
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(matches!(
        result.outcome,
        WorkspaceMergeOutcome::ReviewRequired { .. }
    ));
    assert_eq!(git::get_current_sha(&fixture.repo).await.unwrap(), target);
    let result: WorkspaceMergeResult = decode(
        fixture
            .backend
            .handle(
                METHOD_WORKSPACE_MERGE,
                serde_json::to_value(merge_params(
                    &fixture,
                    "unreviewed-divergence",
                    &candidate,
                    &target,
                    false,
                ))
                .unwrap(),
                Vec::new,
            )
            .await
            .unwrap(),
    )
    .unwrap();
    let WorkspaceMergeOutcome::Done {
        after_sha,
        before_sha,
        ..
    } = result.outcome
    else {
        panic!("normal merge commit")
    };
    assert_eq!(before_sha, target);
    assert_ne!(after_sha, candidate);
    assert_eq!(result.diffstat.unwrap().files_changed, 1);

    let fixture = Fixture::new().await;
    std::fs::write(fixture.path().join("feature.txt"), "candidate\n").unwrap();
    let candidate = git::commit_all(fixture.path(), "candidate").await.unwrap();
    std::fs::write(fixture.repo.join("feature.txt"), "target\n").unwrap();
    let target = git::commit_all(&fixture.repo, "target").await.unwrap();
    let result: WorkspaceMergeResult = decode(
        fixture
            .backend
            .handle(
                METHOD_WORKSPACE_MERGE,
                serde_json::to_value(merge_params(
                    &fixture,
                    "unreviewed-conflict",
                    &candidate,
                    &target,
                    false,
                ))
                .unwrap(),
                Vec::new,
            )
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(
        matches!(result.outcome, WorkspaceMergeOutcome::Conflict { conflict_paths, .. } if conflict_paths == ["feature.txt"])
    );
    assert_eq!(git::get_current_sha(&fixture.repo).await.unwrap(), target);
    assert!(git::is_worktree_clean(&fixture.repo).await.unwrap());
}

#[tokio::test]
async fn interrupted_run_is_settled_and_never_replayed_before_ack() {
    let fixture = Fixture::new().await;
    let params = serde_json::to_value(fixture.run(
        "interrupted-run",
        WorkspaceRunPurpose::CiStep,
        "printf twice >> never-run",
    ))
    .unwrap();
    retain_intent(&fixture, METHOD_WORKSPACE_RUN, params.clone());
    let failure = fixture
        .backend
        .handle(METHOD_WORKSPACE_RUN, params.clone(), Vec::new)
        .await
        .unwrap_err();
    assert_eq!(failure.code, DAEMON_UNAVAILABLE);
    assert_eq!(failure.details.unwrap()["interrupted"], true);
    assert!(fixture
        .journal
        .acknowledge(&JournalAckParams {
            entry_id: operation_entry_id("interrupted-run")
        })
        .is_err());
    let result = reconcile(&fixture, "interrupted-run").await;
    assert!(
        matches!(result.outcome, WorkspaceReconcileOutcome::Error { error } if error.code == DAEMON_UNAVAILABLE && error.details.as_ref().unwrap()["interrupted"] == true)
    );
    assert!(fixture
        .journal
        .operation("interrupted-run")
        .unwrap()
        .unwrap()
        .outcome
        .is_some());
    assert_eq!(
        fixture
            .backend
            .handle(METHOD_WORKSPACE_RUN, params, Vec::new)
            .await
            .unwrap_err()
            .code,
        DAEMON_UNAVAILABLE
    );
    assert!(
        fixture
            .journal
            .acknowledge(&JournalAckParams {
                entry_id: result.entry_id
            })
            .unwrap()
            .acknowledged
    );
    assert!(fixture
        .journal
        .operation("interrupted-run")
        .unwrap()
        .is_none());
    assert!(!fixture.path().join("never-run").exists());
}

#[tokio::test]
async fn interrupted_merge_requires_exact_target_proof_and_reconstructs_diffstat() {
    let fixture = Fixture::new().await;
    std::fs::write(fixture.path().join("feature.txt"), "candidate\n").unwrap();
    let candidate = git::commit_all(fixture.path(), "candidate").await.unwrap();
    let params = serde_json::to_value(merge_params(
        &fixture,
        "interrupted-merge",
        &candidate,
        &fixture.prepared.workspace.base_sha,
        true,
    ))
    .unwrap();
    retain_intent(&fixture, METHOD_WORKSPACE_MERGE, params.clone());
    local_git(&fixture.repo, &["merge", "--ff-only", &candidate])
        .await
        .unwrap();
    let result = reconcile(&fixture, "interrupted-merge").await;
    let WorkspaceReconcileOutcome::Result { result } = result.outcome else {
        panic!("proven merge")
    };
    let merged: WorkspaceMergeResult = decode(result.clone()).unwrap();
    assert!(
        matches!(merged.outcome, WorkspaceMergeOutcome::Done { after_sha, .. } if after_sha == candidate)
    );
    let diffstat = merged.diffstat.unwrap();
    assert_eq!(diffstat.files_changed, 1);
    assert_eq!(diffstat.total_additions, 1);
    assert_eq!(
        fixture
            .backend
            .handle(METHOD_WORKSPACE_MERGE, params, Vec::new)
            .await
            .unwrap(),
        result
    );

    let fixture = Fixture::new().await;
    std::fs::write(fixture.path().join("feature.txt"), "candidate\n").unwrap();
    let candidate = git::commit_all(fixture.path(), "candidate").await.unwrap();
    let params = serde_json::to_value(merge_params(
        &fixture,
        "unproven-merge",
        &candidate,
        &fixture.prepared.workspace.base_sha,
        true,
    ))
    .unwrap();
    retain_intent(&fixture, METHOD_WORKSPACE_MERGE, params);
    local_git(&fixture.repo, &["merge", "--ff-only", &candidate])
        .await
        .unwrap();
    local_git(
        &fixture.repo,
        &["commit", "--allow-empty", "-m", "later-target"],
    )
    .await
    .unwrap();
    let target = git::get_current_sha(&fixture.repo).await.unwrap();
    let result = reconcile(&fixture, "unproven-merge").await;
    assert!(
        matches!(result.outcome, WorkspaceReconcileOutcome::Error { error } if error.code == DAEMON_UNAVAILABLE)
    );
    assert!(fixture
        .journal
        .operation("unproven-merge")
        .unwrap()
        .unwrap()
        .outcome
        .is_some());
    assert_eq!(git::get_current_sha(&fixture.repo).await.unwrap(), target);
}

#[tokio::test]
async fn reconciliation_returns_retained_full_run_result_and_rejects_another_handle() {
    let fixture = Fixture::new().await;
    let params = serde_json::to_value(fixture.run(
        "finished-run",
        WorkspaceRunPurpose::CiStep,
        "printf once >> count; printf retained",
    ))
    .unwrap();
    let output = fixture
        .backend
        .handle(METHOD_WORKSPACE_RUN, params, Vec::new)
        .await
        .unwrap();
    let result = reconcile(&fixture, "finished-run").await;
    assert!(
        matches!(result.outcome, WorkspaceReconcileOutcome::Result { result } if result == output)
    );
    assert_eq!(
        std::fs::read_to_string(fixture.path().join("count")).unwrap(),
        "once"
    );
    let scratch = owner(
        &fixture,
        "other-handle",
        &fixture.prepared.workspace.workspace_handle,
        WorkspaceOwnerOperation::ReviewCheckout {
            commit_sha: fixture.prepared.workspace.base_sha.clone(),
            environment: ProjectEnvironment::default(),
            prepare: true,
        },
    )
    .await;
    let WorkspaceOwnerOperationOutcome::ReviewCheckout { workspace_handle } = scratch.outcome
    else {
        panic!("scratch")
    };
    let mut workspace = fixture.reference();
    workspace.workspace_handle = workspace_handle;
    assert_eq!(
        fixture
            .backend
            .handle(
                METHOD_WORKSPACE_DESCRIBE,
                serde_json::to_value(WorkspaceReconcileParams {
                    integration: api_types::WorkspaceIntegrationBinding::TaskStep,
                    workspace,
                    operation: WorkspaceReconcileOperation::Reconcile,
                    operation_id: "finished-run".into()
                })
                .unwrap(),
                Vec::new
            )
            .await
            .unwrap_err()
            .code,
        WRONG_OWNER
    );
}
