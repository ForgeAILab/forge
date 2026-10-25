//! Workspace owner operations, detached handles, and acknowledgement cleanup.

use super::*;

#[cfg(unix)]
#[tokio::test]
async fn owner_operations_reject_file_and_asset_path_escape() {
    let fixture = Fixture::new().await;
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(outside.path().join("secret"), "outside").unwrap();
    std::os::unix::fs::symlink(outside.path(), fixture.path().join("escape")).unwrap();
    let handle = &fixture.prepared.workspace.workspace_handle;
    for (index, operation) in [WorkspaceOwnerOperation::MaterializeAssets {
        environment: ProjectEnvironment {
            recheck_interval_seconds: 600,
            env: BTreeMap::new(),
            checks: Vec::new(),
            assets: vec![api_types::EnvironmentAsset {
                source: outside.path().join("secret").to_string_lossy().into_owned(),
                target: "asset".into(),
            }],
        },
    }]
    .into_iter()
    .enumerate()
    {
        let operation_id = format!("escape-operation-{index}");
        let params = owner_params(&fixture, &operation_id, handle, operation).await;
        let failure = fixture
            .backend
            .handle(
                METHOD_WORKSPACE_RESET,
                serde_json::to_value(params).unwrap(),
                Vec::new,
            )
            .await
            .unwrap_err();
        assert_eq!(failure.code, OUTSIDE_WORKSPACE_ROOT);
        // Each rejected operation has an acknowledgement identity.
        assert!(failure.details.unwrap()["entry_id"].as_str().is_some());
        fixture
            .journal
            .acknowledge(&JournalAckParams {
                entry_id: operation_entry_id(&operation_id),
            })
            .unwrap();
    }
    let failure = fixture
        .backend
        .handle(
            METHOD_WORKSPACE_READ,
            serde_json::to_value(WorkspaceInspectParams::Files {
                workspace: fixture.reference(),
                path: "escape".into(),
                repository: false,
                max_entries: 2,
                max_bytes: 100,
            })
            .unwrap(),
            Vec::new,
        )
        .await
        .unwrap_err();
    assert_eq!(failure.code, OUTSIDE_WORKSPACE_ROOT);
    assert_eq!(
        std::fs::read_to_string(outside.path().join("secret")).unwrap(),
        "outside"
    );
}

#[tokio::test]
async fn detached_review_handles_are_fenced_replayable_and_reclaimed() {
    let fixture = Fixture::new().await;
    let handle = &fixture.prepared.workspace.workspace_handle;
    std::fs::write(fixture.path().join("feature.txt"), "candidate\n").unwrap();
    let candidate = git::commit_all(fixture.path(), "candidate").await.unwrap();
    std::fs::write(fixture.path().join("feature.txt"), "dirty").unwrap();
    std::fs::write(fixture.path().join("build-output"), "artifact").unwrap();
    let asset = fixture.dir.path().join("seed");
    std::fs::write(&asset, "asset").unwrap();
    let environment = ProjectEnvironment {
        recheck_interval_seconds: 600,
        env: BTreeMap::new(),
        checks: Vec::new(),
        assets: vec![api_types::EnvironmentAsset {
            source: asset.to_string_lossy().into_owned(),
            target: "assets/seed".into(),
        }],
    };
    let params = owner_params(
        &fixture,
        "review-checkout",
        handle,
        WorkspaceOwnerOperation::ReviewCheckout {
            commit_sha: candidate.clone(),
            environment: environment.clone(),
            prepare: true,
        },
    )
    .await;
    let request = serde_json::to_value(params).unwrap();
    let result = fixture
        .backend
        .handle(METHOD_WORKSPACE_RESET, request.clone(), Vec::new)
        .await
        .unwrap();
    let result: WorkspaceOwnerOperationResult = decode(result.clone()).unwrap();
    let WorkspaceOwnerOperationOutcome::ReviewCheckout {
        workspace_handle: scratch,
    } = result.outcome
    else {
        panic!("detached checkout")
    };
    assert_ne!(scratch, *handle);
    let scratch_path = fixture.backend.path_for_handle(&scratch).unwrap();
    assert_eq!(
        git::get_current_sha(&scratch_path).await.unwrap(),
        candidate
    );
    assert_eq!(
        std::fs::read_to_string(scratch_path.join("feature.txt")).unwrap(),
        "candidate\n"
    );
    assert!(!scratch_path.join("build-output").exists());
    assert_eq!(
        std::fs::read_to_string(scratch_path.join("assets/seed")).unwrap(),
        "asset"
    );
    let replay: WorkspaceOwnerOperationResult = decode(
        fixture
            .backend
            .handle(METHOD_WORKSPACE_RESET, request, Vec::new)
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(
        matches!(replay.outcome, WorkspaceOwnerOperationOutcome::ReviewCheckout { workspace_handle } if workspace_handle == scratch)
    );
    let mut scratch_ref = fixture.reference();
    scratch_ref.workspace_handle = scratch.clone();
    scratch_ref.placement_id = "different-placement".into();
    assert_eq!(
        fixture
            .backend
            .handle(
                METHOD_WORKSPACE_DESCRIBE,
                serde_json::to_value(WorkspaceDescribeParams {
                    workspace: scratch_ref
                })
                .unwrap(),
                Vec::new
            )
            .await
            .unwrap_err()
            .code,
        WRONG_OWNER
    );
    owner(
        &fixture,
        "restore-candidate",
        handle,
        WorkspaceOwnerOperation::RestoreCandidate {
            commit_sha: candidate.clone(),
        },
    )
    .await;
    assert_eq!(
        std::fs::read_to_string(fixture.path().join("feature.txt")).unwrap(),
        "candidate\n"
    );
    assert!(!fixture.path().join("build-output").exists());
    assert_eq!(
        fixture
            .backend
            .workspace(&fixture.reference(), false)
            .unwrap()
            .generation,
        1
    );
    owner(
        &fixture,
        "release-review",
        &scratch,
        WorkspaceOwnerOperation::ReleaseReviewCheckout,
    )
    .await;
    assert!(!scratch_path.exists());
    let orphan = owner(
        &fixture,
        "orphan-review",
        handle,
        WorkspaceOwnerOperation::ReviewCheckout {
            commit_sha: candidate.clone(),
            environment,
            prepare: true,
        },
    )
    .await;
    let WorkspaceOwnerOperationOutcome::ReviewCheckout {
        workspace_handle: orphan,
    } = orphan.outcome
    else {
        panic!("orphan checkout")
    };
    let orphan_path = fixture.backend.path_for_handle(&orphan).unwrap();
    let restarted = DaemonWorkspaceBackend::new(
        fixture.dir.path().to_owned(),
        "daemon-1".into(),
        fixture.backend.policy.clone(),
        Arc::clone(&fixture.journal),
    )
    .unwrap();
    let cleanup = WorkspaceCleanupParams {
        fence: fence("cleanup-with-reviews", 1, &candidate),
        workspace_handle: handle.clone(),
    };
    let result = restarted
        .handle(
            METHOD_WORKSPACE_CLEANUP,
            serde_json::to_value(cleanup).unwrap(),
            Vec::new,
        )
        .await
        .unwrap();
    assert!(!orphan_path.exists());
    assert!(!fixture.path().exists());
    assert!(restarted
        .state
        .lock()
        .unwrap()
        .handles
        .contains_key(&orphan));
    restarted
        .acknowledge_journal(&JournalAckParams {
            entry_id: result["entry_id"].as_str().unwrap().into(),
        })
        .await
        .unwrap();
    assert!(!restarted
        .state
        .lock()
        .unwrap()
        .handles
        .contains_key(&orphan));
}

#[tokio::test]
async fn same_generation_owner_operations_reject_future_generations() {
    let fixture = Fixture::new().await;
    let mut params = owner_params(
        &fixture,
        "future-owner-operation",
        &fixture.prepared.workspace.workspace_handle,
        WorkspaceOwnerOperation::RestoreCandidate {
            commit_sha: fixture.prepared.workspace.base_sha.clone(),
        },
    )
    .await;
    params.fence.generation += 1;
    assert_eq!(
        fixture
            .backend
            .handle(
                METHOD_WORKSPACE_RESET,
                serde_json::to_value(params).unwrap(),
                Vec::new
            )
            .await
            .unwrap_err()
            .code,
        STALE_GENERATION
    );
}

#[tokio::test]
async fn root_cleanup_preserves_an_active_detached_checkout_and_reset_fences_old_handles() {
    let fixture = Fixture::new().await;
    let handle = &fixture.prepared.workspace.workspace_handle;
    let base = &fixture.prepared.workspace.base_sha;
    let result = owner(
        &fixture,
        "active-scratch",
        handle,
        WorkspaceOwnerOperation::ReviewCheckout {
            commit_sha: base.clone(),
            environment: ProjectEnvironment::default(),
            prepare: true,
        },
    )
    .await;
    let WorkspaceOwnerOperationOutcome::ReviewCheckout {
        workspace_handle: scratch,
    } = result.outcome
    else {
        panic!("scratch")
    };
    let path = fixture.backend.path_for_handle(&scratch).unwrap();
    fixture
        .backend
        .register_execution("scratch-execution", &path)
        .await
        .unwrap();
    let cleanup = WorkspaceCleanupParams {
        fence: fence("active-scratch-cleanup", 1, base),
        workspace_handle: handle.clone(),
    };
    assert!(fixture
        .backend
        .handle(
            METHOD_WORKSPACE_CLEANUP,
            serde_json::to_value(cleanup).unwrap(),
            || vec!["scratch-execution".into()]
        )
        .await
        .unwrap_err()
        .message
        .contains("active execution"));
    assert!(fixture.path().exists());
    assert!(path.exists());
    let reset = WorkspaceResetParams {
        fence: fence("reset-with-scratch", 2, base),
        workspace_handle: handle.clone(),
        base_ref: "main".into(),
        branch: "task/test".into(),
    };
    let result: WorkspaceResetResult = decode(
        fixture
            .backend
            .handle(
                METHOD_WORKSPACE_RESET,
                serde_json::to_value(reset).unwrap(),
                Vec::new,
            )
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(result.workspace.generation, 2);
    assert!(!path.exists());
    let mut workspace = fixture.reference();
    workspace.workspace_handle = scratch;
    assert_eq!(
        fixture
            .backend
            .handle(
                METHOD_WORKSPACE_DESCRIBE,
                serde_json::to_value(WorkspaceDescribeParams { workspace }).unwrap(),
                Vec::new
            )
            .await
            .unwrap_err()
            .code,
        STALE_GENERATION
    );
}

#[tokio::test]
async fn owner_rebase_preserves_clean_dirty_and_conflict_outcomes() {
    let fixture = Fixture::new().await;
    let handle = &fixture.prepared.workspace.workspace_handle;
    std::fs::write(fixture.path().join("feature.txt"), "candidate\n").unwrap();
    git::commit_all(fixture.path(), "candidate").await.unwrap();
    std::fs::write(fixture.repo.join("target.txt"), "target\n").unwrap();
    let target = git::commit_all(&fixture.repo, "target").await.unwrap();
    let result = owner(
        &fixture,
        "owner-rebase",
        handle,
        WorkspaceOwnerOperation::RebaseTarget {
            target_branch: "main".into(),
            handoff_conflicts: false,
        },
    )
    .await;
    assert!(matches!(
        result.outcome,
        WorkspaceOwnerOperationOutcome::Rebased
    ));
    let head = git::get_current_sha(fixture.path()).await.unwrap();
    assert_eq!(
        local_git(fixture.path(), &["merge-base", &target, "HEAD"])
            .await
            .unwrap(),
        target
    );
    assert_eq!(
        fixture
            .backend
            .workspace(&fixture.reference(), false)
            .unwrap()
            .generation,
        1
    );
    std::fs::write(fixture.path().join("feature.txt"), "dirty\n").unwrap();
    let result = owner(
        &fixture,
        "dirty-rebase",
        handle,
        WorkspaceOwnerOperation::RebaseTarget {
            target_branch: "main".into(),
            handoff_conflicts: false,
        },
    )
    .await;
    assert!(
        matches!(result.outcome, WorkspaceOwnerOperationOutcome::Dirty { files } if files == ["feature.txt"])
    );
    assert_eq!(git::get_current_sha(fixture.path()).await.unwrap(), head);

    let fixture = Fixture::new().await;
    let handle = &fixture.prepared.workspace.workspace_handle;
    std::fs::write(fixture.path().join("feature.txt"), "candidate\n").unwrap();
    let candidate = git::commit_all(fixture.path(), "candidate").await.unwrap();
    std::fs::write(fixture.repo.join("feature.txt"), "target\n").unwrap();
    git::commit_all(&fixture.repo, "target").await.unwrap();
    let result = owner(
        &fixture,
        "abort-owner-rebase",
        handle,
        WorkspaceOwnerOperation::RebaseTarget {
            target_branch: "main".into(),
            handoff_conflicts: false,
        },
    )
    .await;
    assert!(
        matches!(result.outcome, WorkspaceOwnerOperationOutcome::Conflict { conflict_paths, .. } if conflict_paths.is_empty())
    );
    assert_eq!(
        git::get_current_sha(fixture.path()).await.unwrap(),
        candidate
    );
    assert!(!git::detect_rebase_in_progress(fixture.path())
        .await
        .unwrap());
    let result = owner(
        &fixture,
        "handoff-owner-rebase",
        handle,
        WorkspaceOwnerOperation::RebaseTarget {
            target_branch: "main".into(),
            handoff_conflicts: true,
        },
    )
    .await;
    assert!(
        matches!(result.outcome, WorkspaceOwnerOperationOutcome::Conflict { conflict_paths, .. } if conflict_paths == ["feature.txt"])
    );
    assert!(!git::detect_rebase_in_progress(fixture.path())
        .await
        .unwrap());
}

/// An owner restarted inside `git rebase` resumes it as a fresh rebase would
/// end: abort without handoff, continue and hand off with it.
#[tokio::test]
async fn owner_rebase_resumes_an_interrupted_rebase_as_the_fresh_path_ends() {
    for handoff_conflicts in [false, true] {
        let fixture = Fixture::new().await;
        let handle = &fixture.prepared.workspace.workspace_handle;
        std::fs::write(fixture.path().join("feature.txt"), "candidate\n").unwrap();
        let candidate = git::commit_all(fixture.path(), "candidate").await.unwrap();
        std::fs::write(fixture.repo.join("feature.txt"), "target\n").unwrap();
        let target = git::commit_all(&fixture.repo, "target").await.unwrap();
        assert!(git::rebase(fixture.path(), "main").await.is_err());
        assert!(git::detect_rebase_in_progress(fixture.path())
            .await
            .unwrap());
        let result = owner(
            &fixture,
            "resumed-owner-rebase",
            handle,
            WorkspaceOwnerOperation::RebaseTarget {
                target_branch: "main".into(),
                handoff_conflicts,
            },
        )
        .await;
        assert!(!git::detect_rebase_in_progress(fixture.path())
            .await
            .unwrap());
        if handoff_conflicts {
            assert!(
                matches!(&result.outcome, WorkspaceOwnerOperationOutcome::Conflict { conflict_paths, .. } if conflict_paths == &["feature.txt"]),
                "{:?}",
                result.outcome
            );
            assert_eq!(
                local_git(fixture.path(), &["merge-base", &target, "HEAD"])
                    .await
                    .unwrap(),
                target
            );
        } else {
            assert!(
                matches!(&result.outcome, WorkspaceOwnerOperationOutcome::Conflict { conflict_paths, .. } if conflict_paths.is_empty()),
                "{:?}",
                result.outcome
            );
            assert_eq!(
                git::get_current_sha(fixture.path()).await.unwrap(),
                candidate
            );
        }
    }
}

#[tokio::test]
async fn late_review_release_ack_prunes_only_its_retired_handle() {
    let fixture = Fixture::new().await;
    let main = &fixture.prepared.workspace.workspace_handle;
    let review = owner(
        &fixture,
        "late-review",
        main,
        WorkspaceOwnerOperation::ReviewCheckout {
            commit_sha: fixture.prepared.workspace.base_sha.clone(),
            environment: ProjectEnvironment::default(),
            prepare: true,
        },
    )
    .await;
    let WorkspaceOwnerOperationOutcome::ReviewCheckout {
        workspace_handle: review,
    } = review.outcome
    else {
        panic!("review handle")
    };
    let released = owner(
        &fixture,
        "late-release",
        &review,
        WorkspaceOwnerOperation::ReleaseReviewCheckout,
    )
    .await;
    let cleanup = fixture
        .backend
        .handle(
            METHOD_WORKSPACE_CLEANUP,
            serde_json::to_value(WorkspaceCleanupParams {
                fence: fence("pending-cleanup", 1, &fixture.prepared.workspace.base_sha),
                workspace_handle: main.clone(),
            })
            .unwrap(),
            Vec::new,
        )
        .await
        .unwrap();
    fixture
        .backend
        .acknowledge_journal(&JournalAckParams {
            entry_id: released.entry_id,
        })
        .await
        .unwrap();
    let state = fixture
        .journal
        .load_workspace_state::<WorkspaceRegistry>()
        .unwrap();
    assert!(!state.handles.contains_key(&review));
    assert!(
        state.handles.contains_key(main),
        "cleanup is still unacknowledged"
    );
    let describe = fixture
        .backend
        .handle(
            METHOD_WORKSPACE_DESCRIBE,
            serde_json::to_value(WorkspaceDescribeParams {
                workspace: fixture.reference(),
            })
            .unwrap(),
            Vec::new,
        )
        .await
        .unwrap();
    assert_eq!(describe["exists"], false);
    fixture
        .backend
        .acknowledge_journal(&JournalAckParams {
            entry_id: cleanup["entry_id"].as_str().unwrap().into(),
        })
        .await
        .unwrap();
    assert!(!fixture
        .backend
        .state
        .lock()
        .unwrap()
        .handles
        .contains_key(main));
}

#[tokio::test]
async fn late_reset_ack_prunes_only_the_review_handles_it_retired() {
    let fixture = Fixture::new().await;
    let main = &fixture.prepared.workspace.workspace_handle;
    let review = owner(
        &fixture,
        "pre-reset-review",
        main,
        WorkspaceOwnerOperation::ReviewCheckout {
            commit_sha: fixture.prepared.workspace.base_sha.clone(),
            environment: ProjectEnvironment::default(),
            prepare: true,
        },
    )
    .await;
    let WorkspaceOwnerOperationOutcome::ReviewCheckout {
        workspace_handle: review,
    } = review.outcome
    else {
        panic!("review handle")
    };
    let reset = fixture
        .backend
        .handle(
            METHOD_WORKSPACE_RESET,
            serde_json::to_value(WorkspaceResetParams {
                fence: fence("pending-reset", 2, &fixture.prepared.workspace.base_sha),
                workspace_handle: main.clone(),
                base_ref: "main".into(),
                branch: "task/test".into(),
            })
            .unwrap(),
            Vec::new,
        )
        .await
        .unwrap();
    let cleanup = fixture
        .backend
        .handle(
            METHOD_WORKSPACE_CLEANUP,
            serde_json::to_value(WorkspaceCleanupParams {
                fence: fence(
                    "post-reset-cleanup",
                    2,
                    &fixture.prepared.workspace.base_sha,
                ),
                workspace_handle: main.clone(),
            })
            .unwrap(),
            Vec::new,
        )
        .await
        .unwrap();
    // Retiring-operation ownership survives a registry reload.
    let backend = DaemonWorkspaceBackend::new(
        fixture.dir.path().to_owned(),
        "daemon-1".into(),
        crate::daemon_config::DaemonConfig::default().run_policy(),
        fixture.journal.clone(),
    )
    .unwrap();
    backend
        .acknowledge_journal(&JournalAckParams {
            entry_id: reset["entry_id"].as_str().unwrap().into(),
        })
        .await
        .unwrap();
    let state = fixture
        .journal
        .load_workspace_state::<WorkspaceRegistry>()
        .unwrap();
    assert!(!state.handles.contains_key(&review));
    assert!(state.handles.contains_key(main));
    backend
        .acknowledge_journal(&JournalAckParams {
            entry_id: cleanup["entry_id"].as_str().unwrap().into(),
        })
        .await
        .unwrap();
    assert!(!backend.state.lock().unwrap().handles.contains_key(main));
}
