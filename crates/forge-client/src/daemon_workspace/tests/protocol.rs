use super::*;

async fn owner_params(
    fixture: &Fixture,
    id: &str,
    handle: &str,
    operation: WorkspaceOwnerOperation,
) -> WorkspaceOwnerOperationParams {
    let owned = fixture.backend.state.lock().unwrap().handles[handle].clone();
    WorkspaceOwnerOperationParams {
        fence: fence(
            id,
            owned.generation,
            &git::get_current_sha(&owned.path).await.unwrap(),
        ),
        workspace_handle: handle.into(),
        operation,
    }
}

async fn owner(
    fixture: &Fixture,
    id: &str,
    handle: &str,
    operation: WorkspaceOwnerOperation,
) -> WorkspaceOwnerOperationResult {
    serde_json::from_value(
        fixture
            .backend
            .handle(
                METHOD_WORKSPACE_RESET,
                serde_json::to_value(owner_params(fixture, id, handle, operation).await).unwrap(),
                Vec::new,
            )
            .await
            .unwrap(),
    )
    .unwrap()
}

async fn inspect(
    fixture: &Fixture,
    query: WorkspaceGitQuery,
    optional: bool,
    limit: u64,
) -> CommandResult<WorkspaceInspectResult> {
    fixture
        .backend
        .handle(
            METHOD_WORKSPACE_READ,
            serde_json::to_value(WorkspaceInspectParams::Git {
                workspace: fixture.reference(),
                query,
                optional,
                limit,
            })
            .unwrap(),
            Vec::new,
        )
        .await
        .and_then(decode)
}

pub(super) fn merge_params(
    fixture: &Fixture,
    id: &str,
    candidate: &str,
    target: &str,
    reviewed: bool,
) -> WorkspaceReviewedMergeParams {
    WorkspaceReviewedMergeParams {
        reviewed_commit_sha: reviewed.then(|| candidate.into()),
        merge: WorkspaceMergeParams {
            fence: fence(id, 1, candidate),
            workspace_handle: fixture.prepared.workspace.workspace_handle.clone(),
            repo_location_id: "location-1".into(),
            target_branch: "main".into(),
            expected_target_sha: target.into(),
            handed_off_paths: Vec::new(),
        },
    }
}

pub(super) fn retain_intent(fixture: &Fixture, method: &str, request: Value) {
    let fence: WorkspaceMutationFence = decode(request.clone()).unwrap();
    fixture
        .journal
        .retain_entry(&JournalEntry::Operation {
            operation: JournalOperation {
                entry_id: operation_entry_id(&fence.operation_id),
                workspace_handle: request["workspace_handle"].as_str().map(str::to_owned),
                fence,
                method: method.into(),
                request,
                outcome: None,
                acknowledged: false,
                effect_started: true,
            },
        })
        .unwrap();
}

async fn reconcile(fixture: &Fixture, id: &str) -> WorkspaceReconcileResult {
    serde_json::from_value(
        fixture
            .backend
            .handle(
                METHOD_WORKSPACE_DESCRIBE,
                serde_json::to_value(WorkspaceReconcileParams {
                    integration: api_types::WorkspaceIntegrationBinding::TaskStep,
                    workspace: fixture.reference(),
                    operation: WorkspaceReconcileOperation::Reconcile,
                    operation_id: id.into(),
                })
                .unwrap(),
                Vec::new,
            )
            .await
            .unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn inspection_preserves_exact_git_evidence_and_optional_absence() {
    let fixture = Fixture::new().await;
    std::fs::write(fixture.path().join("feature.txt"), "candidate\n").unwrap();
    let candidate = git::commit_all(fixture.path(), "candidate").await.unwrap();
    for (query, expected) in [
        (WorkspaceGitQuery::Head, format!("{candidate}\n")),
        (
            WorkspaceGitQuery::ResolveRef {
                reference: "HEAD".into(),
            },
            format!("{candidate}\n"),
        ),
        (
            WorkspaceGitQuery::MergeBase {
                base_ref: "main".into(),
                head_ref: "HEAD".into(),
            },
            format!("{}\n", fixture.prepared.workspace.base_sha),
        ),
        (
            WorkspaceGitQuery::CandidatePaths {
                base_sha: fixture.prepared.workspace.base_sha.clone(),
                commit_sha: candidate,
            },
            "feature.txt\0".into(),
        ),
        (WorkspaceGitQuery::TrackedChanges, String::new()),
        (WorkspaceGitQuery::StatusPorcelain, String::new()),
        (
            WorkspaceGitQuery::BranchExists {
                branch: "main".into(),
            },
            format!("{}\n", fixture.prepared.workspace.base_sha),
        ),
        (
            WorkspaceGitQuery::TargetHead {
                branch: "main".into(),
            },
            format!("{}\n", fixture.prepared.workspace.base_sha),
        ),
        (WorkspaceGitQuery::RebaseInProgress, "false".into()),
    ] {
        let WorkspaceInspectResult::Git { output } =
            inspect(&fixture, query, false, 4096).await.unwrap()
        else {
            panic!("Git evidence")
        };
        assert_eq!(output.as_deref(), Some(expected.as_str()));
    }
    let WorkspaceInspectResult::Git { output } = inspect(
        &fixture,
        WorkspaceGitQuery::ResolveRef {
            reference: "missing".into(),
        },
        true,
        4096,
    )
    .await
    .unwrap() else {
        panic!("optional evidence")
    };
    assert!(output.is_none());
    assert!(inspect(&fixture, WorkspaceGitQuery::Head, true, 2)
        .await
        .unwrap_err()
        .message
        .contains("size budget"));
}

#[tokio::test]
async fn review_diff_uses_three_dot_fallback_and_utf8_truncation_marker() {
    let fixture = Fixture::new().await;
    std::fs::write(fixture.path().join("feature.txt"), "候補\n".repeat(100)).unwrap();
    git::commit_all(fixture.path(), "candidate").await.unwrap();
    let request = |branch: &str, max_bytes| {
        serde_json::to_value(WorkspaceReviewDiffParams {
            workspace: fixture.reference(),
            operation: WorkspaceReviewDiffOperation::Review,
            default_branch: branch.into(),
            max_bytes,
        })
        .unwrap()
    };
    let result: WorkspaceReviewDiffResult = decode(
        fixture
            .backend
            .handle(METHOD_WORKSPACE_DIFF, request("main", 64 * 1024), Vec::new)
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(result.diff.contains("+候補"));
    let result: WorkspaceReviewDiffResult = decode(
        fixture
            .backend
            .handle(METHOD_WORKSPACE_DIFF, request("main", 123), Vec::new)
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(result.diff.ends_with("[truncated]"));
    std::fs::write(fixture.path().join("README.md"), "fallback\n").unwrap();
    let result: WorkspaceReviewDiffResult = decode(
        fixture
            .backend
            .handle(METHOD_WORKSPACE_DIFF, request("missing", 4096), Vec::new)
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(result.diff.contains("+fallback"));
    assert!(!result.diff.contains("+候補"));
}

#[tokio::test]
async fn file_inspection_is_confined_and_bounded() {
    let fixture = Fixture::new().await;
    std::fs::create_dir(fixture.path().join("docs")).unwrap();
    std::fs::write(fixture.path().join("docs/note.md"), "workspace note\n").unwrap();
    std::fs::create_dir(fixture.repo.join("docs")).unwrap();
    std::fs::write(fixture.repo.join("docs/note.md"), "repository note\n").unwrap();
    let read_files = |path: &str, repository, max_entries, max_bytes| {
        serde_json::to_value(WorkspaceInspectParams::Files {
            workspace: fixture.reference(),
            path: path.into(),
            repository,
            max_entries,
            max_bytes,
        })
        .unwrap()
    };
    let WorkspaceInspectResult::Files { files: captured } = decode(
        fixture
            .backend
            .handle(
                METHOD_WORKSPACE_READ,
                read_files("docs", false, 2, 100),
                Vec::new,
            )
            .await
            .unwrap(),
    )
    .unwrap() else {
        panic!("workspace files")
    };
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].path, "docs/note.md");
    assert_eq!(captured[0].bytes, b"workspace note\n");
    assert!(fixture
        .backend
        .handle(
            METHOD_WORKSPACE_READ,
            read_files("docs", false, 0, 100),
            Vec::new
        )
        .await
        .is_err());
    assert!(fixture
        .backend
        .handle(
            METHOD_WORKSPACE_READ,
            read_files("docs", false, 2, 2),
            Vec::new,
        )
        .await
        .is_err());
    assert_eq!(
        fixture
            .backend
            .handle(
                METHOD_WORKSPACE_READ,
                read_files("../primary", false, 2, 100),
                Vec::new
            )
            .await
            .unwrap_err()
            .code,
        OUTSIDE_WORKSPACE_ROOT
    );
    let WorkspaceInspectResult::Files { files: captured } = decode(
        fixture
            .backend
            .handle(
                METHOD_WORKSPACE_READ,
                read_files("docs", true, 2, 100),
                Vec::new,
            )
            .await
            .unwrap(),
    )
    .unwrap() else {
        panic!("repository files")
    };
    assert_eq!(captured[0].bytes, b"repository note\n");
    let paths: WorkspaceInspectResult = decode(
        fixture
            .backend
            .handle(
                METHOD_WORKSPACE_READ,
                serde_json::to_value(WorkspaceInspectParams::Paths {
                    workspace: fixture.reference(),
                })
                .unwrap(),
                Vec::new,
            )
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(
        matches!(paths, WorkspaceInspectResult::Paths { workspace_path, repo_path }
        if Path::new(&workspace_path) == fixture.path() && Path::new(&repo_path) == fixture.repo.canonicalize().unwrap())
    );
}

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
async fn ci_unbounded_sentinels_do_not_remove_other_purpose_budgets() {
    let fixture = Fixture::new().await;
    let backend = DaemonWorkspaceBackend::new(
        fixture.dir.path().to_owned(),
        "daemon-1".into(),
        WorkspaceRunPolicy {
            allowed_purposes: vec![
                WorkspaceRunPurpose::CiStep,
                WorkspaceRunPurpose::Hook,
                WorkspaceRunPurpose::EnvironmentSetup,
            ],
        },
        Arc::clone(&fixture.journal),
    )
    .unwrap();
    for (index, (timeout_secs, max_output_bytes)) in
        [(0, 4096), (2, u64::MAX)].into_iter().enumerate()
    {
        let mut params = fixture.run(
            &format!("hook-budget-{index}"),
            WorkspaceRunPurpose::Hook,
            "printf unexpected > forbidden-hook",
        );
        params.timeout_secs = timeout_secs;
        params.max_output_bytes = max_output_bytes;
        assert_eq!(
            backend
                .handle(
                    METHOD_WORKSPACE_RUN,
                    serde_json::to_value(params).unwrap(),
                    Vec::new
                )
                .await
                .unwrap_err()
                .code,
            INVALID_INPUT
        );
    }
    assert!(!fixture.path().join("forbidden-hook").exists());
}

#[tokio::test]
async fn ci_output_is_bounded_redacted_and_replayed_only_before_ack() {
    let fixture = Fixture::new().await;
    let journal = Arc::new(DaemonJournal::with_limits(fixture.dir.path(), 1024, 8192));
    let backend = DaemonWorkspaceBackend::new(
        fixture.dir.path().to_owned(),
        "daemon-1".into(),
        fixture.backend.policy.clone(),
        Arc::clone(&journal),
    )
    .unwrap();
    let mut params = fixture.run("unbounded-ci", WorkspaceRunPurpose::CiStep, "head -c 1100000 /dev/zero | tr '\\000' x; printf '%s' \"$FORGE_TEST_SECRET\"; printf stderr >&2");
    params.timeout_secs = 0;
    params.max_output_bytes = u64::MAX;
    params.env = vec![("FORGE_TEST_SECRET".into(), "secret-value".into())];
    let request = serde_json::to_value(params).unwrap();
    let result = backend
        .handle(METHOD_WORKSPACE_RUN, request.clone(), Vec::new)
        .await
        .unwrap();
    let stored_bytes: u64 = std::fs::read_dir(journal.directory())
        .unwrap()
        .map(|entry| entry.unwrap().metadata().unwrap().len())
        .sum();
    assert!(
        stored_bytes <= 8192,
        "receipt and workspace registry share the byte cap: {stored_bytes}, files: {:?}",
        std::fs::read_dir(journal.directory())
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                (entry.file_name(), entry.metadata().unwrap().len())
            })
            .collect::<Vec<_>>()
    );
    let output: WorkspaceRunResult = decode(result.clone()).unwrap();
    assert_eq!(output.exit_code, Some(0));
    assert!(output.stdout.len() <= crate::daemon_persistence::MAX_CI_LOG_BYTES);
    assert!(output
        .stdout
        .starts_with(crate::daemon_persistence::CI_LOG_TRUNCATION_MARKER));
    assert!(!output.stdout.contains("secret-value"));
    assert_eq!(output.stderr, "stderr");
    assert!(!output.timed_out && output.stdout_truncated && !output.stderr_truncated);
    let restarted = DaemonWorkspaceBackend::new(
        fixture.dir.path().to_owned(),
        "daemon-1".into(),
        backend.policy.clone(),
        Arc::clone(&journal),
    )
    .unwrap();
    assert_eq!(
        restarted
            .handle(METHOD_WORKSPACE_RUN, request, Vec::new)
            .await
            .unwrap(),
        result
    );
    journal
        .acknowledge(&JournalAckParams {
            entry_id: output.entry_id,
        })
        .unwrap();
    assert!(journal.operation("unbounded-ci").unwrap().is_none());
}

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

pub(super) fn attempt_request(
    fixture: &Fixture,
    head: &str,
    target: &str,
    generation: i64,
    kind: WorkspaceIntegrationKind,
) -> WorkspaceIntegrationRequest {
    let location = fixture.backend.state.lock().unwrap().locations["location-1"].clone();
    WorkspaceIntegrationRequest {
        fence: IntegrationOwnerFence {
            queue_id: "queue-1".into(),
            attempt_id: "attempt-1".into(),
            generation,
            lease_owner: "queue-worker".into(),
            target_owner: serde_json::json!({"location_id":"location-1","owner_kind":"daemon","daemon_id":"daemon-1","runtime_id":"runtime-1","generation":location.version}),
        },
        kind,
        witness: serde_json::json!({"workspace":{"workspace_id":"workspace-1","placement_id":"placement-1","generation":1,"handle":fixture.prepared.workspace.workspace_handle,"owner":{"kind":"daemon","daemon_id":"daemon-1","runtime_id":"runtime-1"}},"target_branch":"main","expected_head_sha":head,"expected_target_sha":target,"handoff_conflicts":false}),
    }
}

pub(super) fn attempt_merge_params(
    fixture: &Fixture,
    request: WorkspaceIntegrationRequest,
) -> WorkspaceReviewedMergeParams {
    let mut params = merge_params(
        fixture,
        &request.operation_id(),
        request.witness["expected_head_sha"].as_str().unwrap(),
        request.witness["expected_target_sha"].as_str().unwrap(),
        true,
    );
    params.merge.fence.integration = WorkspaceIntegrationBinding::Attempt { request };
    params
}

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

fn task_step_merge_params(
    fixture: &Fixture,
    id: &str,
    head: &str,
    target: &str,
    step_attempts: i64,
    location_version: i64,
) -> WorkspaceReviewedMergeParams {
    let mut request = attempt_request(
        fixture,
        head,
        target,
        step_attempts,
        WorkspaceIntegrationKind::Merge,
    );
    request.fence.lease_owner = "task-step:step-1".into();
    request.fence.target_owner["generation"] = serde_json::json!(location_version);
    let mut params = merge_params(fixture, id, head, target, true);
    params.merge.fence.integration = WorkspaceIntegrationBinding::TaskStepEffect { request };
    params
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

/// Plan 3.4 F: `workspace.run` in a daemon workspace is handed this
/// machine's shared compiler cache, with the store of the worktree's
/// repository. The operator's own environment wins over Forge's, so on a
/// machine whose environment names a wrapper this asserts that rule instead.
#[cfg(unix)]
#[tokio::test]
async fn workspace_run_gets_this_machines_shared_compiler_cache() {
    use std::os::unix::fs::PermissionsExt;
    let fixture = Fixture::new().await;
    let root = fixture.dir.path();
    let worktree = root
        .join(WORKTREE_DIRECTORY)
        .join(&fixture.prepared.workspace.workspace_handle)
        .join("repo");
    let repository =
        executors::compiler_cache::repository_id(&worktree).expect("a linked worktree");
    let wrapper = root.join("kache");
    std::fs::write(&wrapper, "#!/bin/sh\nexec \"$@\"\n").unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
    let cache_dir = root.join(executors::compiler_cache::CACHE_DIR);
    executors::compiler_cache::install(
        root,
        Some(executors::compiler_cache::CompilerCache {
            kind: executors::compiler_cache::WrapperKind::of(&wrapper),
            wrapper: wrapper.clone(),
            dir: cache_dir.clone(),
            max_bytes: 1 << 30,
        }),
    );
    let params = fixture.run(
        "cache-run",
        WorkspaceRunPurpose::CiStep,
        "printf '%s|%s' \"$RUSTC_WRAPPER\" \"$KACHE_CACHE_DIR\"",
    );
    let result = fixture
        .backend
        .handle(
            METHOD_WORKSPACE_RUN,
            serde_json::to_value(params).unwrap(),
            Vec::new,
        )
        .await
        .unwrap();
    executors::compiler_cache::install(root, None);
    let output: WorkspaceRunResult = decode(result).unwrap();
    assert_eq!(output.exit_code, Some(0));
    let operator = |key: &str| std::env::var_os(key).is_some_and(|value| !value.is_empty());
    let (seen, wrapper) = (output.stdout.as_str(), wrapper.to_str().unwrap());
    if operator("RUSTC_WRAPPER") {
        assert!(!seen.starts_with(wrapper), "{seen}");
    } else if operator("KACHE_CACHE_DIR") {
        assert!(seen.starts_with(&format!("{wrapper}|")), "{seen}");
    } else {
        assert_eq!(
            seen,
            format!("{wrapper}|{}", cache_dir.join(repository).display())
        );
    }
}
