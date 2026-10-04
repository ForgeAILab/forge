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

fn merge_params(
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

fn retain_intent(fixture: &Fixture, method: &str, request: Value) {
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
