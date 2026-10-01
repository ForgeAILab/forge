use super::*;
use tempfile::TempDir;

struct Fixture {
    dir: TempDir,
    repo: PathBuf,
    backend: DaemonWorkspaceBackend,
    journal: Arc<DaemonJournal>,
    prepared: WorkspacePrepareResult,
}

impl Fixture {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path().join("primary");
        std::fs::create_dir(&repo).unwrap();
        git::init(&repo).await.unwrap();
        std::fs::write(repo.join("README.md"), "base\n").unwrap();
        let base_sha = git::commit_all(&repo, "base").await.unwrap();
        let journal = Arc::new(DaemonJournal::new(dir.path()));
        let backend = DaemonWorkspaceBackend::new(
            dir.path().to_owned(),
            "daemon-1".into(),
            crate::daemon_config::DaemonConfig::default().run_policy(),
            Arc::clone(&journal),
        )
        .unwrap();
        backend
            .handle(
                METHOD_REPO_LOCATION_VERIFY,
                serde_json::to_value(RepoLocationVerifyParams {
                    repo_location_id: "location-1".into(),
                    daemon_id: "daemon-1".into(),
                    runtime_id: "runtime-1".into(),
                    path: repo.to_string_lossy().into_owned(),
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
        let prepared = serde_json::from_value(
            backend
                .handle(
                    METHOD_WORKSPACE_PREPARE,
                    serde_json::to_value(WorkspacePrepareParams {
                        fence: fence("prepare-1", 1, &base_sha),
                        repo_location_id: "location-1".into(),
                        workspace_id: "workspace-1".into(),
                        task_id: "task-1".into(),
                        base_ref: "main".into(),
                        branch: "task/test".into(),
                    })
                    .unwrap(),
                    Vec::new,
                )
                .await
                .unwrap(),
        )
        .unwrap();
        Self {
            dir,
            repo,
            backend,
            journal,
            prepared,
        }
    }

    fn path(&self) -> &Path {
        Path::new(&self.prepared.workspace.workspace_path)
    }
    fn run(&self, id: &str, purpose: WorkspaceRunPurpose, command: &str) -> WorkspaceRunParams {
        WorkspaceRunParams {
            fence: fence(
                id,
                self.prepared.workspace.generation,
                &self.prepared.workspace.base_sha,
            ),
            workspace_handle: self.prepared.workspace.workspace_handle.clone(),
            purpose,
            command: command.into(),
            env: Vec::new(),
            timeout_secs: 5,
            max_output_bytes: 4096,
        }
    }
    fn reference(&self) -> WorkspaceHandleReference {
        reference(
            &fence(
                "read",
                self.prepared.workspace.generation,
                &self.prepared.workspace.base_sha,
            ),
            &self.prepared.workspace.workspace_handle,
        )
    }

    async fn read(&self, path: &str) -> CommandResult<WorkspaceReadResult> {
        let result = self
            .backend
            .handle(
                METHOD_WORKSPACE_READ,
                serde_json::to_value(WorkspaceReadParams {
                    workspace: self.reference(),
                    path: path.into(),
                    limit: 1024,
                })
                .unwrap(),
                Vec::new,
            )
            .await?;
        Ok(serde_json::from_value(result).unwrap())
    }
}

fn fence(id: &str, generation: u64, sha: &str) -> WorkspaceMutationFence {
    WorkspaceMutationFence {
        daemon_id: "daemon-1".into(),
        runtime_id: "runtime-1".into(),
        placement_id: "placement-1".into(),
        operation_id: id.into(),
        generation,
        expected: WorkspaceOperationExpected::BaseSha { sha: sha.into() },
    }
}

#[tokio::test]
async fn workspace_read_allows_sibling_artifacts_and_reports_missing_files() {
    let fixture = Fixture::new().await;
    assert_eq!(
        fixture.read("../plan.md").await.unwrap_err().code,
        WORKSPACE_FILE_NOT_FOUND
    );
    assert_eq!(
        fixture.read("missing/file").await.unwrap_err().code,
        WORKSPACE_FILE_NOT_FOUND
    );
    let parent = fixture.path().parent().unwrap();
    std::fs::write(parent.join("plan.md"), "- [x] completed\n").unwrap();
    std::fs::create_dir_all(parent.join(".forge-outbox/execution")).unwrap();
    std::fs::write(parent.join(".forge-outbox/execution/worklog"), "evidence").unwrap();
    assert_eq!(
        fixture.read("../plan.md").await.unwrap().bytes,
        b"- [x] completed\n"
    );
    assert_eq!(
        fixture
            .read("../.forge-outbox/execution/worklog")
            .await
            .unwrap()
            .bytes,
        b"evidence"
    );
    assert_eq!(fixture.read("README.md").await.unwrap().bytes, b"base\n");
    for path in [
        "../../x",
        "../../../journal/workspace-state.json",
        "/etc/passwd",
    ] {
        assert_eq!(
            fixture.read(path).await.unwrap_err().code,
            OUTSIDE_WORKSPACE_ROOT,
            "{path}"
        );
    }
}

#[cfg(unix)]
#[tokio::test]
async fn workspace_read_rejects_sibling_symlink_escapes_and_journal_access() {
    let fixture = Fixture::new().await;
    let parent = fixture.path().parent().unwrap();
    std::os::unix::fs::symlink(&fixture.repo, parent.join("plan.md")).unwrap();
    assert_eq!(
        fixture.read("../plan.md/README.md").await.unwrap_err().code,
        OUTSIDE_WORKSPACE_ROOT
    );
    // Even an escape followed by .. must fail before returning to the workspace.
    assert_eq!(
        fixture
            .read("../plan.md/../repo/README.md")
            .await
            .unwrap_err()
            .code,
        OUTSIDE_WORKSPACE_ROOT
    );
    std::fs::remove_file(parent.join("plan.md")).unwrap();
    std::os::unix::fs::symlink(parent.join("missing"), parent.join("plan.md")).unwrap();
    assert_eq!(
        fixture.read("../plan.md").await.unwrap_err().code,
        OUTSIDE_WORKSPACE_ROOT
    );
    std::os::unix::fs::symlink(
        fixture
            .dir
            .path()
            .join(crate::daemon_persistence::JOURNAL_DIRECTORY),
        parent.join("journal"),
    )
    .unwrap();
    assert_eq!(
        fixture
            .read("../journal/workspace-state.json")
            .await
            .unwrap_err()
            .code,
        OUTSIDE_WORKSPACE_ROOT
    );
}

#[tokio::test]
async fn location_verify_accepts_equivalent_remotes_and_rejects_different_repositories() {
    let fixture = Fixture::new().await;
    local_git(
        &fixture.repo,
        &["remote", "add", "origin", "git@github.com:o/r.git"],
    )
    .await
    .unwrap();
    let mut params = RepoLocationVerifyParams {
        repo_location_id: "remote-location".into(),
        daemon_id: "daemon-1".into(),
        runtime_id: "runtime-1".into(),
        path: fixture.repo.to_string_lossy().into_owned(),
        kind: DaemonRepoLocationKind::PrimaryCheckout,
        default_branch: "main".into(),
        remote_url: Some("https://GITHUB.com:443/o/r/".into()),
        expected_version: 0,
        probe: None,
    };
    fixture.backend.verify(params.clone()).await.unwrap();
    params.remote_url = Some("ssh://git@github.com/other/r.git".into());
    let failure = fixture.backend.verify(params.clone()).await.unwrap_err();
    assert_eq!(failure.code, INVALID_INPUT);
    assert_eq!(failure.message, "repository origin remote does not match");

    let origin = fixture.dir.path().join("origin.git");
    local_git(
        &fixture.repo,
        &["remote", "set-url", "origin", origin.to_str().unwrap()],
    )
    .await
    .unwrap();
    params.remote_url = Some(url::Url::from_file_path(origin).unwrap().to_string());
    fixture.backend.verify(params).await.unwrap();
}

#[tokio::test]
async fn duplicate_operation_id_returns_recorded_result_after_ack_and_restart() {
    let fixture = Fixture::new().await;
    let params = serde_json::to_value(fixture.run(
        "run-once",
        WorkspaceRunPurpose::CiStep,
        "printf x >> marker; printf done",
    ))
    .unwrap();
    let result = fixture
        .backend
        .handle(METHOD_WORKSPACE_RUN, params.clone(), Vec::new)
        .await
        .unwrap();
    let repeated = fixture
        .backend
        .handle(METHOD_WORKSPACE_RUN, params.clone(), Vec::new)
        .await
        .unwrap();
    assert_eq!(result, repeated);
    assert_eq!(
        std::fs::read_to_string(fixture.path().join("marker")).unwrap(),
        "x"
    );
    fixture
        .journal
        .acknowledge(&JournalAckParams {
            entry_id: result["entry_id"].as_str().unwrap().into(),
        })
        .unwrap();
    let journal = Arc::new(DaemonJournal::new(fixture.dir.path()));
    let restarted = DaemonWorkspaceBackend::new(
        fixture.dir.path().to_owned(),
        "daemon-1".into(),
        crate::daemon_config::DaemonConfig::default().run_policy(),
        journal,
    )
    .unwrap();
    assert_eq!(
        restarted
            .handle(METHOD_WORKSPACE_RUN, params, Vec::new)
            .await
            .unwrap(),
        result
    );
    assert_eq!(
        std::fs::read_to_string(fixture.path().join("marker")).unwrap(),
        "x"
    );
}

#[tokio::test]
async fn duplicate_prepare_creates_one_worktree_and_handle_survives_restart() {
    let fixture = Fixture::new().await;
    let params = WorkspacePrepareParams {
        fence: fence("prepare-1", 1, &fixture.prepared.workspace.base_sha),
        repo_location_id: "location-1".into(),
        workspace_id: "workspace-1".into(),
        task_id: "task-1".into(),
        base_ref: "main".into(),
        branch: "task/test".into(),
    };
    assert_eq!(
        fixture
            .backend
            .handle(
                METHOD_WORKSPACE_PREPARE,
                serde_json::to_value(params).unwrap(),
                Vec::new
            )
            .await
            .unwrap(),
        serde_json::to_value(&fixture.prepared).unwrap()
    );
    assert_eq!(
        std::fs::read_dir(fixture.dir.path().join(WORKTREE_DIRECTORY))
            .unwrap()
            .count(),
        1
    );
    let restarted = DaemonWorkspaceBackend::new(
        fixture.dir.path().to_owned(),
        "daemon-1".into(),
        crate::daemon_config::DaemonConfig::default().run_policy(),
        Arc::new(DaemonJournal::new(fixture.dir.path())),
    )
    .unwrap();
    let describe = restarted
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
    assert_eq!(describe["exists"], true);
}

#[tokio::test]
async fn stale_generation_and_wrong_owner_are_rejected_before_mutation() {
    let fixture = Fixture::new().await;
    let reset = WorkspaceResetParams {
        fence: fence("reset-1", 2, &fixture.prepared.workspace.base_sha),
        workspace_handle: fixture.prepared.workspace.workspace_handle.clone(),
        base_ref: "main".into(),
        branch: "task/test".into(),
    };
    fixture
        .backend
        .handle(
            METHOD_WORKSPACE_RESET,
            serde_json::to_value(reset).unwrap(),
            Vec::new,
        )
        .await
        .unwrap();
    let params = fixture.run("stale", WorkspaceRunPurpose::CiStep, "touch escaped");
    assert_eq!(
        fixture
            .backend
            .handle(
                METHOD_WORKSPACE_RUN,
                serde_json::to_value(&params).unwrap(),
                Vec::new
            )
            .await
            .unwrap_err()
            .code,
        STALE_GENERATION
    );
    let mut wrong = params;
    wrong.fence.generation = 2;
    wrong.fence.daemon_id = "other-daemon".into();
    assert_eq!(
        fixture
            .backend
            .handle(
                METHOD_WORKSPACE_RUN,
                serde_json::to_value(wrong).unwrap(),
                Vec::new
            )
            .await
            .unwrap_err()
            .code,
        WRONG_OWNER
    );
    assert!(!fixture.path().join("escaped").exists());
}

#[tokio::test]
async fn location_and_mapped_handle_path_escape_are_rejected() {
    let fixture = Fixture::new().await;
    let outside = tempfile::tempdir().unwrap();
    let verify = RepoLocationVerifyParams {
        repo_location_id: "outside".into(),
        daemon_id: "daemon-1".into(),
        runtime_id: "runtime-1".into(),
        path: outside.path().to_string_lossy().into_owned(),
        kind: DaemonRepoLocationKind::PrimaryCheckout,
        default_branch: "main".into(),
        remote_url: None,
        expected_version: 0,
        probe: None,
    };
    assert_eq!(
        fixture
            .backend
            .handle(
                METHOD_REPO_LOCATION_VERIFY,
                serde_json::to_value(verify).unwrap(),
                Vec::new
            )
            .await
            .unwrap_err()
            .code,
        OUTSIDE_WORKSPACE_ROOT
    );
    fixture
        .backend
        .state
        .lock()
        .unwrap()
        .handles
        .get_mut(&fixture.prepared.workspace.workspace_handle)
        .unwrap()
        .path = outside.path().to_owned();
    let params = fixture.run("escape", WorkspaceRunPurpose::CiStep, "touch escaped");
    assert_eq!(
        fixture
            .backend
            .handle(
                METHOD_WORKSPACE_RUN,
                serde_json::to_value(params).unwrap(),
                Vec::new
            )
            .await
            .unwrap_err()
            .code,
        OUTSIDE_WORKSPACE_ROOT
    );
    assert!(!outside.path().join("escaped").exists());
}

#[cfg(unix)]
#[tokio::test]
async fn read_and_cleanup_reject_symlink_escape() {
    let fixture = Fixture::new().await;
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(outside.path().join("secret"), "outside").unwrap();
    std::os::unix::fs::symlink(outside.path(), fixture.path().join("escape")).unwrap();
    let read = WorkspaceReadParams {
        workspace: fixture.reference(),
        path: "escape/secret".into(),
        limit: 100,
    };
    assert_eq!(
        fixture
            .backend
            .handle(
                METHOD_WORKSPACE_READ,
                serde_json::to_value(read).unwrap(),
                Vec::new
            )
            .await
            .unwrap_err()
            .code,
        OUTSIDE_WORKSPACE_ROOT
    );
    let parent = fixture.path().parent().unwrap();
    let original = parent.with_extension("saved");
    std::fs::rename(parent, &original).unwrap();
    std::os::unix::fs::symlink(outside.path(), parent).unwrap();
    let cleanup = WorkspaceCleanupParams {
        fence: fence("escape-cleanup", 1, &fixture.prepared.workspace.base_sha),
        workspace_handle: fixture.prepared.workspace.workspace_handle.clone(),
    };
    assert_eq!(
        fixture
            .backend
            .handle(
                METHOD_WORKSPACE_CLEANUP,
                serde_json::to_value(cleanup).unwrap(),
                Vec::new
            )
            .await
            .unwrap_err()
            .code,
        OUTSIDE_WORKSPACE_ROOT
    );
    assert_eq!(
        std::fs::read_to_string(outside.path().join("secret")).unwrap(),
        "outside"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn a_handle_cannot_be_redirected_to_another_checkout_inside_the_root() {
    let fixture = Fixture::new().await;
    std::fs::rename(fixture.path(), fixture.path().with_extension("saved")).unwrap();
    std::os::unix::fs::symlink(&fixture.repo, fixture.path()).unwrap();
    let params = fixture.run(
        "redirected",
        WorkspaceRunPurpose::CiStep,
        "touch unexpected",
    );
    assert_eq!(
        fixture
            .backend
            .handle(
                METHOD_WORKSPACE_RUN,
                serde_json::to_value(params).unwrap(),
                Vec::new
            )
            .await
            .unwrap_err()
            .code,
        OUTSIDE_WORKSPACE_ROOT
    );
    assert!(!fixture.repo.join("unexpected").exists());
}

#[tokio::test]
async fn dirty_target_merge_is_refused_without_changing_either_head() {
    let fixture = Fixture::new().await;
    std::fs::write(fixture.path().join("feature.txt"), "feature").unwrap();
    let candidate = git::commit_all(fixture.path(), "feature").await.unwrap();
    let target_before = git::get_current_sha(&fixture.repo).await.unwrap();
    std::fs::write(fixture.repo.join("README.md"), "local uncommitted change\n").unwrap();
    let params = WorkspaceMergeParams {
        fence: fence("merge-1", 1, &candidate),
        workspace_handle: fixture.prepared.workspace.workspace_handle.clone(),
        repo_location_id: "location-1".into(),
        target_branch: "main".into(),
        expected_target_sha: target_before.clone(),
        handed_off_paths: Vec::new(),
    };
    let result: WorkspaceMergeResult = serde_json::from_value(
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
    assert!(
        matches!(result.outcome, WorkspaceMergeOutcome::TargetDirty { files } if files.contains(&"README.md".into()))
    );
    assert_eq!(
        git::get_current_sha(&fixture.repo).await.unwrap(),
        target_before
    );
    assert_eq!(
        git::get_current_sha(fixture.path()).await.unwrap(),
        candidate
    );
}

#[tokio::test]
async fn reviewed_merge_fast_forwards_the_verified_primary_and_replays_its_result() {
    let fixture = Fixture::new().await;
    std::fs::write(fixture.path().join("feature.txt"), "feature\n").unwrap();
    let candidate = git::commit_all(fixture.path(), "feature").await.unwrap();
    let before = git::get_current_sha(&fixture.repo).await.unwrap();
    let params = serde_json::to_value(WorkspaceReviewedMergeParams {
        reviewed_commit_sha: Some(candidate.clone()),
        merge: WorkspaceMergeParams {
            fence: fence("merge-clean", 1, &candidate),
            workspace_handle: fixture.prepared.workspace.workspace_handle.clone(),
            repo_location_id: "location-1".into(),
            target_branch: "main".into(),
            expected_target_sha: before.clone(),
            handed_off_paths: Vec::new(),
        },
    })
    .unwrap();
    let result = fixture
        .backend
        .handle(METHOD_WORKSPACE_MERGE, params.clone(), Vec::new)
        .await
        .unwrap();
    let merged: WorkspaceMergeResult = serde_json::from_value(result.clone()).unwrap();
    assert_eq!(
        merged.outcome,
        WorkspaceMergeOutcome::Done {
            before_sha: before,
            after_sha: candidate.clone(),
            branch: "main".into(),
        }
    );
    assert_eq!(merged.diffstat.unwrap().files_changed, 1);
    assert_eq!(
        git::get_current_sha(&fixture.repo).await.unwrap(),
        candidate
    );
    assert_eq!(
        fixture
            .backend
            .handle(METHOD_WORKSPACE_MERGE, params, Vec::new)
            .await
            .unwrap(),
        result
    );
}

#[cfg(unix)]
#[tokio::test]
async fn a_symlink_alias_into_the_workspace_root_is_accepted() {
    let fixture = Fixture::new().await;
    let aliases = tempfile::tempdir().unwrap();
    let alias = aliases.path().join("root");
    std::os::unix::fs::symlink(fixture.dir.path(), &alias).unwrap();
    let result: RepoLocationVerifyResult = serde_json::from_value(
        fixture
            .backend
            .handle(
                METHOD_REPO_LOCATION_VERIFY,
                serde_json::to_value(RepoLocationVerifyParams {
                    repo_location_id: "location-1".into(),
                    daemon_id: "daemon-1".into(),
                    runtime_id: "runtime-1".into(),
                    path: alias.join("primary").to_string_lossy().into_owned(),
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
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        PathBuf::from(result.path),
        fixture.repo.canonicalize().unwrap()
    );
}

#[tokio::test]
async fn shared_mount_probe_is_confined_to_the_workspace_root() {
    let fixture = Fixture::new().await;
    let probe_path = fixture.dir.path().join(".forge-location-probe-test");
    let content = "forge-shared-mount:test";
    std::fs::write(&probe_path, content).unwrap();
    let mut params = RepoLocationVerifyParams {
        repo_location_id: "shared-mount".into(),
        daemon_id: "daemon-1".into(),
        runtime_id: "runtime-1".into(),
        path: fixture.repo.to_string_lossy().into_owned(),
        kind: DaemonRepoLocationKind::SharedMount,
        default_branch: "main".into(),
        remote_url: None,
        expected_version: 0,
        probe: Some(RepoLocationProbe {
            path: probe_path.to_string_lossy().into_owned(),
            content: content.into(),
        }),
    };
    let verified: RepoLocationVerifyResult = serde_json::from_value(
        fixture
            .backend
            .handle(
                METHOD_REPO_LOCATION_VERIFY,
                serde_json::to_value(&params).unwrap(),
                Vec::new,
            )
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(verified.probe_content.as_deref(), Some(content));
    assert_eq!(verified.repo_location_id, params.repo_location_id);
    assert_eq!(
        PathBuf::from(verified.path),
        fixture.repo.canonicalize().unwrap()
    );

    let outside = tempfile::tempdir().unwrap();
    let outside_probe = outside.path().join("probe");
    std::fs::write(&outside_probe, content).unwrap();
    params.probe.as_mut().unwrap().path = outside_probe.to_string_lossy().into_owned();
    assert_eq!(
        fixture
            .backend
            .handle(
                METHOD_REPO_LOCATION_VERIFY,
                serde_json::to_value(&params).unwrap(),
                Vec::new,
            )
            .await
            .unwrap_err()
            .code,
        OUTSIDE_WORKSPACE_ROOT
    );

    #[cfg(unix)]
    {
        let alias = fixture.dir.path().join("probe-link");
        std::os::unix::fs::symlink(&outside_probe, &alias).unwrap();
        params.probe.as_mut().unwrap().path = alias.to_string_lossy().into_owned();
        assert_eq!(
            fixture
                .backend
                .handle(
                    METHOD_REPO_LOCATION_VERIFY,
                    serde_json::to_value(params).unwrap(),
                    Vec::new,
                )
                .await
                .unwrap_err()
                .code,
            OUTSIDE_WORKSPACE_ROOT
        );
    }
}

#[tokio::test]
async fn a_checkout_at_the_workspace_root_excludes_only_forge_runtime_metadata() {
    let dir = tempfile::tempdir().unwrap();
    git::init(dir.path()).await.unwrap();
    std::fs::write(dir.path().join("README.md"), "base\n").unwrap();
    git::commit_all(dir.path(), "base").await.unwrap();
    let exclude = dir.path().join(".git/info/exclude");
    std::fs::write(&exclude, "# local user excludes\nlocal-cache\n").unwrap();
    std::fs::write(dir.path().join("local-cache"), "cached").unwrap();
    let backend = DaemonWorkspaceBackend::new(
        dir.path().to_owned(),
        "daemon-1".into(),
        crate::daemon_config::DaemonConfig::default().run_policy(),
        Arc::new(DaemonJournal::new(dir.path())),
    )
    .unwrap();
    backend
        .handle(
            METHOD_REPO_LOCATION_VERIFY,
            serde_json::to_value(RepoLocationVerifyParams {
                repo_location_id: "location-1".into(),
                daemon_id: "daemon-1".into(),
                runtime_id: "runtime-1".into(),
                path: dir.path().to_string_lossy().into_owned(),
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
    assert!(std::fs::read_to_string(exclude)
        .unwrap()
        .starts_with("# local user excludes\nlocal-cache\n"));
    assert!(git::is_worktree_clean(dir.path()).await.unwrap());
    std::fs::write(dir.path().join("user-change"), "keep visible").unwrap();
    assert!(!git::is_worktree_clean(dir.path()).await.unwrap());
}

#[tokio::test]
async fn cleanup_checks_execution_activity_after_waiting_for_the_operation_lock() {
    let fixture = Fixture::new().await;
    fixture
        .backend
        .register_execution("exec-1", fixture.path())
        .await
        .unwrap();
    let params = serde_json::to_value(WorkspaceCleanupParams {
        fence: fence("cleanup-active", 1, &fixture.prepared.workspace.base_sha),
        workspace_handle: fixture.prepared.workspace.workspace_handle.clone(),
    })
    .unwrap();
    let tracker = crate::daemon_runtime::ActiveExecutionTracker::default();
    let lock = fixture.backend.operation_lock.lock().await;
    let request = fixture
        .backend
        .handle(METHOD_WORKSPACE_CLEANUP, params, || tracker.running_ids());
    tokio::pin!(request);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut request)
            .await
            .is_err()
    );
    let _execution = tracker.track("exec-1".into());
    drop(lock);
    assert_eq!(request.await.unwrap_err().code, INVALID_INPUT);
    assert!(fixture.path().is_dir());
}

#[tokio::test]
async fn run_purpose_denied_never_launches_shell() {
    let fixture = Fixture::new().await;
    for purpose in [
        WorkspaceRunPurpose::Hook,
        WorkspaceRunPurpose::EnvironmentSetup,
    ] {
        let params = fixture.run(&format!("denied-{purpose:?}"), purpose, "touch forbidden");
        assert_eq!(
            fixture
                .backend
                .handle(
                    METHOD_WORKSPACE_RUN,
                    serde_json::to_value(params).unwrap(),
                    Vec::new
                )
                .await
                .unwrap_err()
                .code,
            PURPOSE_DENIED
        );
    }
    assert!(!fixture.path().join("forbidden").exists());
}

#[tokio::test]
async fn run_timeout_and_output_caps_are_enforced() {
    let fixture = Fixture::new().await;
    let mut params = fixture.run(
        "timeout",
        WorkspaceRunPurpose::CiStep,
        "printf 123456789; sleep 5",
    );
    params.timeout_secs = 1;
    params.max_output_bytes = 4;
    let result: WorkspaceRunResult = serde_json::from_value(
        fixture
            .backend
            .handle(
                METHOD_WORKSPACE_RUN,
                serde_json::to_value(params).unwrap(),
                Vec::new,
            )
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(result.timed_out);
    assert_eq!(result.exit_code, None);
    assert_eq!(result.stdout, "6789");
    assert!(result.stdout_truncated);
}

#[tokio::test]
async fn cleanup_replays_until_ack_and_duplicate_remains_idempotent() {
    let fixture = Fixture::new().await;
    let params = serde_json::to_value(WorkspaceCleanupParams {
        fence: fence("cleanup-1", 1, &fixture.prepared.workspace.base_sha),
        workspace_handle: fixture.prepared.workspace.workspace_handle.clone(),
    })
    .unwrap();
    let result = fixture
        .backend
        .handle(METHOD_WORKSPACE_CLEANUP, params.clone(), Vec::new)
        .await
        .unwrap();
    assert!(!fixture.path().exists());
    let restarted = DaemonJournal::new(fixture.dir.path());
    let replay = restarted
        .pending()
        .unwrap()
        .into_iter()
        .filter_map(|entry| {
            entry
                .replay_notification()
                .map(|(method, value)| (method.to_owned(), value))
        })
        .collect::<Vec<_>>();
    assert_eq!(
        replay,
        vec![(METHOD_WORKSPACE_CLEANUP.into(), result.clone())]
    );
    restarted
        .acknowledge(&JournalAckParams {
            entry_id: result["entry_id"].as_str().unwrap().into(),
        })
        .unwrap();
    assert!(!restarted
        .pending()
        .unwrap()
        .iter()
        .any(|entry| entry.replay_notification().is_some()));
    assert_eq!(
        fixture
            .backend
            .handle(METHOD_WORKSPACE_CLEANUP, params, Vec::new)
            .await
            .unwrap(),
        result
    );
}

#[tokio::test]
async fn describe_lists_active_and_journaled_executions_for_this_handle() {
    let fixture = Fixture::new().await;
    fixture
        .backend
        .register_execution("exec-1", fixture.path())
        .await
        .unwrap();
    let report: ExecutionTerminalNotification = serde_json::from_value(serde_json::json!({"terminal_report_id":"report-1", "execution_id":"exec-1", "exit_code":0, "signal":null, "error":null, "ts":"now", "usage_reports":[]})).unwrap();
    fixture.journal.retain(&report).unwrap();
    let result: WorkspaceDescribeResult = serde_json::from_value(
        fixture
            .backend
            .handle(
                METHOD_WORKSPACE_DESCRIBE,
                serde_json::to_value(WorkspaceDescribeParams {
                    workspace: fixture.reference(),
                })
                .unwrap(),
                || vec!["exec-1".into()],
            )
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(result.active_execution_ids, ["exec-1"]);
    assert_eq!(result.journaled_execution_ids, ["exec-1"]);
}

mod protocol;
