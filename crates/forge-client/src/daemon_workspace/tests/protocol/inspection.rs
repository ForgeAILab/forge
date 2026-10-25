//! Workspace Git evidence and bounded file inspection.

use super::*;

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
