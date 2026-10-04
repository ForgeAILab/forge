use super::*;

impl DaemonWorkspaceBackend {
    pub(super) async fn inspect(
        &self,
        params: WorkspaceInspectParams,
    ) -> CommandResult<WorkspaceInspectResult> {
        match params {
            WorkspaceInspectParams::Git {
                workspace,
                query,
                optional,
                limit,
            } => {
                let owned = self.live_workspace(&workspace).await?;
                let result = self.git_query(&owned, query, limit).await;
                let output = match result {
                    Ok(output) => Some(output),
                    Err(failure)
                        if optional
                            && failure.code == WORKSPACE_ERROR
                            && failure.message.contains(" failed: ") =>
                    {
                        None
                    }
                    Err(failure) => return Err(failure),
                };
                Ok(WorkspaceInspectResult::Git { output })
            }
            WorkspaceInspectParams::Paths { workspace } => {
                let owned = self.live_workspace(&workspace).await?;
                let (_, repo) = self.location_path(&owned.repo_location_id).await?;
                Ok(WorkspaceInspectResult::Paths {
                    workspace_path: owned.path.to_string_lossy().into_owned(),
                    repo_path: repo.to_string_lossy().into_owned(),
                })
            }
            WorkspaceInspectParams::Files {
                workspace,
                path,
                repository,
                max_entries,
                max_bytes,
            } => {
                let owned = self.live_workspace(&workspace).await?;
                let root = if repository {
                    self.location_path(&owned.repo_location_id).await?.1
                } else {
                    owned.path
                };
                let directory = self.relative_path(&root, &path)?;
                if !directory.exists() {
                    return Ok(WorkspaceInspectResult::Files { files: Vec::new() });
                }
                let mut pending = vec![directory];
                let mut files = Vec::new();
                let mut remaining = max_bytes;
                while let Some(directory) = pending.pop() {
                    let mut entries = tokio::fs::read_dir(directory).await.map_err(io_error)?;
                    while let Some(entry) = entries.next_entry().await.map_err(io_error)? {
                        let path = entry.path();
                        let relative = path.strip_prefix(&root).map_err(|_| {
                            error(OUTSIDE_WORKSPACE_ROOT, "file read escapes its checkout")
                        })?;
                        let relative = relative
                            .to_str()
                            .ok_or_else(|| error(INVALID_INPUT, "file path is not UTF-8"))?;
                        self.relative_path(&root, relative)?;
                        let kind = entry.file_type().await.map_err(io_error)?;
                        if kind.is_dir() {
                            pending.push(path);
                            continue;
                        }
                        if !kind.is_file() {
                            continue;
                        }
                        let bytes = read_bounded_file(&path, remaining.saturating_add(1)).await?;
                        if files.len() as u64 >= max_entries || bytes.len() as u64 > remaining {
                            return Err(error(
                                WORKSPACE_ERROR,
                                "workspace file read exceeds size budget",
                            ));
                        }
                        remaining -= bytes.len() as u64;
                        files.push(WorkspaceFileContent {
                            path: relative.into(),
                            bytes,
                        });
                    }
                }
                files.sort_by(|left, right| left.path.cmp(&right.path));
                Ok(WorkspaceInspectResult::Files { files })
            }
        }
    }

    async fn git_query(
        &self,
        owned: &OwnedWorkspace,
        query: WorkspaceGitQuery,
        limit: u64,
    ) -> CommandResult<String> {
        if query == WorkspaceGitQuery::RebaseInProgress {
            return Ok(git::detect_rebase_in_progress(&owned.path)
                .await
                .map_err(git_error)?
                .to_string());
        }
        if let WorkspaceGitQuery::MarkerPaths { base, head } = &query {
            let base = resolve_commit(&owned.path, base).await?;
            let head = resolve_commit(&owned.path, head).await?;
            return git::paths_adding_conflict_markers(&owned.path, &base, &head)
                .await
                .map(|paths| paths.join("\n"))
                .map_err(git_error);
        }
        let mut path = owned.path.clone();
        let args = match query {
            WorkspaceGitQuery::MarkerPaths { .. } => unreachable!(),
            WorkspaceGitQuery::IsAncestor { base, head } => vec![
                "merge-base".into(),
                "--is-ancestor".into(),
                resolve_commit(&path, &base).await?,
                resolve_commit(&path, &head).await?,
            ],
            WorkspaceGitQuery::Head => vec!["rev-parse".into(), "HEAD".into()],
            WorkspaceGitQuery::ResolveRef { reference } => {
                let sha = resolve_commit(&path, &reference).await?;
                vec!["rev-parse".into(), "--verify".into(), sha]
            }
            WorkspaceGitQuery::MergeBase { base_ref, head_ref } => vec![
                "merge-base".into(),
                resolve_commit(&path, &base_ref).await?,
                resolve_commit(&path, &head_ref).await?,
            ],
            WorkspaceGitQuery::CandidatePaths {
                base_sha,
                commit_sha,
            } => vec![
                "diff".into(),
                "--name-only".into(),
                "-z".into(),
                "--diff-filter=ACDMRTUXB".into(),
                format!(
                    "{}..{}",
                    resolve_commit(&path, &base_sha).await?,
                    resolve_commit(&path, &commit_sha).await?
                ),
                "--".into(),
            ],
            WorkspaceGitQuery::TrackedChanges => vec![
                "diff".into(),
                "--name-only".into(),
                "HEAD".into(),
                "--".into(),
            ],
            WorkspaceGitQuery::StatusPorcelain => vec!["status".into(), "--porcelain".into()],
            WorkspaceGitQuery::BranchExists { branch } => {
                validate_branch(&path, &branch).await?;
                vec![
                    "rev-parse".into(),
                    "--verify".into(),
                    format!("refs/heads/{branch}"),
                ]
            }
            WorkspaceGitQuery::TargetHead { branch } => {
                path = self.location_path(&owned.repo_location_id).await?.1;
                validate_branch(&path, &branch).await?;
                vec![
                    "rev-parse".into(),
                    "--verify".into(),
                    format!("refs/heads/{branch}"),
                ]
            }
            WorkspaceGitQuery::RebaseInProgress => unreachable!(),
        };
        let output = git_output(&path, &args, usize::try_from(limit).unwrap_or(usize::MAX)).await?;
        if output.stdout_truncated || output.stderr_truncated {
            return Err(error(WORKSPACE_ERROR, "git evidence exceeds size budget"));
        }
        String::from_utf8(output.stdout)
            .map_err(|failure| error(WORKSPACE_ERROR, failure.to_string()))
    }

    pub(super) async fn review_diff(
        &self,
        params: WorkspaceReviewDiffParams,
    ) -> CommandResult<WorkspaceReviewDiffResult> {
        let owned = self.live_workspace(&params.workspace).await?;
        let limit = params.max_bytes.min(64 * 1024) as usize;
        let range = resolve_commit(&owned.path, &params.default_branch).await;
        let args = match range {
            Ok(base) => vec!["diff".into(), format!("{base}...HEAD"), "--".into()],
            Err(_) => vec!["diff".into(), "--".into()],
        };
        let output = match git_output(&owned.path, &args, limit.saturating_add(1)).await {
            Ok(output) => output,
            Err(_) => {
                git_output(
                    &owned.path,
                    &["diff".into(), "--".into()],
                    limit.saturating_add(1),
                )
                .await?
            }
        };
        let mut diff = String::from_utf8_lossy(&output.stdout).into_owned();
        if diff.len() > limit || output.stdout_truncated {
            let mut end = limit.min(diff.len());
            while !diff.is_char_boundary(end) {
                end -= 1;
            }
            diff.truncate(end);
            diff.push_str("[truncated]");
        }
        Ok(WorkspaceReviewDiffResult { diff })
    }

    pub(super) fn workspace_read_path(
        &self,
        worktree: &Path,
        relative: &str,
    ) -> CommandResult<PathBuf> {
        // Forge artifacts live beside repo, within this handle's workspace.
        let root = worktree
            .parent()
            .ok_or_else(|| error(OUTSIDE_WORKSPACE_ROOT, "workspace has no parent"))?;
        let mut path = worktree.to_path_buf();
        for component in Path::new(relative).components() {
            match component {
                Component::Normal(name) => path.push(name),
                Component::CurDir => continue,
                Component::ParentDir => {
                    path.pop();
                }
                _ => {
                    return Err(error(
                        OUTSIDE_WORKSPACE_ROOT,
                        "workspace.read requires a confined relative path",
                    ));
                }
            }
            // Resolve each component before processing .. so a symlink
            // cannot escape and then traverse back into the workspace.
            path = self.confined_path(&path)?;
            if !path.starts_with(root) {
                return Err(error(
                    OUTSIDE_WORKSPACE_ROOT,
                    "read path escapes the workspace",
                ));
            }
        }
        Ok(path)
    }

    pub(super) fn relative_path(&self, root: &Path, relative: &str) -> CommandResult<PathBuf> {
        let relative = Path::new(relative);
        if relative.is_absolute()
            || relative
                .components()
                .any(|part| matches!(part, Component::ParentDir | Component::Prefix(_)))
        {
            return Err(error(
                OUTSIDE_WORKSPACE_ROOT,
                "path must be relative to its checkout",
            ));
        }
        let path = self.confined_path(&root.join(relative))?;
        if !path.starts_with(root) {
            return Err(error(
                OUTSIDE_WORKSPACE_ROOT,
                "path resolves outside its checkout",
            ));
        }
        Ok(path)
    }
}
