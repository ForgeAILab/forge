//! Repository verification, machine probing, provisioning, and runtime metadata exclusions.

use super::*;

impl DaemonWorkspaceBackend {
    pub(super) async fn verify(
        &self,
        params: RepoLocationVerifyParams,
    ) -> CommandResult<RepoLocationVerifyResult> {
        self.check_owner(&params.daemon_id, &params.runtime_id)?;
        validate_id(&params.repo_location_id)?;
        let requested = self.confined_path(Path::new(params.path.trim()))?;
        let path = confined_existing(&requested, &self.workspace_root)?;
        if !path.is_dir()
            || local_git(&path, &["rev-parse", "--is-inside-work-tree"]).await? != "true"
        {
            return Err(error(
                INVALID_INPUT,
                "repository location must be a git work tree",
            ));
        }
        verify_git_dir(&path, &self.workspace_root).await?;
        let top = PathBuf::from(local_git(&path, &["rev-parse", "--show-toplevel"]).await?);
        let path = confined_existing(&top, &self.workspace_root)?;
        let default_branch_sha = resolve_commit(&path, &params.default_branch).await?;
        let branches = git::list_branches(&path).await.map_err(git_error)?;
        if let (Some(origin), Some(expected)) = (&branches.origin_url, params.remote_url.as_deref())
        {
            if git::normalize_remote_url(expected) != git::normalize_remote_url(origin) {
                return Err(error(
                    INVALID_INPUT,
                    "repository origin remote does not match",
                ));
            }
        }
        let probe_content = match params.probe {
            Some(probe) => {
                let probe_path =
                    daemon_fs::validate_within_root(Path::new(&probe.path), &self.workspace_root)
                        .map_err(|_| {
                        error(OUTSIDE_WORKSPACE_ROOT, "probe path escapes workspace_root")
                    })?;
                let bytes = read_bounded_file(&probe_path, 4096).await?;
                let text = String::from_utf8(bytes)
                    .map_err(|_| error(INVALID_INPUT, "probe is not UTF-8"))?;
                if text != probe.content {
                    return Err(error(INVALID_INPUT, "shared-mount probe does not match"));
                }
                Some(text)
            }
            None if params.kind == DaemonRepoLocationKind::SharedMount => {
                return Err(error(INVALID_INPUT, "shared_mount requires a probe"))
            }
            None => None,
        };
        if path == self.workspace_root {
            self.exclude_runtime_metadata(&path).await?;
        }
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(existing) = state.locations.get(&params.repo_location_id) {
            if params.expected_version < existing.version {
                return Err(error(
                    VERSION_CONFLICT,
                    "repository location version is stale",
                ));
            }
            if existing.daemon_id != params.daemon_id || existing.runtime_id != params.runtime_id {
                return Err(error(
                    WRONG_OWNER,
                    "repository location belongs to another owner",
                ));
            }
        }
        let mut updated = state.clone();
        updated.locations.insert(
            params.repo_location_id.clone(),
            VerifiedLocation {
                daemon_id: params.daemon_id,
                runtime_id: params.runtime_id,
                path: path.clone(),
                kind: params.kind,
                version: params.expected_version,
            },
        );
        self.journal
            .save_workspace_state(&updated)
            .map_err(storage_error)?;
        *state = updated;
        Ok(RepoLocationVerifyResult {
            repo_location_id: params.repo_location_id,
            path: path.to_string_lossy().into_owned(),
            default_branch_sha,
            origin_url: branches
                .origin_url
                .map(|origin| git::redact_remote_credentials(&origin, &origin)),
            probe_content,
        })
    }

    pub(super) async fn machine_probe(
        &self,
        params: MachineProbeParams,
    ) -> CommandResult<MachineProbeResult> {
        self.check_owner(&params.daemon_id, &params.runtime_id)?;
        if !self
            .policy
            .allowed_purposes
            .contains(&WorkspaceRunPurpose::EnvironmentProbe)
        {
            return Err(error(
                "run_purpose_denied",
                "environment_probe is denied by local daemon configuration",
            ));
        }
        if params.commands.is_empty()
            || params.commands.len() > 64
            || params.commands.iter().any(|command| {
                command.name.trim().is_empty()
                    || command.command.trim().is_empty()
                    || !(1..=300).contains(&command.timeout_seconds)
            })
        {
            return Err(error(
                INVALID_INPUT,
                "machine.probe requires 1–64 named commands with 1–300 second timeouts",
            ));
        }
        let checkout = match params.repo_location_id.as_deref() {
            Some(id) => Some(self.location_path(id).await?.1),
            None => None,
        };
        let mut results = Vec::new();
        for check in params.commands {
            let scratch = if checkout.is_none() {
                let path = self.confined_path(
                    &self
                        .workspace_root
                        .join(".forge/probes")
                        .join(uuid::Uuid::new_v4().to_string()),
                )?;
                std::fs::create_dir_all(&path).map_err(io_error)?;
                Some(RemoveDirectory(path))
            } else {
                None
            };
            let path = checkout
                .as_deref()
                .unwrap_or_else(|| &scratch.as_ref().expect("scratch").0);
            let mut command = Command::new("bash");
            command
                .args(["-lc", &check.command])
                .envs(&params.env)
                .env("PWD", path)
                .current_dir(path)
                .env_remove("GIT_DIR")
                .env_remove("GIT_WORK_TREE")
                .env_remove("GIT_INDEX_FILE");
            let run_scope = executors::sandbox::SandboxEnv::for_command(
                path,
                executors::sandbox::RunPurpose::Probe,
            );
            executors::run_process::apply_sandboxed(&mut command, &params.env, run_scope.env());
            let output =
                bounded_command_inner(command, check.timeout_seconds, 4096, true, true).await?;
            let raw = [output.stdout, output.stderr].concat();
            let (output_tail, _) = redacted_tail(&raw, &params.env, 4096);
            results.push(MachineProbeCommandResult {
                name: check.name,
                exit_code: output.exit_code,
                timed_out: output.timed_out,
                output_tail,
            });
        }
        Ok(MachineProbeResult { results })
    }

    pub(super) async fn provision_location(
        &self,
        params: RepoLocationProvisionParams,
    ) -> CommandResult<RepoLocationProvisionResult> {
        self.check_owner(&params.daemon_id, &params.runtime_id)?;
        validate_id(&params.repo_id)?;
        if !self
            .policy
            .allowed_purposes
            .contains(&WorkspaceRunPurpose::RepoProvision)
        {
            return Err(error(
                "run_purpose_denied",
                "repo_provision is denied by local daemon configuration",
            ));
        }
        if !(1..=86400).contains(&params.timeout_seconds) {
            return Err(error(
                INVALID_INPUT,
                "provision timeout must be between 1 and 86400 seconds",
            ));
        }
        let deadline = Instant::now() + Duration::from_secs(params.timeout_seconds);
        if params.default_branch.is_empty()
            || params.default_branch.starts_with('-')
            || local_git(
                &self.workspace_root,
                &[
                    "check-ref-format",
                    &format!("refs/heads/{}", params.default_branch),
                ],
            )
            .await
            .is_err()
        {
            return Err(error(INVALID_INPUT, "invalid repository branch"));
        }
        if params.remote_url.trim().is_empty() || params.remote_url.starts_with('-') {
            return Err(error(INVALID_INPUT, "repository remote is required"));
        }
        let requested_path = self.workspace_root.join("repos").join(&params.repo_id);
        let path = self.confined_path(&requested_path)?;
        if path != requested_path {
            return Err(error(
                "path_conflict",
                "managed clone path contains a symlink",
            ));
        }
        if path.exists() {
            let origin = local_git(&path, &["remote", "get-url", "origin"]).await;
            let top = local_git(&path, &["rev-parse", "--show-toplevel"]).await;
            if origin.is_err()
                || top.is_err()
                || Path::new(top.as_ref().expect("checked")) != path
                || git::normalize_remote_url(&params.remote_url)
                    != git::normalize_remote_url(origin.as_ref().expect("checked"))
            {
                return Err(error(
                    "path_conflict",
                    "managed clone path holds another repository or non-repository content",
                ));
            }
            // The identity is unchanged, but owner-configured credentials may
            // have rotated. Fetch uses this request's URL, never a stale origin.
            local_git(&path, &["remote", "set-url", "origin", &params.remote_url]).await?;
        } else {
            let requested_staging = self
                .workspace_root
                .join(".forge/provision")
                .join(&params.repo_id);
            let staging = self.confined_path(&requested_staging)?;
            if staging != requested_staging {
                return Err(error(
                    "path_conflict",
                    "private staging path contains a symlink",
                ));
            }
            // A crash can leave only our private staging directory. Retry removes
            // it before cloning; the final path is published atomically.
            if staging.exists() {
                std::fs::remove_dir_all(&staging).map_err(io_error)?;
            }
            std::fs::create_dir_all(staging.parent().expect("staging parent")).map_err(io_error)?;
            let cleanup = RemoveDirectory(staging.clone());
            let mut command = Command::new("git");
            command
                .arg("clone")
                .arg("--branch")
                .arg(&params.default_branch)
                .arg("--")
                .arg(&params.remote_url)
                .arg(&staging)
                .env("GIT_TERMINAL_PROMPT", "0")
                .current_dir(&self.workspace_root)
                .env_remove("GIT_DIR")
                .env_remove("GIT_WORK_TREE")
                .env_remove("GIT_INDEX_FILE");
            let output = bounded_command(command, params.timeout_seconds, 4096, true).await?;
            if output.timed_out || output.exit_code != Some(0) {
                let (message, _) = redacted_tail(&output.stderr, &BTreeMap::new(), 4096);
                return Err(error(
                    "clone_failed",
                    if output.timed_out {
                        "repository clone timed out".to_owned()
                    } else if message.is_empty() {
                        "repository clone failed".to_owned()
                    } else {
                        message
                    },
                ));
            }
            std::fs::create_dir_all(path.parent().expect("clone parent")).map_err(io_error)?;
            std::fs::rename(&staging, &path).map_err(io_error)?;
            drop(cleanup);
        }
        verify_git_dir(&path, &self.workspace_root).await?;
        if resolve_commit(&path, &format!("refs/heads/{}", params.default_branch))
            .await
            .is_err()
        {
            let mut fetch = Command::new("git");
            fetch
                .args([
                    "fetch",
                    "--",
                    "origin",
                    &format!(
                        "refs/heads/{0}:refs/remotes/origin/{0}",
                        params.default_branch
                    ),
                ])
                .current_dir(&path)
                .env("GIT_TERMINAL_PROMPT", "0")
                .env_remove("GIT_DIR")
                .env_remove("GIT_WORK_TREE")
                .env_remove("GIT_INDEX_FILE");
            let remaining = deadline.saturating_duration_since(Instant::now()).as_secs();
            if remaining == 0 {
                return Err(error("clone_failed", "repository fetch timed out"));
            }
            let fetched = bounded_command(fetch, remaining, 4096, true).await?;
            if fetched.timed_out || fetched.exit_code != Some(0) {
                return Err(error(
                    "clone_failed",
                    String::from_utf8_lossy(&fetched.stderr),
                ));
            }
            local_git(
                &path,
                &[
                    "branch",
                    "--no-track",
                    &params.default_branch,
                    &format!("refs/remotes/origin/{}", params.default_branch),
                ],
            )
            .await?;
        }
        let default_branch = params.default_branch;
        Ok(RepoLocationProvisionResult {
            workspace_root: self.workspace_root.to_string_lossy().into_owned(),
            path: path.to_string_lossy().into_owned(),
            default_branch,
        })
    }

    async fn exclude_runtime_metadata(&self, repo_path: &Path) -> CommandResult<()> {
        // When the advertised root is itself a checkout, Forge's own journal
        // and worktrees must not make the user's integration target dirty.
        let git_path = PathBuf::from(
            local_git(repo_path, &["rev-parse", "--git-path", "info/exclude"]).await?,
        );
        let path = self.confined_path(&if git_path.is_absolute() {
            git_path
        } else {
            repo_path.join(git_path)
        })?;
        let text = match tokio::fs::File::open(&path).await {
            Ok(mut file) => {
                use tokio::io::AsyncSeekExt;
                let metadata = file.metadata().await.map_err(io_error)?;
                if !metadata.is_file() {
                    return Err(error(INVALID_INPUT, "git exclude is not a regular file"));
                }
                file.seek(std::io::SeekFrom::Start(
                    metadata.len().saturating_sub(64 * 1024),
                ))
                .await
                .map_err(io_error)?;
                let mut tail = Vec::new();
                file.take(64 * 1024)
                    .read_to_end(&mut tail)
                    .await
                    .map_err(io_error)?;
                String::from_utf8_lossy(&tail).into_owned()
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(error) => return Err(io_error(error)),
        };
        let mut extra = String::new();
        for pattern in ["/.forge/", "/.forge-daemon/"] {
            if !text.lines().any(|line| line == pattern) {
                extra.push('\n');
                extra.push_str(pattern);
                extra.push('\n');
            }
        }
        if !extra.is_empty() {
            use tokio::io::AsyncWriteExt;
            let parent = path
                .parent()
                .ok_or_else(|| error(OUTSIDE_WORKSPACE_ROOT, "exclude file has no parent"))?;
            self.confined_path(parent)?;
            tokio::fs::create_dir_all(parent).await.map_err(io_error)?;
            let mut file = tokio::fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(path)
                .await
                .map_err(io_error)?;
            file.write_all(extra.as_bytes()).await.map_err(io_error)?;
            file.sync_all().await.map_err(io_error)?;
        }
        Ok(())
    }
}
