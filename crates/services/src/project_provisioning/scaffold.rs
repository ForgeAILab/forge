//! Approved Charter scaffold execution, failure cleanup, and exported guidance.

use super::*;

pub(super) enum ScaffoldStep {
    Continue(ProjectProvisioningOperation),
    Failed(ProjectProvisioningOperation),
}

struct ApprovedCharterScaffold {
    revision_id: String,
    content_digest: String,
    rendered_view: String,
    scaffold: CharterScaffold,
}

struct ScaffoldFailure {
    code: &'static str,
    message: String,
    output_tail: String,
}

/// The scaffold named by the Project's current approved Charter revision.
/// Provisioning reads the approved revision, never a draft: amending the
/// Charter and retrying is how a bad template or pack set gets corrected.
async fn load_approved_charter_scaffold(
    db: &Arc<SqliteDb>,
    project: &Project,
) -> Result<Option<ApprovedCharterScaffold>> {
    let Some(charter) =
        ProjectOrchestrationRepo::get_project_charter_by_project_id(&**db, &project.id).await?
    else {
        return Ok(None);
    };
    let Some(revision_id) = charter.current_approved_revision_id.as_deref() else {
        return Ok(None);
    };
    let Some(revision) =
        ProjectOrchestrationRepo::get_project_charter_revision(&**db, revision_id).await?
    else {
        return Ok(None);
    };
    // Read only the block this checkpoint needs. Provisioning must keep
    // working for Charters saved under an earlier or later content schema,
    // so the full `ProjectCharterContent` (which denies unknown fields) is
    // deliberately not the parse target here.
    #[derive(serde::Deserialize)]
    struct ScaffoldOnly {
        #[serde(default)]
        scaffold: Option<CharterScaffold>,
    }
    let content: ScaffoldOnly = serde_json::from_str(&revision.content_json).map_err(|error| {
        ServiceError::invalid_operation(format!(
            "approved Charter revision {revision_id} content is unreadable: {error}"
        ))
    })?;
    Ok(content.scaffold.map(|scaffold| ApprovedCharterScaffold {
        revision_id: revision.id,
        content_digest: revision.content_digest,
        rendered_view: revision.rendered_view,
        scaffold,
    }))
}

/// `FORGE_SCAFFOLD_COMMAND` split on whitespace, falling back to the pinned
/// default when `forge-cli` did not export one (tests, embedded use).
fn scaffold_command() -> Vec<String> {
    std::env::var(SCAFFOLD_COMMAND_ENV)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| config::DEFAULT_SCAFFOLD_COMMAND.to_owned())
        .split_whitespace()
        .map(str::to_owned)
        .collect()
}

/// Stand the repository directory up from the approved Charter's spark
/// template and packs. Idempotent by inspection of the target: an existing
/// scaffold or git repository is never re-run, and a partial directory left
/// by a failed run is removed so the next attempt starts clean. create-spark
/// refuses an existing target, which is why the directory must not exist yet
/// when the command runs.
pub(super) async fn scaffold_repository(
    db: &Arc<SqliteDb>,
    project: &Project,
    operation: ProjectProvisioningOperation,
    lease_owner: &str,
) -> Result<ScaffoldStep> {
    let Some(approved) = load_approved_charter_scaffold(db, project).await? else {
        let operation = skip_checkpoint(
            db,
            operation,
            "repository_scaffolded",
            json!({"reason": "no_scaffold"}),
            lease_owner,
        )
        .await?;
        return Ok(ScaffoldStep::Continue(operation));
    };

    let operation = load_owned_operation(db, &operation, lease_owner).await?;
    let persisted = persisted_repository_path(db, &operation.id).await?;
    let repo_path = resolve_repository_path(db, project, persisted).await?;
    let mut details = json!({
        "path": repo_path.to_string_lossy(),
        "template": approved.scaffold.template,
        "packs": approved.scaffold.packs,
        "charter_revision_id": approved.revision_id,
    });
    // Persist the target before touching the filesystem, like the init
    // checkpoint does, so an interrupted run resumes on the same directory.
    let operation = persist_checkpoint_details(
        db,
        operation,
        "repository_scaffolded",
        details.clone(),
        lease_owner,
    )
    .await?;

    if repo_path.join(SCAFFOLD_MARKER).is_file() {
        details["already_present"] = json!(true);
        let operation =
            complete_checkpoint(db, operation, "repository_scaffolded", details, lease_owner)
                .await?;
        return Ok(ScaffoldStep::Continue(operation));
    }
    if git::is_git_repo(&repo_path).await {
        details["reason"] = json!("existing_repository");
        let operation =
            skip_checkpoint(db, operation, "repository_scaffolded", details, lease_owner).await?;
        return Ok(ScaffoldStep::Continue(operation));
    }
    match tokio::fs::read_dir(&repo_path).await {
        Ok(mut entries) => {
            let occupied = entries
                .next_entry()
                .await
                .map_err(|error| {
                    ServiceError::invalid_operation(format!(
                        "inspect Project repository directory {}: {error}",
                        repo_path.display()
                    ))
                })?
                .is_some();
            if occupied {
                details["output_tail"] = json!(
                    "target directory exists and is neither a spark scaffold nor a git repository"
                );
                let operation = persist_checkpoint_details(
                    db,
                    operation,
                    "repository_scaffolded",
                    details,
                    lease_owner,
                )
                .await?;
                let operation = fail_operation(
                    db,
                    operation,
                    lease_owner,
                    ProvisioningFailure {
                        checkpoint: "repository_scaffolded",
                        code: "repository_scaffold_failed",
                        message: "Project repository directory already exists but is neither a spark scaffold nor a git repository; move it aside and retry",
                        retryable: true,
                        current_checkpoint: "preflight",
                    },
                )
                .await?;
                return Ok(ScaffoldStep::Failed(operation));
            }
            tokio::fs::remove_dir(&repo_path).await.map_err(|error| {
                ServiceError::invalid_operation(format!(
                    "remove empty Project repository directory {}: {error}",
                    repo_path.display()
                ))
            })?;
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => {
            return Err(ServiceError::invalid_operation(format!(
                "inspect Project repository directory {}: {error}",
                repo_path.display()
            )));
        }
    }

    // A repository path always has a parent: `resolve_repository_path`
    // puts it under `<workspace root>/repos` or takes an absolute one.
    let parent = repo_path.parent().map(Path::to_path_buf).ok_or_else(|| {
        ServiceError::invalid_operation(format!(
            "Project repository directory {} has no parent directory",
            repo_path.display()
        ))
    })?;
    tokio::fs::create_dir_all(&parent).await.map_err(|error| {
        ServiceError::invalid_operation(format!(
            "create Project repositories directory {}: {error}",
            parent.display()
        ))
    })?;
    let dir_name = repo_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| repo_directory_name(&project.name, &project.id));
    let command = scaffold_command();
    details["command"] = json!(command.join(" "));

    // Dependency installation can take a while on a cold cache; keep the
    // lease current before the bounded run rather than risking expiry.
    let operation = renew_owned_lease(db, &operation, lease_owner).await?;
    match run_scaffold_command(&command, &parent, &dir_name, &approved.scaffold).await {
        Ok(output_tail) => {
            details["output_tail"] = json!(output_tail);
            write_scaffold_exports(&repo_path, project, &approved).await?;
            let operation =
                complete_checkpoint(db, operation, "repository_scaffolded", details, lease_owner)
                    .await?;
            Ok(ScaffoldStep::Continue(operation))
        }
        Err(failure) => {
            remove_partial_scaffold(&repo_path).await;
            details["output_tail"] = json!(failure.output_tail);
            let operation = persist_checkpoint_details(
                db,
                operation,
                "repository_scaffolded",
                details,
                lease_owner,
            )
            .await?;
            let operation = fail_operation(
                db,
                operation,
                lease_owner,
                ProvisioningFailure {
                    checkpoint: "repository_scaffolded",
                    code: failure.code,
                    message: &failure.message,
                    retryable: true,
                    current_checkpoint: "preflight",
                },
            )
            .await?;
            Ok(ScaffoldStep::Failed(operation))
        }
    }
}

async fn run_scaffold_command(
    command: &[String],
    cwd: &Path,
    dir_name: &str,
    scaffold: &CharterScaffold,
) -> std::result::Result<String, ScaffoldFailure> {
    if let Err(error) = executors::server_commands::ensure_allowed() {
        return Err(ScaffoldFailure {
            code: executors::server_commands::CODE,
            message: error.to_string(),
            output_tail: String::new(),
        });
    }
    let Some((program, base_args)) = command.split_first() else {
        return Err(ScaffoldFailure {
            code: "scaffold_runtime_unavailable",
            message: format!("scaffold command is empty; set {SCAFFOLD_COMMAND_ENV}"),
            output_tail: String::new(),
        });
    };
    let mut args = base_args.to_vec();
    args.push(dir_name.to_owned());
    args.push("--template".to_owned());
    args.push(scaffold.template.clone());
    args.push("--yes".to_owned());
    if scaffold.packs.is_empty() {
        args.push("--no-packs".to_owned());
    } else {
        args.push("--packs".to_owned());
        args.push(scaffold.packs.join(","));
    }

    let mut child = Command::new(program);
    child
        .args(&args)
        .current_dir(cwd)
        .env("NO_COLOR", "1")
        .env("CI", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let output = match tokio::time::timeout(SCAFFOLD_TIMEOUT, child.output()).await {
        Ok(Ok(output)) => output,
        Ok(Err(error)) if error.kind() == ErrorKind::NotFound => {
            return Err(ScaffoldFailure {
                code: "scaffold_runtime_unavailable",
                message: format!(
                    "scaffold command `{program}` is not installed on the Forge host; install bun or set {SCAFFOLD_COMMAND_ENV}"
                ),
                output_tail: String::new(),
            });
        }
        Ok(Err(error)) => {
            return Err(ScaffoldFailure {
                code: "repository_scaffold_failed",
                message: format!("scaffold command `{program}` could not start: {error}"),
                output_tail: String::new(),
            });
        }
        Err(_) => {
            return Err(ScaffoldFailure {
                code: "repository_scaffold_failed",
                message: format!(
                    "scaffold command exceeded {} seconds and was stopped",
                    SCAFFOLD_TIMEOUT.as_secs()
                ),
                output_tail: String::new(),
            });
        }
    };
    let output_tail = scaffold_output_tail(&output.stdout, &output.stderr);
    if output.status.success() {
        return Ok(output_tail);
    }
    let last_line = String::from_utf8_lossy(&output.stderr)
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| format!("exit status {}", output.status));
    Err(ScaffoldFailure {
        code: "repository_scaffold_failed",
        message: format!("create-spark failed: {last_line}"),
        output_tail,
    })
}

fn scaffold_output_tail(stdout: &[u8], stderr: &[u8]) -> String {
    let combined = format!(
        "{}\n{}",
        String::from_utf8_lossy(stdout).trim_end(),
        String::from_utf8_lossy(stderr).trim_end()
    );
    let combined = combined.trim();
    let skip = combined
        .chars()
        .count()
        .saturating_sub(SCAFFOLD_OUTPUT_TAIL_CHARS);
    combined.chars().skip(skip).collect()
}

/// A directory the failed run created but never turned into a git repository
/// is this operation's own debris; leaving it would make every retry fail on
/// "target directory already exists".
async fn remove_partial_scaffold(repo_path: &Path) {
    if !repo_path.exists() || repo_path.join(".git").exists() {
        return;
    }
    if let Err(error) = tokio::fs::remove_dir_all(repo_path).await {
        tracing::warn!(
            path = %repo_path.display(),
            error = %error,
            "partial spark scaffold could not be removed; the next attempt will report it"
        );
    }
}

/// Put Forge's truth into the scaffold before the first commit: the approved
/// Charter as `docs/spark/project.md` (an export, stamped with its revision)
/// and a Forge section in `AGENTS.md` so every Task Worker reading the
/// repository knows planning and tracking live in Forge.
async fn write_scaffold_exports(
    repo_path: &Path,
    project: &Project,
    approved: &ApprovedCharterScaffold,
) -> Result<()> {
    let docs_dir = repo_path.join("docs").join("spark");
    tokio::fs::create_dir_all(&docs_dir)
        .await
        .map_err(|error| {
            ServiceError::invalid_operation(format!(
                "create {} in the scaffold: {error}",
                docs_dir.display()
            ))
        })?;
    let project_md = format!(
        "# {}\n\n> Exported by Forge from the approved Project Charter (revision `{}`, content digest `{}`).\n> Forge is the source of truth for this Project: Tasks, reviews, milestones, and releases live in Forge, not in `docs/spark/changes/`.\n\n{}",
        project.name, approved.revision_id, approved.content_digest, approved.rendered_view
    );
    tokio::fs::write(docs_dir.join("project.md"), project_md)
        .await
        .map_err(|error| {
            ServiceError::invalid_operation(format!(
                "write docs/spark/project.md in the scaffold: {error}"
            ))
        })?;

    let agents_path = repo_path.join("AGENTS.md");
    let mut agents = match tokio::fs::read_to_string(&agents_path).await {
        Ok(existing) => existing,
        Err(error) if error.kind() == ErrorKind::NotFound => String::new(),
        Err(error) => {
            return Err(ServiceError::invalid_operation(format!(
                "read AGENTS.md in the scaffold: {error}"
            )));
        }
    };
    if !agents.contains("\n## Forge\n") && !agents.starts_with("## Forge\n") {
        if !agents.is_empty() && !agents.ends_with('\n') {
            agents.push('\n');
        }
        if !agents.is_empty() {
            agents.push('\n');
        }
        agents.push_str(FORGE_AGENTS_SECTION);
        tokio::fs::write(&agents_path, agents)
            .await
            .map_err(|error| {
                ServiceError::invalid_operation(format!("write AGENTS.md in the scaffold: {error}"))
            })?;
    }
    Ok(())
}
