use executors::ExecutorError;
use std::collections::BTreeMap;
use std::path::Path;
use tokio::process::Command;

const COMMIT_PREFIX: &str = "agent: ";
const SUBJECT_TEXT_LIMIT: usize = 72;
const DISABLED_HOOKS_PATH: &str = "/dev/null";

/// Chat snapshots disable Task finalization before entering a CLI adapter.
pub(crate) fn auto_commit_enabled(ctx: &executors::ExecutionContext) -> bool {
    ctx.agent_config
        .get("auto_commit")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(true)
}

pub async fn commit_worktree_changes(
    worktree: &Path,
    message_subject: &str,
) -> Result<Option<String>, ExecutorError> {
    if is_worktree_clean_for_finalization(worktree).await? {
        return Ok(None);
    }

    run_git(worktree, &["add", "-A"]).await?;
    // A built-in normalization rule (for example `text eol=lf`) can make a
    // path look dirty before `add` while producing no staged delta. Avoid the
    // no-op commit path: Git's error reporting runs an internal status whose
    // per-submodule config could otherwise re-enable nested worktree scans.
    if !has_staged_changes(worktree).await? {
        return Ok(None);
    }
    run_git(
        worktree,
        &[
            "-c",
            "user.email=agent@forge.local",
            "-c",
            "user.name=Forge Agent",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-m",
            message_subject,
        ],
    )
    .await?;

    let sha = run_git(worktree, &["rev-parse", "HEAD"]).await?;
    Ok(Some(sha.trim().to_owned()))
}

/// Check finalization cleanliness without running repository-defined hooks,
/// monitors, filters, nested submodule status, maintenance, or lazy fetches.
pub async fn is_worktree_clean_for_finalization(worktree: &Path) -> Result<bool, ExecutorError> {
    reject_external_checkin_filters(worktree).await?;
    let status = run_git(
        worktree,
        &[
            "status",
            "--porcelain",
            "--untracked-files=all",
            "--ignore-submodules=dirty",
        ],
    )
    .await?;
    Ok(status.trim().is_empty())
}

/// Commit whatever an execution left in its worktree — unless the execution
/// runs under the read-only worktree policy (reviewer and planner roles,
/// planning/discovery Tasks). A read-only run never authors work; what it
/// leaves behind is build output from the checks it ran, and committing that
/// would move HEAD and make the runner discard the run as a policy breach.
pub async fn commit_execution_changes(
    ctx: &executors::ExecutionContext,
) -> Result<Option<String>, ExecutorError> {
    commit_execution_changes_with_policy(ctx, false).await
}

pub(crate) async fn commit_execution_changes_with_policy(
    ctx: &executors::ExecutionContext,
    force_host_finalization: bool,
) -> Result<Option<String>, ExecutorError> {
    if (!force_host_finalization && !auto_commit_enabled(ctx))
        || executors::is_worktree_read_only(&ctx.agent_config)
    {
        return Ok(None);
    }
    let subject = build_commit_subject(Some(&ctx.description), &ctx.task_id);
    commit_worktree_changes(Path::new(&ctx.worktree_path), &subject).await
}

pub fn build_commit_subject(task_description: Option<&str>, fallback_title: &str) -> String {
    let subject_text = task_description
        .and_then(task_subject_line)
        .or_else(|| first_non_empty_line(fallback_title))
        .unwrap_or("task");
    format!(
        "{COMMIT_PREFIX}{}",
        truncate_chars(subject_text, SUBJECT_TEXT_LIMIT)
    )
}

fn task_subject_line(value: &str) -> Option<&str> {
    value
        .lines()
        .find_map(|line| line.trim().strip_prefix("Task:").map(str::trim))
        .filter(|line| !line.is_empty())
        .or_else(|| first_non_empty_line(value))
}

async fn run_git(worktree: &Path, args: &[&str]) -> Result<String, ExecutorError> {
    let output = hardened_git_command(worktree).args(args).output().await?;
    if !output.status.success() {
        return Err(git_command_error(worktree, args, &output));
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

async fn has_staged_changes(worktree: &Path) -> Result<bool, ExecutorError> {
    let args = [
        "diff",
        "--cached",
        "--quiet",
        "--no-ext-diff",
        "--no-textconv",
        "--ignore-submodules=dirty",
        "--exit-code",
    ];
    let output = hardened_git_command(worktree).args(args).output().await?;
    match output.status.code() {
        Some(0) => Ok(false),
        Some(1) => Ok(true),
        _ => Err(git_command_error(worktree, &args, &output)),
    }
}

/// Build every trusted finalization command with repository-defined program
/// launch points disabled. A Task may author files under its worktree, so
/// hooks, filesystem monitors, maintenance, rerere, nested submodule status,
/// or a partial-clone fetch can otherwise escape the Task sandbox during the
/// host-side status/add/commit sequence.
fn hardened_git_command(worktree: &Path) -> Command {
    let mut command = Command::new("git");
    command
        .arg("-C")
        .arg(worktree)
        .arg("-c")
        .arg(format!("core.hooksPath={DISABLED_HOOKS_PATH}"))
        .arg("-c")
        .arg("core.fsmonitor=false")
        .arg("-c")
        .arg("diff.ignoreSubmodules=dirty")
        .arg("-c")
        .arg("maintenance.auto=false")
        .arg("-c")
        .arg("gc.auto=0")
        .arg("-c")
        .arg("rerere.enabled=false")
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .env_remove("GIT_COMMON_DIR")
        .env_remove("GIT_OBJECT_DIRECTORY")
        .env_remove("GIT_ALTERNATE_OBJECT_DIRECTORIES")
        .env_remove("GIT_CONFIG")
        .env_remove("GIT_CONFIG_COUNT")
        .env_remove("GIT_CONFIG_PARAMETERS")
        .env_remove("GIT_EXTERNAL_DIFF")
        .env_remove("GIT_DIFF_OPTS")
        .env("GIT_NO_LAZY_FETCH", "1");
    command
}

/// Clean and long-running process filters are arbitrary commands invoked by
/// `git status` and `git add`. Silently overriding them would change the bytes
/// stored by repositories that intentionally use LFS, encryption, or another
/// canonicalization filter, so automatic finalization fails closed instead.
async fn reject_external_checkin_filters(worktree: &Path) -> Result<(), ExecutorError> {
    let args = ["config", "--null", "--list"];
    let output = hardened_git_command(worktree).args(args).output().await?;
    if !output.status.success() {
        return Err(ExecutorError::Other(format!(
            "automatic Git finalization could not inspect repository filter configuration in {}: status {}; stderr: {}",
            worktree.display(),
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }

    let executable_filters = configured_external_checkin_filters(&output.stdout)?;
    if executable_filters.is_empty() {
        return Ok(());
    }

    Err(ExecutorError::Other(format!(
        "automatic Git finalization refused configured external check-in filters: {}; commit the Task worktree manually or remove the filter configuration",
        executable_filters.join(", ")
    )))
}

fn configured_external_checkin_filters(configured: &[u8]) -> Result<Vec<String>, ExecutorError> {
    let configured = std::str::from_utf8(configured).map_err(|_| {
        ExecutorError::Other(
            "automatic Git finalization refused non-UTF-8 Git configuration output".to_owned(),
        )
    })?;
    Ok(configured
        .split('\0')
        .filter_map(|entry| entry.split_once('\n'))
        .fold(BTreeMap::new(), |mut values, (key, value)| {
            // Git filter subsection names are case-sensitive even though the
            // section and terminal variable names are not. Preserve the key
            // returned by Git so distinct drivers cannot overwrite each other.
            values.insert(key.to_owned(), value);
            values
        })
        .into_iter()
        .filter(|(key, value)| is_external_checkin_filter(key, value))
        .map(|(key, _)| key)
        .collect())
}

fn is_external_checkin_filter(key: &str, value: &str) -> bool {
    let key = key.to_ascii_lowercase();
    !value.is_empty()
        && key.starts_with("filter.")
        && (key.ends_with(".clean") || key.ends_with(".process"))
}

fn git_command_error(
    worktree: &Path,
    args: &[&str],
    output: &std::process::Output,
) -> ExecutorError {
    ExecutorError::Other(format!(
        "git -C {} {} failed with status {}\nstdout: {}\nstderr: {}",
        worktree.display(),
        args.join(" "),
        output.status,
        String::from_utf8_lossy(&output.stdout).trim(),
        String::from_utf8_lossy(&output.stderr).trim()
    ))
}

fn first_non_empty_line(value: &str) -> Option<&str> {
    value.lines().map(str::trim).find(|line| !line.is_empty())
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    #[tokio::test]
    async fn commits_dirty_worktree_and_returns_new_sha() {
        let tempdir = tempfile::tempdir().expect("tempdir creates");
        init_repo(tempdir.path()).await;
        let before = git_output(tempdir.path(), &["rev-parse", "HEAD"]).await;

        fs::write(tempdir.path().join("changed.txt"), "changed\n").expect("file writes");

        let subject = build_commit_subject(Some("Implement the thing\n\nDetails"), "Fallback");
        let sha = commit_worktree_changes(tempdir.path(), &subject)
            .await
            .expect("commit succeeds")
            .expect("dirty worktree creates commit");
        let log_subject = git_output(tempdir.path(), &["log", "-1", "--pretty=%s"]).await;

        assert_ne!(sha, before);
        assert_eq!(log_subject, "agent: Implement the thing");
    }

    #[tokio::test]
    async fn clean_worktree_returns_none() {
        let tempdir = tempfile::tempdir().expect("tempdir creates");
        init_repo(tempdir.path()).await;

        let sha = commit_worktree_changes(tempdir.path(), "agent: noop")
            .await
            .expect("clean check succeeds");

        assert_eq!(sha, None);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn finalization_disables_repository_hooks_and_fsmonitor_commands() {
        let tempdir = tempfile::tempdir().expect("tempdir creates");
        init_repo(tempdir.path()).await;
        let before = git_output(tempdir.path(), &["rev-parse", "HEAD"]).await;
        let hooks = tempdir.path().join(".forge-hooks");
        fs::create_dir(&hooks).expect("hooks directory creates");
        let hook_marker = tempdir.path().join("hook-ran");
        let fsmonitor_marker = tempdir.path().join("fsmonitor-ran");
        write_executable(
            &hooks.join("pre-commit"),
            &format!("#!/bin/sh\nprintf ran > \"{}\"\n", hook_marker.display()),
        );
        write_executable(
            &tempdir.path().join("fsmonitor.sh"),
            &format!(
                "#!/bin/sh\nprintf ran > \"{}\"\nprintf 'token\\n'\n",
                fsmonitor_marker.display()
            ),
        );
        git_output(
            tempdir.path(),
            &["config", "core.hooksPath", ".forge-hooks"],
        )
        .await;
        git_output(
            tempdir.path(),
            &["config", "core.fsmonitor", "./fsmonitor.sh"],
        )
        .await;
        fs::write(tempdir.path().join("changed.rs"), "fn changed() {}\n")
            .expect("changed file writes");

        let sha = commit_worktree_changes(tempdir.path(), "agent: hardened finalization")
            .await
            .expect("hardened commit succeeds")
            .expect("dirty worktree creates commit");

        assert_ne!(sha, before);
        assert!(!hook_marker.exists(), "repository hook must not execute");
        assert!(
            !fsmonitor_marker.exists(),
            "repository fsmonitor command must not execute"
        );
    }

    #[tokio::test]
    async fn finalization_finds_untracked_files_hidden_by_status_config() {
        let tempdir = tempfile::tempdir().expect("tempdir creates");
        init_repo(tempdir.path()).await;
        let before = git_output(tempdir.path(), &["rev-parse", "HEAD"]).await;
        git_output(
            tempdir.path(),
            &["config", "status.showUntrackedFiles", "no"],
        )
        .await;
        fs::write(tempdir.path().join("new-file.rs"), "fn delivered() {}\n")
            .expect("untracked file writes");

        let sha = commit_worktree_changes(tempdir.path(), "agent: include untracked file")
            .await
            .expect("hardened commit succeeds")
            .expect("explicit untracked discovery creates commit");

        assert_ne!(sha, before);
        assert_eq!(
            git_output(tempdir.path(), &["show", "HEAD:new-file.rs"]).await,
            "fn delivered() {}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn finalization_fails_closed_before_running_checkin_filters() {
        let tempdir = tempfile::tempdir().expect("tempdir creates");
        init_repo(tempdir.path()).await;
        let before = git_output(tempdir.path(), &["rev-parse", "HEAD"]).await;
        let filter_marker = tempdir.path().join("filter-ran");
        write_executable(
            &tempdir.path().join("clean-filter.sh"),
            &format!(
                "#!/bin/sh\nprintf ran > \"{}\"\ncat\n",
                filter_marker.display()
            ),
        );
        git_output(
            tempdir.path(),
            &["config", "filter.worker.clean", "./clean-filter.sh"],
        )
        .await;
        git_output(
            tempdir.path(),
            &["config", "filter.worker.required", "true"],
        )
        .await;
        fs::write(
            tempdir.path().join(".gitattributes"),
            "*.txt filter=worker\n",
        )
        .expect("attributes write");
        fs::write(tempdir.path().join("changed.txt"), "untrusted input\n")
            .expect("filtered file writes");

        let error = commit_worktree_changes(tempdir.path(), "agent: rejected filter")
            .await
            .expect_err("external check-in filter must fail closed");

        assert!(error.to_string().contains("filter.worker.clean"));
        assert!(!filter_marker.exists(), "clean filter must not execute");
        assert_eq!(
            git_output(tempdir.path(), &["rev-parse", "HEAD"]).await,
            before
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn finalization_ignores_submodule_dirt_but_commits_gitlink_updates() {
        let tempdir = tempfile::tempdir().expect("tempdir creates");
        let superproject = tempdir.path().join("superproject");
        let submodule_source = tempdir.path().join("submodule-source");
        fs::create_dir(&superproject).expect("superproject directory creates");
        fs::create_dir(&submodule_source).expect("submodule source directory creates");
        init_repo(&superproject).await;
        init_repo(&submodule_source).await;
        git_output(
            &superproject,
            &[
                "-c",
                "protocol.file.allow=always",
                "submodule",
                "add",
                submodule_source.to_str().expect("test path is UTF-8"),
                "deps/child",
            ],
        )
        .await;
        fs::write(
            superproject.join(".gitattributes"),
            "README.md text eol=lf\n",
        )
        .expect("attributes write");
        git_output(&superproject, &["add", ".gitattributes"]).await;
        git_output(
            &superproject,
            &[
                "-c",
                "user.email=agent@forge.local",
                "-c",
                "user.name=Forge Agent",
                "-c",
                "commit.gpgsign=false",
                "commit",
                "-m",
                "add submodule",
            ],
        )
        .await;
        git_output(
            &superproject,
            &["config", "submodule.deps/child.ignore", "none"],
        )
        .await;

        let child = superproject.join("deps/child");
        let fsmonitor_marker = tempdir.path().join("submodule-fsmonitor-ran");
        write_executable(
            &child.join("fsmonitor.sh"),
            &format!(
                "#!/bin/sh\nprintf ran > \"{}\"\nprintf 'token\\n'\n",
                fsmonitor_marker.display()
            ),
        );
        git_output(
            &child,
            &[
                "config",
                "core.fsmonitor",
                child
                    .join("fsmonitor.sh")
                    .to_str()
                    .expect("test path is UTF-8"),
            ],
        )
        .await;
        fs::write(child.join("README.md"), "dirty submodule worktree\n")
            .expect("submodule file writes");
        fs::write(superproject.join("README.md"), "initial\r\n")
            .expect("normalizable superproject file writes");
        let before = git_output(&superproject, &["rev-parse", "HEAD"]).await;

        let unchanged = commit_worktree_changes(&superproject, "agent: normalize without a commit")
            .await
            .expect("no-op normalization avoids commit fallback status");

        assert_eq!(unchanged, None);
        assert_eq!(
            git_output(&superproject, &["rev-parse", "HEAD"]).await,
            before
        );
        assert!(
            !fsmonitor_marker.exists(),
            "submodule fsmonitor must not execute while checking dirt"
        );

        git_output(&child, &["-c", "core.fsmonitor=false", "add", "README.md"]).await;
        git_output(
            &child,
            &[
                "-c",
                "core.fsmonitor=false",
                "-c",
                "user.email=agent@forge.local",
                "-c",
                "user.name=Forge Agent",
                "-c",
                "commit.gpgsign=false",
                "commit",
                "-m",
                "advance submodule",
            ],
        )
        .await;
        let advanced = commit_worktree_changes(&superproject, "agent: update gitlink")
            .await
            .expect("gitlink update finalizes")
            .expect("changed gitlink creates commit");

        assert_ne!(advanced, before);
        assert!(
            !fsmonitor_marker.exists(),
            "submodule fsmonitor must not execute while finalizing a gitlink"
        );
    }

    #[test]
    fn build_commit_subject_uses_first_non_empty_line_and_truncates() {
        let long = format!("{}\nsecond line", "a".repeat(80));

        let subject = build_commit_subject(Some(&long), "Fallback");

        assert_eq!(subject, format!("agent: {}", "a".repeat(72)));
    }

    #[test]
    fn build_commit_subject_prefers_task_title_in_combined_role_prompt() {
        let description = "Forge role contract (authoritative):\nWorker rules\n\nExecution request:\nTask: Implement Tiny Notes CLI\n\nObjective:\nBuild it";

        let subject = build_commit_subject(Some(description), "task-123");

        assert_eq!(subject, "agent: Implement Tiny Notes CLI");
    }

    #[test]
    fn build_commit_subject_falls_back_for_empty_description() {
        let subject = build_commit_subject(None, "Fallback title");

        assert_eq!(subject, "agent: Fallback title");
    }

    #[test]
    fn build_commit_subject_falls_back_for_whitespace_description() {
        let subject = build_commit_subject(Some(" \n\t\n"), "Fallback title");

        assert_eq!(subject, "agent: Fallback title");
    }

    #[test]
    fn external_checkin_filter_detection_covers_clean_and_process_only() {
        assert!(is_external_checkin_filter("filter.worker.clean", "run"));
        assert!(is_external_checkin_filter("FILTER.worker.PROCESS", "run"));
        assert!(!is_external_checkin_filter("filter.worker.clean", ""));
        assert!(!is_external_checkin_filter("filter.worker.smudge", "run"));
        assert!(!is_external_checkin_filter("diff.worker.command", "run"));
    }

    #[test]
    fn filter_scan_keeps_case_sensitive_driver_names_distinct() {
        let configured = "filter.Worker.clean\nrun-worker\0filter.worker.clean\n\0";
        let executable_filters =
            configured_external_checkin_filters(configured.as_bytes()).expect("config parses");

        assert_eq!(executable_filters, ["filter.Worker.clean"]);
    }

    #[test]
    fn filter_scan_fails_closed_on_non_utf8_driver_identity() {
        let configured = b"filter.worker.clean\nrun\0filter.w\xffrker.clean\n\0";

        let error = configured_external_checkin_filters(configured)
            .expect_err("non-UTF-8 config identity must fail closed");

        assert!(error.to_string().contains("non-UTF-8"));
    }

    #[test]
    fn hardened_git_environment_cannot_redirect_only_the_config_scan() {
        let command = hardened_git_command(Path::new("/tmp/worktree"));
        let arguments = command
            .as_std()
            .get_args()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        let removed = command
            .as_std()
            .get_envs()
            .filter_map(|(key, value)| {
                value
                    .is_none()
                    .then_some(key.to_string_lossy().into_owned())
            })
            .collect::<std::collections::BTreeSet<_>>();

        assert!(removed.contains("GIT_CONFIG"));
        assert!(removed.contains("GIT_CONFIG_COUNT"));
        assert!(removed.contains("GIT_CONFIG_PARAMETERS"));
        assert!(
            arguments
                .iter()
                .any(|arg| arg == "diff.ignoreSubmodules=dirty")
        );
        assert!(arguments.iter().any(|arg| arg == "maintenance.auto=false"));
        assert!(arguments.iter().any(|arg| arg == "gc.auto=0"));
        assert!(arguments.iter().any(|arg| arg == "rerere.enabled=false"));
        assert_eq!(
            command.as_std().get_envs().find_map(|(key, value)| {
                (key == "GIT_NO_LAZY_FETCH").then(|| value.and_then(|value| value.to_str()))
            }),
            Some(Some("1"))
        );
    }

    async fn init_repo(path: &Path) {
        git_output(path, &["init", "-b", "main"]).await;
        fs::write(path.join("README.md"), "initial\n").expect("readme writes");
        git_output(path, &["add", "README.md"]).await;
        git_output(
            path,
            &[
                "-c",
                "user.email=agent@forge.local",
                "-c",
                "user.name=Forge Agent",
                "-c",
                "commit.gpgsign=false",
                "commit",
                "-m",
                "initial",
            ],
        )
        .await;
    }

    async fn git_output(path: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .arg("-C")
            .arg(path)
            .args(args)
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .output()
            .await
            .expect("git command runs");
        assert!(
            output.status.success(),
            "git -C {} {} failed\nstdout: {}\nstderr: {}",
            path.display(),
            args.join(" "),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).trim().to_owned()
    }

    #[cfg(unix)]
    fn write_executable(path: &Path, body: &str) {
        fs::write(path, body).expect("script writes");
        let mut permissions = fs::metadata(path).expect("script metadata").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).expect("script becomes executable");
    }
}

#[cfg(test)]
mod read_only_tests {
    use super::*;
    use std::fs;
    use std::path::Path;

    fn git(worktree: &Path, args: &[&str]) -> String {
        let output = std::process::Command::new("git")
            .args(args)
            .current_dir(worktree)
            .output()
            .expect("git runs");
        assert!(output.status.success(), "git {args:?} failed");
        String::from_utf8_lossy(&output.stdout).trim().to_owned()
    }

    #[tokio::test]
    async fn chat_completion_does_not_require_a_git_repository() {
        let sandbox = tempfile::tempdir().expect("chat sandbox");
        let chat = context(sandbox.path(), serde_json::json!({"auto_commit": false}));
        assert_eq!(
            commit_execution_changes(&chat)
                .await
                .expect("chat skips Git"),
            None
        );
        let task = context(sandbox.path(), serde_json::json!({}));
        assert!(
            commit_execution_changes(&task).await.is_err(),
            "Task finalization still requires its worktree"
        );
    }

    fn context(worktree: &Path, agent_config: serde_json::Value) -> executors::ExecutionContext {
        executors::ExecutionContext {
            task_id: "task-1".to_owned(),
            execution_id: "exec-1".to_owned(),
            worktree_path: worktree.to_string_lossy().into_owned(),
            description: "Review task: keep the worktree intact".to_owned(),
            agent_config,
            logs_path: worktree.join("logs").to_string_lossy().into_owned(),
            heartbeat_interval_seconds: 30,
            max_turns: None,
            log_sender: None,
        }
    }

    fn init_repo(worktree: &Path) {
        git(worktree, &["init", "-q", "-b", "main"]);
        git(worktree, &["config", "user.email", "test@forge.local"]);
        git(worktree, &["config", "user.name", "Forge Test"]);
        fs::write(worktree.join("README.md"), "hello\n").expect("file writes");
        git(worktree, &["add", "-A"]);
        git(worktree, &["commit", "-q", "-m", "init"]);
    }

    #[tokio::test]
    async fn read_only_executions_never_commit_what_their_checks_leave_behind() {
        let tempdir = tempfile::tempdir().expect("tempdir creates");
        init_repo(tempdir.path());
        let head = git(tempdir.path(), &["rev-parse", "HEAD"]);
        fs::write(tempdir.path().join("next-env.d.ts"), "// build output\n").expect("file writes");

        let mut config = serde_json::json!({ "executor_type": "opencode" });
        executors::mark_worktree_read_only(&mut config);
        let sha = commit_execution_changes(&context(tempdir.path(), config))
            .await
            .expect("read-only path never fails");

        assert_eq!(sha, None);
        assert_eq!(git(tempdir.path(), &["rev-parse", "HEAD"]), head);
        assert!(git(tempdir.path(), &["status", "--porcelain"]).contains("next-env.d.ts"));
    }

    #[tokio::test]
    async fn writable_executions_still_commit_their_work() {
        let tempdir = tempfile::tempdir().expect("tempdir creates");
        init_repo(tempdir.path());
        let head = git(tempdir.path(), &["rev-parse", "HEAD"]);
        fs::write(tempdir.path().join("lib.rs"), "fn main() {}\n").expect("file writes");

        let config = serde_json::json!({ "executor_type": "opencode" });
        let sha = commit_execution_changes(&context(tempdir.path(), config))
            .await
            .expect("commit succeeds");

        assert!(sha.is_some());
        assert_ne!(git(tempdir.path(), &["rev-parse", "HEAD"]), head);
        assert!(git(tempdir.path(), &["status", "--porcelain"]).is_empty());
    }

    #[tokio::test]
    async fn managed_writable_execution_forces_host_finalization_when_profile_disables_it() {
        let tempdir = tempfile::tempdir().expect("tempdir creates");
        init_repo(tempdir.path());
        let head = git(tempdir.path(), &["rev-parse", "HEAD"]);
        fs::write(tempdir.path().join("lib.rs"), "fn main() {}\n").expect("file writes");

        let mut config = serde_json::json!({ "auto_commit": false });
        executors::mark_task_role(&mut config, "worker");
        let sha = commit_execution_changes_with_policy(&context(tempdir.path(), config), true)
            .await
            .expect("managed host commit succeeds");

        assert!(sha.is_some());
        assert_ne!(git(tempdir.path(), &["rev-parse", "HEAD"]), head);
        assert!(git(tempdir.path(), &["status", "--porcelain"]).is_empty());
    }

    #[test]
    fn unmanaged_and_managed_read_only_runs_honor_disabled_auto_commit() {
        let sandbox = tempfile::tempdir().expect("sandbox creates");
        let unmanaged = context(sandbox.path(), serde_json::json!({ "auto_commit": false }));
        assert!(!auto_commit_enabled(&unmanaged));

        let mut managed_write = serde_json::json!({ "auto_commit": false });
        executors::mark_task_role(&mut managed_write, "worker");
        assert!(
            !auto_commit_enabled(&context(sandbox.path(), managed_write)),
            "shared CLI policy must not widen non-Codex adapters"
        );

        let mut read_only = serde_json::json!({ "auto_commit": false });
        executors::mark_task_role(&mut read_only, "reviewer");
        executors::mark_worktree_read_only(&mut read_only);
        assert!(!auto_commit_enabled(&context(sandbox.path(), read_only)));
    }
}
