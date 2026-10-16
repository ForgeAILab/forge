//! The verify continuation of RunCiSteps. Only the durable runner executes
//! commands; this hook prepares inputs, waits, and reads its certified receipt.
use super::common::workspace_id;
use crate::{
    check_runner::consumer::{CheckWaitState, TaskCheckConsumers, TaskCheckRequest},
    workflow::HookContext,
};
use api_types::{ReviewVerifyCheckResult, ReviewVerifyDetails, ReviewVerifyStatus as Status};
use db::{CheckDeliveryRepo, WorkspaceRepo};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    path::{Component, Path},
};

const EFFECT: &str = "review_verify";
pub(super) enum Error {
    Refused(String),
    Infrastructure(String),
}
impl From<String> for Error {
    fn from(reason: String) -> Self {
        Self::Refused(reason)
    }
}
impl From<&str> for Error {
    fn from(reason: &str) -> Self {
        Self::Refused(reason.into())
    }
}
fn infrastructure(error: impl std::fmt::Display) -> Error {
    Error::Infrastructure(error.to_string())
}

pub(super) enum Progress {
    Waiting,
    Complete(ReviewVerifyDetails),
}

pub(super) async fn run(
    ctx: &HookContext,
    task: &db::Task,
    review: &db::Review,
    consumers: &TaskCheckConsumers,
) -> Result<Progress, Error> {
    let config = review::verify::config(&ctx.state_config)?.ok_or("verify config disappeared")?;
    let workspace_id = workspace_id(ctx).await.ok_or("verify needs a workspace")?;
    let workspace = WorkspaceRepo::get_by_id(&*ctx.db, &workspace_id)
        .await
        .map_err(infrastructure)?
        .ok_or("verify workspace disappeared")?;
    let placement = db::WorkspacePlacementRepo::get_by_workspace_id(&*ctx.db, &workspace.id)
        .await
        .map_err(infrastructure)?;
    if placement
        .as_ref()
        .is_some_and(|placement| placement.owner_kind == db::PlacementOwnerKind::Daemon)
    {
        return Ok(Progress::Complete(ReviewVerifyDetails {
            status: Status::NotRun,
            reason: Some("placement_unsupported".into()),
            checks: vec![],
            app_log_tail: String::new(),
            notes: vec![],
        }));
    }
    let resolved = ctx
        .workspace_backend_router
        .resolve(&ctx.db, &workspace)
        .await
        .map_err(infrastructure)?;
    // The worktree path comes from the workspace manager's read-only check,
    // never from the raw placement handle.
    let path = crate::workspace_manager::inspect_path(
        &ctx.db,
        &ctx.workspace_root,
        &ctx.workspace_backend_router,
        task,
        workspace.clone(),
    )
    .await
    .map_err(infrastructure)?
    .ok_or_else(|| infrastructure("verify workspace has no usable worktree"))?;
    let directory = review::verify::directory(&path, &review.id)?;
    review::verify::ensure_directory(&directory, &path)?;
    let saved = crate::workflow::engine::durable::hook_effect(&task.id, EFFECT)
        .await
        .map_err(infrastructure)?
        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
        .filter(|v| v["review_id"] == review.id);
    let mut evaluation = saved
        .as_ref()
        .and_then(|v| v["evaluation"].as_u64())
        .unwrap_or(0) as u32;
    let mut consumer = saved
        .as_ref()
        .and_then(|v| v["consumer_id"].as_str())
        .map(str::to_owned);
    let mut port_warnings: Vec<String> = saved
        .as_ref()
        .and_then(|v| serde_json::from_value(v["port_warnings"].clone()).ok())
        .unwrap_or_default();
    loop {
        if consumer.is_none() {
            if evaluation == 0 {
                db::budget::initialize_verify_budget(&ctx.db, task, &review.id)
                    .await
                    .map_err(infrastructure)?;
            }
            // The request is durable before any check can execute. Frozen Task
            // requirements and the same merge-base rule as reviewer admission.
            let context =
                review::contract::load_context(&ctx.db, &task.id, Some(&review.execution_id))
                    .await?;
            let base_sha = review::contract::review_base(&path, &context).await?;
            let commit_sha = git::get_current_sha(&path).await.map_err(infrastructure)?;
            let changed_files =
                review::contract::candidate_changed_paths(&path, &base_sha, &commit_sha).await?;
            let ledger = db::review_findings::finding_ledger(
                &ctx.db
                    .review_window(&task.id)
                    .await
                    .map_err(infrastructure)?
                    .reviews,
            )?;
            let request = json!({"task_id":task.id,"title":task.title,"description":task.description,
                "requirements": context.requirements.iter().map(|r| &r.text).collect::<Vec<_>>(),
                "base_sha":base_sha,"commit_sha":commit_sha,"changed_files":changed_files,
                "findings":ledger.into_iter().filter(|f| f.status.is_open()).collect::<Vec<_>>()});
            let request_path = directory.join("request.json");
            if request_path
                .symlink_metadata()
                .is_ok_and(|m| !m.is_file() || m.file_type().is_symlink())
            {
                return Err("verify request is not a regular file".into());
            }
            std::fs::write(
                request_path,
                serde_json::to_vec_pretty(&request).map_err(infrastructure)?,
            )
            .map_err(infrastructure)?;
            let environment = review::contract::project_environment(&ctx.db, &task.id).await?;
            let spec = review::verify::build_bundle(
                &config,
                &workspace.id,
                u64::try_from(resolved.placement.generation).map_err(infrastructure)?,
                &review.id,
                evaluation,
                environment.env.keys().cloned(),
            )?;
            let mut inputs =
                crate::check_runner::policy::digest_input(&ctx.db, spec, &environment.env)
                    .await
                    .map_err(infrastructure)?;
            // Verify never enters the reusable identity path, even when the
            // surrounding CI observation remains reusable.
            inputs.environment_identity = api_types::CheckEnvironmentIdentity::NotAttested;
            let requested = consumers
                .request(TaskCheckRequest {
                    task_id: task.id.clone(),
                    status_epoch: CheckDeliveryRepo::live_task_epoch(&*ctx.db, &task.id)
                        .await
                        .map_err(infrastructure)?
                        .ok_or("Task disappeared")?,
                    authority: format!("review-verify:{evaluation}:{}", review.id),
                    origin: db::CheckConsumerOrigin::Entry,
                    purpose: api_types::CheckPurpose::ReviewVerify,
                    identity: db::CheckRunIdentity {
                        project_id: task.project_id.clone(),
                        repo_id: workspace.repo_id.clone(),
                        commit_sha,
                        inputs,
                    },
                    workspace_id: Some(workspace.id.clone()),
                    machine_id: None,
                    wall_timeout_seconds: u64::from(config.timeout_seconds.unwrap_or(1200)),
                })
                .await
                .map_err(infrastructure)?;
            consumer = Some(requested.consumer.id);
            crate::workflow::engine::durable::record_hook_effect(
                &task.id,
                EFFECT,
                &json!({"review_id":review.id,"evaluation":evaluation,"consumer_id":consumer,"port_warnings":port_warnings}),
            )
            .await
            .map_err(infrastructure)?;
        }
        let id = consumer.as_deref().expect("verify consumer exists");
        let result = match consumers.wait_state(id).await.map_err(infrastructure)? {
            CheckWaitState::Result(result) => result,
            CheckWaitState::Pending | CheckWaitState::Exhausted => {
                let until = (chrono::Utc::now()
                    + chrono::Duration::seconds(
                        i64::from(config.timeout_seconds.unwrap_or(1200)) + 120,
                    ))
                .to_rfc3339();
                if !crate::workflow::engine::durable::suspend_hook(&task.id, id, until) {
                    return Err("verify requires the Task hooks step".into());
                }
                return Ok(Progress::Waiting);
            }
            CheckWaitState::Lost => {
                return Err(Error::Infrastructure(
                    "verify check was lost before it produced a result".into(),
                ))
            }
        };
        if result.outcome == db::CheckResultOutcome::TimedOut && result.commands.is_empty() {
            return Err(Error::Infrastructure(
                "verify deadline elapsed before any command could start".into(),
            ));
        }
        if result.outcome == db::CheckResultOutcome::Cancelled {
            return Err(Error::Infrastructure(
                "verify check was cancelled before it produced a result".into(),
            ));
        }
        // The app's log and the checks' reports are written by the candidate,
        // not through a command pipe: the runner never redacted them.
        let environment = review::contract::project_environment(&ctx.db, &task.id).await?;
        let mut details =
            read_results(ctx, task, review, &directory, &result, &environment.env).await?;
        details.notes.splice(0..0, port_warnings.iter().cloned());
        if details.status != Status::Inconclusive
            || !db::budget::grant_verify_rerun(
                &ctx.db,
                task,
                &ctx.state_config,
                &review.id,
                evaluation,
            )
            .await
            .map_err(infrastructure)?
        {
            return Ok(Progress::Complete(details));
        }
        evaluation += 1;
        consumer = None;
        port_warnings = details
            .notes
            .into_iter()
            .filter(|note| note.starts_with("Verify port "))
            .collect();
        // Persist the debit before asking again. A restarted hook has the same
        // bounded evaluation; its ledger receipt makes this checkpoint replay safe.
        crate::workflow::engine::durable::record_hook_effect(
            &task.id,
            EFFECT,
            &json!({"review_id":review.id,"evaluation":evaluation,"port_warnings":port_warnings}),
        )
        .await
        .map_err(infrastructure)?;
    }
}

fn read_file(root: &Path, name: &str, limit: u64) -> Result<Option<Vec<u8>>, String> {
    let relative = Path::new(name);
    if relative.is_absolute()
        || relative
            .components()
            .any(|c| !matches!(c, Component::Normal(_) | Component::CurDir))
    {
        return Err("path escapes verify directory".into());
    }
    crate::native_tools::read_bounded_regular_file(&root.join(relative), root, limit, true)
}

async fn read_results(
    ctx: &HookContext,
    task: &db::Task,
    review: &db::Review,
    directory: &Path,
    result: &db::StoredCheckResult,
    env: &BTreeMap<String, String>,
) -> Result<ReviewVerifyDetails, String> {
    let clean = |text: &str| {
        executors::environment::redact_environment_values(&check_executor::strip_ansi(text), env)
    };
    let mut details = ReviewVerifyDetails {
        status: Status::Pass,
        reason: None,
        checks: vec![],
        app_log_tail: String::new(),
        notes: vec![],
    };
    // app.log can be long: only the bounded end is read, never the full file.
    details.app_log_tail = app_tail(directory, env).unwrap_or_else(|error| {
        details.notes.push(format!("app.log: {error}"));
        String::new()
    });
    if let Some(port) = read_file(directory, "port", 8)?
        .and_then(|bytes| String::from_utf8(bytes).ok())
        .and_then(|text| text.trim().parse().ok())
    {
        if let Some(warning) = review::verify::port_warning(port) {
            details.notes.push(warning);
        }
    }
    let mut evidence_bytes = 0;
    let mut start_or_ready_failed = false;
    let start_exited = read_file(directory, "start.exit", 64)?.is_some();
    let run = db::CheckRunRepo::check_run(&*ctx.db, &result.run_id)
        .await
        .map_err(|e| e.to_string())?
        .ok_or("verify run disappeared")?;
    for command in &result.commands {
        let command_id = &run
            .identity
            .inputs
            .spec
            .commands
            .get(command.index)
            .ok_or("verify command identity missing")?
            .id;
        let name = command_id.splitn(4, ':').nth(3).unwrap_or("unknown");
        let name = name.strip_prefix("check:").unwrap_or(name);
        let mut entry = ReviewVerifyCheckResult {
            name: name.into(),
            result: if command.exit_code == 0 {
                Status::Pass
            } else {
                Status::Fail
            },
            summary: format!("{} exited {}", name, command.exit_code),
            evidence: vec![],
            evidence_paths: vec![],
            observed: None,
        };
        // A report that is there but cannot be used is no verdict: a check
        // that exits 0 with it is inconclusive, never a pass.
        let mut unusable = None;
        let report = if command_id.contains(":check:") {
            match read_file(directory, &format!("{name}.report.json"), 256 * 1024) {
                Ok(Some(bytes)) => match serde_json::from_slice::<Value>(&bytes) {
                    Ok(value) if value.is_object() => Some(value),
                    _ => {
                        unusable = Some("malformed report".to_owned());
                        None
                    }
                },
                Ok(None) => None,
                Err(error) => {
                    unusable = Some(format!("report skipped: {error}"));
                    None
                }
            }
        } else {
            None
        };
        if let Some(reason) = &unusable {
            details.notes.push(format!("{name}: {reason}"));
            if command.exit_code == 0 {
                entry.result = Status::Inconclusive;
                entry.summary = format!("{name} exited 0 with an unusable report: {reason}");
            }
        }
        if let Some(report) = &report {
            if let Some(summary) = report["summary"].as_str().filter(|s| !s.trim().is_empty()) {
                entry.summary = clean(summary).chars().take(1000).collect();
            }
            if command.exit_code == 0 {
                // No verdict: the exit code decides. A verdict that is not
                // one of the known words is no verdict either way.
                entry.result = match &report["verdict"] {
                    Value::Null => Status::Pass,
                    verdict => match verdict.as_str().map(str::to_ascii_lowercase).as_deref() {
                        Some("pass" | "passed") => Status::Pass,
                        Some("fail" | "failed") => Status::Fail,
                        Some("inconclusive") => Status::Inconclusive,
                        _ => {
                            details
                                .notes
                                .push(format!("{name}: unrecognized report verdict"));
                            Status::Inconclusive
                        }
                    },
                };
            }
            let findings: Vec<_> = report["findings"]
                .as_array()
                .into_iter()
                .flatten()
                .take(50)
                .map(|f| {
                    ["summary", "where", "expected", "observed"]
                        .iter()
                        .filter_map(|key| {
                            f[*key].as_str().map(|s| {
                                format!(
                                    "{key}: {}",
                                    clean(s).chars().take(1000).collect::<String>()
                                )
                            })
                        })
                        .collect::<Vec<_>>()
                        .join("; ")
                })
                .filter(|s| !s.is_empty())
                .collect();
            if !findings.is_empty() {
                entry.observed = Some(clip_tail(&findings.join("\n"), 4096));
            }
            for (index, evidence) in report["evidence"]
                .as_array()
                .into_iter()
                .flatten()
                .take(50)
                .enumerate()
            {
                let capture = async {
                    let name = evidence["path"]
                        .as_str()
                        .ok_or("evidence path is required")?;
                    let kind = evidence["kind"]
                        .as_str()
                        .ok_or("evidence kind is required")?;
                    if !matches!(
                        kind,
                        "screenshot" | "walkthrough_video" | "log" | "report" | "other"
                    ) {
                        return Err("unsupported evidence kind".into());
                    }
                    let caption = clean(evidence["caption"].as_str().unwrap_or(&entry.summary))
                        .chars()
                        .take(1000)
                        .collect::<String>();
                    let remaining = MAX_EVIDENCE_BYTES - evidence_bytes;
                    let mut bytes = read_file(directory, name, remaining.min(25 * 1024 * 1024))
                        .map_err(|error| if remaining < 25 * 1024 * 1024 {
                            format!("total evidence byte budget (64 MiB) exhausted or file refused: {error}")
                        } else { error })?
                        .ok_or("evidence file does not exist")?;
                    if bytes.is_empty() {
                        return Err("evidence file is empty".into());
                    }
                    let original_size = bytes.len() as u64;
                    // A text file (the app's log, a report) is kept without
                    // the Project's values, as command output is.
                    if let Ok(text) = std::str::from_utf8(&bytes) {
                        let redacted = executors::environment::redact_environment_values(text, env);
                        if redacted != text {
                            bytes = redacted.into_bytes();
                        }
                    }
                    let size = original_size.max(bytes.len() as u64);
                    if size > remaining {
                        return Err("total evidence byte budget (64 MiB) exhausted".into());
                    }
                    evidence_bytes += size;
                    let media_id = uuid::Uuid::new_v5(
                        &uuid::Uuid::NAMESPACE_OID,
                        format!("verify:{}:{}:{}:{index}", review.id, result.id, command_id)
                            .as_bytes(),
                    )
                    .to_string();
                    let filename = Path::new(name)
                        .file_name()
                        .and_then(|s| s.to_str())
                        .ok_or("invalid evidence filename")?;
                    let id = ctx
                        .task_service
                        .capture_verify_evidence(
                            &task.id, &media_id, filename, kind, &caption, &bytes,
                        )
                        .await
                        .map_err(|e| e.to_string())?;
                    Ok::<_, String>((id, directory.join(name).to_string_lossy().into_owned()))
                }
                .await;
                match capture {
                    Ok((id, path)) => {
                        entry.evidence.push(id);
                        entry.evidence_paths.push(path);
                    }
                    Err(error) => details
                        .notes
                        .push(format!("{} evidence {index} skipped: {error}", entry.name)),
                }
            }
        }
        if entry.result == Status::Fail && entry.observed.is_none() {
            entry.observed = Some(clip_tail(
                &format!("{}\n{}", command.output_tail, command.stderr_tail),
                4096,
            ));
        }
        if !command_id.contains(":check:")
            && matches!(entry.name.as_str(), "start" | "ready")
            && entry.result == Status::Fail
        {
            start_or_ready_failed = true;
            entry.observed = Some(details.app_log_tail.clone());
        }
        details.checks.push(entry);
    }
    if start_exited && !start_or_ready_failed {
        details.checks.push(ReviewVerifyCheckResult {
            name: "start".into(),
            result: Status::Fail,
            summary: "app start command exited before verify finished".into(),
            observed: Some(details.app_log_tail.clone()),
            evidence: vec![],
            evidence_paths: vec![],
        });
    }
    if result.outcome == db::CheckResultOutcome::TimedOut
        && !details.checks.iter().any(|c| c.result == Status::Fail)
    {
        if let Some(last) = details.checks.last_mut() {
            last.result = Status::Fail;
            last.summary = "verify wall limit exceeded".into();
        }
    }
    details.status = if details.checks.iter().any(|c| c.result == Status::Fail) {
        Status::Fail
    } else if details
        .checks
        .iter()
        .any(|c| c.result == Status::Inconclusive)
    {
        Status::Inconclusive
    } else {
        Status::Pass
    };
    Ok(details)
}

const APP_LOG_WINDOW_BYTES: u64 = 64 * 1024;
const MAX_EVIDENCE_BYTES: u64 = 64 * 1024 * 1024;

fn clip_tail(text: &str, limit: usize) -> String {
    let mut start = text.len().saturating_sub(limit);
    while !text.is_char_boundary(start) {
        start += 1;
    }
    text[start..].into()
}
fn app_tail(directory: &Path, env: &BTreeMap<String, String>) -> Result<String, String> {
    use std::io::{Read, Seek, SeekFrom};
    let path = directory.join("app.log");
    if !path.exists() {
        return Ok(String::new());
    }
    let meta = path.symlink_metadata().map_err(|e| e.to_string())?;
    if !meta.is_file()
        || meta.file_type().is_symlink()
        || !path
            .canonicalize()
            .map_err(|e| e.to_string())?
            .starts_with(directory.canonicalize().map_err(|e| e.to_string())?)
    {
        return Err("not a confined regular file".into());
    }
    let mut file = std::fs::File::open(path).map_err(|e| e.to_string())?;
    // More than the kept tail is read, so a value that straddles the cut is
    // still masked whole before the tail is taken.
    let window = meta.len().min(APP_LOG_WINDOW_BYTES);
    file.seek(SeekFrom::End(-(window as i64)))
        .map_err(|e| e.to_string())?;
    let mut bytes = Vec::new();
    file.take(APP_LOG_WINDOW_BYTES)
        .read_to_end(&mut bytes)
        .map_err(|e| e.to_string())?;
    Ok(check_executor::redacted_tail(
        &bytes,
        meta.len() > window,
        true,
        env,
        check_executor::OUTPUT_TAIL_BYTES,
    ))
}
