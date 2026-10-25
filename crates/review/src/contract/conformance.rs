//! Reviewer assessment parsing and frozen-contract conformance evaluation.

use super::*;

/// Parse a reviewer's reply: free Markdown ending in one result block
/// `{"result": "pass|fail|blocked", "reason": "..."}`.
///
/// The last complete JSON object that names a result wins, so a reviewer may
/// quote JSON or code in its review, or correct itself later in the same reply. The
/// block is read leniently — unknown keys are ignored, `verdict` is accepted
/// for `result`, and case does not matter — because a well-meant extra field
/// or a slightly different key used to throw away a whole review. Everything
/// else in the reply is kept as the Markdown report.
pub fn parse_assessment(message: &str) -> Result<ReviewAssessment, String> {
    if message.len() > MAX_REPORT_BYTES {
        return Err("review report exceeds size budget".into());
    }
    let (span, result, reason, fixable_by, repeat, findings, resolved) = message
        .match_indices('{')
        .rev()
        .filter_map(|(open, _)| {
            if is_list_element(message, open) {
                return None;
            }
            let mut values =
                serde_json::Deserializer::from_str(&message[open..]).into_iter::<Value>();
            let value = values.next()?.ok()?;
            let (result, reason) = result_block(&value)?;
            let fixable_by = match value.get("fixable_by").and_then(Value::as_str) {
                Some(value) if value.trim().eq_ignore_ascii_case("owner") => FixableBy::Owner,
                _ => FixableBy::Coder,
            };
            let repeat = match value.get("repeat") {
                Some(Value::Bool(value)) => *value,
                Some(Value::String(value)) => value.trim().eq_ignore_ascii_case("true"),
                _ => false,
            };
            Some((
                (open, open + values.byte_offset()),
                result,
                reason,
                fixable_by,
                repeat,
                parse_findings(value.get("findings")),
                parse_resolved(value.get("resolved")),
            ))
        })
        // Prefer the last complete result object, including its closing
        // brace, so a valid block wins over the objects nested inside it.
        .max_by_key(|((_, close), ..)| *close)
        .ok_or(
            "review must end with one result block: {\"result\": \"pass|fail|blocked\", \
             \"reason\": \"one sentence\"}",
        )?;
    Ok(ReviewAssessment {
        result,
        reason,
        fixable_by,
        repeat,
        findings,
        resolved,
        report: report_without_block(message, span),
    })
}

/// An object right after `[` or `,` (with no blank line between) is an item
/// of a list, such as a finding inside a result block whose JSON is broken.
/// It is never the verdict. A correction turn is appended after a blank line,
/// so it stays readable even when the turn before it was cut off mid-list.
fn is_list_element(message: &str, open: usize) -> bool {
    let before = &message[..open];
    let content = before.trim_end();
    before[content.len()..].matches('\n').count() < 2 && content.ends_with(['[', ','])
}

const MAX_FINDINGS: usize = 50;
const MAX_FINDING_TEXT_CHARS: usize = 1_000;

fn finding_text(value: Option<&Value>) -> Option<String> {
    value
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(|text| text.chars().take(MAX_FINDING_TEXT_CHARS).collect())
}

fn parse_findings(value: Option<&Value>) -> Vec<ReviewFinding> {
    value
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|item| {
            Some(ReviewFinding {
                requirement_ids: parse_resolved(item.get("requirement_ids")),
                kind: item.get("kind").and_then(Value::as_str).and_then(|kind| {
                    match kind.trim().to_ascii_lowercase().as_str() {
                        "regression" => Some(api_types::ReviewFindingKind::Regression),
                        "missed" => Some(api_types::ReviewFindingKind::Missed),
                        "out_of_scope" => Some(api_types::ReviewFindingKind::OutOfScope),
                        _ => None,
                    }
                }),
                same_cause_as: finding_text(item.get("same_cause_as")),
                summary: finding_text(item.get("summary"))?,
                id: finding_text(item.get("id")),
                severity: match item.get("severity").and_then(Value::as_str) {
                    Some(severity) if severity.trim().eq_ignore_ascii_case("minor") => {
                        ReviewFindingSeverity::Minor
                    }
                    _ => ReviewFindingSeverity::Blocking,
                },
                r#where: finding_text(item.get("where")),
                expected: finding_text(item.get("expected")),
                observed: finding_text(item.get("observed")),
            })
        })
        .take(MAX_FINDINGS)
        .collect()
}

fn parse_resolved(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|id| finding_text(Some(id)))
        .take(MAX_FINDINGS)
        .collect()
}

fn result_block(value: &Value) -> Option<(ReviewResult, String)> {
    let object = value.as_object()?;
    let result = object
        .get("result")
        .or_else(|| object.get("verdict"))?
        .as_str()?;
    let result = match result.trim().to_ascii_lowercase().as_str() {
        "pass" | "passed" => ReviewResult::Pass,
        "fail" | "failed" => ReviewResult::Fail,
        "blocked" => ReviewResult::Blocked,
        _ => return None,
    };
    let reason = object
        .get("reason")
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or_default()
        .to_owned();
    Some((result, reason))
}

/// The reply with the result block removed, including a Markdown code fence
/// that wraps nothing but the block.
fn report_without_block(message: &str, (open, close): (usize, usize)) -> String {
    let before = message[..open].trim_end();
    let after = message[close..].trim_start();
    let fence_open = before
        .rfind("```")
        .filter(|&index| !before[index + 3..].contains(char::is_whitespace));
    let (before, after) = match (fence_open, after.strip_prefix("```")) {
        (Some(index), Some(rest)) => (&before[..index], rest),
        _ => (before, after),
    };
    let before = before.trim_end();
    let after = after.trim();
    if after.is_empty() {
        before.to_owned()
    } else {
        format!("{before}\n\n{after}").trim().to_owned()
    }
}

pub async fn evaluate(
    db: &SqliteDb,
    execution_id: &str,
    path: &(impl ReviewWorkspace + ?Sized),
    message: &str,
) -> Result<ReviewConformance, String> {
    let contract = db
        .review_contract(execution_id)
        .await
        .map_err(|e| e.to_string())?
        .ok_or("review has no frozen contract; new review required")?;
    if let Some(previous) = db
        .review_conformance(execution_id)
        .await
        .map_err(|e| e.to_string())?
    {
        return Ok(previous);
    }
    let mut result = ReviewConformance {
        status: ConformanceStatus::Unverified,
        contract: Some(contract.clone()),
        ..Default::default()
    };
    let validation = evaluate_inner(db, path, message, &contract, &mut result).await;
    if let Err(reason) = validation {
        if let Some(error) = path.infrastructure_error(&reason) {
            return Err(error.to_string());
        }
        // A check that outran its limit says nothing about the candidate or
        // the reviewer's verdict (a build lock held by a sibling Task is the
        // usual cause). Leave it unrecorded so the checks alone can run again.
        let timed_out = is_check_timeout(&reason);
        result.reason = Some(reason);
        result.status = ConformanceStatus::Unverified;
        if timed_out {
            return Ok(result);
        }
    }
    db.record_review_conformance(&result)
        .await
        .map_err(|e| e.to_string())?;
    Ok(result)
}

/// The Project environment of `task_id`.
///
/// Review evidence must use the same declared environment as the candidate
/// execution. Missing or malformed authority therefore fails closed instead
/// of silently running checks in an empty environment.
pub async fn project_environment(
    db: &SqliteDb,
    task_id: &str,
) -> Result<ProjectEnvironment, String> {
    use db::{ProjectRepo, TaskRepo};
    let task = TaskRepo::get_by_id(db, task_id, false)
        .await
        .map_err(|error| format!("failed to load Task environment authority: {error}"))?
        .ok_or_else(|| format!("Task {task_id} was not found while loading review environment"))?;
    let project = ProjectRepo::get_by_id(db, &task.project_id)
        .await
        .map_err(|error| format!("failed to load Project environment authority: {error}"))?
        .ok_or_else(|| {
            format!(
                "Project {} was not found while loading review environment",
                task.project_id
            )
        })?;
    serde_json::from_str::<ProjectSettings>(&project.settings)
        .map(|settings| settings.environment)
        .map_err(|error| format!("invalid Project environment settings: {error}"))
}

/// Compare review authority using the algorithm frozen in this contract;
/// prompt/audit context may change without changing review authority.
pub(super) async fn verify_contract_context(
    db: &SqliteDb,
    contract: &ReviewContract,
) -> Result<(), String> {
    let source = db
        .review_source(&contract.context.task_id, Some(&contract.execution_id))
        .await
        .map_err(|error| error.to_string())?;
    context_from_source(&source)?;
    if review_source_digest(&source, contract.context.source_digest_version.unwrap_or(1))?
        != contract.context.source_digest
    {
        return Err("review context changed; fresh review required".into());
    }
    Ok(())
}

fn retain_contract_requirement_ids(report: &mut ReviewAssessment, contract: &ReviewContract) {
    for finding in &mut report.findings {
        finding.requirement_ids.retain(|id| {
            contract
                .context
                .requirements
                .iter()
                .any(|requirement| requirement.id == *id)
        });
        finding.requirement_ids.sort();
        finding.requirement_ids.dedup();
    }
}

async fn evaluate_inner(
    db: &SqliteDb,
    path: &(impl ReviewWorkspace + ?Sized),
    message: &str,
    contract: &ReviewContract,
    result: &mut ReviewConformance,
) -> Result<(), String> {
    let mut report = parse_assessment(message)?;
    retain_contract_requirement_ids(&mut report, contract);
    // Keep the review even when the checks below fail: its Markdown is what
    // the coder or the owner reads next.
    result.assessment = Some(report.clone());
    verify_contract_context(db, contract).await?;
    if git_read(path, &["rev-parse", "HEAD"]).await?.trim() != contract.commit_sha {
        return Err("review context or commit changed; fresh review required".into());
    }
    if !git_read(path, &["diff", "--name-only", "HEAD"])
        .await?
        .trim()
        .is_empty()
    {
        return Err("review workspace differs from admitted commit".into());
    }
    // Checks run in the Task's own worktree, verified above to sit at the
    // reviewed commit with no tracked change. Only tracked content is
    // delivered, so the existing dependency and build output (`node_modules/`,
    // `target/`) is reused rather than rebuilt; a fresh clone per review
    // repeated every install and cold build in the temp dir.
    let checkout = path;
    let runs_checks =
        !contract.context.setup_steps.is_empty() || !contract.context.required_checks.is_empty();
    let environment = project_environment(db, &contract.context.task_id).await?;
    if runs_checks {
        checkout.materialize_assets(&environment).await?;
    }
    let timeout = contract
        .context
        .check_timeout_seconds
        .unwrap_or(DEFAULT_CHECK_TIMEOUT_SECONDS);
    let mut setup_failed = false;
    for (index, setup) in contract.context.setup_steps.iter().enumerate() {
        let output = check_workspace_output(checkout, setup, &environment.env, timeout).await?;
        let mut text = output.stdout;
        text.push_str(&output.stderr);
        let text = executors::environment::redact_environment_values(&text, &environment.env);
        let exit_code = output.exit_code.unwrap_or(-1);
        result.checks.push(ConformanceCheckResult {
            triage: None,
            check_id: format!("setup:{index}"),
            command: setup.clone(),
            exit_code,
            output: output_tail(&text),
        });
        if exit_code != 0 {
            setup_failed = true;
            break;
        }
    }
    for check in contract
        .context
        .required_checks
        .iter()
        .take_while(|_| !setup_failed)
    {
        // Entry triage already established that these exact CI failures are
        // non-blocking. Keep their real verdict and frozen classification;
        // do not turn conformance into another untriaged run of that command.
        if let Some(previous) = contract.check_results.iter().find(|previous| {
            previous.check_id == check.id
                && previous.command == check.command
                && previous
                    .triage
                    .as_ref()
                    .is_some_and(|triage| !triage.blocking)
        }) {
            result.checks.push(previous.clone());
            continue;
        }
        let output =
            check_workspace_output(checkout, &check.command, &environment.env, timeout).await?;
        let mut text = output.stdout;
        text.push_str(&output.stderr);
        let text = executors::environment::redact_environment_values(&text, &environment.env);
        result.checks.push(ConformanceCheckResult {
            triage: None,
            check_id: check.id.clone(),
            command: check.command.clone(),
            exit_code: output.exit_code.unwrap_or(-1),
            output: output_tail(&text),
        });
    }
    let mut reproduction_failure = None;
    if runs_checks {
        // The tree was the reviewed commit before the setup steps and checks
        // ran, so a change here comes from them, not the reviewer.
        // It means the candidate does not reproduce from its own commit (a
        // stale lockfile is the usual cause), which is the coder's to fix.
        // Treating it as a reviewer protocol failure retried a reviewer who
        // could never pass until the Task blocked, and the coder never heard.
        let head_moved =
            git_read(checkout, &["rev-parse", "HEAD"]).await?.trim() != contract.commit_sha;
        let changed = git_read(checkout, &["diff", "--name-only", "HEAD"]).await?;
        let changed: Vec<&str> = changed.lines().filter(|line| !line.is_empty()).collect();
        if head_moved {
            reproduction_failure = Some(
                "the review setup steps or checks moved HEAD away from the reviewed commit; \
                 they must not commit"
                    .to_owned(),
            );
        } else if !changed.is_empty() {
            reproduction_failure = Some(format!(
                "running the review setup steps and checks at the reviewed commit modified \
                 tracked files: {}. Commit the regenerated files (for example an updated \
                 lockfile) so the candidate reproduces from its own commit",
                changed.join(", ")
            ));
        }
        // Leave the worktree exactly at the reviewed commit for integration or
        // the coder's next attempt; a tree git cannot restore fails the review.
        restore_reviewed_tree(checkout, &contract.commit_sha).await?;
    }
    if setup_failed {
        result.status = ConformanceStatus::Failed;
        result.reason = Some("review setup steps failed at the reviewed commit".into());
        return Ok(());
    }
    if let Some(reason) = reproduction_failure {
        result.status = ConformanceStatus::Failed;
        result.reason = Some(reason);
        return Ok(());
    }
    let failed_check = result
        .checks
        .iter()
        .any(ConformanceCheckResult::is_blocking);
    // A failing required check is a defect in the candidate whatever the
    // reviewer concluded. Otherwise the reviewer's result stands: a pass is
    // only as strong as the checks Forge ran, which is why projects should
    // configure them.
    let (status, reason) = match report.result {
        _ if failed_check => (
            ConformanceStatus::Failed,
            "required conformance check failed".to_owned(),
        ),
        ReviewResult::Pass => (ConformanceStatus::Passed, report.reason.clone()),
        ReviewResult::Fail => (ConformanceStatus::Failed, report.reason.clone()),
        ReviewResult::Blocked => (ConformanceStatus::Blocked, report.reason.clone()),
    };
    result.status = status;
    result.reason = (!reason.is_empty()).then_some(reason);
    Ok(())
}

#[cfg(test)]
mod tests;
