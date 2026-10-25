use super::*;

const AUTOMATIC_REVIEW_RECOVERY_TRIGGER: &str = "automatic_review_recovery";

tokio::task_local! {
    static AUTOMATIC_REVIEW_RECOVERY_TASK: String;
}

struct CapacityRetry<'a> {
    retry_at: Option<&'a str>,
    reason: &'static str,
}

enum ExecutionRetryDisposition {
    Scheduled,
    NotScheduled(&'static str),
}

impl ExecutionRetryDisposition {
    fn is_scheduled(&self) -> bool {
        matches!(self, Self::Scheduled)
    }
}

/// Whether `task` is already parked by a review-environment block that this
/// reviewer execution raised.
fn review_blocked_by_execution(task: &Task, execution_id: &str) -> bool {
    task.blocked_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
        .is_some_and(|blocked| {
            blocked["kind"] == json!(api_types::FailureKind::ReviewBlocked)
                && blocked["execution_id"].as_str() == Some(execution_id)
        })
}

pub(crate) fn reviewer_execution_lacks_exact_review_binding(
    execution: &Execution,
    review: &Review,
) -> bool {
    !exact_review_binding_matches(execution, review)
}

/// Return the one Review attempt explicitly bound to an execution. Candidate
/// parentage is intentionally excluded: several attempts may inspect the same
/// candidate, so it cannot distinguish an old reviewer from the current one.
pub(crate) fn exact_review_for_execution<'a>(
    execution: &Execution,
    reviews: &'a [Review],
) -> Option<&'a Review> {
    let mut matches = reviews
        .iter()
        .filter(|review| exact_review_binding_matches(execution, review));
    let first = matches.next()?;
    matches.next().is_none().then_some(first)
}

/// Return whether the exact Review attempt bound to this execution has
/// already settled.  Candidate parentage is intentionally not considered:
/// multiple reviewer attempts may inspect the same candidate, while only the
/// durable reviewer/auditor execution binding identifies the attempt whose
/// session a recovery request would resume.
fn exact_review_binding_matches(execution: &Execution, review: &Review) -> bool {
    if review.reviewer_execution_id.is_some() || review.auditor_execution_id.is_some() {
        review.reviewer_execution_id.as_deref() == Some(execution.id.as_str())
            || review.auditor_execution_id.as_deref() == Some(execution.id.as_str())
    } else {
        review.execution_id == execution.id
    }
}

fn render_workflow_guard_follow_up_prompt(
    guard: &str,
    reason: &str,
    attempt: u64,
    budget: u64,
) -> String {
    let checklist_instruction = if guard == "require_conflict_markers_resolved" {
        "Fix the files named above: remove every conflict marker line and keep the correct merged code, verify nothing else in them is still conflicted, then commit the result. Forge re-checks the committed HEAD before the task can go to review."
    } else if guard == super::step_check::STEP_CHECK_RESULT_GUARD {
        "End your reply with exactly one JSON object on its own: {\"result\": \"pass\", \"reason\": \"one sentence\"} or {\"result\": \"fail\", \"reason\": \"one sentence\"}. Do not write a plan and do not change files."
    } else if guard == "planning_plan_ready" {
        "Write a valid Markdown checklist using `task.plan` for a native session or `$FORGE_PLAN_PATH` for a CLI harness."
    } else {
        "Make sure you complete all tasks and fix what is needed for this guard. Update completed checklist items to `- [x]` using `task.plan` for a native session or `$FORGE_PLAN_PATH` for a CLI harness; you do not need to commit if all implementation work is already complete."
    };
    format!(
        "Your previous execution completed, but Forge could not move the task to the next workflow state.\n\nWorkflow guard failed: {guard}\n\nFailure:\n{reason}\n\n{checklist_instruction}\n\nRetry {attempt}/{budget}."
    )
}

fn task_blocked_by_execution(task: &Task, execution_id: &str) -> bool {
    task.blocked_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
        .and_then(|value| {
            value
                .get("execution_id")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .as_deref()
        == Some(execution_id)
}

fn render_automatic_review_recovery_prompt(
    ctx: &crate::workflow::dispatch::AgentDispatchContext,
    failure_reason: &str,
    attempt: usize,
    max_attempts: usize,
) -> String {
    use crate::workflow::dispatch::repair_brief::{RepairBrief, RepairBudget};
    let body = RepairBrief::review(ctx)
        .automatic_recovery(
            RepairBudget {
                name: "automatic_review_recovery".into(),
                spent: attempt as i64,
                limit: max_attempts as i64,
            },
            failure_reason,
        )
        .render();
    match crate::workflow::dispatch::coder_prompt::no_change_brief(ctx) {
        Some(reminder) => format!("{reminder}\n\n{body}"),
        None => body,
    }
}

pub(super) async fn reviewer_final_message(execution: &Execution) -> Result<String> {
    if let Some(logs_path) = execution.logs_path.as_deref() {
        let contents = match tokio::fs::read_to_string(logs_path).await {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(error) => {
                return Err(ServiceError::invalid_operation(format!(
                    "failed to read reviewer logs: {error}"
                )));
            }
        };
        let mut message = String::new();
        let mut stdout_lines = String::new();
        for line in contents.lines() {
            let Ok(entry) = serde_json::from_str::<executors::LogEntry>(line) else {
                continue;
            };
            if entry.kind == executors::LogKind::Assistant {
                let mut candidate = String::new();
                append_reviewer_log_text(&entry.payload, &mut candidate);
                if !candidate.trim().is_empty() {
                    message = candidate;
                }
            } else if entry.kind == executors::LogKind::SessionInfo
                && entry.payload.get("subtype").and_then(Value::as_str) == Some("success")
            {
                if let Some(result) = entry.payload.get("result").and_then(Value::as_str) {
                    message = result.to_owned();
                }
            } else if entry.kind == executors::LogKind::Stdout {
                if let Some(line) = entry.payload.get("line").and_then(Value::as_str) {
                    stdout_lines.push_str(line);
                    stdout_lines.push('\n');
                }
            }
        }
        if !message.trim().is_empty() {
            return Ok(message);
        }
        if !stdout_lines.trim().is_empty() {
            return Ok(stdout_lines);
        }
    }

    Ok(execution.summary.clone().unwrap_or_default())
}

fn append_reviewer_log_text(payload: &Value, message: &mut String) {
    if let Some(text) = payload.get("text").and_then(Value::as_str) {
        message.push_str(text);
    }

    let Some(content) = payload
        .get("message")
        .and_then(|message| message.get("content"))
        .and_then(Value::as_array)
    else {
        return;
    };

    for item in content {
        if item.get("type").and_then(Value::as_str) == Some("text") {
            if let Some(text) = item.get("text").and_then(Value::as_str) {
                message.push_str(text);
            }
        }
    }
}

/// Retry hint plus deterministic jitter, bounded by backoff and six hours.
fn executor_unavailable_dispatch_time(
    retry_at: Option<&str>,
    task_id: &str,
    backoff_seconds: u64,
) -> chrono::DateTime<chrono::Utc> {
    let jitter_seconds = i64::from(
        task_id
            .bytes()
            .fold(0u8, |acc, byte| acc.wrapping_add(byte))
            % 30,
    );
    let now = chrono::Utc::now();
    let floor = now + chrono::Duration::seconds(backoff_seconds as i64);
    let hinted = retry_at
        .and_then(|at| chrono::DateTime::parse_from_rfc3339(at).ok())
        .map(|at| at.with_timezone(&chrono::Utc))
        .unwrap_or(floor)
        .min(now + chrono::Duration::hours(6));
    (hinted + chrono::Duration::seconds(jitter_seconds))
        .max(floor)
        .min(now + chrono::Duration::hours(6))
}

pub(crate) fn should_block_task_for_failed_execution(execution: &Execution) -> bool {
    if super::guards::is_workspace_lease_pre_dispatch_failure(execution) {
        return false;
    }
    matches!(
        execution.role.as_str(),
        "interactive" | "executor" | crate::workflow::default_roles::CODER
    ) || (execution.role == crate::workflow::default_roles::PLANNER
        // Setup refusals keep dispatch's recovery semantics; only a planner
        // that actually ran joins the ordinary execution-failure retry path.
        && execution.stopped_by.as_deref() != Some("system:dispatch"))
}

fn execution_failure_reason(execution: &Execution) -> String {
    execution
        .error
        .as_deref()
        .or(execution.summary.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| format!("ended with status {}", execution.status))
}

/// Upper bound on the reviewer's Markdown carried into its Task comment.
const REVIEW_COMMENT_REPORT_LIMIT: usize = 2 * 1024;
/// Keep the complete open/unconfirmed view, but only resolutions this review
/// introduced. last_checked_attempt remains the last source check, including
/// repeated passes; it is not a resolution timestamp.
fn current_comment_findings(
    ledger: Vec<api_types::ReviewFindingEntry>,
    previous: &[api_types::ReviewFindingEntry],
) -> Vec<api_types::ReviewFindingEntry> {
    use api_types::ReviewFindingStatus as Status;
    ledger
        .iter()
        .filter(|entry| {
            entry.status != Status::Superseded
                && (entry.status != Status::Resolved
                    || !previous
                        .iter()
                        .any(|prior| prior.id == entry.id && prior.status == Status::Resolved))
        })
        .cloned()
        .collect()
}

/// Upper bound on the comment's checklist lines; later findings are named
/// by id only. Comments are read back into agent prompts.
const REVIEW_COMMENT_FINDINGS_LIMIT: usize = 8 * 1024;
const REVIEW_COMMENT_SUMMARY_CHARS: usize = 200;

/// Render the reviewer's Task comment: a `Review <outcome> (attempt N)`
/// headline, the ledger after this attempt, optional verify facts, and the
/// full-report pointer. Only findings-less reviews carry a bounded report.
fn reviewer_comment(
    status: ReviewStatus,
    attempt_number: i64,
    conformance: &api_types::ReviewConformance,
    ledger: &[api_types::ReviewFindingEntry],
    verify: Option<&Value>,
) -> String {
    let reason = conformance
        .reason
        .as_deref()
        .map(one_line)
        .filter(|reason| !reason.is_empty());
    let reason = reason.as_deref();
    let mut out = match status {
        ReviewStatus::AwaitingHuman => format!(
            "Review passed automated checks and is awaiting user approval (attempt {attempt_number})"
        ),
        ReviewStatus::Passed => format!("Review passed (attempt {attempt_number})"),
        ReviewStatus::Failed if conformance.status == api_types::ConformanceStatus::Blocked => {
            format!(
                "Review blocked by its environment (attempt {attempt_number}): {}",
                reason.unwrap_or("no reason given")
            )
        }
        ReviewStatus::Failed => format!(
            "Review failed (attempt {attempt_number}): {}",
            reason.unwrap_or("review conformance failed")
        ),
        _ => format!("Review updated (attempt {attempt_number})"),
    };
    use api_types::{ReviewFindingSource as Source, ReviewFindingStatus as Status};
    // Open findings first, then resolved, then unconfirmed, each in id order,
    // so the size limit drops the least actionable lines.
    let mut entries: Vec<_> = ledger
        .iter()
        .filter(|entry| {
            entry.status != Status::Resolved || entry.last_checked_attempt == attempt_number
        })
        .collect();
    entries.sort_by_key(|entry| match entry.status {
        Status::Open | Status::Reopened => 0,
        Status::Resolved => 1,
        Status::Unconfirmed => 2,
        Status::Flaky | Status::PreExisting | Status::Superseded | Status::OutOfScope => 3,
    });
    let mut listed = 0;
    let mut overflow = Vec::new();
    let mut ci_counts = std::collections::BTreeMap::new();
    let mut ci_omitted = std::collections::BTreeMap::new();
    for entry in entries {
        if entry.source == Source::Ci && entry.id.contains('#') {
            let step = entry.id.split('#').next().unwrap_or(&entry.id);
            let count = ci_counts.entry(step).or_insert(0);
            if *count == 20 {
                *ci_omitted.entry(step).or_insert(0) += 1;
                continue;
            }
            *count += 1;
        }
        // A line ending inside finding text would start a forged list item.
        let id = one_line(&entry.id);
        let summary = clip_chars(&one_line(&entry.summary), REVIEW_COMMENT_SUMMARY_CHARS);
        let mut line = match entry.status {
            Status::Open if entry.first_attempt == attempt_number => format!(
                "- [ ] {id} New: {summary} — open since attempt {}",
                entry.first_attempt
            ),
            Status::Open => format!(
                "- [ ] {id} {summary} — open since attempt {}",
                entry.first_attempt
            ),
            Status::Reopened => format!(
                "- [ ] {id} {summary} — re-broken: resolved in round {}",
                entry.resolved_attempt.unwrap_or_default()
            ),
            Status::OutOfScope => format!("- {id} {summary} — out_of_scope (follow-up candidate)"),
            Status::Resolved => format!(
                "- [x] {id} {summary} — {}",
                if entry.source == Source::Reviewer {
                    "resolved"
                } else {
                    "passing now"
                }
            ),
            Status::Superseded => format!("- {id} {summary} — superseded by per-test findings"),
            Status::Unconfirmed => format!("- {id} {summary} — not re-checked"),
            Status::Flaky => format!("- {id} {summary} — flaky (non-blocking)"),
            Status::PreExisting => format!("- {id} {summary} — pre-existing (non-blocking)"),
        };
        if let Some(note) = &entry.anchoring_note {
            line.push_str(&format!(" — minor: {}", one_line(note)));
        }
        if overflow.is_empty() && listed + line.len() < REVIEW_COMMENT_FINDINGS_LIMIT {
            listed += line.len() + 1;
            out.push('\n');
            out.push_str(&line);
        } else {
            overflow.push(id);
        }
    }
    for (step, count) in ci_omitted {
        out.push_str(&format!("\n{step}: +{count} more (see the Review tab)"));
    }
    if !overflow.is_empty() {
        out.push_str(&format!(
            "\n\nAlso in the ledger (see the Review tab): {}",
            overflow.join(", ")
        ));
    }
    let report = conformance
        .assessment
        .as_ref()
        .map(|assessment| assessment.report.trim())
        .unwrap_or_default();
    let has_reviewer_findings = conformance
        .assessment
        .as_ref()
        .is_some_and(|assessment| !assessment.findings.is_empty());
    if !has_reviewer_findings && !report.is_empty() {
        out.push_str("\n\n");
        let excerpt = truncate_on_char_boundary(report, REVIEW_COMMENT_REPORT_LIMIT);
        out.push_str(excerpt);
        // A cut inside a code block would swallow the lines after it.
        if let Some(fence) = open_code_fence(excerpt) {
            out.push('\n');
            out.push_str(fence);
        }
    }
    if let Some(verify) = verify.filter(|value| value.is_object()) {
        let status = verify["status"].as_str().unwrap_or("unknown");
        let failures: Vec<_> = verify["checks"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|check| matches!(check["result"].as_str(), Some("fail" | "failed")))
            .filter_map(|check| check["name"].as_str())
            .collect();
        let evidence: std::collections::BTreeSet<_> = verify["checks"]
            .as_array()
            .into_iter()
            .flatten()
            .flat_map(|check| check["evidence"].as_array().into_iter().flatten())
            .filter_map(Value::as_str)
            .collect();
        out.push_str(&format!("\n\nVerify: {status}"));
        if let Some(reason) = verify["reason"].as_str() {
            out.push_str(&format!(" — {reason}"));
        }
        if !failures.is_empty() {
            out.push_str(&format!(" — failing checks: {}", failures.join(", ")));
        }
        out.push_str(&format!(" ({} evidence)", evidence.len()));
        for id in evidence {
            out.push_str(&format!(" [evidence](/api/v1/media/{id})"));
        }
    }
    out.push_str(&format!(
        "\n\nFull review: Review tab, attempt {attempt_number}"
    ));
    out
}

fn finding_ids(
    ledger: &[api_types::ReviewFindingEntry],
    include: impl Fn(&api_types::ReviewFindingEntry) -> bool,
) -> String {
    // The same per-step bound as the checklist: a step can carry hundreds
    // of failing-test ids, and comments are read back into agent prompts.
    let mut ids = Vec::new();
    let mut ci_counts = std::collections::BTreeMap::new();
    let mut ci_omitted = std::collections::BTreeMap::new();
    for entry in ledger.iter().filter(|entry| include(entry)) {
        if entry.source == api_types::ReviewFindingSource::Ci && entry.id.contains('#') {
            let step = entry.id.split('#').next().unwrap_or(&entry.id);
            let count = ci_counts.entry(step).or_insert(0);
            if *count == 20 {
                *ci_omitted.entry(step).or_insert(0) += 1;
                continue;
            }
            *count += 1;
        }
        ids.push(entry.id.clone());
    }
    for (step, count) in ci_omitted {
        ids.push(format!("{step} +{count} more"));
    }
    if ids.is_empty() {
        "none".to_owned()
    } else {
        ids.join(", ")
    }
}

fn repair_round_comment(
    attempt: i64,
    spent: i64,
    limit: i64,
    ledger: &[api_types::ReviewFindingEntry],
    budget: &str,
) -> String {
    use api_types::ReviewFindingStatus as Status;
    let open = finding_ids(ledger, |entry| entry.status.is_open());
    let resolved = finding_ids(ledger, |entry| {
        entry.status == Status::Resolved && entry.last_checked_attempt == attempt
    });
    format!("Repair round {attempt} ({budget} budget {spent} of {limit}): {open} open; {resolved} resolved since the last round.")
}

fn clip_chars(text: &str, limit: usize) -> String {
    match text.char_indices().nth(limit) {
        Some((end, _)) => format!("{}…", &text[..end]),
        None => text.to_owned(),
    }
}

/// Text on one Markdown line: CommonMark ends a line at a bare `\r` too.
fn one_line(text: &str) -> String {
    text.split(char::is_control)
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

/// The fence of a code block the Markdown leaves open, if any.
fn open_code_fence(markdown: &str) -> Option<&str> {
    let mut open: Option<&str> = None;
    for line in markdown.lines() {
        let line = line.trim_start();
        let marker = match line.chars().next() {
            Some(ch @ ('`' | '~')) => &line[..line.len() - line.trim_start_matches(ch).len()],
            _ => continue,
        };
        if marker.len() < 3 {
            continue;
        }
        match open {
            None => open = Some(marker),
            Some(fence) if marker.starts_with(fence) && line[marker.len()..].trim().is_empty() => {
                open = None;
            }
            Some(_) => {}
        }
    }
    open
}

fn truncate_on_char_boundary(value: &str, limit: usize) -> &str {
    let mut end = value.len().min(limit);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

mod completion;

mod workflow_guards;

mod failure;

mod review_completion;

mod review_comments;

mod review_recovery;

#[cfg(test)]
mod reviewer_message_tests;
