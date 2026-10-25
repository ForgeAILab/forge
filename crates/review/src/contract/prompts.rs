//! Frozen review prompts, candidate diffs, and prior-review context.

use super::*;

pub fn contract_prompt(contract: &ReviewContract) -> String {
    format!(
        "\n\n{RESPONSE_INSTRUCTION}\n\nFrozen requirement ids: {}\n\nFrozen review contract:\n{}",
        contract
            .context
            .requirements
            .iter()
            .map(|requirement| requirement.id.as_str())
            .collect::<Vec<_>>()
            .join(", "),
        serde_json::to_string(contract).expect("contract serializes")
    )
}

pub async fn prepare_prompt(
    db: &SqliteDb,
    execution_id: &str,
    task_id: &str,
    path: &(impl ReviewWorkspace + ?Sized),
    reviewer: bool,
    shell: bool,
    prompt: String,
) -> Result<String, String> {
    let (context, contract) = if reviewer {
        let contract = admit(db, execution_id, task_id, path).await?;
        (contract.context.clone(), Some(contract))
    } else {
        (load_context(db, task_id, Some(execution_id)).await?, None)
    };
    let mut prompt = assemble_prepared_prompt(&context, contract.as_ref(), shell, prompt)?;
    if !shell {
        if let Some(contract) = contract.as_ref() {
            let budget = MAX_PREPARED_PROMPT_BYTES.saturating_sub(prompt.len() + 1024);
            let include_verify = !prompt.contains("\nVerify results:\n");
            prompt.push_str(
                &reviewer_context_prompt(
                    db,
                    task_id,
                    execution_id,
                    path,
                    contract,
                    budget,
                    include_verify,
                )
                .await,
            );
        }
    }
    Ok(prompt)
}

pub fn assemble_prepared_prompt(
    context: &ReviewGoverningContext,
    contract: Option<&ReviewContract>,
    shell: bool,
    mut prompt: String,
) -> Result<String, String> {
    if shell {
        // Shell descriptions are executable programs. Supply structured context
        // as data without appending natural language to the user's command.
        let quote = |value: &str| format!("'{}'", value.replace('\'', "'\"'\"'"));
        let mut prelude = format!(
            "export FORGE_GOVERNING_CONTEXT={}\n",
            quote(&serde_json::to_string(context).map_err(|e| e.to_string())?)
        );
        if let Some(contract) = contract {
            prelude.push_str(&format!(
                "export FORGE_REVIEW_CONTRACT={}\n",
                quote(&serde_json::to_string(contract).map_err(|e| e.to_string())?)
            ));
        }
        prelude.push_str(&prompt);
        return Ok(prelude);
    }
    if let Some(contract) = contract {
        prompt.push_str(&contract_prompt(contract));
    } else {
        prompt.push_str(&governing_prompt(context));
    }
    if prompt.len() > MAX_PREPARED_PROMPT_BYTES {
        return Err(format!(
            "prepared execution prompt is {} bytes, above the {}-byte admission limit; narrow the Task prompt or review scope",
            prompt.len(),
            MAX_PREPARED_PROMPT_BYTES
        ));
    }
    Ok(prompt)
}

/// Largest candidate diff inlined into a reviewer prompt. A larger delta is
/// summarized by its stat and the reviewer reads the rest with `git diff`.
const MAX_INLINE_DIFF_BYTES: usize = 48 * 1024;
/// Largest slice of the previous review's report carried into a re-review.
const MAX_PRIOR_REPORT_BYTES: usize = 8 * 1024;
/// Largest open-findings list carried into a re-review; ids past it are
/// listed bare. Each line's summary and location are clipped first.
const MAX_PRIOR_FINDINGS_BYTES: usize = 8 * 1024;
const MAX_PRIOR_FINDING_SUMMARY_CHARS: usize = 200;
const MAX_PRIOR_FINDING_WHERE_CHARS: usize = 120;

/// Context a reviewer would otherwise spend its first turns fetching: the
/// candidate diff and, on a re-review, the previous verdict and what changed
/// since it. Everything here is best effort; a git or lookup failure drops
/// that section instead of refusing admission.
async fn reviewer_context_prompt(
    db: &SqliteDb,
    task_id: &str,
    execution_id: &str,
    path: &(impl ReviewWorkspace + ?Sized),
    contract: &ReviewContract,
    budget: usize,
    include_verify: bool,
) -> String {
    let mut out = String::new();
    if include_verify {
        if let Ok(reviews) = db::ReviewRepo::list_by_task(db, task_id).await {
            if let Some(review) = reviews
                .iter()
                .filter(|r| r.status == db::ReviewStatus::Running)
                .max_by_key(|r| r.attempt_number)
            {
                if let Ok(details) = serde_json::from_str::<Value>(&review.step_results_json) {
                    if let Ok(verify) = serde_json::from_value::<api_types::ReviewVerifyDetails>(
                        details["verify"].clone(),
                    ) {
                        out.push_str(&crate::verify::prompt(&verify));
                    }
                }
            }
        }
    }
    if contract.base_sha != contract.commit_sha {
        let range = format!("{}..{}", contract.base_sha, contract.commit_sha);
        let diff_budget = MAX_INLINE_DIFF_BYTES.min(budget / 2);
        if let Some(section) = diff_section(path, &range, diff_budget).await {
            out.push_str("\n\nCandidate diff (");
            out.push_str(&range);
            out.push_str("):\n");
            out.push_str(&section);
        }
    }
    if let Some(section) = prior_review_section(
        db,
        task_id,
        execution_id,
        path,
        contract,
        budget.saturating_sub(out.len()),
    )
    .await
    {
        out.push_str(&section);
    }
    if out.len() > budget {
        return String::new();
    }
    out
}

/// `git diff --stat` plus the full diff when it fits in `limit` bytes.
async fn diff_section(
    path: &(impl ReviewWorkspace + ?Sized),
    range: &str,
    limit: usize,
) -> Option<String> {
    let stat = try_git_read(path, &["diff", "--stat=120", range, "--"])
        .await
        .ok()
        .flatten()?;
    let mut section = format!("```\n{}```\n", stat);
    let diff = try_git_read(path, &["diff", "--no-color", "--no-ext-diff", range, "--"])
        .await
        .ok()
        .flatten();
    match diff {
        Some(diff) if !diff.is_empty() && section.len() + diff.len() + 16 <= limit => {
            section.push_str("```diff\n");
            section.push_str(&diff);
            section.push_str("```\n");
        }
        Some(diff) if !diff.is_empty() => section.push_str(&format!(
            "The full diff ({} bytes) is too large to inline; read the files you need with `git diff {range} -- <path>`.\n",
            diff.len()
        )),
        _ => {}
    }
    (section.len() <= limit).then_some(section)
}

/// The latest earlier review of this Task that reached a verdict, and on a
/// re-review the diff since the commit it judged.
async fn prior_review_section(
    db: &SqliteDb,
    task_id: &str,
    execution_id: &str,
    path: &(impl ReviewWorkspace + ?Sized),
    contract: &ReviewContract,
    budget: usize,
) -> Option<String> {
    let mut reviews = ReviewRepo::list_by_task(db, task_id).await.ok()?;
    let current_attempt = reviews
        .iter()
        .find(|review| {
            review.reviewer_execution_id.as_deref() == Some(execution_id)
                || review.auditor_execution_id.as_deref() == Some(execution_id)
                || review.execution_id == execution_id
        })
        .map(|review| review.attempt_number);
    let earlier =
        |review: &Review| current_attempt.is_none_or(|attempt| review.attempt_number < attempt);
    reviews.retain(earlier);
    // The previous verdict is context whatever the budget window; only the
    // ledger restarts at a recovery reset. A ledger read that fails drops the
    // open list, not the verdict.
    let mut window = db
        .review_window(task_id)
        .await
        .map(|window| window.reviews)
        .unwrap_or_default();
    window.retain(earlier);
    let ledger = db::review_findings::finding_ledger(&window).unwrap_or_default();
    let mut section = String::new();
    let mut prior_commit = None;
    for review in reviews.iter().rev() {
        let Some(reviewer_execution_id) = review
            .reviewer_execution_id
            .as_deref()
            .or(review.auditor_execution_id.as_deref())
        else {
            continue;
        };
        let Ok(Some(conformance)) = db.review_conformance(reviewer_execution_id).await else {
            continue;
        };
        let Some(assessment) = conformance.assessment else {
            continue;
        };
        prior_commit = conformance.contract.map(|prior| prior.commit_sha);
        let result = serde_json::to_value(assessment.result)
            .ok()
            .and_then(|value| value.as_str().map(str::to_owned))
            .unwrap_or_default();
        section.push_str(&format!(
            "\n\nPrevious review (attempt {}, result {result}{}): {}\n",
            review.attempt_number,
            prior_commit
                .as_deref()
                .map(|sha| format!(", commit {sha}"))
                .unwrap_or_default(),
            assessment.reason
        ));
        let report = assessment.report.trim();
        if !report.is_empty() {
            section.push_str("Its report:\n");
            section.push_str(truncate_at_char_boundary(report, MAX_PRIOR_REPORT_BYTES));
            if report.len() > MAX_PRIOR_REPORT_BYTES {
                section.push_str("\n[report truncated]");
            }
            section.push('\n');
        }
        break;
    }
    if section.is_empty() {
        let latest = window.last()?;
        section.push_str(&format!(
            "\n\nPrevious review (attempt {}, status {}):\n",
            latest.attempt_number, latest.status
        ));
    }
    let open: Vec<_> = ledger
        .iter()
        .filter(|entry| entry.status.is_open())
        .collect();
    if !open.is_empty() {
        section.push_str("Open findings in the prior review ledger:\n");
        // Every id fits: the ids are what keep findings stable across rounds.
        let mut listed = 0;
        let mut overflow = Vec::new();
        for entry in open {
            let mut line = format!(
                "- {}: {}",
                entry.id,
                clip_chars(&entry.summary, MAX_PRIOR_FINDING_SUMMARY_CHARS)
            );
            if let Some(location) = &entry.r#where {
                line.push_str(&format!(
                    " ({})",
                    clip_chars(location, MAX_PRIOR_FINDING_WHERE_CHARS)
                ));
            }
            line.push('\n');
            if overflow.is_empty() && listed + line.len() <= MAX_PRIOR_FINDINGS_BYTES {
                listed += line.len();
                section.push_str(&line);
            } else {
                overflow.push(entry.id.as_str());
            }
        }
        if !overflow.is_empty() {
            section.push_str(&format!(
                "Also open (details in the Review history): {}\n",
                overflow.join(", ")
            ));
        }
        section.push_str("For each reviewer finding still open, copy its id into findings. Put fixed reviewer ids in resolved; omission means unconfirmed. Forge re-checks its own CI, check and verify ids.\n");
    }
    section.push_str(&format!("Frozen requirement ids: {}. After the first review in this window, a new blocking finding must be a regression, cite an unmet requirement, or name a file in candidate_changed_paths. Mark shared causes with same_cause_as.\n",
        contract.context.requirements.iter().map(|requirement| requirement.id.as_str()).collect::<Vec<_>>().join(", ")));
    section.push_str("Confirm each problem it raised is resolved, and review what changed since. Do not reopen parts it accepted unless the new changes touch them.\n");
    if let Some(prior_commit) = prior_commit.filter(|sha| *sha != contract.commit_sha) {
        let is_ancestor = try_git_read(
            path,
            &[
                "merge-base",
                "--is-ancestor",
                &prior_commit,
                &contract.commit_sha,
            ],
        )
        .await
        .ok()
        .flatten()
        .is_some();
        if is_ancestor {
            let range = format!("{prior_commit}..{}", contract.commit_sha);
            let limit = MAX_INLINE_DIFF_BYTES.min(budget.saturating_sub(section.len()) / 2);
            if let Some(diff) = diff_section(path, &range, limit).await {
                section.push_str(&format!("Changes since the previous review ({range}):\n"));
                section.push_str(&diff);
            }
        } else {
            section.push_str("The branch was rewritten since that review (for example rebased), so there is no incremental diff; compare against the candidate diff above.\n");
        }
    }
    (section.len() <= budget).then_some(section)
}

fn clip_chars(text: &str, limit: usize) -> String {
    match text.char_indices().nth(limit) {
        Some((end, _)) => format!("{}…", &text[..end]),
        None => text.to_owned(),
    }
}

fn truncate_at_char_boundary(text: &str, limit: usize) -> &str {
    if text.len() <= limit {
        return text;
    }
    let mut end = limit;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

#[cfg(test)]
mod tests;
