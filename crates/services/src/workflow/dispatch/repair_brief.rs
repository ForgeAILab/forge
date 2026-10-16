//! One derived repair request, with bounded attempt memory and shared causes.
use std::collections::{BTreeMap, BTreeSet};

use api_types::{ReviewFindingEntry, ReviewFindingSource as Source, ReviewFindingStatus as Status};
use serde_json::Value;

use super::AgentDispatchContext;

pub const BRIEF_BYTES: usize = 24 * 1024;

#[derive(Debug, Clone)]
pub struct RepairBudget {
    pub name: String,
    pub spent: i64,
    pub limit: i64,
}

impl RepairBudget {
    fn render(&self) -> String {
        format!(
            "{} budget: {} of {} spent; {} remaining",
            self.name,
            self.spent,
            self.limit,
            db::budget::remaining(self.limit, self.spent)
        )
    }
}

#[derive(Debug, Clone)]
pub struct RepairAttempt {
    pub attempt: i64,
    pub from_sha: Option<String>,
    pub to_sha: Option<String>,
    pub files: Vec<String>,
    pub files_unavailable: bool,
    pub summary: Option<String>,
}

struct CheckOutput {
    label: String,
    tail: String,
    failures: Vec<String>,
    truncated: bool,
}

/// The brief is transient: merge findings never enter the persisted ledger.
pub struct RepairBrief {
    task: String,
    round: String,
    open: Vec<ReviewFindingEntry>,
    updates: Vec<ReviewFindingEntry>,
    checks: Vec<CheckOutput>,
    verify: String,
    report: String,
    attempts: Vec<String>,
    requirements: String,
    patterns: Vec<Vec<ReviewFindingEntry>>,
    carried: bool,
    logs: String,
    merge: Option<(&'static str, String)>,
    instructions: &'static str,
    displayed_ci: BTreeSet<String>,
    limited_steps: BTreeSet<String>,
}

impl RepairBrief {
    pub fn review(ctx: &AgentDispatchContext) -> Self {
        let reviews = ctx.review_window.as_deref().unwrap_or(&ctx.prior_reviews);
        let latest = reviews.iter().max_by_key(|review| review.attempt_number);
        let attempt = latest.map_or(0, |review| review.attempt_number);
        let details = latest
            .and_then(|review| serde_json::from_str::<Value>(&review.step_results_json).ok())
            .unwrap_or_default();
        let ledger = db::review_findings::finding_ledger(reviews).unwrap_or_else(|error| {
            tracing::warn!(task_id = %ctx.task.id, %error, "repair brief has no readable finding ledger");
            Vec::new()
        });
        let prior: Vec<_> = reviews
            .iter()
            .filter(|review| review.attempt_number < attempt)
            .cloned()
            .collect();
        let previous_ledger = db::review_findings::finding_ledger(&prior).unwrap_or_default();
        let mut task = format!("Task: {}", bounded_text(&ctx.task.title, 1024));
        if let Some(description) = ctx.task.description.as_deref() {
            task.push_str(&format!(
                "\n\nDescription:\n{}",
                bounded_text(description, 4 * 1024)
            ));
        }
        if let Some(plan) = ctx.plan.as_deref().filter(|plan| !plan.trim().is_empty()) {
            task.push_str(&format!(
                "\n\nOriginal plan:\n{}",
                bounded_text(plan, 4 * 1024)
            ));
        }
        let mut checks = Vec::new();
        let mut waived = Vec::new();
        let mut displayed_ci = BTreeSet::new();
        let mut limited_steps = BTreeSet::new();
        for step in details
            .get("ci_steps")
            .or_else(|| details.as_array().map(|_| &details))
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter(|step| step["exit_code"].as_i64().is_some_and(|exit| exit != 0))
        {
            let index = step["index"].as_u64().unwrap_or(0) as usize;
            let mut failures: Vec<_> = step["failures"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect();
            let triage = step
                .get("triage")
                .cloned()
                .and_then(|triage| serde_json::from_value::<api_types::StepCiTriage>(triage).ok());
            if let Some(triage) = &triage {
                for failure in &triage.failures {
                    if !failures.contains(&failure.signature) {
                        failures.push(failure.signature.clone());
                    }
                }
            }
            if !failures.is_empty() {
                limited_steps.insert(format!("CI-{index}"));
                // Reserve the display allowance for blocking findings before
                // triaged updates, including signatures first seen on rerun.
                displayed_ci.extend(
                    failures
                        .iter()
                        .filter(|signature| {
                            ledger.iter().any(|entry| {
                                entry.id == db::review_findings::ci_finding_id(index, signature)
                                    && entry.status.is_open()
                            })
                        })
                        .take(20)
                        .map(|signature| db::review_findings::ci_finding_id(index, signature)),
                );
            }
            // Waived output goes last: the shared tail allowance is spent on
            // the failures this round must repair first.
            let list = if triage.as_ref().is_some_and(|triage| !triage.blocking) {
                &mut waived
            } else {
                &mut checks
            };
            list.push(CheckOutput {
                label: format!(
                    "CI-{index}: {} (exit {}){}",
                    step["command"].as_str().unwrap_or("CI step"),
                    step["exit_code"],
                    triage.as_ref().map_or_else(String::new, |triage| format!(
                        " — triage {} ({}){}",
                        serde_json::to_value(triage.mode).unwrap().as_str().unwrap(),
                        if triage.blocking {
                            "blocking"
                        } else {
                            "non-blocking"
                        },
                        triage
                            .reason
                            .as_deref()
                            .map_or_else(String::new, |reason| format!("; {reason}")),
                    ))
                ),
                tail: output(step).to_owned(),
                failures,
                truncated: step["failures_truncated"].as_bool().unwrap_or(false),
            });
        }
        for check in details["conformance"]["checks"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|check| check["exit_code"].as_i64().is_some_and(|exit| exit != 0))
        {
            let id = check["check_id"].as_str().unwrap_or("check");
            if id.strip_prefix("ci:").is_some_and(|index| {
                checks
                    .iter()
                    .chain(&waived)
                    .any(|check| check.label.starts_with(&format!("CI-{index}: ")))
            }) {
                continue;
            }
            checks.push(CheckOutput {
                label: format!("{id}: {}", check["command"].as_str().unwrap_or("check")),
                tail: check["output"].as_str().unwrap_or_default().to_owned(),
                failures: Vec::new(),
                truncated: false,
            });
        }
        checks.extend(waived);
        let has_assessment = details["conformance"]["assessment"].is_object()
            || details.get("auditor").is_some_and(|value| !value.is_null());
        let report = if has_assessment {
            let assessment = &details["conformance"]["assessment"];
            match assessment["report"].as_str() {
                Some(report) => format!(
                    "{}\n\n{report}",
                    assessment["reason"].as_str().unwrap_or_default()
                ),
                None => ctx
                    .latest_review_feedback
                    .clone()
                    .or_else(|| details["auditor"]["reason"].as_str().map(str::to_owned))
                    .unwrap_or_default(),
            }
        } else if checks.is_empty() {
            // The rejection that sent this role back is current by
            // construction; it is the only cause when review failed before
            // a Review row was written.
            let rejection = ctx
                .transition_log
                .iter()
                .rev()
                .filter(|entry| !entry.bridge.is_repair_no_change())
                .find(|entry| entry.to_state == ctx.state_name)
                .filter(|entry| entry.rejection)
                .map(|entry| entry.trigger_reason.clone());
            ctx.latest_review_feedback
                .clone()
                .filter(|_| latest.is_some() || ctx.review_window.is_none())
                .or_else(|| ctx.last_manual_bounce_reason.clone())
                .or(rejection)
                .unwrap_or_default()
        } else {
            String::new()
        };
        let mut logs = String::new();
        for (label, value) in [
            ("Reviewer execution", &ctx.latest_review_execution_id),
            ("Reviewer log file", &ctx.latest_review_logs_path),
            (
                "Previous coder execution",
                &ctx.continuation_of_execution_id,
            ),
            ("Previous coder log file", &ctx.continuation_logs_path),
        ] {
            if label.starts_with("Reviewer") && latest.is_none() && ctx.review_window.is_some() {
                continue;
            }
            if let Some(value) = value {
                logs.push_str(&format!("{label}:\n{value}\n"));
            }
        }
        let mut round = format!(
            "Repair after review {attempt}\n{}",
            ctx.repair_budget
                .as_ref()
                .map(RepairBudget::render)
                .unwrap_or_else(|| "Budget accounting unavailable".into())
        );
        if has_assessment {
            round.push_str("\n\nThe reviewer agent flagged the previous implementation.");
        }
        Self {
            task, round,
            open: ledger.iter().filter(|entry| entry.status.is_open()).cloned().collect(),
            updates: ledger.iter().filter(|entry| !entry.status.is_open() && entry.last_checked_attempt == attempt
                && entry.status != Status::Superseded
                && (matches!(entry.status, Status::Flaky | Status::PreExisting)
                    || !previous_ledger.iter().any(|prior| prior.id == entry.id && prior.status == entry.status))).cloned().collect(),
            checks, verify: render_verify(&details["verify"]), report,
            attempts: render_attempts(ctx, reviews),
            requirements: requirement_status(&details, &ledger),
            patterns: finding_patterns(&ledger),
            carried: db::review_findings::has_carried_finding(reviews), logs,
            merge: None,
            instructions: "Address the blocking review and validation findings, preserve the Task's intent, run the relevant checks, and complete delivery according to the execution harness contract. The Task returns for re-verification.",
            displayed_ci, limited_steps,
        }
    }

    pub fn automatic_recovery(mut self, budget: RepairBudget, failure_reason: &str) -> Self {
        self.round = format!(
            "{}\nAutomatic recovery\n{}\nReview failure: {}",
            self.round,
            budget.render(),
            clip(failure_reason, 2000, true)
        );
        self.instructions = "The normal review retry flow is about to block this Task, so this is the final automatic recovery attempt. Inspect the workspace and the review failure context, make the smallest useful change that addresses the failing review, and leave the Task ready for the normal workflow to review again.";
        self
    }

    /// Only the two coder-actionable merge handoffs use the brief. The dirty
    /// worktree and manual-owner messages retain their existing text.
    pub fn merge(ctx: &AgentDispatchContext) -> Option<Self> {
        let transition = ctx
            .transition_log
            .iter()
            .rev()
            .find(|entry| entry.to_state == ctx.state_name);
        let diagnostic = ctx.task.condition.read().diagnostic;
        if diagnostic
            .as_ref()
            .is_some_and(|entry| entry.annotation_type == api_types::FailureKind::DirtyWorktree)
        {
            return None;
        }
        let conflict = transition.is_some_and(|entry| {
            entry.bridge.bridge_kind == Some(api_types::TransitionBridgeKind::ConflictHandoff)
        });
        let ci = diagnostic
            .as_ref()
            .is_some_and(|entry| entry.annotation_type == api_types::FailureKind::CiFailed);
        if !conflict && !ci {
            return None;
        }
        let mut brief = Self::review(ctx);
        brief.round = format!(
            "Merge repair round {}\n{}",
            ctx.repair_budget.as_ref().map_or(0, |budget| budget.spent),
            ctx.repair_budget
                .as_ref()
                .map(RepairBudget::render)
                .unwrap_or_else(|| "Budget accounting unavailable".into())
        );
        if conflict {
            brief.round = format!(
                "Merge conflict handoff round {}\n{}\nmerge_fix is not spent",
                ctx.repair_budget.as_ref().map_or(0, |budget| budget.spent),
                ctx.repair_budget
                    .as_ref()
                    .map(RepairBudget::render)
                    .unwrap_or_else(|| "conflict_handoff budget accounting unavailable".into())
            );
        }
        let evidence = transition
            .map(|entry| entry.trigger_reason.clone())
            .or_else(|| diagnostic.and_then(|entry| entry.message))
            .unwrap_or_default();
        brief.merge = Some((
            if conflict {
                "MERGE-CONFLICT"
            } else {
                "MERGE-CI"
            },
            evidence,
        ));
        // Review authority is handled by Forge after repair; do not imply a
        // new review for a conflict whose authority can carry.
        brief.carried = false;
        brief.patterns.clear();
        brief.requirements.clear();
        brief.open.clear();
        brief.updates.clear();
        brief.checks.clear();
        brief.verify.clear();
        brief.report.clear();
        brief.instructions = if conflict {
            CONFLICT_INSTRUCTIONS
        } else {
            "Checks failed after integration rebased this Task onto the target branch. Fix the command failure shown below, preserve the Task's intent, run the relevant checks, commit, and complete delivery. Never rebase, merge, or rewrite history — Forge owns integration."
        };
        if !conflict && ctx.task.review_passed_at.is_some() {
            brief.round.push_str("\n\nReview already passed before the merge failure. Preserve that reviewed implementation and verify the repair; do not redesign the Task.");
        }
        // A merge handoff has no reviewer execution to inspect.
        brief.logs = String::new();
        for (label, value) in [
            ("Previous execution", &ctx.continuation_of_execution_id),
            ("Previous log file", &ctx.continuation_logs_path),
        ] {
            if let Some(value) = value {
                brief.logs.push_str(&format!("{label}:\n{value}\n"));
            }
        }
        Some(brief)
    }

    pub fn render(self) -> String {
        let mut limits = Limits {
            report: self.report.len(),
            attempts: self.attempts.len(),
            tail: usize::MAX,
            tails_total: usize::MAX,
            ids_only: false,
            verify: usize::MAX,
            compact: false,
            detailed: usize::MAX,
        };
        // D7's ordered degradation first; then the hard-cap reductions of the
        // remaining unprotected sections, and only then finding text.
        for stage in 0..=9 {
            match stage {
                1 => limits.report = limits.report.min(2 * 1024),
                2 => limits.report = 0,
                3..=5 => limits.attempts = limits.attempts.saturating_sub(1),
                6 => limits.tail = 1024,
                7 => limits.ids_only = true,
                8 => {
                    limits.tails_total = 4 * 1024;
                    limits.verify = 2 * 1024;
                }
                9 => limits.compact = true,
                _ => {}
            }
            let body = self.render_sections(&limits);
            if body.len() <= BRIEF_BYTES {
                return body;
            }
        }
        // Past a line budget, the remaining open findings are listed by id.
        let (mut low, mut high) = (0, self.open.len());
        while low < high {
            let mid = (low + high).div_ceil(2);
            limits.detailed = mid;
            if self.render_sections(&limits).len() <= BRIEF_BYTES {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        limits.detailed = low;
        let body = self.render_sections(&limits);
        // An arbitrarily large window can contain more ids than the entire
        // transport budget. Keep the complete ledger authoritative and name the
        // truncation explicitly even in this final, ids-only case.
        bounded_text(&body, BRIEF_BYTES)
    }

    fn open_findings(&self, compact: bool, detailed: usize) -> (String, BTreeMap<&str, usize>) {
        let mut open = String::new();
        let mut counts = BTreeMap::<&str, usize>::new();
        let mut omitted = BTreeMap::<&str, usize>::new();
        let mut bare = Vec::new();
        let mut shown = 0;
        for entry in &self.open {
            if entry.source == Source::Ci && entry.id.contains('#') {
                let step = entry.id.split('#').next().unwrap_or(&entry.id);
                let count = counts.entry(step).or_default();
                let show = if self.limited_steps.contains(step) {
                    self.displayed_ci.contains(&entry.id)
                } else {
                    *count < 20
                };
                if !show {
                    *omitted.entry(step).or_default() += 1;
                    continue;
                }
                *count += 1;
            }
            if shown >= detailed {
                bare.push(clip(&entry.id, 120, compact));
                continue;
            }
            shown += 1;
            open.push_str(&format!(
                "- {} [{}, {}]: {} — survived {} repair round(s)\n",
                clip(&entry.id, 120, compact),
                format!("{:?}", entry.source).to_lowercase(),
                format!("{:?}", entry.severity).to_lowercase(),
                clip(&entry.summary, 200, compact),
                entry.streak.saturating_sub(1)
            ));
            if entry.status == Status::Reopened {
                open.push_str(&format!(
                    "  re-broken: resolved in round {}\n",
                    entry.resolved_attempt.unwrap_or_default()
                ));
            }
            if let Some(note) = &entry.anchoring_note {
                open.push_str(&format!("  {note}\n"));
            }
            if let Some(class) = entry.ci_triage {
                open.push_str(&format!(
                    "  CI classification: {}\n",
                    serde_json::to_value(class).unwrap().as_str().unwrap()
                ));
            }
            for (label, value, chars) in [
                ("where", &entry.r#where, 120),
                ("expected", &entry.expected, 300),
                ("observed", &entry.observed, 300),
            ] {
                if let Some(value) = value {
                    open.push_str(&format!("  {label}: {}\n", clip(value, chars, compact)));
                }
            }
        }
        if !bare.is_empty() {
            open.push_str(&format!(
                "Also open (details in the finding ledger): {}\n",
                bare.join(", ")
            ));
        }
        for (step, count) in omitted {
            open.push_str(&format!("{step}: +{count} more (see the finding ledger)\n"));
        }
        if let Some((id, evidence)) = &self.merge {
            open.push_str(&format!("- {id} [merge, blocking]:\n{evidence}\n"));
        }
        (open, counts)
    }

    fn render_sections(&self, limits: &Limits) -> String {
        let mut out = if self.carried {
            format!("Before editing, write a short root-cause note for each carried finding. Record it as a decision worklog entry.\n\n{}", self.task)
        } else {
            self.task.clone()
        };
        section(
            &mut out,
            "Round",
            &format!("{}\n\n{}", self.round, self.instructions),
        );
        section(&mut out, "Requirement status", &self.requirements);
        let mut patterns = String::new();
        for family in &self.patterns {
            patterns.push_str(
                "These share a cause. Fix it at every call site and list the sites you changed.\n",
            );
            for entry in family {
                patterns.push_str(&format!(
                    "- {}: {}\n",
                    clip(&entry.id, 120, limits.compact),
                    clip(
                        entry.r#where.as_deref().unwrap_or("location unavailable"),
                        120,
                        limits.compact
                    )
                ));
            }
        }
        section(&mut out, "Pattern", &patterns);
        let (open, mut ci_counts) = self.open_findings(limits.compact, limits.detailed);
        section(
            &mut out,
            "Open findings",
            if open.is_empty() {
                "No structured findings; use the failure context below."
            } else {
                &open
            },
        );
        let mut updates = String::new();
        let mut omitted_updates = BTreeMap::<&str, usize>::new();
        let mut updates_bytes = 0;
        let mut updates_omitted = 0;
        for entry in &self.updates {
            if entry.source == Source::Ci && entry.id.contains('#') {
                let step = entry.id.split('#').next().unwrap_or(&entry.id);
                let count = ci_counts.entry(step).or_default();
                if *count >= 20 {
                    *omitted_updates.entry(step).or_default() += 1;
                    continue;
                }
                *count += 1;
            }
            let status = match entry.status {
                Status::Resolved => "resolved",
                Status::Flaky => "flaky (non-blocking)",
                Status::PreExisting => "pre_existing (non-blocking)",
                Status::OutOfScope => "out_of_scope (follow-up candidate)",
                _ => "not re-checked",
            };
            let line = format!(
                "- {}: {status}{}\n",
                clip(&entry.id, 120, limits.compact),
                if limits.ids_only {
                    String::new()
                } else {
                    format!(" — {}", entry.summary)
                }
            );
            if limits.compact && updates_bytes + line.len() > 1024 {
                updates_omitted += 1;
            } else {
                updates_bytes += line.len();
                updates.push_str(&line);
            }
        }
        if updates_omitted > 0 {
            updates.push_str(&format!(
                "+{updates_omitted} more updates (see the finding ledger)\n"
            ));
        }
        for (step, count) in omitted_updates {
            updates.push_str(&format!("{step}: +{count} more (see the finding ledger)\n"));
        }
        let updates = if limits.compact {
            bounded_text(&updates, 1024)
        } else {
            updates
        };
        section(
            &mut out,
            "Resolved and unconfirmed since the last round",
            &updates,
        );
        let mut checks = String::new();
        let mut tails_left = limits.tails_total;
        // The ledger already names these tests above. Legacy unreadable
        // rows still expose their signatures with their check output.
        let signatures_in_ledger = self
            .open
            .iter()
            .chain(&self.updates)
            .any(|entry| entry.source == Source::Ci);
        for check in &self.checks {
            checks.push_str(&format!("{}\n", clip(&check.label, 300, limits.compact)));
            if !signatures_in_ledger {
                for signature in check.failures.iter().take(20) {
                    checks.push_str(&format!("- {}\n", clip(signature, 120, limits.compact)));
                }
            }
            if !signatures_in_ledger && check.failures.len() > 20 {
                checks.push_str(&format!("+{} more\n", check.failures.len() - 20));
            }
            if check.truncated {
                checks.push_str("Additional failure signatures were truncated.\n");
            }
            let output = tail(&check.tail, limits.tail);
            if output.len() <= tails_left {
                tails_left -= output.len();
                checks.push_str(output);
            } else {
                tails_left = 0;
                checks.push_str("(output omitted; see the Review's CI step results)");
            }
            checks.push('\n');
        }
        let checks = if limits.compact {
            bounded_text(&checks, 8 * 1024)
        } else {
            checks
        };
        section(&mut out, "Failing check output", &checks);
        let verify = prefix(&self.verify, limits.verify);
        if verify.len() < self.verify.len() {
            section(
                &mut out,
                "Verify results and evidence",
                &format!("{verify}\n… (truncated; see the Review tab)"),
            );
        } else {
            section(&mut out, "Verify results and evidence", verify);
        }
        section(
            &mut out,
            "Reviewer report",
            prefix(&self.report, limits.report),
        );
        if limits.attempts > 0 {
            let attempts = self
                .attempts
                .iter()
                .skip(self.attempts.len().saturating_sub(limits.attempts))
                .cloned()
                .collect::<Vec<_>>()
                .join("\n\n");
            section(&mut out, "Attempts", &format!("{attempts}\n\nDo not repeat an approach that left a finding open; if you keep one, say why it failed before."));
        }
        section(&mut out, "Logs", &bounded_text(&self.logs, 1024));
        out
    }
}

struct Limits {
    report: usize,
    attempts: usize,
    tail: usize,
    tails_total: usize,
    ids_only: bool,
    verify: usize,
    compact: bool,
    detailed: usize,
}

pub(super) fn repair_rounds(reviews: &[db::Review]) -> Vec<&db::Review> {
    let mut ordered: Vec<_> = reviews
        .iter()
        .filter(|review| {
            matches!(
                review.status,
                db::ReviewStatus::Passed | db::ReviewStatus::Failed
            ) && !serde_json::from_str::<Value>(&review.step_results_json)
                .unwrap_or_default()
                .get("manual_override")
                .is_some_and(|value| !value.is_null())
        })
        .collect();
    ordered.sort_by_key(|review| review.attempt_number);
    ordered
}

fn render_attempts(ctx: &AgentDispatchContext, reviews: &[db::Review]) -> Vec<String> {
    let ordered = repair_rounds(reviews);
    ordered.iter().enumerate().skip(ordered.len().saturating_sub(3)).map(|(index, review)| {
        let memory = ctx.repair_attempts.iter().find(|memory| memory.attempt == review.attempt_number);
        let earlier: Vec<_> = ordered[..index].iter().map(|review| (*review).clone()).collect();
        let previous = db::review_findings::finding_ledger(&earlier).unwrap_or_default();
        let mut through = earlier;
        through.push((*review).clone());
        let ledger = db::review_findings::finding_ledger(&through).unwrap_or_default();
        let ids = |predicate: &dyn Fn(&ReviewFindingEntry) -> bool| ledger.iter().filter(|entry| predicate(entry)).map(|entry| entry.id.clone()).collect::<Vec<_>>().join(", ");
        let resolved = ids(&|entry| entry.status == Status::Resolved && !previous.iter().any(|prior| prior.id == entry.id && prior.status == Status::Resolved));
        let left = ids(&|entry| entry.status.is_open() && previous.iter().any(|prior| prior.id == entry.id && prior.status.is_open()));
        let opened = ids(&|entry| entry.status.is_open() && !previous.iter().any(|prior| prior.id == entry.id && prior.status.is_open()));
        let details: Value = serde_json::from_str(&review.step_results_json).unwrap_or_default();
        let from = memory.and_then(|memory| memory.from_sha.as_deref()).unwrap_or("unavailable");
        let to = memory.and_then(|memory| memory.to_sha.as_deref()).or_else(|| details["conformance"]["contract"]["commit_sha"].as_str()).unwrap_or("unavailable");
        let summary = memory.and_then(|memory| memory.summary.as_deref())
            .or_else(|| (index + 1 == ordered.len()).then_some(ctx.previous_coder_summary.as_deref()).flatten()).unwrap_or("unavailable");
        format!("Round {}: {from}..{to}\nFiles changed: {}\nCoder completion: {}\nResolved: {}\nLeft open: {}\nOpened: {}", review.attempt_number,
            memory.filter(|memory| !memory.files_unavailable).map(|memory| memory.files.join(", ")).unwrap_or_else(|| "unavailable".into()),
            bounded_text(summary, 1024), resolved, left, opened)
    }).collect()
}

fn requirement_status(details: &Value, ledger: &[ReviewFindingEntry]) -> String {
    details["conformance"]["contract"]["context"]["requirements"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|requirement| requirement["id"].as_str())
        .map(|id| {
            let cited: Vec<_> = ledger
                .iter()
                .filter(|entry| entry.requirement_ids.iter().any(|known| known == id))
                .collect();
            let status = if cited.iter().any(|entry| {
                entry.status.is_open()
                    && entry.severity == api_types::ReviewFindingSeverity::Blocking
            }) {
                "unmet"
            } else if !cited.is_empty()
                && cited.iter().all(|entry| entry.status == Status::Resolved)
            {
                "addressed"
            } else {
                "no open finding"
            };
            format!("- {id}: {status}\n")
        })
        .collect()
}

fn finding_patterns(ledger: &[ReviewFindingEntry]) -> Vec<Vec<ReviewFindingEntry>> {
    // Connected components also handle transitive links and defensive cycles.
    let mut families: Vec<BTreeSet<String>> = Vec::new();
    for entry in ledger {
        let Some(parent) = entry
            .same_cause_as
            .as_ref()
            .filter(|parent| ledger.iter().any(|entry| &entry.id == *parent))
        else {
            continue;
        };
        let mut family = BTreeSet::from([entry.id.clone(), parent.clone()]);
        let mut index = 0;
        while index < families.len() {
            if !families[index].is_disjoint(&family) {
                family.extend(families.remove(index));
                index = 0;
            } else {
                index += 1;
            }
        }
        families.push(family);
    }
    families
        .into_iter()
        .map(|family| {
            ledger
                .iter()
                .filter(|entry| family.contains(&entry.id))
                .cloned()
                .collect()
        })
        .collect()
}

/// Finding text clipped like the review comment once the brief is compacted.
fn clip(value: &str, chars: usize, compact: bool) -> String {
    match value.char_indices().nth(chars).filter(|_| compact) {
        Some((end, _)) => format!("{}…", &value[..end]),
        None => value.to_owned(),
    }
}

fn section(out: &mut String, title: &str, content: &str) {
    if !content.trim().is_empty() {
        out.push_str(&format!("\n\n{title}:\n{}", content.trim_end()));
    }
}
fn output(step: &Value) -> &str {
    step["output_tail"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| step["stderr_tail"].as_str())
        .unwrap_or_default()
}
fn prefix(value: &str, bytes: usize) -> &str {
    let mut end = bytes.min(value.len());
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}
fn tail(value: &str, bytes: usize) -> &str {
    let mut start = value.len().saturating_sub(bytes);
    while !value.is_char_boundary(start) {
        start += 1;
    }
    &value[start..]
}
fn render_verify(verify: &Value) -> String {
    if !verify.is_object() {
        return String::new();
    }
    // Keep recorded paths, media ids, start/ready failures, and future optional
    // evidence metadata without inventing a second verify wire contract.
    serde_json::to_string_pretty(verify).unwrap_or_default()
}

const CONFLICT_INSTRUCTIONS: &str = "Forge rebased this Task's branch onto the latest target and committed the merge conflicts with their Git conflict markers left in place. There is no rebase in progress: reconcile the conflicts by editing files, then commit the reconciled files with an ordinary `git commit` as you would any delivery. Never rebase, merge, or rewrite history — Forge owns the branch.\n\nFor every file listed below, keep the intent of BOTH sides — the target's change and this Task's change — and remove every `<<<<<<<`, `=======`, `>>>>>>>`, and diff3 `|||||||` section. Conflicts in shared registration points (export lists, package or workspace member lists, command indexes, README sections) almost always mean keeping both entries. Where a lockfile conflicted, regenerate it with the project's tool (for example `uv lock` or `pnpm install --lockfile-only`) instead of hand-merging it. Do not rewrite or redesign the feature. Then run the relevant checks, commit, and complete delivery; uncommitted edits are not delivered. Forge refuses to integrate a handed-off file that still adds conflict markers.\n\nForge will run the project's checks on the reconciled commit and integrate it, so keep your change limited to reconciling the files listed below, and run the relevant checks yourself before you commit.";

/// Byte budget includes the UTF-8-safe truncation notice.
fn bounded_text(value: &str, bytes: usize) -> String {
    const NOTE: &str = "\n… (truncated; see the Task or finding ledger)";
    if value.len() <= bytes {
        return value.to_owned();
    }
    format!(
        "{}{}",
        prefix(value, bytes.saturating_sub(NOTE.len())),
        NOTE
    )
}
