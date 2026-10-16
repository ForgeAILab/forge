//! One derived repair request. Sections stay separate so later Attempts and
//! Pattern sections can be added without changing sources or budget policy.
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
    previous: String,
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
        let ledger = review::finding_ledger(reviews).unwrap_or_else(|error| {
            tracing::warn!(task_id = %ctx.task.id, %error, "repair brief has no readable finding ledger");
            Vec::new()
        });
        let prior: Vec<_> = reviews
            .iter()
            .filter(|review| review.attempt_number < attempt)
            .cloned()
            .collect();
        let previous_ledger = review::finding_ledger(&prior).unwrap_or_default();
        let mut task = format!("Task: {}", ctx.task.title);
        if let Some(description) = ctx.task.description.as_deref() {
            task.push_str(&format!("\n\nDescription:\n{description}"));
        }
        if let Some(plan) = ctx.plan.as_deref().filter(|plan| !plan.trim().is_empty()) {
            task.push_str(&format!("\n\nOriginal plan:\n{plan}"));
        }
        let mut checks = Vec::new();
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
            let failures: Vec<_> = step["failures"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect();
            if !failures.is_empty() {
                limited_steps.insert(format!("CI-{index}"));
                displayed_ci.extend(
                    failures
                        .iter()
                        .take(20)
                        .map(|signature| review::findings::ci_finding_id(index, signature)),
                );
            }
            checks.push(CheckOutput {
                label: format!(
                    "CI-{index}: {} (exit {})",
                    step["command"].as_str().unwrap_or("CI step"),
                    step["exit_code"]
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
            open: ledger.iter().filter(|entry| entry.status == Status::Open).cloned().collect(),
            updates: ledger.into_iter().filter(|entry| entry.status != Status::Open && entry.last_checked_attempt == attempt
                && !previous_ledger.iter().any(|prior| prior.id == entry.id && prior.status == entry.status)).collect(),
            checks, verify: render_verify(&details["verify"]), report,
            previous: ctx.previous_coder_summary.clone().unwrap_or_default(), logs,
            merge: None,
            instructions: "Address all review and validation findings, preserve the Task's intent, run the relevant checks, and complete delivery according to the execution harness contract. The Task returns for re-verification.",
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
        // A merge handoff has no reviewer execution to inspect.
        brief.logs = String::new();
        if let Some(path) = &ctx.continuation_logs_path {
            brief.logs = format!("Previous coder log file:\n{path}");
        }
        Some(brief)
    }

    pub fn render(self) -> String {
        let mut limits = Limits {
            report: self.report.len(),
            previous: true,
            tail: usize::MAX,
            tails_total: usize::MAX,
            ids_only: false,
            verify: usize::MAX,
            compact: false,
            detailed: usize::MAX,
        };
        // D7's ordered degradation first; then the hard-cap reductions of the
        // remaining unprotected sections, and only then finding text.
        for stage in 0..=7 {
            match stage {
                1 => limits.report = limits.report.min(2 * 1024),
                2 => limits.report = 0,
                3 => limits.previous = false,
                4 => limits.tail = 1024,
                5 => limits.ids_only = true,
                6 => {
                    limits.tails_total = 4 * 1024;
                    limits.verify = 2 * 1024;
                }
                7 => limits.compact = true,
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
        if body.len() > BRIEF_BYTES {
            // Only the Task's own description and plan remain uncut.
            tracing::warn!(
                bytes = body.len(),
                budget = BRIEF_BYTES,
                "repair brief exceeds budget after every permitted reduction"
            );
        }
        body
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
        let mut out = self.task.clone();
        section(
            &mut out,
            "Round",
            &format!("{}\n\n{}", self.round, self.instructions),
        );
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
            let status = if entry.status == Status::Resolved {
                "resolved"
            } else {
                "not re-checked"
            };
            updates.push_str(&format!(
                "- {}: {status}{}\n",
                clip(&entry.id, 120, limits.compact),
                if limits.ids_only {
                    String::new()
                } else {
                    format!(" — {}", entry.summary)
                }
            ));
        }
        for (step, count) in omitted_updates {
            updates.push_str(&format!("{step}: +{count} more (see the finding ledger)\n"));
        }
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
        if limits.previous {
            section(&mut out, "Previous round", &self.previous);
        }
        section(&mut out, "Logs", &self.logs);
        out
    }
}

struct Limits {
    report: usize,
    previous: bool,
    tail: usize,
    tails_total: usize,
    ids_only: bool,
    verify: usize,
    compact: bool,
    detailed: usize,
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
