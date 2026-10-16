//! Pure projection of persisted source observations. Callers supply exactly one
//! review-budget window; computed ids and state never enter frozen assessments.
use std::collections::{BTreeMap, BTreeSet};

use crate::Review;
use api_types::{
    ReviewConformance, ReviewFinding, ReviewFindingEntry, ReviewFindingKind, ReviewFindingSeverity,
    ReviewFindingSource as Source, ReviewFindingStatus as Status, StepResultEntry,
};
use serde_json::Value;
use sha2::{Digest, Sha256};

/// Stable across processes and owners; signatures are normalised by check-executor.
pub fn ci_finding_id(step: usize, signature: &str) -> String {
    let digest = Sha256::digest(signature.as_bytes());
    format!(
        "CI-{step}#{:02x}{:02x}{:02x}{:02x}",
        digest[0], digest[1], digest[2], digest[3]
    )
}

/// Derive every finding, ordered naturally by id. The API's 200-item limit is
/// applied only after derivation, so hidden entries still reserve their ids.
pub fn finding_ledger(window_reviews: &[Review]) -> Result<Vec<ReviewFindingEntry>, String> {
    let mut reviews: Vec<_> = window_reviews.iter().collect();
    reviews.sort_by_key(|review| (review.attempt_number, &review.id));
    let mut ledger = BTreeMap::<String, ReviewFindingEntry>::new();
    let mut next_id = 1;
    let mut observed_contracts = BTreeSet::new();
    // Anchoring starts after the reviewer's first assessment in the window,
    // not after the first Review row: CI can fail, or a Review be cancelled,
    // before any reviewer has set the goalposts.
    let mut assessed = false;
    for review in reviews {
        let details: Value = serde_json::from_str(&review.step_results_json)
            .map_err(|error| format!("invalid Review {} details: {error}", review.id))?;
        // An owner's manual pass copies the failed Review's details verbatim;
        // no source ran in it.
        if details
            .get("manual_override")
            .is_some_and(|value| !value.is_null())
        {
            continue;
        }
        let steps: Vec<StepResultEntry> = match &details {
            Value::Array(_) => serde_json::from_value(details.clone()),
            Value::Object(_) => serde_json::from_value(
                details
                    .get("ci_steps")
                    .cloned()
                    .unwrap_or_else(|| Value::Array(Vec::new())),
            ),
            _ => return Err(format!("invalid Review {} details", review.id)),
        }
        .map_err(|error| format!("invalid Review {} CI steps: {error}", review.id))?;
        let conformance: ReviewConformance = details
            .get("conformance")
            .filter(|value| !value.is_null())
            .map(|value| serde_json::from_value(value.clone()))
            .transpose()
            .map_err(|error| format!("invalid Review {} conformance: {error}", review.id))?
            .unwrap_or_default();
        // A queue-rebase carry copies the passed Review's frozen conformance
        // next to its own CI steps. Each frozen assessment and its checks are
        // one observation, however many Reviews hold a copy.
        let conformance = match &conformance.contract {
            Some(contract) if !observed_contracts.insert(contract.execution_id.clone()) => {
                ReviewConformance::default()
            }
            _ => conformance,
        };
        let attempt = review.attempt_number;
        // A source can occur in both pre-review CI and the frozen conformance.
        // Count it once per attempt, preferring the check's final observation.
        let mut observations = BTreeMap::new();
        let mut detailed_ci = BTreeSet::new();
        let mut superseded = BTreeSet::new();
        let mut non_blocking = BTreeSet::new();
        for step in steps {
            let prefix = format!("CI-{}", step.index);
            let output = if step.output_tail.is_empty() {
                step.stderr_tail.clone()
            } else {
                step.output_tail.clone()
            };
            let classes: BTreeMap<_, _> = step
                .triage
                .as_ref()
                .into_iter()
                .flat_map(|triage| &triage.failures)
                .map(|failure| (failure.signature.clone(), failure.class))
                .collect();
            let signatures: BTreeSet<_> = step
                .failures
                .iter()
                .cloned()
                .chain(classes.keys().cloned())
                .collect();
            // failures_truncated=false only means extraction kept every observed
            // failure. A red runner can stop before later binaries run, so only
            // a passing step proves absent tests have been fixed: a first-run
            // pass, or a triage re-run of the whole step that passed (`flaky`).
            let step_passed = ci_step_passed(step.exit_code, step.triage.as_ref());
            if step_passed {
                for (id, entry) in &ledger {
                    if entry.source == Source::Ci && id.starts_with(&format!("{prefix}#")) {
                        observations.insert(
                            id.clone(),
                            (Source::Ci, step.command.clone(), true, String::new(), None),
                        );
                    }
                }
            }
            if !signatures.is_empty() {
                detailed_ci.insert(prefix.clone());
                for signature in signatures {
                    let class = classes
                        .get(&signature)
                        .copied()
                        .or_else(|| step.triage.as_ref().and_then(|triage| triage.class));
                    let id = ci_finding_id(step.index, &signature);
                    if step
                        .triage
                        .as_ref()
                        .is_some_and(|triage| triage.mode == api_types::ReviewCiTriage::Full)
                        && class.is_some_and(|class| !class.blocks())
                    {
                        non_blocking.insert(id.clone());
                    }
                    observations.insert(
                        id,
                        (
                            Source::Ci,
                            step.command.clone(),
                            step.exit_code == 0,
                            signature,
                            class,
                        ),
                    );
                }
                // Replace an older step-only failure once precise evidence exists.
                if step.exit_code != 0 {
                    superseded.insert(prefix.clone());
                }
                observations.insert(
                    prefix.clone(),
                    (Source::Ci, step.command.clone(), true, String::new(), None),
                );
                if step.triage.as_ref().is_some_and(|triage| {
                    triage.blocking && triage.mode == api_types::ReviewCiTriage::Full
                }) && !classes.is_empty()
                    && classes.values().all(|class| !class.blocks())
                {
                    observations.insert(
                        prefix,
                        (
                            Source::Ci,
                            step.command,
                            false,
                            "Failure evidence is incomplete".into(),
                            None,
                        ),
                    );
                }
                continue;
            }
            let class = step.triage.as_ref().and_then(|triage| triage.class);
            if step.triage.as_ref().is_some_and(|triage| {
                triage.mode == api_types::ReviewCiTriage::Full && !triage.blocking
            }) {
                non_blocking.insert(prefix.clone());
            }
            observations.insert(
                prefix,
                (Source::Ci, step.command, step.exit_code == 0, output, class),
            );
        }
        for check in conformance.checks {
            let (id, source) = match check.check_id.strip_prefix("ci:") {
                Some(index) if index.parse::<usize>().is_ok() => {
                    (format!("CI-{index}"), Source::Ci)
                }
                _ => (format!("CHK-{}", check.check_id), Source::Check),
            };
            let final_ci_pass =
                source == Source::Ci && ci_step_passed(check.exit_code, check.triage.as_ref());
            if final_ci_pass {
                // A final whole-step pass proves earlier omitted signatures fixed,
                // even when this attempt also recorded a detailed red first run.
                let prefix = format!("{id}#");
                for (test_id, entry) in &ledger {
                    if entry.source == Source::Ci && test_id.starts_with(&prefix) {
                        observations.entry(test_id.clone()).or_insert_with(|| {
                            (Source::Ci, check.command.clone(), true, String::new(), None)
                        });
                    }
                }
                for (test_id, (_, _, passed, _, class)) in observations.iter_mut() {
                    if !test_id.starts_with(&prefix) {
                        continue;
                    }
                    if check.exit_code == 0 || *passed {
                        *passed = true;
                    } else {
                        // As with step triage, keep this round's first-run
                        // failures visible as flaky; omitted old failures resolve.
                        *class = Some(api_types::CiFailureClass::Flaky);
                        if check.triage.as_ref().is_some_and(|triage| {
                            triage.mode == api_types::ReviewCiTriage::Full && !triage.blocking
                        }) {
                            non_blocking.insert(test_id.clone());
                        } else {
                            non_blocking.remove(test_id);
                        }
                    }
                }
            }
            if !detailed_ci.contains(&id) {
                let class = check.triage.as_ref().and_then(|triage| triage.class);
                non_blocking.remove(&id);
                if check.triage.as_ref().is_some_and(|triage| {
                    triage.mode == api_types::ReviewCiTriage::Full && !triage.blocking
                }) {
                    non_blocking.insert(id.clone());
                }
                observations.insert(
                    id,
                    (
                        source,
                        check.command,
                        check.exit_code == 0,
                        check.output,
                        class,
                    ),
                );
            }
        }
        // Verify's recorded check shape is independent of its execution path.
        // A not-run or inconclusive bundle provides no pass/fail observation.
        if let Some(verify) = details
            .get("verify")
            .filter(|verify| matches!(verify["status"].as_str(), Some("pass" | "fail")))
        {
            for check in verify["checks"].as_array().into_iter().flatten() {
                let Some(name) = check["name"].as_str() else {
                    continue;
                };
                let passed = match check["result"].as_str() {
                    Some("pass" | "passed") => true,
                    Some("fail" | "failed") => false,
                    _ => continue,
                };
                let observed = check["summary"]
                    .as_str()
                    .or_else(|| check["output_tail"].as_str())
                    .or_else(|| {
                        matches!(name, "start" | "ready")
                            .then(|| verify["app_log_tail"].as_str())
                            .flatten()
                    })
                    .unwrap_or_default()
                    .to_owned();
                observations.insert(
                    format!("V-{name}"),
                    (
                        Source::Verify,
                        check["command"].as_str().unwrap_or(name).to_owned(),
                        passed,
                        observed,
                        None,
                    ),
                );
            }
        }
        for (id, (source, command, passed, observed, class)) in observations {
            if passed {
                if let Some(entry) = ledger.get_mut(&id) {
                    if entry.status != Status::Resolved
                        && entry.status != Status::Superseded
                        && !superseded.contains(&id)
                    {
                        entry.resolved_attempt = Some(attempt);
                    }
                    entry.status = if entry.status == Status::Superseded || superseded.contains(&id)
                    {
                        Status::Superseded
                    } else {
                        Status::Resolved
                    };
                    entry.last_checked_attempt = attempt;
                    entry.streak = 0;
                }
            } else {
                let finding = ReviewFinding {
                    requirement_ids: Vec::new(),
                    kind: None,
                    same_cause_as: None,
                    id: None,
                    summary: if source == Source::Ci && id.contains('#') {
                        observed.clone()
                    } else {
                        format!("{command} failed")
                    },
                    severity: ReviewFindingSeverity::Blocking,
                    r#where: Some(command),
                    expected: Some("Pass".into()),
                    observed: Some(output_tail(&observed)),
                };
                open(&mut ledger, id.clone(), source, &finding, attempt);
                let entry = ledger.get_mut(&id).expect("opened finding");
                entry.ci_triage = class;
                if let Some(class) = class.filter(|_| non_blocking.contains(&id)) {
                    entry.status = match class {
                        api_types::CiFailureClass::Flaky => Status::Flaky,
                        _ => Status::PreExisting,
                    };
                    entry.severity = ReviewFindingSeverity::Minor;
                    entry.streak = 0;
                }
            }
        }
        // An assessment is evidence that the reviewer ran. A bound execution
        // alone is not: CI can stop the attempt before it is dispatched.
        if let Some(assessment) = conformance.assessment {
            let later_review = std::mem::replace(&mut assessed, true);
            let prior = ledger.clone();
            let known: BTreeSet<_> = prior
                .values()
                .filter(|entry| entry.source == Source::Reviewer)
                .map(|entry| entry.id.clone())
                .collect();
            for entry in ledger.values_mut().filter(|entry| {
                entry.source == Source::Reviewer && entry.status != Status::Resolved
            }) {
                entry.status = Status::Unconfirmed;
                entry.streak = 0;
                entry.last_checked_attempt = attempt;
            }
            for id in assessment.resolved.iter().filter(|id| known.contains(*id)) {
                let entry = ledger.get_mut(id).expect("known reviewer finding");
                if entry.status != Status::Resolved {
                    entry.resolved_attempt = Some(attempt);
                }
                entry.status = Status::Resolved;
                entry.streak = 0;
                entry.last_checked_attempt = attempt;
            }
            let mut continued = BTreeSet::new();
            for mut finding in assessment.findings {
                // Frozen input stays untouched. Only contract-owned ids can anchor
                // a finding; older assessments without a contract cite nothing.
                finding.requirement_ids.retain(|id| {
                    conformance.contract.as_ref().is_some_and(|contract| {
                        contract
                            .context
                            .requirements
                            .iter()
                            .any(|requirement| requirement.id == *id)
                    })
                });
                finding.same_cause_as = finding.same_cause_as.filter(|id| prior.contains_key(id));
                let id = match finding.id.as_ref().filter(|id| known.contains(*id)) {
                    // A known id names one finding. A repeated item is the
                    // reviewer's duplicate, not a new open finding that would
                    // count against progress; the first item continues it.
                    Some(id) if !continued.insert(id.clone()) => continue,
                    Some(id) => id.clone(),
                    None => {
                        let id = format!("F{next_id}");
                        next_id += 1;
                        id
                    }
                };
                // Continue from the previous source check, before omission or
                // explicit resolution in this block. A continued item wins.
                if let Some(previous) = prior.get(&id) {
                    if finding.requirement_ids.is_empty() {
                        finding.requirement_ids = previous
                            .requirement_ids
                            .iter()
                            .filter(|id| {
                                conformance.contract.as_ref().is_some_and(|contract| {
                                    contract
                                        .context
                                        .requirements
                                        .iter()
                                        .any(|requirement| requirement.id == **id)
                                })
                            })
                            .cloned()
                            .collect();
                    }
                    finding.kind = finding.kind.or(previous.kind);
                    finding.same_cause_as = finding
                        .same_cause_as
                        .or_else(|| previous.same_cause_as.clone());
                    ledger.insert(id.clone(), previous.clone());
                }
                let outside_scope = finding.kind == Some(ReviewFindingKind::OutOfScope);
                // A continued id keeps an earlier downgrade: the reviewer is told
                // to copy every open id, which must not launder a nit into a
                // blocker. It blocks again only when it now qualifies.
                let anchored_before = prior
                    .get(&id)
                    .is_some_and(|previous| previous.anchoring_note.is_some());
                let downgraded = later_review
                    && (!known.contains(&id) || anchored_before)
                    && finding.severity == ReviewFindingSeverity::Blocking
                    && !outside_scope
                    && finding.kind != Some(ReviewFindingKind::Regression)
                    && finding.requirement_ids.is_empty()
                    && !finding.r#where.as_deref().is_some_and(|location| {
                        conformance.contract.as_ref().is_some_and(|contract| {
                            finding_path(location, &contract.candidate_changed_paths).is_some_and(
                                |path| {
                                    contract
                                        .candidate_changed_paths
                                        .iter()
                                        .any(|changed| changed == path)
                                },
                            )
                        })
                    });
                if downgraded || outside_scope {
                    finding.severity = ReviewFindingSeverity::Minor;
                }
                open(&mut ledger, id.clone(), Source::Reviewer, &finding, attempt);
                let entry = ledger.get_mut(&id).expect("opened finding");
                if downgraded {
                    entry.anchoring_note = Some("outside requirements and Task files".into());
                }
                if outside_scope {
                    entry.status = Status::OutOfScope;
                }
            }
        }
    }
    let mut entries: Vec<_> = ledger.into_values().collect();
    entries.sort_by(|a, b| compare_finding_ids(&a.id, &b.id));
    Ok(entries)
}

/// D5 policy uses the same complete ledger as every display.
#[derive(Debug, Default)]
pub struct RoundPolicy {
    pub derived: bool,
    pub progress: bool,
    pub repeat: Option<String>,
}

pub fn round_policy(reviews: &[Review], current: &Review) -> Result<RoundPolicy, String> {
    let mut window: Vec<_> = reviews
        .iter()
        .filter(|row| row.attempt_number < current.attempt_number)
        .cloned()
        .collect();
    window.push(current.clone());
    let derived = window.iter().all(|row| {
        let details: Value = serde_json::from_str(&row.step_results_json).unwrap_or_default();
        if details
            .get("manual_override")
            .is_some_and(|value| !value.is_null())
        {
            return true;
        }
        let assessment = &details["conformance"]["assessment"];
        assessment["result"].as_str() != Some("fail")
            || assessment["findings"].as_array().is_some_and(|findings| {
                findings.iter().any(|finding| {
                    finding["severity"].as_str().unwrap_or("blocking") == "blocking"
                        && finding["summary"]
                            .as_str()
                            .is_some_and(|summary| !summary.trim().is_empty())
                })
            })
    });
    if !derived {
        return Ok(RoundPolicy::default());
    }
    let previous = finding_ledger(&window[..window.len() - 1])?;
    let ledger = finding_ledger(&window)?;
    let blocking = |entry: &&ReviewFindingEntry| {
        entry.status.is_open() && entry.severity == ReviewFindingSeverity::Blocking
    };
    let progress =
        ledger.iter().filter(blocking).count() < previous.iter().filter(blocking).count();
    let repeat = ledger
        .iter()
        .filter(blocking)
        .find(|entry| {
            entry.source == Source::Reviewer
                && entry.last_checked_attempt == current.attempt_number
                && (entry.streak >= 3 || (entry.streak >= 2 && !progress))
        })
        .map(|entry| format!("repeated finding: {} {}", entry.id, entry.summary));
    Ok(RoundPolicy {
        derived,
        progress,
        repeat,
    })
}

/// Compare numeric runs by value without a fixed-width integer conversion.
/// This also orders source ids such as CI-2 before CI-10.
pub fn compare_finding_ids(a: &str, b: &str) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let (a_bytes, b_bytes) = (a.as_bytes(), b.as_bytes());
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        if a_bytes[i].is_ascii_digit() && b_bytes[j].is_ascii_digit() {
            let (start_i, start_j) = (i, j);
            while i < a.len() && a_bytes[i].is_ascii_digit() {
                i += 1;
            }
            while j < b.len() && b_bytes[j].is_ascii_digit() {
                j += 1;
            }
            let left = a[start_i..i].trim_start_matches('0');
            let right = b[start_j..j].trim_start_matches('0');
            let order = left.len().cmp(&right.len()).then_with(|| left.cmp(right));
            if order != Ordering::Equal {
                return order;
            }
        } else {
            let order = a_bytes[i].cmp(&b_bytes[j]);
            if order != Ordering::Equal {
                return order;
            }
            i += 1;
            j += 1;
        }
    }
    (a.len() - i).cmp(&(b.len() - j)).then_with(|| a.cmp(b))
}

fn open(
    ledger: &mut BTreeMap<String, ReviewFindingEntry>,
    id: String,
    source: Source,
    finding: &ReviewFinding,
    attempt: i64,
) {
    let previous = ledger.get(&id);
    let entry = ReviewFindingEntry {
        requirement_ids: finding.requirement_ids.clone(),
        kind: finding.kind,
        same_cause_as: finding.same_cause_as.clone(),
        anchoring_note: None,
        resolved_attempt: previous.and_then(|entry| entry.resolved_attempt),
        ci_triage: None,
        id: id.clone(),
        source,
        severity: finding.severity,
        summary: finding.summary.clone(),
        r#where: finding.r#where.clone(),
        expected: finding.expected.clone(),
        observed: finding.observed.clone(),
        first_attempt: previous.map_or(attempt, |entry| entry.first_attempt),
        last_checked_attempt: attempt,
        status: if previous.is_some_and(|entry| entry.resolved_attempt.is_some()) {
            Status::Reopened
        } else {
            Status::Open
        },
        streak: if finding.severity == ReviewFindingSeverity::Blocking {
            previous
                .filter(|entry| {
                    entry.status.is_open() && entry.severity == ReviewFindingSeverity::Blocking
                })
                .map_or(1, |entry| entry.streak + 1)
        } else {
            0
        },
    };
    ledger.insert(id, entry);
}

fn ci_step_passed(exit_code: i32, triage: Option<&api_types::StepCiTriage>) -> bool {
    exit_code == 0
        || triage.is_some_and(|triage| triage.class == Some(api_types::CiFailureClass::Flaky))
}

/// The first repository-relative path in a finding's location, without :line.
fn finding_path<'a>(location: &'a str, changed_paths: &[String]) -> Option<&'a str> {
    location
        .split_whitespace()
        .map(|word| word.trim_matches(['`', '\'', '"', '(', ')', ',', ';']))
        // `./src/a.rs` and a sentence's closing period name the same file.
        .map(|word| {
            let mut word = word.trim_end_matches('.');
            while let Some(rest) = word.strip_prefix("./") {
                word = rest;
            }
            word
        })
        .filter_map(|word| {
            let path = word.split(':').next().unwrap_or(word);
            let has_line = word
                .strip_prefix(path)
                .and_then(|suffix| suffix.strip_prefix(':'))
                .is_some_and(|line| line.starts_with(|c: char| c.is_ascii_digit()));
            (!path.starts_with('/')
                && !path.starts_with("../")
                && !path.contains("/../")
                && !path.contains('\\')
                && (path.contains('/')
                    || path.contains('.')
                    || has_line
                    || changed_paths.iter().any(|changed| changed == path)))
            .then_some(path)
        })
        .next()
}

/// Whether a repair contains a blocking reviewer finding checked twice.
pub fn has_carried_finding(reviews: &[Review]) -> bool {
    let attempt = reviews.iter().map(|review| review.attempt_number).max();
    finding_ledger(reviews)
        .unwrap_or_default()
        .iter()
        .any(|entry| {
            entry.source == Source::Reviewer
                && entry.status.is_open()
                && entry.severity == ReviewFindingSeverity::Blocking
                && entry.streak >= 2
                && Some(entry.last_checked_attempt) == attempt
        })
}

/// Match the durable check runner's bounded, UTF-8-safe output tail.
fn output_tail(text: &str) -> String {
    let mut start = text.len().saturating_sub(4096);
    while !text.is_char_boundary(start) {
        start += 1;
    }
    text[start..].to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn review(attempt: i64, details: Value) -> Review {
        Review {
            id: format!("review-{attempt}"),
            task_id: "task".into(),
            execution_id: "candidate".into(),
            reviewer_execution_id: None,
            auditor_execution_id: None,
            attempt_number: attempt,
            status: crate::ReviewStatus::Failed,
            step_results_json: details.to_string(),
            started_at: format!("2026-10-15T00:00:{attempt:02}Z"),
            finished_at: None,
            created_at: format!("2026-10-15T00:00:{attempt:02}Z"),
            updated_at: "now".into(),
        }
    }
    fn reviewer(attempt: i64, findings: Value, resolved: Value) -> Review {
        review(
            attempt,
            json!({"conformance":{"status":"failed","contract":null,"checks":[],"reason":null,"assessment":{"result":"fail","reason":"problems","findings":findings,"resolved":resolved}}}),
        )
    }
    fn entry<'a>(ledger: &'a [ReviewFindingEntry], id: &str) -> &'a ReviewFindingEntry {
        ledger.iter().find(|entry| entry.id == id).unwrap()
    }
    #[test]
    fn finding_ledger_preserves_unobserved_test_signatures_and_excludes_triaged_noise() {
        let first = review(
            1,
            json!({"ci_steps":[{"index":0,"command":"test","exit_code":1,"stderr_tail":"","failures":["fixed","base","flake"]}]}),
        );
        let second = review(
            2,
            json!({"ci_steps":[{"index":0,"command":"test","exit_code":1,"stderr_tail":"","failures":["base","flake"],"triage":{
                "mode":"full","base_status":"available","base_commit":null,"rerun_run_id":null,"base_run_id":null,"reason":null,"class":null,"blocking":false,
                "failures":[{"signature":"base","class":"pre_existing"},{"signature":"flake","class":"flaky"}]
            }}]}),
        );
        let ledger = finding_ledger(&[first, second]).unwrap();
        assert_eq!(ledger.len(), 3);
        let fixed = ledger
            .iter()
            .find(|entry| entry.observed.as_deref() == Some("fixed"))
            .unwrap();
        assert_eq!(fixed.status, Status::Open);
        assert_eq!((fixed.streak, fixed.last_checked_attempt), (1, 1));
        let base = ledger
            .iter()
            .find(|entry| entry.observed.as_deref() == Some("base"))
            .unwrap();
        assert_eq!(
            (base.status, base.streak, base.severity),
            (Status::PreExisting, 0, ReviewFindingSeverity::Minor)
        );
        let flake = ledger
            .iter()
            .find(|entry| entry.observed.as_deref() == Some("flake"))
            .unwrap();
        assert_eq!(
            (flake.status, flake.ci_triage),
            (Status::Flaky, Some(api_types::CiFailureClass::Flaky))
        );
        assert_eq!(
            ledger
                .iter()
                .filter(|entry| entry.status == Status::Open)
                .count(),
            1
        );
    }
    #[test]
    fn finding_ledger_report_keeps_classified_signatures_and_steps_blocking() {
        for signatures in [true, false] {
            let make = |mode: &str| {
                review(
                    1,
                    json!({"ci_steps":[{"index":0,"command":"tests","exit_code":1,"stderr_tail":"failed",
                "failures":if signatures { json!(["base", "flake"]) } else { json!([]) },
                "triage":{"mode":mode,"base_status":"available","base_commit":null,"rerun_run_id":null,"base_run_id":null,"reason":null,
                "class":if signatures { None } else { Some("flaky") },"blocking":mode == "report",
                "failures":if signatures { json!([{"signature":"base","class":"pre_existing"},{"signature":"flake","class":"flaky"}]) } else { json!([]) }}}]}),
                )
            };
            let ledger = finding_ledger(&[make("report")]).unwrap();
            assert_eq!(ledger.len(), if signatures { 2 } else { 1 });
            assert!(ledger.iter().all(|entry| entry.status == Status::Open
                && entry.severity == ReviewFindingSeverity::Blocking
                && entry.ci_triage.is_some()));
            let ledger = finding_ledger(&[make("full")]).unwrap();
            assert!(ledger.iter().all(|entry| matches!(
                entry.status,
                Status::Flaky | Status::PreExisting
            ) && entry.streak == 0));
        }
    }

    #[test]
    fn finding_ledger_continues_resolves_and_assigns_new_ids() {
        let first = reviewer(1, json!([{"summary":"one"},{"summary":"two"}]), json!([]));
        let second = reviewer(
            2,
            json!([{"id":"F1","summary":"still one"},{"id":"unknown","summary":"three"}]),
            json!(["F2", "unknown", "CI-2"]),
        );
        // Input order is immaterial; frozen values never acquire derived ids.
        let reviews = vec![second, first];
        let before = reviews
            .iter()
            .map(|review| review.step_results_json.clone())
            .collect::<Vec<_>>();
        let ledger = finding_ledger(&reviews).unwrap();
        assert_eq!(ledger.len(), 3);
        let one = entry(&ledger, "F1");
        assert_eq!(
            (
                one.status,
                one.streak,
                one.first_attempt,
                one.last_checked_attempt
            ),
            (Status::Open, 2, 1, 2)
        );
        assert_eq!(one.summary, "still one");
        assert_eq!(entry(&ledger, "F2").status, Status::Resolved);
        assert_eq!(entry(&ledger, "F3").first_attempt, 2);
        // A known id repeated in one block is one finding: the duplicate adds
        // no open finding that phase 4's progress count would see.
        let duplicated = reviewer(
            2,
            json!([{"id":"F1","summary":"still one"},{"id":"F1","summary":"one again"}]),
            json!(["F2"]),
        );
        let first = reviewer(1, json!([{"summary":"one"},{"summary":"two"}]), json!([]));
        let ledger = finding_ledger(&[first, duplicated]).unwrap();
        assert_eq!(
            ledger
                .iter()
                .map(|entry| (entry.id.as_str(), entry.status, entry.streak))
                .collect::<Vec<_>>(),
            [("F1", Status::Open, 2), ("F2", Status::Resolved, 0)]
        );
        assert_eq!(entry(&ledger, "F1").summary, "still one");
        assert_eq!(
            before,
            reviews
                .iter()
                .map(|review| review.step_results_json.clone())
                .collect::<Vec<_>>()
        );
    }
    #[test]
    fn finding_ledger_forgotten_is_unconfirmed_and_reopens_with_fresh_streak() {
        let first = reviewer(1, json!([{"summary":"one"},{"summary":"two"}]), json!([]));
        let second = reviewer(2, json!([{"id":"F1","summary":"one"}]), json!([]));
        let ledger = finding_ledger(&[first.clone(), second.clone()]).unwrap();
        assert_eq!(
            (entry(&ledger, "F2").status, entry(&ledger, "F2").streak),
            (Status::Unconfirmed, 0)
        );
        assert_eq!(entry(&ledger, "F1").streak, 2);
        let third = reviewer(3, json!([{"id":"F2","summary":"two again"}]), json!(["F1"]));
        let fourth = reviewer(4, json!([{"id":"F1","summary":"one again"}]), json!([]));
        let ledger = finding_ledger(&[first, second, third, fourth]).unwrap();
        assert_eq!(
            (entry(&ledger, "F1").status, entry(&ledger, "F1").streak),
            (Status::Reopened, 1)
        );
        assert_eq!(entry(&ledger, "F1").first_attempt, 1);
    }
    #[test]
    fn finding_ledger_ci_before_reviewer_keeps_unchecked_findings() {
        let first = reviewer(1, json!([{"summary":"one"}]), json!([]));
        let ci = |attempt, exit| {
            review(
                attempt,
                json!({"ci_steps":[{"index":2,"command":"cargo test","exit_code":exit,"stderr_tail":"stderr","output_tail":"test failure"}]}),
            )
        };
        let ledger = finding_ledger(&[first.clone(), ci(2, 101), ci(3, 101)]).unwrap();
        assert_eq!(
            (
                entry(&ledger, "F1").status,
                entry(&ledger, "F1").streak,
                entry(&ledger, "F1").last_checked_attempt
            ),
            (Status::Open, 1, 1)
        );
        let step = entry(&ledger, "CI-2");
        assert_eq!((step.status, step.streak), (Status::Open, 2));
        assert_eq!(step.r#where.as_deref(), Some("cargo test"));
        assert_eq!(step.observed.as_deref(), Some("test failure"));
        let ledger = finding_ledger(&[first, ci(2, 101), ci(3, 0), ci(4, 101)]).unwrap();
        assert_eq!(
            (
                entry(&ledger, "CI-2").streak,
                entry(&ledger, "CI-2").first_attempt
            ),
            (1, 2)
        );
    }
    #[test]
    fn finding_ledger_required_checks_verify_and_not_run_sources() {
        let checks = |attempt, exit| {
            review(
                attempt,
                json!({"conformance":{"status":"failed","contract":null,"assessment":null,"checks":[{"check_id":"schema","command":"check-schema","exit_code":exit,"output":"x"},{"check_id":"ci:0","command":"tests","exit_code":exit,"output":"x"}],"reason":null},"verify":{"status":"fail","checks":[{"name":"ready","result":"fail"},{"name":"smoke","result":"fail","summary":"button fails"}],"app_log_tail":"bind failed"}}),
            )
        };
        let skipped = review(
            2,
            json!({"verify":{"status":"not_run","checks":[{"name":"smoke","result":"pass"}]}}),
        );
        let ledger = finding_ledger(&[checks(1, 1), skipped, checks(3, 0)]).unwrap();
        assert_eq!(entry(&ledger, "CHK-schema").status, Status::Resolved);
        assert_eq!(entry(&ledger, "CI-0").status, Status::Resolved);
        assert_eq!(
            (
                entry(&ledger, "V-smoke").status,
                entry(&ledger, "V-smoke").streak
            ),
            (Status::Open, 2)
        );
        assert_eq!(
            entry(&ledger, "V-ready").observed.as_deref(),
            Some("bind failed")
        );
        let passed = review(
            4,
            json!({"verify":{"status":"pass","checks":[{"name":"smoke","result":"pass"}]},"execution":{"integration_checks": [{"ci_steps":[{"exit_code":1}]}]}}),
        );
        let ledger = finding_ledger(&[checks(1, 1), passed]).unwrap();
        assert_eq!(entry(&ledger, "V-smoke").status, Status::Resolved);
        assert!(ledger.iter().all(|entry| entry.source != Source::Merge));
    }
    #[test]
    fn finding_ledger_new_window_restarts_ids_and_legacy_reviews_add_nothing() {
        let old = reviewer(
            1,
            json!([{"summary":"old one"},{"summary":"old two"}]),
            json!([]),
        );
        assert_eq!(finding_ledger(&[old]).unwrap().len(), 2);
        let current_window = [
            review(2, json!([])),
            reviewer(3, json!([{"id":"F2","summary":"new"}]), json!(["F1"])),
        ];
        let ledger = finding_ledger(&current_window).unwrap();
        assert_eq!(ledger.len(), 1);
        assert_eq!((ledger[0].id.as_str(), ledger[0].first_attempt), ("F1", 3));
        assert!(finding_ledger(&[]).unwrap().is_empty());
    }
    /// A Review whose reviewer execution froze `assessment` under a contract.
    fn frozen(attempt: i64, status: &str, execution: &str, assessment: Value) -> Value {
        json!({"ci_steps":[{"index":0,"command":"tests","exit_code":0,"stderr_tail":""}],"conformance":{"status":status,"contract":{"execution_id":execution,"policy":"p","commit_sha":format!("c{attempt}"),"base_sha":"b","context":{"project_id":"p","task_id":"task","repo_id":null,"charter_revision_id":null,"charter_digest":null,"charter":null,"task_scope":{},"linked_documents":[],"requirements":[],"required_checks":[],"source_digest":"d"},"digest":"x"},"checks":[{"check_id":"schema","command":"check-schema","exit_code":1,"output":"x"}],"reason":null,"assessment":assessment}})
    }
    #[test]
    fn finding_ledger_counts_a_copied_assessment_once() {
        // The owner's manual pass copies the failed Review's details.
        let failed = frozen(
            1,
            "failed",
            "reviewer-1",
            json!({"result":"fail","reason":"problems","findings":[{"summary":"one"},{"summary":"two"}]}),
        );
        let mut manual_pass = failed.clone();
        manual_pass["manual_override"] = json!({"actor_type":"user","source_attempt_number":1});
        let before = finding_ledger(&[review(1, failed.clone())]).unwrap();
        assert_eq!(
            finding_ledger(&[review(1, failed.clone()), review(2, manual_pass)]).unwrap(),
            before
        );
        // A queue-rebase carry copies the passed Review's frozen conformance
        // next to its own (passing) CI steps.
        let continued = frozen(
            2,
            "passed",
            "reviewer-2",
            json!({"result":"pass","reason":"fine","findings":[{"id":"F1","summary":"one","severity":"minor"}],"resolved":["F2"]}),
        );
        let carried = json!({"ci_steps":[{"index":0,"command":"tests","exit_code":0,"stderr_tail":""}],"conformance":continued["conformance"].clone()});
        let before =
            finding_ledger(&[review(1, failed.clone()), review(2, continued.clone())]).unwrap();
        let after =
            finding_ledger(&[review(1, failed), review(2, continued), review(3, carried)]).unwrap();
        assert_eq!(after.len(), 3, "{after:?}");
        assert_eq!(
            after
                .iter()
                .map(|entry| (entry.id.as_str(), entry.status, entry.streak))
                .collect::<Vec<_>>(),
            before
                .iter()
                .map(|entry| (entry.id.as_str(), entry.status, entry.streak))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            (entry(&after, "F1").status, entry(&after, "F1").streak),
            (Status::Open, 0)
        );
    }
    #[test]
    fn finding_ledger_deduplicates_sources_and_bounds_check_tails() {
        let long = "界".repeat(2_000);
        let review = review(
            1,
            json!({"ci_steps":[{"index":0,"command":"tests","exit_code":1,"stderr_tail":"early"}],"conformance":{"status":"failed","contract":null,"assessment":null,"checks":[{"check_id":"ci:0","command":"tests","exit_code":1,"output":long}],"reason":null}}),
        );
        let ledger = finding_ledger(&[review]).unwrap();
        assert_eq!(ledger.len(), 1);
        assert_eq!(ledger[0].streak, 1);
        assert!(ledger[0].observed.as_ref().unwrap().len() <= 4096);
        assert!(finding_ledger(&[super::tests::review(2, json!({"ci_steps":"bad"}))]).is_err());
    }
    #[test]
    fn finding_ledger_orders_numeric_id_runs_naturally_before_any_cap() {
        let findings: Vec<_> = (1..=50)
            .map(|n| json!({"summary":format!("finding {n}")}))
            .collect();
        let ledger = finding_ledger(&[reviewer(1, json!(findings), json!([]))]).unwrap();
        assert_eq!(
            ledger
                .iter()
                .map(|entry| entry.id.clone())
                .collect::<Vec<_>>(),
            (1..=50).map(|n| format!("F{n}")).collect::<Vec<_>>()
        );
        let mut ids = [
            "V-case10",
            "F10",
            "CI-10",
            "CHK-step:10",
            "CI-2",
            "F2",
            "V-case2",
            "CHK-step:2",
        ];
        ids.sort_by(|a, b| compare_finding_ids(a, b));
        assert_eq!(
            ids,
            [
                "CHK-step:2",
                "CHK-step:10",
                "CI-2",
                "CI-10",
                "F2",
                "F10",
                "V-case2",
                "V-case10"
            ]
        );
    }
    #[test]
    fn finding_ledger_signature_ids_are_stable_and_track_each_test() {
        let ci = |attempt, exit, failures: Value, truncated| {
            review(
                attempt,
                json!({"ci_steps":[{
            "index":2,"command":"cargo test","exit_code":exit,"stderr_tail":"tail",
            "failures":failures,"failures_truncated":truncated}],
            "conformance":{"status":"failed","contract":null,"assessment":null,"reason":null,"checks":[{"check_id":"ci:2","command":"cargo test","exit_code":exit,"output":"final tail"}]}}),
            )
        };
        // Golden digest pins SHA-256, rather than a process-seeded hasher.
        assert_eq!(ci_finding_id(2, "abc"), "CI-2#ba7816bf");
        let a = ci_finding_id(2, "parser::empty");
        let b = ci_finding_id(2, "parser::unicode");
        let reviews = [
            ci(1, 1, json!(["parser::empty", "parser::unicode"]), false),
            ci(2, 1, json!(["parser::empty"]), false),
        ];
        let ledger = finding_ledger(&reviews).unwrap();
        assert_eq!(ledger.len(), 2);
        assert_eq!(
            (entry(&ledger, &a).status, entry(&ledger, &a).streak),
            (Status::Open, 2)
        );
        assert_eq!(
            (
                entry(&ledger, &b).status,
                entry(&ledger, &b).last_checked_attempt
            ),
            (Status::Open, 1)
        );
        assert_eq!(ledger, finding_ledger(&reviews).unwrap());
        let third = ci(3, 0, json!([]), false);
        let fourth = ci(4, 0, json!([]), false);
        let ledger =
            finding_ledger(&[reviews[0].clone(), reviews[1].clone(), third, fourth]).unwrap();
        assert_eq!(entry(&ledger, &a).last_checked_attempt, 4);
        assert_eq!(entry(&ledger, &b).last_checked_attempt, 4);
        assert!(ledger.iter().all(|entry| entry.status == Status::Resolved));
    }

    #[test]
    fn finding_ledger_missing_or_truncated_signatures_cannot_resolve_tests() {
        let ci = |attempt, failures: Value, truncated| {
            review(
                attempt,
                json!({"ci_steps":[{
            "index":0,"command":"test","exit_code":1,"stderr_tail":"tail","failures":failures,"failures_truncated":truncated}]}),
            )
        };
        let a = ci_finding_id(0, "a");
        let b = ci_finding_id(0, "b");
        let ledger = finding_ledger(&[
            ci(1, json!(["a", "b"]), false),
            ci(2, json!(["a"]), true),
            ci(3, json!([]), false),
        ])
        .unwrap();
        assert_eq!(entry(&ledger, &a).status, Status::Open);
        assert_eq!(entry(&ledger, &b).status, Status::Open);
        assert_eq!(entry(&ledger, "CI-0").status, Status::Open);
        let full = finding_ledger(&[ci(1, json!([]), false), ci(2, json!(["a"]), false)]).unwrap();
        assert_eq!(entry(&full, "CI-0").status, Status::Superseded);
        assert_eq!(entry(&full, &a).status, Status::Open);
    }
    #[test]
    fn round_policy_fail_fast_ci_absence_is_neither_progress_nor_a_streak_step() {
        let ci = |attempt, failures, truncated| {
            review(
                attempt,
                json!({"ci_steps":[{
            "index":0,"command":"cargo test","exit_code":101,"stderr_tail":"",
            "failures":failures,"failures_truncated":truncated}]}),
            )
        };
        for truncated in [false, true] {
            let first = ci(1, json!(["binary_a::failure", "binary_b::failure"]), false);
            let second = ci(2, json!(["binary_a::failure"]), truncated);
            let policy = round_policy(std::slice::from_ref(&first), &second).unwrap();
            assert!(!policy.progress);
            let ledger = finding_ledger(&[first, second]).unwrap();
            let unseen = entry(&ledger, &ci_finding_id(0, "binary_b::failure"));
            assert_eq!(
                (unseen.status, unseen.streak, unseen.last_checked_attempt),
                (Status::Open, 1, 1)
            );
        }
    }

    #[test]
    fn finding_ledger_superseded_is_explicit_and_never_an_actual_resolution() {
        let step = |attempt, exit, signatures| {
            review(
                attempt,
                json!({"ci_steps":[{
            "index":0,"command":"cargo test","exit_code":exit,"stderr_tail":"",
            "failures":signatures}]}),
            )
        };
        let rounds = [
            step(1, 101, json!([])),
            step(2, 101, json!(["a"])),
            step(3, 0, json!([])),
        ];
        let ledger = finding_ledger(&rounds[..2]).unwrap();
        assert_eq!(entry(&ledger, "CI-0").status, Status::Superseded);
        assert_eq!(
            serde_json::to_value(entry(&ledger, "CI-0")).unwrap()["status"],
            "superseded"
        );
        let ledger = finding_ledger(&rounds).unwrap();
        assert_eq!(entry(&ledger, "CI-0").status, Status::Superseded);
        assert_eq!(
            entry(&ledger, &ci_finding_id(0, "a")).status,
            Status::Resolved
        );
    }

    #[test]
    fn round_policy_counts_only_open_blocking_and_bounds_carried_findings() {
        let first = reviewer(
            1,
            json!([{"summary":"one"},{"summary":"two"},{"summary":"three"},{"summary":"minor", "severity":"minor"}]),
            json!([]),
        );
        let second = reviewer(
            2,
            json!([{"id":"F1","summary":"one"},{"id":"F4","summary":"minor", "severity":"minor"}]),
            json!(["F2", "F3"]),
        );
        let policy = round_policy(std::slice::from_ref(&first), &second).unwrap();
        assert!(policy.derived && policy.progress);
        assert!(policy.repeat.is_none());
        let third = reviewer(3, json!([{"id":"F1","summary":"one"}]), json!([]));
        let policy = round_policy(&[first.clone(), second], &third).unwrap();
        assert_eq!(policy.repeat.as_deref(), Some("repeated finding: F1 one"));
        let no_progress = reviewer(
            2,
            json!([{"id":"F1","summary":"one"},{"id":"F2","summary":"two"},{"id":"F3","summary":"three"}]),
            json!([]),
        );
        let policy = round_policy(&[first], &no_progress).unwrap();
        assert!(!policy.progress);
        assert_eq!(policy.repeat.as_deref(), Some("repeated finding: F1 one"));
    }

    #[test]
    fn round_policy_findings_less_or_minor_only_fail_disables_the_entire_window() {
        for findings in [json!([]), json!([{"summary":"minor","severity":"minor"}])] {
            let old = reviewer(1, findings, json!([]));
            let first = reviewer(2, json!([{"summary":"one"},{"summary":"two"}]), json!([]));
            let second = reviewer(3, json!([{"id":"F1","summary":"one"}]), json!(["F2"]));
            assert!(
                !round_policy(&[old, first.clone()], &second)
                    .unwrap()
                    .derived
            );
            assert!(
                round_policy(&[first], &second).unwrap().derived,
                "a reset excludes the legacy fail"
            );
        }
    }
    #[test]
    fn finding_ledger_conformance_ci_pass_resolves_prior_test_findings() {
        let first = review(
            1,
            json!({"ci_steps":[{"index":0,"command":"tests","exit_code":1,"stderr_tail":"","failures":["a"]}]}),
        );
        let passed = review(
            2,
            json!({"conformance":{"status":"passed","checks":[{"check_id":"ci:0","command":"tests","exit_code":0,"output":"ok"}]}}),
        );
        let ledger = finding_ledger(&[first, passed]).unwrap();
        assert_eq!(
            entry(&ledger, &ci_finding_id(0, "a")).status,
            Status::Resolved
        );
    }
    // A triage re-run that passed ran the whole step green: every test absent
    // from the first run's list is proven fixed, as after a first-run pass.
    #[test]
    fn finding_ledger_passing_triage_rerun_resolves_tests_absent_from_the_first_run() {
        for (mode, flaky) in [("full", Status::Flaky), ("report", Status::Open)] {
            let first = review(
                1,
                json!({"ci_steps":[{"index":0,"command":"test","exit_code":1,"stderr_tail":"","failures":["a","b"]}]}),
            );
            let second = review(
                2,
                json!({"ci_steps":[{"index":0,"command":"test","exit_code":1,"stderr_tail":"","failures":["c"],"triage":{
                    "mode":mode,"base_status":"available","base_commit":null,"rerun_run_id":"rerun","base_run_id":null,
                    "reason":null,"class":"flaky","blocking":mode == "report",
                    "failures":[{"signature":"c","class":"flaky"}]
                }}]}),
            );
            let ledger = finding_ledger(&[first.clone(), second.clone()]).unwrap();
            for signature in ["a", "b"] {
                let fixed = entry(&ledger, &ci_finding_id(0, signature));
                assert_eq!(
                    (fixed.status, fixed.last_checked_attempt),
                    (Status::Resolved, 2),
                    "{mode} {signature}"
                );
            }
            assert_eq!(
                entry(&ledger, &ci_finding_id(0, "c")).status,
                flaky,
                "{mode}"
            );
            assert!(
                round_policy(std::slice::from_ref(&first), &second)
                    .unwrap()
                    .progress,
                "{mode}"
            );
        }
    }
    #[test]
    fn finding_ledger_minor_appearances_do_not_create_a_blocking_streak() {
        let first = reviewer(
            1,
            json!([{"summary":"nit", "severity":"minor"}, {"summary":"actual blocker"}]),
            json!([]),
        );
        let second = reviewer(
            2,
            json!([{"id":"F1","summary":"now blocking"}]),
            json!(["F2"]),
        );
        let policy = round_policy(std::slice::from_ref(&first), &second).unwrap();
        assert!(policy.derived && !policy.progress);
        assert!(
            policy.repeat.is_none(),
            "earlier minor appearances caused a repeat park"
        );
        let ledger = finding_ledger(&[first, second]).unwrap();
        assert_eq!(entry(&ledger, "F1").streak, 1);
    }

    #[test]
    fn finding_ledger_flaky_conformance_rerun_resolves_prior_signatures() {
        let first = review(
            1,
            json!({"ci_steps":[{"index":0,"command":"tests","exit_code":1,"stderr_tail":"","failures":["a"]}]}),
        );
        let second = review(
            2,
            json!({"conformance":{"status":"passed","checks":[{"check_id":"ci:0","command":"tests","exit_code":1,"output":"first run failed", "triage":{
                "mode":"full","base_status":"available","base_commit":null,"rerun_run_id":"rerun","base_run_id":null,
                "reason":null,"class":"flaky","blocking":false,"failures":[]
            }}]}}),
        );
        let ledger = finding_ledger(&[first, second]).unwrap();
        assert_eq!(
            entry(&ledger, &ci_finding_id(0, "a")).status,
            Status::Resolved
        );
    }
    fn anchored_review(attempt: i64, findings: Value, resolved: Value) -> Review {
        let mut details = frozen(
            attempt,
            "failed",
            &format!("execution-{attempt}"),
            json!({"result":"fail","reason":"problems","findings":findings,"resolved":resolved}),
        );
        details["conformance"]["checks"] = json!([]);
        details["conformance"]["contract"]["candidate_changed_paths"] =
            json!(["src/task.rs", "README.md"]);
        details["conformance"]["contract"]["context"]["requirements"] = json!([
            {"id":"R2", "source":"task", "text":"keep drafts", "universal":false, "allocated_task_id":null}
        ]);
        review(attempt, details)
    }

    #[test]
    fn finding_ledger_anchoring_downgrades_untouched_files_but_keeps_cited_missed_and_regressions()
    {
        let rounds = [
            anchored_review(1, json!([{"summary":"initial"}]), json!([])),
            anchored_review(
                2,
                json!([
                    {"summary":"naming nit", "where":"src/old.rs:42"},
                    {"summary":"draft lost", "kind":"missed", "requirement_ids":["R2"]},
                    {"summary":"broke again", "kind":"regression"},
                    {"summary":"changed file", "where":"`src/task.rs:20`"},
                    {"summary":"readme changed", "where":"README.md:3"},
                    {"summary":"unknown requirement", "kind":"missed", "requirement_ids":["R99"]},
                    {"summary":"absolute path", "where":"/tmp/src/task.rs:4"},
                    {"summary":"first path wins", "where":"src/old.rs:4 and src/task.rs:5"}
                ]),
                json!(["F1"]),
            ),
        ];
        let stored = rounds[1].step_results_json.clone();
        let ledger = finding_ledger(&rounds).unwrap();
        for id in ["F2", "F7", "F8", "F9"] {
            let finding = entry(&ledger, id);
            assert_eq!(finding.severity, ReviewFindingSeverity::Minor, "{id}");
            assert_eq!(finding.streak, 0);
            assert_eq!(
                finding.anchoring_note.as_deref(),
                Some("outside requirements and Task files")
            );
        }
        for id in ["F3", "F4", "F5", "F6"] {
            assert_eq!(
                entry(&ledger, id).severity,
                ReviewFindingSeverity::Blocking,
                "{id}"
            );
        }
        assert!(entry(&ledger, "F7").requirement_ids.is_empty());
        assert_eq!(
            rounds[1].step_results_json, stored,
            "anchoring rewrote the frozen assessment"
        );
    }

    #[test]
    fn finding_ledger_out_of_scope_never_blocks_even_on_first_review() {
        let ledger = finding_ledger(&[anchored_review(1,
            json!([{"summary":"separate project work", "kind":"out_of_scope", "requirement_ids":["R2"]}]), json!([]))]).unwrap();
        assert_eq!(entry(&ledger, "F1").status, Status::OutOfScope);
        assert_eq!(entry(&ledger, "F1").severity, ReviewFindingSeverity::Minor);
        assert_eq!(entry(&ledger, "F1").streak, 0);
    }

    #[test]
    fn finding_ledger_rebroken_test_and_reviewer_are_reopened_with_a_fresh_streak() {
        let ci = |attempt, exit| {
            review(
                attempt,
                json!({"ci_steps":[{"index":1,"command":"tests","exit_code":exit,"stderr_tail":"", "failures": if exit == 0 {json!([])} else {json!(["test::draft"])} }]}),
            )
        };
        let ledger = finding_ledger(&[ci(1, 1), ci(2, 0), ci(3, 1)]).unwrap();
        let finding = entry(&ledger, &ci_finding_id(1, "test::draft"));
        assert_eq!(
            (finding.status, finding.streak, finding.resolved_attempt),
            (Status::Reopened, 1, Some(2))
        );
        let rounds = [
            anchored_review(1, json!([{"summary":"draft"}]), json!([])),
            anchored_review(2, json!([]), json!(["F1"])),
            anchored_review(3, json!([{"id":"F1","summary":"draft again"}]), json!([])),
        ];
        let ledger = finding_ledger(&rounds).unwrap();
        assert_eq!(
            (
                entry(&ledger, "F1").status,
                entry(&ledger, "F1").streak,
                entry(&ledger, "F1").resolved_attempt
            ),
            (Status::Reopened, 1, Some(2))
        );
    }

    #[test]
    fn round_policy_pre_existing_noise_does_not_stall_progress() {
        let first = anchored_review(
            1,
            json!([{"summary":"a"},{"summary":"b"},{"summary":"c"}]),
            json!([]),
        );
        let mut second =
            anchored_review(2, json!([{"id":"F1","summary":"a"}]), json!(["F2", "F3"]));
        let mut details: Value = serde_json::from_str(&second.step_results_json).unwrap();
        let failures: Vec<_> = (0..26).map(|n| format!("base::{n}")).collect();
        details["ci_steps"] = json!([{"index":0,"command":"tests","exit_code":1,"stderr_tail":"", "failures":failures,
            "triage":{"mode":"full","base_status":"available","blocking":false,"class":"pre_existing", "failures":[]}}]);
        second.step_results_json = details.to_string();
        let policy = round_policy(std::slice::from_ref(&first), &second).unwrap();
        assert!(policy.progress && policy.derived);
        assert!(policy.repeat.is_none());
        assert!(has_carried_finding(&[first, second]));
    }
    #[test]
    fn finding_ledger_continued_ids_keep_requirement_and_shared_cause_metadata() {
        let rounds = [
            anchored_review(1, json!([{"summary":"first"}]), json!([])),
            anchored_review(
                2,
                json!([{"summary":"second", "kind":"missed", "same_cause_as":"F1", "requirement_ids":["R2"]}]),
                json!(["F1"]),
            ),
            anchored_review(3, json!([{"id":"F2", "summary":"still second"}]), json!([])),
        ];
        let ledger = finding_ledger(&rounds).unwrap();
        let finding = entry(&ledger, "F2");
        assert_eq!(finding.requirement_ids, ["R2"]);
        assert_eq!(finding.kind, Some(ReviewFindingKind::Missed));
        assert_eq!(finding.same_cause_as.as_deref(), Some("F1"));
    }

    #[test]
    fn finding_ledger_anchors_extensionless_candidate_paths_and_keeps_first_location() {
        let first = anchored_review(1, json!([{"summary":"first"}]), json!([]));
        let mut second = anchored_review(
            2,
            json!([
                {"summary":"build", "where":"Makefile:42"},
                {"summary":"lint", "where":"scripts/check"},
                {"summary":"outside", "where":"Elsewhere:42 and Makefile:9"}
            ]),
            json!(["F1"]),
        );
        let mut details: Value = serde_json::from_str(&second.step_results_json).unwrap();
        details["conformance"]["contract"]["candidate_changed_paths"] =
            json!(["Makefile", "scripts/check"]);
        second.step_results_json = details.to_string();
        let ledger = finding_ledger(&[first, second]).unwrap();
        assert_eq!(
            entry(&ledger, "F2").severity,
            ReviewFindingSeverity::Blocking
        );
        assert_eq!(
            entry(&ledger, "F3").severity,
            ReviewFindingSeverity::Blocking
        );
        assert_eq!(entry(&ledger, "F4").severity, ReviewFindingSeverity::Minor);
    }
    #[test]
    fn finding_ledger_passing_conformance_resolves_after_a_detailed_red_first_run() {
        for (exit, triage) in [
            (0, Value::Null),
            (
                1,
                json!({"mode":"full","base_status":"available","blocking":false,"class":"flaky","failures":[]}),
            ),
        ] {
            let first = review(
                1,
                json!({"ci_steps":[{"index":0,"command":"tests","exit_code":1,"stderr_tail":"","failures":["a", "b"]}]}),
            );
            let second = review(
                2,
                json!({"ci_steps":[{"index":0,"command":"tests","exit_code":1,"stderr_tail":"","failures":["c"]}],
                "conformance":{"status":"passed","checks":[{"check_id":"ci:0","command":"tests","exit_code":exit,"output":"final observation", "triage":triage}]}}),
            );
            let ledger = finding_ledger(&[first, second]).unwrap();
            for signature in ["a", "b"] {
                assert_eq!(
                    entry(&ledger, &ci_finding_id(0, signature)).status,
                    Status::Resolved
                );
            }
            if exit != 0 {
                assert_eq!(entry(&ledger, &ci_finding_id(0, "c")).status, Status::Flaky);
            }
        }
    }

    #[test]
    fn finding_ledger_reopened_history_survives_an_unconfirmed_observation() {
        let ledger = finding_ledger(&[
            anchored_review(1, json!([{"summary":"one"}]), json!([])),
            anchored_review(2, json!([]), json!(["F1"])),
            anchored_review(3, json!([{"id":"F1","summary":"re-broken"}]), json!([])),
            anchored_review(4, json!([]), json!([])),
            anchored_review(5, json!([{"id":"F1","summary":"checked again"}]), json!([])),
        ])
        .unwrap();
        assert_eq!(
            (
                entry(&ledger, "F1").status,
                entry(&ledger, "F1").resolved_attempt,
                entry(&ledger, "F1").streak
            ),
            (Status::Reopened, Some(2), 1)
        );
    }

    #[test]
    fn finding_ledger_continued_downgraded_finding_stays_minor_unless_it_qualifies() {
        // The prior-review prompt lists every open finding and asks the reviewer
        // to copy its id; copying an anchored nit must not make it blocking.
        let nit = json!({"id":"F2","summary":"naming nit", "where":"src/old.rs:1"});
        let rounds = [
            anchored_review(1, json!([{"summary":"initial"}]), json!([])),
            anchored_review(
                2,
                json!([{"summary":"naming nit", "where":"src/old.rs:1"}]),
                json!(["F1"]),
            ),
            anchored_review(3, json!([nit.clone()]), json!([])),
            anchored_review(4, json!([nit]), json!([])),
        ];
        let ledger = finding_ledger(&rounds).unwrap();
        let finding = entry(&ledger, "F2");
        assert_eq!(
            (finding.severity, finding.streak),
            (ReviewFindingSeverity::Minor, 0)
        );
        assert_eq!(
            finding.anchoring_note.as_deref(),
            Some("outside requirements and Task files")
        );
        let policy = round_policy(&rounds[..3], &rounds[3]).unwrap();
        assert!(policy.repeat.is_none(), "{:?}", policy.repeat);
        assert!(!has_carried_finding(&rounds));
        // Citing a requirement later is a legitimate reason to block.
        let mut cited = rounds.to_vec();
        cited.push(anchored_review(
            5,
            json!([{"id":"F2","summary":"drafts lost", "requirement_ids":["R2"]}]),
            json!([]),
        ));
        let ledger = finding_ledger(&cited).unwrap();
        let finding = entry(&ledger, "F2");
        assert_eq!(
            (finding.severity, finding.streak),
            (ReviewFindingSeverity::Blocking, 1)
        );
        assert!(finding.anchoring_note.is_none());
    }

    #[test]
    fn finding_ledger_first_reviewer_assessment_is_not_anchored_by_earlier_rows() {
        // Review CI can fail, or a Review can be cancelled, before any reviewer
        // runs. The first assessment in the window still sets the goalposts.
        let ci_only = review(
            1,
            json!({"ci_steps":[{"index":0,"command":"tests","exit_code":1,"stderr_tail":"boom"}]}),
        );
        let mut cancelled = review(1, json!({}));
        cancelled.status = crate::ReviewStatus::Cancelled;
        for earlier in [ci_only, cancelled] {
            let first_assessment = anchored_review(
                2,
                json!([{"summary":"real bug", "where":"src/other.rs:1"}]),
                json!([]),
            );
            let ledger = finding_ledger(&[earlier, first_assessment]).unwrap();
            let finding = entry(&ledger, "F1");
            assert_eq!(finding.severity, ReviewFindingSeverity::Blocking);
            assert!(finding.anchoring_note.is_none());
        }
    }

    #[test]
    fn finding_ledger_anchoring_reads_dot_relative_and_sentence_final_paths() {
        let first = anchored_review(1, json!([{"summary":"first"}]), json!([]));
        let second = anchored_review(
            2,
            json!([
                {"summary":"dot relative", "where":"./src/task.rs:12"},
                {"summary":"sentence", "where":"Drafts are dropped in src/task.rs."},
                {"summary":"outside", "where":"./src/old.rs:3"}
            ]),
            json!(["F1"]),
        );
        let ledger = finding_ledger(&[first, second]).unwrap();
        for id in ["F2", "F3"] {
            assert_eq!(
                entry(&ledger, id).severity,
                ReviewFindingSeverity::Blocking,
                "{id}"
            );
        }
        assert_eq!(entry(&ledger, "F4").severity, ReviewFindingSeverity::Minor);
    }
}
