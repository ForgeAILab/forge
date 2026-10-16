//! Pure projection of persisted source observations. Callers supply exactly one
//! review-budget window; computed ids and state never enter frozen assessments.
use std::collections::{BTreeMap, BTreeSet};

use api_types::{
    ReviewConformance, ReviewFinding, ReviewFindingEntry, ReviewFindingSeverity,
    ReviewFindingSource as Source, ReviewFindingStatus as Status, StepResultEntry,
};
use db::Review;
use serde_json::Value;
use sha2::{Digest, Sha256};

/// Derive every finding, ordered lexically by id. The API's 200-item limit is
/// applied only after derivation, so hidden entries still reserve their ids.
pub fn finding_ledger(window_reviews: &[Review]) -> Result<Vec<ReviewFindingEntry>, String> {
    let mut reviews: Vec<_> = window_reviews.iter().collect();
    reviews.sort_by_key(|review| (review.attempt_number, &review.id));
    let mut ledger = BTreeMap::<String, ReviewFindingEntry>::new();
    let mut next_id = 1;
    let mut observed_contracts = BTreeSet::new();
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
            if step.exit_code == 0 || !signatures.is_empty() {
                // A completed signature list proves earlier test findings absent.
                if !step.failures_truncated {
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
                        let hash = format!("{:x}", Sha256::digest(signature.as_bytes()));
                        let class = classes
                            .get(&signature)
                            .copied()
                            .or_else(|| step.triage.as_ref().and_then(|triage| triage.class));
                        observations.insert(
                            format!("{prefix}#{}", &hash[..8]),
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
            }
            let class = step.triage.as_ref().and_then(|triage| triage.class);
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
            if !detailed_ci.contains(&id) {
                let class = check.triage.as_ref().and_then(|triage| triage.class);
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
                    entry.status = Status::Resolved;
                    entry.last_checked_attempt = attempt;
                    entry.streak = 0;
                }
            } else {
                let finding = ReviewFinding {
                    id: None,
                    summary: format!("{command} failed"),
                    severity: ReviewFindingSeverity::Blocking,
                    r#where: Some(command),
                    expected: Some("Pass".into()),
                    observed: Some(output_tail(&observed)),
                };
                open(&mut ledger, id.clone(), source, &finding, attempt);
                let entry = ledger.get_mut(&id).expect("opened finding");
                entry.ci_triage = class;
                if let Some(class) = class.filter(|class| !class.blocks()) {
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
                entry.status = Status::Resolved;
                entry.streak = 0;
                entry.last_checked_attempt = attempt;
            }
            let mut continued = BTreeSet::new();
            for finding in assessment.findings {
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
                    ledger.insert(id.clone(), previous.clone());
                }
                open(&mut ledger, id, Source::Reviewer, &finding, attempt);
            }
        }
    }
    Ok(ledger.into_values().collect())
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
        status: Status::Open,
        streak: previous
            .filter(|entry| entry.status == Status::Open)
            .map_or(1, |entry| entry.streak + 1),
    };
    ledger.insert(id, entry);
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
            status: db::ReviewStatus::Failed,
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
    fn finding_ledger_resolves_test_signatures_and_excludes_triaged_noise() {
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
        assert_eq!(fixed.status, Status::Resolved);
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
        assert!(ledger.iter().all(|entry| entry.status != Status::Open));
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
            (Status::Open, 1)
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
            (Status::Open, 2)
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
}
