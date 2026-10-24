//! Reviewer final-message parsing and review comment rendering cases.

use super::*;
use tempfile::NamedTempFile;

fn reviewer_execution(logs_path: String, summary: &str) -> Execution {
    let now = now_rfc3339();
    Execution {
        id: "execution-reviewer".to_owned(),
        task_id: "task-reviewer".to_owned(),
        agent_id: Some("agent-reviewer".to_owned()),
        role: crate::workflow::default_roles::REVIEWER.to_owned(),
        status: ExecutionStatus::Completed,
        stop_reason: None,
        stopped_by: None,
        resume_policy: None,
        stopped_at: None,
        parent_execution_id: None,
        agent_session_id: None,
        agent_message_id: None,
        last_activity_at: None,
        execution_version: 1,
        lease_owner: None,
        lease_expires_at: None,
        hard_deadline_at: None,
        last_heartbeat_at: None,
        last_progress_at: None,
        prompt: None,
        summary: Some(summary.to_owned()),
        logs_path: Some(logs_path),
        before_sha: None,
        after_sha: None,
        error: None,
        executor_config_snapshot_json: None,
        workspace_id: None,
        created_at: now.clone(),
        updated_at: now,
    }
}

#[tokio::test]
async fn reviewer_final_message_reads_claude_assistant_content() {
    let file = NamedTempFile::new().expect("temp log creates");
    let log = json!({
        "schema_version": 1,
        "sequence": 1,
        "timestamp": "2026-04-27T00:00:00Z",
        "execution_id": "execution-reviewer",
        "kind": "assistant",
        "stream": "main",
        "payload": {
            "message": {
                "content": [
                    {
                        "type": "text",
                        "text": "No issues found.\n===REVIEW: PASS==="
                    }
                ]
            }
        },
        "truncated": false
    });
    std::fs::write(file.path(), format!("{log}\n")).expect("log writes");

    let execution = reviewer_execution(
        file.path().to_string_lossy().into_owned(),
        "Truncated summary without marker",
    );

    let message = reviewer_final_message(&execution)
        .await
        .expect("message extracts");
    assert!(message.contains("===REVIEW: PASS==="));
}

#[tokio::test]
async fn reviewer_final_message_reads_claude_result_when_assistant_text_missing() {
    let file = NamedTempFile::new().expect("temp log creates");
    let log = json!({
        "schema_version": 1,
        "sequence": 1,
        "timestamp": "2026-04-27T00:00:00Z",
        "execution_id": "execution-reviewer",
        "kind": "session_info",
        "stream": "main",
        "payload": {
            "subtype": "success",
            "result": "Looks good.\n===REVIEW: PASS==="
        },
        "truncated": false
    });
    std::fs::write(file.path(), format!("{log}\n")).expect("log writes");

    let execution = reviewer_execution(
        file.path().to_string_lossy().into_owned(),
        "Truncated summary without marker",
    );

    let message = reviewer_final_message(&execution)
        .await
        .expect("message extracts");
    assert!(message.contains("===REVIEW: PASS==="));
}

fn conformance(assessment: Option<api_types::ReviewAssessment>) -> api_types::ReviewConformance {
    api_types::ReviewConformance {
        status: api_types::ConformanceStatus::Failed,
        contract: None,
        assessment,
        checks: Vec::new(),
        reason: Some("blocking finding".to_owned()),
    }
}

#[test]
fn reviewer_comment_without_findings_keeps_the_bounded_markdown_report() {
    let assessment = api_types::ReviewAssessment {
        result: api_types::ReviewResult::Fail,
        reason: "api not exported".to_owned(),
        fixable_by: api_types::FixableBy::Coder,
        repeat: false,
        findings: Vec::new(),
        resolved: Vec::new(),
        report: "## R1\n\nExpected the api exported; `src/lib.rs:3` does not.".to_owned(),
    };
    let mut failed = conformance(Some(assessment.clone()));
    failed.reason = Some("api not exported".to_owned());

    assert_eq!(
            reviewer_comment(ReviewStatus::Failed, 2, &failed, &[], None),
            "Review failed (attempt 2): api not exported\n\n\
             ## R1\n\nExpected the api exported; `src/lib.rs:3` does not.\n\nFull review: Review tab, attempt 2"
        );

    let mut blocked = conformance(Some(api_types::ReviewAssessment {
        result: api_types::ReviewResult::Blocked,
        reason: "tsc not found".to_owned(),
        fixable_by: api_types::FixableBy::Coder,
        repeat: false,
        findings: Vec::new(),
        resolved: Vec::new(),
        report: String::new(),
    }));
    blocked.status = api_types::ConformanceStatus::Blocked;
    blocked.reason = Some("tsc not found".to_owned());
    assert_eq!(
            reviewer_comment(ReviewStatus::Failed, 1, &blocked, &[], None),
            "Review blocked by its environment (attempt 1): tsc not found\n\nFull review: Review tab, attempt 1"
        );
}

#[test]
fn reviewer_comment_without_an_assessment_has_the_review_tab_pointer() {
    assert_eq!(
        reviewer_comment(ReviewStatus::Passed, 1, &conformance(None), &[], None),
        "Review passed (attempt 1)\n\nFull review: Review tab, attempt 1"
    );
    assert_eq!(
        reviewer_comment(ReviewStatus::Failed, 1, &conformance(None), &[], None),
        "Review failed (attempt 1): blocking finding\n\nFull review: Review tab, attempt 1"
    );
}
fn comment_finding(
    id: &str,
    status: api_types::ReviewFindingStatus,
) -> api_types::ReviewFindingEntry {
    api_types::ReviewFindingEntry {
        requirement_ids: Vec::new(),
        kind: None,
        same_cause_as: None,
        anchoring_note: None,
        resolved_attempt: None,
        ci_triage: None,
        id: id.into(),
        source: api_types::ReviewFindingSource::Reviewer,
        severity: api_types::ReviewFindingSeverity::Blocking,
        summary: format!("Issue {id}"),
        r#where: None,
        expected: None,
        observed: None,
        first_attempt: 1,
        last_checked_attempt: 2,
        status,
        streak: if status == api_types::ReviewFindingStatus::Open {
            2
        } else {
            0
        },
    }
}

#[test]
fn reviewer_comment_ci_noise_is_labelled_without_a_resolution_checkbox() {
    use api_types::ReviewFindingStatus as Status;
    let ledger = [
        comment_finding("CI-0#flake", Status::Flaky),
        comment_finding("CI-0#base", Status::PreExisting),
    ];
    let text = reviewer_comment(
        ReviewStatus::Passed,
        2,
        &api_types::ReviewConformance::default(),
        &ledger,
        None,
    );
    assert!(text.contains("flaky (non-blocking)"));
    assert!(text.contains("pre-existing (non-blocking)"));
    assert!(!text.contains("[x]") && !text.contains("[ ]"));
}

#[test]
fn reviewer_comment_failed_findings_checklist_omits_the_report() {
    use api_types::ReviewFindingStatus as Status;
    let mut ledger = vec![
        comment_finding("F1", Status::Open),
        comment_finding("F2", Status::Resolved),
        comment_finding("F3", Status::Unconfirmed),
        comment_finding("F4", Status::Open),
        comment_finding("CI-1", Status::Resolved),
        comment_finding("F5", Status::Resolved),
    ];
    ledger[3].first_attempt = 2;
    ledger[4].source = api_types::ReviewFindingSource::Ci;
    let previous = [comment_finding("F5", Status::Resolved)];
    let ledger = current_comment_findings(ledger, &previous);
    let mut result = conformance(None);
    result.assessment = Some(api_types::ReviewAssessment {
        result: api_types::ReviewResult::Fail,
        reason: "blocked".into(),
        report: "FULL REPORT MUST STAY OUT".into(),
        fixable_by: api_types::FixableBy::Coder,
        repeat: false,
        findings: vec![api_types::ReviewFinding {
            requirement_ids: Vec::new(),
            kind: None,
            same_cause_as: None,
            id: Some("F1".into()),
            summary: "Issue F1".into(),
            severity: api_types::ReviewFindingSeverity::Blocking,
            r#where: None,
            expected: None,
            observed: None,
        }],
        resolved: Vec::new(),
    });
    let text = reviewer_comment(ReviewStatus::Failed, 2, &result, &ledger, None);
    assert_eq!(text, "Review failed (attempt 2): blocking finding\n- [ ] F1 Issue F1 — open since attempt 1\n- [ ] F4 New: Issue F4 — open since attempt 2\n- [x] F2 Issue F2 — resolved\n- [x] CI-1 Issue CI-1 — passing now\n- F3 Issue F3 — not re-checked\n\nFull review: Review tab, attempt 2");
    assert!(!text.contains("FULL REPORT"));
    assert!(!text.contains("Verify:"));
}

#[test]
fn reviewer_comment_passed_minor_findings_are_visible() {
    let mut finding = comment_finding("F1", api_types::ReviewFindingStatus::Open);
    finding.severity = api_types::ReviewFindingSeverity::Minor;
    let text = reviewer_comment(
        ReviewStatus::Passed,
        2,
        &conformance(None),
        &[finding],
        None,
    );
    assert_eq!(text, "Review passed (attempt 2)\n- [ ] F1 Issue F1 — open since attempt 1\n\nFull review: Review tab, attempt 2");
}

#[test]
fn reviewer_comment_legacy_report_is_capped_at_two_kib_utf8_bytes() {
    let result = conformance(Some(api_types::ReviewAssessment {
        result: api_types::ReviewResult::Fail,
        reason: "blocked".into(),
        report: "界".repeat(7000),
        fixable_by: api_types::FixableBy::Coder,
        repeat: false,
        findings: Vec::new(),
        resolved: Vec::new(),
    }));
    let text = reviewer_comment(ReviewStatus::Failed, 2, &result, &[], None);
    let report = text.split("\n\n").nth(1).unwrap();
    assert_eq!(report, "界".repeat(2048 / 3));
    assert!(text.ends_with("Full review: Review tab, attempt 2"));
}

#[test]
fn reviewer_comment_verify_only_when_recorded() {
    let verify = json!({"status":"fail","checks":[{"name":"smoke","result":"fail","evidence":["media-1", "media-1"]}]});
    let text = reviewer_comment(
        ReviewStatus::Failed,
        2,
        &conformance(None),
        &[],
        Some(&verify),
    );
    assert!(text.contains(
        "Verify: fail — failing checks: smoke (1 evidence) [evidence](/api/v1/media/media-1)"
    ));
    let skipped = json!({"status":"not_run","reason":"placement_unsupported"});
    let text = reviewer_comment(
        ReviewStatus::Passed,
        2,
        &conformance(None),
        &[],
        Some(&skipped),
    );
    assert!(text.contains("Verify: not_run — placement_unsupported"));
}

#[test]
fn reviewer_comment_ci_failure_lists_forge_findings() {
    let mut finding = comment_finding("CI-2", api_types::ReviewFindingStatus::Open);
    finding.source = api_types::ReviewFindingSource::Ci;
    finding.summary = "cargo test failed".into();
    finding.first_attempt = 2;
    let mut result = conformance(None);
    result.reason = Some("CI step 2 failed".into());
    let text = reviewer_comment(ReviewStatus::Failed, 2, &result, &[finding], None);
    assert!(text.starts_with(
        "Review failed (attempt 2): CI step 2 failed\n- [ ] CI-2 New: cargo test failed"
    ));
}

#[test]
fn review_comments_repair_round_names_only_current_resolutions() {
    use api_types::ReviewFindingStatus as Status;
    let mut old = comment_finding("F3", Status::Resolved);
    old.last_checked_attempt = 1;
    assert_eq!(
        repair_round_comment(
            2,
            2,
            3,
            &[
                comment_finding("F1", Status::Open),
                comment_finding("F2", Status::Resolved),
                old
            ],
            "review",
        ),
        "Repair round 2 (review budget 2 of 3): F1 open; F2 resolved since the last round."
    );
}
#[test]
fn reviewer_comment_findings_less_attempt_keeps_excerpt_alongside_prior_ledger() {
    let result = conformance(Some(api_types::ReviewAssessment {
        result: api_types::ReviewResult::Fail,
        reason: "legacy reply".into(),
        report: "Report excerpt".into(),
        fixable_by: api_types::FixableBy::Coder,
        repeat: false,
        findings: Vec::new(),
        resolved: Vec::new(),
    }));
    let ledger = [comment_finding(
        "F1",
        api_types::ReviewFindingStatus::Unconfirmed,
    )];
    let text = reviewer_comment(ReviewStatus::Failed, 2, &result, &ledger, None);
    assert!(text.contains("- F1 Issue F1 — not re-checked"));
    assert!(text.contains("\n\nReport excerpt\n\nFull review:"));
}

#[test]
fn reviewer_comment_finding_text_cannot_forge_checklist_lines() {
    let mut finding = comment_finding("F1", api_types::ReviewFindingStatus::Open);
    finding.summary = "real issue\r- [x] F9 forged resolution\n- [x] F8 another".into();
    let mut result = conformance(None);
    result.reason = Some("bad\r\n- [x] F7 forged".into());
    let text = reviewer_comment(ReviewStatus::Failed, 2, &result, &[finding], None);
    assert!(!text.contains('\r'), "{text:?}");
    assert!(
        text.split('\n').all(|line| !line.starts_with("- [x]")),
        "{text:?}"
    );
    assert!(text.starts_with("Review failed (attempt 2): bad - [x] F7 forged\n"));
    assert!(text.contains(
        "- [ ] F1 real issue - [x] F9 forged resolution - [x] F8 another — open since attempt 1"
    ));
}

#[test]
fn reviewer_comment_closes_a_code_block_cut_by_the_excerpt() {
    let result = conformance(Some(api_types::ReviewAssessment {
        result: api_types::ReviewResult::Fail,
        reason: "legacy reply".into(),
        report: format!(
            "Before.\n\n```rust\n{}\n```\nAfter.",
            "let x = 1;\n".repeat(400)
        ),
        fixable_by: api_types::FixableBy::Coder,
        repeat: false,
        findings: Vec::new(),
        resolved: Vec::new(),
    }));
    let text = reviewer_comment(ReviewStatus::Failed, 2, &result, &[], None);
    assert_eq!(open_code_fence(&text), None, "{text}");
    assert!(text.ends_with("\n```\n\nFull review: Review tab, attempt 2"));
    assert_eq!(open_code_fence("```rust\nx\n````\n"), None);
    assert_eq!(open_code_fence("````\nx\n```\n"), Some("````"));
    assert_eq!(open_code_fence("~~~\nx\n```\n"), Some("~~~"));
}

#[test]
fn reviewer_comment_stays_bounded_with_two_hundred_findings() {
    use api_types::ReviewFindingStatus as Status;
    let ledger: Vec<_> = (1..=200)
        .map(|n| {
            let mut entry = comment_finding(
                &format!("F{n}"),
                if n > 150 {
                    Status::Open
                } else {
                    Status::Resolved
                },
            );
            entry.summary = "界".repeat(1000);
            entry
        })
        .collect();
    let text = reviewer_comment(ReviewStatus::Failed, 2, &conformance(None), &ledger, None);
    assert!(text.len() < 12 * 1024, "{} bytes", text.len());
    // Open findings are listed first, and every id is still named.
    assert!(text.contains("\n- [ ] F151 "));
    let words: std::collections::BTreeSet<_> =
        text.split(|ch: char| !ch.is_ascii_alphanumeric()).collect();
    for n in 1..=200 {
        assert!(words.contains(format!("F{n}").as_str()), "F{n} missing");
    }
    assert!(text.contains("Also in the ledger (see the Review tab): "));
    assert!(text.ends_with("Full review: Review tab, attempt 2"));
}
#[test]
fn review_comment_does_not_tick_a_step_finding_replaced_by_test_findings() {
    let review = |attempt, failures: Value| {
        db::Review {
            id: format!("r{attempt}"),
            task_id: "task".into(),
            execution_id: "candidate".into(),
            reviewer_execution_id: None,
            auditor_execution_id: None,
            attempt_number: attempt,
            status: ReviewStatus::Failed,
            step_results_json: json!({"ci_steps":[{"index":0,"command":"cargo test","exit_code":101,"stderr_tail":"error[E0425]","failures":failures}]}).to_string(),
            started_at: "now".into(),
            finished_at: None,
            created_at: "now".into(),
            updated_at: "now".into(),
        }
    };
    let prior = db::review_findings::finding_ledger(&[review(1, json!([]))]).unwrap();
    let ledger = db::review_findings::finding_ledger(&[
        review(1, json!([])),
        review(2, json!(["parser::empty"])),
    ])
    .unwrap();
    let ledger = current_comment_findings(ledger, &prior);
    let text = reviewer_comment(ReviewStatus::Failed, 2, &conformance(None), &ledger, None);
    assert!(text.contains("- [ ] CI-0#"), "{text}");
    assert!(!text.contains("[x] CI-0"), "{text}");
    let repair = repair_round_comment(2, 1, 3, &ledger, "review");
    assert!(
        repair.ends_with("none resolved since the last round."),
        "{repair}"
    );
}
#[test]
fn reviewer_comment_does_not_repeat_resolutions_on_later_passing_checks() {
    let review = |attempt, exit| {
        db::Review {
            id: format!("r{attempt}"), task_id: "task".into(), execution_id: "candidate".into(),
            reviewer_execution_id: None, auditor_execution_id: None, attempt_number: attempt,
            status: ReviewStatus::Failed,
            step_results_json: json!({"ci_steps":[{"index":0,"command":"test","exit_code":exit,"stderr_tail":"failure"}]}).to_string(),
            started_at: "now".into(), finished_at: None, created_at: "now".into(), updated_at: "now".into(),
        }
    };
    let prior = db::review_findings::finding_ledger(&[review(1, 1)]).unwrap();
    let ledger = db::review_findings::finding_ledger(&[review(1, 1), review(2, 0)]).unwrap();
    let second = reviewer_comment(
        ReviewStatus::Passed,
        2,
        &conformance(None),
        &current_comment_findings(ledger.clone(), &prior),
        None,
    );
    assert!(second.contains("- [x] CI-0"));
    let third_ledger =
        db::review_findings::finding_ledger(&[review(1, 1), review(2, 0), review(3, 0)]).unwrap();
    assert_eq!(third_ledger[0].last_checked_attempt, 3);
    let third = reviewer_comment(
        ReviewStatus::Passed,
        3,
        &conformance(None),
        &current_comment_findings(third_ledger, &ledger),
        None,
    );
    assert!(!third.contains("CI-0"), "{third}");
}
#[test]
fn repair_and_park_comments_limit_test_ids_per_step() {
    let ledger: Vec<_> = (0..30)
        .map(|index| {
            let mut entry = comment_finding(
                &format!("CI-0#{index:08x}"),
                api_types::ReviewFindingStatus::Open,
            );
            entry.source = api_types::ReviewFindingSource::Ci;
            entry
        })
        .chain([comment_finding("F1", api_types::ReviewFindingStatus::Open)])
        .collect();
    let text = repair_round_comment(2, 1, 3, &ledger, "review");
    assert_eq!(text.matches("CI-0#").count(), 20, "{text}");
    assert!(text.contains("CI-0 +10 more"), "{text}");
    assert!(text.contains("F1"), "{text}");
    let open = finding_ids(&ledger, |entry| entry.status.is_open());
    assert_eq!(open.matches("CI-0#").count(), 20, "{open}");
}
#[test]
fn reviewer_comment_limits_test_findings_per_step() {
    let ledger: Vec<_> = (0..30)
        .map(|index| {
            let mut entry = comment_finding(
                &format!("CI-0#{index:08x}"),
                api_types::ReviewFindingStatus::Open,
            );
            entry.source = api_types::ReviewFindingSource::Ci;
            entry
        })
        .collect();
    let text = reviewer_comment(ReviewStatus::Failed, 2, &conformance(None), &ledger, None);
    assert_eq!(text.matches("- [ ] CI-0#").count(), 20);
    assert!(text.contains("CI-0: +10 more"));
    assert_eq!(ledger.len(), 30);
}
