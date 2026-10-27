use super::*;

#[test]
fn repair_brief_two_rounds_lists_open_resolved_and_previous_summary() {
    let mut ctx = fake_context(default_roles::CODER);
    let first = db::Review {
        step_results_json: json!({"conformance":{"status":"failed","contract":null,"checks":[],"reason":null,"assessment":{"result":"fail","reason":"issues","findings":[{"summary":"carried","expected":"safe","observed":"crash"},{"summary":"fixed"},{"summary":"forgotten"}]}}}).to_string(),
        ..fake_review(1, db::ReviewStatus::Failed)
    };
    let second = db::Review {
        step_results_json: json!({"ci_steps":[
            {"index":0,"command":"cargo test","exit_code":1,"stderr_tail":"","output_tail":"first tail","failures":["parser::empty"]},
            {"index":2,"command":"pnpm test","exit_code":1,"stderr_tail":"second tail"}],
            "conformance":{"status":"failed","contract":null,"checks":[],"reason":null,"assessment":{"result":"fail","reason":"still issues","report":"report detail","findings":[{"id":"F1","summary":"carried","expected":"safe","observed":"crash"},{"summary":"new"}],"resolved":["F2"]}},
            "verify":{"status":"fail","checks":[{"name":"smoke","result":"fail","summary":"button broke","evidence":["screenshots/button.png"]}]}}).to_string(),
        ..fake_review(2, db::ReviewStatus::Failed)
    };
    ctx.prior_reviews = vec![first, second];
    ctx.previous_coder_summary = Some("Implemented the first correction".into());
    ctx.repair_budget = Some(super::super::repair_brief::RepairBudget {
        name: "review".into(),
        spent: 2,
        limit: 3,
    });
    for builder in [
        BUILDER_ID_CODER_REVIEW_FIX_V2,
        BUILDER_ID_WORKER_REVIEW_FIX_V1,
    ] {
        let text = resolve_prompt_builder(builder).build(&ctx).user;
        for expected in [
            "F1",
            "F4",
            "survived 1 repair round",
            "F2: resolved",
            "F3: not re-checked",
            "CI-0#",
            "parser::empty",
            "first tail",
            "second tail",
            "button broke",
            "screenshots/button.png",
            "Implemented the first correction",
            "review budget: 2 of 3 spent; 1 remaining",
        ] {
            assert!(text.contains(expected), "missing {expected}: {text}");
        }
        let positions: Vec<_> = [
            "Task:",
            "Round:",
            "Open findings:",
            "Resolved and unconfirmed since the last round:",
            "Failing check output:",
            "Verify results and evidence:",
            "Reviewer report:",
            "Attempts:",
        ]
        .iter()
        .map(|section| text.find(section).unwrap())
        .collect();
        assert!(positions.windows(2).all(|pair| pair[0] < pair[1]));
    }
}

#[test]
fn repair_brief_budget_trims_report_before_other_sections() {
    let mut ctx = fake_context(default_roles::CODER);
    let finding = json!({"summary":"keep this finding", "expected":"E".repeat(1000), "observed":"O".repeat(1000)});
    ctx.prior_reviews = vec![db::Review {
        step_results_json: json!({"ci_steps":[{"index":0,"command":"cargo test","exit_code":1,"stderr_tail":"","output_tail":"TAIL".repeat(1024)}],
            "conformance":{"status":"failed","contract":null,"checks":[],"reason":null,"assessment":{"result":"fail","reason":"issue","report":"界".repeat(14000),"findings":[finding]}}}).to_string(),
        ..fake_review(1, db::ReviewStatus::Failed)
    }];
    ctx.previous_coder_summary = Some("keep previous summary".into());
    let text = super::super::repair_brief::RepairBrief::review(&ctx).render();
    assert!(text.len() <= 24 * 1024, "{}", text.len());
    assert!(text.contains(&"E".repeat(1000)));
    assert!(text.contains(&"O".repeat(1000)));
    assert!(text.contains(&"TAIL".repeat(1024)));
    assert!(text.contains("keep previous summary"));
    let report = text
        .split("Reviewer report:\n")
        .nth(1)
        .unwrap()
        .split("\n\nAttempts:")
        .next()
        .unwrap();
    assert!(report.len() <= 2 * 1024);
}

#[test]
fn merge_time_ci_failed_brief_carries_target_command_and_tail() {
    for (builder, role) in [
        (BUILDER_ID_CODER_MERGE_FIX_V2, default_roles::CODER),
        (BUILDER_ID_WORKER_MERGE_FIX_V1, default_roles::WORKER),
    ] {
        let mut ctx = fake_context(role);
        ctx.state_name = default_states::MERGE_FAILED.into();
        ctx.task.error_annotation = Some(json!({"type":"ci_failed", "message":"checks failed after the rebase onto main: `cargo test` exited 101: parser::empty FAILED"}).to_string());
        ctx.task.condition = db::compose_condition(&db::ConditionInput::from(&ctx.task));
        let text = resolve_prompt_builder(builder).build(&ctx).user;
        for expected in [
            "MERGE-CI",
            "main",
            "cargo test",
            "parser::empty FAILED",
            "Description:",
            "Original plan:",
        ] {
            assert!(text.contains(expected), "{text}");
        }
        assert!(!text.contains("manual task-worktree repair"));
        assert!(db::review_findings::finding_ledger(&ctx.prior_reviews)
            .unwrap()
            .is_empty());
    }
}

#[test]
fn repair_brief_uses_the_current_window_and_automatic_recovery_budget() {
    use super::super::repair_brief::{RepairBrief, RepairBudget};
    let mut ctx = fake_context(default_roles::CODER);
    ctx.prior_reviews = vec![db::Review {
        step_results_json: json!({"conformance":{"status":"failed","contract":null,"checks":[],"reason":null,"assessment":{"result":"fail","reason":"old","findings":[{"summary":"old-window finding"}]}}}).to_string(),
        ..fake_review(1, db::ReviewStatus::Failed)
    }];
    ctx.review_window = Some(vec![]);
    ctx.latest_review_feedback = Some("old-window feedback".into());
    ctx.latest_review_logs_path = Some("old-window log".into());
    let text = RepairBrief::review(&ctx)
        .automatic_recovery(
            RepairBudget {
                name: "automatic_review_recovery".into(),
                spent: 1,
                limit: 2,
            },
            "budget exhausted",
        )
        .render();
    assert!(!text.contains("old-window"));
    assert!(text.contains("automatic_review_recovery budget: 1 of 2 spent; 1 remaining"));
    assert!(text.contains("Original plan:"));
}

#[test]
fn repair_brief_limits_ci_findings_across_open_and_resolved_sections() {
    let signatures: Vec<_> = (0..28).map(|index| format!("test-{index:03}")).collect();
    let ci = |attempt, exit, failures: &[String]| {
        db::Review {
        step_results_json: json!({"ci_steps":[{"index":0,"command":"test","exit_code":exit,"stderr_tail":"","failures":failures}]}).to_string(),
        ..fake_review(attempt, db::ReviewStatus::Failed)
    }
    };
    for passed in [false, true] {
        let mut ctx = fake_context(default_roles::CODER);
        let remaining = if passed {
            &signatures[..0]
        } else {
            &signatures[..26]
        };
        ctx.prior_reviews = vec![
            ci(1, 1, &signatures),
            ci(2, if passed { 0 } else { 1 }, remaining),
        ];
        for builder in [
            BUILDER_ID_CODER_REVIEW_FIX_V2,
            BUILDER_ID_WORKER_REVIEW_FIX_V1,
        ] {
            let text = resolve_prompt_builder(builder).build(&ctx).user;
            assert_eq!(text.matches("\n- CI-0#").count(), 20, "{text}");
            if passed {
                assert!(text.contains("Resolved and unconfirmed since the last round:"));
                assert!(text.contains("CI-0: +8 more"), "{text}");
            } else {
                // A red fail-fast run cannot resolve the two omitted tests.
                assert!(!text.contains("Resolved and unconfirmed since the last round:"));
                assert!(text.contains("CI-0: +8 more"), "{text}");
            }
        }
    }
}

#[test]
fn repair_brief_does_not_call_a_step_replaced_by_test_findings_resolved() {
    let ci = |attempt, failures: serde_json::Value| {
        db::Review {
        step_results_json: json!({"ci_steps":[{"index":0,"command":"cargo test","exit_code":101,"stderr_tail":"error[E0425]","failures":failures}]}).to_string(),
        ..fake_review(attempt, db::ReviewStatus::Failed)
    }
    };
    let mut ctx = fake_context(default_roles::CODER);
    ctx.prior_reviews = vec![ci(1, json!([])), ci(2, json!(["parser::empty"]))];
    let text = resolve_prompt_builder(BUILDER_ID_CODER_REVIEW_FIX_V2)
        .build(&ctx)
        .user;
    assert!(text.contains("- CI-0#"), "{text}");
    assert!(!text.contains("CI-0: resolved"), "{text}");
}

#[test]
fn repair_brief_hard_cap_clips_fifty_long_findings_and_lists_the_rest_by_id() {
    let long = |tag: &str, index: usize| format!("{tag}{index:02}{}", "x".repeat(998));
    let findings: Vec<_> = (0..50)
        .map(|index| {
            json!({"summary": long("S", index), "where": long("W", index),
                   "expected": long("E", index), "observed": long("O", index)})
        })
        .collect();
    let mut ctx = fake_context(default_roles::CODER);
    ctx.prior_reviews = vec![db::Review {
        step_results_json: json!({"conformance":{"status":"failed","contract":null,"checks":[],"reason":null,
            "assessment":{"result":"fail","reason":"many","report":"r".repeat(8 * 1024),"findings":findings}}}).to_string(),
        ..fake_review(1, db::ReviewStatus::Failed)
    }];
    for builder in [
        BUILDER_ID_CODER_REVIEW_FIX_V2,
        BUILDER_ID_WORKER_REVIEW_FIX_V1,
    ] {
        let text = resolve_prompt_builder(builder).build(&ctx).user;
        assert!(text.len() <= 24 * 1024, "{}", text.len());
        assert!(text.contains("Description:") && text.contains("Original plan:"));
        // Every open finding is still named, in full or by id.
        let bare: Vec<_> = text
            .split("Also open (details in the finding ledger): ")
            .nth(1)
            .expect("bare ids")
            .lines()
            .next()
            .unwrap()
            .split(", ")
            .collect();
        for index in 1..=50 {
            let id = format!("F{index}");
            assert!(
                text.contains(&format!("- {id} [")) || bare.contains(&id.as_str()),
                "{id} missing"
            );
        }
        // Detailed findings are clipped like the review comment.
        assert!(text.contains(&format!("{}…", &long("S", 0)[..200])));
        assert!(!text.contains(&long("S", 0)[..201]));
    }
}

#[test]
fn repair_brief_hard_cap_bounds_every_failing_step_and_the_verify_dump() {
    let steps = |count: usize| -> Vec<serde_json::Value> {
        (0..count)
            .map(|index| {
                json!({"index": index, "command": format!("step-{index}"), "exit_code": 1,
                                "stderr_tail": "", "output_tail": "T".repeat(4 * 1024)})
            })
            .collect()
    };
    let context = |count| {
        let mut ctx = fake_context(default_roles::CODER);
        ctx.prior_reviews = vec![db::Review {
            step_results_json: json!({"ci_steps": steps(count),
                "verify":{"status":"fail","app_log_tail":"L".repeat(40 * 1024),"checks":[]}})
            .to_string(),
            ..fake_review(1, db::ReviewStatus::Failed)
        }];
        ctx
    };
    let text = resolve_prompt_builder(BUILDER_ID_CODER_REVIEW_FIX_V2)
        .build(&context(20))
        .user;
    assert!(text.len() <= 24 * 1024, "{}", text.len());
    for index in 0..20 {
        assert!(text.contains(&format!("CI-{index}: step-{index} (exit 1)")));
    }
    assert!(text.contains("(output omitted; see the Review's CI step results)"));
    // Unprotected sections are cut before any finding text: three failing
    // steps keep every step finding's full observed tail.
    let text = resolve_prompt_builder(BUILDER_ID_CODER_REVIEW_FIX_V2)
        .build(&context(3))
        .user;
    assert!(text.len() <= 24 * 1024, "{}", text.len());
    assert!(text.contains(&format!("observed: {}", "T".repeat(4 * 1024))));
    assert!(text.contains("… (truncated; see the Review tab)"));
    assert!(!text.contains("T…"));
}

#[test]
fn repair_brief_keeps_the_rejection_reason_when_review_wrote_no_row() {
    let mut ctx = fake_context(default_roles::CODER);
    ctx.review_window = Some(Vec::new());
    ctx.transition_log.push(db::TransitionLog {
        id: "rejected".into(),
        task_id: ctx.task.id.clone(),
        from_state: "review".into(),
        to_state: ctx.state_name.clone(),
        triggered_by: "system:workflow".into(),
        trigger_name: None,
        bridge: Default::default(),
        trigger_reason: "review entry failed: the checkout has no commits".into(),
        rejection: true,
        created_at: db::now_rfc3339(),
        hook_results_json: None,
    });
    for builder in [
        BUILDER_ID_CODER_REVIEW_FIX_V2,
        BUILDER_ID_WORKER_REVIEW_FIX_V1,
    ] {
        let text = resolve_prompt_builder(builder).build(&ctx).user;
        assert!(
            text.contains("review entry failed: the checkout has no commits"),
            "{text}"
        );
    }
}

#[test]
fn worker_repair_brief_keeps_the_task_comments() {
    let mut ctx = fake_context(default_roles::WORKER);
    ctx.prior_reviews = vec![db::Review {
        step_results_json: json!({"ci_steps":[{"index":0,"command":"cargo test","exit_code":101,"stderr_tail":"boom"}]}).to_string(),
        ..fake_review(1, db::ReviewStatus::Failed)
    }];
    ctx.comments = vec![db::TaskComment {
        author_name: "Owner".into(),
        content: "Keep the public parser API unchanged.".into(),
        ..fake_comment()
    }];
    let text = resolve_prompt_builder(BUILDER_ID_WORKER_REVIEW_FIX_V1)
        .build(&ctx)
        .user;
    assert!(
        text.contains("Recent comments:\n- Owner: Keep the public parser API unchanged."),
        "{text}"
    );
}

#[test]
fn merge_ci_brief_keeps_the_passed_review_and_the_previous_execution() {
    for (builder, role) in [
        (BUILDER_ID_CODER_MERGE_FIX_V2, default_roles::CODER),
        (BUILDER_ID_WORKER_MERGE_FIX_V1, default_roles::WORKER),
    ] {
        let mut ctx = fake_context(role);
        ctx.state_name = default_states::MERGE_FAILED.into();
        ctx.task.review_passed_at = Some("2026-10-16T00:00:00Z".into());
        ctx.task.error_annotation = Some(json!({"type":"ci_failed", "message":"checks failed after the rebase onto main: `cargo test` exited 101: boom"}).to_string());
        ctx.task.condition = db::compose_condition(&db::ConditionInput::from(&ctx.task));
        ctx.continuation_of_execution_id = Some("execution-previous".into());
        ctx.continuation_logs_path = Some("/logs/previous.jsonl".into());
        let text = resolve_prompt_builder(builder).build(&ctx).user;
        for expected in [
            "Review already passed before the merge failure",
            "execution-previous",
            "/logs/previous.jsonl",
        ] {
            assert!(text.contains(expected), "{expected}: {text}");
        }
    }
}

#[test]
fn automatic_recovery_brief_says_it_is_the_final_attempt() {
    use super::super::repair_brief::{RepairBrief, RepairBudget};
    let ctx = fake_context(default_roles::CODER);
    let text = RepairBrief::review(&ctx)
        .automatic_recovery(
            RepairBudget {
                name: "automatic_review_recovery".into(),
                spent: 1,
                limit: 1,
            },
            "review budget exhausted",
        )
        .render();
    assert!(text.contains("final automatic recovery attempt"), "{text}");
    assert!(text.contains("smallest useful change"), "{text}");
    assert!(
        text.contains("Review failure: review budget exhausted"),
        "{text}"
    );
}

#[test]
fn repair_brief_ci_triage_mode_controls_blocking_findings_and_labels_noise() {
    for mode in ["full", "report"] {
        let mut ctx = fake_context(default_roles::CODER);
        ctx.prior_reviews = vec![db::Review {
            step_results_json: json!({"ci_steps":[{"index":0,"command":"tests","exit_code":1,"stderr_tail":"raw output","failures":["base-test","flaky-test","new-test"],"triage":{
                "mode":mode,"base_status":"available","base_commit":"base","rerun_run_id":"rerun","base_run_id":"base-run","reason":null,"class":null,"blocking":true,
                "failures":[{"signature":"base-test","class":"pre_existing"},{"signature":"flaky-test","class":"flaky"},{"signature":"new-test","class":"introduced"}]
            }}]}).to_string(),
            ..fake_review(1, db::ReviewStatus::Failed)
        }];
        let ledger = db::review_findings::finding_ledger(&ctx.prior_reviews).unwrap();
        assert_eq!(
            ledger
                .iter()
                .filter(|entry| entry.status == api_types::ReviewFindingStatus::Open)
                .count(),
            if mode == "report" { 3 } else { 1 }
        );
        for builder in [
            BUILDER_ID_CODER_REVIEW_FIX_V2,
            BUILDER_ID_WORKER_REVIEW_FIX_V1,
        ] {
            let text = resolve_prompt_builder(builder).build(&ctx).user;
            let open = text
                .split("Open findings:")
                .nth(1)
                .unwrap()
                .split("Resolved and unconfirmed")
                .next()
                .unwrap();
            assert!(open.contains("new-test"), "{text}");
            for signature in ["base-test", "flaky-test"] {
                assert_eq!(open.contains(signature), mode == "report", "{text}");
            }
            for class in ["flaky", "pre_existing"] {
                assert!(
                    text.contains(&format!("CI classification: {class}"))
                        || text.contains(&format!("{class} (non-blocking)")),
                    "{text}"
                );
            }
        }
    }
}

#[test]
fn repair_brief_repeated_nonblocking_ci_stays_labelled_without_false_resolution() {
    let mut ctx = fake_context(default_roles::CODER);
    let ci = |attempt, detailed| db::Review {
        step_results_json: if detailed {
            json!({"ci_steps":[{"index":0,"command":"tests","exit_code":1,"stderr_tail":"failed","failures":["base-test"],"triage":{
                "mode":"full","base_status":"available","base_commit":"base","rerun_run_id":"rerun","base_run_id":"base-run","reason":null,"class":null,"blocking":false,
                "failures":[{"signature":"base-test","class":"pre_existing"}]
            }}]}).to_string()
        } else {
            json!({"ci_steps":[{"index":0,"command":"tests","exit_code":1,"stderr_tail":"failed"}]})
                .to_string()
        },
        ..fake_review(attempt, db::ReviewStatus::Failed)
    };
    let reviews = [ci(1, false), ci(2, true), ci(3, true)];
    for rounds in [2, 3] {
        ctx.prior_reviews = reviews[..rounds].to_vec();
        for builder in [
            BUILDER_ID_CODER_REVIEW_FIX_V2,
            BUILDER_ID_WORKER_REVIEW_FIX_V1,
        ] {
            let text = resolve_prompt_builder(builder).build(&ctx).user;
            assert!(text.contains("pre_existing (non-blocking)"), "{text}");
            assert!(!text.contains("CI-0: resolved"), "{text}");
        }
    }
}

#[test]
fn repair_brief_blocking_check_output_precedes_waived_ci_output() {
    let step = |index: usize, blocking: bool| {
        let (signature, class, tail) = if blocking {
            (
                "new-test".to_owned(),
                "introduced",
                format!("{}BLOCKING-END", "B".repeat(4000)),
            )
        } else {
            (format!("flaky-{index}"), "flaky", "N".repeat(4 * 1024))
        };
        json!({"index": index, "command": format!("step-{index}"), "exit_code": 1,
            "stderr_tail": "", "output_tail": tail, "failures": [signature],
            "triage": {"mode":"full","base_status":"available","base_commit":"base",
                "rerun_run_id":"rerun","base_run_id":"base-run","reason":null,"class":null,
                "blocking":blocking,"failures":[{"signature":signature,"class":class}]}})
    };
    let mut ctx = fake_context(default_roles::CODER);
    ctx.prior_reviews = vec![db::Review {
        step_results_json:
            json!({"ci_steps": (0..6).map(|index| step(index, index == 5)).collect::<Vec<_>>(),
            "verify":{"status":"fail","app_log_tail":"L".repeat(40 * 1024),"checks":[]}})
            .to_string(),
        ..fake_review(1, db::ReviewStatus::Failed)
    }];
    for builder in [
        BUILDER_ID_CODER_REVIEW_FIX_V2,
        BUILDER_ID_WORKER_REVIEW_FIX_V1,
    ] {
        let text = resolve_prompt_builder(builder).build(&ctx).user;
        assert!(text.len() <= 24 * 1024, "{}", text.len());
        for index in 0..6 {
            assert!(
                text.contains(&format!("CI-{index}: step-{index} (exit 1)")),
                "{text}"
            );
        }
        // Waived flaky output must not use up the shared tail allowance
        // before the one failure this round has to repair.
        assert!(text.contains("BLOCKING-END"), "{text}");
    }
}

#[test]
fn repair_brief_hard_cap_with_long_task_and_forty_waived_steps() {
    let mut ctx = fake_context(default_roles::CODER);
    ctx.task.description = Some("界".repeat(10 * 1024));
    ctx.plan = Some("計".repeat(10 * 1024));
    let steps: Vec<_> = (0..40).map(|index| json!({"index":index,"command":format!("step-{index}"),"exit_code":1,
        "stderr_tail":"tail".repeat(1024),"failures":[format!("failure-{index} {}", "x".repeat(280))],
        "triage":{"mode":"full","base_status":"available","blocking":false,"class":"flaky","failures":[]}})).collect();
    ctx.prior_reviews = vec![db::Review {
        step_results_json: json!({"ci_steps":steps}).to_string(),
        ..fake_review(1, db::ReviewStatus::Failed)
    }];
    for builder in [
        BUILDER_ID_CODER_REVIEW_FIX_V2,
        BUILDER_ID_WORKER_REVIEW_FIX_V1,
    ] {
        let text = resolve_prompt_builder(builder).build(&ctx).user;
        assert!(text.len() <= 24 * 1024, "{}", text.len());
        assert!(text.contains("Description:") && text.contains("Original plan:"));
        assert!(text.contains("truncated"));
        assert!(text.contains("flaky (non-blocking)"));
    }
}

fn convergence_contract(attempt: i64) -> serde_json::Value {
    json!({"execution_id":format!("reviewer-{attempt}"),"policy":"p","commit_sha":format!("c{attempt}"),"base_sha":"base",
    "candidate_changed_paths":["src/a.rs", "src/b.rs"],"check_results":[],"digest":"d", "context":{
        "project_id":"p","task_id":"task-1","repo_id":null,"charter_revision_id":null,"charter_digest":null,"charter":null,
        "task_scope":{},"linked_documents":[],"requirements":[
            {"id":"R1","source":"task","text":"No stale apply","universal":false,"allocated_task_id":null},
            {"id":"R2","source":"task","text":"Save drafts","universal":false,"allocated_task_id":null},
            {"id":"R3","source":"task","text":"Other","universal":false,"allocated_task_id":null}
        ],"required_checks":[],"source_digest":"d"
    }})
}

#[test]
fn repair_brief_round_three_has_attempt_memory_requirements_patterns_and_rebroken_findings() {
    use super::super::repair_brief::{RepairAttempt, RepairBrief};
    let mut ctx = fake_context(default_roles::CODER);
    let first = db::Review {
        step_results_json: json!({"ci_steps":[{"index":0,"command":"tests","exit_code":1,"stderr_tail":"","failures":["broken::again"]}],
            "conformance":{"status":"failed","contract":convergence_contract(1), "checks":[],"assessment":{"result":"fail","reason":"issues","findings":[
                {"summary":"stale apply", "where":"src/a.rs:4", "requirement_ids":["R1"]},
                {"summary":"save drafts", "requirement_ids":["R2"]},
                {"summary":"carried problem"}
            ]}}}).to_string(), ..fake_review(1, db::ReviewStatus::Failed)
    };
    let second = db::Review {
        step_results_json: json!({"ci_steps":[{"index":0,"command":"tests","exit_code":0,"stderr_tail":""}],
            "conformance":{"status":"failed","contract":convergence_contract(2), "checks":[],"assessment":{"result":"fail","reason":"two left", "resolved":["F1", "F2"],"findings":[
                {"id":"F3", "summary":"carried problem"},
                {"summary":"another stale apply", "where":"src/b.rs:8", "same_cause_as":"F1", "requirement_ids":["R1"]}
            ]}}}).to_string(), ..fake_review(2, db::ReviewStatus::Failed)
    };
    let third = db::Review {
        step_results_json: json!({"ci_steps":[{"index":0,"command":"tests","exit_code":1,"stderr_tail":"","failures":["broken::again"]}],
            "conformance":{"status":"failed","contract":convergence_contract(3), "checks":[],"assessment":{"result":"fail","reason":"one left", "resolved":["F3"],"findings":[
                {"id":"F4", "summary":"another stale apply", "where":"src/b.rs:8", "same_cause_as":"F1", "requirement_ids":["R1"]}
            ]}}}).to_string(), ..fake_review(3, db::ReviewStatus::Failed)
    };
    ctx.prior_reviews = vec![first, second, third];
    ctx.repair_attempts = (1..=3)
        .map(|attempt| RepairAttempt {
            attempt,
            from_sha: Some(format!("c{}", attempt - 1)),
            to_sha: Some(format!("c{attempt}")),
            files: vec![format!("src/round{attempt}.rs (+2, -1)")],
            files_unavailable: false,
            summary: Some(format!("Approach {attempt}: {}", "s".repeat(2000))),
        })
        .collect();
    let text = RepairBrief::review(&ctx).render();
    assert!(
        text.starts_with("Before editing, write a short root-cause note for each carried finding."),
        "{text}"
    );
    for expected in [
        "Attempts:",
        "Round 1: c0..c1",
        "Round 2: c1..c2",
        "src/round1.rs",
        "src/round2.rs",
        "Approach 1",
        "Approach 2",
        "F1, F2",
        "Left open: F3",
        "Opened: F4",
        "Do not repeat an approach",
        "Requirement status:",
        "R1: unmet",
        "R2: addressed",
        "R3: no open finding",
        "Pattern:",
        "F1: src/a.rs:4",
        "F4: src/b.rs:8",
        "These share a cause. Fix it at every call site and list the sites you changed.",
        "re-broken: resolved in round 2",
    ] {
        assert!(text.contains(expected), "missing {expected}: {text}");
    }
    assert!(!text.contains(&"s".repeat(1025)));
    assert!(text.len() <= 24 * 1024);
}

#[test]
fn repair_brief_attempts_cut_oldest_after_report_and_pattern_is_protected_under_cap() {
    use super::super::repair_brief::{RepairAttempt, RepairBrief};
    let mut ctx = fake_context(default_roles::CODER);
    ctx.task.description = Some("界".repeat(10 * 1024));
    ctx.plan = Some("計".repeat(10 * 1024));
    for attempt in 1..=3 {
        let findings = if attempt == 1 {
            json!([{"summary":"first instance", "where":"src/a.rs:1", "requirement_ids":["R1"]}])
        } else {
            json!([{"id":"F1","summary":"first instance", "where":"src/a.rs:1", "requirement_ids":["R1"]},
                {"summary":"same cause", "where":"src/b.rs:2", "kind":"regression", "same_cause_as":"F1", "observed":"o".repeat(1000)}])
        };
        let steps: Vec<_> = (0..40).map(|index| json!({"index":index,"command":format!("step-{index}"),"exit_code":1,"stderr_tail":"tail".repeat(1024), "failures":[format!("case-{index}")]})).collect();
        ctx.prior_reviews.push(db::Review { step_results_json:json!({"ci_steps":steps,"conformance":{"status":"failed", "contract":convergence_contract(attempt), "checks":[], "assessment":{"result":"fail","reason":"problems", "report":"r".repeat(30000),"findings":findings}}}).to_string(), ..fake_review(attempt, db::ReviewStatus::Failed) });
        ctx.repair_attempts.push(RepairAttempt {
            attempt,
            from_sha: Some(format!("c{}", attempt - 1)),
            to_sha: Some(format!("c{attempt}")),
            files: vec!["path".repeat(1000); 20],
            files_unavailable: false,
            summary: Some("summary".repeat(300)),
        });
    }
    let text = RepairBrief::review(&ctx).render();
    assert!(text.len() <= 24 * 1024, "{}", text.len());
    for expected in [
        "Requirement status:",
        "R1: unmet",
        "Pattern:",
        "F1: src/a.rs:1",
        "src/b.rs:2",
        "These share a cause.",
        "Open findings:",
        "Description:",
        "Original plan:",
    ] {
        assert!(text.contains(expected), "{expected} missing: {text}");
    }
    assert!(!text.contains("Reviewer report:"));
    assert!(!text.contains("Attempts:"));
}

#[test]
fn carried_repair_selection_is_fresh_and_noncarried_keeps_thread_reuse() {
    let mut ctx = fake_context(default_roles::CODER);
    let intent = DispatchIntent {
        builder_id: Some(BUILDER_ID_CODER_REVIEW_FIX_V2.into()),
        execution_policy: Some(EXECUTION_POLICY_RESUME_LATEST_TARGET_ROLE_THREAD.into()),
        prompt_config: json!({}),
    };
    let (_, selection) = build_effective_prompt(&ctx, Some(&intent), None);
    assert_eq!(
        selection.execution_policy,
        EXECUTION_POLICY_RESUME_LATEST_TARGET_ROLE_THREAD
    );
    ctx.carried_repair = true;
    let (_, selection) = build_effective_prompt(&ctx, Some(&intent), None);
    assert_eq!(selection.execution_policy, EXECUTION_POLICY_NEW_EXECUTION);
}

#[test]
fn repair_brief_cap_keeps_open_findings_with_long_attempts_and_a_large_pattern_family() {
    use super::super::repair_brief::{RepairAttempt, RepairBrief};
    let mut ctx = fake_context(default_roles::CODER);
    ctx.task.description = Some("d".repeat(10 * 1024));
    ctx.plan = Some("p".repeat(10 * 1024));
    // Six reviews of fifty findings each, every one sharing F1's cause.
    for attempt in 1..=6i64 {
        let findings: Vec<_> = (0..50)
            .map(|index| {
                json!({"summary":format!("stale apply {attempt}-{index}"), "kind":"regression",
                    "where":format!("src/site_{attempt}_{index}.rs:{index} {}", "w".repeat(900)),
                    "same_cause_as": if attempt == 1 { serde_json::Value::Null } else { json!("F1") }})
            })
            .collect();
        ctx.prior_reviews.push(db::Review {
            step_results_json: json!({"ci_steps":[], "conformance":{"status":"failed", "contract":convergence_contract(attempt), "checks":[],
                "assessment":{"result":"fail","reason":"problems","report":"r".repeat(30000),"findings":findings}}}).to_string(),
            ..fake_review(attempt, db::ReviewStatus::Failed)
        });
        ctx.repair_attempts.push(RepairAttempt {
            attempt,
            from_sha: Some(format!("c{}", attempt - 1)),
            to_sha: Some(format!("c{attempt}")),
            files: vec!["path".repeat(1000); 20],
            files_unavailable: false,
            summary: Some("summary".repeat(300)),
        });
    }
    let text = RepairBrief::review(&ctx).render();
    assert!(text.len() <= 24 * 1024, "{}", text.len());
    for expected in [
        "Pattern:",
        "These share a cause.",
        "F251: src/site_6_0.rs:0",
        "more sites (see the Review tab)",
        "Open findings:",
        // The latest round's new findings are what the coder must fix now.
        "F300",
    ] {
        assert!(text.contains(expected), "{expected} missing: {text}");
    }
}

#[test]
fn repair_brief_caps_requirement_status_before_it_can_crowd_open_findings() {
    let mut ctx = fake_context(default_roles::CODER);
    let mut contract = convergence_contract(1);
    contract["context"]["requirements"]=json!((0..5000).map(|i|json!({"id":format!("R{i}"),"text":"requirement","source":"task","universal":false,"allocated_task_id":null})).collect::<Vec<_>>());
    ctx.prior_reviews=vec![db::Review {step_results_json:json!({"ci_steps":[],"conformance":{"status":"failed","contract":contract,"checks":[],"assessment":{"result":"fail","reason":"broken","findings":[{"summary":"Save still broken","where":"form.rs:9","expected":"persists","observed":"lost","requirement_ids":["R4999"]}]}}}).to_string(),..fake_review(1,db::ReviewStatus::Failed)}];
    let text = super::super::repair_brief::RepairBrief::review(&ctx).render();
    assert!(text.len() <= 24 * 1024, "{}", text.len());
    assert!(text.contains("F1 [reviewer, blocking]"), "{text}");
    assert!(text.contains("Save still broken"), "{text}");
    assert!(text.contains("form.rs:9"));
    assert!(text.contains("persists"));
    assert!(
        text.contains("R4999: unmet"),
        "open requirements come first: {text}"
    );
    assert!(text.contains("+4960 more requirements"), "{text}");
    let requirements = text
        .split("Requirement status:")
        .nth(1)
        .unwrap()
        .split("Open findings:")
        .next()
        .unwrap();
    assert!(requirements.len() < 8 * 1024);
}

#[test]
fn repair_brief_requirement_status_yields_before_open_finding_text_is_clipped() {
    let mut ctx = fake_context(default_roles::CODER);
    let mut contract = convergence_contract(1);
    let requirement_ids = (0..40)
        .map(|i| format!("R{i}{}", "界".repeat(120)))
        .collect::<Vec<_>>();
    contract["context"]["requirements"]=json!(requirement_ids.iter().map(|id|json!({"id":id,"text":"requirement","source":"task","universal":false,"allocated_task_id":null})).collect::<Vec<_>>());
    let summary = "must retain this entire finding ".repeat(20);
    let findings=(0..10).map(|i|json!({"summary":format!("{i} {summary}"),"expected":"e".repeat(300),"observed":"o".repeat(300),"requirement_ids":[requirement_ids[i]]})).collect::<Vec<_>>();
    ctx.prior_reviews=vec![db::Review{step_results_json:json!({"conformance":{"status":"failed","contract":contract,"checks":[],"assessment":{"result":"fail","reason":"issues","findings":findings}}}).to_string(),..fake_review(1,db::ReviewStatus::Failed)}];
    let text = super::super::repair_brief::RepairBrief::review(&ctx).render();
    assert!(text.len() <= 24 * 1024);
    assert!(
        text.contains(&summary),
        "optional requirement status clipped open finding text: {text}"
    );
    assert!(text.contains("F10"));
}
