//! Governing review context, evidence, prompts, and assessment tests.

use super::*;
use serde_json::json;

pub(super) fn source() -> Value {
    let charter: ProjectCharterContent = serde_json::from_value(json!({
            "identity": {"working_name":"CSVPeek", "one_line_vision":"A tiny Rust CLI", "maturity":"mvp"},
            "problem_and_people":{"problem_or_opportunity":"Inspect CSV"},
            "core_experience":{"primary_outcome":"Inspect CSV"}, "scope":{"required_deliverables":["One Rust crate with a CLI and reusable parsing boundaries"]},
            "success":{}, "constraints_and_risks":{"technology":["Rust"]}, "knowledge_ledger":{}
        })).unwrap();
    json!({"project_id":"project", "task_id":"task", "repo_id":"repo", "charter_status":"charter_backed", "charter_setup_required":0,
            "charter":{"id":"charter-r1","approved_id":"charter-r1","content_digest":canonical_digest(&charter).unwrap(),"content":charter},
            "task_charter_revision_id":"charter-r1", "task_scope":{"title":"Parser", "description":"Implement Rust parsing", "config":{}, "allocations":{}},
            "documents":[], "workflow":{}, "project_settings":{}})
}

#[tokio::test]
async fn project_environment_fails_closed_without_task_authority() {
    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    db::run_migrations(&pool).await.expect("migrations run");
    let db = SqliteDb::new(pool);

    let error = project_environment(&db, "missing-task")
        .await
        .expect_err("missing authority must stop review evidence");
    assert!(error.contains("was not found"), "{error}");
}

#[test]
fn task_scope_defers_unassigned_project_outcomes_but_keeps_typed_universal_invariants() {
    let mut s = source();
    s["charter"]["content"]["scope"]["explicit_non_goals"] = json!(["Do not publish user data"]);
    let charter: ProjectCharterContent =
        serde_json::from_value(s["charter"]["content"].clone()).unwrap();
    s["charter"]["content_digest"] = json!(canonical_digest(&charter).unwrap());
    let context = context_from_source(&s).unwrap();
    assert!(!context
        .requirements
        .iter()
        .any(|r| r.source == "/scope/required_deliverables/0"));
    assert!(context
        .requirements
        .iter()
        .any(|r| r.universal && r.text == "Do not publish user data"));
    assert!(!context
        .requirements
        .iter()
        .any(|r| r.universal && r.text == "Rust"));
    assert!(context.deferred_requirement_count >= 1);
    assert!(context.deferred_requirements_digest.is_some());
    assert!(governing_prompt(&context).contains("Rust crate"));
}

#[test]
fn task_scope_can_own_an_exact_nonuniversal_requirement() {
    let mut s = source();
    let id = "charter-r1:/scope/required_deliverables/0";
    s["task_scope"]["config"] = json!({"review":{"requirement_ids":[id]}});
    let context = context_from_source(&s).unwrap();
    assert!(context.requirements.iter().any(|r| r.id == id));
    assert!(!context
        .requirements
        .iter()
        .any(|r| r.source == "/identity/one_line_vision"));

    s["task_scope"]["config"]["review"]["requirement_ids"] = json!(["unknown"]);
    assert!(context_from_source(&s)
        .unwrap_err()
        .contains("unknown governing requirement"));
}

#[test]
fn task_proposal_requirement_ids_are_checked_against_the_current_charter() {
    let s = source();
    let charter: ProjectCharterContent =
        serde_json::from_value(s["charter"]["content"].clone()).unwrap();
    assert!(validate_explicit_task_requirement_ids(
        "charter-r1",
        &charter,
        &["charter-r1:/scope/required_deliverables/0".into()]
    )
    .is_ok());
    assert!(validate_explicit_task_requirement_ids(
        "charter-r1",
        &charter,
        &["charter-r1:/constraints_and_risks/technology/0".into()]
    )
    .is_ok());
    assert!(validate_explicit_task_requirement_ids(
        "charter-r1",
        &charter,
        &["charter-r1:/scope/required_deliverables/99".into()]
    )
    .unwrap_err()
    .contains("unknown Charter review requirement ID"));
}

#[test]
fn missing_stale_tampered_and_oversized_context_fail_closed() {
    for mutated in [
        {
            let mut s = source();
            s["charter"] = Value::Null;
            s
        },
        {
            let mut s = source();
            s["task_charter_revision_id"] = json!("old");
            s
        },
        {
            let mut s = source();
            s["charter"]["content"]["identity"]["one_line_vision"] = json!("Go");
            s
        },
        {
            let mut s = source();
            s["task_scope"]["description"] = json!("x".repeat(MAX_CONTEXT_BYTES));
            s
        },
    ] {
        assert!(context_from_source(&mutated).is_err());
    }
}
#[test]
fn the_result_block_ends_a_markdown_review() {
    let parsed = parse_assessment(
        "## Review\n\n- `task:acceptance` met in `src/lib.rs:1`.\n\n\
             {\"result\": \"pass\", \"reason\": \"All requirements met\"}",
    )
    .unwrap();
    assert_eq!(parsed.result, ReviewResult::Pass);
    assert_eq!(parsed.reason, "All requirements met");
    assert_eq!(
        parsed.report,
        "## Review\n\n- `task:acceptance` met in `src/lib.rs:1`."
    );
    // A fence around nothing but the block is part of the block.
    let fenced = parse_assessment(
        "Notes.\n\n```json\n{\"result\": \"fail\", \"reason\": \"No persistence\"}\n```\n",
    )
    .unwrap();
    assert_eq!(fenced.result, ReviewResult::Fail);
    assert_eq!(fenced.report, "Notes.");
}

#[test]
fn the_result_block_is_read_leniently() {
    // Live reviewers added `taxonomy` and `classification` keys to say an
    // environment was at fault; the strict schema threw each review away.
    let parsed = parse_assessment(
        "{\"result\": \"BLOCKED\", \"classification\": \"environment\", \
             \"reason\": \"tsc not found\"}",
    )
    .unwrap();
    assert_eq!(parsed.result, ReviewResult::Blocked);
    assert_eq!(parsed.reason, "tsc not found");
    let verdict = parse_assessment("{\"verdict\": \"passed\"}").unwrap();
    assert_eq!(verdict.result, ReviewResult::Pass);
    assert_eq!(verdict.reason, "");
    assert_eq!(verdict.fixable_by, FixableBy::Coder);
    assert!(!verdict.repeat);
}

#[test]
fn findings_parser_anchoring_fields_are_lenient_and_omit_empty_values() {
    let parsed = parse_assessment(&json!({"result":"fail","findings":[
            {"summary":"valid", "requirement_ids":[" R2 ",4,null,""], "kind":" MISSED ", "same_cause_as":" F1 "},
            {"summary":"malformed", "requirement_ids":{}, "kind":"new_kind", "same_cause_as":4},
            {"summary":"regressed", "kind":"regression"},
            {"summary":"later work", "kind":"out_of_scope"}
        ]}).to_string()).unwrap();
    assert_eq!(parsed.findings[0].requirement_ids, ["R2"]);
    assert_eq!(
        parsed.findings[0].kind,
        Some(api_types::ReviewFindingKind::Missed)
    );
    assert_eq!(parsed.findings[0].same_cause_as.as_deref(), Some("F1"));
    let malformed = serde_json::to_value(&parsed.findings[1]).unwrap();
    for field in ["requirement_ids", "kind", "same_cause_as"] {
        assert!(malformed.get(field).is_none(), "{malformed}");
    }
    assert_eq!(
        parsed.findings[2].kind,
        Some(api_types::ReviewFindingKind::Regression)
    );
    assert_eq!(
        parsed.findings[3].kind,
        Some(api_types::ReviewFindingKind::OutOfScope)
    );
}

#[test]
fn findings_parser_reads_findings_and_resolutions() {
    let parsed = parse_assessment(r#"Report.
{"result":"fail","reason":"empty input crashes","findings":[{"summary":"Empty input crashes parse()","where":"src/parse.rs:42","expected":"error message","observed":"panic"},{"id":"F2","summary":"typo","severity":" MINOR "}],"resolved":["F1"]}"#).unwrap();
    assert_eq!(parsed.result, ReviewResult::Fail);
    assert_eq!(parsed.report, "Report.");
    assert_eq!(parsed.findings.len(), 2);
    assert_eq!(parsed.findings[0].severity, ReviewFindingSeverity::Blocking);
    assert_eq!(
        parsed.findings[0].r#where.as_deref(),
        Some("src/parse.rs:42")
    );
    assert_eq!(
        parsed.findings[0].expected.as_deref(),
        Some("error message")
    );
    assert_eq!(parsed.findings[0].observed.as_deref(), Some("panic"));
    assert_eq!(parsed.findings[1].severity, ReviewFindingSeverity::Minor);
    assert_eq!(parsed.findings[1].id.as_deref(), Some("F2"));
    assert_eq!(parsed.resolved, ["F1"]);
}

#[test]
fn findings_parser_ignores_malformed_lists_and_items() {
    for malformed in [json!(null), json!("see above"), json!({}), json!(7)] {
        let parsed = parse_assessment(
            &json!({"result":"fail","reason":"x","findings":malformed,"resolved":malformed})
                .to_string(),
        )
        .unwrap();
        assert_eq!(parsed.result, ReviewResult::Fail);
        assert_eq!(parsed.reason, "x");
        assert!(parsed.findings.is_empty());
        assert!(parsed.resolved.is_empty());
    }
    let parsed = parse_assessment(&json!({"result":"pass","findings":[null,4,"x",{}, {"summary":"  "}, {"summary":4}, {"summary":"good","severity":{}}, {"summary":"other","severity":"unknown","where":4}],"resolved":[null,{},8," ","F1"]}).to_string()).unwrap();
    assert_eq!(parsed.findings.len(), 2);
    assert!(parsed
        .findings
        .iter()
        .all(|finding| finding.severity == ReviewFindingSeverity::Blocking));
    assert!(parsed.findings[1].r#where.is_none());
    assert_eq!(parsed.resolved, ["F1"]);
}

#[test]
fn findings_parser_nested_result_keys_cannot_override_the_verdict() {
    let parsed = parse_assessment(r#"{"result":"fail","reason":"outer","findings":[{"summary":"bug","result":"pass"}],"resolved":[{"result":"blocked"}]}"#).unwrap();
    assert_eq!(parsed.result, ReviewResult::Fail);
    assert_eq!(parsed.reason, "outer");
    assert_eq!(parsed.findings.len(), 1);
    assert!(parsed.resolved.is_empty());
    assert!(parsed.report.is_empty());
}

#[test]
fn findings_parser_broken_outer_block_cannot_supply_a_nested_verdict() {
    for key in ["result", "verdict"] {
        for broken in [
                format!(r#"{{"result":"fail","findings":[{{"summary":"bug","{key}":"pass"}}],}}"#),
                format!(r#"{{"result":"fail","findings":[{{"summary":"bug","{key}":"pass"}}]"#),
                format!(r#"{{"result":fail,"findings":[{{"summary":"bug","{key}":"pass"}}]}}"#),
                format!(r#"{{result:"fail","findings":[{{"summary":"bug","{key}":"pass"}}]}}"#),
                format!(r#"{{'result':'fail','findings':[{{"summary":"bug","{key}":"pass"}}]}}"#),
                format!(
                    r#"{{/* comment */"result":"fail","findings":[{{"summary":"bug","{key}":"pass"}}]}}"#
                ),
                format!(
                    r#"{{not-json,"result":"fail","findings":[{{"summary":"bug","{key}":"pass"}}]}}"#
                ),
                format!(
                    r#"{{"result":"fail","findings":[{{"summary":"a"}}, {{"summary":"bug","{key}":"pass"}}],}}"#
                ),
                format!(
                    "{{\n  \"result\": \"fail\",\n  \"findings\": [\n    {{\"summary\": \"bug\", \"{key}\": \"pass\"}}\n  ],\n}}"
                ),
            ] {
                assert!(parse_assessment(&broken).is_err(), "{broken}");
                let corrected =
                    format!("{broken}\n\n{{\"result\":\"fail\",\"reason\":\"corrected\"}}");
                assert_eq!(parse_assessment(&corrected).unwrap().reason, "corrected");
            }
    }
}

/// Replies the parser read before the nested-verdict guard must still
/// read the same: code with apostrophes or lifetimes, unclosed excerpts,
/// wrapped verdicts, and correction turns after any kind of cut-off.
#[test]
fn findings_parser_reads_code_quotes_and_corrections_after_a_cut_off() {
    let fence = "```";
    let correction = "\n\n{\"result\":\"fail\",\"reason\":\"corrected\"}";
    let cases = [
            format!("{fence}rust\npub struct Parser<'a> {{\n    input: &'a str,\n}}\n{fence}\n\n{{\"result\":\"fail\",\"reason\":\"lifetime\"}}"),
            format!("{fence}ts\nconst config = {{\n  retries: 0, // don't retry\n}};\n{fence}\n\n{{\"result\":\"fail\",\"reason\":\"comment\"}}"),
            format!("{fence}js\nswitch (kind) {{\n  case \"a\": return 1; // isn't reached\n}}\n{fence}\n{{\"result\":\"fail\",\"reason\":\"switch\"}}"),
            format!("{fence}js\nconst routes = {{\n  home: '/',\n{fence}\n{{\"result\":\"fail\",\"reason\":\"excerpt\"}}"),
            "`struct S<'a> { s: &'a str }` borrows.\n{\"result\":\"fail\",\"reason\":\"inline\"}".to_owned(),
            "The type `{ id: string, name: user's name }` is off.\n{\"result\":\"fail\",\"reason\":\"type\"}".to_owned(),
            "Partial: {name: \"x\", age: 5\n{\"result\":\"fail\",\"reason\":\"unclosed\"}".to_owned(),
            "{\"review\":{\"result\":\"fail\",\"reason\":\"wrapped\"}}".to_owned(),
            format!("Review.\n\n{{\"result\":\"fail\",\"reason\":\"the parser crashes on emp{correction}"),
            format!("Review.\n\n{{\"result\":\"fail\",\"findings\":[{{\"summary\":\"one\"}},{correction}"),
            format!("Review.\n\n{{\"result\":\"fail\",\"findings\":[{{\"summary\":\"one\",\"where\":\"a.rs\"{correction}"),
            format!("Review.\n\n{{\"result\":\"fail\",\"findings\":[{{\"summary\":\"one\"}}{correction}"),
        ];
    let expected = [
        "lifetime",
        "comment",
        "switch",
        "excerpt",
        "inline",
        "type",
        "unclosed",
        "wrapped",
        "corrected",
        "corrected",
        "corrected",
        "corrected",
    ];
    for (reply, reason) in cases.iter().zip(expected) {
        let parsed = parse_assessment(reply).unwrap_or_else(|error| panic!("{reply}: {error}"));
        assert_eq!(parsed.result, ReviewResult::Fail, "{reply}");
        assert_eq!(parsed.reason, reason, "{reply}");
    }
}

#[test]
fn findings_parser_keeps_legacy_serialization() {
    let parsed = parse_assessment(r#"{"result":"fail","reason":"legacy"}"#).unwrap();
    assert!(parsed.findings.is_empty());
    assert!(parsed.resolved.is_empty());
    // The v0.13.12 shape: defaults are omitted, so a frozen row from that
    // writer and a new default verdict serialize the same way.
    assert_eq!(
        serde_json::to_string(&parsed).unwrap(),
        r#"{"result":"fail","reason":"legacy","report":""}"#
    );
    let routed =
        parse_assessment(r#"{"result":"fail","reason":"x","fixable_by":"owner","repeat":true}"#)
            .unwrap();
    assert_eq!(
        serde_json::to_string(&routed).unwrap(),
        r#"{"result":"fail","reason":"x","fixable_by":"owner","repeat":true,"report":""}"#
    );
}

#[test]
fn findings_parser_caps_valid_items_and_unicode_text() {
    let long = "界".repeat(1_100);
    let parsed = parse_assessment(&json!({"result":"fail","findings":[{"summary":long,"id":long,"where":long,"expected":long,"observed":long}],"resolved":[long]}).to_string()).unwrap();
    let finding = &parsed.findings[0];
    for text in [
        Some(finding.summary.as_str()),
        finding.id.as_deref(),
        finding.r#where.as_deref(),
        finding.expected.as_deref(),
        finding.observed.as_deref(),
        Some(parsed.resolved[0].as_str()),
    ] {
        assert_eq!(text.unwrap().chars().count(), 1_000);
    }
    let mut findings = vec![json!({"summary":""}); 10];
    findings.extend((0..60).map(|n| json!({"summary":format!("finding {n}")})));
    let parsed = parse_assessment(&json!({"result":"fail","findings":findings,"resolved":(0..60).map(|n|format!("F{n}")).collect::<Vec<_>>()} ).to_string()).unwrap();
    assert_eq!(parsed.findings.len(), 50);
    assert_eq!(parsed.findings.last().unwrap().summary, "finding 49");
    assert_eq!(parsed.resolved.len(), 50);
}

#[test]
fn finding_routing_fields_are_read_leniently_from_the_last_result() {
    let owner = parse_assessment(
        r#"{"result":"fail","reason":"Forge linked_documents is empty","fixable_by":"OWNER"}"#,
    )
    .unwrap();
    assert_eq!(owner.fixable_by, FixableBy::Owner);
    assert!(!owner.repeat);
    for repeat in [json!(true), json!("true"), json!(" TRUE ")] {
        let parsed = parse_assessment(
            &json!({
                "result": "failed", "fixable_by": "CoDeR", "repeat": repeat,
            })
            .to_string(),
        )
        .unwrap();
        assert_eq!(parsed.fixable_by, FixableBy::Coder);
        assert!(parsed.repeat);
    }
    for unknown in [json!(null), json!(7), json!("unknown"), json!({})] {
        let parsed = parse_assessment(
            &json!({
                "result": "fail", "fixable_by": unknown, "repeat": unknown,
            })
            .to_string(),
        )
        .unwrap();
        assert_eq!(parsed.fixable_by, FixableBy::Coder);
        assert!(!parsed.repeat);
    }
    for repeat in [json!(false), json!("false"), json!("FALSE")] {
        assert!(
            !parse_assessment(&json!({"result":"fail", "repeat":repeat}).to_string())
                .unwrap()
                .repeat
        );
    }
    let last = parse_assessment(
        "{\"result\":\"fail\",\"fixable_by\":\"owner\",\"repeat\":true}\n\
             {\"result\":\"fail\",\"reason\":\"last result\"}",
    )
    .unwrap();
    assert_eq!(last.reason, "last result");
    assert_eq!(last.fixable_by, FixableBy::Coder);
    assert!(!last.repeat);
}

#[test]
fn the_last_result_block_wins_over_quoted_json_and_code() {
    let parsed = parse_assessment(
        "The config reads `{\"result\": \"pass\"}` from disk, and `fn main() {` is \
             never closed.\n\n{\"result\": \"fail\", \"reason\": \"Unclosed block\"}",
    )
    .unwrap();
    assert_eq!(parsed.result, ReviewResult::Fail);
    // A reviewer that corrects itself later in the same reply is read by
    // its final word.
    let corrected = parse_assessment(
        "{\"result\": \"pass\", \"reason\": \"draft\"}\nOn reflection:\n\
             {\"result\": \"fail\", \"reason\": \"final\"}",
    )
    .unwrap();
    assert_eq!(corrected.reason, "final");
}

#[test]
fn a_reply_without_a_result_block_is_unusable() {
    for message in [
        "===REVIEW: PASS===",
        "Looks good to me.",
        "{\"result\": \"maybe\", \"reason\": \"unsure\"}",
        "{\"reason\": \"no result\"}",
    ] {
        assert!(parse_assessment(message).is_err(), "{message}");
    }
    assert!(parse_assessment(&"x".repeat(MAX_REPORT_BYTES + 1)).is_err());
}

#[test]
fn allocations_shrink_task_scope_and_never_waive_typed_universal_invariants() {
    let mut s = source();
    let id = "charter-r1:/scope/required_deliverables/0";
    s["task_scope"]["allocations"][id] = json!({"task_id":"cli-task"});
    let context = context_from_source(&s).unwrap();
    assert!(!context.requirements.iter().any(|r| r.id == id));

    s["task_scope"]["allocations"][id] = json!({"task_id":"task"});
    let context = context_from_source(&s).unwrap();
    assert!(context.requirements.iter().any(|r| r.id == id));

    s["charter"]["content"]["scope"]["explicit_non_goals"] = json!(["Do not publish user data"]);
    let charter: ProjectCharterContent =
        serde_json::from_value(s["charter"]["content"].clone()).unwrap();
    s["charter"]["content_digest"] = json!(canonical_digest(&charter).unwrap());
    let universal = "charter-r1:/scope/explicit_non_goals/0";
    s["task_scope"]["allocations"][universal] = json!({"task_id":"cli-task"});
    assert!(context_from_source(&s)
        .unwrap_err()
        .contains("cannot be allocated away"));
}
#[test]
fn configured_checks_are_merged_and_bound_to_requirements() {
    let mut s = source();
    s["project_settings"] = json!({"default_review_config":{
        "setup_steps":["cargo fetch"],
        "ci_steps":["cargo test"]
    }});
    s["task_scope"]["config"] = json!({"review":{"conformance_checks":[{"id":"rust","command":"cargo metadata --no-deps --format-version 1","requirement_ids":["charter-r1:/scope/required_deliverables/0"]}]}});
    let c = context_from_source(&s).unwrap();
    assert_eq!(c.required_checks.len(), 2);
    assert_eq!(c.setup_steps, vec!["cargo fetch"]);
    assert_eq!(c.required_checks[0].command, "cargo test");
    assert!(c
        .requirements
        .iter()
        .any(|requirement| requirement.id == "charter-r1:/scope/required_deliverables/0"));
    s["task_scope"]["config"]["review"]["conformance_checks"][0]["requirement_ids"] =
        json!(["unknown"]);
    assert!(context_from_source(&s).is_err());
}

#[test]
fn malformed_ci_configuration_cannot_remove_required_checks() {
    let mut default_source = source();
    default_source["project_settings"] = json!({
        "default_review_config": {"ci_steps": "not-an-array"}
    });
    assert!(context_from_source(&default_source)
        .expect_err("malformed default CI config must fail closed")
        .contains("ci_steps"));

    let mut source = source();
    source["task_scope"]["config"] = json!({
        "review": {"ci_steps": ["cargo test", 7]}
    });
    assert!(context_from_source(&source)
        .expect_err("malformed Task CI config must fail closed")
        .contains("CI"));
}

#[test]
fn read_only_tasks_drop_implementation_ci_but_keep_explicit_conformance_checks() {
    let mut s = source();
    s["task_scope"]["task_type"] = json!("discovery");
    s["project_settings"] = json!({"default_review_config":{
        "setup_steps":["cargo fetch"],
        "ci_steps":["cargo test"],
        "conformance_checks":[{
            "id":"research-shape",
            "command":"test -f findings.md",
            "requirement_ids":["task:acceptance"]
        }]
    }});

    let context = context_from_source(&s).expect("discovery contract");
    assert!(context.setup_steps.is_empty());
    assert_eq!(context.required_checks.len(), 1);
    assert_eq!(context.required_checks[0].id, "research-shape");

    s["task_scope"]["task_type"] = json!("task");
    s["task_scope"]["capability_class"] = json!("repository_read");
    let context = context_from_source(&s).expect("read-only capability contract");
    assert_eq!(context.required_checks.len(), 1);
    assert_eq!(context.required_checks[0].id, "research-shape");
}

#[test]
fn reviewer_prompt_contains_one_frozen_context_even_for_large_charters() {
    let mut s = source();
    s["charter"]["content"]["scope"]["required_deliverables"] = json!((0..106)
        .map(|index| format!(
            "Project deliverable {index}: preserve exact behavior across the integrated product"
        ))
        .collect::<Vec<_>>());
    let charter: ProjectCharterContent =
        serde_json::from_value(s["charter"]["content"].clone()).unwrap();
    s["charter"]["content_digest"] = json!(canonical_digest(&charter).unwrap());
    s["task_scope"]["config"] = json!({
        "review": {
            "requirement_ids": ["charter-r1:/scope/required_deliverables/0"]
        }
    });
    let context = context_from_source(&s).unwrap();
    assert_eq!(
        context
            .requirements
            .iter()
            .filter(|requirement| requirement
                .source
                .starts_with("/scope/required_deliverables/"))
            .count(),
        1,
        "a Task review owns only the selected Project deliverable"
    );
    assert!(context.deferred_requirement_count >= 105);
    let c = ReviewContract {
        execution_id: "execution".into(),
        policy: REVIEW_CONFORMANCE_POLICY.into(),
        commit_sha: "abc".into(),
        base_sha: "base".into(),
        candidate_changed_paths: Vec::new(),
        context,
        check_results: Vec::new(),
        digest: "contract-digest".into(),
    };
    let serialized = serde_json::to_string(&c).unwrap();
    let prompt = contract_prompt(&c);
    assert_eq!(prompt.matches(&c.context.source_digest).count(), 1);
    assert!(!prompt.contains("Forge governing context"));
    assert!(prompt.contains("fixable_by is \"coder\" (default) or \"owner\""));
    assert!(prompt.contains("repeat defaults to false"));
    assert!(prompt.contains("Owner example:") && prompt.contains("Repeat example:"));
    let requirement_ids_bytes: usize = c
        .context
        .requirements
        .iter()
        .map(|requirement| requirement.id.len() + 2)
        .sum();
    assert!(
        prompt.len() <= serialized.len() + RESPONSE_INSTRUCTION.len() + requirement_ids_bytes + 96
    );
    assert!(prompt.len() <= MAX_PREPARED_PROMPT_BYTES);
}

#[tokio::test]
async fn candidate_path_manifest_tracks_only_the_admitted_delta() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();
    git::init(path).await.unwrap();
    tokio::fs::write(path.join("existing.txt"), "before\n")
        .await
        .unwrap();
    let base = git::commit_all(path, "base").await.unwrap();

    assert!(candidate_changed_paths(path, &base, &base)
        .await
        .unwrap()
        .is_empty());

    tokio::fs::write(path.join("existing.txt"), "after\n")
        .await
        .unwrap();
    tokio::fs::write(path.join("added.txt"), "new\n")
        .await
        .unwrap();
    let candidate = git::commit_all(path, "candidate").await.unwrap();

    assert_eq!(
        candidate_changed_paths(path, &base, &candidate)
            .await
            .unwrap(),
        vec!["added.txt".to_owned(), "existing.txt".to_owned()]
    );
}

#[tokio::test]
async fn review_base_resolves_the_fork_point_not_the_target_branch_tip() {
    // Reproduces finding F2: once the target branch moves past the point
    // the task's worktree forked from, every file it picked up in the
    // meantime must not show up in the reviewed diff as a phantom
    // deletion the worker never made.
    let origin = tempfile::tempdir().unwrap();
    let repo_path = origin.path();
    git::init(repo_path).await.unwrap();
    tokio::fs::write(repo_path.join("README.md"), "root\n")
        .await
        .unwrap();
    let fork_point = git::commit_all(repo_path, "initial commit").await.unwrap();

    let worktree_dir = tempfile::tempdir().unwrap();
    let worktree_path = worktree_dir.path().join("task-worktree");
    git::create_worktree(repo_path, "task/branch", &worktree_path)
        .await
        .unwrap();

    // The worker does their change on the task branch, forked at `fork_point`.
    tokio::fs::write(worktree_path.join("feature.txt"), "worker change\n")
        .await
        .unwrap();
    git::commit_all(&worktree_path, "worker change")
        .await
        .unwrap();

    // Meanwhile something else merges into main, moving its tip past the
    // point the task branch forked from.
    tokio::fs::write(repo_path.join("unrelated.txt"), "later main progress\n")
        .await
        .unwrap();
    let main_tip = git::commit_all(repo_path, "unrelated main progress")
        .await
        .unwrap();
    assert_ne!(main_tip, fork_point);

    let mut s = source();
    s["task_scope"]["default_branch"] = json!("main");
    let context = context_from_source(&s).unwrap();

    let base = review_base(&worktree_path, &context).await.unwrap();
    assert_eq!(
        base, fork_point,
        "review base must be the branch's true fork point, not the target branch's current tip"
    );
    assert_ne!(base, main_tip);
}
