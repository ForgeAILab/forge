use serde_json::json;

use crate::workflow::{
    default_roles, default_states,
    dispatch::{
        apply_prompt_overrides, build_effective_prompt, effective_prompt_selection,
        generic_prompt::GenericPromptBuilder, planner_prompt::PlannerPromptBuilder,
        resolve_prompt_builder, reviewer_prompt::ReviewerPromptBuilder, AgentDispatchContext,
        DispatchIntent, PromptBuilder, BUILDER_ID_CODER_IMPLEMENTATION_V2,
        BUILDER_ID_CODER_MERGE_FIX_V2, BUILDER_ID_CODER_REVIEW_FIX_V2,
        BUILDER_ID_GENERIC_DEFAULT_V2, BUILDER_ID_PLANNER_DEFAULT_V2,
        BUILDER_ID_REVIEWER_DEFAULT_V2, BUILDER_ID_WORKER_AUTONOMOUS_V1,
        BUILDER_ID_WORKER_MERGE_FIX_V1, BUILDER_ID_WORKER_REVIEW_FIX_V1,
    },
};

fn fake_task(id: &str, title: &str, description: Option<&str>) -> db::Task {
    db::Task {
        id: id.to_string(),
        project_id: "project-1".to_string(),
        repo_id: Some("repo-1".to_string()),
        parent_task_id: None,
        subtask_order: None,
        assignee_type: None,
        assignee_id: None,
        title: title.to_string(),
        description: description.map(str::to_string),
        task_type: "task".to_string(),
        status: default_states::IN_PROGRESS.to_string(),
        is_automation: false,
        priority: 0,
        board_position: 1.0,
        task_state_config: None,
        merge_config: None,
        metadata_json: None,
        plan: None,
        error_annotation: None,
        blocked_json: None,
        failed_json: None,
        entry_barrier_json: None,
        review_passed_at: None,
        archived_at: None,
        deleted_at: None,
        version: 1,
        created_at: "2026-04-17T00:00:00Z".to_string(),
        updated_at: "2026-04-17T00:00:00Z".to_string(),
    }
}

fn fake_review(attempt_number: i64, status: db::ReviewStatus) -> db::Review {
    db::Review {
        id: format!("review-{attempt_number}"),
        task_id: "task-1".to_string(),
        execution_id: format!("execution-{attempt_number}"),
        attempt_number,
        status,
        step_results_json: "{}".to_string(),
        started_at: "2026-04-17T00:00:00Z".to_string(),
        finished_at: Some("2026-04-17T00:01:00Z".to_string()),
        created_at: "2026-04-17T00:00:00Z".to_string(),
        updated_at: "2026-04-17T00:01:00Z".to_string(),
    }
}

fn fake_context(role: &str) -> AgentDispatchContext {
    AgentDispatchContext {
        task: fake_task(
            "task-1",
            "Add dispatch context",
            Some("Build prompt context for workflow agents."),
        ),
        role: role.to_string(),
        state_name: default_states::IN_PROGRESS.to_string(),
        state_config: json!({}),
        transition_log: Vec::new(),
        comments: Vec::new(),
        plan: Some("1. Load context\n2. Build prompt".to_string()),
        prior_reviews: Vec::new(),
        parent_task: None,
        sub_tasks: Vec::new(),
        last_manual_bounce_reason: None,
        continuation_of_execution_id: None,
        continuation_logs_path: None,
        latest_review_feedback: None,
        latest_review_execution_id: None,
        latest_review_logs_path: None,
    }
}

const FAILURE_TAXONOMY_LINE: &str = "Failure taxonomy: classify any blocker using exactly this taxonomy: transient | input_missing | environment | code_bug | design_gap | review_failed | systemic.";

#[test]
fn default_prompt_builders_include_managed_contract_and_role_boundaries() {
    let cases = vec![
        (
            BUILDER_ID_CODER_IMPLEMENTATION_V2,
            default_roles::CODER,
            vec![
                "Coder boundary:",
                "Must implement only the requested task in the task worktree.",
                "Must not change unrelated behavior",
            ],
        ),
        (
            BUILDER_ID_CODER_REVIEW_FIX_V2,
            default_roles::CODER,
            vec![
                "Review-fix boundary:",
                "Must address prior review or CI feedback precisely",
                "Must not reopen solved work or add unrelated changes.",
            ],
        ),
        (
            BUILDER_ID_CODER_MERGE_FIX_V2,
            default_roles::CODER,
            vec![
                "Merge-fix boundary:",
                "Must resolve merge conflicts minimally while preserving implementation intent",
                "Must not rewrite the feature or add unrelated cleanup.",
            ],
        ),
        (
            BUILDER_ID_REVIEWER_DEFAULT_V2,
            default_roles::REVIEWER,
            vec![
                "Reviewer boundary:",
                "Must remain read-only",
                "Must not edit files, stage changes, commit changes",
            ],
        ),
        (
            BUILDER_ID_PLANNER_DEFAULT_V2,
            default_roles::PLANNER,
            vec![
                "Planner boundary:",
                "Must investigate enough to produce an executable plan",
                "Must not modify code or mark implementation items done",
            ],
        ),
        (
            BUILDER_ID_WORKER_AUTONOMOUS_V1,
            default_roles::WORKER,
            vec!["Worker boundary:", "plan internally", "self-validation"],
        ),
        (
            BUILDER_ID_WORKER_REVIEW_FIX_V1,
            default_roles::WORKER,
            vec!["Worker boundary:", "plan internally", "self-validation"],
        ),
        (
            BUILDER_ID_WORKER_MERGE_FIX_V1,
            default_roles::WORKER,
            vec!["Worker boundary:", "plan internally", "self-validation"],
        ),
        (
            BUILDER_ID_GENERIC_DEFAULT_V2,
            "security_engineer",
            vec![
                "Generic boundary:",
                "Must follow the assigned role from the dispatch context",
                "Must not modify code unless the assigned role explicitly requires implementation work.",
            ],
        ),
    ];

    for (builder_id, role, role_lines) in cases {
        let prompt = resolve_prompt_builder(builder_id).build(&fake_context(role));

        assert!(
            prompt.system.contains(FAILURE_TAXONOMY_LINE),
            "{builder_id} missing failure taxonomy"
        );
        assert!(
            prompt.system.contains(
                "Before acting, restate objective, constraints, and acceptance criteria."
            ),
            "{builder_id} missing restatement rule"
        );
        assert!(
            prompt
                .system
                .contains("Never hide failed verification; report failures explicitly."),
            "{builder_id} missing failed verification rule"
        );
        for line in role_lines {
            assert!(
                prompt.system.contains(line),
                "{builder_id} missing role line: {line}"
            );
        }
    }
}

#[test]
fn worker_prompt_builders_cover_autonomous_handoff_and_use_coder_tools() {
    let worker_tools = resolve_prompt_builder(BUILDER_ID_WORKER_AUTONOMOUS_V1)
        .build(&fake_context(default_roles::WORKER))
        .tools;
    let coder_tools = resolve_prompt_builder(BUILDER_ID_CODER_IMPLEMENTATION_V2)
        .build(&fake_context(default_roles::CODER))
        .tools;
    assert_eq!(worker_tools, coder_tools);

    let autonomous = resolve_prompt_builder(BUILDER_ID_WORKER_AUTONOMOUS_V1)
        .build(&fake_context(default_roles::WORKER));
    assert!(autonomous.system.contains("plan internally"));
    assert!(autonomous.system.contains("repair failures"));
    assert!(autonomous.system.contains("STRUCTURED"));
    assert!(autonomous.system.contains("Uncertainty | Scope Changes"));
    assert!(autonomous.user.contains("Objective:"));

    let mut review_ctx = fake_context(default_roles::WORKER);
    review_ctx.continuation_of_execution_id = Some("worker-execution-1".to_owned());
    review_ctx.latest_review_feedback = Some("Add evidence for the edge case".to_owned());
    let review_fix = resolve_prompt_builder(BUILDER_ID_WORKER_REVIEW_FIX_V1).build(&review_ctx);
    assert!(review_fix.user.contains("review and validation findings"));
    assert!(review_fix.user.contains("Add evidence for the edge case"));
    assert!(review_fix.user.contains("worker-execution-1"));

    let merge_fix = resolve_prompt_builder(BUILDER_ID_WORKER_MERGE_FIX_V1)
        .build(&fake_context(default_roles::WORKER));
    assert!(merge_fix.user.contains("Merge repair is required"));
}

/// The coder reports through the worklog the reviewer reads, and proves
/// behaviour with a captured artifact rather than prose. Forge posts the final
/// completion summary itself, so the prompt must not ask for a handoff block.
#[test]
fn coder_family_prompts_require_a_worklog_and_captured_proof() {
    for builder_id in [
        BUILDER_ID_CODER_IMPLEMENTATION_V2,
        BUILDER_ID_CODER_REVIEW_FIX_V2,
        BUILDER_ID_CODER_MERGE_FIX_V2,
    ] {
        let prompt = resolve_prompt_builder(builder_id).build(&fake_context(default_roles::CODER));

        assert!(
            prompt.system.contains("`task.worklog`"),
            "{builder_id} missing the worklog contract"
        );
        assert!(
            prompt
                .system
                .contains("not for every tool call or file edit"),
            "{builder_id} missing the worklog cadence rule"
        );
        assert!(
            prompt.system.contains("`task.evidence`"),
            "{builder_id} missing the captured-proof rule"
        );
        assert!(
            prompt
                .system
                .contains("Proof of behaviour is a captured artifact, not prose"),
            "{builder_id} missing the proof-is-an-artifact rule"
        );
        assert!(
            !prompt
                .system
                .contains("Summary | Deliverables | Verification | Deviations | Next Step"),
            "{builder_id} still asks for a handoff block Forge now writes itself"
        );
    }
}

#[test]
fn reviewer_prompt_requires_structured_findings_and_existing_verdict_marker() {
    let prompt = resolve_prompt_builder(BUILDER_ID_REVIEWER_DEFAULT_V2)
        .build(&fake_context(default_roles::REVIEWER));

    assert!(prompt
        .system
        .contains("Reviewer findings: Put structured findings before the verdict."));
    assert!(prompt
        .system
        .contains("Each BLOCKING finding must include evidence"));
    assert!(prompt.system.contains("expected vs actual behavior"));
    assert!(prompt
        .system
        .contains("Separate NON-BLOCKING findings from BLOCKING findings."));
    assert!(prompt
        .system
        .contains("End your response with EXACTLY ONE verdict marker in the existing format:"));
    assert!(prompt.system.contains("===REVIEW: PASS==="));
    assert!(prompt.system.contains("===REVIEW: FAIL: <short reason>==="));
}

#[test]
fn coder_prompt_includes_manual_bounce_reason_and_failed_attempt() {
    let mut ctx = fake_context(default_roles::CODER);
    ctx.last_manual_bounce_reason = Some("add tests for the error path".to_string());
    ctx.prior_reviews = vec![
        fake_review(1, db::ReviewStatus::Failed),
        fake_review(2, db::ReviewStatus::Passed),
    ];

    let prompt = resolve_prompt_builder(BUILDER_ID_CODER_IMPLEMENTATION_V2).build(&ctx);

    assert!(prompt
        .system
        .contains("sent back with the following feedback"));
    assert!(prompt.system.contains("add tests for the error path"));
    assert!(prompt.system.contains("failed review 1 time(s)"));
    assert!(prompt.user.contains("Build prompt context"));
    assert!(prompt.user.contains("Load context"));
}

#[test]
fn coder_prompt_omits_manual_bounce_reason_when_absent() {
    let ctx = fake_context(default_roles::CODER);

    let prompt = resolve_prompt_builder(BUILDER_ID_CODER_IMPLEMENTATION_V2).build(&ctx);

    assert!(!prompt
        .system
        .contains("sent back with the following feedback"));
    assert!(prompt.user.contains("Add dispatch context"));
}

#[test]
fn coder_prompt_merge_failed_follow_up_contains_rereview_directive() {
    let mut ctx = fake_context(default_roles::CODER);
    ctx.state_name = default_states::MERGE_FAILED.to_string();
    ctx.continuation_of_execution_id = Some("parent-exec".to_string());
    ctx.task.review_passed_at = Some("2026-04-17T10:00:00Z".to_string());

    let prompt = resolve_prompt_builder(BUILDER_ID_CODER_MERGE_FIX_V2).build(&ctx);

    assert!(prompt.user.contains("merge failed due to conflicts"));
    assert!(
        prompt.user.contains("review already passed")
            || prompt.user.contains("reviewer will not re-review")
    );
}

#[test]
fn coder_prompt_review_follow_up_is_self_contained() {
    let mut ctx = fake_context(default_roles::CODER);
    ctx.continuation_of_execution_id = Some("parent-exec".to_string());
    ctx.prior_reviews = vec![db::Review {
        step_results_json: json!({
            "ci_steps": [],
            "auditor": {
                "verdict": "fail",
                "reason": "deployment was not verified"
            }
        })
        .to_string(),
        ..fake_review(2, db::ReviewStatus::Failed)
    }];
    ctx.continuation_logs_path = Some("/tmp/forge/logs/parent-exec.jsonl".to_string());
    ctx.latest_review_feedback =
        Some("Actual reviewer text: deployment was not verified".to_string());
    ctx.latest_review_execution_id = Some("review-exec-2".to_string());
    ctx.latest_review_logs_path = Some("/tmp/forge/logs/review-exec-2.jsonl".to_string());

    let prompt = resolve_prompt_builder(BUILDER_ID_CODER_REVIEW_FIX_V2).build(&ctx);

    assert!(prompt.user.contains("Task: Add dispatch context"));
    assert!(prompt.user.contains("Description:"));
    assert!(prompt.user.contains("Original plan:"));
    assert!(prompt
        .user
        .contains("Actual reviewer text: deployment was not verified"));
    assert!(prompt.user.contains("Reviewer execution:"));
    assert!(prompt.user.contains("review-exec-2"));
    assert!(prompt.user.contains("Reviewer log file:"));
    assert!(prompt.user.contains("/tmp/forge/logs/review-exec-2.jsonl"));
    assert!(prompt.user.contains("Previous coder execution:"));
    assert!(prompt.user.contains("parent-exec"));
    assert!(prompt.user.contains("Previous coder log file:"));
    assert!(prompt.user.contains("/tmp/forge/logs/parent-exec.jsonl"));
    assert!(!prompt.user.contains("Previous execution transcript tail:"));
    assert!(!prompt.user.contains("Continue the existing coder chat"));
}

#[test]
fn coder_prompt_ci_follow_up_is_focused_on_failing_check() {
    let mut ctx = fake_context(default_roles::CODER);
    ctx.continuation_of_execution_id = Some("parent-exec".to_string());
    ctx.continuation_logs_path = Some("/tmp/forge/logs/parent-exec.jsonl".to_string());
    ctx.latest_review_feedback =
        Some("Long previous coder response that should not be used as CI feedback".to_string());
    ctx.prior_reviews = vec![db::Review {
        step_results_json: json!({
            "ci_steps": [{
                "index": 0,
                "command": "./scripts/ci-forge-review.sh",
                "exit_code": 1,
                "stderr_tail": "",
                "output_tail": "Diff in crates/mcp-server/src/tests.rs"
            }]
        })
        .to_string(),
        ..fake_review(2, db::ReviewStatus::Failed)
    }];

    let prompt = resolve_prompt_builder(BUILDER_ID_CODER_REVIEW_FIX_V2).build(&ctx);

    assert!(prompt.user.contains("CI failed during review"));
    assert!(prompt.user.contains("./scripts/ci-forge-review.sh"));
    assert!(prompt
        .user
        .contains("Diff in crates/mcp-server/src/tests.rs"));
    assert!(!prompt.user.contains("Description:"));
    assert!(!prompt.user.contains("Original plan:"));
    assert!(!prompt.user.contains("Long previous coder response"));
    assert!(!prompt.user.contains("The reviewer agent flagged"));
}

#[test]
fn coder_prompt_first_time_does_not_contain_rereview_directive() {
    let ctx = fake_context(default_roles::CODER);

    let prompt = resolve_prompt_builder(BUILDER_ID_CODER_IMPLEMENTATION_V2).build(&ctx);

    assert!(prompt.system.contains("implement code changes"));
    assert!(prompt
        .system
        .contains("Proof of work for app-touching changes"));
    assert!(prompt
        .system
        .contains("forge-ctl task media upload --task-id <id> --file <path>"));
    assert!(prompt.system.contains("planner agent already investigated"));
    assert!(prompt.system.contains("Treat the provided plan"));
    assert!(prompt.user.contains("Implementation objective:"));
    assert!(prompt
        .user
        .contains("Make the requested code changes in the worktree"));
    assert!(!prompt.user.contains("merge failed due to conflicts"));
    assert!(!prompt.user.contains("reviewer will not re-review"));
}

#[test]
fn reviewer_prompt_reads_ci_steps_from_review_config() {
    let mut ctx = fake_context(default_roles::REVIEWER);
    ctx.state_name = default_states::REVIEW.to_string();
    ctx.state_config = json!({
        "review": {
            "ci_steps": ["cargo test -p services", "cargo clippy -p services"],
            "review_prompt": "Focus on prompt regressions."
        }
    });
    ctx.prior_reviews = vec![fake_review(1, db::ReviewStatus::Failed)];

    let prompt = ReviewerPromptBuilder.build(&ctx);

    assert!(prompt.user.contains("cargo test -p services"));
    assert!(prompt.user.contains("cargo clippy -p services"));
    assert!(prompt.user.contains("Focus on prompt regressions."));
    assert!(prompt.user.contains("Attempt 1"));
}

#[test]
fn planner_prompt_includes_parent_task_context() {
    let mut ctx = fake_context(default_roles::PLANNER);
    ctx.parent_task = Some(fake_task(
        "parent-task",
        "Parent workflow task",
        Some("Parent description for planning context."),
    ));

    let prompt = PlannerPromptBuilder.build(&ctx);

    assert!(prompt.user.contains("Parent task"));
    assert!(prompt.user.contains("Parent workflow task"));
    assert!(prompt
        .user
        .contains("Parent description for planning context."));
}

#[test]
fn generic_prompt_dumps_core_context_for_unknown_role() {
    let mut ctx = fake_context("security_engineer");
    ctx.state_name = "security_review".to_string();
    ctx.last_manual_bounce_reason = Some("threat model missing".to_string());
    ctx.state_config = json!({ "scanner": "semgrep" });

    let prompt = GenericPromptBuilder.build(&ctx);

    assert!(prompt.user.contains("\"task_id\": \"task-1\""));
    assert!(prompt.user.contains("\"role\": \"security_engineer\""));
    assert!(prompt.user.contains("\"state\": \"security_review\""));
    assert!(prompt.user.contains("\"scanner\": \"semgrep\""));
    assert!(prompt.user.contains("threat model missing"));
}

#[test]
fn prompt_overrides_replace_and_append_from_state_config() {
    let prompt = apply_prompt_overrides(
        resolve_prompt_builder(BUILDER_ID_PLANNER_DEFAULT_V2)
            .build(&fake_context(default_roles::PLANNER)),
        &json!({
            "system": "Use the project planning rubric.",
            "system_append": "Require explicit verification.",
            "user_prefix": "Workflow note: split risky work first.",
            "user_append": "Return concise checklist updates."
        }),
    );

    assert!(prompt
        .system
        .starts_with("Use the project planning rubric."));
    assert!(prompt.system.contains("Require explicit verification."));
    assert!(prompt
        .user
        .starts_with("Workflow note: split risky work first."));
    assert!(prompt.user.contains("Plan task: Add dispatch context"));
    assert!(prompt.user.contains("Return concise checklist updates."));
}

#[test]
fn builder_precedence_prefers_trigger_then_state_then_role_default() {
    let trigger = DispatchIntent {
        builder_id: Some(BUILDER_ID_CODER_REVIEW_FIX_V2.to_string()),
        execution_policy: None,
        prompt_config: json!({}),
    };
    let state = DispatchIntent {
        builder_id: Some(BUILDER_ID_REVIEWER_DEFAULT_V2.to_string()),
        execution_policy: None,
        prompt_config: json!({}),
    };
    let selected = effective_prompt_selection(default_roles::CODER, Some(&trigger), Some(&state));
    assert_eq!(selected.builder_id, BUILDER_ID_CODER_REVIEW_FIX_V2);

    let selected_state = effective_prompt_selection(default_roles::CODER, None, Some(&state));
    assert_eq!(selected_state.builder_id, BUILDER_ID_REVIEWER_DEFAULT_V2);

    let selected_role_default = effective_prompt_selection(default_roles::CODER, None, None);
    assert_eq!(
        selected_role_default.builder_id,
        BUILDER_ID_CODER_IMPLEMENTATION_V2
    );
}

#[test]
fn builder_precedence_falls_back_to_generic_for_unknown_role() {
    let selected = effective_prompt_selection("custom_role", None, None);
    assert_eq!(selected.builder_id, BUILDER_ID_GENERIC_DEFAULT_V2);
}

#[test]
fn custom_reviewer_role_can_use_explicit_reviewer_builder() {
    let mut ctx = fake_context("reviewer1");
    ctx.state_name = "security_review".to_string();
    let state_dispatch = DispatchIntent {
        builder_id: Some(BUILDER_ID_REVIEWER_DEFAULT_V2.to_string()),
        execution_policy: None,
        prompt_config: json!({}),
    };

    let (prompt, selection) = build_effective_prompt(&ctx, None, Some(&state_dispatch));

    assert_eq!(selection.builder_id, BUILDER_ID_REVIEWER_DEFAULT_V2);
    assert!(prompt.system.contains("reviewer"));
    assert!(prompt.user.contains("Review task: Add dispatch context"));
}
