use serde_json::json;

use crate::workflow::{
    default_roles, default_states,
    dispatch::{
        apply_prompt_overrides, build_effective_prompt, effective_prompt_selection,
        generic_prompt::GenericPromptBuilder, planner_prompt::PlannerPromptBuilder,
        resolve_prompt_builder, reviewer_prompt::ReviewerPromptBuilder, AgentDispatchContext,
        AgentPrompt, DispatchIntent, PromptBuilder, TaskDelivery,
        BUILDER_ID_CODER_IMPLEMENTATION_V2, BUILDER_ID_CODER_MERGE_FIX_V2,
        BUILDER_ID_CODER_REVIEW_FIX_V2, BUILDER_ID_GENERIC_DEFAULT_V2,
        BUILDER_ID_PLANNER_DEFAULT_V2, BUILDER_ID_READ_ONLY_TASK_V1,
        BUILDER_ID_REVIEWER_CONFORMANCE_V1, BUILDER_ID_WORKER_AUTONOMOUS_V1,
        BUILDER_ID_WORKER_MERGE_FIX_V1, BUILDER_ID_WORKER_REVIEW_FIX_V1,
        EXECUTION_POLICY_NEW_EXECUTION, EXECUTION_POLICY_RESUME_LATEST_TARGET_ROLE_THREAD,
    },
};

#[test]
fn execution_input_preserves_role_contract_and_user_request() {
    let prompt = AgentPrompt {
        system: "Worker boundary: do not integrate the target branch.".to_owned(),
        user: "Implement the parser.".to_owned(),
        tools: Vec::new(),
    };

    let input = prompt.execution_input(Some("Keep the public format stable."));

    assert!(input.starts_with("Forge role contract (authoritative):"));
    assert!(input.contains("do not integrate the target branch"));
    assert!(input.contains("[User context: Keep the public format stable.]"));
    assert!(input.ends_with("Implement the parser."));
}

fn fake_task(id: &str, title: &str, description: Option<&str>) -> db::Task {
    db::Task {
        condition: Default::default(),
        id: id.to_string(),
        project_id: "project-1".to_string(),
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
        reviewer_execution_id: None,
        auditor_execution_id: None,
        attempt_number,
        status,
        step_results_json: "{}".to_string(),
        started_at: "2026-04-17T00:00:00Z".to_string(),
        finished_at: Some("2026-04-17T00:01:00Z".to_string()),
        created_at: "2026-04-17T00:00:00Z".to_string(),
        updated_at: "2026-04-17T00:01:00Z".to_string(),
    }
}

fn fake_comment() -> db::TaskComment {
    db::TaskComment {
        id: "comment".to_owned(),
        task_id: "task-1".to_owned(),
        author_type: db::CommentAuthorType::Agent,
        author_id: None,
        author_name: "Agent".to_owned(),
        content: String::new(),
        execution_id: None,
        role: None,
        worklog_kind: None,
        idempotency_key: None,
        created_at: "2026-10-12T10:00:00Z".to_owned(),
        updated_at: "2026-10-12T10:00:00Z".to_owned(),
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
        review_window: None,
        previous_coder_summary: None,
        repair_attempts: Vec::new(),
        carried_repair: false,
        reset_model_after_repair: false,
        repair_budget: None,
        parent_task: None,
        sub_tasks: Vec::new(),
        last_manual_bounce_reason: None,
        continuation_of_execution_id: None,
        continuation_logs_path: None,
        latest_review_feedback: None,
        latest_review_execution_id: None,
        latest_review_logs_path: None,
        read_only_task: false,
        delivery: TaskDelivery::NativeTools,
        review_ci_steps: Vec::new(),
        step_diff: None,
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
                "must never run Git to rebase, merge, or rewrite history",
                "Must not rewrite the feature or add unrelated cleanup.",
            ],
        ),
        (
            BUILDER_ID_REVIEWER_CONFORMANCE_V1,
            default_roles::REVIEWER,
            vec![
                "Reviewer boundary:",
                "Must remain read-only",
                "do not edit, stage, or commit tracked files",
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

        if builder_id == BUILDER_ID_REVIEWER_CONFORMANCE_V1 {
            // The reviewer answers with a result block; the taxonomy and
            // restatement rules made reviewers invent extra keys and spend
            // turns narrating instead of verifying.
            assert!(!prompt.system.contains(FAILURE_TAXONOMY_LINE));
            assert!(!prompt.system.contains("Before acting, restate objective"));
            assert!(prompt
                .system
                .contains("owner-only or repeated findings may park for the owner"));
            continue;
        }
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
    assert!(merge_fix.user.contains("real branch conflict"));
    assert!(merge_fix.user.contains("Do not rebase"));
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
fn reviewer_base_prompt_defers_the_frozen_contract_to_launch() {
    let prompt = resolve_prompt_builder(BUILDER_ID_REVIEWER_CONFORMANCE_V1)
        .build(&fake_context(default_roles::REVIEWER));

    assert!(prompt
        .system
        .contains("Reviewer findings: Write findings in your Markdown review."));
    assert!(prompt
        .system
        .contains("Each BLOCKING finding must be reproducible"));
    assert!(prompt.system.contains("expected vs actual behavior"));
    assert!(prompt
        .system
        .contains("Decide whether the change works by exercising it"));
    assert!(prompt
        .system
        .contains("Separate NON-BLOCKING findings from BLOCKING findings."));
    assert!(prompt
        .system
        .contains("frozen contract Forge appends at launch"));
    assert!(!prompt.system.contains("contract_digest"));
    assert!(!prompt.user.contains("contract_digest"));
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
    ctx.task.error_annotation = Some(json!({"type": "merge_conflict"}).to_string());
    ctx.task.condition = db::map_legacy_condition(&db::LegacyConditionInput::from(&ctx.task));

    let prompt = resolve_prompt_builder(BUILDER_ID_CODER_MERGE_FIX_V2).build(&ctx);

    assert!(prompt.user.contains("real merge conflict"));
    assert!(prompt.user.contains("Do not rebase"));
    assert!(prompt.user.contains("manual task-worktree repair"));
    // The repaired commit is re-reviewed, so the prompt must not promise the
    // reviewer is done with it.
    assert!(prompt.user.contains("fresh review"));
    assert!(!prompt.user.contains("will not re-review"));
}

/// A rebase conflict Forge committed with markers is the Worker's to
/// reconcile: both merge-fix builders must say so instead of the manual-repair
/// refusal, and must name the conflicted files.
#[test]
fn merge_fix_prompts_hand_a_committed_conflict_to_the_worker() {
    let handoff = db::TransitionLog {
        id: "handoff".to_owned(),
        task_id: "task".to_owned(),
        from_state: default_states::MERGING.to_owned(),
        to_state: default_states::MERGE_FAILED.to_owned(),
        trigger_name: None,
        triggered_by: "system:workflow".to_owned(),
        bridge: api_types::TransitionBridge::new(api_types::TransitionBridgeKind::ConflictHandoff),
        trigger_reason:
            "rebased onto main; conflicts were committed with markers in: src/pkg/__init__.py, uv.lock"
                .to_owned(),
        hook_results_json: None,
        rejection: true,
        created_at: "2026-09-22T10:00:00Z".to_owned(),
    };
    for (builder_id, role) in [
        (BUILDER_ID_CODER_MERGE_FIX_V2, default_roles::CODER),
        (BUILDER_ID_WORKER_MERGE_FIX_V1, default_roles::WORKER),
    ] {
        let mut ctx = fake_context(role);
        ctx.state_name = default_states::MERGE_FAILED.to_string();
        ctx.task.error_annotation = Some(json!({"type": "merge_conflict"}).to_string());
        ctx.task.condition = db::map_legacy_condition(&db::LegacyConditionInput::from(&ctx.task));
        ctx.transition_log = vec![handoff.clone()];

        let prompt = resolve_prompt_builder(builder_id).build(&ctx);

        assert!(
            prompt.user.contains("committed the merge conflicts"),
            "{builder_id}: {}",
            prompt.user
        );
        assert!(prompt.user.contains("src/pkg/__init__.py"), "{builder_id}");
        assert!(prompt.user.contains("keep the intent of BOTH sides"));
        assert!(prompt.user.contains("regenerate it"));
        // The embedded executor refuses uncommitted edits, so the Worker must
        // be told to commit; saying Forge commits for it wasted two attempts
        // on a live handoff.
        assert!(
            prompt.user.contains("`git commit`"),
            "{builder_id}: {}",
            prompt.user
        );
        assert!(!prompt.user.contains("Forge commits your changes"));
        // The repair is integrated by Forge after its own checks; the prompt
        // does not promise (or mention) a review.
        assert!(prompt.user.contains("Forge will run the project's checks"));
        assert!(
            !prompt.user.contains("review"),
            "{builder_id}: {}",
            prompt.user
        );
        assert!(
            !prompt
                .user
                .contains("manual task-worktree repair is required"),
            "{builder_id} must not refuse a conflict it can reconcile"
        );
    }
}

#[test]
fn coder_prompt_dirty_worktree_does_not_rebase_or_discard_changes() {
    let mut ctx = fake_context(default_roles::CODER);
    ctx.state_name = default_states::MERGE_FAILED.to_string();
    ctx.task.review_passed_at = Some("2026-04-17T10:00:00Z".to_string());
    ctx.task.error_annotation = Some(
        json!({
            "type": "dirty_worktree",
            "message": "worktree has uncommitted changes: src/lib.rs"
        })
        .to_string(),
    );
    ctx.task.condition = db::map_legacy_condition(&db::LegacyConditionInput::from(&ctx.task));

    let prompt = resolve_prompt_builder(BUILDER_ID_CODER_MERGE_FIX_V2).build(&ctx);

    assert!(prompt.user.contains("uncommitted changes"));
    assert!(prompt.user.contains("not a merge conflict"));
    assert!(prompt.user.contains("do not rebase or discard it"));
    assert!(!prompt.user.contains("real merge conflict"));
    assert!(!prompt
        .user
        .contains("Rebase your worktree branch onto the latest default branch"));
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
    assert!(prompt.user.contains("Round:"));
    assert!(prompt.user.contains("Open findings:"));
    assert!(prompt.user.contains("Reviewer report:"));
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

    assert!(prompt.user.contains("Failing check output:"));
    assert!(prompt.user.contains("./scripts/ci-forge-review.sh"));
    assert!(prompt
        .user
        .contains("Diff in crates/mcp-server/src/tests.rs"));
    assert!(prompt.user.contains("Description:"));
    assert!(prompt
        .user
        .contains("Build prompt context for workflow agents."));
    assert!(prompt.user.contains("Original plan:"));
    assert!(prompt.user.contains("1. Load context"));
    assert!(!prompt.user.contains("Long previous coder response"));
    assert!(!prompt.user.contains("The reviewer agent flagged"));
}

#[test]
fn coder_prompt_first_time_does_not_contain_rereview_directive() {
    let ctx = fake_context(default_roles::CODER);

    let prompt = resolve_prompt_builder(BUILDER_ID_CODER_IMPLEMENTATION_V2).build(&ctx);

    assert!(prompt.system.contains("implement code changes"));
    // An executing agent holds no forge-ctl credential for this server;
    // proof goes through its own delivery channel instead.
    assert!(!prompt.system.contains("forge-ctl"));
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
fn reviewer_prompt_includes_ci_results_from_the_running_review() {
    let mut ctx = fake_context(default_roles::REVIEWER);
    ctx.state_name = default_states::REVIEW.to_string();
    ctx.state_config = json!({ "review": { "ci_steps": ["npm test"] } });
    let mut running = fake_review(2, db::ReviewStatus::Running);
    running.step_results_json = json!({
        "ci_steps": [{ "index": 0, "command": "npm test", "exit_code": 0, "output_tail": "12 passing\n" }]
    })
    .to_string();
    ctx.prior_reviews = vec![fake_review(1, db::ReviewStatus::Failed), running];

    let prompt = ReviewerPromptBuilder.build(&ctx);

    assert!(prompt.user.contains("Forge already ran these checks"));
    assert!(prompt.user.contains("- `npm test` exited 0"));
    assert!(prompt.user.contains("12 passing"));
}

#[test]
fn read_only_reviewer_prompt_omits_inherited_implementation_ci_steps() {
    let mut ctx = fake_context(default_roles::REVIEWER);
    ctx.state_name = default_states::REVIEW.to_string();
    ctx.read_only_task = true;
    ctx.state_config = json!({
        "review": {
            "ci_steps": ["false"],
            "review_prompt": "Assess the research evidence."
        }
    });

    let prompt = ReviewerPromptBuilder.build(&ctx);

    assert!(!prompt.user.contains("Required CI steps"));
    assert!(!prompt.user.contains("- false"));
    assert!(prompt.user.contains("Assess the research evidence."));
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
fn planner_layout_guidance_is_shared_by_native_and_outbox_delivery() {
    const RULE: &str = forge_agent_host::MERGE_FRIENDLY_LAYOUT_GUIDANCE;
    for delivery in [TaskDelivery::NativeTools, TaskDelivery::Outbox] {
        let mut ctx = fake_context(default_roles::PLANNER);
        ctx.delivery = delivery;
        let prompt = PlannerPromptBuilder.build(&ctx);
        assert_eq!(prompt.system.matches(RULE).count(), 1);
        assert!(prompt.system.contains("For sub-task planning:"));
        assert!(prompt.user.contains("owned repository-relative paths"));
        assert!(
            !prompt.user.contains(RULE),
            "doctrine is paid for once per run"
        );
    }
}

#[test]
fn cli_plan_writers_use_the_brokered_plan_path() {
    let mut planner = fake_context(default_roles::PLANNER);
    planner.delivery = TaskDelivery::Outbox;
    let planner_prompt = PlannerPromptBuilder.build(&planner);
    assert!(planner_prompt.user.contains("$FORGE_PLAN_PATH"));
    assert!(!planner_prompt.user.contains("`../plan.md`"));

    let mut coder = fake_context(default_roles::CODER);
    coder.delivery = TaskDelivery::Outbox;
    let coder_prompt = resolve_prompt_builder(BUILDER_ID_CODER_IMPLEMENTATION_V2).build(&coder);
    assert!(coder_prompt.system.contains("$FORGE_PLAN_PATH"));
    assert!(coder_prompt.system.contains("Forge has copied"));

    for builder_id in [
        BUILDER_ID_WORKER_AUTONOMOUS_V1,
        BUILDER_ID_WORKER_REVIEW_FIX_V1,
        BUILDER_ID_WORKER_MERGE_FIX_V1,
    ] {
        let mut worker = fake_context(default_roles::WORKER);
        worker.delivery = TaskDelivery::Outbox;
        let worker_prompt = resolve_prompt_builder(builder_id).build(&worker);
        assert!(
            worker_prompt.system.contains("$FORGE_PLAN_PATH"),
            "{builder_id} missing brokered plan path"
        );
        assert!(
            worker_prompt.system.contains("entire checklist"),
            "{builder_id} missing full-checklist contract"
        );
        assert!(
            !worker_prompt.system.contains("typed `task.plan` operation"),
            "{builder_id} names the unavailable native plan tool"
        );
    }
}

#[test]
fn native_worker_plan_writers_use_the_typed_full_checklist_contract() {
    for builder_id in [
        BUILDER_ID_WORKER_AUTONOMOUS_V1,
        BUILDER_ID_WORKER_REVIEW_FIX_V1,
        BUILDER_ID_WORKER_MERGE_FIX_V1,
    ] {
        let worker_prompt =
            resolve_prompt_builder(builder_id).build(&fake_context(default_roles::WORKER));
        assert!(
            worker_prompt.system.contains("typed `task.plan` operation"),
            "{builder_id} missing native plan tool"
        );
        assert!(
            worker_prompt
                .system
                .contains(r#"{"action":"write","content":"<full Markdown checklist>"}"#),
            "{builder_id} missing write-full-checklist payload"
        );
        assert!(
            worker_prompt.system.contains("entire checklist"),
            "{builder_id} missing replacement semantics"
        );
        assert!(
            !worker_prompt.system.contains("$FORGE_PLAN_PATH"),
            "{builder_id} names the unavailable CLI plan path"
        );
    }
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
        builder_id: Some(BUILDER_ID_REVIEWER_CONFORMANCE_V1.to_string()),
        execution_policy: None,
        prompt_config: json!({}),
    };
    let selected = effective_prompt_selection(default_roles::CODER, Some(&trigger), Some(&state));
    assert_eq!(selected.builder_id, BUILDER_ID_CODER_REVIEW_FIX_V2);

    let selected_state = effective_prompt_selection(default_roles::CODER, None, Some(&state));
    assert_eq!(
        selected_state.builder_id,
        BUILDER_ID_REVIEWER_CONFORMANCE_V1
    );

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
        builder_id: Some(BUILDER_ID_REVIEWER_CONFORMANCE_V1.to_string()),
        execution_policy: None,
        prompt_config: json!({}),
    };

    let (prompt, selection) = build_effective_prompt(&ctx, None, Some(&state_dispatch));

    assert_eq!(selection.builder_id, BUILDER_ID_REVIEWER_CONFORMANCE_V1);
    assert!(prompt.system.contains("reviewer"));
    assert!(prompt.user.contains("Review task: Add dispatch context"));
}

#[test]
fn read_only_task_overrides_coder_dispatch_with_a_no_write_contract() {
    let mut ctx = fake_context(default_roles::CODER);
    ctx.task.task_type = "discovery".to_owned();
    ctx.read_only_task = true;
    let state_dispatch = DispatchIntent {
        builder_id: Some(BUILDER_ID_CODER_IMPLEMENTATION_V2.to_owned()),
        execution_policy: None,
        prompt_config: json!({
            "user": "Inspect the parser and write the implementation."
        }),
    };

    let (prompt, selection) = build_effective_prompt(&ctx, None, Some(&state_dispatch));

    assert_eq!(selection.builder_id, BUILDER_ID_READ_ONLY_TASK_V1);
    assert!(prompt.system.contains("read-only investigator"));
    assert!(prompt
        .user
        .contains("Do not modify, create, delete, or commit"));
    assert!(prompt.user.contains("clean unchanged worktree"));
}

/// A failed review's feedback must come from the reviewer's own execution.
/// `Review::execution_id` names the execution that was *reviewed*, so reading
/// feedback from it hands the coder its own transcript and the review→fix loop
/// can never converge on the finding.
#[tokio::test]
async fn review_feedback_comes_from_the_reviewer_execution_not_the_reviewed_one() {
    use db::{
        CreateExecution, CreateProject, CreateRepo, CreateReview, CreateTask, ExecutionRepo,
        ExecutionStatus, ProjectRepo, RepoRepo, ReviewRepo, TaskRepo,
    };

    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    db::run_migrations(&pool).await.expect("migrations run");
    let db = std::sync::Arc::new(db::SqliteDb::new(pool));
    let now = db::now_rfc3339();
    let project_id = db::new_uuid_v4();
    let repo_id = db::new_uuid_v4();
    ProjectRepo::create(
        &*db,
        CreateProject {
            id: project_id.clone(),
            name: "Review feedback".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");
    RepoRepo::create(
        &*db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "repo".to_owned(),
            remote_url: Some("https://example.com/repo.git".to_owned()),
            local_path: None,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo creates");
    let task = TaskRepo::create(
        &*db,
        CreateTask {
            id: db::new_uuid_v4(),
            project_id: project_id.clone(),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "Escape control characters".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: default_states::IN_PROGRESS.to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("task creates");

    let execution = |id: String, role: &str, created_at: String, logs: &str| CreateExecution {
        id,
        task_id: task.id.clone(),
        agent_id: None,
        role: role.to_owned(),
        status: ExecutionStatus::Completed,
        stop_reason: None,
        stopped_by: None,
        resume_policy: None,
        stopped_at: None,
        parent_execution_id: None,
        agent_session_id: None,
        agent_message_id: None,
        last_activity_at: None,
        summary: None,
        logs_path: Some(logs.to_owned()),
        before_sha: None,
        after_sha: None,
        error: None,
        executor_config_snapshot_json: Some(r#"{"executor_type":"shell","config":{}}"#.to_owned()),
        workspace_id: None,
        created_at: created_at.clone(),
        updated_at: created_at,
    };
    let coder_id = db::new_uuid_v4();
    let reviewer_id = db::new_uuid_v4();
    ExecutionRepo::create(
        &*db,
        execution(
            coder_id.clone(),
            default_roles::CODER,
            "2026-04-17T00:00:00Z".to_owned(),
            "/logs/coder.jsonl",
        ),
    )
    .await
    .expect("coder execution creates");
    ExecutionRepo::create(
        &*db,
        execution(
            reviewer_id.clone(),
            default_roles::REVIEWER,
            "2026-04-17T00:02:00Z".to_owned(),
            "/logs/reviewer.jsonl",
        ),
    )
    .await
    .expect("reviewer execution creates");

    let review = ReviewRepo::create(
        &*db,
        CreateReview {
            id: db::new_uuid_v4(),
            task_id: task.id.clone(),
            // The reviewed execution — the coder's.
            execution_id: coder_id.clone(),
            attempt_number: 1,
            status: db::ReviewStatus::Running,
            step_results_json: "{}".to_owned(),
            started_at: "2026-04-17T00:01:00Z".to_owned(),
            created_at: "2026-04-17T00:01:00Z".to_owned(),
            updated_at: "2026-04-17T00:01:00Z".to_owned(),
        },
    )
    .await
    .expect("review creates");
    // Dispatching a reviewer binds its execution to the attempt; without
    // that binding the attempt only names the execution it reviewed.
    sqlx::query("UPDATE review SET reviewer_execution_id = ? WHERE id = ?")
        .bind(&reviewer_id)
        .bind(&review.id)
        .execute(db.pool())
        .await
        .expect("reviewer execution binds to the attempt");
    ReviewRepo::update_status(
        &*db,
        &review.id,
        db::ReviewStatus::Failed,
        json!({
            "ci_steps": [],
            "auditor": {"verdict": "fail", "reason": "reviewer identified a violation"}
        })
        .to_string(),
        Some("2026-04-17T00:03:00Z".to_owned()),
        "2026-04-17T00:03:00Z",
    )
    .await
    .expect("review fails");

    let conformance = json!({
        "status": "failed",
        "contract": null,
        "assessment": {
            "result": "fail",
            "reason": "The JSON escaper leaves U+0008 unescaped.",
            "report": "- task:acceptance violated: the JSON escaper leaves U+0008 unescaped \
                       (tsvsort/src/main.rs:10-20)."
        },
        "checks": [],
        "reason": "The JSON escaper leaves U+0008 unescaped."
    })
    .to_string();
    sqlx::query("INSERT INTO execution_review_contract VALUES (?, ?, ?, ?, ?, ?)")
        .bind(&reviewer_id)
        .bind(&task.id)
        .bind("digest")
        .bind("source-digest")
        .bind(json!({"execution_id": reviewer_id}).to_string())
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("contract persists");
    sqlx::query("INSERT INTO execution_review_assessment VALUES (?, ?, ?)")
        .bind(&reviewer_id)
        .bind(&conformance)
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("assessment persists");

    let context = crate::workflow::dispatch::loader::load_agent_dispatch_context(
        crate::workflow::dispatch::loader::DispatchContextParams {
            db: db.clone(),
            router: &crate::diff::embedded_read_router_for_test(db.clone()),
            task_id: &task.id,
            role: default_roles::CODER,
            state_name: default_states::IN_PROGRESS,
            state_config: json!({}),
            execution_policy: None,
            workflow: &crate::workflow::default_workflow::default_workflow(),
        },
    )
    .await
    .expect("dispatch context loads");

    assert_eq!(
        context.latest_review_execution_id.as_deref(),
        Some(reviewer_id.as_str()),
        "the coder must be pointed at the reviewer's execution"
    );
    assert_eq!(
        context.latest_review_logs_path.as_deref(),
        Some("/logs/reviewer.jsonl")
    );
    let feedback = context
        .latest_review_feedback
        .expect("the failed review supplies feedback");
    assert!(
        feedback.starts_with("The JSON escaper leaves U+0008 unescaped.\n\n"),
        "feedback leads with the reviewer's reason: {feedback}"
    );
    assert!(
        feedback.contains("tsvsort/src/main.rs:10-20"),
        "feedback carries the reviewer's Markdown review: {feedback}"
    );
}

#[test]
fn cli_delivery_prompts_name_the_outbox_instead_of_native_tools() {
    for (builder_id, role) in [
        (BUILDER_ID_CODER_IMPLEMENTATION_V2, default_roles::CODER),
        (BUILDER_ID_CODER_REVIEW_FIX_V2, default_roles::CODER),
        (BUILDER_ID_REVIEWER_CONFORMANCE_V1, default_roles::REVIEWER),
    ] {
        let mut ctx = fake_context(role);
        ctx.delivery = TaskDelivery::Outbox;
        let system = resolve_prompt_builder(builder_id).build(&ctx).system;
        assert!(system.contains("`FORGE_OUTBOX`"), "{builder_id}: {system}");
        assert!(
            system.contains("$FORGE_OUTBOX/worklog.jsonl"),
            "{builder_id}"
        );
        assert!(
            system.contains("$FORGE_OUTBOX/evidence.jsonl"),
            "{builder_id}"
        );
        assert!(
            system.contains("one JSON object per line to `$FORGE_OUTBOX/worklog.jsonl`"),
            "{builder_id}"
        );
        assert!(
            system.contains("one JSON object per line to `$FORGE_OUTBOX/evidence.jsonl`"),
            "{builder_id}"
        );
        assert!(
            system.contains("`screenshot`, `walkthrough_video`, `log`, `report`, or `other`"),
            "{builder_id}"
        );
        assert!(!system.contains("`task.worklog`"), "{builder_id}");
        assert!(!system.contains("`task.evidence`"), "{builder_id}");
    }

    let mut reviewer = fake_context(default_roles::REVIEWER);
    reviewer.delivery = TaskDelivery::Outbox;
    let system = resolve_prompt_builder(BUILDER_ID_REVIEWER_CONFORMANCE_V1)
        .build(&reviewer)
        .system;
    assert!(system.contains("the only place you may write"));
}

#[test]
fn native_reviewer_prompt_reports_through_native_tools() {
    let ctx = fake_context(default_roles::REVIEWER);
    let system = resolve_prompt_builder(BUILDER_ID_REVIEWER_CONFORMANCE_V1)
        .build(&ctx)
        .system;
    assert!(system.contains("`task.worklog`"));
    assert!(system.contains("`task.evidence`"));
    assert!(!system.contains("FORGE_OUTBOX"));
}

#[test]
fn delivery_follows_the_agent_backend_kind() {
    assert_eq!(
        TaskDelivery::for_backend_kind("native"),
        TaskDelivery::NativeTools
    );
    assert_eq!(TaskDelivery::for_backend_kind("cli"), TaskDelivery::Outbox);
}

#[test]
fn implementation_prompts_list_the_checks_that_gate_review() {
    for (builder_id, role) in [
        (BUILDER_ID_CODER_IMPLEMENTATION_V2, default_roles::CODER),
        (BUILDER_ID_CODER_REVIEW_FIX_V2, default_roles::CODER),
        (BUILDER_ID_WORKER_AUTONOMOUS_V1, default_roles::WORKER),
    ] {
        let mut ctx = fake_context(role);
        ctx.review_ci_steps = vec![
            "python3 -m unittest discover -s tests -v".to_owned(),
            "ruff check .".to_owned(),
        ];
        let system = resolve_prompt_builder(builder_id).build(&ctx).system;
        assert!(
            system.contains("Required checks:"),
            "{builder_id}: {system}"
        );
        assert!(
            system.contains("`bash -lc` from the worktree root"),
            "{builder_id}"
        );
        assert!(
            system.contains("- `python3 -m unittest discover -s tests -v`\n- `ruff check .`"),
            "{builder_id}"
        );
    }

    let mut read_only = fake_context(default_roles::CODER);
    read_only.read_only_task = true;
    read_only.review_ci_steps = vec!["make check".to_owned()];
    let system = resolve_prompt_builder(BUILDER_ID_CODER_IMPLEMENTATION_V2)
        .build(&read_only)
        .system;
    assert!(!system.contains("Required checks:"));

    let none = fake_context(default_roles::CODER);
    let system = resolve_prompt_builder(BUILDER_ID_CODER_IMPLEMENTATION_V2)
        .build(&none)
        .system;
    assert!(!system.contains("Required checks:"));
}

/// A planner run on a root Task with subtasks is its lead: the coordination
/// builder replaces the default planner builder, carries the breakdown with
/// states as user-role input, and asks for no checklist plan.
#[test]
fn a_planner_on_a_coordination_root_gets_the_lead_prompt() {
    let mut ctx = fake_context(default_roles::PLANNER);
    ctx.state_name = default_states::PLANNING.to_string();
    let state = DispatchIntent {
        builder_id: Some(BUILDER_ID_PLANNER_DEFAULT_V2.to_owned()),
        execution_policy: None,
        prompt_config: json!({}),
    };
    let (plain, selection) = build_effective_prompt(&ctx, None, Some(&state));
    assert_eq!(selection.builder_id, BUILDER_ID_PLANNER_DEFAULT_V2);
    assert!(plain.user.contains("Plan task: Add dispatch context"));

    let mut first = fake_task("child-1", "Add the schema", Some("Migration and model."));
    first.parent_task_id = Some("task-1".to_owned());
    first.status = default_states::TODO.to_owned();
    let mut second = fake_task("child-2", "Wire the endpoint", None);
    second.parent_task_id = Some("task-1".to_owned());
    second.status = default_states::TODO.to_owned();
    ctx.sub_tasks = vec![first, second];
    let (lead, selection) = build_effective_prompt(&ctx, None, Some(&state));
    assert_eq!(
        selection.builder_id,
        super::BUILDER_ID_PLANNER_COORDINATION_V1
    );
    assert!(lead.system.contains("Lead doctrine:"));
    assert!(lead.system.contains(FAILURE_TAXONOMY_LINE));
    // Volatile state is user-role input, never part of the stable prefix.
    assert!(!lead.system.contains("Add the schema"));
    assert!(lead
        .user
        .contains("Lead this feature: Add dispatch context"));
    assert!(lead.user.contains("1. [todo] Add the schema"));
    assert!(lead.user.contains("   Migration and model."));
    assert!(lead.user.contains("2. [todo] Wire the endpoint"));
    assert!(!lead.user.contains("plan.md"));
    // The doctrine stays within its budget (about 1,200 tokens).
    assert!(lead.system.len() < 4_800, "{}", lead.system.len());

    assert!(lead.user.contains("no subtask has started"));
    // A breakdown already under way is not described as a kickoff.
    ctx.sub_tasks[0].status = default_states::DONE.to_owned();
    let (late, _) = build_effective_prompt(&ctx, None, Some(&state));
    assert!(!late.user.contains("no subtask has started"));
    assert!(late.user.contains("1. [done] Add the schema"));
    assert_eq!(late.system, lead.system, "the stable prefix does not move");

    // A subtask's own planner is not a lead.
    ctx.task.parent_task_id = Some("root".to_owned());
    let (_, selection) = build_effective_prompt(&ctx, None, Some(&state));
    assert_eq!(selection.builder_id, BUILDER_ID_PLANNER_DEFAULT_V2);
}

/// The step check: a planner run on a subtask in the `lead_check` gate gets
/// the step-check prompt whatever builder the state or role names. It carries
/// one subtask, demands the result block, and asks for no plan.
#[test]
fn a_planner_on_a_subtask_in_the_step_check_gets_the_step_check_prompt() {
    let mut ctx = fake_context(default_roles::PLANNER);
    ctx.task.parent_task_id = Some("root".to_owned());
    ctx.task.status = default_states::LEAD_CHECK.to_owned();
    ctx.state_name = default_states::LEAD_CHECK.to_owned();
    ctx.parent_task = Some(fake_task(
        "root",
        "Ship the importer",
        Some("Root history."),
    ));
    let mut note = fake_comment();
    note.author_name = "Coder One".to_owned();
    note.content = "validation: cargo test -p importer: 12 passed".to_owned();
    ctx.comments = vec![note];
    for state in [
        None,
        Some(DispatchIntent {
            builder_id: Some(BUILDER_ID_PLANNER_DEFAULT_V2.to_owned()),
            execution_policy: None,
            prompt_config: json!({}),
        }),
    ] {
        let (prompt, selection) = build_effective_prompt(&ctx, None, state.as_ref());
        assert_eq!(
            selection.builder_id,
            super::BUILDER_ID_PLANNER_STEP_CHECK_V1
        );
        assert!(prompt.system.contains("Step-check doctrine:"));
        assert!(prompt
            .system
            .contains("{\"result\": \"pass\", \"reason\": \"one sentence\"}"));
        assert!(prompt.system.contains(FAILURE_TAXONOMY_LINE));
        // One subtask as user-role input; nothing of the root but its title.
        assert!(!prompt.system.contains("Add dispatch context"));
        assert!(prompt
            .user
            .contains("Check this finished step: Add dispatch context"));
        assert!(prompt
            .user
            .contains("It is a subtask of: Ship the importer"));
        assert!(prompt
            .user
            .contains("Build prompt context for workflow agents."));
        assert!(prompt
            .user
            .contains("- Coder One: validation: cargo test -p importer: 12 passed"));
        assert!(!prompt.user.contains("Root history."));
        assert!(!prompt.user.contains("plan.md"));
        assert_eq!(prompt.tools, vec!["read_files".to_owned()]);
        assert!(prompt.system.len() < 4_800, "{}", prompt.system.len());
    }
    // The worker's reports are bounded, newest kept.
    let mut many = Vec::new();
    for index in 0..40 {
        let mut comment = fake_comment();
        comment.content = format!("report {index} {}", "x".repeat(600));
        many.push(comment);
    }
    ctx.comments = many;
    let (bounded, _) = build_effective_prompt(&ctx, None, None);
    assert!(bounded.user.len() < 12_000, "{}", bounded.user.len());
    assert!(bounded.user.contains("report 39 "));
    assert!(!bounded.user.contains("report 0 "));
    assert!(bounded.user.contains("[older reports left out]"));

    // A subtask the Project workflow governs (a state the inherited workflow
    // does not have) keeps its own planner prompt.
    ctx.state_name = default_states::PLANNING.to_owned();
    ctx.task.status = default_states::PLANNING.to_owned();
    let (_, selection) = build_effective_prompt(&ctx, None, None);
    assert_eq!(selection.builder_id, BUILDER_ID_PLANNER_DEFAULT_V2);
}

/// A step the lead returned: the worker's next implementation prompt carries
/// the lead's findings, taken from the rejection that brought the subtask
/// back, and only while that rejection is the current entry.
#[test]
fn a_returned_step_carries_the_leads_findings_into_the_implementation_prompt() {
    let mut ctx = fake_context(default_roles::CODER);
    ctx.task.parent_task_id = Some("root".to_owned());
    let returned = db::TransitionLog {
        id: "returned".to_owned(),
        task_id: "task-1".to_owned(),
        from_state: default_states::LEAD_CHECK.to_owned(),
        to_state: default_states::IN_PROGRESS.to_owned(),
        trigger_name: None,
        triggered_by: "system:workflow".to_owned(),
        bridge: api_types::TransitionBridge::new(api_types::TransitionBridgeKind::GateRejected),
        trigger_reason: "The lead returned this step: the last row is dropped".to_owned(),
        hook_results_json: None,
        rejection: true,
        created_at: "2026-10-12T10:00:00Z".to_owned(),
    };
    ctx.transition_log = vec![returned.clone()];
    let user = resolve_prompt_builder(BUILDER_ID_CODER_IMPLEMENTATION_V2)
        .build(&ctx)
        .user;
    assert!(user.contains("The lead checked this step and returned it."));
    assert!(user.contains("The lead returned this step: the last row is dropped"));

    // A later entry that is not a return says nothing of it.
    let mut later = returned;
    later.id = "later".to_owned();
    later.from_state = default_states::TODO.to_owned();
    later.rejection = false;
    later.bridge = Default::default();
    ctx.transition_log.push(later);
    let user = resolve_prompt_builder(BUILDER_ID_CODER_IMPLEMENTATION_V2)
        .build(&ctx)
        .user;
    assert!(!user.contains("returned it"));
}

#[test]
fn coder_prompt_ci_follow_up_lists_signatures_for_every_failing_step() {
    let mut ctx = fake_context(default_roles::CODER);
    ctx.prior_reviews = vec![db::Review {
        step_results_json: json!({"ci_steps":[
            {"index":0,"command":"cargo test","exit_code":1,"output_tail":"rust failure","failures":["parser::empty", "parser::unicode"]},
            {"index":1,"command":"true","exit_code":0,"failures":["must not appear"]},
            {"index":2,"command":"pnpm test","exit_code":1,"stderr_tail":"web failure","failures":["web > input"]},
            {"index":3,"command":"unknown-runner","exit_code":2,"stderr_tail":"unknown failure"}
        ]}).to_string(),
        ..fake_review(2, db::ReviewStatus::Failed)
    }];
    let prompt = resolve_prompt_builder(BUILDER_ID_CODER_REVIEW_FIX_V2).build(&ctx);
    for expected in [
        "cargo test",
        "parser::empty",
        "parser::unicode",
        "pnpm test",
        "web > input",
        "unknown-runner",
        "rust failure",
        "web failure",
        "unknown failure",
    ] {
        assert!(
            prompt.user.contains(expected),
            "missing {expected}: {}",
            prompt.user
        );
    }
    assert!(!prompt.user.contains("must not appear"));
}

#[test]
fn coder_prompt_ci_signatures_have_per_step_display_limit() {
    let mut ctx = fake_context(default_roles::CODER);
    ctx.prior_reviews = vec![db::Review {
        step_results_json: json!({"ci_steps":[{"command":"test","exit_code":1,"failures":(0..23).map(|n| format!("signature-{n:03}")).collect::<Vec<_>>(),"failures_truncated":true}]}).to_string(),
        ..fake_review(1, db::ReviewStatus::Failed)
    }];
    let prompt = resolve_prompt_builder(BUILDER_ID_CODER_REVIEW_FIX_V2).build(&ctx);
    assert!(prompt.user.contains("signature-019"));
    assert!(!prompt.user.contains("signature-020"));
    assert!(prompt.user.contains("+3 more"));
    assert!(prompt
        .user
        .contains("Additional failure signatures were truncated"));
}

#[test]
fn coder_prompt_no_change_brief_precedes_prior_ci_feedback() {
    let mut ctx = fake_context(default_roles::CODER);
    ctx.transition_log.push(db::TransitionLog {
        id: "no-change".into(),
        task_id: ctx.task.id.clone(),
        from_state: "review".into(),
        to_state: ctx.state_name.clone(),
        triggered_by: "system:workflow".into(),
        trigger_name: None,
        bridge: api_types::TransitionBridge::recovery("repair_no_change", false),
        trigger_reason: "No change since review 2\nCommit the repair before resubmitting.".into(),
        rejection: false,
        created_at: db::now_rfc3339(),
        hook_results_json: None,
    });
    for builder in [
        BUILDER_ID_CODER_REVIEW_FIX_V2,
        BUILDER_ID_WORKER_REVIEW_FIX_V1,
    ] {
        let prompt = resolve_prompt_builder(builder).build(&ctx);
        assert!(prompt.user.starts_with("No change since review 2\n"));
    }
}

#[test]
fn coder_prompt_ci_follow_up_stays_bounded_with_many_failing_steps() {
    // Every failing step keeps its command and signatures; stored tails
    // (4 KiB each) stop once the prompt's output budget is spent.
    let mut ctx = fake_context(default_roles::CODER);
    let steps: Vec<_> = (0..12)
        .map(|n| json!({"index":n,"command":format!("step-{n}"),"exit_code":1,"output_tail":"x".repeat(4096),"failures":[format!("signature-{n}")]}))
        .collect();
    ctx.prior_reviews = vec![db::Review {
        step_results_json: json!({ "ci_steps": steps }).to_string(),
        ..fake_review(1, db::ReviewStatus::Failed)
    }];
    for builder in [
        BUILDER_ID_CODER_REVIEW_FIX_V2,
        BUILDER_ID_WORKER_REVIEW_FIX_V1,
    ] {
        let prompt = resolve_prompt_builder(builder).build(&ctx);
        for n in 0..12 {
            assert!(prompt.user.contains(&format!("step-{n}")));
            assert!(prompt.user.contains(&format!("signature-{n}")));
        }
        assert!(prompt.user.len() < 16 * 1024, "{}", prompt.user.len());
    }
}

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
    ctx.repair_budget = Some(super::repair_brief::RepairBudget {
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
    let text = super::repair_brief::RepairBrief::review(&ctx).render();
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
        ctx.task.condition = db::map_legacy_condition(&db::LegacyConditionInput::from(&ctx.task));
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
    use super::repair_brief::{RepairBrief, RepairBudget};
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
        ctx.task.condition = db::map_legacy_condition(&db::LegacyConditionInput::from(&ctx.task));
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
    use super::repair_brief::{RepairBrief, RepairBudget};
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
    use super::repair_brief::{RepairAttempt, RepairBrief};
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
    use super::repair_brief::{RepairAttempt, RepairBrief};
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
