//! Workflow action review authority carry tests.

use super::*;

#[tokio::test]
async fn review_digest_completion_uses_the_frozen_fingerprint_version() {
    for (version, change, expected) in [
        (2, "settings", api_types::ConformanceStatus::Passed),
        (2, "description", api_types::ConformanceStatus::Unverified),
        (1, "unchanged", api_types::ConformanceStatus::Passed),
        (1, "settings", api_types::ConformanceStatus::Unverified),
    ] {
        let (harness, scenario) =
            build_carry_scenario("task-digest-completion", "agent-digest-completion", false).await;
        let ctx = &harness.ctx;
        let execution_id = new_uuid_v4();
        create_carry_execution(ctx, &execution_id, "reviewer", &scenario.workspace_id, true).await;
        let source = ctx
            .db
            .review_source(&ctx.task_id, Some(&execution_id))
            .await
            .unwrap();
        let mut context = ::review::contract::context_from_source(&source).unwrap();
        assert_eq!(
            context.source_digest_version,
            Some(2),
            "new admissions use v2"
        );
        if version == 1 {
            // Simulate an in-flight pre-upgrade contract, without a version field.
            context.source_digest_version = None;
            context.source_digest = api_types::canonical_digest(&source).unwrap();
        }
        let mut contract = api_types::ReviewContract {
            execution_id: execution_id.clone(),
            policy: api_types::REVIEW_CONFORMANCE_POLICY.into(),
            commit_sha: scenario.commit_sha.clone(),
            base_sha: run_git(&scenario.worktree, &["rev-parse", "HEAD~1"]),
            candidate_changed_paths: scenario.changed_paths.clone(),
            context,
            check_results: Vec::new(),
            digest: String::new(),
        };
        contract.digest = api_types::canonical_digest(&contract).unwrap();
        ctx.db.create_review_contract(&contract).await.unwrap();
        match change {
            "settings" => {
                sqlx::query("UPDATE project SET settings = json_set(settings, '$.max_active_tasks', 7, '$.recheck_interval_seconds', 30) WHERE id = ?")
                    .bind(&ctx.project_id).execute(ctx.db.pool()).await.unwrap();
            }
            "description" => {
                sqlx::query(
                    "UPDATE task SET description = 'Changed acceptance criteria' WHERE id = ?",
                )
                .bind(&ctx.task_id)
                .execute(ctx.db.pool())
                .await
                .unwrap();
            }
            "unchanged" => {}
            _ => unreachable!(),
        }
        let conformance = ::review::contract::evaluate(
            &ctx.db,
            &execution_id,
            &scenario.worktree,
            r#"{"result":"pass","reason":"verified candidate"}"#,
        )
        .await
        .unwrap();
        assert_eq!(
            conformance.status, expected,
            "v{version} {change}: {:?}",
            conformance.reason
        );
    }
}

/// The target moved under a reviewed candidate. Before the queue the entry
/// hook rebased the branch and bounced the Task through `merge_failed` and
/// `review`. Now it only admits the Task: nothing is rebased by the hook, no
/// transition is asked for and the passed review stays; the queue rebases
/// the candidate when it reaches the head.
#[tokio::test]
async fn a_moved_target_admits_the_task_instead_of_bouncing_it() {
    let (harness, scenario) =
        build_carry_scenario("task-queue-admit", "agent-queue-admit", false).await;
    let ctx = harness.ctx.clone();
    let result = RunMerge.execute(&ctx).await;
    assert!(matches!(result, HookResult::Ok), "{result:?}");
    assert_eq!(
        run_git(&scenario.worktree, &["rev-parse", "HEAD"]),
        scenario.commit_sha,
        "the hook rebases nothing"
    );
    let attempt = current_attempt(&ctx).await;
    assert_eq!(attempt.state, db::IntegrationAttemptState::Queued);
    assert_eq!(
        attempt.original_candidate_sha.as_deref(),
        Some(scenario.commit_sha.as_str())
    );
    assert_eq!(
        attempt.workspace_id.as_deref(),
        Some(scenario.workspace_id.as_str())
    );
    assert!(attempt.placement_id.is_some() && attempt.repo_location_id.is_some());
    assert_eq!(attempt.owner_kind, Some(db::IntegrationOwnerKind::Server));
    assert!(attempt.placement_generation.is_some());
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert!(task.review_passed_at.is_some(), "the passed review stays");
    // A hook that runs again for the same entry gets the same row.
    let again = RunMerge.execute(&ctx).await;
    assert!(matches!(again, HookResult::Ok), "{again:?}");
    assert_eq!(current_attempt(&ctx).await.id, attempt.id);
}

#[tokio::test]
async fn clean_rebase_carries_review_authority_and_merges_without_a_reviewer() {
    let (harness, scenario) =
        build_carry_scenario("task-carry-clean", "agent-carry-clean", false).await;
    let mut ctx = harness.ctx.clone();
    let reviewers_before = reviewer_execution_count(&ctx).await;

    let (reason, bridge) = pre_queue_bounce(&ctx).await;
    assert!(
        bridge.bridge_kind == Some(api_types::TransitionBridgeKind::TargetMovedRebase),
        "{reason}"
    );
    let rebased_head = run_git(&scenario.worktree, &["rev-parse", "HEAD"]);
    assert_ne!(rebased_head, scenario.commit_sha, "the branch was rebased");

    enter_review_after_bridge(
        &mut ctx,
        &reason,
        &bridge,
        api_types::Actor::system(api_types::SystemComponent::Workflow),
        CARRY_REFRESH_REASON,
        json!(["test -d ."]),
    )
    .await;
    let ci = run_ci(&ctx).await;
    assert!(matches!(ci, HookResult::Ok), "{ci:?}");

    let carried = super::CarryReviewAuthority.execute(&ctx).await;
    let HookResult::Cascade { to, reason, bridge } = carried else {
        panic!("a clean rebase with passing checks carries the review, got {carried:?}");
    };
    assert_eq!(to, default_states::MERGING);
    assert!(
        bridge.bridge_kind == Some(api_types::TransitionBridgeKind::ReviewCarry),
        "{reason}"
    );

    assert_eq!(reviewer_execution_count(&ctx).await, reviewers_before);
    let reviews = ReviewRepo::list_by_task(&*ctx.db, &ctx.task_id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 2);
    assert!(reviews
        .iter()
        .all(|review| review.status == ReviewStatus::Passed));
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert!(task.review_passed_at.is_some());
    let rows = carry_rows(&ctx).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1, rebased_head);
    assert_eq!(
        rows[0].2,
        run_git(&scenario.repo_path, &["rev-parse", "main"])
    );
    assert_eq!(rows[0].3, "clean_rebase");

    // Back in `merging` the Task is admitted to the queue with the carried
    // commit as its candidate; the entry hook merges nothing itself.
    let admitted = merge_again_from_review(&mut ctx).await;
    assert!(matches!(admitted, HookResult::Ok), "{admitted:?}");
    let attempt = current_attempt(&ctx).await;
    assert_eq!(attempt.state, db::IntegrationAttemptState::Queued);
    assert_eq!(
        attempt.original_candidate_sha.as_deref(),
        Some(rebased_head.as_str())
    );
    assert_ne!(
        run_git(&scenario.repo_path, &["rev-parse", "main"]),
        rebased_head,
        "admission performs no Git effect"
    );
}

#[tokio::test]
async fn conflict_repair_by_the_worker_carries_review_authority() {
    let (harness, scenario) =
        build_carry_scenario("task-carry-conflict", "agent-carry-conflict", true).await;
    let mut ctx = harness.ctx.clone();
    let reviewers_before = reviewer_execution_count(&ctx).await;

    let (reason, bridge) = pre_queue_bounce(&ctx).await;
    assert!(
        bridge.bridge_kind == Some(api_types::TransitionBridgeKind::ConflictHandoff),
        "{reason}"
    );
    // The Worker reconciles only the handed-off file and commits.
    std::fs::write(
        scenario.worktree.join("shared.txt"),
        "core = 1\ncli = 1\napi = 1\n",
    )
    .expect("Worker resolves both sides");
    run_git(&scenario.worktree, &["add", "-A"]);
    run_git(&scenario.worktree, &["commit", "-m", "resolve handoff"]);
    let repaired_head = run_git(&scenario.worktree, &["rev-parse", "HEAD"]);

    enter_review_after_bridge(
        &mut ctx,
        &reason,
        &bridge,
        api_types::Actor::system(api_types::SystemComponent::Test),
        "worker completed the merge repair",
        json!(["test -d ."]),
    )
    .await;
    let ci = run_ci(&ctx).await;
    assert!(matches!(ci, HookResult::Ok), "{ci:?}");

    let carried = super::CarryReviewAuthority.execute(&ctx).await;
    assert!(
        matches!(&carried, HookResult::Cascade { to, .. } if to == default_states::MERGING),
        "{carried:?}"
    );
    assert_eq!(reviewer_execution_count(&ctx).await, reviewers_before);
    let rows = carry_rows(&ctx).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1, repaired_head);
    assert_eq!(rows[0].3, "conflict_repair");

    let admitted = merge_again_from_review(&mut ctx).await;
    assert!(matches!(admitted, HookResult::Ok), "{admitted:?}");
    let attempt = current_attempt(&ctx).await;
    assert_eq!(attempt.state, db::IntegrationAttemptState::Queued);
    assert_eq!(
        attempt.original_candidate_sha.as_deref(),
        Some(repaired_head.as_str()),
        "the Worker's repair is the candidate the queue is given"
    );
}

#[tokio::test]
async fn repair_touching_a_new_path_gets_a_full_review() {
    let (mut harness, scenario) =
        build_carry_scenario("task-carry-scope", "agent-carry-scope", true).await;
    let mut ctx = harness.ctx.clone();
    let reviewers_before = reviewer_execution_count(&ctx).await;
    let (reason, bridge) = pre_queue_bounce(&ctx).await;
    std::fs::write(
        scenario.worktree.join("shared.txt"),
        "core = 1\ncli = 1\napi = 1\n",
    )
    .expect("Worker resolves both sides");
    std::fs::write(scenario.worktree.join("extra.txt"), "not reviewed\n")
        .expect("Worker adds a file");
    run_git(&scenario.worktree, &["add", "-A"]);
    run_git(&scenario.worktree, &["commit", "-m", "resolve and extend"]);

    enter_review_after_bridge(
        &mut ctx,
        &reason,
        &bridge,
        api_types::Actor::system(api_types::SystemComponent::Test),
        "worker completed the merge repair",
        json!(["test -d ."]),
    )
    .await;
    let ci = run_ci(&ctx).await;
    assert!(matches!(ci, HookResult::Ok), "{ci:?}");

    let carried = super::CarryReviewAuthority.execute(&ctx).await;
    let HookResult::Skipped { reason } = carried else {
        panic!("a path outside the reviewed change set must not carry, got {carried:?}");
    };
    assert!(reason.contains("extra.txt"), "{reason}");
    assert!(carry_rows(&ctx).await.is_empty());

    // The ordinary reviewer dispatch follows.
    let dispatched = DispatchRoleAgent.execute(&ctx).await;
    assert!(matches!(dispatched, HookResult::Ok), "{dispatched:?}");
    tokio::time::timeout(std::time::Duration::from_secs(1), harness.rx.recv())
        .await
        .expect("reviewer executor spawned in time")
        .expect("reviewer execution context received");
    assert_eq!(reviewer_execution_count(&ctx).await, reviewers_before + 1);
}

#[tokio::test]
async fn no_configured_checks_means_a_full_review() {
    let (harness, _scenario) =
        build_carry_scenario("task-carry-no-ci", "agent-carry-no-ci", false).await;
    let mut ctx = harness.ctx.clone();
    let reviewers_before = reviewer_execution_count(&ctx).await;
    let (reason, bridge) = pre_queue_bounce(&ctx).await;
    enter_review_after_bridge(
        &mut ctx,
        &reason,
        &bridge,
        api_types::Actor::system(api_types::SystemComponent::Workflow),
        CARRY_REFRESH_REASON,
        json!([]),
    )
    .await;

    let carried = super::CarryReviewAuthority.execute(&ctx).await;
    let HookResult::Skipped { reason } = carried else {
        panic!("nothing verified the rebased tree, got {carried:?}");
    };
    assert!(reason.contains("no ci_steps"), "{reason}");
    assert!(carry_rows(&ctx).await.is_empty());
    assert_eq!(reviewer_execution_count(&ctx).await, reviewers_before);
}

#[tokio::test]
async fn changed_governing_context_means_a_full_review() {
    let (harness, _scenario) =
        build_carry_scenario("task-carry-context", "agent-carry-context", false).await;
    let mut ctx = harness.ctx.clone();
    let (reason, bridge) = pre_queue_bounce(&ctx).await;
    sqlx::query("UPDATE task SET description = 'a different acceptance scope' WHERE id = ?")
        .bind(&ctx.task_id)
        .execute(ctx.db.pool())
        .await
        .expect("task scope changes");
    enter_review_after_bridge(
        &mut ctx,
        &reason,
        &bridge,
        api_types::Actor::system(api_types::SystemComponent::Workflow),
        CARRY_REFRESH_REASON,
        json!(["test -d ."]),
    )
    .await;
    let ci = run_ci(&ctx).await;
    assert!(matches!(ci, HookResult::Ok), "{ci:?}");

    let carried = super::CarryReviewAuthority.execute(&ctx).await;
    let HookResult::Skipped { reason } = carried else {
        panic!("changed Task scope must not ride an old approval, got {carried:?}");
    };
    assert!(reason.contains("stale"), "{reason}");
    assert!(carry_rows(&ctx).await.is_empty());
}

#[tokio::test]
async fn other_review_entries_never_carry() {
    let (harness, _scenario) =
        build_carry_scenario("task-carry-entry", "agent-carry-entry", false).await;
    let mut ctx = harness.ctx.clone();
    let (reason, bridge) = pre_queue_bounce(&ctx).await;
    // A user moving the Task is never mechanical.
    enter_review_after_bridge(
        &mut ctx,
        &reason,
        &bridge,
        api_types::Actor::user(api_types::UserActionSource::Api),
        CARRY_REFRESH_REASON,
        json!(["test -d ."]),
    )
    .await;
    let carried = super::CarryReviewAuthority.execute(&ctx).await;
    assert!(matches!(carried, HookResult::Skipped { .. }), "{carried:?}");

    // Neither is an entry whose bridge was not a Forge rebase/handoff.
    let entries = db::TransitionLogRepo::list_by_task(&*ctx.db, &ctx.task_id)
        .await
        .expect("entries load");
    assert_eq!(
        crate::workflow::review_carry_entry_kind(&entries[..entries.len() - 1], &ctx.workflow),
        None,
        "a bridge alone is not an entry into review"
    );
}

#[tokio::test]
async fn carry_for_an_old_review_is_ignored_after_a_newer_real_review() {
    let (harness, scenario) =
        build_carry_scenario("task-carry-superseded", "agent-carry-superseded", false).await;
    let mut ctx = harness.ctx.clone();
    let (reason, bridge) = pre_queue_bounce(&ctx).await;
    let rebased_head = run_git(&scenario.worktree, &["rev-parse", "HEAD"]);
    enter_review_after_bridge(
        &mut ctx,
        &reason,
        &bridge,
        api_types::Actor::system(api_types::SystemComponent::Workflow),
        CARRY_REFRESH_REASON,
        json!(["test -d ."]),
    )
    .await;
    assert!(matches!(run_ci(&ctx).await, HookResult::Ok));
    assert!(matches!(
        super::CarryReviewAuthority.execute(&ctx).await,
        HookResult::Cascade { .. }
    ));

    let guard = ctx
        .db
        .lock_review_integration(&ctx.task_id)
        .await
        .expect("carried review authorises integration");
    let candidate = guard.candidate.clone().expect("a reviewed candidate");
    guard.release().await.expect("guard releases");
    assert_eq!(candidate.commit_sha, rebased_head);

    // A newer real review of a later commit supersedes the carry.
    std::fs::write(scenario.worktree.join("feature.txt"), "feature v2\n").expect("edit writes");
    run_git(&scenario.worktree, &["add", "-A"]);
    run_git(&scenario.worktree, &["commit", "-m", "second pass"]);
    let later_head = run_git(&scenario.worktree, &["rev-parse", "HEAD"]);
    let base = run_git(&scenario.repo_path, &["rev-parse", "main"]);
    let mut carried_scenario = CarryScenario {
        worktree: scenario.worktree.clone(),
        repo_path: scenario.repo_path.clone(),
        workspace_id: scenario.workspace_id.clone(),
        executor_execution_id: scenario.executor_execution_id.clone(),
        commit_sha: later_head.clone(),
        changed_paths: scenario.changed_paths.clone(),
    };
    carried_scenario.changed_paths.sort();
    seed_passed_carry_review(
        &ctx,
        &carried_scenario,
        &later_head,
        &base,
        &carried_scenario.changed_paths,
        3,
    )
    .await;

    let guard = ctx
        .db
        .lock_review_integration(&ctx.task_id)
        .await
        .expect("the newer review authorises integration");
    let candidate = guard.candidate.clone().expect("a reviewed candidate");
    guard.release().await.expect("guard releases");
    assert_eq!(candidate.commit_sha, later_head);
    assert_eq!(candidate.base_sha, base);
    assert_eq!(carry_rows(&ctx).await.len(), 1, "history stays intact");
}

#[tokio::test]
async fn carries_are_bounded_per_review() {
    let (harness, scenario) =
        build_carry_scenario("task-carry-bound", "agent-carry-bound", false).await;
    let mut ctx = harness.ctx.clone();
    let (reason, bridge) = pre_queue_bounce(&ctx).await;
    let contract_execution_id: String =
        sqlx::query_scalar("SELECT execution_id FROM execution_review_contract WHERE task_id = ?")
            .bind(&ctx.task_id)
            .fetch_one(ctx.db.pool())
            .await
            .expect("contract execution loads");
    for _ in 0..super::carry::MAX_REVIEW_CARRIES {
        sqlx::query(
            "INSERT INTO review_authority_carry
                (id, task_id, contract_execution_id, commit_sha, base_sha, kind,
                 changed_paths_json, created_at)
             VALUES (?, ?, ?, ?, ?, 'clean_rebase', '[]', ?)",
        )
        .bind(new_uuid_v4())
        .bind(&ctx.task_id)
        .bind(&contract_execution_id)
        .bind(&scenario.commit_sha)
        .bind(&scenario.commit_sha)
        .bind(now_rfc3339())
        .execute(ctx.db.pool())
        .await
        .expect("carry row seeds");
    }
    sqlx::query("INSERT INTO task_budget(task_id,kind,window_id,spent) VALUES(?,'review_carry',?,5) ON CONFLICT(task_id,kind) DO UPDATE SET spent=5,window_id=excluded.window_id").bind(&ctx.task_id).bind(&contract_execution_id).execute(ctx.db.pool()).await.unwrap();
    enter_review_after_bridge(
        &mut ctx,
        &reason,
        &bridge,
        api_types::Actor::system(api_types::SystemComponent::Workflow),
        CARRY_REFRESH_REASON,
        json!(["test -d ."]),
    )
    .await;
    assert!(matches!(run_ci(&ctx).await, HookResult::Ok));

    let carried = super::CarryReviewAuthority.execute(&ctx).await;
    let HookResult::Skipped { reason } = carried else {
        panic!("the carry bound must force a real review, got {carried:?}");
    };
    assert!(reason.contains("already carried"), "{reason}");
}

#[tokio::test]
async fn hook_block_rebases_a_content_edit_under_its_lease_without_cas_retry() {
    use db::TaskStepRepo;
    let ctx = build_test_ctx("block-content-race", "in_progress", "in_progress", None).await;
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .unwrap()
        .unwrap();
    let id = new_uuid_v4();
    ctx.db
        .enqueue_step(&db::EnqueueTaskStep {
            id: id.clone(),
            task_id: task.id.clone(),
            kind: "hooks".into(),
            payload_json: "{}".into(),
            causation_step_id: None,
            causation_key: id.clone(),
            chain_id: id,
            chain_position: 1,
            expected_status: task.status.clone(),
            expected_version: task.version,
            expected_epoch: None,
            lane: "fast".into(),
            available_at: now_rfc3339(),
        })
        .await
        .unwrap();
    let step = ctx
        .db
        .claim_step(
            "hook",
            Some(&task.id),
            &(chrono::Utc::now() + chrono::Duration::seconds(60)).to_rfc3339(),
        )
        .await
        .unwrap()
        .unwrap();
    sqlx::query("UPDATE task SET title='owner edit',version=version+1 WHERE id=?")
        .bind(&task.id)
        .execute(ctx.db.pool())
        .await
        .unwrap();
    db::task_writer::in_task_step(
        step.clone(),
        super::common::block_task_with_annotation(
            &ctx,
            &task,
            "CI failed",
            api_types::FailureKind::CiFailed,
            Some("CI"),
            Some(json!({"type":"ci_failed"}).to_string()),
        ),
    )
    .await
    .unwrap();
    let result = TaskRepo::get_by_id(&*ctx.db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.title, "owner edit");
    assert!(result
        .blocked_json
        .as_deref()
        .unwrap()
        .contains("CI failed"));
    assert_eq!(result.version, task.version + 2);
    let mut tx = db::begin_immediate(ctx.db.pool()).await.unwrap();
    ctx.db
        .finish_step_in_tx(&mut tx, &step, "done", None)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    ctx.db.release_step(&step.id, "hook").await.unwrap();
    assert_eq!(ctx.db.task_steps(&task.id).await.unwrap()[0].attempts, 1);
}
