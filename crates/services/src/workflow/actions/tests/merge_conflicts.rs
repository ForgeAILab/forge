//! Workflow action merge conflicts tests.

use super::*;

#[tokio::test]
async fn coordination_root_merge_conflict_becomes_manual_repair_block() {
    let ctx = build_test_ctx(
        "task-root-merge-conflict",
        default_states::MERGING,
        default_states::MERGING,
        None,
    )
    .await;
    let now = now_rfc3339();
    TaskRepo::create(
        &*ctx.db,
        CreateTask {
            id: new_uuid_v4(),
            project_id: ctx.project_id.clone(),
            parent_task_id: Some(ctx.task_id.clone()),
            subtask_order: Some(0),
            assignee_type: None,
            assignee_id: None,
            title: "completed child".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: default_states::DONE.to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("coordination child creates");
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("root loads")
        .expect("root exists");

    let result = merge_failure_result(
        &ctx,
        &task,
        "merge conflict in shared branch".to_owned(),
        api_types::FailureKind::MergeConflict,
    )
    .await;

    assert!(matches!(result, HookResult::Ok));
    let blocked = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("blocked root loads")
        .expect("blocked root exists");
    assert!(blocked.blocked_json.is_some());
    let annotation: api_types::TaskAnnotation = serde_json::from_str(
        blocked
            .error_annotation
            .as_deref()
            .expect("root has recovery annotation"),
    )
    .expect("recovery annotation parses");
    let api_types::TaskAnnotation::Blocking(annotation) = annotation else {
        panic!("coordination merge block must be typed")
    };
    assert_eq!(annotation.blocked_by.as_deref(), Some("coordination_root"));
    assert!(serde_json::to_value(&annotation)
        .unwrap()
        .get("recovery_actions")
        .is_none());
}

#[tokio::test]
async fn ordinary_merge_conflict_parks_for_manual_repair_and_invalidates_review() {
    let ctx = build_test_ctx(
        "task-merge-conflict",
        default_states::MERGING,
        default_states::MERGING,
        None,
    )
    .await;
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let task = TaskRepo::set_review_passed_at_cas(
        &*ctx.db,
        &task.id,
        task.version,
        Some("2026-09-12T10:00:00Z".to_owned()),
        &now_rfc3339(),
    )
    .await
    .expect("review marker sets");

    let result = merge_failure_result(
        &ctx,
        &task,
        "conflict in src/lib.rs".to_owned(),
        api_types::FailureKind::MergeConflict,
    )
    .await;

    assert!(matches!(result, HookResult::Ok));
    let blocked = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("blocked task loads")
        .expect("blocked task exists");
    assert!(blocked.blocked_json.is_some());
    assert_eq!(blocked.review_passed_at, None);
    let annotation: api_types::TaskAnnotation = serde_json::from_str(
        blocked
            .error_annotation
            .as_deref()
            .expect("task has recovery annotation"),
    )
    .expect("recovery annotation parses");
    let api_types::TaskAnnotation::Blocking(annotation) = annotation else {
        panic!("merge conflict block must be typed")
    };
    assert_eq!(
        annotation.blocked_by.as_deref(),
        Some("manual_workspace_repair")
    );
    assert!(serde_json::to_value(&annotation)
        .unwrap()
        .get("recovery_actions")
        .is_none());
}

#[tokio::test]
async fn conflict_handoff_preserves_later_merge_fix_follow_up() {
    let ctx = build_test_ctx(
        "task-handoff-then-dirty",
        default_states::MERGING,
        default_states::MERGING,
        None,
    )
    .await;
    record_conflict_handoff(
        &ctx,
        "rebased onto main; conflicts were committed with markers in: exports.py".to_owned(),
        true,
    )
    .await;
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let result = merge_failure_result(
        &ctx,
        &task,
        "worktree has uncommitted changes".to_owned(),
        api_types::FailureKind::DirtyWorktree,
    )
    .await;
    assert!(matches!(result, HookResult::Cascade { to, .. } if to == default_states::MERGE_FAILED));
    let current = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert!(
        current.blocked_json.is_none(),
        "later merge failure gets its follow-up"
    );
    seed_transition_log(
        &ctx.db,
        &ctx.task_id,
        default_states::MERGING,
        default_states::MERGE_FAILED,
        true,
    )
    .await;
    let budget_result = super::merge::CheckMergeFixBudget.execute(&ctx).await;
    assert!(matches!(budget_result, HookResult::Ok), "{budget_result:?}");
}

// Ported to the queue path from the test of the same name: the rebase is the
// queue head's, the handoff its `send_back` step's.
#[tokio::test]
async fn rebase_conflict_is_handed_back_to_the_worker_with_committed_markers() {
    let (ctx, _dir, worktree_path, handoff) = queue_conflict_handoff("task-conflict-handoff").await;
    assert!(
        handoff.bridge.bridge_kind == Some(api_types::TransitionBridgeKind::ConflictHandoff),
        "{}",
        handoff.trigger_reason
    );
    assert!(
        !handoff.bridge.is_review_refresh(),
        "a conflict needs the Worker, not a bare re-review: {}",
        handoff.trigger_reason
    );
    assert!(
        handoff.trigger_reason.contains("exports.py"),
        "{}",
        handoff.trigger_reason
    );
    assert_eq!(
        handoff.bridge.conflict_paths(),
        Some(vec!["exports.py".to_owned()])
    );

    // The branch sits on the new target, fully rebased, with the conflict
    // committed for the Worker to reconcile by editing files.
    let status = run_git(&worktree_path, &["status", "--porcelain"]);
    assert!(status.is_empty(), "worktree is clean: {status}");
    assert!(!git::detect_rebase_in_progress(&worktree_path)
        .await
        .expect("rebase state reads"));
    let exports = std::fs::read_to_string(worktree_path.join("exports.py")).expect("exports reads");
    assert!(exports.contains("<<<<<<< ") && exports.contains(">>>>>>> "));
    assert!(worktree_path.join("api.py").exists());
    assert_eq!(
        run_git(&worktree_path, &["merge-base", "HEAD", "main"]),
        run_git(&worktree_path, &["rev-parse", "main"]),
    );

    let current = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert_eq!(current.status, default_states::MERGE_FAILED);
    assert!(current.blocked_json.is_none(), "nothing parks for a human");
    let annotation: serde_json::Value = serde_json::from_str(
        current
            .error_annotation
            .as_deref()
            .expect("the conflict is annotated for the Worker prompt"),
    )
    .expect("annotation parses");
    assert_eq!(annotation["type"], json!("merge_conflict"));
    // The handoff is one charge of the conflict-handoff allowance and one
    // comment naming the files.
    assert_eq!(
        db::budget::spent(
            ctx.db.pool(),
            &ctx.task_id,
            db::budget::Kind::ConflictHandoff.key()
        )
        .await
        .expect("budget reads"),
        1
    );
    let comments: Vec<String> = sqlx::query_scalar(
        "SELECT content FROM task_comment WHERE task_id = ? AND content LIKE 'Merge conflict handed back to the Worker:%'",
    )
    .bind(&ctx.task_id)
    .fetch_all(ctx.db.pool())
    .await
    .expect("comments load");
    assert_eq!(comments.len(), 1, "{comments:?}");
    assert!(comments[0].contains("exports.py"), "{comments:?}");
}

// Ported: the marker block is admission's now. A Task whose Worker left the
// handed-off markers in place is blocked at the entry and never queued.
#[tokio::test]
async fn run_merge_blocks_unresolved_handed_off_markers() {
    let (ctx, _dir, _worktree_path, _handoff) =
        queue_conflict_handoff("task-handoff-marker-block").await;
    let ejected = current_attempt(&ctx).await;
    assert_eq!(ejected.state, db::IntegrationAttemptState::Ejected);
    reenter_merging(&ctx).await;

    let result = run_merge_entry_hook(&ctx).await;
    assert!(matches!(result, HookResult::Ok), "{result:?}");
    let blocked = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert!(blocked
        .blocked_json
        .as_deref()
        .is_some_and(|value| value.contains("unresolved Git conflict markers")));
    // Nothing was admitted: the candidate still carries the markers.
    let after = current_attempt(&ctx).await;
    assert_eq!(after.id, ejected.id);
    assert_eq!(after.state, db::IntegrationAttemptState::Ejected);
}

// Ported: the guard reads the handoff the queue's `send_back` step recorded.
#[tokio::test]
async fn merge_failed_exit_guard_rejects_unresolved_markers_before_review() {
    let (mut ctx, _dir, worktree_path, _handoff) =
        queue_conflict_handoff("task-handoff-exit-guard").await;
    ctx.from_state = default_states::MERGE_FAILED.to_owned();
    ctx.to_state = default_states::REVIEW.to_owned();
    ctx.triggered_by = api_types::Actor::system(api_types::SystemComponent::Workflow);

    // The Worker has not touched the marked files yet.
    let HookResult::Failed { reason } = super::merge::RequireConflictMarkersResolved
        .execute(&ctx)
        .await
    else {
        panic!("committed markers must keep the task out of review");
    };
    assert!(reason.contains("exports.py"), "{reason}");

    // Other exits from merge_failed are not this guard's concern.
    let mut other = ctx.clone();
    other.to_state = default_states::IN_PROGRESS.to_owned();
    assert!(matches!(
        super::merge::RequireConflictMarkersResolved
            .execute(&other)
            .await,
        HookResult::Skipped { .. }
    ));

    // A repair that is committed clears the guard.
    std::fs::write(worktree_path.join("exports.py"), "resolved = True\n").expect("repair writes");
    git::commit_all(&worktree_path, "resolve handoff")
        .await
        .expect("repair commits");
    let result = super::merge::RequireConflictMarkersResolved
        .execute(&ctx)
        .await;
    assert!(matches!(result, HookResult::Ok), "{result:?}");
}

// Ported: the repaired candidate is admitted again (the ejected attempt is
// superseded, keeping its place) and the queue fast-forwards it.
#[tokio::test]
async fn handed_off_conflict_resolves_and_run_merge_integrates() {
    let (ctx, dir, worktree_path, _handoff) = queue_conflict_handoff("task-handoff-resolved").await;
    let ejected = current_attempt(&ctx).await;
    std::fs::write(
        worktree_path.join("exports.py"),
        "core = 1\ncli = 1\napi = 1\n",
    )
    .expect("Worker resolves both sides");
    run_git(&worktree_path, &["add", "-A"]);
    run_git(&worktree_path, &["commit", "-m", "resolve handoff"]);
    reenter_merging(&ctx).await;

    let result = run_merge_entry_hook(&ctx).await;
    assert!(matches!(result, HookResult::Ok), "{result:?}");
    let admitted = current_attempt(&ctx).await;
    assert_eq!(
        admitted.predecessor_attempt_id.as_deref(),
        Some(ejected.id.as_str())
    );
    assert_eq!(
        admitted.queue_seq, ejected.queue_seq,
        "the Task keeps its place"
    );
    ctx.task_service
        .drain(&ctx.task_id)
        .await
        .expect("the queue merges the repair");
    let done = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert_eq!(done.status, default_states::DONE);
    assert_eq!(
        std::fs::read_to_string(dir.path().join("repo/exports.py"))
            .expect("integrated exports read"),
        "core = 1\ncli = 1\napi = 1\n"
    );
}

// Ported: the conflict-handoff allowance is the budget ledger's now. With it
// spent, the next conflict escalates instead of going to the Worker, in one
// Task write.
#[tokio::test]
async fn repeated_conflict_handoffs_escalate_in_a_single_task_write() {
    let mut ctx = build_test_ctx(
        "task-conflict-handoff-exhausted",
        default_states::MERGING,
        default_states::MERGING,
        None,
    )
    .await;
    let (dir, _worktree_path) = seed_sibling_conflict_workspace(&ctx).await;
    ctx.merge_service = Some(Arc::new(crate::merge_service::MergeService::new_for_test(
        Arc::clone(&ctx.db),
        dir.path().to_path_buf(),
    )));
    let limit = db::budget::Kind::ConflictHandoff.default_limit() as i64;
    let mut tx = ctx.db.pool().begin().await.expect("tx");
    for n in 0..limit {
        db::budget::admit(
            &mut tx,
            &ctx.task_id,
            db::budget::Kind::ConflictHandoff.key(),
            &format!("prior-handoff-{n}"),
        )
        .await
        .expect("prior handoff charges");
    }
    tx.commit().await.expect("commit");

    let result = RunMerge.execute(&ctx).await;
    assert!(matches!(result, HookResult::Ok), "{result:?}");
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    ctx.task_service
        .drain(&ctx.task_id)
        .await
        .expect("the queue escalates the conflict");

    let blocked = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert_eq!(blocked.status, default_states::MERGING, "no Worker handoff");
    assert_eq!(
        blocked.version,
        task.version + 1,
        "block and annotation land in one write, so one interruption event wakes the Project Agent"
    );
    assert!(blocked.blocked_json.is_some());
    let annotation: api_types::TaskAnnotation = serde_json::from_str(
        blocked
            .error_annotation
            .as_deref()
            .expect("escalation is annotated"),
    )
    .expect("annotation parses");
    let api_types::TaskAnnotation::Blocking(annotation) = annotation else {
        panic!("escalation must be typed")
    };
    assert_eq!(
        annotation.blocked_by.as_deref(),
        Some("manual_workspace_repair")
    );
    assert!(db::TransitionLogRepo::list_by_task(&*ctx.db, &ctx.task_id)
        .await
        .expect("transitions load")
        .iter()
        .all(|entry| entry.to_state != default_states::MERGE_FAILED));
}

/// A candidate with no agent review contract (a human reviewer, or no
/// reviewer) merges without a pinned object, so it meets a sibling's change as
/// a plain merge conflict. That conflict is still the Worker's to reconcile.
#[tokio::test]
async fn plain_merge_conflict_is_handed_back_to_the_worker() {
    let mut ctx = build_test_ctx(
        "task-plain-merge-conflict",
        default_states::MERGING,
        default_states::MERGING,
        None,
    )
    .await;
    let (dir, worktree_path) = seed_sibling_conflict_workspace(&ctx).await;
    ctx.merge_service = Some(Arc::new(crate::merge_service::MergeService::new_for_test(
        Arc::clone(&ctx.db),
        dir.path().to_path_buf(),
    )));

    // The entry hook only admits the Task: it finds no conflict itself.
    let result = RunMerge.execute(&ctx).await;
    assert!(matches!(result, HookResult::Ok), "{result:?}");
    let admitted = current_attempt(&ctx).await;
    assert_eq!(admitted.state, db::IntegrationAttemptState::Queued);

    // The queue's rebase meets the sibling's change, commits the conflict
    // with its markers and releases the slot; its `send_back` step hands the
    // files to the Worker.
    ctx.task_service
        .drain(&ctx.task_id)
        .await
        .expect("the queue hands the conflict back");
    let handoff = db::TransitionLogRepo::list_by_task(&*ctx.db, &ctx.task_id)
        .await
        .expect("transitions load")
        .into_iter()
        .find(|entry| {
            entry.from_state == default_states::MERGING
                && entry.to_state == default_states::MERGE_FAILED
        })
        .expect("a plain merge conflict goes back to the Worker");
    assert!(
        handoff.bridge.bridge_kind == Some(api_types::TransitionBridgeKind::ConflictHandoff),
        "{}",
        handoff.trigger_reason
    );
    assert_eq!(
        handoff.bridge.conflict_paths(),
        Some(vec!["exports.py".to_owned()])
    );
    let exports = std::fs::read_to_string(worktree_path.join("exports.py")).expect("exports reads");
    assert!(exports.contains("<<<<<<< ") && exports.contains(">>>>>>> "));
    let current = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert!(current.blocked_json.is_none(), "nothing parks for a human");
    assert_eq!(current.review_passed_at, None);
    assert_eq!(
        db::IntegrationQueueRepo::integration_attempt(&*ctx.db, &admitted.id)
            .await
            .expect("attempt loads")
            .expect("attempt exists")
            .state,
        db::IntegrationAttemptState::Ejected
    );
}
