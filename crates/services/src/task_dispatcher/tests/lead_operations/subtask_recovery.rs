//! Tests for subtask recovery in lead operations.

use super::*;
/// End to end, the park ends by the lead's own operation: a subtask out of
/// retries sends the root to planning, the lead's run retries it, the root
/// leaves planning and the recovered subtask runs. Recovery is bounded per
/// subtask on the root's ledger.
#[tokio::test]
async fn a_lead_recovers_a_parked_subtask_and_it_runs_again() {
    let ops = ops_fixture(true).await;
    let lead = &ops.lead;
    let (root, child, later) = root_with_parked_child(lead, true).await;
    let retry = Call::AtVersion(
        "lead.recover",
        child.id.clone(),
        json!({"task_id": child.id, "action": {"verb": "retry"}}),
    );
    ops.script(vec![vec![retry.clone()]]);
    calls_succeeded(&ops, 1).await;
    until(lead, "the recovered subtask runs", async || {
        reload(&lead.db, &root.id).await.status == "in_progress"
            && running(lead, &child.id, CODER).await
    })
    .await;
    let results = ops.results();
    assert_eq!(results.len(), 1, "{results:?}");
    assert!(results[0].1.is_ok(), "{results:?}");
    assert_eq!(
        log(lead, &root.id).await,
        vec![
            (
                "in_progress".to_owned(),
                "planning".to_owned(),
                Some("replan".to_owned())
            ),
            ("planning".to_owned(), "in_progress".to_owned(), None),
        ]
    );
    assert_eq!(spent(lead, &root.id, "replan").await, 1);
    assert_eq!(
        spent(lead, &root.id, &db::budget::lead_recover_key(&child.id)).await,
        1
    );
    let recovered = reload(&lead.db, &child.id).await;
    assert!(
        crate::task_hierarchy::subtask_decision_park(&recovered).is_none(),
        "the park ended: {:?}",
        recovered.condition
    );
    assert!(
        !running(lead, &later.id, CODER).await,
        "one subtask at a time"
    );
    // The root waits on its subtasks; no further lead run is started.
    passes(lead, 4).await;
    assert_eq!(
        executions(&lead.db, &root.id).await,
        vec![(PLANNER.to_owned(), "completed".to_owned())]
    );
}

/// The recover bound. Two recoveries of one subtask fit in a root's budget
/// window; the third is refused, typed, the subtask stays parked, the root
/// returns to implementation and waits on its subtasks (the Project Agent's
/// from there).
#[tokio::test]
async fn a_third_recovery_of_one_subtask_is_refused() {
    let ops = ops_fixture(true).await;
    let lead = &ops.lead;
    let (root, child, _) = root_with_parked_child(lead, true).await;
    let mut tx = lead.db.pool().begin().await.unwrap();
    for step in ["earlier-1", "earlier-2"] {
        db::budget::charge(
            &mut tx,
            &root.id,
            &db::budget::lead_recover_key(&child.id),
            db::budget::LEAD_RECOVER_LIMIT,
            step,
        )
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
    ops.script(vec![vec![Call::AtVersion(
        "lead.recover",
        child.id.clone(),
        json!({"task_id": child.id, "action": {"verb": "retry"}}),
    )]]);
    until(
        lead,
        "the lead ran and the root implements again",
        async || {
            !ops.results().is_empty() && reload(&lead.db, &root.id).await.status == "in_progress"
        },
    )
    .await;
    passes(lead, 3).await;
    let results = ops.results();
    let refused = results[0].1.clone().unwrap_err();
    assert!(refused.contains("lead_limit_reached"), "{refused}");
    let parked = reload(&lead.db, &child.id).await;
    assert!(crate::task_hierarchy::subtask_decision_park(&parked).is_some());
    assert!(!running(lead, &child.id, CODER).await);
    assert!(
        park(&lead.db, &root.id).await.contains("Children"),
        "{}",
        park(&lead.db, &root.id).await
    );
    assert_eq!(
        spent(lead, &root.id, "replan").await,
        1,
        "one re-plan per park"
    );
    assert!(
        !crate::task_hierarchy::lead_handles_subtask_park(&lead.db, &child.id)
            .await
            .unwrap(),
        "the Project Agent is woken for the park the lead could not end"
    );
}

/// End to end, the lead changes the breakdown: it cancels the parked subtask
/// and adds one that replaces it. The root leaves planning and the sequence
/// goes on with the next subtask.
#[tokio::test]
async fn a_lead_drops_a_parked_subtask_and_adds_its_replacement() {
    let ops = ops_fixture(true).await;
    let lead = &ops.lead;
    let (root, child, later) = root_with_parked_child(lead, true).await;
    ops.script(vec![vec![
        Call::AtVersion(
            "lead.recover",
            child.id.clone(),
            json!({"task_id": child.id, "action": {"verb": "cancel", "reason": "replaced by a narrower step"}}),
        ),
        Call::Raw(
            "lead.subtask",
            json!({"action": "create", "title": "first step, narrower", "description": "link statically"}),
        ),
    ]]);
    calls_succeeded(&ops, 2).await;
    until(lead, "the sequence goes on", async || {
        reload(&lead.db, &root.id).await.status == "in_progress"
            && running(lead, &later.id, CODER).await
    })
    .await;
    let results = ops.results();
    assert!(
        results.iter().all(|(_, result)| result.is_ok()),
        "{results:?}"
    );
    assert_eq!(reload(&lead.db, &child.id).await.status, "cancelled");
    let all = children(lead, &root.id).await;
    assert_eq!(
        all.iter()
            .map(|task| (task.title.as_str(), task.status.as_str()))
            .collect::<Vec<_>>(),
        vec![
            ("first step", "cancelled"),
            ("second step", "in_progress"),
            ("first step, narrower", "todo"),
        ]
    );
    assert_eq!(all[2].parent_task_id.as_deref(), Some(root.id.as_str()));
    // The added subtask is the root's work now: the root does not go on to
    // review while it has not run.
    passes(lead, 4).await;
    assert_eq!(reload(&lead.db, &root.id).await.status, "in_progress");
    assert_eq!(
        spent(lead, &root.id, db::budget::LEAD_SUBTASKS_KEY).await,
        1
    );
}

/// End to end, a rejected aggregate review becomes fix work: the lead adds a
/// subtask, the root leaves planning for implementation (not review), and
/// the fix subtask runs.
#[tokio::test]
async fn a_lead_turns_a_rejected_review_into_a_fix_subtask_that_runs() {
    let ops = ops_fixture(true).await;
    let lead = &ops.lead;
    let root = root_with_failed_review(lead, true).await;
    ops.script(vec![vec![Call::Raw(
        "lead.subtask",
        json!({"action": "create", "title": "fix the build", "description": "make verify fails on the combined result"}),
    )]]);
    calls_succeeded(&ops, 1).await;
    until(lead, "the fix subtask runs", async || {
        let all = children(lead, &root.id).await;
        all.len() == 2 && running(lead, &all[1].id, CODER).await
    })
    .await;
    let results = ops.results();
    assert!(
        results.iter().all(|(_, result)| result.is_ok()),
        "{results:?}"
    );
    assert_eq!(
        log(lead, &root.id).await[1..],
        [
            (
                "review".to_owned(),
                "planning".to_owned(),
                Some("replan".to_owned())
            ),
            ("planning".to_owned(), "in_progress".to_owned(), None),
        ]
    );
    passes(lead, 4).await;
    let implementing = reload(&lead.db, &root.id).await;
    assert_eq!(
        implementing.status, "in_progress",
        "the root waits for the fix"
    );
    assert_eq!(spent(lead, &root.id, "replan").await, 1);
    assert_eq!(spent(lead, &root.id, "review").await, 1);
    let fix = &children(lead, &root.id).await[1];
    assert_eq!(
        (fix.title.as_str(), fix.status.as_str()),
        ("fix the build", "in_progress")
    );
}

/// A hand-off note is the first thing the subtask's next implementation run
/// reads, attributed to the lead, ahead of its description and its ordinary
/// comments.
#[tokio::test]
async fn a_handoff_note_is_first_in_the_subtasks_next_implementation_prompt() {
    let ops = ops_fixture(true).await;
    let (root, child) = root_with_child(&ops.lead, true).await;
    ops.service
        .add_user_comment(
            &child.id,
            "owner".to_owned(),
            "an ordinary comment".to_owned(),
        )
        .await
        .unwrap();
    ops.script(vec![vec![Call::Raw(
        "lead.handoff",
        json!({"task_id": child.id, "note": "start from the parser; the lexer is done"}),
    )]]);
    calls_succeeded(&ops, 1).await;
    {
        let lead = &ops.lead;
        until(lead, "the subtask starts", async || {
            reload(&lead.db, &root.id).await.status == "in_progress"
                && running(lead, &child.id, CODER).await
        })
        .await;
    }
    assert!(
        ops.results().iter().all(|(_, result)| result.is_ok()),
        "{:?}",
        ops.results()
    );
    let prompt: Option<String> =
        sqlx::query_scalar("SELECT prompt FROM execution WHERE task_id=? AND role='coder'")
            .bind(&child.id)
            .fetch_one(ops.lead.db.pool())
            .await
            .unwrap();
    let prompt = prompt.expect("the worker's prompt");
    let note = prompt
        .find("Hand-off notes from the lead")
        .unwrap_or_else(|| panic!("the note is in the prompt: {prompt}"));
    assert!(
        prompt.contains("- lead: start from the parser; the lexer is done"),
        "{prompt}"
    );
    let description = prompt.find("echo subtask").expect("the description");
    let ordinary = prompt
        .find("an ordinary comment")
        .expect("the ordinary comment");
    assert!(note < description && note < ordinary, "{prompt}");
    assert_eq!(
        prompt.matches("start from the parser").count(),
        1,
        "the note is not repeated among the ordinary comments: {prompt}"
    );
}

/// The lead's recovery opens a new subtask gate window, without resetting
/// the root's two-recovery bound. Its validation may reject in that window.
#[tokio::test]
async fn a_lead_recovery_of_a_spent_step_check_opens_a_new_gate_window() {
    let ops = ops_fixture(true).await;
    let lead = &ops.lead;
    let root = seed_task(&lead.db, &lead.project, "root", "in_progress", 1).await;
    let child = seed_subtask(&lead.db, &root, "child", "lead_check", 0).await;
    assign_role(&lead.db, &root.id, CODER, &lead.coder).await;
    assign_role(&lead.db, &root.id, PLANNER, &lead.planner).await;
    let key = db::budget::gate_key("lead_check");
    let mut tx = db::begin_immediate(lead.db.pool()).await.unwrap();
    for n in 0..2 {
        assert!(
            db::budget::charge(&mut tx, &child.id, &key, 2, &format!("rejection:{n}"))
                .await
                .unwrap()
                .charged
        );
    }
    tx.commit().await.unwrap();
    super::root_replan::park_child(lead, &child, "step-check budget spent").await;
    ops.script(vec![
        vec![Call::AtVersion(
            "lead.recover",
            child.id.clone(),
            json!({"task_id": child.id, "action": {"verb": "retry"}}),
        )],
        vec![Call::Verdict(
            r#"{"result":"fail","reason":"still fails validation"}"#,
        )],
    ]);
    calls_succeeded(&ops, 1).await;
    until(lead, "the recovered validation rejects", async || {
        reload(&lead.db, &child.id).await.status == "in_progress"
    })
    .await;
    assert_eq!(spent(lead, &child.id, &key).await, 1);
    assert_eq!(
        spent(lead, &root.id, &db::budget::lead_recover_key(&child.id)).await,
        1
    );
    let resets: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM transition_log WHERE task_id=? AND bridge_kind='retry_window_reset'",
    )
    .bind(&child.id)
    .fetch_one(lead.db.pool())
    .await
    .unwrap();
    assert_eq!(resets, 1);
}

#[tokio::test]
async fn replacing_a_live_planning_lead_stops_the_old_run_and_rejects_its_operations() {
    let ops = ops_fixture(false).await;
    let lead = &ops.lead;
    let (root, child) = root_with_child(lead, true).await;
    until(lead, "the old lead launches", async || {
        running(lead, &root.id, PLANNER).await
    })
    .await;
    let old = ExecutionRepo::list_running_by_task(&*lead.db, &root.id)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let replacement = crate::test_support::seed_native_agent(&lead.db).await;
    ops.service
        .reassign_role(
            db::CreateTaskRoleAssignment {
                id: db::new_uuid_v4(),
                task_id: root.id.clone(),
                role_name: PLANNER.to_owned(),
                assignee_type: Some(db::AssigneeKind::Agent),
                assignee_id: Some(replacement.clone()),
                created_at: now_rfc3339(),
                updated_at: now_rfc3339(),
            },
            false,
            false,
        )
        .await
        .unwrap();
    until(lead, "the replacement leads the root", async || {
        ExecutionRepo::list_running_by_task(&*lead.db, &root.id)
            .await
            .unwrap()
            .iter()
            .any(|run| run.agent_id.as_deref() == Some(replacement.as_str()))
    })
    .await;
    assert_eq!(
        ExecutionRepo::get_by_id(&*lead.db, &old.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        ExecutionStatus::Cancelled
    );
    let refused = ops
        .call(
            &root.id,
            "lead.subtask",
            json!({"action":"create", "title":"late work", "description":"old lead"}),
            "after-replacement",
        )
        .await
        .unwrap_err();
    assert_eq!(
        children(lead, &root.id).await.len(),
        1,
        "late operation was refused: {refused}"
    );
    passes(lead, 4).await;
    assert_eq!(executions(&lead.db, &root.id).await.len(), 2);
    assert!(executions(&lead.db, &child.id).await.is_empty());
}
