//! Tests for question lifecycle in lead operations.

use super::*;
/// A lead that drops every open subtask does not finish the feature by
/// doing so: with nothing left to run the root goes on to its aggregate
/// review gate like any root whose sequence is complete, and never from
/// implementation to merging or done.
#[tokio::test]
async fn a_lead_that_drops_every_open_subtask_still_goes_through_the_review_gate() {
    let ops = ops_fixture(true).await;
    let lead = &ops.lead;
    let (root, child, later) = root_with_parked_child(lead, true).await;
    ops.script(vec![vec![
        Call::AtVersion(
            "lead.recover",
            child.id.clone(),
            json!({"task_id": child.id, "action": {"verb": "cancel", "reason": "not needed"}}),
        ),
        Call::AtVersion(
            "lead.subtask",
            later.id.clone(),
            json!({"action": "cancel", "task_id": later.id, "reason": "not needed either"}),
        ),
    ]]);
    calls_succeeded(&ops, 2).await;
    until(lead, "the root left planning", async || {
        reload(&lead.db, &root.id).await.status != "planning"
    })
    .await;
    passes(lead, 6).await;
    assert_eq!(
        children(lead, &root.id)
            .await
            .iter()
            .map(|task| task.status.as_str())
            .collect::<Vec<_>>(),
        vec!["cancelled", "cancelled"]
    );
    // The root goes through its review gate; nothing takes it from
    // implementation straight to merging or done. (This fixture's review has
    // no reviewer and no checks, so the gate passes by configuration, as it
    // does for a root without a lead whose owner cancelled its subtasks.)
    let hops = log(lead, &root.id).await;
    let from_implementation = hops
        .iter()
        .filter(|(from, _, _)| from == "in_progress")
        .map(|(_, to, _)| to.as_str())
        .collect::<Vec<_>>();
    assert_eq!(from_implementation, ["planning", "review"], "{hops:?}");
    assert!(
        hops.iter()
            .filter(|(_, to, _)| to == "merging" || to == "done")
            .all(|(from, _, _)| from == "review" || from == "merging"),
        "{hops:?}"
    );
    assert_eq!(spent(lead, &root.id, "replan").await, 1);
}

/// A root that waits for an answer and is cancelled by its owner no longer
/// waits, and its question is not left open for the owner to find later.
#[tokio::test]
async fn cancelling_a_root_that_waits_for_an_answer_closes_the_question() {
    let ops = ops_fixture(true).await;
    let (root, child) = root_with_child(&ops.lead, true).await;
    ops.script(vec![vec![Call::Raw(
        "lead.request_input",
        json!({"question": "A or B?"}),
    )]]);
    let lead = &ops.lead;
    until(lead, "the root waits for the answer", async || {
        park(&lead.db, &root.id).await.contains("HumanWork")
    })
    .await;
    assert_eq!(question_items(lead, "open").await, 1);
    let waiting = reload(&lead.db, &root.id).await;
    ops.service
        .perform_task_action(
            &root.id,
            api_types::TaskAction::Cancel {
                reason: Some("not now".to_owned()),
            },
            waiting.version,
        )
        .await
        .expect("the owner cancels a root that waits for an answer");
    passes(lead, 4).await;
    let cancelled = reload(&lead.db, &root.id).await;
    assert_eq!(cancelled.status, "cancelled");
    let condition = serde_json::to_string(&cancelled.condition.public()).unwrap();
    assert!(!condition.contains("lead_input"), "{condition}");
    assert_eq!(question_items(lead, "open").await, 0, "no orphan question");
    assert_eq!(question_items(lead, "resolved").await, 1);
    assert_eq!(reload(&lead.db, &child.id).await.status, "cancelled");
    let runs = executions(&lead.db, &root.id).await;
    assert!(
        runs.len() == 1 && runs[0].0 == PLANNER && runs[0].1 != "running",
        "the lead is not run again: {runs:?}"
    );
}

/// The lead is removed while its root waits for an answer: the question
/// stays the owner's to answer (the park keeps its working action), and the
/// answer releases the root, which goes on without a lead.
#[tokio::test]
async fn a_lead_removed_during_the_wait_leaves_an_answerable_question() {
    let ops = ops_fixture(true).await;
    let (root, child) = root_with_child(&ops.lead, true).await;
    ops.script(vec![vec![Call::Raw(
        "lead.request_input",
        json!({"question": "A or B?"}),
    )]]);
    let lead = &ops.lead;
    until(lead, "the root waits for the answer", async || {
        park(&lead.db, &root.id).await.contains("HumanWork")
    })
    .await;
    ops.service
        .remove_role(&root.id, PLANNER, false, false)
        .await
        .expect("the owner removes the lead");
    passes(lead, 4).await;
    let waiting = reload(&lead.db, &root.id).await;
    let (item, version): (String, i64) = sqlx::query_as(
        "SELECT id, version FROM attention_projection WHERE recommended_action='answer_lead_input' AND status='open'",
    )
    .fetch_one(lead.db.pool())
    .await
    .expect("the question is still open while the root still waits");
    assert_eq!(waiting.status, "planning");
    crate::attention_service::AttentionService::new(
        Arc::clone(&lead.db),
        crate::test_support::noop_wake_turns(),
    )
    .resolve_with_answer("owner", &item, version, Some("B"))
    .await
    .expect("the answer is taken");
    until(lead, "the root implements without a lead", async || {
        reload(&lead.db, &root.id).await.status == "in_progress"
            && running(lead, &child.id, CODER).await
    })
    .await;
    assert_eq!(question_items(lead, "open").await, 0);
    assert_eq!(
        executions(&lead.db, &root.id).await,
        vec![(PLANNER.to_owned(), "cancelled".to_owned())],
        "nobody plans a root that has no lead"
    );
}

/// Hold and Release keep an ended asking run's question answerable.
#[tokio::test]
async fn a_hold_during_the_leads_question_preserves_the_wait_after_release() {
    let ops = ops_fixture(true).await;
    let (root, child) = root_with_child(&ops.lead, true).await;
    ops.script(vec![
        vec![Call::Raw(
            "lead.request_input",
            json!({"question":"A or B?"}),
        )],
        vec![],
    ]);
    let (item, version) = waiting_question(&ops, &root.id).await;
    let waiting = reload(&ops.lead.db, &root.id).await;
    ops.service
        .perform_task_action(
            &root.id,
            api_types::TaskAction::Hold { reason: None },
            waiting.version,
        )
        .await
        .unwrap();
    passes(&ops.lead, 3).await;
    let held = reload(&ops.lead.db, &root.id).await;
    assert!(held
        .condition
        .reasons()
        .any(|reason| matches!(reason, db::ParkReason::Held { .. })));
    ops.service
        .perform_task_action(
            &root.id,
            api_types::TaskAction::Release { reason: None },
            held.version,
        )
        .await
        .unwrap();
    passes(&ops.lead, 5).await;
    assert_eq!(executions(&ops.lead.db, &root.id).await.len(), 1);
    assert_eq!(question_items(&ops.lead, "open").await, 1);
    assert!(park(&ops.lead.db, &root.id).await.contains("HumanWork"));
    assert_answer_runs_once(&ops, &root.id, &child.id, &item, version).await;
}

#[tokio::test]
async fn a_project_pause_during_the_leads_question_preserves_the_answerable_wait() {
    let ops = ops_fixture(true).await;
    let (root, child) = root_with_child(&ops.lead, true).await;
    ops.script(vec![
        vec![Call::Raw(
            "lead.request_input",
            json!({"question":"A or B?"}),
        )],
        vec![],
    ]);
    let (item, version) = waiting_question(&ops, &root.id).await;
    for paused in [Some(now_rfc3339()), None] {
        ProjectRepo::set_paused_at(&*ops.lead.db, &ops.lead.project, paused)
            .await
            .unwrap();
        passes(&ops.lead, 5).await;
        assert_eq!(executions(&ops.lead.db, &root.id).await.len(), 1);
        assert_eq!(question_items(&ops.lead, "open").await, 1);
        assert!(executions(&ops.lead.db, &child.id).await.is_empty());
    }
    assert_answer_runs_once(&ops, &root.id, &child.id, &item, version).await;
}

#[tokio::test]
async fn a_failed_asking_run_waits_for_its_answer_instead_of_retrying() {
    let ops = ops_fixture(true).await;
    let (root, child) = root_with_child(&ops.lead, true).await;
    ops.script(vec![
        vec![
            Call::Raw("lead.request_input", json!({"question":"A or B?"})),
            Call::Fail,
        ],
        vec![],
    ]);
    let (item, version) = waiting_question(&ops, &root.id).await;
    passes(&ops.lead, 5).await;
    assert_eq!(
        executions(&ops.lead.db, &root.id).await,
        [(PLANNER.to_owned(), "failed".to_owned())]
    );
    assert!(executions(&ops.lead.db, &child.id).await.is_empty());
    assert_answer_runs_once(&ops, &root.id, &child.id, &item, version).await;
}
