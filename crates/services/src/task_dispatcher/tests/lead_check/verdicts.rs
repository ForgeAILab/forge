//! Tests for verdicts in lead check.

use super::*;
/// The failing-first test of stage 3. No lead is a first-class mode: a
/// finished subtask enters the step check, the gate is skipped at once (no
/// execution, no person, no extra dispatcher decision), and the next sibling
/// starts.
#[tokio::test]
async fn without_a_lead_a_finished_subtask_passes_the_step_check_at_once() {
    let steps = steps(&[], Some("second")).await;
    let (root, first, second) = root_with_two_steps(&steps, false).await;

    until(&steps, "the first subtask is done", async || {
        reload(&steps.db, &first.id).await.status == "done"
    })
    .await;
    assert_eq!(
        log(&steps.db, &first.id).await,
        vec![
            hop("todo", "in_progress", None, false),
            hop("in_progress", "lead_check", None, false),
            hop("lead_check", "done", Some("gate_skipped"), false),
        ],
        "{}",
        dump(&steps.db).await
    );
    // Nothing ran in the gate, and nothing of the review machinery was used.
    assert_eq!(
        executions(&steps.db, &first.id).await,
        vec![("coder".to_owned(), "completed".to_owned())]
    );
    assert_eq!(
        count(
            &steps.db,
            "SELECT COUNT(*) FROM review WHERE task_id=?",
            &first.id
        )
        .await,
        0
    );
    // The next sibling was woken and runs.
    until(&steps, "the second subtask runs", async || {
        executions(&steps.db, &second.id).await == vec![("coder".to_owned(), "running".to_owned())]
    })
    .await;
    assert_eq!(reload(&steps.db, &root.id).await.status, "in_progress");
    assert_eq!(executions(&steps.db, &root.id).await, Vec::new());
}

/// The action endpoint's owner send-back is a rejected transition for audit,
/// but it is not an automatic lead verdict and spends no retry budget.
#[tokio::test]
async fn owner_send_back_from_lead_check_spends_no_automatic_budget() {
    let steps = steps(&[], Some("first")).await;
    let (_root, child) = in_progress_root_with_a_step_in_the_gate(&steps, true).await;
    let service = &steps.dispatcher.task_service;
    let owner = Actor::user(api_types::UserActionSource::Api);
    let offers = service.task_action_offers(&child.id, &owner).await.unwrap();
    assert!(offers
        .available_actions
        .iter()
        .any(|offer| offer.action.verb() == "send_back"));
    let sent_back = service
        .perform_task_action_as(
            &child.id,
            api_types::TaskAction::SendBack {
                guidance: "Please cover the final row".to_owned(),
            },
            offers.version,
            owner,
        )
        .await
        .unwrap()
        .task;
    service.drain(&child.id).await.unwrap();
    assert_eq!(sent_back.status, "in_progress");
    let transitions = log(&steps.db, &child.id).await;
    assert_eq!(transitions.len(), 1, "{transitions:?}");
    assert_eq!(transitions[0].0, "lead_check");
    assert_eq!(transitions[0].1, "in_progress");
    assert!(
        transitions[0].3,
        "the owner decision retains rejection audit evidence"
    );
    assert_eq!(gate_spent(&steps.db, &child.id).await, 0);
    assert_eq!(
        count(
            &steps.db,
            "SELECT COUNT(*) FROM task_budget_charge WHERE task_id=?",
            &child.id
        )
        .await,
        0,
        "neither the gate nor any other automatic budget is charged"
    );
    assert!(
        executions(&steps.db, &child.id)
            .await
            .iter()
            .all(|(role, _)| role == "coder"),
        "no lead verdict caused the send-back"
    );
}

/// With a lead, the lead runs on the subtask (a planner execution under the
/// root's lead Agent, not a review) and its `pass` moves the subtask to
/// `done`; the next sibling starts only then.
#[tokio::test]
async fn a_lead_that_passes_the_step_moves_it_to_done_and_releases_the_next() {
    let steps = steps(&[PASS], Some("second")).await;
    let (root, first, second) = root_with_two_steps(&steps, true).await;

    until(&steps, "the first subtask is done", async || {
        reload(&steps.db, &first.id).await.status == "done"
    })
    .await;
    assert_eq!(
        log(&steps.db, &first.id).await,
        vec![
            hop("todo", "in_progress", None, false),
            hop("in_progress", "lead_check", None, false),
            hop("lead_check", "done", None, false),
        ],
        "{}",
        dump(&steps.db).await
    );
    assert_eq!(
        executions(&steps.db, &first.id).await,
        vec![
            ("coder".to_owned(), "completed".to_owned()),
            ("planner".to_owned(), "completed".to_owned()),
        ]
    );
    let check_agent: Option<String> =
        sqlx::query_scalar("SELECT agent_id FROM execution WHERE task_id=? AND role='planner'")
            .bind(&first.id)
            .fetch_one(steps.db.pool())
            .await
            .unwrap();
    assert_eq!(check_agent.as_deref(), Some(steps.planner.as_str()));
    // No row was copied onto the subtask, and no review machinery ran.
    assert_eq!(
        count(
            &steps.db,
            "SELECT COUNT(*) FROM task_role_assignment WHERE task_id=?",
            &first.id
        )
        .await,
        0
    );
    assert_eq!(
        count(
            &steps.db,
            "SELECT COUNT(*) FROM review WHERE task_id=?",
            &first.id
        )
        .await,
        0
    );
    assert_eq!(check_runs(&steps.db).await, 0);
    assert_eq!(gate_spent(&steps.db, &first.id).await, 0);
    let verdict: Vec<String> =
        sqlx::query_scalar("SELECT content FROM task_comment WHERE task_id=? ORDER BY created_at")
            .bind(&first.id)
            .fetch_all(steps.db.pool())
            .await
            .unwrap();
    assert!(
        verdict
            .iter()
            .any(|content| content == "The lead accepted this step: verified"),
        "{verdict:?}"
    );
    until(&steps, "the second subtask runs", async || {
        executions(&steps.db, &second.id).await == vec![("coder".to_owned(), "running".to_owned())]
    })
    .await;
    // The root planned once (kickoff) and no step check ran on it.
    assert_eq!(
        executions(&steps.db, &root.id).await,
        vec![("planner".to_owned(), "completed".to_owned())]
    );
}

/// A `fail` returns the subtask to implementation as a rejection of the gate:
/// the gate's own budget is charged, the worker is dispatched again with the
/// lead's findings, and the next sibling keeps waiting.
#[tokio::test]
async fn a_lead_that_fails_the_step_returns_it_to_the_same_worker_with_the_findings() {
    let steps = steps(&[FAIL], None).await;
    let (_root, first, second) = root_with_two_steps(&steps, true).await;

    // First check fails, the worker runs again, the second check is running.
    until(&steps, "the second step check runs", async || {
        executions(&steps.db, &first.id).await
            == vec![
                ("coder".to_owned(), "completed".to_owned()),
                ("planner".to_owned(), "completed".to_owned()),
                ("coder".to_owned(), "completed".to_owned()),
                ("planner".to_owned(), "running".to_owned()),
            ]
    })
    .await;
    assert_eq!(
        log(&steps.db, &first.id).await,
        vec![
            hop("todo", "in_progress", None, false),
            hop("in_progress", "lead_check", None, false),
            hop("lead_check", "in_progress", Some("gate_rejected"), true),
            hop("in_progress", "lead_check", None, false),
        ],
        "{}",
        dump(&steps.db).await
    );
    assert_eq!(gate_spent(&steps.db, &first.id).await, 1);
    // The returned run is the same worker, told what the lead found.
    let coder_runs: Vec<(Option<String>, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT agent_id, prompt, parent_execution_id FROM execution
         WHERE task_id=? AND role='coder' ORDER BY created_at, id",
    )
    .bind(&first.id)
    .fetch_all(steps.db.pool())
    .await
    .unwrap();
    assert_eq!(coder_runs[0].0, coder_runs[1].0);
    let prompt = coder_runs[1].1.clone().unwrap_or_default();
    assert!(
        prompt.contains("The lead returned this step: the last row is dropped")
            && prompt.contains("The parser drops the last row."),
        "{prompt}"
    );
    // The next sibling has not started, and nothing reviewed anything.
    assert_eq!(reload(&steps.db, &second.id).await.status, "todo");
    assert_eq!(executions(&steps.db, &second.id).await, Vec::new());
    assert_eq!(
        count(
            &steps.db,
            "SELECT COUNT(*) FROM review WHERE task_id=?",
            &first.id
        )
        .await,
        0
    );
    assert_eq!(check_runs(&steps.db).await, 0);
    // The Review budget is not the step check's.
    assert_eq!(
        db::budget::spent(steps.db.pool(), &first.id, db::budget::Kind::Review.key())
            .await
            .unwrap(),
        0
    );
}

/// `max_rejections` bounds the returns: after the second return the next
/// entry into the gate parks the subtask (typed, with a failure kind) and
/// starts no third check. The sequence stops at that subtask.
#[tokio::test]
async fn the_entry_after_the_last_allowed_return_parks_the_subtask() {
    let steps = steps(&[FAIL, FAIL], None).await;
    let (_root, first, second) = root_with_two_steps(&steps, true).await;

    until(
        &steps,
        "the subtask is parked on its spent budget",
        async || {
            let task = reload(&steps.db, &first.id).await;
            task.status == "lead_check" && task.condition.read().interruption_present
        },
    )
    .await;
    passes(&steps, 4).await;
    let parked = reload(&steps.db, &first.id).await;
    assert_eq!(
        parked.condition.read().failure_kind,
        Some(api_types::FailureKind::RetryExhausted),
        "{}",
        dump(&steps.db).await
    );
    assert_eq!(gate_spent(&steps.db, &first.id).await, 2);
    assert_eq!(
        executions(&steps.db, &first.id).await,
        vec![
            ("coder".to_owned(), "completed".to_owned()),
            ("planner".to_owned(), "completed".to_owned()),
            ("coder".to_owned(), "completed".to_owned()),
            ("planner".to_owned(), "completed".to_owned()),
            ("coder".to_owned(), "completed".to_owned()),
        ],
        "no third step check"
    );
    assert_eq!(executions(&steps.db, &second.id).await, Vec::new());
    // The park has a working action for its owner.
    let snapshot = crate::task_actions::load_snapshot(
        &steps.db,
        parked.clone(),
        WorkflowEngine::resolve_workflow_for_task(
            &parked,
            "{}",
            &Actor::system(SystemComponent::TaskDispatcher),
        ),
        &Actor::user(api_types::UserActionSource::Test),
        None,
    )
    .await
    .expect("the snapshot loads");
    let actions = crate::available_actions(&snapshot);
    assert!(
        actions
            .iter()
            .any(|offer| matches!(offer.action, api_types::TaskAction::Retry { .. })),
        "{actions:?}"
    );
}

/// A step check that ends without a verdict is not a pass and not a return:
/// it goes through the bounded completion-guard path, and with no session to
/// ask again the subtask is blocked with a visible failure.
#[tokio::test]
async fn a_step_check_without_a_result_block_blocks_the_subtask_visibly() {
    let steps = steps(&["Looks fine to me."], None).await;
    let (_root, first, second) = root_with_two_steps(&steps, true).await;

    until(&steps, "the subtask is blocked", async || {
        reload(&steps.db, &first.id)
            .await
            .condition
            .read()
            .interruption_present
    })
    .await;
    let blocked = reload(&steps.db, &first.id).await;
    assert_eq!(blocked.status, "lead_check");
    assert_eq!(
        blocked.condition.read().failure_kind,
        Some(api_types::FailureKind::WorkflowGuardRejected),
        "{}",
        dump(&steps.db).await
    );
    assert_eq!(gate_spent(&steps.db, &first.id).await, 0, "not a return");
    assert_eq!(executions(&steps.db, &second.id).await, Vec::new());
    passes(&steps, 3).await;
    assert_eq!(
        executions(&steps.db, &first.id).await.len(),
        2,
        "the check is not started again on its own"
    );
}
