//! Tests for budgets in lead check.

use super::*;
/// No lead: the skip is enqueued by the entry into the gate (its own hook
/// step cascades in the same chain); it does not wait for a dispatcher pass
/// or a sweep.
#[tokio::test]
async fn without_a_lead_the_skip_rides_the_entry_s_own_chain() {
    let steps = steps(&[], Some("second")).await;
    let (_root, first, _second) = root_with_two_steps(&steps, false).await;
    until(&steps, "the first subtask is done", async || {
        reload(&steps.db, &first.id).await.status == "done"
    })
    .await;
    let skips: Vec<(String, String)> = sqlx::query_as(
        "SELECT triggered_by, trigger_reason FROM transition_log
         WHERE task_id=? AND from_state='lead_check'",
    )
    .bind(&first.id)
    .fetch_all(steps.db.pool())
    .await
    .unwrap();
    // Exactly one skip. It is the cascade of the entry's own hook step
    // (same chain, enqueued without any dispatcher decision). A dispatcher
    // pass that happens to see the subtask between those two queue steps
    // may issue the same skip (`Step::SkipGate`); whichever commits first is
    // the one transition, and the subtask needs no further pass either way.
    assert_eq!(skips.len(), 1, "{skips:?}");
    assert!(
        skips[0].1 == "No planner role assigned; optional gate skipped"
            || skips[0].1.starts_with("Nobody holds the lead_check gate"),
        "{skips:?}"
    );
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT kind, chain_id FROM task_step WHERE task_id=? AND expected_status='lead_check'
         ORDER BY seq",
    )
    .bind(&first.id)
    .fetch_all(steps.db.pool())
    .await
    .unwrap();
    let chain = |kind: &str| -> Vec<&str> {
        rows.iter()
            .filter(|row| row.0 == kind)
            .map(|row| row.1.as_str())
            .collect()
    };
    assert_eq!(chain("hooks").len(), 1, "{rows:?}");
    assert_eq!(chain("cascade"), chain("hooks"), "one chain: {rows:?}");
    // Nobody is asked for anything: no Attention item, no park.
    let open = open_attention(&steps, &first.id).await;
    let items: Vec<(String, String, String, String)> = sqlx::query_as(
        "SELECT attention_type, status, summary, details_json FROM attention_projection",
    )
    .fetch_all(steps.db.pool())
    .await
    .unwrap();
    assert_eq!(open, 0, "{items:#?}");
}

/// A `planner` row on a subtask would hold nothing (the step check is the
/// root's lead): assigning one is refused and names the root.
#[tokio::test]
async fn assigning_the_step_check_role_on_a_subtask_is_refused_and_names_the_root() {
    let steps = steps(&[], Some("first")).await;
    let (root, first, _second) = root_with_two_steps(&steps, false).await;
    let now = now_rfc3339();
    let refused = steps
        .dispatcher
        .task_service
        .reassign_role(
            db::CreateTaskRoleAssignment {
                id: new_uuid_v4(),
                task_id: first.id.clone(),
                role_name: crate::workflow::default_roles::PLANNER.to_owned(),
                assignee_type: Some(db::AssigneeKind::Agent),
                assignee_id: Some(steps.planner.clone()),
                created_at: now.clone(),
                updated_at: now,
            },
            false,
            false,
        )
        .await;
    let error = refused.expect_err("the assignment is refused").to_string();
    assert!(
        error.contains("held by the lead of its root") && error.contains(&root.id),
        "{error}"
    );
    assert_eq!(
        count(
            &steps.db,
            "SELECT COUNT(*) FROM task_role_assignment WHERE task_id=? AND role_name='planner'",
            &first.id
        )
        .await,
        0
    );
}

/// A `blocked` verdict is not a pass and not a return: the lead is asked
/// again under the completion-guard bound and the subtask parks, typed.
#[tokio::test]
async fn a_blocked_verdict_parks_the_subtask_without_spending_a_return() {
    let steps = steps(&[BLOCKED], None).await;
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
        Some(api_types::FailureKind::WorkflowGuardRejected)
    );
    assert_eq!(gate_spent(&steps.db, &first.id).await, 0);
    assert_eq!(executions(&steps.db, &second.id).await, Vec::new());
    assert!(
        open_attention(&steps, &first.id).await >= 1,
        "the park is shown"
    );
}

/// The spent return budget: one budget check per entry, one typed park with
/// an Attention item, and the owner's Retry opens a new window and has the
/// LEAD check the work that is already there (validate first; the worker is
/// not run again before anybody looked).
#[tokio::test]
async fn the_spent_budget_park_is_shown_once_and_retry_has_the_lead_check_the_work() {
    let steps = steps(&[FAIL, FAIL, PASS], Some("second")).await;
    let (_root, first, second) = root_with_two_steps(&steps, true).await;
    until(&steps, "the subtask is parked", async || {
        let task = reload(&steps.db, &first.id).await;
        task.status == "lead_check" && task.condition.read().interruption_present
    })
    .await;
    passes(&steps, 3).await;
    let parked = reload(&steps.db, &first.id).await;
    assert_eq!(
        parked.condition.read().failure_kind,
        Some(api_types::FailureKind::RetryExhausted)
    );
    // The budget is checked once per entry into the gate.
    let hooks: Option<String> = sqlx::query_scalar(
        "SELECT hook_results_json FROM transition_log
         WHERE task_id=? AND to_state='lead_check' ORDER BY created_at DESC, rowid DESC LIMIT 1",
    )
    .bind(&first.id)
    .fetch_one(steps.db.pool())
    .await
    .unwrap();
    let hooks = hooks.unwrap_or_default();
    assert_eq!(
        hooks.matches("check_retry_budget").count(),
        1,
        "one budget check per entry: {hooks}"
    );
    assert_eq!(
        open_attention(&steps, &first.id).await,
        1,
        "one Attention item"
    );

    steps
        .dispatcher
        .task_service
        .perform_task_action(
            &parked.id,
            api_types::TaskAction::Retry {
                reason: None,
                fresh_session: None,
                refresh_workspace: None,
                reset_budget: Some(true),
                guidance: None,
            },
            parked.version,
        )
        .await
        .expect("the owner retries the parked step");
    until(&steps, "the step is done", async || {
        reload(&steps.db, &first.id).await.status == "done"
    })
    .await;
    assert_eq!(
        executions(&steps.db, &first.id).await,
        vec![
            ("coder".to_owned(), "completed".to_owned()),
            ("planner".to_owned(), "completed".to_owned()),
            ("coder".to_owned(), "completed".to_owned()),
            ("planner".to_owned(), "completed".to_owned()),
            ("coder".to_owned(), "completed".to_owned()),
            ("planner".to_owned(), "completed".to_owned()),
        ],
        "Retry starts the lead's check of the last rework, not another rework\n{}",
        dump(&steps.db).await
    );
    until(&steps, "the next step runs", async || {
        !executions(&steps.db, &second.id).await.is_empty()
    })
    .await;
}

/// Retry is accepted while the root's automatic re-plan still owns the
/// workspace. The root cannot finish until released, forcing replay of the
/// child's accepted action to encounter the planning parent.
#[tokio::test]
async fn retry_of_a_spent_lead_check_waits_for_the_roots_replan() {
    let steps = steps(&[HELD_FAIL, FAIL, PASS], Some("second")).await;
    let (root, first, _second) = root_with_two_steps(&steps, true).await;
    until(&steps, "the first check holds its verdict", async || {
        running_check(&steps.db, &first.id).await && steps.replies.lock().unwrap().len() == 2
    })
    .await;
    steps
        .hold_root_planner
        .store(true, std::sync::atomic::Ordering::SeqCst);
    steps.release.add_permits(1);
    until(
        &steps,
        "the root re-plans the spent child gate",
        async || {
            reload(&steps.db, &root.id).await.status == "planning"
                && running_check(&steps.db, &root.id).await
                && reload(&steps.db, &first.id)
                    .await
                    .condition
                    .read()
                    .failure_kind
                    == Some(api_types::FailureKind::RetryExhausted)
        },
    )
    .await;
    let parked = reload(&steps.db, &first.id).await;
    steps
        .dispatcher
        .task_service
        .perform_task_action(
            &first.id,
            api_types::TaskAction::Retry {
                reason: None,
                fresh_session: None,
                refresh_workspace: None,
                reset_budget: Some(true),
                guidance: None,
            },
            parked.version,
        )
        .await
        .expect("Retry is accepted while the root plans");
    passes(&steps, 3).await;
    let waiting = reload(&steps.db, &first.id).await;
    assert!(
        crate::deferred_dispatch::queued_recovery(&waiting).is_some(),
        "the accepted Retry survives the planning parent\n{}",
        dump(&steps.db).await
    );
    assert_eq!(
        gate_spent(&steps.db, &first.id).await,
        0,
        "a new gate budget window"
    );
    assert!(
        waiting.condition.reasons().any(|reason| matches!(reason,
        db::ParkReason::Parent { cause, .. } if cause == "planning")),
        "the wait names the root's planning step"
    );
    // Admission can also lose after the dispatcher's eligibility read.
    // Exercise that refusal boundary with the real hierarchy denial, and
    // with a database authority conflict whose child version did not move.
    let refused = crate::task_hierarchy::ensure_subtask_dispatch_order(&steps.db, &waiting)
        .await
        .expect_err("the root is still planning");
    assert!(crate::task_hierarchy::is_subtask_wait_refusal(&refused));
    assert!(
        matches!(refused, ServiceError::InvalidOperation { .. }),
        "callers are refused as before"
    );
    for refusal in [refused, ServiceError::Db(db::DbError::VersionConflict)] {
        assert!(steps
            .dispatcher
            .task_service
            .settle_queued_task_action_refusal(&waiting, &refusal)
            .await
            .unwrap());
        assert!(
            crate::deferred_dispatch::queued_recovery(&reload(&steps.db, &first.id).await)
                .is_some(),
            "the admission race retains Retry"
        );
    }
    assert!(
        crate::deferred_dispatch::queued_recovery(&reload(&steps.db, &first.id).await)
            .unwrap()
            .awaits_hierarchy,
        "the refusal is recorded on the accepted action"
    );
    assert_eq!(
        executions(&steps.db, &first.id).await.len(),
        5,
        "nothing executes beside the root's re-plan"
    );
    steps.release.add_permits(1);
    until(
        &steps,
        "the lead checks the rework after the root finishes",
        async || reload(&steps.db, &first.id).await.status == "done",
    )
    .await;
    assert_eq!(
        executions(&steps.db, &first.id).await,
        vec![
            ("coder".to_owned(), "completed".to_owned()),
            ("planner".to_owned(), "completed".to_owned()),
            ("coder".to_owned(), "completed".to_owned()),
            ("planner".to_owned(), "completed".to_owned()),
            ("coder".to_owned(), "completed".to_owned()),
            ("planner".to_owned(), "completed".to_owned())
        ]
    );
}
