//! Tests for lifecycle in lead check.

use super::*;
/// A subtask that rests in the gate with nobody holding it (its root's lead
/// was removed after it entered, or the entry was interrupted) is passed by
/// the dispatcher from stored state: no person, no run, and never back to
/// implementation.
#[tokio::test]
async fn a_step_resting_in_the_gate_without_a_lead_is_passed_from_stored_state() {
    let steps = steps(&[], None).await;
    let (_root, child) = in_progress_root_with_a_step_in_the_gate(&steps, false).await;
    // An own planner row on the subtask holds nothing: the role is the
    // root's lead and nothing else.
    assign_role(
        &steps.db,
        &child.id,
        crate::workflow::default_roles::PLANNER,
        &steps.planner,
    )
    .await;

    until(&steps, "the step is done", async || {
        reload(&steps.db, &child.id).await.status == "done"
    })
    .await;
    assert_eq!(
        log(&steps.db, &child.id).await,
        vec![hop("lead_check", "done", Some("gate_skipped"), false)],
        "{}",
        dump(&steps.db).await
    );
    assert_eq!(executions(&steps.db, &child.id).await, Vec::new());
}

/// Removing the lead while a step waits in the gate wakes that step, and the
/// dispatcher passes it.
#[tokio::test]
async fn removing_the_lead_passes_a_step_that_waits_in_the_gate() {
    let steps = steps(&[], None).await;
    let (root, child) = in_progress_root_with_a_step_in_the_gate(&steps, true).await;
    // Hold the Project so the lead's check is not started before the lead
    // is removed.
    let project = ProjectRepo::get_by_id(&*steps.db, &steps.project)
        .await
        .unwrap()
        .unwrap();
    ProjectRepo::update_at_version(
        &*steps.db,
        UpdateProject {
            id: project.id.clone(),
            name: None,
            settings: None,
            primary_repo_id: None,
            paused_at: Some(Some(now_rfc3339())),
            updated_at: now_rfc3339(),
        },
        project.version,
        None,
    )
    .await
    .unwrap();
    passes(&steps, 2).await;
    assert_eq!(reload(&steps.db, &child.id).await.status, "lead_check");
    assert_eq!(executions(&steps.db, &child.id).await, Vec::new());

    steps
        .dispatcher
        .task_service
        .remove_role(
            &root.id,
            crate::workflow::default_roles::PLANNER,
            false,
            false,
        )
        .await
        .expect("the lead is removed");
    let dirty: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM task_schedule_dirty WHERE task_id=? AND dirty=1)",
    )
    .bind(&child.id)
    .fetch_one(steps.db.pool())
    .await
    .unwrap();
    assert!(dirty, "removing the lead marks the waiting step");
    let project = ProjectRepo::get_by_id(&*steps.db, &steps.project)
        .await
        .unwrap()
        .unwrap();
    ProjectRepo::update_at_version(
        &*steps.db,
        UpdateProject {
            id: project.id.clone(),
            name: None,
            settings: None,
            primary_repo_id: None,
            paused_at: Some(None),
            updated_at: now_rfc3339(),
        },
        project.version,
        None,
    )
    .await
    .unwrap();
    until(&steps, "the step is done", async || {
        reload(&steps.db, &child.id).await.status == "done"
    })
    .await;
    assert_eq!(executions(&steps.db, &child.id).await, Vec::new());
}

/// A crash between the entry into the gate and the lead's run leaves a
/// subtask in the gate with no execution: the dispatcher starts the check
/// from stored state, under the root's lead.
#[tokio::test]
async fn a_step_resting_in_the_gate_under_a_lead_gets_its_check_from_stored_state() {
    let steps = steps(&[], None).await;
    let (root, child) = in_progress_root_with_a_step_in_the_gate(&steps, true).await;

    until(&steps, "the step check runs", async || {
        executions(&steps.db, &child.id).await == vec![("planner".to_owned(), "running".to_owned())]
    })
    .await;
    assert_eq!(reload(&steps.db, &child.id).await.status, "lead_check");
    // Stable: no second check, nothing on the root.
    passes(&steps, 3).await;
    assert_eq!(executions(&steps.db, &child.id).await.len(), 1);
    assert_eq!(executions(&steps.db, &root.id).await, Vec::new());
    let prompt: Option<String> =
        sqlx::query_scalar("SELECT prompt FROM execution WHERE task_id=? AND role='planner'")
            .bind(&child.id)
            .fetch_one(steps.db.pool())
            .await
            .unwrap();
    let prompt = prompt.unwrap_or_default();
    assert!(
        prompt.contains("Step-check doctrine:")
            && prompt.contains("Check this finished step: first"),
        "{prompt}"
    );
}

/// The lead is removed while its check runs: the run is stopped, its verdict
/// (whatever it would have been) moves nothing, and the subtask passes the
/// gate once, as under any root without a lead.
#[tokio::test]
async fn removing_the_lead_while_its_check_runs_stops_the_check_and_passes_the_step() {
    let steps = steps(&[HELD_FAIL], Some("second")).await;
    let (root, first, second) = root_with_two_steps(&steps, true).await;
    until(&steps, "the step check runs", async || {
        running_check(&steps.db, &first.id).await
    })
    .await;
    steps
        .dispatcher
        .task_service
        .remove_role(
            &root.id,
            crate::workflow::default_roles::PLANNER,
            false,
            false,
        )
        .await
        .expect("the lead is removed");
    // Without the run ever answering.
    until(&steps, "the step is done", async || {
        reload(&steps.db, &first.id).await.status == "done"
    })
    .await;
    assert!(
        !running_check(&steps.db, &first.id).await,
        "the removed lead's run is stopped: {}",
        dump(&steps.db).await
    );
    // A late answer changes nothing.
    steps.release.add_permits(8);
    passes(&steps, 4).await;
    let hops = log(&steps.db, &first.id).await;
    assert_eq!(
        hops.iter().filter(|hop| hop.0 == "lead_check").count(),
        1,
        "{hops:?}"
    );
    assert_eq!(
        hops.last(),
        Some(&hop("lead_check", "done", Some("gate_skipped"), false))
    );
    assert_eq!(gate_spent(&steps.db, &first.id).await, 0);
    until(&steps, "the next step runs", async || {
        !executions(&steps.db, &second.id).await.is_empty()
    })
    .await;
}

/// Hold and Release on a subtask whose check is running: the hold stops the
/// check and parks the subtask as held; the release has the lead check it
/// again. No verdict of the stopped run moves it.
#[tokio::test]
async fn a_hold_on_a_step_in_the_gate_stops_the_check_and_release_starts_it_again() {
    let steps = steps(&[HELD_FAIL, PASS], Some("second")).await;
    let (_root, first, _second) = root_with_two_steps(&steps, true).await;
    until(&steps, "the step check runs", async || {
        running_check(&steps.db, &first.id).await
    })
    .await;
    let service = &steps.dispatcher.task_service;
    let resting = reload(&steps.db, &first.id).await;
    service
        .perform_task_action(
            &first.id,
            api_types::TaskAction::Hold { reason: None },
            resting.version,
        )
        .await
        .expect("a step in the gate is held");
    passes(&steps, 3).await;
    steps.release.add_permits(8);
    passes(&steps, 3).await;
    let held = reload(&steps.db, &first.id).await;
    assert_eq!(held.status, "lead_check", "{}", dump(&steps.db).await);
    assert!(!running_check(&steps.db, &first.id).await);
    assert_eq!(gate_spent(&steps.db, &first.id).await, 0);
    let offers = service.test_action_values(&first.id).await.unwrap();
    assert!(
        offers.contains(&api_types::TaskAction::Release { reason: None }),
        "{offers:?}\n{}",
        condition(&steps.db, &first.id).await
    );
    service
        .perform_task_action(
            &first.id,
            api_types::TaskAction::Release { reason: None },
            held.version,
        )
        .await
        .expect("the step is released");
    until(&steps, "the step is done", async || {
        reload(&steps.db, &first.id).await.status == "done"
    })
    .await;
    assert_eq!(
        log(&steps.db, &first.id).await.last(),
        Some(&hop("lead_check", "done", None, false)),
        "the lead's own pass, not a skip"
    );
}

/// Cancelling a subtask in the gate ends the check; a late verdict moves
/// nothing.
#[tokio::test]
async fn cancelling_a_step_in_the_gate_ends_the_check_and_drops_its_verdict() {
    let steps = steps(&[HELD_FAIL], Some("second")).await;
    let (_root, first, _second) = root_with_two_steps(&steps, true).await;
    until(&steps, "the step check runs", async || {
        running_check(&steps.db, &first.id).await
    })
    .await;
    let resting = reload(&steps.db, &first.id).await;
    steps
        .dispatcher
        .task_service
        .perform_task_action(
            &first.id,
            api_types::TaskAction::Cancel { reason: None },
            resting.version,
        )
        .await
        .expect("a step in the gate is cancelled");
    passes(&steps, 3).await;
    steps.release.add_permits(8);
    passes(&steps, 3).await;
    assert_eq!(reload(&steps.db, &first.id).await.status, "cancelled");
    assert!(!running_check(&steps.db, &first.id).await);
    assert_eq!(gate_spent(&steps.db, &first.id).await, 0);
    assert_eq!(
        log(&steps.db, &first.id)
            .await
            .iter()
            .filter(|hop| hop.0 == "lead_check")
            .count(),
        1
    );
}

/// Cancelling the root while a subtask is in the gate ends the check and the
/// subtask with it.
#[tokio::test]
async fn cancelling_the_root_ends_a_step_check_and_its_subtask() {
    let steps = steps(&[HELD_FAIL], Some("second")).await;
    let (root, first, second) = root_with_two_steps(&steps, true).await;
    until(&steps, "the step check runs", async || {
        running_check(&steps.db, &first.id).await
    })
    .await;
    let resting = reload(&steps.db, &root.id).await;
    steps
        .dispatcher
        .task_service
        .perform_task_action(
            &root.id,
            api_types::TaskAction::Cancel { reason: None },
            resting.version,
        )
        .await
        .expect("the root is cancelled");
    passes(&steps, 3).await;
    steps.release.add_permits(8);
    passes(&steps, 3).await;
    assert_eq!(reload(&steps.db, &root.id).await.status, "cancelled");
    for child in [&first, &second] {
        assert_eq!(
            reload(&steps.db, &child.id).await.status,
            "cancelled",
            "{}",
            dump(&steps.db).await
        );
    }
    assert!(!running_check(&steps.db, &first.id).await);
}

/// A root whose subtasks are running offers no Hold (as before this gate
/// existed): a step in the gate is held on the subtask itself.
#[tokio::test]
async fn a_root_with_a_step_in_the_gate_offers_no_hold() {
    let steps = steps(&[HELD_FAIL], Some("second")).await;
    let (root, first, _second) = root_with_two_steps(&steps, true).await;
    until(&steps, "the step check runs", async || {
        running_check(&steps.db, &first.id).await
    })
    .await;
    let offers = steps
        .dispatcher
        .task_service
        .test_action_values(&root.id)
        .await
        .unwrap();
    assert!(
        !offers.contains(&api_types::TaskAction::Hold { reason: None }),
        "{offers:?}"
    );
}

/// A Project pause while a check runs does not lose the step: after the
/// resume the subtask reaches its verdict.
#[tokio::test]
async fn a_project_pause_during_a_step_check_does_not_strand_the_step() {
    let steps = steps(&[HELD_FAIL, PASS, PASS], Some("second")).await;
    let (_root, first, _second) = root_with_two_steps(&steps, true).await;
    until(&steps, "the step check runs", async || {
        running_check(&steps.db, &first.id).await
    })
    .await;
    for paused_at in [Some(now_rfc3339()), None] {
        let project = ProjectRepo::get_by_id(&*steps.db, &steps.project)
            .await
            .unwrap()
            .unwrap();
        ProjectRepo::update_at_version(
            &*steps.db,
            UpdateProject {
                id: project.id.clone(),
                name: None,
                settings: None,
                primary_repo_id: None,
                paused_at: Some(paused_at),
                updated_at: now_rfc3339(),
            },
            project.version,
            None,
        )
        .await
        .unwrap();
        passes(&steps, 3).await;
        steps.release.add_permits(8);
        passes(&steps, 2).await;
    }
    until(&steps, "the step is done", async || {
        reload(&steps.db, &first.id).await.status == "done"
    })
    .await;
}

/// A subtask in the gate holds the one Project slot it held while it was
/// implemented: the lead's check is that Task's run, not a second slot, and
/// its root holds none beside it. A parked step holds none.
#[tokio::test]
async fn a_step_in_the_gate_holds_one_project_slot_and_a_parked_one_none() {
    let steps = steps(&[HELD_FAIL, FAIL], None).await;
    let project = ProjectRepo::get_by_id(&*steps.db, &steps.project)
        .await
        .unwrap()
        .unwrap();
    let mut settings: serde_json::Value = serde_json::from_str(&project.settings).unwrap();
    settings["max_active_tasks"] = serde_json::json!(1);
    ProjectRepo::update_at_version(
        &*steps.db,
        UpdateProject {
            id: project.id.clone(),
            name: None,
            settings: Some(settings.to_string()),
            primary_repo_id: None,
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        project.version,
        None,
    )
    .await
    .unwrap();
    let (_root, first, _second) = root_with_two_steps(&steps, true).await;
    // With a limit of one, the whole flow still runs: the check is admitted
    // in the slot the subtask already holds.
    until(&steps, "the step check runs", async || {
        running_check(&steps.db, &first.id).await
    })
    .await;
    let slots = async || {
        let project = ProjectRepo::get_by_id(&*steps.db, &steps.project)
            .await
            .unwrap()
            .unwrap();
        crate::task_dispatcher::slots::load_project_slots(&steps.db, &project)
            .await
            .unwrap()
    };
    let resting = slots().await;
    assert_eq!((resting.active, resting.parked), (1, 0), "{resting:?}");
    steps.release.add_permits(8);
    until(
        &steps,
        "the subtask is parked on its spent budget",
        async || {
            let task = reload(&steps.db, &first.id).await;
            task.status == "lead_check" && task.condition.read().interruption_present
        },
    )
    .await;
    let parked = slots().await;
    assert_eq!(parked.parked, 1, "{parked:?}");
}

/// A lead assigned while a step rests in the gate (nothing skipped it yet:
/// the Project is paused) checks that step: the assignment marks the subtask
/// and the dispatcher starts the check instead of the skip.
#[tokio::test]
async fn a_lead_assigned_while_a_step_rests_in_the_gate_checks_it() {
    let steps = steps(&[PASS], None).await;
    let (root, child) = in_progress_root_with_a_step_in_the_gate(&steps, false).await;
    let pause = async |paused_at: Option<String>| {
        let project = ProjectRepo::get_by_id(&*steps.db, &steps.project)
            .await
            .unwrap()
            .unwrap();
        ProjectRepo::update_at_version(
            &*steps.db,
            UpdateProject {
                id: project.id.clone(),
                name: None,
                settings: None,
                primary_repo_id: None,
                paused_at: Some(paused_at),
                updated_at: now_rfc3339(),
            },
            project.version,
            None,
        )
        .await
        .unwrap();
    };
    pause(Some(now_rfc3339())).await;
    passes(&steps, 2).await;
    assert_eq!(reload(&steps.db, &child.id).await.status, "lead_check");
    let now = now_rfc3339();
    steps
        .dispatcher
        .task_service
        .reassign_role(
            db::CreateTaskRoleAssignment {
                id: new_uuid_v4(),
                task_id: root.id.clone(),
                role_name: crate::workflow::default_roles::PLANNER.to_owned(),
                assignee_type: Some(db::AssigneeKind::Agent),
                assignee_id: Some(steps.planner.clone()),
                created_at: now.clone(),
                updated_at: now,
            },
            false,
            false,
        )
        .await
        .expect("the lead is assigned on the root");
    pause(None).await;
    until(&steps, "the step is done", async || {
        reload(&steps.db, &child.id).await.status == "done"
    })
    .await;
    assert_eq!(
        executions(&steps.db, &child.id).await,
        vec![("planner".to_owned(), "completed".to_owned())],
        "the new lead checked it"
    );
    assert_eq!(
        log(&steps.db, &child.id).await,
        vec![hop("lead_check", "done", None, false)]
    );
}
