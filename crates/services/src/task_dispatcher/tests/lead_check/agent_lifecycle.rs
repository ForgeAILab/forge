//! Tests for agent lifecycle in lead check.

use super::*;
/// The lead's Agent is archived while its check runs. The assignment row is
/// cleared by the archive sweep; that is a lead change like any other: the
/// run is stopped and the subtask passes the gate now, not when the run ends.
#[tokio::test]
async fn archiving_the_lead_s_agent_while_its_check_runs_stops_the_check_and_passes_the_step() {
    let steps = steps(&[HELD_FAIL], Some("second")).await;
    let (_root, first, second) = root_with_two_steps(&steps, true).await;
    until(&steps, "the step check runs", async || {
        running_check(&steps.db, &first.id).await
    })
    .await;
    steps
        .dispatcher
        .task_service
        .on_agent_deleted(&steps.planner)
        .await
        .expect("the lead's Agent is swept");
    assert!(
        !running_check(&steps.db, &first.id).await,
        "the archived lead's run is stopped by the sweep itself: {}",
        dump(&steps.db).await
    );
    // Without the run ever answering.
    until(&steps, "the step is done", async || {
        reload(&steps.db, &first.id).await.status == "done"
    })
    .await;
    steps.release.add_permits(8);
    passes(&steps, 4).await;
    let hops = log(&steps.db, &first.id).await;
    assert_eq!(
        hops.last(),
        Some(&hop("lead_check", "done", Some("gate_skipped"), false)),
        "{hops:?}"
    );
    assert_eq!(gate_spent(&steps.db, &first.id).await, 0);
    until(&steps, "the next step runs", async || {
        !executions(&steps.db, &second.id).await.is_empty()
    })
    .await;
}

/// The archive sweep wakes a subtask that rests in the gate with no run (here
/// behind a Project pause): it is marked for the dispatcher at once.
#[tokio::test]
async fn archiving_the_lead_s_agent_wakes_a_step_that_waits_in_the_gate() {
    let steps = steps(&[], None).await;
    let (_root, child) = in_progress_root_with_a_step_in_the_gate(&steps, true).await;
    ProjectRepo::set_paused_at(&*steps.db, &steps.project, Some(now_rfc3339()))
        .await
        .unwrap();
    passes(&steps, 2).await;
    assert_eq!(executions(&steps.db, &child.id).await, Vec::new());
    sqlx::query("UPDATE task_schedule_dirty SET dirty=0 WHERE task_id=?")
        .bind(&child.id)
        .execute(steps.db.pool())
        .await
        .unwrap();
    steps
        .dispatcher
        .task_service
        .on_agent_deleted(&steps.planner)
        .await
        .expect("the lead's Agent is swept");
    let dirty: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM task_schedule_dirty WHERE task_id=? AND dirty=1)",
    )
    .bind(&child.id)
    .fetch_one(steps.db.pool())
    .await
    .unwrap();
    assert!(dirty, "the sweep marks the waiting step");
    ProjectRepo::set_paused_at(&*steps.db, &steps.project, None)
        .await
        .unwrap();
    until(&steps, "the step is done", async || {
        reload(&steps.db, &child.id).await.status == "done"
    })
    .await;
    assert_eq!(executions(&steps.db, &child.id).await, Vec::new());
}

/// The lead's Agent is paused and no check runs: the subtask does not pass
/// (a pause is temporary, and nobody validated the step) and does not rest
/// silently: it is parked on that Agent by name, as a planning root is on its
/// paused planner. Resuming the Agent starts the check.
#[tokio::test]
async fn a_paused_lead_agent_parks_the_step_on_that_agent_until_it_resumes() {
    let steps = steps(&[PASS], None).await;
    let (_root, child) = in_progress_root_with_a_step_in_the_gate(&steps, true).await;
    db::AgentRepo::set_paused(&*steps.db, &steps.planner, true)
        .await
        .unwrap();
    passes(&steps, 4).await;
    let parked = reload(&steps.db, &child.id).await;
    assert_eq!(parked.status, "lead_check", "{}", dump(&steps.db).await);
    assert_eq!(executions(&steps.db, &child.id).await, Vec::new());
    let stored = condition(&steps.db, &child.id).await;
    let stored: serde_json::Value = serde_json::from_str(&stored).expect("a condition is stored");
    assert_eq!(stored["kind"], "parked", "{stored}");
    assert_eq!(stored["primary"]["kind"], "agent", "{stored}");
    assert_eq!(
        stored["primary"]["agent_id"],
        steps.planner.as_str(),
        "the park names the paused lead: {stored}"
    );
    assert_eq!(stored["primary"]["status"], "paused", "{stored}");

    db::AgentRepo::set_paused(&*steps.db, &steps.planner, false)
        .await
        .unwrap();
    until(&steps, "the step is done", async || {
        reload(&steps.db, &child.id).await.status == "done"
    })
    .await;
    assert_eq!(
        log(&steps.db, &child.id).await,
        vec![hop("lead_check", "done", None, false)],
        "the lead's own pass, not a skip"
    );
}

/// A pause after admission must not turn the unlaunched check into a manual
/// retry. The Agent's resume owns recovery and starts a fresh check.
#[tokio::test]
async fn resuming_a_lead_paused_between_check_admission_and_launch_starts_a_fresh_check() {
    let steps = steps(&[PASS], None).await;
    let (_root, child) = in_progress_root_with_a_step_in_the_gate(&steps, true).await;
    // The admission has stored both the execution and its workspace grant.
    // Pause at that boundary, before start_execution verifies the grant.
    sqlx::query(&format!(
        "CREATE TRIGGER pause_lead_before_launch AFTER INSERT ON workspace_lease
         WHEN NEW.task_id='{}'
         BEGIN UPDATE agent_identity SET paused=1, version=version+1 WHERE id='{}'; END",
        child.id, steps.planner
    ))
    .execute(steps.db.pool())
    .await
    .unwrap();
    until(
        &steps,
        "the unlaunched check failed and parked on its Agent",
        async || {
            let stored = condition(&steps.db, &child.id).await;
            serde_json::from_str::<serde_json::Value>(&stored).is_ok_and(|stored| {
                stored["primary"]["kind"] == "agent"
                    && stored["primary"]["agent_id"] == steps.planner
                    && stored["primary"]["status"] == "paused"
            }) && executions(&steps.db, &child.id).await
                == vec![("planner".into(), "failed".into())]
        },
    )
    .await;
    let failed: (String, String) =
        sqlx::query_as("SELECT id,error FROM execution WHERE task_id=? AND role='planner'")
            .bind(&child.id)
            .fetch_one(steps.db.pool())
            .await
            .unwrap();
    assert!(
        failed.1.contains("execution setup required"),
        "{}",
        failed.1
    );
    assert_eq!(reload(&steps.db, &child.id).await.status, "lead_check");
    assert_eq!(steps.replies.lock().unwrap().len(), 1, "nothing launched");
    sqlx::query("DROP TRIGGER pause_lead_before_launch")
        .execute(steps.db.pool())
        .await
        .unwrap();
    db::AgentRepo::set_paused(&*steps.db, &steps.planner, false)
        .await
        .unwrap();
    until(
        &steps,
        "resume starts a fresh check and accepts the step",
        async || reload(&steps.db, &child.id).await.status == "done",
    )
    .await;
    assert_eq!(
        executions(&steps.db, &child.id).await,
        vec![
            ("planner".into(), "failed".into()),
            ("planner".into(), "completed".into())
        ]
    );
    let fresh: (String, Option<String>) = sqlx::query_as(
        "SELECT id,parent_execution_id FROM execution WHERE task_id=? AND status='completed'",
    )
    .bind(&child.id)
    .fetch_one(steps.db.pool())
    .await
    .unwrap();
    assert_ne!(fresh.0, failed.0);
    assert_eq!(fresh.1, None, "a fresh check rather than a resumed run");
    assert_eq!(
        log(&steps.db, &child.id).await,
        vec![hop("lead_check", "done", None, false)]
    );
    assert_eq!(gate_spent(&steps.db, &child.id).await, 0);
}

/// While the lead's Agent stays paused, the unlaunched check is not started
/// again: one run, and the typed park that names the Agent, over ten passes.
#[tokio::test]
async fn a_lead_paused_before_launch_is_not_relaunched_while_it_stays_paused() {
    let steps = steps(&[PASS], None).await;
    let (_root, child) = in_progress_root_with_a_step_in_the_gate(&steps, true).await;
    at_admission_before_launch(&steps, &child.id, &steps.planner, "paused=1").await;
    until(&steps, "the step parks on its paused lead", async || {
        parked_on_paused(&condition(&steps.db, &child.id).await, &steps.planner)
            && !executions(&steps.db, &child.id).await.is_empty()
    })
    .await;
    passes(&steps, 10).await;
    assert_eq!(
        executions(&steps.db, &child.id).await,
        vec![("planner".into(), "failed".into())],
        "no launch, defer, relaunch loop\n{}",
        dump(&steps.db).await
    );
    assert!(
        parked_on_paused(&condition(&steps.db, &child.id).await, &steps.planner),
        "{}",
        dump(&steps.db).await
    );
    assert_eq!(reload(&steps.db, &child.id).await.status, "lead_check");
    assert_eq!(steps.replies.lock().unwrap().len(), 1, "nothing launched");
}

/// The same race for a coder on a plain Task: the Task is not blocked for a
/// person and is not relaunched while the Agent stays paused; it parks on
/// the paused Agent by name, and the resume starts a fresh run.
#[tokio::test]
async fn a_coder_paused_before_launch_parks_on_the_agent_and_runs_when_it_resumes() {
    let steps = steps(&[], None).await;
    let task = seed_task(&steps.db, &steps.project, "plain", "todo", 1).await;
    assign_role(
        &steps.db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &steps.coder,
    )
    .await;
    at_admission_before_launch(&steps, &task.id, &steps.coder, "paused=1").await;
    until(&steps, "the Task parks on its paused coder", async || {
        parked_on_paused(&condition(&steps.db, &task.id).await, &steps.coder)
            && !executions(&steps.db, &task.id).await.is_empty()
    })
    .await;
    passes(&steps, 10).await;
    assert_eq!(
        executions(&steps.db, &task.id).await,
        vec![("coder".into(), "failed".into())],
        "no launch, defer, relaunch loop\n{}",
        dump(&steps.db).await
    );
    let parked = reload(&steps.db, &task.id).await;
    assert!(
        parked_on_paused(&condition(&steps.db, &task.id).await, &steps.coder),
        "{}",
        dump(&steps.db).await
    );
    assert_eq!(parked.blocked_json, None, "not a block for a person");
    assert_eq!(parked.error_annotation, None, "{}", dump(&steps.db).await);
    sqlx::query("DROP TRIGGER change_agent_before_launch")
        .execute(steps.db.pool())
        .await
        .unwrap();
    db::AgentRepo::set_paused(&*steps.db, &steps.coder, false)
        .await
        .unwrap();
    until(&steps, "the resume starts a fresh coder run", async || {
        executions(&steps.db, &task.id)
            .await
            .iter()
            .any(|(role, status)| role == "coder" && status != "failed")
    })
    .await;
}

/// An Agent archived (not paused) at the same boundary is not deferred: no
/// resume will come. The run fails once, is not started again, and the
/// archive sweep passes the step as for any removed lead.
#[tokio::test]
async fn a_lead_archived_before_launch_is_not_deferred_and_the_sweep_passes_the_step() {
    let steps = steps(&[PASS], None).await;
    let (_root, child) = in_progress_root_with_a_step_in_the_gate(&steps, true).await;
    at_admission_before_launch(
        &steps,
        &child.id,
        &steps.planner,
        "archived_at='2026-01-01T00:00:00Z'",
    )
    .await;
    until(&steps, "the unlaunched check failed", async || {
        executions(&steps.db, &child.id).await == vec![("planner".into(), "failed".into())]
    })
    .await;
    passes(&steps, 10).await;
    assert_eq!(
        executions(&steps.db, &child.id).await,
        vec![("planner".into(), "failed".into())],
        "{}",
        dump(&steps.db).await
    );
    steps
        .dispatcher
        .task_service
        .on_agent_deleted(&steps.planner)
        .await
        .expect("the lead's Agent is swept");
    until(&steps, "the step is done", async || {
        reload(&steps.db, &child.id).await.status == "done"
    })
    .await;
    assert_eq!(steps.replies.lock().unwrap().len(), 1, "nothing launched");
}

/// The lead's Agent is paused while its check runs: a pause starts nothing
/// new and stops nothing, as for every other run. The check finishes and its
/// verdict settles the step.
#[tokio::test]
async fn pausing_the_lead_s_agent_lets_its_running_check_finish_and_settle() {
    let steps = steps(&[HELD_PASS], Some("second")).await;
    let (_root, first, _second) = root_with_two_steps(&steps, true).await;
    // The run is past its launch: the executor holds its reply.
    until(&steps, "the step check runs", async || {
        running_check(&steps.db, &first.id).await && steps.replies.lock().unwrap().is_empty()
    })
    .await;
    db::AgentRepo::set_paused(&*steps.db, &steps.planner, true)
        .await
        .unwrap();
    passes(&steps, 3).await;
    assert!(
        running_check(&steps.db, &first.id).await,
        "{}",
        dump(&steps.db).await
    );
    steps.release.add_permits(1);
    until(&steps, "the step is done", async || {
        reload(&steps.db, &first.id).await.status == "done"
    })
    .await;
    assert_eq!(
        log(&steps.db, &first.id).await.last(),
        Some(&hop("lead_check", "done", None, false))
    );
}

/// Project default role assignments are given to a subtask the dispatcher
/// releases (`Step::ApplyDefaults`). The step-check role is not among them:
/// on a subtask it is the root's lead and nothing else, and no row is copied.
#[tokio::test]
async fn project_defaults_do_not_copy_the_step_check_role_onto_a_subtask() {
    let steps = steps(&[], None).await;
    let settings = serde_json::json!({"default_role_assignments": [
        {"role_name": "planner", "assignee_type": "agent", "assignee_id": steps.planner},
        {"role_name": "coder", "assignee_type": "agent", "assignee_id": steps.coder},
    ]});
    sqlx::query("UPDATE project SET settings=? WHERE id=?")
        .bind(settings.to_string())
        .bind(&steps.project)
        .execute(steps.db.pool())
        .await
        .unwrap();
    let root = seed_task(&steps.db, &steps.project, "root", "in_progress", 1).await;
    let child = seed_subtask(&steps.db, &root, "first", "todo", 0).await;
    steps
        .dispatcher
        .task_service
        .assign_project_default_roles(&child)
        .await
        .expect("defaults are applied");
    let roles: Vec<String> = sqlx::query_scalar(
        "SELECT role_name FROM task_role_assignment WHERE task_id=? ORDER BY role_name",
    )
    .bind(&child.id)
    .fetch_all(steps.db.pool())
    .await
    .unwrap();
    assert_eq!(roles, vec!["coder".to_owned()]);
    // A root still gets every default role.
    let other = seed_task(&steps.db, &steps.project, "single", "todo", 1).await;
    steps
        .dispatcher
        .task_service
        .assign_project_default_roles(&other)
        .await
        .expect("defaults are applied");
    let roles: Vec<String> = sqlx::query_scalar(
        "SELECT role_name FROM task_role_assignment WHERE task_id=? ORDER BY role_name",
    )
    .bind(&other.id)
    .fetch_all(steps.db.pool())
    .await
    .unwrap();
    assert_eq!(roles, vec!["coder".to_owned(), "planner".to_owned()]);
}
