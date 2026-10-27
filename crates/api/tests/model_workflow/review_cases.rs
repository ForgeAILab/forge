//! Review-entry check progress across pauses, holds, releases and restarts.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_project_pause_lets_the_pending_entry_check_finish_and_resume_does_not_repeat_it() {
    if let Err(violation) = pause_while_the_entry_check_is_pending(PauseKind::Project, false).await
    {
        panic!("{violation}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_agent_pause_lets_the_pending_entry_check_finish_and_resume_does_not_repeat_it() {
    if let Err(violation) = pause_while_the_entry_check_is_pending(PauseKind::Agents, false).await {
        panic!("{violation}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_task_waiting_for_its_pending_entry_check_offers_cancel_and_no_hold() {
    if let Err(violation) = pause_while_the_entry_check_is_pending(PauseKind::TaskHold, false).await
    {
        panic!("{violation}");
    }
}

/// The other state of that wait: the machine had a slot and the entry check
/// runs. The Task waits for its check all the same and offers the same, also
/// when its reviewer or its Project is paused meanwhile: the check wait is
/// what parks the Task, so no pause puts a `hold` on offer beside it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_task_whose_entry_check_runs_offers_cancel_and_no_hold() {
    for pause in [None, Some(PauseKind::Agents), Some(PauseKind::Project)] {
        let mut world = World::new().await;
        let outcome: Result<(), String> = async {
            world.apply(&Action::Create).await?;
            world.quiesce().await?;
            // A check that runs until this test has looked at it: it ends
            // when the gate file appears, not after a time the look has to
            // fit into. (It also ends when the model directory is removed,
            // so a failed case leaves no process behind.)
            let gate = world.dir.path().join("entry-check-may-end");
            sqlx::query("UPDATE task SET task_state_config=json_set(task_state_config,'$.review.ci_steps',json_array(?)) WHERE id=?")
                .bind(format!(
                    "until [ -e '{gate}' ] || [ ! -d '{dir}' ]; do sleep 0.05; done; test -f {file}",
                    gate = gate.display(),
                    dir = world.dir.path().display(),
                    file = deliverable(0)
                ))
                .bind(&world.tasks[0])
                .execute(&world.live().pool)
                .await
                .map_err(|error| error.to_string())?;
            world.apply(&Action::Finish(0, Outcome::Success)).await?;
            // Not `quiesce`: that waits for the check to end. The Task's
            // steps run beside this look.
            let drain = |world: &World| {
                let service = world.live().state.task_service.clone();
                let id = world.tasks[0].clone();
                tokio::spawn(async move { service.drain(&id).await.map(|_| ()) })
            };
            let mut steps = drain(&world);
            let mut looked = 0;
            loop {
                if steps.is_finished() {
                    steps = drain(&world);
                }
                let task = world.task(0).await?;
                let runs = world.check_runs(0).await?;
                if task["status"] == "review" && runs == ["running"] {
                    break;
                }
                looked += 1;
                if looked > 400 || runs.iter().any(|state| state != "queued" && state != "running")
                {
                    return Err(format!(
                        "expected a Task in review with one running check, got {} {runs:?} {}",
                        task["status"], task["condition"]
                    ));
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            match pause {
                Some(PauseKind::Project) => {
                    world.apply(&Action::PauseProject).await?;
                }
                Some(PauseKind::Agents) => world.pause_agents(true).await?,
                _ => {}
            }
            world
                .live()
                .runtime
                .task_dispatcher
                .check_once()
                .await
                .map_err(|error| error.to_string())?;
            let task = world.task(0).await?;
            let (_, offers) = world.offers(0).await?;
            let verbs: Vec<&str> = offers
                .iter()
                .filter_map(|offer| offer["action"]["verb"].as_str())
                .collect();
            world.trace.push(format!(
                "entry check running, pause {pause:?}: offers {verbs:?}, condition {}",
                task["condition"]
            ));
            if world.check_runs(0).await? != ["running"] {
                return Err("the check ended before the offers were read".to_owned());
            }
            if verbs != ["cancel"] {
                return Err(format!(
                    "a Task whose entry check runs (pause {pause:?}) offers {verbs:?}, expected only cancel"
                ));
            }
            std::fs::write(&gate, b"").map_err(|error| error.to_string())?;
            let _ = steps.await;
            // `drive_to_settlement` resumes the Project.
            if pause == Some(PauseKind::Agents) {
                world.pause_agents(false).await?;
            }
            world.drive_to_settlement().await
        }
        .await;
        if let Err(violation) = outcome {
            panic!("{violation}\ntrace:\n{}", world.trace.join("\n"));
        }
    }
}

/// As the owner does it: whichever order the machine produces. (It asserted
/// exactly two runs, and failed in CI when the release asked before the check
/// worker had looked at the run.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hold_command_during_the_entry_check_is_resumed_by_its_release() {
    if let Err(violation) = hold_during_the_entry_check(HoldFinds::TheRunInFlight).await {
        panic!("{violation}");
    }
}

/// The first order, every run: the check worker stops the run nobody waits
/// for, and the release runs the check again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hold_that_stops_the_running_entry_check_runs_it_again_at_its_release() {
    if let Err(violation) = hold_during_the_entry_check(HoldFinds::ARunItStops).await {
        panic!("{violation}");
    }
}

/// The second order, every run: the run ended before the hold. Its result is
/// not applied under the hold; the release's entry is answered by it, and
/// the same commit and configuration do not run a second time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hold_after_the_entry_check_ended_keeps_its_result_for_the_release() {
    hold_during_the_entry_check_or_panic(HoldFinds::AFinishedRun).await;
}

/// The second order as CI met it: the release follows its hold before the
/// check worker has looked at the run. The run was never stopped, the entry
/// joins it, and its result is the one answer. (On a machine too slow for
/// that, this is the first order.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_release_right_after_the_hold_joins_the_entry_check_that_still_runs() {
    hold_during_the_entry_check_or_panic(HoldFinds::ARunItsReleaseJoins).await;
}

/// Between the two: the release arrives while the run is being stopped. The
/// entry never takes that run's cancellation for an answer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_release_while_the_entry_check_is_being_stopped_still_gets_one_pass() {
    hold_during_the_entry_check_or_panic(HoldFinds::ARunBeingStopped).await;
}

/// The hold stops the hooks step after it asked for the check and before it
/// began to wait. The request is recorded on the step with the request
/// itself, so the hold withdraws it as it does for a step that waits: the
/// run nobody waits for is stopped, no result is delivered to a step that is
/// gone, the attempt is closed under the hold, and the release asks again.
/// (The step is stopped at that exact point, in the real hook.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hold_that_stops_the_step_that_just_asked_withdraws_the_entry_check_it_asked_for() {
    hold_during_the_entry_check_or_panic(HoldFinds::AStepThatJustAsked).await;
}

/// The same window under a Cancel: nothing is left of the entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancel_that_stops_the_step_that_just_asked_leaves_nothing_of_the_entry_check() {
    hold_during_the_entry_check_or_panic(HoldFinds::AStepThatJustAskedCancelled).await;
}

/// A held Task in `review` and a system re-entry of its review (recovery, a
/// queued command). The re-entry is refused while the hold stands and changes
/// nothing. With the real dispatcher nothing has to ask again after the
/// release: the dispatcher gives the review its reviewer run and the Task
/// goes on to its end by itself. (A Task that waits for its entry CHECK
/// cannot be held at all: see the two cases above.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_held_task_in_review_refuses_a_system_re_entry_and_goes_on_by_itself_after_release() {
    let mut world = World::new().await;
    let outcome: Result<(), String> = async {
        world.apply(&Action::Create).await?;
        world.quiesce().await?;
        world.apply(&Action::Finish(0, Outcome::Success)).await?;
        // The reviewer is paused: the Task waits in `review` for it, its
        // entry check applied, and that wait is one the owner can hold.
        world.pause_agents(true).await?;
        world.quiesce().await?;
        let facts = |world: &World| {
            let pool = world.live().pool.clone();
            let id = world.tasks[0].clone();
            async move {
                let count = |sql: &'static str| {
                    let (pool, id) = (pool.clone(), id.clone());
                    async move {
                        sqlx::query_scalar::<_, i64>(sql)
                            .bind(id)
                            .fetch_one(&pool)
                            .await
                            .map_err(|error| error.to_string())
                    }
                };
                Ok::<_, String>((
                    count("SELECT COUNT(*) FROM review WHERE task_id = ?").await?,
                    // Review-entry requests. The queue head's recorded reuse
                    // of the entry's passed run (origin `integration`, written
                    // when the released Task merges) is not an entry that ran
                    // again; that it has no run of its own is asserted below.
                    count("SELECT COUNT(*) FROM check_consumer WHERE task_id = ? AND origin = 'entry'").await?,
                    count("SELECT COUNT(*) FROM task_step WHERE task_id = ? AND kind = 'hooks'")
                        .await?,
                    count("SELECT status_epoch FROM task WHERE id = ?").await?,
                    count("SELECT COUNT(*) FROM execution WHERE task_id = ? AND role = 'reviewer'").await?,
                ))
            }
        };
        let task = world.task(0).await?;
        let reviewing = facts(&world).await?;
        if task["status"] != "review" || reviewing.4 != 0 {
            return Err(format!(
                "expected a Task in review waiting for its paused reviewer, got {} {reviewing:?} {}",
                task["status"], task["condition"]
            ));
        }
        let (version, offers) = world.offers(0).await?;
        let hold = offers
            .iter()
            .find(|offer| offer["action"]["verb"] == "hold")
            .ok_or_else(|| {
                format!(
                    "a Task waiting in review for its reviewer offers no hold: {offers:?} {}",
                    task["condition"]
                )
            })?;
        world.apply_offer(0, version, hold).await?;
        world.quiesce().await?;
        let held = facts(&world).await?;
        let id = world.tasks[0]
            .parse()
            .map_err(|_| "task id".to_owned())?;
        let refused = world
            .live()
            .state
            .task_service
            .rerun_review(
                id,
                api_types::Actor::system(api_types::SystemComponent::CrashRecovery),
            )
            .await;
        if !matches!(refused, Err(services::ServiceError::Conflict(_))) {
            return Err(format!(
                "a system re-entry ran on a held Task: {:?}",
                refused.map(|task| task.status)
            ));
        }
        world.quiesce().await?;
        if facts(&world).await? != held {
            return Err(format!(
                "the refused re-entry changed the held Task: {held:?} -> {:?}",
                facts(&world).await?
            ));
        }
        let (version, offers) = world.offers(0).await?;
        let release = offers
            .iter()
            .find(|offer| offer["action"]["verb"] == "release")
            .ok_or_else(|| format!("the held Task offers no release: {offers:?}"))?;
        world.apply_offer(0, version, release).await?;
        world.pause_agents(false).await?;
        world.quiesce().await?;
        // Nothing asked again and no offer was applied: the dispatcher gave
        // the review its reviewer run, on the check that was applied before
        // the hold (no second check, no second attempt).
        let released = facts(&world).await?;
        let task = world.task(0).await?;
        world.trace.push(format!(
            "released: {} {released:?} {}",
            task["status"], task["condition"]
        ));
        if released.4 < 1 {
            return Err(format!(
                "after the release nothing continues the review: {} {released:?} {}",
                task["status"], task["condition"]
            ));
        }
        if (released.0, released.1) != (held.0, held.1) {
            return Err(format!(
                "the release ran the entry again: {held:?} -> {released:?}"
            ));
        }
        let runs = world.check_runs(0).await?;
        let heads = world.head_consumers_reuse_the_entry_run(0).await?;
        if runs != ["succeeded"] || heads > 1 {
            return Err(format!(
                "the release ran a check again: runs {runs:?}, head reuses {heads}"
            ));
        }
        world.drive_to_settlement().await
    }
    .await;
    if let Err(violation) = outcome {
        panic!("{violation}\ntrace:\n{}", world.trace.join("\n"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_after_the_check_request_before_the_suspend_asks_once() {
    restart_while_the_entry_check_is_awaited(RestartPoint::AfterRequestBeforeSuspend).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_after_the_check_delivery_before_the_wake_resumes_the_step() {
    restart_while_the_entry_check_is_awaited(RestartPoint::AfterDeliveryBeforeWake).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_after_the_wake_before_the_settle_settles_from_the_stored_result() {
    restart_while_the_entry_check_is_awaited(RestartPoint::AfterWakeBeforeSettle).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dead_lettered_check_delivery_is_read_by_the_step_at_its_deadline() {
    restart_while_the_entry_check_is_awaited(RestartPoint::DeliveryDeadLettered).await;
}

/// One slot. After the Project is resumed, the first Task's reviewer run
/// takes the slot and the second Task (whose coder run ended under the pause)
/// is refused a run slot. Its `machine_capacity` wait and its
/// `task_schedule_wait` row (`daemon_id = '*'`) are written after the
/// dispatcher read the machine as full. When the reviewer run ended between
/// that read and those writes, the mark for "waiters on a run slot" found no
/// waiter: the Task stayed `in_progress`, parked on `capacity` (scope
/// `machine`), with the machine idle, no step pending and nothing due.
///
/// A Task that starts waiting for a run slot is now read once more after its
/// wait row exists (`SqliteDb::schedule_wait`), and every such row carries a
/// recheck deadline. This case failed in about two of three runs of the
/// whole target on a loaded machine.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_run_slot_wait_registered_after_the_release_is_still_woken() {
    if let Err(violation) = pause_while_the_entry_check_is_pending(PauseKind::Project, true).await {
        panic!("{violation}");
    }
}
