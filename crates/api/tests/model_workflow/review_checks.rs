//! Shared review-entry pause, hold, restart and check-delivery scenarios.

use super::*;

/// How a pause lands on a Task whose review-entry check is requested and not
/// yet finished (its hooks step is suspended).
#[derive(Clone, Copy, PartialEq, Debug)]
pub(super) enum PauseKind {
    Project,
    /// Both of the Project's Agents: the reviewer is the check's follow-up.
    Agents,
    /// The Task itself: `hold` is the owner's Task-level pause.
    TaskHold,
}

impl World {
    /// The states of the check runs the Task asked for, oldest first.
    /// The states of the check RUNS a Task's consumers were answered by, one
    /// entry per run. Two consumers of one run (the review entry and the
    /// queue head that reuses its passed result) are one run.
    pub(super) async fn check_runs(&self, index: usize) -> Result<Vec<String>, String> {
        sqlx::query_scalar(
            "SELECT r.state FROM check_run r
             WHERE r.id IN (SELECT c.run_id FROM check_consumer c WHERE c.task_id = ?)
             ORDER BY r.created_at, r.rowid",
        )
        .bind(&self.tasks[index])
        .fetch_all(&self.live().pool)
        .await
        .map_err(|error| error.to_string())
    }

    /// The queue head of an unchanged commit and target is a recorded
    /// consumer of the review entry's passed run: it never has a run of its
    /// own, never a delivery that is applied a second time, and there is at
    /// most one of it per merging entry.
    pub(super) async fn head_consumers_reuse_the_entry_run(
        &self,
        index: usize,
    ) -> Result<i64, String> {
        /// Origin, run, result, applied at.
        type Consumer = (String, Option<String>, Option<String>, Option<String>);
        let rows: Vec<Consumer> = sqlx::query_as(
            "SELECT origin, run_id, result_id, applied_at FROM check_consumer
             WHERE task_id = ? ORDER BY created_at, rowid",
        )
        .bind(&self.tasks[index])
        .fetch_all(&self.live().pool)
        .await
        .map_err(|error| error.to_string())?;
        let passed: Vec<(String, String)> = sqlx::query_as(
            "SELECT c.run_id, c.result_id FROM check_consumer c
             JOIN check_result r ON r.id = c.result_id
             WHERE c.task_id = ? AND c.origin = 'entry' AND r.outcome = 'pass'",
        )
        .bind(&self.tasks[index])
        .fetch_all(&self.live().pool)
        .await
        .map_err(|error| error.to_string())?;
        let mut heads = 0;
        for (origin, run, result, applied) in &rows {
            if origin == "entry" {
                continue;
            }
            heads += 1;
            let reused = matches!((run, result), (Some(run), Some(result))
                if passed.iter().any(|(r, s)| r == run && s == result));
            // A reuse is created answered: the `settle` step that records it
            // applies it, and no delivery step is spent on it.
            if origin != "integration" || !reused || applied.is_none() {
                return Err(format!(
                    "a consumer beside the review entry is not a reuse of its passed run: {rows:?}"
                ));
            }
        }
        Ok(heads)
    }

    pub(super) async fn pause_agents(&mut self, pause: bool) -> Result<(), String> {
        let verb = if pause { "pause" } else { "resume" };
        for agent in [self.coder_id.clone(), self.reviewer_id.clone()] {
            let uri = format!("/api/v1/agents/{agent}/{verb}");
            let (status, body) = self.request(Method::POST, &uri, None).await?;
            if !status.is_success() {
                return Err(format!("agent {verb} was refused: {status} {body}"));
            }
        }
        Ok(())
    }
}

/// A pause while the Task's hooks step is suspended on its review-entry
/// check (here: queued behind a full machine). The check is machine work
/// already asked for:
/// - **Project pause, Agent pause**: the check runs to its end and its result
///   is applied; the pause only holds what comes after it (the reviewer run).
///   Resuming continues from the applied result: the check is not run again.
/// - **Task hold**: not on offer in this state. A Task waiting for its entry
///   check offers `cancel` only (which abandons the wait and stops the run);
///   there is no Task-level pause to resume from, and the check finishes.
pub(super) async fn pause_while_the_entry_check_is_pending(
    kind: PauseKind,
    keep_one_slot: bool,
) -> Result<Vec<String>, String> {
    // Pinned: the case starts with two Tasks running side by side.
    PINNED_RUN_CAP
        .scope(
            Some(MODEL_RUN_CAP),
            pause_with_two_slots_then_one(kind, keep_one_slot),
        )
        .await
}

pub(super) async fn pause_with_two_slots_then_one(
    kind: PauseKind,
    keep_one_slot: bool,
) -> Result<Vec<String>, String> {
    let mut world = World::new().await;
    let outcome: Result<(), String> = async {
        // Two Tasks run. The machine then has one slot, which the second
        // Task's run holds: the first Task's entry check is requested and
        // waits for it, its hooks step suspended.
        for action in [Action::Create, Action::Create] {
            world.apply(&action).await?;
            world.quiesce().await?;
        }
        let cap = &world.live().state.db.server_run_cap;
        cap.set(Some(1), 1, &cap.embedded_machine_id());
        world.apply(&Action::Finish(0, Outcome::Success)).await?;
        world.quiesce().await?;
        let task = world.task(0).await?;
        if task["status"] != "review" || world.check_runs(0).await? != ["queued"] {
            return Err(format!(
                "expected a Task in review with one queued check, got {} {:?}",
                task["status"],
                world.check_runs(0).await?
            ));
        }
        match kind {
            PauseKind::Project => {
                world.apply(&Action::PauseProject).await?;
            }
            PauseKind::Agents => world.pause_agents(true).await?,
            PauseKind::TaskHold => {
                let (_, offers) = world.offers(0).await?;
                let verbs: Vec<&str> = offers
                    .iter()
                    .filter_map(|offer| offer["action"]["verb"].as_str())
                    .collect();
                if verbs != ["cancel"] {
                    return Err(format!(
                        "a Task waiting for its entry check offers {verbs:?}, expected only cancel"
                    ));
                }
            }
        }
        world.quiesce().await?;
        // The slot frees: the check that was asked for before the pause runs.
        world.apply(&Action::Finish(1, Outcome::Success)).await?;
        let settled = tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                world.quiesce().await?;
                let runs = world.check_runs(0).await?;
                if runs
                    .iter()
                    .all(|state| ["succeeded", "failed", "cancelled"].contains(&state.as_str()))
                {
                    return Ok::<_, String>(runs);
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .map_err(|_| "the check did not finish under the pause".to_owned())??;
        world.quiesce().await?;
        let paused = world.task(0).await?;
        world.trace.push(format!(
            "{kind:?} paused: checks {settled:?}, task {} {}",
            paused["status"], paused["condition"]
        ));
        let reviewer_runs: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM execution WHERE role = 'reviewer' AND task_id = ?",
        )
        .bind(&world.tasks[0])
        .fetch_one(&world.live().pool)
        .await
        .map_err(|error| error.to_string())?;
        match kind {
            PauseKind::Project | PauseKind::Agents => {
                if settled != ["succeeded"] {
                    return Err(format!("the check did not run to its end: {settled:?}"));
                }
                let applied: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM check_consumer WHERE task_id = ? AND origin='entry' AND applied_at IS NOT NULL",
                )
                .bind(&world.tasks[0])
                .fetch_one(&world.live().pool)
                .await
                .map_err(|error| error.to_string())?;
                if applied != 1 {
                    return Err("the finished check's result was not applied".to_owned());
                }
                if paused["status"] != "review" || reviewer_runs != 0 {
                    return Err(format!(
                        "the pause did not hold the reviewer run: {} with {reviewer_runs} reviewer run(s)",
                        paused["status"]
                    ));
                }
            }
            PauseKind::TaskHold => {
                if settled != ["succeeded"] {
                    return Err(format!("the check did not run to its end: {settled:?}"));
                }
            }
        }
        if !keep_one_slot {
            // The pause is what this case is about. With one slot, the two
            // Tasks' follow-up runs would also race for it after the resume.
            let cap = &world.live().state.db.server_run_cap;
            cap.set(Some(MODEL_RUN_CAP), MODEL_RUN_CAP, &cap.embedded_machine_id());
        }
        match kind {
            PauseKind::Project => {
                world.apply(&Action::ResumeProject).await?;
            }
            PauseKind::Agents => world.pause_agents(false).await?,
            PauseKind::TaskHold => {}
        }
        world.quiesce().await?;
        world.drive_to_settlement().await?;
        for index in 0..2 {
            let task = world.task(index).await?;
            if task["status"] != "done" {
                return Err(format!("task {index} ended in {}", task["status"]));
            }
        }
        let runs = world.check_runs(0).await?;
        let heads = world.head_consumers_reuse_the_entry_run(0).await?;
        if runs != ["succeeded"] || heads > 1 {
            return Err(format!(
                "check runs after resume: {runs:?} with {heads} head reuse(s), expected one run"
            ));
        }
        Ok(())
    }
    .await;
    match outcome {
        Ok(()) => Ok(world.trace),
        Err(violation) => Err(format!("{violation}\ntrace:\n{}", world.trace.join("\n"))),
    }
}

/// Where a hold command finds the entry check of the Task it holds.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum HoldFinds {
    /// The check runs and the hold is sent at once: whichever of the orders
    /// below the machine produces.
    TheRunInFlight,
    /// The check runs and has a long way to go: the check worker stops it
    /// before the release.
    ARunItStops,
    /// The same, and the release is sent the moment the check worker begins
    /// to stop the run.
    ARunBeingStopped,
    /// The check runs, and the release follows the hold at once: before the
    /// check worker's next look, unless the machine is very slow.
    ARunItsReleaseJoins,
    /// The run has ended and its result is stored, but no Task step has
    /// read it yet.
    AFinishedRun,
    /// The check is asked for and the step that asked has not begun to wait
    /// for it: the hold stops that step in the middle.
    AStepThatJustAsked,
    /// The same window, and the owner command is a Cancel.
    AStepThatJustAskedCancelled,
}

/// A hold COMMAND during the entry check. It is not on offer (see above) and
/// is accepted all the same, because a hold may stop hooks in flight: the
/// hooks step is superseded, its check request withdrawn, the attempt closed.
/// The release runs the review entry again by itself. Before, the released
/// Task had no check, and its reviewer was given the review.
///
/// The hold withdraws the request; it does not wait for the run. The check
/// worker stops a run nobody waits for at its next look (within a second),
/// so one of two things is true at the release, and both are right:
/// - the run was stopped: the entry asks again and a second run answers it;
/// - the run was not stopped (it had ended, or the release asked before the
///   worker looked): its result answers the entry, and nothing runs twice.
///
/// Either way the Task stays in `review` while it is held and no reviewer
/// runs under the hold; one run of the check passes, one review attempt
/// passes on it, and every attempt before that one is closed.
pub(super) async fn hold_during_the_entry_check(finds: HoldFinds) -> Result<Vec<String>, String> {
    use HoldFinds::*;
    const REVIEWER_RUNS: &str =
        "SELECT COUNT(*) FROM execution WHERE task_id = ? AND role = 'reviewer'";
    // Results the check worker delivered to a waiting step.
    const DELIVERED: &str = "SELECT COUNT(*) FROM check_consumer WHERE task_id = ? AND origin='entry' AND applied_at IS NOT NULL";
    const CONSUMERS: &str =
        "SELECT c.origin || ':' || COALESCE(r.state,'-') || ':' || COALESCE(x.outcome,'-')
                || CASE WHEN c.cancelled_at IS NOT NULL THEN ':withdrawn' ELSE '' END
                || CASE WHEN c.applied_at IS NOT NULL THEN ':delivered' ELSE '' END
         FROM check_consumer c LEFT JOIN check_run r ON r.id = c.run_id
         LEFT JOIN check_result x ON x.id = c.result_id
         WHERE c.task_id = ? ORDER BY c.created_at, c.rowid";
    const ENTRY_RUNS: &str = "SELECT r.state FROM check_run r WHERE r.id IN
           (SELECT c.run_id FROM check_consumer c WHERE c.task_id = ? AND c.origin = 'entry')
         ORDER BY r.created_at, r.rowid";
    const REVIEWS: &str =
        "SELECT attempt_number || ':' || status FROM review WHERE task_id = ? ORDER BY attempt_number";
    const STEPS: &str =
        "SELECT kind || ':' || status FROM task_step WHERE task_id = ? ORDER BY seq";
    const SET_CHECK: &str = "UPDATE task SET task_state_config=json_set(task_state_config,'$.review.ci_steps',json_array(?)) WHERE id=?";
    // The check worker alone runs the check in these (no drain beside it):
    // the check waits for the one slot, which a second Task's run holds,
    // until that run ends. The model has no step worker, so no Task step
    // runs meanwhile.
    let slot_seam = !matches!(finds, TheRunInFlight | ARunItStops);
    let just_asked = matches!(finds, AStepThatJustAsked | AStepThatJustAskedCancelled);
    let mut world = World::new().await;
    let outcome: Result<(), String> = async {
        for _ in 0..if slot_seam { 2 } else { 1 } {
            world.apply(&Action::Create).await?;
            world.quiesce().await?;
        }
        let pool = world.live().pool.clone();
        let task_id = world.tasks[0].clone();
        let sql = |error: sqlx::Error| error.to_string();
        let count = |sql: &'static str| {
            let (pool, id) = (pool.clone(), task_id.clone());
            async move {
                sqlx::query_scalar::<_, i64>(sql)
                    .bind(id)
                    .fetch_one(&pool)
                    .await
                    .map_err(|error| error.to_string())
            }
        };
        let list = |sql: &'static str| {
            let (pool, id) = (pool.clone(), task_id.clone());
            async move {
                sqlx::query_scalar::<_, String>(sql)
                    .bind(id)
                    .fetch_all(&pool)
                    .await
                    .map_err(|error| error.to_string())
            }
        };
        // A check that is still running when the hold arrives. The one the
        // check worker must stop runs long the first time only: its second
        // run is the one the release waits for.
        let first = world.dir.path().join("entry-check-ran");
        let command = match finds {
            TheRunInFlight | ARunItsReleaseJoins => {
                Some(format!("sleep 4 && test -f {}", deliverable(0)))
            }
            ARunItStops | ARunBeingStopped => Some(format!(
                "if [ ! -e '{0}' ]; then : > '{0}'; sleep 60; fi; test -f {1}",
                first.display(),
                deliverable(0)
            )),
            AFinishedRun | AStepThatJustAsked | AStepThatJustAskedCancelled => None,
        };
        if let Some(command) = command {
            sqlx::query(SET_CHECK)
                .bind(command)
                .bind(&task_id)
                .execute(&pool)
                .await
                .map_err(sql)?;
        }
        let mut steps = None;
        if slot_seam {
            let cap = &world.live().state.db.server_run_cap;
            cap.set(Some(1), 1, &cap.embedded_machine_id());
            if just_asked {
                // The real hooks step asks for the check (the run waits for
                // the one slot) and is stopped right there, before it
                // suspends: it holds its claim, and the drain that carries
                // it stays alive for the owner command to preempt.
                use services::workflow::actions::review_entry_halt as halt;
                let reached = halt::arm(&task_id, halt::AFTER_REQUEST_BEFORE_SUSPEND);
                world.apply(&Action::Finish(0, Outcome::Success)).await?;
                let drain = |world: &World| {
                    let service = world.live().state.task_service.clone();
                    let id = world.tasks[0].clone();
                    tokio::spawn(async move { service.drain(&id).await.map(|_| ()) })
                };
                let mut running = drain(&world);
                let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
                let asked = reached.notified();
                tokio::pin!(asked);
                loop {
                    tokio::select! {
                        _ = &mut asked => break,
                        _ = tokio::time::sleep(Duration::from_millis(25)) => {}
                    }
                    if running.is_finished() {
                        running = drain(&world);
                    }
                    if tokio::time::Instant::now() >= deadline {
                        halt::disarm(&task_id);
                        running.abort();
                        let task = world.task(0).await?;
                        return Err(format!(
                            "the entry never asked for its check: {} steps {:?}",
                            task["status"],
                            list(STEPS).await?
                        ));
                    }
                }
                let claimed = list(
                    "SELECT kind || ':' || status FROM task_step WHERE task_id = ? AND status IN ('claimed','suspended')",
                )
                .await?;
                if world.check_runs(0).await? != ["queued"] || claimed != ["hooks:claimed"] {
                    halt::disarm(&task_id);
                    running.abort();
                    return Err(format!(
                        "expected a queued check and a claimed hooks step: {:?} {claimed:?}",
                        world.check_runs(0).await?
                    ));
                }
                steps = Some(running);
            } else {
                world.apply(&Action::Finish(0, Outcome::Success)).await?;
                world.quiesce().await?;
                if world.check_runs(0).await? != ["queued"] {
                    return Err(format!(
                        "expected one queued check: {:?}",
                        world.check_runs(0).await?
                    ));
                }
                // The slot frees and the check worker runs the check.
                world.apply(&Action::Finish(1, Outcome::Success)).await?;
                let reached = if finds == AFinishedRun {
                    "SELECT COUNT(*) FROM check_consumer c JOIN check_run r ON r.id = c.run_id
                     WHERE c.task_id = ? AND r.state = 'succeeded' AND c.delivery_step_id IS NOT NULL"
                } else {
                    "SELECT COUNT(*) FROM check_consumer c JOIN check_run r ON r.id = c.run_id
                     WHERE c.task_id = ? AND r.state = 'running' AND r.dispatch_json IS NOT NULL"
                };
                tokio::time::timeout(Duration::from_secs(60), async {
                    while count(reached).await? != 1 {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Ok::<_, String>(())
                })
                .await
                .map_err(|_| format!("the check did not get there: {finds:?}"))??;
                if count("SELECT COUNT(*) FROM task_step WHERE task_id = ? AND status = 'suspended'")
                    .await?
                    != 1
                    || count(DELIVERED).await? != 0
                {
                    return Err("expected a suspended hooks step and an unread result".to_owned());
                }
            }
        } else {
            world.apply(&Action::Finish(0, Outcome::Success)).await?;
            // Not `quiesce`: that waits for the check to end.
            let drain = |world: &World| {
                let service = world.live().state.task_service.clone();
                let id = world.tasks[0].clone();
                tokio::spawn(async move { service.drain(&id).await.map(|_| ()) })
            };
            let mut running = drain(&world);
            let mut looked = 0;
            loop {
                if running.is_finished() {
                    running = drain(&world);
                }
                let task = world.task(0).await?;
                let runs = world.check_runs(0).await?;
                if task["status"] == "review" && runs == ["running"] {
                    break;
                }
                looked += 1;
                if looked > 400 {
                    return Err(format!("no running check: {} {runs:?}", task["status"]));
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            steps = Some(running);
        }
        let (version, offers) = world.offers(0).await?;
        if finds == AStepThatJustAskedCancelled {
            let cancel = offers
                .iter()
                .find(|offer| offer["action"]["verb"] == "cancel")
                .ok_or_else(|| format!("no cancel is offered: {offers:?}"))?;
            world.apply_offer(0, version, cancel).await?;
        } else {
            let (status, body) = world
                .request(
                    Method::POST,
                    &format!("/api/v1/tasks/{task_id}/actions"),
                    Some(json!({ "version": version, "action": { "verb": "hold" } })),
                )
                .await?;
            if !status.is_success() {
                return Err(format!("the hold command was refused: {status} {body}"));
            }
        }
        if just_asked {
            services::workflow::actions::review_entry_halt::disarm(&task_id);
        }
        if let Some(steps) = steps {
            let _ = steps.await;
        }
        // What the check worker has to do for the case, the hold standing.
        let stopping: &[&str] = match finds {
            ARunItStops => &["cancelled"],
            ARunBeingStopped => &["cancelling", "cancelled"],
            _ => &[],
        };
        if !stopping.is_empty() {
            let stopped = tokio::time::timeout(Duration::from_secs(45), async {
                loop {
                    let runs = world.check_runs(0).await?;
                    if runs.len() == 1 && stopping.contains(&runs[0].as_str()) {
                        return Ok::<_, String>(());
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
            .await;
            match stopped {
                Ok(done) => done?,
                Err(_) => {
                    return Err(format!(
                        "the withdrawn check was not stopped: runs {:?} consumers {:?} reviews {:?} steps {:?}",
                        world.check_runs(0).await?,
                        list(CONSUMERS).await?,
                        list(REVIEWS).await?,
                        list("SELECT kind || ':' || status || ':' || COALESCE(awaited_consumer_id,'-') || ':' || COALESCE(last_error,'-') FROM task_step WHERE task_id = ? ORDER BY seq").await?
                    ))
                }
            }
        }
        if just_asked {
            // The request was withdrawn with the step that made it, its
            // queued run cancelled with it, and its attempt closed, before
            // the slot frees.
            let (consumers, reviews) = (list(CONSUMERS).await?, list(REVIEWS).await?);
            if consumers != ["entry:cancelled:-:withdrawn"] || reviews != ["1:cancelled"] {
                return Err(format!(
                    "the request of the stopped step was not withdrawn: consumers {consumers:?} reviews {reviews:?}"
                ));
            }
            // The withdrawal cancelled the queued run nobody waits for, in
            // the same write and while the machine is still full: no slot
            // has to free for it.
            let runs = world.check_runs(0).await?;
            if runs != ["cancelled"] {
                return Err(format!(
                    "the run nobody waits for was not cancelled at the withdrawal: runs {runs:?} consumers {:?}",
                    list(CONSUMERS).await?
                ));
            }
            // The slot frees under the hold: nothing is left to run in it.
            world.apply(&Action::Finish(1, Outcome::Success)).await?;
            let runs = world.check_runs(0).await?;
            if runs != ["cancelled"] {
                return Err(format!("the cancelled run changed after the slot freed: {runs:?}"));
            }
        }
        if slot_seam {
            // The hold is what these cases are about, not the one slot.
            let cap = &world.live().state.db.server_run_cap;
            cap.set(Some(MODEL_RUN_CAP), MODEL_RUN_CAP, &cap.embedded_machine_id());
        }
        // The hold has done all it does when its command is answered; the
        // two cases that race the check worker do not give it time.
        if !matches!(finds, ARunBeingStopped | ARunItsReleaseJoins) {
            world.quiesce().await?;
        }
        if finds == AStepThatJustAskedCancelled {
            // Nothing is left of the entry: the Task is cancelled, the
            // request withdrawn, its run stopped, the attempt closed, and no
            // step waits or was delivered to.
            world.quiesce().await?;
            let task = world.task(0).await?;
            let (runs, reviews, consumers, steps) = (
                world.check_runs(0).await?,
                list(REVIEWS).await?,
                list(CONSUMERS).await?,
                list(STEPS).await?,
            );
            world.trace.push(format!(
                "{finds:?}: {} runs {runs:?} consumers {consumers:?} reviews {reviews:?} steps {steps:?}",
                task["status"]
            ));
            let live = count(
                "SELECT COUNT(*) FROM task_step WHERE task_id = ? AND status IN ('pending','claimed','suspended')",
            )
            .await?;
            if task["status"] != "cancelled"
                || runs != ["cancelled"]
                || reviews != ["1:cancelled"]
                // No result row: the queued run was never claimed.
                || consumers != ["entry:cancelled:-:withdrawn"]
                || count(DELIVERED).await? != 0
                || count(REVIEWER_RUNS).await? != 0
                || live != 0
            {
                return Err(format!(
                    "after the cancel: {} runs {runs:?} reviews {reviews:?} consumers {consumers:?}, {live} live step(s)",
                    task["status"]
                ));
            }
            return Ok(());
        }
        let held = world.task(0).await?;
        let runs = world.check_runs(0).await?;
        let reviews = list(REVIEWS).await?;
        let delivered = count(DELIVERED).await?;
        world.trace.push(format!(
            "{finds:?} held: {} runs {runs:?} consumers {:?} reviews {reviews:?} steps {:?} {}",
            held["status"],
            list(CONSUMERS).await?,
            list(STEPS).await?,
            held["condition"]
        ));
        if held["status"] != "review" || held["condition"]["primary"]["kind"] != "held" {
            return Err(format!(
                "the held Task is not held in review: {} {}",
                held["status"], held["condition"]
            ));
        }
        if count(REVIEWER_RUNS).await? != 0 {
            return Err("a reviewer ran while the Task was held".to_owned());
        }
        // The step that waited for the check is stopped, its request
        // withdrawn before any result was delivered, and its attempt closed.
        // A step stopped after it asked and before it began to wait is no
        // different: the request is recorded on the step when it is made.
        let closed = reviews == ["1:cancelled"] && delivered == 0;
        if !closed {
            return Err(format!(
                "under the hold: review attempts {reviews:?}, {delivered} result(s) delivered"
            ));
        }
        if !match finds {
            ARunItStops | AStepThatJustAsked => runs == ["cancelled"],
            AFinishedRun => runs == ["succeeded"],
            _ => runs.len() == 1,
        } {
            return Err(format!("check runs under the hold: {runs:?}"));
        }
        let (version, offers) = world.offers(0).await?;
        let release = offers
            .iter()
            .find(|offer| offer["action"]["verb"] == "release")
            .ok_or_else(|| format!("the held Task offers no release: {offers:?}"))?;
        world.apply_offer(0, version, release).await?;
        world.quiesce().await?;
        let runs = list(ENTRY_RUNS).await?;
        let released = world.task(0).await?;
        let delivered = count(DELIVERED).await?;
        world.trace.push(format!(
            "{finds:?} released: {} entry runs {runs:?} consumers {:?} reviews {:?} steps {:?} {}",
            released["status"],
            list(CONSUMERS).await?,
            list(REVIEWS).await?,
            list(STEPS).await?,
            released["condition"]
        ));
        // One run passed. A second one exists only when the first was
        // stopped, and never when the first was left to end.
        let again = runs == ["cancelled", "succeeded"];
        let kept = runs == ["succeeded"];
        if !match finds {
            ARunItStops => again && delivered == 1,
            // The entry's own step reads a result that exists when it asks:
            // nothing is delivered.
            AFinishedRun => kept && delivered == 0,
            AStepThatJustAsked => again && delivered == 1,
            AStepThatJustAskedCancelled => false,
            TheRunInFlight | ARunBeingStopped | ARunItsReleaseJoins => again || kept,
        } {
            return Err(format!(
                "the entry check after the release: {runs:?}, {delivered} result(s) delivered, {} {}",
                released["status"], released["condition"]
            ));
        }
        world.drive_to_settlement().await?;
        let done = world.task(0).await?;
        let reviews = list(REVIEWS).await?;
        world.trace.push(format!(
            "{finds:?} settled: {} reviews {reviews:?}",
            done["status"]
        ));
        // The attempt the hold stopped is closed, and one attempt passed on
        // the result. (An entry that joined a run in the middle of being
        // stopped is answered "cancelled": it closes its attempt too and
        // asks again.)
        let (last, before) = reviews.split_last().ok_or("no review attempt")?;
        if done["status"] != "done"
            || !last.ends_with(":passed")
            || before.is_empty()
            || before.iter().any(|attempt| !attempt.ends_with(":cancelled"))
        {
            return Err(format!(
                "the released Task ended in {} with review attempts {reviews:?}",
                done["status"]
            ));
        }
        // The owner's rule, for every check this Task asked for: one commit,
        // target and configuration passes in one run.
        let repeated = count(
            "SELECT COUNT(*) - COUNT(DISTINCT r.identity_key) FROM check_run r
             WHERE r.state = 'succeeded'
               AND r.id IN (SELECT c.run_id FROM check_consumer c WHERE c.task_id = ?)",
        )
        .await?;
        if repeated != 0 {
            return Err(format!("{repeated} check(s) ran to a pass a second time"));
        }
        Ok(())
    }
    .await;
    match outcome {
        Ok(()) => Ok(world.trace),
        Err(violation) => Err(format!("{violation}\ntrace:\n{}", world.trace.join("\n"))),
    }
}

/// The cases that start with two Tasks running side by side pin the cap.
pub(super) async fn hold_during_the_entry_check_or_panic(finds: HoldFinds) {
    let case = hold_during_the_entry_check(finds);
    if let Err(violation) = PINNED_RUN_CAP.scope(Some(MODEL_RUN_CAP), case).await {
        panic!("{violation}");
    }
}

/// Where the server dies while a Task's hooks step waits for its review-entry
/// check. The process is dropped and a new one is built on the same database.
#[derive(Clone, Copy, PartialEq, Debug)]
pub(super) enum RestartPoint {
    /// The check was requested; the step had not yet been suspended.
    AfterRequestBeforeSuspend,
    /// The result's delivery step is enqueued and has not run.
    AfterDeliveryBeforeWake,
    /// The delivery woke the hooks step and the runtime stopped as that
    /// step, claimed, was about to read the result.
    AfterWakeBeforeSettle,
    /// The delivery step is dead-lettered: nothing wakes the hooks step but
    /// its own deadline.
    DeliveryDeadLettered,
}

pub(super) async fn restart_while_the_entry_check_is_awaited(point: RestartPoint) {
    // Pinned: the case starts with two Tasks running side by side.
    PINNED_RUN_CAP
        .scope(Some(MODEL_RUN_CAP), restart_with_two_slots_then_one(point))
        .await;
}

pub(super) async fn restart_with_two_slots_then_one(point: RestartPoint) {
    let mut world = World::new().await;
    let outcome: Result<(), String> = async {
        // As in the pause tests: the first Task's check waits for the one
        // slot, which the second Task's run holds.
        for action in [Action::Create, Action::Create] {
            world.apply(&action).await?;
            world.quiesce().await?;
        }
        let cap = &world.live().state.db.server_run_cap;
        cap.set(Some(1), 1, &cap.embedded_machine_id());
        world.apply(&Action::Finish(0, Outcome::Success)).await?;
        world.quiesce().await?;
        if world.check_runs(0).await? != ["queued"] {
            return Err(format!("expected one queued check: {:?}", world.check_runs(0).await?));
        }
        let task_id = world.tasks[0].clone();
        let pool = world.live().pool.clone();
        let sql = |error: sqlx::Error| error.to_string();
        if point == RestartPoint::AfterRequestBeforeSuspend {
            // The consumer and its run are stored; the step still holds the
            // claim of a process that is gone.
            let changed = sqlx::query(
                "UPDATE task_step SET status='claimed', claimed_by='dead-process',
                        lease_until='2000-01-01T00:00:00+00:00', attempts=attempts+1,
                        suspended_until=NULL
                 WHERE task_id = ? AND status = 'suspended'",
            )
            .bind(&task_id)
            .execute(&pool)
            .await
            .map_err(sql)?
            .rows_affected();
            if changed != 1 {
                return Err("no suspended hooks step to rewind".to_owned());
            }
        } else {
            // The slot frees; the check worker runs the check and enqueues
            // its delivery. No Task step runs: the model has no step worker.
            world.apply(&Action::Finish(1, Outcome::Success)).await?;
            let delivery: String = tokio::time::timeout(Duration::from_secs(60), async {
                loop {
                    let step: Option<String> = sqlx::query_scalar(
                        "SELECT c.delivery_step_id FROM check_consumer c JOIN check_run r ON r.id = c.run_id
                         WHERE c.task_id = ? AND r.state = 'succeeded' AND c.delivery_step_id IS NOT NULL",
                    )
                    .bind(&task_id)
                    .fetch_optional(&pool)
                    .await
                    .map_err(sql)?;
                    if let Some(step) = step {
                        return Ok::<_, String>(step);
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await
            .map_err(|_| "the check result was not delivered".to_owned())??;
            let pending: Option<String> =
                sqlx::query_scalar("SELECT status FROM task_step WHERE id = ?")
                    .bind(&delivery)
                    .fetch_optional(&pool)
                    .await
                    .map_err(sql)?;
            let suspended: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM task_step WHERE task_id = ? AND status = 'suspended'",
            )
            .bind(&task_id)
            .fetch_one(&pool)
            .await
            .map_err(sql)?;
            if pending.as_deref() != Some("pending") || suspended != 1 {
                return Err(format!(
                    "expected an unrun delivery and a suspended step, got {pending:?} / {suspended}"
                ));
            }
            match point {
                RestartPoint::AfterWakeBeforeSettle => {
                    // The real runtime runs the delivery step and claims the
                    // woken hooks step, and is stopped right there: the step
                    // parks at the armed point and the drain that carries it
                    // is dropped, as a process that died holding the claim.
                    use services::workflow::actions::review_entry_halt as halt;
                    let reached = halt::arm(&task_id, halt::AFTER_WAKE_BEFORE_SETTLE);
                    let service = world.live().state.task_service.clone();
                    let id = task_id.clone();
                    let running = tokio::spawn(async move { service.drain(&id).await });
                    let stopped =
                        tokio::time::timeout(Duration::from_secs(60), reached.notified()).await;
                    running.abort();
                    let _ = running.await;
                    halt::disarm(&task_id);
                    if stopped.is_err() {
                        return Err("the woken hooks step never ran".to_owned());
                    }
                    let delivered: Option<String> =
                        sqlx::query_scalar("SELECT status FROM task_step WHERE id = ?")
                            .bind(&delivery)
                            .fetch_optional(&pool)
                            .await
                            .map_err(sql)?;
                    let applied: i64 = sqlx::query_scalar(
                        "SELECT COUNT(*) FROM check_consumer WHERE task_id = ? AND origin='entry' AND applied_at IS NOT NULL",
                    )
                    .bind(&task_id)
                    .fetch_one(&pool)
                    .await
                    .map_err(sql)?;
                    let claimed: Vec<(String, String)> = sqlx::query_as(
                        "SELECT kind, status FROM task_step WHERE task_id = ? AND status IN ('claimed','pending','suspended')",
                    )
                    .bind(&task_id)
                    .fetch_all(&pool)
                    .await
                    .map_err(sql)?;
                    if delivered.as_deref() != Some("done")
                        || applied != 1
                        || claimed != [("hooks".to_owned(), "claimed".to_owned())]
                    {
                        return Err(format!(
                            "expected a finished delivery and a claimed hooks step, got {delivered:?} / {applied} / {claimed:?}"
                        ));
                    }
                    let reviews: Vec<String> =
                        sqlx::query_scalar("SELECT status FROM review WHERE task_id = ?")
                            .bind(&task_id)
                            .fetch_all(&pool)
                            .await
                            .map_err(sql)?;
                    if reviews != ["running"] {
                        return Err(format!("the attempt settled before the stop: {reviews:?}"));
                    }
                    // Time passes: the dead process's claim runs out.
                    sqlx::query(
                        "UPDATE task_step SET lease_until='2000-01-01T00:00:00+00:00' WHERE task_id = ? AND status = 'claimed'",
                    )
                    .bind(&task_id)
                    .execute(&pool)
                    .await
                    .map_err(sql)?;
                }
                RestartPoint::DeliveryDeadLettered => {
                    sqlx::query(
                        "UPDATE task_step SET status='failed', last_error='dead-lettered', completed_at=?, updated_at=? WHERE id = ?",
                    )
                    .bind(db::now_rfc3339())
                    .bind(db::now_rfc3339())
                    .bind(&delivery)
                    .execute(&pool)
                    .await
                    .map_err(sql)?;
                    // Only the step's own deadline is left; it has passed.
                    sqlx::query(
                        "UPDATE task_step SET suspended_until='2000-01-01T00:00:00+00:00' WHERE task_id = ? AND status = 'suspended'",
                    )
                    .bind(&task_id)
                    .execute(&pool)
                    .await
                    .map_err(sql)?;
                }
                _ => {}
            }
        }
        drop(pool);
        world.crash_and_restart().await?;
        world.quiesce().await?;
        world.drive_to_settlement().await?;
        for index in 0..2 {
            let task = world.task(index).await?;
            if task["status"] != "done" {
                return Err(format!("task {index} ended in {}", task["status"]));
            }
        }
        // One request, one run, one review attempt that passed: the restart
        // repeated nothing.
        // The Task merged on an unchanged commit and target, so its queue
        // head is a second CONSUMER of that one run (a recorded reuse), never
        // a second run and never a second review-entry request.
        let runs = world.check_runs(0).await?;
        let consumers: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM check_consumer WHERE task_id = ? AND origin = 'entry'",
        )
        .bind(&task_id)
        .fetch_one(&world.live().pool)
        .await
        .map_err(sql)?;
        let heads = world.head_consumers_reuse_the_entry_run(0).await?;
        if runs != ["succeeded"] || consumers != 1 || heads > 1 {
            return Err(format!(
                "check runs {runs:?}, entry requests {consumers}, head reuses {heads}; expected one run, one request, at most one reuse"
            ));
        }
        let reviews: Vec<String> =
            sqlx::query_scalar("SELECT status FROM review WHERE task_id = ? ORDER BY attempt_number")
                .bind(&task_id)
                .fetch_all(&world.live().pool)
                .await
                .map_err(sql)?;
        if reviews != ["passed"] {
            return Err(format!("review attempts {reviews:?}, expected one that passed"));
        }
        Ok(())
    }
    .await;
    if let Err(violation) = outcome {
        world.record_history().await;
        panic!("{point:?}: {violation}\ntrace:\n{}", world.trace.join("\n"));
    }
}
