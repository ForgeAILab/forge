//! Shared queued-start and post-rebase review recovery scenarios.

use super::*;

pub(super) async fn start_on_a_queued_task() {
    use Action::{Create, PauseAgent, Take};
    for (sequence, expected) in [
        (vec![Create, Create, Create, Take(2, "start")], "start"),
        (
            vec![PauseAgent, Create, Take(0, "start")],
            "start not offered",
        ),
    ] {
        let mut world = World::new().await;
        let outcome: Result<(), String> = async {
            let mut applied = String::new();
            for action in &sequence {
                applied = world.apply(action).await?;
                world.quiesce().await?;
                let state = world.check_quiescent().await?;
                world
                    .trace
                    .push(format!("{action:?} -> {applied}: {state}"));
            }
            if applied != expected {
                return Err(format!(
                    "taking `start` answered `{applied}`, expected `{expected}`"
                ));
            }
            let started = world.tasks.len() - 1;
            let task = world.task(started).await?;
            // Waiting for its paused Agent is a visible wait, not a park the
            // refused claim left behind.
            let agent_wait = task["condition"]["primary"]["kind"] == "agent";
            if task["status"] != "todo" || (task["condition"]["kind"] == "parked" && !agent_wait) {
                return Err(format!(
                    "(b) `start` on a queued Task must leave it queued, not parked or moved: {} {}",
                    task["status"], task["condition"]
                ));
            }
            world.drive_to_settlement().await
        }
        .await;
        if let Err(violation) = outcome {
            panic!(
                "{violation}\nsequence {sequence:?}\ntrace:\n{}",
                world.trace.join("\n")
            );
        }
    }
}

pub(super) async fn second_review_entry_after_a_rebase() {
    let mut world = World::new().await;
    let outcome: Result<(), String> = async {
        for action in [
            Action::Create,
            Action::Create,
            Action::Finish(0, Outcome::Success),
            Action::Finish(1, Outcome::Success),
        ] {
            let applied = world.apply(&action).await?;
            world.quiesce().await?;
            let state = world.check_quiescent().await?;
            world
                .trace
                .push(format!("{action:?} -> {applied}: {state}"));
        }
        world.drive_to_settlement().await?;
        let pool = world.live().pool.clone();
        let sql = |error: sqlx::Error| error.to_string();
        for index in 0..2 {
            let task = world.task(index).await?;
            if task["status"] != "done" {
                return Err(format!("task {index} ended in {}", task["status"]));
            }
        }
        // Whichever Task merged second is the one the queue rebased: it stays
        // in `merging` (a clean rebase no longer bounces through
        // `merge_failed` and `review`), and the commit that landed is not the
        // one its review passed.
        let rebased: Vec<(String, String)> = sqlx::query_as(
            "SELECT DISTINCT task_ref, integrated_sha FROM integration_attempt
             WHERE integrated_sha IS NOT NULL AND original_candidate_sha IS NOT NULL
               AND original_candidate_sha <> integrated_sha",
        )
        .fetch_all(&pool)
        .await
        .map_err(sql)?;
        let [(task_id, landed)] = rebased.as_slice() else {
            return Err(format!(
                "expected exactly one Task to be rebased by the merge queue, got {rebased:?}"
            ));
        };
        // Every check the Task asked for ran, finished and its verdict was
        // used, and one of them ran on the rebased commit that landed: the
        // entry check of a second review entry, or the queue's head check.
        //
        // A verdict is used in one of two orders. The delivery step applies
        // it (`applied_at`) and wakes the waiting entry step. Or the entry
        // step reads the stored result itself first (the run ended before
        // the step looked, or the step's wake deadline came before the
        // delivery sweep): it settles the Review and the Task leaves the
        // status entry, so the delivery that follows is superseded as stale
        // and `applied_at` stays empty. What shows the verdict was used then
        // is the Review that asked for the check: passed, with the check's
        // commands recorded on it.
        let checks: Vec<(String, String, String, Option<String>, i64, bool)> = sqlx::query_as(
            "SELECT c.origin, r.commit_sha, r.state, c.applied_at, c.status_epoch,
                    c.origin = 'entry' AND EXISTS(
                        SELECT 1 FROM review v
                        WHERE v.task_id = c.task_id AND v.status = 'passed'
                          AND c.request_key LIKE '%|' || v.id
                          AND json_array_length(v.step_results_json, '$.ci_steps') > 0)
             FROM check_consumer c JOIN check_run r ON r.id = c.run_id
             WHERE c.task_id = ? ORDER BY c.created_at, c.rowid",
        )
        .bind(task_id)
        .fetch_all(&pool)
        .await
        .map_err(sql)?;
        if checks
            .iter()
            .any(|(origin, _, state, applied, _, read_by_entry)| {
                // In either order the entry's Review carries the result.
                state != "succeeded"
                    || if origin == "entry" {
                        !read_by_entry
                    } else {
                        applied.is_none()
                    }
            })
        {
            return Err(format!("a check did not run and settle: {checks:?}"));
        }
        if !checks.iter().any(|(_, commit, ..)| commit == landed) {
            return Err(format!(
                "no check ran on the rebased commit {landed} before it merged: {checks:?}"
            ));
        }
        let reviews: Vec<String> = sqlx::query_scalar(
            "SELECT status FROM review WHERE task_id = ? ORDER BY attempt_number",
        )
        .bind(task_id)
        .fetch_all(&pool)
        .await
        .map_err(sql)?;
        if reviews.is_empty() || reviews.iter().any(|status| status != "passed") {
            return Err(format!(
                "review attempts {reviews:?}; expected every one passed"
            ));
        }
        Ok(())
    }
    .await;
    if let Err(violation) = outcome {
        world.record_history().await;
        panic!("{violation}\ntrace:\n{}", world.trace.join("\n"));
    }
}
