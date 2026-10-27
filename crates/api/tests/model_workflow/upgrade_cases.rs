//! Pre-queue migration variants, owner commands and root integration cases.

use super::*;

/// Each fixture twice: as the last pre-queue build left it (with the passive
/// "shadow" attempt that build recorded for a merge), and with every attempt
/// and queue row removed, which is what an install that never ran the passive
/// stages has (the importer classifies those Tasks from their own rows).
macro_rules! pre_queue {
    ($($test:ident, $bare:ident: $name:literal => $expected:ident;)*) => {$(
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $test() {
            log_to_stderr();
            if let Err(error) = pre_queue_upgrade($name, true, Upgraded::$expected, None).await {
                panic!("{error}");
            }
        }
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $bare() {
            log_to_stderr();
            if let Err(error) = pre_queue_upgrade($name, false, Upgraded::$expected, None).await {
                panic!("{error}");
            }
        }
    )*};
}

/// One owner command, through the real action endpoint, on a Task the
/// upgrade left stopped in its merge state: what it does to the Task's status
/// and epoch, and that the Task then merges (or, cancelled, stays cancelled).
macro_rules! pre_queue_command {
    ($($test:ident: $name:literal, $verb:literal;)*) => {$(
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $test() {
            log_to_stderr();
            if let Err(error) =
                pre_queue_upgrade($name, true, Upgraded::Parked, Some($verb)).await
            {
                panic!("{error}");
            }
        }
    )*};
}

pre_queue_command! {
    pre_queue_command_hold_under_a_paused_project: "p10_project_paused_at_merge_gate", "hold";
    pre_queue_command_release_a_held_task: "p13_held_in_merging", "release";
    pre_queue_command_restart_a_held_task: "p13_held_in_merging", "restart";
    pre_queue_command_approve_a_held_task: "p13_held_in_merging", "approve";
    pre_queue_command_cancel_a_held_task: "p13_held_in_merging", "cancel";
    pre_queue_command_retry_a_failed_merge: "p19_target_dirty", "retry";
    pre_queue_command_approve_a_failed_merge: "p19_target_dirty", "approve";
}

pre_queue! {
    pre_queue_p01_outcome_done_task_merging, pre_queue_bare_p01_outcome_done_task_merging: "p01_outcome_done_task_merging" => Reported;
    pre_queue_p02_merge_intent_landed_no_outcome, pre_queue_bare_p02_merge_intent_landed_no_outcome: "p02_merge_intent_landed_no_outcome" => Reported;
    pre_queue_p02_merge_intent_not_landed, pre_queue_bare_p02_merge_intent_not_landed: "p02_merge_intent_not_landed" => Merges;
    pre_queue_p04_pending_hooks, pre_queue_bare_p04_pending_hooks: "p04_pending_hooks" => Merges;
    pre_queue_p05_rebase_target_no_outcome, pre_queue_bare_p05_rebase_target_no_outcome: "p05_rebase_target_no_outcome" => Merges;
    pre_queue_p06_clean_rebase_in_merge_failed, pre_queue_bare_p06_clean_rebase_in_merge_failed: "p06_clean_rebase_in_merge_failed" => Merges;
    pre_queue_p06_clean_rebase_in_review, pre_queue_bare_p06_clean_rebase_in_review: "p06_clean_rebase_in_review" => Merges;
    pre_queue_p07_conflict_coder_repairing, pre_queue_bare_p07_conflict_coder_repairing: "p07_conflict_coder_repairing" => Repaired;
    pre_queue_p07_conflict_handoff_entry_pending, pre_queue_bare_p07_conflict_handoff_entry_pending: "p07_conflict_handoff_entry_pending" => Repaired;
    pre_queue_p09_back_at_merge_gate_after_bounce, pre_queue_bare_p09_back_at_merge_gate_after_bounce: "p09_back_at_merge_gate_after_bounce" => Merges;
    pre_queue_p10_project_paused_at_merge_gate, pre_queue_bare_p10_project_paused_at_merge_gate: "p10_project_paused_at_merge_gate" => Parked;
    pre_queue_p11_project_resumed_not_continued, pre_queue_bare_p11_project_resumed_not_continued: "p11_project_resumed_not_continued" => Merges;
    pre_queue_p13_held_in_merging_project_paused, pre_queue_bare_p13_held_in_merging_project_paused: "p13_held_in_merging_project_paused" => Parked;
    pre_queue_p13_held_in_merging, pre_queue_bare_p13_held_in_merging: "p13_held_in_merging" => Parked;
    pre_queue_p14_merge_failed_on_a_locked_index, pre_queue_bare_p14_merge_failed_on_a_locked_index: "p14_merge_failed_on_a_locked_index" => Repaired;
    pre_queue_p19_dirty_worktree, pre_queue_bare_p19_dirty_worktree: "p19_dirty_worktree" => Repaired;
    pre_queue_p19_target_dirty, pre_queue_bare_p19_target_dirty: "p19_target_dirty" => Parked;
}

/// A coordination root and its subtask: the subtask's work lands on the
/// root's branch, and the root merges through the queue like any Task (one
/// completed attempt, its branch on main), with no owner action.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_coordination_root_merges_through_the_queue() {
    log_to_stderr();
    let run = async {
        let mut world = World::new().await;
        world.create(None).await?;
        let child = world.create(Some(0)).await?;
        if child.starts_with("refused") {
            return Err(format!("the subtask was {child}"));
        }
        world.quiesce().await?;
        world.drive_to_settlement().await?;
        let owner: Vec<&String> = world
            .trace
            .iter()
            .filter(|line| line.contains("(0)") || line.contains("(1)"))
            .collect();
        if !owner.is_empty() {
            return Err(format!("an owner action was needed: {owner:#?}"));
        }
        for index in 0..world.tasks.len() {
            let task = world.task(index).await?;
            if task["status"] != "done" {
                return Err(format!(
                    "task {index} ended {}: {}; trace {:#?}",
                    task["status"], task["condition"], world.trace
                ));
            }
        }
        let attempts: Vec<(String, String)> = sqlx::query_as(
            "SELECT task_ref, state FROM integration_attempt ORDER BY created_at, rowid",
        )
        .fetch_all(&world.live().pool)
        .await
        .map_err(|error| error.to_string())?;
        let of = |index: usize| -> Vec<&str> {
            attempts
                .iter()
                .filter(|(task, _)| task == &world.tasks[index])
                .map(|(_, state)| state.as_str())
                .collect()
        };
        if of(0).last() != Some(&"completed") {
            return Err(format!(
                "the root has no completed attempt: root {:?}, subtask {:?}",
                of(0),
                of(1)
            ));
        }
        if !of(1).is_empty() {
            return Err(format!("a subtask was in the queue: {:?}", of(1)));
        }
        let tip = git(
            &world.dir.path().join("repo"),
            &[
                "rev-parse",
                &format!("refs/heads/task/{}", &world.tasks[0][..8]),
            ],
        )?;
        if !world.on_main(&tip) {
            return Err(format!("the root's branch {tip} is not on main"));
        }
        for index in 0..2 {
            let file = world.dir.path().join("repo").join(deliverable(index));
            let tracked = git(
                &world.dir.path().join("repo"),
                &[
                    "cat-file",
                    "-e",
                    &format!("refs/heads/main:{}", deliverable(index)),
                ],
            );
            if tracked.is_err() {
                return Err(format!("main lacks {}", file.display()));
            }
        }
        Ok(())
    };
    if let Err(error) = run.await {
        panic!("{error}");
    }
}
