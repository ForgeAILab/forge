//! Tests for prompts and recovery in lead check.

use super::*;
/// The step-check prompt carries the step's own diff: the shared workspace
/// against the end of the previous step (the root's branch point for the
/// first step), bounded at 32 KiB with the cut marked.
#[tokio::test]
async fn the_step_check_prompt_carries_the_step_s_own_diff() {
    let steps = steps(&[PASS], None).await;
    steps
        .commit_bytes
        .store(64, std::sync::atomic::Ordering::SeqCst);
    let (_root, first, second) = root_with_two_steps(&steps, true).await;
    until(&steps, "the first step is done", async || {
        reload(&steps.db, &first.id).await.status == "done"
    })
    .await;
    let prompt = check_prompt(&steps.db, &first.id).await;
    assert!(
        prompt.contains("+++ b/first.txt") && prompt.contains("+work of first"),
        "{prompt}"
    );
    assert!(prompt.contains("the root's branch point"), "{prompt}");
    assert!(!prompt.contains("[diff truncated"), "{prompt}");
    // The diff is per-run input, after the stable role contract.
    let (contract, input) = prompt
        .split_once("Execution request:")
        .expect("the prompt has both parts");
    assert!(!contract.contains("first.txt") && input.contains("first.txt"));

    // The second step is large: its diff is cut at the bound, and it does not
    // repeat the first step's changes.
    steps
        .commit_bytes
        .store(48 * 1024, std::sync::atomic::Ordering::SeqCst);
    until(&steps, "the second step check runs", async || {
        running_check(&steps.db, &second.id).await
    })
    .await;
    let prompt = check_prompt(&steps.db, &second.id).await;
    assert!(prompt.contains("+++ b/second.txt"), "{prompt}");
    assert!(
        !prompt.contains("first.txt"),
        "the previous step is not shown again"
    );
    assert!(prompt.contains("the end of the previous step"));
    assert!(prompt.contains("[diff truncated at 32 KiB"));
    assert!(prompt.len() < 32 * 1024 + 16 * 1024, "{}", prompt.len());
}

/// A crash between a check that completed and its settlement leaves a
/// finished planner run on a subtask that still rests in the gate. The
/// dispatcher settles it from what is stored: the verdict is applied once and
/// no second check is started.
#[tokio::test]
async fn a_check_that_finished_before_a_crash_is_settled_from_stored_state() {
    let steps = steps(&[], None).await;
    let (_root, child) = in_progress_root_with_a_step_in_the_gate(&steps, true).await;
    seed_finished_check(&steps, &child, FAIL).await;
    until(&steps, "the step is returned", async || {
        reload(&steps.db, &child.id).await.status != "lead_check"
    })
    .await;
    passes(&steps, 3).await;
    let hops = log(&steps.db, &child.id).await;
    assert_eq!(
        hops.first(),
        Some(&hop(
            "lead_check",
            "in_progress",
            Some("gate_rejected"),
            true
        )),
        "{}",
        dump(&steps.db).await
    );
    assert_eq!(
        hops.iter()
            .filter(|hop| hop.0 == "lead_check" && hop.3)
            .count(),
        1,
        "{hops:?}"
    );
    assert_eq!(gate_spent(&steps.db, &child.id).await, 1);
    assert_eq!(verdict_comments(&steps.db, &child.id).await, 1);
}

/// Settling the same finished check twice (a replayed step, two passes that
/// race) applies its verdict once: one transition, one comment, one charge.
#[tokio::test]
async fn settling_a_finished_check_twice_applies_its_verdict_once() {
    let steps = steps(&[], Some("first")).await;
    let (_root, child) = in_progress_root_with_a_step_in_the_gate(&steps, true).await;
    seed_finished_check(&steps, &child, FAIL).await;
    let version: i64 = sqlx::query_scalar("SELECT version FROM project WHERE id=?")
        .bind(&steps.project)
        .fetch_one(steps.db.pool())
        .await
        .unwrap();
    let stale = reload(&steps.db, &child.id).await;
    let dispatcher = &steps.dispatcher;
    for _ in 0..2 {
        // Both calls hold the Task as it was before the first settlement.
        dispatcher
            .reconcile_terminal_role_execution(
                &stale,
                crate::workflow::default_roles::PLANNER,
                version,
            )
            .await
            .expect("settlement does not fail");
    }
    passes(&steps, 2).await;
    let hops = log(&steps.db, &child.id).await;
    assert_eq!(
        hops.iter().filter(|hop| hop.0 == "lead_check").count(),
        1,
        "{hops:?}\n{}",
        dump(&steps.db).await
    );
    assert_eq!(gate_spent(&steps.db, &child.id).await, 1);
    assert_eq!(verdict_comments(&steps.db, &child.id).await, 1);
}

/// A check that ends without a verdict while its session is still there is
/// asked again in that session (not a new check, not a return), and the
/// answer settles the step.
#[tokio::test]
async fn a_check_without_a_verdict_is_asked_again_in_its_live_session() {
    let steps = steps(&["@session:Looks fine to me.", PASS], Some("second")).await;
    let (_root, first, _second) = root_with_two_steps(&steps, true).await;
    until(&steps, "the step is done", async || {
        reload(&steps.db, &first.id).await.status == "done"
    })
    .await;
    let runs: Vec<(String, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT status, parent_execution_id, prompt FROM execution
         WHERE task_id=? AND role='planner' ORDER BY created_at, id",
    )
    .bind(&first.id)
    .fetch_all(steps.db.pool())
    .await
    .unwrap();
    assert_eq!(runs.len(), 2, "{}", dump(&steps.db).await);
    assert!(
        runs[1].1.is_some(),
        "the second run continues the first: {runs:?}"
    );
    assert!(
        runs[1]
            .2
            .as_deref()
            .unwrap_or_default()
            .contains("no usable result block"),
        "{:?}",
        runs[1].2
    );
    assert_eq!(gate_spent(&steps.db, &first.id).await, 0, "not a return");
    assert_eq!(
        log(&steps.db, &first.id).await.last(),
        Some(&hop("lead_check", "done", None, false))
    );
    assert!(
        !reload(&steps.db, &first.id)
            .await
            .condition
            .read()
            .interruption_present
    );
}

/// The step check is a read-only run, enforced by the host and not by the
/// prompt: what a lead writes to the shared workspace is discarded, the run
/// fails loudly, and its verdict passes nothing.
#[tokio::test]
async fn a_step_check_that_writes_the_workspace_is_discarded_and_fails() {
    let steps = steps(&[&format!("@write:{PASS}")], Some("second")).await;
    steps
        .commit_bytes
        .store(64, std::sync::atomic::Ordering::SeqCst);
    let (_root, first, second) = root_with_two_steps(&steps, true).await;
    until(&steps, "the step check ended", async || {
        executions(&steps.db, &first.id)
            .await
            .iter()
            .any(|run| run.0 == "planner" && run.1 != "running")
    })
    .await;
    let (status, error, worktree): (String, Option<String>, String) = sqlx::query_as(
        "SELECT e.status, e.error, w.worktree_path FROM execution e
         JOIN workspace w ON w.id = e.workspace_id
         WHERE e.task_id=? AND e.role='planner' ORDER BY e.created_at, e.id LIMIT 1",
    )
    .bind(&first.id)
    .fetch_one(steps.db.pool())
    .await
    .unwrap();
    assert_eq!(status, "failed", "{}", dump(&steps.db).await);
    assert!(
        error
            .as_deref()
            .unwrap_or_default()
            .contains("read-only execution produced worktree changes"),
        "{error:?}"
    );
    assert!(
        !Path::new(&worktree).join("lead-was-here.txt").exists(),
        "the lead's write is discarded"
    );
    assert!(
        Path::new(&worktree).join("first.txt").exists(),
        "the step's own work stays"
    );
    let moved = reload(&steps.db, &first.id).await;
    assert_ne!(moved.status, "done", "a failed check passes nothing");
    assert_eq!(executions(&steps.db, &second.id).await, Vec::new());
}

/// The manual exits the board and Task detail offer for a card in the step
/// check (done, in_progress, cancelled) are the owner's own decision on the
/// step, and the server takes each: the lead's running check is ended and
/// its verdict is not applied afterwards.
#[tokio::test]
async fn the_owner_can_take_a_step_out_of_the_gate_by_hand() {
    for target in ["done", "in_progress", "cancelled"] {
        let steps = steps(&[HELD_FAIL], Some("second")).await;
        let (_root, first, _second) = root_with_two_steps(&steps, true).await;
        until(&steps, "the step check runs", async || {
            running_check(&steps.db, &first.id).await
        })
        .await;
        let resting = reload(&steps.db, &first.id).await;
        assert_eq!(resting.status, "lead_check");
        let check: String = sqlx::query_scalar(
            "SELECT id FROM execution WHERE task_id=? AND role='planner' AND status='running'",
        )
        .bind(&first.id)
        .fetch_one(steps.db.pool())
        .await
        .unwrap();
        // What REST's transition route passes for a manual change.
        let options = crate::task_service::TransitionOptions::from((
            resting.version,
            Some("the owner decided".to_owned()),
        ));
        let moved = steps
            .dispatcher
            .task_service
            .transition(&first.id, target.to_owned(), options)
            .await;
        assert!(moved.is_ok(), "{target}: {:?}", moved.err());
        // The owner's own move out of the gate, sending it back included,
        // is not a return the lead made: it charges nothing.
        assert_eq!(gate_spent(&steps.db, &first.id).await, 0, "{target}");
        passes(&steps, 3).await;
        steps.release.add_permits(8);
        passes(&steps, 3).await;
        // The check that ran when the owner decided is over. (Sent back to
        // implementation, the step is worked on and checked again: that is a
        // new check of new work.)
        let status: String = sqlx::query_scalar("SELECT status FROM execution WHERE id=?")
            .bind(&check)
            .fetch_one(steps.db.pool())
            .await
            .unwrap();
        assert_ne!(
            status, "running",
            "{target}: the lead's check does not outlive the owner's decision"
        );
        let after = reload(&steps.db, &first.id).await;
        if target != "in_progress" {
            assert_eq!(after.status, target, "{}", dump(&steps.db).await);
            assert!(!running_check(&steps.db, &first.id).await, "{target}");
        }
        // Passing or dropping the step by hand charges no return. (Sending
        // it back is a return like the lead's own and may be charged as one.)
        if target != "in_progress" {
            assert_eq!(gate_spent(&steps.db, &first.id).await, 0, "{target}");
        }
        assert!(gate_spent(&steps.db, &first.id).await <= 1, "{target}");
    }
}
