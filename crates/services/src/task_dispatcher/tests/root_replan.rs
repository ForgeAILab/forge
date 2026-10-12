//! Stage 2 of the root lead: the root goes back to its planning state when
//! its aggregate review is rejected, and when its current subtask parks
//! needing a decision. Both charge the `replan` budget once per trigger.
use super::root_lead::{executions, fixture, lead_fixture, park, passes, reload, until, Lead};
use super::*;

const PLANNER: &str = crate::workflow::default_roles::PLANNER;
const CODER: &str = crate::workflow::default_roles::CODER;

async fn spent(lead: &Lead, task: &str, key: &str) -> i64 {
    db::budget::spent(lead.db.pool(), task, key).await.unwrap()
}

async fn log(lead: &Lead, task: &str) -> Vec<(String, String, Option<String>)> {
    sqlx::query_as(
        "SELECT from_state, to_state, bridge_kind FROM transition_log WHERE task_id=? ORDER BY created_at, rowid",
    )
    .bind(task)
    .fetch_all(lead.db.pool())
    .await
    .unwrap()
}

/// A coordination root in aggregate review whose only subtask is done, with
/// a review that failed (on its checks) longer ago than the recovery grace.
async fn root_with_failed_review(lead: &Lead, with_planner: bool) -> Task {
    let root = seed_task(&lead.db, &lead.project, "root", "review", 1).await;
    seed_subtask(&lead.db, &root, "child", "done", 0).await;
    assign_role(&lead.db, &root.id, CODER, &lead.coder).await;
    if with_planner {
        assign_role(&lead.db, &root.id, PLANNER, &lead.planner).await;
    }
    fail_review(lead, &root, 1).await;
    reload(&lead.db, &root.id).await
}

/// Record a failed aggregate review of `root`'s present `review` entry.
async fn fail_review(lead: &Lead, root: &Task, attempt: i64) {
    let candidate = seed_completed_coder_execution(&lead.db, &root.id).await;
    let failed_at = (chrono::Utc::now() - chrono::Duration::minutes(3)).to_rfc3339();
    let entered_at =
        (chrono::Utc::now() - chrono::Duration::minutes(3) - chrono::Duration::seconds(1))
            .to_rfc3339();
    ReviewRepo::create(
        &*lead.db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: root.id.clone(),
            execution_id: candidate,
            attempt_number: attempt,
            status: ReviewStatus::Failed,
            step_results_json: r#"{"ci_steps":[{"index":0,"command":"make verify","exit_code":1,"stderr_tail":"the combined result does not build"}]}"#
                .to_owned(),
            started_at: failed_at.clone(),
            created_at: failed_at.clone(),
            updated_at: failed_at,
        },
    )
    .await
    .unwrap();
    TransitionLogRepo::insert(
        &*lead.db,
        db::CreateTransitionLog {
            id: new_uuid_v4(),
            task_id: root.id.clone(),
            from_state: "in_progress".to_owned(),
            to_state: "review".to_owned(),
            trigger_name: None,
            triggered_by: Actor::system(SystemComponent::Workflow).display(),
            bridge: Default::default(),
            trigger_reason: "all ordered subtasks completed".to_owned(),
            hook_results_json: None,
            rejection: false,
            created_at: entered_at,
        },
    )
    .await
    .unwrap();
}

/// The failing-first test of stage 2. A rejected aggregate review of a root
/// with a lead sends the root to its planning state, charges one re-plan and
/// one review rejection, and the lead's input carries the findings.
#[tokio::test]
async fn a_rejected_aggregate_review_sends_a_lead_root_back_to_planning() {
    let mut lead = lead_fixture(false).await;
    let root = root_with_failed_review(&lead, true).await;
    passes(&lead, 3).await;

    let planning = reload(&lead.db, &root.id).await;
    assert_eq!(planning.status, "planning", "{:?}", log(&lead, &root.id).await);
    assert_eq!(spent(&lead, &root.id, "replan").await, 1);
    assert_eq!(spent(&lead, &root.id, "review").await, 1);
    assert_eq!(
        log(&lead, &root.id).await.last().cloned(),
        Some((
            "review".to_owned(),
            "planning".to_owned(),
            Some("replan".to_owned())
        ))
    );
    assert!(
        executions(&lead.db, &root.id)
            .await
            .contains(&(PLANNER.to_owned(), "running".to_owned())),
        "the lead runs again: {:?}",
        executions(&lead.db, &root.id).await
    );
    let ctx = tokio::time::timeout(Duration::from_secs(5), lead.rx.recv())
        .await
        .expect("planner context arrives")
        .expect("planner context");
    assert_eq!(ctx.task_id, root.id);
    assert!(
        ctx.description.contains("aggregate review")
            && ctx.description.contains("make verify")
            && ctx.description.contains("the combined result does not build"),
        "the lead's input carries the review findings: {}",
        ctx.description
    );
    // The re-plan is one entry: further passes charge nothing more.
    passes(&lead, 3).await;
    assert_eq!(spent(&lead, &root.id, "replan").await, 1);
    assert_eq!(spent(&lead, &root.id, "review").await, 1);
}
