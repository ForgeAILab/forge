//! Stage 2 of the root lead: the root goes back to its planning state when
//! its aggregate review is rejected, and when its current subtask parks
//! needing a decision. Both charge the `replan` budget once per trigger.
use super::root_lead::{executions, lead_fixture, park, passes, reload, until, Lead};
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
    assert_eq!(
        planning.status,
        "planning",
        "{:?}",
        log(&lead, &root.id).await
    );
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
            && ctx
                .description
                .contains("the combined result does not build"),
        "the lead's input carries the review findings: {}",
        ctx.description
    );
    // The re-plan is one entry: further passes charge nothing more.
    passes(&lead, 3).await;
    assert_eq!(spent(&lead, &root.id, "replan").await, 1);
    assert_eq!(spent(&lead, &root.id, "review").await, 1);
    // The rejection happened before queue admission: nothing integrates.
    let attempts: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM integration_attempt WHERE task_ref=? AND current=1 AND state NOT IN ('completed','cancelled','superseded','ejected','needs_review')",
    )
    .bind(&root.id)
    .fetch_one(lead.db.pool())
    .await
    .unwrap();
    assert_eq!(attempts, 0, "no integration attempt is owned for the root");
}

/// With no lead nothing changes: the rejection goes where the gate says.
#[tokio::test]
async fn a_rejected_aggregate_review_without_a_lead_keeps_the_configured_target() {
    let lead = lead_fixture(false).await;
    let root = root_with_failed_review(&lead, false).await;
    lead.dispatcher.check_once_and_drain().await.unwrap();

    let entries = log(&lead, &root.id).await;
    assert_eq!(
        entries.get(1).cloned(),
        Some(("review".to_owned(), "in_progress".to_owned(), None)),
        "{entries:?}"
    );
    assert!(entries
        .iter()
        .all(|(_, to, bridge)| to != "planning" && bridge.is_none()));
    assert_eq!(spent(&lead, &root.id, "replan").await, 0);
    assert_eq!(
        executions(&lead.db, &root.id)
            .await
            .iter()
            .filter(|(role, _)| role == PLANNER)
            .count(),
        0
    );
    let budgets: Vec<String> = sqlx::query_scalar("SELECT kind FROM task_budget WHERE task_id=?")
        .bind(&root.id)
        .fetch_all(lead.db.pool())
        .await
        .unwrap();
    assert!(!budgets.iter().any(|kind| kind == "replan"), "{budgets:?}");
}

/// The Review budget is untouched by the lead: a root whose review budget is
/// spent blocks in review, typed, and no re-plan is charged.
#[tokio::test]
async fn an_exhausted_review_budget_blocks_a_lead_root_in_review() {
    let lead = lead_fixture(false).await;
    let root = root_with_failed_review(&lead, true).await;
    sqlx::query("UPDATE task SET task_state_config = ? WHERE id = ?")
        .bind(serde_json::json!({"retry_budgets":{"review":1}}).to_string())
        .bind(&root.id)
        .execute(lead.db.pool())
        .await
        .unwrap();
    passes(&lead, 3).await;

    let blocked = reload(&lead.db, &root.id).await;
    assert_eq!(blocked.status, "review");
    assert_eq!(
        blocked
            .condition
            .read()
            .diagnostic
            .map(|diagnostic| diagnostic.annotation_type),
        Some(api_types::FailureKind::ReviewBudgetExhausted),
    );
    assert_eq!(spent(&lead, &root.id, "replan").await, 0);
    assert_eq!(spent(&lead, &root.id, "review").await, 1);
    assert!(log(&lead, &root.id)
        .await
        .iter()
        .all(|(_, to, _)| to != "planning"));
}

/// A rejected review with the re-plan budget spent parks the root in review
/// with the typed budget park; it does not fall back to implementation.
#[tokio::test]
async fn a_rejected_review_with_no_replan_left_parks_the_root_in_review() {
    let lead = lead_fixture(false).await;
    let root = root_with_failed_review(&lead, true).await;
    spend_replans(&lead, &root.id, 5).await;
    passes(&lead, 3).await;

    let blocked = reload(&lead.db, &root.id).await;
    assert_eq!(blocked.status, "review", "{:?}", log(&lead, &root.id).await);
    assert!(replan_exhausted(&blocked), "{:?}", blocked.condition);
    assert_eq!(spent(&lead, &root.id, "replan").await, 5);
}

async fn spend_replans(lead: &Lead, root: &str, count: usize) {
    let mut tx = db::begin_immediate(lead.db.pool()).await.unwrap();
    for index in 0..count {
        let charge = db::budget::charge(&mut tx, root, "replan", 5, &format!("earlier:{index}"))
            .await
            .unwrap();
        assert!(charge.charged);
    }
    tx.commit().await.unwrap();
}

fn replan_exhausted(task: &Task) -> bool {
    task.condition.reasons().any(|reason| {
        matches!(
            reason,
            db::ParkReason::BudgetExhausted {
                budget_kind: Some(db::budget::Kind::Replan),
                ..
            }
        )
    })
}

/// An implementing root with a lead and two subtasks; the first one has run
/// out of retries and is parked for a decision.
async fn root_with_parked_child(lead: &Lead, with_planner: bool) -> (Task, Task, Task) {
    let root = seed_task(&lead.db, &lead.project, "root", "in_progress", 1).await;
    let child = seed_subtask(&lead.db, &root, "first step", "in_progress", 0).await;
    let later = seed_subtask(&lead.db, &root, "second step", "todo", 1).await;
    assign_role(&lead.db, &root.id, CODER, &lead.coder).await;
    if with_planner {
        assign_role(&lead.db, &root.id, PLANNER, &lead.planner).await;
    }
    let child = park_child(lead, &child, "the build keeps failing on the linker").await;
    (reload(&lead.db, &root.id).await, child, later)
}

async fn park_child(lead: &Lead, child: &Task, message: &str) -> Task {
    let current = reload(&lead.db, &child.id).await;
    let at = now_rfc3339();
    TaskRepo::update(
        &*lead.db,
        db::UpdateTask {
            id: current.id.clone(),
            expected_version: current.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(
                serde_json::json!({
                    "type": api_types::FailureKind::RetryExhausted,
                    "blocking_reason": "execution retry budget exhausted",
                    "message": message,
                    "blocked_at": at,
                })
                .to_string(),
            )),
            blocked_json: Some(Some(
                serde_json::json!({
                    "reason": message,
                    "created_at": at,
                    "kind": api_types::FailureKind::RetryExhausted,
                    "source": null,
                    "execution_id": null,
                })
                .to_string(),
            )),
            failed_json: Some(None),
            task_state_config: None,
            parent_task_id: None,
            updated_at: at.clone(),
        },
    )
    .await
    .unwrap()
}

async fn child_receipt(lead: &Lead, child: &Task) -> String {
    let child = reload(&lead.db, &child.id).await;
    let park = crate::task_hierarchy::subtask_decision_park(&child).expect("the child is parked");
    crate::task_hierarchy::replan_receipt_for_subtask(&child.id, &park)
}

/// A subtask out of retries sends its lead-managed root back to planning,
/// once, with the subtask's evidence in the lead's input. The later subtask
/// is held while the root plans.
#[tokio::test]
async fn a_parked_subtask_sends_a_lead_root_back_to_planning() {
    let mut lead = lead_fixture(false).await;
    let (root, child, later) = root_with_parked_child(&lead, true).await;
    assert!(
        crate::task_hierarchy::lead_handles_subtask_park(&lead.db, &child.id)
            .await
            .unwrap(),
        "a fresh park under a lead is the lead's, not the Project Agent's"
    );
    passes(&lead, 3).await;

    let planning = reload(&lead.db, &root.id).await;
    assert_eq!(
        planning.status,
        "planning",
        "{:?}",
        log(&lead, &root.id).await
    );
    assert_eq!(
        log(&lead, &root.id).await,
        vec![(
            "in_progress".to_owned(),
            "planning".to_owned(),
            Some("replan".to_owned())
        )]
    );
    assert_eq!(spent(&lead, &root.id, "replan").await, 1);
    assert!(db::budget::receipt_recorded(
        lead.db.pool(),
        &root.id,
        "replan",
        &child_receipt(&lead, &child).await
    )
    .await
    .unwrap());
    assert_eq!(
        executions(&lead.db, &root.id).await,
        vec![(PLANNER.to_owned(), "running".to_owned())]
    );
    let ctx = tokio::time::timeout(Duration::from_secs(5), lead.rx.recv())
        .await
        .expect("planner context arrives")
        .expect("planner context");
    assert!(
        ctx.description.contains("first step")
            && ctx.description.contains("is parked and needs a decision")
            && ctx
                .description
                .contains("the build keeps failing on the linker"),
        "the lead's input names the parked subtask and why: {}",
        ctx.description
    );
    // The subtasks keep their place: the parked one stays parked, the later
    // one is held by its planning root and starts nothing.
    assert_eq!(reload(&lead.db, &child.id).await.status, "in_progress");
    let held = reload(&lead.db, &later.id).await;
    assert_eq!(held.status, "todo");
    assert_eq!(executions(&lead.db, &later.id).await, Vec::new());
    assert!(
        crate::task_hierarchy::lead_handles_subtask_park(&lead.db, &child.id)
            .await
            .unwrap(),
        "while the lead plans the Project Agent is not woken for the subtask"
    );
    passes(&lead, 3).await;
    assert_eq!(spent(&lead, &root.id, "replan").await, 1);
    assert_eq!(executions(&lead.db, &root.id).await.len(), 1);
}

/// O3: one re-plan per park. The lead finishes and leaves the subtask
/// parked: the root returns to implementation, waits on its subtasks, and
/// the park is the Project Agent's from here.
#[tokio::test]
async fn a_park_the_lead_left_in_place_is_not_replanned_again() {
    let lead = lead_fixture(true).await;
    let (root, child, _) = root_with_parked_child(&lead, true).await;
    until(
        &lead,
        "the lead re-planned and the root implements again",
        async || {
            let entries = log(&lead, &root.id).await;
            entries.len() >= 2 && reload(&lead.db, &root.id).await.status == "in_progress"
        },
    )
    .await;
    passes(&lead, 4).await;

    assert_eq!(
        log(&lead, &root.id).await,
        vec![
            (
                "in_progress".to_owned(),
                "planning".to_owned(),
                Some("replan".to_owned())
            ),
            ("planning".to_owned(), "in_progress".to_owned(), None),
        ]
    );
    assert_eq!(spent(&lead, &root.id, "replan").await, 1);
    assert_eq!(
        executions(&lead.db, &root.id).await,
        vec![(PLANNER.to_owned(), "completed".to_owned())]
    );
    assert!(
        park(&lead.db, &root.id).await.contains("Children"),
        "the root waits on its subtasks: {}",
        park(&lead.db, &root.id).await
    );
    assert!(
        !crate::task_hierarchy::lead_handles_subtask_park(&lead.db, &child.id)
            .await
            .unwrap(),
        "the Project Agent is woken for a park the lead left in place"
    );

    // A new park of the same subtask is a new decision.
    let cleared = reload(&lead.db, &child.id).await;
    TaskRepo::update(
        &*lead.db,
        db::UpdateTask {
            id: cleared.id.clone(),
            expected_version: cleared.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(None),
            blocked_json: Some(None),
            failed_json: Some(None),
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    park_child(&lead, &child, "it failed again after the retry").await;
    until(&lead, "the second park is re-planned", async || {
        spent(&lead, &root.id, "replan").await == 2
    })
    .await;
}

/// The sixth re-plan is refused: the root parks typed, where it is, with a
/// working owner action, and the subtask's park is the Project Agent's.
#[tokio::test]
async fn a_spent_replan_budget_parks_the_root_with_a_typed_park_and_an_exit() {
    let lead = lead_fixture(false).await;
    let (root, child, _) = root_with_parked_child(&lead, true).await;
    spend_replans(&lead, &root.id, 5).await;
    passes(&lead, 3).await;

    let blocked = reload(&lead.db, &root.id).await;
    assert_eq!(
        blocked.status,
        "in_progress",
        "{:?}",
        log(&lead, &root.id).await
    );
    assert!(replan_exhausted(&blocked), "{:?}", blocked.condition);
    assert_eq!(spent(&lead, &root.id, "replan").await, 5);
    assert_eq!(executions(&lead.db, &root.id).await, Vec::new());
    assert!(
        !crate::task_hierarchy::lead_handles_subtask_park(&lead.db, &child.id)
            .await
            .unwrap(),
        "with the root parked the Project Agent is woken"
    );
    // Stable: further passes write nothing.
    passes(&lead, 3).await;
    assert_eq!(reload(&lead.db, &root.id).await.version, blocked.version);

    // The exit: the owner's retry with a fresh budget lets the lead plan.
    let service = &lead.dispatcher.task_service;
    let offers = service.test_action_values(&root.id).await.unwrap();
    let retry = offers
        .iter()
        .find(|action| {
            matches!(
                action,
                api_types::TaskAction::Retry {
                    reset_budget: Some(true),
                    ..
                }
            )
        })
        .cloned()
        .unwrap_or_else(|| {
            panic!("a budget-parked root offers a retry with a new budget: {offers:?}")
        });
    service
        .perform_task_action(&root.id, retry, blocked.version)
        .await
        .expect("the retry with a new budget is accepted");
    until(
        &lead,
        "the root re-plans under the new budget",
        async || reload(&lead.db, &root.id).await.status == "planning",
    )
    .await;
    assert_eq!(spent(&lead, &root.id, "replan").await, 1);
}

/// A crash between the charge and the planning entry leaves a receipt and
/// no entry: the next pass enters planning and charges nothing more.
#[tokio::test]
async fn a_replan_charged_before_a_crash_is_not_charged_again() {
    let lead = lead_fixture(false).await;
    let (root, child, _) = root_with_parked_child(&lead, true).await;
    let receipt = child_receipt(&lead, &child).await;
    let mut tx = db::begin_immediate(lead.db.pool()).await.unwrap();
    assert!(
        db::budget::charge(&mut tx, &root.id, "replan", 5, &receipt)
            .await
            .unwrap()
            .charged
    );
    tx.commit().await.unwrap();
    passes(&lead, 3).await;

    assert_eq!(reload(&lead.db, &root.id).await.status, "planning");
    assert_eq!(spent(&lead, &root.id, "replan").await, 1);
    let charges: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_budget_charge WHERE task_id=? AND kind='replan'",
    )
    .bind(&root.id)
    .fetch_one(lead.db.pool())
    .await
    .unwrap();
    assert_eq!(charges, 1);
}

/// A paused Project does not re-plan; resuming it does. (A root cannot be
/// held while it implements: no hold is offered there. A blocked or held
/// root is covered by the policy test `replan_is_due_once_per_park...`.)
#[tokio::test]
async fn a_paused_project_does_not_replan() {
    let lead = lead_fixture(false).await;
    let (root, _, _) = root_with_parked_child(&lead, true).await;
    ProjectRepo::set_paused_at(&*lead.db, &lead.project, Some(now_rfc3339()))
        .await
        .unwrap();
    passes(&lead, 3).await;
    assert_eq!(reload(&lead.db, &root.id).await.status, "in_progress");
    assert_eq!(spent(&lead, &root.id, "replan").await, 0);
    assert_eq!(executions(&lead.db, &root.id).await, Vec::new());
    ProjectRepo::set_paused_at(&*lead.db, &lead.project, None)
        .await
        .unwrap();
    until(&lead, "the resumed root re-plans", async || {
        reload(&lead.db, &root.id).await.status == "planning"
    })
    .await;
    assert_eq!(spent(&lead, &root.id, "replan").await, 1);
}

/// Without a lead a parked subtask changes nothing on the root: it waits on
/// its subtasks and the Project Agent owns the park, as before.
#[tokio::test]
async fn a_parked_subtask_without_a_lead_leaves_the_root_alone() {
    let lead = lead_fixture(false).await;
    let (root, child, _) = root_with_parked_child(&lead, false).await;
    passes(&lead, 3).await;

    assert_eq!(reload(&lead.db, &root.id).await.status, "in_progress");
    assert_eq!(log(&lead, &root.id).await, Vec::new());
    assert_eq!(spent(&lead, &root.id, "replan").await, 0);
    assert!(park(&lead.db, &root.id).await.contains("Children"));
    assert!(
        !crate::task_hierarchy::lead_handles_subtask_park(&lead.db, &child.id)
            .await
            .unwrap()
    );
}
