//! Root lead, stage 2: the aggregate review of a coordination root with a
//! lead fails on its ENTRY checks (`settle_failed_ci_entry`). The rejection
//! re-plans exactly as a reviewer's rejection does, and the loop it opens
//! (planning, implementation, review again) ends on the Review budget.
use super::*;

async fn spent(db: &SqliteDb, task: &str, kind: db::budget::Kind) -> i64 {
    db::budget::spent(db.pool(), task, kind.key())
        .await
        .unwrap()
}

/// Make the fixture's Task a coordination root whose only subtask is done,
/// with or without a lead.
async fn make_root(fixture: &FailedCiFixture, with_lead: bool) {
    let now = now_rfc3339();
    let child = new_uuid_v4();
    TaskRepo::create(
        &*fixture.db,
        db::CreateTask {
            id: child.clone(),
            project_id: fixture.task.project_id.clone(),
            parent_task_id: Some(fixture.task.id.clone()),
            subtask_order: Some(0),
            assignee_type: None,
            assignee_id: None,
            title: "only step".to_owned(),
            description: None,
            task_type: "sub_task".to_owned(),
            status: "done".to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    // The aggregate review of a root reviews what its subtasks produced.
    sqlx::query("UPDATE execution SET task_id = ? WHERE task_id = ?")
        .bind(&child)
        .bind(&fixture.task.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    if with_lead {
        let lead = crate::test_support::seed_native_agent(&fixture.db).await;
        db::TaskRoleAssignmentRepo::assign(
            &*fixture.db,
            db::CreateTaskRoleAssignment {
                id: new_uuid_v4(),
                task_id: fixture.task.id.clone(),
                role_name: default_roles::PLANNER.to_owned(),
                assignee_type: Some(db::AssigneeKind::Agent),
                assignee_id: Some(lead),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .unwrap();
    }
}

async fn enter_review(fixture: &FailedCiFixture) -> db::Task {
    move_to(fixture, "review").await
}

async fn move_to(fixture: &FailedCiFixture, target: &str) -> db::Task {
    let task = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    let result = fixture
        .engine
        .workflow_execution()
        .transition_with_authority(
            &task.id,
            target,
            task.version,
            &fixture.workflow,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "all ordered subtasks completed",
            false,
            fixture.workflow_authority().await,
        )
        .await
        .unwrap();
    drain_result(fixture.engine.clone(), result).await.task
}

async fn entries(fixture: &FailedCiFixture) -> Vec<db::TransitionLog> {
    TransitionLogRepo::list_by_task(&*fixture.db, &fixture.task.id)
        .await
        .unwrap()
}

/// T4 through the entry failure path, then the second round: the lead adds
/// nothing, the root comes back to review, its checks fail again, and the
/// Review budget parks it. One re-plan was charged, not two.
#[tokio::test]
async fn a_failed_review_entry_of_a_lead_root_replans_once_and_the_second_exhausts_the_review_budget(
) {
    let fixture = failed_ci_fixture(2, FailurePolicy::Block).await;
    make_root(&fixture, true).await;

    let task = enter_review(&fixture).await;
    let log = entries(&fixture).await;
    assert_eq!(
        task.status,
        "planning",
        "{:?}",
        log.iter()
            .map(|entry| (&entry.from_state, &entry.to_state, &entry.trigger_reason))
            .collect::<Vec<_>>()
    );
    let rejection = log
        .iter()
        .find(|entry| entry.from_state == "review" && entry.rejection)
        .expect("the failed entry is a rejection");
    assert_eq!(rejection.to_state, "planning");
    assert_eq!(
        rejection.bridge.bridge_kind,
        Some(api_types::TransitionBridgeKind::Replan)
    );
    assert!(
        rejection.trigger_reason.starts_with("CI step 0 failed"),
        "{}",
        rejection.trigger_reason
    );
    assert_eq!(
        spent(&fixture.db, &task.id, db::budget::Kind::Review).await,
        1
    );
    assert_eq!(
        spent(&fixture.db, &task.id, db::budget::Kind::Replan).await,
        1
    );
    assert!(task.entry_barrier_json.is_none());

    // Round two. The lead adds no work: the root implements again with its
    // sequence still complete and goes back to its aggregate review.
    let back = move_to(&fixture, "in_progress").await;
    assert_eq!(back.status, "in_progress", "{:?}", back.condition);
    let task = enter_review(&fixture).await;
    assert_eq!(task.status, "review", "{:?}", task.condition);
    let annotation: serde_json::Value =
        serde_json::from_str(task.error_annotation.as_deref().unwrap()).unwrap();
    assert_eq!(annotation["type"], "review_budget_exhausted");
    assert_eq!(
        spent(&fixture.db, &task.id, db::budget::Kind::Replan).await,
        1,
        "the rejection that exhausts the Review budget charges no re-plan"
    );
    assert_eq!(
        entries(&fixture)
            .await
            .iter()
            .filter(|entry| entry.to_state == "planning")
            .count(),
        1
    );
}

/// The same failed entry of a root without a lead goes where the gate says.
#[tokio::test]
async fn a_failed_review_entry_of_a_root_without_a_lead_keeps_the_configured_target() {
    let fixture = failed_ci_fixture(2, FailurePolicy::Block).await;
    make_root(&fixture, false).await;
    let task = enter_review(&fixture).await;
    assert_eq!(task.status, "in_progress");
    let log = entries(&fixture).await;
    assert!(log
        .iter()
        .all(|entry| entry.to_state != "planning" && entry.bridge.bridge_kind.is_none()));
    assert_eq!(
        spent(&fixture.db, &task.id, db::budget::Kind::Replan).await,
        0
    );
}
