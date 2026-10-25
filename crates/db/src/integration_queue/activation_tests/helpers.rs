//! Shared fixtures for integration activation tests.

use super::*;

pub(super) const NEW_MIGRATION: &str = "V202610091612__integration_activation.sql";

pub(super) const NOW: &str = "2026-10-08T00:00:00Z";

pub(super) const LATER: &str = "2026-10-08T00:01:00Z";

pub(super) const FAR: &str = "2099-01-01T00:00:00Z";

pub(super) async fn add_task(db: &SqliteDb, id: &str) {
    sqlx::query("INSERT INTO task(id,project_id,title,status,review_passed_at,created_at,updated_at) VALUES(?,'p',?,'merging',?,?,?)").bind(id).bind(id).bind(FIXED_TIME).bind(FIXED_TIME).bind(FIXED_TIME).execute(db.pool()).await.unwrap();
}

pub(super) async fn main_queue(db: &SqliteDb) -> IntegrationQueue {
    db.create_or_get_integration_queue("r", "main")
        .await
        .unwrap()
}

pub(super) async fn admit(db: &SqliteDb, q: &IntegrationQueue, task: &str) -> IntegrationAttempt {
    db.admit_integration_attempt(admission(q, task, &format!("entry-{task}")))
        .await
        .unwrap()
}

pub(super) async fn claim(
    db: &SqliteDb,
    queue_id: &str,
    now: &str,
    until: &str,
) -> Result<IntegrationQueue> {
    let q = db.integration_queue(queue_id).await.unwrap().unwrap();
    db.claim_integration_queue(queue_id, q.revision, "worker", now, until)
        .await
}

pub(super) async fn reread(db: &SqliteDb, id: &str) -> IntegrationAttempt {
    db.integration_attempt(id).await.unwrap().unwrap()
}

pub(super) async fn exec(db: &SqliteDb, sql: &str) {
    sqlx::query(sql).execute(db.pool()).await.unwrap();
}

/// Every table access in the plan goes through an index, and `index` is one.
pub(super) async fn assert_plan_uses(db: &SqliteDb, sql: &str, index: &str) {
    let rows = sqlx::query(&format!("EXPLAIN QUERY PLAN {sql}"))
        .fetch_all(db.pool())
        .await
        .unwrap();
    let plan: Vec<String> = rows
        .iter()
        .map(|row| row.try_get::<String, _>("detail").unwrap())
        .collect();
    assert!(
        plan.iter().any(|line| line.contains(index)),
        "{index} unused: {plan:?}"
    );
    for line in &plan {
        if (line.starts_with("SCAN") || line.starts_with("SEARCH"))
            && !line.contains("VIRTUAL TABLE")
        {
            assert!(line.contains("USING"), "unindexed access: {plan:?}");
        }
    }
}

pub(super) fn effect(fence: IntegrationOwnerFence) -> IntegrationEffectRequest {
    IntegrationEffectRequest {
        fence,
        kind: IntegrationOperationKind::FastForward,
        witness: serde_json::json!({"workspace":"frozen","head":"head","target":"target"}),
    }
}

/// A claimed head whose fast-forward reply was lost: an uncertain receipt.
pub(super) async fn uncertain_head(db: &SqliteDb) -> (IntegrationQueue, IntegrationEffectRequest) {
    let q = main_queue(db).await;
    let a = admit(db, &q, "a").await;
    let q = claim(db, &q.id, NOW, FAR).await.unwrap();
    let request = effect(db.integration_owner_fence(&a.id).await.unwrap().unwrap());
    let IntegrationEffectAdmission::Started(mut guard) =
        db.begin_integration_effect(request.clone()).await.unwrap()
    else {
        panic!("effect not admitted");
    };
    assert!(guard.start().await.unwrap().is_none());
    guard
        .record(
            serde_json::json!({"kind":"infrastructure"}),
            IntegrationOperationState::Uncertain,
        )
        .await
        .unwrap();
    (q, request)
}

pub(super) async fn step(db: &SqliteDb, task: &str, id: &str, kind: &str, key: &str, status: &str) {
    use crate::TaskStepRepo;
    db.enqueue_step(&crate::EnqueueTaskStep {
        id: id.into(),
        task_id: task.into(),
        kind: kind.into(),
        payload_json: "{}".into(),
        causation_step_id: None,
        causation_key: key.into(),
        chain_id: id.into(),
        chain_position: 1,
        expected_status: "merging".into(),
        expected_version: 1,
        expected_epoch: Some(0),
        lane: "fast".into(),
        available_at: NOW.into(),
    })
    .await
    .unwrap();
    sqlx::query("UPDATE task_step SET status=? WHERE id=?")
        .bind(status)
        .bind(id)
        .execute(db.pool())
        .await
        .unwrap();
}

pub(super) async fn listed(db: &SqliteDb) -> Vec<String> {
    db.claimable_integration_queues(NOW, None, 10)
        .await
        .unwrap()
        .into_iter()
        .map(|queue| queue.id)
        .collect()
}
