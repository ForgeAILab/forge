//! Test effect fencing and intent preservation after ownership takeover.

use super::*;
use crate::integration_queue::tests::{admission, fixture};
#[tokio::test]
async fn takeover_keeps_original_intent_and_refuses_old_fence_and_new_effect() {
    let db = fixture().await;
    let q = db
        .create_or_get_integration_queue("r", "main")
        .await
        .unwrap();
    let a = db
        .admit_integration_attempt(admission(&q, "a", "takeover"))
        .await
        .unwrap();
    let q = db.integration_queue(&q.id).await.unwrap().unwrap();
    let first = db
        .claim_integration_queue(
            &q.id,
            q.revision,
            "first",
            "2026-10-08T00:00:00Z",
            "2099-01-01T00:00:00Z",
        )
        .await
        .unwrap();
    sqlx::query("UPDATE integration_queue SET last_error_kind='infrastructure',last_error='owner reply missing' WHERE id=?").bind(&q.id).execute(db.pool()).await.unwrap();
    let f = db.integration_owner_fence(&a.id).await.unwrap().unwrap();
    let request = IntegrationEffectRequest {
        fence: f.clone(),
        kind: IntegrationOperationKind::Rebase,
        witness: serde_json::json!({"workspace":"w"}),
    };
    let IntegrationEffectAdmission::Started(mut g) =
        db.begin_integration_effect(request.clone()).await.unwrap()
    else {
        panic!("not admitted")
    };
    assert!(g.start().await.unwrap().is_none());
    drop(g);
    let next = db
        .claim_integration_queue(
            &q.id,
            first.revision,
            "next",
            "2099-01-01T00:00:01Z",
            "2099-01-01T00:01:00Z",
        )
        .await
        .unwrap();
    assert_eq!(next.fence_generation, 2);
    assert_eq!(
        next.last_error_kind,
        Some(IntegrationFailureKind::Infrastructure)
    );
    assert_eq!(next.last_error.as_deref(), Some("owner reply missing"));
    let attempt = db.integration_attempt(&a.id).await.unwrap().unwrap();
    assert_eq!(attempt.state, IntegrationAttemptState::Reconciling);
    assert!(matches!(
        db.begin_integration_effect(request).await.unwrap(),
        IntegrationEffectAdmission::Refused(IntegrationEffectRefusal::StaleFence)
    ));
    let raw: String =
        sqlx::query_scalar("SELECT effect_intent_json FROM integration_attempt WHERE id=?")
            .bind(&a.id)
            .fetch_one(db.pool())
            .await
            .unwrap();
    let old: Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(old["request"]["fence"]["generation"], 1);
    let new_request = IntegrationEffectRequest {
        fence: db.integration_owner_fence(&a.id).await.unwrap().unwrap(),
        kind: IntegrationOperationKind::Rebase,
        witness: serde_json::json!({"workspace":"w"}),
    };
    assert!(matches!(
        db.begin_integration_effect(new_request).await.unwrap(),
        IntegrationEffectAdmission::Refused(IntegrationEffectRefusal::ReconciliationRequired)
    ));
}
