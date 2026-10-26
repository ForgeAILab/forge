use super::tests::{claim, db, task};
use super::*;
use crate::{TaskMetadataMutation, TaskRepo};
use serde_json::json;

/// Dual-written legacy columns still decide corruption repair at this checkpoint.
#[tokio::test]
async fn undecodable_stored_condition_is_recomputed_not_fatal() {
    let db = db().await;
    undecodable_stored_condition(&db, "undecodable", "{\"kind\":\"clear\"}").await;
    let condition = db.task_condition("undecodable").await.unwrap();
    assert!(matches!(condition, TaskCondition::Clear { .. }));
    let details = db
        .task_diagnostic_texts("undecodable", None, 100)
        .await
        .unwrap()
        .items;
    assert!(details.iter().any(|d| d.text == r#"{"kind":"clear"}"#));
    assert_eq!(
        db.check_task_conditions_of(&["undecodable".into()])
            .await
            .unwrap(),
        0
    );
    assert!(db.task_condition_violations().await.unwrap().is_empty());
}
/// The same legacy writes over a newer build's encoding land too, and leave
/// that encoding quarantined: the writers state nothing, so nothing is refused.
#[tokio::test]
async fn undecodable_newer_condition_is_quarantined_not_fatal() {
    let db = db().await;
    undecodable_stored_condition(&db, "newer", "{\"kind\":\"from_a_future_release\"}").await;
    assert!(db.task_condition("newer").await.is_err());
    assert_eq!(
        db.task_condition_violations().await.unwrap(),
        vec!["newer".to_owned()]
    );
}
async fn undecodable_stored_condition(db: &SqliteDb, id: &str, undecodable: &str) {
    let quarantined = matches!(classify(undecodable.as_bytes()), Stored::Newer);
    let t = task(db, id).await;
    let corrupt = || async {
        sqlx::query("UPDATE task SET condition_json=? WHERE id=?")
            .bind(undecodable)
            .bind(&t.id)
            .execute(db.pool())
            .await
            .unwrap();
    };
    corrupt().await;
    assert!(db.task_condition(&t.id).await.is_err());
    let step = claim(db, &t.id).await;
    crate::task_writer::in_task_step(step, async {
        let updated = TaskRepo::mutate_metadata(
            db,
            &t.id,
            Some(t.version),
            vec![TaskMetadataMutation::Set {
                key: "owner_wait".into(),
                value: json!({"daemon_id":"d"}),
            }],
            &crate::now_rfc3339(),
        )
        .await
        .unwrap();
        assert_eq!(
            db.check_task_condition_invariant(&updated).await.is_err(),
            quarantined
        );
        corrupt().await;
        crate::task_writer::TaskQuery::new(
            db,
            &t.id,
            "UPDATE task SET metadata_json=json_remove(metadata_json,'$.owner_wait') WHERE id=?",
            crate::ConditionChange::Legacy,
        )
        .bind(&t.id)
        .execute(db.pool())
        .await
        .unwrap();
    })
    .await;
}
