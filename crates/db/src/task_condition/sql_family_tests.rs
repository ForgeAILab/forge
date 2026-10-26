//! Exact archived-main results for each former SQL-classifier family.
use super::sql_replay_tests::{assert_main, clock, db, stored};
use super::tests::{claim, task};
use super::*;
use crate::task_writer::{BulkTaskQuery, TaskQuery, TaskQueryResult};
use crate::{TaskMutation, TaskStepRepo};
use serde_json::json;

async fn family(name: &str, query: &str, change: ConditionChange) {
    for mode in ["inline", "queued", "bulk-inline", "bulk-queued"] {
        let _clock = clock();
        let db = db().await;
        task(&db, "t").await;
        sqlx::query("UPDATE task SET status='planning' WHERE id='t'")
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO task_role_assignment(id,task_id,role_name,assignee_type,assignee_id,created_at,updated_at) VALUES('role','t','planner','user','owner','at','at')")
            .execute(db.pool()).await.unwrap();
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        db.sync_condition_in_tx(&mut tx, "t").await.unwrap();
        tx.commit().await.unwrap();
        let before = stored(&db, "t").await;
        if name == "none" {
            // A no-change write must leave even recoverable corruption untouched.
            sqlx::query("UPDATE task SET condition_json='{}' WHERE id='t'")
                .execute(db.pool())
                .await
                .unwrap();
        }
        let apply_inline = async {
            let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
            if mode == "bulk-inline" {
                let result = BulkTaskQuery::new(&db, query, change)
                    .bind("t")
                    .execute_in_tx(&mut tx)
                    .await
                    .unwrap();
                assert_eq!((result.applied, result.queued), (1, 0));
            } else {
                assert_eq!(
                    TaskQuery::new(&db, "t", query, change)
                        .bind("t")
                        .execute_in_tx(&mut tx)
                        .await
                        .unwrap(),
                    TaskQueryResult::Applied(1)
                );
            }
            tx.commit().await.unwrap();
        };
        if mode.ends_with("inline") {
            let step = claim(&db, "t").await;
            crate::task_writer::in_task_step(step, apply_inline).await;
        } else {
            let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
            if mode == "bulk-queued" {
                let result = BulkTaskQuery::new(&db, query, change)
                    .bind("t")
                    .execute_in_tx(&mut tx)
                    .await
                    .unwrap();
                assert_eq!((result.applied, result.queued), (0, 1));
            } else {
                assert!(matches!(
                    TaskQuery::new(&db, "t", query, change)
                        .bind("t")
                        .execute_in_tx(&mut tx)
                        .await
                        .unwrap(),
                    TaskQueryResult::Queued { .. }
                ));
            }
            tx.commit().await.unwrap();
            let step = claim_queued(&db).await;
            let mutation: TaskMutation = serde_json::from_str(&step.payload_json).unwrap();
            assert!(
                matches!(mutation, TaskMutation::Sql { change: stored_change, .. } if stored_change == change)
            );
            crate::task_writer::in_task_step(step.clone(), db.execute_task_mutation(&step))
                .await
                .unwrap();
        }
        let actual = stored(&db, "t").await;
        assert_main(&format!("{name}-{mode}"), actual.clone());
        match change {
            ConditionChange::None => {
                assert_eq!(actual["condition"], "{}");
                assert_eq!(actual["diagnostics"], json!([]));
            }
            _ => assert_ne!(before["condition"], actual["condition"]),
        }
    }
}

async fn claim_queued(db: &SqliteDb) -> crate::TaskStep {
    db.claim_step(
        "sql-family",
        Some("t"),
        &crate::task_writer::lease_deadline(),
    )
    .await
    .unwrap()
    .unwrap()
}

#[tokio::test]
async fn explicit_none_matches_main() {
    family(
        "none",
        "UPDATE task SET title='edited',version=version+1 WHERE id=? AND blocked_json IS NULL",
        ConditionChange::None,
    )
    .await;
}

#[tokio::test]
async fn explicit_legacy_matches_main() {
    family("legacy", "UPDATE task SET blocked_json='{\"kind\":\"executor_failed\",\"reason\":\"literal failure\"}',version=version+1 WHERE id=?", ConditionChange::Legacy).await;
}

#[tokio::test]
async fn explicit_entry_matches_main() {
    family("entry", "UPDATE task SET status='done',status_epoch=status_epoch+1,version=version+1 WHERE id=? AND blocked_json IS NULL", ConditionChange::Entry).await;
}

#[tokio::test]
async fn explicit_human_matches_main() {
    // BulkTaskQuery operates on Task rows; role writes use TaskQuery only.
    for queued in [false, true] {
        let _clock = clock();
        let db = db().await;
        task(&db, "t").await;
        sqlx::query("UPDATE task SET status='planning' WHERE id='t'")
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO task_role_assignment(id,task_id,role_name,assignee_type,assignee_id,created_at,updated_at) VALUES('role','t','planner','user','owner','at','at')").execute(db.pool()).await.unwrap();
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        db.sync_condition_in_tx(&mut tx, "t").await.unwrap();
        tx.commit().await.unwrap();
        assert!(db.task_condition("t").await.unwrap().read().human_wait);
        let write = async {
            let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
            let result = TaskQuery::new(
                &db,
                "t",
                "DELETE FROM task_role_assignment WHERE task_id=?",
                ConditionChange::Human,
            )
            .bind("t")
            .execute_in_tx(&mut tx)
            .await
            .unwrap();
            assert_eq!(result.applied().is_some(), !queued);
            tx.commit().await.unwrap();
        };
        if queued {
            write.await;
            let step = claim_queued(&db).await;
            crate::task_writer::in_task_step(step.clone(), db.execute_task_mutation(&step))
                .await
                .unwrap();
        } else {
            let step = claim(&db, "t").await;
            crate::task_writer::in_task_step(step, write).await;
        }
        assert!(!db.task_condition("t").await.unwrap().read().human_wait);
        assert_main(
            if queued {
                "human-queued"
            } else {
                "human-inline"
            },
            stored(&db, "t").await,
        );
    }
}

#[tokio::test]
async fn workflow_edit_matches_main_without_metadata_scan() {
    use crate::ProjectRepo;
    let _clock = clock();
    let db = db().await;
    task(&db, "root").await;
    task(&db, "child").await;
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    sqlx::query(
        "UPDATE task SET metadata_json='{\"coordination_review_pending\":true}' WHERE id='root'",
    )
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query("UPDATE task SET parent_task_id='root',status='released' WHERE id='child'")
        .execute(&mut *tx)
        .await
        .unwrap();
    db.sync_condition_in_tx(&mut tx, "child").await.unwrap();
    db.sync_condition_in_tx(&mut tx, "root").await.unwrap();
    tx.commit().await.unwrap();
    assert!(db.task_condition("root").await.unwrap().is_blocked());
    let project = ProjectRepo::get_by_id(&db, "p").await.unwrap().unwrap();
    ProjectRepo::update_workflow(
        &db,
        "p",
        &json!({"states":[{"name":"todo","kind":"initial"},{"name":"released","kind":"terminal"}]})
            .to_string(),
        None,
        project.version,
        &crate::now_rfc3339(),
    )
    .await
    .unwrap();
    assert!(!db.task_condition("root").await.unwrap().is_blocked());
    for id in ["root", "child"] {
        assert_main(&format!("workflow-{id}"), stored(&db, id).await);
    }
    assert_eq!(
        db.task_condition_violations().await.unwrap(),
        Vec::<String>::new()
    );
}
