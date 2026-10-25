//! Imported integration attempts and merge continuation ownership.

use super::*;

/// One writer per merge across an upgrade: the worker takes an attempt only
/// once it was handed over. An attempt admission made is; one from before
/// the queue (a `shadow:` row, an imported row) is once its import
/// continuation step settled and the old merge's own hooks step is gone.
#[tokio::test]
async fn a_pre_queue_attempt_is_not_claimable_until_the_import_has_continued_it() {
    for imported in [false, true] {
        let db = fixture().await;
        let q = main_queue(&db).await;
        let mut row = admission(&q, "a", if imported { "import:a:0" } else { "shadow:a:0" });
        if imported {
            row.import_source_json = Some(serde_json::json!({"version":1,"priority":4}));
        }
        let pre = db.admit_integration_attempt(row).await.unwrap();
        let key = format!("integration-import:{}", pre.id);

        // No continuation yet, then one that is still to run or running.
        assert!(listed(&db).await.is_empty(), "imported={imported}");
        assert!(matches!(
            claim(&db, &q.id, NOW, LATER).await,
            Err(DbError::NotFound)
        ));
        step(&db, "a", "cont", "integration", &key, "pending").await;
        for live in ["pending", "claimed"] {
            exec(
                &db,
                &format!("UPDATE task_step SET status='{live}' WHERE id='cont'"),
            )
            .await;
            assert!(listed(&db).await.is_empty(), "{live}");
            assert!(matches!(
                claim(&db, &q.id, NOW, LATER).await,
                Err(DbError::NotFound)
            ));
        }
        // Continued, but the old merge's own hooks step (it recorded a
        // rebase target) is still live: never claimable.
        exec(&db, "UPDATE task_step SET status='done' WHERE id='cont'").await;
        step(&db, "a", "legacy", "hooks", "legacy", "pending").await;
        exec(&db, "INSERT INTO task_hook_checkpoint(step_id,hook_index,started_at,effects_json) VALUES('legacy',0,'2026-10-08T00:00:00Z','{\"rebase_target\":\"\\\"abc\\\"\"}')").await;
        for live in ["pending", "claimed", "suspended"] {
            exec(
                &db,
                &format!("UPDATE task_step SET status='{live}' WHERE id='legacy'"),
            )
            .await;
            assert!(listed(&db).await.is_empty(), "legacy {live}");
            assert!(matches!(
                claim(&db, &q.id, NOW, LATER).await,
                Err(DbError::NotFound)
            ));
        }
        // A hooks step with no effect of the old merge path only admits: it
        // holds nothing.
        exec(
            &db,
            "UPDATE task_hook_checkpoint SET effects_json='{}' WHERE step_id='legacy'",
        )
        .await;
        assert_eq!(listed(&db).await, vec![q.id.clone()]);
        exec(&db, "UPDATE task_hook_checkpoint SET effects_json='{\"merge_intent\":\"{}\"}' WHERE step_id='legacy'").await;
        assert!(listed(&db).await.is_empty());
        // The old step settled: handed over.
        exec(&db, "UPDATE task_step SET status='done' WHERE id='legacy'").await;
        assert_eq!(listed(&db).await, vec![q.id.clone()]);
        let claimed = claim(&db, &q.id, NOW, LATER).await.unwrap();
        assert_eq!(claimed.head_attempt_id.as_deref(), Some(pre.id.as_str()));
    }
}

/// An attempt admission made is claimable at once, also while its entry's
/// hooks step (the admission itself) is still running, and it is taken ahead
/// of an older pre-queue row that was not handed over.
#[tokio::test]
async fn an_admitted_attempt_is_claimed_past_a_pre_queue_row_that_was_not_handed_over() {
    let db = fixture().await;
    let q = main_queue(&db).await;
    let shadow = db
        .admit_integration_attempt(admission(&q, "a", "shadow:a:0"))
        .await
        .unwrap();
    let admitted = db
        .admit_integration_attempt(admission(&q, "b", "admit:b:0"))
        .await
        .unwrap();
    step(&db, "b", "entry", "hooks", "entry", "claimed").await;
    assert!(shadow.queue_seq < admitted.queue_seq);
    assert_eq!(listed(&db).await, vec![q.id.clone()]);
    let claimed = claim(&db, &q.id, NOW, LATER).await.unwrap();
    assert_eq!(
        claimed.head_attempt_id.as_deref(),
        Some(admitted.id.as_str())
    );
}

/// An imported unknown result is a reserved head. The worker does not drive
/// it while the old merge's own hooks step or the row's import continuation
/// step is live, and does once neither is.
#[tokio::test]
async fn a_reserved_imported_head_is_held_while_the_old_merge_step_is_live() {
    let db = fixture().await;
    let q = main_queue(&db).await;
    let mut row = admission(&q, "a", "import:a:0");
    row.import_source_json = Some(serde_json::json!({"version":1,"priority":2}));
    let head = db.admit_integration_attempt(row).await.unwrap();
    sqlx::query("UPDATE integration_attempt SET state='reconciling' WHERE id=?")
        .bind(&head.id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE integration_queue SET state='quarantined',head_attempt_id=?,revision=revision+1 WHERE id=?")
        .bind(&head.id)
        .bind(&q.id)
        .execute(db.pool())
        .await
        .unwrap();
    step(&db, "a", "legacy", "hooks", "legacy", "claimed").await;
    exec(&db, "INSERT INTO task_hook_checkpoint(step_id,hook_index,started_at,effects_json) VALUES('legacy',0,'2026-10-08T00:00:00Z','{\"merge_intent\":\"{}\"}')").await;
    assert!(listed(&db).await.is_empty());
    assert!(matches!(
        claim(&db, &q.id, NOW, LATER).await,
        Err(DbError::VersionConflict)
    ));
    exec(&db, "UPDATE task_step SET status='done' WHERE id='legacy'").await;
    // The old step is gone and the row's import continuation is still to
    // run, running or waiting (it retries behind a suspended hooks step). It
    // reports a row it finds `applied`: a driver that took the head now and
    // reconciled it to `applied` would hold the merge beside it (seen in CI
    // as `[ImportContinuation, WorkerDriver]` on a P1 row).
    let key = format!("integration-import:{}", head.id);
    step(&db, "a", "cont", "integration", &key, "pending").await;
    for live in ["pending", "claimed", "suspended"] {
        exec(
            &db,
            &format!("UPDATE task_step SET status='{live}' WHERE id='cont'"),
        )
        .await;
        assert!(listed(&db).await.is_empty(), "continuation {live}");
        assert!(
            matches!(
                claim(&db, &q.id, NOW, LATER).await,
                Err(DbError::VersionConflict)
            ),
            "continuation {live}"
        );
    }
    exec(&db, "UPDATE task_step SET status='done' WHERE id='cont'").await;
    assert_eq!(listed(&db).await, vec![q.id.clone()]);
    let claimed = claim(&db, &q.id, NOW, LATER).await.unwrap();
    assert_eq!(claimed.head_attempt_id.as_deref(), Some(head.id.as_str()));
}

/// A reserved imported head whose settled continuation step was pruned (or
/// that never had one) is not held: only a live step holds it.
#[tokio::test]
async fn a_reserved_imported_head_without_a_live_continuation_is_claimable() {
    let db = fixture().await;
    let q = main_queue(&db).await;
    let mut row = admission(&q, "a", "import:a:0");
    row.import_source_json = Some(serde_json::json!({"version":1,"priority":1}));
    let head = db.admit_integration_attempt(row).await.unwrap();
    sqlx::query("UPDATE integration_attempt SET state='reconciling' WHERE id=?")
        .bind(&head.id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE integration_queue SET state='quarantined',head_attempt_id=?,revision=revision+1 WHERE id=?")
        .bind(&head.id)
        .bind(&q.id)
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(listed(&db).await, vec![q.id.clone()]);
    let claimed = claim(&db, &q.id, NOW, LATER).await.unwrap();
    assert_eq!(claimed.head_attempt_id.as_deref(), Some(head.id.as_str()));
}

/// A passive-stage `shadow:` row of a Task that was deleted gets no
/// continuation step and holds no slot: nothing would ever end it. The
/// importer's start pass deletes it. A shadow row of a live Task, one that
/// is its queue's head, and one whose effect is unsettled or unknown stay.
#[tokio::test]
async fn shadow_rows_of_deleted_tasks_are_deleted_and_every_other_row_is_kept() {
    let db = fixture().await;
    let q = main_queue(&db).await;
    let mut ids = std::collections::BTreeMap::new();
    for task in [
        "gone", "live", "head", "unknown", "admitted", "history", "landed", "inflight", "ended",
    ] {
        add_task(&db, task).await;
        let attempt = admit(&db, &q, task).await;
        if task != "admitted" {
            sqlx::query("UPDATE integration_attempt SET admission_key=? WHERE id=?")
                .bind(format!("shadow:{task}:0"))
                .bind(&attempt.id)
                .execute(db.pool())
                .await
                .unwrap();
        }
        ids.insert(task, attempt.id);
    }
    exec(
        &db,
        "UPDATE integration_attempt SET current_operation_state='uncertain' WHERE task_ref='unknown'",
    )
    .await;
    sqlx::query("UPDATE integration_queue SET head_attempt_id=?,revision=revision+1 WHERE id=?")
        .bind(&ids["head"])
        .bind(&q.id)
        .execute(db.pool())
        .await
        .unwrap();
    // A history row (a later admission superseded it), a row that records a
    // landed merge, one whose effect may be in flight, and a terminal row.
    exec(
        &db,
        "UPDATE integration_attempt SET current=0,state='superseded' WHERE task_ref='history'",
    )
    .await;
    exec(
        &db,
        "UPDATE integration_attempt SET state='applied' WHERE task_ref='landed'",
    )
    .await;
    exec(
        &db,
        "UPDATE integration_attempt SET state='reconciling' WHERE task_ref='inflight'",
    )
    .await;
    exec(
        &db,
        "UPDATE integration_attempt SET current=0,state='cancelled' WHERE task_ref='ended'",
    )
    .await;
    assert_eq!(db.delete_orphan_shadow_attempts().await.unwrap(), 0);
    exec(
        &db,
        "UPDATE task SET deleted_at='2026-10-08T00:00:00Z' WHERE id<>'live'",
    )
    .await;
    // The cost at start and per sweep. The delete walks the current rows
    // (one per Task that ever merged), and the foreign key that nulls a
    // successor's `predecessor_attempt_id` seeks its index instead of
    // scanning the attempt history once per deleted row. `integration_queue`
    // (one row per repo and target branch) is the only table read whole.
    let delete: Vec<String> = sqlx::query(&format!(
        "EXPLAIN QUERY PLAN DELETE {}",
        super::shadow::ORPHAN_SHADOW_ATTEMPTS
    ))
    .fetch_all(db.pool())
    .await
    .unwrap()
    .iter()
    .map(|row| row.try_get::<String, _>("detail").unwrap())
    .collect();
    assert!(
        delete
            .iter()
            .any(|line| line.contains("integration_attempt_current_task")),
        "{delete:?}"
    );
    assert!(
        delete
            .iter()
            .any(|line| line.contains("integration_attempt_predecessor")),
        "{delete:?}"
    );
    assert!(
        !delete.iter().any(|line| line == "SCAN integration_attempt"),
        "the attempt table is scanned: {delete:?}"
    );
    assert_plan_uses(
        &db,
        super::shadow::QUEUED_WITHOUT_CONTINUATION,
        "integration_attempt_retention",
    )
    .await;
    assert_eq!(db.delete_orphan_shadow_attempts().await.unwrap(), 1);
    assert!(db
        .integration_attempt(&ids["gone"])
        .await
        .unwrap()
        .is_none());
    for kept in [
        "live", "head", "unknown", "admitted", "history", "landed", "inflight", "ended",
    ] {
        assert!(
            db.integration_attempt(&ids[kept]).await.unwrap().is_some(),
            "{kept}"
        );
    }
    assert_eq!(db.delete_orphan_shadow_attempts().await.unwrap(), 0);
}
