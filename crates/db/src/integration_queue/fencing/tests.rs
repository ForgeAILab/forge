//! Test integration effect fencing, receipts, settlement, and owner removal.

use super::*;
use crate::integration_queue::tests::{admission, fixture};
async fn claimed() -> (SqliteDb, IntegrationOwnerFence) {
    let db = fixture().await;
    let q = db
        .create_or_get_integration_queue("r", "main")
        .await
        .unwrap();
    let a = db
        .admit_integration_attempt(admission(&q, "a", "fence"))
        .await
        .unwrap();
    let q = db.integration_queue(&q.id).await.unwrap().unwrap();
    db.claim_integration_queue(
        &q.id,
        q.revision,
        "owner",
        "2026-10-08T00:00:00Z",
        "2099-01-01T00:00:00Z",
    )
    .await
    .unwrap();
    let f = db.integration_owner_fence(&a.id).await.unwrap().unwrap();
    (db, f)
}
fn request(f: IntegrationOwnerFence) -> IntegrationEffectRequest {
    IntegrationEffectRequest {
        fence: f,
        kind: IntegrationOperationKind::Rebase,
        witness: serde_json::json!({"workspace":"frozen","head":"head","target":"target"}),
    }
}
#[tokio::test]
async fn stale_and_foreign_fences_are_typed_refusals_before_intent() {
    let (db, f) = claimed().await;
    let mut stale = f.clone();
    stale.generation += 1;
    assert!(matches!(
        db.begin_integration_effect(request(stale)).await.unwrap(),
        IntegrationEffectAdmission::Refused(IntegrationEffectRefusal::StaleFence)
    ));
    let mut foreign = f.clone();
    foreign.target_owner["generation"] = serde_json::json!(99);
    assert!(matches!(
        db.begin_integration_effect(request(foreign)).await.unwrap(),
        IntegrationEffectAdmission::Refused(IntegrationEffectRefusal::ForeignOwner)
    ));
    let raw: Option<String> =
        sqlx::query_scalar("SELECT effect_intent_json FROM integration_attempt WHERE id=?")
            .bind(f.attempt_id)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert!(raw.is_none());
}
#[tokio::test]
async fn completed_duplicate_replays_and_reused_key_with_changed_input_is_refused() {
    let (db, f) = claimed().await;
    let r = request(f);
    let IntegrationEffectAdmission::Started(g) =
        db.begin_integration_effect(r.clone()).await.unwrap()
    else {
        panic!("not admitted")
    };
    let receipt = g
        .record(
            serde_json::json!({"kind":"rebased"}),
            IntegrationOperationState::Succeeded,
        )
        .await
        .unwrap();
    let IntegrationEffectAdmission::Replay(replayed) =
        db.begin_integration_effect(r.clone()).await.unwrap()
    else {
        panic!("not replayed")
    };
    assert_eq!(receipt, replayed);
    let mut changed = r.clone();
    changed.witness["head"] = serde_json::json!("different");
    assert!(matches!(
        db.begin_integration_effect(changed).await.unwrap(),
        IntegrationEffectAdmission::Refused(IntegrationEffectRefusal::RequestConflict)
    ));
    assert_eq!(
        db.integration_effect_receipt(&r).await.unwrap(),
        Some(receipt)
    );
    let n: i64 = sqlx::query_scalar(
        "SELECT json_array_length(effect_receipts_json) FROM integration_attempt WHERE id=?",
    )
    .bind(&r.fence.attempt_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(n, 1);
}
#[tokio::test]
async fn dropped_owner_guard_keeps_durable_intent_and_never_repeats_an_unknown_effect() {
    let (db, f) = claimed().await;
    let r = request(f);
    let IntegrationEffectAdmission::Started(mut g) =
        db.begin_integration_effect(r.clone()).await.unwrap()
    else {
        panic!("not admitted")
    };
    assert!(g.start().await.unwrap().is_none());
    drop(g);
    assert!(db.integration_effect_receipt(&r).await.unwrap().is_none());
    assert!(matches!(
        db.begin_integration_effect(r.clone()).await.unwrap(),
        IntegrationEffectAdmission::Refused(IntegrationEffectRefusal::ReconciliationRequired)
    ));
    let mut attempt = db
        .integration_attempt(&r.fence.attempt_id)
        .await
        .unwrap()
        .unwrap();
    assert!(attempt.effect_intent_json.is_some());
    attempt.state = IntegrationAttemptState::Parked;
    attempt.current_operation_state = Some(IntegrationOperationState::Succeeded);
    assert!(matches!(
        db.transition_integration_attempt(attempt).await,
        Err(DbError::InvalidTransition)
    ));
}
#[tokio::test]
async fn task_step_binds_unobserved_shadow_workspace_without_claiming_queue() {
    use crate::TaskStepRepo;
    let db = fixture().await;
    crate::integration_queue::tests::seed_delivery(&db, "a").await;
    let path: String = sqlx::query_scalar("SELECT worktree_path FROM workspace WHERE id='w-a'")
        .fetch_one(db.pool())
        .await
        .unwrap();
    let now = now_rfc3339();
    sqlx::query("INSERT INTO workspace_placement(id,workspace_id,task_id,owner_kind,repo_location_id,workspace_handle,generation,state,selected_by,selection_reason,created_at,updated_at) VALUES('pl','w-a','a','server','l',?,1,'ready','scheduler','{}',?,?)").bind(&path).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    let q = db
        .create_or_get_integration_queue("r", "main")
        .await
        .unwrap();
    let attempt = db
        .admit_integration_attempt(admission(&q, "a", "unobserved-workspace"))
        .await
        .unwrap();
    assert!(attempt.workspace_ref.is_none());
    db.enqueue_step(&crate::EnqueueTaskStep {
        id: "nullable-step".into(),
        task_id: "a".into(),
        kind: "hooks".into(),
        payload_json: "{}".into(),
        causation_step_id: None,
        causation_key: "nullable-step".into(),
        chain_id: "nullable-chain".into(),
        chain_position: 1,
        expected_status: "merging".into(),
        expected_version: 1,
        expected_epoch: Some(0),
        lane: "long".into(),
        available_at: now,
    })
    .await
    .unwrap();
    let step = db
        .claim_step("owner", Some("a"), "2099-01-01T00:00:00Z")
        .await
        .unwrap()
        .unwrap();
    crate::task_writer::in_task_step(step, async {
            let witness = serde_json::json!({"workspace":{"workspace_id":"w-a","placement_id":"pl","generation":1,"owner":{"kind":"server"},"handle":path},"target_branch":"main","expected_head_sha":"head","expected_target_sha":"target"});
            let request = db.task_step_integration_request("w-a", IntegrationOperationKind::Merge, witness).await.unwrap().unwrap();
            let IntegrationEffectAdmission::Started(mut guard) = db.begin_integration_effect(request).await.unwrap() else { panic!("not admitted"); };
            assert!(guard.start().await.unwrap().is_none());
            assert!(guard.server_workspace_paths().await.unwrap().is_some());
            guard.record(serde_json::json!({"kind":"not_performed"}), IntegrationOperationState::Failed).await.unwrap();
        }).await;
    assert!(db
        .integration_queue(&q.id)
        .await
        .unwrap()
        .unwrap()
        .head_attempt_id
        .is_none());
}

/// One Task-step fixture: a ready server placement, a shadow attempt and
/// a claimable merging step for `task`.
async fn task_step_fixture(db: &SqliteDb, task: &str) {
    use crate::TaskStepRepo;
    crate::integration_queue::tests::seed_delivery(db, task).await;
    let now = now_rfc3339();
    sqlx::query("INSERT INTO workspace_placement(id,workspace_id,task_id,owner_kind,repo_location_id,workspace_handle,generation,state,selected_by,selection_reason,created_at,updated_at) VALUES(?,?,?,'server','l',?,1,'ready','scheduler','{}',?,?)").bind(format!("pl-{task}")).bind(format!("w-{task}")).bind(task).bind(format!("tree-{task}")).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    let q = db
        .create_or_get_integration_queue("r", "main")
        .await
        .unwrap();
    db.admit_integration_attempt(admission(&q, task, &format!("shadow-{task}")))
        .await
        .unwrap();
    db.enqueue_step(&crate::EnqueueTaskStep {
        id: format!("step-{task}"),
        task_id: task.into(),
        kind: "hooks".into(),
        payload_json: "{}".into(),
        causation_step_id: None,
        causation_key: format!("step-{task}"),
        chain_id: format!("chain-{task}"),
        chain_position: 1,
        expected_status: "merging".into(),
        expected_version: 1,
        expected_epoch: Some(0),
        lane: "long".into(),
        available_at: now,
    })
    .await
    .unwrap();
}
fn task_step_witness(task: &str, head: &str) -> Value {
    serde_json::json!({"workspace":{"workspace_id":format!("w-{task}"),"placement_id":format!("pl-{task}"),"generation":1,"owner":{"kind":"server"},"handle":format!("tree-{task}")},"target_branch":"main","expected_head_sha":head,"expected_target_sha":"target"})
}
async fn claim(db: &SqliteDb, task: &str) -> crate::TaskStep {
    use crate::TaskStepRepo;
    sqlx::query("UPDATE task_step SET lease_until='2000-01-01T00:00:00Z' WHERE task_id=? AND status='claimed'")
            .bind(task)
            .execute(db.pool())
            .await
            .unwrap();
    db.claim_step("owner", Some(task), "2099-01-01T00:00:00Z")
        .await
        .unwrap()
        .unwrap()
}

/// The wedge this guards: a Task-step merge that started and never wrote a
/// receipt (crash, dropped future, failed receipt write) used to leave an
/// intent that refused the Task's next claim and every other Task's merge
/// on the same checkout with `ReconciliationRequired`, forever.
#[tokio::test]
async fn orphaned_task_step_intent_never_refuses_a_later_claim_or_another_task() {
    let db = fixture().await;
    task_step_fixture(&db, "a").await;
    task_step_fixture(&db, "b").await;
    let first = claim(&db, "a").await;
    let orphan = crate::task_writer::in_task_step(first, async {
        let request = db
            .task_step_integration_request(
                "w-a",
                IntegrationOperationKind::Merge,
                task_step_witness("a", "head-1"),
            )
            .await
            .unwrap()
            .unwrap();
        let IntegrationEffectAdmission::Started(mut guard) =
            db.begin_integration_effect(request.clone()).await.unwrap()
        else {
            panic!("first claim not admitted");
        };
        assert!(guard.start().await.unwrap().is_none());
        // The step dies between the started intent and its receipt.
        drop(guard);
        request
    })
    .await;

    // Another Task merges into the same checkout: admitted, not refused.
    let other = claim(&db, "b").await;
    crate::task_writer::in_task_step(other, async {
        let request = db
            .task_step_integration_request(
                "w-b",
                IntegrationOperationKind::Merge,
                task_step_witness("b", "head-b"),
            )
            .await
            .unwrap()
            .unwrap();
        let IntegrationEffectAdmission::Started(mut guard) =
            db.begin_integration_effect(request).await.unwrap()
        else {
            panic!("another Task's merge was refused by an orphaned intent");
        };
        assert!(guard.start().await.unwrap().is_none());
        guard
            .record(
                serde_json::json!({"kind":"completed"}),
                IntegrationOperationState::Succeeded,
            )
            .await
            .unwrap();
    })
    .await;
    let settled = db
        .integration_effect_receipt(&orphan)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(settled.operation_state, IntegrationOperationState::Failed);
    assert_eq!(settled.result["kind"], "infrastructure");
    let attempt = db
        .integration_attempt(&orphan.fence.attempt_id)
        .await
        .unwrap()
        .unwrap();
    assert!(attempt.effect_intent_json.is_none());

    // The same Task's next claim (a new step attempt) is admitted too,
    // both after a settled orphan and directly over a fresh one.
    for head in ["head-2", "head-3"] {
        let next = claim(&db, "a").await;
        crate::task_writer::in_task_step(next, async {
            let request = db
                .task_step_integration_request(
                    "w-a",
                    IntegrationOperationKind::Merge,
                    task_step_witness("a", head),
                )
                .await
                .unwrap()
                .unwrap();
            let IntegrationEffectAdmission::Started(mut guard) =
                db.begin_integration_effect(request).await.unwrap()
            else {
                panic!("the Task's next claim was refused by its own orphaned intent");
            };
            assert!(guard.start().await.unwrap().is_none());
        })
        .await;
    }
}

/// Two Tasks on one checkout, one merging and one rebasing after a target
/// move, is the normal contended case. A rebase locks only its workspace,
/// so another workspace's in-flight merge must not refuse it.
#[tokio::test]
async fn in_flight_merge_does_not_refuse_another_workspaces_rebase() {
    let db = fixture().await;
    task_step_fixture(&db, "a").await;
    task_step_fixture(&db, "b").await;
    let merging = claim(&db, "a").await;
    let held = crate::task_writer::in_task_step(merging, async {
        let request = db
            .task_step_integration_request(
                "w-a",
                IntegrationOperationKind::Merge,
                task_step_witness("a", "head"),
            )
            .await
            .unwrap()
            .unwrap();
        let IntegrationEffectAdmission::Started(mut guard) =
            db.begin_integration_effect(request).await.unwrap()
        else {
            panic!("merge not admitted");
        };
        assert!(guard.start().await.unwrap().is_none());
        guard
    })
    .await;
    let rebasing = claim(&db, "b").await;
    crate::task_writer::in_task_step(rebasing, async {
        let request = db
            .task_step_integration_request(
                "w-b",
                IntegrationOperationKind::Rebase,
                task_step_witness("b", "head-b"),
            )
            .await
            .unwrap()
            .unwrap();
        let admission = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            db.begin_integration_effect(request),
        )
        .await
        .expect("a rebase waited on another workspace's merge")
        .unwrap();
        assert!(matches!(admission, IntegrationEffectAdmission::Started(_)));
    })
    .await;
    // The in-flight merge was neither settled nor disturbed.
    held.record(
        serde_json::json!({"kind":"completed"}),
        IntegrationOperationState::Succeeded,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn intent_without_effect_settles_not_performed() {
    let (db, f) = claimed().await;
    let r = request(f);
    let IntegrationEffectAdmission::Started(g) =
        db.begin_integration_effect(r.clone()).await.unwrap()
    else {
        panic!("not admitted")
    };
    drop(g);
    let IntegrationEffectAdmission::Replay(receipt) =
        db.begin_integration_effect(r.clone()).await.unwrap()
    else {
        panic!("not settled")
    };
    assert_eq!(receipt.result["kind"], "not_performed");
    assert_eq!(receipt.operation_state, IntegrationOperationState::Failed);
    assert!(db
        .integration_attempt(&r.fence.attempt_id)
        .await
        .unwrap()
        .unwrap()
        .effect_intent_json
        .is_none());
}

#[tokio::test]
async fn refusal_after_intent_commits_settles_not_performed() {
    let (db, f) = claimed().await;
    let r = request(f);
    let IntegrationEffectAdmission::Started(mut g) =
        db.begin_integration_effect(r.clone()).await.unwrap()
    else {
        panic!("not admitted")
    };
    sqlx::query("UPDATE integration_queue SET lease_until='2000-01-01T00:00:00Z' WHERE id=?")
        .bind(&r.fence.queue_id)
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(
        g.start().await.unwrap(),
        Some(IntegrationEffectRefusal::StaleFence)
    );
    drop(g);
    let receipt = db.integration_effect_receipt(&r).await.unwrap().unwrap();
    assert_eq!(receipt.result["kind"], "not_performed");
    assert!(db
        .integration_attempt(&r.fence.attempt_id)
        .await
        .unwrap()
        .unwrap()
        .effect_intent_json
        .is_none());
}

#[tokio::test]
async fn effect_guard_does_not_hold_sqlite_writer() {
    let (db, f) = claimed().await;
    let IntegrationEffectAdmission::Started(mut g) =
        db.begin_integration_effect(request(f)).await.unwrap()
    else {
        panic!("not admitted")
    };
    assert!(g.start().await.unwrap().is_none());
    tokio::time::timeout(std::time::Duration::from_millis(250), async {
        let mut tx = begin_immediate(db.pool()).await.unwrap();
        sqlx::query("UPDATE repo SET name='second writer' WHERE id='r'")
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    })
    .await
    .expect("effect retained SQLite's writer");
    g.record(
        serde_json::json!({"kind":"rebased"}),
        IntegrationOperationState::Succeeded,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn deleting_the_repo_removes_an_attempt_that_holds_a_fence_and_a_receipt() {
    let (db, f) = claimed().await;
    let IntegrationEffectAdmission::Started(g) = db
        .begin_integration_effect(request(f.clone()))
        .await
        .unwrap()
    else {
        panic!("not admitted")
    };
    g.record(
        serde_json::json!({"kind":"rebased"}),
        IntegrationOperationState::Succeeded,
    )
    .await
    .unwrap();
    crate::RepoRepo::delete(&db, "r").await.unwrap();
    for table in ["integration_queue", "integration_attempt"] {
        let rows: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(rows, 0, "{table}");
    }
}
#[tokio::test]
async fn a_refused_claim_keeps_a_quarantined_queue_quarantined() {
    let (db, f) = claimed().await;
    sqlx::query(
            "UPDATE integration_queue SET state='quarantined',lease_until='2026-10-08T00:00:30Z' WHERE id=?",
        )
        .bind(&f.queue_id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE repo_location SET status='unavailable' WHERE id='l'")
        .execute(db.pool())
        .await
        .unwrap();
    let q = db.integration_queue(&f.queue_id).await.unwrap().unwrap();
    assert!(matches!(
        db.claim_integration_queue(
            &q.id,
            q.revision,
            "next",
            "2026-10-09T00:00:00Z",
            "2026-10-09T00:01:00Z",
        )
        .await,
        Err(DbError::VersionConflict)
    ));
    let refused = db.integration_queue(&f.queue_id).await.unwrap().unwrap();
    assert_eq!(refused.state, IntegrationQueueState::Quarantined);
    assert_eq!(
        refused.last_error_kind,
        Some(IntegrationFailureKind::TargetUnavailable)
    );
    assert_eq!(refused.fence_generation, q.fence_generation);
}
/// Deleting the target location while an effect is in flight is refused
/// (`integration_unresolved`): the effect, the attempt and the queue are
/// left as they are, and the owner's receipt still settles the effect.
/// (Before the guard the deletion settled the effect as a permanent
/// loss; a landed merge could be recorded as failed that way. Removing
/// the machine is the operation that settles an effect nobody can
/// answer for: `machine_removal_settles_attempt_and_preserves_quarantine`.)
#[tokio::test]
async fn location_removal_is_refused_while_an_effect_is_in_flight() {
    for quarantined in [false, true] {
        let (db, f) = claimed().await;
        let request = request(f.clone());
        let IntegrationEffectAdmission::Started(mut guard) =
            db.begin_integration_effect(request.clone()).await.unwrap()
        else {
            panic!("not admitted");
        };
        assert!(guard.start().await.unwrap().is_none());
        if quarantined {
            sqlx::query("UPDATE integration_queue SET state='quarantined' WHERE id=?")
                .bind(&f.queue_id)
                .execute(db.pool())
                .await
                .unwrap();
        }
        let refused = crate::RepoLocationRepo::delete(&db, "l").await;
        assert!(
            matches!(
                &refused,
                Err(crate::DbError::IntegrationUnresolved { task_id, .. }) if task_id == "a"
            ),
            "{refused:?}"
        );
        assert!(db
            .integration_effect_receipt(&request)
            .await
            .unwrap()
            .is_none());
        let attempt = db
            .integration_attempt(&f.attempt_id)
            .await
            .unwrap()
            .unwrap();
        assert!(attempt.effect_intent_json.is_some(), "the intent stands");
        let queue = db.integration_queue(&f.queue_id).await.unwrap().unwrap();
        assert!(queue.target_location_id.is_some(), "the target stands");
        // The owner's receipt settles the effect; then the deletion works.
        guard
            .record(
                serde_json::json!({"kind":"rebased"}),
                IntegrationOperationState::Succeeded,
            )
            .await
            .unwrap();
    }
}

/// A rebase performed off the target's owner (the Task's checkout is on
/// a daemon, the default checkout on the server) belongs to that daemon:
/// it is listed as that daemon's outstanding effect, not the server's,
/// and removing the daemon settles it. The queue's target is not the
/// removed machine, so the queue is not suspended.
#[tokio::test]
async fn an_effect_is_listed_and_settled_by_the_owner_that_performs_it() {
    let db = fixture().await;
    let now = now_rfc3339();
    sqlx::query("INSERT INTO daemon (id,machine_id,hostname,os,arch,status,created_at,updated_at) VALUES ('d','remote-machine','host','linux','aarch64','offline',?,?)").bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO runtime(id,daemon_id,kind,workspace_root,status,labels_json,created_at,updated_at) VALUES ('rt','d','codex','root','offline','{}',?,?)").bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    let q = db
        .create_or_get_integration_queue("r", "main")
        .await
        .unwrap();
    // Admission records the owner of the Task's checkout on the attempt.
    let mut admitted = admission(&q, "a", "task-owner");
    admitted.owner_kind = Some(IntegrationOwnerKind::Daemon);
    admitted.daemon_id = Some("d".into());
    admitted.runtime_id = Some("rt".into());
    let a = db.admit_integration_attempt(admitted).await.unwrap();
    // A second member waits behind the head.
    let b = db
        .admit_integration_attempt(admission(&q, "b", "second"))
        .await
        .unwrap();
    let q = db.integration_queue(&q.id).await.unwrap().unwrap();
    db.claim_integration_queue(&q.id, q.revision, "worker", &now, "2099-01-01T00:00:00Z")
        .await
        .unwrap();
    let fence = db.integration_owner_fence(&a.id).await.unwrap().unwrap();
    assert_eq!(fence.target_owner["owner_kind"], "server");
    let request = IntegrationEffectRequest {
        fence,
        kind: IntegrationOperationKind::Rebase,
        witness: serde_json::json!({"workspace":{"handle":"task-tree"},"effect_owner":{"location_id":"task-location","owner_kind":"daemon","daemon_id":"d","runtime_id":"rt"}}),
    };
    assert_eq!(request.effect_owner()["daemon_id"], "d");
    // The witness alone names no owner: one the attempt does not record
    // (another daemon, another runtime of the daemon, the target's own
    // owner), one on another effect than the rebase, and a rebase that
    // names none although the Task's checkout is elsewhere are refused
    // before any intent exists.
    let forged = |owner: Value, kind: IntegrationOperationKind| {
        let mut witness = serde_json::json!({"workspace":{"handle":"task-tree"}});
        if !owner.is_null() {
            witness["effect_owner"] = owner;
        }
        IntegrationEffectRequest {
            fence: request.fence.clone(),
            kind,
            witness,
        }
    };
    for (owner, kind) in [
        (
            serde_json::json!({"location_id":"task-location","owner_kind":"daemon","daemon_id":"other","runtime_id":"rt"}),
            IntegrationOperationKind::Rebase,
        ),
        (
            serde_json::json!({"location_id":"task-location","owner_kind":"daemon","daemon_id":"d","runtime_id":"other"}),
            IntegrationOperationKind::Rebase,
        ),
        (
            serde_json::json!({"location_id":"task-location","owner_kind":"server","daemon_id":null,"runtime_id":null}),
            IntegrationOperationKind::Rebase,
        ),
        (
            serde_json::json!({"location_id":"task-location","owner_kind":"daemon","daemon_id":"d","runtime_id":"rt"}),
            IntegrationOperationKind::FastForward,
        ),
        (serde_json::json!("d"), IntegrationOperationKind::Rebase),
        (Value::Null, IntegrationOperationKind::Rebase),
    ] {
        assert!(
            matches!(
                db.begin_integration_effect(forged(owner.clone(), kind))
                    .await
                    .unwrap(),
                IntegrationEffectAdmission::Refused(IntegrationEffectRefusal::ForeignOwner)
            ),
            "{owner} {kind:?}"
        );
    }
    let untouched = db.integration_attempt(&a.id).await.unwrap().unwrap();
    assert!(untouched.effect_intent_json.is_none());
    assert_eq!(untouched.effect_receipts_json, serde_json::json!([]));
    let IntegrationEffectAdmission::Started(mut guard) =
        db.begin_integration_effect(request.clone()).await.unwrap()
    else {
        panic!("not admitted");
    };
    assert!(guard.start().await.unwrap().is_none());
    // Asked of the daemon that performs it, never of the target's owner.
    assert_eq!(
        db.outstanding_integration_effects("daemon", Some("d"))
            .await
            .unwrap(),
        vec![request.clone()]
    );
    assert!(db
        .outstanding_integration_effects("daemon", Some("other"))
        .await
        .unwrap()
        .is_empty());
    assert!(db
        .outstanding_integration_effects("server", None)
        .await
        .unwrap()
        .is_empty());
    // An effect without `effect_owner` belongs to the target's owner.
    let plain = IntegrationEffectRequest {
        witness: serde_json::json!({"workspace":{"handle":"task-tree"}}),
        ..request.clone()
    };
    assert_eq!(plain.effect_owner(), &plain.fence.target_owner);

    db.remove_daemon("d", "admin", true, "local-host", false)
        .await
        .unwrap();
    let settled = db.integration_attempt(&a.id).await.unwrap().unwrap();
    assert!(settled.effect_intent_json.is_none());
    assert_eq!(settled.state, IntegrationAttemptState::Parked);
    assert_eq!(
        settled.failure_kind,
        Some(IntegrationFailureKind::Infrastructure)
    );
    let receipt = db
        .integration_effect_receipt(&request)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.result["kind"], "infrastructure");
    assert_eq!(receipt.operation_state, IntegrationOperationState::Failed);
    // The default checkout is still there: the queue stays open.
    let q = db.integration_queue(&q.id).await.unwrap().unwrap();
    assert_eq!(q.state, IntegrationQueueState::Open);
    drop(guard);
    // The parked attempt holds no slot: the member behind it is claimed
    // next, the queue does not wait for the removed machine's Task.
    assert_eq!(q.head_attempt_id, None);
    assert_eq!(q.lease_owner, None);
    let claimed = db
        .claim_integration_queue(&q.id, q.revision, "worker", &now, "2099-01-01T00:00:00Z")
        .await
        .unwrap();
    assert_eq!(claimed.head_attempt_id.as_deref(), Some(b.id.as_str()));
}

/// The owner lock follows the owner that performs the effect, and a
/// rebase locks only the Task's own worktree there. Two queues whose
/// heads are placed on one daemon rebase at the same time: neither waits
/// for the other, and the default checkout's owner is not locked at all.
/// The same worktree is still one effect at a time.
#[tokio::test]
async fn two_queues_rebasing_on_one_task_owner_do_not_wait_for_each_other() {
    let db = fixture().await;
    let now = now_rfc3339();
    let mut guards = Vec::new();
    let mut fences = Vec::new();
    for (branch, task, handle) in [("main", "a", "tree-a"), ("release", "b", "tree-b")] {
        let q = db
            .create_or_get_integration_queue("r", branch)
            .await
            .unwrap();
        let mut admitted = admission(&q, task, branch);
        admitted.owner_kind = Some(IntegrationOwnerKind::Daemon);
        admitted.daemon_id = Some("d".into());
        admitted.runtime_id = Some("rt".into());
        let a = db.admit_integration_attempt(admitted).await.unwrap();
        let q = db.integration_queue(&q.id).await.unwrap().unwrap();
        db.claim_integration_queue(&q.id, q.revision, "worker", &now, "2099-01-01T00:00:00Z")
            .await
            .unwrap();
        let fence = db.integration_owner_fence(&a.id).await.unwrap().unwrap();
        fences.push(fence.clone());
        let request = IntegrationEffectRequest {
            fence,
            kind: IntegrationOperationKind::Rebase,
            witness: serde_json::json!({"workspace":{"handle":handle},"effect_owner":{"location_id":"task-location","owner_kind":"daemon","daemon_id":"d","runtime_id":"rt"}}),
        };
        // The first queue's rebase is still in flight (its guard is held).
        let admitted = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            db.begin_integration_effect(request),
        )
        .await
        .expect("a rebase does not wait behind another queue's rebase")
        .unwrap();
        let IntegrationEffectAdmission::Started(mut guard) = admitted else {
            panic!("not admitted");
        };
        assert!(guard.start().await.unwrap().is_none());
        guards.push(guard);
    }
    // Both are listed for the daemon that performs them.
    assert_eq!(
        db.outstanding_integration_effects("daemon", Some("d"))
            .await
            .unwrap()
            .len(),
        2
    );
    // The server's default checkout is free meanwhile: its lock is not
    // one of the two held (taking it does not wait).
    let server = IntegrationEffectRequest {
        fence: fences[0].clone(),
        kind: IntegrationOperationKind::FastForward,
        witness: serde_json::json!({}),
    };
    let free = tokio::time::timeout(std::time::Duration::from_secs(5), lock_owner(&server))
        .await
        .expect("the target owner's checkout lock is not held by a rebase elsewhere");
    drop(free);
    // The same worktree on that daemon is one effect at a time.
    let same = IntegrationEffectRequest {
        fence: fences[1].clone(),
        kind: IntegrationOperationKind::Rebase,
        witness: serde_json::json!({"workspace":{"handle":"tree-a"},"effect_owner":{"location_id":"task-location","owner_kind":"daemon","daemon_id":"d","runtime_id":"rt"}}),
    };
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(200), lock_owner(&same))
            .await
            .is_err(),
        "one worktree, one effect"
    );
    drop(guards);
}

#[tokio::test]
async fn machine_removal_settles_attempt_and_preserves_quarantine() {
    for quarantined in [false, true] {
        let db = fixture().await;
        let now = now_rfc3339();
        sqlx::query("INSERT INTO daemon (id,machine_id,hostname,os,arch,status,created_at,updated_at) VALUES ('d','remote-machine','host','linux','aarch64','offline',?,?)").bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO runtime(id,daemon_id,kind,workspace_root,status,labels_json,created_at,updated_at) VALUES ('rt','d','codex','root','offline','{}',?,?)").bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        sqlx::query("UPDATE repo_location SET owner_kind='daemon',daemon_id='d',runtime_id='rt' WHERE id='l'").execute(db.pool()).await.unwrap();
        let q = db
            .create_or_get_integration_queue("r", "main")
            .await
            .unwrap();
        let a = db
            .admit_integration_attempt(admission(&q, "a", "removed-machine"))
            .await
            .unwrap();
        let q = db.integration_queue(&q.id).await.unwrap().unwrap();
        db.claim_integration_queue(&q.id, q.revision, "worker", &now, "2099-01-01T00:00:00Z")
            .await
            .unwrap();
        let fence = db.integration_owner_fence(&a.id).await.unwrap().unwrap();
        let request = request(fence.clone());
        let IntegrationEffectAdmission::Started(mut guard) =
            db.begin_integration_effect(request.clone()).await.unwrap()
        else {
            panic!("not admitted");
        };
        assert!(guard.start().await.unwrap().is_none());
        if quarantined {
            sqlx::query("UPDATE integration_queue SET state='quarantined' WHERE id=?")
                .bind(&q.id)
                .execute(db.pool())
                .await
                .unwrap();
        }
        db.remove_daemon("d", "admin", true, "local-host", false)
            .await
            .unwrap();
        let settled = db.integration_attempt(&a.id).await.unwrap().unwrap();
        assert!(settled.effect_intent_json.is_none());
        assert_eq!(
            settled.failure_kind,
            Some(IntegrationFailureKind::Infrastructure)
        );
        assert_eq!(
            settled.current_operation_state,
            Some(IntegrationOperationState::Failed)
        );
        let receipt = db
            .integration_effect_receipt(&request)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(receipt.result["kind"], "infrastructure");
        let q = db.integration_queue(&q.id).await.unwrap().unwrap();
        assert_eq!(
            q.state,
            if quarantined {
                IntegrationQueueState::Quarantined
            } else {
                IntegrationQueueState::Suspended
            }
        );
        assert!(q.lease_owner.is_none());
        // A parked attempt gives up the slot with the removal; a
        // quarantined one keeps it, its queue re-opens through it.
        assert_eq!(
            settled.state,
            if quarantined {
                IntegrationAttemptState::Quarantined
            } else {
                IntegrationAttemptState::Parked
            }
        );
        assert_eq!(
            q.head_attempt_id,
            quarantined.then(|| a.id.clone()),
            "quarantined={quarantined}"
        );
        drop(guard);
    }
}

#[tokio::test]
async fn uncertain_receipt_reconciliation_keeps_one_attempt_key() {
    let (db, f) = claimed().await;
    let request = request(f);
    let IntegrationEffectAdmission::Started(mut guard) =
        db.begin_integration_effect(request.clone()).await.unwrap()
    else {
        panic!("not admitted");
    };
    assert!(guard.start().await.unwrap().is_none());
    guard
        .record(
            serde_json::json!({"kind":"infrastructure"}),
            IntegrationOperationState::Uncertain,
        )
        .await
        .unwrap();
    let reconciliation = db
        .lock_integration_reconciliation(&request)
        .await
        .unwrap()
        .unwrap();
    reconciliation
        .record(
            serde_json::json!({"kind":"completed"}),
            IntegrationOperationState::Succeeded,
        )
        .await
        .unwrap();
    let count: i64 = sqlx::query_scalar(
        "SELECT json_array_length(effect_receipts_json) FROM integration_attempt WHERE id=?",
    )
    .bind(&request.fence.attempt_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(count, 1);
    assert!(db
        .integration_attempt(&request.fence.attempt_id)
        .await
        .unwrap()
        .unwrap()
        .effect_intent_json
        .is_none());
}

#[tokio::test]
async fn fencing_json_bounds_and_effect_enum_sql_parity() {
    let (db, f) = claimed().await;
    let too_large = "x".repeat(1048577);
    for column in [
        "owner_fence_json",
        "effect_intent_json",
        "effect_receipts_json",
    ] {
        let json = if column == "effect_receipts_json" {
            serde_json::json!([too_large]).to_string()
        } else {
            serde_json::json!({"data":too_large}).to_string()
        };
        assert!(sqlx::query(&format!(
            "UPDATE integration_attempt SET {column}=? WHERE id=?"
        ))
        .bind(json)
        .bind(&f.attempt_id)
        .execute(db.pool())
        .await
        .is_err());
    }
    let sql = include_str!("../../../migrations/V202610080123__integration_queue.sql");
    let list = sql
        .split("operation_kind IN (")
        .nth(1)
        .unwrap()
        .split(')')
        .next()
        .unwrap();
    let mut sql_values: Vec<_> = list
        .split(',')
        .map(|s| s.trim().trim_matches('\'').to_owned())
        .collect();
    sql_values.sort();
    let mut rust_values: Vec<_> = IntegrationOperationKind::ALL
        .iter()
        .map(ToString::to_string)
        .collect();
    rust_values.sort();
    assert_eq!(sql_values, rust_values);
}
