use super::*;
use crate::task_condition::channels as channel;
use crate::{EnqueueTaskStep, TaskRepo, TaskStepRepo};
use serde_json::Value;

// Main's json!-built placement refusal, copied before any typed writer runs.
const MAIN_PLACEMENT: &str = concat!(
    r#"{"code":"placement_unavailable","machines":["worker"],"message":"upgrade daemon","#,
    r#""missing_capabilities":["workspaces"],"rejected_candidates":[],"#,
    r#""state":"in_progress","type":"dispatch_failed"}"#,
);
const SUPERSEDING_ANNOTATION: &str = concat!(
    r#"{"blocked_at":"at","blocked_by":"system:heartbeat_monitor","#,
    r#""blocking_reason":"owner_workspace_reset_required","message":"reset","#,
    r#""type":"workspace_reset_required"}"#,
);

#[tokio::test]
async fn pre_upgrade_exact_clear_matches_and_preserves_a_superseding_annotation() {
    let (db, _, step, at) = fixture("in_progress", Some(MAIN_PLACEMENT)).await;
    crate::task_writer::in_task_step(step, async {
        let rendered = serde_json::from_str::<Value>(MAIN_PLACEMENT)
            .unwrap()
            .to_string();
        assert_eq!(rendered.as_bytes(), MAIN_PLACEMENT.as_bytes());
        for superseded in [false, true] {
            let annotation = if superseded {
                SUPERSEDING_ANNOTATION
            } else {
                MAIN_PLACEMENT
            };
            sqlx::query("UPDATE task SET error_annotation=?,metadata_json='{}' WHERE id='t'")
                .bind(annotation)
                .execute(db.pool())
                .await
                .unwrap();
            let row = TaskRepo::get_by_id(&db, "t", false).await.unwrap().unwrap();
            assert_eq!(
                db.clear_placement_dispatch_refusal(ClearPlacementDispatchRefusalInput {
                    annotation: rendered.clone(),
                    updated_at: at.clone(),
                    task_id: "t",
                    version: row.version,
                    metadata_json: &row.metadata_json,
                })
                .await
                .unwrap(),
                1
            );
            let after = TaskRepo::get_by_id(&db, "t", false).await.unwrap().unwrap();
            assert_eq!(
                after.error_annotation.as_deref(),
                superseded.then_some(annotation)
            );
        }
    })
    .await;
}

#[tokio::test]
async fn sql_appends_preserve_envelopes_and_machine_byte_fences() {
    let (db, task, step, _) = fixture("review", None).await;
    crate::task_writer::in_task_step(step, async {
        let machine = channel::ConditionMachine::Daemon {
            daemon_id: Some("daemon".into()),
            runtime_id: Some("runtime".into()),
        };
        let machine_json = serde_json::to_string(&machine).unwrap();
        assert_eq!(
            machine_json,
            r#"{"daemon_id":"daemon","owner_kind":"daemon","runtime_id":"runtime"}"#
        );
        let wait = channel::EnvironmentWait {
            checks: vec!["check".into()],
            kind: Some(channel::EnvironmentWaitKind::EnvironmentProbePending),
            last_error: None,
            machine: machine.clone(),
        };
        let metadata = format!(
            concat!(
                r#"{{"deferred_dispatch":{{"not_before":"later","target_state":"review"}},"#,
                r#""environment_wait":{},"caller_extension":[1,null]}}"#
            ),
            serde_json::to_string(&wait).unwrap()
        );
        sqlx::query("UPDATE task SET metadata_json=? WHERE id='t'")
            .bind(&metadata)
            .execute(db.pool())
            .await
            .unwrap();
        assert_eq!(
            db.repository_provision_environment_wait_tasks("p", &machine_json)
                .await
                .unwrap(),
            vec![("t".into(), task.version)]
        );
        let mut tx = db.pool().begin().await.unwrap();
        db.record_environment_probe_error_for_task_waits_in_tx(
            &mut tx,
            "probe failed",
            "p",
            machine_json.clone(),
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        let row = TaskRepo::get_by_id(&db, "t", false).await.unwrap().unwrap();
        let raw = row.metadata_json.unwrap();
        let expected = format!(
            concat!(
                r#"{{"deferred_dispatch":{{"not_before":"later","target_state":"review","#,
                r#""reason":"environment_probe_pending; last failure: probe failed"}},"#,
                r#""environment_wait":{{"checks":["check"],"kind":"environment_probe_pending","#,
                r#""machine":{},"last_error":"probe failed"}},"caller_extension":[1,null]}}"#
            ),
            machine_json
        );
        assert_eq!(raw, expected);
        let value: Value = serde_json::from_str(&raw).unwrap();
        let typed: channel::EnvironmentWait =
            serde_json::from_value(value["environment_wait"].clone()).unwrap();
        assert_eq!(
            typed,
            channel::EnvironmentWait {
                last_error: Some("probe failed".into()),
                ..wait
            }
        );
        let typed: channel::DeferredDispatch =
            serde_json::from_value(value["deferred_dispatch"].clone()).unwrap();
        assert_eq!(
            typed,
            channel::DeferredDispatch {
                kind: None,
                not_before: "later".into(),
                reason: "environment_probe_pending; last failure: probe failed".into(),
                target_state: "review".into(),
            }
        );
        // Provision refresh appends reason to a historical marker that lacked it.
        sqlx::query("UPDATE task SET metadata_json=? WHERE id='t'")
            .bind(&metadata)
            .execute(db.pool())
            .await
            .unwrap();
        let mut tx = db.pool().begin().await.unwrap();
        let applied = db
            .record_repository_provision_task_wait_in_tx(
                &mut tx,
                crate::RepositoryProvisionTaskWaitInput {
                    task_id: "t",
                    reason: "provision failed",
                    version: task.version,
                    machine_json: &machine_json,
                    project_id: "p",
                    daemon_id: &Some("daemon".into()),
                    runtime_id: &Some("runtime".into()),
                    readiness_version: -1,
                },
            )
            .await
            .unwrap();
        assert_eq!(applied.require_applied().unwrap(), 1);
        tx.commit().await.unwrap();
        let row = TaskRepo::get_by_id(&db, "t", false).await.unwrap().unwrap();
        let expected = metadata.replacen(
            r#""target_state":"review"}"#,
            r#""target_state":"review","reason":"provision failed"}"#,
            1,
        );
        assert_eq!(row.metadata_json.as_deref(), Some(expected.as_str()));
        // SQL appends started_at to the end of a review-CI barrier that lacked it.
        let initial = concat!(
            r#"{"blocking_reason":"ci unavailable","interrupted_at":"at","state":"review","#,
            r#""status":"blocked","updated_at":"at"}"#
        );
        sqlx::query("UPDATE task SET entry_barrier_json=? WHERE id='t'")
            .bind(initial)
            .execute(db.pool())
            .await
            .unwrap();
        let mut tx = db.pool().begin().await.unwrap();
        db.restart_owner_reconnect_review_barrier_in_tx(&mut tx, "window", "t")
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let row = TaskRepo::get_by_id(&db, "t", false).await.unwrap().unwrap();
        let raw = row.entry_barrier_json.unwrap();
        assert_eq!(
            raw,
            concat!(
                r#"{"blocking_reason":"ci unavailable","interrupted_at":"at","state":"review","#,
                r#""status":"blocked","updated_at":"at","started_at":"window"}"#
            )
        );
        let typed: channel::EntryBarrier = serde_json::from_str(&raw).unwrap();
        assert_eq!(typed.started_at, Some(Some("window".into())));
    })
    .await;
}

async fn fixture(
    status: &str,
    annotation: Option<&str>,
) -> (SqliteDb, crate::Task, crate::TaskStep, String) {
    let pool = crate::create_sqlite_pool("sqlite::memory:").await.unwrap();
    crate::run_migrations(&pool).await.unwrap();
    let db = SqliteDb::new(pool);
    let at = crate::now_rfc3339();
    sqlx::query(concat!(
        "INSERT INTO project(id,name,settings,workflow_definition,created_at,updated_at) ",
        "VALUES ('p','p','{}','{}',?,?)"
    ))
    .bind(&at)
    .bind(&at)
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query(concat!(
        "INSERT INTO task(id,project_id,title,status,error_annotation,metadata_json,created_at,updated_at) ",
        "VALUES ('t','p','t',?,?,'{}',?,?)"
    ))
    .bind(status)
    .bind(annotation)
    .bind(&at)
    .bind(&at)
    .execute(db.pool())
    .await
    .unwrap();
    let task = TaskRepo::get_by_id(&db, "t", false).await.unwrap().unwrap();
    db.enqueue_step(&EnqueueTaskStep {
        id: "step".into(),
        task_id: task.id.clone(),
        kind: "command".into(),
        payload_json: "{}".into(),
        causation_step_id: None,
        causation_key: "step".into(),
        chain_id: "step".into(),
        chain_position: 1,
        expected_status: task.status.clone(),
        expected_version: task.version,
        expected_epoch: None,
        lane: "fast".into(),
        available_at: at.clone(),
    })
    .await
    .unwrap();
    let step = db
        .claim_step(
            "fence-test",
            Some("t"),
            &crate::task_writer::lease_deadline(),
        )
        .await
        .unwrap()
        .unwrap();
    (db, task, step, at)
}
