use super::*;
use crate::check_runner::{
    consumer::{CheckApplyOutcome, TaskCheckConsumers, TaskCheckRequest},
    CheckRunner,
};
use db::{
    CheckAdmission, CheckConsumerOrigin, CheckProgressDelivery, CheckRunRepo, CheckWorkerRepo,
};
use serde_json::json;

struct Root {
    db: Arc<SqliteDb>,
    service: crate::TaskService,
    attempt: IntegrationAttempt,
}

impl Root {
    async fn new() -> Self {
        let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let db = Arc::new(SqliteDb::new(pool));
        let now = db::now_rfc3339();
        sqlx::query("INSERT INTO project(id,name,workflow_definition,created_at,updated_at) VALUES('p','project','{}',?,?)")
            .bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO repo(id,project_id,name,default_branch,created_at,updated_at) VALUES('r','p','repo','main',?,?)")
            .bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES('root','p','root','merging',?,?)")
            .bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        let queue = db
            .create_or_get_integration_queue("r", "main")
            .await
            .unwrap();
        let mut attempt = IntegrationAttempt::new(
            Some(queue.id),
            "root".into(),
            "p".into(),
            "admit-root".into(),
            "merging".into(),
            0,
            1,
        );
        attempt.state = S::Checking;
        attempt.original_candidate_sha = Some("a".repeat(40));
        attempt.candidate_sha = attempt.original_candidate_sha.clone();
        let attempt = db.admit_integration_attempt(attempt).await.unwrap();
        let service = crate::TaskService::new(db.clone(), Arc::new(events::EventBus::default()));
        let root = Self {
            db,
            service,
            attempt,
        };
        let mut tx = root.db.pool().begin().await.unwrap();
        root.db.sync_condition_in_tx(&mut tx, "root").await.unwrap();
        tx.commit().await.unwrap();
        root
    }

    async fn task(&self) -> db::Task {
        TaskRepo::get_by_id(&*self.db, "root", false)
            .await
            .unwrap()
            .unwrap()
    }

    async fn coordinate(&self, block: Option<&str>, visible_children: bool) -> db::Task {
        let now = db::now_rfc3339();
        let mut tx = self.db.pool().begin().await.unwrap();
        if visible_children {
            sqlx::query("INSERT INTO task(id,project_id,parent_task_id,title,status,created_at,updated_at) VALUES('child','p','root','child','todo',?,?)")
                .bind(&now).bind(&now).execute(&mut *tx).await.unwrap();
        }
        let annotation =
            block.map(|kind| json!({"type":kind,"blocking_reason":"blocked root"}).to_string());
        sqlx::query("UPDATE task SET error_annotation=?,metadata_json='{\"coordination_review_pending\":true}' WHERE id='root'")
            .bind(annotation).execute(&mut *tx).await.unwrap();
        self.db.sync_condition_in_tx(&mut tx, "root").await.unwrap();
        tx.commit().await.unwrap();
        self.task().await
    }

    async fn claim(&self) -> TaskStep {
        let task = self.task().await;
        let id = db::new_uuid_v4();
        self.db
            .enqueue_step(&db::EnqueueTaskStep {
                id: id.clone(),
                task_id: task.id,
                kind: "command".into(),
                payload_json: "{}".into(),
                causation_step_id: None,
                causation_key: id.clone(),
                chain_id: id,
                chain_position: 1,
                expected_status: task.status,
                expected_version: task.version,
                expected_epoch: Some(0),
                lane: "fast".into(),
                available_at: db::now_rfc3339(),
            })
            .await
            .unwrap();
        self.db
            .claim_step(
                "coordination-tests",
                Some("root"),
                &db::task_writer::lease_deadline(),
            )
            .await
            .unwrap()
            .unwrap()
    }

    async fn assert_legacy_projection(
        &self,
        before: &db::Task,
        block: Option<&str>,
        visible_children: bool,
    ) {
        let after = self.task().await;
        assert_eq!(after.version, before.version);
        assert_eq!(
            db::LegacyConditionInput::from(&after),
            db::LegacyConditionInput::from(before)
        );
        let mut c = self.db.pool().acquire().await.unwrap();
        let facts = db::ConditionFacts::load(&mut c, "root").await.unwrap();
        drop(c);
        let mut input = db::LegacyConditionInput::from(&after);
        input.facts = Some(facts);
        let expected = db::map_legacy_condition(&input);
        assert_eq!(after.condition.typed(), expected.typed());
        if visible_children {
            assert_eq!(
                after
                    .condition
                    .reasons()
                    .filter(|reason| matches!(reason, db::ParkReason::Children { .. }))
                    .collect::<Vec<_>>(),
                vec![&db::ParkReason::Children {
                    root_id: "root".into(),
                    remaining: vec!["child".into()],
                }]
            );
            let primary = after.condition.reasons().next().unwrap();
            match block {
                Some("manual_stop") => assert!(matches!(primary, db::ParkReason::Held { .. })),
                Some("workspace_error") => {
                    assert!(matches!(primary, db::ParkReason::Failure { .. }))
                }
                None => assert!(matches!(primary, db::ParkReason::Children { .. })),
                _ => unreachable!(),
            }
        } else {
            let observation = db::ParkReason::UnknownCondition {
                source: db::ConditionSource {
                    field: db::LegacyConditionField::MetadataJson,
                    key: Some("coordination_review_pending".into()),
                },
                problem: db::UnknownConditionProblem::UnownedEntry,
            };
            assert_eq!(
                after
                    .condition
                    .evidence()
                    .observations
                    .iter()
                    .filter(|reason| **reason == observation)
                    .count(),
                1
            );
        }
        assert_eq!(after.condition.public(), expected.public());
        self.db
            .check_task_condition_invariant(&after)
            .await
            .unwrap();
    }
}

async fn integration_restatement(block: Option<&str>, visible_children: bool) {
    let root = Root::new().await;
    let before = root.coordinate(block, visible_children).await;
    assert!(before.condition.integration_reason().is_none());
    assert!(root
        .db
        .integration_reconcile_task_owned("root")
        .await
        .unwrap());
    let step = root.claim().await;
    db::task_writer::in_task_step(step, async {
        assert!(root
            .service
            .reconcile_integration_admission(&before)
            .await
            .unwrap());
        assert!(matches!(root.task().await.condition.integration_reason(),
            Some(IntegrationReason::Owned { attempt_id, phase: IntegrationPhase::Checking })
                if attempt_id.as_str() == root.attempt.id));
        assert!(!root
            .service
            .reconcile_integration_admission(&root.task().await)
            .await
            .unwrap());
    })
    .await;
    root.assert_legacy_projection(&before, block, visible_children)
        .await;
}

async fn check_progress(block: Option<&str>, visible_children: bool) {
    let root = Root::new().await;
    let consumers =
        TaskCheckConsumers::new(root.db.clone(), Arc::new(CheckRunner::new(root.db.clone())));
    IntegrationCheckFamily::register(
        &consumers,
        Arc::new(IntegrationSteps::new(root.service.clone())),
    );
    let step = root.claim().await;
    let request = TaskCheckRequest {
        task_id: "root".into(),
        status_epoch: 0,
        authority: check_authority(&root.attempt).unwrap(),
        origin: CheckConsumerOrigin::Integration,
        purpose: api_types::CheckPurpose::QueueHeadCi,
        identity: db::CheckRunIdentity {
            project_id: "p".into(),
            repo_id: "r".into(),
            commit_sha: root.attempt.candidate_sha.clone().unwrap(),
            inputs: api_types::CheckDigestInput {
                spec: check_executor::legacy_ci_spec("true", &Default::default(), 0, false),
                environment: Default::default(),
                environment_identity: api_types::CheckEnvironmentIdentity::Attested {
                    input_digest: "a".repeat(64),
                },
                execution_revision: api_types::CheckExecutionRevision {
                    number: 0,
                    audit_ref: None,
                },
            },
        },
        workspace_id: None,
        machine_id: None,
        wall_timeout_seconds: 1800,
    };
    let requested = db::task_writer::in_task_step(step.clone(), consumers.request(request))
        .await
        .unwrap();
    sqlx::query("DELETE FROM task_step WHERE id=?")
        .bind(&step.id)
        .execute(root.db.pool())
        .await
        .unwrap();
    // The check was requested before a hold/failure or child-list change.
    // Its progress still belongs to this entry and exact integration candidate.
    let before = root.coordinate(block, visible_children).await;
    let run_id = requested.consumer.run_id.as_deref().unwrap();
    let now = db::now_rfc3339();
    // A queued check that previously waited for capacity is admitted after
    // the root changed. Admission enqueues the real progress command.
    sqlx::query("UPDATE check_run SET capacity_wait_since=? WHERE id=?")
        .bind(&now)
        .bind(run_id)
        .execute(root.db.pool())
        .await
        .unwrap();
    let run = root.db.check_run(run_id).await.unwrap().unwrap();
    assert!(matches!(
        root.db
            .admit_check_run(
                &run,
                "check-worker",
                &now,
                &db::task_writer::lease_deadline()
            )
            .await
            .unwrap(),
        CheckAdmission::Admitted(_)
    ));
    let step = root
        .db
        .claim_step(
            "coordination-tests",
            Some("root"),
            &db::task_writer::lease_deadline(),
        )
        .await
        .unwrap()
        .unwrap();
    let payload: serde_json::Value = serde_json::from_str(&step.payload_json).unwrap();
    assert_eq!(payload["operation"], "apply_check_progress");
    let note: CheckProgressDelivery = serde_json::from_value(payload["arguments"].clone()).unwrap();
    db::task_writer::in_task_step(step, async {
        for _ in 0..2 {
            assert_eq!(
                consumers.progress(&note).await.unwrap(),
                CheckApplyOutcome::Applied
            );
        }
    })
    .await;
    let task = root.task().await;
    assert_eq!(
        task.condition.check_wait().unwrap().consumer_id,
        requested.consumer.id
    );
    assert_eq!(
        task.condition.check_wait().unwrap().phase,
        api_types::CheckWaitPhase::Result
    );
    root.assert_legacy_projection(&before, block, visible_children)
        .await;
}

#[tokio::test]
async fn integration_restatement_accepts_held_root_with_children() {
    integration_restatement(Some("manual_stop"), true).await;
}

#[tokio::test]
async fn integration_restatement_accepts_failed_root_with_children() {
    integration_restatement(Some("workspace_error"), true).await;
}

#[tokio::test]
async fn integration_restatement_accepts_flagged_root_without_children() {
    integration_restatement(None, false).await;
}

#[tokio::test]
async fn integration_restatement_accepts_unblocked_root_with_children() {
    integration_restatement(None, true).await;
}

#[tokio::test]
async fn check_progress_accepts_held_root_with_children() {
    check_progress(Some("manual_stop"), true).await;
}

#[tokio::test]
async fn check_progress_accepts_failed_root_with_children() {
    check_progress(Some("workspace_error"), true).await;
}

#[tokio::test]
async fn check_progress_accepts_flagged_root_without_children() {
    check_progress(None, false).await;
}

#[tokio::test]
async fn check_progress_accepts_unblocked_root_with_children() {
    check_progress(None, true).await;
}
