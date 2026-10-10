#![allow(dead_code, clippy::assertions_on_constants)]
//! A Task's wait on the durable check runner, end to end: stated by the
//! consumer contract, read through REST, answered by the real Task-step
//! worker's `apply_check_result` arm, and re-asked by the owner's `retry`.
mod common;

use api_types::{
    CheckDigestInput, CheckEnvironmentIdentity, CheckExecutionRevision, CheckPurpose,
    TaskActionsResponse, TaskResponse,
};
use axum::http::{Method, StatusCode};
use db::{
    CheckCleanup, CheckConsumerOrigin, CheckDeliveryRepo, CheckResultEvidence, CheckResultOutcome,
    CheckRunFence, CheckRunIdentity, CheckRunRepo, CheckRunState, CheckWorkerRepo, StoredCheckRun,
    TaskStepRepo,
};
use serde_json::{json, Value};
use services::check_runner::{
    consumer::{CheckApplication, CheckConsumerFamily, CheckVerdict, TaskCheckRequest},
    CheckRequestOutcome,
};
use std::sync::{Arc, Mutex};

struct Family {
    applied: Mutex<Vec<&'static str>>,
}
#[async_trait::async_trait]
impl CheckConsumerFamily for Family {
    async fn current_authority(
        &self,
        _task_id: &str,
        _status_epoch: i64,
    ) -> services::Result<Option<String>> {
        Ok(Some("attempt-1".into()))
    }
    async fn apply(&self, application: &CheckApplication) -> services::Result<()> {
        self.applied
            .lock()
            .unwrap()
            .push(match &application.verdict {
                CheckVerdict::InfrastructureExhausted(_) => "no-verdict",
                CheckVerdict::Result(result) if result.outcome == CheckResultOutcome::Pass => {
                    "pass"
                }
                CheckVerdict::Result(_) => "other",
            });
        Ok(())
    }
}

/// Run `future` as a claimed step of the Task, the way a hook or command
/// that asks for a check runs. The step is dated in the future so the real
/// worker never takes it, and removed afterwards.
async fn in_task_step<T>(
    db: &db::SqliteDb,
    task: &TaskResponse,
    future: impl std::future::Future<Output = T>,
) -> T {
    let id = db::new_uuid_v4();
    db.enqueue_step(&db::EnqueueTaskStep {
        id: id.clone(),
        task_id: task.id.clone(),
        kind: "command".into(),
        payload_json: "{}".into(),
        causation_step_id: None,
        causation_key: id.clone(),
        chain_id: id.clone(),
        chain_position: 1,
        expected_status: task.status.clone(),
        expected_version: task.version,
        expected_epoch: None,
        lane: "fast".into(),
        available_at: (chrono::Utc::now() + chrono::Duration::hours(1)).to_rfc3339(),
    })
    .await
    .unwrap();
    sqlx::query("UPDATE task_step SET status='claimed',claimed_by='check-conditions',lease_until=? WHERE id=?")
        .bind(db::task_writer::lease_deadline())
        .bind(&id)
        .execute(db.pool())
        .await
        .unwrap();
    let step = db
        .task_steps(&task.id)
        .await
        .unwrap()
        .into_iter()
        .find(|step| step.id == id)
        .unwrap();
    let out = db::task_writer::in_task_step(step, future).await;
    sqlx::query("DELETE FROM task_step WHERE id=?")
        .bind(&id)
        .execute(db.pool())
        .await
        .unwrap();
    out
}

/// What a check owner would settle: one result for the run, no process.
async fn settle(db: &db::SqliteDb, run: &StoredCheckRun, outcome: CheckResultOutcome) {
    let now = db::now_rfc3339();
    let until = (chrono::Utc::now() + chrono::Duration::minutes(1)).to_rfc3339();
    let fence = |run: &StoredCheckRun| CheckRunFence {
        run_id: run.id.clone(),
        version: run.version,
        lease_owner: run.lease_owner.clone(),
        lease_generation: run.lease_generation,
    };
    let run = db
        .claim_check_run(&run.id, run.version, "check-conditions", &now, &until)
        .await
        .unwrap();
    let run = db
        .transition_check_run(&fence(&run), CheckRunState::Cleaning, &now)
        .await
        .unwrap();
    db.finish_check_run(
        &fence(&run),
        CheckResultEvidence {
            outcome,
            cleanup: CheckCleanup::NotPerformed,
            commands: if outcome == CheckResultOutcome::Pass {
                vec![api_types::CheckCommandOutcome {
                    index: 0,
                    command: "true".into(),
                    exit_code: 0,
                    stderr_tail: String::new(),
                    output_tail: String::new(),
                    started_at: now.clone(),
                    finished_at: now.clone(),
                }]
            } else {
                vec![]
            },
            output_truncated: false,
            redaction_values: vec![],
            reusable: true,
        },
        &now,
    )
    .await
    .unwrap();
}

async fn task(harness: &common::Harness, id: &str) -> Value {
    common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{id}"),
        StatusCode::OK,
    )
    .await
}
/// The Task's check wait phase as REST reports it, or `None`.
fn phase(task: &Value) -> Option<String> {
    let condition = &task["condition"];
    std::iter::once(&condition["primary"])
        .chain(condition["additional"].as_array().into_iter().flatten())
        .find(|reason| reason["kind"] == "check")
        .map(|reason| reason["wait"]["phase"].as_str().unwrap().to_owned())
}
async fn wait_for_phase(harness: &common::Harness, id: &str, expected: Option<&str>) -> Value {
    for _ in 0..200 {
        let current = task(harness, id).await;
        if phase(&current).as_deref() == expected {
            return current;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!(
        "Task never reached check phase {expected:?}: {}",
        task(harness, id).await["condition"]
    );
}
async fn offers(harness: &common::Harness, id: &str) -> TaskActionsResponse {
    common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{id}/actions"),
        StatusCode::OK,
    )
    .await
}

#[tokio::test]
async fn a_check_wait_is_read_through_rest_applied_by_the_step_worker_and_retried_by_the_owner() {
    let workspace_root = common::TestDir::new("check-conditions");
    let harness = common::test_app(workspace_root.path(), "check-conditions").await;
    let repo_path = common::setup_git_repo(workspace_root.path());
    let (project_id, repo_id) =
        common::create_project_and_repo(&harness.app, "Check Conditions", &repo_path).await;
    let created: TaskResponse = common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/tasks"),
        json!({ "title": "waits on a check" }),
        StatusCode::OK,
    )
    .await;
    let db = harness.state.db.clone();
    // This test is the check owner: it settles every run by hand.
    harness.step_worker.stop_checks().await;
    let consumers = harness
        .state
        .task_service
        .check_consumers()
        .expect("the runtime composes the check consumers");
    let family = Arc::new(Family {
        applied: Mutex::default(),
    });
    consumers.register(CheckConsumerOrigin::Entry, family.clone());

    // A step of the Task asks for a check: the wait is stated with it.
    let request = TaskCheckRequest {
        task_id: created.id.clone(),
        status_epoch: db.live_task_epoch(&created.id).await.unwrap().unwrap(),
        authority: "attempt-1".into(),
        origin: CheckConsumerOrigin::Entry,
        purpose: CheckPurpose::EntryCi,
        identity: CheckRunIdentity {
            project_id: project_id.clone(),
            repo_id,
            commit_sha: "a".repeat(40),
            inputs: CheckDigestInput {
                spec: check_executor::legacy_ci_spec("true", &Default::default(), 0, false),
                environment: Default::default(),
                environment_identity: CheckEnvironmentIdentity::Attested {
                    input_digest: "a".repeat(64),
                },
                execution_revision: CheckExecutionRevision {
                    number: 0,
                    audit_ref: None,
                },
            },
        },
        workspace_id: None,
        machine_id: None,
        wall_timeout_seconds: 1800,
    };
    let asked = in_task_step(&db, &created, consumers.request(request))
        .await
        .unwrap();
    let CheckRequestOutcome::Scheduled(mut run) = asked.outcome else {
        panic!("a first request schedules a run");
    };

    // REST shows the wait as owned work: no failure, no block, no retry.
    let waiting = task(&harness, &created.id).await;
    assert_eq!(waiting["condition"]["kind"], "parked");
    assert_eq!(
        waiting["condition"]["primary"],
        json!({"kind":"check","wait":{"phase":"result","consumer_id":asked.consumer.id,"origin":"entry"}})
    );
    let details = &waiting["condition"]["details"];
    assert_eq!(details["owner"], "check_runner");
    assert_eq!(details["recovery"], "wait_for_check");
    assert_eq!(
        (&details["failed"], &details["blocked"]),
        (&json!(false), &json!(false))
    );
    assert_eq!(
        waiting["version"], created.version,
        "stating a wait bumps no version"
    );
    let offered = offers(&harness, &created.id).await;
    assert!(
        offered
            .available_actions
            .iter()
            .all(|offer| offer.action.verb() == "cancel"),
        "{:?}",
        offered.available_actions
    );

    // The check never produces a verdict: two automatic retries, then the
    // third infrastructure failure is delivered as the consumer's answer.
    for _ in 0..2 {
        settle(&db, &run, CheckResultOutcome::InfrastructureFailed).await;
        assert_eq!(db.enqueue_check_result_steps(10).await.unwrap(), 0);
        run = db
            .retry_infrastructure_check(&run.id, &db::now_rfc3339())
            .await
            .unwrap()
            .expect("an automatic retry");
    }
    settle(&db, &run, CheckResultOutcome::InfrastructureFailed).await;
    assert_eq!(db.enqueue_check_result_steps(10).await.unwrap(), 1);
    // The real step worker runs the `apply_check_result` command.
    let parked = wait_for_phase(&harness, &created.id, Some("infrastructure_exhausted")).await;
    assert_eq!(
        parked["status"], created.status,
        "no verdict moves the Task"
    );
    assert_eq!(parked["condition"]["details"]["owner"], "user");
    assert_eq!(parked["condition"]["details"]["recovery"], "retry_check");
    assert_eq!(parked["condition"]["details"]["failed"], false);
    assert_eq!(*family.applied.lock().unwrap(), vec!["no-verdict"]);
    common::assert_condition_readable(
        &harness.state,
        &harness.app,
        &created.id,
        "exhausted check retries",
    )
    .await;

    // The operator sees the park: it is infrastructure, not the change.
    let exhausted_items = |status: &Value| {
        status["recent_errors"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|item| {
                item["entity_type"] == "task_check_exhausted"
                    && item["entity_id"] == created.id.as_str()
            })
            .count()
    };
    let operations = || async {
        common::empty_request_with_bearer::<Value>(
            &harness.app,
            Method::GET,
            "/api/v1/operations/status",
            &common::admin_jwt(),
            StatusCode::OK,
        )
        .await
    };
    let status = operations().await;
    assert_eq!(exhausted_items(&status), 1, "{status}");
    assert_ne!(status["overall_severity"], "healthy");

    // The owner is offered retry and cancel; retry asks again.
    let offered = offers(&harness, &created.id).await;
    let verbs: Vec<_> = offered
        .available_actions
        .iter()
        .map(|offer| offer.action.verb())
        .collect();
    assert_eq!(verbs, vec!["cancel", "retry"]);
    assert_eq!(
        offered.available_actions[1].reason,
        "check_infrastructure_exhausted"
    );
    let _: Value = common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/actions", created.id),
        json!({ "version": offered.version, "action": {"verb":"retry"} }),
        StatusCode::OK,
    )
    .await;
    let retried = task(&harness, &created.id).await;
    assert_eq!(phase(&retried).as_deref(), Some("result"));
    assert_eq!(retried["condition"]["details"]["owner"], "check_runner");
    assert_eq!(exhausted_items(&operations().await), 0);
    // The same consumer, on a fresh run, with its automatic retries back.
    assert_eq!(
        db.retryable_check_runs(10).await.unwrap(),
        vec![run.id.clone()]
    );
    let run = db
        .retry_infrastructure_check(&run.id, &db::now_rfc3339())
        .await
        .unwrap()
        .expect("the owner's retry schedules a run");
    settle(&db, &run, CheckResultOutcome::Pass).await;
    assert_eq!(db.enqueue_check_result_steps(10).await.unwrap(), 1);
    let done = wait_for_phase(&harness, &created.id, None).await;
    assert_ne!(done["condition"]["kind"], "failed");
    assert_eq!(*family.applied.lock().unwrap(), vec!["no-verdict", "pass"]);
    common::assert_condition_readable(
        &harness.state,
        &harness.app,
        &created.id,
        "the check passed",
    )
    .await;
}
