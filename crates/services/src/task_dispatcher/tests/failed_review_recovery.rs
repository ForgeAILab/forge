//! Tests for failed review recovery in the task dispatcher.

use super::*;
#[tokio::test]
async fn dispatcher_failed_review_recovers_after_grace_idempotently() {
    let fixture = failed_review_fixture(chrono::Duration::minutes(3), 3).await;
    assert_eq!(fixture.dispatcher.check_once_and_drain().await.unwrap(), 1);
    let current = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.status, "in_progress");
    assert!(current.blocked_json.is_none());
    assert!(current.review_passed_at.is_none());
    let entries = TransitionLogRepo::list_by_task(&*fixture.db, &fixture.task.id)
        .await
        .unwrap();
    assert_eq!(
        crate::task_diagnostics::audit_gate_rejections_since_boundary(&entries, "review"),
        1
    );
    // The remediation transition fences a duplicate delivery of this entry.
    assert!(!fixture
        .dispatcher
        .recover_failed_review(&fixture.task)
        .await
        .unwrap()
        .unwrap_or(false));
    let entries = TransitionLogRepo::list_by_task(&*fixture.db, &fixture.task.id)
        .await
        .unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(
        ReviewRepo::list_by_task(&*fixture.db, &fixture.task.id)
            .await
            .unwrap()[0]
            .status,
        ReviewStatus::Failed
    );
}

#[tokio::test]
async fn dispatcher_failed_review_corrupt_details_routes_once() {
    for details in [
        "{corrupt",
        "true",
        r#"{"conformance":{"status":"unknown"}}"#,
    ] {
        let fixture = failed_review_fixture(chrono::Duration::minutes(3), 3).await;
        sqlx::query("UPDATE review SET step_results_json = ? WHERE task_id = ?")
            .bind(details)
            .bind(&fixture.task.id)
            .execute(fixture.db.pool())
            .await
            .unwrap();
        let review = ReviewRepo::list_by_task(&*fixture.db, &fixture.task.id)
            .await
            .unwrap()
            .remove(0);
        let execution = ExecutionRepo::get_by_id(&*fixture.db, &review.execution_id)
            .await
            .unwrap()
            .unwrap();
        assert!(
            fixture
                .dispatcher
                .task_service
                .reconcile_settled_reviewer_completion(&fixture.task, &execution, &review, true)
                .await
                .is_err(),
            "live completion remains strict"
        );
        assert!(fixture
            .dispatcher
            .recover_failed_review(&fixture.task)
            .await
            .unwrap()
            .unwrap_or(false));
        let current = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.status, "in_progress");
        assert!(!fixture
            .dispatcher
            .recover_failed_review(&current)
            .await
            .unwrap()
            .unwrap_or(false));
        assert_eq!(
            TaskRepo::get_by_id(&*fixture.db, &current.id, false)
                .await
                .unwrap()
                .unwrap()
                .version,
            current.version
        );
    }
}

#[tokio::test]
async fn dispatcher_failed_review_composes_finding_routing_with_ci_budget() {
    for (owner, repeat, failed_check, expected_rejections) in [
        (true, false, false, 0),
        (false, true, false, 0),
        (true, true, true, 1),
    ] {
        let fixture = failed_review_fixture(chrono::Duration::minutes(3), 3).await;
        let review = ReviewRepo::list_by_task(&*fixture.db, &fixture.task.id)
            .await
            .unwrap()
            .remove(0);
        let reason = "needs an owner decision";
        let checks = if failed_check {
            serde_json::json!([{"check_id":"ci", "command":"false", "exit_code":1, "output":"CI failed"}])
        } else {
            serde_json::json!([])
        };
        let details = serde_json::json!({
            "conformance": {
                "status":"failed", "contract":null, "checks":checks,
                "reason": if failed_check { "CI failed" } else { reason },
                "assessment": {"result":"fail", "reason":reason,
                    "fixable_by":if owner { "owner" } else { "coder" }, "repeat":repeat}
            }
        });
        if repeat {
            sqlx::query("UPDATE review SET attempt_number = 2 WHERE id = ?")
                .bind(&review.id)
                .execute(fixture.db.pool())
                .await
                .unwrap();
            ReviewRepo::create(
                &*fixture.db,
                db::CreateReview {
                    id: new_uuid_v4(),
                    task_id: fixture.task.id.clone(),
                    execution_id: review.execution_id.clone(),
                    attempt_number: 1,
                    status: ReviewStatus::Failed,
                    step_results_json: details.to_string(),
                    started_at: review.started_at.clone(),
                    created_at: review.created_at.clone(),
                    updated_at: review.updated_at.clone(),
                },
            )
            .await
            .unwrap();
        }
        sqlx::query("UPDATE review SET step_results_json = ? WHERE id = ?")
            .bind(details.to_string())
            .bind(&review.id)
            .execute(fixture.db.pool())
            .await
            .unwrap();
        assert!(fixture
            .dispatcher
            .recover_failed_review(&fixture.task)
            .await
            .unwrap()
            .unwrap_or(false));
        let current = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
            .await
            .unwrap()
            .unwrap();
        if failed_check {
            assert_eq!(current.status, "in_progress");
            assert!(current.blocked_json.is_none());
        } else {
            assert_eq!(current.status, "review");
            let annotation: serde_json::Value =
                serde_json::from_str(current.error_annotation.as_deref().unwrap()).unwrap();
            assert_eq!(annotation["type"], "review_needs_owner");
            assert!(
                ExecutionRepo::list_running_by_task(&*fixture.db, &current.id)
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
        let entries = TransitionLogRepo::list_by_task(&*fixture.db, &current.id)
            .await
            .unwrap();
        assert_eq!(
            crate::task_diagnostics::audit_gate_rejections_since_boundary(&entries, "review"),
            expected_rejections
        );
        assert!(!fixture
            .dispatcher
            .recover_failed_review(&current)
            .await
            .unwrap()
            .unwrap_or(false));
    }
}

#[tokio::test]
async fn dispatcher_failed_review_records_exhausted_budget_once() {
    let fixture = failed_review_fixture(chrono::Duration::minutes(3), 1).await;
    assert_eq!(fixture.dispatcher.check_once_and_drain().await.unwrap(), 1);
    let current = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.status, "review");
    let annotation: serde_json::Value =
        serde_json::from_str(current.error_annotation.as_deref().unwrap()).unwrap();
    assert_eq!(annotation["type"], "review_budget_exhausted");
    assert!(!fixture
        .dispatcher
        .recover_failed_review(&current)
        .await
        .unwrap()
        .unwrap_or(false));
    let unchanged = TaskRepo::get_by_id(&*fixture.db, &current.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(unchanged.version, current.version);
}

#[tokio::test]
async fn dispatcher_failed_review_rejects_stale_task_version() {
    for budget in [1, 3] {
        let fixture = failed_review_fixture(chrono::Duration::minutes(3), budget).await;
        sqlx::query("UPDATE task SET version = version + 1 WHERE id = ?")
            .bind(&fixture.task.id)
            .execute(fixture.db.pool())
            .await
            .unwrap();
        let error = fixture
            .dispatcher
            .recover_failed_review(&fixture.task)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ServiceError::Db(db::DbError::VersionConflict)
        ));
        let current = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.status, "review");
        assert!(current.blocked_json.is_none());
    }
}

#[tokio::test]
async fn dispatcher_failed_review_respects_grace_and_recovery_fences() {
    for fence in [
        "grace",
        "annotation",
        "blocked",
        "running",
        "awaiting_human",
        "user",
        "barrier",
        "old_review",
        "new_review",
    ] {
        let age = if fence == "grace" {
            chrono::Duration::seconds(30)
        } else {
            chrono::Duration::minutes(3)
        };
        let fixture = failed_review_fixture(age, 3).await;
        match fence {
            "annotation" => {
                sqlx::query("UPDATE task SET error_annotation = ? WHERE id = ?")
                    .bind(r#"{"type":"review_blocked","message":"owner action required"}"#)
                    .bind(&fixture.task.id)
                    .execute(fixture.db.pool())
                    .await
                    .unwrap();
            }
            "blocked" => {
                sqlx::query("UPDATE task SET blocked_json = '{}' WHERE id = ?")
                    .bind(&fixture.task.id)
                    .execute(fixture.db.pool())
                    .await
                    .unwrap();
            }
            "running" => {
                let agent =
                    seed_agent(&fixture.db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
                seed_running_execution(&fixture.db, &fixture.task.id, &agent, "coder").await;
            }
            "awaiting_human" => {
                sqlx::query("UPDATE task SET metadata_json = ? WHERE id = ?")
                    .bind(r#"{"awaiting_human":true}"#)
                    .bind(&fixture.task.id)
                    .execute(fixture.db.pool())
                    .await
                    .unwrap();
            }
            "user" => {
                sqlx::query("UPDATE transition_log SET triggered_by = 'user:override:api' WHERE task_id = ?")
                .bind(&fixture.task.id).execute(fixture.db.pool()).await.unwrap();
            }
            "barrier" => {
                sqlx::query("UPDATE task SET entry_barrier_json = ? WHERE id = ?")
                    .bind(serde_json::json!({"state":"review","status":"blocked","retry_started_at":now_rfc3339()}).to_string())
                    .bind(&fixture.task.id)
                    .execute(fixture.db.pool())
                    .await
                    .unwrap();
            }
            "old_review" => {
                sqlx::query("UPDATE transition_log SET created_at = ? WHERE task_id = ?")
                    .bind(now_rfc3339())
                    .bind(&fixture.task.id)
                    .execute(fixture.db.pool())
                    .await
                    .unwrap();
            }
            "new_review" => {
                let candidate = seed_completed_coder_execution(&fixture.db, &fixture.task.id).await;
                let now = now_rfc3339();
                ReviewRepo::create(
                    &*fixture.db,
                    db::CreateReview {
                        id: new_uuid_v4(),
                        task_id: fixture.task.id.clone(),
                        execution_id: candidate,
                        attempt_number: 2,
                        status: ReviewStatus::Running,
                        step_results_json: r#"{"ci_steps":[]}"#.to_owned(),
                        started_at: now.clone(),
                        created_at: now.clone(),
                        updated_at: now,
                    },
                )
                .await
                .unwrap();
            }
            _ => {}
        }
        let task = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert!(
            !fixture
                .dispatcher
                .recover_failed_review(&task)
                .await
                .unwrap()
                .unwrap_or(false),
            "{fence}"
        );
        let current = TaskRepo::get_by_id(&*fixture.db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.version, task.version, "{fence}");
        assert_eq!(current.status, "review", "{fence}");
    }
}
