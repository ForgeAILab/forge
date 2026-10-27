use super::*;

struct CapacityLimitedExecutor {
    retry_after: Option<std::time::Duration>,
}

#[async_trait]
impl TaskExecutor for CapacityLimitedExecutor {
    async fn execute(
        &self,
        _ctx: ExecutionContext,
    ) -> std::result::Result<ExecutionResult, ExecutorError> {
        Ok(ExecutionResult {
            status: ExecutionOutcome::Failed,
            error: Some("You've hit your usage limit".to_owned()),
            failure_class: Some(executors::ExecutionFailureClass::ExecutorUnavailable),
            retry_after: self.retry_after,
            route_attempts: vec![executors::RouteAttempt {
                candidate_key: "only-candidate".to_owned(),
                outcome: executors::RouteAttemptOutcome::UsageExhausted,
            }],
            ..Default::default()
        })
    }

    async fn cancel(&self, _execution_id: &str) -> std::result::Result<(), ExecutorError> {
        Ok(())
    }
}

#[tokio::test]
async fn capacity_limit_without_fallback_defers_until_reset_or_backoff() {
    for hint in [
        Some(std::time::Duration::from_secs(3900)),
        None,
        Some(std::time::Duration::from_secs(7 * 86400)),
    ] {
        let db = Arc::new(sqlite_db().await);
        let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(16)));
        let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
        let agent_id = seed_agent(&db).await;
        let task = service
            .create_task(
                project_id,
                "Wait for quota",
                Some("implement the task".to_owned()),
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();
        let claimed = service
            .claim_task(task.id.clone(), Assignee::Agent(agent_id), None)
            .await
            .unwrap();
        let retry_count = u64::from(hint.is_none());
        if retry_count > 0 {
            seed_budget_spent(
                &db,
                &task.id,
                db::budget::Kind::Execution,
                retry_count as i64,
            )
            .await;
            sqlx::query("UPDATE task SET metadata_json = ?, version = version + 1 WHERE id = ?")
                .bind("{}")
                .bind(&task.id)
                .execute(db.pool())
                .await
                .unwrap();
        }
        let before = chrono::Utc::now();
        let execution = service
            .run_execution(
                claimed.execution.id,
                &CapacityLimitedExecutor { retry_after: hint },
            )
            .await
            .unwrap();
        let after = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        let metadata: Value =
            serde_json::from_str(after.metadata_json.as_deref().unwrap()).unwrap();
        let at = chrono::DateTime::parse_from_rfc3339(
            metadata["deferred_dispatch"]["not_before"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
        let delay = (at.with_timezone(&chrono::Utc) - before).num_seconds();
        let expected = hint.map_or(20, |hint| hint.as_secs().min(6 * 3600)) as i64;
        assert!(
            delay >= expected && delay <= expected + 30,
            "{delay} vs {expected}"
        );
        assert_eq!(
            json!(
                db::budget::spent(db.pool(), &task.id, db::budget::Kind::Execution.key())
                    .await
                    .unwrap()
            ),
            retry_count + 1
        );
        let persisted_execution = ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            persisted_execution.resume_policy,
            Some(db::ResumePolicy::Auto)
        );
        assert!(after.version > claimed.task.version);
        assert!(after.blocked_json.is_none());
        assert!(after.failed_json.is_none());
        assert!(after.error_annotation.is_none());
        let assignments = db::TaskRoleAssignmentRepo::list_by_task(&*db, &task.id)
            .await
            .unwrap();
        let health = crate::task_diagnostics::derive_workflow_health(
            &after,
            &crate::workflow::default_workflow::default_workflow(),
            &assignments,
            None,
            Some(&execution),
            false,
            None,
        );
        assert_eq!(health.kind, api_types::WorkflowHealthKind::WaitingForAgent);
        assert_eq!(health.label, "Retry Scheduled");
        assert!(health.message.unwrap().contains("usage limit"));

        service
            .annotate_executor_unavailable_block(
                &execution,
                hint.map(|hint| {
                    (chrono::Utc::now() + chrono::Duration::seconds(hint.as_secs() as i64))
                        .to_rfc3339()
                }),
                vec![db::task_condition::channels::ExecutorRouteAttempt {
                    candidate_key: "candidate".into(),
                    outcome: "usage_exhausted".into(),
                }],
            )
            .await
            .unwrap();
        let repeated = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(repeated.metadata_json, after.metadata_json);
        assert_eq!(repeated.version, after.version);
    }
}

#[tokio::test]
async fn capacity_limit_retry_budget_exhaustion_blocks() {
    assert_capacity_limit_retry_block(3, 3, "retry budget exhausted").await;
}

#[tokio::test]
async fn capacity_limit_zero_retry_budget_blocks_as_disabled() {
    assert_capacity_limit_retry_block(0, 0, "automatic retries are disabled").await;
}

async fn assert_capacity_limit_retry_block(budget: u64, retry_count: u64, reason: &str) {
    let db = Arc::new(sqlite_db().await);
    let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(16)));
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = service
        .create_task(
            project_id,
            "Exhausted quota retries",
            Some("implement the task".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    let claimed = service
        .claim_task(task.id.clone(), Assignee::Agent(agent_id), None)
        .await
        .unwrap();
    seed_budget_spent(
        &db,
        &task.id,
        db::budget::Kind::Execution,
        retry_count as i64,
    )
    .await;
    sqlx::query("UPDATE task SET metadata_json = ?, task_state_config = ?, version = version + 1 WHERE id = ?")
        .bind("{}")
        .bind(json!({"retry_budgets": {"execution": budget}}).to_string())
        .bind(&task.id)
        .execute(db.pool())
        .await
        .unwrap();
    let execution = service
        .run_execution(
            claimed.execution.id,
            &CapacityLimitedExecutor {
                retry_after: Some(std::time::Duration::from_secs(3900)),
            },
        )
        .await
        .unwrap();
    let after = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    let annotation: api_types::TaskBlockingAnnotation =
        serde_json::from_str(after.error_annotation.as_deref().unwrap()).unwrap();
    assert_eq!(
        annotation.annotation_type,
        api_types::FailureKind::ExecutorUnavailable
    );
    let message = annotation.message.unwrap();
    assert!(message.contains(reason), "{message}");
    if budget == 0 {
        assert!(!message.contains("retry budget exhausted"), "{message}");
    }
    assert!(after.blocked_json.is_some());
    assert!(after.failed_json.is_none());
    assert_eq!(execution.resume_policy, Some(db::ResumePolicy::Manual));
    let metadata: Value = serde_json::from_str(after.metadata_json.as_deref().unwrap()).unwrap();
    assert_eq!(
        json!(
            db::budget::spent(db.pool(), &task.id, db::budget::Kind::Execution.key())
                .await
                .unwrap()
        ),
        retry_count
    );
    assert!(metadata.get("deferred_dispatch").is_none());
}
