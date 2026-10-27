use super::{
    blocked_metadata_annotation, blocking_annotation_for_projection,
    retry_budget_exhausted_for_state,
};

#[tokio::test]
async fn owner_wait_and_review_ci_blockers_project_in_task_response() {
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let db = std::sync::Arc::new(db::SqliteDb::new(pool));
    sqlx::query("INSERT INTO project (id, name, created_at, updated_at) VALUES ('project', 'projection', '2026-10-01T00:00:00Z', '2026-10-01T00:00:00Z')")
            .execute(db.pool()).await.unwrap();
    let service = services::TaskService::new_for_test(
        db.clone(),
        std::sync::Arc::new(events::EventBus::default()),
    );
    let workflow = services::workflow::default_workflow::default_workflow();
    for kind in [
        api_types::FailureKind::RecoveryRequired,
        api_types::FailureKind::BeforeWorkHookFailed,
    ] {
        let mut task = test_task();
        task.blocked_json = Some(serde_json::json!({"reason":"owner unavailable", "created_at":"2026-10-01T00:00:00Z", "kind":kind, "execution_id":null}).to_string());
        task.condition = db::compose_condition(&db::ConditionInput::from(&task));
        let response = super::task_response_inner(
            &db,
            &service.workspace_backend_router(),
            task,
            false,
            false,
            None,
            None,
            &workflow,
        )
        .await
        .unwrap();
        let blocked = response
            .condition
            .details()
            .interruption
            .clone()
            .expect("public blocked field must survive projection");
        assert_eq!(blocked.kind, Some(kind));
        assert_eq!(blocked.reason, "owner unavailable");
        assert_eq!(blocked.created_at, "2026-10-01T00:00:00Z");
    }
}

#[tokio::test]
async fn integration_condition_survives_the_task_response_projection() {
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let db = std::sync::Arc::new(db::SqliteDb::new(pool));
    sqlx::query("INSERT INTO project (id, name, created_at, updated_at) VALUES ('project', 'projection', '2026-10-01T00:00:00Z', '2026-10-01T00:00:00Z')").execute(db.pool()).await.unwrap();
    let service = services::TaskService::new_for_test(
        db.clone(),
        std::sync::Arc::new(events::EventBus::default()),
    );
    let workflow = services::workflow::default_workflow::default_workflow();
    let mut task = test_task();
    task.status = "merging".into();
    task.condition = db::ConditionFacts {
        task_id: task.id.clone(),
        state: task.status.clone(),
        integration: Some(api_types::IntegrationReason::Waiting {
            attempt_id: api_types::IntegrationAttemptId::new("attempt"),
        }),
        ..Default::default()
    }
    .condition(&db::ConditionInput::default());
    let expected = task.condition.public();
    let response = super::task_response_inner(
        &db,
        &service.workspace_backend_router(),
        task,
        false,
        false,
        None,
        None,
        &workflow,
    )
    .await
    .unwrap();
    assert_eq!(response.condition, expected);
    assert!(!response.condition.details().blocked);
    assert!(!response.awaiting_human);
    assert!(response.workflow_exception.is_none());
}

fn annotation(_actions: Vec<api_types::TaskAction>) -> api_types::TaskBlockingAnnotation {
    api_types::TaskBlockingAnnotation {
        annotation_type: api_types::FailureKind::RecoveryRequired,
        blocking_reason: "test blocker".to_owned(),
        blocked_by: None,
        blocked_at: None,
        blocked_execution_id: None,
        artifact: None,
        message: None,
        hook: None,
    }
}

#[test]
fn populated_typed_annotation_wins_over_legacy_blocked_metadata() {
    let task = db::Task {
        status: services::workflow::default_states::IN_PROGRESS.to_owned(),
        ..test_task()
    };
    let mut legacy = annotation(vec![api_types::TaskAction::Cancel { reason: None }]);
    legacy.annotation_type = api_types::FailureKind::TargetRepoDirty;
    let typed = annotation(vec![api_types::TaskAction::Restart { reason: None }]);

    let selected = blocking_annotation_for_projection(&task, Some(&legacy), Some(&typed))
        .expect("one blocker selected");

    assert_eq!(selected.annotation_type, typed.annotation_type);
}

#[test]
fn legacy_blocked_metadata_fills_an_empty_typed_annotation() {
    let task = db::Task {
        status: services::workflow::default_states::MERGING.to_owned(),
        ..test_task()
    };
    let mut legacy = annotation(vec![api_types::TaskAction::Cancel { reason: None }]);
    legacy.annotation_type = api_types::FailureKind::TargetRepoDirty;
    let mut empty_typed = annotation(Vec::new());
    empty_typed.annotation_type = api_types::FailureKind::Unknown;

    let selected = blocking_annotation_for_projection(&task, Some(&legacy), Some(&empty_typed))
        .expect("one blocker selected");

    assert_eq!(
        selected.annotation_type,
        api_types::FailureKind::TargetRepoDirty
    );
}

#[test]
fn generic_legacy_blocker_does_not_override_empty_typed_annotation() {
    let task = test_task();
    let legacy = annotation(vec![api_types::TaskAction::Cancel { reason: None }]);
    let empty_typed = annotation(Vec::new());

    let selected = blocking_annotation_for_projection(&task, Some(&legacy), Some(&empty_typed))
        .expect("typed annotation remains authoritative");

    assert_eq!(
        selected.annotation_type,
        api_types::FailureKind::RecoveryRequired
    );
}

#[test]
fn unknown_legacy_blocker_cannot_advertise_recovery_actions() {
    let task = db::Task {
        blocked_json: Some(
            serde_json::json!({
                "kind": "future_failure_kind",
                "recovery_actions": ["cancel_task"]
            })
            .to_string(),
        ),
        ..test_task()
    };

    let parsed = blocked_metadata_annotation(&task).expect("legacy blocker parses");
    assert_eq!(parsed.annotation_type, api_types::FailureKind::Unknown);
    assert!(serde_json::to_value(&parsed)
        .unwrap()
        .get("recovery_actions")
        .is_none());
}

#[test]
fn legacy_retry_blocker_uses_the_service_allowlist() {
    let task = db::Task {
        blocked_json: Some(
            serde_json::json!({
                "kind": "retry_exhausted",
                "recovery_actions": ["resume_session"]
            })
            .to_string(),
        ),
        ..test_task()
    };

    let parsed = blocked_metadata_annotation(&task).expect("legacy blocker parses");
    assert_eq!(
        parsed.annotation_type,
        api_types::FailureKind::RetryExhausted
    );
    assert!(serde_json::to_value(parsed)
        .unwrap()
        .get("recovery_actions")
        .is_none());
}

fn test_task() -> db::Task {
    db::Task {
        condition_digest: None,
        condition: Default::default(),
        id: "task".to_owned(),
        project_id: "project".to_owned(),
        parent_task_id: None,
        assignee_type: None,
        assignee_id: None,
        title: "task".to_owned(),
        description: None,
        task_type: "task".to_owned(),
        status: services::workflow::default_states::IN_PROGRESS.to_owned(),
        is_automation: false,
        priority: 0,
        board_position: 0.0,
        subtask_order: None,
        task_state_config: None,
        merge_config: None,
        metadata_json: None,
        plan: None,
        error_annotation: None,
        blocked_json: None,
        failed_json: None,
        entry_barrier_json: None,
        review_passed_at: None,
        archived_at: None,
        deleted_at: None,
        version: 1,
        created_at: "2026-01-01T00:00:00Z".to_owned(),
        updated_at: "2026-01-01T00:00:00Z".to_owned(),
    }
}

#[test]
fn exhausted_merge_annotation_forces_zero_retry_projection() {
    let workflow = services::workflow::default_workflow::default_workflow();
    let merging = workflow
        .states
        .iter()
        .find(|state| state.name == services::workflow::default_states::MERGING)
        .expect("default workflow has merging gate");
    let annotation = api_types::TaskBlockingAnnotation {
        annotation_type: api_types::FailureKind::MergeFixBudgetExhausted,
        blocking_reason: "merge-fix retry budget exhausted".to_owned(),
        blocked_by: None,
        blocked_at: None,
        blocked_execution_id: None,
        artifact: None,
        message: None,
        hook: None,
    };

    assert!(retry_budget_exhausted_for_state(
        services::workflow::default_states::MERGING,
        merging,
        &annotation
    ));
}
