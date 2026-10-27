//! Wake turn consumer delivery turns tests.

use super::*;

#[tokio::test]
async fn admitted_wake_becomes_a_project_agent_turn() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    let profile_id = identity_with_profile(&db, &identity_id).await;
    let (project_id, chat_id) = bound_project(&db, &identity_id, &profile_id).await;
    let a = seed_incident(
        &db,
        &project_id,
        "execution_failed",
        &format!("attention:execution_failed:project:{project_id}:task:task-1"),
        "Task execution failed",
        serde_json::json!({"scope_type": "project", "scope_id": project_id}),
        "inspect_run",
    )
    .await;
    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 1);
    let (turn_count, status, responder): (i64, String, String) = sqlx::query_as(
        "SELECT COUNT(*), MAX(status), MAX(responder_identity_id)
         FROM agent_chat_turn_job WHERE chat_id = ?",
    )
    .bind(&chat_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(turn_count, 1);
    assert_eq!(status, "queued");
    assert_eq!(responder, identity_id);
    let message: (String, String) = sqlx::query_as(
        "SELECT author_type, content FROM agent_chat_message
         WHERE chat_id = ? ORDER BY sequence DESC LIMIT 1",
    )
    .bind(&chat_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(message.0, "system");
    assert!(message.1.contains("Task execution failed"));
    assert!(message.1.contains("inspect_run"));

    // Replays (another sweep, a direct admission) never create a second turn.
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 0);
    assert!(matches!(
        service
            .admit_wake(project_request(
                &identity_id,
                &project_id,
                &a,
                &now_rfc3339()
            ))
            .await
            .unwrap(),
        services::WakeAdmissionResult::Suppressed {
            reason: services::WakeSuppressionReason::DuplicateIncident
        }
    ));
    assert_eq!(
        chat_turn_count(&db, &chat_id).await,
        1,
        "replay must reuse the turn"
    );
}

/// The wake that fires when the last Task finishes is the moment validation is
/// owed: it names the exact ids and requires the record itself.
#[tokio::test]
async fn delivery_followup_with_all_tasks_done_orders_validation_before_readiness() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    let profile_id = identity_with_profile(&db, &identity_id).await;
    let (project_id, chat_id) = bound_project(&db, &identity_id, &profile_id).await;
    let fixture = seed_delivery_milestone(&db, &project_id).await;
    let task_id = seed_governed_task(&db, &project_id, &fixture.milestone_id, "done").await;
    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
    let first = seed_delivery_incident(
        &db,
        &project_id,
        &format!("attention:delivery_followup:project:{project_id}:task:done"),
        &task_id,
    )
    .await;
    let before = admitted_count(&db).await;
    service
        .admit_wake(project_request(
            &identity_id,
            &project_id,
            &first,
            &now_rfc3339(),
        ))
        .await
        .unwrap();
    assert_eq!(admitted_count(&db).await - before, 1);
    let (_, content, source_metadata_json) = wake_turn_for(&db, &chat_id, &first).await;
    assert!(content.contains("every Task bound to it is done"));
    assert!(content.contains(&format!("milestone_id={}", fixture.milestone_id)));
    assert!(content.contains("milestone_version=3"));
    assert!(content.contains(&format!(
        "definition_revision_id={}",
        fixture.milestone_revision_id
    )));
    assert!(content.contains("`project.validation` (action `record`)"));
    assert!(content.contains(&fixture.agent_check_id));
    assert!(content.contains(&fixture.manual_check_id));
    assert!(content.contains("you may never record one yourself"));
    let source_metadata: serde_json::Value = serde_json::from_str(&source_metadata_json).unwrap();
    assert_eq!(
        source_metadata["turn_postcondition"]["required_event_type"],
        "project.milestone.check.recorded",
        "the turn owes the validation record, not a readiness evaluation"
    );

    settle_check(
        &db,
        &project_id,
        &fixture,
        &fixture.agent_check_id,
        "passed",
    )
    .await;
    let second = seed_delivery_incident(
        &db,
        &project_id,
        &format!("attention:delivery_followup:project:{project_id}:task:done:2"),
        &task_id,
    )
    .await;
    let before = admitted_count(&db).await;
    service
        .admit_wake(project_request(
            &identity_id,
            &project_id,
            &second,
            &now_rfc3339(),
        ))
        .await
        .unwrap();
    assert_eq!(admitted_count(&db).await - before, 1);
    let (_, second_content, second_metadata) = wake_turn_for(&db, &chat_id, &second).await;
    assert!(!second_content.contains(&format!(
        "Settle yourself, in this turn: {}",
        fixture.agent_check_id
    )));
    assert!(second_content.contains(&fixture.manual_check_id));
    let second_metadata: serde_json::Value = serde_json::from_str(&second_metadata).unwrap();
    assert_eq!(
        second_metadata["turn_postcondition"]["required_event_type"],
        "milestone.readiness.evaluated",
        "with nothing left to record, readiness is what the turn owes"
    );
}

#[tokio::test]
async fn delivery_followup_reports_open_tasks_without_claiming_completion() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    let profile_id = identity_with_profile(&db, &identity_id).await;
    let (project_id, chat_id) = bound_project(&db, &identity_id, &profile_id).await;
    let fixture = seed_delivery_milestone(&db, &project_id).await;
    let done_task = seed_governed_task(&db, &project_id, &fixture.milestone_id, "done").await;
    seed_governed_task(&db, &project_id, &fixture.milestone_id, "in_progress").await;
    let a = seed_delivery_incident(
        &db,
        &project_id,
        &format!("attention:delivery_followup:project:{project_id}:task:done"),
        &done_task,
    )
    .await;
    AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    )
    .admit_wake(project_request(
        &identity_id,
        &project_id,
        &a,
        &now_rfc3339(),
    ))
    .await
    .unwrap();
    let (_, content, _) = wake_turn_for(&db, &chat_id, &a).await;
    assert!(content.contains("1 Task(s) still open"));
    assert!(!content.contains("every Task bound to it is done"));
}

#[tokio::test]
async fn delivery_followup_requires_newer_readiness_before_turn_success() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    let profile_id = identity_with_profile(&db, &identity_id).await;
    let (project_id, chat_id) = bound_project(&db, &identity_id, &profile_id).await;
    let fixture = seed_delivery_milestone(&db, &project_id).await;
    settle_check(
        &db,
        &project_id,
        &fixture,
        &fixture.agent_check_id,
        "passed",
    )
    .await;
    settle_check(
        &db,
        &project_id,
        &fixture,
        &fixture.manual_check_id,
        "passed",
    )
    .await;
    let task_id = seed_governed_task(&db, &project_id, &fixture.milestone_id, "done").await;
    let a = seed_delivery_incident(
        &db,
        &project_id,
        &format!("attention:delivery_followup:project:{project_id}:task:done"),
        &task_id,
    )
    .await;
    AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    )
    .admit_wake(project_request(
        &identity_id,
        &project_id,
        &a,
        &now_rfc3339(),
    ))
    .await
    .unwrap();
    let (turn_id, content, source_metadata_json) = wake_turn_for(&db, &chat_id, &a).await;
    // The postcondition is anchored on the admitting audit event.
    let wake_event_sequence: i64 = sqlx::query_scalar(
        "SELECT sequence FROM domain_event WHERE event_type = 'agent.wake.admitted'
         AND json_extract(payload_json, '$.turn_job_id') = ?",
    )
    .bind(&turn_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert!(content
        .contains("Every required acceptance check already has a current authoritative result"));
    assert!(content.contains("project.readiness"));
    let source_metadata: serde_json::Value = serde_json::from_str(&source_metadata_json).unwrap();
    assert_eq!(
        source_metadata["turn_postcondition"]["schema_version"],
        "forge.delivery-followup-postcondition/v1"
    );
    assert_eq!(
        source_metadata["turn_postcondition"]["after_event_sequence"],
        wake_event_sequence
    );
    let worker = AgentChatTurnWorker::with_runner(
        Arc::clone(&db),
        Arc::new(ProseOnlyWakeRunner) as Arc<dyn AgentChatTurnRunner>,
    );
    assert_eq!(worker.run_once().await.unwrap(), 1);
    let first = AgentChatTurnJobRepo::get_agent_chat_turn_job(&*db, &turn_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.status, AgentChatTurnState::RetryWait);
    assert_eq!(
        first.error_code.as_deref(),
        Some("delivery_followup_postcondition_failed")
    );
    let agent_responses: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_chat_message WHERE chat_id = ? AND author_type = 'agent'",
    )
    .bind(&chat_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(
        agent_responses, 0,
        "prose-only success must not be committed"
    );
    let readiness_event_id = new_uuid_v4();
    db.append_event(CreateDomainEvent {
        id: readiness_event_id.clone(),
        event_type: "milestone.readiness.evaluated".to_owned(),
        entity_type: "project_milestone".to_owned(),
        entity_id: "milestone-test".to_owned(),
        actor_type: "project_agent".to_owned(),
        actor_id: Some(identity_id),
        scope_type: "project".to_owned(),
        scope_id: project_id,
        correlation_id: readiness_event_id.clone(),
        causation_id: Some(turn_id.clone()),
        causation_depth: 1,
        dedupe_key: Some(format!("delivery-readiness:{readiness_event_id}")),
        payload_json: r#"{"result":"blocked"}"#.to_owned(),
        created_at: now_rfc3339(),
    })
    .await
    .unwrap();
    sqlx::query(
        "UPDATE agent_chat_turn_job SET next_attempt_at = '1970-01-01T00:00:00Z',
             version = version + 1, updated_at = ?
         WHERE id = ? AND status = 'retry_wait'",
    )
    .bind(now_rfc3339())
    .bind(&turn_id)
    .execute(db.pool())
    .await
    .unwrap();
    assert_eq!(worker.run_once().await.unwrap(), 1);
    let completed = AgentChatTurnJobRepo::get_agent_chat_turn_job(&*db, &turn_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed.status, AgentChatTurnState::Succeeded);
    assert_eq!(completed.attempt_count, 2);
    assert!(completed.response_message_id.is_some());
}

#[tokio::test]
async fn wake_incident_for_another_project_fails_closed_without_cross_project_turn() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    let profile_id = identity_with_profile(&db, &identity_id).await;
    let (event_project_id, event_chat_id) = bound_project(&db, &identity_id, &profile_id).await;
    let (attention_project_id, attention_chat_id) =
        bound_project(&db, &identity_id, &profile_id).await;
    let a = seed_incident(
        &db,
        &attention_project_id,
        "delivery_followup",
        &format!("attention:cross_project:project:{attention_project_id}"),
        "Other project incident",
        serde_json::json!({"scope_type": "project", "scope_id": attention_project_id}),
        "inspect_run",
    )
    .await;
    // The request claims the first Project; the incident belongs to the other.
    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
    let before = admitted_count(&db).await;
    let result = service
        .admit_wake(project_request(
            &identity_id,
            &event_project_id,
            &a,
            &now_rfc3339(),
        ))
        .await
        .unwrap();
    assert!(matches!(
        result,
        services::WakeAdmissionResult::Suppressed {
            reason: services::WakeSuppressionReason::IneligibleScope
        }
    ));
    assert_eq!(admitted_count(&db).await - before, 0);
    for chat_id in [&event_chat_id, &attention_chat_id] {
        assert_eq!(
            chat_turn_count(&db, chat_id).await,
            0,
            "cross-project wake must not enqueue a turn"
        );
    }
    // An incident with no Attention row is resolved, not admitted.
    let mut missing = project_request(&identity_id, &event_project_id, &a, &now_rfc3339());
    missing.incident_key = format!("attention:missing:project:{event_project_id}");
    assert!(matches!(
        service.admit_wake(missing).await.unwrap(),
        services::WakeAdmissionResult::Suppressed {
            reason: services::WakeSuppressionReason::ResolvedIncident
        }
    ));
}

#[tokio::test]
async fn admitted_wake_runner_failure_is_terminal_on_budget_and_keeps_admission_disposition() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    let profile_id = identity_with_profile(&db, &identity_id).await;
    let (project_id, chat_id) = bound_project(&db, &identity_id, &profile_id).await;
    let a = seed_incident(
        &db,
        &project_id,
        "delivery_followup",
        &format!("attention:runner_failure:project:{project_id}"),
        "Runner failure",
        serde_json::json!({"scope_type": "project", "scope_id": project_id}),
        "inspect_run",
    )
    .await;
    AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    )
    .admit_wake(project_request(
        &identity_id,
        &project_id,
        &a,
        &now_rfc3339(),
    ))
    .await
    .unwrap();
    let (turn_id, _, _) = wake_turn_for(&db, &chat_id, &a).await;
    let worker = AgentChatTurnWorker::with_runner(
        Arc::clone(&db),
        Arc::new(BackendFailingWakeRunner) as Arc<dyn AgentChatTurnRunner>,
    );
    assert_eq!(worker.run_once().await.unwrap(), 1);
    let first = AgentChatTurnJobRepo::get_agent_chat_turn_job(&*db, &turn_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.status, AgentChatTurnState::RetryWait);
    assert_eq!(first.attempt_count, 1);
    for expected_attempt in [1_i64, 2_i64] {
        sqlx::query(
            "UPDATE agent_chat_turn_job SET next_attempt_at = '1970-01-01T00:00:00Z',
                 version = version + 1, updated_at = ?
             WHERE id = ? AND status = 'retry_wait' AND attempt_count = ?",
        )
        .bind(now_rfc3339())
        .bind(&turn_id)
        .bind(expected_attempt)
        .execute(db.pool())
        .await
        .unwrap();
        assert_eq!(worker.run_once().await.unwrap(), 1);
    }
    let terminal = AgentChatTurnJobRepo::get_agent_chat_turn_job(&*db, &turn_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(terminal.status, AgentChatTurnState::Failed);
    assert_eq!(terminal.attempt_count, 3);
    assert!(terminal.next_attempt_at.is_none());
    assert_eq!(terminal.error_code.as_deref(), Some("backend_failed"));
    let (count, disposition, reason): (i64, String, String) = sqlx::query_as(
        "SELECT COUNT(*), MAX(disposition), MAX(reason) FROM agent_wake_disposition
         WHERE turn_job_id = ?",
    )
    .bind(&turn_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(
        (count, disposition.as_str(), reason.as_str()),
        (1, "turn_admitted", "turn_admitted")
    );
}

#[tokio::test]
async fn setup_required_wake_reconsiders_after_binding_change() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    let profile_id = identity_with_profile(&db, &identity_id).await;
    let (project_id, chat_id) = bound_project(&db, &identity_id, &profile_id).await;
    sqlx::query(
        "UPDATE project_agent_binding
         SET identity_id = NULL, profile_id = NULL, state = 'agent_setup_required',
             updated_at = ?, version = version + 1
         WHERE project_id = ?",
    )
    .bind(now_rfc3339())
    .bind(&project_id)
    .execute(db.pool())
    .await
    .unwrap();
    let a = seed_incident(
        &db,
        &project_id,
        "execution_failed",
        &format!("attention:setup:project:{project_id}"),
        "Setup incident",
        serde_json::json!({"scope_type": "project", "scope_id": project_id}),
        "configure_binding",
    )
    .await;
    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 0);
    let (disposition, linked): (String, Option<String>) = sqlx::query_as(
        "SELECT disposition, attention_id FROM agent_wake_disposition ORDER BY created_at LIMIT 1",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(disposition, "setup_required");
    assert_eq!(
        linked.as_deref(),
        Some(a.id.as_str()),
        "setup must link an Attention row"
    );
    sqlx::query(
        "UPDATE project_agent_binding
         SET identity_id = ?, profile_id = ?, state = 'active',
             updated_at = ?, version = version + 1
         WHERE project_id = ?",
    )
    .bind(&identity_id)
    .bind(&profile_id)
    .bind("9999-01-01T00:00:00Z")
    .bind(&project_id)
    .execute(db.pool())
    .await
    .unwrap();
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 1);
    assert_eq!(admitted_count(&db).await, 1);
    assert_eq!(chat_turn_count(&db, &chat_id).await, 1);
}

/// The whole autonomy loop: a Task execution fails, the durable attempt event
/// stays audit-only, the Task interruption commits, and only that wakes the
/// Project Agent, through the projection and its sweep.
#[tokio::test]
async fn actionable_task_interruption_wakes_the_project_agent_end_to_end() {
    use db::{
        ClaimExecutionLease, CreateExecution, ExecutionLeaseDisposition, ExecutionRepo,
        ExecutionStatus, ResumePolicy, StopReason, TaskRepo, TerminalizeExecution, UpdateTask,
    };
    let (db, database_path) = file_database().await;
    let identity_id = new_uuid_v4();
    let profile_id = identity_with_profile(&db, &identity_id).await;
    let (project_id, chat_id) = bound_project(&db, &identity_id, &profile_id).await;
    let task_id = new_uuid_v4();
    let now = now_rfc3339();
    sqlx::query(
        "INSERT INTO task (id, project_id, title, status, created_at, updated_at)
         VALUES (?, ?, 'wake loop task', 'in_progress', ?, ?)",
    )
    .bind(&task_id)
    .bind(&project_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .unwrap();
    let execution_id = new_uuid_v4();
    let running = ExecutionRepo::create_with_lease(
        &*db,
        CreateExecution {
            id: execution_id.clone(),
            task_id: task_id.clone(),
            agent_id: None,
            role: "worker".to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        ClaimExecutionLease {
            execution_id: execution_id.clone(),
            expected_version: 1,
            owner: "embedded:wake-turn-test".to_owned(),
            lease_expires_at: "9999-01-01T00:00:00+00:00".to_owned(),
            hard_deadline_at: Some("9999-01-01T00:00:00+00:00".to_owned()),
            now: now.clone(),
        },
    )
    .await
    .unwrap();
    ExecutionRepo::terminalize(
        &*db,
        TerminalizeExecution {
            execution_id: execution_id.clone(),
            expected_version: running.execution_version,
            lease_owner: running.lease_owner.clone(),
            status: ExecutionStatus::Failed,
            stop_reason: Some(Some(StopReason::ExecutorFailed)),
            stopped_by: Some(Some("embedded:wake-turn-test".to_owned())),
            stopped_at: Some(Some(now.clone())),
            resume_policy: Some(Some(ResumePolicy::Manual)),
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            last_progress_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: Some(Some("gemini exited with status 1".to_owned())),
            executor_config_snapshot_json: None,
            updated_at: now.clone(),
            actor_type: "system".to_owned(),
            actor_id: Some("wake-turn-test".to_owned()),
            correlation_id: Some(format!("wake-turn:{execution_id}")),
            causation_id: None,
            causation_depth: 0,
            lease_disposition: ExecutionLeaseDisposition::Expire,
        },
    )
    .await
    .unwrap();
    let task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    let annotation = serde_json::json!({
        "type": "executor_failed",
        "blocking_reason": "executor_failed",
        "blocked_by": "system:executor",
        "blocked_at": now,
        "blocked_execution_id": execution_id,
        "artifact": {"kind": "execution", "id": execution_id},
        "recovery_actions": ["reexecute", "reset_to_initial", "cancel_task"]
    });
    TaskRepo::update(
        &*db,
        UpdateTask {
            id: task.id,
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(annotation.to_string())),
            blocked_json: Some(Some(
                serde_json::json!({
                    "reason": "gemini exited with status 1",
                    "kind": "internal_command_failed",
                    "execution_id": execution_id
                })
                .to_string(),
            )),
            failed_json: Some(None),
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    )
    .project_once(100)
    .await
    .unwrap();
    let wake_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE event_type = 'agent.wake.admitted'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(
        wake_count, 1,
        "only the actionable Task disposition must admit one wake"
    );
    let (status, responder, content): (String, String, String) = sqlx::query_as(
        "SELECT job.status, job.responder_identity_id, message.content
         FROM agent_chat_turn_job AS job
         JOIN agent_chat_message AS message ON message.id = job.triggering_message_id
         WHERE job.chat_id = ?",
    )
    .bind(&chat_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(status, "queued");
    assert_eq!(responder, identity_id);
    assert!(content.contains("Task needs recovery"));
    drop(db);
    let _ = std::fs::remove_file(database_path);
}
