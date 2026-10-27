//! Attention service task incidents tests.

use super::*;

#[tokio::test]
async fn user_pause_and_stop_keep_manual_controls_without_recovery_wakes() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    identity(&db, &identity_id).await;
    let project_id = configured_project(&db, &identity_id, "intentional-stops").await;
    let tasks =
        services::TaskService::new_for_test(Arc::clone(&db), Arc::new(events::EventBus::default()));
    let attention = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );

    for pause in [true, false] {
        let task = project_task(&db, &project_id, "Wait for the user to resume").await;
        let execution = task_execution(
            &db,
            &task,
            &identity_id,
            ExecutionStatus::Running,
            &now_rfc3339(),
            None,
        )
        .await;
        if pause {
            tasks
                .pause_execution(&execution.id, "Inspect this work".to_owned())
                .await
                .unwrap();
        } else {
            tasks
                .stop_execution(&execution.id, "Stop this work".to_owned())
                .await
                .unwrap();
        }
        let current = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        let annotation: serde_json::Value =
            serde_json::from_str(current.error_annotation.as_deref().unwrap()).unwrap();
        assert_eq!(annotation["type"], "manual_stop");
        assert!(annotation.get("recovery_actions").is_none());
        let offers = tasks
            .task_action_offers(
                &task.id,
                &api_types::Actor::user(api_types::UserActionSource::Test),
            )
            .await
            .unwrap();
        assert!(offers
            .available_actions
            .iter()
            .any(|offer| offer.action.verb() == "cancel"));
        let run = attention.project_once(100).await.unwrap();
        assert_eq!(
            run.processed_events, run.claimed_events,
            "intentional stops must not hold the event cursor for orphan grace"
        );
        let payload: String = sqlx::query_scalar("SELECT payload_json FROM domain_event WHERE event_type = 'task.interruption_changed' AND entity_id = ? ORDER BY sequence DESC LIMIT 1")
            .bind(&task.id).fetch_one(db.pool()).await.unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&payload).unwrap()["material_blocker"]
                ["requires_intervention"],
            false
        );
    }
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM attention_projection WHERE scope_id = ? AND attention_type = 'execution_failed'")
        .bind(&project_id).fetch_one(db.pool()).await.unwrap(), 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_wake_lease WHERE scope_id = ?")
            .bind(&project_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn superseded_terminal_attempts_never_become_orphans_or_delay_other_events() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    identity(&db, &identity_id).await;
    let project_id = configured_project(&db, &identity_id, "recovered-attempts").await;
    let attention = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
    for age_seconds in [0, 60] {
        for (successor_status, linked) in [
            (ExecutionStatus::Running, true),
            (ExecutionStatus::Completed, true),
            (ExecutionStatus::Running, false),
            (ExecutionStatus::Completed, false),
        ] {
            let task = project_task(&db, &project_id, "Already recovered").await;
            let terminal_at = (Utc::now() - Duration::seconds(age_seconds)).to_rfc3339();
            let stopped = task_execution(
                &db,
                &task,
                &identity_id,
                ExecutionStatus::Failed,
                &terminal_at,
                None,
            )
            .await;
            append_attention_event_at(&db, "execution.failed", &task.id, &project_id,
                serde_json::json!({"execution_id": stopped.id, "task_id": task.id, "role": "coder", "status": "failed"}), &terminal_at).await;
            // The successor is authoritative even when its creation timestamp
            // is identical to its parent (for example a coarse imported clock).
            // A newer independent re-execution need not link to the old one.
            let successor_at = if linked {
                terminal_at.clone()
            } else {
                (Utc::now() + Duration::seconds(1)).to_rfc3339()
            };
            task_execution(
                &db,
                &task,
                &identity_id,
                successor_status,
                &successor_at,
                linked.then_some(stopped.id),
            )
            .await;
        }
    }
    let run = attention.project_once(100).await.unwrap();
    assert_eq!(
        run.processed_events, run.claimed_events,
        "old failures must not delay unrelated events"
    );
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM attention_projection WHERE scope_id = ? AND attention_type = 'execution_failed'")
        .bind(&project_id).fetch_one(db.pool()).await.unwrap(), 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_wake_lease WHERE scope_id = ?")
            .bind(&project_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn obsolete_orphan_wakes_recheck_task_and_attempt_before_spending_budget() {
    for outcome in [
        "running",
        "completed",
        "cancelled",
        "deleted",
        "deferred",
        "manual_stop",
    ] {
        let db = database().await;
        let identity_id = new_uuid_v4();
        identity(&db, &identity_id).await;
        let project_id = configured_project(&db, &identity_id, outcome).await;
        // Project the incident while the responder is unavailable, then change
        // execution/Task truth before retrying admission for the open incident.
        sqlx::query("UPDATE agent_chat SET status = 'agent_setup_required' WHERE project_id = ?")
            .bind(&project_id)
            .execute(db.pool())
            .await
            .unwrap();
        let task = project_task(&db, &project_id, "Recovery already settled").await;
        let terminal_at = (Utc::now() - Duration::minutes(1)).to_rfc3339();
        let stopped = task_execution(
            &db,
            &task,
            &identity_id,
            ExecutionStatus::Failed,
            &terminal_at,
            None,
        )
        .await;
        append_attention_event_at(
            &db,
            "execution.failed",
            &task.id,
            &project_id,
            serde_json::json!({"execution_id": stopped.id, "task_id": task.id}),
            &terminal_at,
        )
        .await;
        let attention = AttentionService::new(
            Arc::clone(&db),
            Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
        );
        attention.project_once(100).await.unwrap();
        let (id, incident_key, source_event_id, status): (String, String, String, String) =
            sqlx::query_as(
                "SELECT id, dedupe_key, source_event_id, status FROM attention_projection
             WHERE scope_id = ? AND attention_type = 'execution_failed'",
            )
            .bind(&project_id)
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(status, "open");
        match outcome {
            "running" | "completed" => {
                let status = if outcome == "running" {
                    ExecutionStatus::Running
                } else {
                    ExecutionStatus::Completed
                };
                task_execution(
                    &db,
                    &task,
                    &identity_id,
                    status,
                    &terminal_at,
                    Some(stopped.id),
                )
                .await;
            }
            "cancelled" => {
                TaskRepo::update_status(
                    &*db,
                    db::UpdateTaskStatus {
                        id: task.id.clone(),
                        expected_version: task.version,
                        status: "cancelled".to_owned(),
                        assignee_id: None,
                        error_annotation: None,
                        blocked_json: None,
                        failed_json: None,
                        updated_at: now_rfc3339(),
                    },
                )
                .await
                .unwrap();
            }
            "deleted" => {
                sqlx::query("UPDATE task SET deleted_at = ?, version = version + 1 WHERE id = ?")
                    .bind(now_rfc3339())
                    .bind(&task.id)
                    .execute(db.pool())
                    .await
                    .unwrap();
            }
            "deferred" => {
                TaskRepo::mutate_metadata(
                    &*db,
                    &task.id,
                    None,
                    vec![db::TaskMetadataMutation::Set {
                        key: "deferred_dispatch".to_owned(),
                        value: serde_json::json!({
                            "not_before": (Utc::now() + Duration::minutes(1)).to_rfc3339(),
                            "reason": "retry scheduled", "target_state": "in_progress"
                        }),
                    }],
                    &now_rfc3339(),
                )
                .await
                .unwrap();
            }
            "manual_stop" => {
                sqlx::query(
                    "UPDATE task SET error_annotation = ?, version = version + 1 WHERE id = ?",
                )
                .bind(
                    serde_json::json!({"type": "manual_stop", "recovery_actions": ["reexecute"]})
                        .to_string(),
                )
                .bind(&task.id)
                .execute(db.pool())
                .await
                .unwrap();
                db.check_task_conditions_of(std::slice::from_ref(&task.id))
                    .await
                    .unwrap();
            }
            _ => unreachable!(),
        }
        sqlx::query("UPDATE agent_chat SET status = 'ready' WHERE project_id = ?")
            .bind(&project_id)
            .execute(db.pool())
            .await
            .unwrap();
        let result = attention
            .admit_wake(WakeAdmissionRequest {
                scope_type: "project".to_owned(),
                scope_id: project_id.clone(),
                causation_id: Some(source_event_id),
                now: now_rfc3339(),
                ..request(&identity_id, &incident_key)
            })
            .await
            .unwrap();
        assert!(
            matches!(
                result,
                WakeAdmissionResult::Suppressed {
                    reason: WakeSuppressionReason::ResolvedIncident
                }
            ),
            "{outcome}: {result:?}"
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT status FROM attention_projection WHERE id = ?")
                .bind(&id)
                .fetch_one(db.pool())
                .await
                .unwrap(),
            "resolved",
            "{outcome}"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM agent_wake_budget_window WHERE scope_id = ?"
            )
            .bind(&project_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            0,
            "{outcome}"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM agent_wake_lease WHERE scope_id = ?"
            )
            .bind(&project_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
            0,
            "{outcome}"
        );
    }
}

#[tokio::test]
async fn auto_retried_execution_failure_is_audit_only() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    identity(&db, &identity_id).await;
    let project_id = configured_project(&db, &identity_id, "auto-retry-project").await;
    let task = project_task(&db, &project_id, "Retry without waking the Project Agent").await;
    let execution_id = new_uuid_v4();
    let now = now_rfc3339();

    // Use a real terminal Execution row so the projection exercises the
    // retry/resume policy boundary rather than trusting prose in the event.
    ExecutionRepo::create(
        &*db,
        CreateExecution {
            id: execution_id.clone(),
            task_id: task.id.clone(),
            agent_id: Some(identity_id),
            role: "coder".to_owned(),
            status: ExecutionStatus::Failed,
            stop_reason: Some(db::StopReason::ExecutorFailed),
            stopped_by: Some("system:executor".to_owned()),
            resume_policy: Some(ResumePolicy::Auto),
            stopped_at: Some(now.clone()),
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: Some("transient executor failure".to_owned()),
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    append_attention_event(
        &db,
        "execution.failed",
        &task.id,
        &project_id,
        serde_json::json!({
            "execution_id": execution_id,
            "task_id": task.id,
            "role": "coder",
            "status": "failed",
            "stop_reason": "executor_failed",
            "error": "transient executor failure",
        }),
    )
    .await;

    AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    )
    .project_once(100)
    .await
    .unwrap();

    let attention_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM attention_projection
         WHERE attention_type = 'execution_failed' AND scope_id = ?",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    let wake_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event
         WHERE event_type = 'agent.wake.admitted' AND scope_id = ?",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(
        attention_count, 0,
        "automatic retry must not create Attention"
    );
    assert_eq!(wake_count, 0, "automatic retry must not wake Project Agent");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_wake_lease WHERE scope_id = ?",)
            .bind(&project_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn undisposed_manual_execution_failure_becomes_actionable_after_grace() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    identity(&db, &identity_id).await;
    let project_id = configured_project(&db, &identity_id, "orphaned-failure-project").await;
    let task = project_task(&db, &project_id, "Recover an orphaned terminal attempt").await;
    let execution_id = new_uuid_v4();
    let terminal_at = (Utc::now() - Duration::minutes(1)).to_rfc3339();

    ExecutionRepo::create(
        &*db,
        CreateExecution {
            id: execution_id.clone(),
            task_id: task.id.clone(),
            agent_id: Some(identity_id),
            role: "coder".to_owned(),
            status: ExecutionStatus::Failed,
            stop_reason: Some(db::StopReason::ExecutorFailed),
            stopped_by: Some("system:executor".to_owned()),
            resume_policy: Some(ResumePolicy::Manual),
            stopped_at: Some(terminal_at.clone()),
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: Some("terminalization committed before disposition".to_owned()),
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: terminal_at.clone(),
            updated_at: terminal_at.clone(),
        },
    )
    .await
    .unwrap();
    append_attention_event_at(
        &db,
        "execution.failed",
        &task.id,
        &project_id,
        serde_json::json!({
            "execution_id": execution_id,
            "task_id": task.id,
            "role": "coder",
            "status": "failed",
            "stop_reason": "executor_failed",
        }),
        &terminal_at,
    )
    .await;

    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
    service.project_once(100).await.unwrap();

    let attention: (String, String) = sqlx::query_as(
        "SELECT status, details_json FROM attention_projection
         WHERE attention_type = 'execution_failed' AND scope_id = ?",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(attention.0, "open");
    let details: serde_json::Value = serde_json::from_str(&attention.1).unwrap();
    assert_eq!(details["source_event_type"], "execution.failed");
    assert_eq!(details["recovery"]["requires_intervention"], true);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event
             WHERE event_type = 'agent.wake.admitted' AND scope_id = ?",
        )
        .bind(&project_id)
        .fetch_one(db.pool())
        .await
        .unwrap(),
        1
    );

    // A late Task disposition updates the same incident rather than creating
    // an execution-keyed duplicate, and its later recovery resolves it.
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    let blocked = TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: current.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(
                serde_json::json!({
                    "type": "executor_failed",
                    "recovery_actions": ["reexecute", "cancel_task"]
                })
                .to_string(),
            )),
            blocked_json: Some(Some(
                serde_json::json!({
                    "reason": "late dispatcher disposition",
                    "kind": "executor_failed",
                    "execution_id": execution_id
                })
                .to_string(),
            )),
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    service.project_once(100).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM attention_projection
             WHERE attention_type = 'execution_failed' AND scope_id = ?",
        )
        .bind(&project_id)
        .fetch_one(db.pool())
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event
             WHERE event_type = 'agent.wake.admitted' AND scope_id = ?",
        )
        .bind(&project_id)
        .fetch_one(db.pool())
        .await
        .unwrap(),
        1,
        "late Task disposition must not create a second wake for the incident"
    );

    TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: blocked.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(None),
            blocked_json: Some(None),
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    service.project_once(100).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT status FROM attention_projection
             WHERE attention_type = 'execution_failed' AND scope_id = ?",
        )
        .bind(&project_id)
        .fetch_one(db.pool())
        .await
        .unwrap(),
        "resolved"
    );
}

#[tokio::test]
async fn review_owner_parks_stay_visible_without_wakes_or_escalations() {
    for initially_paused in [false, true] {
        let db = database().await;
        let identity_id = new_uuid_v4();
        identity(&db, &identity_id).await;
        let project_id = configured_project(&db, &identity_id, "owner-only-review").await;
        sqlx::query("UPDATE agent_identity SET paused=? WHERE id=?")
            .bind(initially_paused)
            .bind(&identity_id)
            .execute(db.pool())
            .await
            .unwrap();
        let task = project_task(&db, &project_id, "Owner must decide").await;
        let now = now_rfc3339();
        let annotation = serde_json::json!({
            "type": "review_needs_owner", "message": "repeated finding F1"
        });
        let condition = serde_json::json!({
            "kind": "review_needs_owner", "reason": "repeated finding F1", "created_at": now
        });
        TaskRepo::update_status(
            &*db,
            db::UpdateTaskStatus {
                id: task.id.clone(),
                expected_version: task.version,
                status: "review".into(),
                assignee_id: None,
                error_annotation: Some(Some(annotation.to_string())),
                blocked_json: Some(Some(condition.to_string())),
                failed_json: Some(None),
                updated_at: now.clone(),
            },
        )
        .await
        .unwrap();
        let service = || {
            AttentionService::new(
                Arc::clone(&db),
                Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
            )
        };
        service().project_once(100).await.unwrap();
        let attention_id: String = sqlx::query_scalar(
            "SELECT id FROM attention_projection
         WHERE attention_type='review_needs_owner' AND scope_id=? AND status='open'",
        )
        .bind(&project_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
        let owner_park = db.get_attention(&attention_id).await.unwrap().unwrap();
        assert_eq!(
            owner_park.summary,
            "Review needs the owner: repeated finding F1"
        );
        // Fresh service instances exercise startup sweeps as well as event projection.
        for _ in 0..2 {
            assert_eq!(service().sweep_once_at(&now_rfc3339()).await.unwrap(), 0);
            service().project_once(100).await.unwrap();
        }
        for table in [
            "agent_chat_turn_job",
            "agent_wake_budget_window",
            "agent_wake_lease",
        ] {
            let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
                .fetch_one(db.pool())
                .await
                .unwrap();
            assert_eq!(count, 0, "owner review parks must not create {table}");
        }
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM domain_event WHERE event_type LIKE 'agent.wake.%'"
            )
            .fetch_one(db.pool())
            .await
            .unwrap(),
            0,
            "owner-only work needs neither an Agent wake nor a setup decision"
        );

        // An ordinary blocker beside the owner park still wakes and escalates normally.
        sqlx::query("UPDATE agent_identity SET paused=0 WHERE id=?")
            .bind(&identity_id)
            .execute(db.pool())
            .await
            .unwrap();
        let failure = project_task(&db, &project_id, "Recover executor failure").await;
        let annotation = serde_json::json!({"type": "executor_failed"});
        let condition = serde_json::json!({
            "kind": "executor_failed", "reason": "executor failed", "created_at": now
        });
        TaskRepo::update_status(
            &*db,
            db::UpdateTaskStatus {
                id: failure.id.clone(),
                expected_version: failure.version,
                status: "in_progress".into(),
                assignee_id: None,
                error_annotation: Some(Some(annotation.to_string())),
                blocked_json: Some(Some(condition.to_string())),
                failed_json: Some(None),
                updated_at: now_rfc3339(),
            },
        )
        .await
        .unwrap();
        service().project_once(100).await.unwrap();
        let linked: Vec<String> = sqlx::query_scalar(
            "SELECT a.attention_type FROM agent_wake_blocker b
         JOIN attention_projection a ON a.id=b.attention_id",
        )
        .fetch_all(db.pool())
        .await
        .unwrap();
        assert_eq!(linked, vec!["execution_failed"]);
        let turn_id: String = sqlx::query_scalar("SELECT id FROM agent_chat_turn_job")
            .fetch_one(db.pool())
            .await
            .unwrap();
        // A turn admitted before this policy changed can still finish after an
        // upgrade. Its old owner-park link must not duplicate the owner request.
        sqlx::query(
            "INSERT INTO agent_wake_blocker
                    (attention_id,incident_digest,turn_job_id,admitted_at) VALUES (?,?,?,?)",
        )
        .bind(&attention_id)
        .bind(services::wake_attention_incident_digest(&owner_park))
        .bind(&turn_id)
        .bind(&now)
        .execute(db.pool())
        .await
        .unwrap();
        sqlx::query(
            "UPDATE agent_chat_turn_job SET status='succeeded',version=version+1 WHERE id=?",
        )
        .bind(&turn_id)
        .execute(db.pool())
        .await
        .unwrap();
        services::project_escalation::ProjectEscalationService::new(Arc::clone(&db))
            .after_wake_turn(&turn_id)
            .await
            .unwrap();
        service().project_once(100).await.unwrap();
        let needs: Vec<String> = sqlx::query_scalar(
            "SELECT details_json FROM attention_projection
         WHERE attention_type='human_input_required' AND status='open'",
        )
        .fetch_all(db.pool())
        .await
        .unwrap();
        assert_eq!(needs.len(), 1, "the ordinary blocker still escalates");
        assert!(needs[0].contains(&failure.id));
        assert!(
            !needs[0].contains(&task.id),
            "the owner park must not be escalated again"
        );
        assert_eq!(
            db.get_attention(&attention_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            "open"
        );
    }
}

#[tokio::test]
async fn a_task_leaving_its_owner_park_for_an_agent_cause_wakes_the_project_agent() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    identity(&db, &identity_id).await;
    let project_id = configured_project(&db, &identity_id, "owner-park-exit").await;
    let task = project_task(&db, &project_id, "Leaves the owner park").await;
    let now = now_rfc3339();
    let park = |kind: &str, status: &str, version: i64| db::UpdateTaskStatus {
        id: task.id.clone(),
        expected_version: version,
        status: status.into(),
        assignee_id: None,
        error_annotation: Some(Some(serde_json::json!({"type": kind}).to_string())),
        blocked_json: Some(Some(
            serde_json::json!({"kind": kind, "reason": kind, "created_at": now}).to_string(),
        )),
        failed_json: Some(None),
        updated_at: now_rfc3339(),
    };
    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
    TaskRepo::update_status(&*db, park("review_needs_owner", "review", task.version))
        .await
        .unwrap();
    service.project_once(100).await.unwrap();
    assert_eq!(service.sweep_once_at(&now_rfc3339()).await.unwrap(), 0);
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    TaskRepo::update_status(
        &*db,
        park("executor_failed", "in_progress", current.version),
    )
    .await
    .unwrap();
    service.project_once(100).await.unwrap();
    service.sweep_once_at(&now_rfc3339()).await.unwrap();

    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT attention_type, status FROM attention_projection
         WHERE attention_type IN ('review_needs_owner', 'execution_failed')
         ORDER BY attention_type",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    assert_eq!(
        rows,
        vec![
            ("execution_failed".to_owned(), "open".to_owned()),
            ("review_needs_owner".to_owned(), "resolved".to_owned()),
        ]
    );
    let linked: Vec<String> = sqlx::query_scalar(
        "SELECT a.attention_type FROM agent_wake_blocker b
         JOIN attention_projection a ON a.id=b.attention_id",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    assert_eq!(
        linked,
        vec!["execution_failed"],
        "the Agent-handled cause that replaced the owner park must wake the Project Agent"
    );
}

#[tokio::test]
async fn actionable_task_interruption_wakes_once_and_resolution_closes_attention() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    identity(&db, &identity_id).await;
    let project_id = configured_project(&db, &identity_id, "interruption-project").await;
    let task = project_task(&db, &project_id, "Recover actionable interruption").await;
    let execution_id = new_uuid_v4();
    let now = now_rfc3339();
    let blocked_json = serde_json::json!({
        "reason": "executor failed",
        "created_at": now,
        "kind": "executor_failed",
        "execution_id": execution_id
    })
    .to_string();
    let error_annotation = serde_json::json!({
        "type": "executor_failed",
        "recovery_actions": ["reexecute", "cancel_task"]
    })
    .to_string();
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: current.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(error_annotation)),
            blocked_json: Some(Some(blocked_json)),
            failed_json: Some(None),
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();

    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
    service.project_once(100).await.unwrap();

    let attention: (String, String, String) = sqlx::query_as(
        "SELECT attention_type, status, details_json FROM attention_projection
         WHERE attention_type = 'execution_failed' AND scope_id = ?",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(attention.0, "execution_failed");
    assert_eq!(attention.1, "open");
    let details: serde_json::Value = serde_json::from_str(&attention.2).unwrap();
    let task_service =
        services::TaskService::new_for_test(Arc::clone(&db), Arc::new(events::EventBus::new(32)));
    let expected = task_service
        .task_action_offers(&task.id, &api_types::Actor::agent(&identity_id))
        .await
        .unwrap()
        .available_actions;
    assert_eq!(
        details["recovery"]["actions"],
        serde_json::to_value(&expected).unwrap()
    );
    assert_eq!(
        expected
            .iter()
            .map(|offer| offer.action.verb())
            .collect::<Vec<_>>(),
        vec!["cancel", "restart"]
    );
    assert!(expected.iter().all(|offer| offer
        .parameters
        .iter()
        .any(|parameter| parameter.name == "reason" && parameter.required)));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event
             WHERE event_type = 'agent.wake.admitted' AND scope_id = ?",
        )
        .bind(&project_id)
        .fetch_one(db.pool())
        .await
        .unwrap(),
        1
    );

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: current.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(None),
            blocked_json: Some(None),
            failed_json: Some(None),
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    service.project_once(100).await.unwrap();

    let status: String = sqlx::query_scalar(
        "SELECT status FROM attention_projection
         WHERE attention_type = 'execution_failed' AND scope_id = ?",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(status, "resolved");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event
             WHERE event_type = 'agent.wake.admitted' AND scope_id = ?",
        )
        .bind(&project_id)
        .fetch_one(db.pool())
        .await
        .unwrap(),
        1,
        "resolution must not admit a second wake"
    );
}

#[tokio::test]
async fn completed_task_wakes_project_agent_until_readiness_is_reconciled() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    identity(&db, &identity_id).await;
    let project_id = configured_project(&db, &identity_id, "delivery-followup-project").await;
    let now = now_rfc3339();

    let task = project_task(&db, &project_id, "Delivered task").await;
    TaskRepo::update_status(
        &*db,
        db::UpdateTaskStatus {
            id: task.id.clone(),
            expected_version: task.version,
            status: "done".to_owned(),
            assignee_id: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    DomainEventRepo::append_event(
        &*db,
        CreateDomainEvent::task_transition(
            new_uuid_v4(),
            task.id.clone(),
            project_id.clone(),
            "in_progress",
            "done",
            Some("accept"),
            "system:workflow",
            "task completed",
            false,
            now,
            services::workflow::transition_event::transition_workflow_snapshot(
                &task,
                &default_workflow::default_workflow(),
                "in_progress",
                "done",
            )
            .unwrap(),
        ),
    )
    .await
    .unwrap();

    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
    service.project_once(100).await.unwrap();
    let attention: (String, String, String, Option<String>) = sqlx::query_as(
        "SELECT attention_type, status, recommended_action, identity_id
         FROM attention_projection WHERE scope_type = 'project' AND scope_id = ?",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(attention.0, "delivery_followup");
    assert_eq!(attention.1, "open");
    assert_eq!(attention.2, "reconcile_delivery");
    assert_eq!(attention.3.as_deref(), Some(identity_id.as_str()));
    let admitted_identity: String = sqlx::query_scalar(
        "SELECT json_extract(payload_json, '$.identity_id')
         FROM domain_event WHERE event_type = 'agent.wake.admitted'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(admitted_identity, identity_id);

    DomainEventRepo::append_event(
        &*db,
        CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "milestone.readiness.evaluated".to_owned(),
            entity_type: "project_milestone".to_owned(),
            entity_id: new_uuid_v4(),
            actor_type: "agent".to_owned(),
            actor_id: Some(identity_id),
            scope_type: "project".to_owned(),
            scope_id: project_id.clone(),
            correlation_id: new_uuid_v4(),
            causation_id: None,
            causation_depth: 1,
            dedupe_key: Some(new_uuid_v4()),
            payload_json: r#"{"result":"blocked"}"#.to_owned(),
            created_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    service.project_once(100).await.unwrap();
    let resolved: String = sqlx::query_scalar(
        "SELECT status FROM attention_projection
         WHERE attention_type = 'delivery_followup' AND scope_id = ?",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(resolved, "resolved");
}

#[tokio::test]
async fn expected_cancellation_is_audit_only_without_attention_or_wake() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    identity(&db, &identity_id).await;
    let profile_id: String =
        sqlx::query_scalar("SELECT selected_profile_id FROM agent_identity WHERE id = ?")
            .bind(&identity_id)
            .fetch_one(db.pool())
            .await
            .unwrap();
    let project_id = new_uuid_v4();
    let now = now_rfc3339();
    ProjectRepo::create(
        &*db,
        CreateProject {
            id: project_id.clone(),
            name: "stopped-execution-project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    sqlx::query(
        "UPDATE project_agent_binding
         SET identity_id = ?, profile_id = ?, state = 'active', wake_budget = 10,
             version = version + 1, updated_at = ?
         WHERE project_id = ? AND state = 'agent_setup_required'",
    )
    .bind(&identity_id)
    .bind(&profile_id)
    .bind(&now)
    .bind(&project_id)
    .execute(db.pool())
    .await
    .unwrap();
    let task = TaskRepo::create(
        &*db,
        CreateTask {
            id: new_uuid_v4(),
            project_id: project_id.clone(),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "Recover interrupted delivery".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "in_progress".to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    let execution_id = new_uuid_v4();
    ExecutionRepo::create(
        &*db,
        CreateExecution {
            id: execution_id.clone(),
            task_id: task.id.clone(),
            agent_id: Some(identity_id.clone()),
            role: "coder".to_owned(),
            status: ExecutionStatus::Cancelled,
            stop_reason: Some(db::StopReason::UserCancelled),
            stopped_by: Some("user:api".to_owned()),
            resume_policy: Some(ResumePolicy::None),
            stopped_at: Some(now.clone()),
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
    )
    .await
    .unwrap();
    DomainEventRepo::append_event(
        &*db,
        CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "execution.cancelled".to_owned(),
            entity_type: "task".to_owned(),
            entity_id: task.id.clone(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "project".to_owned(),
            scope_id: project_id.clone(),
            correlation_id: new_uuid_v4(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(new_uuid_v4()),
            payload_json: serde_json::json!({
                "execution_id": execution_id,
                "task_id": task.id,
                "role": "coder",
                "stop_reason": "user_cancelled",
                "error": "execution stopped for reassignment"
            })
            .to_string(),
            created_at: now,
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

    let attention_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM attention_projection
         WHERE attention_type = 'execution_failed' AND scope_id = ?",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    let admitted_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event
         WHERE event_type = 'agent.wake.admitted' AND scope_id = ?",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(attention_count, 0);
    assert_eq!(admitted_count, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_wake_lease WHERE scope_id = ?",)
            .bind(&project_id)
            .fetch_one(db.pool())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn review_ready_wakes_only_for_a_human_required_review_gate() {
    let db = database().await;
    // The default workflow's review gate is run by the reviewer Agent: the
    // Project Agent has nothing to decide, so no attention and no wake.
    let agent_reviewed =
        serde_json::to_string(&default_workflow::default_workflow()).expect("serialize workflow");
    assert!(
        review_attention_rows(&db, &agent_reviewed).await.is_empty(),
        "an agent-run review gate must not raise review_ready attention"
    );
    // The autonomous workflow's review gate is a user decision.
    let human_reviewed =
        serde_json::to_string(&default_autonomous_workflow::default_autonomous_workflow())
            .expect("serialize workflow");
    assert_eq!(
        review_attention_rows(&db, &human_reviewed).await,
        vec!["review_ready".to_owned()],
        "a human-required review gate must raise exactly one review_ready attention"
    );
}
