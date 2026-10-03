//! Permanent adaptation of audit_21c1_repro: both the original notifying
//! outcomes and silent metadata mutations share the durable consumer.
use super::*;
use api_types::{Actor, TaskAction, UserActionSource};
use db::{
    CreateProject, CreateTask, DomainEventRepo, ProjectRepo, ReviewStatus, TaskMetadataMutation,
};
use serde_json::json;

async fn database() -> Arc<SqliteDb> {
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    Arc::new(SqliteDb::new(pool))
}
async fn project(db: &SqliteDb) -> db::Project {
    let mut workflow = crate::workflow::default_workflow::default_workflow();
    for state in &mut workflow.states {
        state.hooks.on_enter.clear();
        state.hooks.on_exit.clear();
    }
    ProjectRepo::create(
        db,
        CreateProject {
            id: new_uuid_v4(),
            name: "Notification parity".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: serde_json::to_string(&workflow).unwrap(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap()
}
async fn task(db: &SqliteDb, project: &db::Project, title: &str, status: &str) -> db::Task {
    TaskRepo::create(
        db,
        CreateTask {
            id: new_uuid_v4(),
            project_id: project.id.clone(),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: title.to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: status.to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap()
}
async fn review(db: &SqliteDb, task: &db::Task, status: ReviewStatus) -> db::Review {
    let execution = new_uuid_v4();
    sqlx::query("INSERT INTO execution (id, task_id, role, status, created_at, updated_at) VALUES (?, ?, 'executor', 'completed', ?, ?)")
        .bind(&execution).bind(&task.id).bind(now_rfc3339()).bind(now_rfc3339()).execute(db.pool()).await.unwrap();
    ReviewRepo::create(
        db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: execution,
            attempt_number: 1,
            status,
            step_results_json:
                json!({"ci_steps":[], "auditor":{"verdict":"fail", "reason":"needs tests"}})
                    .to_string(),
            started_at: now_rfc3339(),
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap()
}
async fn update_condition(
    db: &SqliteDb,
    task: &db::Task,
    blocked: Option<Value>,
    failed: Option<Value>,
    annotation: Option<Value>,
) {
    TaskRepo::update(
        db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: annotation.map(|v| Some(v.to_string())),
            blocked_json: Some(blocked.map(|v| v.to_string())),
            failed_json: Some(failed.map(|v| v.to_string())),
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
}
fn ci_context(
    db: Arc<SqliteDb>,
    bus: Arc<EventBus>,
    project: &db::Project,
    task: &db::Task,
    service: crate::TaskService,
) -> crate::workflow::HookContext {
    crate::workflow::HookContext {
        task_id: task.id.clone(),
        project_id: project.id.clone(),
        from_state: "in_progress".to_owned(),
        to_state: "review".to_owned(),
        workspace_backend_router: crate::diff::embedded_read_router_for_test(Arc::clone(&db)),
        db,
        event_bus: bus,
        gate_config: None,
        workflow: Arc::new(crate::workflow::engine::WorkflowEngine::resolve_workflow(
            &project.workflow_definition,
        )),
        project_version: Some(project.version),
        project_workflow_definition: Some(project.workflow_definition.clone()),
        triggered_by: Actor::system(api_types::SystemComponent::Test),
        review_runner: None,
        merge_service: None,
        cleanup_scheduler: None,
        task_service: service,
        daemon_connections: None,
        workspace_exec_locks: None,
        terminal_activity: None,
        workspace_root: std::env::temp_dir(),
        repo_cache_locks: None,
        workspace_id: None,
        agent_id: None,
        execution_id: None,
        state_config: json!({}),
    }
}

#[tokio::test]
async fn exact_old_notification_set_excludes_hold_ci_restore_and_human_review_writes() {
    let db = database().await;
    let project = project(&db).await;
    let bus = Arc::new(EventBus::new(128));
    let service = crate::TaskService::new_for_test(Arc::clone(&db), Arc::clone(&bus));

    // The audit's exact owner-Hold mutation, now through the real 2.2 action.
    let held = task(&db, &project, "owner hold", "todo").await;
    crate::deferred_dispatch::record_dispatch_disposition(
        &db,
        &held,
        "machine_capacity",
        "waiting for capacity",
    )
    .await
    .unwrap();
    let held = TaskRepo::get_by_id(&*db, &held.id, false)
        .await
        .unwrap()
        .unwrap();
    service
        .perform_task_action(
            &held.id,
            TaskAction::Hold {
                reason: Some("held by owner".to_owned()),
            },
            held.version,
        )
        .await
        .unwrap();
    let held = TaskRepo::get_by_id(&*db, &held.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(held.blocked_json.as_deref().unwrap()).unwrap()["kind"],
        "manual_stop"
    );

    // The actual infrastructure writer covers retry exhaustion, unavailable
    // owners and reset-required CI, including kinds also used by notifying work.
    for (title, retry, reset, attempts) in [
        ("ci unavailable", false, false, 0),
        ("ci exhausted", true, false, 4),
        ("ci reset", false, true, 0),
    ] {
        let ci = task(&db, &project, title, "review").await;
        sqlx::query("UPDATE task SET entry_barrier_json = ? WHERE id = ?")
            .bind(json!({"infrastructure_attempts":attempts}).to_string())
            .bind(&ci.id)
            .execute(db.pool())
            .await
            .unwrap();
        let ci = TaskRepo::get_by_id(&*db, &ci.id, false)
            .await
            .unwrap()
            .unwrap();
        let ctx = ci_context(
            Arc::clone(&db),
            Arc::clone(&bus),
            &project,
            &ci,
            service.clone(),
        );
        service
            .annotate_review_ci_interruption(&ci, &ctx, "CI owner unavailable", retry, reset)
            .await
            .unwrap();
    }
    // Failed queued action restoration may restore ANY prior kind, including
    // a kind that otherwise legitimately produces task.failed/task.blocked.
    for failed in [false, true] {
        let restored = task(
            &db,
            &project,
            if failed {
                "restored failure"
            } else {
                "restored block"
            },
            "in_progress",
        )
        .await;
        TaskRepo::mutate_metadata(
            &*db,
            &restored.id,
            Some(restored.version),
            vec![TaskMetadataMutation::Set {
                key: "queued_recovery".to_owned(),
                value: json!({"id":"refused-action"}),
            }],
            &now_rfc3339(),
        )
        .await
        .unwrap();
        let restored = TaskRepo::get_by_id(&*db, &restored.id, false)
            .await
            .unwrap()
            .unwrap();
        TaskRepo::restore_queued_recovery(
            &*db,
            db::RestoreQueuedRecovery {
                task_id: restored.id,
                expected_version: restored.version,
                queued_recovery_id: "refused-action".to_owned(),
                error_annotation: Some(
                    json!({"type":"executor_failed","blocking_reason":"dispatch refused"})
                        .to_string(),
                ),
                blocked_json: (!failed).then(|| {
                    json!({"kind":"executor_failed","reason":"restored block"}).to_string()
                }),
                failed_json: failed.then(|| {
                    json!({"kind":"executor_failed","reason":"restored failure"}).to_string()
                }),
                updated_at: now_rfc3339(),
            },
        )
        .await
        .unwrap();
    }
    for approve in [true, false] {
        let human = task(
            &db,
            &project,
            if approve {
                "human approval"
            } else {
                "human rejection"
            },
            "review",
        )
        .await;
        let r = review(&db, &human, ReviewStatus::AwaitingHuman).await;
        if approve {
            service
                .approve_review_as(&human.id, Actor::user(UserActionSource::Api))
                .await
                .unwrap();
        } else {
            // The repo-free audit fixture reaches the authoritative human
            // settlement, then stops at admission to the active reject target.
            // No executor or external repository is needed to test this write.
            let result = service
                .reject_review_as(
                    &human.id,
                    Some("human decision".to_owned()),
                    Actor::user(UserActionSource::Api),
                )
                .await;
            assert!(matches!(
                result,
                Err(crate::ServiceError::MissingPrimaryRepo { .. })
            ));
            assert_eq!(
                ReviewRepo::get_by_id(&*db, &r.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                ReviewStatus::Failed
            );
        }
        let events = db.list_events_after(0, 100).await.unwrap();
        let event = events
            .iter()
            .find(|e| e.event_type == "review.status_changed" && e.entity_id == r.id)
            .unwrap();
        assert_eq!(event.actor_type, "user");
    }
    let manual = task(&db, &project, "manual review pass", "review").await;
    let r = review(&db, &manual, ReviewStatus::Failed).await;
    ReviewRepo::create_manual_pass_with_task_authority(
        &*db,
        db::CreateManualReviewPass {
            id: new_uuid_v4(),
            source_review_id: r.id,
            source_review_updated_at: r.updated_at,
            task_id: manual.id,
            candidate_execution_id: r.execution_id,
            step_results_json: json!({"ci_steps":[],"manual_override":{"actor_type":"user"}})
                .to_string(),
            expected_task_version: manual.version,
            expected_task_status: manual.status,
            expected_project_version: project.version,
            expected_workflow_definition: project.workflow_definition.clone(),
            occurred_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();

    // Original notifying outcomes: committed transitions and the exact
    // outcome mutations made by block_task, fail_task and merge failure.
    let done = task(&db, &project, "completed task", "review").await;
    service
        .transition(
            &done.id,
            "done".to_owned(),
            crate::task_service::TransitionOptions {
                version: done.version,
                reason: None,
                triggered_by: Actor::user(UserActionSource::Api),
                rejection: false,
                defer_dispatch_seconds: None,
            },
        )
        .await
        .unwrap();
    let blocked = task(&db, &project, "blocked task", "in_progress").await;
    update_condition(
        &db,
        &blocked,
        Some(json!({"kind":"before_work_hook_failed","reason":"real work hook failed"})),
        None,
        None,
    )
    .await;
    let failed = task(&db, &project, "failed task", "in_progress").await;
    update_condition(
        &db,
        &failed,
        None,
        Some(json!({"kind":"executor_failed","reason":"work failed"})),
        None,
    )
    .await;
    let merged = task(&db, &project, "merge task", "merging").await;
    update_condition(
        &db,
        &merged,
        None,
        None,
        Some(json!({"type":"merge_conflict","message":"conflict","detected_at":now_rfc3339()})),
    )
    .await;
    for reason in ["crash_recovery", "agent_timeout", "shutdown"] {
        let recovered = task(&db, &project, reason, "in_progress").await;
        update_condition(
            &db,
            &recovered,
            None,
            None,
            Some(json!({"type":"recovery_required","blocking_reason":reason})),
        )
        .await;
    }
    for status in [ReviewStatus::Passed, ReviewStatus::Failed] {
        let t = task(
            &db,
            &project,
            if status == ReviewStatus::Passed {
                "runner pass"
            } else {
                "runner fail"
            },
            "review",
        )
        .await;
        let r = review(&db, &t, ReviewStatus::Running).await;
        ReviewRepo::update_status_with_task_authority(
            &*db,
            &r.id,
            status.clone(),
            r.step_results_json,
            Some(now_rfc3339()),
            &now_rfc3339(),
            t.version,
            (status == ReviewStatus::Passed).then(now_rfc3339),
            db::ReviewEventOrigin::Runner,
        )
        .await
        .unwrap();
    }
    let notifications = Arc::new(NotificationService::new(Arc::clone(&db), bus));
    WorkerRuntime::new(Arc::clone(&db), notifications)
        .run_once(100)
        .await
        .unwrap();
    let rows: Vec<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT event_type, title, body FROM notification ORDER BY event_type, title",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    assert_eq!(
        rows,
        vec![
            (
                "merge.failed".to_owned(),
                "merge task".to_owned(),
                Some("conflict".to_owned())
            ),
            (
                "review.failed".to_owned(),
                "Review failed: runner fail".to_owned(),
                Some("needs tests".to_owned())
            ),
            (
                "review.passed".to_owned(),
                "Review passed: runner pass".to_owned(),
                None
            ),
            (
                "task.blocked".to_owned(),
                "blocked task".to_owned(),
                Some("real work hook failed".to_owned())
            ),
            ("task.done".to_owned(), "completed task".to_owned(), None),
            (
                "task.failed".to_owned(),
                "failed task".to_owned(),
                Some("work failed".to_owned())
            ),
            (
                "task.recovery_required".to_owned(),
                "agent_timeout".to_owned(),
                Some("Needs manual recovery after an agent heartbeat timeout".to_owned())
            ),
            (
                "task.recovery_required".to_owned(),
                "crash_recovery".to_owned(),
                Some("Needs manual recovery after a server restart".to_owned())
            ),
        ]
    );
}
