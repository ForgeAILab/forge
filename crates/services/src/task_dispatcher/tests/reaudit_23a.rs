//! Re-audit 2.3a regressions: same-state recovery markers never move the Task,
//! so they must not supersede queued cascades (status-epoch fence), and the
//! owner Advance clear publishes its resolving interruption event.
use super::*;
use db::{TaskStepRepo, TransitionLogRepo};

fn system_options(version: i64, reason: &str) -> crate::task_service::TransitionOptions {
    crate::task_service::TransitionOptions {
        bridge: Default::default(),
        version,
        reason: Some(reason.to_owned()),
        triggered_by: Actor::system(SystemComponent::General),
        rejection: false,
        defer_dispatch_seconds: None,
    }
}

/// Same shape as `recovery_marker()` in task_service/execution/recovery.rs.
fn same_state_marker(task_id: &str, state: &str) -> db::CreateTransitionLog {
    db::CreateTransitionLog {
        id: new_uuid_v4(),
        task_id: task_id.to_owned(),
        from_state: state.to_owned(),
        to_state: state.to_owned(),
        trigger_name: Some("retry".to_owned()),
        triggered_by: Actor::user(api_types::UserActionSource::Test).display(),
        bridge: Default::default(),
        trigger_reason: "retry window reset".to_owned(),
        hook_results_json: None,
        rejection: false,
        created_at: now_rfc3339(),
    }
}

/// Production writer `clear_retry_exhausted_blocking_metadata_with_marker`
/// (TaskRepo::update_with_recovery_marker) does not move the Task; its marker
/// row must not drop the pending review -> merging cascade.
#[tokio::test]
async fn reaudit_23a_recovery_marker_without_status_change_keeps_cascade() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_dir = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let task = seed_task(&db, &project_id, "marker", "in_progress", 1).await;
    let (dispatcher, _rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let result = dispatcher
        .task_service
        .transition(
            task.id.clone(),
            "review".to_owned(),
            system_options(task.version, "agent completed"),
        )
        .await
        .unwrap();
    assert_eq!(result.task.status, "review");
    assert_eq!(result.pending_steps, 1);

    let marked = dispatcher
        .task_service
        .clear_retry_exhausted_blocking_metadata_with_marker(
            &result.task,
            same_state_marker(&task.id, "review"),
        )
        .await
        .unwrap();
    assert_eq!(
        marked.status, "review",
        "the marker writer does not move the Task"
    );

    let settled = dispatcher.task_service.drain(&task.id).await.unwrap();
    let steps = db.task_steps(&task.id).await.unwrap();
    println!(
        "reaudit steps: {:?}",
        steps
            .iter()
            .map(|s| (&s.status, s.expected_epoch, &s.last_error))
            .collect::<Vec<_>>()
    );
    for _ in 0..3 {
        dispatcher.check_once_and_drain().await.unwrap();
    }
    let after = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    println!("reaudit settled={} after={}", settled.status, after.status);
    assert_eq!(steps[0].status, "done", "a same-state marker is not a move");
    assert_eq!(after.status, "merging");
}

/// The repo-level helper used for every recovery marker must not either.
#[tokio::test]
async fn reaudit_23a_insert_recovery_marker_keeps_cascade() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_dir = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let task = seed_task(&db, &project_id, "marker2", "in_progress", 1).await;
    let (dispatcher, _rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    let result = dispatcher
        .task_service
        .transition(
            task.id.clone(),
            "review".to_owned(),
            system_options(task.version, "agent completed"),
        )
        .await
        .unwrap();
    assert_eq!(result.pending_steps, 1);
    TransitionLogRepo::insert_recovery_marker(
        &*db,
        &task.id,
        "review",
        "retry",
        "user:test",
        "retry window reset",
    )
    .await
    .unwrap();
    let settled = dispatcher.task_service.drain(&task.id).await.unwrap();
    let steps = db.task_steps(&task.id).await.unwrap();
    println!(
        "reaudit2 steps: {:?} settled={}",
        steps
            .iter()
            .map(|s| (&s.status, &s.last_error))
            .collect::<Vec<_>>(),
        settled.status
    );
    assert_eq!(steps[0].status, "done", "a same-state marker is not a move");
    assert_eq!(settled.status, "merging");
}

/// The owner's annotation clear appends a durable `task.interruption_changed`
/// (requires_intervention=false) in its own transaction; the attention service
/// resolves "execution_failed" items on it.
#[tokio::test]
async fn reaudit_23a_advance_annotation_clear_emits_interruption_event() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_dir = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let task = seed_task(&db, &project_id, "advanced", "todo", 1).await;
    let annotated = TaskRepo::update(
        &*db,
        UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(
                serde_json::json!({"type": "executor_failed", "message": "agent stopped"})
                    .to_string(),
            )),
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    let (dispatcher, _rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    let project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    let actor = Actor::user(api_types::UserActionSource::Test);
    let workflow = crate::workflow::engine::WorkflowEngine::resolve_workflow_for_task(
        &annotated,
        &project.workflow_definition,
        &actor,
    );
    let before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM domain_event WHERE entity_id=? AND event_type='task.interruption_changed'")
        .bind(&task.id).fetch_one(db.pool()).await.unwrap();
    let advanced = dispatcher
        .task_service
        .advance_task_condition(
            &annotated,
            &workflow,
            "planning".to_owned(),
            "owner advance".to_owned(),
            actor,
        )
        .await
        .unwrap();
    assert!(advanced.error_annotation.is_none());
    let persisted = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(
        persisted.error_annotation.is_none(),
        "annotation cleared in the DB"
    );
    let cleared: Vec<String> = sqlx::query_scalar("SELECT payload_json FROM domain_event WHERE entity_id=? AND event_type='task.interruption_changed' ORDER BY rowid")
        .bind(&task.id).fetch_all(db.pool()).await.unwrap();
    println!("reaudit3 before={before} events_after={:?}", cleared);
    assert!(
        cleared
            .iter()
            .skip(before as usize)
            .any(|p| p.contains("\"requires_intervention\":false")),
        "the annotation clear must publish a resolving task.interruption_changed"
    );
}

/// Default-workflow owner Retry on the planning gate (reject target =
/// planning) with no planner: continue_task_process transitions
/// planning -> planning (auto_cascade_on_unassigned_role enqueues
/// planning -> in_progress), then inserts its same-state recovery marker,
/// which must not supersede that cascade.
#[tokio::test]
async fn reaudit_23a_planning_retry_marker_keeps_own_cascade() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_dir = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let task = seed_task(&db, &project_id, "planning-retry", "planning", 1).await;
    let annotated = TaskRepo::update(
        &*db,
        UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(
                serde_json::json!({"type": "executor_failed", "message": "planner stopped"})
                    .to_string(),
            )),
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    let (dispatcher, _rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    let retried = dispatcher
        .task_service
        .continue_task_process(annotated, Some("retry planning".to_owned()), None)
        .await;
    println!(
        "reaudit4 retry result: {:?}",
        retried.as_ref().map(|t| (&t.status, t.version))
    );
    let retried = retried.unwrap();
    assert_eq!(retried.status, "planning");
    let settled = dispatcher.task_service.drain(&task.id).await.unwrap();
    let steps = db.task_steps(&task.id).await.unwrap();
    println!(
        "reaudit4 steps: {:?} settled={}",
        steps
            .iter()
            .map(|s| (&s.expected_status, &s.status, &s.last_error))
            .collect::<Vec<_>>(),
        settled.status
    );
    let rows: Vec<(String, String, Option<String>, Option<i64>)> = sqlx::query_as("SELECT from_state,to_state,trigger_name,status_epoch FROM transition_log WHERE task_id=? ORDER BY created_at,id")
        .bind(&task.id).fetch_all(db.pool()).await.unwrap();
    println!(
        "reaudit4 expected_epochs={:?} log={:?}",
        steps.iter().map(|s| s.expected_epoch).collect::<Vec<_>>(),
        rows
    );
    let mut after = settled.status.clone();
    for i in 0..3 {
        dispatcher.check_once_and_drain().await.unwrap();
        after = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap()
            .status;
        println!("reaudit4 sweep {i}: {after}");
    }
    assert!(
        steps.iter().all(|s| s.status != "superseded"),
        "own retry marker superseded the cascade"
    );
    assert_eq!(settled.status, "in_progress");
    let _ = after;
}

/// Same as above with a coder assigned: the retry still reaches in_progress.
#[tokio::test]
async fn reaudit_23a_planning_retry_marker_heal_check_with_coder() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_dir = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "planning-retry2", "planning", 1).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    let annotated = TaskRepo::update(
        &*db,
        UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(
                serde_json::json!({"type": "executor_failed", "message": "planner stopped"})
                    .to_string(),
            )),
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    let (dispatcher, _rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    let retried = dispatcher
        .task_service
        .continue_task_process(annotated, Some("retry planning".to_owned()), None)
        .await
        .unwrap();
    let settled = dispatcher.task_service.drain(&task.id).await.unwrap();
    let steps = db.task_steps(&task.id).await.unwrap();
    println!(
        "reaudit5 retried={} annotation={:?} steps={:?} settled={}",
        retried.status,
        settled.error_annotation,
        steps
            .iter()
            .map(|s| (&s.status, &s.last_error))
            .collect::<Vec<_>>(),
        settled.status
    );
    assert!(steps.iter().all(|s| s.status != "superseded"));
    assert_eq!(settled.status, "in_progress");
    for i in 0..3 {
        let _ = dispatcher.check_once_and_drain().await;
        let now = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        println!(
            "reaudit5 sweep {i}: {} annotation={:?}",
            now.status, now.error_annotation
        );
    }
}
