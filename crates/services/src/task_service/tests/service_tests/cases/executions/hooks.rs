//! Before-work hooks, retries and one-time hook bypasses.
use super::*;

#[tokio::test]
async fn before_enter_runs_required_before_work_hook_before_role_dispatch() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus))
        .with_task_executor(Arc::new(NoDiffExecutor))
        .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let settings = json!({
        "lifecycle_hooks": {
            "before_work": [{
                "type": "script",
                "command": "printf required-ok > required-hook.out; exit 0",
                "timeout_seconds": 5,
                "blocking": true
            }]
        }
    });
    sqlx::query("UPDATE project SET settings = ?, updated_at = ? WHERE id = ?")
        .bind(settings.to_string())
        .bind(now_rfc3339())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project settings update");
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "todo".to_owned()).await;
    service
        .reassign_role(
            role_assignment_input(
                &task.id,
                crate::workflow::default_roles::CODER,
                Some(agent_id.clone()),
                None,
            ),
            false,
            false,
        )
        .await
        .expect("coder role assignment succeeds");

    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads after the role assignment")
        .expect("task exists");

    let transitioned = service
        .transition(task.id.clone(), "in_progress".to_owned(), task.version)
        .await
        .expect("required hook passes and transition succeeds");

    service.drain(&task.id).await.unwrap();
    let executions = ExecutionRepo::list_by_task(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 10,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("executions list");
    assert_eq!(executions.items.len(), 1);
    assert_eq!(
        executions.items[0].role,
        crate::workflow::default_roles::CODER
    );
    assert_eq!(
        executions.items[0].agent_id.as_deref(),
        Some(agent_id.as_str())
    );
    assert_eq!(transitioned.task.entry_barrier_json, None);

    let workspace =
        WorkspaceRepo::get_by_id(&*db, executions.items[0].workspace_id.as_deref().unwrap())
            .await
            .expect("workspace loads")
            .expect("workspace exists");
    let marker = std::fs::read_to_string(
        std::path::Path::new(
            &service
                .workspace_backend_router()
                .embedded_path(&db, &workspace)
                .await
                .expect("workspace path resolves"),
        )
        .join("required-hook.out"),
    )
    .expect("required hook marker exists");
    assert_eq!(marker, "required-ok");
}

#[tokio::test]
async fn before_enter_blocks_when_required_before_work_hook_fails() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus))
        .with_task_executor(Arc::new(NoDiffExecutor))
        .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let settings = json!({
        "lifecycle_hooks": {
            "before_work": [{
                "type": "script",
                "command": "echo preflight-out; echo preflight-err >&2; exit 9",
                "timeout_seconds": 5,
                "blocking": true
            }]
        }
    });
    sqlx::query("UPDATE project SET settings = ?, updated_at = ? WHERE id = ?")
        .bind(settings.to_string())
        .bind(now_rfc3339())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project settings update");
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "todo".to_owned()).await;
    service
        .reassign_role(
            role_assignment_input(
                &task.id,
                crate::workflow::default_roles::CODER,
                Some(agent_id),
                None,
            ),
            false,
            false,
        )
        .await
        .expect("coder role assignment succeeds");

    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads after the role assignment")
        .expect("task exists");

    let result = service
        .transition(task.id.clone(), "in_progress".to_owned(), task.version)
        .await
        .expect("required hook failure records a blocked entry");
    assert_eq!(result.task.status, "in_progress");
    let executions = ExecutionRepo::list_by_task(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 10,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("executions list");
    assert!(
        executions.items.is_empty(),
        "no execution should be created"
    );

    service.drain(&task.id).await.unwrap();
    let blocked = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(blocked.status, "in_progress");
    let barrier: serde_json::Value = serde_json::from_str(
        blocked
            .entry_barrier_json
            .as_deref()
            .expect("entry barrier remains blocked"),
    )
    .expect("entry barrier parses");
    assert_eq!(barrier["state"], "in_progress");
    assert_eq!(barrier["status"], "blocked");
    let annotation: serde_json::Value = serde_json::from_str(
        blocked
            .error_annotation
            .as_deref()
            .expect("blocking annotation is recorded"),
    )
    .expect("annotation parses");
    assert_eq!(annotation["type"], "before_work_hook_failed");
    assert_eq!(annotation["artifact"]["kind"], "hook");
    assert_eq!(annotation["hook"]["exit_code"], 9);
    assert_eq!(annotation["hook"]["stdout"], "preflight-out\n");
    assert_eq!(annotation["hook"]["stderr"], "preflight-err\n");
    assert!(annotation.get("recovery_actions").is_none());
    let offers = service
        .task_action_offers(&task.id, &Actor::user(UserActionSource::Test))
        .await
        .unwrap()
        .available_actions;
    assert!(offers
        .iter()
        .any(|offer| offer.reason == "entry_barrier_blocked"));
    assert!(offers.iter().any(|offer| offer.action.verb() == "approve"));
    let interruption_payload: String = sqlx::query_scalar(
        "SELECT payload_json FROM domain_event
         WHERE event_type = 'task.interruption_changed' AND entity_id = ?
         ORDER BY sequence DESC LIMIT 1",
    )
    .bind(&task.id)
    .fetch_one(db.pool())
    .await
    .expect("blocking annotation commits an interruption event");
    let interruption: serde_json::Value =
        serde_json::from_str(&interruption_payload).expect("interruption event parses");
    assert_eq!(
        interruption["material_blocker"]["requires_intervention"],
        true
    );
    assert_eq!(
        interruption["material_blocker"]["interruption"]["recovery_actions"],
        annotation["recovery_actions"]
    );
    let log_path = annotation["hook"]["log_path"]
        .as_str()
        .expect("hook log path recorded");
    assert!(
        std::path::Path::new(log_path).exists(),
        "hook log path should exist: {log_path}"
    );
}

#[tokio::test]
async fn retry_hook_reruns_blocked_before_enter_and_dispatches_when_it_passes() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus))
        .with_task_executor(Arc::new(NoDiffExecutor))
        .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let failing_settings = json!({
        "lifecycle_hooks": {
            "before_work": [{
                "type": "script",
                "command": "echo preflight-out; echo preflight-err >&2; exit 9",
                "timeout_seconds": 5,
                "blocking": true
            }]
        }
    });
    sqlx::query("UPDATE project SET settings = ?, updated_at = ? WHERE id = ?")
        .bind(failing_settings.to_string())
        .bind(now_rfc3339())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project settings update");
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "todo".to_owned()).await;
    service
        .reassign_role(
            role_assignment_input(
                &task.id,
                crate::workflow::default_roles::CODER,
                Some(agent_id.clone()),
                None,
            ),
            false,
            false,
        )
        .await
        .expect("coder role assignment succeeds");

    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads after the role assignment")
        .expect("task exists");

    service
        .transition(task.id.clone(), "in_progress".to_owned(), task.version)
        .await
        .expect("required hook failure records a blocked entry");
    service.drain(&task.id).await.unwrap();
    let blocked = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert!(blocked.entry_barrier_json.is_some());
    assert!(blocked.error_annotation.is_some());

    let passing_settings = json!({
        "lifecycle_hooks": {
            "before_work": [{
                "type": "script",
                "command": "printf retry-ok > retry-hook.out; exit 0",
                "timeout_seconds": 5,
                "blocking": true
            }]
        }
    });
    sqlx::query("UPDATE project SET settings = ?, updated_at = ? WHERE id = ?")
        .bind(passing_settings.to_string())
        .bind(now_rfc3339())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project settings update");

    let recovered = service
        .test_apply_action(task.id.clone(), api_types::TaskAction::retry(), None, None)
        .await
        .expect("retry hook recovers");

    let recovered = service.drain(&recovered.id).await.unwrap();
    assert_eq!(recovered.status, "in_progress");
    assert_eq!(recovered.entry_barrier_json, None);
    assert_eq!(recovered.error_annotation, None);
    let executions = ExecutionRepo::list_by_task(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 10,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("executions list");
    assert_eq!(executions.items.len(), 1);
    assert_eq!(
        executions.items[0].agent_id.as_deref(),
        Some(agent_id.as_str())
    );
    let workspace =
        WorkspaceRepo::get_by_id(&*db, executions.items[0].workspace_id.as_deref().unwrap())
            .await
            .expect("workspace loads")
            .expect("workspace exists");
    let marker = std::fs::read_to_string(
        std::path::Path::new(
            &service
                .workspace_backend_router()
                .embedded_path(&db, &workspace)
                .await
                .expect("workspace path resolves"),
        )
        .join("retry-hook.out"),
    )
    .expect("retry hook marker exists");
    assert_eq!(marker, "retry-ok");
}

#[tokio::test]
async fn retry_hook_after_manual_merge_repair_returns_to_fresh_review_without_worker_dispatch() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let mut workflow = crate::workflow::default_workflow::default_workflow();
    workflow
        .states
        .iter_mut()
        .find(|state| state.name == crate::workflow::default_states::REVIEW)
        .and_then(|state| state.gate_config.as_mut())
        .expect("review gate config")
        .requires_user_approval = Some(true);
    sqlx::query("UPDATE project SET workflow_definition = ? WHERE id = ?")
        .bind(serde_json::to_string(&workflow).expect("workflow serializes"))
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project workflow requires human review");
    let task = seed_task_with_status(
        &db,
        &project_id,
        crate::workflow::default_states::MERGING.to_owned(),
    )
    .await;
    let task = TaskRepo::set_review_passed_at_cas(
        &*db,
        &task.id,
        task.version,
        Some("2026-09-12T10:00:00Z".to_owned()),
        &now_rfc3339(),
    )
    .await
    .expect("review authority seeds");
    let annotation = api_types::TaskAnnotation::Blocking(api_types::TaskBlockingAnnotation {
        annotation_type: api_types::FailureKind::MergeConflict,
        blocking_reason: "manual task-worktree repair required".to_owned(),
        blocked_by: Some("manual_workspace_repair".to_owned()),
        blocked_at: Some(now_rfc3339()),
        blocked_execution_id: None,
        artifact: None,
        message: Some("repair and re-review".to_owned()),
        hook: None,
    });
    let task = TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(
                serde_json::to_string(&annotation).expect("annotation serializes"),
            )),
            blocked_json: Some(Some(
                json!({
                    "reason": "manual task-worktree repair required",
                    "kind": "merge_conflict"
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
    .expect("manual repair blocker seeds");

    let recovered = service
        .test_apply_action(
            task.id.clone(),
            api_types::TaskAction::retry(),
            Some("manual conflict repair committed".to_owned()),
            None,
        )
        .await
        .expect("manual repair returns through review");

    assert_eq!(recovered.status, crate::workflow::default_states::REVIEW);
    assert_eq!(recovered.review_passed_at, None);
    assert_eq!(recovered.error_annotation, None);
    assert_eq!(recovered.error_annotation, None);
    let entries = TransitionLogRepo::list_by_task(&*db, &task.id)
        .await
        .expect("transition log loads");
    assert!(entries.iter().any(|entry| {
        entry.from_state == crate::workflow::default_states::MERGING
            && entry.to_state == crate::workflow::default_states::MERGE_FAILED
            && entry.bridge.is_review_refresh()
    }));
    // Both repair rows are Workflow-authored, so the backfill rule
    // (`[review-refresh]` from a system actor) classifies history the same.
    let workflow_actor = api_types::Actor::system(api_types::SystemComponent::Workflow).display();
    let repair_rows: Vec<_> = entries
        .iter()
        .filter(|entry| entry.bridge.is_review_refresh())
        .map(|entry| {
            (
                entry.from_state.as_str(),
                entry.to_state.as_str(),
                entry.triggered_by.as_str(),
            )
        })
        .collect();
    assert_eq!(
        repair_rows,
        vec![
            ("merging", "merge_failed", workflow_actor.as_str()),
            ("merge_failed", "review", workflow_actor.as_str()),
        ]
    );
    let executions = ExecutionRepo::list_by_task(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 10,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("executions list");
    assert!(
        executions.items.is_empty(),
        "review refresh must not dispatch a merge-fix Worker"
    );
}

#[tokio::test]
async fn update_workspace_and_retry_hook_rebases_before_retrying_blocked_hook() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus))
        .with_task_executor(Arc::new(NoDiffExecutor))
        .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, _repo_id, repo_dir) = seed_project_repo(&db).await;
    let settings = json!({
        "lifecycle_hooks": {
            "before_work": [{
                "type": "script",
                "command": "test -f hook-marker.txt",
                "timeout_seconds": 5,
                "blocking": true
            }]
        }
    });
    sqlx::query("UPDATE project SET settings = ?, updated_at = ? WHERE id = ?")
        .bind(settings.to_string())
        .bind(now_rfc3339())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project settings update");
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "todo".to_owned()).await;
    service
        .reassign_role(
            role_assignment_input(
                &task.id,
                crate::workflow::default_roles::CODER,
                Some(agent_id.clone()),
                None,
            ),
            false,
            false,
        )
        .await
        .expect("coder role assignment succeeds");

    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads after the role assignment")
        .expect("task exists");

    service
        .transition(task.id.clone(), "in_progress".to_owned(), task.version)
        .await
        .expect("required hook failure records a blocked entry");
    service.drain(&task.id).await.unwrap();
    let blocked = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert!(blocked.entry_barrier_json.is_some());
    let workspace = WorkspaceRepo::get_by_task_id(&*db, &task.id)
        .await
        .expect("workspace loads")
        .expect("workspace exists");
    assert!(!std::path::Path::new(
        &service
            .workspace_backend_router()
            .embedded_path(&db, &workspace)
            .await
            .expect("workspace path resolves")
    )
    .join("hook-marker.txt")
    .exists());

    std::fs::write(repo_dir.path().join("hook-marker.txt"), "updated\n").expect("marker writes");
    run_git(repo_dir.path(), &["add", "-A"]);
    run_git(repo_dir.path(), &["commit", "-m", "add hook marker"]);

    let recovered = service
        .test_apply_action(
            task.id.clone(),
            api_types::TaskAction::Retry {
                reason: None,
                fresh_session: None,
                refresh_workspace: Some(true),
                reset_budget: None,
                guidance: None,
            },
            None,
            None,
        )
        .await
        .expect("update workspace and retry hook recovers");

    let recovered = service.drain(&recovered.id).await.unwrap();
    assert_eq!(recovered.status, "in_progress");
    assert_eq!(recovered.entry_barrier_json, None);
    assert_eq!(recovered.error_annotation, None);
    assert!(std::path::Path::new(
        &service
            .workspace_backend_router()
            .embedded_path(&db, &workspace)
            .await
            .expect("workspace path resolves")
    )
    .join("hook-marker.txt")
    .exists());
}

#[tokio::test]
async fn skip_hook_once_bypasses_only_one_dispatch_attempt() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus))
        .with_task_executor(Arc::new(NoDiffExecutor))
        .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let settings = json!({
        "lifecycle_hooks": {
            "before_work": [{
                "type": "script",
                "command": "test -f hook-marker.txt",
                "timeout_seconds": 5,
                "blocking": true
            }]
        }
    });
    sqlx::query("UPDATE project SET settings = ?, updated_at = ? WHERE id = ?")
        .bind(settings.to_string())
        .bind(now_rfc3339())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project settings update");
    let planner_id = seed_agent(&db).await;
    let coder_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "todo".to_owned()).await;
    service
        .reassign_role(
            role_assignment_input(
                &task.id,
                crate::workflow::default_roles::PLANNER,
                Some(planner_id.clone()),
                None,
            ),
            false,
            false,
        )
        .await
        .expect("planner role assignment succeeds");
    service
        .reassign_role(
            role_assignment_input(
                &task.id,
                crate::workflow::default_roles::CODER,
                Some(coder_id.clone()),
                None,
            ),
            false,
            false,
        )
        .await
        .expect("coder role assignment succeeds");

    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads after the role assignment")
        .expect("task exists");

    service
        .transition(task.id.clone(), "planning".to_owned(), task.version)
        .await
        .expect("blocking hook failure records a blocked entry");
    service.drain(&task.id).await.unwrap();
    let blocked = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert!(blocked.entry_barrier_json.is_some());

    let recovered = service
        .test_apply_action(
            task.id.clone(),
            api_types::TaskAction::Approve {
                reason: None,
                override_checks: Some(true),
            },
            Some("Skip the known fixture hook once".to_owned()),
            None,
        )
        .await
        .expect("skip hook once recovers");
    let recovered = service.drain(&recovered.id).await.unwrap();
    assert_eq!(recovered.status, "planning");
    assert_eq!(recovered.entry_barrier_json, None);
    assert_eq!(recovered.error_annotation, None);

    let executions = ExecutionRepo::list_by_task(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 10,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("executions list");
    assert_eq!(executions.items.len(), 1);
    assert_eq!(
        executions.items[0].agent_id.as_deref(),
        Some(planner_id.as_str())
    );

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    let transitioned = service
        .transition(
            task.id.clone(),
            "planning".to_owned(),
            (current.version, None, true),
        )
        .await
        .expect("second transition runs hook normally");
    assert_eq!(transitioned.task.status, "planning");
    service.drain(&task.id).await.unwrap();
    let blocked_again = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert!(blocked_again.entry_barrier_json.is_some());
    let annotation: serde_json::Value = serde_json::from_str(
        blocked_again
            .error_annotation
            .as_deref()
            .expect("blocking annotation is recorded"),
    )
    .expect("annotation parses");
    assert_eq!(annotation["type"], "before_work_hook_failed");
}
