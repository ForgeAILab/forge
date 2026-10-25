use super::*;

#[tokio::test]
async fn both_scheduler_paths_record_both_project_wait_subcauses() {
    for production in [false, true] {
        for wait in [db::ProjectWait::AtCapacity, db::ProjectWait::WaitingOnOwner] {
            let db = Arc::new(sqlite_db().await);
            let repo_dir = TempDir::new().unwrap();
            let workspace_dir = TempDir::new().unwrap();
            let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
            let agent = seed_agent(&db, 4, DaemonStatus::Online, AgentStatus::Idle).await;
            let message = match wait {
                db::ProjectWait::AtCapacity => {
                    seed_task(&db, &project_id, "active", "in_progress", 0).await;
                    "project_at_capacity: waiting for a slot (1/1 active)"
                }
                db::ProjectWait::WaitingOnOwner => {
                    for _ in 0..2 {
                        let parked = seed_task(&db, &project_id, "owner review", "review", 0).await;
                        set_task_error_annotation(&db, &parked, r#"{"type":"review_needs_owner"}"#)
                            .await;
                    }
                    "project_waiting_on_owner: 2 parked tasks waiting on the owner"
                }
            };
            let queued = seed_task(&db, &project_id, "queued", "todo", 0).await;
            assign_role(&db, &queued.id, "coder", &agent).await;
            let project = set_project_active_task_limit(&db, &project_id, 1).await;
            let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
            let (dispatcher, mut rx) =
                build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
            let dispatched = if production {
                dispatcher.check_once().await.unwrap()
            } else {
                dispatcher
                    .dispatch_initial_tasks(&project, &workflow)
                    .await
                    .unwrap()
            };
            assert_eq!(dispatched, 0);
            assert!(rx.try_recv().is_err());
            let waiting = TaskRepo::get_by_id(&*db, &queued.id, false)
                .await
                .unwrap()
                .unwrap();
            let refusal = deferred_dispatch::current_dispatch_disposition(&waiting).unwrap();
            assert_eq!(refusal.project_wait, Some(wait), "production={production}");
            assert_eq!(refusal.safe_message, message);
            assert_eq!(refusal.capacity_reason().0, wait.reason());
            assert_eq!(waiting.condition.read().refusal.unwrap(), refusal);
        }
    }
}

#[tokio::test]
async fn retiring_machine_wait_records_typed_project_capacity() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    seed_task(&db, &project_id, "active", "in_progress", 0).await;
    let task = seed_task(&db, &project_id, "waiting", "in_progress", 0).await;
    set_project_active_task_limit(&db, &project_id, 1).await;
    deferred_dispatch::record_capacity_wait(&db, &task, crate::placement::CapacityWait::Machine)
        .await
        .unwrap();
    let waiting = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(
        crate::placement::machine_precheck::retire_wait(&db, &waiting)
            .await
            .unwrap()
    );
    let waiting = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    let refusal = waiting.condition.read().refusal.unwrap();
    assert_eq!(refusal.project_wait, Some(db::ProjectWait::AtCapacity));
    assert_eq!(
        refusal.safe_message,
        "project_at_capacity: waiting for a Project slot"
    );
}
