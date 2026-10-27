use crate::worker_runtime::queue::writer_parity::case;

#[tokio::test]
async fn workflow_guard() {
    case!("workflow-guard", "in_progress", f, {
        let execution = f.execution(db::ExecutionStatus::Failed).await;
        f.ctx
            .task_service
            .annotate_workflow_guard_block(&execution, &f.task, "guard", "rejected")
            .await
            .unwrap();
    });
}

#[tokio::test]
async fn guard_metadata() {
    case!("guard-metadata", "in_progress", f, {
        let mut execution = f.execution(db::ExecutionStatus::Failed).await;
        let at = db::now_rfc3339();
        db::AgentRepo::create(
            &*f.db,
            db::CreateAgent {
                id: "agent".into(),
                name: "agent".into(),
                description: None,
                executor_type: "null".into(),
                model: None,
                reasoning_effort: None,
                permission_policy: None,
                prompt_template: None,
                capabilities_json: "[]".into(),
                config_json: "{}".into(),
                credential_ref: None,
                daemon_id: None,
                max_concurrent_tasks: 1,
                heartbeat_interval_seconds: 30,
                max_missed_heartbeats: 3,
                status: db::AgentStatus::Idle,
                last_heartbeat_at: None,
                is_default: false,
                paused: false,
                owner_id: None,
                visibility: "global".into(),
                created_at: at.clone(),
                updated_at: at.clone(),
            },
        )
        .await
        .unwrap();
        db::TaskRoleAssignmentRepo::assign(
            &*f.db,
            db::CreateTaskRoleAssignment {
                id: "assignment".into(),
                task_id: f.task.id.clone(),
                role_name: "coder".into(),
                assignee_type: Some(db::AssigneeKind::Agent),
                assignee_id: Some("agent".into()),
                created_at: at.clone(),
                updated_at: at,
            },
        )
        .await
        .unwrap();
        sqlx::query("UPDATE execution SET agent_id='agent',agent_session_id='session',updated_at=? WHERE id=?").bind(db::now_rfc3339()).bind(&execution.id).execute(f.db.pool()).await.unwrap();
        execution = db::ExecutionRepo::get_by_id(&*f.db, &execution.id)
            .await
            .unwrap()
            .unwrap();
        let state = f
            .ctx
            .workflow
            .states
            .iter()
            .find(|state| state.name == f.task.status)
            .unwrap();
        let current = db::TaskRepo::get_by_id(&*f.db, &f.task.id, false)
            .await
            .unwrap()
            .unwrap();
        let result = f
            .ctx
            .task_service
            .handle_executor_completion_guard_rejection(
                &execution, &current, state, "guard", "rejected",
            )
            .await;
        let row = db::TaskRepo::get_by_id(&*f.db, &f.task.id, false)
            .await
            .unwrap()
            .unwrap();
        let metadata: serde_json::Value =
            serde_json::from_str(row.metadata_json.as_ref().unwrap()).unwrap();
        assert_eq!(
            metadata["last_workflow_guard_execution_id"], execution.id,
            "{result:?}"
        );
        assert_eq!(metadata["last_workflow_guard_name"], "guard");
    });
}
