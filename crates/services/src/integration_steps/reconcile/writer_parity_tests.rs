use crate::worker_runtime::queue::writer_parity::case;

#[tokio::test]
async fn integration_admission() {
    case!("integration-admission", "merging", f, {
        let merge = std::sync::Arc::new(crate::MergeService::new(f.db.clone(), Default::default()));
        f.ctx
            .task_service
            .clone()
            .with_merge_service(merge)
            .reconcile_integration_admission(&f.task)
            .await
            .unwrap();
    });
}

#[tokio::test]
async fn bounded_subtask_admission() {
    case!("subtask-admission", "merging", f, {
        let mut workflow = (*f.ctx.workflow).clone();
        workflow.states.retain(|state| state.name != "done");
        sqlx::query("UPDATE project SET workflow_definition=? WHERE id=?")
            .bind(serde_json::to_string(&workflow).unwrap())
            .bind(&f.task.project_id)
            .execute(f.db.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE task_step SET attempts=?,payload_json=? WHERE id=?")
            .bind(crate::worker_runtime::queue::MAX_STEP_ATTEMPTS)
            .bind(serde_json::json!({"operation":"reconcile_integration_admission"}).to_string())
            .bind(&f.step.id)
            .execute(f.db.pool())
            .await
            .unwrap();
        assert!(!f
            .ctx
            .task_service
            .finish_subtask_in_merge_state(&f.task, &workflow)
            .await
            .unwrap());
    });
}
