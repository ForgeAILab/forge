use crate::worker_runtime::queue::lane_conditions::case;

#[tokio::test]
async fn review_recovery_parks() {
    for execution in [Some("execution"), None] {
        case!(
            &format!("review-recovery-budget-{}", execution.is_some()),
            "review",
            f,
            {
                f.ctx
                    .task_service
                    .review_failure_target_with_config(
                        &f.task,
                        execution,
                        Some(&serde_json::json!({"retry_budgets":{"review":0}})),
                    )
                    .await
                    .unwrap();
            }
        );
        case!(
            &format!("review-recovery-target-{}", execution.is_some()),
            "review",
            f,
            {
                let mut workflow = (*f.ctx.workflow).clone();
                workflow.states.retain(|state| state.name != "in_progress");
                sqlx::query("UPDATE project SET workflow_definition=? WHERE id=?")
                    .bind(serde_json::to_string(&workflow).unwrap())
                    .bind(&f.task.project_id)
                    .execute(f.db.pool())
                    .await
                    .unwrap();
                f.ctx
                    .task_service
                    .review_failure_target(&f.task, execution)
                    .await
                    .unwrap();
            }
        );
    }
}
