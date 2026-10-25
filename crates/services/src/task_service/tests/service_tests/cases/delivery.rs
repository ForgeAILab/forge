//! Provider diagnostics cannot establish a worktree delivery failure.

use super::super::*;

struct ProviderFailureExecutor(String);

#[async_trait]
impl TaskExecutor for ProviderFailureExecutor {
    async fn execute(
        &self,
        _ctx: ExecutionContext,
    ) -> std::result::Result<ExecutionResult, ExecutorError> {
        Ok(ExecutionResult {
            status: ExecutionOutcome::Failed,
            error: Some(self.0.clone()),
            failure_class: Some(executors::ExecutionFailureClass::TaskFailed),
            ..Default::default()
        })
    }

    async fn cancel(&self, _execution_id: &str) -> std::result::Result<(), ExecutorError> {
        Ok(())
    }
}

#[tokio::test]
async fn provider_failure_echoing_uncommitted_diagnostic_does_not_claim_preserved_work() {
    for error in [
        format!(
            "provider failed: {}",
            crate::task_service::execution::UNCOMMITTED_WORKTREE_FAILURE
        ),
        format!(
            "provider quoted: {} (request rejected)",
            crate::task_service::execution::UNCOMMITTED_WORKTREE_FAILURE
        ),
    ] {
        let db = Arc::new(sqlite_db().await);
        let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(16)));
        let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
        let agent_id = seed_agent(&db).await;
        let task = service
            .create_task(
                project_id,
                "Provider diagnostic is not delivery evidence",
                Some("implement the task".to_owned()),
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();
        let claimed = service
            .claim_task(task.id.clone(), Assignee::Agent(agent_id), None)
            .await
            .unwrap();
        let execution = service
            .run_execution(
                claimed.execution.id,
                &ProviderFailureExecutor(error.clone()),
            )
            .await
            .unwrap();
        assert_eq!(execution.status, ExecutionStatus::Failed);
        assert_eq!(execution.error.as_deref(), Some(error.as_str()));
        let comments = db::TaskCommentRepo::list_comments(
            &*db,
            &task.id,
            db::PageRequest {
                cursor: None,
                limit: 100,
                include_total: false,
                sort_by: db::SortBy::CreatedAt,
                sort_order: db::SortOrder::Asc,
            },
        )
        .await
        .unwrap();
        assert!(
            comments
                .items
                .iter()
                .all(|comment| !comment.content.contains("worktree was preserved")),
            "provider wording must not produce the uncommitted-worktree corrective comment"
        );
    }
}
