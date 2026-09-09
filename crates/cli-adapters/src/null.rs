use async_trait::async_trait;
use executors::{
    AvailabilityInfo, AvailabilityStatus, CodingExecutorAdapter, DiscoverContext,
    DiscoveredOptions, ExecutionContext, ExecutionOutcome, ExecutionResult, ExecutorError,
    ExecutorKind, NullConfig,
};
use std::time::Duration;

pub struct NullAdapter;

impl NullAdapter {
    pub fn new() -> Self {
        Self
    }
}

impl Default for NullAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CodingExecutorAdapter for NullAdapter {
    fn kind(&self) -> ExecutorKind {
        ExecutorKind::Null
    }

    fn check_availability(&self) -> AvailabilityInfo {
        AvailabilityInfo {
            status: AvailabilityStatus::Authenticated,
            authenticated_at: None,
            config_path: None,
        }
    }

    async fn discover_options(
        &self,
        _ctx: DiscoverContext,
    ) -> Result<DiscoveredOptions, ExecutorError> {
        Ok(DiscoveredOptions::default())
    }

    async fn execute(&self, ctx: ExecutionContext) -> Result<ExecutionResult, ExecutorError> {
        let config: NullConfig =
            serde_json::from_value(ctx.agent_config.clone()).unwrap_or_default();
        tokio::time::sleep(Duration::from_secs(config.delay_seconds)).await;
        Ok(ExecutionResult {
            status: ExecutionOutcome::Completed,
            after_sha: None,
            agent_session_id: None,
            summary: Some("Null executor completed successfully.".to_owned()),
            error: None,
            usage_reports: Vec::new(),
            ..Default::default()
        })
    }

    async fn cancel(&self, _execution_id: &str) -> Result<(), ExecutorError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn null_adapter_completes_after_delay() {
        let adapter = NullAdapter::new();
        let start = std::time::Instant::now();

        let result = adapter
            .execute(ExecutionContext {
                task_id: "task".to_owned(),
                execution_id: "execution".to_owned(),
                worktree_path: ".".to_owned(),
                description: "test".to_owned(),
                agent_config: serde_json::json!({"delay_seconds": 1}),
                logs_path: "/dev/null".to_owned(),
                heartbeat_interval_seconds: 30,
                max_turns: None,
                log_sender: None,
            })
            .await
            .expect("null adapter executes");

        assert_eq!(result.status, ExecutionOutcome::Completed);
        assert!(start.elapsed() >= Duration::from_secs(1));
    }
}
