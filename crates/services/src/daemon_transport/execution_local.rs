use std::sync::Arc;

use async_trait::async_trait;
use tracing::Instrument;

use crate::daemon_transport::providers::ExecutionProvider;
use crate::{Result, TaskService};

#[derive(Clone)]
pub struct EmbeddedExecutionProvider {
    task_service: Arc<TaskService>,
    task_executor: Arc<dyn executors::TaskExecutor>,
}

impl EmbeddedExecutionProvider {
    /// Facts for the server's built-in execution paths. Admission checks
    /// installation, authentication, and policy separately from adapter support.
    pub(crate) fn adapter_capabilities(
        executor_type: &str,
    ) -> api_types::ExecutorAdapterCapabilityFacts {
        use executors::ExecutorKind;
        let Ok(kind) = executor_type.parse::<ExecutorKind>() else {
            return Default::default();
        };
        let session_executor = matches!(
            kind,
            ExecutorKind::Embedded
                | ExecutorKind::Codex
                | ExecutorKind::ClaudeCode
                | ExecutorKind::Cursor
                | ExecutorKind::Opencode
                | ExecutorKind::Gemini
                | ExecutorKind::Smith
        );
        api_types::ExecutorAdapterCapabilityFacts {
            structured_events: session_executor,
            usage: matches!(
                kind,
                ExecutorKind::Embedded
                    | ExecutorKind::Codex
                    | ExecutorKind::ClaudeCode
                    | ExecutorKind::Smith
            ),
            resume: session_executor,
            cancel_ack: true,
            terminal_observed: true,
        }
    }

    pub fn new(
        task_service: Arc<TaskService>,
        task_executor: Arc<dyn executors::TaskExecutor>,
    ) -> Self {
        Self {
            task_service,
            task_executor,
        }
    }
}

#[async_trait]
impl ExecutionProvider for EmbeddedExecutionProvider {
    async fn start(
        &self,
        params: api_types::ExecutionStartParams,
    ) -> Result<api_types::ExecutionStartResult> {
        let execution_id = params.execution_id.clone();
        let task_service = Arc::clone(&self.task_service);
        let task_executor = Arc::clone(&self.task_executor);
        let dispatch_span = tracing::info_span!(
            "embedded_execution_dispatch",
            execution_id = %execution_id,
        );
        tokio::spawn(
            async move {
                if let Err(error) = task_service
                    .run_execution(execution_id.clone(), task_executor.as_ref())
                    .await
                {
                    tracing::error!(%error, "embedded execution dispatch failed");
                    let _ = task_service
                        .fail_execution_before_dispatch(&execution_id, error.to_string())
                        .await;
                    return;
                }
                if let Err(error) = task_service
                    .maybe_cascade_executor_completion(&execution_id)
                    .await
                {
                    tracing::error!(%error, "embedded execution cascade failed");
                }
            }
            .instrument(dispatch_span),
        );
        Ok(api_types::ExecutionStartResult {
            execution_id: params.execution_id,
            accepted: true,
        })
    }

    async fn cancel(
        &self,
        params: api_types::ExecutionCancelParams,
    ) -> Result<api_types::ExecutionCancelResult> {
        self.task_executor.cancel(&params.execution_id).await?;
        Ok(api_types::ExecutionCancelResult {
            execution_id: params.execution_id,
            cancelled: true,
        })
    }
}
