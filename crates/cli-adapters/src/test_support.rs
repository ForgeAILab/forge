//! Hermetic adapter fixtures for service and API tests.

use async_trait::async_trait;
use executors::{
    AdapterRegistry, AvailabilityInfo, AvailabilityStatus, CodingExecutorAdapter, DiscoverContext,
    DiscoveredOptions, ExecutionContext, ExecutionResult, ExecutorError, ExecutorKind,
};

/// An adapter whose availability is supplied by the fixture, without CLI or
/// credential discovery. AI execution must be supplied by the test separately.
pub struct TestAdapter {
    kind: ExecutorKind,
    availability: AvailabilityStatus,
    local_executor: Option<Box<dyn CodingExecutorAdapter>>,
}

impl TestAdapter {
    pub fn new(kind: ExecutorKind, availability: AvailabilityStatus) -> Self {
        let local_executor: Option<Box<dyn CodingExecutorAdapter>> = match kind {
            ExecutorKind::Shell => Some(Box::new(crate::ShellAdapter::new())),
            ExecutorKind::Null => Some(Box::new(crate::NullAdapter::new())),
            _ => None,
        };
        Self {
            kind,
            availability,
            local_executor,
        }
    }
}

#[async_trait]
impl CodingExecutorAdapter for TestAdapter {
    fn kind(&self) -> ExecutorKind {
        self.kind.clone()
    }

    fn check_availability(&self) -> AvailabilityInfo {
        AvailabilityInfo {
            status: self.availability.clone(),
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
        match &self.local_executor {
            Some(adapter) => adapter.execute(ctx).await,
            None => Err(ExecutorError::Other(format!(
                "test must inject execution behavior for {}",
                self.kind
            ))),
        }
    }

    async fn cancel(&self, execution_id: &str) -> Result<(), ExecutorError> {
        match &self.local_executor {
            Some(adapter) => adapter.cancel(execution_id).await,
            None => Ok(()),
        }
    }
}

/// Available built-in CLI families for fixtures. Shell retains its local execution
/// behavior; no adapter consults the host for availability or options.
pub fn test_registry() -> AdapterRegistry {
    let mut registry = AdapterRegistry::new();
    for kind in [
        ExecutorKind::Shell,
        ExecutorKind::Codex,
        ExecutorKind::ClaudeCode,
        ExecutorKind::Cursor,
        ExecutorKind::Opencode,
        ExecutorKind::Gemini,
        ExecutorKind::Smith,
    ] {
        registry.register(Box::new(TestAdapter::new(
            kind,
            AvailabilityStatus::Authenticated,
        )));
    }
    registry
}
