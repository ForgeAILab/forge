//! Tool result filtering, observation, and terminal denial caching.

use super::*;

/// A callback notified with the exact call id and result of one tool
/// invocation, before an [`ScopeToolComposition::observe_results`]-wrapped
/// composition returns it to the runtime.
pub type ToolResultObserver =
    Arc<dyn Fn(&ToolCallId, &Result<ToolOutcome, RuntimeError>) + Send + Sync>;

type DenialKey = (String, Option<String>, String);
type TurnDenials = Arc<Mutex<BTreeMap<DenialKey, Value>>>;

#[derive(Debug)]
pub(super) struct TerminalDenialTool {
    pub(super) inner: Arc<dyn Tool>,
    pub(super) denials: TurnDenials,
    pub(super) provider: Arc<dyn ForgeToolProvider>,
    pub(super) actor_identity_id: String,
    pub(super) scope: CanonicalScope,
}

#[async_trait]
impl Tool for TerminalDenialTool {
    fn spec(&self) -> ToolSpec {
        self.inner.spec()
    }

    fn normalize_arguments(&self, arguments: Value) -> Result<Value, RuntimeError> {
        self.inner.normalize_arguments(arguments)
    }

    async fn prepare(
        &self,
        arguments: Value,
        ctx: &PreparationContext,
    ) -> Result<PreparedToolCall, RuntimeError> {
        self.inner.prepare(arguments, ctx).await
    }

    async fn invoke(
        &self,
        prepared: PreparedToolCall,
        ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        let operation = prepared
            .arguments()
            .get("operation")
            .and_then(Value::as_str)
            .unwrap_or(FORGE_PUBLIC_WEB_SEARCH_TOOL)
            .to_owned();
        let key = (
            ctx.session.to_string(),
            ctx.turn.as_ref().map(ToString::to_string),
            operation.clone(),
        );
        let cached = self
            .denials
            .lock()
            .map_err(|_| RuntimeError::tool("Forge denial cache is unavailable"))?
            .get(&key)
            .cloned();
        if let Some(value) = cached {
            return Ok(tool_error_outcome(value));
        }
        // Never hold a cache lock across provider work. Already-running calls
        // evaluate independently; later calls observe a completed denial.
        let result = self.inner.invoke(prepared, ctx).await?;
        let outcome = if result.is_error {
            serde_json::from_value::<OrchestrationOutcome>(result.value.clone()).ok()
        } else {
            None
        };
        if let Some(cause) = outcome
            .as_ref()
            .filter(|outcome| {
                result.is_error
                    && outcome.code == OutcomeCode::PolicyDenied
                    && outcome.retry.as_ref().is_some_and(|retry| {
                        retry.action == RetryAction::None
                            && retry.scope == Some(api_types::RetryScope::Session)
                    })
            })
            .and_then(|outcome| outcome.denied_by.as_ref())
            .filter(|cause| cause.withdraws_operation())
        {
            self.denials
                .lock()
                .map_err(|_| RuntimeError::tool("Forge denial cache is unavailable"))?
                .insert(key, result.value.clone());
            if let Err(error) = self
                .provider
                .record_terminal_denial(
                    &self.actor_identity_id,
                    &self.scope,
                    ctx.session.as_str(),
                    &operation,
                    cause,
                )
                .await
            {
                tracing::warn!(operation, denied_by = %cause, error = %error,
                    "could not persist native terminal denial");
            }
        } else if !result.is_error {
            if let Err(error) = self
                .provider
                .clear_terminal_denials(
                    &self.actor_identity_id,
                    &self.scope,
                    ctx.session.as_str(),
                    &operation,
                )
                .await
            {
                tracing::warn!(operation, error = %error, "could not clear native terminal denial");
            }
        }
        Ok(result)
    }
}

#[async_trait]
pub(crate) trait ToolResultFilter: Send + Sync + fmt::Debug {
    async fn filter(
        &self,
        call_id: &ToolCallId,
        arguments: &Value,
        outcome: ToolOutcome,
    ) -> Result<ToolOutcome, RuntimeError>;
}

#[derive(Debug)]
pub(super) struct FilteredTool {
    pub(super) inner: Arc<dyn Tool>,
    pub(super) filter: Arc<dyn ToolResultFilter>,
}
#[async_trait]
impl Tool for FilteredTool {
    fn spec(&self) -> ToolSpec {
        self.inner.spec()
    }
    fn normalize_arguments(&self, arguments: Value) -> Result<Value, RuntimeError> {
        self.inner.normalize_arguments(arguments)
    }

    async fn prepare(
        &self,
        arguments: Value,
        ctx: &PreparationContext,
    ) -> Result<PreparedToolCall, RuntimeError> {
        self.inner.prepare(arguments, ctx).await
    }
    fn supports_interaction(&self) -> bool {
        self.inner.supports_interaction()
    }
    fn interaction_request(
        &self,
        prepared: &PreparedToolCall,
        origin: InteractionOrigin,
        deadline: Deadline,
    ) -> Result<Option<InteractionRequest>, RuntimeError> {
        self.inner.interaction_request(prepared, origin, deadline)
    }
    fn resolve_interaction(
        &self,
        prepared: &PreparedToolCall,
        response: &InteractionResponse,
    ) -> Result<ToolOutcome, RuntimeError> {
        self.inner.resolve_interaction(prepared, response)
    }
    async fn invoke(
        &self,
        prepared: PreparedToolCall,
        ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        let arguments = prepared.arguments().clone();
        let outcome = self.inner.invoke(prepared, ctx).await?;
        self.filter.filter(&ctx.call_id, &arguments, outcome).await
    }
}

pub(super) struct ObservedTool {
    pub(super) inner: Arc<dyn Tool>,
    pub(super) observer: ToolResultObserver,
}

impl fmt::Debug for ObservedTool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ObservedTool")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl Tool for ObservedTool {
    fn spec(&self) -> ToolSpec {
        self.inner.spec()
    }

    fn normalize_arguments(&self, arguments: Value) -> Result<Value, RuntimeError> {
        self.inner.normalize_arguments(arguments)
    }

    async fn prepare(
        &self,
        arguments: Value,
        ctx: &PreparationContext,
    ) -> Result<PreparedToolCall, RuntimeError> {
        self.inner.prepare(arguments, ctx).await
    }

    fn interaction_request(
        &self,
        prepared: &PreparedToolCall,
        origin: InteractionOrigin,
        deadline: Deadline,
    ) -> Result<Option<InteractionRequest>, RuntimeError> {
        self.inner.interaction_request(prepared, origin, deadline)
    }

    fn supports_interaction(&self) -> bool {
        self.inner.supports_interaction()
    }

    fn resolve_interaction(
        &self,
        prepared: &PreparedToolCall,
        response: &InteractionResponse,
    ) -> Result<ToolOutcome, RuntimeError> {
        let result = self.inner.resolve_interaction(prepared, response);
        (self.observer)(prepared.call_id(), &result);
        result
    }

    async fn invoke(
        &self,
        prepared: PreparedToolCall,
        ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        let call_id = ctx.call_id.clone();
        let result = self.inner.invoke(prepared, ctx).await;
        (self.observer)(&call_id, &result);
        result
    }
}
