//! Native inquiry admission and execution.

use super::*;

impl CoordinationToolProvider {
    /// Return what Task runs actually reported: worklog entries with the
    /// execution and role that wrote them, and the artifacts those runs
    /// captured.
    ///
    /// Captured text comes back inline. That is the point of the operation: an
    /// Agent that cannot run anything still has to be able to read what the run
    /// found before it cites that run as authority, and before it decides the
    /// outcome needs a corrective Task.
    /// Whether this caller may dispatch an inquiry: the runner, the owning
    /// account and the chat the run record hangs off.
    ///
    /// The registry's MainChat rule confines this to a Main Chat: it rejects a
    /// Project Chat outright and requires an Account scope's id to be the
    /// caller's own, so an inquiry can only ever be run against the account
    /// that dispatched it.
    async fn inquiry_admission(
        &self,
        _actor_identity_id: &str,
        scope: &CanonicalScope,
    ) -> Result<(Arc<dyn InquiryRunner>, String, String), AgentHostError> {
        let runner = self.inquiry_runner_handle().ok_or_else(|| {
            AgentHostError::Unsupported("inquiries are not available on this server".to_owned())
        })?;
        let account_id = self
            .authorization
            .main_account_target(scope)
            .await
            .map_err(native_scope_error)?;
        // The run record hangs off the conversation the user is watching, so
        // an inquiry is only dispatchable from a chat, never from a bare
        // Account session (which is what an inquiry sub-agent itself holds).
        match scope.scope_type {
            CanonicalScopeType::AgentChat => Ok((runner, account_id, scope.scope_id.clone())),
            _ => Err(AgentHostError::Authority(
                "inquiries are dispatched from a Main Chat".to_owned(),
            )),
        }
    }

    /// Dispatch one ephemeral inquiry sub-agent and block on its findings.
    pub(super) async fn inquiry_run(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        input: operation_registry::main_reads::InquiryQuery,
    ) -> Result<Value, AgentHostError> {
        let (runner, account_id, chat_id) =
            self.inquiry_admission(actor_identity_id, scope).await?;
        let title = input.title.trim().to_owned();
        let question = input.question.trim().to_owned();
        // Preserve semantic whitespace checks and trimming in the handler.
        if title.is_empty() {
            return Err(AgentHostError::Unsupported(
                "an inquiry needs a title".to_owned(),
            ));
        }
        if question.is_empty() {
            return Err(AgentHostError::Unsupported(
                "an inquiry needs a question".to_owned(),
            ));
        }
        let context = input
            .context
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned);

        let chat_id_for_log = chat_id.clone();
        let outcome = runner
            .dispatch(
                InquiryRequest {
                    chat_id,
                    // The turn job is not addressable from inside a tool
                    // call; the chat binding is what the run record needs.
                    turn_job_id: None,
                    identity_id: actor_identity_id.to_owned(),
                    account_id,
                    title,
                    question,
                    context,
                },
                tokio_util::sync::CancellationToken::new(),
            )
            .await
            .inspect_err(|error| {
                // A bare `internal_failure` with nothing in the log is not
                // debuggable, and the model can only see the safe message.
                tracing::warn!(%error, chat_id = %chat_id_for_log, "inquiry dispatch failed");
            })
            .map_err(service_error)?;

        Ok(json!({
            "inquiry_id": outcome.inquiry_id,
            "status": outcome.status.to_string(),
            "findings": outcome.findings,
            "findings_path": outcome.findings_path,
            "duration_ms": outcome.duration_ms,
            // Disjoint counters: context size is input + cache_read +
            // cache_write. They are reported separately so neither the model
            // nor the analytics rollup can double-count a cached prefix.
            "token_usage": {
                "input_tokens": outcome.input_tokens,
                "output_tokens": outcome.output_tokens,
                "cache_read_tokens": outcome.cache_read_tokens,
                "cache_write_tokens": outcome.cache_write_tokens,
            },
        }))
    }
}
