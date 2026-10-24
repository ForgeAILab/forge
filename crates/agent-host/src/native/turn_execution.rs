//! Native turn execution, cancellation, event delivery, and usage reporting.

use super::*;

pub(super) struct ActiveNativeSession {
    generation: String,
    session: SessionHandle,
}

/// Cancelling a caller may drop the backend future while the runtime's
/// independently spawned driver is still active. Keep cancellation and
/// registry cleanup tied to that future's lifetime.
struct ActiveNativeTurn {
    session: SessionHandle,
    runtime_session_id: String,
    generation: String,
    active: Arc<Mutex<HashMap<String, ActiveNativeSession>>>,
    finished: bool,
}

impl ActiveNativeTurn {
    fn finish(&mut self) {
        remove_active_turn(&self.active, &self.runtime_session_id, &self.generation);
        self.finished = true;
    }
}

fn remove_active_turn(
    active: &Mutex<HashMap<String, ActiveNativeSession>>,
    runtime_session_id: &str,
    generation: &str,
) {
    let Ok(mut active) = active.lock() else {
        return;
    };
    if active
        .get(runtime_session_id)
        .is_some_and(|entry| entry.generation == generation)
    {
        active.remove(runtime_session_id);
    }
}

impl Drop for ActiveNativeTurn {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        self.session.cancel_session(CancelReason::Shutdown);
        let session = self.session.clone();
        let active = Arc::clone(&self.active);
        let runtime_session_id = self.runtime_session_id.clone();
        let generation = self.generation.clone();
        tokio::spawn(async move {
            let _ = session.shutdown().await;
            remove_active_turn(&active, &runtime_session_id, &generation);
        });
    }
}

#[async_trait]
impl AgentSessionBackend for NativeAgentRuntimeBackend {
    fn capabilities(&self, scope: &CanonicalScope) -> BackendCapabilities {
        // Capabilities are scope-level, so Task advertises the worker/planner
        // persistence ceiling. A reviewer turn narrows that further when its
        // role-bound session is composed.
        let mode = turn_context_mode(scope, None);
        BackendCapabilities {
            native_runtime: true,
            persistent_session: mode.persistent_session,
            protected_checkpoints: mode.persistent_session,
            lcm: mode.lcm,
            cancel: true,
            steer: true,
            workspace: scope.workspace_access,
        }
    }

    async fn run_turn(
        &self,
        request: AgentTurnRequest,
        sink: Arc<dyn TurnEventSink>,
    ) -> Result<AgentTurnOutput, AgentHostError> {
        // Demo: no agent runs inside the server process.
        executors::server_commands::ensure_agents_allowed()
            .map_err(|error| AgentHostError::Authority(error.to_string()))?;
        if request.cancellation.is_cancelled() {
            return Err(AgentHostError::Runtime("turn cancelled".to_owned()));
        }
        let id = SessionId::new(&request.runtime_session_id);
        let _conversion_admission = self.protected_store.foreground_session(&id).await?;
        let PreparedNativeRuntime {
            runtime,
            context_mode,
            saved,
            lcm_link,
            tool_result_summaries,
            needs_adoption,
            summary_cap: _,
            topic_reads,
        } = match self.prepare_runtime(&request, None, true).await {
            Ok(prepared) => prepared,
            Err(error @ (AgentHostError::Runtime(_) | AgentHostError::ProtectedPersistence)) => {
                return Err(self
                    .retired_open_error(&request.forge_session_id, error)
                    .await);
            }
            Err(error) => return Err(error),
        };
        let start = if !context_mode.persistent_session {
            let mut start = StartSession::ephemeral(Vec::new());
            start.session_id = Some(id);
            start
        } else if saved {
            let start = StartSession::resume(id);
            if needs_adoption {
                start.with_lcm_policy(agent_runtime::harness::LcmRecoveryPolicy::Adopt)
            } else {
                start
            }
        } else {
            StartSession::create(id, request.history)
        };
        #[cfg(feature = "test-support")]
        {
            let barrier = self.open_barrier.lock().expect("open barrier lock").take();
            if let Some(barrier) = barrier {
                barrier.wait().await;
                barrier.wait().await;
            }
        }
        let session = match runtime.start_session(start).await {
            Ok(session) => session,
            Err(error) if error.kind == agent_runtime::core::error::ErrorKind::Conflict => {
                return Err(self
                    .retired_open_error(&request.forge_session_id, host_runtime_error(error))
                    .await);
            }
            Err(error) => return Err(host_runtime_error(error)),
        };
        let mut topic_reads_guard = topic_reads.as_ref().map(|_| TopicReadTurnGuard {
            failed: Arc::clone(&self.failed_topic_reads),
            runtime_session_id: request.runtime_session_id.clone(),
            completed: false,
        });
        // A persistent session restores its accumulated usage ledger, so
        // everything already in it was reported by the turn that made the
        // call. Only records appended from here on belong to this turn.
        let usage_baseline = session.snapshot().usage.records().len();
        if let Some(reads) = &topic_reads {
            reads.begin_turn(session.history().len());
        }
        let mut events = session.subscribe();
        if request.cancellation.is_cancelled() {
            return Err(AgentHostError::Runtime("turn cancelled".to_owned()));
        }
        let generation = db::new_uuid_v4();
        {
            let mut active = self.active.lock().map_err(|_| {
                AgentHostError::Runtime("active session registry failed".to_owned())
            })?;
            if active.contains_key(&request.runtime_session_id) {
                return Err(AgentHostError::Runtime(
                    "a turn is already active for this runtime session".to_owned(),
                ));
            }
            active.insert(
                request.runtime_session_id.clone(),
                ActiveNativeSession {
                    generation: generation.clone(),
                    session: session.clone(),
                },
            );
        }
        let mut active_turn = ActiveNativeTurn {
            session: session.clone(),
            runtime_session_id: request.runtime_session_id.clone(),
            generation,
            active: Arc::clone(&self.active),
            finished: false,
        };
        let input = session.with_history(|history| retry_aware_input(history, request.input));
        let turn = session
            .send(UserInput::text(input))
            .map_err(host_runtime_error)?;
        let turn_id = turn.id().clone();
        let mut last_turn_error: Option<RuntimeError> = None;
        let mut provider_failure = None;
        let mut provider_auth_rejected = false;
        let mut context_overflow = false;
        let mut cleanup = Vec::new();
        let mut terminal_published = false;
        let finish_result = loop {
            tokio::select! {
                biased;
                _ = request.cancellation.cancelled() => {
                    turn.interrupt(CancelReason::UserRequested);
                    let result = session.shutdown().await;
                    terminal_published = result.is_ok();
                    cleanup.push(("shutdown", result));
                    break Ok(TurnFinish::Cancelled { reason: CancelReason::UserRequested });
                }
                event = events.next() => {
                    let Some(event) = event else {
                        break Err(AgentHostError::Runtime(
                            "runtime event stream ended before completion".to_owned(),
                        ));
                    };
                    if event.turn.as_ref() != Some(&turn_id) {
                        continue;
                    }
                    match &event.payload {
                        RuntimeEvent::CachePlanChanged { first_changed_fragment, .. } => {
                            tracing::debug!(runtime_session_id = %request.runtime_session_id, ?first_changed_fragment, "native topic cache prefix change");
                            sink.cache_plan_changed(first_changed_fragment.as_deref()).await;
                        }
                        RuntimeEvent::TextDelta { text, .. } => sink.text_delta(text).await,
                        RuntimeEvent::ReasoningDelta { text, redacted, .. } => {
                            sink.reasoning_delta(text, *redacted).await;
                        }
                        RuntimeEvent::ToolCallRequested {
                            call,
                            name,
                            argument_keys,
                            arguments,
                            ..
                        } => {
                            let preview = arguments
                                .as_ref()
                                .map(crate::build_tool_argument_preview)
                                .unwrap_or_default();
                            sink.tool_call_started(call.as_str(), name, argument_keys, &preview)
                                .await;
                        }
                        RuntimeEvent::ToolCallCompleted {
                            call,
                            name,
                            is_error,
                        } => {
                            let summary = tool_result_summaries
                                .lock()
                                .ok()
                                .and_then(|mut summaries| summaries.remove(call.as_str()))
                                .unwrap_or_else(|| {
                                    ToolResultSummary::unclassified(*is_error, call.as_str())
                                });
                            sink.tool_call_finished(call.as_str(), name, *is_error, &summary)
                                .await;
                        }
                        RuntimeEvent::ProviderAttemptFinished { error, .. } => {
                            provider_failure = error.as_ref().map(provider_turn_failure);
                            provider_auth_rejected = error.as_ref().is_some_and(|error| {
                                error.kind == agent_runtime::core::provider::ProviderErrorKind::Auth
                            });
                        }
                        RuntimeEvent::BudgetFailure { category: BudgetCategory::Input, .. } => {
                            context_overflow = true;
                        }
                        RuntimeEvent::Error { error } => {
                            last_turn_error = Some(error.clone());
                            #[cfg(feature = "test-support")]
                            {
                                let barrier = self.failure_barrier.lock().expect("failure barrier lock").take();
                                if let Some(barrier) = barrier {
                                    barrier.wait().await;
                                    barrier.wait().await;
                                }
                            }
                        }
                        RuntimeEvent::TurnCompleted { finish, .. } => break Ok(finish.clone()),
                        _ => {}
                    }
                }
            }
        };
        if finish_result.is_err() {
            cleanup.push(("shutdown", session.shutdown().await));
        } else if cleanup.is_empty() {
            turn.completed().await;
            let persist = session.persist().await;
            terminal_published = persist.is_ok();
            let persist_failed = persist.is_err();
            cleanup.push(("persist", persist));
            if persist_failed {
                // Shutdown closes journal leases even when its final save is fenced.
                cleanup.push(("shutdown", session.shutdown().await));
            }
        }
        active_turn.finish();
        let primary_failed = !matches!(
            finish_result,
            Ok(TurnFinish::Completed | TurnFinish::NeedsInput { .. })
        );
        let cleanup_result = native_turn_cleanup(primary_failed, cleanup);
        let finish = finish_result?;
        cleanup_result?;
        if terminal_published && context_mode.persistent_session {
            self.protected_store
                .journals()
                .post_terminal(&request.runtime_session_id)
                .await;
        }

        let pending_interaction_id = match finish {
            TurnFinish::Completed => None,
            TurnFinish::NeedsInput { ref request } => Some(request.to_string()),
            TurnFinish::Cancelled { .. } | TurnFinish::LimitReached { .. } | TurnFinish::Failed => {
                None
            }
        };
        let history = session.history();
        if matches!(
            finish,
            TurnFinish::Completed | TurnFinish::NeedsInput { .. }
        ) {
            if let Some(reads) = &topic_reads {
                reads.commit(&history).await?;
            }
            if let Some(guard) = topic_reads_guard.as_mut() {
                guard.complete();
            }
        }
        let text = history
            .iter()
            .rev()
            .find(|message| message.role == Role::Assistant)
            .map(|message| {
                message
                    .content
                    .iter()
                    .filter_map(ContentPart::as_text)
                    .collect::<Vec<_>>()
                    .join("")
            })
            .unwrap_or_default();
        let snapshot = session.snapshot();
        let context_manifest =
            RuntimeContextManifestLink::from_snapshot(&snapshot).map(|manifest| match lcm_link {
                Some((timeline_id, binding_revision)) => manifest.with_lcm_binding(
                    timeline_id,
                    binding_revision,
                    FORGE_LCM_STORE_REVISION,
                ),
                None => manifest,
            });
        let turn_records = turn_usage_records(snapshot.usage.records(), usage_baseline);
        let mut usage = agent_runtime::core::usage::UsageDelta::new();
        for record in turn_records {
            usage.merge(&record.delta);
        }
        let output_tokens = usage
            .get(CounterKind::Output)
            .checked_add(usage.get(CounterKind::Reasoning))
            .ok_or_else(|| AgentHostError::Runtime("usage counter overflow".to_owned()))?;
        let usage_reports = turn_records
            .iter()
            .enumerate()
            .filter(|(_, record)| {
                matches!(
                    record.source,
                    UsageSource::ProviderAttempt | UsageSource::SemanticSummary
                )
            })
            .map(|(offset, record)| {
                let counters = record_counters(&record.delta)?;
                let request_id = record.provenance.request.as_ref().map(ToString::to_string);
                let attempt_id = record.provenance.attempt.as_ref().map(ToString::to_string);
                // The record's position in the session ledger, which never
                // changes, keeps the id unique across the session's turns.
                let report_id = format!(
                    "native:{}:{}:{}:{}",
                    request.runtime_session_id,
                    record.provenance.purpose.as_deref().unwrap_or("turn"),
                    request_id
                        .as_deref()
                        .or(attempt_id.as_deref())
                        .unwrap_or(turn_id.as_str()),
                    usage_baseline + offset
                );
                Ok(AgentTurnUsageReport {
                    report_id,
                    request_id,
                    attempt_id,
                    provider_id: Some(request.provider.provider.clone()),
                    model_id: Some(request.provider.model.clone()),
                    input_tokens: counters.map(|counters| counters[0]),
                    output_tokens: counters.map(|counters| counters[1]),
                    cache_read_tokens: counters.map(|counters| counters[2]),
                    cache_write_tokens: counters.map(|counters| counters[3]),
                    telemetry_state: if counters.is_some() {
                        AgentTurnTelemetryState::Metered
                    } else {
                        AgentTurnTelemetryState::Unmetered
                    },
                    failed: record.provenance.failed,
                })
            })
            .collect::<Result<Vec<_>, AgentHostError>>()?;
        let output = AgentTurnOutput {
            runtime_session_id: request.runtime_session_id,
            text,
            // Disjoint by contract (see `AgentTurnOutput::input_tokens`): the
            // runtime's `input_tokens()` folds the cached and cache-write
            // prefixes back in, which would double-count them against the
            // disjoint counters below once they reach the usage ledger.
            input_tokens: usage.get(CounterKind::InputUncached),
            output_tokens,
            cache_read_tokens: usage.get(CounterKind::InputCached),
            cache_write_tokens: usage.get(CounterKind::CacheWrite),
            telemetry_state: if usage_reports
                .iter()
                .any(|report| report.telemetry_state == AgentTurnTelemetryState::Metered)
            {
                AgentTurnTelemetryState::Metered
            } else {
                AgentTurnTelemetryState::Unmetered
            },
            usage_reports,
            context_manifest,
            pending_interaction_id,
        };

        // Preserve the reports observed before a failed/cancelled provider
        // turn leaves the runtime.  The caller owns the domain terminal CAS,
        // so returning them alongside the failure lets it settle only after
        // that CAS wins (or mark the invocation pending when cancellation
        // won first).  Reports are deliberately absent from the compact
        // error variants used before a provider attempt starts.
        match finish {
            TurnFinish::Completed | TurnFinish::NeedsInput { .. } => Ok(output),
            TurnFinish::Cancelled { .. } => Err(AgentHostError::RuntimeWithUsage {
                message: "turn cancelled".to_owned(),
                failure: api_types::TurnFailure::Unclassified,
                provider_auth_rejected: false,
                usage_reports: output.usage_reports,
            }),
            TurnFinish::LimitReached { limit } => Err(AgentHostError::RuntimeWithUsage {
                failure: if context_overflow {
                    api_types::TurnFailure::ContextOverflow
                } else {
                    provider_failure.unwrap_or_else(|| {
                        AgentHostError::TurnLimitReached {
                            limit: limit.into(),
                        }
                        .turn_failure()
                    })
                },
                message: format!(
                    "runtime turn limit reached: {}",
                    AgentTurnLimit::from(limit)
                ),
                provider_auth_rejected,
                usage_reports: output.usage_reports,
            }),
            TurnFinish::Failed => Err(AgentHostError::RuntimeWithUsage {
                failure: if context_overflow {
                    api_types::TurnFailure::ContextOverflow
                } else {
                    provider_failure.unwrap_or_else(|| {
                        last_turn_error
                            .as_ref()
                            .map(runtime_turn_failure)
                            .unwrap_or(api_types::TurnFailure::Unclassified)
                    })
                },
                message: match last_turn_error {
                    Some(detail) => format!("turn failed: {detail}"),
                    None => "turn failed".to_owned(),
                },
                provider_auth_rejected,
                usage_reports: output.usage_reports,
            }),
        }
    }

    async fn cancel(&self, runtime_session_id: &str) -> Result<(), AgentHostError> {
        let session = self
            .active
            .lock()
            .map_err(|_| AgentHostError::Runtime("active session registry failed".to_owned()))?
            .get(runtime_session_id)
            .map(|entry| entry.session.clone())
            .ok_or(AgentHostError::SessionNotFound)?;
        session
            .interrupt_current_turn(CancelReason::UserRequested)
            .map_err(host_runtime_error)?;
        Ok(())
    }

    async fn steer(&self, runtime_session_id: &str, content: String) -> Result<(), AgentHostError> {
        let session = self
            .active
            .lock()
            .map_err(|_| AgentHostError::Runtime("active session registry failed".to_owned()))?
            .get(runtime_session_id)
            .map(|entry| entry.session.clone())
            .ok_or(AgentHostError::SessionNotFound)?;
        session
            .steer_current_turn(None, UserInput::text(content))
            .map_err(|error| AgentHostError::Runtime(error.to_string()))?;
        Ok(())
    }
}
/// The usage records one turn appended to its session's ledger.
///
/// The runtime's ledger is append-only and accumulates for the life of the
/// session, so a turn's own records are the ones past the length the ledger
/// had when the turn started. The whole ledger would repeat every earlier
/// turn's provider calls on each turn of a persistent session.
fn turn_usage_records(
    ledger: &[agent_runtime::core::usage::UsageRecord],
    baseline: usize,
) -> &[agent_runtime::core::usage::UsageRecord] {
    ledger.get(baseline..).unwrap_or_default()
}

/// One provider attempt's disjoint `[input, output, cache_read, cache_write]`
/// counters, or `None` when the attempt reported no usage.
///
/// Every provider adapter splits one reported prompt total into these buckets
/// and leaves a bucket out of the delta only when it is zero, so in a metered
/// record an absent bucket is a known zero. Reporting it as unknown left every
/// Gemini turn without a cache hit, and every full cache hit, uncosted.
fn record_counters(
    delta: &agent_runtime::core::usage::UsageDelta,
) -> Result<Option<[u64; 4]>, AgentHostError> {
    let buckets = [
        sparse_counter(delta, CounterKind::InputUncached),
        sparse_counter(delta, CounterKind::Output),
        sparse_counter(delta, CounterKind::Reasoning),
        sparse_counter(delta, CounterKind::InputCached),
        sparse_counter(delta, CounterKind::CacheWrite),
    ];
    if buckets.iter().all(Option::is_none) {
        return Ok(None);
    }
    let [input, output, reasoning, cache_read, cache_write] =
        buckets.map(Option::unwrap_or_default);
    let output = output
        .checked_add(reasoning)
        .ok_or_else(|| AgentHostError::Runtime("usage counter overflow".to_owned()))?;
    Ok(Some([input, output, cache_read, cache_write]))
}

fn sparse_counter(
    delta: &agent_runtime::core::usage::UsageDelta,
    kind: CounterKind,
) -> Option<u64> {
    delta
        .iter()
        .find_map(|(counter_kind, value)| (counter_kind == kind).then_some(value))
}

/// Builds the bounded `ToolResultSummary` this turn attaches to a completed
/// tool call.
///
/// A native Forge command result already carries a full
/// `OrchestrationOutcome` serialized as the tool's JSON value (see
/// `typed_tools::provider_result_to_tool_outcome`); when the value round-trips
/// through that exact shape, its already-redacted fields are reused
/// unchanged. Any other tool result — a worktree read/write/command, public
/// search, or a raw runtime failure — is not vetted safe to echo verbatim, so
/// it receives a fixed, generic summary instead of a message built from its
/// content.
pub(super) fn tool_result_summary(
    call_id: &str,
    result: &Result<ToolOutcome, RuntimeError>,
) -> ToolResultSummary {
    match result {
        Ok(outcome) => serde_json::from_value::<OrchestrationOutcome>(outcome.value.clone())
            .map(|outcome| ToolResultSummary::from_orchestration_outcome(&outcome))
            .unwrap_or_else(|_| ToolResultSummary::unclassified(outcome.is_error, call_id)),
        Err(_error) => ToolResultSummary::unclassified(true, call_id),
    }
}

#[cfg(test)]
mod tool_result_summary_tests;
#[cfg(test)]
mod usage_counter_tests;
