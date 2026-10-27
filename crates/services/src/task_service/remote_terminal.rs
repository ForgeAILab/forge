//! Remote execution terminal reports and late-result settlement.

use super::*;

const REMOTE_TERMINAL_MAX_CAS_RETRIES: u8 = 2;

fn bounded_redacted_remote_optional(value: Option<&str>) -> Option<String> {
    value.map(bounded_redacted_remote_diagnostic)
}

fn remote_owner_belongs_to_daemon(owner: &str, daemon_id: &str) -> bool {
    owner.starts_with(&format!("daemon:{daemon_id}:connection:"))
}

fn remote_terminal_report_digest(
    notification: &api_types::ExecutionTerminalNotification,
) -> Result<String> {
    let payload = serde_json::to_vec(notification).map_err(|error| {
        ServiceError::invalid_operation(format!(
            "failed to fingerprint remote terminal notification: {error}"
        ))
    })?;
    let mut hasher = Sha256::new();
    hasher.update(payload);
    Ok(hex::encode(hasher.finalize()))
}

fn remote_usage_reports(reports: &[api_types::RemoteUsageReport]) -> Vec<executors::UsageReport> {
    reports
        .iter()
        .map(|report| executors::UsageReport {
            report_id: report.report_id.clone(),
            request_id: report.request_id.clone(),
            report_sequence: report.report_sequence,
            candidate_key: report.candidate_key.clone(),
            attempt_ordinal: report.attempt_ordinal,
            provider_id: report.provider_id.clone(),
            model_id: report.model_id.clone(),
            counters: executors::UsageCounters {
                input_tokens: report.input_tokens,
                output_tokens: report.output_tokens,
                cache_read_tokens: report.cache_read_tokens,
                cache_write_tokens: report.cache_write_tokens,
            },
            telemetry_state: match report.telemetry_state {
                api_types::UsageTelemetryState::Metered => executors::UsageTelemetryState::Metered,
                api_types::UsageTelemetryState::Unmetered => {
                    executors::UsageTelemetryState::Unmetered
                }
                api_types::UsageTelemetryState::Pending => executors::UsageTelemetryState::Pending,
                api_types::UsageTelemetryState::Unsettled => {
                    executors::UsageTelemetryState::Unsettled
                }
            },
            context_tokens: report.context_tokens,
            selected_tier: report.selected_tier.clone(),
            reported_cost_usd: report.reported_cost_usd.clone(),
            outcome: None,
            partial: report.partial,
        })
        .collect()
}

impl TaskService {
    /// Retain a bounded diagnostic when a terminal notification arrives after
    /// another actor already won the execution CAS.  The terminal CAS event
    /// persists the owner that was displaced (`previous_lease_owner`), which
    /// lets this method attribute a late result even after terminalization has
    /// cleared the execution row's lease owner.  If that durable proof is
    /// absent, the notification is intentionally dropped.
    pub(crate) async fn record_late_remote_terminal(
        &self,
        daemon_id: &str,
        connection_id: u64,
        notification: &api_types::ExecutionTerminalNotification,
    ) -> Result<()> {
        if !self
            .daemon_connections
            .as_ref()
            .is_none_or(|registry| registry.is_current(daemon_id, connection_id))
        {
            return Ok(());
        }
        validate_required("execution_id", &notification.execution_id)?;
        let lease_owner = crate::daemon_transport::execution_lease_owner(daemon_id, connection_id);
        let Some(execution) =
            ExecutionRepo::get_by_id(&*self.db, &notification.execution_id).await?
        else {
            return Ok(());
        };
        if execution.status == ExecutionStatus::Running {
            return Ok(());
        }
        let terminal_event = self
            .db
            .execution_terminal_owner_event_id(&execution.task_id, &lease_owner, &execution.id)
            .await?;
        let Some(terminal_event) = terminal_event else {
            // Monitor/user terminalization did not persist this connection as
            // the winning owner, so the late result cannot be attributed
            // safely and must not create a misleading diagnostic.
            return Ok(());
        };
        let terminal_event_id: String = terminal_event;
        let Some(task) = TaskRepo::get_by_id(&*self.db, &execution.task_id, true).await? else {
            // The durable receipt remains acknowledgeable after later Task
            // deletion; there is no mutable Task outcome left to repair.
            return Ok(());
        };
        let event_id = new_uuid_v4();
        let dedupe_key = format!(
            "execution-late-terminal:{}:{}:{}",
            execution.id, lease_owner, terminal_event_id
        );
        let payload_json = serde_json::json!({
            "execution_id": execution.id,
            "task_id": execution.task_id,
            "status": execution.status.to_string(),
            "notification_status": bounded_redacted_remote_optional(notification.status.as_deref()),
            "exit_code": notification.exit_code,
            "signal": bounded_redacted_remote_optional(notification.signal.as_deref()),
            "error": bounded_redacted_remote_optional(notification.error.as_deref()),
            "connection_owner": stable_remote_owner_ref(&lease_owner),
            "reason": "terminal_cas_already_won",
        })
        .to_string();
        DomainEventRepo::append_event(
            &*self.db,
            CreateDomainEvent {
                id: event_id.clone(),
                event_type: "execution.late_terminal_rejected".to_owned(),
                entity_type: "task".to_owned(),
                entity_id: execution.task_id.clone(),
                actor_type: "daemon".to_owned(),
                actor_id: Some(stable_remote_owner_ref(&lease_owner)),
                scope_type: "project".to_owned(),
                scope_id: task.project_id,
                correlation_id: format!("remote-execution-late:{}", execution.id),
                causation_id: Some(terminal_event_id),
                causation_depth: 1,
                dedupe_key: Some(dedupe_key),
                payload_json,
                created_at: now_rfc3339(),
            },
        )
        .await?;
        Ok(())
    }

    /// Authorize a terminal report at the service boundary before reading any
    /// task/pricing state or materializing a usage invocation.  The transport
    /// sink also performs this check for ordinary output, but terminal frames
    /// must remain safe when they arrive through a stale or reordered sink.
    ///
    /// A running execution requires the exact current socket lease owner.  A
    /// terminal execution may accept a late report only when its committed
    /// terminal event proves that the same daemon (possibly after reconnect)
    /// owned the lease that was displaced.  Connection identity is strict
    /// while running; daemon identity is the durable proof after terminal CAS.
    async fn authorize_remote_terminal_execution(
        &self,
        daemon_id: &str,
        connection_id: u64,
        execution: &Execution,
    ) -> Result<bool> {
        let lease_owner = crate::daemon_transport::execution_lease_owner(daemon_id, connection_id);
        if execution.status == ExecutionStatus::Running {
            return Ok(remote_execution_lease_is_active(
                execution.lease_owner.as_deref(),
                execution.lease_expires_at.as_deref(),
                execution.hard_deadline_at.as_deref(),
                &lease_owner,
                Utc::now(),
            ));
        }

        let previous_owner = self
            .db
            .execution_terminal_previous_owner(&execution.task_id, &execution.id)
            .await?
            .flatten();
        Ok(previous_owner
            .as_deref()
            .is_some_and(|owner| remote_owner_belongs_to_daemon(owner, daemon_id)))
    }

    /// A durable receipt is the only replay authority after a server restart.
    /// Its domain event stores the lease owner that produced the terminal
    /// outcome; require that proof before acknowledging an exact payload from
    /// a daemon which did not own the execution.
    async fn terminal_receipt_owned_by_daemon(
        &self,
        receipt: &db::ExecutionTerminalReceipt,
        daemon_id: &str,
    ) -> Result<bool> {
        let previous_owner = self
            .db
            .execution_terminal_receipt_previous_owner(&receipt.event_id)
            .await?
            .flatten();
        Ok(previous_owner
            .as_deref()
            .is_some_and(|owner| remote_owner_belongs_to_daemon(owner, daemon_id)))
    }

    async fn terminal_outcome_from_receipt(
        &self,
        receipt: &db::ExecutionTerminalReceipt,
        execution: Execution,
    ) -> Result<ExecutionTerminalOutcome> {
        let event = DomainEventRepo::get_event(&*self.db, &receipt.event_id)
            .await?
            .ok_or(ServiceError::Db(DbError::IdempotencyConflict))?;
        let payload = serde_json::from_str::<Value>(&event.payload_json)
            .map_err(|_| ServiceError::Db(DbError::IdempotencyConflict))?;
        if payload.get("terminal_report_id").and_then(Value::as_str)
            != Some(receipt.terminal_report_id.as_str())
            || payload
                .get("terminal_report_digest")
                .and_then(Value::as_str)
                != Some(receipt.payload_digest.as_str())
        {
            return Err(ServiceError::Db(DbError::IdempotencyConflict));
        }
        let workspace_lease_id = payload
            .get("workspace_lease_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let workspace_lease_status = payload
            .get("workspace_lease_status")
            .and_then(Value::as_str)
            .map(str::to_owned);
        Ok(ExecutionTerminalOutcome::Committed {
            execution,
            event: Box::new(event),
            workspace_lease_id,
            workspace_lease_status,
            replayed: true,
        })
    }

    pub(crate) async fn complete_remote_execution(
        &self,
        daemon_id: &str,
        connection_id: u64,
        notification: api_types::ExecutionTerminalNotification,
    ) -> Result<ExecutionTerminalOutcome> {
        validate_required("execution_id", &notification.execution_id)?;
        validate_required("terminal_report_id", &notification.terminal_report_id)?;
        if !self
            .daemon_connections
            .as_ref()
            .is_none_or(|registry| registry.is_current(daemon_id, connection_id))
        {
            return Ok(ExecutionTerminalOutcome::Concurrent { current: None });
        }
        let lease_owner = crate::daemon_transport::execution_lease_owner(daemon_id, connection_id);
        // The terminal report identity is durable, not an in-memory transport
        // concern.  The composite DB boundary below decides whether this is
        // an exact replay (including after restart) or a conflicting/stale
        // notification.  Do not reject a terminal row before giving that
        // receipt lookup a chance to acknowledge it.
        let terminal_report_digest = remote_terminal_report_digest(&notification)?;
        let current_execution = ExecutionRepo::get_by_id(&*self.db, &notification.execution_id)
            .await?
            .ok_or_else(|| {
                ServiceError::not_found("execution", notification.execution_id.clone())
            })?;

        // This authorization gate intentionally precedes every receipt,
        // pricing-selection, and usage-ledger read/write. A stale/non-owner
        // daemon therefore leaves no usage rows, events, or settlement
        // evidence behind, including when the target is already terminal and
        // would otherwise enter the late-drain path. Terminal reconnects are
        // authorized from the terminal event's preserved previous owner.
        if !self
            .authorize_remote_terminal_execution(daemon_id, connection_id, &current_execution)
            .await?
        {
            return Ok(ExecutionTerminalOutcome::Concurrent {
                current: Some(current_execution),
            });
        }

        // Reject a conflicting report before materializing any remote
        // invocations. The invocation-admission helper is intentionally
        // idempotent, but it may still create a newly observed candidate; a
        // report-id conflict must not leave that side effect behind. An exact
        // receipt replay can skip materialization entirely because the
        // composite terminal boundary will return the durable outcome.
        let durable_receipt = if let Some(receipt) = ExecutionRepo::get_execution_terminal_receipt(
            &*self.db,
            &notification.terminal_report_id,
        )
        .await?
        {
            if receipt.execution_id != notification.execution_id
                || receipt.payload_digest != terminal_report_digest
            {
                return Err(ServiceError::Db(DbError::IdempotencyConflict));
            }
            if !self
                .terminal_receipt_owned_by_daemon(&receipt, daemon_id)
                .await?
            {
                return Err(ServiceError::Db(DbError::IdempotencyConflict));
            }
            Some(receipt)
        } else {
            None
        };
        if let Some(receipt) = durable_receipt {
            // The receipt and terminal row are durable, but the process may
            // have exited before consuming the outbox or applying Task-level
            // retry/block effects. Replay those idempotent post-commit steps
            // before acknowledging the daemon again.
            let outcome = self
                .terminal_outcome_from_receipt(&receipt, current_execution)
                .await?;
            if let ExecutionTerminalOutcome::Committed { execution, .. } = &outcome {
                self.settle_remote_terminal_postcommit(execution, &notification, false)
                    .await?;
            }
            return Ok(outcome);
        }

        let signal = notification
            .signal
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let error = notification
            .error
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let succeeded = notification.exit_code == Some(0) && signal.is_none() && error.is_none();
        let outcome = notification.status.as_deref().unwrap_or(if succeeded {
            "completed"
        } else {
            "failed"
        });
        let (mut status, mut stop_reason, mut stopped_by, mut stopped_at, mut terminal_error) =
            match outcome {
                "completed" => (ExecutionStatus::Completed, None, None, None, None),
                "cancelled" => (
                    ExecutionStatus::Cancelled,
                    Some(db::StopReason::ExecutorCancelled),
                    Some(Actor::system(api_types::SystemComponent::Executor).display()),
                    Some(notification.ts.clone()),
                    None,
                ),
                _ => (
                    ExecutionStatus::Failed,
                    Some(db::StopReason::ExecutorFailed),
                    Some(Actor::system(api_types::SystemComponent::Executor).display()),
                    Some(notification.ts.clone()),
                    Some(remote_terminal_error_message(
                        notification.exit_code,
                        signal,
                        error,
                    )),
                ),
            };

        let executor_unavailable = notification.failure_class
            == Some(api_types::RemoteExecutionFailureClass::ExecutorUnavailable);
        let route_outcome = crate::task_service::config::RouteOutcome {
            selected: notification.resolved_candidate.as_ref().map(|candidate| {
                (
                    candidate.candidate_key.clone(),
                    candidate.executor_type.clone(),
                    candidate.config.clone(),
                )
            }),
            attempts: notification
                .route_attempts
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|attempt| (attempt.candidate_key.clone(), attempt.outcome.clone()))
                .collect(),
            unavailable_retry_at: executor_unavailable.then(|| notification.retry_at.clone()),
        };
        let snapshot_update = match current_execution.executor_config_snapshot_json.as_deref() {
            Some(snapshot) => crate::task_service::config::apply_route_outcome_to_snapshot(
                snapshot,
                &route_outcome,
            )?,
            None => None,
        };

        let environment =
            match TaskRepo::get_by_id(&*self.db, &current_execution.task_id, true).await? {
                Some(task) => self.project_environment(&task.project_id).await?.env,
                None => Default::default(),
            };
        let plan = if executors::task_role_can_write_plan(Some(&current_execution.role)) {
            Some(
                crate::plan_artifact::transport::candidate(
                    &self.db,
                    &current_execution,
                    notification.plan_text.as_deref(),
                    &environment,
                )
                .await?,
            )
        } else {
            None
        };
        if status == ExecutionStatus::Completed
            && notification
                .plan_text
                .as_ref()
                .is_some_and(|text| text.len() as u64 > api_types::MAX_EXECUTION_PLAN_BYTES)
        {
            status = ExecutionStatus::Failed;
            stop_reason = Some(db::StopReason::ExecutorFailed);
            stopped_by = Some(Actor::system(api_types::SystemComponent::Executor).display());
            stopped_at = Some(notification.ts.clone());
            terminal_error = Some(format!(
                "transported execution plan exceeds the {}-byte limit: {} bytes",
                api_types::MAX_EXECUTION_PLAN_BYTES,
                notification.plan_text.as_ref().unwrap().len()
            ));
        }

        let remote_reports = remote_usage_reports(&notification.usage_reports);
        let snapshot_value = current_execution
            .executor_config_snapshot_json
            .as_deref()
            .map(|snapshot| parse_json_value("executor config snapshot", snapshot))
            .transpose()?;

        // Hold the SQLite writer lock across the remote pricing/usage reads.
        // This is an authority guard, not a second best-effort liveness read:
        // a lease takeover cannot commit while the immutable selection,
        // invocation, rate, and replay-event payloads are being prepared.
        // The terminal CAS below reacquires the same lock and still verifies
        // the owner/version, so a takeover in the short hand-off gap produces
        // a side-effect-free concurrent result.
        let (remote_invocations, usage_settlements) = {
            let mut preparation_tx = db::begin_immediate(self.db.pool()).await?;
            let authority = self
                .db
                .execution_terminal_authority_in_tx(&mut preparation_tx, &notification.execution_id)
                .await?;
            let Some(authority) = authority else {
                preparation_tx.rollback().await?;
                return Ok(ExecutionTerminalOutcome::Concurrent { current: None });
            };
            let authority_status: String = authority.status;
            let authority_owner: Option<String> = authority.lease_owner;
            let authority_lease_expires_at: Option<String> = authority.lease_expires_at;
            let authority_hard_deadline_at: Option<String> = authority.hard_deadline_at;
            let authority_ok = if authority_status == ExecutionStatus::Running.to_string() {
                remote_execution_lease_is_active(
                    authority_owner.as_deref(),
                    authority_lease_expires_at.as_deref(),
                    authority_hard_deadline_at.as_deref(),
                    &lease_owner,
                    Utc::now(),
                )
            } else {
                let previous_owner = self
                    .db
                    .execution_terminal_report_previous_owner_in_tx(
                        &mut preparation_tx,
                        &current_execution.task_id,
                        &current_execution.id,
                    )
                    .await?
                    .flatten();
                previous_owner
                    .as_deref()
                    .is_some_and(|owner| remote_owner_belongs_to_daemon(owner, daemon_id))
            };
            if !authority_ok {
                preparation_tx.rollback().await?;
                let current =
                    ExecutionRepo::get_by_id(&*self.db, &notification.execution_id).await?;
                return Ok(ExecutionTerminalOutcome::Concurrent { current });
            }
            let remote_invocations = execution::ledger::ensure_remote_task_usage_invocations_in_tx(
                &self.db,
                &mut preparation_tx,
                &current_execution,
                &remote_reports,
                snapshot_value.as_ref(),
                &notification.ts,
            )
            .await?;
            let usage_settlements =
                execution::ledger::build_task_usage_settlements_with_prepared_invocations_in_tx(
                    &self.db,
                    &mut preparation_tx,
                    &notification.execution_id,
                    &remote_reports,
                    &notification.ts,
                    &remote_invocations,
                )
                .await?;
            preparation_tx.commit().await?;
            (remote_invocations, usage_settlements)
        };

        // Heartbeats advance execution_version.  A terminal notification that
        // raced one of those renewals must retry against the same owner rather
        // than being mistaken for a competing terminal winner.  The owner
        // predicate remains in every attempt, so a replacement connection or
        // expiry monitor still wins immediately.
        let terminal_resume_policy = match &status {
            ExecutionStatus::Completed => Some(None),
            ExecutionStatus::Cancelled | ExecutionStatus::Failed => {
                Some(Some(db::ResumePolicy::Manual))
            }
            ExecutionStatus::Running => unreachable!("remote terminal status cannot be running"),
        };
        let mut terminal_candidate = current_execution.clone();
        let mut terminal_retries = 0u8;
        let terminal_updated_at = now_rfc3339();
        let terminal_outcome = loop {
            if !self
                .daemon_connections
                .as_ref()
                .is_none_or(|registry| registry.is_current(daemon_id, connection_id))
            {
                break ExecutionTerminalOutcome::Concurrent {
                    current: Some(terminal_candidate),
                };
            }
            let attempt = ExecutionRepo::terminalize_with_ledger_and_invocations(
                &*self.db,
                {
                    let mut input = execution::ledger::terminal_with_late_ledger(
                        TerminalizeExecution {
                            execution_id: notification.execution_id.clone(),
                            expected_version: terminal_candidate.execution_version,
                            lease_owner: Some(lease_owner.clone()),
                            status: status.clone(),
                            stop_reason: stop_reason.clone().map(Some),
                            stopped_by: stopped_by.clone().map(Some),
                            stopped_at: stopped_at.clone().map(Some),
                            resume_policy: terminal_resume_policy.clone(),
                            agent_session_id: notification.agent_session_id.clone().map(Some),
                            agent_message_id: None,
                            last_activity_at: None,
                            last_progress_at: None,
                            summary: notification.summary.clone().map(Some),
                            logs_path: None,
                            before_sha: None,
                            after_sha: notification.after_sha.clone().map(Some),
                            error: terminal_error.clone().map(Some),
                            executor_config_snapshot_json: snapshot_update.clone().map(Some),
                            updated_at: terminal_updated_at.clone(),
                            actor_type: "daemon".to_owned(),
                            actor_id: Some(lease_owner.clone()),
                            correlation_id: Some(format!(
                                "remote-execution:{}",
                                notification.execution_id
                            )),
                            causation_id: None,
                            causation_depth: 0,
                            lease_disposition: ExecutionLeaseDisposition::Revoke,
                        },
                        usage_settlements.clone(),
                        Some(notification.terminal_report_id.clone()),
                        Some(terminal_report_digest.clone()),
                    );
                    input.plan = plan.clone();
                    input
                },
                remote_invocations.clone(),
            )
            .await?;

            match attempt {
                committed @ ExecutionTerminalOutcome::Committed { .. } => break committed,
                ExecutionTerminalOutcome::Concurrent {
                    current: Some(current),
                } if terminal_retries < REMOTE_TERMINAL_MAX_CAS_RETRIES
                    && self
                        .daemon_connections
                        .as_ref()
                        .is_none_or(|registry| registry.is_current(daemon_id, connection_id))
                    && current.status == ExecutionStatus::Running
                    && current.lease_owner.as_deref() == Some(lease_owner.as_str())
                    && terminal_candidate.execution_version < current.execution_version =>
                {
                    // The bounded retry handles self-heartbeat version churn;
                    // after two refreshes let the typed concurrent result
                    // propagate so a pathological race cannot spin forever.
                    terminal_retries += 1;
                    terminal_candidate = current;
                    continue;
                }
                outcome => break outcome,
            }
        };
        let (updated, replayed) = match &terminal_outcome {
            ExecutionTerminalOutcome::Committed {
                execution,
                replayed,
                ..
            } => (execution, *replayed),
            ExecutionTerminalOutcome::Concurrent { .. } => return Ok(terminal_outcome),
        };
        self.settle_remote_terminal_postcommit(updated, &notification, !replayed)
            .await?;

        Ok(terminal_outcome)
    }

    pub(super) async fn settle_remote_terminal_postcommit(
        &self,
        execution: &Execution,
        notification: &api_types::ExecutionTerminalNotification,
        publish_terminal_event: bool,
    ) -> Result<()> {
        let Some(task) = TaskRepo::get_by_id(&*self.db, &execution.task_id, true).await? else {
            return Ok(());
        };
        let Some(project) = ProjectRepo::get_by_id(&*self.db, &task.project_id).await? else {
            return Ok(());
        };
        // A failure is not fenced: it is retried or blocks its Task under the
        // current Project revision (`annotate_executor_failure_block`).
        if execution.role != crate::workflow::default_roles::INTERACTIVE
            && execution.status != ExecutionStatus::Failed
            && execution_dispatch_project_version(execution) != Some(project.version)
        {
            tracing::info!(
                task_id = %task.id,
                execution_id = %execution.id,
                execution_role = %execution.role,
                dispatch_project_version = ?execution_dispatch_project_version(execution),
                current_project_version = project.version,
                "ignoring terminal post-commit effects from a superseded Project revision"
            );
            return Ok(());
        }
        let workspace = match execution.workspace_id.as_deref() {
            Some(workspace_id) => WorkspaceRepo::get_by_id(&*self.db, workspace_id).await?,
            None => WorkspaceRepo::get_by_task_id(&*self.db, &task.id).await?,
        };

        if execution.status != ExecutionStatus::Completed {
            if let Some(workspace) = workspace.as_ref() {
                // The plan stage sits in the Task root, beside the worktree.
                if let Ok(path) = crate::workspace_backend::EmbeddedWorkspaceBackend::ensure_recorded_server_placement(&self.db, workspace)
                    .await
                    .map_err(ServiceError::from)
                    .and_then(|placement| crate::workspace_manager::task_root_anchor_of(&placement))
                {
                    execution::discard_execution_plan_stage(&path.to_string_lossy(), &execution.id);
                }
            }
        }

        if let Err(error) = self
            .memory_service
            .record_execution_summary_if_present(&task.project_id, execution)
            .await
        {
            tracing::warn!(error = %error, "memory indexing failed (non-fatal)");
        }

        if task.deleted_at.is_some()
            || !self
                .execution_owns_current_role_attempt(&task, execution)
                .await?
        {
            return Ok(());
        }

        if execution.status == ExecutionStatus::Completed {
            let reviewer_outcome_pending =
                matches!(execution.role.as_str(), "reviewer" | "auditor");
            if !reviewer_outcome_pending
                && !execution::clear_execution_retry_metadata_for_latest_execution(
                    &self.db,
                    &task,
                    execution,
                    project.version,
                )
                .await?
            {
                return Ok(());
            }
        } else if execution.status == ExecutionStatus::Failed
            && notification.failure_class
                == Some(api_types::RemoteExecutionFailureClass::ExecutorUnavailable)
        {
            let attempts = notification
                .route_attempts
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(
                    |attempt| db::task_condition::channels::ExecutorRouteAttempt {
                        candidate_key: attempt.candidate_key.clone(),
                        outcome: attempt.outcome.clone(),
                    },
                )
                .collect();
            self.annotate_executor_unavailable_block(
                execution,
                notification.retry_at.clone(),
                attempts,
            )
            .await?;
        } else if execution.status == ExecutionStatus::Failed
            && execution::should_block_task_for_failed_execution(execution)
        {
            self.annotate_executor_failure_block(execution).await?;
        }

        if publish_terminal_event {
            execution::publish_terminal_execution_event(self, execution);
        }
        Ok(())
    }
}
