//! Execution terminal settlement with usage ledger and invocation accounting.

use super::*;

impl SqliteDb {
    pub(super) async fn settle_execution_with_ledger(
        &self,
        input: TerminalizeExecutionWithLedger,
        remote_invocations: Vec<CreateUsageInvocation>,
    ) -> Result<ExecutionTerminalOutcome> {
        let terminal = &input.terminal;
        if terminal.expected_version < 1 {
            return Err(DbError::Check(
                "execution terminalization requires a positive version".to_owned(),
            ));
        }
        if terminal.status == ExecutionStatus::Running {
            return Err(DbError::InvalidTransition);
        }
        if !(0..=16).contains(&terminal.causation_depth) {
            return Err(DbError::Check(
                "execution terminal event causation depth must be between 0 and 16".to_owned(),
            ));
        }
        if input.terminal_report_id.is_some() != input.terminal_report_digest.is_some() {
            return Err(DbError::Check(
                "terminal report identity requires a complete id and digest".to_owned(),
            ));
        }
        if input
            .terminal_report_id
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
            || input
                .terminal_report_digest
                .as_deref()
                .is_some_and(|value| value.trim().is_empty())
        {
            return Err(DbError::Check(
                "terminal report identity cannot be empty".to_owned(),
            ));
        }

        let mut transaction = crate::begin_immediate(&self.pool).await?;

        // A remote daemon may replay a fully committed terminal payload after
        // a server restart. The receipt table is the durable idempotency
        // authority; compare both id and digest before attempting the
        // execution CAS. Search the whole database before attempting that
        // CAS: a terminal_report_id is globally unique, not merely unique
        // within one execution. Reusing it for a different execution or
        // payload is an idempotency conflict even when that execution is
        // still running.
        if let (Some(report_id), Some(report_digest)) = (
            input.terminal_report_id.as_deref(),
            input.terminal_report_digest.as_deref(),
        ) {
            if let Some(receipt) = terminal_receipt_in_tx(
                &mut transaction,
                report_id,
                report_digest,
                &terminal.execution_id,
                terminal.lease_owner.as_deref(),
            )
            .await?
            {
                transaction.commit().await?;
                return Ok(receipt);
            }
        }

        let mut query = sqlx::QueryBuilder::<Sqlite>::new("UPDATE execution SET status = ");
        query.push_bind(terminal.status.to_string());
        if let Some(stop_reason) = terminal.stop_reason.as_ref() {
            query.push(", stop_reason = ");
            query.push_bind(stop_reason.as_ref().map(ToString::to_string));
        }
        if let Some(stopped_by) = terminal.stopped_by.as_ref() {
            query
                .push(", stopped_by = ")
                .push_bind(stopped_by.as_deref());
        }
        query.push(", stopped_at = ");
        match terminal.stopped_at.as_ref() {
            Some(Some(stopped_at)) => query.push_bind(stopped_at),
            Some(None) | None => query.push_bind(terminal.updated_at.as_str()),
        };
        if let Some(resume_policy) = terminal.resume_policy.as_ref() {
            query.push(", resume_policy = ");
            query.push_bind(resume_policy.as_ref().map(ToString::to_string));
        }
        if let Some(agent_session_id) = terminal.agent_session_id.as_ref() {
            query
                .push(", agent_session_id = ")
                .push_bind(agent_session_id.as_deref());
        }
        if let Some(agent_message_id) = terminal.agent_message_id.as_ref() {
            query
                .push(", agent_message_id = ")
                .push_bind(agent_message_id.as_deref());
        }
        if let Some(last_activity_at) = terminal.last_activity_at.as_ref() {
            query
                .push(", last_activity_at = ")
                .push_bind(last_activity_at.as_deref());
        }
        if let Some(last_progress_at) = terminal.last_progress_at.as_ref() {
            query
                .push(", last_progress_at = ")
                .push_bind(last_progress_at.as_deref());
        }
        if let Some(summary) = terminal.summary.as_ref() {
            query.push(", summary = ").push_bind(summary.as_deref());
        }
        if let Some(logs_path) = terminal.logs_path.as_ref() {
            query.push(", logs_path = ").push_bind(logs_path.as_deref());
        }
        if let Some(before_sha) = terminal.before_sha.as_ref() {
            query
                .push(", before_sha = ")
                .push_bind(before_sha.as_deref());
        }
        if let Some(after_sha) = terminal.after_sha.as_ref() {
            query.push(", after_sha = ").push_bind(after_sha.as_deref());
        }
        if let Some(error) = terminal.error.as_ref() {
            query.push(", error = ").push_bind(error.as_deref());
        }
        if let Some(snapshot) = terminal.executor_config_snapshot_json.as_ref() {
            query
                .push(", executor_config_snapshot_json = ")
                .push_bind(snapshot.as_deref());
        }
        query.push(
            ", lease_owner = NULL, lease_expires_at = NULL,
                 execution_version = execution_version + 1,
                 updated_at = ",
        );
        query.push_bind(&terminal.updated_at);
        query.push(" WHERE id = ");
        query.push_bind(&terminal.execution_id);
        query.push(" AND status = 'running' AND execution_version = ");
        query.push_bind(terminal.expected_version);
        if let Some(owner) = terminal.lease_owner.as_deref() {
            query.push(" AND lease_owner = ").push_bind(owner);
            // Only a remote owner reporting its own outcome proves liveness
            // here; see `require_live_owner_lease`. The lease token alone
            // cannot stand in for that intent, because a local caller carries
            // whatever owner the row already holds — including a daemon token.
            // Gating on the token instead left every expired lease
            // un-terminalizable by the two callers that exist to clear one: a
            // user cancel and the recovery reaper both lost the CAS forever,
            // and reported the untouched running row as success.
            if input.require_live_owner_lease {
                query.push(
                    " AND lease_expires_at IS NOT NULL AND julianday(lease_expires_at) > julianday(",
                );
                query.push_bind(&terminal.updated_at);
                query.push(")");
                query.push(
                    " AND (hard_deadline_at IS NULL OR julianday(hard_deadline_at) > julianday(",
                );
                query.push_bind(&terminal.updated_at);
                query.push("))");
            }
        }

        let result = query.build().execute(&mut *transaction).await?;
        let terminal_cas_won = result.rows_affected() == 1;
        let current_after_cas = if terminal_cas_won {
            None
        } else {
            let current = execution_in_tx(&mut transaction, &terminal.execution_id).await?;
            if !input.allow_late_settlement
                || current
                    .as_ref()
                    .is_none_or(|execution| execution.status == ExecutionStatus::Running)
            {
                transaction.rollback().await?;
                return Ok(ExecutionTerminalOutcome::Concurrent { current });
            }
            current
        };

        let (workspace_lease_id, workspace_lease_status) = if terminal_cas_won {
            let workspace_lease_id = sqlx::query_scalar::<_, String>(
                "SELECT id FROM workspace_lease
                 WHERE execution_id = ? AND status = 'active'
                 ORDER BY issued_at DESC, id DESC LIMIT 1",
            )
            .bind(&terminal.execution_id)
            .fetch_optional(&mut *transaction)
            .await?;
            let workspace_lease_status =
                workspace_lease_id
                    .as_ref()
                    .map(|_| match terminal.lease_disposition {
                        ExecutionLeaseDisposition::Revoke => "revoked".to_owned(),
                        ExecutionLeaseDisposition::Expire => "expired".to_owned(),
                    });
            if let Some(status) = workspace_lease_status.as_deref() {
                sqlx::query(
                    "UPDATE workspace_lease
                     SET status = ?, revoked_at = ?, version = version + 1,
                         updated_at = ?
                     WHERE execution_id = ? AND status = 'active'",
                )
                .bind(status)
                .bind(&terminal.updated_at)
                .bind(&terminal.updated_at)
                .bind(&terminal.execution_id)
                .execute(&mut *transaction)
                .await?;
            }
            (workspace_lease_id, workspace_lease_status)
        } else {
            (None, None)
        };

        let updated = match current_after_cas {
            Some(current) => current,
            None => execution_in_tx(&mut transaction, &terminal.execution_id)
                .await?
                .ok_or(DbError::NotFound)?,
        };

        // A remote late-drain is allowed only when the terminal event proves
        // that this same daemon owned the lease which was displaced.  The
        // service performs the read-side gate before pricing work; repeat the
        // proof inside this transaction so an owner takeover between those
        // reads cannot materialize invocations or append a usage event to a
        // victim execution.
        if !terminal_cas_won && input.allow_late_settlement {
            if let Some(expected_owner) = terminal.lease_owner.as_deref() {
                let previous_owner = sqlx::query_scalar::<_, Option<String>>(
                    "SELECT json_extract(payload_json, '$.previous_lease_owner')
                     FROM domain_event
                     WHERE entity_type = 'task'
                       AND entity_id = ?
                       AND event_type IN (
                           'execution.completed', 'execution.failed',
                           'execution.cancelled', 'execution.terminal_report.received'
                       )
                       AND json_extract(payload_json, '$.execution_id') = ?
                     ORDER BY sequence DESC
                     LIMIT 1",
                )
                .bind(&updated.task_id)
                .bind(&updated.id)
                .fetch_optional(&mut *transaction)
                .await?
                .flatten();
                if !previous_owner
                    .as_deref()
                    .is_some_and(|owner| same_remote_daemon_owner(owner, expected_owner))
                {
                    transaction.rollback().await?;
                    return Ok(ExecutionTerminalOutcome::Concurrent {
                        current: Some(updated),
                    });
                }
            }
        }

        // The service normally prepares these rows from the target
        // execution's selections, but keep the composite boundary defensive:
        // a malformed caller must not be able to terminalize one execution
        // while materializing an invocation belonging to another source. This
        // validation deliberately occurs only after the owner CAS (or the
        // durable late-owner proof above), while BEGIN IMMEDIATE is held. No
        // pricing-selection read is exposed to a caller that lost ownership
        // or was taken over concurrently.
        for invocation in &remote_invocations {
            if invocation.domain_kind != crate::PricingDomainKind::Execution
                || invocation.surface != UsageSurface::TaskExecution
                || invocation.source_id != terminal.execution_id
                || invocation.execution_id.as_deref() != Some(terminal.execution_id.as_str())
            {
                return Err(DbError::IdempotencyConflict);
            }
            let Some(selection) = sqlx::query(
                "SELECT source_id, execution_id, domain_kind, surface,
                        candidate_key, attempt_ordinal
                 FROM pricing_selection WHERE id = ?",
            )
            .bind(&invocation.pricing_selection_id)
            .fetch_optional(&mut *transaction)
            .await?
            else {
                return Err(DbError::NotFound);
            };
            let selection_source_id: String = selection.try_get("source_id")?;
            let selection_execution_id: Option<String> = selection.try_get("execution_id")?;
            let selection_domain_kind: String = selection.try_get("domain_kind")?;
            let selection_surface: String = selection.try_get("surface")?;
            let selection_candidate_key: Option<String> = selection.try_get("candidate_key")?;
            let selection_attempt_ordinal: i64 = selection.try_get("attempt_ordinal")?;
            if selection_source_id != terminal.execution_id
                || selection_execution_id.as_deref() != Some(terminal.execution_id.as_str())
                || selection_domain_kind != crate::PricingDomainKind::Execution.to_string()
                || selection_surface != UsageSurface::TaskExecution.to_string()
                || selection_candidate_key != invocation.candidate_key
                || selection_attempt_ordinal != invocation.attempt_ordinal
            {
                return Err(DbError::IdempotencyConflict);
            }
        }
        let project_id: Option<String> =
            sqlx::query_scalar("SELECT project_id FROM task WHERE id = ?")
                .bind(&updated.task_id)
                .fetch_optional(&mut *transaction)
                .await?;
        if !terminal_cas_won && input.terminal_report_id.is_none() {
            transaction.rollback().await?;
            return Ok(ExecutionTerminalOutcome::Concurrent {
                current: Some(updated),
            });
        }
        let event_id = new_uuid_v4();
        let event_type = if terminal_cas_won {
            match updated.status {
                ExecutionStatus::Completed => "execution.completed",
                ExecutionStatus::Failed => "execution.failed",
                ExecutionStatus::Cancelled => "execution.cancelled",
                ExecutionStatus::Running => unreachable!("terminal CAS rejects running status"),
            }
        } else {
            "execution.terminal_report.received"
        };
        let event = CreateDomainEvent {
            id: event_id.clone(),
            event_type: event_type.to_owned(),
            entity_type: "task".to_owned(),
            entity_id: updated.task_id.clone(),
            actor_type: terminal.actor_type.clone(),
            actor_id: terminal.actor_id.clone(),
            scope_type: if project_id.is_some() {
                "project".to_owned()
            } else {
                "task".to_owned()
            },
            scope_id: project_id
                .clone()
                .unwrap_or_else(|| updated.task_id.clone()),
            correlation_id: terminal
                .correlation_id
                .clone()
                .unwrap_or_else(|| event_id.clone()),
            causation_id: terminal.causation_id.clone(),
            causation_depth: terminal.causation_depth,
            dedupe_key: if terminal_cas_won {
                Some(format!(
                    "execution-terminal:{}:{}",
                    updated.id, updated.status
                ))
            } else {
                Some(format!(
                    "execution-terminal-report:{}",
                    input.terminal_report_id.as_deref().ok_or_else(|| {
                        DbError::Check(
                            "late terminal report is missing its durable report id".to_owned(),
                        )
                    })?
                ))
            },
            payload_json: serde_json::json!({
                "execution_id": updated.id,
                "task_id": updated.task_id,
                "project_id": project_id,
                "role": updated.role,
                "status": updated.status.to_string(),
                "previous_lease_owner": terminal.lease_owner.clone(),
                "stop_reason": updated.stop_reason.as_ref().map(ToString::to_string),
                "error": updated.error.as_deref().map(|value| value.chars().take(500).collect::<String>()),
                "workspace_lease_id": workspace_lease_id,
                "workspace_lease_status": workspace_lease_status,
                "terminal_report_id": input.terminal_report_id.clone(),
                "terminal_report_digest": input.terminal_report_digest.clone(),
                "late_settlement": !terminal_cas_won,
            })
            .to_string(),
            created_at: terminal.updated_at.clone(),
        };

        // Remote terminal reports are evidence that the daemon crossed the
        // provider-call boundary.  Materialize and start those invocations
        // only after the ownership/CAS checks above, and keep the inserts in
        // this same transaction as terminalization and settlement.  A
        // conflict, receipt replay, or persistence failure therefore leaves
        // no newly observed invocation behind.
        for invocation_input in &remote_invocations {
            let invocation = self
                .create_usage_invocation_in_tx(&mut transaction, invocation_input.clone())
                .await?;
            if invocation.lifecycle == UsageInvocationLifecycle::Admitted {
                self.start_usage_invocation_in_tx(
                    &mut transaction,
                    StartUsageInvocation {
                        id: invocation.id,
                        expected_version: invocation.version,
                        started_at: terminal.updated_at.clone(),
                        updated_at: terminal.updated_at.clone(),
                    },
                )
                .await?;
            }
        }

        let mut settled_ids = HashSet::new();
        // A cancellation may carry reports observed before the cancellation
        // won the execution CAS. Settle those supplied invocations first and
        // only leave calls with no observed report pending for late drain.
        for settlement in &input.settlements {
            let belongs: Option<String> =
                sqlx::query_scalar("SELECT execution_id FROM usage_invocation WHERE id = ?")
                    .bind(&settlement.invocation_id)
                    .fetch_optional(&mut *transaction)
                    .await?;
            if belongs.as_deref() != Some(terminal.execution_id.as_str()) {
                return Err(DbError::IdempotencyConflict);
            }
            let mut expected_version = settlement.expected_version;
            let existing = crate::sqlite::pricing::select_invocation_in_tx(
                &mut transaction,
                &settlement.invocation_id,
            )
            .await?
            .ok_or(DbError::NotFound)?;
            if existing.lifecycle == UsageInvocationLifecycle::Settled {
                crate::sqlite::pricing::validate_settled_usage_invocation_in_tx(
                    &mut transaction,
                    &existing,
                    settlement,
                )
                .await?;
                settled_ids.insert(existing.id);
                continue;
            }
            if existing.lifecycle == UsageInvocationLifecycle::Unsettled {
                return Err(DbError::IdempotencyConflict);
            }
            if existing.lifecycle == UsageInvocationLifecycle::Admitted {
                self.start_usage_invocation_in_tx(
                    &mut transaction,
                    StartUsageInvocation {
                        id: existing.id,
                        expected_version: existing.version,
                        started_at: settlement.settled_at.clone(),
                        updated_at: settlement.updated_at.clone(),
                    },
                )
                .await?;
                expected_version = existing.version + 1;
            }
            let invocation = self
                .settle_usage_invocation_in_tx(
                    &mut transaction,
                    SettleUsageInvocation {
                        id: settlement.invocation_id.clone(),
                        expected_version,
                        telemetry_state: settlement.telemetry_state,
                        terminal_reason: settlement.terminal_reason.clone(),
                        settled_at: settlement.settled_at.clone(),
                        updated_at: settlement.updated_at.clone(),
                    },
                )
                .await?;
            settled_ids.insert(invocation.id);
            for usage_event in &settlement.events {
                self.append_usage_event_in_tx(&mut transaction, usage_event.clone())
                    .await?;
            }
        }

        if !terminal_cas_won {
            // The execution already has an authoritative terminal outcome.
            // Only supplied reports are drained here; untouched invocations
            // remain pending for a later replay and no execution/workspace
            // state is changed.
        } else if updated.status == ExecutionStatus::Cancelled {
            // Cancellation owns the domain outcome but leaves only calls
            // without an observed report pending so a late durable result can
            // settle their ledger later.
            sqlx::query(
                "UPDATE usage_invocation
                 SET lifecycle = 'pending_settlement', updated_at = ?,
                     version = version + 1
                 WHERE execution_id = ? AND lifecycle = 'started'",
            )
            .bind(&terminal.updated_at)
            .bind(&terminal.execution_id)
            .execute(&mut *transaction)
            .await?;
            if input.mark_unreplayable_pending_unsettled {
                // Recovery has already established that this execution was
                // owned by a local runtime and therefore cannot deliver a
                // replayable terminal result after a process crash.  Keep the
                // proof and the ledger disposition in this same transaction;
                // a crash after the terminal CAS can no longer strand a
                // pending invocation forever.
                let pending_rows = sqlx::query(
                    "SELECT id, version FROM usage_invocation
                     WHERE execution_id = ? AND lifecycle = 'pending_settlement'",
                )
                .bind(&terminal.execution_id)
                .fetch_all(&mut *transaction)
                .await?;
                for row in pending_rows {
                    let id: String = row.try_get("id")?;
                    let version: i64 = row.try_get("version")?;
                    self.mark_usage_invocation_unsettled_in_tx(
                        &mut transaction,
                        MarkUsageInvocationUnsettled {
                            id,
                            expected_version: version,
                            terminal_reason: "recovery_no_replayable_result".to_owned(),
                            settled_at: terminal.updated_at.clone(),
                            updated_at: terminal.updated_at.clone(),
                        },
                    )
                    .await?;
                }
            }
        } else if input.preserve_pending_settlement {
            // Recovery of a daemon-owned execution can race the daemon's
            // replayable terminal notification. Keep calls without an
            // observed report pending so that late delivery can settle them;
            // never manufacture an unmetered result at this boundary.
            sqlx::query(
                "UPDATE usage_invocation
                 SET lifecycle = 'pending_settlement', updated_at = ?,
                     version = version + 1
                 WHERE execution_id = ? AND lifecycle = 'started'",
            )
            .bind(&terminal.updated_at)
            .bind(&terminal.execution_id)
            .execute(&mut *transaction)
            .await?;
            if input.mark_unreplayable_pending_unsettled {
                let pending_rows = sqlx::query(
                    "SELECT id, version FROM usage_invocation
                     WHERE execution_id = ? AND lifecycle = 'pending_settlement'",
                )
                .bind(&terminal.execution_id)
                .fetch_all(&mut *transaction)
                .await?;
                for row in pending_rows {
                    let id: String = row.try_get("id")?;
                    let version: i64 = row.try_get("version")?;
                    self.mark_usage_invocation_unsettled_in_tx(
                        &mut transaction,
                        MarkUsageInvocationUnsettled {
                            id,
                            expected_version: version,
                            terminal_reason: "recovery_no_replayable_result".to_owned(),
                            settled_at: terminal.updated_at.clone(),
                            updated_at: terminal.updated_at.clone(),
                        },
                    )
                    .await?;
                }
            }
        } else {
            if input.mark_unreplayable_pending_unsettled {
                // A local runtime may have crossed the provider boundary but
                // lost its result before this recovery terminal CAS. Keep
                // that uncertainty explicit: move active attempts to
                // pending, then close them as `unsettled` in this same
                // transaction. This avoids manufacturing an unmetered result
                // and removes the crash window between terminalization and a
                // follow-up recovery sweep.
                sqlx::query(
                    "UPDATE usage_invocation
                     SET lifecycle = 'pending_settlement', updated_at = ?,
                         version = version + 1
                     WHERE execution_id = ? AND lifecycle = 'started'",
                )
                .bind(&terminal.updated_at)
                .bind(&terminal.execution_id)
                .execute(&mut *transaction)
                .await?;
                let pending_rows = sqlx::query(
                    "SELECT id, version FROM usage_invocation
                     WHERE execution_id = ? AND lifecycle = 'pending_settlement'",
                )
                .bind(&terminal.execution_id)
                .fetch_all(&mut *transaction)
                .await?;
                for row in pending_rows {
                    let id: String = row.try_get("id")?;
                    if settled_ids.contains(&id) {
                        continue;
                    }
                    let version: i64 = row.try_get("version")?;
                    self.mark_usage_invocation_unsettled_in_tx(
                        &mut transaction,
                        MarkUsageInvocationUnsettled {
                            id,
                            expected_version: version,
                            terminal_reason: "recovery_no_replayable_result".to_owned(),
                            settled_at: terminal.updated_at.clone(),
                            updated_at: terminal.updated_at.clone(),
                        },
                    )
                    .await?;
                }
            } else {
                // A provider may return no report at all. Still close every
                // started/pending invocation explicitly as unmetered so no
                // call remains in an ambiguous pending state after
                // terminalization.
                let active_rows = sqlx::query(
                    "SELECT id, version FROM usage_invocation
                     WHERE execution_id = ? AND lifecycle IN ('started', 'pending_settlement')",
                )
                .bind(&terminal.execution_id)
                .fetch_all(&mut *transaction)
                .await?;
                for row in active_rows {
                    let id: String = row.try_get("id")?;
                    if settled_ids.contains(&id) {
                        continue;
                    }
                    let version: i64 = row.try_get("version")?;
                    self.settle_usage_invocation_in_tx(
                        &mut transaction,
                        SettleUsageInvocation {
                            id,
                            expected_version: version,
                            telemetry_state: UsageTelemetryState::Unmetered,
                            terminal_reason: Some("missing_usage_report".to_owned()),
                            settled_at: terminal.updated_at.clone(),
                            updated_at: terminal.updated_at.clone(),
                        },
                    )
                    .await?;
                }
            }
        }

        let event = DomainEventRepo::append_event_in_tx(self, &mut transaction, &event).await?;
        if let (Some(terminal_report_id), Some(terminal_report_digest)) = (
            input.terminal_report_id.as_deref(),
            input.terminal_report_digest.as_deref(),
        ) {
            persist_terminal_receipt_in_tx(
                &mut transaction,
                &ExecutionTerminalReceipt {
                    terminal_report_id: terminal_report_id.to_owned(),
                    execution_id: terminal.execution_id.clone(),
                    payload_digest: terminal_report_digest.to_owned(),
                    event_id: event.id.clone(),
                    created_at: event.created_at.clone(),
                },
            )
            .await?;
        }
        // Late accounting may settle after cancellation; only the winning
        // terminal update owns an execution's plan candidate.
        if let Some(plan) = input.plan.as_ref().filter(|_| terminal_cas_won) {
            sqlx::query("INSERT INTO execution_plan_transport (execution_id, candidate_text, candidate_error, candidate_size, terminal_report_id, updated_at)
                VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(execution_id) DO UPDATE SET
                candidate_text=excluded.candidate_text, candidate_error=excluded.candidate_error,
                candidate_size=excluded.candidate_size, terminal_report_id=excluded.terminal_report_id,
                updated_at=excluded.updated_at")
                .bind(&terminal.execution_id).bind(&plan.content).bind(&plan.error).bind(plan.size)
                .bind(&input.terminal_report_id).bind(&terminal.updated_at)
                .execute(&mut *transaction).await?;
        }
        // The terminal result is authoritative; the shadow never refuses it.
        if updated.role != "interactive" {
            crate::task_condition::produce_best_effort(
                &mut transaction,
                &updated.task_id,
                crate::ConditionChange::Execution,
            )
            .await;
        }
        transaction.commit().await?;
        Ok(ExecutionTerminalOutcome::Committed {
            execution: updated,
            event: Box::new(event),
            workspace_lease_id,
            workspace_lease_status,
            replayed: false,
        })
    }
}
