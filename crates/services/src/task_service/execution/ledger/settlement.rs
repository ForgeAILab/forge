//! Usage-report settlement preparation and terminal ledger payloads.

use super::*;

pub(crate) async fn build_task_usage_settlements(
    db: &SqliteDb,
    execution_id: &str,
    reports: &[UsageReport],
    now: &str,
) -> Result<Vec<UsageLedgerSettlement>> {
    build_task_usage_settlements_with_prepared_invocations(db, execution_id, reports, now, &[])
        .await
}

pub(crate) async fn build_task_usage_settlements_with_prepared_invocations(
    db: &SqliteDb,
    execution_id: &str,
    reports: &[UsageReport],
    now: &str,
    prepared: &[CreateUsageInvocation],
) -> Result<Vec<UsageLedgerSettlement>> {
    let mut invocations =
        db::UsageLedgerRepo::list_usage_invocations_for_source(db, execution_id).await?;
    let existing_ids: std::collections::HashSet<String> = invocations
        .iter()
        .map(|invocation| invocation.id.clone())
        .collect();
    for input in prepared {
        if existing_ids.contains(&input.id) {
            continue;
        }
        invocations.push(UsageInvocation {
            id: input.id.clone(),
            owner_user_id: input.owner_user_id.clone(),
            project_id: input.project_id.clone(),
            domain_kind: input.domain_kind,
            surface: input.surface,
            source_id: input.source_id.clone(),
            execution_id: input.execution_id.clone(),
            task_id: input.task_id.clone(),
            domain_idempotency_key: input.domain_idempotency_key.clone(),
            candidate_key: input.candidate_key.clone(),
            attempt_ordinal: input.attempt_ordinal,
            pricing_selection_id: input.pricing_selection_id.clone(),
            admitted_provider_id: input.admitted_provider_id.clone(),
            admitted_model_id: input.admitted_model_id.clone(),
            admitted_runtime_model: input.admitted_runtime_model.clone(),
            pricing_subject_id: input.pricing_subject_id.clone(),
            pricing_subject_revision_id: input.pricing_subject_revision_id.clone(),
            subject_revision_digest: input.subject_revision_digest.clone(),
            agent_id: input.agent_id.clone(),
            profile_id: input.profile_id.clone(),
            agent_name_snapshot: input.agent_name_snapshot.clone(),
            project_name_snapshot: input.project_name_snapshot.clone(),
            executor_type: input.executor_type.clone(),
            backend_kind: input.backend_kind.clone(),
            provenance_kind: input.provenance_kind,
            lifecycle: UsageInvocationLifecycle::Started,
            telemetry_state: UsageTelemetryState::Pending,
            terminal_reason: None,
            version: 2,
            admitted_at: input.admitted_at.clone(),
            started_at: Some(now.to_owned()),
            settled_at: None,
            created_at: input.created_at.clone(),
            updated_at: input.updated_at.clone(),
        });
    }
    let selections =
        db::UsageLedgerRepo::list_pricing_selections_for_source(db, execution_id).await?;
    let selections_by_id: HashMap<String, PricingSelection> = selections
        .into_iter()
        .map(|selection| (selection.id.clone(), selection))
        .collect();
    let mut settlements = Vec::with_capacity(invocations.len());
    for invocation in invocations {
        let is_settled = invocation.lifecycle == UsageInvocationLifecycle::Settled;
        if !is_settled
            && !matches!(
                invocation.lifecycle,
                UsageInvocationLifecycle::Admitted
                    | UsageInvocationLifecycle::Started
                    | UsageInvocationLifecycle::PendingSettlement
            )
        {
            continue;
        }
        let selection = selections_by_id
            .get(&invocation.pricing_selection_id)
            .ok_or_else(|| {
                ServiceError::invalid_operation("usage invocation selection is missing")
            })?;
        validate_persisted_selection_digest(db, selection).await?;
        let matching = reports
            .iter()
            .filter(|report| {
                i64::from(report.attempt_ordinal) == invocation.attempt_ordinal
                    && report.candidate_key.as_deref() == invocation.candidate_key.as_deref()
            })
            .collect::<Vec<_>>();
        // A settled invocation is only included when this delivery contains
        // a report for it. This lets the DB boundary compare an exact replay
        // while keeping unrelated terminal reports a no-op.
        if is_settled && matching.is_empty() {
            continue;
        }
        let existing_events = if is_settled {
            db::UsageLedgerRepo::list_usage_events_for_invocation(db, &invocation.id).await?
        } else {
            Vec::new()
        };
        let settled_at = invocation
            .settled_at
            .clone()
            .unwrap_or_else(|| now.to_owned());
        let updated_at = if is_settled {
            invocation.updated_at.clone()
        } else {
            now.to_owned()
        };
        let mut events = Vec::new();
        let mut metered = false;
        for report in matching {
            if report.telemetry_state == executors::UsageTelemetryState::Metered
                && report.counters.has_any()
            {
                metered = true;
            }
            let source_report_id = opaque_source_report_id(&invocation.id, report);
            let event_now = existing_events
                .iter()
                .find(|event| event.source_report_id == source_report_id)
                .map(|event| event.created_at.as_str())
                .unwrap_or(settled_at.as_str());
            if let Some(mut event) =
                usage_event_for_report(db, &invocation, selection, report, event_now).await?
            {
                // Preserve both immutable timestamps when rebuilding a
                // settled report. Task reports normally use one timestamp,
                // but a replay must compare every event field exactly.
                if let Some(existing) = existing_events
                    .iter()
                    .find(|existing| existing.source_report_id == source_report_id)
                {
                    event.occurred_at = existing.occurred_at.clone();
                    event.created_at = existing.created_at.clone();
                }
                events.push(event);
            }
        }
        settlements.push(UsageLedgerSettlement {
            invocation_id: invocation.id,
            expected_version: invocation.version,
            telemetry_state: if metered {
                UsageTelemetryState::Metered
            } else {
                UsageTelemetryState::Unmetered
            },
            terminal_reason: if metered {
                None
            } else {
                Some("unmetered".to_owned())
            },
            settled_at,
            updated_at,
            events,
        });
    }
    Ok(settlements)
}

/// Transaction-scoped counterpart used by remote terminal preparation. Every
/// pricing-selection/rate/invocation/event read stays behind the same
/// `BEGIN IMMEDIATE` owner guard; the caller commits the guard only after this
/// complete immutable preparation has finished.
pub(crate) async fn build_task_usage_settlements_with_prepared_invocations_in_tx(
    db: &SqliteDb,
    tx: &mut Transaction<'_, Sqlite>,
    execution_id: &str,
    reports: &[UsageReport],
    now: &str,
    prepared: &[CreateUsageInvocation],
) -> Result<Vec<UsageLedgerSettlement>> {
    let mut invocations =
        db::UsageLedgerRepo::list_usage_invocations_for_source_in_tx(db, tx, execution_id).await?;
    let existing_ids: std::collections::HashSet<String> = invocations
        .iter()
        .map(|invocation| invocation.id.clone())
        .collect();
    for input in prepared {
        if existing_ids.contains(&input.id) {
            continue;
        }
        invocations.push(UsageInvocation {
            id: input.id.clone(),
            owner_user_id: input.owner_user_id.clone(),
            project_id: input.project_id.clone(),
            domain_kind: input.domain_kind,
            surface: input.surface,
            source_id: input.source_id.clone(),
            execution_id: input.execution_id.clone(),
            task_id: input.task_id.clone(),
            domain_idempotency_key: input.domain_idempotency_key.clone(),
            candidate_key: input.candidate_key.clone(),
            attempt_ordinal: input.attempt_ordinal,
            pricing_selection_id: input.pricing_selection_id.clone(),
            admitted_provider_id: input.admitted_provider_id.clone(),
            admitted_model_id: input.admitted_model_id.clone(),
            admitted_runtime_model: input.admitted_runtime_model.clone(),
            pricing_subject_id: input.pricing_subject_id.clone(),
            pricing_subject_revision_id: input.pricing_subject_revision_id.clone(),
            subject_revision_digest: input.subject_revision_digest.clone(),
            agent_id: input.agent_id.clone(),
            profile_id: input.profile_id.clone(),
            agent_name_snapshot: input.agent_name_snapshot.clone(),
            project_name_snapshot: input.project_name_snapshot.clone(),
            executor_type: input.executor_type.clone(),
            backend_kind: input.backend_kind.clone(),
            provenance_kind: input.provenance_kind,
            lifecycle: UsageInvocationLifecycle::Started,
            telemetry_state: UsageTelemetryState::Pending,
            terminal_reason: None,
            version: 2,
            admitted_at: input.admitted_at.clone(),
            started_at: Some(now.to_owned()),
            settled_at: None,
            created_at: input.created_at.clone(),
            updated_at: input.updated_at.clone(),
        });
    }
    let selections =
        db::UsageLedgerRepo::list_pricing_selections_for_source_in_tx(db, tx, execution_id).await?;
    let selections_by_id: HashMap<String, PricingSelection> = selections
        .into_iter()
        .map(|selection| (selection.id.clone(), selection))
        .collect();
    let mut settlements = Vec::with_capacity(invocations.len());
    for invocation in invocations {
        let is_settled = invocation.lifecycle == UsageInvocationLifecycle::Settled;
        if !is_settled
            && !matches!(
                invocation.lifecycle,
                UsageInvocationLifecycle::Admitted
                    | UsageInvocationLifecycle::Started
                    | UsageInvocationLifecycle::PendingSettlement
            )
        {
            continue;
        }
        let selection = selections_by_id
            .get(&invocation.pricing_selection_id)
            .ok_or_else(|| {
                ServiceError::invalid_operation("usage invocation selection is missing")
            })?;
        validate_persisted_selection_digest_in_tx(db, tx, selection).await?;
        let matching = reports
            .iter()
            .filter(|report| {
                i64::from(report.attempt_ordinal) == invocation.attempt_ordinal
                    && report.candidate_key.as_deref() == invocation.candidate_key.as_deref()
            })
            .collect::<Vec<_>>();
        if is_settled && matching.is_empty() {
            continue;
        }
        let existing_events = if is_settled {
            db::UsageLedgerRepo::list_usage_events_for_invocation_in_tx(db, tx, &invocation.id)
                .await?
        } else {
            Vec::new()
        };
        let settled_at = invocation
            .settled_at
            .clone()
            .unwrap_or_else(|| now.to_owned());
        let updated_at = if is_settled {
            invocation.updated_at.clone()
        } else {
            now.to_owned()
        };
        let mut events = Vec::new();
        let mut metered = false;
        for report in matching {
            if report.telemetry_state == executors::UsageTelemetryState::Metered
                && report.counters.has_any()
            {
                metered = true;
            }
            let source_report_id = opaque_source_report_id(&invocation.id, report);
            let event_now = existing_events
                .iter()
                .find(|event| event.source_report_id == source_report_id)
                .map(|event| event.created_at.as_str())
                .unwrap_or(settled_at.as_str());
            if let Some(mut event) =
                usage_event_for_report_in_tx(db, tx, &invocation, selection, report, event_now)
                    .await?
            {
                if let Some(existing) = existing_events
                    .iter()
                    .find(|existing| existing.source_report_id == source_report_id)
                {
                    event.occurred_at = existing.occurred_at.clone();
                    event.created_at = existing.created_at.clone();
                }
                events.push(event);
            }
        }
        settlements.push(UsageLedgerSettlement {
            invocation_id: invocation.id,
            expected_version: invocation.version,
            telemetry_state: if metered {
                UsageTelemetryState::Metered
            } else {
                UsageTelemetryState::Unmetered
            },
            terminal_reason: if metered {
                None
            } else {
                Some("unmetered".to_owned())
            },
            settled_at,
            updated_at,
            events,
        });
    }
    Ok(settlements)
}

pub(crate) async fn settle_late_task_usage(
    db: &SqliteDb,
    execution_id: &str,
    reports: &[UsageReport],
    now: &str,
) -> Result<()> {
    let settlements = build_task_usage_settlements(db, execution_id, reports, now).await?;
    if !settlements.is_empty() {
        db::UsageLedgerRepo::settle_usage_invocations_with_events(db, settlements).await?;
    }
    Ok(())
}

pub(crate) fn terminal_with_ledger(
    terminal: db::TerminalizeExecution,
    settlements: Vec<UsageLedgerSettlement>,
    terminal_report_id: Option<String>,
    terminal_report_digest: Option<String>,
) -> TerminalizeExecutionWithLedger {
    TerminalizeExecutionWithLedger {
        plan: None,
        terminal,
        settlements,
        terminal_report_id,
        terminal_report_digest,
        mark_unreplayable_pending_unsettled: false,
        allow_late_settlement: false,
        preserve_pending_settlement: false,
        require_live_owner_lease: false,
    }
}

pub(crate) fn terminal_with_late_ledger(
    terminal: db::TerminalizeExecution,
    settlements: Vec<UsageLedgerSettlement>,
    terminal_report_id: Option<String>,
    terminal_report_digest: Option<String>,
) -> TerminalizeExecutionWithLedger {
    let mut input = terminal_with_ledger(
        terminal,
        settlements,
        terminal_report_id,
        terminal_report_digest,
    );
    input.allow_late_settlement = true;
    // The remote owner is reporting its own terminal outcome, so it proves a
    // live lease and hard deadline in the same CAS.
    input.require_live_owner_lease = true;
    input
}
