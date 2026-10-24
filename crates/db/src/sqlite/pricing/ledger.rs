//! Pricing selections, usage invocations, and ledger event writes.

use super::*;

#[async_trait]
impl UsageLedgerRepo for SqliteDb {
    async fn create_pricing_selection(
        &self,
        input: CreatePricingSelection,
    ) -> Result<PricingSelection> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let selection = self
            .create_pricing_selection_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(selection)
    }

    async fn create_pricing_selection_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreatePricingSelection,
    ) -> Result<PricingSelection> {
        // The candidate identity has its own unique index.  Resolve it before
        // looking at the digest so a retry with a different digest cannot
        // accidentally return the existing candidate row.
        if let Some(row) = sqlx::query(
            "SELECT * FROM pricing_selection
             WHERE owner_user_id IS ? AND project_id IS ?
               AND domain_kind = ? AND surface = ? AND source_id = ?
               AND candidate_key IS ? AND attempt_ordinal = ?",
        )
        .bind(input.owner_user_id.as_deref())
        .bind(input.project_id.as_deref())
        .bind(input.domain_kind.to_string())
        .bind(input.surface.to_string())
        .bind(&input.source_id)
        .bind(input.candidate_key.as_deref())
        .bind(input.attempt_ordinal)
        .fetch_optional(&mut **transaction)
        .await?
        {
            let existing = map_selection(row)?;
            if existing.selection_digest == input.selection_digest
                && pricing_selection_matches_input(&existing, &input)
            {
                return Ok(existing);
            }
            return Err(DbError::IdempotencyConflict);
        }
        let result = sqlx::query(
            "INSERT INTO pricing_selection (
                id, owner_user_id, project_id, domain_kind, surface, source_id,
                execution_id, task_id, candidate_key, attempt_ordinal,
                invocation_id, subject_id, subject_revision_id,
                subject_revision_digest, binding_id, rate_revision_id,
                catalog_snapshot_id, catalog_freshness, runtime_model, admitted_provider_id,
                admitted_model_id, source_kind, provenance_kind, selection_status,
                selection_reason, selection_digest, selected_at, created_at
             ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
             )",
        )
        .bind(&input.id)
        .bind(input.owner_user_id.as_deref())
        .bind(input.project_id.as_deref())
        .bind(input.domain_kind.to_string())
        .bind(input.surface.to_string())
        .bind(&input.source_id)
        .bind(input.execution_id.as_deref())
        .bind(input.task_id.as_deref())
        .bind(input.candidate_key.as_deref())
        .bind(input.attempt_ordinal)
        .bind(input.subject_id.as_deref())
        .bind(input.subject_revision_id.as_deref())
        .bind(input.subject_revision_digest.as_deref())
        .bind(input.binding_id.as_deref())
        .bind(input.rate_revision_id.as_deref())
        .bind(input.catalog_snapshot_id.as_deref())
        .bind(input.catalog_freshness.as_deref())
        .bind(input.runtime_model.as_deref())
        .bind(input.admitted_provider_id.as_deref())
        .bind(input.admitted_model_id.as_deref())
        .bind(input.source_kind.map(|value| value.to_string()))
        .bind(input.provenance_kind.to_string())
        .bind(input.selection_status.to_string())
        .bind(input.selection_reason.map(|value| value.to_string()))
        .bind(&input.selection_digest)
        .bind(&input.selected_at)
        .bind(&input.created_at)
        .execute(&mut **transaction)
        .await;
        if let Err(error) = result {
            if let Some(row) = sqlx::query(
                "SELECT * FROM pricing_selection
                 WHERE owner_user_id IS ? AND project_id IS ?
                   AND domain_kind = ? AND surface = ? AND source_id = ?
                   AND candidate_key IS ? AND attempt_ordinal = ?",
            )
            .bind(input.owner_user_id.as_deref())
            .bind(input.project_id.as_deref())
            .bind(input.domain_kind.to_string())
            .bind(input.surface.to_string())
            .bind(&input.source_id)
            .bind(input.candidate_key.as_deref())
            .bind(input.attempt_ordinal)
            .fetch_optional(&mut **transaction)
            .await?
            {
                let existing = map_selection(row)?;
                if existing.selection_digest == input.selection_digest
                    && pricing_selection_matches_input(&existing, &input)
                {
                    return Ok(existing);
                }
                return Err(DbError::IdempotencyConflict);
            }
            return Err(duplicate_error(error));
        }
        sqlx::query("SELECT * FROM pricing_selection WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut **transaction)
            .await
            .map_err(check_error)
            .and_then(map_selection)
    }

    async fn get_pricing_selection(&self, id: &str) -> Result<Option<PricingSelection>> {
        sqlx::query("SELECT * FROM pricing_selection WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .map(map_selection)
            .transpose()
    }

    async fn list_pricing_selections_for_source(
        &self,
        source_id: &str,
    ) -> Result<Vec<PricingSelection>> {
        sqlx::query(
            "SELECT * FROM pricing_selection
             WHERE source_id = ? ORDER BY attempt_ordinal ASC, id ASC",
        )
        .bind(source_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_selection)
        .collect()
    }

    async fn list_pricing_selections_for_source_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        source_id: &str,
    ) -> Result<Vec<PricingSelection>> {
        sqlx::query(
            "SELECT * FROM pricing_selection
             WHERE source_id = ? ORDER BY attempt_ordinal ASC, id ASC",
        )
        .bind(source_id)
        .fetch_all(&mut **transaction)
        .await?
        .into_iter()
        .map(map_selection)
        .collect()
    }

    async fn create_usage_invocation(
        &self,
        input: CreateUsageInvocation,
    ) -> Result<UsageInvocation> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let invocation = self
            .create_usage_invocation_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(invocation)
    }

    async fn create_usage_invocation_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreateUsageInvocation,
    ) -> Result<UsageInvocation> {
        if let Some(row) =
            sqlx::query("SELECT * FROM usage_invocation WHERE domain_idempotency_key = ?")
                .bind(&input.domain_idempotency_key)
                .fetch_optional(&mut **transaction)
                .await?
        {
            let existing = map_invocation(row)?;
            if invocation_matches_input(&existing, &input) {
                return Ok(existing);
            }
            return Err(DbError::IdempotencyConflict);
        }
        sqlx::query(
            "INSERT INTO usage_invocation (
                id, owner_user_id, project_id, domain_kind, surface, source_id,
                execution_id, task_id, domain_idempotency_key, candidate_key,
                attempt_ordinal, pricing_selection_id, admitted_provider_id,
                admitted_model_id, admitted_runtime_model, pricing_subject_id,
                pricing_subject_revision_id, subject_revision_digest, agent_id,
                profile_id, agent_name_snapshot, project_name_snapshot,
                executor_type, backend_kind, provenance_kind, lifecycle,
                telemetry_state, terminal_reason, version, admitted_at,
                started_at, settled_at, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'admitted', 'pending', NULL, 1, ?, NULL, NULL, ?, ?)",
        )
        .bind(&input.id)
        .bind(input.owner_user_id.as_deref())
        .bind(input.project_id.as_deref())
        .bind(input.domain_kind.to_string())
        .bind(input.surface.to_string())
        .bind(&input.source_id)
        .bind(input.execution_id.as_deref())
        .bind(input.task_id.as_deref())
        .bind(&input.domain_idempotency_key)
        .bind(input.candidate_key.as_deref())
        .bind(input.attempt_ordinal)
        .bind(&input.pricing_selection_id)
        .bind(input.admitted_provider_id.as_deref())
        .bind(input.admitted_model_id.as_deref())
        .bind(input.admitted_runtime_model.as_deref())
        .bind(input.pricing_subject_id.as_deref())
        .bind(input.pricing_subject_revision_id.as_deref())
        .bind(input.subject_revision_digest.as_deref())
        .bind(input.agent_id.as_deref())
        .bind(input.profile_id.as_deref())
        .bind(input.agent_name_snapshot.as_deref())
        .bind(input.project_name_snapshot.as_deref())
        .bind(input.executor_type.as_deref())
        .bind(input.backend_kind.as_deref())
        .bind(input.provenance_kind.to_string())
        .bind(&input.admitted_at)
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&mut **transaction)
        .await
        .map_err(duplicate_error)?;

        let link = sqlx::query(
            "UPDATE pricing_selection SET invocation_id = ?
             WHERE id = ? AND invocation_id IS NULL",
        )
        .bind(&input.id)
        .bind(&input.pricing_selection_id)
        .execute(&mut **transaction)
        .await
        .map_err(check_error)?;
        if link.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }
        sqlx::query("SELECT * FROM usage_invocation WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut **transaction)
            .await
            .map_err(check_error)
            .and_then(map_invocation)
    }

    async fn get_usage_invocation(&self, id: &str) -> Result<Option<UsageInvocation>> {
        sqlx::query("SELECT * FROM usage_invocation WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .map(map_invocation)
            .transpose()
    }

    async fn get_usage_invocation_by_idempotency(
        &self,
        domain_idempotency_key: &str,
    ) -> Result<Option<UsageInvocation>> {
        sqlx::query("SELECT * FROM usage_invocation WHERE domain_idempotency_key = ?")
            .bind(domain_idempotency_key)
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .map(map_invocation)
            .transpose()
    }

    async fn list_usage_invocations_for_source(
        &self,
        source_id: &str,
    ) -> Result<Vec<UsageInvocation>> {
        sqlx::query(
            "SELECT * FROM usage_invocation
             WHERE source_id = ? ORDER BY attempt_ordinal ASC, id ASC",
        )
        .bind(source_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_invocation)
        .collect()
    }

    async fn list_usage_invocations_for_source_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        source_id: &str,
    ) -> Result<Vec<UsageInvocation>> {
        sqlx::query(
            "SELECT * FROM usage_invocation
             WHERE source_id = ? ORDER BY attempt_ordinal ASC, id ASC",
        )
        .bind(source_id)
        .fetch_all(&mut **transaction)
        .await?
        .into_iter()
        .map(map_invocation)
        .collect()
    }

    async fn list_usage_invocations_needing_settlement(
        &self,
        limit: i64,
    ) -> Result<Vec<UsageInvocation>> {
        sqlx::query(
            "SELECT * FROM usage_invocation
             WHERE lifecycle IN ('started', 'pending_settlement')
             ORDER BY COALESCE(started_at, admitted_at) ASC, id ASC
             LIMIT ?",
        )
        .bind(limit.clamp(1, 5000))
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_invocation)
        .collect()
    }

    async fn list_usage_invocations_for_project(
        &self,
        project_id: &str,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<Vec<UsageInvocation>> {
        let mut builder =
            sqlx::QueryBuilder::<Sqlite>::new("SELECT * FROM usage_invocation WHERE project_id = ");
        builder.push_bind(project_id);
        if let Some(from) = from {
            builder.push(" AND admitted_at >= ").push_bind(from);
        }
        if let Some(to) = to {
            builder.push(" AND admitted_at < ").push_bind(to);
        }
        builder.push(" ORDER BY admitted_at ASC, id ASC");
        builder
            .build()
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(map_invocation)
            .collect()
    }

    async fn start_usage_invocation(&self, input: StartUsageInvocation) -> Result<UsageInvocation> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let invocation = self
            .start_usage_invocation_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(invocation)
    }

    async fn start_usage_invocation_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: StartUsageInvocation,
    ) -> Result<UsageInvocation> {
        let result = sqlx::query(
            "UPDATE usage_invocation
             SET lifecycle = 'started', telemetry_state = 'pending', started_at = ?,
                 version = version + 1, updated_at = ?
             WHERE id = ? AND version = ? AND lifecycle = 'admitted'
               AND started_at IS NULL",
        )
        .bind(&input.started_at)
        .bind(&input.updated_at)
        .bind(&input.id)
        .bind(input.expected_version)
        .execute(&mut **transaction)
        .await
        .map_err(check_error)?;
        if result.rows_affected() == 0 {
            return conflict_or_not_found(
                &input.id,
                input.expected_version,
                fetch_version(transaction, "usage_invocation", &input.id).await?,
            );
        }
        select_invocation_in_tx(transaction, &input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn mark_usage_invocation_pending_settlement(
        &self,
        input: MarkUsageInvocationPendingSettlement,
    ) -> Result<UsageInvocation> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let invocation = self
            .mark_usage_invocation_pending_settlement_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(invocation)
    }

    async fn mark_usage_invocation_pending_settlement_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: MarkUsageInvocationPendingSettlement,
    ) -> Result<UsageInvocation> {
        let result = sqlx::query(
            "UPDATE usage_invocation
             SET lifecycle = 'pending_settlement', updated_at = ?, version = version + 1
             WHERE id = ? AND version = ? AND lifecycle = 'started'",
        )
        .bind(&input.updated_at)
        .bind(&input.id)
        .bind(input.expected_version)
        .execute(&mut **transaction)
        .await
        .map_err(check_error)?;
        if result.rows_affected() == 0 {
            return conflict_or_not_found(
                &input.id,
                input.expected_version,
                fetch_version(transaction, "usage_invocation", &input.id).await?,
            );
        }
        select_invocation_in_tx(transaction, &input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn settle_usage_invocation(
        &self,
        input: SettleUsageInvocation,
    ) -> Result<UsageInvocation> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let invocation = self
            .settle_usage_invocation_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(invocation)
    }

    async fn settle_usage_invocation_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: SettleUsageInvocation,
    ) -> Result<UsageInvocation> {
        if !matches!(
            input.telemetry_state,
            UsageTelemetryState::Metered | UsageTelemetryState::Unmetered
        ) {
            return Err(DbError::Check(
                "settled invocation must be metered or unmetered".to_owned(),
            ));
        }
        let result = sqlx::query(
            "UPDATE usage_invocation
             SET lifecycle = 'settled', telemetry_state = ?, terminal_reason = ?,
                 settled_at = ?, updated_at = ?, version = version + 1
             WHERE id = ? AND version = ?
               AND lifecycle IN ('started', 'pending_settlement')
               AND settled_at IS NULL",
        )
        .bind(input.telemetry_state.to_string())
        .bind(input.terminal_reason.as_deref())
        .bind(&input.settled_at)
        .bind(&input.updated_at)
        .bind(&input.id)
        .bind(input.expected_version)
        .execute(&mut **transaction)
        .await
        .map_err(check_error)?;
        if result.rows_affected() == 0 {
            return conflict_or_not_found(
                &input.id,
                input.expected_version,
                fetch_version(transaction, "usage_invocation", &input.id).await?,
            );
        }
        select_invocation_in_tx(transaction, &input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn mark_usage_invocation_unsettled(
        &self,
        input: MarkUsageInvocationUnsettled,
    ) -> Result<UsageInvocation> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let invocation = self
            .mark_usage_invocation_unsettled_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(invocation)
    }

    async fn mark_usage_invocation_unsettled_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: MarkUsageInvocationUnsettled,
    ) -> Result<UsageInvocation> {
        if input.terminal_reason.trim().is_empty() {
            return Err(DbError::Check(
                "unsettled invocation needs a reason".to_owned(),
            ));
        }
        let result = sqlx::query(
            "UPDATE usage_invocation
             SET lifecycle = 'unsettled', telemetry_state = 'unsettled',
                 terminal_reason = ?, settled_at = ?, updated_at = ?, version = version + 1
             WHERE id = ? AND version = ?
               AND lifecycle IN ('started', 'pending_settlement')
               AND settled_at IS NULL",
        )
        .bind(&input.terminal_reason)
        .bind(&input.settled_at)
        .bind(&input.updated_at)
        .bind(&input.id)
        .bind(input.expected_version)
        .execute(&mut **transaction)
        .await
        .map_err(check_error)?;
        if result.rows_affected() == 0 {
            return conflict_or_not_found(
                &input.id,
                input.expected_version,
                fetch_version(transaction, "usage_invocation", &input.id).await?,
            );
        }
        select_invocation_in_tx(transaction, &input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn append_usage_event(&self, input: CreateUsageEvent) -> Result<UsageEvent> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let event = self
            .append_usage_event_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(event)
    }

    async fn append_usage_event_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreateUsageEvent,
    ) -> Result<UsageEvent> {
        if let Some(row) = sqlx::query("SELECT * FROM usage_event WHERE event_idempotency_key = ?")
            .bind(&input.event_idempotency_key)
            .fetch_optional(&mut **transaction)
            .await?
        {
            let existing = map_usage_event(row)?;
            if usage_event_matches_input(&existing, &input) {
                return Ok(existing);
            }
            return Err(DbError::IdempotencyConflict);
        }

        // Report identity is independent from the caller's idempotency key.
        // A changed retry payload must not create a second billable event.
        if let Some(row) = sqlx::query(
            "SELECT * FROM usage_event
             WHERE invocation_id = ? AND source_report_id = ?",
        )
        .bind(&input.invocation_id)
        .bind(&input.source_report_id)
        .fetch_optional(&mut **transaction)
        .await?
        {
            let existing = map_usage_event(row)?;
            if usage_event_matches_input(&existing, &input) {
                return Ok(existing);
            }
            return Err(DbError::IdempotencyConflict);
        }
        if let Some(row) = sqlx::query(
            "SELECT * FROM usage_event
             WHERE invocation_id = ? AND report_mode = ? AND report_sequence = ?",
        )
        .bind(&input.invocation_id)
        .bind(input.report_mode.to_string())
        .bind(input.report_sequence)
        .fetch_optional(&mut **transaction)
        .await?
        {
            let existing = map_usage_event(row)?;
            if usage_event_matches_input(&existing, &input) {
                return Ok(existing);
            }
            return Err(DbError::IdempotencyConflict);
        }

        let mut query = sqlx::QueryBuilder::<Sqlite>::new(
            "INSERT INTO usage_event (
                id, invocation_id, owner_user_id, project_id, surface, source_id,
                execution_id, task_id, event_idempotency_key, source_report_id,
                report_sequence, report_mode, provenance_kind, legacy_source_table,
                legacy_source_id, legacy_provider_raw, legacy_provider_sqlite_type,
                legacy_provider_sql_literal, legacy_model_raw, legacy_model_sqlite_type,
                legacy_model_sql_literal, legacy_counter_values_json,
                legacy_cost_usd_raw, legacy_created_at_raw, legacy_project_owner_raw,
                legacy_invalid_usage, provider_id, model_id, runtime_model,
                candidate_key, attempt_ordinal, agent_id, profile_id,
                agent_name_snapshot, project_name_snapshot, executor_type,
                pricing_subject_revision_id, subject_revision_digest, telemetry_state,
                input_tokens, output_tokens, cache_read_tokens, cache_write_tokens,
                context_tokens, selected_tier, provider_reported_nano_usd,
                legacy_reported_cost_usd, estimated_nano_usd, cost_kind,
                rate_revision_id, catalog_snapshot_id, formula_revision,
                retrospective, coverage_reason_code, occurred_at, created_at
             ) VALUES (",
        );
        query.push_bind(&input.id).push(", ");
        query.push_bind(&input.invocation_id).push(", ");
        query.push_bind(input.owner_user_id.as_deref()).push(", ");
        query.push_bind(input.project_id.as_deref()).push(", ");
        query.push_bind(input.surface.to_string()).push(", ");
        query.push_bind(&input.source_id).push(", ");
        query.push_bind(input.execution_id.as_deref()).push(", ");
        query.push_bind(input.task_id.as_deref()).push(", ");
        query.push_bind(&input.event_idempotency_key).push(", ");
        query.push_bind(&input.source_report_id).push(", ");
        query.push_bind(input.report_sequence).push(", ");
        query.push_bind(input.report_mode.to_string()).push(", ");
        query
            .push_bind(input.provenance_kind.to_string())
            .push(", ");
        query
            .push_bind(input.legacy_source_table.as_deref())
            .push(", ");
        query
            .push_bind(input.legacy_source_id.as_deref())
            .push(", ");
        query
            .push_bind(input.legacy_provider_raw.as_deref())
            .push(", ");
        query
            .push_bind(input.legacy_provider_sqlite_type.as_deref())
            .push(", ");
        query
            .push_bind(input.legacy_provider_sql_literal.as_deref())
            .push(", ");
        query
            .push_bind(input.legacy_model_raw.as_deref())
            .push(", ");
        query
            .push_bind(input.legacy_model_sqlite_type.as_deref())
            .push(", ");
        query
            .push_bind(input.legacy_model_sql_literal.as_deref())
            .push(", ");
        query
            .push_bind(&input.legacy_counter_values_json)
            .push(", ");
        query
            .push_bind(input.legacy_cost_usd_raw.as_deref())
            .push(", ");
        query
            .push_bind(input.legacy_created_at_raw.as_deref())
            .push(", ");
        query
            .push_bind(input.legacy_project_owner_raw.as_deref())
            .push(", ");
        query
            .push_bind(i64::from(input.legacy_invalid_usage))
            .push(", ");
        query.push_bind(input.provider_id.as_deref()).push(", ");
        query.push_bind(input.model_id.as_deref()).push(", ");
        query.push_bind(input.runtime_model.as_deref()).push(", ");
        query.push_bind(input.candidate_key.as_deref()).push(", ");
        query.push_bind(input.attempt_ordinal).push(", ");
        query.push_bind(input.agent_id.as_deref()).push(", ");
        query.push_bind(input.profile_id.as_deref()).push(", ");
        query
            .push_bind(input.agent_name_snapshot.as_deref())
            .push(", ");
        query
            .push_bind(input.project_name_snapshot.as_deref())
            .push(", ");
        query.push_bind(input.executor_type.as_deref()).push(", ");
        query
            .push_bind(input.pricing_subject_revision_id.as_deref())
            .push(", ");
        query
            .push_bind(input.subject_revision_digest.as_deref())
            .push(", ");
        query
            .push_bind(input.telemetry_state.to_string())
            .push(", ");
        query.push_bind(input.input_tokens).push(", ");
        query.push_bind(input.output_tokens).push(", ");
        query.push_bind(input.cache_read_tokens).push(", ");
        query.push_bind(input.cache_write_tokens).push(", ");
        query.push_bind(input.context_tokens).push(", ");
        query.push_bind(input.selected_tier.as_deref()).push(", ");
        query.push_bind(input.provider_reported_nano_usd).push(", ");
        query.push_bind(input.legacy_reported_cost_usd).push(", ");
        query.push_bind(input.estimated_nano_usd).push(", ");
        query.push_bind(input.cost_kind.to_string()).push(", ");
        query
            .push_bind(input.rate_revision_id.as_deref())
            .push(", ");
        query
            .push_bind(input.catalog_snapshot_id.as_deref())
            .push(", ");
        query
            .push_bind(input.formula_revision.as_deref())
            .push(", ");
        query.push_bind(i64::from(input.retrospective)).push(", ");
        query
            .push_bind(input.coverage_reason_code.map(|value| value.to_string()))
            .push(", ");
        query.push_bind(&input.occurred_at).push(", ");
        query.push_bind(&input.created_at).push(")");
        query
            .build()
            .execute(&mut **transaction)
            .await
            .map_err(duplicate_error)?;
        select_usage_event_in_tx(transaction, &input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn settle_usage_invocations_with_events(
        &self,
        settlements: Vec<UsageLedgerSettlement>,
    ) -> Result<Vec<UsageInvocation>> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let mut settled = Vec::with_capacity(settlements.len());
        for settlement in settlements {
            let existing = select_invocation_in_tx(&mut transaction, &settlement.invocation_id)
                .await?
                .ok_or(DbError::NotFound)?;
            if existing.lifecycle == UsageInvocationLifecycle::Settled {
                validate_settled_usage_invocation_in_tx(&mut transaction, &existing, &settlement)
                    .await?;
                settled.push(existing);
                continue;
            }
            if existing.lifecycle == UsageInvocationLifecycle::Unsettled {
                return Err(DbError::IdempotencyConflict);
            }
            let invocation = self
                .settle_usage_invocation_in_tx(
                    &mut transaction,
                    SettleUsageInvocation {
                        id: settlement.invocation_id.clone(),
                        expected_version: settlement.expected_version,
                        telemetry_state: settlement.telemetry_state,
                        terminal_reason: settlement.terminal_reason,
                        settled_at: settlement.settled_at,
                        updated_at: settlement.updated_at,
                    },
                )
                .await?;
            for event in settlement.events {
                self.append_usage_event_in_tx(&mut transaction, event)
                    .await?;
            }
            settled.push(invocation);
        }
        transaction.commit().await?;
        Ok(settled)
    }

    async fn get_usage_event(&self, id: &str) -> Result<Option<UsageEvent>> {
        sqlx::query("SELECT * FROM usage_event WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .map(map_usage_event)
            .transpose()
    }

    async fn get_usage_event_by_idempotency(
        &self,
        event_idempotency_key: &str,
    ) -> Result<Option<UsageEvent>> {
        sqlx::query("SELECT * FROM usage_event WHERE event_idempotency_key = ?")
            .bind(event_idempotency_key)
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .map(map_usage_event)
            .transpose()
    }

    async fn list_usage_events_for_invocation(
        &self,
        invocation_id: &str,
    ) -> Result<Vec<UsageEvent>> {
        sqlx::query(
            "SELECT * FROM usage_event
             WHERE invocation_id = ? ORDER BY occurred_at ASC, id ASC",
        )
        .bind(invocation_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_usage_event)
        .collect()
    }

    async fn list_usage_events_for_invocation_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        invocation_id: &str,
    ) -> Result<Vec<UsageEvent>> {
        sqlx::query(
            "SELECT * FROM usage_event
             WHERE invocation_id = ? ORDER BY occurred_at ASC, id ASC",
        )
        .bind(invocation_id)
        .fetch_all(&mut **transaction)
        .await?
        .into_iter()
        .map(map_usage_event)
        .collect()
    }

    async fn list_usage_events_for_project(
        &self,
        project_id: &str,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<Vec<UsageEvent>> {
        list_events_with_scope(&self.pool, Some(project_id), None, from, to).await
    }

    async fn list_usage_events_for_owner(
        &self,
        owner_user_id: &str,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<Vec<UsageEvent>> {
        list_events_with_scope(&self.pool, None, Some(owner_user_id), from, to).await
    }
}
