//! Retrospective preview, run, and estimate revision persistence.

use super::*;

#[async_trait]
impl RetrospectiveEstimateRepo for SqliteDb {
    async fn create_cost_estimation_preview(
        &self,
        input: CreateCostEstimationPreview,
    ) -> Result<CostEstimationPreview> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let preview = self
            .create_cost_estimation_preview_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(preview)
    }

    async fn create_cost_estimation_preview_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreateCostEstimationPreview,
    ) -> Result<CostEstimationPreview> {
        if let Some(row) =
            sqlx::query("SELECT * FROM cost_estimation_preview WHERE idempotency_key = ?")
                .bind(&input.idempotency_key)
                .fetch_optional(&mut **transaction)
                .await?
        {
            let existing = map_preview(row)?;
            if existing.id == input.id
                && existing.owner_user_id == input.owner_user_id
                && existing.project_id == input.project_id
                && existing.catalog_snapshot_id == input.catalog_snapshot_id
                && existing.catalog_freshness == input.catalog_freshness
                && existing.usage_set_digest == input.usage_set_digest
                && existing.window_from == input.window_from
                && existing.window_to == input.window_to
                && existing.filters_json == input.filters_json
                && existing.expires_at == input.expires_at
                && existing.created_at == input.created_at
            {
                return Ok(existing);
            }
            return Err(DbError::IdempotencyConflict);
        }
        sqlx::query(
            "INSERT INTO cost_estimation_preview (
                id, owner_user_id, project_id, catalog_snapshot_id,
                catalog_freshness, usage_set_digest, window_from, window_to, filters_json,
                eligible_event_count, unmatched_event_count,
                already_reported_event_count, projected_cost_summary_json,
                status, idempotency_key, version, expires_at, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, 1, ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.owner_user_id)
        .bind(&input.project_id)
        .bind(&input.catalog_snapshot_id)
        .bind(input.catalog_freshness.to_string())
        .bind(&input.usage_set_digest)
        .bind(input.window_from.as_deref())
        .bind(input.window_to.as_deref())
        .bind(&input.filters_json)
        .bind(input.eligible_event_count)
        .bind(input.unmatched_event_count)
        .bind(input.already_reported_event_count)
        .bind(&input.projected_cost_summary_json)
        .bind(&input.idempotency_key)
        .bind(&input.expires_at)
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&mut **transaction)
        .await
        .map_err(duplicate_error)?;
        sqlx::query("SELECT * FROM cost_estimation_preview WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut **transaction)
            .await
            .map_err(check_error)
            .and_then(map_preview)
    }

    async fn get_cost_estimation_preview(&self, id: &str) -> Result<Option<CostEstimationPreview>> {
        sqlx::query("SELECT * FROM cost_estimation_preview WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .map(map_preview)
            .transpose()
    }

    async fn get_cost_estimation_preview_by_idempotency(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<CostEstimationPreview>> {
        sqlx::query("SELECT * FROM cost_estimation_preview WHERE idempotency_key = ?")
            .bind(idempotency_key)
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .map(map_preview)
            .transpose()
    }

    async fn list_cost_estimation_previews(
        &self,
        project_id: &str,
    ) -> Result<Vec<CostEstimationPreview>> {
        sqlx::query(
            "SELECT * FROM cost_estimation_preview
             WHERE project_id = ? ORDER BY created_at DESC, id DESC",
        )
        .bind(project_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_preview)
        .collect()
    }

    async fn update_cost_estimation_preview(
        &self,
        input: UpdateCostEstimationPreview,
    ) -> Result<CostEstimationPreview> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let preview = self
            .update_cost_estimation_preview_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(preview)
    }

    async fn update_cost_estimation_preview_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: UpdateCostEstimationPreview,
    ) -> Result<CostEstimationPreview> {
        let current = sqlx::query("SELECT * FROM cost_estimation_preview WHERE id = ?")
            .bind(&input.id)
            .fetch_optional(&mut **transaction)
            .await
            .map_err(check_error)?
            .map(map_preview)
            .transpose()?
            .ok_or(DbError::NotFound)?;
        let result = sqlx::query(
            "UPDATE cost_estimation_preview
             SET status = ?, eligible_event_count = ?, unmatched_event_count = ?,
                 already_reported_event_count = ?, projected_cost_summary_json = ?,
                 version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(input.status.to_string())
        .bind(
            input
                .eligible_event_count
                .unwrap_or(current.eligible_event_count),
        )
        .bind(
            input
                .unmatched_event_count
                .unwrap_or(current.unmatched_event_count),
        )
        .bind(
            input
                .already_reported_event_count
                .unwrap_or(current.already_reported_event_count),
        )
        .bind(
            input
                .projected_cost_summary_json
                .unwrap_or(current.projected_cost_summary_json),
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
                fetch_version(transaction, "cost_estimation_preview", &input.id).await?,
            );
        }
        sqlx::query("SELECT * FROM cost_estimation_preview WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut **transaction)
            .await
            .map_err(check_error)
            .and_then(map_preview)
    }

    async fn create_cost_estimation_run(
        &self,
        input: CreateCostEstimationRun,
    ) -> Result<CostEstimationRun> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let run = self
            .create_cost_estimation_run_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(run)
    }

    async fn create_cost_estimation_run_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreateCostEstimationRun,
    ) -> Result<CostEstimationRun> {
        if let Some(row) =
            sqlx::query("SELECT * FROM cost_estimation_run WHERE idempotency_key = ?")
                .bind(&input.idempotency_key)
                .fetch_optional(&mut **transaction)
                .await?
        {
            let existing = map_run(row)?;
            if existing.id == input.id
                && existing.owner_user_id == input.owner_user_id
                && existing.project_id == input.project_id
                && existing.preview_id == input.preview_id
                && existing.catalog_snapshot_id == input.catalog_snapshot_id
                && existing.usage_set_digest == input.usage_set_digest
                && existing.supersedes_run_id == input.supersedes_run_id
                && existing.created_at == input.created_at
            {
                return Ok(existing);
            }
            return Err(DbError::IdempotencyConflict);
        }
        sqlx::query(
            "INSERT INTO cost_estimation_run (
                id, owner_user_id, project_id, preview_id, catalog_snapshot_id,
                usage_set_digest, status, applied_event_count,
                unmatched_event_count, already_reported_event_count,
                cost_summary_json, idempotency_key, supersedes_run_id, version,
                created_at, updated_at, completed_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, 0, ?, ?, ?, 1, ?, ?, NULL)",
        )
        .bind(&input.id)
        .bind(&input.owner_user_id)
        .bind(&input.project_id)
        .bind(&input.preview_id)
        .bind(&input.catalog_snapshot_id)
        .bind(&input.usage_set_digest)
        .bind(input.status.to_string())
        .bind(&input.cost_summary_json)
        .bind(&input.idempotency_key)
        .bind(input.supersedes_run_id.as_deref())
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&mut **transaction)
        .await
        .map_err(duplicate_error)?;
        sqlx::query("SELECT * FROM cost_estimation_run WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut **transaction)
            .await
            .map_err(check_error)
            .and_then(map_run)
    }

    async fn get_cost_estimation_run(&self, id: &str) -> Result<Option<CostEstimationRun>> {
        sqlx::query("SELECT * FROM cost_estimation_run WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .map(map_run)
            .transpose()
    }

    async fn get_cost_estimation_run_by_idempotency(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<CostEstimationRun>> {
        sqlx::query("SELECT * FROM cost_estimation_run WHERE idempotency_key = ?")
            .bind(idempotency_key)
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .map(map_run)
            .transpose()
    }

    async fn list_cost_estimation_runs(&self, project_id: &str) -> Result<Vec<CostEstimationRun>> {
        sqlx::query(
            "SELECT * FROM cost_estimation_run
             WHERE project_id = ? ORDER BY created_at DESC, id DESC",
        )
        .bind(project_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_run)
        .collect()
    }

    async fn update_cost_estimation_run(
        &self,
        input: UpdateCostEstimationRun,
    ) -> Result<CostEstimationRun> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let run = self
            .update_cost_estimation_run_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(run)
    }

    async fn update_cost_estimation_run_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: UpdateCostEstimationRun,
    ) -> Result<CostEstimationRun> {
        let current = sqlx::query("SELECT * FROM cost_estimation_run WHERE id = ?")
            .bind(&input.id)
            .fetch_optional(&mut **transaction)
            .await
            .map_err(check_error)?
            .map(map_run)
            .transpose()?
            .ok_or(DbError::NotFound)?;
        let result = sqlx::query(
            "UPDATE cost_estimation_run
             SET status = ?, applied_event_count = ?, unmatched_event_count = ?,
                 already_reported_event_count = ?, cost_summary_json = ?,
                 completed_at = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(input.status.to_string())
        .bind(
            input
                .applied_event_count
                .unwrap_or(current.applied_event_count),
        )
        .bind(
            input
                .unmatched_event_count
                .unwrap_or(current.unmatched_event_count),
        )
        .bind(
            input
                .already_reported_event_count
                .unwrap_or(current.already_reported_event_count),
        )
        .bind(input.cost_summary_json.unwrap_or(current.cost_summary_json))
        .bind(input.completed_at.unwrap_or(current.completed_at))
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
                fetch_version(transaction, "cost_estimation_run", &input.id).await?,
            );
        }
        sqlx::query("SELECT * FROM cost_estimation_run WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut **transaction)
            .await
            .map_err(check_error)
            .and_then(map_run)
    }

    async fn create_cost_estimate_revision(
        &self,
        input: CreateCostEstimateRevision,
    ) -> Result<CostEstimateRevision> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let revision = self
            .create_cost_estimate_revision_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(revision)
    }

    async fn create_cost_estimate_revision_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreateCostEstimateRevision,
    ) -> Result<CostEstimateRevision> {
        if let Some(row) = sqlx::query(
            "SELECT * FROM cost_estimate_revision
             WHERE run_id = ? AND usage_event_id = ?",
        )
        .bind(&input.run_id)
        .bind(&input.usage_event_id)
        .fetch_optional(&mut **transaction)
        .await?
        {
            let existing = map_estimate_revision(row)?;
            if existing.id == input.id
                && existing.owner_user_id == input.owner_user_id
                && existing.project_id == input.project_id
                && existing.usage_event_id == input.usage_event_id
                && existing.revision == input.revision
                && existing.supersedes_revision_id == input.supersedes_revision_id
                && existing.estimate_digest == input.estimate_digest
                && existing.state == input.state
                && existing.rate_revision_id == input.rate_revision_id
                && existing.catalog_snapshot_id == input.catalog_snapshot_id
                && existing.estimated_nano_usd == input.estimated_nano_usd
                && existing.formula_revision == input.formula_revision
                && existing.reason_code == input.reason_code
                && existing.created_at == input.created_at
            {
                return Ok(existing);
            }
            return Err(DbError::IdempotencyConflict);
        }
        if input.state == CostEstimateRevisionState::Applied {
            let provider_reported: Option<String> =
                sqlx::query_scalar("SELECT cost_kind FROM usage_event WHERE id = ?")
                    .bind(&input.usage_event_id)
                    .fetch_optional(&mut **transaction)
                    .await?;
            if provider_reported.as_deref() == Some("provider_reported") {
                return Err(DbError::Check(
                    "retrospective estimates cannot replace provider-reported cost".to_owned(),
                ));
            }
        }
        sqlx::query(
            "INSERT INTO cost_estimate_revision (
                id, run_id, owner_user_id, project_id, usage_event_id, revision,
                supersedes_revision_id, state, rate_revision_id,
                catalog_snapshot_id, estimated_nano_usd, formula_revision,
                retrospective, reason_code, estimate_digest, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.run_id)
        .bind(&input.owner_user_id)
        .bind(&input.project_id)
        .bind(&input.usage_event_id)
        .bind(input.revision)
        .bind(input.supersedes_revision_id.as_deref())
        .bind(input.state.to_string())
        .bind(input.rate_revision_id.as_deref())
        .bind(input.catalog_snapshot_id.as_deref())
        .bind(input.estimated_nano_usd)
        .bind(input.formula_revision.as_deref())
        .bind(input.reason_code.map(|value| value.to_string()))
        .bind(&input.estimate_digest)
        .bind(&input.created_at)
        .execute(&mut **transaction)
        .await
        .map_err(duplicate_error)?;
        sqlx::query("SELECT * FROM cost_estimate_revision WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut **transaction)
            .await
            .map_err(check_error)
            .and_then(map_estimate_revision)
    }

    async fn get_cost_estimate_revision(&self, id: &str) -> Result<Option<CostEstimateRevision>> {
        sqlx::query("SELECT * FROM cost_estimate_revision WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .map(map_estimate_revision)
            .transpose()
    }

    async fn get_cost_estimate_revision_by_digest(
        &self,
        estimate_digest: &str,
    ) -> Result<Option<CostEstimateRevision>> {
        sqlx::query("SELECT * FROM cost_estimate_revision WHERE estimate_digest = ?")
            .bind(estimate_digest)
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .map(map_estimate_revision)
            .transpose()
    }

    async fn list_cost_estimate_revisions_for_run(
        &self,
        run_id: &str,
    ) -> Result<Vec<CostEstimateRevision>> {
        sqlx::query(
            "SELECT * FROM cost_estimate_revision
             WHERE run_id = ? ORDER BY created_at ASC, id ASC",
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_estimate_revision)
        .collect()
    }

    async fn list_cost_estimate_revisions_for_invocation(
        &self,
        invocation_id: &str,
    ) -> Result<Vec<CostEstimateRevision>> {
        sqlx::query(
            "SELECT revision.* FROM cost_estimate_revision revision
             JOIN usage_event event ON event.id = revision.usage_event_id
             WHERE event.invocation_id = ?
             ORDER BY revision.usage_event_id, revision.revision DESC,
                      revision.created_at DESC, revision.id DESC",
        )
        .bind(invocation_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_estimate_revision)
        .collect()
    }
}
