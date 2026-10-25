//! Pricing catalog snapshots, activation, and rate revisions.

use super::*;

#[async_trait]
impl PricingCatalogRepo for SqliteDb {
    async fn create_pricing_catalog_snapshot(
        &self,
        input: CreatePricingCatalogSnapshot,
    ) -> Result<PricingCatalogSnapshot> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let snapshot = self
            .create_pricing_catalog_snapshot_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(snapshot)
    }

    async fn create_pricing_catalog_snapshot_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreatePricingCatalogSnapshot,
    ) -> Result<PricingCatalogSnapshot> {
        let result = sqlx::query(
            "INSERT INTO pricing_catalog_snapshot (
                id, source_kind, source_url, http_etag, payload_sha256,
                parser_revision, revision_digest, payload_json, fetched_at, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(input.source_kind.to_string())
        .bind(&input.source_url)
        .bind(input.http_etag.as_deref())
        .bind(&input.payload_sha256)
        .bind(&input.parser_revision)
        .bind(&input.revision_digest)
        .bind(&input.payload_json)
        .bind(&input.fetched_at)
        .bind(&input.created_at)
        .execute(&mut **transaction)
        .await;
        if let Err(error) = result {
            if let Some(row) =
                sqlx::query("SELECT * FROM pricing_catalog_snapshot WHERE revision_digest = ?")
                    .bind(&input.revision_digest)
                    .fetch_optional(&mut **transaction)
                    .await?
            {
                let existing = map_catalog_snapshot(row)?;
                if catalog_snapshot_matches_input(&existing, &input) {
                    return Ok(existing);
                }
                return Err(DbError::IdempotencyConflict);
            }
            return Err(duplicate_error(error));
        }
        sqlx::query("SELECT * FROM pricing_catalog_snapshot WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut **transaction)
            .await
            .map_err(check_error)
            .and_then(map_catalog_snapshot)
    }

    async fn get_pricing_catalog_snapshot(
        &self,
        id: &str,
    ) -> Result<Option<PricingCatalogSnapshot>> {
        sqlx::query("SELECT * FROM pricing_catalog_snapshot WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .map(map_catalog_snapshot)
            .transpose()
    }

    async fn get_pricing_catalog_snapshot_by_revision(
        &self,
        revision_digest: &str,
    ) -> Result<Option<PricingCatalogSnapshot>> {
        sqlx::query("SELECT * FROM pricing_catalog_snapshot WHERE revision_digest = ?")
            .bind(revision_digest)
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .map(map_catalog_snapshot)
            .transpose()
    }

    async fn list_pricing_catalog_models(
        &self,
        query: PricingCatalogModelQuery,
    ) -> Result<Page<PricingCatalogModelRate>> {
        let offset = decode_offset(&query.page.cursor)?;
        let mut builder = sqlx::QueryBuilder::<Sqlite>::new(
            "SELECT r.id AS rate_revision_id, r.catalog_snapshot_id AS snapshot_id,
                    r.catalog_provider_id AS provider_id,
                    r.catalog_model_id AS model_id,
                    r.input_nano_usd_per_million,
                    r.output_nano_usd_per_million,
                    r.cache_read_nano_usd_per_million,
                    r.cache_write_nano_usd_per_million,
                    r.tiers_json, r.source_last_updated, r.source_kind,
                    r.rate_digest, r.effective_at
             FROM pricing_rate_revision r
             WHERE r.source_kind = 'models_dev_catalog'",
        );
        if let Some(snapshot_id) = &query.snapshot_id {
            builder
                .push(" AND r.catalog_snapshot_id = ")
                .push_bind(snapshot_id);
        }
        if let Some(provider_id) = &query.provider_id {
            builder
                .push(" AND r.catalog_provider_id = ")
                .push_bind(provider_id);
        }
        if let Some(search) = &query.query {
            builder
                .push(" AND (r.catalog_provider_id LIKE ")
                .push_bind(format!("%{search}%"))
                .push(" OR r.catalog_model_id LIKE ")
                .push_bind(format!("%{search}%"))
                .push(')');
        }
        builder
            .push(" ORDER BY r.catalog_provider_id ASC, r.catalog_model_id ASC, r.id ASC LIMIT ");
        builder.push_bind(limit(&query.page) + 1);
        builder.push(" OFFSET ").push_bind(offset);
        let rows = builder.build().fetch_all(&self.pool).await?;
        let items = rows
            .into_iter()
            .map(map_catalog_model_rate)
            .collect::<Result<Vec<_>>>()?;

        let mut count_builder = sqlx::QueryBuilder::<Sqlite>::new(
            "SELECT COUNT(*) FROM pricing_rate_revision r
             WHERE r.source_kind = 'models_dev_catalog'",
        );
        if let Some(snapshot_id) = &query.snapshot_id {
            count_builder
                .push(" AND r.catalog_snapshot_id = ")
                .push_bind(snapshot_id);
        }
        if let Some(provider_id) = &query.provider_id {
            count_builder
                .push(" AND r.catalog_provider_id = ")
                .push_bind(provider_id);
        }
        if let Some(search) = &query.query {
            count_builder
                .push(" AND (r.catalog_provider_id LIKE ")
                .push_bind(format!("%{search}%"))
                .push(" OR r.catalog_model_id LIKE ")
                .push_bind(format!("%{search}%"))
                .push(')');
        }
        let total = if query.page.include_total {
            Some(
                count_builder
                    .build_query_scalar::<i64>()
                    .fetch_one(&self.pool)
                    .await?,
            )
        } else {
            None
        };
        page_from_items(items, &query.page, offset, total)
    }

    async fn get_pricing_catalog_state(&self) -> Result<Option<PricingCatalogState>> {
        sqlx::query("SELECT * FROM pricing_catalog_state WHERE id = 'models_dev_catalog'")
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .map(map_catalog_state)
            .transpose()
    }

    async fn ensure_pricing_catalog_state(&self, now: &str) -> Result<PricingCatalogState> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        sqlx::query(
            "INSERT OR IGNORE INTO pricing_catalog_state
                (id, state, version, created_at, updated_at)
             VALUES ('models_dev_catalog', 'absent', 1, ?, ?)",
        )
        .bind(now)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(check_error)?;
        let row =
            sqlx::query("SELECT * FROM pricing_catalog_state WHERE id = 'models_dev_catalog'")
                .fetch_one(&mut *transaction)
                .await
                .map_err(check_error)?;
        let state = map_catalog_state(row)?;
        transaction.commit().await?;
        Ok(state)
    }

    async fn update_pricing_catalog_state(
        &self,
        input: UpdatePricingCatalogState,
    ) -> Result<PricingCatalogState> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let state = self
            .update_catalog_state_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(state)
    }

    async fn record_pricing_catalog_check(
        &self,
        input: RecordPricingCatalogCheck,
    ) -> Result<PricingCatalogState> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let state = self
            .record_catalog_check_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(state)
    }

    async fn activate_pricing_catalog(
        &self,
        input: ActivatePricingCatalog,
    ) -> Result<PricingCatalogState> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let state = self
            .activate_pricing_catalog_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(state)
    }

    async fn activate_pricing_catalog_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: ActivatePricingCatalog,
    ) -> Result<PricingCatalogState> {
        let snapshot_exists: Option<i64> =
            sqlx::query_scalar("SELECT 1 FROM pricing_catalog_snapshot WHERE id = ?")
                .bind(&input.snapshot_id)
                .fetch_optional(&mut **transaction)
                .await?;
        if snapshot_exists.is_none() {
            return Err(DbError::NotFound);
        }
        sqlx::query(
            "INSERT OR IGNORE INTO pricing_catalog_state
                (id, state, version, created_at, updated_at)
             VALUES ('models_dev_catalog', 'absent', 1, ?, ?)",
        )
        .bind(&input.checked_at)
        .bind(&input.updated_at)
        .execute(&mut **transaction)
        .await
        .map_err(check_error)?;
        let current = self
            .catalog_state_in_tx(transaction)
            .await?
            .ok_or(DbError::NotFound)?;
        if input
            .idempotency_key
            .as_deref()
            .is_some_and(|key| current.last_idempotency_key.as_deref() == Some(key))
        {
            return Ok(current);
        }
        let result = sqlx::query(
            "UPDATE pricing_catalog_state
             SET active_snapshot_id = ?, state = 'fresh', http_etag = ?,
                 last_checked_at = ?, last_successful_check_at = ?, stale_after = ?,
                 last_error_code = NULL, last_idempotency_key = ?,
                 version = version + 1, updated_at = ?
             WHERE id = 'models_dev_catalog' AND version = ?",
        )
        .bind(&input.snapshot_id)
        .bind(input.http_etag.as_deref())
        .bind(&input.checked_at)
        .bind(&input.checked_at)
        .bind(input.stale_after.as_deref())
        .bind(input.idempotency_key.as_deref())
        .bind(&input.updated_at)
        .bind(input.expected_version)
        .execute(&mut **transaction)
        .await;
        let result = match result {
            Ok(result) => result,
            Err(error) => return Err(check_error(error)),
        };
        if result.rows_affected() == 0 {
            return conflict_or_not_found(
                "models_dev_catalog",
                input.expected_version,
                fetch_version(transaction, "pricing_catalog_state", "models_dev_catalog").await?,
            );
        }
        self.catalog_state_in_tx(transaction)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn create_pricing_rate_revision(
        &self,
        input: CreatePricingRateRevision,
    ) -> Result<PricingRateRevision> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let revision = self
            .create_pricing_rate_revision_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(revision)
    }

    async fn create_pricing_rate_revision_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreatePricingRateRevision,
    ) -> Result<PricingRateRevision> {
        let result = sqlx::query(
            "INSERT INTO pricing_rate_revision (
                id, source_kind, owner_user_id, catalog_snapshot_id,
                catalog_provider_id, catalog_model_id,
                pricing_subject_revision_id, pricing_subject_revision_digest,
                runtime_model, source_model_key, source_last_updated, currency,
                input_nano_usd_per_million, output_nano_usd_per_million,
                cache_read_nano_usd_per_million, cache_write_nano_usd_per_million,
                tiers_json, legacy_context_over_200k_json, context_tier_state,
                received_rates_json, rate_digest, effective_at, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(input.source_kind.to_string())
        .bind(input.owner_user_id.as_deref())
        .bind(input.catalog_snapshot_id.as_deref())
        .bind(input.catalog_provider_id.as_deref())
        .bind(input.catalog_model_id.as_deref())
        .bind(input.pricing_subject_revision_id.as_deref())
        .bind(input.pricing_subject_revision_digest.as_deref())
        .bind(input.runtime_model.as_deref())
        .bind(input.source_model_key.as_deref())
        .bind(input.source_last_updated.as_deref())
        .bind(&input.currency)
        .bind(input.rates.input)
        .bind(input.rates.output)
        .bind(input.rates.cache_read)
        .bind(input.rates.cache_write)
        .bind(&input.tiers_json)
        .bind(input.legacy_context_over_200k_json.as_deref())
        .bind(&input.context_tier_state)
        .bind(&input.received_rates_json)
        .bind(&input.rate_digest)
        .bind(&input.effective_at)
        .bind(&input.created_at)
        .execute(&mut **transaction)
        .await;
        if let Err(error) = result {
            if let Some(row) =
                sqlx::query("SELECT * FROM pricing_rate_revision WHERE rate_digest = ?")
                    .bind(&input.rate_digest)
                    .fetch_optional(&mut **transaction)
                    .await?
            {
                let existing = map_rate_revision(row)?;
                if rate_revision_matches_input(&existing, &input) {
                    return Ok(existing);
                }
                return Err(DbError::IdempotencyConflict);
            }
            return Err(duplicate_error(error));
        }
        sqlx::query("SELECT * FROM pricing_rate_revision WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut **transaction)
            .await
            .map_err(check_error)
            .and_then(map_rate_revision)
    }

    async fn get_pricing_rate_revision(&self, id: &str) -> Result<Option<PricingRateRevision>> {
        sqlx::query("SELECT * FROM pricing_rate_revision WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .map(map_rate_revision)
            .transpose()
    }

    async fn get_pricing_rate_revision_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        id: &str,
    ) -> Result<Option<PricingRateRevision>> {
        sqlx::query("SELECT * FROM pricing_rate_revision WHERE id = ?")
            .bind(id)
            .fetch_optional(&mut **transaction)
            .await
            .map_err(check_error)?
            .map(map_rate_revision)
            .transpose()
    }

    async fn get_pricing_rate_revision_by_digest(
        &self,
        rate_digest: &str,
    ) -> Result<Option<PricingRateRevision>> {
        sqlx::query("SELECT * FROM pricing_rate_revision WHERE rate_digest = ?")
            .bind(rate_digest)
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .map(map_rate_revision)
            .transpose()
    }

    async fn get_catalog_rate_revision(
        &self,
        snapshot_id: &str,
        provider_id: &str,
        model_id: &str,
    ) -> Result<Option<PricingRateRevision>> {
        sqlx::query(
            "SELECT * FROM pricing_rate_revision
             WHERE source_kind = 'models_dev_catalog'
               AND catalog_snapshot_id = ? AND catalog_provider_id = ?
               AND catalog_model_id = ?",
        )
        .bind(snapshot_id)
        .bind(provider_id)
        .bind(model_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(check_error)?
        .map(map_rate_revision)
        .transpose()
    }

    async fn list_pricing_rate_revisions_for_snapshot(
        &self,
        snapshot_id: &str,
    ) -> Result<Vec<PricingRateRevision>> {
        sqlx::query(
            "SELECT * FROM pricing_rate_revision
             WHERE catalog_snapshot_id = ? ORDER BY catalog_provider_id ASC,
                catalog_model_id ASC, id ASC",
        )
        .bind(snapshot_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_rate_revision)
        .collect()
    }

    async fn list_pricing_rate_revisions_for_subject_model(
        &self,
        subject_revision_id: &str,
        runtime_model: &str,
    ) -> Result<Vec<PricingRateRevision>> {
        sqlx::query(
            "SELECT * FROM pricing_rate_revision
             WHERE source_kind = 'manual_override'
               AND pricing_subject_revision_id = ? AND runtime_model = ?
             ORDER BY effective_at DESC, id DESC",
        )
        .bind(subject_revision_id)
        .bind(runtime_model)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_rate_revision)
        .collect()
    }
}

impl SqliteDb {
    async fn catalog_state_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
    ) -> Result<Option<PricingCatalogState>> {
        sqlx::query("SELECT * FROM pricing_catalog_state WHERE id = 'models_dev_catalog'")
            .fetch_optional(&mut **transaction)
            .await
            .map_err(check_error)?
            .map(map_catalog_state)
            .transpose()
    }

    async fn update_catalog_state_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: UpdatePricingCatalogState,
    ) -> Result<PricingCatalogState> {
        let current = self
            .catalog_state_in_tx(transaction)
            .await?
            .ok_or(DbError::NotFound)?;
        let result = sqlx::query(
            "UPDATE pricing_catalog_state
             SET active_snapshot_id = ?, state = ?, http_etag = ?,
                 last_checked_at = ?, last_successful_check_at = ?, stale_after = ?,
                 last_error_code = ?, last_idempotency_key = ?,
                 version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(
            input
                .active_snapshot_id
                .unwrap_or(current.active_snapshot_id),
        )
        .bind(input.state.to_string())
        .bind(input.http_etag.unwrap_or(current.http_etag))
        .bind(input.last_checked_at.unwrap_or(current.last_checked_at))
        .bind(
            input
                .last_successful_check_at
                .unwrap_or(current.last_successful_check_at),
        )
        .bind(input.stale_after.unwrap_or(current.stale_after))
        .bind(input.last_error_code.unwrap_or(current.last_error_code))
        .bind(
            input
                .last_idempotency_key
                .unwrap_or(current.last_idempotency_key),
        )
        .bind(&input.updated_at)
        .bind(&input.id)
        .bind(input.expected_version)
        .execute(&mut **transaction)
        .await
        .map_err(duplicate_error)?;
        if result.rows_affected() == 0 {
            return conflict_or_not_found(
                &input.id,
                input.expected_version,
                fetch_version(transaction, "pricing_catalog_state", &input.id).await?,
            );
        }
        self.catalog_state_in_tx(transaction)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn record_catalog_check_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: RecordPricingCatalogCheck,
    ) -> Result<PricingCatalogState> {
        let current = self
            .catalog_state_in_tx(transaction)
            .await?
            .ok_or(DbError::NotFound)?;
        if input
            .idempotency_key
            .as_deref()
            .is_some_and(|key| current.last_idempotency_key.as_deref() == Some(key))
        {
            return Ok(current);
        }
        let result = sqlx::query(
            "UPDATE pricing_catalog_state
             SET active_snapshot_id = ?, state = ?, http_etag = ?,
                 last_checked_at = ?, last_successful_check_at = ?, stale_after = ?,
                 last_error_code = ?, last_idempotency_key = ?,
                 version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(
            input
                .active_snapshot_id
                .unwrap_or(current.active_snapshot_id),
        )
        .bind(input.state.to_string())
        .bind(input.http_etag.unwrap_or(current.http_etag))
        .bind(&input.checked_at)
        .bind(
            input
                .successful_check_at
                .unwrap_or(current.last_successful_check_at),
        )
        .bind(input.stale_after.unwrap_or(current.stale_after))
        .bind(input.last_error_code.unwrap_or(current.last_error_code))
        .bind(input.idempotency_key)
        .bind(&input.updated_at)
        .bind(&input.id)
        .bind(input.expected_version)
        .execute(&mut **transaction)
        .await
        .map_err(duplicate_error)?;
        if result.rows_affected() == 0 {
            return conflict_or_not_found(
                &input.id,
                input.expected_version,
                fetch_version(transaction, "pricing_catalog_state", &input.id).await?,
            );
        }
        self.catalog_state_in_tx(transaction)
            .await?
            .ok_or(DbError::NotFound)
    }
}
