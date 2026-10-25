//! SQLite catalog refresh and activation adapter.

use super::*;

#[async_trait]
impl PricingCatalogRepository for SqlitePricingRepository {
    async fn catalog_status(&self) -> Result<CatalogStatus, CatalogRepositoryError> {
        let Some(state) = db::PricingCatalogRepo::get_pricing_catalog_state(&*self.db)
            .await
            .map_err(catalog_error)?
        else {
            return Ok(CatalogStatus::absent());
        };
        status_for_state(&self.db, state).await
    }

    async fn replay_catalog_refresh(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<CatalogRefreshOutcome>, CatalogRepositoryError> {
        validate_identifier(idempotency_key, "catalog idempotency key")
            .map_err(catalog_conversion)?;
        let Some(row) = sqlx::query(
            "SELECT request_digest, result_json
             FROM pricing_operation_receipt
             WHERE operation_scope = ? AND idempotency_key = ?",
        )
        .bind(CATALOG_REFRESH_OPERATION_SCOPE)
        .bind(idempotency_key)
        .fetch_optional(self.db.pool())
        .await
        .map_err(catalog_sqlx)?
        else {
            return Ok(None);
        };
        let request_digest: String = row.try_get("request_digest").map_err(catalog_sqlx)?;
        if request_digest != catalog_refresh_request_digest() {
            return Err(catalog_conversion(
                "catalog refresh receipt has an incompatible request digest",
            ));
        }
        let result_json: String = row.try_get("result_json").map_err(catalog_sqlx)?;
        let status = catalog_status_from_result_json(&result_json).map_err(catalog_conversion)?;
        let value: Value = serde_json::from_str(&result_json).map_err(|error| {
            catalog_conversion(format!("invalid catalog receipt JSON: {error}"))
        })?;
        let outcome = value
            .get("outcome")
            .and_then(Value::as_str)
            .unwrap_or("already_refreshed");
        match outcome {
            "activated" => {
                let snapshot_id = value
                    .get("snapshot_id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| catalog_conversion("activated receipt omits snapshot_id"))?;
                let snapshot =
                    db::PricingCatalogRepo::get_pricing_catalog_snapshot(&*self.db, snapshot_id)
                        .await
                        .map_err(catalog_error)?
                        .ok_or_else(|| {
                            catalog_conversion(
                                "activated receipt points to a missing catalog snapshot",
                            )
                        })?;
                let snapshot = snapshot_from_db(snapshot)
                    .await
                    .map_err(catalog_conversion)?;
                Ok(Some(CatalogRefreshOutcome::Activated {
                    snapshot: Box::new(snapshot),
                    status,
                }))
            }
            "not_modified" => Ok(Some(CatalogRefreshOutcome::NotModified { status })),
            "failed" => {
                let error_code = value
                    .get("error_code")
                    .and_then(Value::as_str)
                    .ok_or_else(|| catalog_conversion("failed receipt omits error_code"))?;
                let code = catalog_refresh_error_code(error_code).map_err(catalog_conversion)?;
                Ok(Some(CatalogRefreshOutcome::Failed { status, code }))
            }
            "already_refreshed" => Ok(Some(CatalogRefreshOutcome::AlreadyRefreshed { status })),
            other => Err(catalog_conversion(format!(
                "unknown catalog refresh receipt outcome {other:?}"
            ))),
        }
    }

    async fn record_catalog_refresh_already_refreshed(
        &self,
        checked_at: SystemTime,
        idempotency_key: &str,
    ) -> Result<CatalogStatus, CatalogRepositoryError> {
        validate_identifier(idempotency_key, "catalog idempotency key")
            .map_err(catalog_conversion)?;
        let checked_at = db_time(checked_at);
        let request_digest = catalog_refresh_request_digest();
        let mut transaction = db::begin_immediate(self.db.pool())
            .await
            .map_err(catalog_sqlx)?;
        if let Some(result_json) = operation_receipt_in_tx(
            &mut transaction,
            CATALOG_REFRESH_OPERATION_SCOPE,
            idempotency_key,
            &request_digest,
        )
        .await
        .map_err(catalog_operation_error)?
        {
            let status =
                catalog_status_from_result_json(&result_json).map_err(catalog_conversion)?;
            transaction.commit().await.map_err(catalog_sqlx)?;
            return Ok(status);
        }
        let state = ensure_catalog_state_in_tx(&mut transaction, &checked_at)
            .await
            .map_err(catalog_error)?;
        let revision =
            active_revision_digest_in_tx(&mut transaction, state.active_snapshot_id.as_deref())
                .await
                .map_err(catalog_error)?;
        let status = catalog_status_from_db(state, revision)
            .map_err(catalog_conversion)?
            .at(system_time(&checked_at).map_err(catalog_conversion)?);
        let result_json = catalog_status_result_json(&status, "already_refreshed", None, None);
        insert_operation_receipt_in_tx(
            &mut transaction,
            CATALOG_REFRESH_OPERATION_SCOPE,
            idempotency_key,
            &request_digest,
            &result_json,
            &checked_at,
        )
        .await
        .map_err(catalog_conversion)?;
        transaction.commit().await.map_err(catalog_sqlx)?;
        Ok(status)
    }

    async fn active_catalog_snapshot(
        &self,
    ) -> Result<Option<CatalogSnapshot>, CatalogRepositoryError> {
        let Some(state) = db::PricingCatalogRepo::get_pricing_catalog_state(&*self.db)
            .await
            .map_err(catalog_error)?
        else {
            return Ok(None);
        };
        let Some(snapshot_id) = state.active_snapshot_id else {
            return Ok(None);
        };
        let snapshot =
            db::PricingCatalogRepo::get_pricing_catalog_snapshot(&*self.db, &snapshot_id)
                .await
                .map_err(catalog_error)?
                .ok_or_else(|| catalog_conversion("catalog state points to a missing snapshot"))?;
        snapshot_from_db(snapshot)
            .await
            .map(Some)
            .map_err(catalog_conversion)
    }

    async fn activate_catalog_snapshot(
        &self,
        snapshot: CatalogSnapshot,
        idempotency_key: &str,
    ) -> Result<CatalogStatus, CatalogRepositoryError> {
        if idempotency_key.trim().is_empty() || idempotency_key.len() > 256 {
            return Err(catalog_conversion(
                "catalog idempotency key is empty or too long",
            ));
        }
        if snapshot.models.is_empty() {
            return Err(catalog_conversion(
                "catalog snapshot has no normalized models",
            ));
        }
        let input = snapshot_input(&snapshot)?;
        let request_digest = catalog_refresh_request_digest();
        let checked_at = input.fetched_at.clone();
        let mut transaction = db::begin_immediate(self.db.pool())
            .await
            .map_err(catalog_sqlx)?;
        if let Some(result_json) = operation_receipt_in_tx(
            &mut transaction,
            CATALOG_REFRESH_OPERATION_SCOPE,
            idempotency_key,
            &request_digest,
        )
        .await
        .map_err(catalog_operation_error)?
        {
            let status =
                catalog_status_from_result_json(&result_json).map_err(catalog_conversion)?;
            transaction.commit().await.map_err(catalog_sqlx)?;
            return Ok(status);
        }
        let current = ensure_catalog_state_in_tx(&mut transaction, &checked_at)
            .await
            .map_err(catalog_error)?;

        // `revision_digest` is the immutable source identity.  SQLite keeps
        // it unique, so a retry or a parser-normalized replay may arrive with
        // a fresh application snapshot ID.  Reuse the retained row's ID and
        // rate foreign keys instead of attempting to create a second logical
        // snapshot (or leaving an orphan row when activation later fails).
        let existing_snapshot = sqlx::query(
            "SELECT id, source_url, payload_sha256, parser_revision,
                    revision_digest, payload_json
             FROM pricing_catalog_snapshot WHERE revision_digest = ?",
        )
        .bind(&snapshot.revision_digest)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(catalog_sqlx)?;
        let has_existing_snapshot = existing_snapshot.is_some();
        let activation_snapshot_id = if let Some(row) = existing_snapshot {
            let existing_source_url: String = row.try_get("source_url").map_err(catalog_sqlx)?;
            let existing_payload_sha256: String =
                row.try_get("payload_sha256").map_err(catalog_sqlx)?;
            let existing_parser_revision: String =
                row.try_get("parser_revision").map_err(catalog_sqlx)?;
            let existing_revision_digest: String =
                row.try_get("revision_digest").map_err(catalog_sqlx)?;
            let existing_payload_json: String =
                row.try_get("payload_json").map_err(catalog_sqlx)?;
            if existing_source_url != snapshot.source_url
                || existing_payload_sha256 != snapshot.payload_sha256
                || existing_parser_revision != snapshot.parser_revision
                || existing_revision_digest != snapshot.revision_digest
                || existing_payload_json.as_bytes() != snapshot.raw_payload.as_slice()
            {
                return Err(catalog_conversion(
                    "catalog revision digest collides with different immutable payload metadata",
                ));
            }
            row.try_get::<String, _>("id").map_err(catalog_sqlx)?
        } else {
            snapshot.id.clone()
        };
        let mut persisted_snapshot = snapshot.clone();
        persisted_snapshot.id = activation_snapshot_id.clone();

        if !has_existing_snapshot {
            db::PricingCatalogRepo::create_pricing_catalog_snapshot_in_tx(
                &*self.db,
                &mut transaction,
                input,
            )
            .await
            .map_err(catalog_error)?;
        }
        for model in &persisted_snapshot.models {
            revision_for_snapshot_in_tx(&self.db, &mut transaction, &persisted_snapshot, model)
                .await
                .map_err(catalog_error)?;
        }
        let state = db::PricingCatalogRepo::activate_pricing_catalog_in_tx(
            &*self.db,
            &mut transaction,
            db::ActivatePricingCatalog {
                snapshot_id: activation_snapshot_id,
                expected_version: current.version,
                http_etag: snapshot.etag.clone(),
                checked_at: checked_at.clone(),
                stale_after: Some(
                    DateTime::parse_from_rfc3339(&checked_at)
                        .map_err(|error| catalog_conversion(error.to_string()))?
                        .with_timezone(&Utc)
                        .checked_add_signed(
                            chrono::Duration::from_std(pricing::MODELS_DEV_STALE_AFTER)
                                .map_err(|error| catalog_conversion(error.to_string()))?,
                        )
                        .ok_or_else(|| catalog_conversion("catalog stale boundary overflow"))?
                        .to_rfc3339_opts(SecondsFormat::Nanos, true),
                ),
                idempotency_key: Some(idempotency_key.to_owned()),
                updated_at: checked_at.clone(),
            },
        )
        .await
        .map_err(catalog_error)?;
        let status = catalog_status_from_db(state, Some(snapshot.revision_digest))
            .map_err(catalog_conversion)?;
        let result_json = catalog_status_result_json(
            &status,
            "activated",
            status.active_snapshot_id.as_deref(),
            None,
        );
        insert_operation_receipt_in_tx(
            &mut transaction,
            CATALOG_REFRESH_OPERATION_SCOPE,
            idempotency_key,
            &request_digest,
            &result_json,
            &checked_at,
        )
        .await
        .map_err(catalog_conversion)?;
        transaction.commit().await.map_err(catalog_sqlx)?;
        Ok(status)
    }

    async fn record_catalog_not_modified(
        &self,
        checked_at: SystemTime,
        etag: Option<String>,
        idempotency_key: &str,
    ) -> Result<CatalogStatus, CatalogRepositoryError> {
        if idempotency_key.trim().is_empty() || idempotency_key.len() > 256 {
            return Err(catalog_conversion(
                "catalog idempotency key is empty or too long",
            ));
        }
        validate_optional_text(etag.as_deref(), "catalog ETag").map_err(catalog_conversion)?;
        let checked_at = db_time(checked_at);
        let request_digest = catalog_refresh_request_digest();
        let mut transaction = db::begin_immediate(self.db.pool())
            .await
            .map_err(catalog_sqlx)?;
        if let Some(result_json) = operation_receipt_in_tx(
            &mut transaction,
            CATALOG_REFRESH_OPERATION_SCOPE,
            idempotency_key,
            &request_digest,
        )
        .await
        .map_err(catalog_operation_error)?
        {
            let status =
                catalog_status_from_result_json(&result_json).map_err(catalog_conversion)?;
            transaction.commit().await.map_err(catalog_sqlx)?;
            return Ok(status);
        }
        let current = ensure_catalog_state_in_tx(&mut transaction, &checked_at)
            .await
            .map_err(catalog_error)?;
        if current.active_snapshot_id.is_none() {
            return Err(catalog_conversion(
                "cannot record a 304 response without a retained catalog snapshot",
            ));
        }
        let stale_after = DateTime::parse_from_rfc3339(&checked_at)
            .map_err(|error| catalog_conversion(error.to_string()))?
            .with_timezone(&Utc)
            .checked_add_signed(
                chrono::Duration::from_std(pricing::MODELS_DEV_STALE_AFTER)
                    .map_err(|error| catalog_conversion(error.to_string()))?,
            )
            .ok_or_else(|| catalog_conversion("catalog stale boundary overflow"))?
            .to_rfc3339_opts(SecondsFormat::Nanos, true);
        let state = update_catalog_check_in_tx(
            &mut transaction,
            CatalogCheckUpdate {
                current: &current,
                state: db::PricingCatalogStateKind::Fresh,
                http_etag: etag.as_deref().or(current.http_etag.as_deref()),
                checked_at: &checked_at,
                successful_check_at: Some(&checked_at),
                stale_after: Some(&stale_after),
                last_error_code: None,
                idempotency_key,
                updated_at: &checked_at,
            },
        )
        .await?;
        let revision =
            active_revision_digest_in_tx(&mut transaction, state.active_snapshot_id.as_deref())
                .await
                .map_err(catalog_error)?;
        let status = catalog_status_from_db(state, revision).map_err(catalog_conversion)?;
        let result_json = catalog_status_result_json(&status, "not_modified", None, None);
        insert_operation_receipt_in_tx(
            &mut transaction,
            CATALOG_REFRESH_OPERATION_SCOPE,
            idempotency_key,
            &request_digest,
            &result_json,
            &checked_at,
        )
        .await
        .map_err(catalog_conversion)?;
        transaction.commit().await.map_err(catalog_sqlx)?;
        Ok(status)
    }

    async fn record_catalog_refresh_failure(
        &self,
        checked_at: SystemTime,
        code: CatalogRefreshErrorCode,
        idempotency_key: &str,
    ) -> Result<CatalogStatus, CatalogRepositoryError> {
        if idempotency_key.trim().is_empty() || idempotency_key.len() > 256 {
            return Err(catalog_conversion(
                "catalog idempotency key is empty or too long",
            ));
        }
        let checked_at = db_time(checked_at);
        let request_digest = catalog_refresh_request_digest();
        let mut transaction = db::begin_immediate(self.db.pool())
            .await
            .map_err(catalog_sqlx)?;
        if let Some(result_json) = operation_receipt_in_tx(
            &mut transaction,
            CATALOG_REFRESH_OPERATION_SCOPE,
            idempotency_key,
            &request_digest,
        )
        .await
        .map_err(catalog_operation_error)?
        {
            let status =
                catalog_status_from_result_json(&result_json).map_err(catalog_conversion)?;
            transaction.commit().await.map_err(catalog_sqlx)?;
            return Ok(status);
        }
        let current = ensure_catalog_state_in_tx(&mut transaction, &checked_at)
            .await
            .map_err(catalog_error)?;
        let state = update_catalog_check_in_tx(
            &mut transaction,
            CatalogCheckUpdate {
                current: &current,
                state: db::PricingCatalogStateKind::RefreshFailed,
                http_etag: current.http_etag.as_deref(),
                checked_at: &checked_at,
                successful_check_at: current.last_successful_check_at.as_deref(),
                stale_after: current.stale_after.as_deref(),
                last_error_code: Some(code.as_str()),
                idempotency_key,
                updated_at: &checked_at,
            },
        )
        .await?;
        let revision =
            active_revision_digest_in_tx(&mut transaction, state.active_snapshot_id.as_deref())
                .await
                .map_err(catalog_error)?;
        let status = catalog_status_from_db(state, revision).map_err(catalog_conversion)?;
        let result_json = catalog_status_result_json(&status, "failed", None, Some(code.as_str()));
        insert_operation_receipt_in_tx(
            &mut transaction,
            CATALOG_REFRESH_OPERATION_SCOPE,
            idempotency_key,
            &request_digest,
            &result_json,
            &checked_at,
        )
        .await
        .map_err(catalog_conversion)?;
        transaction.commit().await.map_err(catalog_sqlx)?;
        Ok(status)
    }
}
