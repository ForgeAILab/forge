//! Pricing subjects, revisions, bindings, and adjustments.

use super::*;

#[async_trait]
impl PricingSubjectRepo for SqliteDb {
    async fn create_pricing_subject(&self, input: CreatePricingSubject) -> Result<PricingSubject> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let subject = self
            .create_pricing_subject_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(subject)
    }

    async fn create_pricing_subject_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreatePricingSubject,
    ) -> Result<PricingSubject> {
        // A revision belongs to an already-created subject.  Insert the
        // subject without its optional pointer, then validate/set the pointer
        // through the same CAS path when a caller supplied one.
        sqlx::query(
            "INSERT INTO pricing_subject (
                id, owner_user_id, subject_kind, provider_entry_id, daemon_id,
                executor_type, current_revision_id, state, last_idempotency_key,
                last_update_digest, version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, 1, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.owner_user_id)
        .bind(input.subject_kind.to_string())
        .bind(input.provider_entry_id.as_deref())
        .bind(input.daemon_id.as_deref())
        .bind(input.executor_type.as_deref())
        .bind(input.state.to_string())
        .bind(input.last_idempotency_key.as_deref())
        .bind(input.last_update_digest.as_deref())
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&mut **transaction)
        .await
        .map_err(duplicate_error)?;

        if let Some(revision_id) = input.current_revision_id {
            sqlx::query(
                "UPDATE pricing_subject SET current_revision_id = ?, version = version + 1,
                    updated_at = ? WHERE id = ? AND version = 1",
            )
            .bind(revision_id)
            .bind(&input.updated_at)
            .bind(&input.id)
            .execute(&mut **transaction)
            .await
            .map_err(check_error)?;
        }
        sqlx::query("SELECT * FROM pricing_subject WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut **transaction)
            .await
            .map_err(check_error)
            .and_then(map_subject)
    }

    async fn get_pricing_subject(&self, id: &str) -> Result<Option<PricingSubject>> {
        sqlx::query("SELECT * FROM pricing_subject WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .map(map_subject)
            .transpose()
    }

    async fn get_pricing_subject_for_provider(
        &self,
        owner_user_id: &str,
        provider_entry_id: &str,
    ) -> Result<Option<PricingSubject>> {
        sqlx::query(
            "SELECT * FROM pricing_subject
             WHERE owner_user_id = ? AND subject_kind = 'provider_entry'
               AND provider_entry_id = ?",
        )
        .bind(owner_user_id)
        .bind(provider_entry_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(check_error)?
        .map(map_subject)
        .transpose()
    }

    async fn get_pricing_subject_for_cli_runtime(
        &self,
        owner_user_id: &str,
        daemon_id: &str,
        executor_type: &str,
    ) -> Result<Option<PricingSubject>> {
        sqlx::query(
            "SELECT * FROM pricing_subject
             WHERE owner_user_id = ? AND subject_kind = 'cli_runtime'
               AND daemon_id = ? AND executor_type = ?",
        )
        .bind(owner_user_id)
        .bind(daemon_id)
        .bind(executor_type)
        .fetch_optional(&self.pool)
        .await
        .map_err(check_error)?
        .map(map_subject)
        .transpose()
    }

    #[allow(clippy::too_many_arguments)]
    async fn resolve_active_pricing_subject_binding_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        owner_user_id: &str,
        provider_entry_id: Option<&str>,
        daemon_id: Option<&str>,
        executor_type: Option<&str>,
        scope_key: &str,
        runtime_model: &str,
    ) -> Result<Option<ResolvedPricingSubjectBinding>> {
        let subject_row = if let Some(provider_entry_id) = provider_entry_id {
            sqlx::query(
                "SELECT * FROM pricing_subject
                 WHERE owner_user_id = ? AND subject_kind = 'provider_entry'
                   AND provider_entry_id = ? AND state = 'active'",
            )
            .bind(owner_user_id)
            .bind(provider_entry_id)
            .fetch_optional(&mut **transaction)
            .await?
        } else if let (Some(daemon_id), Some(executor_type)) = (daemon_id, executor_type) {
            sqlx::query(
                "SELECT * FROM pricing_subject
                 WHERE owner_user_id = ? AND subject_kind = 'cli_runtime'
                   AND daemon_id = ? AND executor_type = ? AND state = 'active'",
            )
            .bind(owner_user_id)
            .bind(daemon_id)
            .bind(executor_type)
            .fetch_optional(&mut **transaction)
            .await?
        } else {
            None
        };
        let Some(subject) = subject_row.map(map_subject).transpose()? else {
            return Ok(None);
        };
        let Some(revision_id) = subject.current_revision_id.as_deref() else {
            return Ok(None);
        };
        let Some(revision) = sqlx::query(
            "SELECT * FROM pricing_subject_revision
             WHERE id = ? AND subject_id = ?",
        )
        .bind(revision_id)
        .bind(&subject.id)
        .fetch_optional(&mut **transaction)
        .await?
        .map(map_subject_revision)
        .transpose()?
        else {
            return Ok(None);
        };
        if revision.revision_digest.trim().is_empty()
            || revision.subject_id != subject.id
            || revision.subject_kind != subject.subject_kind
            || revision.provider_entry_id != subject.provider_entry_id
            || revision.daemon_id != subject.daemon_id
            || revision.executor_type != subject.executor_type
        {
            return Err(DbError::Check(
                "pricing subject current revision identity is invalid".to_owned(),
            ));
        }
        let binding = sqlx::query(
            "SELECT * FROM pricing_subject_binding
             WHERE subject_id = ? AND subject_revision_id = ?
               AND subject_revision_digest = ? AND scope_key = ?
               AND runtime_model = ? AND state = 'active'
             ORDER BY CASE source_kind
                        WHEN 'manual_override' THEN 0
                        WHEN 'models_dev_catalog' THEN 1
                        ELSE 2
                      END,
                      effective_at DESC, id DESC
             LIMIT 1",
        )
        .bind(&subject.id)
        .bind(&revision.id)
        .bind(&revision.revision_digest)
        .bind(scope_key)
        .bind(runtime_model)
        .fetch_optional(&mut **transaction)
        .await?
        .map(map_binding)
        .transpose()?;
        let Some(binding) = binding else {
            return Ok(Some(ResolvedPricingSubjectBinding {
                subject,
                revision,
                binding: None,
                rate: None,
            }));
        };
        if binding.owner_user_id != subject.owner_user_id
            || binding.subject_revision_digest != revision.revision_digest
        {
            return Err(DbError::Check(
                "pricing binding subject revision identity is invalid".to_owned(),
            ));
        }
        let Some(rate) = sqlx::query("SELECT * FROM pricing_rate_revision WHERE id = ?")
            .bind(&binding.rate_revision_id)
            .fetch_optional(&mut **transaction)
            .await?
            .map(map_rate_revision)
            .transpose()?
        else {
            return Err(DbError::Check(
                "pricing binding rate revision is missing".to_owned(),
            ));
        };
        if rate.source_kind != binding.source_kind {
            return Err(DbError::Check(
                "pricing binding source differs from rate revision".to_owned(),
            ));
        }
        match binding.source_kind {
            PricingRateSourceKind::ManualOverride => {
                if rate.catalog_snapshot_id.is_some()
                    || rate.catalog_provider_id.is_some()
                    || rate.catalog_model_id.is_some()
                    || rate.pricing_subject_revision_id.as_deref() != Some(revision.id.as_str())
                    || rate.pricing_subject_revision_digest.as_deref()
                        != Some(revision.revision_digest.as_str())
                    || rate.runtime_model.as_deref() != Some(runtime_model)
                    || binding.catalog_provider_id.is_some()
                    || binding.catalog_model_id.is_some()
                {
                    return Err(DbError::Check(
                        "manual pricing binding canonical identity is invalid".to_owned(),
                    ));
                }
            }
            PricingRateSourceKind::ModelsDevCatalog => {
                if rate.catalog_snapshot_id.is_none()
                    || rate.catalog_provider_id.as_deref() != binding.catalog_provider_id.as_deref()
                    || rate.catalog_model_id.as_deref() != binding.catalog_model_id.as_deref()
                    || binding.catalog_provider_id.is_none()
                    || binding.catalog_model_id.is_none()
                    || rate.pricing_subject_revision_id.is_some()
                    || rate.pricing_subject_revision_digest.is_some()
                    || rate.runtime_model.is_some()
                {
                    return Err(DbError::Check(
                        "catalog pricing binding canonical identity is invalid".to_owned(),
                    ));
                }
            }
        }
        Ok(Some(ResolvedPricingSubjectBinding {
            subject,
            revision,
            binding: Some(binding),
            rate: Some(rate),
        }))
    }

    async fn list_pricing_subjects(&self, owner_user_id: &str) -> Result<Vec<PricingSubject>> {
        sqlx::query(
            "SELECT * FROM pricing_subject
             WHERE owner_user_id = ? ORDER BY updated_at DESC, id DESC",
        )
        .bind(owner_user_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_subject)
        .collect()
    }

    async fn update_pricing_subject(&self, input: UpdatePricingSubject) -> Result<PricingSubject> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let subject = self
            .update_pricing_subject_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(subject)
    }

    async fn update_pricing_subject_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: UpdatePricingSubject,
    ) -> Result<PricingSubject> {
        let current = sqlx::query("SELECT * FROM pricing_subject WHERE id = ?")
            .bind(&input.id)
            .fetch_optional(&mut **transaction)
            .await
            .map_err(check_error)?
            .map(map_subject)
            .transpose()?
            .ok_or(DbError::NotFound)?;
        let result = sqlx::query(
            "UPDATE pricing_subject
             SET current_revision_id = ?, state = ?, last_idempotency_key = ?,
                 last_update_digest = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(
            input
                .current_revision_id
                .unwrap_or(current.current_revision_id),
        )
        .bind(input.state.unwrap_or(current.state).to_string())
        .bind(
            input
                .last_idempotency_key
                .unwrap_or(current.last_idempotency_key),
        )
        .bind(
            input
                .last_update_digest
                .unwrap_or(current.last_update_digest),
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
                fetch_version(transaction, "pricing_subject", &input.id).await?,
            );
        }
        sqlx::query("SELECT * FROM pricing_subject WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut **transaction)
            .await
            .map_err(check_error)
            .and_then(map_subject)
    }

    async fn create_pricing_subject_revision(
        &self,
        input: CreatePricingSubjectRevision,
    ) -> Result<PricingSubjectRevision> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let revision = self
            .create_pricing_subject_revision_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(revision)
    }

    async fn create_pricing_subject_revision_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreatePricingSubjectRevision,
    ) -> Result<PricingSubjectRevision> {
        let result = sqlx::query(
            "INSERT INTO pricing_subject_revision (
                id, subject_id, owner_user_id, revision, revision_digest,
                subject_kind, provider_entry_id, daemon_id, executor_type,
                provider_kind, credential_method, endpoint_class,
                runtime_fingerprint, schema_revision, non_secret_identity_json,
                created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.subject_id)
        .bind(&input.owner_user_id)
        .bind(input.revision)
        .bind(&input.revision_digest)
        .bind(input.subject_kind.to_string())
        .bind(input.provider_entry_id.as_deref())
        .bind(input.daemon_id.as_deref())
        .bind(input.executor_type.as_deref())
        .bind(&input.provider_kind)
        .bind(&input.credential_method)
        .bind(&input.endpoint_class)
        .bind(input.runtime_fingerprint.as_deref())
        .bind(&input.schema_revision)
        .bind(&input.non_secret_identity_json)
        .bind(&input.created_at)
        .execute(&mut **transaction)
        .await;
        if let Err(error) = result {
            if let Some(row) = sqlx::query(
                "SELECT * FROM pricing_subject_revision
                 WHERE subject_id = ? AND revision_digest = ?",
            )
            .bind(&input.subject_id)
            .bind(&input.revision_digest)
            .fetch_optional(&mut **transaction)
            .await?
            {
                let existing = map_subject_revision(row)?;
                if subject_revision_matches_input(&existing, &input) {
                    return Ok(existing);
                }
                return Err(DbError::IdempotencyConflict);
            }
            return Err(duplicate_error(error));
        }
        sqlx::query("SELECT * FROM pricing_subject_revision WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut **transaction)
            .await
            .map_err(check_error)
            .and_then(map_subject_revision)
    }

    async fn get_pricing_subject_revision(
        &self,
        id: &str,
    ) -> Result<Option<PricingSubjectRevision>> {
        sqlx::query("SELECT * FROM pricing_subject_revision WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .map(map_subject_revision)
            .transpose()
    }

    async fn list_pricing_subject_revisions(
        &self,
        subject_id: &str,
    ) -> Result<Vec<PricingSubjectRevision>> {
        sqlx::query(
            "SELECT * FROM pricing_subject_revision
             WHERE subject_id = ? ORDER BY revision DESC, id DESC",
        )
        .bind(subject_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_subject_revision)
        .collect()
    }

    async fn create_pricing_subject_binding(
        &self,
        input: CreatePricingSubjectBinding,
    ) -> Result<PricingSubjectBinding> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let binding = self
            .create_pricing_subject_binding_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(binding)
    }

    async fn create_pricing_subject_binding_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreatePricingSubjectBinding,
    ) -> Result<PricingSubjectBinding> {
        sqlx::query(
            "INSERT INTO pricing_subject_binding (
                id, owner_user_id, subject_id, subject_revision_id,
                subject_revision_digest, scope_key, runtime_model, source_kind,
                catalog_provider_id, catalog_model_id, rate_revision_id,
                binding_digest, state, version, effective_at, retired_at,
                created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', 1, ?, NULL, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.owner_user_id)
        .bind(&input.subject_id)
        .bind(&input.subject_revision_id)
        .bind(&input.subject_revision_digest)
        .bind(&input.scope_key)
        .bind(&input.runtime_model)
        .bind(input.source_kind.to_string())
        .bind(input.catalog_provider_id.as_deref())
        .bind(input.catalog_model_id.as_deref())
        .bind(&input.rate_revision_id)
        .bind(&input.binding_digest)
        .bind(&input.effective_at)
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&mut **transaction)
        .await
        .map_err(duplicate_error)?;
        sqlx::query("SELECT * FROM pricing_subject_binding WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut **transaction)
            .await
            .map_err(check_error)
            .and_then(map_binding)
    }

    async fn get_pricing_subject_binding(&self, id: &str) -> Result<Option<PricingSubjectBinding>> {
        sqlx::query("SELECT * FROM pricing_subject_binding WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .map(map_binding)
            .transpose()
    }

    async fn get_active_pricing_subject_binding(
        &self,
        subject_id: &str,
        runtime_model: &str,
    ) -> Result<Option<PricingSubjectBinding>> {
        sqlx::query(
            "SELECT * FROM pricing_subject_binding
             WHERE subject_id = ? AND runtime_model = ? AND state = 'active'
             ORDER BY CASE source_kind
                        WHEN 'manual_override' THEN 0
                        WHEN 'models_dev_catalog' THEN 1
                        ELSE 2
                      END,
                      effective_at DESC, id DESC
             LIMIT 1",
        )
        .bind(subject_id)
        .bind(runtime_model)
        .fetch_optional(&self.pool)
        .await
        .map_err(check_error)?
        .map(map_binding)
        .transpose()
    }

    async fn get_pricing_adjustment(
        &self,
        owner_user_id: &str,
        scope: &PricingAdjustmentScope,
    ) -> Result<Option<PricingAdjustment>> {
        get_adjustment_on(&self.pool, owner_user_id, scope).await
    }

    async fn get_pricing_adjustment_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        owner_user_id: &str,
        scope: &PricingAdjustmentScope,
    ) -> Result<Option<PricingAdjustment>> {
        get_adjustment_on(&mut **transaction, owner_user_id, scope).await
    }

    async fn upsert_pricing_adjustment(
        &self,
        input: UpsertPricingAdjustment,
    ) -> Result<PricingAdjustment> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let current =
            get_adjustment_on(&mut *transaction, &input.owner_user_id, &input.scope).await?;
        let current_version = current.as_ref().map_or(0, |row| row.version);
        if current_version != input.expected_version {
            return Err(DbError::VersionConflict);
        }
        let (provider_entry_id, daemon_id, executor_type, agent_id) =
            adjustment_scope_columns(&input.scope);
        let id = current
            .as_ref()
            .map(|row| row.id.clone())
            .unwrap_or_else(new_uuid_v4);
        sqlx::query(
            "INSERT INTO pricing_adjustment (
                id, owner_user_id, scope_kind, provider_entry_id, daemon_id,
                executor_type, agent_id, mode, discount_bps,
                input_nano_usd_per_million, output_nano_usd_per_million,
                cache_read_nano_usd_per_million, cache_write_nano_usd_per_million,
                catalog_provider_id, catalog_model_id, version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
             ON CONFLICT(id) DO UPDATE SET
                mode = excluded.mode,
                discount_bps = excluded.discount_bps,
                input_nano_usd_per_million = excluded.input_nano_usd_per_million,
                output_nano_usd_per_million = excluded.output_nano_usd_per_million,
                cache_read_nano_usd_per_million = excluded.cache_read_nano_usd_per_million,
                cache_write_nano_usd_per_million = excluded.cache_write_nano_usd_per_million,
                catalog_provider_id = excluded.catalog_provider_id,
                catalog_model_id = excluded.catalog_model_id,
                version = pricing_adjustment.version + 1,
                updated_at = excluded.updated_at",
        )
        .bind(&id)
        .bind(&input.owner_user_id)
        .bind(input.scope.kind())
        .bind(provider_entry_id)
        .bind(daemon_id)
        .bind(executor_type)
        .bind(agent_id)
        .bind(input.mode.to_string())
        .bind(input.discount_bps)
        .bind(input.fixed_rates.input)
        .bind(input.fixed_rates.output)
        .bind(input.fixed_rates.cache_read)
        .bind(input.fixed_rates.cache_write)
        .bind(input.catalog_provider_id.as_deref())
        .bind(input.catalog_model_id.as_deref())
        .bind(&input.now)
        .bind(&input.now)
        .execute(&mut *transaction)
        .await
        .map_err(check_error)?;
        let saved = get_adjustment_on(&mut *transaction, &input.owner_user_id, &input.scope)
            .await?
            .ok_or(DbError::NotFound)?;
        transaction.commit().await?;
        Ok(saved)
    }

    async fn delete_pricing_adjustment(
        &self,
        owner_user_id: &str,
        scope: &PricingAdjustmentScope,
        expected_version: i64,
    ) -> Result<()> {
        let (provider_entry_id, daemon_id, executor_type, agent_id) =
            adjustment_scope_columns(scope);
        let deleted = sqlx::query(&format!(
            "DELETE FROM pricing_adjustment WHERE {ADJUSTMENT_SCOPE_FILTER} AND version = ?"
        ))
        .bind(owner_user_id)
        .bind(scope.kind())
        .bind(provider_entry_id)
        .bind(daemon_id)
        .bind(executor_type)
        .bind(agent_id)
        .bind(expected_version)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if deleted == 0 {
            return Err(
                if get_adjustment_on(&self.pool, owner_user_id, scope)
                    .await?
                    .is_some()
                {
                    DbError::VersionConflict
                } else {
                    DbError::NotFound
                },
            );
        }
        Ok(())
    }

    async fn list_pricing_subject_bindings(
        &self,
        subject_id: &str,
        include_retired: bool,
    ) -> Result<Vec<PricingSubjectBinding>> {
        let sql = if include_retired {
            "SELECT * FROM pricing_subject_binding
             WHERE subject_id = ? ORDER BY runtime_model ASC, effective_at DESC, id DESC"
        } else {
            "SELECT * FROM pricing_subject_binding
             WHERE subject_id = ? AND state = 'active'
             ORDER BY runtime_model ASC, effective_at DESC, id DESC"
        };
        sqlx::query(sql)
            .bind(subject_id)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(map_binding)
            .collect()
    }

    async fn update_pricing_subject_binding(
        &self,
        input: UpdatePricingSubjectBinding,
    ) -> Result<PricingSubjectBinding> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let binding = self
            .update_pricing_subject_binding_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(binding)
    }

    async fn update_pricing_subject_binding_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: UpdatePricingSubjectBinding,
    ) -> Result<PricingSubjectBinding> {
        let result = sqlx::query(
            "UPDATE pricing_subject_binding
             SET source_kind = ?, catalog_provider_id = ?, catalog_model_id = ?,
                 rate_revision_id = ?, binding_digest = ?, effective_at = ?,
                 version = version + 1, updated_at = ?
             WHERE id = ? AND version = ? AND state = 'active'",
        )
        .bind(input.source_kind.to_string())
        .bind(input.catalog_provider_id.as_deref())
        .bind(input.catalog_model_id.as_deref())
        .bind(&input.rate_revision_id)
        .bind(&input.binding_digest)
        .bind(&input.effective_at)
        .bind(&input.updated_at)
        .bind(&input.id)
        .bind(input.expected_version)
        .execute(&mut **transaction)
        .await
        .map_err(check_error)?;
        if result.rows_affected() == 0 {
            let actual = fetch_version(transaction, "pricing_subject_binding", &input.id).await?;
            return conflict_or_not_found(&input.id, input.expected_version, actual);
        }
        sqlx::query("SELECT * FROM pricing_subject_binding WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut **transaction)
            .await
            .map_err(check_error)
            .and_then(map_binding)
    }

    async fn retire_pricing_subject_binding(
        &self,
        input: RetirePricingSubjectBinding,
    ) -> Result<PricingSubjectBinding> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let binding = self
            .retire_pricing_subject_binding_in_tx(&mut transaction, input)
            .await?;
        transaction.commit().await?;
        Ok(binding)
    }

    async fn retire_pricing_subject_binding_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: RetirePricingSubjectBinding,
    ) -> Result<PricingSubjectBinding> {
        let result = sqlx::query(
            "UPDATE pricing_subject_binding
             SET state = 'retired', retired_at = ?, version = version + 1,
                 updated_at = ?
             WHERE id = ? AND version = ? AND state = 'active'",
        )
        .bind(&input.retired_at)
        .bind(&input.updated_at)
        .bind(&input.id)
        .bind(input.expected_version)
        .execute(&mut **transaction)
        .await
        .map_err(check_error)?;
        if result.rows_affected() == 0 {
            let actual = fetch_version(transaction, "pricing_subject_binding", &input.id).await?;
            return conflict_or_not_found(&input.id, input.expected_version, actual);
        }
        sqlx::query("SELECT * FROM pricing_subject_binding WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut **transaction)
            .await
            .map_err(check_error)
            .and_then(map_binding)
    }
}
