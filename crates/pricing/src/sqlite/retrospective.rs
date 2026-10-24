//! SQLite retrospective preview validation and estimate commits.

use super::*;

struct PreviewRecord {
    owner_user_id: String,
    project_id: String,
    catalog_snapshot_id: String,
    catalog_freshness: db::PricingCatalogFreshness,
    usage_set_digest: String,
    eligible_event_count: i64,
    unmatched_event_count: i64,
    already_reported_event_count: i64,
    filters_json: String,
    projected_cost_summary_json: String,
    status: db::CostEstimationPreviewStatus,
    version: i64,
    expires_at: String,
    created_at: String,
}

async fn preview_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    id: &str,
) -> Result<Option<PreviewRecord>, db::DbError> {
    let Some(row) = sqlx::query(
        "SELECT owner_user_id, project_id, catalog_snapshot_id,
                catalog_freshness, usage_set_digest,
                eligible_event_count, unmatched_event_count,
                already_reported_event_count, filters_json,
                projected_cost_summary_json, status, version, expires_at, created_at
         FROM cost_estimation_preview WHERE id = ?",
    )
    .bind(id)
    .fetch_optional(&mut **transaction)
    .await?
    else {
        return Ok(None);
    };
    let status: String = row.try_get("status")?;
    Ok(Some(PreviewRecord {
        owner_user_id: row.try_get("owner_user_id")?,
        project_id: row.try_get("project_id")?,
        catalog_snapshot_id: row.try_get("catalog_snapshot_id")?,
        catalog_freshness: row
            .try_get::<String, _>("catalog_freshness")?
            .parse()
            .map_err(|error| db::DbError::Check(format!("invalid catalog freshness: {error}")))?,
        usage_set_digest: row.try_get("usage_set_digest")?,
        eligible_event_count: row.try_get("eligible_event_count")?,
        unmatched_event_count: row.try_get("unmatched_event_count")?,
        already_reported_event_count: row.try_get("already_reported_event_count")?,
        filters_json: row.try_get("filters_json")?,
        projected_cost_summary_json: row.try_get("projected_cost_summary_json")?,
        status: status
            .parse()
            .map_err(|error| db::DbError::Check(format!("invalid preview status: {error}")))?,
        version: row.try_get("version")?,
        expires_at: row.try_get("expires_at")?,
        created_at: row.try_get("created_at")?,
    }))
}

fn preview_source_event_ids_from_filters(filters_json: &str) -> Result<Vec<String>, String> {
    let value: Value = serde_json::from_str(filters_json).map_err(|error| {
        format!("stored retrospective preview filters are invalid JSON: {error}")
    })?;
    let object = value
        .as_object()
        .ok_or_else(|| "stored retrospective preview filters must be an object".to_owned())?;
    let ids = object
        .get("source_event_ids")
        .and_then(Value::as_array)
        .ok_or_else(|| "stored retrospective preview omits source_event_ids".to_owned())?;
    if ids.len() > MAX_RETROSPECTIVE_SOURCE_EVENTS {
        return Err("stored retrospective preview contains too many source events".to_owned());
    }
    let mut result = Vec::with_capacity(ids.len());
    for id in ids {
        let id = id
            .as_str()
            .ok_or_else(|| "stored retrospective source event IDs must be strings".to_owned())?;
        validate_identifier(id, "retrospective source event id")?;
        result.push(id.to_owned());
    }
    result.sort();
    result.dedup();
    if result.len() != ids.len() {
        return Err("stored retrospective preview contains duplicate source event IDs".to_owned());
    }
    Ok(result)
}

fn preview_snapshot_revision_digest_from_filters(filters_json: &str) -> Result<String, String> {
    let value: Value = serde_json::from_str(filters_json).map_err(|error| {
        format!("stored retrospective preview filters are invalid JSON: {error}")
    })?;
    let object = value
        .as_object()
        .ok_or_else(|| "stored retrospective preview filters must be an object".to_owned())?;
    let digest = object
        .get("snapshot_revision_digest")
        .and_then(Value::as_str)
        .ok_or_else(|| "stored retrospective preview omits snapshot_revision_digest".to_owned())?;
    validate_identifier(digest, "retrospective snapshot revision digest")?;
    Ok(digest.to_owned())
}

fn preview_filters_json(
    preview: &RetrospectivePreview,
    snapshot_revision_digest: &str,
    catalog_status: &CatalogStatus,
    catalog_freshness: CatalogFreshness,
    now: SystemTime,
) -> String {
    let catalog_status_at = catalog_status.at(now);
    let mut source_event_ids = preview.source_event_ids.clone();
    source_event_ids.sort();
    json!({
        "schema": "retrospective-preview-v1",
        "source_event_ids": source_event_ids,
        "snapshot_revision_digest": snapshot_revision_digest,
        "catalog_freshness": catalog_freshness_name(catalog_freshness),
        "catalog_status": {
            "state": catalog_state_name(catalog_status_at.state),
            "active_snapshot_id": catalog_status.active_snapshot_id.clone(),
            "revision": catalog_status.revision.clone(),
            "last_checked_at": catalog_status_at.last_checked_at.map(db_time),
            "last_successful_check_at": catalog_status_at.last_successful_check_at.map(db_time),
            "stale_after": catalog_status_at.stale_after.map(db_time),
        },
        "eligible_events": preview.eligible_events.iter().map(|candidate| json!({
            "event_id": candidate.event_id,
            "amount_nano_usd": candidate.amount.as_nano_usd(),
            "rate_revision_id": candidate.rate_revision_id,
            "snapshot_id": candidate.snapshot_id,
            "selected_tier": candidate.selected_tier,
        })).collect::<Vec<_>>(),
        "unmatched_events": preview.unmatched_events.iter().map(|event| json!({
            "event_id": event.event_id,
            "reason": event.reason.as_str(),
        })).collect::<Vec<_>>(),
    })
    .to_string()
}

fn catalog_freshness_name(freshness: CatalogFreshness) -> &'static str {
    match freshness {
        CatalogFreshness::Fresh => "fresh",
        CatalogFreshness::Stale => "stale",
        CatalogFreshness::RefreshFailed => "refresh_failed",
        CatalogFreshness::NotApplicable => "not_applicable",
    }
}

fn db_catalog_freshness(
    freshness: CatalogFreshness,
) -> Result<db::PricingCatalogFreshness, String> {
    match freshness {
        CatalogFreshness::Fresh => Ok(db::PricingCatalogFreshness::Fresh),
        CatalogFreshness::Stale => Ok(db::PricingCatalogFreshness::Stale),
        CatalogFreshness::RefreshFailed => Ok(db::PricingCatalogFreshness::RefreshFailed),
        CatalogFreshness::NotApplicable => {
            Err("catalog retrospective freshness cannot be not_applicable".to_owned())
        }
    }
}

fn service_catalog_freshness(freshness: db::PricingCatalogFreshness) -> CatalogFreshness {
    match freshness {
        db::PricingCatalogFreshness::Fresh => CatalogFreshness::Fresh,
        db::PricingCatalogFreshness::Stale => CatalogFreshness::Stale,
        db::PricingCatalogFreshness::RefreshFailed => CatalogFreshness::RefreshFailed,
    }
}

fn parse_catalog_freshness(value: &str) -> Result<CatalogFreshness, String> {
    match value {
        "fresh" => Ok(CatalogFreshness::Fresh),
        "stale" => Ok(CatalogFreshness::Stale),
        "refresh_failed" => Ok(CatalogFreshness::RefreshFailed),
        "not_applicable" => {
            Err("catalog retrospective freshness cannot be not_applicable".to_owned())
        }
        other => Err(format!("unknown catalog retrospective freshness {other:?}")),
    }
}

fn catalog_freshness_from_json(
    json_text: &str,
    field: &str,
) -> Result<CatalogFreshness, RetrospectiveRepositoryError> {
    let value: Value = serde_json::from_str(json_text).map_err(|error| {
        retrospective_conversion(format!("invalid persisted {field} JSON: {error}"))
    })?;
    let freshness = value
        .as_object()
        .and_then(|object| object.get("catalog_freshness"))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            retrospective_conversion(format!("persisted {field} omits catalog_freshness"))
        })?;
    parse_catalog_freshness(freshness).map_err(retrospective_conversion)
}

fn canonical_event_list_equal(
    left: &[RetrospectiveEstimateCandidate],
    right: &[RetrospectiveEstimateCandidate],
) -> bool {
    let mut left = left.to_vec();
    let mut right = right.to_vec();
    left.sort_by(|a, b| a.event_id.cmp(&b.event_id));
    right.sort_by(|a, b| a.event_id.cmp(&b.event_id));
    left == right
}

fn canonical_unmatched_list_equal(
    left: &[RetrospectiveUnmatchedEvent],
    right: &[RetrospectiveUnmatchedEvent],
) -> bool {
    let mut left = left.to_vec();
    let mut right = right.to_vec();
    left.sort_by(|a, b| a.event_id.cmp(&b.event_id));
    right.sort_by(|a, b| a.event_id.cmp(&b.event_id));
    left == right
}

async fn usage_event_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    event_id: &str,
    owner_user_id: &str,
    project_id: &str,
) -> Result<RetrospectiveUsageEvent, RetrospectiveRepositoryError> {
    validate_identifier(event_id, "retrospective source event id")
        .map_err(retrospective_conversion)?;
    let row = sqlx::query(
        "SELECT id, owner_user_id, project_id, provider_id, model_id,
                input_tokens, output_tokens, cache_read_tokens,
                cache_write_tokens, context_tokens, provider_reported_nano_usd,
                legacy_reported_cost_usd IS NOT NULL AS has_legacy_reported_cost,
                cost_kind, occurred_at
         FROM usage_event WHERE id = ?",
    )
    .bind(event_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(retrospective_sqlx)?
    .ok_or(RetrospectiveRepositoryError::UsageSetConflict)?;

    let stored_id: String = row
        .try_get("id")
        .map_err(|error| retrospective_conversion(error.to_string()))?;
    let stored_owner: Option<String> = row
        .try_get("owner_user_id")
        .map_err(|error| retrospective_conversion(error.to_string()))?;
    let stored_project: Option<String> = row
        .try_get("project_id")
        .map_err(|error| retrospective_conversion(error.to_string()))?;
    if stored_id != event_id
        || stored_owner.as_deref() != Some(owner_user_id)
        || stored_project.as_deref() != Some(project_id)
    {
        return Err(RetrospectiveRepositoryError::UsageSetConflict);
    }

    let optional_identifier = |value: Option<String>, field: &str| {
        value
            .filter(|value| !value.trim().is_empty())
            .map(|value| {
                validate_identifier(&value, field)
                    .map_err(retrospective_conversion)
                    .map(|()| value)
            })
            .transpose()
    };
    let provider_id: Option<String> = optional_identifier(
        row.try_get("provider_id")
            .map_err(|error| retrospective_conversion(error.to_string()))?,
        "retrospective provider id",
    )?;
    let model_id: Option<String> = optional_identifier(
        row.try_get("model_id")
            .map_err(|error| retrospective_conversion(error.to_string()))?,
        "retrospective model id",
    )?;

    let input_tokens: Option<i64> = row
        .try_get("input_tokens")
        .map_err(|error| retrospective_conversion(error.to_string()))?;
    let output_tokens: Option<i64> = row
        .try_get("output_tokens")
        .map_err(|error| retrospective_conversion(error.to_string()))?;
    let cache_read_tokens: Option<i64> = row
        .try_get("cache_read_tokens")
        .map_err(|error| retrospective_conversion(error.to_string()))?;
    let cache_write_tokens: Option<i64> = row
        .try_get("cache_write_tokens")
        .map_err(|error| retrospective_conversion(error.to_string()))?;
    let counters = match (
        input_tokens,
        output_tokens,
        cache_read_tokens,
        cache_write_tokens,
    ) {
        (Some(input), Some(output), Some(cache_read), Some(cache_write))
            if input >= 0 && output >= 0 && cache_read >= 0 && cache_write >= 0 =>
        {
            Some(EventTokenCounts::new(
                u64::try_from(input).map_err(|_| {
                    retrospective_conversion("input token count exceeds the supported range")
                })?,
                u64::try_from(output).map_err(|_| {
                    retrospective_conversion("output token count exceeds the supported range")
                })?,
                u64::try_from(cache_read).map_err(|_| {
                    retrospective_conversion("cache-read token count exceeds the supported range")
                })?,
                u64::try_from(cache_write).map_err(|_| {
                    retrospective_conversion("cache-write token count exceeds the supported range")
                })?,
            ))
        }
        (Some(_), Some(_), Some(_), Some(_)) => {
            return Err(RetrospectiveRepositoryError::UsageSetConflict);
        }
        _ => None,
    };

    let context_tokens: Option<i64> = row
        .try_get("context_tokens")
        .map_err(|error| retrospective_conversion(error.to_string()))?;
    let context_tokens = match context_tokens {
        Some(value) if value >= 0 => Some(u64::try_from(value).map_err(|_| {
            retrospective_conversion("context token count exceeds the supported range")
        })?),
        Some(_) => return Err(RetrospectiveRepositoryError::UsageSetConflict),
        None => None,
    };
    let provider_reported_nano_usd: Option<i64> = row
        .try_get("provider_reported_nano_usd")
        .map_err(|error| retrospective_conversion(error.to_string()))?;
    // Legacy rows store an old REAL amount.  We only need to know whether
    // that authoritative provider-reported field was populated; selecting a
    // boolean SQLite expression avoids loading/coercing money through a
    // floating-point type.
    let has_legacy_reported_cost: i64 = row
        .try_get("has_legacy_reported_cost")
        .map_err(|error| retrospective_conversion(error.to_string()))?;
    let has_legacy_reported_cost = has_legacy_reported_cost != 0;
    let cost_kind: String = row
        .try_get("cost_kind")
        .map_err(|error| retrospective_conversion(error.to_string()))?;
    let provider_reported_amount = match provider_reported_nano_usd {
        Some(value) => Some(
            NanoUsd::from_nano_usd(value).ok_or(RetrospectiveRepositoryError::UsageSetConflict)?,
        ),
        None => None,
    };
    let provider_reported_amount = if provider_reported_amount.is_some()
        || has_legacy_reported_cost
        || cost_kind == "provider_reported"
    {
        // A legacy REAL amount is intentionally not converted through a
        // floating-point representation.
        // It still proves that the event is provider-reported and therefore
        // must not receive a Forge estimate when its typed amount is absent.
        provider_reported_amount
    } else {
        None
    };
    let occurred_at: String = row
        .try_get("occurred_at")
        .map_err(|error| retrospective_conversion(error.to_string()))?;
    let occurred_at = system_time(&occurred_at).map_err(retrospective_conversion)?;
    Ok(RetrospectiveUsageEvent {
        event_id: stored_id,
        provider_id,
        model_id,
        // Typed provider-reported money is enough for the domain to exclude
        // the row and the original counters remain part of the source digest.
        // A legacy REAL amount or an untyped provider-reported marker has no
        // safe fixed-point representation, so it is deliberately unmetered.
        counters: if provider_reported_amount.is_some() {
            counters
        } else if has_legacy_reported_cost || cost_kind == "provider_reported" {
            None
        } else {
            counters
        },
        provider_reported_amount,
        context_tokens,
        occurred_at,
    })
}

async fn source_events_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    preview: &RetrospectivePreview,
    owner_user_id: &str,
) -> Result<Vec<RetrospectiveUsageEvent>, RetrospectiveRepositoryError> {
    if preview.source_event_ids.len() > MAX_RETROSPECTIVE_SOURCE_EVENTS {
        return Err(retrospective_conversion(
            "retrospective preview contains too many source events",
        ));
    }
    let mut ids = preview.source_event_ids.clone();
    ids.sort();
    ids.dedup();
    if ids.len() != preview.source_event_ids.len() {
        return Err(RetrospectiveRepositoryError::UsageSetConflict);
    }
    let mut events = Vec::with_capacity(ids.len());
    for event_id in ids {
        events.push(
            usage_event_in_tx(transaction, &event_id, owner_user_id, &preview.project_id).await?,
        );
    }
    Ok(events)
}

async fn next_estimate_revision_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    event_id: &str,
) -> Result<(i64, Option<String>), db::DbError> {
    let row = sqlx::query(
        "SELECT revision, id FROM cost_estimate_revision
         WHERE usage_event_id = ? ORDER BY revision DESC, id DESC LIMIT 1",
    )
    .bind(event_id)
    .fetch_optional(&mut **transaction)
    .await?;
    match row {
        Some(row) => {
            let revision: i64 = row.try_get("revision")?;
            let id: String = row.try_get("id")?;
            Ok((
                revision
                    .checked_add(1)
                    .ok_or_else(|| db::DbError::Check("estimate revision overflow".to_owned()))?,
                Some(id),
            ))
        }
        None => Ok((1, None)),
    }
}

fn persisted_reason(
    reason: PriceSelectionReasonCode,
) -> Result<db::CostCoverageReasonCode, RetrospectiveRepositoryError> {
    match reason {
        PriceSelectionReasonCode::MissingProvider => {
            Ok(db::CostCoverageReasonCode::MissingProvider)
        }
        PriceSelectionReasonCode::MissingModel => Ok(db::CostCoverageReasonCode::MissingModel),
        PriceSelectionReasonCode::MissingBinding | PriceSelectionReasonCode::RetiredBinding => {
            Ok(db::CostCoverageReasonCode::MissingBinding)
        }
        PriceSelectionReasonCode::IdentityMismatch => {
            Ok(db::CostCoverageReasonCode::IdentityMismatch)
        }
        PriceSelectionReasonCode::MissingRate => Ok(db::CostCoverageReasonCode::MissingRate),
        PriceSelectionReasonCode::UnresolvedTier => Ok(db::CostCoverageReasonCode::UnresolvedTier),
        PriceSelectionReasonCode::Unmetered => Err(retrospective_conversion(
            "V135 estimate revisions have no unmetered reason code",
        )),
    }
}

fn preview_summary(
    preview: &RetrospectivePreview,
    catalog_status: &CatalogStatus,
    catalog_freshness: CatalogFreshness,
    now: SystemTime,
) -> String {
    let catalog_status_at = catalog_status.at(now);
    json!({
        "currency": "USD",
        "decimal": preview.projected_cost.map(NanoUsd::to_usd_decimal),
        "nano_usd": preview.projected_cost.map(NanoUsd::as_nano_usd),
        "eligible_event_count": preview.eligible_event_count,
        "unmatched_event_count": preview.unmatched_event_count,
        "already_reported_event_count": preview.already_reported_event_count,
        "catalog_freshness": catalog_freshness_name(catalog_freshness),
        "catalog_state": catalog_state_name(catalog_status_at.state),
        "catalog_active_snapshot_id": catalog_status_at.active_snapshot_id.clone(),
        "catalog_revision": catalog_status_at.revision.clone(),
        "catalog_last_checked_at": catalog_status_at.last_checked_at.map(db_time),
        "catalog_last_successful_check_at": catalog_status_at.last_successful_check_at.map(db_time),
        "catalog_stale_after": catalog_status_at.stale_after.map(db_time),
    })
    .to_string()
}

fn cost_from_summary(summary: &str) -> Result<Option<NanoUsd>, RetrospectiveRepositoryError> {
    let value: Value = serde_json::from_str(summary)
        .map_err(|error| retrospective_conversion(format!("invalid cost summary JSON: {error}")))?;
    let nano = value
        .as_object()
        .and_then(|object| object.get("nano_usd"))
        .ok_or_else(|| retrospective_conversion("cost summary omits nano_usd"))?;
    match nano {
        Value::Null => Ok(None),
        Value::Number(number) => {
            let value = number.as_i64().ok_or_else(|| {
                retrospective_conversion("cost summary nano_usd is not an integer")
            })?;
            NanoUsd::from_nano_usd(value)
                .ok_or_else(|| retrospective_conversion("cost summary nano_usd is negative"))
                .map(Some)
        }
        _ => Err(retrospective_conversion(
            "cost summary nano_usd must be an integer or null",
        )),
    }
}

fn count_i64(value: usize, field: &str) -> Result<i64, RetrospectiveRepositoryError> {
    value.try_into().map_err(|_| {
        retrospective_conversion(format!("{field} does not fit the database integer range"))
    })
}

fn validate_retrospective_commit_bounds(
    preview: &RetrospectivePreview,
    request: &RetrospectiveCommitRequest,
) -> Result<(), RetrospectiveRepositoryError> {
    validate_identifier(&request.preview_id, "retrospective preview id")
        .map_err(retrospective_conversion)?;
    validate_identifier(&request.usage_set_digest, "retrospective usage-set digest")
        .map_err(retrospective_conversion)?;
    validate_identifier(&request.idempotency_key, "retrospective idempotency key")
        .map_err(retrospective_conversion)?;
    validate_identifier(&preview.id, "retrospective preview id")
        .map_err(retrospective_conversion)?;
    validate_identifier(&preview.project_id, "retrospective project id")
        .map_err(retrospective_conversion)?;
    validate_identifier(&preview.snapshot_id, "retrospective snapshot id")
        .map_err(retrospective_conversion)?;
    validate_identifier(&preview.usage_set_digest, "retrospective usage-set digest")
        .map_err(retrospective_conversion)?;
    if preview.source_event_ids.len() > MAX_RETROSPECTIVE_SOURCE_EVENTS
        || preview.eligible_events.len() > MAX_RETROSPECTIVE_SOURCE_EVENTS
        || preview.unmatched_events.len() > MAX_RETROSPECTIVE_SOURCE_EVENTS
    {
        return Err(retrospective_conversion(
            "retrospective preview contains too many events",
        ));
    }
    for event_id in &preview.source_event_ids {
        validate_identifier(event_id, "retrospective source event id")
            .map_err(retrospective_conversion)?;
    }
    for candidate in &preview.eligible_events {
        validate_identifier(&candidate.event_id, "retrospective candidate event id")
            .map_err(retrospective_conversion)?;
        validate_identifier(
            &candidate.rate_revision_id,
            "retrospective candidate rate revision id",
        )
        .map_err(retrospective_conversion)?;
        validate_identifier(
            &candidate.snapshot_id,
            "retrospective candidate snapshot id",
        )
        .map_err(retrospective_conversion)?;
        validate_optional_text(
            candidate.selected_tier.as_deref(),
            "retrospective candidate selected tier",
        )
        .map_err(retrospective_conversion)?;
    }
    for unmatched in &preview.unmatched_events {
        validate_identifier(&unmatched.event_id, "retrospective unmatched event id")
            .map_err(retrospective_conversion)?;
    }
    Ok(())
}

fn retrospective_commit_request_digest(
    preview: &RetrospectivePreview,
    request: &RetrospectiveCommitRequest,
) -> String {
    let mut source_event_ids = preview.source_event_ids.clone();
    source_event_ids.sort();
    let mut candidates = preview
        .eligible_events
        .iter()
        .map(|candidate| {
            json!({
                "event_id": candidate.event_id,
                "amount_nano_usd": candidate.amount.as_nano_usd(),
                "rate_revision_id": candidate.rate_revision_id,
                "snapshot_id": candidate.snapshot_id,
                "selected_tier": candidate.selected_tier,
            })
            .to_string()
        })
        .collect::<Vec<_>>();
    candidates.sort();
    let mut unmatched = preview
        .unmatched_events
        .iter()
        .map(|event| {
            json!({
                "event_id": event.event_id,
                "reason": event.reason.as_str(),
            })
            .to_string()
        })
        .collect::<Vec<_>>();
    unmatched.sort();
    structured_digest(
        "retrospective-commit-request-v1",
        [
            Some(request.preview_id.clone()),
            Some(request.usage_set_digest.clone()),
            Some(request.idempotency_key.clone()),
            Some(preview.id.clone()),
            Some(preview.project_id.clone()),
            Some(preview.snapshot_id.clone()),
            Some(catalog_freshness_name(preview.catalog_freshness).to_owned()),
            Some(preview.usage_set_digest.clone()),
            Some(serde_json::to_string(&source_event_ids).unwrap_or_default()),
            Some(preview.eligible_event_count.to_string()),
            Some(preview.unmatched_event_count.to_string()),
            Some(preview.already_reported_event_count.to_string()),
            preview
                .projected_cost
                .map(|cost| cost.as_nano_usd().to_string()),
            Some(serde_json::to_string(&candidates).unwrap_or_default()),
            Some(serde_json::to_string(&unmatched).unwrap_or_default()),
            Some(db_time(preview.expires_at)),
        ],
    )
}

fn retrospective_run_result_json(run: &db::CostEstimationRun) -> String {
    json!({
        "kind": "retrospective_run",
        "result_ref": {
            "run_id": run.id,
            "preview_id": run.preview_id,
            "project_id": run.project_id,
            "snapshot_id": run.catalog_snapshot_id,
            "usage_set_digest": run.usage_set_digest,
        },
    })
    .to_string()
}

fn retrospective_run_id_from_result_json(
    value: &str,
    preview: &RetrospectivePreview,
) -> Result<String, RetrospectiveRepositoryError> {
    let value: Value = serde_json::from_str(value)
        .map_err(|error| retrospective_conversion(format!("invalid run receipt JSON: {error}")))?;
    let reference = value
        .get("result_ref")
        .and_then(Value::as_object)
        .ok_or_else(|| retrospective_conversion("run receipt omits result_ref"))?;
    for (field, expected) in [
        ("preview_id", preview.id.as_str()),
        ("project_id", preview.project_id.as_str()),
        ("snapshot_id", preview.snapshot_id.as_str()),
        ("usage_set_digest", preview.usage_set_digest.as_str()),
    ] {
        if reference.get(field).and_then(Value::as_str) != Some(expected) {
            return Err(RetrospectiveRepositoryError::Conflict);
        }
    }
    let run_id = reference
        .get("run_id")
        .and_then(Value::as_str)
        .ok_or_else(|| retrospective_conversion("run receipt omits result_ref.run_id"))?;
    validate_identifier(run_id, "retrospective run id").map_err(retrospective_conversion)?;
    Ok(run_id.to_owned())
}

fn retrospective_run(
    run: db::CostEstimationRun,
    cost: Option<NanoUsd>,
) -> Result<RetrospectiveRun, RetrospectiveRepositoryError> {
    Ok(RetrospectiveRun {
        id: run.id,
        project_id: run.project_id,
        preview_id: run.preview_id,
        snapshot_id: run.catalog_snapshot_id,
        usage_set_digest: run.usage_set_digest,
        applied_event_count: run.applied_event_count.try_into().map_err(|_| {
            retrospective_conversion("stored retrospective applied count is negative")
        })?,
        unmatched_event_count: run.unmatched_event_count.try_into().map_err(|_| {
            retrospective_conversion("stored retrospective unmatched count is negative")
        })?,
        cost,
        created_at: system_time(&run.created_at).map_err(retrospective_conversion)?,
    })
}

#[async_trait]
impl RetrospectiveEstimateRepository for SqlitePricingRepository {
    async fn commit_retrospective_preview(
        &self,
        preview: RetrospectivePreview,
        request: RetrospectiveCommitRequest,
        now: SystemTime,
    ) -> Result<RetrospectiveRun, RetrospectiveRepositoryError> {
        if request.idempotency_key.trim().is_empty() || request.idempotency_key.len() > 256 {
            return Err(RetrospectiveRepositoryError::Conflict);
        }
        validate_retrospective_commit_bounds(&preview, &request)?;
        let request_digest = retrospective_commit_request_digest(&preview, &request);
        let mut receipt_transaction = db::begin_immediate(self.db.pool())
            .await
            .map_err(retrospective_sqlx)?;
        if let Some(result_json) = operation_receipt_in_tx(
            &mut receipt_transaction,
            RETROSPECTIVE_COMMIT_OPERATION_SCOPE,
            &request.idempotency_key,
            &request_digest,
        )
        .await
        .map_err(retrospective_operation_error)?
        {
            let run_id = retrospective_run_id_from_result_json(&result_json, &preview)?;
            receipt_transaction
                .commit()
                .await
                .map_err(retrospective_sqlx)?;
            let run = db::RetrospectiveEstimateRepo::get_cost_estimation_run(&*self.db, &run_id)
                .await
                .map_err(retrospective_error)?
                .ok_or_else(|| {
                    retrospective_conversion(
                        "retrospective operation receipt points to a missing run",
                    )
                })?;
            if run.status != db::CostEstimationRunStatus::Committed {
                return Err(RetrospectiveRepositoryError::Conflict);
            }
            return retrospective_run(run.clone(), cost_from_summary(&run.cost_summary_json)?);
        }
        receipt_transaction
            .commit()
            .await
            .map_err(retrospective_sqlx)?;

        if request.preview_id != preview.id || request.usage_set_digest != preview.usage_set_digest
        {
            return Err(RetrospectiveRepositoryError::UsageSetConflict);
        }
        validate_identifier(&preview.id, "retrospective preview id")
            .map_err(retrospective_conversion)?;
        validate_identifier(&preview.project_id, "retrospective project id")
            .map_err(retrospective_conversion)?;
        validate_identifier(&preview.snapshot_id, "retrospective snapshot id")
            .map_err(retrospective_conversion)?;
        if preview.source_event_ids.len() > MAX_RETROSPECTIVE_SOURCE_EVENTS {
            return Err(retrospective_conversion(
                "retrospective preview contains too many source events",
            ));
        }
        let project = db::ProjectRepo::get_by_id(&*self.db, &preview.project_id)
            .await
            .map_err(retrospective_error)?
            .ok_or_else(|| retrospective_conversion("retrospective project was not found"))?;
        let owner_user_id = project.owner_id.ok_or_else(|| {
            retrospective_conversion("retrospective project has no owner for DB scope")
        })?;
        let snapshot =
            db::PricingCatalogRepo::get_pricing_catalog_snapshot(&*self.db, &preview.snapshot_id)
                .await
                .map_err(retrospective_error)?
                .ok_or_else(|| retrospective_conversion("retrospective snapshot was not found"))?;
        let snapshot = snapshot_from_db(snapshot)
            .await
            .map_err(retrospective_conversion)?;
        if snapshot.id != preview.snapshot_id {
            return Err(retrospective_conversion(
                "retrospective snapshot identity does not match the preview",
            ));
        }

        let operation_key = retrospective_commit_idempotency_key(&request.idempotency_key);

        let now_text = db_time(now);

        let mut transaction = db::begin_immediate(self.db.pool())
            .await
            .map_err(retrospective_sqlx)?;
        // Recheck the immutable receipt after acquiring the commit
        // transaction.  The earlier read is an inexpensive fast path, but
        // two callers can both pass it before either one commits.  A
        // transaction-local preflight makes the receipt the durable
        // all-key idempotency authority and prevents the second caller from
        // touching the preview, usage set, or estimate revisions.
        if let Some(result_json) = operation_receipt_in_tx(
            &mut transaction,
            RETROSPECTIVE_COMMIT_OPERATION_SCOPE,
            &request.idempotency_key,
            &request_digest,
        )
        .await
        .map_err(retrospective_operation_error)?
        {
            let run_id = retrospective_run_id_from_result_json(&result_json, &preview)?;
            transaction.commit().await.map_err(retrospective_sqlx)?;
            let run = db::RetrospectiveEstimateRepo::get_cost_estimation_run(&*self.db, &run_id)
                .await
                .map_err(retrospective_error)?
                .ok_or_else(|| {
                    retrospective_conversion(
                        "retrospective operation receipt points to a missing run",
                    )
                })?;
            if run.status != db::CostEstimationRunStatus::Committed {
                return Err(RetrospectiveRepositoryError::Conflict);
            }
            return retrospective_run(run.clone(), cost_from_summary(&run.cost_summary_json)?);
        }
        let stored_preview = preview_in_tx(&mut transaction, &preview.id)
            .await
            .map_err(retrospective_error)?;
        let had_stored_preview = stored_preview.is_some();
        let source_event_ids = if let Some(stored) = &stored_preview {
            if preview_snapshot_revision_digest_from_filters(&stored.filters_json)
                .map_err(|_| RetrospectiveRepositoryError::UsageSetConflict)?
                != snapshot.revision_digest
            {
                return Err(RetrospectiveRepositoryError::UsageSetConflict);
            }
            preview_source_event_ids_from_filters(&stored.filters_json)
                .map_err(|_| RetrospectiveRepositoryError::UsageSetConflict)?
        } else {
            preview.source_event_ids.clone()
        };
        let mut source_preview = preview.clone();
        source_preview.source_event_ids = source_event_ids;
        let source_events =
            source_events_in_tx(&mut transaction, &source_preview, &owner_user_id).await?;
        // Freshness belongs to the mutable catalog status, not the immutable
        // snapshot payload.  Read it inside the same write transaction used
        // for preview verification so a successful 304 is reflected and the
        // persisted provenance cannot be based on a stale pre-transaction
        // status read.
        let catalog_status = catalog_status_in_tx(&mut transaction).await?;
        let current_catalog_freshness = catalog_status.freshness_for_snapshot(&snapshot, now);
        let catalog_freshness = stored_preview
            .as_ref()
            .map(|stored| service_catalog_freshness(stored.catalog_freshness))
            .unwrap_or(current_catalog_freshness);
        // Recompute every candidate, amount, tier, and digest from the
        // append-only ledger while holding the write transaction.  A caller
        // can mutate its in-memory preview, but it cannot mutate this server
        // side reconstruction without changing the usage-set digest.
        let mut canonical_preview = pricing::preview_retrospective_estimates_with_freshness(
            preview.project_id.clone(),
            &snapshot,
            &source_events,
            now,
            std::time::Duration::ZERO,
            catalog_freshness,
        )
        .map_err(|error| retrospective_conversion(error.to_string()))?;
        if canonical_preview.id != preview.id
            || canonical_preview.catalog_freshness != preview.catalog_freshness
            || canonical_preview.usage_set_digest != preview.usage_set_digest
            || canonical_preview.source_event_ids != source_preview.source_event_ids
            || canonical_preview.eligible_event_count != preview.eligible_event_count
            || canonical_preview.unmatched_event_count != preview.unmatched_event_count
            || canonical_preview.already_reported_event_count
                != preview.already_reported_event_count
            || canonical_preview.projected_cost != preview.projected_cost
            || !canonical_event_list_equal(
                &canonical_preview.eligible_events,
                &preview.eligible_events,
            )
            || !canonical_unmatched_list_equal(
                &canonical_preview.unmatched_events,
                &preview.unmatched_events,
            )
        {
            return Err(RetrospectiveRepositoryError::UsageSetConflict);
        }
        for unmatched in &canonical_preview.unmatched_events {
            // Fail before any estimate row is inserted when the current DB
            // vocabulary cannot retain a reason without changing its meaning.
            let _ = persisted_reason(unmatched.reason)?;
        }
        if let Some(stored) = &stored_preview {
            if catalog_freshness_from_json(&stored.filters_json, "retrospective preview filters")?
                != catalog_freshness
                || catalog_freshness_from_json(
                    &stored.projected_cost_summary_json,
                    "retrospective preview summary",
                )? != catalog_freshness
            {
                return Err(RetrospectiveRepositoryError::UsageSetConflict);
            }
        }

        let snapshot_revision_digest = snapshot.revision_digest.clone();
        let summary = preview_summary(&canonical_preview, &catalog_status, catalog_freshness, now);
        let filters_json = preview_filters_json(
            &canonical_preview,
            &snapshot_revision_digest,
            &catalog_status,
            catalog_freshness,
            now,
        );
        let eligible_count = count_i64(
            canonical_preview.eligible_event_count,
            "eligible event count",
        )?;
        let unmatched_count = count_i64(
            canonical_preview.unmatched_event_count,
            "unmatched event count",
        )?;
        let reported_count = count_i64(
            canonical_preview.already_reported_event_count,
            "already reported event count",
        )?;

        let (preview_version, preview_status, preview_created_at, expires_text) =
            if let Some(stored) = stored_preview {
                if stored.owner_user_id != owner_user_id
                    || stored.project_id != canonical_preview.project_id
                    || stored.catalog_snapshot_id != canonical_preview.snapshot_id
                    || service_catalog_freshness(stored.catalog_freshness) != catalog_freshness
                    || stored.usage_set_digest != canonical_preview.usage_set_digest
                    || stored.eligible_event_count != eligible_count
                    || stored.unmatched_event_count != unmatched_count
                    || stored.already_reported_event_count != reported_count
                    || cost_from_summary(&stored.projected_cost_summary_json)?
                        != canonical_preview.projected_cost
                {
                    return Err(RetrospectiveRepositoryError::Conflict);
                }
                if stored.status == db::CostEstimationPreviewStatus::Expired
                    || (stored.status == db::CostEstimationPreviewStatus::Active
                        && now_text >= stored.expires_at)
                {
                    return Err(RetrospectiveRepositoryError::Expired);
                }
                if stored.status == db::CostEstimationPreviewStatus::Invalidated {
                    return Err(RetrospectiveRepositoryError::Conflict);
                }
                (
                    stored.version,
                    stored.status,
                    stored.created_at,
                    stored.expires_at,
                )
            } else {
                if now >= preview.expires_at {
                    return Err(RetrospectiveRepositoryError::Expired);
                }
                let maximum_expiry = now
                    .checked_add(MAX_RETROSPECTIVE_PREVIEW_TTL)
                    .ok_or_else(|| retrospective_conversion("retrospective expiry overflow"))?;
                let expires_at = preview.expires_at.min(maximum_expiry);
                (
                    1,
                    db::CostEstimationPreviewStatus::Active,
                    now_text.clone(),
                    db_time(expires_at),
                )
            };
        canonical_preview.expires_at =
            system_time(&expires_text).map_err(retrospective_conversion)?;

        if !had_stored_preview {
            db::RetrospectiveEstimateRepo::create_cost_estimation_preview_in_tx(
                &*self.db,
                &mut transaction,
                db::CreateCostEstimationPreview {
                    id: canonical_preview.id.clone(),
                    owner_user_id: owner_user_id.clone(),
                    project_id: canonical_preview.project_id.clone(),
                    catalog_snapshot_id: canonical_preview.snapshot_id.clone(),
                    catalog_freshness: db_catalog_freshness(catalog_freshness)
                        .map_err(retrospective_conversion)?,
                    usage_set_digest: canonical_preview.usage_set_digest.clone(),
                    window_from: None,
                    window_to: None,
                    filters_json: filters_json.clone(),
                    eligible_event_count: eligible_count,
                    unmatched_event_count: unmatched_count,
                    already_reported_event_count: reported_count,
                    projected_cost_summary_json: summary.clone(),
                    idempotency_key: format!("retrospective_preview:{}", canonical_preview.id),
                    expires_at: expires_text.clone(),
                    created_at: preview_created_at.clone(),
                    updated_at: now_text.clone(),
                },
            )
            .await
            .map_err(retrospective_error)?;
        }

        let run_id = structured_digest(
            "retrospective-run-v2",
            [
                Some(canonical_preview.id.clone()),
                Some(canonical_preview.usage_set_digest.clone()),
                Some(request.idempotency_key.clone()),
            ],
        );
        let run = db::RetrospectiveEstimateRepo::create_cost_estimation_run_in_tx(
            &*self.db,
            &mut transaction,
            db::CreateCostEstimationRun {
                id: run_id.clone(),
                owner_user_id: owner_user_id.clone(),
                project_id: canonical_preview.project_id.clone(),
                preview_id: canonical_preview.id.clone(),
                catalog_snapshot_id: canonical_preview.snapshot_id.clone(),
                usage_set_digest: canonical_preview.usage_set_digest.clone(),
                status: db::CostEstimationRunStatus::Pending,
                cost_summary_json: summary.clone(),
                idempotency_key: operation_key,
                supersedes_run_id: None,
                created_at: preview_created_at,
                updated_at: now_text.clone(),
            },
        )
        .await
        .map_err(retrospective_error)?;
        if run.status == db::CostEstimationRunStatus::Committed {
            let cost = cost_from_summary(&run.cost_summary_json)?;
            let result_json = retrospective_run_result_json(&run);
            insert_operation_receipt_in_tx(
                &mut transaction,
                RETROSPECTIVE_COMMIT_OPERATION_SCOPE,
                &request.idempotency_key,
                &request_digest,
                &result_json,
                &now_text,
            )
            .await
            .map_err(retrospective_conversion)?;
            transaction.commit().await.map_err(retrospective_sqlx)?;
            return retrospective_run(run, cost);
        }

        for candidate in &canonical_preview.eligible_events {
            create_candidate_revision(
                &self.db,
                &mut transaction,
                &run,
                &owner_user_id,
                &canonical_preview,
                candidate,
                &now_text,
            )
            .await
            .map_err(retrospective_error)?;
        }
        for unmatched in &canonical_preview.unmatched_events {
            create_unmatched_revision(
                &self.db,
                &mut transaction,
                &run,
                &owner_user_id,
                &canonical_preview,
                unmatched,
                &now_text,
            )
            .await
            .map_err(retrospective_error)?;
        }

        if preview_status == db::CostEstimationPreviewStatus::Active {
            db::RetrospectiveEstimateRepo::update_cost_estimation_preview_in_tx(
                &*self.db,
                &mut transaction,
                db::UpdateCostEstimationPreview {
                    id: canonical_preview.id.clone(),
                    expected_version: preview_version,
                    status: db::CostEstimationPreviewStatus::Committed,
                    eligible_event_count: Some(eligible_count),
                    unmatched_event_count: Some(unmatched_count),
                    already_reported_event_count: Some(reported_count),
                    projected_cost_summary_json: Some(summary.clone()),
                    updated_at: now_text.clone(),
                },
            )
            .await
            .map_err(retrospective_error)?;
        }
        let committed = db::RetrospectiveEstimateRepo::update_cost_estimation_run_in_tx(
            &*self.db,
            &mut transaction,
            db::UpdateCostEstimationRun {
                id: run.id.clone(),
                expected_version: run.version,
                status: db::CostEstimationRunStatus::Committed,
                applied_event_count: Some(eligible_count),
                unmatched_event_count: Some(unmatched_count),
                already_reported_event_count: Some(reported_count),
                cost_summary_json: Some(summary),
                completed_at: Some(Some(now_text.clone())),
                updated_at: now_text.clone(),
            },
        )
        .await
        .map_err(retrospective_error)?;
        let result_json = retrospective_run_result_json(&committed);
        insert_operation_receipt_in_tx(
            &mut transaction,
            RETROSPECTIVE_COMMIT_OPERATION_SCOPE,
            &request.idempotency_key,
            &request_digest,
            &result_json,
            &now_text,
        )
        .await
        .map_err(retrospective_conversion)?;
        transaction.commit().await.map_err(retrospective_sqlx)?;
        retrospective_run(committed, canonical_preview.projected_cost)
    }
}

async fn create_candidate_revision(
    db: &db::SqliteDb,
    transaction: &mut Transaction<'_, Sqlite>,
    run: &db::CostEstimationRun,
    owner_user_id: &str,
    preview: &RetrospectivePreview,
    candidate: &RetrospectiveEstimateCandidate,
    now_text: &str,
) -> db::Result<db::CostEstimateRevision> {
    if candidate.snapshot_id != preview.snapshot_id || candidate.rate_revision_id.trim().is_empty()
    {
        return Err(db::DbError::Check(
            "retrospective candidate provenance does not match its preview".to_owned(),
        ));
    }
    let (revision, supersedes_revision_id) =
        next_estimate_revision_in_tx(transaction, &candidate.event_id).await?;
    let amount = candidate.amount.as_nano_usd();
    let estimate_digest = structured_digest(
        "retrospective-estimate-v2",
        [
            Some(run.id.clone()),
            Some(candidate.event_id.clone()),
            Some(revision.to_string()),
            Some(candidate.rate_revision_id.clone()),
            Some(candidate.snapshot_id.clone()),
            Some(amount.to_string()),
            candidate.selected_tier.clone(),
        ],
    );
    db::RetrospectiveEstimateRepo::create_cost_estimate_revision_in_tx(
        db,
        transaction,
        db::CreateCostEstimateRevision {
            id: estimate_digest.clone(),
            run_id: run.id.clone(),
            owner_user_id: owner_user_id.to_owned(),
            project_id: preview.project_id.clone(),
            usage_event_id: candidate.event_id.clone(),
            revision,
            supersedes_revision_id,
            state: db::CostEstimateRevisionState::Applied,
            rate_revision_id: Some(candidate.rate_revision_id.clone()),
            catalog_snapshot_id: Some(candidate.snapshot_id.clone()),
            estimated_nano_usd: Some(amount),
            formula_revision: Some(pricing::COST_FORMULA_REVISION.to_owned()),
            reason_code: None,
            estimate_digest,
            created_at: now_text.to_owned(),
        },
    )
    .await
}

async fn create_unmatched_revision(
    db: &db::SqliteDb,
    transaction: &mut Transaction<'_, Sqlite>,
    run: &db::CostEstimationRun,
    owner_user_id: &str,
    preview: &RetrospectivePreview,
    unmatched: &RetrospectiveUnmatchedEvent,
    now_text: &str,
) -> db::Result<db::CostEstimateRevision> {
    let reason = persisted_reason(unmatched.reason)
        .map_err(|error| db::DbError::Check(error.to_string()))?;
    let (revision, supersedes_revision_id) =
        next_estimate_revision_in_tx(transaction, &unmatched.event_id).await?;
    let estimate_digest = structured_digest(
        "retrospective-unmatched-v2",
        [
            Some(run.id.clone()),
            Some(unmatched.event_id.clone()),
            Some(revision.to_string()),
            Some(reason.to_string()),
        ],
    );
    db::RetrospectiveEstimateRepo::create_cost_estimate_revision_in_tx(
        db,
        transaction,
        db::CreateCostEstimateRevision {
            id: estimate_digest.clone(),
            run_id: run.id.clone(),
            owner_user_id: owner_user_id.to_owned(),
            project_id: preview.project_id.clone(),
            usage_event_id: unmatched.event_id.clone(),
            revision,
            supersedes_revision_id,
            state: db::CostEstimateRevisionState::Unmatched,
            rate_revision_id: None,
            catalog_snapshot_id: None,
            estimated_nano_usd: None,
            formula_revision: None,
            reason_code: Some(reason),
            estimate_digest,
            created_at: now_text.to_owned(),
        },
    )
    .await
}
