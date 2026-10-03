use super::*;
use db::RetrospectiveEstimateRepo;

/// Overlay the latest applied retrospective estimate without mutating the
/// immutable usage event. The broad analytics repository performs this join
/// in its dataset query; these smaller public projections use the repository
/// list methods and apply the same effective-cost rule per event.
pub(super) async fn list_effective_usage_events(
    db: &db::SqliteDb,
    invocation: &UsageInvocation,
) -> Result<Vec<EffectiveUsageEvent>> {
    let events = UsageLedgerRepo::list_usage_events_for_invocation(db, &invocation.id).await?;
    // Batched per invocation: an agent's lifetime aggregate walks every event
    // it ever produced, so per-event queries made list endpoints linear in
    // usage history.
    let mut applied_by_event = HashMap::new();
    for revision in
        RetrospectiveEstimateRepo::list_cost_estimate_revisions_for_invocation(db, &invocation.id)
            .await?
    {
        if revision.state == CostEstimateRevisionState::Applied {
            applied_by_event
                .entry(revision.usage_event_id.clone())
                .or_insert(revision);
        }
    }
    let mut metadata_cache = HashMap::new();
    let mut effective = Vec::with_capacity(events.len());
    for event in events {
        if let Some(revision) = applied_by_event.remove(&event.id) {
            if event.cost_kind == UsageCostKind::ProviderReported {
                return Err(invalid_transition());
            }
            let Some(estimated_nano_usd) = revision.estimated_nano_usd else {
                return Err(invalid_transition());
            };
            let mut event = event;
            event.estimated_nano_usd = Some(estimated_nano_usd);
            event.cost_kind = UsageCostKind::Estimated;
            event.rate_revision_id = revision.rate_revision_id.or(event.rate_revision_id);
            event.catalog_snapshot_id = revision.catalog_snapshot_id.or(event.catalog_snapshot_id);
            event.formula_revision = revision.formula_revision.or(event.formula_revision);
            event.retrospective = revision.retrospective;
            let source_metadata = cached_event_source_metadata(
                &mut metadata_cache,
                db,
                invocation,
                &event,
                Some(&revision.id),
            )
            .await?;
            effective.push(EffectiveUsageEvent {
                source: event_source_with_metadata(&event, Some(&source_metadata))?,
                event,
            });
        } else {
            let source_metadata =
                cached_event_source_metadata(&mut metadata_cache, db, invocation, &event, None)
                    .await?;
            effective.push(EffectiveUsageEvent {
                source: event_source_with_metadata(&event, Some(&source_metadata))?,
                event,
            });
        }
    }
    Ok(effective)
}

/// The metadata join depends only on the invocation plus these three ids, and
/// an invocation's events almost always share them.
type EventSourceMetadataKey = (Option<String>, Option<String>, Option<String>);

async fn cached_event_source_metadata(
    cache: &mut HashMap<EventSourceMetadataKey, EventSourceMetadata>,
    db: &db::SqliteDb,
    invocation: &UsageInvocation,
    event: &UsageEvent,
    applied_revision_id: Option<&str>,
) -> Result<EventSourceMetadata> {
    let key = (
        event.rate_revision_id.clone(),
        event.catalog_snapshot_id.clone(),
        applied_revision_id.map(str::to_owned),
    );
    if let Some(metadata) = cache.get(&key) {
        return Ok(metadata.clone());
    }
    let metadata = load_event_source_metadata(db, invocation, event, applied_revision_id).await?;
    cache.insert(key, metadata.clone());
    Ok(metadata)
}

/// Read the immutable pricing provenance attached to an invocation/event.
/// Selection freshness is admission-time truth; retrospective estimates use
/// the immutable preview freshness instead. Rate/snapshot fields are joined by
/// their frozen IDs and never re-resolved from current mutable bindings.
async fn load_event_source_metadata(
    db: &db::SqliteDb,
    invocation: &UsageInvocation,
    event: &UsageEvent,
    applied_revision_id: Option<&str>,
) -> Result<EventSourceMetadata> {
    let row = sqlx::query(
        "SELECT
             ps.source_kind AS selection_source_kind,
             ps.catalog_freshness AS selection_catalog_freshness,
             ps.rate_revision_id AS selection_rate_revision_id,
             ps.catalog_snapshot_id AS selection_catalog_snapshot_id,
             r.source_kind AS rate_source_kind,
             r.catalog_snapshot_id AS rate_catalog_snapshot_id,
             r.effective_at AS rate_effective_at,
             c.revision_digest AS catalog_digest,
             c.fetched_at AS catalog_fetched_at,
             ep.catalog_freshness AS estimate_catalog_freshness
         FROM usage_invocation i
         LEFT JOIN pricing_selection ps ON ps.id = i.pricing_selection_id
         LEFT JOIN cost_estimate_revision er ON er.id = ?
         LEFT JOIN cost_estimation_run erun ON erun.id = er.run_id
         LEFT JOIN cost_estimation_preview ep ON ep.id = erun.preview_id
         LEFT JOIN pricing_rate_revision r
           ON r.id = COALESCE(?, ps.rate_revision_id)
         LEFT JOIN pricing_catalog_snapshot c
           ON c.id = COALESCE(?, r.catalog_snapshot_id, ps.catalog_snapshot_id)
         WHERE i.id = ?",
    )
    .bind(applied_revision_id)
    .bind(event.rate_revision_id.as_deref())
    .bind(event.catalog_snapshot_id.as_deref())
    .bind(&invocation.id)
    .fetch_one(db.pool())
    .await?;

    event_source_metadata_from_row(&row, event)
}

pub(super) async fn old_usage_aggregate_for_task(
    db: &db::SqliteDb,
    task_id: &str,
) -> Result<UsageAggregate> {
    let execution_rows = sqlx::query("SELECT id, status FROM execution WHERE task_id = ?")
        .bind(task_id)
        .fetch_all(db.pool())
        .await?;
    let mut execution_ids = BTreeSet::new();
    let mut domain_runs = Vec::with_capacity(execution_rows.len());
    for row in execution_rows {
        let id: String = sqlx::Row::try_get(&row, "id")?;
        let status: String = sqlx::Row::try_get(&row, "status")?;
        execution_ids.insert(id.clone());
        domain_runs.push(UsageDomainRun {
            surface: DbUsageSurface::TaskExecution,
            source_id: id,
            pending: status == "running",
        });
    }

    let source_rows = sqlx::query(
        "SELECT DISTINCT source_id FROM usage_invocation
         WHERE task_id = ?
            OR execution_id IN (SELECT id FROM execution WHERE task_id = ?)
            OR source_id IN (SELECT id FROM execution WHERE task_id = ?)
         UNION SELECT DISTINCT source_id FROM usage_event
         WHERE task_id = ?
            OR execution_id IN (SELECT id FROM execution WHERE task_id = ?)
            OR source_id IN (SELECT id FROM execution WHERE task_id = ?)
         ORDER BY source_id ASC",
    )
    .bind(task_id)
    .bind(task_id)
    .bind(task_id)
    .bind(task_id)
    .bind(task_id)
    .bind(task_id)
    .fetch_all(db.pool())
    .await?;
    let mut invocations = Vec::new();
    let mut events_by_invocation = HashMap::new();
    for row in source_rows {
        let source_id: String = sqlx::Row::try_get(&row, "source_id")?;
        for invocation in UsageLedgerRepo::list_usage_invocations_for_source(db, &source_id).await?
        {
            let invocation_matches = invocation.task_id.as_deref() == Some(task_id)
                || execution_ids.contains(&invocation.source_id);
            let events = list_effective_usage_events(db, &invocation).await?;
            let events = if invocation_matches {
                events
            } else {
                events
                    .into_iter()
                    .filter(|effective| {
                        effective.event.task_id.as_deref() == Some(task_id)
                            || execution_ids.contains(&effective.event.source_id)
                    })
                    .collect()
            };
            if !invocation_matches && events.is_empty() {
                continue;
            }
            events_by_invocation.insert(invocation.id.clone(), events);
            invocations.push(invocation);
        }
    }
    aggregate_usage_with_sources(&invocations, &events_by_invocation, &domain_runs)
}

/// The per-invocation walk the Operations summary used before it was batched.
pub(crate) async fn per_invocation_usage_aggregate_for_operations(
    db: &db::SqliteDb,
) -> Result<UsageAggregate> {
    let source_rows = sqlx::query(
        "SELECT DISTINCT source_id FROM usage_invocation
         UNION SELECT DISTINCT source_id FROM usage_event
         ORDER BY source_id ASC",
    )
    .fetch_all(db.pool())
    .await?;
    let mut invocations = Vec::new();
    let mut events_by_invocation = HashMap::new();
    for row in source_rows {
        let source_id: String = sqlx::Row::try_get(&row, "source_id")?;
        for invocation in UsageLedgerRepo::list_usage_invocations_for_source(db, &source_id).await?
        {
            let events = list_effective_usage_events(db, &invocation).await?;
            events_by_invocation.insert(invocation.id.clone(), events);
            invocations.push(invocation);
        }
    }
    let mut domain_runs = invocations
        .iter()
        .map(|invocation| UsageDomainRun {
            surface: invocation.surface,
            source_id: invocation.source_id.clone(),
            pending: false,
        })
        .collect::<Vec<_>>();
    let execution_rows = sqlx::query("SELECT id, status FROM execution")
        .fetch_all(db.pool())
        .await?;
    for row in execution_rows {
        let source_id: String = sqlx::Row::try_get(&row, "id")?;
        let status: String = sqlx::Row::try_get(&row, "status")?;
        domain_runs.push(UsageDomainRun {
            surface: DbUsageSurface::TaskExecution,
            source_id,
            pending: status == "running",
        });
    }
    aggregate_usage_with_sources(&invocations, &events_by_invocation, &domain_runs)
}
