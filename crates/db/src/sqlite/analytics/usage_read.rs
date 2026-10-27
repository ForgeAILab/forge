//! Ledger usage datasets, Genesis attribution, and analytics repository reads.

use super::*;

/// Resolve Genesis-to-Project attribution only through the immutable handoff
/// packet and its delivered receipt.  In particular, do not use a mutable
/// session `updated_at` (or a current Project relation) to classify old Main
/// Chat responses. Source message aliases are read from the immutable handoff
/// packet rather than the session's mutable working list. The query also
/// supplies no-provider domain runs, whose ledger invocation is legitimately
/// absent because no provider call was admitted.
async fn fetch_genesis_attributions(
    db: &SqliteDb,
    owner_user_id: Option<&str>,
    project_id: Option<&str>,
) -> Result<Vec<GenesisAttribution>> {
    let (sql, bind) = if let Some(project_id) = project_id {
        (
            "SELECT m.id AS message_id, m.source_id, m.source_message_id,
                    h.source_turn_job_id, g.project_id, m.created_at,
                    delivery.created_at AS delivery_created_at
             FROM agent_chat_message m
             JOIN agent_chat source_chat ON source_chat.id = m.chat_id
             JOIN product_genesis_session g
               ON g.main_chat_id = source_chat.id
              AND g.lifecycle = 'handed_off'
              AND g.project_id = ?1
              AND g.handoff_id IS NOT NULL
             JOIN project p
               ON p.id = g.project_id
              AND p.owner_id = source_chat.account_id
             JOIN project_admission_receipt receipt
               ON receipt.project_id = g.project_id
              AND receipt.source_kind = 'genesis_handoff'
              AND receipt.handoff_id = g.handoff_id
             JOIN agent_handoff h
               ON h.id = g.handoff_id
              AND h.source_chat_id = source_chat.id
              AND h.status = 'delivered'
             JOIN agent_chat target_chat
               ON target_chat.id = h.target_chat_id
              AND target_chat.kind = 'project'
              AND target_chat.project_id = g.project_id
             JOIN agent_handoff_delivery delivery
               ON delivery.handoff_id = h.id
              AND delivery.delivery_sequence = 1
              AND delivery.status = 'delivered'
              AND delivery.target_message_id IS h.target_message_id
              AND delivery.target_turn_job_id IS h.target_turn_job_id
             WHERE m.author_type = 'agent'
               AND m.status IN ('complete', 'failed', 'cancelled')
               AND m.source_type != 'handoff'
               AND json_valid(h.source_revisions_json)
               AND json_extract(
                       h.source_revisions_json,
                       '$.request.source_revisions_digest'
                   ) = receipt.payload_digest
               -- Keep the SQL prefilter conservative; Rust below compares
               -- parsed instants exactly, avoiding Julian-day precision loss.
               AND julianday(m.created_at) <= julianday(delivery.created_at)
                    + 1.0 / 86400.0
               AND (
                   (
                       h.source_turn_job_id IS NOT NULL
                       AND m.source_id = h.source_turn_job_id
                   )
                   OR EXISTS (
                       SELECT 1
                       FROM agent_chat_turn_job source_turn
                       WHERE source_turn.id = h.source_turn_job_id
                         AND source_turn.chat_id = source_chat.id
                         AND source_turn.response_message_id = m.id
                   )
                   OR EXISTS (
                       SELECT 1
                       FROM json_each(
                           json_extract(h.source_revisions_json, '$.source.message_ids')
                       ) source_message
                       WHERE source_message.value = m.id
                          OR source_message.value = m.source_message_id
                   )
               )
             ORDER BY m.created_at ASC, m.id ASC",
            project_id,
        )
    } else {
        let owner_user_id = owner_user_id.ok_or(DbError::InvalidTransition)?;
        (
            "SELECT m.id AS message_id, m.source_id, m.source_message_id,
                    h.source_turn_job_id, g.project_id, m.created_at,
                    delivery.created_at AS delivery_created_at
             FROM agent_chat_message m
             JOIN agent_chat source_chat ON source_chat.id = m.chat_id
             JOIN product_genesis_session g
               ON g.main_chat_id = source_chat.id
              AND g.lifecycle = 'handed_off'
              AND g.project_id IS NOT NULL
              AND g.handoff_id IS NOT NULL
             JOIN project p
               ON p.id = g.project_id
              AND p.owner_id = source_chat.account_id
             JOIN agent_handoff h
               ON h.id = g.handoff_id
              AND h.source_chat_id = source_chat.id
              AND h.status = 'delivered'
             JOIN project_admission_receipt receipt
               ON receipt.project_id = g.project_id
              AND receipt.source_kind = 'genesis_handoff'
              AND receipt.handoff_id = g.handoff_id
             JOIN agent_chat target_chat
               ON target_chat.id = h.target_chat_id
              AND target_chat.kind = 'project'
              AND target_chat.project_id = g.project_id
             JOIN agent_handoff_delivery delivery
               ON delivery.handoff_id = h.id
              AND delivery.delivery_sequence = 1
              AND delivery.status = 'delivered'
              AND delivery.target_message_id IS h.target_message_id
              AND delivery.target_turn_job_id IS h.target_turn_job_id
             WHERE p.owner_id = ?1
               AND m.author_type = 'agent'
               AND m.status IN ('complete', 'failed', 'cancelled')
               AND m.source_type != 'handoff'
               AND json_valid(h.source_revisions_json)
               AND json_extract(
                       h.source_revisions_json,
                       '$.request.source_revisions_digest'
                   ) = receipt.payload_digest
               -- Keep the SQL prefilter conservative; Rust below compares
               -- parsed instants exactly, avoiding Julian-day precision loss.
               AND julianday(m.created_at) <= julianday(delivery.created_at)
                    + 1.0 / 86400.0
               AND (
                   (
                       h.source_turn_job_id IS NOT NULL
                       AND m.source_id = h.source_turn_job_id
                   )
                   OR EXISTS (
                       SELECT 1
                       FROM agent_chat_turn_job source_turn
                       WHERE source_turn.id = h.source_turn_job_id
                         AND source_turn.chat_id = source_chat.id
                         AND source_turn.response_message_id = m.id
                   )
                   OR EXISTS (
                       SELECT 1
                       FROM json_each(
                           json_extract(h.source_revisions_json, '$.source.message_ids')
                       ) source_message
                       WHERE source_message.value = m.id
                          OR source_message.value = m.source_message_id
                   )
               )
             ORDER BY m.created_at ASC, m.id ASC",
            owner_user_id,
        )
    };
    let rows = sqlx::query(sql).bind(bind).fetch_all(db.pool()).await?;
    rows.into_iter()
        .map(|row| -> Result<Option<GenesisAttribution>> {
            let occurred_at: String = row.try_get("created_at")?;
            let delivery_created_at: String = row.try_get("delivery_created_at")?;
            let Some(message_at) = parse_timestamp(&occurred_at) else {
                return Ok(None);
            };
            let Some(delivery_at) = parse_timestamp(&delivery_created_at) else {
                return Ok(None);
            };
            if message_at > delivery_at {
                return Ok(None);
            }
            Ok(Some(GenesisAttribution {
                message_id: row.try_get("message_id")?,
                source_id: row.try_get("source_id")?,
                source_message_id: row.try_get("source_message_id")?,
                source_turn_job_id: row.try_get("source_turn_job_id")?,
                project_id: row.try_get("project_id")?,
                occurred_at,
            }))
        })
        .collect::<Result<Vec<_>>>()
        .map(|rows| rows.into_iter().flatten().collect())
}

/// Load immutable handoff boundaries even when the source turn has no
/// assistant message. Genesis control transfer intentionally completes the
/// source turn without writing a response, so an analytics join rooted only
/// at `agent_chat_message` would lose both its Project attribution and its
/// domain-run count. Source message references come from the immutable
/// handoff packet, not the mutable Genesis session working list.
async fn fetch_genesis_boundaries(
    db: &SqliteDb,
    owner_user_id: Option<&str>,
    project_id: Option<&str>,
) -> Result<Vec<GenesisBoundary>> {
    let (sql, bind) = if let Some(project_id) = project_id {
        (
            "SELECT g.project_id, h.source_turn_job_id,
                    delivery.created_at AS source_turn_occurred_at,
                    json_extract(h.source_revisions_json, '$.source.message_ids')
                        AS source_message_ids_json
             FROM product_genesis_session g
             JOIN agent_chat source_chat
               ON source_chat.id = g.main_chat_id
              AND source_chat.kind = 'account_main'
             JOIN project p
               ON p.id = g.project_id
              AND p.owner_id = source_chat.account_id
             JOIN project_admission_receipt receipt
               ON receipt.project_id = g.project_id
              AND receipt.source_kind = 'genesis_handoff'
              AND receipt.handoff_id = g.handoff_id
             JOIN agent_handoff h
               ON h.id = g.handoff_id
              AND h.source_chat_id = source_chat.id
              AND h.status = 'delivered'
             JOIN agent_chat target_chat
               ON target_chat.id = h.target_chat_id
              AND target_chat.kind = 'project'
              AND target_chat.project_id = g.project_id
             JOIN agent_handoff_delivery delivery
               ON delivery.handoff_id = h.id
              AND delivery.delivery_sequence = 1
              AND delivery.status = 'delivered'
              AND delivery.target_message_id IS h.target_message_id
              AND delivery.target_turn_job_id IS h.target_turn_job_id
             WHERE g.lifecycle = 'handed_off'
               AND g.project_id = ?1
               AND json_valid(h.source_revisions_json)
               AND json_extract(
                       h.source_revisions_json,
                       '$.request.source_revisions_digest'
                   ) = receipt.payload_digest
             ORDER BY delivery.created_at ASC, g.id ASC",
            project_id,
        )
    } else {
        let owner_user_id = owner_user_id.ok_or(DbError::InvalidTransition)?;
        (
            "SELECT g.project_id, h.source_turn_job_id,
                    delivery.created_at AS source_turn_occurred_at,
                    json_extract(h.source_revisions_json, '$.source.message_ids')
                        AS source_message_ids_json
             FROM product_genesis_session g
             JOIN agent_chat source_chat
               ON source_chat.id = g.main_chat_id
              AND source_chat.kind = 'account_main'
             JOIN project p
               ON p.id = g.project_id
              AND p.owner_id = source_chat.account_id
             JOIN agent_handoff h
               ON h.id = g.handoff_id
              AND h.source_chat_id = source_chat.id
              AND h.status = 'delivered'
             JOIN project_admission_receipt receipt
               ON receipt.project_id = g.project_id
              AND receipt.source_kind = 'genesis_handoff'
              AND receipt.handoff_id = g.handoff_id
             JOIN agent_chat target_chat
               ON target_chat.id = h.target_chat_id
              AND target_chat.kind = 'project'
              AND target_chat.project_id = g.project_id
             JOIN agent_handoff_delivery delivery
               ON delivery.handoff_id = h.id
              AND delivery.delivery_sequence = 1
              AND delivery.status = 'delivered'
              AND delivery.target_message_id IS h.target_message_id
              AND delivery.target_turn_job_id IS h.target_turn_job_id
             WHERE g.lifecycle = 'handed_off'
               AND p.owner_id = ?1
               AND json_valid(h.source_revisions_json)
               AND json_extract(
                       h.source_revisions_json,
                       '$.request.source_revisions_digest'
                   ) = receipt.payload_digest
             ORDER BY delivery.created_at ASC, g.id ASC",
            owner_user_id,
        )
    };
    let rows = sqlx::query(sql).bind(bind).fetch_all(db.pool()).await?;
    rows.into_iter()
        .map(|row| {
            let source_message_ids_json: Option<String> = row.try_get("source_message_ids_json")?;
            let source_message_ids = source_message_ids_json
                .map(|json| serde_json::from_str::<Vec<String>>(&json))
                .transpose()
                .map_err(|_| DbError::InvalidTransition)?
                .unwrap_or_default();
            Ok(GenesisBoundary {
                project_id: row.try_get("project_id")?,
                source_turn_job_id: row.try_get("source_turn_job_id")?,
                source_turn_occurred_at: row.try_get("source_turn_occurred_at")?,
                source_message_ids,
            })
        })
        .collect()
}

async fn fetch_ledger_invocations(
    db: &SqliteDb,
    owner_user_id: Option<&str>,
    project_id: Option<&str>,
    from: Option<&str>,
    to: Option<&str>,
    genesis_source_ids_json: Option<&str>,
) -> Result<Vec<LedgerInvocationRow>> {
    let (sql, bind) = if let Some(project_id) = project_id {
        (
            "SELECT id, owner_user_id, project_id, surface,
                    inv.source_id AS source_id,
                    candidate_key, attempt_ordinal, admitted_provider_id,
                    admitted_model_id, agent_id, profile_id,
                    agent_name_snapshot, project_name_snapshot, executor_type,
                    lifecycle, telemetry_state, admitted_at, settled_at
             FROM usage_invocation inv
             WHERE inv.surface IN ('task_execution', 'project_chat', 'genesis_chat')
               AND (inv.project_id = ?1
                    OR (inv.surface = 'genesis_chat'
                        AND inv.project_id IS NULL
                        AND inv.owner_user_id = (
                            SELECT owner_id FROM project WHERE id = ?1
                        )
                        AND EXISTS (
                            SELECT 1
                            FROM json_each(?4) genesis_source
                            WHERE genesis_source.value = inv.source_id
                        )))
               AND (
                    EXISTS (
                        SELECT 1 FROM usage_event e
                        WHERE e.invocation_id = inv.id
                          AND (?2 IS NULL OR julianday(e.occurred_at) >= julianday(?2) - 1.0 / 86400.0)
                          AND (?3 IS NULL OR julianday(e.occurred_at) < julianday(?3) + 1.0 / 86400.0)
                    )
                    OR (
                        NOT EXISTS (
                            SELECT 1 FROM usage_event e
                            WHERE e.invocation_id = inv.id
                        )
                        AND (?2 IS NULL OR julianday(COALESCE(inv.settled_at, inv.admitted_at)) >= julianday(?2) - 1.0 / 86400.0)
                        AND (?3 IS NULL OR julianday(COALESCE(inv.settled_at, inv.admitted_at)) < julianday(?3) + 1.0 / 86400.0)
                    )
               )
             ORDER BY inv.id ASC",
            project_id,
        )
    } else {
        let owner_user_id = owner_user_id.ok_or(DbError::InvalidTransition)?;
        (
            "SELECT id, owner_user_id, project_id, surface,
                    inv.source_id AS source_id,
                    candidate_key, attempt_ordinal, admitted_provider_id,
                    admitted_model_id, agent_id, profile_id,
                    agent_name_snapshot, project_name_snapshot, executor_type,
                    lifecycle, telemetry_state, admitted_at, settled_at
             FROM usage_invocation inv
             WHERE inv.owner_user_id = ?
               AND (
                    EXISTS (
                        SELECT 1 FROM usage_event e
                        WHERE e.invocation_id = inv.id
                          AND (?2 IS NULL OR julianday(e.occurred_at) >= julianday(?2) - 1.0 / 86400.0)
                          AND (?3 IS NULL OR julianday(e.occurred_at) < julianday(?3) + 1.0 / 86400.0)
                    )
                    OR (
                        NOT EXISTS (
                            SELECT 1 FROM usage_event e
                            WHERE e.invocation_id = inv.id
                        )
                        AND (?2 IS NULL OR julianday(COALESCE(inv.settled_at, inv.admitted_at)) >= julianday(?2) - 1.0 / 86400.0)
                        AND (?3 IS NULL OR julianday(COALESCE(inv.settled_at, inv.admitted_at)) < julianday(?3) + 1.0 / 86400.0)
                    )
               )
             ORDER BY inv.id ASC",
            owner_user_id,
        )
    };
    let rows = if project_id.is_some() {
        sqlx::query(sql)
            .bind(bind)
            .bind(from)
            .bind(to)
            .bind(genesis_source_ids_json.unwrap_or("[]"))
            .fetch_all(db.pool())
            .await?
    } else {
        sqlx::query(sql)
            .bind(bind)
            .bind(from)
            .bind(to)
            .fetch_all(db.pool())
            .await?
    };
    let mut invocations: Vec<LedgerInvocationRow> = rows
        .into_iter()
        .map(|row| {
            Ok(LedgerInvocationRow {
                id: row.try_get("id")?,
                project_id: row.try_get("project_id")?,
                surface: parse_db_enum(row.try_get("surface")?)?,
                source_id: row.try_get("source_id")?,
                admitted_provider_id: row.try_get("admitted_provider_id")?,
                admitted_model_id: row.try_get("admitted_model_id")?,
                agent_id: row.try_get("agent_id")?,
                profile_id: row.try_get("profile_id")?,
                agent_name_snapshot: row.try_get("agent_name_snapshot")?,
                project_name_snapshot: row.try_get("project_name_snapshot")?,
                executor_type: row.try_get("executor_type")?,
                lifecycle: parse_db_enum(row.try_get("lifecycle")?)?,
                telemetry_state: parse_db_enum(row.try_get("telemetry_state")?)?,
                admitted_at: row.try_get("admitted_at")?,
                settled_at: row.try_get("settled_at")?,
            })
        })
        .collect::<Result<_>>()?;

    // Chat producers historically used either the response message id or its
    // durable turn/source id as the ledger source.  Resolve those aliases in
    // one bounded query so a logical turn is counted once, while avoiding a
    // mutable chat/session join in the accounting projection.
    let source_ids: BTreeSet<String> = invocations
        .iter()
        .filter(|invocation| {
            matches!(
                invocation.surface,
                UsageSurface::ProjectChat | UsageSurface::MainChat | UsageSurface::GenesisChat
            )
        })
        .map(|invocation| invocation.source_id.clone())
        .collect();
    if !source_ids.is_empty() {
        let source_ids_json =
            serde_json::to_string(&source_ids).map_err(|_| DbError::InvalidTransition)?;
        let aliases = sqlx::query(
            "SELECT source.value AS raw_source_id, m.id AS message_id
             FROM json_each(?1) source
             JOIN agent_chat_message m
               ON m.author_type = 'agent'
              AND (m.id = source.value OR m.source_id = source.value)
             ORDER BY source.value ASC,
                      CASE WHEN m.id = source.value THEN 0 ELSE 1 END,
                      m.created_at DESC, m.id ASC",
        )
        .bind(source_ids_json)
        .fetch_all(db.pool())
        .await?;
        let mut canonical_ids = HashMap::<String, String>::new();
        for alias in aliases {
            let raw_source_id: String = alias.try_get("raw_source_id")?;
            canonical_ids
                .entry(raw_source_id)
                .or_insert(alias.try_get("message_id")?);
        }
        for invocation in &mut invocations {
            if let Some(message_id) = canonical_ids.get(&invocation.source_id) {
                invocation.source_id.clone_from(message_id);
            }
        }
    }
    Ok(invocations)
}

async fn fetch_ledger_events(
    db: &SqliteDb,
    owner_user_id: Option<&str>,
    project_id: Option<&str>,
    from: Option<&str>,
    to: Option<&str>,
    genesis_source_ids_json: Option<&str>,
) -> Result<Vec<LedgerEventRow>> {
    let (sql, bind) = if let Some(project_id) = project_id {
        (
            "SELECT e.id, e.invocation_id, e.project_id, e.surface, e.source_id,
                    e.provider_id, e.model_id, e.runtime_model, e.candidate_key,
                    e.attempt_ordinal, e.agent_id, e.profile_id,
                    e.agent_name_snapshot, e.project_name_snapshot,
                    e.executor_type, e.telemetry_state, e.input_tokens,
                    e.output_tokens, e.cache_read_tokens, e.cache_write_tokens,
                    e.context_tokens, e.selected_tier,
                    e.provider_reported_nano_usd,
                    e.legacy_reported_cost_usd,
                    e.legacy_cost_usd_raw,
                    CAST(e.legacy_reported_cost_usd AS TEXT) AS legacy_reported_cost_text,
                    COALESCE(er.estimated_nano_usd, e.estimated_nano_usd) AS estimated_nano_usd,
                    CASE WHEN er.id IS NOT NULL THEN 'estimated' ELSE e.cost_kind END AS cost_kind,
                    e.provenance_kind,
                    COALESCE(er.rate_revision_id, e.rate_revision_id) AS rate_revision_id,
                    COALESCE(er.catalog_snapshot_id, e.catalog_snapshot_id) AS catalog_snapshot_id,
                    COALESCE(er.formula_revision, e.formula_revision) AS formula_revision,
                    CASE WHEN er.id IS NOT NULL THEN 1 ELSE e.retrospective END AS retrospective,
                    e.coverage_reason_code, e.occurred_at,
                    r.source_kind AS rate_source_kind, r.effective_at AS rate_effective_at,
                    r.input_nano_usd_per_million AS rate_input,
                    r.output_nano_usd_per_million AS rate_output,
                    r.cache_read_nano_usd_per_million AS rate_cache_read,
                    r.cache_write_nano_usd_per_million AS rate_cache_write,
                    c.revision_digest AS catalog_digest,
                    c.fetched_at AS catalog_fetched_at,
                    CASE WHEN er.id IS NOT NULL
                         THEN ep.catalog_freshness
                         ELSE ps.catalog_freshness
                    END AS catalog_freshness
             FROM usage_event e
             JOIN usage_invocation i ON i.id = e.invocation_id
            LEFT JOIN cost_estimate_revision er
               ON er.usage_event_id = e.id
              AND e.provider_reported_nano_usd IS NULL
              AND e.legacy_reported_cost_usd IS NULL
              AND er.state = 'applied'
              AND er.revision = (
                  SELECT MAX(latest.revision)
                  FROM cost_estimate_revision latest
                  WHERE latest.usage_event_id = e.id
                    AND latest.state = 'applied'
              )
             LEFT JOIN cost_estimation_run erun ON erun.id = er.run_id
             LEFT JOIN cost_estimation_preview ep ON ep.id = erun.preview_id
             LEFT JOIN pricing_selection ps ON ps.id = i.pricing_selection_id
             LEFT JOIN pricing_rate_revision r
               ON r.id = COALESCE(er.rate_revision_id, e.rate_revision_id)
             LEFT JOIN pricing_catalog_snapshot c
               ON c.id = COALESCE(er.catalog_snapshot_id, e.catalog_snapshot_id, r.catalog_snapshot_id)
             WHERE e.surface IN ('task_execution', 'project_chat', 'genesis_chat')
               AND (e.project_id = ?1
                OR (e.surface = 'genesis_chat'
                    AND e.project_id IS NULL
                    AND i.project_id IS NULL
                    AND i.owner_user_id = (
                        SELECT owner_id FROM project WHERE id = ?1
                    )
                    AND EXISTS (
                        SELECT 1
                        FROM json_each(?4) genesis_source
                        WHERE genesis_source.value = i.source_id
                    )))
               AND (?2 IS NULL OR julianday(e.occurred_at) >= julianday(?2) - 1.0 / 86400.0)
               AND (?3 IS NULL OR julianday(e.occurred_at) < julianday(?3) + 1.0 / 86400.0)
             ORDER BY e.invocation_id ASC, e.occurred_at ASC, e.id ASC",
            project_id,
        )
    } else {
        let owner_user_id = owner_user_id.ok_or(DbError::InvalidTransition)?;
        (
            "SELECT e.id, e.invocation_id, e.project_id, e.surface, e.source_id,
                    e.provider_id, e.model_id, e.runtime_model, e.candidate_key,
                    e.attempt_ordinal, e.agent_id, e.profile_id,
                    e.agent_name_snapshot, e.project_name_snapshot,
                    e.executor_type, e.telemetry_state, e.input_tokens,
                    e.output_tokens, e.cache_read_tokens, e.cache_write_tokens,
                    e.context_tokens, e.selected_tier,
                    e.provider_reported_nano_usd,
                    e.legacy_reported_cost_usd,
                    e.legacy_cost_usd_raw,
                    CAST(e.legacy_reported_cost_usd AS TEXT) AS legacy_reported_cost_text,
                    COALESCE(er.estimated_nano_usd, e.estimated_nano_usd) AS estimated_nano_usd,
                    CASE WHEN er.id IS NOT NULL THEN 'estimated' ELSE e.cost_kind END AS cost_kind,
                    e.provenance_kind,
                    COALESCE(er.rate_revision_id, e.rate_revision_id) AS rate_revision_id,
                    COALESCE(er.catalog_snapshot_id, e.catalog_snapshot_id) AS catalog_snapshot_id,
                    COALESCE(er.formula_revision, e.formula_revision) AS formula_revision,
                    CASE WHEN er.id IS NOT NULL THEN 1 ELSE e.retrospective END AS retrospective,
                    e.coverage_reason_code, e.occurred_at,
                    r.source_kind AS rate_source_kind, r.effective_at AS rate_effective_at,
                    r.input_nano_usd_per_million AS rate_input,
                    r.output_nano_usd_per_million AS rate_output,
                    r.cache_read_nano_usd_per_million AS rate_cache_read,
                    r.cache_write_nano_usd_per_million AS rate_cache_write,
                    c.revision_digest AS catalog_digest,
                    c.fetched_at AS catalog_fetched_at,
                    CASE WHEN er.id IS NOT NULL
                         THEN ep.catalog_freshness
                         ELSE ps.catalog_freshness
                    END AS catalog_freshness
             FROM usage_event e
             JOIN usage_invocation i ON i.id = e.invocation_id
            LEFT JOIN cost_estimate_revision er
               ON er.usage_event_id = e.id
              AND e.provider_reported_nano_usd IS NULL
              AND e.legacy_reported_cost_usd IS NULL
              AND er.state = 'applied'
              AND er.revision = (
                  SELECT MAX(latest.revision)
                  FROM cost_estimate_revision latest
                  WHERE latest.usage_event_id = e.id
                    AND latest.state = 'applied'
              )
             LEFT JOIN cost_estimation_run erun ON erun.id = er.run_id
             LEFT JOIN cost_estimation_preview ep ON ep.id = erun.preview_id
             LEFT JOIN pricing_selection ps ON ps.id = i.pricing_selection_id
             LEFT JOIN pricing_rate_revision r
               ON r.id = COALESCE(er.rate_revision_id, e.rate_revision_id)
             LEFT JOIN pricing_catalog_snapshot c
               ON c.id = COALESCE(er.catalog_snapshot_id, e.catalog_snapshot_id, r.catalog_snapshot_id)
             WHERE i.owner_user_id = ?
               AND (?2 IS NULL OR julianday(e.occurred_at) >= julianday(?2) - 1.0 / 86400.0)
               AND (?3 IS NULL OR julianday(e.occurred_at) < julianday(?3) + 1.0 / 86400.0)
             ORDER BY e.invocation_id ASC, e.occurred_at ASC, e.id ASC",
            owner_user_id,
        )
    };
    let rows = if project_id.is_some() {
        sqlx::query(sql)
            .bind(bind)
            .bind(from)
            .bind(to)
            .bind(genesis_source_ids_json.unwrap_or("[]"))
            .fetch_all(db.pool())
            .await?
    } else {
        sqlx::query(sql)
            .bind(bind)
            .bind(from)
            .bind(to)
            .fetch_all(db.pool())
            .await?
    };
    rows.into_iter()
        .map(|row| {
            let legacy_reported_cost_text: Option<String> =
                row.try_get("legacy_reported_cost_text")?;
            let legacy_cost_usd_raw: Option<String> = row.try_get("legacy_cost_usd_raw")?;
            Ok(LedgerEventRow {
                invocation_id: row.try_get("invocation_id")?,
                project_id: row.try_get("project_id")?,
                provider_id: row.try_get("provider_id")?,
                model_id: row.try_get("model_id")?,
                runtime_model: row.try_get("runtime_model")?,
                agent_id: row.try_get("agent_id")?,
                profile_id: row.try_get("profile_id")?,
                agent_name_snapshot: row.try_get("agent_name_snapshot")?,
                project_name_snapshot: row.try_get("project_name_snapshot")?,
                executor_type: row.try_get("executor_type")?,
                counters: [
                    row.try_get("input_tokens")?,
                    row.try_get("output_tokens")?,
                    row.try_get("cache_read_tokens")?,
                    row.try_get("cache_write_tokens")?,
                ],
                provider_reported_nano_usd: row.try_get("provider_reported_nano_usd")?,
                legacy_reported_cost_text: legacy_cost_usd_raw.or(legacy_reported_cost_text),
                estimated_nano_usd: row.try_get("estimated_nano_usd")?,
                cost_kind: parse_db_enum(row.try_get("cost_kind")?)?,
                provenance_kind: parse_db_enum(row.try_get("provenance_kind")?)?,
                rate_revision_id: row.try_get("rate_revision_id")?,
                catalog_snapshot_id: row.try_get("catalog_snapshot_id")?,
                formula_revision: row.try_get("formula_revision")?,
                retrospective: row.try_get::<i64, _>("retrospective")? != 0,
                coverage_reason_code: row
                    .try_get::<Option<String>, _>("coverage_reason_code")?
                    .map(parse_db_enum)
                    .transpose()?,
                occurred_at: row.try_get("occurred_at")?,
                rate_source_kind: row.try_get("rate_source_kind")?,
                rate_effective_at: row.try_get("rate_effective_at")?,
                rate_input: row.try_get("rate_input")?,
                rate_output: row.try_get("rate_output")?,
                rate_cache_read: row.try_get("rate_cache_read")?,
                rate_cache_write: row.try_get("rate_cache_write")?,
                catalog_digest: row.try_get("catalog_digest")?,
                catalog_fetched_at: row.try_get("catalog_fetched_at")?,
                catalog_freshness: row.try_get("catalog_freshness")?,
            })
        })
        .collect()
}

async fn fetch_domain_runs(
    db: &SqliteDb,
    owner_user_id: Option<&str>,
    project_id: Option<&str>,
    from: Option<&str>,
    to: Option<&str>,
) -> Result<Vec<DomainRun>> {
    let mut runs = Vec::new();
    if let Some(project_id) = project_id {
        let rows = sqlx::query(
            "SELECT e.id, COALESCE(e.stopped_at, e.created_at) AS occurred_at, t.project_id
             FROM execution e JOIN task t ON t.id = e.task_id
             WHERE t.project_id = ?
               AND e.status IN ('completed', 'failed', 'cancelled')
               AND (?2 IS NULL OR julianday(COALESCE(e.stopped_at, e.created_at)) >= julianday(?2) - 1.0 / 86400.0)
               AND (?3 IS NULL OR julianday(COALESCE(e.stopped_at, e.created_at)) < julianday(?3) + 1.0 / 86400.0)",
        )
        .bind(project_id)
        .bind(from)
        .bind(to)
        .fetch_all(db.pool())
        .await?;
        for row in rows {
            let occurred_at: String = row.try_get("occurred_at")?;
            if timestamp_in_window(&occurred_at, from, to) {
                runs.push(DomainRun {
                    surface: UsageSurface::TaskExecution,
                    source_id: row.try_get("id")?,
                    project_id: Some(row.try_get("project_id")?),
                });
            }
        }
        let rows = sqlx::query(
            "SELECT m.id, m.created_at, c.project_id
             FROM agent_chat_message m JOIN agent_chat c ON c.id = m.chat_id
             WHERE c.kind = 'project' AND c.project_id = ?
               AND m.author_type = 'agent'
               AND m.status IN ('complete', 'failed', 'cancelled')
               AND m.source_type != 'handoff'
               AND (?2 IS NULL OR julianday(m.created_at) >= julianday(?2) - 1.0 / 86400.0)
               AND (?3 IS NULL OR julianday(m.created_at) < julianday(?3) + 1.0 / 86400.0)",
        )
        .bind(project_id)
        .bind(from)
        .bind(to)
        .fetch_all(db.pool())
        .await?;
        for row in rows {
            let occurred_at: String = row.try_get("created_at")?;
            if timestamp_in_window(&occurred_at, from, to) {
                runs.push(DomainRun {
                    surface: UsageSurface::ProjectChat,
                    source_id: row.try_get("id")?,
                    project_id: Some(row.try_get("project_id")?),
                });
            }
        }
        return Ok(runs);
    }

    let owner_user_id = owner_user_id.ok_or(DbError::InvalidTransition)?;
    let rows = sqlx::query(
        "SELECT e.id, COALESCE(e.stopped_at, e.created_at) AS occurred_at, t.project_id
         FROM execution e JOIN task t ON t.id = e.task_id
         JOIN project p ON p.id = t.project_id
         WHERE p.owner_id = ?
           AND e.status IN ('completed', 'failed', 'cancelled')
           AND (?2 IS NULL OR julianday(COALESCE(e.stopped_at, e.created_at)) >= julianday(?2) - 1.0 / 86400.0)
           AND (?3 IS NULL OR julianday(COALESCE(e.stopped_at, e.created_at)) < julianday(?3) + 1.0 / 86400.0)",
    )
    .bind(owner_user_id)
    .bind(from)
    .bind(to)
    .fetch_all(db.pool())
    .await?;
    for row in rows {
        let occurred_at: String = row.try_get("occurred_at")?;
        if timestamp_in_window(&occurred_at, from, to) {
            runs.push(DomainRun {
                surface: UsageSurface::TaskExecution,
                source_id: row.try_get("id")?,
                project_id: Some(row.try_get("project_id")?),
            });
        }
    }
    let rows = sqlx::query(
        "SELECT m.id, m.created_at, c.project_id
         FROM agent_chat_message m JOIN agent_chat c ON c.id = m.chat_id
         JOIN project p ON p.id = c.project_id
         WHERE c.kind = 'project' AND p.owner_id = ?
           AND m.author_type = 'agent'
           AND m.status IN ('complete', 'failed', 'cancelled')
           AND m.source_type != 'handoff'
           AND (?2 IS NULL OR julianday(m.created_at) >= julianday(?2) - 1.0 / 86400.0)
           AND (?3 IS NULL OR julianday(m.created_at) < julianday(?3) + 1.0 / 86400.0)",
    )
    .bind(owner_user_id)
    .bind(from)
    .bind(to)
    .fetch_all(db.pool())
    .await?;
    for row in rows {
        let occurred_at: String = row.try_get("created_at")?;
        if timestamp_in_window(&occurred_at, from, to) {
            runs.push(DomainRun {
                surface: UsageSurface::ProjectChat,
                source_id: row.try_get("id")?,
                project_id: Some(row.try_get("project_id")?),
            });
        }
    }
    let rows = sqlx::query(
        "SELECT m.id, m.created_at
         FROM agent_chat_message m JOIN agent_chat c ON c.id = m.chat_id
         WHERE c.kind = 'account_main' AND c.account_id = ?
           AND m.author_type = 'agent'
           AND m.status IN ('complete', 'failed', 'cancelled')
           AND m.source_type != 'handoff'
           AND (?2 IS NULL OR julianday(m.created_at) >= julianday(?2) - 1.0 / 86400.0)
           AND (?3 IS NULL OR julianday(m.created_at) < julianday(?3) + 1.0 / 86400.0)",
    )
    .bind(owner_user_id)
    .bind(from)
    .bind(to)
    .fetch_all(db.pool())
    .await?;
    for row in rows {
        let occurred_at: String = row.try_get("created_at")?;
        if timestamp_in_window(&occurred_at, from, to) {
            runs.push(DomainRun {
                surface: UsageSurface::MainChat,
                source_id: row.try_get("id")?,
                project_id: None,
            });
        }
    }
    let rows = sqlx::query(
        "SELECT id, finished_at
         FROM agent_inquiry
         WHERE owner_user_id = ?
           AND status IN ('succeeded', 'failed', 'cancelled')
           AND finished_at IS NOT NULL
           AND (?2 IS NULL OR julianday(finished_at) >= julianday(?2) - 1.0 / 86400.0)
           AND (?3 IS NULL OR julianday(finished_at) < julianday(?3) + 1.0 / 86400.0)",
    )
    .bind(owner_user_id)
    .bind(from)
    .bind(to)
    .fetch_all(db.pool())
    .await?;
    for row in rows {
        let occurred_at: String = row.try_get("finished_at")?;
        if timestamp_in_window(&occurred_at, from, to) {
            runs.push(DomainRun {
                surface: UsageSurface::MainInquiry,
                source_id: row.try_get("id")?,
                project_id: None,
            });
        }
    }
    Ok(runs)
}

async fn fetch_usage_dataset(
    db: &SqliteDb,
    owner_user_id: Option<&str>,
    project_id: Option<&str>,
    from: Option<&str>,
    to: Option<&str>,
) -> Result<UsageDataset> {
    validate_window(from, to)?;
    let genesis_attributions = fetch_genesis_attributions(db, owner_user_id, project_id).await?;
    let genesis_boundaries = fetch_genesis_boundaries(db, owner_user_id, project_id).await?;
    let mut genesis_sources = HashMap::<String, Option<String>>::new();
    for attribution in &genesis_attributions {
        let project = Some(attribution.project_id.clone());
        genesis_sources.insert(attribution.message_id.clone(), project.clone());
        if let Some(source_id) = attribution.source_id.as_deref() {
            genesis_sources.insert(source_id.to_owned(), project.clone());
        }
        if let Some(source_message_id) = attribution.source_message_id.as_deref() {
            genesis_sources.insert(source_message_id.to_owned(), project);
        }
        if let Some(source_turn_job_id) = attribution.source_turn_job_id.as_deref() {
            genesis_sources.insert(
                source_turn_job_id.to_owned(),
                Some(attribution.project_id.clone()),
            );
        }
    }
    for boundary in &genesis_boundaries {
        let project = Some(boundary.project_id.clone());
        if let Some(source_turn_job_id) = boundary.source_turn_job_id.as_deref() {
            genesis_sources.insert(source_turn_job_id.to_owned(), project.clone());
        }
        for source_message_id in &boundary.source_message_ids {
            genesis_sources.insert(source_message_id.clone(), project.clone());
        }
    }
    let genesis_source_ids: BTreeSet<String> = genesis_sources.keys().cloned().collect();
    let genesis_source_ids_json =
        serde_json::to_string(&genesis_source_ids).map_err(|_| DbError::InvalidTransition)?;
    let invocation_rows = fetch_ledger_invocations(
        db,
        owner_user_id,
        project_id,
        from,
        to,
        Some(&genesis_source_ids_json),
    )
    .await?;
    let event_rows = fetch_ledger_events(
        db,
        owner_user_id,
        project_id,
        from,
        to,
        Some(&genesis_source_ids_json),
    )
    .await?;
    let mut events_by_invocation: HashMap<String, Vec<LedgerEventRow>> = HashMap::new();
    for event in event_rows {
        events_by_invocation
            .entry(event.invocation_id.clone())
            .or_default()
            .push(event);
    }
    let mut invocations = Vec::new();
    for row in invocation_rows {
        let all_events = events_by_invocation.remove(&row.id).unwrap_or_default();
        let mut immutable_project_id = if row.surface == UsageSurface::GenesisChat {
            row.project_id
                .clone()
                .or_else(|| all_events.iter().find_map(|event| event.project_id.clone()))
                .or_else(|| genesis_sources.get(&row.source_id).cloned().flatten())
        } else {
            row.project_id.clone()
        };
        if let Some(project_id) = project_id {
            if row.surface == UsageSurface::GenesisChat
                && immutable_project_id.as_deref() != Some(project_id)
            {
                continue;
            }
        }
        let events_in_window: Vec<_> = all_events
            .iter()
            .filter(|event| timestamp_in_window(&event.occurred_at, from, to))
            .cloned()
            .collect();
        let has_events = !all_events.is_empty();
        let include = if has_events {
            !events_in_window.is_empty()
        } else {
            let occurrence = row.settled_at.as_deref().unwrap_or(&row.admitted_at);
            timestamp_in_window(occurrence, from, to)
        };
        if include {
            let mut row = row;
            // A historical Main Chat invocation may have been admitted before
            // the immutable Genesis handoff proof was materialized. Once the
            // proof binds that source to a Project, classify the invocation as
            // Genesis exactly once, rather than leaving provider attempts in
            // ordinary Main Chat while domain runs are reclassified below.
            if row.surface == UsageSurface::MainChat {
                if let Some(project_id) = genesis_sources.get(&row.source_id) {
                    row.surface = UsageSurface::GenesisChat;
                    immutable_project_id = project_id.clone();
                }
            }
            row.project_id = immutable_project_id.clone();
            invocations.push(LedgerInvocationView {
                row,
                events: events_in_window,
            });
        }
    }
    let mut domain_runs = fetch_domain_runs(db, owner_user_id, project_id, from, to).await?;
    if project_id.is_some() {
        domain_runs.extend(
            genesis_attributions
                .iter()
                .filter(|attribution| timestamp_in_window(&attribution.occurred_at, from, to))
                .map(|attribution| DomainRun {
                    surface: UsageSurface::GenesisChat,
                    source_id: attribution.message_id.clone(),
                    project_id: Some(attribution.project_id.clone()),
                }),
        );
    }
    // A successful Genesis handoff may terminalize its source turn through a
    // control-transfer event and intentionally write no assistant message.
    // Count that immutable turn job exactly once when its own terminal time is
    // in the requested window. If an assistant response exists, its immutable
    // message attribution already represents the same logical turn.
    let genesis_message_sources: HashSet<String> = genesis_attributions
        .iter()
        .flat_map(|attribution| {
            std::iter::once(attribution.message_id.clone())
                .chain(attribution.source_id.clone())
                .chain(attribution.source_message_id.clone())
                .chain(attribution.source_turn_job_id.clone())
        })
        .collect();
    domain_runs.extend(genesis_boundaries.iter().filter_map(|boundary| {
        let source_id = boundary.source_turn_job_id.clone()?;
        if genesis_message_sources.contains(&source_id) {
            return None;
        }
        let occurred_at = boundary.source_turn_occurred_at.as_deref()?;
        timestamp_in_window(occurred_at, from, to).then_some(DomainRun {
            surface: UsageSurface::GenesisChat,
            source_id,
            project_id: Some(boundary.project_id.clone()),
        })
    }));
    for run in &mut domain_runs {
        if run.surface == UsageSurface::MainChat {
            if let Some(project_id) = genesis_sources.get(&run.source_id) {
                run.surface = UsageSurface::GenesisChat;
                run.project_id = project_id.clone();
            }
        }
    }
    Ok(UsageDataset {
        invocations,
        domain_runs,
    })
}

#[async_trait]
impl UsageAnalyticsRepo for SqliteDb {
    async fn get_project_usage_analytics(
        &self,
        project_id: &str,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<UsageAnalytics> {
        usage_analytics(fetch_usage_dataset(self, None, Some(project_id), from, to).await?)
    }

    async fn get_account_usage_analytics(
        &self,
        owner_user_id: &str,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<AccountUsageAnalyticsResponse> {
        account_usage_analytics(
            fetch_usage_dataset(self, Some(owner_user_id), None, from, to).await?,
            from,
            to,
        )
    }

    async fn count_project_released_milestones(
        &self,
        project_id: &str,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<i64> {
        validate_window(from, to)?;
        // Raw RFC3339 text is not ordered by instant when offsets differ.  Use
        // the parsed timestamp predicate shared by usage events so boundaries
        // remain correct for equivalent `Z` and offset-form timestamps.
        let rows = sqlx::query(
            "SELECT authorization_occurred_at FROM project_release
             WHERE project_id = ?
               AND (?2 IS NULL OR julianday(authorization_occurred_at) >= julianday(?2) - 1.0 / 86400.0)
               AND (?3 IS NULL OR julianday(authorization_occurred_at) < julianday(?3) + 1.0 / 86400.0)
             ORDER BY authorization_occurred_at ASC, id ASC",
        )
        .bind(project_id)
        .bind(from)
        .bind(to)
        .fetch_all(self.pool())
        .await?;
        let mut count = 0_i64;
        for row in rows {
            let occurred_at: String = row.try_get("authorization_occurred_at")?;
            if timestamp_in_window(&occurred_at, from, to) {
                count = count.checked_add(1).ok_or(DbError::InvalidTransition)?;
            }
        }
        Ok(count)
    }
}

/// The timing snapshot is decoded after the caller loads ledger usage, retaining
/// the route's read and error ordering.
pub struct TaskExecutionTimingRead {
    row: SqliteRow,
}

impl TaskExecutionTimingRead {
    pub fn with_usage(
        self,
        usage: UsageAggregate,
    ) -> Result<api_types::TaskExecutionObservability> {
        Ok(api_types::TaskExecutionObservability {
            counts: usage.counts,
            tokens: usage.tokens,
            cost: usage.cost,
            active_execution_id: self.row.try_get("active_execution_id")?,
            active_role: self.row.try_get("active_role")?,
            active_started_at: self.row.try_get("active_started_at")?,
            active_elapsed_seconds: self
                .row
                .try_get::<Option<i64>, _>("active_elapsed_seconds")?
                .map(|value| value as f64),
            latest_execution_id: self.row.try_get("latest_execution_id")?,
            latest_execution_status: self.row.try_get("latest_execution_status")?,
            latest_role: self.row.try_get("latest_role")?,
            latest_started_at: self.row.try_get("latest_started_at")?,
            latest_stopped_at: self.row.try_get("latest_stopped_at")?,
            latest_runtime_seconds: self
                .row
                .try_get::<Option<i64>, _>("latest_runtime_seconds")?
                .map(|value| value as f64),
            total_runtime_seconds: self.row.try_get::<i64, _>("total_runtime_seconds")? as f64,
        })
    }
}

impl SqliteDb {
    pub async fn task_execution_timing(&self, task_id: &str) -> Result<TaskExecutionTimingRead> {
        let row = sqlx::query(
        "WITH task_executions AS (
             SELECT * FROM execution WHERE task_id = ?
         )
         SELECT
             (SELECT COALESCE(SUM(max(COALESCE(
                 (CASE
                     WHEN status = 'running' THEN CAST(strftime('%s', 'now') AS INTEGER)
                     ELSE CAST(strftime('%s', COALESCE(stopped_at, updated_at)) AS INTEGER)
                  END) - CAST(strftime('%s', created_at) AS INTEGER),
                 0), 0)), 0)
              FROM task_executions) AS total_runtime_seconds,
             (SELECT id FROM task_executions WHERE status = 'running' ORDER BY created_at DESC, id DESC LIMIT 1) AS active_execution_id,
             (SELECT role FROM task_executions WHERE status = 'running' ORDER BY created_at DESC, id DESC LIMIT 1) AS active_role,
             (SELECT created_at FROM task_executions WHERE status = 'running' ORDER BY created_at DESC, id DESC LIMIT 1) AS active_started_at,
             (SELECT max(COALESCE(
                 CAST(strftime('%s', 'now') AS INTEGER) - CAST(strftime('%s', created_at) AS INTEGER),
                 0), 0)
              FROM task_executions WHERE status = 'running' ORDER BY created_at DESC, id DESC LIMIT 1) AS active_elapsed_seconds,
             (SELECT id FROM task_executions ORDER BY created_at DESC, id DESC LIMIT 1) AS latest_execution_id,
             (SELECT status FROM task_executions ORDER BY created_at DESC, id DESC LIMIT 1) AS latest_execution_status,
             (SELECT role FROM task_executions ORDER BY created_at DESC, id DESC LIMIT 1) AS latest_role,
             (SELECT created_at FROM task_executions ORDER BY created_at DESC, id DESC LIMIT 1) AS latest_started_at,
             (SELECT stopped_at FROM task_executions ORDER BY created_at DESC, id DESC LIMIT 1) AS latest_stopped_at,
             (SELECT max(COALESCE(
                 (CASE
                     WHEN status = 'running' THEN CAST(strftime('%s', 'now') AS INTEGER)
                     ELSE CAST(strftime('%s', COALESCE(stopped_at, updated_at)) AS INTEGER)
                  END) - CAST(strftime('%s', created_at) AS INTEGER),
                 0), 0)
              FROM task_executions ORDER BY created_at DESC, id DESC LIMIT 1) AS latest_runtime_seconds
        ",
    )
    .bind(task_id)
    .fetch_one(self.pool())
    .await?;

        Ok(TaskExecutionTimingRead { row })
    }
}
