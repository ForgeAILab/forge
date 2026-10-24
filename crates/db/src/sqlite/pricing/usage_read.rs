//! Usage index readers and scoped usage source projections.

use super::*;

// Narrow batch readers share the caller's connection, including read snapshots.
impl SqliteDb {
    pub async fn usage_invocations_for_sources(
        connection: &mut sqlx::SqliteConnection,
        source_ids: &[String],
    ) -> Result<Vec<UsageInvocation>> {
        let mut result = Vec::new();
        for chunk in source_ids.chunks(400) {
            let mut query = sqlx::QueryBuilder::<Sqlite>::new(
                "SELECT * FROM usage_invocation WHERE source_id IN (",
            );
            let mut ids = query.separated(", ");
            for id in chunk {
                ids.push_bind(id);
            }
            ids.push_unseparated(") ORDER BY source_id, attempt_ordinal ASC, id ASC");
            result.extend(
                query
                    .build()
                    .fetch_all(&mut *connection)
                    .await?
                    .into_iter()
                    .map(map_invocation)
                    .collect::<Result<Vec<_>>>()?,
            );
        }
        Ok(result)
    }
    pub async fn usage_events_for_invocations(
        connection: &mut sqlx::SqliteConnection,
        invocation_ids: &[String],
    ) -> Result<Vec<UsageEvent>> {
        let mut result = Vec::new();
        for chunk in invocation_ids.chunks(400) {
            let mut query = sqlx::QueryBuilder::<Sqlite>::new(
                "SELECT * FROM usage_event WHERE invocation_id IN (",
            );
            let mut ids = query.separated(", ");
            for id in chunk {
                ids.push_bind(id);
            }
            ids.push_unseparated(") ORDER BY invocation_id, occurred_at, id");
            result.extend(
                query
                    .build()
                    .fetch_all(&mut *connection)
                    .await?
                    .into_iter()
                    .map(map_usage_event)
                    .collect::<Result<Vec<_>>>()?,
            );
        }
        Ok(result)
    }
    pub async fn cost_estimate_revisions_for_invocations(
        connection: &mut sqlx::SqliteConnection,
        invocation_ids: &[String],
    ) -> Result<Vec<CostEstimateRevision>> {
        let mut result = Vec::new();
        for chunk in invocation_ids.chunks(400) {
            let mut query = sqlx::QueryBuilder::<Sqlite>::new("SELECT revision.* FROM usage_event event JOIN cost_estimate_revision revision ON revision.usage_event_id = event.id WHERE event.invocation_id IN (");
            let mut ids = query.separated(", ");
            for id in chunk {
                ids.push_bind(id);
            }
            ids.push_unseparated(") ORDER BY revision.usage_event_id, revision.revision DESC, revision.created_at DESC, revision.id DESC");
            result.extend(
                query
                    .build()
                    .fetch_all(&mut *connection)
                    .await?
                    .into_iter()
                    .map(map_estimate_revision)
                    .collect::<Result<Vec<_>>>()?,
            );
        }
        Ok(result)
    }
}

// Bounded payload readers for the process-local usage index. Rowid ranges are
// backed by the table's integer rowid; changed IDs use primary-key lookups.
impl SqliteDb {
    pub async fn usage_invocation_slice(
        connection: &mut sqlx::SqliteConnection,
        after: i64,
        through: i64,
    ) -> Result<Vec<(i64, UsageInvocation)>> {
        sqlx::query("SELECT rowid AS ledger_rowid, * FROM usage_invocation WHERE rowid > ? AND rowid <= ? ORDER BY rowid LIMIT 400")
            .bind(after).bind(through).fetch_all(connection).await?.into_iter()
            .map(|row| Ok((row.try_get("ledger_rowid")?, map_invocation(row)?))).collect()
    }
    pub async fn usage_event_slice(
        connection: &mut sqlx::SqliteConnection,
        after: i64,
        through: i64,
    ) -> Result<Vec<(i64, UsageEvent)>> {
        sqlx::query("SELECT rowid AS ledger_rowid, * FROM usage_event WHERE rowid > ? AND rowid <= ? ORDER BY rowid LIMIT 400")
            .bind(after).bind(through).fetch_all(connection).await?.into_iter()
            .map(|row| Ok((row.try_get("ledger_rowid")?, map_usage_event(row)?))).collect()
    }
    pub async fn usage_invocations_by_ids(
        connection: &mut sqlx::SqliteConnection,
        ids: &[String],
    ) -> Result<Vec<UsageInvocation>> {
        let mut result = Vec::new();
        for chunk in ids.chunks(400) {
            let json = serde_json::to_string(chunk).map_err(|_| DbError::InvalidTransition)?;
            result.extend(
                sqlx::query(
                    "SELECT * FROM usage_invocation WHERE id IN (SELECT value FROM json_each(?))",
                )
                .bind(json)
                .fetch_all(&mut *connection)
                .await?
                .into_iter()
                .map(map_invocation)
                .collect::<Result<Vec<_>>>()?,
            );
        }
        Ok(result)
    }
    pub async fn usage_events_by_ids(
        connection: &mut sqlx::SqliteConnection,
        ids: &[String],
    ) -> Result<Vec<UsageEvent>> {
        let mut result = Vec::new();
        for chunk in ids.chunks(400) {
            let json = serde_json::to_string(chunk).map_err(|_| DbError::InvalidTransition)?;
            result.extend(
                sqlx::query(
                    "SELECT * FROM usage_event WHERE id IN (SELECT value FROM json_each(?))",
                )
                .bind(json)
                .fetch_all(&mut *connection)
                .await?
                .into_iter()
                .map(map_usage_event)
                .collect::<Result<Vec<_>>>()?,
            );
        }
        Ok(result)
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct UsageIndexEstimateChangeRow {
    row: sqlx::sqlite::SqliteRow,
}
impl UsageIndexEstimateChangeRow {
    pub fn revision_rowid(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("revision_rowid")?)
    }
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct UsageIndexInvocationChangeRow {
    row: sqlx::sqlite::SqliteRow,
}
impl UsageIndexInvocationChangeRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn revision(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("revision")?)
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct UsageIndexEffectiveEventRow {
    row: sqlx::sqlite::SqliteRow,
}
impl UsageIndexEffectiveEventRow {
    pub fn applied_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("applied_id")?)
    }
    pub fn applied_nanos(&self) -> crate::Result<Option<i64>> {
        Ok(self.row.try_get("applied_nanos")?)
    }
    pub fn applied_rate(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("applied_rate")?)
    }
    pub fn applied_catalog(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("applied_catalog")?)
    }
    pub fn applied_formula(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("applied_formula")?)
    }
    pub fn applied_retrospective(&self) -> crate::Result<bool> {
        Ok(self.row.try_get("applied_retrospective")?)
    }
    pub fn event_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("event_id")?)
    }
    pub fn rate_source_kind(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("rate_source_kind")?)
    }
    pub fn selection_source_kind(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("selection_source_kind")?)
    }
    pub fn selection_catalog_freshness(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("selection_catalog_freshness")?)
    }
    pub fn estimate_catalog_freshness(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("estimate_catalog_freshness")?)
    }
    pub fn selection_rate_revision_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("selection_rate_revision_id")?)
    }
    pub fn selection_catalog_snapshot_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("selection_catalog_snapshot_id")?)
    }
    pub fn rate_catalog_snapshot_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("rate_catalog_snapshot_id")?)
    }
    pub fn catalog_digest(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("catalog_digest")?)
    }
    pub fn rate_effective_at(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("rate_effective_at")?)
    }
    pub fn catalog_fetched_at(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("catalog_fetched_at")?)
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct UsageIndexActualRowidsRow {
    row: sqlx::sqlite::SqliteRow,
}
impl UsageIndexActualRowidsRow {
    pub fn i(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("i")?)
    }
    pub fn e(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("e")?)
    }
    pub fn r(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("r")?)
    }
    pub fn x(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("x")?)
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct UsageIndexAffectedAgentRow {
    row: sqlx::sqlite::SqliteRow,
}
impl UsageIndexAffectedAgentRow {}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct UsageIndexWatermarksRow {
    row: sqlx::sqlite::SqliteRow,
}
impl UsageIndexWatermarksRow {
    pub fn invocation_rowid(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("invocation_rowid")?)
    }
    pub fn event_rowid(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("event_rowid")?)
    }
    pub fn estimate_rowid(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("estimate_rowid")?)
    }
    pub fn execution_rowid(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("execution_rowid")?)
    }
    pub fn invocation_revision(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("invocation_revision")?)
    }
    pub fn execution_revision(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("execution_revision")?)
    }
    pub fn deletion_generation(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("deletion_generation")?)
    }
    pub fn owned_execution_count(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("owned_execution_count")?)
    }
    pub fn invocation_count(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("invocation_count")?)
    }
    pub fn event_count(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("event_count")?)
    }
    pub fn estimate_count(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("estimate_count")?)
    }
    pub fn execution_count(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("execution_count")?)
    }
}

pub async fn usage_index_watermarks_on_connection(
    connection: &mut sqlx::SqliteConnection,
) -> crate::Result<UsageIndexWatermarksRow> {
    let rows = sqlx::query("SELECT * FROM usage_ledger_revision WHERE id = 1")
        .fetch_one(connection)
        .await?;
    Ok(UsageIndexWatermarksRow { row: rows })
}

pub struct UsageIndexAffectedAgentsOnConnectionInput {
    pub old_event_rowid: i64,
    pub event_rowid: i64,
    pub old_estimate_rowid: i64,
    pub estimate_rowid: i64,
    pub old_invocation_rowid: i64,
    pub invocation_rowid: i64,
    pub old_invocation_revision: i64,
    pub invocation_revision: i64,
    pub old_execution_rowid: i64,
    pub execution_rowid: i64,
}

pub async fn usage_index_affected_agents_on_connection(
    connection: &mut sqlx::SqliteConnection,
    input: UsageIndexAffectedAgentsOnConnectionInput,
) -> crate::Result<Vec<UsageIndexAffectedAgentRow>> {
    let rows = sqlx::query("SELECT i.agent_id AS owner,e.agent_id AS attributed FROM usage_event e JOIN usage_invocation i ON i.id=e.invocation_id WHERE e.rowid>?1 AND e.rowid<=?2
                  UNION SELECT i.agent_id,e.agent_id FROM cost_estimate_revision r JOIN usage_event e ON e.id=r.usage_event_id JOIN usage_invocation i ON i.id=e.invocation_id WHERE r.rowid>?3 AND r.rowid<=?4
                  UNION SELECT agent_id,NULL FROM usage_invocation WHERE rowid>?5 AND rowid<=?6
                  UNION SELECT i.agent_id,NULL FROM usage_changed_invocation c JOIN usage_invocation i ON i.id=c.id WHERE c.revision>?7 AND c.revision<=?8
                  UNION SELECT agent_id,NULL FROM execution WHERE rowid>?9 AND rowid<=?10",)
.bind(input.old_event_rowid)
.bind(input.event_rowid)
.bind(input.old_estimate_rowid)
.bind(input.estimate_rowid)
.bind(input.old_invocation_rowid)
.bind(input.invocation_rowid)
.bind(input.old_invocation_revision)
.bind(input.invocation_revision)
.bind(input.old_execution_rowid)
.bind(input.execution_rowid)
.fetch_all(connection).await?;
    Ok(rows
        .into_iter()
        .map(|row| UsageIndexAffectedAgentRow { row })
        .collect())
}

pub async fn usage_index_actual_rowids_on_connection(
    connection: &mut sqlx::SqliteConnection,
) -> crate::Result<UsageIndexActualRowidsRow> {
    let rows = sqlx::query("SELECT COALESCE((SELECT MAX(rowid) FROM usage_invocation),0) AS i,COALESCE((SELECT MAX(rowid) FROM usage_event),0) AS e,COALESCE((SELECT MAX(rowid) FROM cost_estimate_revision),0) AS r,COALESCE((SELECT MAX(rowid) FROM execution),0) AS x",)
.fetch_one(connection).await?;
    Ok(UsageIndexActualRowidsRow { row: rows })
}

pub async fn usage_index_effective_event_sources_on_connection(
    connection: &mut sqlx::SqliteConnection,
    event_ids_json: String,
    through_estimate_rowid: i64,
) -> crate::Result<Vec<UsageIndexEffectiveEventRow>> {
    let rows = sqlx::query("WITH requested AS (SELECT value AS id FROM json_each(?))
            SELECT e.id AS event_id, er.id AS applied_id, er.estimated_nano_usd AS applied_nanos,
              er.rate_revision_id AS applied_rate, er.catalog_snapshot_id AS applied_catalog,
              er.formula_revision AS applied_formula, er.retrospective AS applied_retrospective,
              ps.source_kind AS selection_source_kind, ps.catalog_freshness AS selection_catalog_freshness,
              ps.rate_revision_id AS selection_rate_revision_id, ps.catalog_snapshot_id AS selection_catalog_snapshot_id,
              r.source_kind AS rate_source_kind, r.catalog_snapshot_id AS rate_catalog_snapshot_id,
              r.effective_at AS rate_effective_at, c.revision_digest AS catalog_digest,
              c.fetched_at AS catalog_fetched_at, ep.catalog_freshness AS estimate_catalog_freshness
            FROM requested JOIN usage_event e ON e.id=requested.id
            JOIN usage_invocation i ON i.id=e.invocation_id
            LEFT JOIN cost_estimate_revision er ON er.id=(SELECT prior.id FROM cost_estimate_revision prior
              WHERE prior.usage_event_id=e.id AND prior.state='applied' AND prior.rowid<=?
              ORDER BY prior.revision DESC,prior.created_at DESC,prior.id DESC LIMIT 1)
            LEFT JOIN pricing_selection ps ON ps.id=i.pricing_selection_id
            LEFT JOIN cost_estimation_run erun ON erun.id=er.run_id
            LEFT JOIN cost_estimation_preview ep ON ep.id=erun.preview_id
            LEFT JOIN pricing_rate_revision r ON r.id=COALESCE(er.rate_revision_id,e.rate_revision_id,ps.rate_revision_id)
            LEFT JOIN pricing_catalog_snapshot c ON c.id=COALESCE(er.catalog_snapshot_id,e.catalog_snapshot_id,r.catalog_snapshot_id,ps.catalog_snapshot_id)",)
.bind(event_ids_json)
.bind(through_estimate_rowid)
.fetch_all(connection).await?;
    Ok(rows
        .into_iter()
        .map(|row| UsageIndexEffectiveEventRow { row })
        .collect())
}

pub struct UsageIndexCitationEventIdsOnConnectionInput<'a> {
    pub estimate_rowid: i64,
    pub event_rowid: i64,
    pub agent_id: Option<&'a str>,
    pub rate_revision_id: &'a Option<String>,
    pub catalog_snapshot_id: &'a Option<String>,
    pub formula_revision: &'a Option<String>,
    pub retrospective: bool,
    pub offset: i64,
}

pub async fn usage_index_citation_event_ids_on_connection(
    connection: &mut sqlx::SqliteConnection,
    input: UsageIndexCitationEventIdsOnConnectionInput<'_>,
) -> crate::Result<Vec<String>> {
    Ok(sqlx::query_scalar::<_,String>("SELECT e.id FROM usage_event e
              JOIN usage_invocation i ON i.id=e.invocation_id
              LEFT JOIN cost_estimate_revision er ON er.id=(SELECT prior.id FROM cost_estimate_revision prior
                WHERE prior.usage_event_id=e.id AND prior.state='applied' AND prior.rowid<=?1
                ORDER BY prior.revision DESC,prior.created_at DESC,prior.id DESC LIMIT 1)
              LEFT JOIN pricing_selection ps ON ps.id=i.pricing_selection_id
              LEFT JOIN pricing_rate_revision r ON r.id=COALESCE(er.rate_revision_id,e.rate_revision_id,ps.rate_revision_id)
              WHERE e.rowid<=?2 AND i.lifecycle!='unsettled'
                AND (?3 IS NULL OR i.agent_id=?3 OR e.agent_id=?3)
                AND COALESCE(er.rate_revision_id,e.rate_revision_id,ps.rate_revision_id) IS ?4
                AND COALESCE(er.catalog_snapshot_id,e.catalog_snapshot_id,r.catalog_snapshot_id,ps.catalog_snapshot_id) IS ?5
                AND COALESCE(er.formula_revision,e.formula_revision) IS ?6
                AND COALESCE(er.retrospective,e.retrospective)=?7
              ORDER BY i.source_id DESC,i.attempt_ordinal DESC,i.id DESC,e.occurred_at DESC,e.id DESC
              LIMIT 150 OFFSET ?8",)
.bind(input.estimate_rowid)
.bind(input.event_rowid)
.bind(input.agent_id)
.bind(input.rate_revision_id)
.bind(input.catalog_snapshot_id)
.bind(input.formula_revision)
.bind(input.retrospective)
.bind(input.offset)
.fetch_all(connection).await?)
}

pub async fn usage_index_changed_invocations_on_connection(
    connection: &mut sqlx::SqliteConnection,
    after_revision: i64,
    through_revision: i64,
    through_rowid: i64,
) -> crate::Result<Vec<UsageIndexInvocationChangeRow>> {
    let rows = sqlx::query("SELECT c.id,c.revision FROM usage_changed_invocation c JOIN usage_invocation i ON i.id=c.id WHERE c.revision>? AND c.revision<=? AND i.rowid<=?3 ORDER BY c.revision LIMIT 400",)
.bind(after_revision)
.bind(through_revision)
.bind(through_rowid)
.fetch_all(connection).await?;
    Ok(rows
        .into_iter()
        .map(|row| UsageIndexInvocationChangeRow { row })
        .collect())
}

pub async fn usage_index_changed_estimates_on_connection(
    connection: &mut sqlx::SqliteConnection,
    after_rowid: i64,
    through_rowid: i64,
    through_event_rowid: i64,
) -> crate::Result<Vec<UsageIndexEstimateChangeRow>> {
    let rows = sqlx::query("SELECT r.rowid AS revision_rowid,e.id FROM cost_estimate_revision r JOIN usage_event e ON e.id=r.usage_event_id WHERE r.rowid>? AND r.rowid<=? AND e.rowid<=? ORDER BY r.rowid LIMIT 400",)
.bind(after_rowid)
.bind(through_rowid)
.bind(through_event_rowid)
.fetch_all(connection).await?;
    Ok(rows
        .into_iter()
        .map(|row| UsageIndexEstimateChangeRow { row })
        .collect())
}

pub async fn usage_index_source_batch_on_connection(
    connection: &mut sqlx::SqliteConnection,
    first: bool,
    all_agents: bool,
    ids_json: &str,
    after: &str,
) -> crate::Result<Vec<String>> {
    let boundary = if first { ">=" } else { ">" };
    let query=format!("SELECT source_id FROM (SELECT DISTINCT source_id FROM usage_invocation WHERE source_id {boundary} ?3 AND (?1 OR agent_id IN (SELECT value FROM json_each(?2))) ORDER BY source_id LIMIT 128)
            UNION SELECT source_id FROM (SELECT DISTINCT source_id FROM usage_event WHERE source_id {boundary} ?3 AND (?1 OR agent_id IN (SELECT value FROM json_each(?2))) ORDER BY source_id LIMIT 128)
            UNION SELECT source_id FROM (SELECT id AS source_id FROM execution WHERE id {boundary} ?3 AND (?1 OR agent_id IN (SELECT value FROM json_each(?2))) ORDER BY id LIMIT 128)
            ORDER BY source_id LIMIT 128");

    Ok(sqlx::query_scalar::<_, String>(&query)
        .bind(all_agents)
        .bind(ids_json)
        .bind(after)
        .fetch_all(connection)
        .await?)
}

#[derive(Clone, Copy)]
pub enum UsageIndexAffectedAgentField {
    Owner,
    Attributed,
}
impl UsageIndexAffectedAgentRow {
    pub fn agent(&self, field: UsageIndexAffectedAgentField) -> crate::Result<Option<String>> {
        Ok(self.row.try_get(match field {
            UsageIndexAffectedAgentField::Owner => "owner",
            UsageIndexAffectedAgentField::Attributed => "attributed",
        })?)
    }
}

pub trait UsageEventSourceMetadataRead {
    fn rate_source_kind(&self) -> crate::Result<Option<String>>;
    fn selection_source_kind(&self) -> crate::Result<Option<String>>;
    fn selection_catalog_freshness(&self) -> crate::Result<Option<String>>;
    fn estimate_catalog_freshness(&self) -> crate::Result<Option<String>>;
    fn selection_rate_revision_id(&self) -> crate::Result<Option<String>>;
    fn selection_catalog_snapshot_id(&self) -> crate::Result<Option<String>>;
    fn rate_catalog_snapshot_id(&self) -> crate::Result<Option<String>>;
    fn catalog_digest(&self) -> crate::Result<Option<String>>;
    fn rate_effective_at(&self) -> crate::Result<Option<String>>;
    fn catalog_fetched_at(&self) -> crate::Result<Option<String>>;
}
impl UsageEventSourceMetadataRead for sqlx::sqlite::SqliteRow {
    fn rate_source_kind(&self) -> crate::Result<Option<String>> {
        Ok(self.try_get("rate_source_kind")?)
    }
    fn selection_source_kind(&self) -> crate::Result<Option<String>> {
        Ok(self.try_get("selection_source_kind")?)
    }
    fn selection_catalog_freshness(&self) -> crate::Result<Option<String>> {
        Ok(self.try_get("selection_catalog_freshness")?)
    }
    fn estimate_catalog_freshness(&self) -> crate::Result<Option<String>> {
        Ok(self.try_get("estimate_catalog_freshness")?)
    }
    fn selection_rate_revision_id(&self) -> crate::Result<Option<String>> {
        Ok(self.try_get("selection_rate_revision_id")?)
    }
    fn selection_catalog_snapshot_id(&self) -> crate::Result<Option<String>> {
        Ok(self.try_get("selection_catalog_snapshot_id")?)
    }
    fn rate_catalog_snapshot_id(&self) -> crate::Result<Option<String>> {
        Ok(self.try_get("rate_catalog_snapshot_id")?)
    }
    fn catalog_digest(&self) -> crate::Result<Option<String>> {
        Ok(self.try_get("catalog_digest")?)
    }
    fn rate_effective_at(&self) -> crate::Result<Option<String>> {
        Ok(self.try_get("rate_effective_at")?)
    }
    fn catalog_fetched_at(&self) -> crate::Result<Option<String>> {
        Ok(self.try_get("catalog_fetched_at")?)
    }
}
impl UsageEventSourceMetadataRead for UsageIndexEffectiveEventRow {
    fn rate_source_kind(&self) -> crate::Result<Option<String>> {
        self.rate_source_kind()
    }
    fn selection_source_kind(&self) -> crate::Result<Option<String>> {
        self.selection_source_kind()
    }
    fn selection_catalog_freshness(&self) -> crate::Result<Option<String>> {
        self.selection_catalog_freshness()
    }
    fn estimate_catalog_freshness(&self) -> crate::Result<Option<String>> {
        self.estimate_catalog_freshness()
    }
    fn selection_rate_revision_id(&self) -> crate::Result<Option<String>> {
        self.selection_rate_revision_id()
    }
    fn selection_catalog_snapshot_id(&self) -> crate::Result<Option<String>> {
        self.selection_catalog_snapshot_id()
    }
    fn rate_catalog_snapshot_id(&self) -> crate::Result<Option<String>> {
        self.rate_catalog_snapshot_id()
    }
    fn catalog_digest(&self) -> crate::Result<Option<String>> {
        self.catalog_digest()
    }
    fn rate_effective_at(&self) -> crate::Result<Option<String>> {
        self.rate_effective_at()
    }
    fn catalog_fetched_at(&self) -> crate::Result<Option<String>> {
        self.catalog_fetched_at()
    }
}

pub struct AgentUsageSourceRow {
    row: SqliteRow,
}
impl AgentUsageSourceRow {
    pub fn agent_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("agent_id")?)
    }
    pub fn source_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("source_id")?)
    }
}

impl SqliteDb {
    pub async fn agent_usage_source_ids_on_connection(
        connection: &mut sqlx::SqliteConnection,
        ids_json: &str,
    ) -> crate::Result<Vec<AgentUsageSourceRow>> {
        let rows = sqlx::query(
        "SELECT agent_id, source_id FROM usage_invocation WHERE agent_id IN (SELECT value FROM json_each(?1))
         UNION SELECT agent_id, source_id FROM usage_event WHERE agent_id IN (SELECT value FROM json_each(?1))
         ORDER BY agent_id, source_id",
    ).bind(ids_json).fetch_all(connection).await?;
        Ok(rows
            .into_iter()
            .map(|row| AgentUsageSourceRow { row })
            .collect())
    }
}

pub struct TaskUsageSourceRow {
    row: SqliteRow,
}
impl TaskUsageSourceRow {
    pub fn source_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("source_id")?)
    }
}

impl SqliteDb {
    pub async fn task_usage_source_ids_on_connection(
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
    ) -> crate::Result<Vec<TaskUsageSourceRow>> {
        let rows = sqlx::query(TASK_USAGE_SOURCES_SQL)
            .bind(task_id)
            .bind(task_id)
            .bind(task_id)
            .bind(task_id)
            .bind(task_id)
            .bind(task_id)
            .fetch_all(connection)
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| TaskUsageSourceRow { row })
            .collect())
    }
}

pub struct OperationsUsageSourceRow {
    row: SqliteRow,
}
impl OperationsUsageSourceRow {
    pub fn source_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("source_id")?)
    }
}

impl SqliteDb {
    pub async fn operations_usage_source_ids_on_connection(
        connection: &mut sqlx::SqliteConnection,
    ) -> crate::Result<Vec<OperationsUsageSourceRow>> {
        let rows = sqlx::query(
            "SELECT DISTINCT source_id FROM usage_invocation
         UNION SELECT DISTINCT source_id FROM usage_event
         ORDER BY source_id ASC",
        )
        .fetch_all(connection)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| OperationsUsageSourceRow { row })
            .collect())
    }
}

/// Collect the execution attempts belonging to one Task.  Runtime rows are
/// included as domain runs even when no invocation was admitted, preserving
/// no-provider and active/pending coverage in Task observability.
// Separate branches keep every predicate backed by its task/execution/source index.
pub const TASK_USAGE_SOURCES_SQL: &str = "
    SELECT source_id FROM usage_invocation WHERE task_id = ?
    UNION SELECT source_id FROM usage_invocation WHERE execution_id IN (SELECT id FROM execution WHERE task_id = ?)
    UNION SELECT source_id FROM usage_invocation WHERE source_id IN (SELECT id FROM execution WHERE task_id = ?)
    UNION SELECT source_id FROM usage_event WHERE task_id = ?
    UNION SELECT source_id FROM usage_event WHERE execution_id IN (SELECT id FROM execution WHERE task_id = ?)
    UNION SELECT source_id FROM usage_event WHERE source_id IN (SELECT id FROM execution WHERE task_id = ?)
    ORDER BY source_id";

pub struct EffectiveUsageEventMetadataRow {
    row: SqliteRow,
}
impl EffectiveUsageEventMetadataRow {
    pub fn event_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("event_id")?)
    }
}
impl UsageEventSourceMetadataRead for EffectiveUsageEventMetadataRow {
    fn rate_source_kind(&self) -> crate::Result<Option<String>> {
        UsageEventSourceMetadataRead::rate_source_kind(&self.row)
    }
    fn selection_source_kind(&self) -> crate::Result<Option<String>> {
        UsageEventSourceMetadataRead::selection_source_kind(&self.row)
    }
    fn selection_catalog_freshness(&self) -> crate::Result<Option<String>> {
        UsageEventSourceMetadataRead::selection_catalog_freshness(&self.row)
    }
    fn estimate_catalog_freshness(&self) -> crate::Result<Option<String>> {
        UsageEventSourceMetadataRead::estimate_catalog_freshness(&self.row)
    }
    fn selection_rate_revision_id(&self) -> crate::Result<Option<String>> {
        UsageEventSourceMetadataRead::selection_rate_revision_id(&self.row)
    }
    fn selection_catalog_snapshot_id(&self) -> crate::Result<Option<String>> {
        UsageEventSourceMetadataRead::selection_catalog_snapshot_id(&self.row)
    }
    fn rate_catalog_snapshot_id(&self) -> crate::Result<Option<String>> {
        UsageEventSourceMetadataRead::rate_catalog_snapshot_id(&self.row)
    }
    fn catalog_digest(&self) -> crate::Result<Option<String>> {
        UsageEventSourceMetadataRead::catalog_digest(&self.row)
    }
    fn rate_effective_at(&self) -> crate::Result<Option<String>> {
        UsageEventSourceMetadataRead::rate_effective_at(&self.row)
    }
    fn catalog_fetched_at(&self) -> crate::Result<Option<String>> {
        UsageEventSourceMetadataRead::catalog_fetched_at(&self.row)
    }
}
impl SqliteDb {
    pub async fn effective_usage_event_metadata_on_connection(
        connection: &mut sqlx::SqliteConnection,
        chunk: &[(crate::UsageEvent, Option<String>)],
    ) -> crate::Result<Vec<EffectiveUsageEventMetadataRow>> {
        let mut query = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
            "WITH requested(event_id, invocation_id, rate_id, catalog_id, revision_id) AS (",
        );
        query.push_values(chunk, |mut row, (event, revision)| {
            row.push_bind(&event.id)
                .push_bind(&event.invocation_id)
                .push_bind(event.rate_revision_id.as_deref())
                .push_bind(event.catalog_snapshot_id.as_deref())
                .push_bind(revision.as_deref());
        });
        query.push(") SELECT requested.event_id,
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
         FROM requested JOIN usage_invocation i ON i.id = requested.invocation_id
         LEFT JOIN pricing_selection ps ON ps.id = i.pricing_selection_id
         LEFT JOIN cost_estimate_revision er ON er.id = requested.revision_id
         LEFT JOIN cost_estimation_run erun ON erun.id = er.run_id
         LEFT JOIN cost_estimation_preview ep ON ep.id = erun.preview_id
         LEFT JOIN pricing_rate_revision r ON r.id = COALESCE(requested.rate_id, ps.rate_revision_id)
         LEFT JOIN pricing_catalog_snapshot c ON c.id = COALESCE(requested.catalog_id, r.catalog_snapshot_id, ps.catalog_snapshot_id)");
        let rows = query.build().fetch_all(&mut *connection).await?;
        Ok(rows
            .into_iter()
            .map(|row| EffectiveUsageEventMetadataRow { row })
            .collect())
    }
}

pub async fn execution_pricing_selection_count_in_tx(
    connection: &mut sqlx::SqliteConnection,
    execution_id: &str,
) -> crate::Result<i64> {
    Ok(sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM pricing_selection WHERE source_id = ? AND domain_kind = 'execution'",
    )
    .bind(execution_id)
    .fetch_one(connection)
    .await?)
}
