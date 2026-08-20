use std::collections::HashSet;

use async_trait::async_trait;
use sqlx::{sqlite::SqliteRow, Row, Sqlite, Transaction};

use crate::{
    AgentLcmEntryRecord, AgentLcmMutationResult, AgentLcmNodeRecord, AgentLcmOperation,
    AgentLcmRepo, AgentLcmTimeline, AgentLcmTruncation, AppendAgentLcmEntries,
    CommitAgentLcmCondensation, CommitAgentLcmLeaf, CreateAgentLcmTimeline, DbError, Result,
    SqliteDb,
};

const MAX_LCM_PAGE: i64 = 1_024;

#[async_trait]
impl AgentLcmRepo for SqliteDb {
    async fn create_or_get_lcm_timeline(
        &self,
        input: CreateAgentLcmTimeline,
    ) -> Result<AgentLcmTimeline> {
        sqlx::query(
            "INSERT INTO agent_lcm_timeline (
                id, identity_id, scope_type, scope_id, authorization_revision,
                revision, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, 0, ?, ?)
             ON CONFLICT(identity_id, scope_type, scope_id) DO NOTHING",
        )
        .bind(&input.id)
        .bind(&input.identity_id)
        .bind(&input.scope_type)
        .bind(&input.scope_id)
        .bind(&input.authorization_revision)
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(self.pool())
        .await?;

        let timeline = self
            .get_lcm_timeline_for_binding(&input.identity_id, &input.scope_type, &input.scope_id)
            .await?
            .ok_or(DbError::NotFound)?;
        if timeline.authorization_revision != input.authorization_revision {
            return Err(DbError::Check(
                "LCM binding already exists with a different authorization revision".to_owned(),
            ));
        }
        Ok(timeline)
    }

    async fn get_lcm_timeline(&self, id: &str) -> Result<Option<AgentLcmTimeline>> {
        sqlx::query("SELECT * FROM agent_lcm_timeline WHERE id = ?")
            .bind(id)
            .fetch_optional(self.pool())
            .await?
            .map(map_timeline)
            .transpose()
    }

    async fn get_lcm_timeline_for_binding(
        &self,
        identity_id: &str,
        scope_type: &str,
        scope_id: &str,
    ) -> Result<Option<AgentLcmTimeline>> {
        sqlx::query(
            "SELECT * FROM agent_lcm_timeline
             WHERE identity_id = ? AND scope_type = ? AND scope_id = ?",
        )
        .bind(identity_id)
        .bind(scope_type)
        .bind(scope_id)
        .fetch_optional(self.pool())
        .await?
        .map(map_timeline)
        .transpose()
    }

    async fn list_lcm_entries(
        &self,
        timeline_id: &str,
        start: i64,
        end: i64,
        limit: i64,
    ) -> Result<Vec<AgentLcmEntryRecord>> {
        if start < 0 || end < start {
            return Err(DbError::Check("invalid LCM entry range".to_owned()));
        }
        sqlx::query(
            "SELECT * FROM agent_lcm_entry
             WHERE timeline_id = ? AND sequence BETWEEN ? AND ?
             ORDER BY sequence ASC LIMIT ?",
        )
        .bind(timeline_id)
        .bind(start)
        .bind(end)
        .bind(limit.clamp(1, MAX_LCM_PAGE))
        .fetch_all(self.pool())
        .await?
        .into_iter()
        .map(map_entry)
        .collect()
    }

    async fn list_lcm_nodes(
        &self,
        timeline_id: &str,
        active_only: bool,
    ) -> Result<Vec<AgentLcmNodeRecord>> {
        let query = if active_only {
            "SELECT * FROM agent_lcm_node
             WHERE timeline_id = ? AND superseded_by IS NULL
             ORDER BY range_start ASC, range_end ASC, node_id ASC"
        } else {
            "SELECT * FROM agent_lcm_node
             WHERE timeline_id = ?
             ORDER BY range_start ASC, range_end ASC, node_id ASC"
        };
        sqlx::query(query)
            .bind(timeline_id)
            .fetch_all(self.pool())
            .await?
            .into_iter()
            .map(map_node)
            .collect()
    }

    async fn get_lcm_node(
        &self,
        timeline_id: &str,
        node_id: &str,
    ) -> Result<Option<AgentLcmNodeRecord>> {
        sqlx::query("SELECT * FROM agent_lcm_node WHERE timeline_id = ? AND node_id = ?")
            .bind(timeline_id)
            .bind(node_id)
            .fetch_optional(self.pool())
            .await?
            .map(map_node)
            .transpose()
    }

    async fn get_lcm_operation(
        &self,
        timeline_id: &str,
        operation_id: &str,
    ) -> Result<Option<AgentLcmOperation>> {
        sqlx::query(
            "SELECT * FROM agent_lcm_operation
             WHERE timeline_id = ? AND operation_id = ?",
        )
        .bind(timeline_id)
        .bind(operation_id)
        .fetch_optional(self.pool())
        .await?
        .map(map_operation)
        .transpose()
    }

    async fn append_lcm_entries(
        &self,
        input: AppendAgentLcmEntries,
    ) -> Result<AgentLcmMutationResult> {
        let mut transaction = crate::begin_immediate(self.pool()).await?;
        if let Some(existing) = existing_operation(
            &mut transaction,
            &input.timeline_id,
            &input.operation_id,
            &input.operation_fingerprint,
            "append",
        )
        .await?
        {
            transaction.commit().await?;
            return Ok(operation_result(existing, true));
        }
        ensure_operation_fingerprint_available(
            &mut transaction,
            &input.timeline_id,
            &input.operation_fingerprint,
        )
        .await?;
        let current = timeline_revision(&mut transaction, &input.timeline_id).await?;
        if current != input.expected_revision {
            return Err(DbError::VersionConflict);
        }

        let mut entry_ids = HashSet::new();
        let mut expected_sequence = input.expected_sequence;
        let mut all_existing = !input.entries.is_empty();
        for entry in &input.entries {
            if entry.timeline_id != input.timeline_id || !entry_ids.insert(&entry.entry_id) {
                return Err(DbError::Check(
                    "LCM entry crosses timeline or repeats an id".to_owned(),
                ));
            }
            if entry.sequence != expected_sequence {
                return Err(DbError::Check(format!(
                    "LCM append sequence gap expected {} actual {}",
                    expected_sequence, entry.sequence
                )));
            }
            let existing = sqlx::query(
                "SELECT * FROM agent_lcm_entry
                 WHERE timeline_id = ? AND (entry_id = ? OR sequence = ?)",
            )
            .bind(&input.timeline_id)
            .bind(&entry.entry_id)
            .bind(entry.sequence)
            .fetch_optional(&mut *transaction)
            .await?;
            if let Some(row) = existing {
                let existing = map_entry(row)?;
                if existing != *entry {
                    return Err(DbError::Check("LCM immutable entry conflict".to_owned()));
                }
            } else {
                all_existing = false;
            }
            expected_sequence = expected_sequence
                .checked_add(1)
                .ok_or_else(|| DbError::Check("LCM sequence exhausted".to_owned()))?;
        }

        let revision = if input.entries.is_empty() || all_existing {
            current
        } else {
            let next = current
                .checked_add(1)
                .ok_or_else(|| DbError::Check("LCM revision exhausted".to_owned()))?;
            for entry in &input.entries {
                sqlx::query(
                    "INSERT INTO agent_lcm_entry (
                        timeline_id, entry_id, sequence, content_json,
                        content_fingerprint, source_json, created_at
                     ) VALUES (?, ?, ?, ?, ?, ?, ?)",
                )
                .bind(&entry.timeline_id)
                .bind(&entry.entry_id)
                .bind(entry.sequence)
                .bind(&entry.content_json)
                .bind(&entry.content_fingerprint)
                .bind(&entry.source_json)
                .bind(&entry.created_at)
                .execute(&mut *transaction)
                .await?;
            }
            update_timeline_revision(
                &mut transaction,
                &input.timeline_id,
                next,
                &input.updated_at,
            )
            .await?;
            next
        };
        insert_operation(
            &mut transaction,
            &input.timeline_id,
            &input.operation_id,
            "append",
            &input.operation_fingerprint,
            revision,
            input.entries.len() as i64,
            None,
            &input.updated_at,
        )
        .await?;
        transaction.commit().await?;
        Ok(AgentLcmMutationResult {
            revision,
            already_committed: input.entries.is_empty() || all_existing,
            entries: input.entries.len() as i64,
            node_id: None,
        })
    }

    async fn truncate_lcm_entries_from(
        &self,
        timeline_id: &str,
        from_sequence: i64,
        updated_at: &str,
    ) -> Result<AgentLcmTruncation> {
        let mut transaction = crate::begin_immediate(self.pool()).await?;
        let current = timeline_revision(&mut transaction, timeline_id).await?;
        let node_reaches_span: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                SELECT 1 FROM agent_lcm_node
                WHERE timeline_id = ? AND range_end >= ?
             )",
        )
        .bind(timeline_id)
        .bind(from_sequence)
        .fetch_one(&mut *transaction)
        .await?;
        if node_reaches_span {
            return Err(DbError::Check(
                "LCM truncation reaches a summary node's source range".to_owned(),
            ));
        }
        let removed =
            sqlx::query("DELETE FROM agent_lcm_entry WHERE timeline_id = ? AND sequence >= ?")
                .bind(timeline_id)
                .bind(from_sequence)
                .execute(&mut *transaction)
                .await?
                .rows_affected();
        let revision = if removed == 0 {
            current
        } else {
            let next = current
                .checked_add(1)
                .ok_or_else(|| DbError::Check("LCM revision exhausted".to_owned()))?;
            update_timeline_revision(&mut transaction, timeline_id, next, updated_at).await?;
            next
        };
        transaction.commit().await?;
        Ok(AgentLcmTruncation {
            revision,
            removed: i64::try_from(removed)
                .map_err(|_| DbError::Check("LCM truncation count exceeds i64".to_owned()))?,
        })
    }

    async fn commit_lcm_leaf(&self, input: CommitAgentLcmLeaf) -> Result<AgentLcmMutationResult> {
        let mut transaction = crate::begin_immediate(self.pool()).await?;
        if let Some(existing) = existing_operation(
            &mut transaction,
            &input.timeline_id,
            &input.operation_id,
            &input.operation_fingerprint,
            "leaf",
        )
        .await?
        {
            transaction.commit().await?;
            return Ok(operation_result(existing, true));
        }
        ensure_operation_fingerprint_available(
            &mut transaction,
            &input.timeline_id,
            &input.operation_fingerprint,
        )
        .await?;
        let current = timeline_revision(&mut transaction, &input.timeline_id).await?;
        if current != input.expected_revision {
            return Err(DbError::VersionConflict);
        }
        validate_node_record(&input.node, &input.timeline_id, "leaf")?;
        if input.node.revision != current + 1 {
            return Err(DbError::Check(
                "LCM node revision does not match CAS revision".to_owned(),
            ));
        }
        if input.entry_ids.is_empty()
            || input.entry_ids.len() as i64 != input.node.range_end - input.node.range_start + 1
        {
            return Err(DbError::Check("LCM leaf edge range is invalid".to_owned()));
        }
        let mut seen = HashSet::new();
        for (offset, entry_id) in input.entry_ids.iter().enumerate() {
            if !seen.insert(entry_id) {
                return Err(DbError::Check("LCM leaf repeats an entry".to_owned()));
            }
            let sequence = input.node.range_start + offset as i64;
            let row = sqlx::query(
                "SELECT entry_id FROM agent_lcm_entry
                 WHERE timeline_id = ? AND entry_id = ? AND sequence = ?",
            )
            .bind(&input.timeline_id)
            .bind(entry_id)
            .bind(sequence)
            .fetch_optional(&mut *transaction)
            .await?;
            if row.is_none() {
                return Err(DbError::NotFound);
            }
        }
        let overlap = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agent_lcm_node
             WHERE timeline_id = ? AND kind = 'leaf' AND superseded_by IS NULL
               AND range_start <= ? AND range_end >= ?",
        )
        .bind(&input.timeline_id)
        .bind(input.node.range_end)
        .bind(input.node.range_start)
        .fetch_one(&mut *transaction)
        .await?;
        if overlap > 0 {
            return Err(DbError::Check(
                "LCM leaf range overlaps an active leaf".to_owned(),
            ));
        }
        insert_node(&mut transaction, &input.node).await?;
        let revision = current + 1;
        update_timeline_revision(
            &mut transaction,
            &input.timeline_id,
            revision,
            &input.updated_at,
        )
        .await?;
        insert_operation(
            &mut transaction,
            &input.timeline_id,
            &input.operation_id,
            "leaf",
            &input.operation_fingerprint,
            revision,
            0,
            Some(&input.node.node_id),
            &input.updated_at,
        )
        .await?;
        transaction.commit().await?;
        Ok(AgentLcmMutationResult {
            revision,
            already_committed: false,
            entries: 0,
            node_id: Some(input.node.node_id),
        })
    }

    async fn commit_lcm_condensation(
        &self,
        input: CommitAgentLcmCondensation,
    ) -> Result<AgentLcmMutationResult> {
        let mut transaction = crate::begin_immediate(self.pool()).await?;
        if let Some(existing) = existing_operation(
            &mut transaction,
            &input.timeline_id,
            &input.operation_id,
            &input.operation_fingerprint,
            "condensation",
        )
        .await?
        {
            transaction.commit().await?;
            return Ok(operation_result(existing, true));
        }
        ensure_operation_fingerprint_available(
            &mut transaction,
            &input.timeline_id,
            &input.operation_fingerprint,
        )
        .await?;
        let current = timeline_revision(&mut transaction, &input.timeline_id).await?;
        if current != input.expected_revision {
            return Err(DbError::VersionConflict);
        }
        validate_node_record(&input.node, &input.timeline_id, "condensed")?;
        if input.node.revision != current + 1 || input.child_node_ids.len() < 2 {
            return Err(DbError::Check(
                "LCM condensation revision or children are invalid".to_owned(),
            ));
        }
        let mut seen = HashSet::new();
        for child_id in &input.child_node_ids {
            if !seen.insert(child_id) {
                return Err(DbError::Check(
                    "LCM condensation repeats a child".to_owned(),
                ));
            }
            let row = sqlx::query(
                "SELECT node_id FROM agent_lcm_node
                 WHERE timeline_id = ? AND node_id = ? AND superseded_by IS NULL",
            )
            .bind(&input.timeline_id)
            .bind(child_id)
            .fetch_optional(&mut *transaction)
            .await?;
            if row.is_none() {
                return Err(DbError::NotFound);
            }
        }
        insert_node(&mut transaction, &input.node).await?;
        for child_id in &input.child_node_ids {
            let result = sqlx::query(
                "UPDATE agent_lcm_node
                 SET superseded_by = ?
                 WHERE timeline_id = ? AND node_id = ? AND superseded_by IS NULL",
            )
            .bind(&input.node.node_id)
            .bind(&input.timeline_id)
            .bind(child_id)
            .execute(&mut *transaction)
            .await?;
            if result.rows_affected() != 1 {
                return Err(DbError::VersionConflict);
            }
        }
        let revision = current + 1;
        update_timeline_revision(
            &mut transaction,
            &input.timeline_id,
            revision,
            &input.updated_at,
        )
        .await?;
        insert_operation(
            &mut transaction,
            &input.timeline_id,
            &input.operation_id,
            "condensation",
            &input.operation_fingerprint,
            revision,
            0,
            Some(&input.node.node_id),
            &input.updated_at,
        )
        .await?;
        transaction.commit().await?;
        Ok(AgentLcmMutationResult {
            revision,
            already_committed: false,
            entries: 0,
            node_id: Some(input.node.node_id),
        })
    }
}

async fn timeline_revision(
    transaction: &mut Transaction<'_, Sqlite>,
    timeline_id: &str,
) -> Result<i64> {
    sqlx::query_scalar("SELECT revision FROM agent_lcm_timeline WHERE id = ?")
        .bind(timeline_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(DbError::NotFound)
}

async fn update_timeline_revision(
    transaction: &mut Transaction<'_, Sqlite>,
    timeline_id: &str,
    revision: i64,
    updated_at: &str,
) -> Result<()> {
    let result =
        sqlx::query("UPDATE agent_lcm_timeline SET revision = ?, updated_at = ? WHERE id = ?")
            .bind(revision)
            .bind(updated_at)
            .bind(timeline_id)
            .execute(&mut **transaction)
            .await?;
    if result.rows_affected() != 1 {
        return Err(DbError::NotFound);
    }
    Ok(())
}

async fn existing_operation(
    transaction: &mut Transaction<'_, Sqlite>,
    timeline_id: &str,
    operation_id: &str,
    operation_fingerprint: &str,
    operation_kind: &str,
) -> Result<Option<AgentLcmOperation>> {
    let Some(row) = sqlx::query(
        "SELECT * FROM agent_lcm_operation
         WHERE timeline_id = ? AND operation_id = ?",
    )
    .bind(timeline_id)
    .bind(operation_id)
    .fetch_optional(&mut **transaction)
    .await?
    else {
        return Ok(None);
    };
    let existing = map_operation(row)?;
    if existing.operation_kind != operation_kind
        || existing.operation_fingerprint != operation_fingerprint
    {
        return Err(DbError::Check(
            "LCM operation identity was reused".to_owned(),
        ));
    }
    Ok(Some(existing))
}

async fn ensure_operation_fingerprint_available(
    transaction: &mut Transaction<'_, Sqlite>,
    timeline_id: &str,
    operation_fingerprint: &str,
) -> Result<()> {
    let existing = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM agent_lcm_operation
         WHERE timeline_id = ? AND operation_fingerprint = ?",
    )
    .bind(timeline_id)
    .bind(operation_fingerprint)
    .fetch_one(&mut **transaction)
    .await?;
    if existing > 0 {
        return Err(DbError::Check(
            "LCM operation fingerprint was reused".to_owned(),
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn insert_operation(
    transaction: &mut Transaction<'_, Sqlite>,
    timeline_id: &str,
    operation_id: &str,
    operation_kind: &str,
    operation_fingerprint: &str,
    result_revision: i64,
    result_entries: i64,
    result_node_id: Option<&str>,
    created_at: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO agent_lcm_operation (
            timeline_id, operation_id, operation_kind, operation_fingerprint,
            result_revision, result_entries, result_node_id, created_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(timeline_id)
    .bind(operation_id)
    .bind(operation_kind)
    .bind(operation_fingerprint)
    .bind(result_revision)
    .bind(result_entries)
    .bind(result_node_id)
    .bind(created_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn insert_node(
    transaction: &mut Transaction<'_, Sqlite>,
    node: &AgentLcmNodeRecord,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO agent_lcm_node (
            timeline_id, node_id, kind, range_start, range_end, edges_json,
            source_fingerprint, summary_revision, summary, policy_revision,
            algorithm_revision, sizer_revision, provenance_json, token_count,
            source_token_count, classification_json, revision, superseded_by,
            operation_id, operation_fingerprint, created_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&node.timeline_id)
    .bind(&node.node_id)
    .bind(&node.kind)
    .bind(node.range_start)
    .bind(node.range_end)
    .bind(&node.edges_json)
    .bind(&node.source_fingerprint)
    .bind(&node.summary_revision)
    .bind(&node.summary)
    .bind(&node.policy_revision)
    .bind(&node.algorithm_revision)
    .bind(&node.sizer_revision)
    .bind(&node.provenance_json)
    .bind(node.token_count)
    .bind(node.source_token_count)
    .bind(&node.classification_json)
    .bind(node.revision)
    .bind(node.superseded_by.as_deref())
    .bind(&node.operation_id)
    .bind(&node.operation_fingerprint)
    .bind(&node.created_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn validate_node_record(node: &AgentLcmNodeRecord, timeline_id: &str, kind: &str) -> Result<()> {
    if node.timeline_id != timeline_id
        || node.kind != kind
        || node.range_start < 0
        || node.range_end < node.range_start
        || node.summary.trim().is_empty()
        || node.source_token_count <= node.token_count
    {
        return Err(DbError::Check("invalid LCM node record".to_owned()));
    }
    Ok(())
}

fn operation_result(
    operation: AgentLcmOperation,
    already_committed: bool,
) -> AgentLcmMutationResult {
    AgentLcmMutationResult {
        revision: operation.result_revision,
        already_committed,
        entries: operation.result_entries,
        node_id: operation.result_node_id,
    }
}

fn map_timeline(row: SqliteRow) -> Result<AgentLcmTimeline> {
    Ok(AgentLcmTimeline {
        id: row.try_get("id")?,
        identity_id: row.try_get("identity_id")?,
        scope_type: row.try_get("scope_type")?,
        scope_id: row.try_get("scope_id")?,
        authorization_revision: row.try_get("authorization_revision")?,
        revision: row.try_get("revision")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_entry(row: SqliteRow) -> Result<AgentLcmEntryRecord> {
    Ok(AgentLcmEntryRecord {
        timeline_id: row.try_get("timeline_id")?,
        entry_id: row.try_get("entry_id")?,
        sequence: row.try_get("sequence")?,
        content_json: row.try_get("content_json")?,
        content_fingerprint: row.try_get("content_fingerprint")?,
        source_json: row.try_get("source_json")?,
        created_at: row.try_get("created_at")?,
    })
}

fn map_node(row: SqliteRow) -> Result<AgentLcmNodeRecord> {
    Ok(AgentLcmNodeRecord {
        timeline_id: row.try_get("timeline_id")?,
        node_id: row.try_get("node_id")?,
        kind: row.try_get("kind")?,
        range_start: row.try_get("range_start")?,
        range_end: row.try_get("range_end")?,
        edges_json: row.try_get("edges_json")?,
        source_fingerprint: row.try_get("source_fingerprint")?,
        summary_revision: row.try_get("summary_revision")?,
        summary: row.try_get("summary")?,
        policy_revision: row.try_get("policy_revision")?,
        algorithm_revision: row.try_get("algorithm_revision")?,
        sizer_revision: row.try_get("sizer_revision")?,
        provenance_json: row.try_get("provenance_json")?,
        token_count: row.try_get("token_count")?,
        source_token_count: row.try_get("source_token_count")?,
        classification_json: row.try_get("classification_json")?,
        revision: row.try_get("revision")?,
        superseded_by: row.try_get("superseded_by")?,
        operation_id: row.try_get("operation_id")?,
        operation_fingerprint: row.try_get("operation_fingerprint")?,
        created_at: row.try_get("created_at")?,
    })
}

fn map_operation(row: SqliteRow) -> Result<AgentLcmOperation> {
    Ok(AgentLcmOperation {
        timeline_id: row.try_get("timeline_id")?,
        operation_id: row.try_get("operation_id")?,
        operation_kind: row.try_get("operation_kind")?,
        operation_fingerprint: row.try_get("operation_fingerprint")?,
        result_revision: row.try_get("result_revision")?,
        result_entries: row.try_get("result_entries")?,
        result_node_id: row.try_get("result_node_id")?,
        created_at: row.try_get("created_at")?,
    })
}
