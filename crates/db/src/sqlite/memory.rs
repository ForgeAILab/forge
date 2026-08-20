use super::*;
use crate::{
    ContextManifest, ContextManifestSource, CreateContextManifest, CreateContextManifestSource,
    CreateForgeMemorySourceBinding, CreateMemoryLifecycleAssertion, ForgeMemorySourceBinding,
    MemoryAccessQuery, MemoryGetQuery, MemoryItem, MemoryLifecycleAssertion, MemoryRepository,
    MemoryScopeGrant, ScopedMemoryRepository,
};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MemoryCursor {
    created_at: String,
    id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ScopedMemoryCursor {
    rank: i64,
    created_at: String,
    id: String,
}

#[derive(Debug, Clone)]
struct MemoryCandidate {
    id: String,
    rank: i64,
    created_at: String,
    grant: MemoryScopeGrant,
}

fn decode_memory_cursor(cursor: Option<String>) -> Result<Option<MemoryCursor>> {
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    let bytes = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| DbError::InvalidCursor)?;
    let cursor: MemoryCursor =
        serde_json::from_slice(&bytes).map_err(|_| DbError::InvalidCursor)?;
    if cursor.created_at.is_empty() || cursor.id.is_empty() {
        return Err(DbError::InvalidCursor);
    }
    Ok(Some(cursor))
}

fn map_memory_rows(rows: Vec<SqliteRow>) -> Result<Vec<MemoryItem>> {
    rows.into_iter()
        .map(|row| MemoryItem::from_row(&row).map_err(DbError::from))
        .collect()
}

fn literal_fts_query(query: &str) -> Option<String> {
    let terms = query
        .split_whitespace()
        .map(|term| format!("\"{}\"", term.replace('"', "\"\"")))
        .collect::<Vec<_>>();
    if terms.is_empty() {
        None
    } else {
        Some(terms.join(" "))
    }
}

fn reject_retired_room_memory(item: &MemoryItem) -> Result<()> {
    if item.source_type == "room" || item.kind == "room_message" {
        return Err(DbError::Check(
            "retired Room memory values cannot be written through the live repository".to_owned(),
        ));
    }
    Ok(())
}

#[async_trait]
impl MemoryRepository for SqliteDb {
    async fn insert_memory_item(&self, item: &MemoryItem) -> Result<()> {
        reject_retired_room_memory(item)?;
        // Legacy room_id/source_room_sequence columns intentionally remain in
        // the database for migration provenance, but live writers omit them.
        sqlx::query("INSERT INTO memory_item (id, project_id, task_id, execution_id, scope_type, scope_id, visibility, owner_identity_id, authority, sensitivity, retention_priority, provenance_json, publication_source_id, supersedes_id, valid_from, valid_until, source_event_id, source_scope_type, source_scope_id, source_revision, source_type, kind, title, summary, body, metadata_json, confidence, quality_score, created_by_type, created_by_id, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(&item.id)
            .bind(item.project_id.as_deref())
            .bind(item.task_id.as_deref())
            .bind(item.execution_id.as_deref())
            .bind(&item.scope_type)
            .bind(&item.scope_id)
            .bind(&item.visibility)
            .bind(item.owner_identity_id.as_deref())
            .bind(&item.authority)
            .bind(&item.sensitivity)
            .bind(item.retention_priority)
            .bind(&item.provenance_json)
            .bind(item.publication_source_id.as_deref())
            .bind(item.supersedes_id.as_deref())
            .bind(item.valid_from.as_deref())
            .bind(item.valid_until.as_deref())
            .bind(item.source_event_id.as_deref())
            .bind(item.source_scope_type.as_deref())
            .bind(item.source_scope_id.as_deref())
            .bind(item.source_revision.as_deref())
            .bind(&item.source_type)
            .bind(&item.kind)
            .bind(&item.title)
            .bind(item.summary.as_deref())
            .bind(&item.body)
            .bind(&item.metadata_json)
            .bind(item.confidence.as_deref())
            .bind(item.quality_score)
            .bind(item.created_by_type.as_deref())
            .bind(item.created_by_id.as_deref())
            .bind(&item.created_at)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn get_memory_item(&self, id: &str) -> Result<Option<MemoryItem>> {
        sqlx::query("SELECT * FROM memory_item WHERE id = ? AND source_type <> 'room' AND kind <> 'room_message'")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(|row| MemoryItem::from_row(&row).map_err(DbError::from))
            .transpose()
    }

    async fn memory_source_exists(
        &self,
        project_id: &str,
        source_type: &str,
        source_ref: &str,
    ) -> Result<bool> {
        let exists = sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(SELECT 1 FROM memory_item WHERE project_id = ? AND source_type = ? AND source_type <> 'room' AND kind <> 'room_message' AND CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.source_ref') END = ? LIMIT 1)",
        )
        .bind(project_id)
        .bind(source_type)
        .bind(source_ref)
        .fetch_one(&self.pool)
        .await?;
        Ok(exists != 0)
    }

    async fn memory_source_exists_with_confidence(
        &self,
        project_id: &str,
        source_type: &str,
        source_ref: &str,
        confidence: &str,
    ) -> Result<bool> {
        let exists = sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(SELECT 1 FROM memory_item WHERE project_id = ? AND source_type = ? AND source_type <> 'room' AND kind <> 'room_message' AND CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.source_ref') END = ? AND confidence = ? LIMIT 1)",
        )
        .bind(project_id)
        .bind(source_type)
        .bind(source_ref)
        .bind(confidence)
        .fetch_one(&self.pool)
        .await?;
        Ok(exists != 0)
    }

    async fn search_memory_items(
        &self,
        project_id: &str,
        query: &str,
        limit: usize,
        cursor: Option<String>,
    ) -> Result<(Vec<MemoryItem>, bool)> {
        let limit = limit.clamp(1, 500);
        let Some(fts_query) = literal_fts_query(query) else {
            return Ok((Vec::new(), false));
        };
        let cursor = decode_memory_cursor(cursor)?;
        let rows = if let Some(cursor) = cursor {
            sqlx::query(
                "SELECT memory_item.* FROM memory_item JOIN memory_item_fts ON memory_item_fts.rowid = memory_item.row_id WHERE memory_item.project_id = ? AND memory_item.source_type <> 'room' AND memory_item.kind <> 'room_message' AND memory_item_fts MATCH ? AND (memory_item.created_at < ? OR (memory_item.created_at = ? AND memory_item.id < ?)) ORDER BY memory_item.created_at DESC, memory_item.id DESC LIMIT ?",
            )
            .bind(project_id)
            .bind(&fts_query)
            .bind(&cursor.created_at)
            .bind(&cursor.created_at)
            .bind(&cursor.id)
            .bind(limit as i64 + 1)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
                "SELECT memory_item.* FROM memory_item JOIN memory_item_fts ON memory_item_fts.rowid = memory_item.row_id WHERE memory_item.project_id = ? AND memory_item.source_type <> 'room' AND memory_item.kind <> 'room_message' AND memory_item_fts MATCH ? ORDER BY memory_item.created_at DESC, memory_item.id DESC LIMIT ?",
            )
            .bind(project_id)
            .bind(&fts_query)
            .bind(limit as i64 + 1)
            .fetch_all(&self.pool)
            .await?
        };
        let mut items = map_memory_rows(rows)?;
        let has_more = items.len() > limit;
        if has_more {
            items.truncate(limit);
        }
        Ok((items, has_more))
    }

    async fn list_memory_items_by_source(
        &self,
        project_id: &str,
        source_type: &str,
        source_id: &str,
    ) -> Result<Vec<MemoryItem>> {
        let rows = sqlx::query("SELECT * FROM memory_item WHERE project_id = ? AND source_type = ? AND source_type <> 'room' AND kind <> 'room_message' AND (task_id = ? OR execution_id = ?) ORDER BY created_at DESC, id DESC")
            .bind(project_id)
            .bind(source_type)
            .bind(source_id)
            .bind(source_id)
            .fetch_all(&self.pool)
            .await?;
        map_memory_rows(rows)
    }
}

#[async_trait]
impl ScopedMemoryRepository for SqliteDb {
    async fn insert_memory_item_if_source_absent(
        &self,
        item: &MemoryItem,
        source_type: &str,
        source_ref: &str,
    ) -> Result<(MemoryItem, bool)> {
        reject_retired_room_memory(item)?;
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let existing = sqlx::query("SELECT * FROM memory_item WHERE scope_type = ? AND scope_id = ? AND source_type = ? AND source_type <> 'room' AND kind <> 'room_message' AND json_valid(metadata_json) AND json_extract(metadata_json, '$.source_ref') = ? ORDER BY created_at ASC, id ASC LIMIT 1")
            .bind(&item.scope_type)
            .bind(&item.scope_id)
            .bind(source_type)
            .bind(source_ref)
            .fetch_optional(&mut *transaction)
            .await?
            .map(|row| MemoryItem::from_row(&row).map_err(DbError::from))
            .transpose()?;
        if let Some(existing) = existing {
            transaction.commit().await?;
            return Ok((existing, false));
        }
        sqlx::query("INSERT INTO memory_item (id, project_id, task_id, execution_id, scope_type, scope_id, visibility, owner_identity_id, authority, sensitivity, retention_priority, provenance_json, publication_source_id, supersedes_id, valid_from, valid_until, source_event_id, source_scope_type, source_scope_id, source_revision, source_type, kind, title, summary, body, metadata_json, confidence, quality_score, created_by_type, created_by_id, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(&item.id)
            .bind(item.project_id.as_deref())
            .bind(item.task_id.as_deref())
            .bind(item.execution_id.as_deref())
            .bind(&item.scope_type)
            .bind(&item.scope_id)
            .bind(&item.visibility)
            .bind(item.owner_identity_id.as_deref())
            .bind(&item.authority)
            .bind(&item.sensitivity)
            .bind(item.retention_priority)
            .bind(&item.provenance_json)
            .bind(item.publication_source_id.as_deref())
            .bind(item.supersedes_id.as_deref())
            .bind(item.valid_from.as_deref())
            .bind(item.valid_until.as_deref())
            .bind(item.source_event_id.as_deref())
            .bind(item.source_scope_type.as_deref())
            .bind(item.source_scope_id.as_deref())
            .bind(item.source_revision.as_deref())
            .bind(&item.source_type)
            .bind(&item.kind)
            .bind(&item.title)
            .bind(item.summary.as_deref())
            .bind(&item.body)
            .bind(&item.metadata_json)
            .bind(item.confidence.as_deref())
            .bind(item.quality_score)
            .bind(item.created_by_type.as_deref())
            .bind(item.created_by_id.as_deref())
            .bind(&item.created_at)
            .execute(&mut *transaction)
            .await?;
        let receipt_result = sqlx::query(
            "INSERT INTO memory_source_receipt
             (source_type, source_scope_type, source_scope_id, source_ref, memory_item_id, created_at)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(source_type)
        .bind(&item.scope_type)
        .bind(&item.scope_id)
        .bind(source_ref)
        .bind(&item.id)
        .bind(&item.created_at)
        .execute(&mut *transaction)
        .await;
        if let Err(error) = receipt_result {
            if error.to_string().contains("UNIQUE") {
                transaction.rollback().await?;
                let existing_id = sqlx::query_scalar::<_, String>(
                    "SELECT memory_item_id FROM memory_source_receipt
                     WHERE source_type = ? AND source_scope_type = ?
                       AND source_scope_id = ? AND source_ref = ?",
                )
                .bind(source_type)
                .bind(&item.scope_type)
                .bind(&item.scope_id)
                .bind(source_ref)
                .fetch_optional(&self.pool)
                .await?;
                if let Some(existing_id) = existing_id {
                    let existing = self
                        .get_memory_item(&existing_id)
                        .await?
                        .ok_or(DbError::NotFound)?;
                    return Ok((existing, false));
                }
            }
            return Err(error.into());
        }
        transaction.commit().await?;
        Ok((item.clone(), true))
    }

    async fn get_memory_item_scoped(&self, query: MemoryGetQuery) -> Result<Option<MemoryItem>> {
        for grant in query.grants {
            let Some(row_id) = authorized_memory_row_id(
                &self.pool,
                &query.id,
                query.identity_id.as_deref(),
                &grant,
                query.include_retracted,
            )
            .await?
            else {
                continue;
            };
            let row = sqlx::query("SELECT * FROM memory_item WHERE row_id = ?")
                .bind(row_id)
                .fetch_optional(&self.pool)
                .await?;
            if let Some(row) = row {
                return MemoryItem::from_row(&row).map(Some).map_err(DbError::from);
            }
        }
        Ok(None)
    }

    async fn search_memory_items_scoped(
        &self,
        query: MemoryAccessQuery,
    ) -> Result<(Vec<MemoryItem>, bool)> {
        let limit = query.limit.clamp(1, 500) as usize;
        let Some(fts_query) = literal_fts_query(&query.query) else {
            return Ok((Vec::new(), false));
        };
        let cursor = decode_scoped_memory_cursor(query.cursor)?;
        let mut candidates = BTreeMap::<String, MemoryCandidate>::new();
        for grant in query.grants {
            let placeholders = std::iter::repeat_n("?", grant.visibility.len())
                .collect::<Vec<_>>()
                .join(", ");
            if placeholders.is_empty() {
                continue;
            }
            let lifecycle = if query.include_retracted {
                String::new()
            } else {
                " AND NOT EXISTS (SELECT 1 FROM memory_lifecycle_assertion AS lifecycle WHERE lifecycle.memory_item_id = memory_item.id AND lifecycle.assertion_type IN ('superseded', 'retracted', 'expired'))".to_owned()
            };
            let cursor_sql = if cursor.is_some() {
                " AND ((CASE memory_item.authority WHEN 'decision' THEN 600 WHEN 'procedure' THEN 500 WHEN 'verified_fact' THEN 450 WHEN 'proposal' THEN 300 WHEN 'hypothesis' THEN 200 ELSE 100 END + memory_item.retention_priority) < ? OR ((CASE memory_item.authority WHEN 'decision' THEN 600 WHEN 'procedure' THEN 500 WHEN 'verified_fact' THEN 450 WHEN 'proposal' THEN 300 WHEN 'hypothesis' THEN 200 ELSE 100 END + memory_item.retention_priority) = ? AND (memory_item.created_at < ? OR (memory_item.created_at = ? AND memory_item.id < ?))))"
            } else {
                ""
            };
            let sql = format!(
                "SELECT memory_item.row_id, memory_item.id, memory_item.created_at, (CASE memory_item.authority WHEN 'decision' THEN 600 WHEN 'procedure' THEN 500 WHEN 'verified_fact' THEN 450 WHEN 'proposal' THEN 300 WHEN 'hypothesis' THEN 200 ELSE 100 END + memory_item.retention_priority) AS rank FROM memory_item JOIN memory_item_fts ON memory_item_fts.rowid = memory_item.row_id WHERE memory_item.scope_type = ? AND memory_item.scope_id = ? AND memory_item.source_type <> 'room' AND memory_item.kind <> 'room_message' AND memory_item.sensitivity <> 'secret' AND (memory_item.visibility IN ({placeholders}) OR (memory_item.visibility = 'private' AND memory_item.owner_identity_id = ?)){lifecycle} AND memory_item_fts MATCH ?{cursor_sql} ORDER BY rank DESC, memory_item.created_at DESC, memory_item.id DESC LIMIT ?"
            );
            let mut statement = sqlx::query(&sql)
                .bind(&grant.scope_type)
                .bind(&grant.scope_id);
            for visibility in &grant.visibility {
                statement = statement.bind(visibility);
            }
            statement = statement.bind(query.identity_id.as_deref());
            statement = statement.bind(&fts_query);
            if let Some(cursor) = &cursor {
                statement = statement
                    .bind(cursor.rank)
                    .bind(cursor.rank)
                    .bind(&cursor.created_at)
                    .bind(&cursor.created_at)
                    .bind(&cursor.id);
            }
            let rows = statement
                .bind(limit as i64 + 1)
                .fetch_all(&self.pool)
                .await?;
            for row in rows {
                let candidate = MemoryCandidate {
                    id: row.try_get("id")?,
                    rank: row.try_get("rank")?,
                    created_at: row.try_get("created_at")?,
                    grant: grant.clone(),
                };
                candidates
                    .entry(candidate.id.clone())
                    .and_modify(|current| {
                        if (candidate.rank, &candidate.created_at, &candidate.id)
                            > (current.rank, &current.created_at, &current.id)
                        {
                            *current = candidate.clone();
                        }
                    })
                    .or_insert(candidate);
            }
        }
        let mut candidates = candidates.into_values().collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            right
                .rank
                .cmp(&left.rank)
                .then_with(|| right.created_at.cmp(&left.created_at))
                .then_with(|| right.id.cmp(&left.id))
        });
        let has_more = candidates.len() > limit;
        candidates.truncate(limit);
        let mut items = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let Some(item) = self
                .get_memory_item_scoped(MemoryGetQuery {
                    id: candidate.id,
                    identity_id: query.identity_id.clone(),
                    grants: vec![candidate.grant],
                    include_retracted: query.include_retracted,
                })
                .await?
            else {
                continue;
            };
            items.push(item);
        }
        Ok((items, has_more))
    }

    async fn insert_memory_lifecycle_assertion(
        &self,
        input: CreateMemoryLifecycleAssertion,
    ) -> Result<MemoryLifecycleAssertion> {
        sqlx::query("INSERT INTO memory_lifecycle_assertion (id, memory_item_id, assertion_type, related_memory_id, reason, evidence_json, asserted_by_type, asserted_by_id, source_event_id, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(&input.id)
            .bind(&input.memory_item_id)
            .bind(&input.assertion_type)
            .bind(input.related_memory_id.as_deref())
            .bind(input.reason.as_deref())
            .bind(&input.evidence_json)
            .bind(&input.asserted_by_type)
            .bind(input.asserted_by_id.as_deref())
            .bind(input.source_event_id.as_deref())
            .bind(&input.created_at)
            .execute(&self.pool)
            .await?;
        self.list_memory_lifecycle_assertions(&input.memory_item_id)
            .await?
            .into_iter()
            .find(|assertion| assertion.id == input.id)
            .ok_or(DbError::NotFound)
    }

    async fn list_memory_lifecycle_assertions(
        &self,
        memory_item_id: &str,
    ) -> Result<Vec<MemoryLifecycleAssertion>> {
        sqlx::query("SELECT * FROM memory_lifecycle_assertion WHERE memory_item_id = ? ORDER BY created_at ASC, id ASC")
            .bind(memory_item_id)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(map_memory_lifecycle_assertion)
            .collect()
    }

    async fn create_memory_source_binding(
        &self,
        input: CreateForgeMemorySourceBinding,
    ) -> Result<ForgeMemorySourceBinding> {
        // The legacy room_id column remains for migration provenance; live
        // bindings intentionally leave it NULL by omitting it from INSERT.
        sqlx::query("INSERT INTO forge_memory_source_binding (id, identity_id, context_scope_id, scope_type, scope_id, account_id, project_id, task_id, policy_revision, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(&input.id)
            .bind(&input.identity_id)
            .bind(&input.context_scope_id)
            .bind(&input.scope_type)
            .bind(&input.scope_id)
            .bind(input.account_id.as_deref())
            .bind(input.project_id.as_deref())
            .bind(input.task_id.as_deref())
            .bind(&input.policy_revision)
            .bind(&input.created_at)
            .execute(&self.pool)
            .await?;
        self.get_memory_source_binding(&input.identity_id, &input.context_scope_id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn get_memory_source_binding(
        &self,
        identity_id: &str,
        context_scope_id: &str,
    ) -> Result<Option<ForgeMemorySourceBinding>> {
        sqlx::query("SELECT * FROM forge_memory_source_binding WHERE identity_id = ? AND context_scope_id = ?")
            .bind(identity_id)
            .bind(context_scope_id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_memory_source_binding)
            .transpose()
    }

    async fn create_context_manifest(
        &self,
        input: CreateContextManifest,
    ) -> Result<ContextManifest> {
        sqlx::query("INSERT INTO context_manifest (id, identity_id, agent_session_id, context_scope_id, scope_type, scope_id, policy_revision, domain_revision, lcm_binding_revision, runtime_manifest_id, runtime_manifest_fingerprint, combined_fingerprint, request_fingerprint, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(&input.id)
            .bind(&input.identity_id)
            .bind(input.agent_session_id.as_deref())
            .bind(&input.context_scope_id)
            .bind(&input.scope_type)
            .bind(&input.scope_id)
            .bind(&input.policy_revision)
            .bind(&input.domain_revision)
            .bind(input.lcm_binding_revision.as_deref())
            .bind(input.runtime_manifest_id.as_deref())
            .bind(input.runtime_manifest_fingerprint.as_deref())
            .bind(&input.combined_fingerprint)
            .bind(&input.request_fingerprint)
            .bind(&input.created_at)
            .execute(&self.pool)
            .await?;
        self.get_context_manifest(&input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn append_context_manifest_source(
        &self,
        input: CreateContextManifestSource,
    ) -> Result<ContextManifestSource> {
        sqlx::query("INSERT INTO context_manifest_source (manifest_id, ordinal, source_id, source_type, source_revision, selection_reason, disposition, retention_priority, fragment_fingerprint) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(&input.manifest_id)
            .bind(input.ordinal)
            .bind(&input.source_id)
            .bind(&input.source_type)
            .bind(&input.source_revision)
            .bind(&input.selection_reason)
            .bind(&input.disposition)
            .bind(input.retention_priority)
            .bind(&input.fragment_fingerprint)
            .execute(&self.pool)
            .await?;
        self.list_context_manifest_sources(&input.manifest_id)
            .await?
            .into_iter()
            .find(|source| source.ordinal == input.ordinal)
            .ok_or(DbError::NotFound)
    }

    async fn get_context_manifest(&self, id: &str) -> Result<Option<ContextManifest>> {
        sqlx::query("SELECT * FROM context_manifest WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_context_manifest)
            .transpose()
    }

    async fn get_context_manifest_scoped(
        &self,
        id: &str,
        identity_id: &str,
        context_scope_id: &str,
    ) -> Result<Option<ContextManifest>> {
        sqlx::query(
            "SELECT * FROM context_manifest
             WHERE id = ? AND identity_id = ? AND context_scope_id = ?",
        )
        .bind(id)
        .bind(identity_id)
        .bind(context_scope_id)
        .fetch_optional(&self.pool)
        .await?
        .map(map_context_manifest)
        .transpose()
    }

    async fn list_context_manifests_scoped(
        &self,
        identity_id: &str,
        context_scope_id: Option<&str>,
        limit: i64,
    ) -> Result<Vec<ContextManifest>> {
        sqlx::query(
            "SELECT * FROM context_manifest
             WHERE identity_id = ?
               AND (? IS NULL OR context_scope_id = ?)
             ORDER BY created_at DESC, id DESC
             LIMIT ?",
        )
        .bind(identity_id)
        .bind(context_scope_id)
        .bind(context_scope_id)
        .bind(limit.clamp(1, 100))
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_context_manifest)
        .collect()
    }

    async fn list_context_manifest_sources(
        &self,
        manifest_id: &str,
    ) -> Result<Vec<ContextManifestSource>> {
        sqlx::query(
            "SELECT * FROM context_manifest_source WHERE manifest_id = ? ORDER BY ordinal ASC",
        )
        .bind(manifest_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_context_manifest_source)
        .collect()
    }
}

async fn authorized_memory_row_id(
    pool: &SqlitePool,
    id: &str,
    identity_id: Option<&str>,
    grant: &MemoryScopeGrant,
    include_retracted: bool,
) -> Result<Option<i64>> {
    let placeholders = std::iter::repeat_n("?", grant.visibility.len())
        .collect::<Vec<_>>()
        .join(", ");
    if placeholders.is_empty() {
        return Ok(None);
    }
    let lifecycle = if include_retracted {
        String::new()
    } else {
        " AND NOT EXISTS (SELECT 1 FROM memory_lifecycle_assertion AS lifecycle WHERE lifecycle.memory_item_id = memory_item.id AND lifecycle.assertion_type IN ('superseded', 'retracted', 'expired'))".to_owned()
    };
    let sql = format!(
        "SELECT memory_item.row_id FROM memory_item WHERE memory_item.id = ? AND memory_item.scope_type = ? AND memory_item.scope_id = ? AND memory_item.source_type <> 'room' AND memory_item.kind <> 'room_message' AND memory_item.sensitivity <> 'secret' AND (memory_item.visibility IN ({placeholders}) OR (memory_item.visibility = 'private' AND memory_item.owner_identity_id = ?)){lifecycle} LIMIT 1"
    );
    let mut statement = sqlx::query(&sql)
        .bind(id)
        .bind(&grant.scope_type)
        .bind(&grant.scope_id);
    for visibility in &grant.visibility {
        statement = statement.bind(visibility);
    }
    statement = statement.bind(identity_id);
    Ok(statement
        .fetch_optional(pool)
        .await?
        .map(|row| row.try_get("row_id"))
        .transpose()?)
}

fn decode_scoped_memory_cursor(cursor: Option<String>) -> Result<Option<ScopedMemoryCursor>> {
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    let bytes = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| DbError::InvalidCursor)?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|_| DbError::InvalidCursor)
}

fn map_memory_lifecycle_assertion(
    row: SqliteRow,
) -> std::result::Result<MemoryLifecycleAssertion, DbError> {
    Ok(MemoryLifecycleAssertion {
        id: row.try_get("id")?,
        memory_item_id: row.try_get("memory_item_id")?,
        assertion_type: row.try_get("assertion_type")?,
        related_memory_id: row.try_get("related_memory_id")?,
        reason: row.try_get("reason")?,
        evidence_json: row.try_get("evidence_json")?,
        asserted_by_type: row.try_get("asserted_by_type")?,
        asserted_by_id: row.try_get("asserted_by_id")?,
        source_event_id: row.try_get("source_event_id")?,
        created_at: row.try_get("created_at")?,
    })
}

fn map_memory_source_binding(
    row: SqliteRow,
) -> std::result::Result<ForgeMemorySourceBinding, DbError> {
    Ok(ForgeMemorySourceBinding {
        id: row.try_get("id")?,
        identity_id: row.try_get("identity_id")?,
        context_scope_id: row.try_get("context_scope_id")?,
        scope_type: row.try_get("scope_type")?,
        scope_id: row.try_get("scope_id")?,
        account_id: row.try_get("account_id")?,
        project_id: row.try_get("project_id")?,
        task_id: row.try_get("task_id")?,
        policy_revision: row.try_get("policy_revision")?,
        created_at: row.try_get("created_at")?,
    })
}

fn map_context_manifest(row: SqliteRow) -> std::result::Result<ContextManifest, DbError> {
    Ok(ContextManifest {
        id: row.try_get("id")?,
        identity_id: row.try_get("identity_id")?,
        agent_session_id: row.try_get("agent_session_id")?,
        context_scope_id: row.try_get("context_scope_id")?,
        scope_type: row.try_get("scope_type")?,
        scope_id: row.try_get("scope_id")?,
        policy_revision: row.try_get("policy_revision")?,
        domain_revision: row.try_get("domain_revision")?,
        lcm_binding_revision: row.try_get("lcm_binding_revision")?,
        runtime_manifest_id: row.try_get("runtime_manifest_id")?,
        runtime_manifest_fingerprint: row.try_get("runtime_manifest_fingerprint")?,
        combined_fingerprint: row.try_get("combined_fingerprint")?,
        request_fingerprint: row.try_get("request_fingerprint")?,
        created_at: row.try_get("created_at")?,
    })
}

fn map_context_manifest_source(
    row: SqliteRow,
) -> std::result::Result<ContextManifestSource, DbError> {
    Ok(ContextManifestSource {
        manifest_id: row.try_get("manifest_id")?,
        ordinal: row.try_get("ordinal")?,
        source_id: row.try_get("source_id")?,
        source_type: row.try_get("source_type")?,
        source_revision: row.try_get("source_revision")?,
        selection_reason: row.try_get("selection_reason")?,
        disposition: row.try_get("disposition")?,
        retention_priority: row.try_get("retention_priority")?,
        fragment_fingerprint: row.try_get("fragment_fingerprint")?,
    })
}
