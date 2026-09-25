mod relevance;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use db::{
    now_rfc3339, CreateForgeMemorySourceBinding, MemoryAccessQuery, MemoryItem,
    ScopedMemoryRepository, SqliteDb,
};
use serde_json::Value;
use uuid::Uuid;

use crate::{MemoryAccessContext, Result, ServiceError};

const MAX_RECALL_RESULTS: u32 = 12;
const MAX_RECALL_ARM_RESULTS: u32 = 32;

/// The immutable admission identity used to construct a ForgeMemorySource.
/// The source never accepts a caller-supplied scope after construction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemorySourceBindingInput {
    pub binding_id: Uuid,
    pub identity_id: Uuid,
    pub context_scope_id: Uuid,
    pub scope_type: String,
    pub scope_id: String,
    pub account_id: Option<String>,
    pub project_id: Option<String>,
    pub task_id: Option<String>,
    pub policy_revision: String,
    pub access: MemoryAccessContext,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgeMemoryQuery {
    pub query: String,
    pub limit: u32,
    /// Stable source ids already represented by recent canonical Agent Chat
    /// history or an admitted LCM timeline. Legacy transcript ids are treated
    /// as migration provenance and are suppressed after ACL filtering.
    pub represented_source_ids: Vec<String>,
    pub cursor: Option<String>,
}

/// Non-pageable, bounded recall for agent context construction.
///
/// Recall is deliberately distinct from [`ForgeMemoryQuery`]: paginated search
/// keeps its stable authority/recency cursor, while recall may combine several
/// authorized lexical result sets and reorder them by query relevance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgeMemoryRecallQuery {
    pub query: String,
    pub limit: u32,
    /// Stable source ids already represented by recent canonical Agent Chat
    /// history or an admitted LCM timeline.
    pub represented_source_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgeMemoryRecord {
    pub id: Uuid,
    pub revision: String,
    pub authority: String,
    pub title: String,
    pub summary: Option<String>,
    pub body: String,
    pub sensitivity: String,
    pub retention_priority: i64,
    pub source_type: String,
    pub source_ref: Option<String>,
    pub provenance_json: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgeMemoryRecallRecord {
    pub record: ForgeMemoryRecord,
    pub matched_terms: Vec<String>,
    pub selection_reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgeMemoryRecall {
    pub records: Vec<ForgeMemoryRecallRecord>,
    pub query_terms: Vec<String>,
    pub candidate_count: u32,
    pub deduplicated_source_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgeMemorySearch {
    pub records: Vec<ForgeMemoryRecord>,
    pub has_more: bool,
    pub next_cursor: Option<String>,
    pub deduplicated_source_ids: Vec<String>,
}

#[derive(Clone)]
pub struct ForgeMemorySource<R = SqliteDb> {
    db: Arc<R>,
    binding_id: String,
    identity_id: String,
    context_scope_id: String,
    scope_type: String,
    scope_id: String,
    access: MemoryAccessContext,
    allow_restricted: bool,
    max_results: u32,
}

impl<R> ForgeMemorySource<R>
where
    R: ScopedMemoryRepository + Send + Sync + 'static,
{
    pub async fn bind(db: Arc<R>, input: MemorySourceBindingInput) -> Result<Self> {
        let identity_id = input.identity_id.to_string();
        let context_scope_id = input.context_scope_id.to_string();
        if input.access.identity_id.as_deref() != Some(identity_id.as_str()) {
            return Err(ServiceError::invalid_operation(
                "memory source identity must match the admitted access identity",
            ));
        }
        if !matches!(
            input.scope_type.as_str(),
            "account" | "project" | "agent_chat" | "task"
        ) {
            return Err(ServiceError::invalid_operation(
                "memory source requires an admitted canonical scope",
            ));
        }
        let Some(canonical_grant) = input
            .access
            .grants
            .iter()
            .find(|grant| grant.scope_type == input.scope_type && grant.scope_id == input.scope_id)
            .cloned()
        else {
            return Err(ServiceError::invalid_operation(
                "memory source requires an admitted canonical-scope grant",
            ));
        };
        let create = db
            .create_memory_source_binding(CreateForgeMemorySourceBinding {
                id: input.binding_id.to_string(),
                identity_id: identity_id.clone(),
                context_scope_id: context_scope_id.clone(),
                scope_type: input.scope_type.clone(),
                scope_id: input.scope_id.clone(),
                account_id: input.account_id,
                project_id: input.project_id,
                task_id: input.task_id,
                policy_revision: input.policy_revision,
                created_at: now_rfc3339(),
            })
            .await;
        let binding = match create {
            Ok(binding) => binding,
            Err(db::DbError::Sqlx(_)) => db
                .get_memory_source_binding(&identity_id, &context_scope_id)
                .await?
                .ok_or(db::DbError::NotFound)?,
            Err(error) => return Err(error.into()),
        };
        if binding.identity_id != identity_id
            || binding.context_scope_id != context_scope_id
            || binding.scope_type != input.scope_type
            || binding.scope_id != input.scope_id
        {
            return Err(ServiceError::Conflict(
                "memory source binding is immutable and cannot be retargeted".to_owned(),
            ));
        }
        Ok(Self {
            db,
            binding_id: binding.id,
            identity_id: binding.identity_id,
            context_scope_id: binding.context_scope_id,
            scope_type: binding.scope_type,
            scope_id: binding.scope_id,
            // A source is permanently canonical-scope bound. Additional
            // grants in the admission context cannot widen it after bind.
            access: MemoryAccessContext {
                identity_id: input.access.identity_id,
                grants: vec![canonical_grant],
            },
            allow_restricted: false,
            max_results: 50,
        })
    }

    pub fn binding_id(&self) -> &str {
        &self.binding_id
    }

    pub fn identity_id(&self) -> &str {
        &self.identity_id
    }

    pub fn context_scope_id(&self) -> &str {
        &self.context_scope_id
    }

    pub fn scope_type(&self) -> &str {
        &self.scope_type
    }

    pub fn scope_id(&self) -> &str {
        &self.scope_id
    }

    pub fn with_max_results(mut self, max_results: u32) -> Self {
        self.max_results = max_results.clamp(1, 500);
        self
    }

    /// Restricted records remain host-owned by default. Secret records are
    /// filtered in the repository before this method can observe them.
    pub fn allow_restricted(mut self, allow: bool) -> Self {
        self.allow_restricted = allow;
        self
    }

    pub async fn search(&self, query: ForgeMemoryQuery) -> Result<ForgeMemorySearch> {
        let requested = query.limit.min(self.max_results).max(1);
        let (items, has_more) = self
            .db
            .search_memory_items_scoped(MemoryAccessQuery {
                identity_id: self.access.identity_id.clone(),
                grants: self.access.grants.clone(),
                query: query.query.clone(),
                limit: i64::from(requested),
                cursor: query.cursor.clone(),
                include_retracted: false,
            })
            .await?;
        let raw_cursor = items.last().map(scoped_cursor_for_item).transpose()?;
        let (records, mut deduplicated_source_ids) =
            self.records_from_items(items, &query.represented_source_ids)?;

        // Preserve the existing stable paginated search. Only when the exact
        // all-term query produced no usable record on its first page do we
        // fall back to the bounded, non-pageable recall path.
        if records.is_empty() && query.cursor.is_none() {
            let recall = self
                .recall(ForgeMemoryRecallQuery {
                    query: query.query,
                    limit: requested,
                    represented_source_ids: query.represented_source_ids,
                })
                .await?;
            deduplicated_source_ids.extend(recall.deduplicated_source_ids);
            deduplicated_source_ids.sort();
            deduplicated_source_ids.dedup();
            return Ok(ForgeMemorySearch {
                records: recall
                    .records
                    .into_iter()
                    .map(|record| record.record)
                    .collect(),
                has_more: false,
                next_cursor: None,
                deduplicated_source_ids,
            });
        }

        Ok(ForgeMemorySearch {
            records,
            has_more,
            next_cursor: if has_more { raw_cursor } else { None },
            deduplicated_source_ids,
        })
    }

    /// Recall a small, query-relevant context set without requiring embeddings.
    ///
    /// The source first derives a bounded list of salient query terms. It runs
    /// one exact all-term arm plus one arm per term through the existing
    /// scope-authorized repository query, fuses the ranked lists with
    /// reciprocal-rank fusion, and then applies deterministic field/authority
    /// tie-breakers. This method is intentionally non-pageable.
    pub async fn recall(&self, query: ForgeMemoryRecallQuery) -> Result<ForgeMemoryRecall> {
        let requested = query
            .limit
            .min(self.max_results)
            .min(MAX_RECALL_RESULTS)
            .max(1);
        let (query_terms, arms) = relevance::query_arms(&query.query);
        if arms.is_empty() {
            return Ok(ForgeMemoryRecall {
                records: Vec::new(),
                query_terms,
                candidate_count: 0,
                deduplicated_source_ids: Vec::new(),
            });
        }

        let represented_source_ids = query
            .represented_source_ids
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        let arm_limit = requested.saturating_mul(4).clamp(8, MAX_RECALL_ARM_RESULTS);
        let mut candidates = BTreeMap::new();
        let mut deduplicated_source_ids = BTreeSet::new();

        for arm in &arms {
            let (items, _) = self
                .db
                .search_memory_items_scoped(MemoryAccessQuery {
                    identity_id: self.access.identity_id.clone(),
                    grants: self.access.grants.clone(),
                    query: arm.query.clone(),
                    limit: i64::from(arm_limit),
                    cursor: None,
                    include_retracted: false,
                })
                .await?;

            let mut eligible = Vec::with_capacity(items.len());
            for item in items {
                let item_source_ref = source_ref(&item);
                let represented = represented_source_ids.contains(item.id.as_str())
                    || item_source_ref
                        .as_deref()
                        .is_some_and(|source_id| represented_source_ids.contains(source_id));
                if represented {
                    deduplicated_source_ids
                        .insert(item_source_ref.unwrap_or_else(|| item.id.clone()));
                    continue;
                }
                if item.sensitivity == "secret"
                    || (!self.allow_restricted && item.sensitivity == "restricted")
                {
                    continue;
                }
                eligible.push(item);
            }
            relevance::merge_ranked_items(&mut candidates, eligible, arm);
        }

        let candidate_count = u32::try_from(candidates.len()).unwrap_or(u32::MAX);
        let ranked = relevance::rank_candidates(&query_terms, candidates);
        let mut seen_sources = BTreeSet::new();
        let mut records = Vec::with_capacity(requested as usize);

        for candidate in ranked {
            let source_key =
                source_ref(&candidate.item).unwrap_or_else(|| candidate.item.id.clone());
            if !seen_sources.insert(source_key) {
                continue;
            }
            let matched_terms = if candidate.all_terms_match {
                query_terms.clone()
            } else {
                candidate.matched_terms.iter().cloned().collect()
            };
            let selection_reason = relevance::selection_reason(&candidate, &query_terms);
            records.push(ForgeMemoryRecallRecord {
                record: record_from_item(candidate.item)?,
                matched_terms,
                selection_reason,
            });
            if records.len() == requested as usize {
                break;
            }
        }

        Ok(ForgeMemoryRecall {
            records,
            query_terms,
            candidate_count,
            deduplicated_source_ids: deduplicated_source_ids.into_iter().collect(),
        })
    }

    fn records_from_items(
        &self,
        items: Vec<MemoryItem>,
        represented_source_ids: &[String],
    ) -> Result<(Vec<ForgeMemoryRecord>, Vec<String>)> {
        let represented_source_ids = represented_source_ids
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        let mut deduplicated_source_ids = BTreeSet::new();
        let mut records = Vec::with_capacity(items.len());

        for item in items {
            let item_source_ref = source_ref(&item);
            let represented = represented_source_ids.contains(item.id.as_str())
                || item_source_ref
                    .as_deref()
                    .is_some_and(|source_id| represented_source_ids.contains(source_id));
            if represented {
                deduplicated_source_ids.insert(item_source_ref.unwrap_or_else(|| item.id.clone()));
                continue;
            }
            if item.sensitivity == "secret"
                || (!self.allow_restricted && item.sensitivity == "restricted")
            {
                continue;
            }
            records.push(record_from_item(item)?);
        }

        Ok((
            records,
            deduplicated_source_ids.into_iter().collect::<Vec<_>>(),
        ))
    }
}

fn record_from_item(item: MemoryItem) -> Result<ForgeMemoryRecord> {
    let source_ref = source_ref(&item);
    Ok(ForgeMemoryRecord {
        id: Uuid::parse_str(&item.id).map_err(|error| {
            ServiceError::invalid_operation(format!("invalid memory id: {error}"))
        })?,
        revision: item
            .source_revision
            .clone()
            .unwrap_or_else(|| item.created_at.clone()),
        authority: item.authority,
        title: item.title,
        summary: item.summary,
        body: item.body,
        sensitivity: item.sensitivity,
        retention_priority: item.retention_priority,
        source_type: item.source_type,
        source_ref,
        provenance_json: item.provenance_json,
        created_at: item.created_at,
    })
}

fn scoped_cursor_for_item(item: &MemoryItem) -> Result<String> {
    let rank = match item.authority.as_str() {
        "decision" => 600,
        "procedure" => 500,
        "verified_fact" => 450,
        "proposal" => 300,
        "hypothesis" => 200,
        _ => 100,
    } + item.retention_priority;
    let value = serde_json::json!({
        "rank": rank,
        "created_at": item.created_at,
        "id": item.id,
    });
    let bytes = serde_json::to_vec(&value).map_err(|error| {
        ServiceError::invalid_operation(format!("invalid memory cursor: {error}"))
    })?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn source_ref(item: &MemoryItem) -> Option<String> {
    serde_json::from_str::<Value>(&item.metadata_json)
        .ok()
        .and_then(|value| {
            value
                .get("source_ref")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
}
