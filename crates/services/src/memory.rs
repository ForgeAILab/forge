use std::{str::FromStr, sync::Arc};

use async_trait::async_trait;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use db::{
    now_rfc3339, AgentChat, AgentChatMessage, AgentChatMessageAuthorType, CommentAuthorType,
    CreateMemoryLifecycleAssertion, DomainEvent, Execution, ExecutionStatus, MemoryAccessQuery,
    MemoryConfidence, MemoryGetQuery, MemoryItem, MemoryKind, MemoryRepository, MemoryScopeGrant,
    MemorySourceType, Review, ReviewStatus, ScopedMemoryRepository, SqliteDb, TaskComment,
    TransitionLog,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::Row;
use uuid::Uuid;

use crate::{Result, ServiceError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryCreator {
    pub creator_type: String,
    pub creator_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryReferences {
    pub source_ref: String,
    pub project_id: Option<String>,
    pub task_id: Option<String>,
    pub execution_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemorySearchResult {
    pub id: Uuid,
    pub kind: MemoryKind,
    pub title: String,
    pub source_type: MemorySourceType,
    pub summary: Option<String>,
    pub body: Option<String>,
    pub references: Option<MemoryReferences>,
    pub confidence: Option<MemoryConfidence>,
    pub created_at: Option<String>,
    pub creator: Option<MemoryCreator>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryItemInput {
    pub project_id: Uuid,
    pub task_id: Option<String>,
    pub execution_id: Option<String>,
    pub source_type: MemorySourceType,
    pub source_ref: String,
    pub kind: MemoryKind,
    pub title: String,
    pub summary: Option<String>,
    pub body: String,
    pub confidence: Option<MemoryConfidence>,
    pub quality_score: Option<i64>,
    pub creator: Option<MemoryCreator>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryAccessContext {
    pub identity_id: Option<String>,
    pub grants: Vec<MemoryScopeGrant>,
}

impl MemoryAccessContext {
    pub fn for_scope(
        identity_id: Option<String>,
        scope_type: impl Into<String>,
        scope_id: impl Into<String>,
        visibility: Vec<String>,
    ) -> Self {
        Self {
            identity_id: identity_id.clone(),
            grants: vec![MemoryScopeGrant {
                scope_type: scope_type.into(),
                scope_id: scope_id.into(),
                visibility,
                identity_id,
            }],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryPublicationInput {
    pub source_id: Uuid,
    pub source_scope_type: String,
    pub source_scope_id: String,
    pub target_scope_type: String,
    pub target_scope_id: String,
    pub target_project_id: Option<String>,
    pub target_task_id: Option<String>,
    pub target_visibility: String,
    pub target_authority: String,
    pub actor_identity_id: String,
    pub reason: String,
    pub evidence_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryLifecycleInput {
    pub memory_id: Uuid,
    pub assertion_type: String,
    pub related_memory_id: Option<Uuid>,
    pub reason: Option<String>,
    pub evidence_json: String,
    pub actor_identity_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackfillSummary {
    pub items: Vec<BackfillTypeResult>,
    pub indexed: u64,
    pub skipped: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackfillTypeResult {
    pub source_type: MemorySourceType,
    pub indexed: u64,
    pub skipped: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryBackfillSource {
    pub project_id: String,
    pub task_id: Option<String>,
    pub execution_id: Option<String>,
    pub source_type: MemorySourceType,
    pub source_ref: String,
    pub kind: MemoryKind,
    pub title: String,
    pub summary: Option<String>,
    pub body: String,
    pub confidence: Option<MemoryConfidence>,
    pub creator: Option<MemoryCreator>,
}

#[async_trait]
pub trait MemoryBackfillRepository: MemoryRepository {
    async fn list_memory_backfill_sources(&self) -> Result<Vec<MemoryBackfillSource>>;
}

#[derive(Clone)]
pub struct MemoryService<R = SqliteDb> {
    db: Arc<R>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MemoryLayer {
    One,
    Two,
    Three,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MemoryCursor {
    created_at: String,
    id: String,
}

impl<R> MemoryService<R>
where
    R: MemoryRepository + Send + Sync,
{
    pub fn new(db: Arc<R>) -> Self {
        Self { db }
    }

    pub async fn search(
        &self,
        project_id: Uuid,
        query: String,
        layer: Option<u8>,
        token_budget: Option<u32>,
        limit: u32,
        cursor: Option<String>,
    ) -> Result<(Vec<MemorySearchResult>, bool, Option<String>)> {
        let layer = resolve_layer(layer, token_budget)?;
        let (items, has_more) = self
            .db
            .search_memory_items(&project_id.to_string(), &query, limit as usize, cursor)
            .await?;
        let next_cursor = if has_more {
            items.last().map(memory_cursor_for_item).transpose()?
        } else {
            None
        };
        let results = items
            .into_iter()
            .map(|item| shape_item(item, layer))
            .collect::<Result<Vec<_>>>()?;
        Ok((results, has_more, next_cursor))
    }

    pub async fn get(&self, id: Uuid, layer: Option<u8>) -> Result<MemorySearchResult> {
        let layer = resolve_layer(layer, None)?;
        let item = self
            .db
            .get_memory_item(&id.to_string())
            .await?
            .ok_or_else(|| ServiceError::not_found("memory_item", id.to_string()))?;
        shape_item(item, layer)
    }

    pub async fn record_from_source(&self, input: MemoryItemInput) -> Result<MemoryItem> {
        let scope_type = if input.task_id.is_some() {
            "task".to_owned()
        } else {
            "project".to_owned()
        };
        let scope_id = input
            .task_id
            .as_ref()
            .cloned()
            .unwrap_or_else(|| input.project_id.to_string());
        let item = build_memory_item(input, scope_type, scope_id, "project");
        self.db.insert_memory_item(&item).await?;
        Ok(item)
    }
}

impl<R> MemoryService<R>
where
    R: MemoryRepository + ScopedMemoryRepository + Send + Sync,
{
    /// Search only through candidates admitted by the caller's immutable
    /// canonical-scope grants. The repository performs ACL filtering before
    /// FTS/body retrieval, so this service never turns an inaccessible id into
    /// a distinguishable result.
    pub async fn search_scoped(
        &self,
        access: &MemoryAccessContext,
        query: String,
        layer: Option<u8>,
        limit: u32,
        cursor: Option<String>,
    ) -> Result<(Vec<MemorySearchResult>, bool, Option<String>)> {
        let (items, has_more) = self
            .db
            .search_memory_items_scoped(MemoryAccessQuery {
                identity_id: access.identity_id.clone(),
                grants: access.grants.clone(),
                query,
                limit: i64::from(limit),
                cursor,
                include_retracted: false,
            })
            .await?;
        let next_cursor = if has_more {
            items
                .last()
                .map(scoped_memory_cursor_for_item)
                .transpose()?
        } else {
            None
        };
        let layer = resolve_layer(layer, None)?;
        let results = items
            .into_iter()
            .map(|item| shape_item(item, layer))
            .collect::<Result<Vec<_>>>()?;
        Ok((results, has_more, next_cursor))
    }

    pub async fn get_scoped(
        &self,
        access: &MemoryAccessContext,
        id: Uuid,
        layer: Option<u8>,
    ) -> Result<MemorySearchResult> {
        let item = self
            .db
            .get_memory_item_scoped(MemoryGetQuery {
                id: id.to_string(),
                identity_id: access.identity_id.clone(),
                grants: access.grants.clone(),
                include_retracted: false,
            })
            .await?
            .ok_or_else(|| ServiceError::not_found("memory_item", id.to_string()))?;
        shape_item(item, resolve_layer(layer, None)?)
    }

    /// Publish or promote by creating a new immutable record. The original
    /// private assertion is never widened or edited; the lifecycle assertion
    /// links it to the newly-created shared record and records evidence.
    pub async fn publish(
        &self,
        access: &MemoryAccessContext,
        input: MemoryPublicationInput,
    ) -> Result<MemoryItem> {
        let evidence_json = guard_evidence_json(&input.evidence_json)?;
        let reason = guard_memory_reason(&input.reason)?;
        let source = self
            .db
            .get_memory_item_scoped(MemoryGetQuery {
                id: input.source_id.to_string(),
                identity_id: access.identity_id.clone(),
                grants: access
                    .grants
                    .iter()
                    .filter(|grant| {
                        grant.scope_type == input.source_scope_type
                            && grant.scope_id == input.source_scope_id
                    })
                    .cloned()
                    .collect(),
                include_retracted: false,
            })
            .await?
            .ok_or_else(|| ServiceError::not_found("memory_item", input.source_id.to_string()))?;
        if source.visibility != "private" {
            return Err(ServiceError::invalid_operation(
                "only private memory can be explicitly published",
            ));
        }
        if source.owner_identity_id.as_deref() != Some(input.actor_identity_id.as_str()) {
            return Err(ServiceError::not_found(
                "memory_item",
                input.source_id.to_string(),
            ));
        }
        let source_revision = source
            .source_revision
            .clone()
            .filter(|revision| !revision.trim().is_empty())
            .ok_or_else(|| {
                ServiceError::invalid_operation(
                    "memory publication requires an exact canonical source revision",
                )
            })?;
        let target_visibility_allowed = match input.target_scope_type.as_str() {
            "account" => input.target_visibility == "account",
            "project" | "task" => input.target_visibility == "project",
            "agent_chat" => input.target_visibility == "chat",
            _ => false,
        };
        if !target_visibility_allowed {
            return Err(ServiceError::invalid_operation(
                "target visibility is not valid for the target scope",
            ));
        }
        let target_grant = access
            .grants
            .iter()
            .find(|grant| {
                grant.scope_type == input.target_scope_type
                    && grant.scope_id == input.target_scope_id
                    && grant
                        .visibility
                        .iter()
                        .any(|visibility| visibility == &input.target_visibility)
            })
            .ok_or_else(|| {
                ServiceError::not_found("memory_scope", input.target_scope_id.clone())
            })?;
        if (target_grant.scope_type != source.scope_type
            || target_grant.scope_id != source.scope_id)
            && evidence_json.trim().is_empty()
        {
            return Err(ServiceError::invalid_operation(
                "scope publication requires explicit evidence",
            ));
        }
        if input.target_authority != "observation"
            && input.target_authority != "hypothesis"
            && input.target_authority != "proposal"
            && input.target_authority != "decision"
            && input.target_authority != "verified_fact"
            && input.target_authority != "procedure"
        {
            return Err(ServiceError::invalid_operation(
                "invalid memory authority for publication",
            ));
        }
        if input.target_authority != source.authority && evidence_json.trim().is_empty() {
            return Err(ServiceError::invalid_operation(
                "authority promotion requires explicit evidence",
            ));
        }
        let now = now_rfc3339();
        let published = MemoryItem {
            row_id: 0,
            id: Uuid::new_v4().to_string(),
            project_id: input
                .target_project_id
                .clone()
                .or_else(|| source.project_id.clone()),
            task_id: input.target_task_id.clone(),
            execution_id: source.execution_id.clone(),
            scope_type: input.target_scope_type.clone(),
            scope_id: input.target_scope_id.clone(),
            visibility: input.target_visibility.clone(),
            owner_identity_id: Some(input.actor_identity_id.clone()),
            authority: input.target_authority.clone(),
            sensitivity: source.sensitivity.clone(),
            retention_priority: source.retention_priority.max(50),
            provenance_json: json!({
                "publication_source_id": source.id,
                "reason": reason.clone(),
                "evidence": serde_json::from_str::<Value>(&evidence_json).unwrap_or(Value::String(evidence_json.clone())),
            })
            .to_string(),
            publication_source_id: Some(source.id.clone()),
            supersedes_id: None,
            valid_from: Some(now.clone()),
            valid_until: None,
            source_event_id: None,
            // Publication changes the ACL scope, not the canonical source.
            // Preserve the source artifact's scope and exact revision so the
            // shared projection cannot become a separately editable copy or
            // silently drift to a publication timestamp.
            source_scope_type: source
                .source_scope_type
                .clone()
                .or_else(|| Some(source.scope_type.clone())),
            source_scope_id: source
                .source_scope_id
                .clone()
                .or_else(|| Some(source.scope_id.clone())),
            source_revision: Some(source_revision),
            source_type: source.source_type.clone(),
            kind: source.kind.clone(),
            title: source.title.clone(),
            summary: source.summary.clone(),
            body: source.body.clone(),
            metadata_json: source.metadata_json.clone(),
            confidence: source.confidence.clone(),
            quality_score: source.quality_score,
            created_by_type: Some("agent".to_owned()),
            created_by_id: Some(input.actor_identity_id.clone()),
            created_at: now.clone(),
        };
        self.db.insert_memory_item(&published).await?;
        self.db
            .insert_memory_lifecycle_assertion(CreateMemoryLifecycleAssertion {
                id: Uuid::new_v4().to_string(),
                memory_item_id: source.id,
                assertion_type: if input.target_authority != "observation" {
                    "promoted".to_owned()
                } else {
                    "published".to_owned()
                },
                related_memory_id: Some(published.id.clone()),
                reason: Some(reason),
                evidence_json,
                asserted_by_type: "agent".to_owned(),
                asserted_by_id: Some(input.actor_identity_id),
                source_event_id: None,
                created_at: now,
            })
            .await?;
        Ok(published)
    }

    pub async fn assert_lifecycle(
        &self,
        access: &MemoryAccessContext,
        input: MemoryLifecycleInput,
    ) -> Result<db::MemoryLifecycleAssertion> {
        let evidence_json = guard_evidence_json(&input.evidence_json)?;
        let reason = input
            .reason
            .as_deref()
            .map(guard_memory_reason)
            .transpose()?;
        let source = self
            .db
            .get_memory_item_scoped(MemoryGetQuery {
                id: input.memory_id.to_string(),
                identity_id: access.identity_id.clone(),
                grants: access.grants.clone(),
                include_retracted: true,
            })
            .await?
            .ok_or_else(|| ServiceError::not_found("memory_item", input.memory_id.to_string()))?;
        let owner = source.owner_identity_id.as_deref() == Some(input.actor_identity_id.as_str());
        let destructive = matches!(
            input.assertion_type.as_str(),
            "superseded" | "retracted" | "expired"
        );
        if (destructive || input.assertion_type == "published") && !owner {
            return Err(ServiceError::not_found(
                "memory_item",
                input.memory_id.to_string(),
            ));
        }
        let assertion_type = input.assertion_type.as_str();
        if !matches!(
            assertion_type,
            "published"
                | "promoted"
                | "superseded"
                | "retracted"
                | "disputed"
                | "expired"
                | "evidence"
        ) {
            return Err(ServiceError::invalid_operation(
                "invalid memory lifecycle assertion",
            ));
        }
        if matches!(assertion_type, "published" | "promoted" | "superseded")
            && input.related_memory_id.is_none()
        {
            return Err(ServiceError::invalid_operation(
                "publication, promotion, and supersession assertions require a related memory",
            ));
        }
        if input.assertion_type == "promoted" && evidence_json.trim().is_empty() {
            return Err(ServiceError::invalid_operation(
                "promotion requires explicit evidence",
            ));
        }
        if let Some(related_memory_id) = input.related_memory_id {
            let related = self
                .db
                .get_memory_item_scoped(MemoryGetQuery {
                    id: related_memory_id.to_string(),
                    identity_id: access.identity_id.clone(),
                    grants: access.grants.clone(),
                    include_retracted: true,
                })
                .await?;
            if related.is_none() {
                return Err(ServiceError::not_found(
                    "memory_item",
                    related_memory_id.to_string(),
                ));
            }
        }
        Ok(self
            .db
            .insert_memory_lifecycle_assertion(CreateMemoryLifecycleAssertion {
                id: Uuid::new_v4().to_string(),
                memory_item_id: source.id,
                assertion_type: input.assertion_type,
                related_memory_id: input.related_memory_id.map(|id| id.to_string()),
                reason,
                evidence_json,
                asserted_by_type: "agent".to_owned(),
                asserted_by_id: Some(input.actor_identity_id),
                source_event_id: None,
                created_at: now_rfc3339(),
            })
            .await?)
    }

    /// Index one finalized Agent Chat message in the chat's canonical memory
    /// scope. The singular-chat visibility label and chat id are the
    /// canonical ACL/provenance boundary. The
    /// source-receipt write is part of the repository transaction, so replay
    /// after a lease expiry cannot create a duplicate memory item.
    pub async fn record_agent_chat_message_event(
        &self,
        event: &DomainEvent,
        chat: &AgentChat,
        message: &AgentChatMessage,
    ) -> Result<Option<MemoryItem>> {
        if !matches!(
            event.event_type.as_str(),
            "agent_chat.message.admitted"
                | "agent_chat.response.completed"
                | "agent_chat.message.completed"
        ) || message.status.to_string() != "complete"
            || !matches!(
                message.author_type,
                AgentChatMessageAuthorType::User | AgentChatMessageAuthorType::Agent
            )
        {
            return Ok(None);
        }
        if message.sensitivity == "secret"
            || message.content.trim().is_empty()
            || contains_protected_marker(&message.content)
        {
            return Ok(None);
        }

        let (scope_type, scope_id, visibility) =
            ("agent_chat".to_owned(), chat.id.clone(), "chat".to_owned());
        if chat.kind == "project" {
            if chat.project_id.is_none() {
                return Err(ServiceError::invalid_operation(
                    "Project Agent Chat has no Project scope",
                ));
            }
        } else {
            if chat.account_id.is_none() {
                return Err(ServiceError::invalid_operation(
                    "Main Agent Chat has no account scope",
                ));
            }
        }
        let now = now_rfc3339();
        let item = MemoryItem {
            row_id: 0,
            id: Uuid::new_v4().to_string(),
            project_id: chat.project_id.clone(),
            task_id: None,
            execution_id: None,
            scope_type: scope_type.clone(),
            scope_id: scope_id.clone(),
            visibility,
            owner_identity_id: match message.author_type {
                AgentChatMessageAuthorType::Agent => message.author_id.clone(),
                _ => None,
            },
            authority: "observation".to_owned(),
            sensitivity: message.sensitivity.clone(),
            retention_priority: 20,
            provenance_json: json!({
                "source_event_id": event.id,
                "agent_chat_id": chat.id,
                "message_id": message.id,
                "sequence": message.sequence,
                // Agent turns carry the immutable server/runtime context
                // manifest used for admission. Keep only its canonical ID in
                // memory provenance; the manifest sources remain the source
                // of truth for exact artifact revisions and stale pointers.
                "context_manifest_id": message.context_manifest_id,
            })
            .to_string(),
            publication_source_id: None,
            supersedes_id: None,
            valid_from: Some(message.created_at.clone()),
            valid_until: None,
            source_event_id: Some(event.id.clone()),
            source_scope_type: Some(scope_type.clone()),
            source_scope_id: Some(scope_id.clone()),
            source_revision: Some(message.sequence.to_string()),
            source_type: "agent_chat".to_owned(),
            kind: "observation".to_owned(),
            title: format!("Agent Chat {} message", message.author_type),
            summary: snippet(&message.content),
            body: message.content.clone(),
            metadata_json: source_metadata_json(&message.id),
            confidence: Some(MemoryConfidence::Confirmed.to_string()),
            quality_score: None,
            created_by_type: Some(message.author_type.to_string()),
            created_by_id: message.author_id.clone(),
            created_at: now,
        };
        let (item, inserted) = self
            .db
            .insert_memory_item_if_source_absent(&item, "agent_chat", &message.id)
            .await?;
        Ok(inserted.then_some(item))
    }
}

impl<R> MemoryService<R>
where
    R: MemoryBackfillRepository + Send + Sync,
{
    pub async fn backfill_all(db: Arc<R>) -> Result<BackfillSummary> {
        Self::new(db).backfill_sources().await
    }

    pub async fn backfill_sources(&self) -> Result<BackfillSummary> {
        let mut results = backfill_results_by_type();
        for source in self.db.list_memory_backfill_sources().await? {
            let result = backfill_result_for_source(&mut results, &source.source_type)
                .expect("all source types are pre-seeded");
            if backfill_source_exists(self.db.as_ref(), &source).await? {
                result.skipped += 1;
                continue;
            }

            let input = MemoryItemInput {
                project_id: parse_uuid(&source.project_id, "project_id")?,
                task_id: source.task_id,
                execution_id: source.execution_id,
                source_type: source.source_type.clone(),
                source_ref: source.source_ref,
                kind: source.kind,
                title: source.title,
                summary: source.summary,
                body: source.body,
                confidence: source.confidence,
                quality_score: None,
                creator: source.creator,
            };
            self.record_from_source(input).await?;
            result.indexed += 1;
        }
        Ok(backfill_summary_from_results(results))
    }

    pub(crate) async fn record_transition_if_failure(
        &self,
        project_id: &str,
        transition: &TransitionLog,
        hook_results_json: Option<&str>,
    ) -> Result<Option<Uuid>> {
        if !transition_has_failure_signal(
            &transition.from_state,
            &transition.to_state,
            &transition.trigger_reason,
            hook_results_json.or(transition.hook_results_json.as_deref()),
            transition.rejection,
        ) {
            return Ok(None);
        }
        if self
            .db
            .memory_source_exists(
                project_id,
                &MemorySourceType::Transition.to_string(),
                transition.id.as_str(),
            )
            .await?
        {
            return Ok(None);
        }
        let body = transition_body(transition, hook_results_json);
        let item = self
            .record_from_source(MemoryItemInput {
                project_id: parse_uuid(project_id, "project_id")?,
                task_id: Some(transition.task_id.clone()),
                execution_id: None,
                source_type: MemorySourceType::Transition,
                source_ref: transition.id.clone(),
                kind: MemoryKind::Transition,
                title: format!(
                    "Transition {} -> {}",
                    transition.from_state, transition.to_state
                ),
                summary: snippet(&transition.trigger_reason).or_else(|| snippet(&body)),
                body,
                confidence: Some(MemoryConfidence::Confirmed),
                quality_score: None,
                creator: None,
            })
            .await?;
        parse_uuid(&item.id, "memory_item.id").map(Some)
    }
}

impl<R> MemoryService<R>
where
    R: MemoryRepository + Send + Sync,
{
    pub(crate) async fn record_execution_summary_if_present(
        &self,
        project_id: &str,
        execution: &Execution,
    ) -> Result<Option<Uuid>> {
        if !matches!(
            &execution.status,
            ExecutionStatus::Completed | ExecutionStatus::Failed
        ) {
            return Ok(None);
        }
        let Some(body) = execution_summary_body(execution) else {
            return Ok(None);
        };
        if self
            .db
            .memory_source_exists(
                project_id,
                &MemorySourceType::Execution.to_string(),
                execution.id.as_str(),
            )
            .await?
        {
            return Ok(None);
        }
        let title = format!("{} execution {}", execution.status, execution.role);
        let item = self
            .record_from_source(MemoryItemInput {
                project_id: parse_uuid(project_id, "project_id")?,
                task_id: Some(execution.task_id.clone()),
                execution_id: Some(execution.id.clone()),
                source_type: MemorySourceType::Execution,
                source_ref: execution.id.clone(),
                kind: MemoryKind::ExecutionSummary,
                title,
                summary: snippet(&body),
                body,
                confidence: Some(if execution.status == ExecutionStatus::Completed {
                    MemoryConfidence::Confirmed
                } else {
                    MemoryConfidence::Partial
                }),
                quality_score: None,
                creator: execution.agent_id.clone().map(agent_creator),
            })
            .await?;
        parse_uuid(&item.id, "memory_item.id").map(Some)
    }

    pub(crate) async fn record_review_result_if_final(
        &self,
        project_id: &str,
        review: &Review,
    ) -> Result<Option<Uuid>> {
        if review.status == ReviewStatus::Running {
            return Ok(None);
        }
        let has_confirmed_outcome = review_has_confirmed_outcome(review);
        if review_memory_source_exists(
            self.db.as_ref(),
            project_id,
            review.id.as_str(),
            has_confirmed_outcome,
        )
        .await?
        {
            return Ok(None);
        }
        let item = self
            .record_from_source(MemoryItemInput {
                project_id: parse_uuid(project_id, "project_id")?,
                task_id: Some(review.task_id.clone()),
                execution_id: Some(review.execution_id.clone()),
                source_type: MemorySourceType::Review,
                source_ref: review.id.clone(),
                kind: MemoryKind::ReviewResult,
                title: format!("Review {} attempt {}", review.status, review.attempt_number),
                summary: review_summary(review),
                body: review.step_results_json.clone(),
                confidence: Some(if has_confirmed_outcome {
                    MemoryConfidence::Confirmed
                } else {
                    MemoryConfidence::Partial
                }),
                quality_score: None,
                creator: None,
            })
            .await?;
        parse_uuid(&item.id, "memory_item.id").map(Some)
    }

    pub(crate) async fn record_task_comment(
        &self,
        project_id: &str,
        comment: &TaskComment,
    ) -> Result<MemoryItem> {
        let creator = match &comment.author_type {
            CommentAuthorType::Agent => comment.author_id.clone().map(agent_creator),
            CommentAuthorType::User => comment.author_id.clone().map(user_creator),
            CommentAuthorType::System => Some(system_creator()),
        };
        self.record_from_source(MemoryItemInput {
            project_id: parse_uuid(project_id, "project_id")?,
            task_id: Some(comment.task_id.clone()),
            execution_id: None,
            source_type: MemorySourceType::Comment,
            source_ref: comment.id.clone(),
            kind: MemoryKind::Comment,
            title: format!("Comment by {}", comment.author_name),
            summary: snippet(&comment.content),
            body: comment.content.clone(),
            confidence: Some(MemoryConfidence::Confirmed),
            quality_score: None,
            creator,
        })
        .await
    }
}

#[async_trait]
impl MemoryBackfillRepository for SqliteDb {
    async fn list_memory_backfill_sources(&self) -> Result<Vec<MemoryBackfillSource>> {
        let mut sources = Vec::new();
        sources.extend(list_execution_sources(self).await?);
        sources.extend(list_review_sources(self).await?);
        sources.extend(list_comment_sources(self).await?);
        sources.extend(list_transition_sources(self).await?);
        Ok(sources)
    }
}

async fn list_execution_sources(db: &SqliteDb) -> Result<Vec<MemoryBackfillSource>> {
    let rows = sqlx::query(
        "SELECT t.project_id, e.id, e.task_id, e.agent_id, e.role, e.status, e.summary, e.error \
         FROM execution e JOIN task t ON t.id = e.task_id \
         WHERE e.status IN ('completed', 'failed') \
           AND (TRIM(COALESCE(e.summary, '')) <> '' OR TRIM(COALESCE(e.error, '')) <> '') \
         ORDER BY e.created_at ASC, e.id ASC",
    )
    .fetch_all(db.pool())
    .await?;
    rows.into_iter()
        .map(|row| {
            let status: String = row.try_get("status")?;
            let summary: Option<String> = row.try_get("summary")?;
            let error: Option<String> = row.try_get("error")?;
            let body = summary
                .clone()
                .or(error.clone())
                .unwrap_or_else(|| status.clone());
            Ok(MemoryBackfillSource {
                project_id: row.try_get("project_id")?,
                task_id: Some(row.try_get("task_id")?),
                execution_id: Some(row.try_get("id")?),
                source_type: MemorySourceType::Execution,
                source_ref: row.try_get("id")?,
                kind: MemoryKind::ExecutionSummary,
                title: format!("Execution {status}: {}", row.try_get::<String, _>("role")?),
                summary: snippet(&body),
                body,
                confidence: Some(if status == "completed" {
                    MemoryConfidence::Confirmed
                } else {
                    MemoryConfidence::Partial
                }),
                creator: row
                    .try_get::<Option<String>, _>("agent_id")?
                    .map(agent_creator),
            })
        })
        .collect()
}

async fn list_review_sources(db: &SqliteDb) -> Result<Vec<MemoryBackfillSource>> {
    let rows = sqlx::query(
        "SELECT t.project_id, r.id, r.task_id, r.execution_id, r.attempt_number, r.status, r.step_results_json \
         FROM review r JOIN task t ON t.id = r.task_id \
         WHERE r.status != 'running' \
         ORDER BY r.created_at ASC, r.id ASC",
    )
    .fetch_all(db.pool())
    .await?;
    rows.into_iter()
        .map(|row| {
            let status: String = row.try_get("status")?;
            let body: String = row.try_get("step_results_json")?;
            Ok(MemoryBackfillSource {
                project_id: row.try_get("project_id")?,
                task_id: Some(row.try_get("task_id")?),
                execution_id: Some(row.try_get("execution_id")?),
                source_type: MemorySourceType::Review,
                source_ref: row.try_get("id")?,
                kind: MemoryKind::ReviewResult,
                title: format!(
                    "Review {status} attempt {}",
                    row.try_get::<i64, _>("attempt_number")?
                ),
                summary: review_summary_from_json(&body, &status),
                body,
                confidence: Some(if status == "passed" || status == "failed" {
                    MemoryConfidence::Confirmed
                } else {
                    MemoryConfidence::Partial
                }),
                creator: None,
            })
        })
        .collect()
}

async fn list_comment_sources(db: &SqliteDb) -> Result<Vec<MemoryBackfillSource>> {
    let rows = sqlx::query(
        "SELECT t.project_id, c.id, c.task_id, c.author_type, c.author_id, c.author_name, c.content \
         FROM task_comment c JOIN task t ON t.id = c.task_id \
         ORDER BY c.created_at ASC, c.id ASC",
    )
    .fetch_all(db.pool())
    .await?;
    rows.into_iter()
        .map(|row| {
            let author_type: String = row.try_get("author_type")?;
            let author_id: Option<String> = row.try_get("author_id")?;
            let content: String = row.try_get("content")?;
            let author_name: String = row.try_get("author_name")?;
            Ok(MemoryBackfillSource {
                project_id: row.try_get("project_id")?,
                task_id: Some(row.try_get("task_id")?),
                execution_id: None,
                source_type: MemorySourceType::Comment,
                source_ref: row.try_get("id")?,
                kind: MemoryKind::Comment,
                title: format!("Comment by {author_name}"),
                summary: snippet(&content),
                body: content,
                confidence: Some(MemoryConfidence::Confirmed),
                creator: match author_type.as_str() {
                    "agent" => author_id.map(agent_creator),
                    "user" => author_id.map(user_creator),
                    "system" => Some(system_creator()),
                    _ => None,
                },
            })
        })
        .collect()
}

async fn list_transition_sources(db: &SqliteDb) -> Result<Vec<MemoryBackfillSource>> {
    let rows = sqlx::query(
        "SELECT t.project_id, tl.id, tl.task_id, tl.from_state, tl.to_state, tl.trigger_name, tl.triggered_by, \
                tl.trigger_reason, tl.hook_results_json, tl.rejection, tl.created_at \
         FROM transition_log tl JOIN task t ON t.id = tl.task_id \
         ORDER BY tl.created_at ASC, tl.id ASC",
    )
    .fetch_all(db.pool())
    .await?;
    let mut sources = Vec::new();
    for row in rows {
        let transition = TransitionLog {
            id: row.try_get("id")?,
            task_id: row.try_get("task_id")?,
            from_state: row.try_get("from_state")?,
            to_state: row.try_get("to_state")?,
            trigger_name: row.try_get("trigger_name")?,
            triggered_by: row.try_get("triggered_by")?,
            trigger_reason: row.try_get("trigger_reason")?,
            hook_results_json: row.try_get("hook_results_json")?,
            rejection: row.try_get::<i64, _>("rejection")? != 0,
            created_at: row.try_get("created_at")?,
        };
        if !transition_has_failure_signal(
            &transition.from_state,
            &transition.to_state,
            &transition.trigger_reason,
            transition.hook_results_json.as_deref(),
            transition.rejection,
        ) {
            continue;
        }
        let body = transition_body(&transition, None);
        sources.push(MemoryBackfillSource {
            project_id: row.try_get("project_id")?,
            task_id: Some(transition.task_id.clone()),
            execution_id: None,
            source_type: MemorySourceType::Transition,
            source_ref: transition.id.clone(),
            kind: MemoryKind::Transition,
            title: format!(
                "Transition {} -> {}",
                transition.from_state, transition.to_state
            ),
            summary: snippet(&transition.trigger_reason).or_else(|| snippet(&body)),
            body,
            confidence: Some(MemoryConfidence::Confirmed),
            creator: None,
        });
    }
    Ok(sources)
}

fn resolve_layer(layer: Option<u8>, token_budget: Option<u32>) -> Result<MemoryLayer> {
    match layer {
        Some(1) => Ok(MemoryLayer::One),
        Some(2) => Ok(MemoryLayer::Two),
        Some(3) => Ok(MemoryLayer::Three),
        Some(other) => Err(ServiceError::invalid_operation(format!(
            "invalid memory layer {other}; expected 1, 2, or 3"
        ))),
        None => Ok(match token_budget {
            Some(budget) if budget < 200 => MemoryLayer::One,
            Some(budget) if budget <= 1000 => MemoryLayer::Two,
            _ => MemoryLayer::Three,
        }),
    }
}

fn shape_item(item: MemoryItem, layer: MemoryLayer) -> Result<MemorySearchResult> {
    let id = parse_uuid(&item.id, "memory_item.id")?;
    let kind = MemoryKind::from_str(&item.kind).map_err(ServiceError::invalid_operation)?;
    let source_type =
        MemorySourceType::from_str(&item.source_type).map_err(ServiceError::invalid_operation)?;
    let references = MemoryReferences {
        source_ref: source_ref_from_metadata(&item.metadata_json)
            .unwrap_or_else(|| item.id.clone()),
        project_id: item.project_id.clone(),
        task_id: item.task_id.clone(),
        execution_id: item.execution_id.clone(),
    };
    let confidence = item
        .confidence
        .as_deref()
        .map(MemoryConfidence::from_str)
        .transpose()
        .map_err(ServiceError::invalid_operation)?;
    let creator = item
        .created_by_type
        .clone()
        .map(|creator_type| MemoryCreator {
            creator_type,
            creator_id: item.created_by_id.clone(),
        });
    let metadata = serde_json::from_str::<Value>(&item.metadata_json).ok();

    Ok(match layer {
        MemoryLayer::One => MemorySearchResult {
            id,
            kind,
            title: item.title,
            source_type,
            summary: None,
            body: None,
            references: None,
            confidence: None,
            created_at: None,
            creator: None,
            metadata: None,
        },
        MemoryLayer::Two => MemorySearchResult {
            id,
            kind,
            title: item.title,
            source_type,
            summary: item.summary,
            body: None,
            references: Some(references),
            confidence,
            created_at: Some(item.created_at),
            creator,
            metadata: None,
        },
        MemoryLayer::Three => MemorySearchResult {
            id,
            kind,
            title: item.title,
            source_type,
            summary: item.summary,
            body: Some(item.body),
            references: Some(references),
            confidence,
            created_at: Some(item.created_at),
            creator,
            metadata,
        },
    })
}

fn memory_cursor_for_item(item: &MemoryItem) -> Result<String> {
    let bytes = serde_json::to_vec(&MemoryCursor {
        created_at: item.created_at.clone(),
        id: item.id.clone(),
    })
    .map_err(|error| ServiceError::invalid_operation(format!("invalid memory cursor: {error}")))?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn scoped_memory_cursor_for_item(item: &MemoryItem) -> Result<String> {
    let rank = match item.authority.as_str() {
        "decision" => 600,
        "procedure" => 500,
        "verified_fact" => 450,
        "proposal" => 300,
        "hypothesis" => 200,
        _ => 100,
    } + item.retention_priority;
    let bytes = serde_json::to_vec(&json!({
        "rank": rank,
        "created_at": item.created_at,
        "id": item.id,
    }))
    .map_err(|error| ServiceError::invalid_operation(format!("invalid memory cursor: {error}")))?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn parse_uuid(value: &str, field: &str) -> Result<Uuid> {
    Uuid::parse_str(value).map_err(|error| {
        ServiceError::invalid_operation(format!("invalid {field} UUID '{value}': {error}"))
    })
}

fn source_metadata_json(source_ref: &str) -> String {
    json!({ "source_ref": source_ref }).to_string()
}

fn build_memory_item(
    input: MemoryItemInput,
    scope_type: String,
    scope_id: String,
    visibility: &str,
) -> MemoryItem {
    let owner_identity_id = input.creator.as_ref().and_then(|creator| {
        (creator.creator_type == "agent")
            .then(|| creator.creator_id.clone())
            .flatten()
    });
    let (body, sensitivity) = guard_memory_body(&input.body);
    let (title, summary, body, sensitivity) =
        guard_memory_fields(&input.title, input.summary.as_deref(), body, sensitivity);
    let created_at = now_rfc3339();
    MemoryItem {
        row_id: 0,
        id: Uuid::new_v4().to_string(),
        project_id: Some(input.project_id.to_string()),
        task_id: input.task_id,
        execution_id: input.execution_id,
        scope_type: scope_type.clone(),
        scope_id: scope_id.clone(),
        visibility: visibility.to_owned(),
        owner_identity_id,
        authority: "observation".to_owned(),
        sensitivity,
        retention_priority: 10,
        provenance_json: json!({"source": "forge-memory-service"}).to_string(),
        publication_source_id: None,
        supersedes_id: None,
        valid_from: Some(created_at.clone()),
        valid_until: None,
        source_event_id: None,
        source_scope_type: Some(scope_type),
        source_scope_id: Some(scope_id),
        source_revision: None,
        source_type: input.source_type.to_string(),
        kind: input.kind.to_string(),
        title,
        summary,
        body,
        metadata_json: source_metadata_json(&input.source_ref),
        confidence: input.confidence.map(|value| value.to_string()),
        quality_score: input.quality_score,
        created_by_type: input
            .creator
            .as_ref()
            .map(|creator| creator.creator_type.clone()),
        created_by_id: input.creator.and_then(|creator| creator.creator_id),
        created_at,
    }
}

fn guard_memory_body(body: &str) -> (String, String) {
    if contains_protected_marker(body) {
        (
            "[protected value redacted before semantic-memory indexing]".to_owned(),
            "restricted".to_owned(),
        )
    } else {
        (body.to_owned(), "internal".to_owned())
    }
}

fn guard_memory_fields(
    title: &str,
    summary: Option<&str>,
    body: String,
    sensitivity: String,
) -> (String, Option<String>, String, String) {
    if contains_protected_marker(title) || summary.is_some_and(contains_protected_marker) {
        return (
            "[protected memory redacted before semantic-memory indexing]".to_owned(),
            None,
            "[protected memory redacted before semantic-memory indexing]".to_owned(),
            "restricted".to_owned(),
        );
    }
    (
        title.to_owned(),
        summary.map(str::to_owned),
        body,
        sensitivity,
    )
}

fn guard_evidence_json(evidence: &str) -> Result<String> {
    let evidence = evidence.trim();
    if evidence.is_empty() {
        return Ok(String::new());
    }
    if evidence.len() > 64 * 1024 {
        return Err(ServiceError::invalid_operation(
            "memory evidence exceeds the 64 KiB limit",
        ));
    }
    if contains_protected_marker(evidence) || evidence.to_ascii_lowercase().contains("-----begin") {
        return Err(ServiceError::invalid_operation(
            "protected values cannot be stored as memory evidence",
        ));
    }
    serde_json::from_str::<Value>(evidence).map_err(|error| {
        ServiceError::invalid_operation(format!("memory evidence must be valid JSON: {error}"))
    })?;
    Ok(evidence.to_owned())
}

fn guard_memory_reason(reason: &str) -> Result<String> {
    let reason = reason.trim();
    if reason.len() > 4 * 1024 {
        return Err(ServiceError::invalid_operation(
            "memory lifecycle reason exceeds the 4 KiB limit",
        ));
    }
    if contains_protected_marker(reason) || reason.to_ascii_lowercase().contains("-----begin") {
        return Err(ServiceError::invalid_operation(
            "protected values cannot be stored as memory lifecycle reasons",
        ));
    }
    Ok(reason.to_owned())
}

fn contains_protected_marker(body: &str) -> bool {
    let body = body.to_ascii_lowercase();
    body.contains("authorization: bearer")
        || body.contains("api_key")
        || body.contains("sk-")
        || body.contains("private key")
}

fn source_ref_from_metadata(metadata_json: &str) -> Option<String> {
    serde_json::from_str::<Value>(metadata_json)
        .ok()
        .and_then(|value| {
            value
                .get("source_ref")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
}

async fn backfill_source_exists<R>(db: &R, source: &MemoryBackfillSource) -> Result<bool>
where
    R: MemoryRepository + Send + Sync,
{
    if source.source_type == MemorySourceType::Review {
        return review_memory_source_exists(
            db,
            &source.project_id,
            &source.source_ref,
            source.confidence == Some(MemoryConfidence::Confirmed),
        )
        .await;
    }

    Ok(db
        .memory_source_exists(
            &source.project_id,
            &source.source_type.to_string(),
            &source.source_ref,
        )
        .await?)
}

async fn review_memory_source_exists<R>(
    db: &R,
    project_id: &str,
    source_ref: &str,
    has_confirmed_outcome: bool,
) -> Result<bool>
where
    R: MemoryRepository + Send + Sync,
{
    let source_type = MemorySourceType::Review.to_string();
    if has_confirmed_outcome {
        return Ok(db
            .memory_source_exists_with_confidence(
                project_id,
                &source_type,
                source_ref,
                &MemoryConfidence::Confirmed.to_string(),
            )
            .await?);
    }

    Ok(db
        .memory_source_exists(project_id, &source_type, source_ref)
        .await?)
}

fn review_has_confirmed_outcome(review: &Review) -> bool {
    matches!(&review.status, ReviewStatus::Passed | ReviewStatus::Failed)
}

fn snippet(content: &str) -> Option<String> {
    let trimmed = content.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed.chars().take(240).collect())
}

fn execution_summary_body(execution: &Execution) -> Option<String> {
    execution
        .summary
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .or_else(|| {
            execution
                .error
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
        })
}

fn review_summary(review: &Review) -> Option<String> {
    review_summary_from_json(&review.step_results_json, &review.status.to_string())
}

fn review_summary_from_json(step_results_json: &str, status: &str) -> Option<String> {
    let details = serde_json::from_str::<Value>(step_results_json).ok()?;
    if let Some(reason) = details
        .get("auditor")
        .and_then(|auditor| auditor.get("reason"))
        .and_then(Value::as_str)
    {
        return Some(reason.to_owned());
    }
    if let Some(verdict) = details
        .get("auditor")
        .and_then(|auditor| auditor.get("verdict"))
        .and_then(Value::as_str)
    {
        return Some(format!("review {status}: {verdict}"));
    }
    Some(format!("review {status}"))
}

fn transition_body(transition: &TransitionLog, hook_results_json: Option<&str>) -> String {
    json!({
        "from_state": &transition.from_state,
        "to_state": &transition.to_state,
        "trigger_name": &transition.trigger_name,
        "triggered_by": &transition.triggered_by,
        "trigger_reason": &transition.trigger_reason,
        "rejection": transition.rejection,
        "hook_results": hook_results_json.or(transition.hook_results_json.as_deref()),
    })
    .to_string()
}

fn transition_has_failure_signal(
    from_state: &str,
    to_state: &str,
    trigger_reason: &str,
    hook_results_json: Option<&str>,
    rejection: bool,
) -> bool {
    if rejection
        || state_name_is_failure(from_state)
        || state_name_is_failure(to_state)
        || text_has_failure_signal(trigger_reason)
    {
        return true;
    }
    let Some(hook_results_json) = hook_results_json else {
        return false;
    };
    serde_json::from_str::<Value>(hook_results_json)
        .ok()
        .and_then(|value| value.as_array().cloned())
        .is_some_and(|entries| entries.iter().any(hook_result_is_failure))
}

fn hook_result_is_failure(entry: &Value) -> bool {
    entry
        .get("outcome")
        .and_then(Value::as_str)
        .is_some_and(|outcome| outcome == "failed")
        || entry
            .get("status")
            .and_then(Value::as_str)
            .is_some_and(|status| matches!(status, "failure" | "hook_error"))
        || entry
            .get("error")
            .and_then(Value::as_str)
            .is_some_and(text_has_failure_signal)
}

fn state_name_is_failure(state: &str) -> bool {
    let lower = state.to_ascii_lowercase();
    lower.contains("fail") || lower.contains("error") || lower.contains("blocked")
}

fn text_has_failure_signal(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    lower.contains("failed")
        || lower.contains("failure")
        || lower.contains("error")
        || lower.contains("hook_error")
}

fn agent_creator(agent_id: String) -> MemoryCreator {
    MemoryCreator {
        creator_type: "agent".to_owned(),
        creator_id: Some(agent_id),
    }
}

fn user_creator(user_id: String) -> MemoryCreator {
    MemoryCreator {
        creator_type: "user".to_owned(),
        creator_id: Some(user_id),
    }
}

fn system_creator() -> MemoryCreator {
    MemoryCreator {
        creator_type: "system".to_owned(),
        creator_id: None,
    }
}

fn backfill_results_by_type() -> Vec<BackfillTypeResult> {
    [
        MemorySourceType::Execution,
        MemorySourceType::Review,
        MemorySourceType::Comment,
        MemorySourceType::Transition,
    ]
    .into_iter()
    .map(|source_type| BackfillTypeResult {
        source_type,
        indexed: 0,
        skipped: 0,
    })
    .collect()
}

fn backfill_result_for_source<'a>(
    results: &'a mut [BackfillTypeResult],
    source_type: &MemorySourceType,
) -> Option<&'a mut BackfillTypeResult> {
    results
        .iter_mut()
        .find(|result| result.source_type == *source_type)
}

fn backfill_summary_from_results(items: Vec<BackfillTypeResult>) -> BackfillSummary {
    let indexed = items.iter().map(|item| item.indexed).sum();
    let skipped = items.iter().map(|item| item.skipped).sum();
    BackfillSummary {
        items,
        indexed,
        skipped,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use db::{
        create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, CreateExecution,
        CreateProject, CreateTask, Execution, ExecutionRepo, ExecutionStatus, MemorySourceType,
        ProjectRepo, Review, ReviewStatus, SqliteDb, TaskRepo,
    };
    use serde_json::json;
    use uuid::Uuid;

    use super::{guard_evidence_json, guard_memory_reason, MemoryService};

    async fn sqlite_db() -> Arc<SqliteDb> {
        let pool = create_sqlite_pool("sqlite::memory:")
            .await
            .expect("pool creates");
        run_migrations(&pool).await.expect("migrations run");
        Arc::new(SqliteDb::new(pool))
    }

    async fn seed_project_task_execution(db: &SqliteDb) -> (Uuid, String, Execution) {
        let project_id = Uuid::new_v4();
        let now = now_rfc3339();
        ProjectRepo::create(
            db,
            CreateProject {
                id: project_id.to_string(),
                name: "Project".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("project creates");
        let task_id = new_uuid_v4();
        TaskRepo::create(
            db,
            CreateTask {
                id: task_id.clone(),
                project_id: project_id.to_string(),
                parent_task_id: None,
                assignee_type: None,
                assignee_id: None,
                title: "Task".to_owned(),
                description: Some("Task description".to_owned()),
                task_type: "task".to_owned(),
                status: "review".to_owned(),
                is_automation: false,
                priority: 0,
                subtask_order: None,
                task_state_config: None,
                merge_config: None,
                plan: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("task creates");
        let execution = ExecutionRepo::create(
            db,
            CreateExecution {
                id: new_uuid_v4(),
                task_id: task_id.clone(),
                agent_id: None,
                role: "coder".to_owned(),
                status: ExecutionStatus::Completed,
                stop_reason: None,
                stopped_by: None,
                resume_policy: None,
                stopped_at: None,
                parent_execution_id: None,
                agent_session_id: None,
                agent_message_id: None,
                last_activity_at: None,
                summary: Some("completed execution summary".to_owned()),
                logs_path: None,
                before_sha: None,
                after_sha: None,
                error: None,
                executor_config_snapshot_json: None,
                workspace_id: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("execution creates");
        (project_id, task_id, execution)
    }

    async fn memory_count(db: &SqliteDb, project_id: &Uuid, source_type: MemorySourceType) -> i64 {
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM memory_item WHERE project_id = ? AND source_type = ?",
        )
        .bind(project_id.to_string())
        .bind(source_type.to_string())
        .fetch_one(db.pool())
        .await
        .expect("memory count loads")
    }

    #[tokio::test]
    async fn record_review_result_if_final_is_idempotent() {
        let db = sqlite_db().await;
        let (project_id, task_id, execution) = seed_project_task_execution(&db).await;
        let service = MemoryService::new(Arc::clone(&db));
        let now = now_rfc3339();
        let review = Review {
            id: new_uuid_v4(),
            task_id,
            execution_id: execution.id,
            reviewer_execution_id: None,
            auditor_execution_id: None,
            attempt_number: 1,
            status: ReviewStatus::Passed,
            step_results_json: json!({ "auditor": { "verdict": "pass" } }).to_string(),
            started_at: now.clone(),
            finished_at: Some(now.clone()),
            created_at: now.clone(),
            updated_at: now,
        };

        let first = service
            .record_review_result_if_final(&project_id.to_string(), &review)
            .await
            .expect("first review memory records");
        let second = service
            .record_review_result_if_final(&project_id.to_string(), &review)
            .await
            .expect("second review memory is skipped");

        assert!(first.is_some());
        assert!(second.is_none());
        assert_eq!(
            memory_count(&db, &project_id, MemorySourceType::Review).await,
            1
        );
    }

    #[tokio::test]
    async fn record_review_result_if_final_records_final_after_awaiting_human() {
        let db = sqlite_db().await;
        let (project_id, task_id, execution) = seed_project_task_execution(&db).await;
        let service = MemoryService::new(Arc::clone(&db));
        let now = now_rfc3339();
        let review_id = new_uuid_v4();
        let awaiting_review = Review {
            id: review_id.clone(),
            task_id: task_id.clone(),
            execution_id: execution.id.clone(),
            reviewer_execution_id: None,
            auditor_execution_id: None,
            attempt_number: 1,
            status: ReviewStatus::AwaitingHuman,
            step_results_json: json!({ "manual": { "state": "waiting" } }).to_string(),
            started_at: now.clone(),
            finished_at: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        };
        let passed_review = Review {
            status: ReviewStatus::Passed,
            step_results_json: json!({ "manual": { "verdict": "pass" } }).to_string(),
            finished_at: Some(now.clone()),
            updated_at: now,
            ..awaiting_review.clone()
        };

        let awaiting = service
            .record_review_result_if_final(&project_id.to_string(), &awaiting_review)
            .await
            .expect("awaiting-human review memory records");
        let passed = service
            .record_review_result_if_final(&project_id.to_string(), &passed_review)
            .await
            .expect("final review memory records");
        let duplicate_passed = service
            .record_review_result_if_final(&project_id.to_string(), &passed_review)
            .await
            .expect("duplicate final review memory is skipped");

        assert!(awaiting.is_some());
        assert!(passed.is_some());
        assert!(duplicate_passed.is_none());
        assert_eq!(
            memory_count(&db, &project_id, MemorySourceType::Review).await,
            2
        );
    }

    #[tokio::test]
    async fn record_execution_summary_if_present_is_idempotent() {
        let db = sqlite_db().await;
        let (project_id, _task_id, execution) = seed_project_task_execution(&db).await;
        let service = MemoryService::new(Arc::clone(&db));

        let first = service
            .record_execution_summary_if_present(&project_id.to_string(), &execution)
            .await
            .expect("first execution memory records");
        let second = service
            .record_execution_summary_if_present(&project_id.to_string(), &execution)
            .await
            .expect("second execution memory is skipped");

        assert!(first.is_some());
        assert!(second.is_none());
        assert_eq!(
            memory_count(&db, &project_id, MemorySourceType::Execution).await,
            1
        );
    }

    #[test]
    fn lifecycle_evidence_and_reason_reject_protected_values() {
        assert!(guard_evidence_json(r#"{"note":"Authorization: Bearer sk-secret"}"#).is_err());
        assert!(guard_memory_reason("private key material").is_err());
        assert_eq!(guard_evidence_json("{}").expect("valid evidence"), "{}");
    }
}
