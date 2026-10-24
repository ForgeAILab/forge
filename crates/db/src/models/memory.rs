//! Memory items, lifecycle assertions, and context manifests.

use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemoryKind {
    Observation,
    Decision,
    Handoff,
    Failure,
    ReviewResult,
    ExecutionSummary,
    Comment,
    Transition,
    Artifact,
    Lesson,
    ContextPack,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemorySourceType {
    Execution,
    Review,
    Comment,
    Transition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemoryConfidence {
    Confirmed,
    Partial,
    Unconfirmed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryItem {
    pub row_id: i64,
    pub id: String,
    pub project_id: Option<String>,
    pub task_id: Option<String>,
    pub execution_id: Option<String>,
    pub scope_type: String,
    pub scope_id: String,
    pub visibility: String,
    pub owner_identity_id: Option<String>,
    pub authority: String,
    pub sensitivity: String,
    pub retention_priority: i64,
    pub provenance_json: String,
    pub publication_source_id: Option<String>,
    pub supersedes_id: Option<String>,
    pub valid_from: Option<String>,
    pub valid_until: Option<String>,
    pub source_event_id: Option<String>,
    pub source_scope_type: Option<String>,
    pub source_scope_id: Option<String>,
    pub source_revision: Option<String>,
    pub source_type: String,
    pub kind: String,
    pub title: String,
    pub summary: Option<String>,
    pub body: String,
    pub metadata_json: String,
    pub confidence: Option<String>,
    pub quality_score: Option<i64>,
    pub created_by_type: Option<String>,
    pub created_by_id: Option<String>,
    pub created_at: String,
}

impl MemoryItem {
    pub fn from_row(row: &SqliteRow) -> std::result::Result<MemoryItem, sqlx::Error> {
        Ok(MemoryItem {
            row_id: row.try_get("row_id")?,
            id: row.try_get("id")?,
            project_id: row.try_get("project_id")?,
            task_id: row.try_get("task_id")?,
            execution_id: row.try_get("execution_id")?,
            scope_type: row.try_get("scope_type")?,
            scope_id: row.try_get("scope_id")?,
            visibility: row.try_get("visibility")?,
            owner_identity_id: row.try_get("owner_identity_id")?,
            authority: row.try_get("authority")?,
            sensitivity: row.try_get("sensitivity")?,
            retention_priority: row.try_get("retention_priority")?,
            provenance_json: row.try_get("provenance_json")?,
            publication_source_id: row.try_get("publication_source_id")?,
            supersedes_id: row.try_get("supersedes_id")?,
            valid_from: row.try_get("valid_from")?,
            valid_until: row.try_get("valid_until")?,
            source_event_id: row.try_get("source_event_id")?,
            source_scope_type: row.try_get("source_scope_type")?,
            source_scope_id: row.try_get("source_scope_id")?,
            source_revision: row.try_get("source_revision")?,
            source_type: row.try_get("source_type")?,
            kind: row.try_get("kind")?,
            title: row.try_get("title")?,
            summary: row.try_get("summary")?,
            body: row.try_get("body")?,
            metadata_json: row.try_get("metadata_json")?,
            confidence: row.try_get("confidence")?,
            quality_score: row.try_get("quality_score")?,
            created_by_type: row.try_get("created_by_type")?,
            created_by_id: row.try_get("created_by_id")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

/// One server-authorized canonical scope that a MemorySource may search.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryScopeGrant {
    pub scope_type: String,
    pub scope_id: String,
    pub visibility: Vec<String>,
    pub identity_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryAccessQuery {
    pub identity_id: Option<String>,
    pub grants: Vec<MemoryScopeGrant>,
    pub query: String,
    pub limit: i64,
    pub cursor: Option<String>,
    pub include_retracted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryGetQuery {
    pub id: String,
    pub identity_id: Option<String>,
    pub grants: Vec<MemoryScopeGrant>,
    pub include_retracted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateMemoryLifecycleAssertion {
    pub id: String,
    pub memory_item_id: String,
    pub assertion_type: String,
    pub related_memory_id: Option<String>,
    pub reason: Option<String>,
    pub evidence_json: String,
    pub asserted_by_type: String,
    pub asserted_by_id: Option<String>,
    pub source_event_id: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryLifecycleAssertion {
    pub id: String,
    pub memory_item_id: String,
    pub assertion_type: String,
    pub related_memory_id: Option<String>,
    pub reason: Option<String>,
    pub evidence_json: String,
    pub asserted_by_type: String,
    pub asserted_by_id: Option<String>,
    pub source_event_id: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateForgeMemorySourceBinding {
    pub id: String,
    pub identity_id: String,
    pub context_scope_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub account_id: Option<String>,
    pub project_id: Option<String>,
    pub task_id: Option<String>,
    pub policy_revision: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgeMemorySourceBinding {
    pub id: String,
    pub identity_id: String,
    pub context_scope_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub account_id: Option<String>,
    pub project_id: Option<String>,
    pub task_id: Option<String>,
    pub policy_revision: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateContextManifest {
    pub id: String,
    pub identity_id: String,
    pub agent_session_id: Option<String>,
    pub context_scope_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub policy_revision: String,
    pub domain_revision: String,
    pub lcm_binding_revision: Option<String>,
    pub runtime_manifest_id: Option<String>,
    pub runtime_manifest_fingerprint: Option<String>,
    pub combined_fingerprint: String,
    pub request_fingerprint: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextManifest {
    pub id: String,
    pub identity_id: String,
    pub agent_session_id: Option<String>,
    pub context_scope_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub policy_revision: String,
    pub domain_revision: String,
    pub lcm_binding_revision: Option<String>,
    pub runtime_manifest_id: Option<String>,
    pub runtime_manifest_fingerprint: Option<String>,
    pub combined_fingerprint: String,
    pub request_fingerprint: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateContextManifestSource {
    pub manifest_id: String,
    pub ordinal: i64,
    pub source_id: String,
    pub source_type: String,
    pub source_revision: String,
    pub selection_reason: String,
    pub disposition: String,
    pub retention_priority: i64,
    pub fragment_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextManifestSource {
    pub manifest_id: String,
    pub ordinal: i64,
    pub source_id: String,
    pub source_type: String,
    pub source_revision: String,
    pub selection_reason: String,
    pub disposition: String,
    pub retention_priority: i64,
    pub fragment_fingerprint: String,
}
