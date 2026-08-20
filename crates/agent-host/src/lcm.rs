//! Forge's least-authority SQLite adapter for Agent Runtime LCM.
//!
//! The runtime owns the timeline/DAG contracts and context projection. Forge
//! owns the identity + canonical-scope binding and persists opaque runtime
//! values behind a host-issued [`LcmView`]. Possession of a timeline/node ID
//! is never sufficient to read or mutate this store.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt,
    sync::Arc,
};

use agent_runtime::{
    context::Sensitivity,
    core::{
        content::{ContentPart, Message},
        guard::ContentGuardRevision,
    },
    harness::{LcmTimelineBinding, LcmViewAuthority},
    lcm::{
        AppendResult, CondensationCommit, ExpansionItem, ExpansionRequest, Fingerprint,
        LcmAppendRequest, LcmClassification, LcmEdge, LcmEntry, LcmEntryId, LcmError, LcmExpansion,
        LcmNode, LcmNodeId, LcmNodeKind, LcmOperationFingerprint, LcmOperationId, LcmRange,
        LcmReader, LcmRevision, LcmSequence, LcmSourceMetadata, LcmSummaryError, LcmSummaryModel,
        LcmSummaryModelRequest, LcmSummaryModelResponse, LcmTimelineId, LcmView, LcmWriter,
        LeafCommit, TruncateResult,
    },
    registry::{RegistryRevision, TrustClass},
};
use async_trait::async_trait;
use db::{
    AgentLcmEntryRecord, AgentLcmNodeRecord, AgentLcmRepo, AppendAgentLcmEntries,
    CommitAgentLcmCondensation, CommitAgentLcmLeaf, CreateAgentLcmTimeline, DbError, SqliteDb,
};
use sqlx::Row;

use crate::AgentHostError;

/// The durable store implementation revision included in checkpoints and
/// compatibility diagnostics.
pub const FORGE_LCM_STORE_REVISION: &str = "forge-sqlite-lcm-1";

/// Stable host policy revision for Task/runtime record projection.
pub const FORGE_TASK_LCM_PROJECTION_REVISION: &str = "forge-task-lcm-projection-1";

/// One admitted Task/runtime/tool record ready for structured LCM storage.
///
/// The message is retained only behind an authorized LCM view.  Its source
/// metadata is immutable and carries the caller's sensitivity, trust, guard,
/// and transformation provenance into the runtime's append fingerprint.
#[derive(Debug, Clone, PartialEq)]
pub struct TaskRuntimeLcmRecord {
    pub source_id: String,
    pub message: Message,
    pub source: LcmSourceMetadata,
}

impl TaskRuntimeLcmRecord {
    pub fn new(source_id: impl Into<String>, message: Message, source: LcmSourceMetadata) -> Self {
        Self {
            source_id: source_id.into(),
            message,
            source,
        }
    }

    /// Builds a record using Forge's conservative role-to-trust mapping.
    pub fn from_message(
        source_id: impl Into<String>,
        message: Message,
        policy: &TaskLcmProjectionPolicy,
    ) -> Self {
        let trust = match message.role {
            agent_runtime::core::content::Role::System => TrustClass::HostPolicy,
            agent_runtime::core::content::Role::User => TrustClass::UserContent,
            agent_runtime::core::content::Role::Assistant => TrustClass::ExternalContent,
            agent_runtime::core::content::Role::Tool => TrustClass::ToolOutput,
        };
        let mut classification = LcmClassification::new(policy.sensitivity, trust);
        if let Some(revision) = &policy.guard_revision {
            classification = classification.with_guard_revision(revision.clone());
        }
        if let Some(revision) = &policy.transformation_revision {
            classification = classification.with_transformation_revision(revision.clone());
        }
        let source = LcmSourceMetadata::new(classification)
            .with_source_revision(policy.source_revision.clone());
        Self::new(source_id, message, source)
    }

    /// Converts canonical runtime history to stable record identities.  The
    /// identity is derived from the source position and exact message
    /// fingerprint, matching Agent Runtime's history-entry convention.
    pub fn from_history(
        history: &[Message],
        policy: &TaskLcmProjectionPolicy,
    ) -> Result<Vec<Self>, LcmError> {
        history
            .iter()
            .enumerate()
            .map(|(sequence, message)| {
                let encoded = serde_json::to_vec(message).map_err(|_| LcmError::StoreFailure)?;
                let fingerprint = Fingerprint::of(encoded);
                Ok(Self::from_message(
                    format!("history:{sequence}:{}", fingerprint.as_str()),
                    message.clone(),
                    policy,
                ))
            })
            .collect()
    }
}

/// Host-selected provenance policy for records projected from a Task turn.
#[derive(Debug, Clone)]
pub struct TaskLcmProjectionPolicy {
    pub source_revision: RegistryRevision,
    pub sensitivity: Sensitivity,
    pub guard_revision: Option<ContentGuardRevision>,
    pub transformation_revision: Option<RegistryRevision>,
}

impl TaskLcmProjectionPolicy {
    pub fn new(source_revision: impl Into<String>) -> Self {
        Self {
            source_revision: RegistryRevision::new(source_revision),
            sensitivity: Sensitivity::Sensitive,
            guard_revision: None,
            transformation_revision: None,
        }
    }

    pub fn with_sensitivity(mut self, sensitivity: Sensitivity) -> Self {
        self.sensitivity = sensitivity;
        self
    }

    pub fn with_guard_revision(mut self, revision: ContentGuardRevision) -> Self {
        self.guard_revision = Some(revision);
        self
    }

    pub fn with_transformation_revision(mut self, revision: RegistryRevision) -> Self {
        self.transformation_revision = Some(revision);
        self
    }
}

/// A bounded host-owned summary adapter used by the embedded runtime until a
/// provider-specific summary route is selected. It deliberately preserves
/// source provenance while never receiving secret-class sources (the runtime
/// coordinator and this store both fail closed for those).
#[derive(Debug, Clone)]
pub struct DeterministicLcmSummaryModel {
    revision: RegistryRevision,
}

impl Default for DeterministicLcmSummaryModel {
    fn default() -> Self {
        Self {
            revision: RegistryRevision::new("forge-lcm-summary-1"),
        }
    }
}

impl DeterministicLcmSummaryModel {
    pub fn new(revision: RegistryRevision) -> Self {
        Self { revision }
    }
}

#[async_trait]
impl LcmSummaryModel for DeterministicLcmSummaryModel {
    fn id(&self) -> &str {
        "forge.lcm.deterministic"
    }

    fn revision(&self) -> &RegistryRevision {
        &self.revision
    }

    async fn summarize(
        &self,
        request: &LcmSummaryModelRequest,
    ) -> Result<LcmSummaryModelResponse, LcmSummaryError> {
        let mut text = String::new();
        for message in &request.messages {
            let body = message.joined_text();
            if body.is_empty() {
                continue;
            }
            if !text.is_empty() {
                text.push('\n');
            }
            text.push_str(body.as_str());
        }
        if text.is_empty() {
            return Err(LcmSummaryError::EmptySource);
        }
        let max_chars = request.target_tokens.saturating_mul(4) as usize;
        if max_chars > 0 && text.chars().count() > max_chars {
            let mut bounded = text.chars().take(max_chars).collect::<String>();
            bounded.push_str(" …");
            text = bounded;
        }
        Ok(LcmSummaryModelResponse {
            input_tokens: (text.chars().count() as u64).saturating_div(4).max(1),
            output_tokens: (text.chars().count() as u64).saturating_div(4).max(1),
            text,
        })
    }
}

#[derive(Clone)]
pub struct SqliteLcmStore {
    db: Arc<SqliteDb>,
    timeline: db::AgentLcmTimeline,
    authority: LcmViewAuthority,
}

impl fmt::Debug for SqliteLcmStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqliteLcmStore")
            .field("timeline", &self.timeline.id)
            .field("identity", &self.timeline.identity_id)
            .field("scope_type", &self.timeline.scope_type)
            .field("scope_id", &self.timeline.scope_id)
            .finish_non_exhaustive()
    }
}

impl SqliteLcmStore {
    /// Creates an adapter for an already-authorized Forge binding.
    ///
    /// Callers should obtain `timeline` through the scope authorization layer,
    /// then retain this adapter for the lifetime of one runtime binding. A
    /// caller cannot construct a usable request view from the timeline ID.
    pub fn new(db: Arc<SqliteDb>, timeline: db::AgentLcmTimeline) -> Self {
        Self {
            db,
            timeline,
            authority: LcmViewAuthority::new(),
        }
    }

    /// Creates or retrieves the durable timeline for one canonical scope.
    pub async fn open_for_binding(
        db: Arc<SqliteDb>,
        identity_id: &str,
        scope_type: &str,
        scope_id: &str,
        authorization_revision: &str,
        now: &str,
    ) -> Result<Self, AgentHostError> {
        let timeline = db
            .create_or_get_lcm_timeline(CreateAgentLcmTimeline {
                id: db::new_uuid_v4(),
                identity_id: identity_id.to_owned(),
                scope_type: scope_type.to_owned(),
                scope_id: scope_id.to_owned(),
                authorization_revision: authorization_revision.to_owned(),
                created_at: now.to_owned(),
                updated_at: now.to_owned(),
            })
            .await
            .map_err(|_| AgentHostError::ProtectedPersistence)?;
        Ok(Self::new(db, timeline))
    }

    /// Host-issued view for the adapter's one timeline.
    pub fn view(&self) -> LcmView {
        self.authority.issue(
            LcmTimelineId::new(self.timeline.id.clone()),
            self.timeline.authorization_revision.clone(),
        )
    }

    /// Authority used to construct runtime timeline bindings.
    pub fn authority(&self) -> LcmViewAuthority {
        self.authority.clone()
    }

    /// Binds a replaceable runtime session to this stable logical timeline.
    pub fn runtime_binding(
        &self,
        runtime_session_id: agent_runtime::core::ids::SessionId,
    ) -> Result<LcmTimelineBinding, AgentHostError> {
        LcmTimelineBinding::new(
            runtime_session_id,
            LcmTimelineId::new(self.timeline.id.clone()),
            RegistryRevision::new(self.timeline.authorization_revision.clone()),
            self.authority.clone(),
        )
        .map_err(|error| AgentHostError::Configuration(error.to_string()))
    }

    pub fn timeline_id(&self) -> &str {
        &self.timeline.id
    }

    pub fn identity_id(&self) -> &str {
        &self.timeline.identity_id
    }

    pub fn scope_type(&self) -> &str {
        &self.timeline.scope_type
    }

    pub fn scope_id(&self) -> &str {
        &self.timeline.scope_id
    }

    pub fn authorization_revision(&self) -> &str {
        &self.timeline.authorization_revision
    }

    /// Projects a complete Task/runtime history into the Task-scoped
    /// timeline. Native runtime turns already use the runtime coordinator's
    /// canonical history projection; this host entry point is for Forge Task
    /// records recovered from an executor or admitted by another host path.
    /// It deliberately rejects a partial tool exchange so no compaction can
    /// split a call from its result.
    pub async fn project_task_records(
        &self,
        view: &LcmView,
        operation_id: &str,
        records: &[TaskRuntimeLcmRecord],
    ) -> Result<AppendResult, LcmError> {
        self.authorize_view(view)?;
        if self.timeline.scope_type != "task" {
            return Err(LcmError::Unauthorized);
        }
        if records.is_empty() {
            return Err(LcmError::Invalid {
                reason: "Task LCM projection requires at least one record".to_owned(),
            });
        }
        let operation_id = LcmOperationId::new(operation_id.to_owned());
        operation_id.validate().map_err(|_| LcmError::Invalid {
            reason: "invalid Task LCM operation identity".to_owned(),
        })?;
        validate_task_records(records)?;

        let entry_ids = records
            .iter()
            .map(|record| task_entry_id(&record.source_id))
            .collect::<Result<Vec<_>, _>>()?;
        let existing_operation = self
            .db
            .get_lcm_operation(&self.timeline.id, operation_id.as_str())
            .await
            .map_err(map_db_error)?;
        let entries = if existing_operation.is_some() {
            // Reconstruct the original sequence positions for an idempotent
            // retry.  Recomputing from the current tail would alter the
            // append fingerprint and incorrectly turn a replay into a
            // conflict.
            self.load_task_entries(&entry_ids).await?
        } else {
            let next_sequence = sqlx::query_scalar::<_, Option<i64>>(
                "SELECT MAX(sequence) + 1 FROM agent_lcm_entry WHERE timeline_id = ?",
            )
            .bind(&self.timeline.id)
            .fetch_one(self.db.pool())
            .await
            .map_err(store_failure)?
            .unwrap_or(0);
            let next_sequence = u64::try_from(next_sequence).map_err(|_| LcmError::StoreFailure)?;
            records
                .iter()
                .zip(entry_ids.iter())
                .enumerate()
                .map(|(offset, (record, entry_id))| {
                    let sequence = next_sequence
                        .checked_add(offset as u64)
                        .ok_or(LcmError::InvalidBound)?;
                    Ok(LcmEntry::new(
                        LcmTimelineId::new(self.timeline.id.clone()),
                        entry_id.clone(),
                        LcmSequence::new(sequence),
                        record.message.clone(),
                        record.source.clone(),
                    ))
                })
                .collect::<Result<Vec<_>, LcmError>>()?
        };

        // A previously recorded operation must have all of its immutable
        // entries available. If it does not, append will return the durable
        // idempotency conflict rather than fabricating a replay result.
        if entries.len() != records.len() {
            return Err(LcmError::IdempotencyConflict);
        }
        if existing_operation.is_some()
            && entries.iter().zip(records).zip(entry_ids.iter()).any(
                |((entry, record), expected_id)| {
                    entry.id.as_str() != expected_id.as_str()
                        || entry.content != record.message
                        || entry.source != record.source
                },
            )
        {
            return Err(LcmError::IdempotencyConflict);
        }
        let request = LcmAppendRequest::new(operation_id, entries);
        LcmWriter::append(self, view, request).await
    }

    /// Projects canonical Task history with stable role-based trust and
    /// caller-supplied sensitivity/guard provenance.
    pub async fn project_task_history(
        &self,
        view: &LcmView,
        operation_id: &str,
        history: &[Message],
        policy: &TaskLcmProjectionPolicy,
    ) -> Result<AppendResult, LcmError> {
        let records = TaskRuntimeLcmRecord::from_history(history, policy)?;
        self.project_task_records(view, operation_id, &records)
            .await
    }

    async fn load_task_entries(&self, entry_ids: &[LcmEntryId]) -> Result<Vec<LcmEntry>, LcmError> {
        let mut entries = Vec::with_capacity(entry_ids.len());
        for entry_id in entry_ids {
            let row = sqlx::query(
                "SELECT timeline_id, entry_id, sequence, content_json, content_fingerprint, source_json, created_at
                 FROM agent_lcm_entry WHERE timeline_id = ? AND entry_id = ?",
            )
            .bind(&self.timeline.id)
            .bind(entry_id.as_str())
            .fetch_optional(self.db.pool())
            .await
            .map_err(store_failure)?
            .ok_or(LcmError::IdempotencyConflict)?;
            let record = AgentLcmEntryRecord {
                timeline_id: row.try_get("timeline_id").map_err(store_failure)?,
                entry_id: row.try_get("entry_id").map_err(store_failure)?,
                sequence: row.try_get("sequence").map_err(store_failure)?,
                content_json: row.try_get("content_json").map_err(store_failure)?,
                content_fingerprint: row.try_get("content_fingerprint").map_err(store_failure)?,
                source_json: row.try_get("source_json").map_err(store_failure)?,
                created_at: row.try_get("created_at").map_err(store_failure)?,
            };
            entries.push(entry_from_record(record)?);
        }
        Ok(entries)
    }

    /// Lazily admits migrated Agent Chat messages into this identity's Agent
    /// Chat timeline. The operation is deliberately model-free: it only reads
    /// already-authorized durable messages, appends missing immutable entries
    /// in bounded batches, and can be retried after a crash.
    pub async fn backfill_agent_chat_messages(
        &self,
        view: &LcmView,
        chat_id: &str,
    ) -> Result<usize, LcmError> {
        self.authorize_view(view)?;
        if self.timeline.scope_type != "agent_chat" || self.timeline.scope_id != chat_id {
            return Err(LcmError::Unauthorized);
        }
        let rows = sqlx::query(
            "SELECT id, author_type, content, sensitivity
             FROM agent_chat_message WHERE chat_id = ? ORDER BY sequence ASC, id ASC",
        )
        .bind(chat_id)
        .fetch_all(self.db.pool())
        .await
        .map_err(store_failure)?;
        let admitted = sqlx::query_scalar::<_, String>(
            "SELECT entry_id FROM agent_lcm_entry WHERE timeline_id = ?",
        )
        .bind(&self.timeline.id)
        .fetch_all(self.db.pool())
        .await
        .map_err(store_failure)?
        .into_iter()
        .collect::<std::collections::HashSet<_>>();
        let next_sequence = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT MAX(sequence) + 1 FROM agent_lcm_entry WHERE timeline_id = ?",
        )
        .bind(&self.timeline.id)
        .fetch_one(self.db.pool())
        .await
        .map_err(store_failure)?
        .unwrap_or(0);
        let mut missing = Vec::new();
        for row in rows {
            let id: String = row.try_get("id").map_err(store_failure)?;
            if admitted.contains(&id) {
                continue;
            }
            let author_type: String = row.try_get("author_type").map_err(store_failure)?;
            let content: String = row.try_get("content").map_err(store_failure)?;
            let sensitivity: String = row.try_get("sensitivity").map_err(store_failure)?;
            let role = match author_type.as_str() {
                "user" => agent_runtime::core::content::Role::User,
                "agent" => agent_runtime::core::content::Role::Assistant,
                _ => agent_runtime::core::content::Role::System,
            };
            let sensitivity = match sensitivity.as_str() {
                "public" => agent_runtime::context::Sensitivity::Public,
                "sensitive" => agent_runtime::context::Sensitivity::Sensitive,
                "secret" => agent_runtime::context::Sensitivity::Secret,
                _ => agent_runtime::context::Sensitivity::Internal,
            };
            let missing_offset = missing.len();
            missing.push(LcmEntry::new(
                LcmTimelineId::new(self.timeline.id.clone()),
                agent_runtime::lcm::LcmEntryId::new(id),
                LcmSequence::new(
                    u64::try_from(next_sequence)
                        .map_err(|_| LcmError::InvalidBound)?
                        .saturating_add(missing_offset as u64),
                ),
                Message::text(role, content),
                LcmSourceMetadata::new(LcmClassification::new(
                    sensitivity,
                    agent_runtime::registry::TrustClass::UserContent,
                ))
                .with_source_revision(RegistryRevision::new("forge-agent-chat-backfill-1")),
            ));
        }
        let mut admitted_count = 0;
        for chunk in missing.chunks(128) {
            let first = chunk.first().ok_or(LcmError::StoreFailure)?;
            let last = chunk.last().ok_or(LcmError::StoreFailure)?;
            let request = LcmAppendRequest::new(
                agent_runtime::lcm::LcmOperationId::new(format!(
                    "agent-chat-backfill-{}-{}-{}",
                    chat_id, first.id, last.id
                )),
                chunk.to_vec(),
            );
            self.append(view, request).await?;
            admitted_count += chunk.len();
        }
        Ok(admitted_count)
    }
}

#[async_trait]
impl LcmReader for SqliteLcmStore {
    fn store_revision(&self) -> RegistryRevision {
        RegistryRevision::new(FORGE_LCM_STORE_REVISION)
    }

    fn authorize_view(&self, view: &LcmView) -> Result<(), LcmError> {
        // This must remain before all database lookups. The opaque authority
        // check is deliberately separate from the timeline-ID equality check.
        self.authority.authorize(view)?;
        if view.timeline_id().as_str() != self.timeline.id {
            return Err(LcmError::Unauthorized);
        }
        if view.authorization_revision() != Some(self.timeline.authorization_revision.as_str()) {
            return Err(LcmError::Unauthorized);
        }
        Ok(())
    }

    async fn current_revision(&self, view: &LcmView) -> Result<LcmRevision, LcmError> {
        self.authorize_view(view)?;
        let timeline = self
            .db
            .get_lcm_timeline(&self.timeline.id)
            .await
            .map_err(map_db_error)?
            .ok_or(LcmError::MissingSource)?;
        Ok(LcmRevision::new(nonnegative_revision(timeline.revision)?))
    }

    async fn load_range(
        &self,
        view: &LcmView,
        range: LcmRange,
        limit: usize,
    ) -> Result<Vec<LcmEntry>, LcmError> {
        self.authorize_view(view)?;
        validate_limit(limit)?;
        let records = self
            .db
            .list_lcm_entries(
                &self.timeline.id,
                i64::try_from(range.start.get()).map_err(|_| LcmError::InvalidBound)?,
                i64::try_from(range.end.get()).map_err(|_| LcmError::InvalidBound)?,
                i64::try_from(limit).map_err(|_| LcmError::InvalidBound)?,
            )
            .await
            .map_err(map_db_error)?;
        records.into_iter().map(entry_from_record).collect()
    }

    async fn active_nodes(&self, view: &LcmView) -> Result<Vec<LcmNode>, LcmError> {
        self.authorize_view(view)?;
        self.db
            .list_lcm_nodes(&self.timeline.id, true)
            .await
            .map_err(map_db_error)?
            .into_iter()
            .map(node_from_record)
            .collect()
    }

    async fn node(&self, view: &LcmView, node_id: &LcmNodeId) -> Result<LcmNode, LcmError> {
        self.authorize_view(view)?;
        node_id.validate().map_err(|_| LcmError::Invalid {
            reason: "invalid LCM node identity".to_owned(),
        })?;
        let record = self
            .db
            .get_lcm_node(&self.timeline.id, node_id.as_str())
            .await
            .map_err(map_db_error)?
            .ok_or(LcmError::MissingSource)?;
        node_from_record(record)
    }

    async fn expand(
        &self,
        view: &LcmView,
        request: ExpansionRequest,
    ) -> Result<LcmExpansion, LcmError> {
        self.authorize_view(view)?;
        validate_limit(request.limit)?;
        let node = self.node(view, &request.node_id).await?;
        let source_fingerprint = expansion_fingerprint(&node);
        let offset = match request.cursor {
            None => 0,
            Some(cursor)
                if cursor.node_id == node.id && cursor.source_fingerprint == source_fingerprint =>
            {
                cursor.offset
            }
            Some(_) => return Err(LcmError::InvalidCursor),
        };
        if offset > node.edges.len() {
            return Err(LcmError::InvalidCursor);
        }
        let end = offset.saturating_add(request.limit).min(node.edges.len());
        let entries = if node.kind == LcmNodeKind::Leaf {
            self.db
                .list_lcm_entries(
                    &self.timeline.id,
                    i64::try_from(node.range.start.get()).map_err(|_| LcmError::InvalidBound)?,
                    i64::try_from(node.range.end.get()).map_err(|_| LcmError::InvalidBound)?,
                    i64::try_from(node.range.len()).map_err(|_| LcmError::InvalidBound)?,
                )
                .await
                .map_err(map_db_error)?
                .into_iter()
                .map(entry_from_record)
                .collect::<Result<Vec<_>, _>>()?
        } else {
            Vec::new()
        };
        let entries_by_id = entries
            .into_iter()
            .map(|entry| (entry.id.clone(), entry))
            .collect::<HashMap<_, _>>();
        let mut items = Vec::with_capacity(end.saturating_sub(offset));
        for edge in &node.edges[offset..end] {
            items.push(match edge {
                LcmEdge::Entry(entry_id) => ExpansionItem::Entry(
                    entries_by_id
                        .get(entry_id)
                        .cloned()
                        .ok_or(LcmError::MissingSource)?,
                ),
                LcmEdge::Node(child_id) => ExpansionItem::Node(self.node(view, child_id).await?),
            });
        }
        let complete = end == node.edges.len();
        Ok(LcmExpansion {
            node_id: node.id,
            source_fingerprint: source_fingerprint.clone(),
            items,
            complete,
            next_cursor: (!complete).then_some(agent_runtime::lcm::LcmExpansionCursor {
                node_id: request.node_id,
                offset: end,
                source_fingerprint,
            }),
        })
    }
}

#[async_trait]
impl LcmWriter for SqliteLcmStore {
    async fn append(
        &self,
        view: &LcmView,
        request: LcmAppendRequest,
    ) -> Result<AppendResult, LcmError> {
        self.authorize_view(view)?;
        request
            .operation_id
            .validate()
            .map_err(|_| LcmError::Invalid {
                reason: "invalid LCM operation identity".to_owned(),
            })?;
        if !request.validate_fingerprint() {
            return Err(LcmError::IdempotencyConflict);
        }
        for entry in &request.entries {
            entry.validate().map_err(|_| LcmError::Invalid {
                reason: "invalid LCM entry".to_owned(),
            })?;
            if entry.timeline_id.as_str() != self.timeline.id {
                return Err(LcmError::CrossTimeline);
            }
        }
        if let Some(operation) = self
            .db
            .get_lcm_operation(&self.timeline.id, request.operation_id.as_str())
            .await
            .map_err(map_db_error)?
        {
            if operation.operation_kind != "append"
                || operation.operation_fingerprint != request.operation_fingerprint.to_string()
            {
                return Err(LcmError::IdempotencyConflict);
            }
            return Ok(AppendResult {
                revision: LcmRevision::new(nonnegative_revision(operation.result_revision)?),
                entries: usize::try_from(operation.result_entries)
                    .map_err(|_| LcmError::StoreFailure)?,
                already_committed: true,
            });
        }
        // Append requests intentionally do not carry a CAS revision in the
        // runtime contract: the durable store serializes them at its current
        // tail. Refresh the timeline row so a long-lived adapter survives
        // multiple commits and process-local session rotation.
        let expected_revision = self
            .db
            .get_lcm_timeline(&self.timeline.id)
            .await
            .map_err(map_db_error)?
            .ok_or(LcmError::MissingSource)?
            .revision;
        let tail = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT MAX(sequence) FROM agent_lcm_entry WHERE timeline_id = ?",
        )
        .bind(&self.timeline.id)
        .fetch_one(self.db.pool())
        .await
        .map_err(store_failure)?
        .map(|value| value.saturating_add(1))
        .unwrap_or(0);
        if let Some(first) = request.entries.first() {
            let actual = i64::try_from(first.sequence.get()).map_err(|_| LcmError::InvalidBound)?;
            if actual != tail {
                return Err(LcmError::SequenceGap {
                    expected: u64::try_from(tail).map_err(|_| LcmError::StoreFailure)?,
                    actual: first.sequence.get(),
                });
            }
        }
        let records = request
            .entries
            .iter()
            .map(entry_record)
            .collect::<Result<Vec<_>, _>>()?;
        let result = self
            .db
            .append_lcm_entries(AppendAgentLcmEntries {
                timeline_id: self.timeline.id.clone(),
                expected_revision,
                operation_id: request.operation_id.to_string(),
                operation_fingerprint: request.operation_fingerprint.to_string(),
                entries: records,
                expected_sequence: tail,
                updated_at: db::now_rfc3339(),
            })
            .await
            .map_err(|error| self.map_mutation_error(error, expected_revision))?;
        Ok(AppendResult {
            revision: LcmRevision::new(nonnegative_revision(result.revision)?),
            entries: usize::try_from(result.entries).map_err(|_| LcmError::StoreFailure)?,
            already_committed: result.already_committed,
        })
    }

    async fn commit_leaf(
        &self,
        view: &LcmView,
        request: LeafCommit,
    ) -> Result<agent_runtime::lcm::CommitResult, LcmError> {
        self.authorize_view(view)?;
        let operation_fingerprint = request.operation_fingerprint.clone().unwrap_or_else(|| {
            request.computed_operation_fingerprint(&LcmTimelineId::new(self.timeline.id.clone()))
        });
        let computed_fingerprint =
            request.computed_operation_fingerprint(&LcmTimelineId::new(self.timeline.id.clone()));
        if operation_fingerprint != computed_fingerprint {
            return Err(LcmError::IdempotencyConflict);
        }
        request
            .operation_id
            .validate()
            .map_err(|_| LcmError::Invalid {
                reason: "invalid LCM operation identity".to_owned(),
            })?;
        request.node_id.validate().map_err(|_| LcmError::Invalid {
            reason: "invalid LCM node identity".to_owned(),
        })?;
        if request.classification.is_secret() {
            return Err(LcmError::SecretSource);
        }
        let source_entries = self
            .db
            .list_lcm_entries(
                &self.timeline.id,
                i64::try_from(request.range.start.get()).map_err(|_| LcmError::InvalidBound)?,
                i64::try_from(request.range.end.get()).map_err(|_| LcmError::InvalidBound)?,
                i64::try_from(request.range.len()).map_err(|_| LcmError::InvalidBound)?,
            )
            .await
            .map_err(map_db_error)?;
        let source_entries = source_entries
            .into_iter()
            .map(entry_from_record)
            .collect::<Result<Vec<_>, _>>()?;
        if source_entries
            .iter()
            .any(|entry| entry.source.classification.is_secret())
        {
            return Err(LcmError::SecretSource);
        }
        let canonical_entry_ids = source_entries
            .iter()
            .map(|entry| entry.id.clone())
            .collect::<Vec<_>>();
        if canonical_entry_ids != request.entry_ids
            || agent_runtime::lcm::source_fingerprint_entries(&source_entries)
                != request.source_fingerprint
            || LcmClassification::join_all(
                source_entries
                    .iter()
                    .map(|entry| entry.source.classification.clone()),
            ) != request.classification
        {
            return Err(LcmError::Invalid {
                reason: "LCM leaf source metadata does not match entries".to_owned(),
            });
        }
        let expected_revision = request.expected_revision.get();
        let expected_revision_i64 =
            i64::try_from(expected_revision).map_err(|_| LcmError::InvalidBound)?;
        let actual_revision = self
            .db
            .get_lcm_timeline(&self.timeline.id)
            .await
            .map_err(map_db_error)?
            .ok_or(LcmError::MissingSource)?
            .revision;
        if actual_revision != expected_revision_i64 {
            return Err(LcmError::RevisionConflict {
                expected: request.expected_revision,
                actual: LcmRevision::new(nonnegative_revision(actual_revision)?),
            });
        }
        let revision = expected_revision.checked_add(1).ok_or(LcmError::Invalid {
            reason: "LCM revision exhausted".to_owned(),
        })?;
        let node = LcmNode {
            timeline_id: LcmTimelineId::new(self.timeline.id.clone()),
            id: request.node_id.clone(),
            kind: LcmNodeKind::Leaf,
            range: request.range,
            edges: request
                .entry_ids
                .iter()
                .cloned()
                .map(LcmEdge::Entry)
                .collect(),
            source_fingerprint: request.source_fingerprint.clone(),
            summary_revision: LcmNode::compute_summary_revision(
                &request.source_fingerprint,
                &request.provenance,
                &request.summary,
            ),
            summary: request.summary.clone(),
            policy_revision: request.policy_revision.clone(),
            algorithm_revision: request.algorithm_revision.clone(),
            sizer_revision: request.sizer_revision.clone(),
            provenance: request.provenance.clone(),
            token_count: request.token_count,
            source_token_count: request.source_token_count,
            classification: request.classification.clone(),
            revision: LcmRevision::new(revision),
            superseded_by: None,
            operation_id: request.operation_id.clone(),
            operation_fingerprint: operation_fingerprint.clone(),
        };
        node.validate().map_err(|_| LcmError::Invalid {
            reason: "invalid LCM leaf node".to_owned(),
        })?;
        let result = self
            .db
            .commit_lcm_leaf(CommitAgentLcmLeaf {
                timeline_id: self.timeline.id.clone(),
                expected_revision: expected_revision_i64,
                operation_id: request.operation_id.to_string(),
                operation_fingerprint: operation_fingerprint.to_string(),
                node: node_record(&node)?,
                entry_ids: request.entry_ids.iter().map(ToString::to_string).collect(),
                updated_at: db::now_rfc3339(),
            })
            .await
            .map_err(|error| self.map_mutation_error(error, expected_revision_i64))?;
        let node = self
            .db
            .get_lcm_node(
                &self.timeline.id,
                result.node_id.as_deref().unwrap_or(node.id.as_str()),
            )
            .await
            .map_err(map_db_error)?
            .ok_or(LcmError::MissingSource)
            .and_then(node_from_record)?;
        Ok(agent_runtime::lcm::CommitResult {
            node,
            revision: LcmRevision::new(nonnegative_revision(result.revision)?),
            already_committed: result.already_committed,
        })
    }

    async fn commit_condensation(
        &self,
        view: &LcmView,
        request: CondensationCommit,
    ) -> Result<agent_runtime::lcm::CommitResult, LcmError> {
        self.authorize_view(view)?;
        let operation_fingerprint = request.operation_fingerprint.clone().unwrap_or_else(|| {
            request.computed_operation_fingerprint(&LcmTimelineId::new(self.timeline.id.clone()))
        });
        let computed_fingerprint =
            request.computed_operation_fingerprint(&LcmTimelineId::new(self.timeline.id.clone()));
        if operation_fingerprint != computed_fingerprint {
            return Err(LcmError::IdempotencyConflict);
        }
        request
            .operation_id
            .validate()
            .map_err(|_| LcmError::Invalid {
                reason: "invalid LCM operation identity".to_owned(),
            })?;
        request.node_id.validate().map_err(|_| LcmError::Invalid {
            reason: "invalid LCM node identity".to_owned(),
        })?;
        if request.classification.is_secret() {
            return Err(LcmError::SecretSource);
        }
        let child_nodes = self
            .db
            .list_lcm_nodes(&self.timeline.id, false)
            .await
            .map_err(map_db_error)?
            .into_iter()
            .filter(|node| {
                request
                    .child_ids
                    .iter()
                    .any(|child| child.as_str() == node.node_id)
            })
            .map(node_from_record)
            .collect::<Result<Vec<_>, _>>()?;
        if child_nodes.len() != request.child_ids.len()
            || child_nodes.iter().any(|node| !node.is_active())
            || child_nodes
                .iter()
                .map(|node| node.id.clone())
                .collect::<Vec<_>>()
                != request.child_ids
            || agent_runtime::lcm::source_fingerprint_nodes(&child_nodes)
                != request.source_fingerprint
            || LcmClassification::join_all(
                child_nodes.iter().map(|node| node.classification.clone()),
            ) != request.classification
        {
            return Err(LcmError::Invalid {
                reason: "LCM condensation source metadata does not match children".to_owned(),
            });
        }
        if child_nodes
            .iter()
            .any(|node| node.classification.is_secret())
        {
            return Err(LcmError::SecretSource);
        }
        let expected_revision = request.expected_revision.get();
        let expected_revision_i64 =
            i64::try_from(expected_revision).map_err(|_| LcmError::InvalidBound)?;
        let actual_revision = self
            .db
            .get_lcm_timeline(&self.timeline.id)
            .await
            .map_err(map_db_error)?
            .ok_or(LcmError::MissingSource)?
            .revision;
        if actual_revision != expected_revision_i64 {
            return Err(LcmError::RevisionConflict {
                expected: request.expected_revision,
                actual: LcmRevision::new(nonnegative_revision(actual_revision)?),
            });
        }
        let revision = expected_revision.checked_add(1).ok_or(LcmError::Invalid {
            reason: "LCM revision exhausted".to_owned(),
        })?;
        let node = LcmNode {
            timeline_id: LcmTimelineId::new(self.timeline.id.clone()),
            id: request.node_id.clone(),
            kind: LcmNodeKind::Condensed,
            range: request.range,
            edges: request
                .child_ids
                .iter()
                .cloned()
                .map(LcmEdge::Node)
                .collect(),
            source_fingerprint: request.source_fingerprint.clone(),
            summary_revision: LcmNode::compute_summary_revision(
                &request.source_fingerprint,
                &request.provenance,
                &request.summary,
            ),
            summary: request.summary.clone(),
            policy_revision: request.policy_revision.clone(),
            algorithm_revision: request.algorithm_revision.clone(),
            sizer_revision: request.sizer_revision.clone(),
            provenance: request.provenance.clone(),
            token_count: request.token_count,
            source_token_count: request.source_token_count,
            classification: request.classification.clone(),
            revision: LcmRevision::new(revision),
            superseded_by: None,
            operation_id: request.operation_id.clone(),
            operation_fingerprint: operation_fingerprint.clone(),
        };
        node.validate().map_err(|_| LcmError::Invalid {
            reason: "invalid LCM condensed node".to_owned(),
        })?;
        let result = self
            .db
            .commit_lcm_condensation(CommitAgentLcmCondensation {
                timeline_id: self.timeline.id.clone(),
                expected_revision: expected_revision_i64,
                operation_id: request.operation_id.to_string(),
                operation_fingerprint: operation_fingerprint.to_string(),
                node: node_record(&node)?,
                child_node_ids: request.child_ids.iter().map(ToString::to_string).collect(),
                updated_at: db::now_rfc3339(),
            })
            .await
            .map_err(|error| self.map_mutation_error(error, expected_revision_i64))?;
        let node = self
            .db
            .get_lcm_node(
                &self.timeline.id,
                result.node_id.as_deref().unwrap_or(node.id.as_str()),
            )
            .await
            .map_err(map_db_error)?
            .ok_or(LcmError::MissingSource)
            .and_then(node_from_record)?;
        Ok(agent_runtime::lcm::CommitResult {
            node,
            revision: LcmRevision::new(nonnegative_revision(result.revision)?),
            already_committed: result.already_committed,
        })
    }

    async fn truncate_from(
        &self,
        view: &LcmView,
        from: LcmSequence,
    ) -> Result<TruncateResult, LcmError> {
        self.authorize_view(view)?;
        let from = i64::try_from(from.get()).map_err(|_| LcmError::InvalidBound)?;
        let result = self
            .db
            .truncate_lcm_entries_from(&self.timeline.id, from, &db::now_rfc3339())
            .await
            .map_err(|error| match error {
                DbError::Check(message) if message.contains("summary node") => {
                    LcmError::RangeOverlap
                }
                other => map_db_error(other),
            })?;
        Ok(TruncateResult {
            revision: LcmRevision::new(nonnegative_revision(result.revision)?),
            removed: usize::try_from(result.removed).map_err(|_| LcmError::StoreFailure)?,
        })
    }
}

impl SqliteLcmStore {
    fn map_mutation_error(&self, error: DbError, expected: i64) -> LcmError {
        match error {
            DbError::VersionConflict => LcmError::RevisionConflict {
                expected: LcmRevision::new(expected as u64),
                actual: LcmRevision::new(
                    self.timeline.revision.max(0).try_into().unwrap_or(u64::MAX),
                ),
            },
            DbError::Check(message) if message.contains("operation") => {
                LcmError::IdempotencyConflict
            }
            DbError::Check(message) if message.contains("sequence gap") => {
                parse_sequence_gap(&message)
            }
            DbError::Check(message) if message.contains("overlaps") => LcmError::RangeOverlap,
            DbError::NotFound => LcmError::MissingSource,
            other => map_db_error(other),
        }
    }
}

fn validate_limit(limit: usize) -> Result<(), LcmError> {
    if limit == 0 || limit > 1_024 {
        Err(LcmError::InvalidBound)
    } else {
        Ok(())
    }
}

fn task_entry_id(source_id: &str) -> Result<LcmEntryId, LcmError> {
    if source_id.trim().is_empty() {
        return Err(LcmError::Invalid {
            reason: "Task LCM source identity must not be blank".to_owned(),
        });
    }
    Ok(LcmEntryId::new(format!(
        "task-record:{}",
        Fingerprint::of(source_id.as_bytes()).as_str()
    )))
}

fn validate_task_records(records: &[TaskRuntimeLcmRecord]) -> Result<(), LcmError> {
    let mut source_ids = BTreeSet::new();
    let mut calls = BTreeMap::new();
    let mut results = BTreeSet::new();
    for record in records {
        if !source_ids.insert(record.source_id.as_str()) {
            return Err(LcmError::IdempotencyConflict);
        }
        record.source.validate().map_err(|_| LcmError::Invalid {
            reason: "Task LCM source provenance is invalid".to_owned(),
        })?;
        if record.source.classification.is_secret() {
            return Err(LcmError::SecretSource);
        }
        for part in &record.message.content {
            match part {
                ContentPart::ToolCall(call) => {
                    if record.message.role != agent_runtime::core::content::Role::Assistant {
                        return Err(LcmError::Invalid {
                            reason: "Task LCM tool calls require an assistant record".to_owned(),
                        });
                    }
                    if calls
                        .insert(call.id.as_str().to_owned(), call.name.clone())
                        .is_some()
                    {
                        return Err(LcmError::Invalid {
                            reason: "Task LCM tool call identity is duplicated".to_owned(),
                        });
                    }
                }
                ContentPart::ToolResult(result) => {
                    if record.message.role != agent_runtime::core::content::Role::Tool {
                        return Err(LcmError::Invalid {
                            reason: "Task LCM tool results require a tool record".to_owned(),
                        });
                    }
                    if !results.insert(result.call_id.as_str().to_owned()) {
                        return Err(LcmError::Invalid {
                            reason: "Task LCM tool result identity is duplicated".to_owned(),
                        });
                    }
                    let Some(call_name) = calls.get(result.call_id.as_str()) else {
                        return Err(LcmError::Invalid {
                            reason: "Task LCM tool result precedes its call".to_owned(),
                        });
                    };
                    if call_name != &result.name {
                        return Err(LcmError::Invalid {
                            reason: "Task LCM tool call/result names do not match".to_owned(),
                        });
                    }
                }
                _ => {}
            }
        }
    }
    let call_ids = calls.keys().collect::<BTreeSet<_>>();
    let result_ids = results.iter().collect::<BTreeSet<_>>();
    if call_ids != result_ids {
        return Err(LcmError::Invalid {
            reason: "Task LCM tool call/result pair is incomplete".to_owned(),
        });
    }
    Ok(())
}

fn nonnegative_revision(revision: i64) -> Result<u64, LcmError> {
    u64::try_from(revision).map_err(|_| LcmError::StoreFailure)
}

fn map_db_error(error: DbError) -> LcmError {
    match error {
        DbError::NotFound => LcmError::MissingSource,
        DbError::VersionConflict => LcmError::StoreFailure,
        DbError::Check(message) if message.contains("operation") => LcmError::IdempotencyConflict,
        DbError::Check(message) if message.contains("sequence gap") => parse_sequence_gap(&message),
        DbError::Check(message) if message.contains("overlaps") => LcmError::RangeOverlap,
        DbError::Check(_) => LcmError::Invalid {
            reason: "invalid LCM persistence record".to_owned(),
        },
        other => {
            tracing::warn!(error = %other, "LCM store operation failed");
            LcmError::StoreFailure
        }
    }
}

/// Logs a swallowed sqlx error before collapsing it to `StoreFailure`. A bare
/// `.map_err(|_| LcmError::StoreFailure)` used to discard the cause, which
/// made a SQLite busy/busy-snapshot failure indistinguishable from any other
/// store error in the logs.
fn store_failure(error: impl std::fmt::Display) -> LcmError {
    tracing::warn!(error = %error, "LCM store operation failed");
    LcmError::StoreFailure
}

fn parse_sequence_gap(message: &str) -> LcmError {
    let mut numbers = message
        .split_whitespace()
        .filter_map(|value| value.parse::<u64>().ok());
    LcmError::SequenceGap {
        expected: numbers.next().unwrap_or(0),
        actual: numbers.next().unwrap_or(0),
    }
}

fn entry_record(entry: &LcmEntry) -> Result<AgentLcmEntryRecord, LcmError> {
    Ok(AgentLcmEntryRecord {
        timeline_id: entry.timeline_id.to_string(),
        entry_id: entry.id.to_string(),
        sequence: i64::try_from(entry.sequence.get()).map_err(|_| LcmError::InvalidBound)?,
        content_json: serde_json::to_string(&entry.content).map_err(|_| LcmError::StoreFailure)?,
        content_fingerprint: entry.content_fingerprint.to_string(),
        source_json: serde_json::to_string(&entry.source).map_err(|_| LcmError::StoreFailure)?,
        created_at: db::now_rfc3339(),
    })
}

fn entry_from_record(record: AgentLcmEntryRecord) -> Result<LcmEntry, LcmError> {
    let content: Message =
        serde_json::from_str(&record.content_json).map_err(|_| LcmError::StoreFailure)?;
    let source: LcmSourceMetadata =
        serde_json::from_str(&record.source_json).map_err(|_| LcmError::StoreFailure)?;
    let entry = LcmEntry::with_fingerprint(
        LcmTimelineId::new(record.timeline_id),
        agent_runtime::lcm::LcmEntryId::new(record.entry_id),
        LcmSequence::new(u64::try_from(record.sequence).map_err(|_| LcmError::StoreFailure)?),
        content,
        Fingerprint::from_hex(record.content_fingerprint),
        source,
    );
    entry.validate().map_err(|_| LcmError::StoreFailure)?;
    Ok(entry)
}

fn node_record(node: &LcmNode) -> Result<AgentLcmNodeRecord, LcmError> {
    Ok(AgentLcmNodeRecord {
        timeline_id: node.timeline_id.to_string(),
        node_id: node.id.to_string(),
        kind: match node.kind {
            LcmNodeKind::Leaf => "leaf".to_owned(),
            LcmNodeKind::Condensed => "condensed".to_owned(),
        },
        range_start: i64::try_from(node.range.start.get()).map_err(|_| LcmError::InvalidBound)?,
        range_end: i64::try_from(node.range.end.get()).map_err(|_| LcmError::InvalidBound)?,
        edges_json: serde_json::to_string(&node.edges).map_err(|_| LcmError::StoreFailure)?,
        source_fingerprint: node.source_fingerprint.to_string(),
        summary_revision: node.summary_revision.to_string(),
        summary: node.summary.clone(),
        policy_revision: node.policy_revision.to_string(),
        algorithm_revision: node.algorithm_revision.to_string(),
        sizer_revision: node.sizer_revision.to_string(),
        provenance_json: serde_json::to_string(&node.provenance)
            .map_err(|_| LcmError::StoreFailure)?,
        token_count: i64::try_from(node.token_count).map_err(|_| LcmError::InvalidBound)?,
        source_token_count: i64::try_from(node.source_token_count)
            .map_err(|_| LcmError::InvalidBound)?,
        classification_json: serde_json::to_string(&node.classification)
            .map_err(|_| LcmError::StoreFailure)?,
        revision: i64::try_from(node.revision.get()).map_err(|_| LcmError::InvalidBound)?,
        superseded_by: node.superseded_by.as_ref().map(ToString::to_string),
        operation_id: node.operation_id.to_string(),
        operation_fingerprint: node.operation_fingerprint.to_string(),
        created_at: db::now_rfc3339(),
    })
}

fn node_from_record(record: AgentLcmNodeRecord) -> Result<LcmNode, LcmError> {
    let kind = match record.kind.as_str() {
        "leaf" => LcmNodeKind::Leaf,
        "condensed" => LcmNodeKind::Condensed,
        _ => return Err(LcmError::StoreFailure),
    };
    let edges: Vec<LcmEdge> =
        serde_json::from_str(&record.edges_json).map_err(|_| LcmError::StoreFailure)?;
    let provenance =
        serde_json::from_str(&record.provenance_json).map_err(|_| LcmError::StoreFailure)?;
    let classification: LcmClassification =
        serde_json::from_str(&record.classification_json).map_err(|_| LcmError::StoreFailure)?;
    let node = LcmNode {
        timeline_id: LcmTimelineId::new(record.timeline_id),
        id: LcmNodeId::new(record.node_id),
        kind,
        range: LcmRange::new(
            LcmSequence::new(
                u64::try_from(record.range_start).map_err(|_| LcmError::StoreFailure)?,
            ),
            LcmSequence::new(u64::try_from(record.range_end).map_err(|_| LcmError::StoreFailure)?),
        )
        .map_err(|_| LcmError::StoreFailure)?,
        edges,
        source_fingerprint: Fingerprint::from_hex(record.source_fingerprint),
        summary_revision: RegistryRevision::new(record.summary_revision),
        summary: record.summary,
        policy_revision: RegistryRevision::new(record.policy_revision),
        algorithm_revision: RegistryRevision::new(record.algorithm_revision),
        sizer_revision: RegistryRevision::new(record.sizer_revision),
        provenance,
        token_count: u64::try_from(record.token_count).map_err(|_| LcmError::StoreFailure)?,
        source_token_count: u64::try_from(record.source_token_count)
            .map_err(|_| LcmError::StoreFailure)?,
        classification,
        revision: LcmRevision::new(
            u64::try_from(record.revision).map_err(|_| LcmError::StoreFailure)?,
        ),
        superseded_by: record.superseded_by.map(LcmNodeId::new),
        operation_id: agent_runtime::lcm::LcmOperationId::new(record.operation_id),
        operation_fingerprint: LcmOperationFingerprint::new(Fingerprint::from_hex(
            record.operation_fingerprint,
        )),
    };
    node.validate().map_err(|_| LcmError::StoreFailure)?;
    Ok(node)
}

fn expansion_fingerprint(node: &LcmNode) -> Fingerprint {
    let mut values = vec![node.source_fingerprint.to_string()];
    values.extend(node.edges.iter().map(|edge| match edge {
        LcmEdge::Entry(id) => format!("entry:{id}"),
        LcmEdge::Node(id) => format!("node:{id}"),
    }));
    Fingerprint::of_fields(values)
}
