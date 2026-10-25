//! Shared media assets, attachments, uploads, and release pins.

use super::*;

/// Project-owned metadata for one existing task media blob.
///
/// The `id`, `storage_key`, and legacy task media reference are deliberately
/// kept separate from the Project evidence attachment.  This lets Project
/// evidence and immutable release pins reuse the exact bytes without changing
/// the historical Task media API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaAsset {
    pub id: String,
    pub project_id: String,
    pub legacy_task_media_id: Option<String>,
    pub display_filename: String,
    pub content_type: String,
    pub byte_size: i64,
    pub storage_key: String,
    pub checksum: Option<String>,
    pub availability: String,
    pub gc_state: String,
    pub gc_candidate_at: Option<String>,
    pub gc_lease_owner: Option<String>,
    pub gc_lease_expires_at: Option<String>,
    pub version: i64,
    pub deleted_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectMediaAttachment {
    pub id: String,
    pub project_id: String,
    pub asset_id: String,
    pub attachment_kind: String,
    pub task_media_id: Option<String>,
    pub task_id: Option<String>,
    pub milestone_id: Option<String>,
    pub milestone_check_id: Option<String>,
    pub source_task_id: Option<String>,
    pub source_execution_id: Option<String>,
    pub source_validation_id: Option<String>,
    pub source_task_version: Option<i64>,
    pub source_context_digest: Option<String>,
    pub source_definition_revision_id: Option<String>,
    pub acceptance_check_ids_json: String,
    pub caption: Option<String>,
    pub evidence_kind: Option<String>,
    pub checksum: Option<String>,
    pub availability: String,
    pub project_url: Option<String>,
    pub author_type: String,
    pub author_id: Option<String>,
    pub authorization_json: String,
    pub created_at: String,
}

/// Project media upload metadata.  The bytes are written by the API's
/// storage adapter; this record makes the metadata insert and its durable
/// idempotency event one transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectMediaAsset {
    pub id: String,
    pub project_id: String,
    pub display_filename: String,
    pub content_type: String,
    pub byte_size: i64,
    pub storage_key: String,
    pub checksum: String,
    pub idempotency_key: String,
    pub mutation_fingerprint: String,
    pub expected_project_version: i64,
    pub actor_type: String,
    pub actor_id: Option<String>,
    pub authorization_event_id: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BeginProjectMediaUpload {
    pub project_id: String,
    pub idempotency_key: String,
    pub mutation_fingerprint: String,
    pub expected_project_version: i64,
    pub asset_id: String,
    pub final_storage_key: String,
    pub staging_storage_key: String,
    pub display_filename: String,
    pub content_type: String,
    pub byte_size: i64,
    pub checksum: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectMediaUpload {
    pub project_id: String,
    pub idempotency_key: String,
    pub mutation_fingerprint: String,
    pub expected_project_version: i64,
    pub asset_id: String,
    pub final_storage_key: String,
    pub staging_storage_key: Option<String>,
    pub display_filename: String,
    pub content_type: String,
    pub byte_size: i64,
    pub checksum: String,
    pub status: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectMediaTombstone {
    pub asset_id: String,
    pub project_id: String,
    pub expected_version: i64,
    pub idempotency_key: String,
    pub mutation_fingerprint: String,
    pub target_availability: String,
    pub principal_type: String,
    pub principal_id: String,
    pub authorization_basis: String,
    pub authorization_action: String,
    pub authorization_occurred_at: String,
    pub authorization_event_id: String,
    pub authorization_json: String,
    pub reason: String,
    pub created_at: String,
}

/// Mutation context for an evidence attachment.  The legacy attachment
/// method remains available to Task/media migration code; Project evidence
/// uses this composite form so the milestone CAS and source event commit
/// together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectMediaAttachmentMutation {
    pub attachment: CreateProjectMediaAttachment,
    pub expected_milestone_version: i64,
    pub idempotency_key: String,
    pub mutation_fingerprint: String,
    pub authorization_event_id: String,
    /// Optional shared-command provenance.  When present, the attachment,
    /// event, frozen receipt, and (for approved AgentActions) execution are
    /// committed by one transaction.  Legacy callers leave these fields
    /// unset while they are migrated to the command boundary.
    pub command_receipt: Option<CreateCommandReceipt>,
    pub action_execution: Option<CreateAgentActionExecution>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SoftDeleteProjectMediaAttachmentMutation {
    pub id: String,
    pub project_id: String,
    pub milestone_id: String,
    pub expected_version: i64,
    pub idempotency_key: String,
    pub mutation_fingerprint: String,
    pub actor_type: String,
    pub actor_id: Option<String>,
    pub authorization_json: String,
    pub authorization_event_id: String,
    pub deleted_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectMediaAttachment {
    pub id: String,
    pub project_id: String,
    pub asset_id: String,
    pub attachment_kind: String,
    pub task_media_id: Option<String>,
    pub task_id: Option<String>,
    pub milestone_id: Option<String>,
    pub milestone_check_id: Option<String>,
    pub source_task_id: Option<String>,
    pub source_execution_id: Option<String>,
    pub source_validation_id: Option<String>,
    pub source_task_version: Option<i64>,
    pub source_context_digest: Option<String>,
    pub source_definition_revision_id: Option<String>,
    pub acceptance_check_ids_json: String,
    pub caption: Option<String>,
    pub evidence_kind: Option<String>,
    pub checksum: Option<String>,
    pub availability: String,
    pub project_url: Option<String>,
    pub author_type: String,
    pub author_id: Option<String>,
    pub authorization_json: String,
    pub version: i64,
    pub created_at: String,
    pub deleted_at: Option<String>,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectReleaseMediaPin {
    pub id: String,
    pub project_id: String,
    pub release_id: String,
    pub asset_id: String,
    pub attachment_id: Option<String>,
    pub legacy_task_media_id: Option<String>,
    pub asset_checksum: String,
    pub attachment_digest: String,
    pub availability: String,
    pub pin_digest: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectReleaseMediaPin {
    pub id: String,
    pub project_id: String,
    pub release_id: String,
    pub asset_id: String,
    pub attachment_id: Option<String>,
    pub legacy_task_media_id: Option<String>,
    pub asset_checksum: String,
    pub attachment_digest: String,
    pub availability: String,
    pub pin_digest: String,
    pub created_at: String,
}
