//! Task media and shared media repositories.

use super::*;

#[async_trait]
pub trait TaskMediaRepo: Send + Sync {
    async fn create_media(&self, input: CreateTaskMedia) -> Result<TaskMedia>;
    async fn list_media(&self, task_id: &str, page: PageRequest) -> Result<Page<TaskMedia>>;
    async fn list_active_media_for_task(&self, task_id: &str) -> Result<Vec<TaskMedia>>;
    async fn get_media_by_id(&self, id: &str, include_deleted: bool) -> Result<Option<TaskMedia>>;
    async fn soft_delete_media(&self, id: &str, deleted_at: &str) -> Result<TaskMedia>;
}

/// Repository boundary for the additive Project-owned media metadata layer.
///
/// Implementations must keep reference changes transactional.  In
/// particular, a cleanup worker may only claim an asset after checking active
/// attachments and release pins in the same write transaction.  Claims carry
/// a persisted owner/expiry lease and asset version; reset/finalize operations
/// must present both exact values.  Every attachment/pin insertion must reject
/// an asset already queued for deletion or marked as deleted.
#[async_trait]
pub trait SharedMediaRepo: Send + Sync {
    async fn get_media_asset(&self, asset_id: &str) -> Result<Option<MediaAsset>>;
    async fn get_media_asset_for_task_media(
        &self,
        task_media_id: &str,
    ) -> Result<Option<MediaAsset>>;
    async fn begin_project_media_upload(
        &self,
        input: BeginProjectMediaUpload,
    ) -> Result<ProjectMediaUpload>;
    async fn list_pending_project_media_uploads(
        &self,
        limit: i64,
    ) -> Result<Vec<ProjectMediaUpload>>;
    async fn delete_pending_project_media_upload(
        &self,
        project_id: &str,
        idempotency_key: &str,
    ) -> Result<bool>;
    async fn create_project_media_asset(
        &self,
        input: CreateProjectMediaAsset,
    ) -> Result<MediaAsset>;
    async fn finalize_project_media_upload(
        &self,
        project_id: &str,
        asset_id: &str,
        now: &str,
    ) -> Result<MediaAsset>;
    /// Persist a checksum discovered from the unchanged on-disk bytes.  The
    /// compare-and-set is intentionally narrow so concurrent reconciliation
    /// cannot overwrite an existing digest.
    async fn set_media_asset_checksum(
        &self,
        asset_id: &str,
        expected_byte_size: i64,
        checksum: &str,
        now: &str,
    ) -> Result<MediaAsset>;
    /// Return purged assets whose bytes still need idempotent physical
    /// cleanup after a crash between the tombstone transaction and unlink.
    async fn list_purged_media_assets(&self, limit: i64) -> Result<Vec<MediaAsset>>;
    async fn mark_purged_media_asset_reconciled(
        &self,
        asset_id: &str,
        reconciled_at: &str,
    ) -> Result<()>;
    /// Resolve an already committed Project media tombstone without applying
    /// a new mutation.  API routes use this before validating the current
    /// authorization so an immutable receipt can be replayed exactly; a
    /// changed receipt returns `IdempotencyConflict`.
    async fn replay_project_media_tombstone(
        &self,
        input: ProjectMediaTombstone,
    ) -> Result<Option<MediaAsset>>;
    async fn tombstone_project_media_asset(
        &self,
        input: ProjectMediaTombstone,
    ) -> Result<MediaAsset>;
    async fn create_project_media_attachment_mutation(
        &self,
        input: CreateProjectMediaAttachmentMutation,
    ) -> Result<ProjectMediaAttachment>;
    async fn soft_delete_project_media_attachment_mutation(
        &self,
        input: SoftDeleteProjectMediaAttachmentMutation,
    ) -> Result<ProjectMediaAttachment>;
    async fn create_project_media_attachment(
        &self,
        input: CreateProjectMediaAttachment,
    ) -> Result<ProjectMediaAttachment>;
    async fn soft_delete_project_media_attachment(
        &self,
        id: &str,
        deleted_at: &str,
    ) -> Result<ProjectMediaAttachment>;
    async fn create_project_release_media_pin(
        &self,
        input: CreateProjectReleaseMediaPin,
    ) -> Result<ProjectReleaseMediaPin>;
    async fn list_project_release_media_pins(
        &self,
        release_id: &str,
    ) -> Result<Vec<ProjectReleaseMediaPin>>;
    async fn reconcile_media_asset(&self, asset_id: &str, now: &str) -> Result<Option<MediaAsset>>;
    async fn claim_media_gc_candidates(
        &self,
        now: &str,
        lease_owner: &str,
        lease_expires_at: &str,
        limit: i64,
    ) -> Result<Vec<MediaAsset>>;
    async fn claim_media_gc_candidate(
        &self,
        asset_id: &str,
        now: &str,
        lease_owner: &str,
        lease_expires_at: &str,
    ) -> Result<Option<MediaAsset>>;
    async fn reset_media_gc_candidate(
        &self,
        asset_id: &str,
        lease_owner: &str,
        expected_version: i64,
        now: &str,
    ) -> Result<Option<MediaAsset>>;
    async fn complete_media_gc(
        &self,
        asset_id: &str,
        lease_owner: &str,
        expected_version: i64,
        deleted_at: &str,
    ) -> Result<Option<MediaAsset>>;
}
