//! Memory access and context manifest repositories.

use super::*;

#[async_trait]
pub trait MemoryRepository: Send + Sync {
    async fn insert_memory_item(&self, item: &MemoryItem) -> std::result::Result<(), DbError>;
    async fn get_memory_item(&self, id: &str) -> std::result::Result<Option<MemoryItem>, DbError>;
    async fn memory_source_exists(
        &self,
        project_id: &str,
        source_type: &str,
        source_ref: &str,
    ) -> std::result::Result<bool, DbError>;
    async fn memory_source_exists_with_confidence(
        &self,
        project_id: &str,
        source_type: &str,
        source_ref: &str,
        confidence: &str,
    ) -> std::result::Result<bool, DbError>;
    async fn search_memory_items(
        &self,
        project_id: &str,
        query: &str,
        limit: usize,
        cursor: Option<String>,
    ) -> std::result::Result<(Vec<MemoryItem>, bool), DbError>;
    async fn list_memory_items_by_source(
        &self,
        project_id: &str,
        source_type: &str,
        source_id: &str,
    ) -> std::result::Result<Vec<MemoryItem>, DbError>;
}

/// ACL-first semantic-memory operations.  These methods are intentionally a
/// separate trait so legacy project-indexing code cannot accidentally omit the
/// authorization context when it is migrated to scoped retrieval.
#[async_trait]
pub trait ScopedMemoryRepository: Send + Sync {
    async fn get_memory_item_scoped(
        &self,
        query: MemoryGetQuery,
    ) -> std::result::Result<Option<MemoryItem>, DbError>;
    async fn search_memory_items_scoped(
        &self,
        query: MemoryAccessQuery,
    ) -> std::result::Result<(Vec<MemoryItem>, bool), DbError>;
    async fn insert_memory_lifecycle_assertion(
        &self,
        input: CreateMemoryLifecycleAssertion,
    ) -> std::result::Result<MemoryLifecycleAssertion, DbError>;
    async fn list_memory_lifecycle_assertions(
        &self,
        memory_item_id: &str,
    ) -> std::result::Result<Vec<MemoryLifecycleAssertion>, DbError>;
    async fn create_memory_source_binding(
        &self,
        input: CreateForgeMemorySourceBinding,
    ) -> std::result::Result<ForgeMemorySourceBinding, DbError>;
    async fn get_memory_source_binding(
        &self,
        identity_id: &str,
        context_scope_id: &str,
    ) -> std::result::Result<Option<ForgeMemorySourceBinding>, DbError>;
    async fn create_context_manifest(
        &self,
        input: CreateContextManifest,
    ) -> std::result::Result<ContextManifest, DbError>;
    async fn append_context_manifest_source(
        &self,
        input: CreateContextManifestSource,
    ) -> std::result::Result<ContextManifestSource, DbError>;
    async fn get_context_manifest(
        &self,
        id: &str,
    ) -> std::result::Result<Option<ContextManifest>, DbError>;
    async fn get_context_manifest_scoped(
        &self,
        id: &str,
        identity_id: &str,
        context_scope_id: &str,
    ) -> std::result::Result<Option<ContextManifest>, DbError>;
    async fn list_context_manifests_scoped(
        &self,
        identity_id: &str,
        context_scope_id: Option<&str>,
        limit: i64,
    ) -> std::result::Result<Vec<ContextManifest>, DbError>;
    async fn list_context_manifest_sources(
        &self,
        manifest_id: &str,
    ) -> std::result::Result<Vec<ContextManifestSource>, DbError>;
}
