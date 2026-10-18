use std::{collections::BTreeSet, sync::Arc};

use api_types::canonical_digest_with_schema;
use db::{
    now_rfc3339, ContextManifest, ContextManifestSource, CreateContextManifest,
    CreateContextManifestSource, ScopedMemoryRepository, SqliteDb,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{Result, ServiceError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextManifestInput {
    pub id: Uuid,
    pub identity_id: Uuid,
    pub agent_session_id: Option<Uuid>,
    pub context_scope_id: Uuid,
    pub scope_type: String,
    pub scope_id: String,
    pub policy_revision: String,
    pub domain_revision: String,
    pub lcm_binding_revision: Option<String>,
    pub runtime_manifest_id: Option<String>,
    pub runtime_manifest_fingerprint: Option<String>,
    pub request_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextSourceInput {
    pub ordinal: i64,
    pub source_id: String,
    pub source_type: String,
    pub source_revision: String,
    pub selection_reason: String,
    pub disposition: String,
    pub retention_priority: i64,
    pub fragment_fingerprint: String,
    pub sensitivity: String,
}

#[derive(Clone)]
pub struct ContextManifestService<R = SqliteDb> {
    db: Arc<R>,
}

impl<R> ContextManifestService<R>
where
    R: ScopedMemoryRepository + Send + Sync,
{
    pub fn new(db: Arc<R>) -> Self {
        Self { db }
    }

    pub async fn create(
        &self,
        input: ContextManifestInput,
        offered_sources: &[ContextSourceInput],
    ) -> Result<ContextManifest> {
        validate_manifest_input(&input)?;
        if offered_sources
            .iter()
            .any(|source| source.sensitivity.eq_ignore_ascii_case("secret"))
        {
            return Err(ServiceError::invalid_operation(
                "secret content cannot enter a context manifest",
            ));
        }
        validate_manifest_sources(offered_sources)?;
        let combined_fingerprint = combined_fingerprint(&input, offered_sources)?;
        self.db
            .create_context_manifest(CreateContextManifest {
                id: input.id.to_string(),
                identity_id: input.identity_id.to_string(),
                agent_session_id: input.agent_session_id.map(|id| id.to_string()),
                context_scope_id: input.context_scope_id.to_string(),
                scope_type: input.scope_type,
                scope_id: input.scope_id,
                policy_revision: input.policy_revision,
                domain_revision: input.domain_revision,
                lcm_binding_revision: input.lcm_binding_revision,
                runtime_manifest_id: input.runtime_manifest_id,
                runtime_manifest_fingerprint: input.runtime_manifest_fingerprint,
                combined_fingerprint,
                request_fingerprint: input.request_fingerprint,
                created_at: now_rfc3339(),
            })
            .await
            .map_err(Into::into)
    }

    pub async fn append_source(
        &self,
        manifest_id: Uuid,
        identity_id: Uuid,
        context_scope_id: Uuid,
        source: ContextSourceInput,
    ) -> Result<ContextManifestSource> {
        // A manifest source is immutable, but source insertion is still an
        // authority-bearing operation.  Re-authorize the manifest before
        // touching the child table so a caller cannot append to a manifest
        // merely by guessing its id.
        self.get_authorized(manifest_id, identity_id, context_scope_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("context_manifest", manifest_id.to_string()))?;
        if source.sensitivity.eq_ignore_ascii_case("secret") {
            return Err(ServiceError::invalid_operation(
                "secret content cannot enter a context manifest",
            ));
        }
        validate_manifest_source(&source)?;
        if !matches!(
            source.disposition.as_str(),
            "offered" | "included" | "summarized" | "omitted" | "deduplicated" | "rejected"
        ) {
            return Err(ServiceError::invalid_operation(
                "invalid context manifest disposition",
            ));
        }
        let manifest_id_string = manifest_id.to_string();
        let existing_sources = self
            .db
            .list_context_manifest_sources(&manifest_id_string)
            .await?;
        if let Some(existing) = existing_sources.into_iter().find(|existing| {
            existing.ordinal == source.ordinal
                || (existing.source_id == source.source_id
                    && existing.source_revision == source.source_revision)
        }) {
            let exact_replay = existing.ordinal == source.ordinal
                && existing.source_id == source.source_id
                && existing.source_type == source.source_type
                && existing.source_revision == source.source_revision
                && existing.selection_reason == source.selection_reason
                && existing.disposition == source.disposition
                && existing.retention_priority == source.retention_priority
                && existing.fragment_fingerprint == source.fragment_fingerprint;
            if exact_replay {
                return Ok(existing);
            }
            return Err(ServiceError::invalid_operation(
                "context manifest source idempotency conflict",
            ));
        }
        self.db
            .append_context_manifest_source(CreateContextManifestSource {
                manifest_id: manifest_id_string,
                ordinal: source.ordinal,
                source_id: source.source_id,
                source_type: source.source_type,
                source_revision: source.source_revision,
                selection_reason: source.selection_reason,
                disposition: source.disposition,
                retention_priority: source.retention_priority,
                fragment_fingerprint: source.fragment_fingerprint,
            })
            .await
            .map_err(Into::into)
    }

    pub async fn get_authorized(
        &self,
        id: Uuid,
        identity_id: Uuid,
        context_scope_id: Uuid,
    ) -> Result<Option<ContextManifest>> {
        self.db
            .get_context_manifest_scoped(
                &id.to_string(),
                &identity_id.to_string(),
                &context_scope_id.to_string(),
            )
            .await
            .map_err(Into::into)
    }

    pub async fn list_authorized(
        &self,
        identity_id: Uuid,
        context_scope_id: Option<Uuid>,
        limit: u32,
    ) -> Result<Vec<ContextManifest>> {
        let context_scope_id = context_scope_id.map(|id| id.to_string());
        self.db
            .list_context_manifests_scoped(
                &identity_id.to_string(),
                context_scope_id.as_deref(),
                i64::from(limit.clamp(1, 100)),
            )
            .await
            .map_err(Into::into)
    }

    pub async fn sources(
        &self,
        id: Uuid,
        identity_id: Uuid,
        context_scope_id: Uuid,
    ) -> Result<Vec<ContextManifestSource>> {
        // Do the scoped parent lookup first.  The source query only runs for
        // a manifest owned by this identity and context scope, keeping
        // source counts and ordering from becoming a cross-scope oracle.
        self.get_authorized(id, identity_id, context_scope_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("context_manifest", id.to_string()))?;
        self.db
            .list_context_manifest_sources(&id.to_string())
            .await
            .map_err(Into::into)
    }
}

pub fn fragment_fingerprint(source_id: &str, source_revision: &str, body: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(source_id.as_bytes());
    hasher.update([0]);
    hasher.update(source_revision.as_bytes());
    hasher.update([0]);
    hasher.update(body.as_bytes());
    hex::encode(hasher.finalize())
}

fn validate_manifest_input(input: &ContextManifestInput) -> Result<()> {
    guard_manifest_field("policy_revision", &input.policy_revision, 256)?;
    guard_manifest_field("domain_revision", &input.domain_revision, 256)?;
    if let Some(value) = input.lcm_binding_revision.as_deref() {
        guard_manifest_field("lcm_binding_revision", value, 512)?;
    }
    if let Some(value) = input.runtime_manifest_id.as_deref() {
        guard_manifest_field("runtime_manifest_id", value, 512)?;
    }
    if let Some(value) = input.runtime_manifest_fingerprint.as_deref() {
        guard_manifest_field("runtime_manifest_fingerprint", value, 512)?;
    }
    guard_manifest_field("request_fingerprint", &input.request_fingerprint, 512)?;
    guard_manifest_field("scope_type", &input.scope_type, 64)?;
    guard_manifest_field("scope_id", &input.scope_id, 256)?;
    if !matches!(
        input.scope_type.as_str(),
        "account" | "project" | "task" | "agent_chat"
    ) {
        return Err(ServiceError::invalid_operation(
            "context manifest scope is not a live canonical scope",
        ));
    }
    Ok(())
}

fn validate_manifest_source(source: &ContextSourceInput) -> Result<()> {
    guard_manifest_field("source_id", &source.source_id, 512)?;
    guard_manifest_field("source_type", &source.source_type, 128)?;
    guard_manifest_field("source_revision", &source.source_revision, 512)?;
    guard_manifest_field("selection_reason", &source.selection_reason, 4 * 1024)?;
    guard_manifest_field("disposition", &source.disposition, 64)?;
    guard_manifest_field("fragment_fingerprint", &source.fragment_fingerprint, 512)?;
    Ok(())
}

fn validate_manifest_sources(sources: &[ContextSourceInput]) -> Result<()> {
    let mut ordinals = BTreeSet::new();
    let mut source_revisions = BTreeSet::new();
    for source in sources {
        validate_manifest_source(source)?;
        if !ordinals.insert(source.ordinal) {
            return Err(ServiceError::invalid_operation(
                "context manifest source ordinals must be unique",
            ));
        }
        if !source_revisions.insert((&source.source_id, &source.source_revision)) {
            return Err(ServiceError::invalid_operation(
                "context manifest source identity and revision must be unique",
            ));
        }
    }
    Ok(())
}

fn guard_manifest_field(name: &str, value: &str, max_len: usize) -> Result<()> {
    if value.trim().is_empty() {
        return Err(ServiceError::invalid_operation(format!(
            "context manifest {name} must not be empty"
        )));
    }
    if value.len() > max_len {
        return Err(ServiceError::invalid_operation(format!(
            "context manifest {name} exceeds the {max_len}-byte limit"
        )));
    }
    let lower = value.to_ascii_lowercase();
    // "sk-" only counts as an OpenAI-style key marker at a word boundary —
    // ordinary words like "task-context" contain the same bytes.
    let has_key_prefix_marker = lower
        .match_indices("sk-")
        .any(|(index, _)| index == 0 || !lower.as_bytes()[index - 1].is_ascii_alphanumeric());
    if lower.contains("authorization: bearer")
        || lower.contains("api_key")
        || has_key_prefix_marker
        || lower.contains("private key")
        || lower.contains("-----begin")
    {
        return Err(ServiceError::invalid_operation(format!(
            "protected values cannot be stored in context manifest {name}"
        )));
    }
    Ok(())
}

fn combined_fingerprint(
    input: &ContextManifestInput,
    sources: &[ContextSourceInput],
) -> Result<String> {
    let mut ordered = sources.iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| {
        left.ordinal
            .cmp(&right.ordinal)
            .then_with(|| left.source_id.cmp(&right.source_id))
            .then_with(|| left.source_revision.cmp(&right.source_revision))
            .then_with(|| left.source_type.cmp(&right.source_type))
            .then_with(|| left.selection_reason.cmp(&right.selection_reason))
            .then_with(|| left.disposition.cmp(&right.disposition))
            .then_with(|| left.retention_priority.cmp(&right.retention_priority))
            .then_with(|| left.fragment_fingerprint.cmp(&right.fragment_fingerprint))
            .then_with(|| left.sensitivity.cmp(&right.sensitivity))
    });
    let value = json!({
        "identity_id": input.identity_id.to_string(),
        "context_scope_id": input.context_scope_id.to_string(),
        "scope_type": input.scope_type,
        "scope_id": input.scope_id,
        "policy_revision": input.policy_revision,
        "domain_revision": input.domain_revision,
        "lcm_binding_revision": input.lcm_binding_revision,
        "runtime_manifest_id": input.runtime_manifest_id,
        "runtime_manifest_fingerprint": input.runtime_manifest_fingerprint,
        "request_fingerprint": input.request_fingerprint,
        "sources": ordered
            .into_iter()
            .map(|source| json!({
                "ordinal": source.ordinal,
                "source_id": source.source_id,
                "source_type": source.source_type,
                "source_revision": source.source_revision,
                "selection_reason": source.selection_reason,
                "disposition": source.disposition,
                "retention_priority": source.retention_priority,
                "fragment_fingerprint": source.fragment_fingerprint,
                "sensitivity": source.sensitivity,
            }))
            .collect::<Vec<_>>(),
    });
    canonical_digest_with_schema("forge.context-manifest/v1", &value).map_err(|error| {
        ServiceError::invalid_operation(format!(
            "context manifest canonical fingerprint failed: {error}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input() -> ContextManifestInput {
        ContextManifestInput {
            id: Uuid::from_u128(1),
            identity_id: Uuid::from_u128(2),
            agent_session_id: Some(Uuid::from_u128(3)),
            context_scope_id: Uuid::from_u128(4),
            scope_type: "project".to_owned(),
            scope_id: "project\nwith\tdelimiters".to_owned(),
            policy_revision: "policy".to_owned(),
            domain_revision: "domain".to_owned(),
            lcm_binding_revision: Some("lcm\nrevision".to_owned()),
            runtime_manifest_id: Some("runtime".to_owned()),
            runtime_manifest_fingerprint: Some("runtime-digest".to_owned()),
            request_fingerprint: "request".to_owned(),
        }
    }

    fn source() -> ContextSourceInput {
        ContextSourceInput {
            ordinal: 0,
            source_id: "source\nwith\tdelimiters".to_owned(),
            source_type: "project_document".to_owned(),
            source_revision: "revision-1".to_owned(),
            selection_reason: "approved\nsource".to_owned(),
            disposition: "included".to_owned(),
            retention_priority: 50,
            fragment_fingerprint: "digest".to_owned(),
            sensitivity: "internal".to_owned(),
        }
    }

    #[test]
    fn combined_fingerprint_is_schema_versioned_and_covers_every_source_field() {
        let input = input();
        let source = source();
        let first =
            combined_fingerprint(&input, std::slice::from_ref(&source)).expect("canonical digest");

        let mut changed = source.clone();
        changed.retention_priority += 1;
        let second =
            combined_fingerprint(&input, std::slice::from_ref(&changed)).expect("canonical digest");
        assert_ne!(first, second, "source metadata must be digest-bound");

        let mut reordered = source.clone();
        reordered.ordinal = 1;
        let third = combined_fingerprint(&input, &[reordered]).expect("canonical digest");
        assert_ne!(first, third, "source ordinal must be digest-bound");

        let mut second_source = source.clone();
        second_source.source_id = "source-2".to_owned();
        second_source.source_revision = "revision-2".to_owned();
        second_source.ordinal = 1;
        let forward = combined_fingerprint(&input, &[source.clone(), second_source.clone()])
            .expect("canonical digest");
        let reverse =
            combined_fingerprint(&input, &[second_source, source]).expect("canonical digest");
        assert_eq!(forward, reverse, "source order must not affect the digest");
    }

    #[test]
    fn manifest_references_fail_closed_when_revision_or_provenance_is_missing() {
        let mut invalid_input = input();
        invalid_input.request_fingerprint.clear();
        assert!(validate_manifest_input(&invalid_input).is_err());

        let mut invalid_source = source();
        invalid_source.source_revision.clear();
        assert!(validate_manifest_source(&invalid_source).is_err());
    }

    #[test]
    fn manifest_source_set_rejects_duplicate_ordinals_and_source_revisions() {
        let first = source();
        let mut duplicate_ordinal = source();
        duplicate_ordinal.source_id = "different-source".to_owned();
        assert!(validate_manifest_sources(&[first.clone(), duplicate_ordinal]).is_err());

        let mut duplicate_source_revision = source();
        duplicate_source_revision.ordinal = 1;
        duplicate_source_revision.source_type = "different_projection".to_owned();
        assert!(validate_manifest_sources(&[first, duplicate_source_revision]).is_err());
    }
}
