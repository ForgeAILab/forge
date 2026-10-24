//! Native Project state, Charter, skill, observation, and queue reads.

use super::*;

impl CoordinationToolProvider {
    /// `repo.integration_queue`: the merge queues of the bound Project's
    /// repository. Read-only; the Project is the admitted scope's.
    pub(super) async fn repo_integration_queue_read(
        &self,
        project_id: &str,
        input: operation_registry::project_reads::IntegrationQueueArguments,
    ) -> Result<Value, AgentHostError> {
        crate::integration_reads::project_integration_queues(
            &self.db,
            project_id,
            input.target_branch.as_deref(),
            input.limit,
        )
        .await
        .map_err(|_| AgentHostError::ProtectedPersistence)
    }

    pub(super) async fn project_observations_read(
        &self,
        _actor_identity_id: &str,
        _scope: &CanonicalScope,
        project_id: &str,
        input: operation_registry::project_reads::ObservationsArguments,
    ) -> Result<Value, AgentHostError> {
        let limit = input.limit.unwrap_or(20).clamp(1, 50) as i64;
        let task_filter = input.task_id;

        let worklog = self
            .db
            .native_project_worklog(
                project_id,
                task_filter.as_deref(),
                task_filter.as_deref(),
                limit,
            )
            .await
            .map_err(|_| AgentHostError::ProtectedPersistence)?;

        let artifacts = self
            .db
            .native_project_artifacts(
                project_id,
                task_filter.as_deref(),
                task_filter.as_deref(),
                limit,
            )
            .await
            .map_err(|_| AgentHostError::ProtectedPersistence)?;

        let media_root = self.media_root_handle();
        let artifacts = artifacts
            .into_iter()
            .map(|row| {
                let content_type: String = row.content_type().unwrap_or_default();
                let byte_size: i64 = row.byte_size().unwrap_or_default();
                let storage_key: String = row.storage_key().unwrap_or_default();
                // Text is what an Agent can actually evaluate; binary artifacts
                // are named so the user can open them, never inlined.
                let content = (content_type.starts_with("text/")
                    && byte_size <= MAX_INLINE_ARTIFACT_BYTES)
                    .then_some(media_root.as_ref())
                    .flatten()
                    .and_then(|root| safe_media_destination(root, &storage_key).ok())
                    .and_then(|path| std::fs::read_to_string(path).ok())
                    .map(|text| truncate(&text, MAX_INLINE_ARTIFACT_CHARS));
                json!({
                    "asset_id": row.asset_id().unwrap_or_default(),
                    "task_id": row.task_id().unwrap_or_default(),
                    "filename": row.display_filename().unwrap_or_default(),
                    "content_type": content_type,
                    "byte_size": byte_size,
                    "checksum": row.checksum().unwrap_or_default(),
                    "captured_at": row.created_at().unwrap_or_default(),
                    "content": content,
                })
            })
            .collect::<Vec<_>>();

        Ok(json!({
            "scope": {"type": "project", "id": project_id},
            "worklog": worklog
                .into_iter()
                .map(|row| json!({
                    "id": row.id().unwrap_or_default(),
                    "task_id": row.task_id().unwrap_or_default(),
                    "task_title": truncate(&row.title().unwrap_or_default(), 160),
                    "kind": row.worklog_kind().unwrap_or_default(),
                    "role": row.role_optional().unwrap_or_default(),
                    "execution_id": row.execution_id().unwrap_or_default(),
                    "summary": truncate(&row.content().unwrap_or_default(), 2_000),
                    "created_at": row.created_at().unwrap_or_default(),
                }))
                .collect::<Vec<_>>(),
            "artifacts": artifacts,
        }))
    }

    pub(super) async fn project_charter_read(
        &self,
        _actor_identity_id: &str,
        _scope: &CanonicalScope,
        project_id: &str,
    ) -> Result<Value, AgentHostError> {
        let row = self
            .db
            .native_approved_project_charter(project_id)
            .await
            .map_err(|_| AgentHostError::ProtectedPersistence)?
            .ok_or_else(|| {
                AgentHostError::Authority("the bound Project has no approved Charter".to_owned())
            })?;
        let revision_id = row.revision_id().unwrap_or_default();
        // The rendered Charter is prose; `review_requirement_ids` on a Task
        // proposal are exact `<revision_id>:<JSON Pointer>` strings. Without
        // the catalog the Agent has to guess array indices, and every guess is
        // rejected as an unknown requirement ID — which blocks Task creation
        // outright now that the field is required. Universal requirements are
        // omitted: they already apply to every Task and naming one is an error.
        let selectable_requirements = row
            .content_json()
            .ok()
            .and_then(|content| {
                serde_json::from_str::<api_types::ProjectCharterContent>(&content).ok()
            })
            .and_then(|charter| {
                ::review::contract::charter_requirements(&revision_id, &charter).ok()
            })
            .map(|requirements| {
                requirements
                    .into_iter()
                    .filter(|requirement| !requirement.universal)
                    .map(|requirement| json!({"id": requirement.id, "text": requirement.text}))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        Ok(json!({
            "scope": {"type": "project", "id": project_id},
            "charter_id": row.charter_id().unwrap_or_default(),
            "revision_id": revision_id,
            "revision": row.revision().unwrap_or_default(),
            "charter_version": row.charter_version().unwrap_or_default(),
            "project_mode": row.project_mode().unwrap_or_default(),
            "content_digest": row.content_digest().unwrap_or_default(),
            "render_digest": row.rendered_digest().unwrap_or_default(),
            "rendered_markdown": row.rendered_view().unwrap_or_default(),
            "selectable_review_requirements": selectable_requirements,
        }))
    }

    pub(super) async fn project_skill_section_read(
        &self,
        _actor_identity_id: &str,
        _scope: &CanonicalScope,
        _project_id: &str,
        input: operation_registry::project_reads::SectionArguments,
    ) -> Result<Value, AgentHostError> {
        // Doctrine text is static server-owned content, but the read still
        // authenticates the Project binding so the operation cannot become an
        // unauthorized liveness probe for foreign scopes.
        let section = input.section.as_str();
        let body = crate::operating_skills::project_skill_section(section).ok_or_else(|| {
            AgentHostError::Unsupported(format!(
                "unknown doctrine section `{section}`; sections: {}",
                crate::operating_skills::PROJECT_SKILL_SECTIONS
                    .iter()
                    .map(|(name, _)| *name)
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        })?;
        Ok(json!({
            "skill_key": crate::operating_skills::PROJECT_OPERATING_SKILL_KEY,
            "section": section,
            "content": body,
        }))
    }

    pub(super) async fn project_current_state_read(
        &self,
        _actor_identity_id: &str,
        _scope: &CanonicalScope,
        project_id: &str,
        input: operation_registry::project_reads::CurrentStateArguments,
    ) -> Result<Value, AgentHostError> {
        let limit = input.limit.map(|value| value as i64);
        let projection = load_effective_project_state(&self.db, project_id, limit)
            .await
            .map_err(|_| AgentHostError::ProtectedPersistence)?;
        let execution_setup = crate::load_project_execution_setup(&self.db, project_id)
            .await
            .map_err(|_| AgentHostError::ProtectedPersistence)?;
        let adoption_charter = load_project_adoption_charter(&self.db, project_id)
            .await
            .map_err(|_| AgentHostError::ProtectedPersistence)?;
        serde_json::to_value(ProjectCurrentStateResponse {
            scope: "project".to_owned(),
            effective_state: projection,
            execution_setup: Some(execution_setup),
            adoption_charter,
        })
        .map_err(|_| AgentHostError::ProtectedPersistence)
    }

    pub(super) async fn project_summary_read(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        arguments: Value,
    ) -> Result<Value, AgentHostError> {
        let account_id = self
            .authorization
            .main_account_id(actor_identity_id, scope)
            .await
            .map_err(native_scope_error)?;
        let project_id = arguments
            .get("project_id")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| invalid_arguments("project_id is required".to_owned()))?;
        let row = self
            .db
            .native_owned_project_summary(project_id, &account_id)
            .await
            .map_err(|_| AgentHostError::ProtectedPersistence)?
            .ok_or_else(|| invalid_arguments("Project summary is unavailable".to_owned()))?;
        Ok(json!({
            "id": row.id().unwrap_or_default(),
            "name": row.name().unwrap_or_default(),
            "paused": row.paused_at().ok().flatten().is_some(),
            "task_count": row.task_count().unwrap_or_default(),
            "created_at": row.created_at().unwrap_or_default(),
            "updated_at": row.updated_at().unwrap_or_default(),
        }))
    }
}
