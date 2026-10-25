//! Milestone definition reads and Project-scoped hydration.

use super::*;

impl MilestoneRuntime {
    /// List only milestones belonging to the requested Project.
    ///
    /// A milestone can legitimately have `current_definition_revision_id ==
    /// NULL` mid-flow (a "define" proposal's first revision stays `draft`
    /// until promoted, and the pointer trigger only accepts a
    /// `proposed`/`approved` target).  Failing the whole list on one such row
    /// would hide every other milestone in the Project, so this falls back to
    /// each unset milestone's latest definition revision instead.
    pub async fn list(&self, project_id: &str) -> crate::Result<Vec<ProjectMilestone>> {
        let rows = ProjectOrchestrationRepo::list_project_milestones(&*self.db, project_id).await?;
        let mut milestones = Vec::with_capacity(rows.len());
        for record in rows {
            if let Some(milestone) = self.project_milestone_projection(record).await? {
                milestones.push(milestone);
            }
        }
        Ok(milestones)
    }

    /// Load one milestone after checking its Project scope at the query
    /// boundary.  A caller cannot use this method to probe another Project's
    /// milestone by identifier.
    pub async fn get(
        &self,
        project_id: &str,
        milestone_id: &str,
    ) -> crate::Result<Option<ProjectMilestone>> {
        let Some(row) =
            ProjectOrchestrationRepo::get_project_milestone(&*self.db, milestone_id).await?
        else {
            return Ok(None);
        };
        if row.project_id != project_id {
            return Ok(None);
        }
        self.project_milestone_projection(row).await
    }

    /// Project a milestone record into its public API shape, tolerating an
    /// unset `current_definition_revision_id` pointer (see `list` above) by
    /// falling back to the latest definition revision of any lifecycle.
    /// Returns `None` only when the milestone has no definition revision at
    /// all -- which atomic milestone creation should make impossible -- so
    /// that one corrupt row is skipped instead of failing every caller.
    async fn project_milestone_projection(
        &self,
        record: ProjectMilestoneRecord,
    ) -> crate::Result<Option<ProjectMilestone>> {
        if record.current_definition_revision_id.is_some() {
            return project_milestone_from_record(record).map(Some);
        }
        let revisions =
            ProjectOrchestrationRepo::list_project_milestone_revisions(&*self.db, &record.id)
                .await?;
        let Some(latest_id) = revisions
            .into_iter()
            .max_by_key(|revision| revision.revision)
            .map(|revision| revision.id)
        else {
            tracing::warn!(
                milestone_id = %record.id,
                "milestone has no definition revision; skipping from projection"
            );
            return Ok(None);
        };
        project_milestone_from_record_with_definition(record, latest_id).map(Some)
    }

    /// Return a current definition revision only when it belongs to the
    /// requested Project/milestone pair.
    pub async fn definition(
        &self,
        project_id: &str,
        milestone_id: &str,
    ) -> crate::Result<Option<MilestoneDefinitionRevision>> {
        let Some(milestone) = self.get_record(project_id, milestone_id).await? else {
            return Ok(None);
        };
        let Some(revision_id) = milestone.current_definition_revision_id else {
            return Ok(None);
        };
        let Some(revision) =
            ProjectOrchestrationRepo::get_project_milestone_revision(&*self.db, &revision_id)
                .await?
        else {
            return Ok(None);
        };
        if revision.milestone_id != milestone_id {
            return Ok(None);
        }
        let definition = definition_from_record(revision, project_id)?;
        Ok(Some(self.hydrate_definition(project_id, definition).await?))
    }

    /// Load one exact immutable definition revision within Project scope.
    pub async fn definition_revision(
        &self,
        project_id: &str,
        milestone_id: &str,
        revision_id: &str,
    ) -> crate::Result<Option<MilestoneDefinitionRevision>> {
        let Some(revision) =
            ProjectOrchestrationRepo::get_project_milestone_revision(&*self.db, revision_id)
                .await?
        else {
            return Ok(None);
        };
        if revision.milestone_id != milestone_id {
            return Ok(None);
        }
        let Some(milestone) = self.get_record(project_id, milestone_id).await? else {
            return Ok(None);
        };
        if milestone.project_id != project_id {
            return Ok(None);
        }
        let definition = definition_from_record(revision, project_id)?;
        Ok(Some(self.hydrate_definition(project_id, definition).await?))
    }

    /// List immutable definition revisions for one Project-local milestone.
    pub async fn revisions(
        &self,
        project_id: &str,
        milestone_id: &str,
    ) -> crate::Result<Vec<MilestoneDefinitionRevision>> {
        let Some(milestone) = self.get_record(project_id, milestone_id).await? else {
            return Ok(Vec::new());
        };
        if milestone.project_id != project_id {
            return Ok(Vec::new());
        }
        let definitions =
            ProjectOrchestrationRepo::list_project_milestone_revisions(&*self.db, milestone_id)
                .await?
                .into_iter()
                .map(|revision| definition_from_record(revision, project_id))
                .collect::<crate::Result<Vec<_>>>()?;
        let mut hydrated = Vec::with_capacity(definitions.len());
        for definition in definitions {
            hydrated.push(self.hydrate_definition(project_id, definition).await?);
        }
        Ok(hydrated)
    }

    pub(super) async fn get_record(
        &self,
        project_id: &str,
        milestone_id: &str,
    ) -> crate::Result<Option<ProjectMilestoneRecord>> {
        let row = ProjectOrchestrationRepo::get_project_milestone(&*self.db, milestone_id).await?;
        Ok(row.filter(|row| row.project_id == project_id))
    }

    /// Reload a milestone using the release transaction's connection.  The
    /// no-op version update in `release` is the SQLite write lock; every
    /// release-authoritative read must stay on this transaction afterwards.
    pub(super) async fn get_record_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
        milestone_id: &str,
    ) -> crate::Result<Option<ProjectMilestoneRecord>> {
        self.db
            .project_milestone_record_in_tx(tx, milestone_id, project_id)
            .await
            .map_err(crate::ServiceError::from)
    }

    pub(super) async fn definition_record_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
        milestone_id: &str,
        revision_id: &str,
    ) -> crate::Result<Option<ProjectMilestoneRevisionRecord>> {
        self.db
            .milestone_definition_record_in_tx(tx, revision_id, milestone_id, project_id)
            .await
            .map_err(crate::ServiceError::from)
    }

    pub(super) async fn hydrate_charter_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
        definition: &mut MilestoneDefinitionRevision,
    ) -> crate::Result<()> {
        let Some(charter) = definition.content.charter_revision.as_mut() else {
            return Ok(());
        };
        let row = self
            .db
            .milestone_charter_authority_in_tx(tx, project_id, &charter.revision_id)
            .await?;
        let Some(row) = row else {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        };
        let current_approved_revision_id: Option<String> = row.current_approved_revision_id.clone();
        let lifecycle: String = row.lifecycle.clone();
        let revision_id: String = row.revision_id.clone();
        if current_approved_revision_id.as_deref() != Some(revision_id.as_str())
            || lifecycle != "approved"
        {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }
        charter.artifact_id = row.charter_id.clone();
        charter.revision_id = revision_id;
        charter.content_digest = row.content_digest.clone();
        charter.render_version = Some(row.render_version.clone());
        charter.render_digest = Some(row.rendered_digest.clone());
        Ok(())
    }

    /// Public definition reads must expose the exact approved Charter
    /// artifact rather than the identifier-only placeholder used while
    /// constructing a transaction-local definition. Missing Charter rows
    /// fail closed in `hydrate_charter_in_tx`.
    async fn hydrate_definition(
        &self,
        project_id: &str,
        mut definition: MilestoneDefinitionRevision,
    ) -> crate::Result<MilestoneDefinitionRevision> {
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        self.hydrate_charter_in_tx(&mut tx, project_id, &mut definition)
            .await?;
        tx.commit().await?;
        Ok(definition)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn check_results_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
        milestone_id: &str,
        definition_revision_id: &str,
        charter_revision_id: Option<&str>,
    ) -> crate::Result<Vec<ValidationResult>> {
        let rows = self
            .db
            .milestone_check_result_rows_in_tx(tx, project_id, milestone_id)
            .await?;
        validation_results_from_rows(
            rows,
            project_id,
            definition_revision_id,
            charter_revision_id,
        )
    }

    pub(super) async fn evidence_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
        milestone_id: &str,
    ) -> crate::Result<Vec<EvidenceAttachment>> {
        let rows = self
            .db
            .milestone_evidence_rows_in_tx(tx, project_id, milestone_id)
            .await?;
        evidence_from_rows(rows, project_id)
    }
}
