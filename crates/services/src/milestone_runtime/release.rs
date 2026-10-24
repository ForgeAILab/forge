//! Immutable milestone release creation and readback.

use super::*;

impl MilestoneRuntime {
    /// Re-authorize the exact candidate, recompute its digest, and atomically
    /// write one immutable release revision plus evidence pins.  This method
    /// intentionally does not create a new readiness snapshot during release.
    #[allow(clippy::too_many_arguments)]
    pub async fn release(
        &self,
        project_id: &str,
        actor: &PrincipalRef,
        authorization: &AuthorizationProvenance,
        milestone_id: &str,
        expected_milestone_version: i64,
        readiness_snapshot_id: &str,
        readiness_digest: &str,
        idempotency_key: &str,
    ) -> crate::Result<ProjectRelease> {
        if actor.kind != PrincipalKind::User {
            return Err(crate::ServiceError::InvalidOperation {
                message: "only an authenticated user may release a milestone".to_owned(),
            });
        }
        if idempotency_key.trim().is_empty() {
            return Err(crate::ServiceError::InvalidOperation {
                message: "release idempotency key is required".to_owned(),
            });
        }
        // The immutable snapshot digests the releasing principal twice — as
        // `released_by` and inside the authorization receipt — and the row
        // persists one display name for both. Bind the actor to the label the
        // receipt carries so the write and every later read agree byte for
        // byte; the route already proved both name the same authenticated user.
        let actor = &PrincipalRef {
            kind: actor.kind,
            id: actor.id.clone(),
            display_name: authorization.principal.display_name.clone(),
        };
        if let Some(existing) = self
            .release_by_idempotency(project_id, idempotency_key)
            .await?
        {
            if existing.milestone_id != milestone_id
                || existing.readiness_snapshot_id != readiness_snapshot_id
                || existing.readiness_digest != readiness_digest
                || existing.releasing_principal_id != actor.id
                || existing.releasing_principal_type != principal_kind_name(actor.kind)
                || existing.releasing_principal_display_name != actor.display_name
                || existing.releasing_principal_id != authorization.principal.id
                || existing.releasing_principal_type
                    != principal_kind_name(authorization.principal.kind)
                || existing.authorization_basis != authorization.authorization_basis
                || existing.authorization_action != authorization.action
                || existing.explicit_event != authorization.event_id
                || existing.authorization_occurred_at != authorization.occurred_at
            {
                return Err(crate::ServiceError::Db(db::DbError::IdempotencyConflict));
            }
            let replay = self.release_from_record(existing).await?;
            if replay.snapshot.expected_milestone_version.checked_add(1)
                != Some(expected_milestone_version)
            {
                return Err(crate::ServiceError::Db(db::DbError::IdempotencyConflict));
            }
            return Ok(replay);
        }
        validate_authorization(authorization, actor, "project.milestone.release")?;
        // A second Confirm may carry a fresh UI key after the first request
        // committed but before the Overview refreshed. The immutable
        // readiness target is already released; return that exact record
        // instead of re-verifying it against the now-released lifecycle.
        if let Some(existing) = self
            .release_by_readiness_target(
                project_id,
                milestone_id,
                readiness_snapshot_id,
                readiness_digest,
            )
            .await?
        {
            if existing.releasing_principal_type != principal_kind_name(actor.kind)
                || existing.releasing_principal_id != actor.id
            {
                return Err(crate::ServiceError::Conflict(
                    "this readiness snapshot is already released by another principal".to_owned(),
                ));
            }
            return self.release_from_record(existing).await;
        }

        // Acquire the milestone's write lock before loading any release-gating
        // inputs. SQLite has no FOR UPDATE; a guarded no-op update is the
        // compare-and-swap lock for this transaction. Every source reload and
        // the manifest/pins/lifecycle/event commit below happens while this
        // lock is held.
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        let locked = self
            .db
            .lock_milestone_version_in_tx(
                &mut tx,
                milestone_id,
                project_id,
                expected_milestone_version,
            )
            .await?;
        if locked != 1 {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }

        let candidate = self
            .readiness_by_id_in_tx(&mut tx, project_id, readiness_snapshot_id)
            .await?
            .ok_or_else(|| {
                crate::ServiceError::not_found("readiness_snapshot", readiness_snapshot_id)
            })?;
        if candidate.milestone_id != milestone_id || candidate.project_id != project_id {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }
        let recheck = self
            .recheck_readiness_in_tx(&mut tx, project_id, milestone_id, candidate)
            .await?;
        let ReadinessRecheck {
            milestone,
            definition,
            candidate_snapshot,
            recomputed,
            evidence,
            task_states,
        } = recheck;
        if !readiness_inputs_match(&candidate_snapshot, &recomputed) {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }
        let included_decisions = self
            .effective_decision_references_in_tx(
                &mut tx,
                project_id,
                milestone_id,
                definition
                    .content
                    .charter_revision
                    .as_ref()
                    .map(|charter| charter.revision_id.as_str()),
            )
            .await?;
        let pin_metadata = self
            .release_pin_metadata_in_tx(&mut tx, project_id, milestone_id, &evidence)
            .await?;
        let release_revision = self.release_revision_in_tx(&mut tx, milestone_id).await?;
        let project_agent = self
            .project_agent_principal_in_tx(&mut tx, project_id)
            .await?;
        let verified = verify_release_candidate(
            &project_milestone_from_record(milestone.clone())?,
            &candidate_snapshot,
            &recomputed,
            readiness_snapshot_id,
            readiness_digest,
            release_revision,
            actor,
            &project_agent,
        )
        .map_err(map_orchestration_error)?;
        if milestone.version != expected_milestone_version {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }
        if milestone.version != candidate_snapshot.expected_milestone_version + 1 {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }

        let released_at = now_rfc3339();
        let release_id = new_uuid_v4();
        let snapshot = release_snapshot(
            project_id,
            &milestone,
            &definition,
            &candidate_snapshot,
            &verified,
            &release_id,
            &evidence,
            &pin_metadata,
            &task_states,
            &included_decisions,
            actor,
            authorization,
            idempotency_key,
            &released_at,
        )?;
        if let Some(existing) = self
            .db
            .release_by_idempotency_in_tx(&mut tx, project_id, idempotency_key)
            .await?
        {
            if existing.milestone_id != milestone_id
                || existing.readiness_snapshot_id != readiness_snapshot_id
                || existing.readiness_digest != readiness_digest
                || existing.releasing_principal_id != actor.id
                || existing.releasing_principal_type != principal_kind_name(actor.kind)
                || existing.releasing_principal_display_name != actor.display_name
                || existing.authorization_basis != authorization.authorization_basis
                || existing.authorization_action != authorization.action
                || existing.explicit_event != authorization.event_id
                || existing.authorization_occurred_at != authorization.occurred_at
            {
                return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
            }
            tx.commit().await?;
            return self.release_from_record(existing).await;
        }
        let current_version: i64 = self
            .db
            .milestone_version_in_tx(&mut tx, milestone_id, project_id)
            .await?
            .ok_or(db::DbError::NotFound)?;
        if current_version != expected_milestone_version {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }
        let current_candidate = self
            .db
            .milestone_readiness_by_id_in_tx(
                &mut tx,
                readiness_snapshot_id,
                project_id,
                milestone_id,
            )
            .await?
            .ok_or(db::DbError::NotFound)?;
        if current_candidate.readiness_digest != readiness_digest
            || current_candidate.readiness_digest != verified.readiness_digest
            || current_candidate.definition_revision_id
                != candidate_snapshot.milestone_definition_revision_id
        {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }
        let inserted = self
            .db
            .insert_milestone_release_in_tx(
                &mut tx,
                &db::InsertMilestoneReleaseInput {
                    id: &release_id,
                    project_id,
                    milestone_id,
                    release_revision,
                    release_identifier: &snapshot.release_identity,
                    milestone_revision_id: &snapshot.milestone_definition_revision_id,
                    readiness_snapshot_id,
                    readiness_digest: &snapshot.readiness_digest,
                    summary: &snapshot.summary,
                    changelog: &(serde_json::to_string(&snapshot.changelog).map_err(json_error)?),
                    known_issues_json: &(serde_json::to_string(&snapshot.known_issues)
                        .map_err(json_error)?),
                    charter_revision_id: (!snapshot.charter_revision.revision_id.is_empty())
                        .then_some(snapshot.charter_revision.revision_id.as_str()),
                    document_revisions_json: &(serde_json::to_string(&snapshot.document_revisions)
                        .map_err(json_error)?),
                    decision_ids_json: &(serde_json::to_string(&snapshot.included_decisions)
                        .map_err(json_error)?),
                    task_references_json: &(serde_json::to_string(&snapshot.included_tasks)
                        .map_err(json_error)?),
                    validation_references_json: &(serde_json::to_string(
                        &snapshot.validation_results,
                    )
                    .map_err(json_error)?),
                    git_references_json: &(serde_json::to_string(&snapshot.repository_references)
                        .map_err(json_error)?),
                    evidence_references_json: &(serde_json::to_string(&snapshot.evidence_pins)
                        .map_err(json_error)?),
                    waivers_json: &(serde_json::to_string(&snapshot.waived_check_ids)
                        .map_err(json_error)?),
                    principal_type: "user",
                    principal_id: &actor.id,
                    principal_display_name: actor.display_name.as_deref(),
                    authorization_basis: &authorization.authorization_basis,
                    authorization_action: &authorization.action,
                    explicit_event: &authorization.event_id,
                    authorization_occurred_at: &authorization.occurred_at,
                    schema_version: RELEASE_SCHEMA_VERSION,
                    snapshot_digest: &snapshot.snapshot_digest,
                    idempotency_key,
                    created_at: &released_at,
                },
            )
            .await
            .map_err(map_db_conflict)?;
        if inserted != 1 {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }
        let mut reference_ordinal: i64 = 0;
        for task in &snapshot.included_tasks {
            self.db
                .insert_release_task_reference_in_tx(
                    &mut tx,
                    &db::InsertReleaseTaskReferenceInput {
                        release_id: &release_id,
                        ordinal: reference_ordinal,
                        record_id: &task.task_id,
                        record_version: &task.task_version.to_string(),
                        record_state: &task.task_state,
                        metadata_json: &(serde_json::to_string(task).map_err(json_error)?),
                    },
                )
                .await
                .map_err(map_db_conflict)?;
            reference_ordinal += 1;
        }
        for validation in &snapshot.validation_results {
            self.db
                .insert_release_validation_reference_in_tx(
                    &mut tx,
                    &db::InsertReleaseValidationReferenceInput {
                        release_id: &release_id,
                        ordinal: reference_ordinal,
                        record_id: &validation.validation_id,
                        record_version: &validation.evaluated_at.clone(),
                        record_state: &(format!("{:?}", validation.status).to_ascii_lowercase()),
                        record_digest: &validation.result_digest,
                        metadata_json: &(serde_json::to_string(validation).map_err(json_error)?),
                    },
                )
                .await
                .map_err(map_db_conflict)?;
            reference_ordinal += 1;
        }
        for document in &snapshot.document_revisions {
            self.db
                .insert_release_document_reference_in_tx(
                    &mut tx,
                    &db::InsertReleaseDocumentReferenceInput {
                        release_id: &release_id,
                        ordinal: reference_ordinal,
                        record_id: &document.revision_id,
                        record_digest: &document.content_digest,
                        metadata_json: &(serde_json::to_string(document).map_err(json_error)?),
                    },
                )
                .await
                .map_err(map_db_conflict)?;
            reference_ordinal += 1;
        }
        for decision in &snapshot.included_decisions {
            self.db
                .insert_release_decision_reference_in_tx(
                    &mut tx,
                    &db::InsertReleaseDecisionReferenceInput {
                        release_id: &release_id,
                        ordinal: reference_ordinal,
                        record_id: &decision.decision_id,
                        record_state: &(format!("{:?}", decision.state).to_ascii_lowercase()),
                        record_digest: &decision.digest,
                        metadata_json: &(serde_json::to_string(decision).map_err(json_error)?),
                    },
                )
                .await
                .map_err(map_db_conflict)?;
            reference_ordinal += 1;
        }
        for repository in &snapshot.repository_references {
            let reference: RepositoryContextReference =
                parse_json_required(repository, "release repository reference")?;
            validate_repository_context_reference(&reference)?;
            let record_id = reference
                .execution_id
                .as_deref()
                .unwrap_or(reference.task_id.as_str())
                .to_owned();
            let record_digest =
                canonical_digest_with_schema(MILESTONE_RELEASE_DIGEST_SCHEMA_VERSION, &reference)
                    .map_err(json_error)?;
            self.db
                .insert_release_git_reference_in_tx(
                    &mut tx,
                    &db::InsertReleaseGitReferenceInput {
                        release_id: &release_id,
                        ordinal: reference_ordinal,
                        record_id: &record_id,
                        record_version: &reference.task_version.to_string(),
                        execution_status: reference.execution_status.as_deref(),
                        record_digest: &record_digest,
                        metadata_json: &(serde_json::to_string(&reference).map_err(json_error)?),
                    },
                )
                .await
                .map_err(map_db_conflict)?;
            reference_ordinal += 1;
            if let Some(review_id) = reference.review_id.as_deref() {
                let review_version = reference
                    .review_updated_at
                    .clone()
                    .or_else(|| reference.review_created_at.clone());
                let review_digest = canonical_digest_with_schema(
                    MILESTONE_RELEASE_DIGEST_SCHEMA_VERSION,
                    &json!({
                        "review_id": review_id,
                        "status": reference.review_status.clone(),
                        "ci_results": reference.ci_results.clone(),
                        "audit_result": reference.audit_result.clone(),
                        "human_decision": reference.human_decision.clone(),
                        "created_at": reference.review_created_at.clone(),
                        "updated_at": reference.review_updated_at.clone(),
                    }),
                )
                .map_err(json_error)?;
                self.db
                    .insert_release_review_reference_in_tx(
                        &mut tx,
                        &db::InsertReleaseReviewReferenceInput {
                            release_id: &release_id,
                            ordinal: reference_ordinal,
                            record_id: review_id,
                            review_version,
                            record_state: reference.review_status.as_deref(),
                            record_digest: &review_digest,
                            metadata_json: &(serde_json::to_string(&reference)
                                .map_err(json_error)?),
                        },
                    )
                    .await
                    .map_err(map_db_conflict)?;
                reference_ordinal += 1;
            }
        }
        for issue in &snapshot.known_issues {
            self.db
                .insert_release_known_issue_reference_in_tx(
                    &mut tx,
                    &release_id,
                    reference_ordinal,
                    issue,
                )
                .await
                .map_err(map_db_conflict)?;
            reference_ordinal += 1;
        }
        for (ordinal, pin) in snapshot.evidence_pins.iter().enumerate() {
            let metadata = serde_json::to_string(pin).map_err(json_error)?;
            self.db
                .insert_release_evidence_pin_in_tx(
                    &mut tx,
                    &db::InsertReleaseEvidencePinInput {
                        id: &pin.id,
                        project_id,
                        release_id: &pin.release_id,
                        asset_id: &pin.asset_id,
                        attachment_id: &pin.attachment_id,
                        task_media_id: pin.task_media_id.as_deref(),
                        asset_checksum: &pin.asset_checksum,
                        attachment_digest: &pin.attachment_digest,
                        availability: evidence_availability_name(pin.availability),
                        pin_digest: &(canonical_digest_with_schema(
                            MILESTONE_RELEASE_DIGEST_SCHEMA_VERSION,
                            &metadata,
                        )
                        .map_err(json_error)?),
                        created_at: &released_at,
                    },
                )
                .await
                .map_err(map_db_conflict)?;
            let _ = ordinal;
        }
        let transitioned = self
            .db
            .mark_milestone_released_in_tx(
                &mut tx,
                &released_at,
                milestone_id,
                project_id,
                expected_milestone_version,
            )
            .await?;
        if transitioned != 1 {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }
        self.repair_primary_pointer_in_tx(&mut tx, project_id, milestone_id)
            .await?;
        append_milestone_event(
            &self.db,
            &mut tx,
            "milestone.released",
            project_id,
            milestone_id,
            actor,
            idempotency_key,
            json!({
                "release_id": release_id,
                "release_identity": snapshot.release_identity,
                "readiness_snapshot_id": readiness_snapshot_id,
                "readiness_digest": readiness_digest,
                "snapshot_digest": snapshot.snapshot_digest,
                "authorization": authorization,
            }),
            &released_at,
        )
        .await?;
        let row = self
            .db
            .release_record_by_id_in_tx(&mut tx, &release_id)
            .await?;
        tx.commit().await?;
        self.release_from_record(row).await
    }

    pub async fn set_primary(
        &self,
        project_id: &str,
        expected_project_version: i64,
        primary_milestone_id: Option<&str>,
        actor: &PrincipalRef,
        authorization: &AuthorizationProvenance,
        idempotency_key: &str,
    ) -> crate::Result<()> {
        validate_authorization(authorization, actor, "project.milestone.primary.set")?;
        if idempotency_key.trim().is_empty() {
            return Err(crate::ServiceError::InvalidOperation {
                message: "primary milestone idempotency key is required".to_owned(),
            });
        }
        let dedupe = format!("milestone.primary.set:{project_id}:{idempotency_key}");
        if let Some(event) = DomainEventRepo::get_event_by_dedupe(&*self.db, &dedupe).await? {
            let payload: Value =
                parse_json_required(&event.payload_json, "primary milestone event")?;
            let existing_primary = payload.get("primary_milestone_id").and_then(Value::as_str);
            if existing_primary != primary_milestone_id
                || payload
                    .get("expected_project_version")
                    .and_then(Value::as_i64)
                    != Some(expected_project_version)
                || payload.get("principal_id").and_then(Value::as_str) != Some(actor.id.as_str())
            {
                return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
            }
            return Ok(());
        }

        let mut tx = db::begin_immediate(self.db.pool()).await?;
        let locked = self
            .db
            .lock_primary_milestone_project_version_in_tx(
                &mut tx,
                project_id,
                expected_project_version,
            )
            .await?;
        if locked != 1 {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }
        let active_count: i64 = self
            .db
            .active_milestone_count_in_tx(&mut tx, project_id)
            .await?;
        if active_count > 0 && primary_milestone_id.is_none() {
            return Err(crate::ServiceError::InvalidOperation {
                message: "an active Project must retain an explicit primary milestone".to_owned(),
            });
        }
        if let Some(id) = primary_milestone_id {
            let valid: Option<i64> = self
                .db
                .active_milestone_exists_in_tx(&mut tx, id, project_id)
                .await?;
            if valid.is_none() {
                return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
            }
        }
        let occurred_at = now_rfc3339();
        let updated = self
            .db
            .set_primary_milestone_in_tx(
                &mut tx,
                primary_milestone_id,
                &occurred_at,
                project_id,
                expected_project_version,
            )
            .await?;
        if updated != 1 {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }
        let entity_id = primary_milestone_id
            .map(str::to_owned)
            .unwrap_or_else(|| "project".to_owned());
        append_milestone_event(
            &self.db,
            &mut tx,
            "milestone.primary.set",
            project_id,
            &entity_id,
            actor,
            idempotency_key,
            json!({
                "primary_milestone_id": primary_milestone_id,
                "expected_project_version": expected_project_version,
                "principal_id": actor.id.clone(),
            }),
            &occurred_at,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Read one immutable release by Project-scoped identifier.
    pub async fn get_release(
        &self,
        project_id: &str,
        release_id: &str,
    ) -> crate::Result<Option<api_types::ProjectRelease>> {
        let Some(row) = self
            .db
            .project_release_record_by_id(release_id, project_id)
            .await?
        else {
            return Ok(None);
        };
        Ok(Some(self.release_from_record(row).await?))
    }

    pub async fn list_releases(
        &self,
        project_id: &str,
        milestone_id: &str,
    ) -> crate::Result<Vec<ProjectRelease>> {
        let rows = self
            .db
            .milestone_release_records(project_id, milestone_id)
            .await?;
        let mut releases = Vec::with_capacity(rows.len());
        for row in rows {
            releases.push(self.release_from_record(row).await?);
        }
        Ok(releases)
    }

    async fn release_revision_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        milestone_id: &str,
    ) -> crate::Result<i64> {
        Ok(self
            .db
            .next_milestone_release_revision_in_tx(tx, milestone_id)
            .await?)
    }

    async fn release_by_idempotency(
        &self,
        project_id: &str,
        key: &str,
    ) -> crate::Result<Option<ProjectReleaseRecord>> {
        Ok(self
            .db
            .project_release_by_idempotency(project_id, key)
            .await?)
    }

    async fn release_by_readiness_target(
        &self,
        project_id: &str,
        milestone_id: &str,
        readiness_snapshot_id: &str,
        readiness_digest: &str,
    ) -> crate::Result<Option<ProjectReleaseRecord>> {
        Ok(self
            .db
            .project_release_by_readiness_target(
                project_id,
                milestone_id,
                readiness_snapshot_id,
                readiness_digest,
            )
            .await?)
    }

    async fn release_from_record(
        &self,
        record: ProjectReleaseRecord,
    ) -> crate::Result<ProjectRelease> {
        if record.schema_version != RELEASE_SCHEMA_VERSION
            || record.snapshot_digest.trim().is_empty()
            || record.release_revision <= 0
            || record.release_sequence <= 0
        {
            return Err(crate::ServiceError::InvalidOperation {
                message: "immutable release record has invalid schema or identity fields"
                    .to_owned(),
            });
        }
        let milestone = self
            .get_record(&record.project_id, &record.milestone_id)
            .await?
            .ok_or_else(|| {
                crate::ServiceError::not_found("milestone", record.milestone_id.clone())
            })?;
        let definition_record = ProjectOrchestrationRepo::get_project_milestone_revision(
            &*self.db,
            &record.milestone_revision_id,
        )
        .await?
        .ok_or_else(|| {
            crate::ServiceError::not_found(
                "milestone_definition_revision",
                record.milestone_revision_id.clone(),
            )
        })?;
        if definition_record.milestone_id != record.milestone_id {
            return Err(crate::ServiceError::InvalidOperation {
                message: "immutable release definition belongs to another milestone".to_owned(),
            });
        }
        let definition = definition_from_record(definition_record, &record.project_id)?;
        let readiness = self
            .readiness_by_id(&record.project_id, &record.readiness_snapshot_id)
            .await?
            .ok_or_else(|| {
                crate::ServiceError::not_found(
                    "readiness_snapshot",
                    record.readiness_snapshot_id.clone(),
                )
            })?;
        let readiness = readiness_from_record(readiness)?;
        if record.readiness_digest != readiness.readiness_digest {
            return Err(crate::ServiceError::InvalidOperation {
                message: "immutable release authority references disagree with readiness"
                    .to_owned(),
            });
        }
        let pins = self
            .release_pins(
                &record.project_id,
                &record.id,
                &record.evidence_references_json,
            )
            .await?;
        let charter_revision =
            if let Some(charter_revision_id) = record.charter_revision_id.as_deref() {
                let row = self
                    .db
                    .release_charter_revision_source(charter_revision_id, &record.project_id)
                    .await?;
                row.map(|row| {
                    Ok::<_, crate::ServiceError>(ArtifactRef {
                        artifact_id: row.charter_id.clone(),
                        revision_id: charter_revision_id.to_owned(),
                        content_digest: row.content_digest.clone(),
                        render_version: Some(row.render_version.clone()),
                        render_digest: Some(row.rendered_digest.clone()),
                    })
                })
                .transpose()?
                .ok_or_else(|| crate::ServiceError::InvalidOperation {
                    message: format!(
                    "immutable release references missing Charter revision {charter_revision_id}"
                ),
                })?
            } else {
                return Err(crate::ServiceError::InvalidOperation {
                    message: "immutable release is missing its Charter revision".to_owned(),
                });
            };
        let releasing_principal = PrincipalRef {
            kind: principal_kind(&record.releasing_principal_type)?,
            id: record.releasing_principal_id.clone(),
            display_name: record.releasing_principal_display_name.clone(),
        };
        let release_authorization = AuthorizationProvenance {
            principal: releasing_principal.clone(),
            authorization_basis: record.authorization_basis.clone(),
            action: record.authorization_action.clone(),
            event_id: record.explicit_event.clone(),
            occurred_at: record.authorization_occurred_at.clone(),
        };
        validate_persisted_authorization(&release_authorization, "project.milestone.release")?;
        let snapshot = ReleaseSnapshot {
            schema_version: record.schema_version.clone(),
            project_id: record.project_id.clone(),
            milestone_id: record.milestone_id.clone(),
            milestone_canonical_id: milestone.milestone_key.clone(),
            release_revision: record.release_revision,
            release_identity: record.release_identifier.clone(),
            milestone_definition_revision_id: record.milestone_revision_id.clone(),
            milestone_definition_digest: definition.content_digest,
            expected_milestone_version: readiness.expected_milestone_version,
            display_label: milestone.display_label.clone(),
            summary: record.summary.clone(),
            changelog: parse_json_required(&record.changelog, "release changelog")?,
            known_issues: parse_json_required(&record.known_issues_json, "release known_issues")?,
            readiness_snapshot_id: readiness.id,
            readiness_digest: record.readiness_digest.clone(),
            source_event_watermark: readiness.source_event_watermark.clone(),
            charter_revision,
            document_revisions: parse_json_required(
                &record.document_revisions_json,
                "release document_revisions",
            )?,
            included_decisions: parse_json_required(
                &record.decision_ids_json,
                "release decision_ids",
            )?,
            included_tasks: parse_json_required(
                &record.task_references_json,
                "release task_references",
            )?,
            validation_results: parse_json_required(
                &record.validation_references_json,
                "release validation_references",
            )?,
            repository_references: parse_json_required(
                &record.git_references_json,
                "release git_references",
            )?,
            evidence_pins: pins,
            waived_check_ids: parse_json_required(&record.waivers_json, "release waivers")?,
            released_by: releasing_principal,
            authorization: release_authorization,
            released_at: record.created_at.clone(),
            idempotency_key: record.idempotency_key.clone(),
            snapshot_digest: record.snapshot_digest.clone(),
        };
        let computed_snapshot_digest =
            release_snapshot_digest(&snapshot).map_err(map_orchestration_error)?;
        if computed_snapshot_digest != record.snapshot_digest {
            return Err(crate::ServiceError::InvalidOperation {
                message: "immutable release snapshot digest does not match its contents".to_owned(),
            });
        }
        self.validate_release_references(&record.id, &snapshot)
            .await?;
        Ok(ProjectRelease {
            id: record.id,
            project_id: record.project_id,
            milestone_id: record.milestone_id,
            release_sequence: record.release_sequence,
            release_identity: record.release_identifier.clone(),
            snapshot,
            version: record.release_revision,
            created_at: record.created_at,
        })
    }

    async fn validate_release_references(
        &self,
        release_id: &str,
        snapshot: &ReleaseSnapshot,
    ) -> crate::Result<()> {
        type ExpectedReference = (
            &'static str,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            String,
        );
        let mut expected: Vec<ExpectedReference> = Vec::new();
        for task in &snapshot.included_tasks {
            expected.push((
                "task",
                task.task_id.clone(),
                Some(task.task_version.to_string()),
                Some(task.task_state.clone()),
                None,
                serde_json::to_string(task).map_err(json_error)?,
            ));
        }
        for validation in &snapshot.validation_results {
            expected.push((
                "validation",
                validation.validation_id.clone(),
                Some(validation.evaluated_at.clone()),
                Some(format!("{:?}", validation.status).to_ascii_lowercase()),
                Some(validation.result_digest.clone()),
                serde_json::to_string(validation).map_err(json_error)?,
            ));
        }
        for document in &snapshot.document_revisions {
            expected.push((
                "document",
                document.revision_id.clone(),
                None,
                Some("approved".to_owned()),
                Some(document.content_digest.clone()),
                serde_json::to_string(document).map_err(json_error)?,
            ));
        }
        for decision in &snapshot.included_decisions {
            expected.push((
                "decision",
                decision.decision_id.clone(),
                None,
                Some(format!("{:?}", decision.state).to_ascii_lowercase()),
                Some(decision.digest.clone()),
                serde_json::to_string(decision).map_err(json_error)?,
            ));
        }
        for repository in &snapshot.repository_references {
            let reference: RepositoryContextReference =
                parse_json_required(repository, "release repository reference")?;
            validate_repository_context_reference(&reference)?;
            let record_id = reference
                .execution_id
                .as_deref()
                .unwrap_or(reference.task_id.as_str())
                .to_owned();
            let record_digest =
                canonical_digest_with_schema(MILESTONE_RELEASE_DIGEST_SCHEMA_VERSION, &reference)
                    .map_err(json_error)?;
            expected.push((
                "git_ref",
                record_id,
                Some(reference.task_version.to_string()),
                reference.execution_status.clone(),
                Some(record_digest),
                serde_json::to_string(&reference).map_err(json_error)?,
            ));
            if let Some(review_id) = reference.review_id.as_deref() {
                let review_version = reference
                    .review_updated_at
                    .clone()
                    .or_else(|| reference.review_created_at.clone());
                let review_digest = canonical_digest_with_schema(
                    MILESTONE_RELEASE_DIGEST_SCHEMA_VERSION,
                    &json!({
                        "review_id": review_id,
                        "status": reference.review_status.clone(),
                        "ci_results": reference.ci_results.clone(),
                        "audit_result": reference.audit_result.clone(),
                        "human_decision": reference.human_decision.clone(),
                        "created_at": reference.review_created_at.clone(),
                        "updated_at": reference.review_updated_at.clone(),
                    }),
                )
                .map_err(json_error)?;
                expected.push((
                    "review",
                    review_id.to_owned(),
                    review_version,
                    reference.review_status.clone(),
                    Some(review_digest),
                    serde_json::to_string(&reference).map_err(json_error)?,
                ));
            }
        }
        for issue in &snapshot.known_issues {
            expected.push((
                "known_issue",
                issue.clone(),
                None,
                None,
                None,
                "{}".to_owned(),
            ));
        }
        let rows = self.db.release_reference_rows(release_id).await?;
        if rows.len() != expected.len() {
            return Err(crate::ServiceError::InvalidOperation {
                message: "immutable release references do not match its snapshot".to_owned(),
            });
        }
        for (row, (kind, id, version, state, digest, metadata)) in rows.into_iter().zip(expected) {
            if row.reference_kind.clone() != kind
                || row.record_id.clone() != id
                || row.record_version.clone() != version
                || row.record_state.clone() != state
                || row.record_digest.clone() != digest
                || row.metadata_json.clone() != metadata
            {
                return Err(crate::ServiceError::InvalidOperation {
                    message: "immutable release reference disagrees with its snapshot".to_owned(),
                });
            }
        }
        Ok(())
    }

    async fn release_pins(
        &self,
        project_id: &str,
        release_id: &str,
        frozen_evidence_json: &str,
    ) -> crate::Result<Vec<api_types::EvidencePin>> {
        let frozen: Vec<api_types::EvidencePin> =
            parse_json_required(frozen_evidence_json, "release evidence_references")?;
        let rows = self
            .db
            .release_evidence_pin_rows(project_id, release_id)
            .await?;
        rows.into_iter()
            .map(|row| {
                let attachment_id = row.attachment_id.clone().ok_or_else(|| {
                    crate::ServiceError::InvalidOperation {
                        message: "immutable release pin is missing attachment_id".to_owned(),
                    }
                })?;
                let attachment_digest = row
                    .attachment_digest
                    .clone()
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| crate::ServiceError::InvalidOperation {
                        message: "immutable release pin is missing attachment digest".to_owned(),
                    })?;
                let asset_checksum = row
                    .asset_checksum
                    .clone()
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| crate::ServiceError::InvalidOperation {
                        message: "immutable release pin is missing asset checksum".to_owned(),
                    })?;
                if let Some(live_asset_checksum) = row
                    .live_asset_checksum
                    .clone()
                    .filter(|value| !value.trim().is_empty())
                {
                    if live_asset_checksum != asset_checksum {
                        return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
                    }
                }
                let availability = evidence_availability(&row.availability.clone())?;
                let pin_digest: String = row
                    .pin_digest
                    .clone()
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| crate::ServiceError::InvalidOperation {
                        message: "immutable release pin is missing pin_digest".to_owned(),
                    })?;
                let availability_projection = match row.tombstone_availability.clone() {
                    Some(value) => release_evidence_tombstone(&value)?,
                    None => release_evidence_availability(availability),
                };
                let id: String = row.id.clone();
                let frozen_pin = frozen.iter().find(|pin| pin.id == id).ok_or_else(|| {
                    crate::ServiceError::InvalidOperation {
                        message: format!("release pin {id} is missing frozen metadata"),
                    }
                })?;
                if frozen_pin.attachment_digest != attachment_digest
                    || frozen_pin.asset_checksum != asset_checksum
                    || frozen_pin.attachment_id != attachment_id
                    || frozen_pin.asset_id != row.asset_id.clone()
                    || frozen_pin.release_id != row.release_id.clone()
                    || frozen_pin.availability != availability
                    || frozen_pin.availability_projection
                        != release_evidence_availability(availability)
                    || frozen_pin.pinned_at != row.created_at.clone()
                {
                    return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
                }
                let frozen_metadata = serde_json::to_string(frozen_pin).map_err(json_error)?;
                let expected_pin_digest = canonical_digest_with_schema(
                    MILESTONE_RELEASE_DIGEST_SCHEMA_VERSION,
                    &frozen_metadata,
                )
                .map_err(json_error)?;
                if pin_digest != expected_pin_digest {
                    return Err(crate::ServiceError::InvalidOperation {
                        message: "immutable release pin digest does not match its metadata"
                            .to_owned(),
                    });
                }
                Ok(api_types::EvidencePin {
                    id,
                    release_id: row.release_id.clone(),
                    attachment_id,
                    asset_id: row.asset_id.clone(),
                    attachment_digest,
                    asset_checksum,
                    availability,
                    availability_projection,
                    task_media_id: frozen_pin.task_media_id.clone(),
                    stable_project_url: frozen_pin.stable_project_url.clone(),
                    pinned_at: row.created_at.clone(),
                })
            })
            .collect::<crate::Result<Vec<_>>>()
            .and_then(|mut pins| {
                if pins.len() != frozen.len() {
                    return Err(crate::ServiceError::InvalidOperation {
                        message: "release evidence pin set does not match frozen metadata"
                            .to_owned(),
                    });
                }
                // The release digest was taken over the pin order the release
                // transaction produced, while this query returns them by pin id.
                // Pin ids are fresh UUIDs, so the two orders agree only by
                // accident and the snapshot digest could never reproduce for a
                // release carrying more than one piece of evidence. The frozen
                // metadata is the immutable record of what was digested, so it
                // is what defines the order here.
                let order: HashMap<&str, usize> = frozen
                    .iter()
                    .enumerate()
                    .map(|(index, pin)| (pin.id.as_str(), index))
                    .collect();
                pins.sort_by_key(|pin| order.get(pin.id.as_str()).copied().unwrap_or(usize::MAX));
                Ok(pins)
            })
    }
}
