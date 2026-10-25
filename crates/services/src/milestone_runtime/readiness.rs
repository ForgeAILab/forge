//! Readiness evaluation, freshness, and transactional rechecks.

use super::*;

impl MilestoneRuntime {
    /// Compute one readiness candidate without persisting it.  Command
    /// services use this read-locked computation to prepare the complete
    /// immutable snapshot, then hand that snapshot to the command-aware DB
    /// composite, which commits the snapshot, lifecycle projection, event,
    /// receipt, and optional action execution together.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn evaluate_candidate(
        &self,
        project_id: &str,
        actor: &PrincipalRef,
        authorization: &AuthorizationProvenance,
        milestone_id: &str,
        expected_milestone_version: i64,
    ) -> crate::Result<ReadinessSnapshot> {
        if expected_milestone_version < 1
            || project_id.trim().is_empty()
            || milestone_id.trim().is_empty()
        {
            return Err(crate::ServiceError::InvalidOperation {
                message: "readiness candidate references are incomplete".to_owned(),
            });
        }
        validate_authorization(authorization, actor, "project.milestone.readiness")?;
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        if actor.kind == PrincipalKind::Agent {
            let bound_agent = self
                .project_agent_principal_in_tx(&mut tx, project_id)
                .await?;
            if bound_agent.id != actor.id {
                return Err(crate::ServiceError::InvalidOperation {
                    message: "readiness may only be requested by the bound Project Agent"
                        .to_owned(),
                });
            }
        }
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
        let milestone = self
            .get_record_in_tx(&mut tx, project_id, milestone_id)
            .await?
            .ok_or_else(|| crate::ServiceError::not_found("milestone", milestone_id))?;
        let revision_id = milestone
            .current_definition_revision_id
            .clone()
            .ok_or_else(|| crate::ServiceError::InvalidOperation {
                message: "milestone has no current definition revision".to_owned(),
            })?;
        let definition_record = self
            .definition_record_in_tx(&mut tx, project_id, milestone_id, &revision_id)
            .await?
            .ok_or_else(|| {
                crate::ServiceError::not_found("milestone_definition_revision", revision_id.clone())
            })?;
        let mut definition = definition_from_record(definition_record, project_id)?;
        self.hydrate_charter_in_tx(&mut tx, project_id, &mut definition)
            .await?;
        let definition_contract_reasons = milestone_definition_contract_reasons(&definition);
        let check_results = self
            .check_results_in_tx(
                &mut tx,
                project_id,
                milestone_id,
                &revision_id,
                definition
                    .content
                    .charter_revision
                    .as_ref()
                    .map(|charter| charter.revision_id.as_str()),
            )
            .await?;
        let evidence = self
            .evidence_in_tx(&mut tx, project_id, milestone_id)
            .await?;
        let waiver_ids = self
            .waiver_ids_in_tx(&mut tx, project_id, milestone_id)
            .await?;
        let (task_states, document_states) = self
            .source_states_in_tx(&mut tx, project_id, milestone_id, &definition)
            .await?;
        let commit_build_check_context = self
            .commit_build_check_context_in_tx(&mut tx, project_id, &task_states)
            .await?;
        let input_manifest = self
            .input_manifest_in_tx(
                &mut tx,
                project_id,
                milestone_id,
                &revision_id,
                definition.content.charter_revision.as_ref(),
                &check_results,
                &evidence,
                &waiver_ids,
                &task_states,
                &document_states,
                &commit_build_check_context,
            )
            .await?;
        let source_event_watermark = self.source_watermark_in_tx(&mut tx, project_id).await?;
        let evaluation = evaluate_readiness(ReadinessEvaluationInput {
            milestone: project_milestone_from_record(milestone)?,
            definition,
            source_event_watermark,
            computing_policy_revision: COMPUTING_POLICY_REVISION.to_owned(),
            input_manifest,
            check_results,
            evidence,
            waiver_ids,
            task_states,
            document_states,
            commit_build_check_context,
            definition_contract_reasons,
            authorization: authorization.clone(),
        })
        .map_err(map_orchestration_error)?;
        let snapshot = evaluation.into_snapshot(new_uuid_v4(), now_rfc3339());
        tx.commit().await?;
        Ok(snapshot)
    }

    /// Compute and persist one immutable readiness candidate.  The exact
    /// source rows are loaded before the insert and their versions/digests are
    /// included in the pure evaluator's ordered manifest.  A successful
    /// candidate compare-and-swaps `active` to `ready_for_release`; non-ready
    /// candidates remain active and persist typed projection reasons.
    #[allow(clippy::too_many_arguments)]
    pub async fn evaluate(
        &self,
        project_id: &str,
        actor: &PrincipalRef,
        authorization: &AuthorizationProvenance,
        milestone_id: &str,
        expected_milestone_version: i64,
        idempotency_key: &str,
    ) -> crate::Result<ReadinessSnapshot> {
        if idempotency_key.trim().is_empty() {
            return Err(crate::ServiceError::InvalidOperation {
                message: "readiness idempotency key is required".to_owned(),
            });
        }

        // Read and write the complete readiness candidate on one connection.
        // SQLite's write transaction is opened before any source reload; this
        // prevents a check/evidence mutation from being observed after the
        // candidate was computed but before the lifecycle CAS.
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        if let Some(existing) = self
            .db
            .readiness_by_idempotency_in_tx(&mut tx, project_id, idempotency_key)
            .await?
        {
            if existing.milestone_id != milestone_id
                || existing.expected_milestone_version != expected_milestone_version
                || existing.principal_type != principal_kind_name(actor.kind)
                || existing.principal_id != actor.id
                || existing.principal_type != principal_kind_name(authorization.principal.kind)
                || existing.principal_id != authorization.principal.id
                || existing.authorization_basis != authorization.authorization_basis
                || existing.authorization_action != authorization.action
                || existing.explicit_event != authorization.event_id
                || existing.authorization_occurred_at != authorization.occurred_at
            {
                return Err(crate::ServiceError::Db(db::DbError::IdempotencyConflict));
            }
            tx.commit().await?;
            return readiness_from_record(existing);
        }
        validate_authorization(authorization, actor, "project.milestone.readiness")?;
        if actor.kind == PrincipalKind::Agent {
            let bound_agent = self
                .project_agent_principal_in_tx(&mut tx, project_id)
                .await?;
            if bound_agent.id != actor.id {
                return Err(crate::ServiceError::InvalidOperation {
                    message: "readiness may only be requested by the bound Project Agent"
                        .to_owned(),
                });
            }
        }

        // Acquire the milestone write lock before loading any source row. A
        // readiness candidate must observe one authoritative SQLite snapshot;
        // a later lifecycle CAS alone would still permit a source mutation to
        // race between the first read and the evaluation.
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

        let milestone = self
            .get_record_in_tx(&mut tx, project_id, milestone_id)
            .await?
            .ok_or_else(|| crate::ServiceError::not_found("milestone", milestone_id))?;
        if milestone.version != expected_milestone_version {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }
        let revision_id = milestone
            .current_definition_revision_id
            .clone()
            .ok_or_else(|| crate::ServiceError::InvalidOperation {
                message: "milestone has no current definition revision".to_owned(),
            })?;
        let definition_record = self
            .definition_record_in_tx(&mut tx, project_id, milestone_id, &revision_id)
            .await?
            .ok_or_else(|| {
                crate::ServiceError::not_found("milestone_definition_revision", revision_id.clone())
            })?;
        let mut definition = definition_from_record(definition_record, project_id)?;
        self.hydrate_charter_in_tx(&mut tx, project_id, &mut definition)
            .await?;
        let definition_contract_reasons = milestone_definition_contract_reasons(&definition);
        let check_results = self
            .check_results_in_tx(
                &mut tx,
                project_id,
                milestone_id,
                &revision_id,
                definition
                    .content
                    .charter_revision
                    .as_ref()
                    .map(|charter| charter.revision_id.as_str()),
            )
            .await?;
        let evidence = self
            .evidence_in_tx(&mut tx, project_id, milestone_id)
            .await?;
        let waiver_ids = self
            .waiver_ids_in_tx(&mut tx, project_id, milestone_id)
            .await?;
        let (task_states, document_states) = self
            .source_states_in_tx(&mut tx, project_id, milestone_id, &definition)
            .await?;
        let commit_build_check_context = self
            .commit_build_check_context_in_tx(&mut tx, project_id, &task_states)
            .await?;
        let input_manifest = self
            .input_manifest_in_tx(
                &mut tx,
                project_id,
                milestone_id,
                &revision_id,
                definition.content.charter_revision.as_ref(),
                &check_results,
                &evidence,
                &waiver_ids,
                &task_states,
                &document_states,
                &commit_build_check_context,
            )
            .await?;
        let source_event_watermark = self.source_watermark_in_tx(&mut tx, project_id).await?;
        let evaluation = evaluate_readiness(ReadinessEvaluationInput {
            milestone: project_milestone_from_record(milestone.clone())?,
            definition,
            source_event_watermark,
            computing_policy_revision: COMPUTING_POLICY_REVISION.to_owned(),
            input_manifest,
            check_results,
            evidence,
            waiver_ids,
            task_states: task_states.clone(),
            document_states: document_states.clone(),
            commit_build_check_context,
            definition_contract_reasons,
            authorization: authorization.clone(),
        })
        .map_err(map_orchestration_error)?;
        let snapshot = evaluation
            .clone()
            .into_snapshot(new_uuid_v4(), now_rfc3339());

        let current: i64 = self
            .db
            .milestone_version_in_tx(&mut tx, milestone_id, project_id)
            .await?
            .ok_or(db::DbError::NotFound)?;
        if current != expected_milestone_version {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }
        insert_readiness(
            &self.db,
            &mut tx,
            &snapshot,
            authorization,
            idempotency_key,
            expected_milestone_version,
        )
        .await?;
        let projection_reasons = projection_reasons(&evaluation.reasons);
        let (next_lifecycle, blocker, stale, reconciliation) = if milestone.lifecycle == "released"
        {
            // Correction readiness is observational only for an immutable
            // release. Never regress a released milestone to active, even
            // when the fresh candidate is blocked/stale/failed.
            (
                "released",
                serde_json::to_string(&projection_reasons.blockers).map_err(json_error)?,
                serde_json::to_string(&projection_reasons.stale).map_err(json_error)?,
                serde_json::to_string(&projection_reasons.reconciliation).map_err(json_error)?,
            )
        } else if evaluation.result == ReadinessResult::Ready {
            (
                if milestone.lifecycle == "released" {
                    "released"
                } else {
                    "ready_for_release"
                },
                "[]".to_owned(),
                "[]".to_owned(),
                "[]".to_owned(),
            )
        } else {
            (
                "active",
                serde_json::to_string(&projection_reasons.blockers).map_err(json_error)?,
                serde_json::to_string(&projection_reasons.stale).map_err(json_error)?,
                serde_json::to_string(&projection_reasons.reconciliation).map_err(json_error)?,
            )
        };
        let changed = self
            .db
            .update_milestone_readiness_projection_in_tx(
                &mut tx,
                &db::UpdateMilestoneReadinessProjectionInput {
                    lifecycle: next_lifecycle,
                    blocker_json: &blocker,
                    stale_json: &stale,
                    reconciliation_json: &reconciliation,
                    now: &snapshot.computed_at,
                    milestone_id,
                    project_id,
                    expected_milestone_version,
                },
            )
            .await?;
        if changed != 1 {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }
        self.repair_primary_pointer_in_tx(&mut tx, project_id, milestone_id)
            .await?;
        append_milestone_event(
            &self.db,
            &mut tx,
            "milestone.readiness.evaluated",
            project_id,
            milestone_id,
            actor,
            idempotency_key,
            json!({
                "readiness_snapshot_id": snapshot.id,
                "readiness_digest": snapshot.readiness_digest,
                "result": snapshot.result,
                "projection_reasons": projection_reasons,
                "authorization": authorization,
            }),
            &snapshot.computed_at,
        )
        .await?;
        tx.commit().await?;
        Ok(snapshot)
    }

    pub async fn get_readiness(
        &self,
        project_id: &str,
        milestone_id: &str,
        snapshot_id: &str,
    ) -> crate::Result<Option<ReadinessSnapshot>> {
        let Some(row) = self
            .db
            .milestone_readiness_record_by_id(snapshot_id, project_id, milestone_id)
            .await?
        else {
            return Ok(None);
        };
        Ok(Some(readiness_from_record(row)?))
    }

    /// Recompute the current governed inputs for one immutable readiness
    /// snapshot without changing any persisted state.  This is deliberately
    /// the same recheck used by release: a live Overview must not infer
    /// freshness from the broad Project event stream, because attention
    /// events and readiness audit events do not change release authority.
    pub async fn readiness_freshness(
        &self,
        project_id: &str,
        milestone_id: &str,
        snapshot_id: &str,
    ) -> crate::Result<ReadinessFreshness> {
        let mut tx = self.db.pool().begin().await?;
        let Some(record) = self
            .readiness_by_id_in_tx(&mut tx, project_id, snapshot_id)
            .await?
        else {
            tx.commit().await?;
            return Ok(ReadinessFreshness {
                status: ReadinessFreshnessStatus::Unavailable,
                reason: Some("The immutable readiness snapshot is unavailable.".to_owned()),
                snapshot_source_event_watermark: None,
                current_source_event_watermark: None,
            });
        };
        if record.milestone_id != milestone_id {
            tx.commit().await?;
            return Ok(ReadinessFreshness {
                status: ReadinessFreshnessStatus::Unavailable,
                reason: Some(
                    "The readiness snapshot does not belong to this milestone.".to_owned(),
                ),
                snapshot_source_event_watermark: None,
                current_source_event_watermark: None,
            });
        }

        let candidate = readiness_from_record(record.clone())?;
        let snapshot_watermark = Some(candidate.source_event_watermark.clone());
        let current_watermark = Some(self.source_watermark_in_tx(&mut tx, project_id).await?);
        // A release consumes exactly one readiness snapshot. `release()` only
        // accepts the snapshot whose readiness transition produced the
        // milestone's current version, then records its own one-step CAS, so
        // a released milestone sits two versions past the snapshot it pinned.
        let pinned_by_release = self
            .db
            .releases_pinning_readiness_count_in_tx(&mut tx, project_id, milestone_id, snapshot_id)
            .await?
            > 0;
        let recheck = self
            .recheck_readiness_in_tx(&mut tx, project_id, milestone_id, record)
            .await;

        let (status, reason) = match recheck {
            Ok(recheck) => {
                let expected_current_version = candidate.expected_milestone_version.checked_add(1);
                // The snapshot a release pinned is current at exactly the
                // readiness transition plus the release transition. A later
                // correction on the released milestone (the direct runtime
                // evaluator's one-step CAS, or the command composite that
                // leaves a terminal version unchanged) is a different
                // snapshot with its own expected version; active/ready
                // candidates still require the one readiness transition.
                let version_matches = if recheck.milestone.lifecycle == "released"
                    && pinned_by_release
                {
                    candidate
                        .expected_milestone_version
                        .checked_add(2)
                        .is_some_and(|version| recheck.milestone.version == version)
                } else if recheck.milestone.lifecycle == "released" {
                    recheck.milestone.version == candidate.expected_milestone_version
                        || expected_current_version
                            .is_some_and(|version| recheck.milestone.version == version)
                } else {
                    expected_current_version
                        .is_some_and(|version| recheck.milestone.version == version)
                };
                let inputs_match = readiness_inputs_match(
                    &recheck.candidate_snapshot,
                    &recheck.recomputed,
                );
                if version_matches && inputs_match {
                    (ReadinessFreshnessStatus::Current, None)
                } else if !version_matches {
                    (
                        ReadinessFreshnessStatus::Stale,
                        Some(
                            "The milestone changed beyond the one readiness transition represented by this snapshot."
                                .to_owned(),
                        ),
                    )
                } else {
                    (
                        ReadinessFreshnessStatus::Stale,
                        Some(
                            "One or more governed readiness inputs changed after this immutable snapshot was computed."
                                .to_owned(),
                        ),
                    )
                }
            }
            Err(crate::ServiceError::Db(db::DbError::VersionConflict)) => (
                ReadinessFreshnessStatus::Stale,
                Some(
                    "One or more governed readiness inputs no longer match this immutable snapshot."
                        .to_owned(),
                ),
            ),
            Err(error) => {
                tracing::warn!(
                    project_id,
                    milestone_id,
                    snapshot_id,
                    error = %error,
                    "readiness freshness recomputation unavailable"
                );
                (
                    ReadinessFreshnessStatus::Unavailable,
                    Some("The current readiness inputs could not be rechecked.".to_owned()),
                )
            }
        };
        tx.commit().await?;
        Ok(ReadinessFreshness {
            status,
            reason,
            // These values are diagnostic only.  Freshness is determined by
            // the exact input comparison above, never by watermark equality.
            snapshot_source_event_watermark: snapshot_watermark,
            current_source_event_watermark: current_watermark,
        })
    }

    pub async fn list_readiness(
        &self,
        project_id: &str,
        milestone_id: &str,
    ) -> crate::Result<Vec<ReadinessSnapshot>> {
        let rows = self
            .db
            .milestone_readiness_records(project_id, milestone_id)
            .await?;
        rows.into_iter().map(readiness_from_record).collect()
    }

    pub(super) async fn readiness_by_id_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
        id: &str,
    ) -> crate::Result<Option<ProjectReadinessSnapshotRecord>> {
        self.db
            .project_readiness_record_in_tx(tx, project_id, id)
            .await
            .map_err(crate::ServiceError::from)
    }

    /// Recompute the exact current readiness inputs for a persisted candidate.
    /// The candidate's expected milestone version is retained for the pure
    /// evaluator because recording readiness is the one permitted transition
    /// between candidate creation and the live milestone row.  This mirrors
    /// the release verification path; callers decide whether to persist or
    /// merely compare the result.
    pub(super) async fn recheck_readiness_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
        milestone_id: &str,
        candidate: ProjectReadinessSnapshotRecord,
    ) -> crate::Result<ReadinessRecheck> {
        let milestone = self
            .get_record_in_tx(tx, project_id, milestone_id)
            .await?
            .ok_or_else(|| crate::ServiceError::not_found("milestone", milestone_id))?;
        if candidate.milestone_id != milestone_id || candidate.project_id != project_id {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }
        let expected_revision_id = milestone
            .current_definition_revision_id
            .clone()
            .ok_or_else(|| crate::ServiceError::InvalidOperation {
                message: "milestone has no current definition revision".to_owned(),
            })?;
        let definition_record = self
            .definition_record_in_tx(tx, project_id, milestone_id, &expected_revision_id)
            .await?
            .ok_or_else(|| {
                crate::ServiceError::not_found(
                    "milestone_definition_revision",
                    expected_revision_id.clone(),
                )
            })?;
        let mut definition = definition_from_record(definition_record, project_id)?;
        self.hydrate_charter_in_tx(tx, project_id, &mut definition)
            .await?;
        if candidate.definition_revision_id != expected_revision_id {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }

        let definition_contract_reasons = milestone_definition_contract_reasons(&definition);

        let check_results = self
            .check_results_in_tx(
                tx,
                project_id,
                milestone_id,
                &expected_revision_id,
                definition
                    .content
                    .charter_revision
                    .as_ref()
                    .map(|charter| charter.revision_id.as_str()),
            )
            .await?;
        let evidence = self.evidence_in_tx(tx, project_id, milestone_id).await?;
        let waiver_ids = self.waiver_ids_in_tx(tx, project_id, milestone_id).await?;
        let (task_states, document_states) = self
            .source_states_in_tx(tx, project_id, milestone_id, &definition)
            .await?;
        let commit_build_check_context = self
            .commit_build_check_context_in_tx(tx, project_id, &task_states)
            .await?;
        let mut input_manifest = self
            .input_manifest_in_tx(
                tx,
                project_id,
                milestone_id,
                &expected_revision_id,
                definition.content.charter_revision.as_ref(),
                &check_results,
                &evidence,
                &waiver_ids,
                &task_states,
                &document_states,
                &commit_build_check_context,
            )
            .await?;
        let candidate_expected_milestone_version = candidate.expected_milestone_version;
        let candidate_snapshot = readiness_from_record(candidate)?;

        // Evaluating readiness itself advances the mutable milestone one
        // version and (for a ready result) changes active -> ready_for_release.
        // Normalize only the self-authored milestone source entry back to the
        // immutable candidate identity.  This is also correct for a released
        // correction: released rows remain terminal while their observational
        // readiness candidate is recomputed against the same one-step version.
        if let Some(milestone_input) = input_manifest
            .iter_mut()
            .find(|input| input.source_kind == "milestone")
        {
            let candidate_milestone_input = candidate_snapshot
                .input_manifest
                .iter()
                .find(|input| input.source_kind == "milestone")
                .ok_or_else(|| crate::ServiceError::InvalidOperation {
                    message: "readiness snapshot is missing its milestone source manifest"
                        .to_owned(),
                })?;
            milestone_input.source_version = candidate_expected_milestone_version;
            milestone_input.source_digest = candidate_milestone_input.source_digest.clone();
            milestone_input.observed_at = candidate_milestone_input.observed_at.clone();
        }
        let recomputed = evaluate_readiness(ReadinessEvaluationInput {
            milestone: ProjectMilestone {
                version: candidate_expected_milestone_version,
                ..project_milestone_from_record(milestone.clone())?
            },
            definition: definition.clone(),
            // Preserve the candidate's immutable diagnostic watermark while
            // recomputing its governed inputs.  The current watermark is
            // returned separately by `readiness_freshness`; using it here
            // would make the digest/release token depend on unrelated events.
            source_event_watermark: candidate_snapshot.source_event_watermark.clone(),
            computing_policy_revision: COMPUTING_POLICY_REVISION.to_owned(),
            input_manifest,
            check_results,
            evidence: evidence.clone(),
            waiver_ids,
            task_states: task_states.clone(),
            document_states: document_states.clone(),
            commit_build_check_context,
            definition_contract_reasons,
            authorization: candidate_snapshot.authorization.clone(),
        })
        .map_err(map_orchestration_error)?;
        Ok(ReadinessRecheck {
            milestone,
            definition,
            candidate_snapshot,
            recomputed,
            evidence,
            task_states,
        })
    }

    async fn source_watermark_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
    ) -> crate::Result<String> {
        let event_id: Option<String> = self
            .db
            .milestone_source_event_watermark_in_tx(tx, project_id)
            .await?;
        Ok(event_id.unwrap_or_else(|| "none".to_owned()))
    }

    pub(super) async fn project_agent_principal_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
    ) -> crate::Result<PrincipalRef> {
        let row = self
            .db
            .active_project_agent_identity_in_tx(tx, project_id)
            .await?
            .ok_or_else(|| crate::ServiceError::not_found("project_agent_binding", project_id))?;
        Ok(PrincipalRef {
            kind: PrincipalKind::Agent,
            id: row.identity_id.clone(),
            display_name: None,
        })
    }

    pub(super) async fn readiness_by_id(
        &self,
        project_id: &str,
        id: &str,
    ) -> crate::Result<Option<ProjectReadinessSnapshotRecord>> {
        Ok(self.db.project_readiness_record(project_id, id).await?)
    }
}
