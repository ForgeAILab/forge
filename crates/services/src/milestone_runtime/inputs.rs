//! Milestone input manifests, source state, and authorization context.

use super::*;

impl MilestoneRuntime {
    pub(super) async fn release_pin_metadata_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
        milestone_id: &str,
        evidence: &[EvidenceAttachment],
    ) -> crate::Result<Vec<ReleasePinMetadata>> {
        if evidence.is_empty() {
            return Ok(Vec::new());
        }
        let rows = self
            .db
            .milestone_release_pin_metadata_in_tx(tx, project_id, milestone_id)
            .await?;
        let metadata: Vec<ReleasePinMetadata> = rows
            .into_iter()
            .map(|row| {
                let attachment_digest: String = row
                    .attachment_digest
                    .clone()
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| crate::ServiceError::InvalidOperation {
                        message: "release evidence attachment is missing its digest".to_owned(),
                    })?;
                let asset_checksum: String = row
                    .asset_checksum
                    .clone()
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| crate::ServiceError::InvalidOperation {
                        message: "release evidence asset is missing its checksum".to_owned(),
                    })?;
                Ok(ReleasePinMetadata {
                    attachment_id: row.id.clone(),
                    asset_id: row.asset_id.clone(),
                    attachment_digest,
                    asset_checksum,
                    task_media_id: row.task_media_id.clone(),
                    stable_project_url: row.project_url.clone(),
                })
            })
            .collect::<crate::Result<Vec<_>>>()?;
        for attachment in evidence {
            let Some(pin) = metadata
                .iter()
                .find(|pin| pin.attachment_id == attachment.id)
            else {
                return Err(crate::ServiceError::InvalidOperation {
                    message: format!(
                        "release evidence attachment {} is missing immutable pin metadata",
                        attachment.id
                    ),
                });
            };
            if pin.asset_id != attachment.asset_id || pin.attachment_digest != attachment.checksum {
                return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
            }
        }
        Ok(metadata)
    }

    pub(super) async fn waiver_ids_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
        milestone_id: &str,
    ) -> crate::Result<Vec<String>> {
        let rows = self
            .db
            .active_milestone_waiver_rows_in_tx(tx, project_id)
            .await?;
        let mut ids = Vec::new();
        for row in rows {
            let affected: String = row.affected_records_json.clone();
            let affected: Value = parse_json_required(&affected, "waiver affected_records")?;
            if affected.get("milestone_id").and_then(Value::as_str) == Some(milestone_id) {
                if let Some(check_id) = affected.get("check_id").and_then(Value::as_str) {
                    ids.push(check_id.to_owned());
                }
            }
        }
        ids.sort();
        ids.dedup();
        Ok(ids)
    }

    pub(super) async fn effective_decision_references_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
        milestone_id: &str,
        charter_revision_id: Option<&str>,
    ) -> crate::Result<Vec<ReleaseDecisionReference>> {
        let rows = self
            .db
            .effective_milestone_decision_rows_in_tx(
                tx,
                project_id,
                milestone_id,
                charter_revision_id,
            )
            .await?;
        rows.into_iter()
            .map(|row| {
                let decision_id: String = row.id.clone();
                let affected: Value = parse_json_required(
                    &row.affected_records_json.clone(),
                    "decision affected_records",
                )?;
                let affected_milestone_id = affected
                    .get("milestone_id")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned);
                let affected_check_id = affected
                    .get("check_id")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned);
                let principal = PrincipalRef {
                    kind: principal_kind(&row.principal_type.clone())?,
                    id: row.principal_id.clone(),
                    display_name: None,
                };
                let authorization = AuthorizationProvenance {
                    principal,
                    authorization_basis: row.authority_basis.clone(),
                    action: row.authorization_action.clone(),
                    event_id: row.explicit_event.clone(),
                    occurred_at: row.authorization_occurred_at.clone(),
                };
                let decision_class: String = row.decision_class.clone();
                let question: String = row.question.clone();
                let context: Value =
                    parse_json_required(&row.context_json.clone(), "decision context")?;
                let options: Value =
                    parse_json_required(&row.options_json.clone(), "decision options")?;
                let selected_outcome: String = row.selected_outcome.clone();
                let rationale: String = row.rationale.clone();
                let state: String = row.state.clone();
                let source_refs: Value =
                    parse_json_required(&row.source_refs_json.clone(), "decision source_refs")?;
                let charter_revision_id: Option<String> = row.charter_revision_id.clone();
                let supersedes_decision_id: Option<String> = row.supersedes_decision_id.clone();
                let created_at: String = row.created_at.clone();
                validate_persisted_authorization_receipt(
                    &authorization,
                    &format!("decision {decision_id}"),
                )?;
                let digest = canonical_digest_with_schema(
                    MILESTONE_READINESS_DIGEST_SCHEMA_VERSION,
                    &json!({
                        "decision_id": decision_id,
                        "state": state,
                        "decision_class": decision_class,
                        "question": question,
                        "context": context,
                        "options": options,
                        "selected_outcome": selected_outcome,
                        "rationale": rationale,
                        "charter_revision_id": charter_revision_id,
                        "source_refs": source_refs,
                        "affected_records": affected,
                        "supersedes_decision_id": supersedes_decision_id,
                        "created_at": created_at,
                        "authorization": authorization,
                    }),
                )
                .map_err(json_error)?;
                let state = match state.as_str() {
                    "active" => api_types::DecisionRecordState::Active,
                    "superseded" => api_types::DecisionRecordState::Superseded,
                    "invalidated" => api_types::DecisionRecordState::Invalidated,
                    other => {
                        return Err(crate::ServiceError::InvalidOperation {
                            message: format!("unknown persisted waiver state {other}"),
                        });
                    }
                };
                Ok(ReleaseDecisionReference {
                    decision_id,
                    state,
                    digest,
                    rationale,
                    authorization,
                    affected_milestone_id,
                    affected_check_id,
                })
            })
            .collect()
    }

    pub(super) async fn repair_primary_pointer_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
        changed_milestone_id: &str,
    ) -> crate::Result<()> {
        let primary: Option<String> = self.db.primary_milestone_id_in_tx(tx, project_id).await?;
        if primary.as_deref() != Some(changed_milestone_id) {
            return Ok(());
        }
        let changed_lifecycle: String = self
            .db
            .milestone_lifecycle_in_tx(tx, changed_milestone_id, project_id)
            .await?;
        // Readiness and release advance the primary outcome; they do not make
        // the Project forget which milestone it delivered. Keep the pointer
        // until a user explicitly emphasizes another milestone. Only a
        // lifecycle that removes the milestone from the Project's intended
        // outcome set should require automatic repair.
        if matches!(
            changed_lifecycle.as_str(),
            "planned" | "active" | "ready_for_release" | "released"
        ) {
            return Ok(());
        }
        let replacement: Option<String> = self
            .db
            .replacement_active_milestone_id_in_tx(tx, project_id, changed_milestone_id)
            .await?;
        self.db
            .repair_primary_milestone_in_tx(
                tx,
                replacement,
                &(now_rfc3339()),
                project_id,
                changed_milestone_id,
            )
            .await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn input_manifest_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
        milestone_id: &str,
        definition_revision_id: &str,
        charter: Option<&ArtifactRef>,
        checks: &[ValidationResult],
        evidence: &[EvidenceAttachment],
        waiver_ids: &[String],
        task_states: &[ReadinessTaskState],
        document_states: &[ReadinessDocumentState],
        commit_build_check_context: &[String],
    ) -> crate::Result<Vec<ReadinessInput>> {
        let milestone_source = self
            .db
            .milestone_readiness_source_in_tx(tx, milestone_id, project_id)
            .await?;
        let definition_source = self
            .db
            .milestone_definition_readiness_source_in_tx(tx, definition_revision_id, milestone_id)
            .await?;
        let milestone_version: i64 = milestone_source.version;
        let milestone_observed_at: String = milestone_source.updated_at.clone();
        if milestone_version <= 0 || milestone_observed_at.trim().is_empty() {
            return Err(crate::ServiceError::InvalidOperation {
                message: "milestone source manifest has invalid version or timestamp".to_owned(),
            });
        }
        let current_definition_revision_id: Option<String> =
            milestone_source.current_definition_revision_id.clone();
        if current_definition_revision_id.as_deref() != Some(definition_revision_id) {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }
        let milestone_digest = canonical_digest_with_schema(
            MILESTONE_READINESS_DIGEST_SCHEMA_VERSION,
            &json!({
                "project_id": project_id,
                "milestone_id": milestone_id,
                "milestone_key": milestone_source.milestone_key.clone(),
                "display_label": milestone_source.display_label.clone(),
                "lifecycle": milestone_source.lifecycle.clone(),
                "current_definition_revision_id": milestone_source.current_definition_revision_id.clone(),
            }),
        )
        .map_err(json_error)?;
        let definition_revision: i64 = definition_source.revision;
        let definition_digest: String = definition_source.content_digest.clone();
        let definition_observed_at: String = definition_source.created_at.clone();
        let definition_source_digest = canonical_digest_with_schema(
            MILESTONE_READINESS_DIGEST_SCHEMA_VERSION,
            &json!({
                "revision": definition_revision,
                "content_digest": definition_digest,
                "rendered_digest": definition_source.rendered_digest.clone(),
                "render_version": definition_source.render_version.clone(),
                "lifecycle": definition_source.lifecycle.clone(),
            }),
        )
        .map_err(json_error)?;
        let mut inputs = vec![
            ReadinessInput {
                source_kind: "milestone".to_owned(),
                source_id: milestone_id.to_owned(),
                source_version: milestone_version,
                source_digest: milestone_digest,
                observed_at: milestone_observed_at,
            },
            ReadinessInput {
                source_kind: "milestone_definition".to_owned(),
                source_id: definition_revision_id.to_owned(),
                source_version: definition_revision,
                source_digest: definition_source_digest,
                observed_at: definition_observed_at.clone(),
            },
        ];
        if let Some(charter) = charter {
            let charter_source = self
                .db
                .charter_readiness_source_in_tx(tx, &charter.revision_id, &charter.artifact_id)
                .await?;
            let charter_digest = canonical_digest_with_schema(
                MILESTONE_READINESS_DIGEST_SCHEMA_VERSION,
                &json!({
                    "artifact_id": charter.artifact_id,
                    "revision_id": charter.revision_id,
                    "content_digest": charter_source.content_digest.clone(),
                    "render_version": charter_source.render_version.clone(),
                    "render_digest": charter_source.rendered_digest.clone(),
                }),
            )
            .map_err(json_error)?;
            inputs.push(ReadinessInput {
                source_kind: "charter".to_owned(),
                source_id: charter.revision_id.clone(),
                source_version: charter_source.revision,
                source_digest: charter_digest,
                observed_at: charter_source.created_at.clone(),
            });
        }
        for result in checks {
            inputs.push(ReadinessInput {
                source_kind: "validation".to_owned(),
                source_id: result.id.clone(),
                source_version: result.expected_version,
                source_digest: result.result_digest.clone(),
                observed_at: result.evaluated_at.clone(),
            });
        }
        for attachment in evidence {
            let authorization: AuthorizationProvenance = parse_json_required(
                &self
                    .db
                    .evidence_authorization_json_in_tx(tx, &attachment.id, project_id)
                    .await?,
                "evidence authorization",
            )?;
            if !same_principal(&authorization.principal, &attachment.author) {
                return Err(crate::ServiceError::InvalidOperation {
                    message: format!(
                        "evidence attachment {} authorization principal disagrees with its author",
                        attachment.id
                    ),
                });
            }
            validate_persisted_authorization(&authorization, EVIDENCE_ATTACH_AUTHORIZATION_ACTION)?;
            inputs.push(ReadinessInput {
                source_kind: "evidence".to_owned(),
                source_id: attachment.id.clone(),
                source_version: attachment.version,
                source_digest: canonical_digest_with_schema(
                    MILESTONE_READINESS_DIGEST_SCHEMA_VERSION,
                    &json!({
                        "attachment": attachment,
                        "authorization": authorization,
                    }),
                )
                .map_err(json_error)?,
                observed_at: attachment.captured_at.clone(),
            });
        }
        for waiver_id in waiver_ids {
            let waiver = self
                .db
                .active_check_waiver_source_in_tx(tx, project_id, milestone_id, waiver_id)
                .await?
                .ok_or_else(|| crate::ServiceError::InvalidOperation {
                    message: format!("active waiver for check {waiver_id} is unavailable"),
                })?;
            let waiver_principal = PrincipalRef {
                kind: principal_kind(&waiver.principal_type.clone())?,
                id: waiver
                    .principal_id
                    .clone()
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| crate::ServiceError::InvalidOperation {
                        message: format!("active waiver {waiver_id} is missing its principal"),
                    })?,
                display_name: None,
            };
            let waiver_authorization = AuthorizationProvenance {
                principal: waiver_principal,
                authorization_basis: waiver.authority_basis.clone(),
                action: waiver.authorization_action.clone(),
                event_id: waiver.explicit_event.clone(),
                occurred_at: waiver.authorization_occurred_at.clone(),
            };
            validate_persisted_authorization_receipt(
                &waiver_authorization,
                &format!("waiver {waiver_id}"),
            )?;
            let waiver_payload = json!({
                "id": waiver.id.clone(),
                "rationale": waiver.rationale.clone(),
                "authorization": waiver_authorization,
                "affected_records": parse_json_required::<Value>(
                    &waiver.affected_records_json.clone(),
                    "waiver affected_records",
                )?,
            });
            inputs.push(ReadinessInput {
                source_kind: "waiver".to_owned(),
                source_id: waiver_id.clone(),
                source_version: 1,
                source_digest: canonical_digest_with_schema(
                    MILESTONE_READINESS_DIGEST_SCHEMA_VERSION,
                    &waiver_payload,
                )
                .map_err(json_error)?,
                observed_at: waiver.created_at.clone(),
            });
        }
        for task in task_states {
            let source_digest = canonical_digest_with_schema(
                MILESTONE_READINESS_DIGEST_SCHEMA_VERSION,
                &json!({
                    "task_id": task.task_id,
                    "version": task.version,
                    "task_type": task.task_type,
                    "state": task.state,
                    "observed_at": task.observed_at,
                }),
            )
            .map_err(json_error)?;
            inputs.push(ReadinessInput {
                source_kind: "task".to_owned(),
                source_id: task.task_id.clone(),
                source_version: task.version,
                source_digest,
                observed_at: task.observed_at.clone(),
            });
        }
        for document in document_states {
            let source_digest = canonical_digest_with_schema(
                MILESTONE_READINESS_DIGEST_SCHEMA_VERSION,
                &json!({
                    "document_id": document.document_id,
                    "revision_id": document.revision_id,
                    "version": document.version,
                    "lifecycle": document.lifecycle,
                    "current_approved": document.current_approved,
                    "content_digest": document.content_digest,
                }),
            )
            .map_err(json_error)?;
            inputs.push(ReadinessInput {
                source_kind: "document".to_owned(),
                source_id: document.revision_id.clone(),
                source_version: document.version,
                source_digest,
                observed_at: document.observed_at.clone(),
            });
        }
        for context in commit_build_check_context {
            let reference: RepositoryContextReference =
                parse_json_required(context, "repository execution/review context")?;
            validate_repository_context_reference(&reference)?;
            inputs.push(ReadinessInput {
                source_kind: "repository_context".to_owned(),
                source_id: reference
                    .execution_id
                    .clone()
                    .unwrap_or_else(|| format!("task:{}", reference.task_id)),
                source_version: reference.task_version,
                source_digest: canonical_digest_with_schema(
                    MILESTONE_READINESS_DIGEST_SCHEMA_VERSION,
                    &reference,
                )
                .map_err(json_error)?,
                observed_at: reference.observed_at,
            });
        }
        Ok(inputs)
    }

    pub(super) async fn source_states_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
        milestone_id: &str,
        definition: &MilestoneDefinitionRevision,
    ) -> crate::Result<(Vec<ReadinessTaskState>, Vec<ReadinessDocumentState>)> {
        // A milestone gates on every Task bound to it, not only the ones its
        // definition happens to list: governance binds Tasks to milestones at
        // creation, and a definition authored before (or without) that Task
        // selection must not compute "ready" while governed work is open.
        // Cancelled/archived/deleted governed Tasks no longer gate.
        let governed_task_ids: Vec<String> = self
            .db
            .governed_milestone_task_ids_in_tx(tx, milestone_id, project_id)
            .await?;
        let mut selected_task_ids: Vec<&String> = definition.content.task_ids.iter().collect();
        for task_id in &governed_task_ids {
            if !selected_task_ids.contains(&task_id) {
                selected_task_ids.push(task_id);
            }
        }
        let mut tasks = Vec::with_capacity(selected_task_ids.len());
        for task_id in selected_task_ids {
            let row = self
                .db
                .milestone_task_source_in_tx(tx, task_id, project_id)
                .await?
                .ok_or_else(|| crate::ServiceError::InvalidOperation {
                    message: format!("milestone references missing Task {task_id}"),
                })?;
            let deleted_at: Option<String> = row.deleted_at.clone();
            let version: i64 = row.version;
            let task_type: String = row.task_type.clone();
            let task_status: String = row.status.clone();
            let observed_at: String = row.updated_at.clone();
            if version <= 0
                || task_type.trim().is_empty()
                || task_status.trim().is_empty()
                || observed_at.trim().is_empty()
            {
                return Err(crate::ServiceError::InvalidOperation {
                    message: format!("selected Task {task_id} has incomplete source metadata"),
                });
            }
            tasks.push(ReadinessTaskState {
                task_id: task_id.clone(),
                version,
                task_type,
                state: if deleted_at.is_some() {
                    "deleted".to_owned()
                } else {
                    task_status
                },
                observed_at,
            });
        }

        let mut documents = Vec::with_capacity(definition.content.document_revisions.len());
        for document in &definition.content.document_revisions {
            let row = self
                .db
                .milestone_document_source_in_tx(
                    tx,
                    &document.revision_id,
                    &document.artifact_id,
                    project_id,
                )
                .await?
                .ok_or_else(|| crate::ServiceError::InvalidOperation {
                    message: format!(
                        "milestone references missing Document revision {}",
                        document.revision_id
                    ),
                })?;
            let current_approved_revision_id: Option<String> =
                row.current_approved_revision_id.clone();
            let content_digest: String = row.content_digest.clone();
            if document.content_digest.trim().is_empty()
                || document.content_digest != content_digest
            {
                return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
            }
            let version: i64 = row.version;
            let lifecycle: String = row.lifecycle.clone();
            let observed_at: String = row.updated_at.clone();
            if version <= 0 || lifecycle.trim().is_empty() || observed_at.trim().is_empty() {
                return Err(crate::ServiceError::InvalidOperation {
                    message: format!(
                        "Document revision {} has incomplete source metadata",
                        document.revision_id
                    ),
                });
            }
            documents.push(ReadinessDocumentState {
                document_id: document.artifact_id.clone(),
                revision_id: row.revision_id.clone(),
                version,
                lifecycle,
                current_approved: current_approved_revision_id.as_deref()
                    == Some(document.revision_id.as_str()),
                content_digest,
                observed_at,
            });
        }
        tasks.sort_by(|left, right| left.task_id.cmp(&right.task_id));
        documents.sort_by(|left, right| {
            (left.document_id.as_str(), left.revision_id.as_str())
                .cmp(&(right.document_id.as_str(), right.revision_id.as_str()))
        });
        Ok((tasks, documents))
    }

    pub(super) async fn commit_build_check_context_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
        tasks: &[ReadinessTaskState],
    ) -> crate::Result<Vec<String>> {
        let mut context = Vec::new();
        for task in tasks {
            let rows = self
                .db
                .milestone_repository_context_in_tx(tx, &task.task_id, project_id)
                .await?;
            let row = rows.ok_or_else(|| crate::ServiceError::InvalidOperation {
                message: format!("selected Task {} has no repository record", task.task_id),
            })?;
            let reference = RepositoryContextReference {
                task_id: task.task_id.clone(),
                task_version: task.version,
                repository_id: row.repo_id.clone(),
                repository_name: row.repository_name.clone(),
                repository_kind: row.repository_kind.clone(),
                remote_url: db::normalize_repo_remote_url(row.remote_url.clone()),
                default_branch: row.default_branch.clone(),
                execution_id: row.execution_id.clone(),
                execution_status: row.execution_status.clone(),
                execution_role: row.execution_role.clone(),
                before_sha: row.before_sha.clone(),
                after_sha: row.after_sha.clone(),
                execution_summary: row.execution_summary.clone(),
                execution_created_at: row.execution_created_at.clone(),
                execution_updated_at: row.execution_updated_at.clone(),
                review_id: row.review_id.clone(),
                review_status: row.review_status.clone(),
                ci_results: row.ci_results.clone(),
                audit_result: row.audit_result.clone(),
                human_decision: row.human_decision.clone(),
                review_created_at: row.review_created_at.clone(),
                review_updated_at: row.review_updated_at.clone(),
                observed_at: task.observed_at.clone(),
            };
            validate_repository_context_reference(&reference).map_err(|error| {
                crate::ServiceError::InvalidOperation {
                    message: format!(
                        "selected Task {} has incomplete repository context: {error}",
                        task.task_id
                    ),
                }
            })?;
            context.push(api_types::canonical_json(&reference).map_err(json_error)?);
        }
        context.sort();
        Ok(context)
    }
}
