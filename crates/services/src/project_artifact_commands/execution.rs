//! Project evidence, validation, and document command execution.

use super::*;

impl ProjectArtifactCommandService {
    #[must_use]
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self { db }
    }

    /// Attach one Project evidence reference.  The domain mutation, event,
    /// command receipt, and optional action execution are finalized by the DB
    /// composite in one SQLite transaction.
    pub async fn attach_evidence(
        &self,
        command: ProjectEvidenceCommand,
        action: Option<AgentActionProvenance>,
    ) -> Result<ProjectMediaAttachment> {
        validate_evidence_command(&command)?;
        if command.expected_milestone_version < 1 {
            return Err(ServiceError::invalid_operation(
                "expected_milestone_version must be positive for an authenticated user command",
            ));
        }
        let context = command_context(&command, action)?;
        self.attach_evidence_with_context(command, context).await
    }

    /// Record one agent-observed acceptance-check result. `manual` checks are
    /// deliberately unreachable here: a human attestation is only ever a
    /// human's, and readiness enforces that separately when it reads the row
    /// back. Everything else about the row -- the governing Charter and
    /// baseline revisions, the definition revision, the check version -- is
    /// derived here rather than accepted from the caller.
    pub async fn record_validation_with_context(
        &self,
        command: ProjectValidationCommand,
        context: CommandContext,
    ) -> Result<db::ProjectMilestoneCheckResultRecord> {
        if context.operation() != PROJECT_VALIDATION_COMMAND
            || context.canonical_scope().scope_type() != CommandScopeType::Project
            || context.canonical_scope().scope_id() != command.project_id
            || context.principal().principal_type() != command.authorization.principal_type
            || context.principal().principal_id() != command.authorization.principal_id
        {
            return Err(ServiceError::invalid_operation(
                "validation command context does not match its Project authorization",
            ));
        }
        if command.result.trim().is_empty() || command.input_digest.trim().is_empty() {
            return Err(ServiceError::invalid_operation(
                "a validation result requires an observation and an input digest",
            ));
        }
        // The agent speaks the same acceptance-check vocabulary the user-facing
        // manual attestation route speaks; the persisted outcome column has its
        // own narrower vocabulary. Translating here is what makes a recorded
        // observation land at all -- an untranslated `pass` fails the outcome
        // CHECK constraint instead of settling the check.
        let outcome = validation_outcome(&command.status).ok_or_else(|| {
            ServiceError::invalid_operation(
                "validation status must be pass, fail, blocked, stale, or unavailable",
            )
        })?;
        authorize_project_principal(&self.db, &command.project_id, &command.authorization).await?;

        // A cited Task must be one that really ran. The citation itself is
        // optional -- the Project Agent can exercise the software in its own
        // workspace -- but a named Task is always verified, so provenance is
        // either genuinely absent or genuinely true, never merely asserted.
        if let Some(observed_task_id) = command.observed_task_id.as_deref() {
            let observed_task_status: Option<String> = self
                .db
                .validation_observed_task_status(observed_task_id, &command.project_id)
                .await?;
            let observed_task_status = observed_task_status.ok_or_else(|| {
                ServiceError::invalid_operation(
                    "observed_task_id must name a Task in this Project that produced the observation",
                )
            })?;
            let project = ProjectRepo::get_by_id(&*self.db, &command.project_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("project", command.project_id.clone()))?;
            let workflow = crate::workflow::engine::WorkflowEngine::resolve_workflow(
                &project.workflow_definition,
            );
            if !matches!(observed_task_status.as_str(), "done" | "merging")
                && !crate::workflow::is_review_state(&workflow, &observed_task_status)
            {
                return Err(ServiceError::invalid_operation(
                    "the observed Task has not produced a delivered result to validate against",
                ));
            }
        }
        if let Some(asset_id) = command.evidence_asset_id.as_deref() {
            let asset_belongs: Option<i64> = self
                .db
                .validation_evidence_asset_in_project(asset_id, &command.project_id)
                .await?;
            if asset_belongs.is_none() {
                return Err(ServiceError::invalid_operation(
                    "evidence_asset_id must name a captured artifact in this Project",
                ));
            }
        }

        let milestone =
            ProjectOrchestrationRepo::get_project_milestone(&*self.db, &command.milestone_id)
                .await?
                .ok_or_else(|| {
                    ServiceError::not_found("project_milestone", &command.milestone_id)
                })?;
        if milestone.project_id != command.project_id {
            return Err(ServiceError::NotFound {
                entity: "milestone",
                id: command.milestone_id,
            });
        }
        if milestone.version != command.expected_milestone_version {
            return Err(ServiceError::Db(db::DbError::VersionConflict));
        }
        let current_definition_revision_id = milestone
            .current_definition_revision_id
            .clone()
            .ok_or_else(|| {
                ServiceError::invalid_operation("the milestone has no current definition revision")
            })?;
        if command.definition_revision_id != current_definition_revision_id {
            return Err(ServiceError::conflict(
                "the acceptance check belongs to a superseded milestone definition revision",
            ));
        }

        let check = self
            .db
            .validation_acceptance_check_source(
                &command.check_id,
                &command.project_id,
                &command.milestone_id,
                &current_definition_revision_id,
            )
            .await?
            .ok_or_else(|| ServiceError::not_found("project_milestone_check", &command.check_id))?;
        let source_kind: String = check.source_kind()?;
        if source_kind == "manual" {
            return Err(ServiceError::invalid_operation(
                "a manual acceptance check is attested by a user, not recorded by an agent",
            ));
        }
        let expected_check_version: i64 = check.version()?;
        let command_observations = self
            .verified_command_observations(&command, &current_definition_revision_id, outcome)
            .await?;

        // The approved Charter is the authority this observation binds to.
        // Readiness re-derives it when it reads the result, so deriving it
        // here keeps a stale caller from binding a result to authority that
        // has already moved.
        let governance = self
            .db
            .validation_project_charter_source(&command.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", &command.project_id))?;
        let governing_charter_revision_id: String = governance
            .current_charter_revision_id()?
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                ServiceError::conflict(
                    "recording a validation result requires the current approved Project Charter",
                )
            })?;
        let result_id = new_uuid_v4();
        let now = now_rfc3339();
        let source_manifest = validation_source_manifest(ValidationManifestInputs {
            result: &command.result,
            governing_charter_revision_id: &governing_charter_revision_id,
            check_definition_revision_id: &current_definition_revision_id,
            principal_type: &command.authorization.principal_type,
            principal_id: &command.authorization.principal_id,
            observed_task_id: command.observed_task_id.as_deref(),
            evidence_asset_id: command.evidence_asset_id.as_deref(),
            command_observations: &command_observations,
        });
        let result_json = json!({
            "operation": PROJECT_VALIDATION_COMMAND,
            "project_id": command.project_id,
            "milestone_id": command.milestone_id,
            "check_id": command.check_id,
            "result_id": result_id,
            "outcome": outcome,
            "domain_committed": true,
        })
        .to_string();
        let (mut receipt, execution) = validation_command_bundle(&context, &result_json);
        if let Some(execution) = execution.as_ref() {
            receipt.agent_action_execution_id = Some(execution.id.clone());
        }
        ProjectOrchestrationRepo::append_project_milestone_check_result(
            &*self.db,
            db::AppendProjectMilestoneCheckResultCommand {
                result: db::CreateProjectMilestoneCheckResult {
                    id: result_id,
                    project_id: command.project_id.clone(),
                    milestone_id: command.milestone_id.clone(),
                    check_id: command.check_id.clone(),
                    definition_revision_id: current_definition_revision_id,
                    outcome: outcome.to_owned(),
                    source_kind,
                    source_manifest_json: source_manifest.to_string(),
                    input_digest: command.input_digest.clone(),
                    governing_charter_revision_id: Some(governing_charter_revision_id),
                    principal_type: command.authorization.principal_type.clone(),
                    principal_id: command.authorization.principal_id.clone(),
                    authorization_basis: command.authorization.authorization_basis.clone(),
                    authorization_action: command.authorization.authorization_action.clone(),
                    authorization_occurred_at: command
                        .authorization
                        .authorization_occurred_at
                        .clone(),
                    expected_version: expected_check_version,
                    explicit_event: command.authorization.authorization_event_id.clone(),
                    idempotency_key: command.idempotency_key.clone(),
                    created_at: now,
                },
                command_receipt: Some(receipt),
                action_execution: execution,
            },
        )
        .await
        .map_err(Into::into)
    }

    /// Native Project Agent execution supplies the already-authorized command
    /// context built from its admitted AgentAction.  Keeping this entry point
    /// separate prevents the service from reconstructing or weakening that
    /// provenance.
    /// The observations a `task_validation` result stands on. An Agent's
    /// pass or fail must cite commands it ran itself, in its own checkout,
    /// after the delivered work landed: a Task's worklog or a reviewer's
    /// report is narration about someone else's run and settles nothing.
    /// Blocked, stale, and unavailable results carry no observation because
    /// they claim none; a user attestation never reaches this path.
    async fn verified_command_observations(
        &self,
        command: &ProjectValidationCommand,
        definition_revision_id: &str,
        outcome: &str,
    ) -> Result<Vec<Value>> {
        let claims_observation = matches!(outcome, "passed" | "failed");
        if command.authorization.principal_type != "agent" || !claims_observation {
            return Ok(Vec::new());
        }
        if command.observed_command_ids.is_empty() {
            return Err(ServiceError::invalid_operation(
                "a task_validation pass or fail must cite observed_command_ids: run the delivered \
                 software in your checkout with forge_task_command and cite the observation_id \
                 each call returns; a Task's or reviewer's report cannot settle the check",
            ));
        }
        // Verification must postdate the work it verifies: the newest delivered
        // Task bound to this milestone, or the definition itself when no Task
        // has landed yet.
        let delivered_at: Option<String> = self
            .db
            .validation_latest_delivered_task_at(&command.milestone_id, &command.project_id)
            .await?;
        let watermark = match delivered_at {
            Some(delivered_at) => delivered_at,
            None => {
                self.db
                    .validation_milestone_definition_created_at(definition_revision_id)
                    .await?
            }
        };
        let mut observations = Vec::with_capacity(command.observed_command_ids.len());
        for observation_id in &command.observed_command_ids {
            let row = self
                .db
                .validation_command_observation_source(
                    observation_id,
                    &command.project_id,
                    &command.authorization.principal_id,
                )
                .await?
                .ok_or_else(|| {
                    ServiceError::invalid_operation(format!(
                        "observed_command_id {observation_id} is not a command this Agent ran in \
                     this Project; cite the observation_id forge_task_command returned"
                    ))
                })?;
            let created_at: String = row.created_at()?;
            if created_at <= watermark {
                return Err(ServiceError::invalid_operation(format!(
                    "observed_command_id {observation_id} predates the delivered work; run the \
                     delivered software again and cite the new observation"
                )));
            }
            let args: Value =
                serde_json::from_str(&row.args_json()?).unwrap_or(Value::Array(Vec::new()));
            observations.push(json!({
                "id": observation_id,
                "program": row.program()?,
                "args": args,
                "exit_code": row.exit_code()?,
                "success": row.success()? == 1,
                "output_digest": row.output_digest()?,
                "recorded_at": created_at,
            }));
        }
        Ok(observations)
    }

    pub async fn attach_evidence_with_context(
        &self,
        command: ProjectEvidenceCommand,
        context: CommandContext,
    ) -> Result<ProjectMediaAttachment> {
        validate_evidence_command(&command)?;
        if context.operation() != PROJECT_EVIDENCE_COMMAND
            || context.canonical_scope().scope_type() != CommandScopeType::Project
            || context.canonical_scope().scope_id() != command.project_id
            || context.principal().principal_type() != command.authorization.principal_type
            || context.principal().principal_id() != command.authorization.principal_id
        {
            return Err(ServiceError::invalid_operation(
                "evidence command context does not match its Project authorization",
            ));
        }

        // Resolve the receipt before membership, mutable asset/milestone
        // checks, or server identity allocation. A response-loss retry must
        // return the original attachment even after its current projections
        // have changed.
        if let Some(existing) = self.replay_evidence(&context).await? {
            return Ok(existing);
        }
        authorize_project_principal(&self.db, &command.project_id, &command.authorization).await?;

        let milestone =
            ProjectOrchestrationRepo::get_project_milestone(&*self.db, &command.milestone_id)
                .await?
                .ok_or_else(|| {
                    ServiceError::not_found("project_milestone", &command.milestone_id)
                })?;
        if milestone.project_id != command.project_id {
            // Treat a milestone belonging to another Project as absent from
            // this scope. This keeps the public route from revealing that a
            // caller-supplied milestone exists elsewhere while retaining the
            // service's strict Project-scope rejection.
            return Err(ServiceError::NotFound {
                entity: "milestone",
                id: command.milestone_id,
            });
        }

        let asset = SharedMediaRepo::get_media_asset(&*self.db, &command.asset_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("media_asset", &command.asset_id))?;
        if asset.project_id != command.project_id
            || asset.deleted_at.is_some()
            || asset.availability != "available"
        {
            return Err(ServiceError::conflict(
                "evidence media is unavailable or cross-Project",
            ));
        }
        if asset.checksum.as_deref() != Some(command.checksum.as_str()) {
            return Err(ServiceError::conflict(
                "evidence checksum does not match the media asset",
            ));
        }

        let expected_milestone_version = command.expected_milestone_version;
        let attachment_id = new_uuid_v4();
        let now = now_rfc3339();
        let result = json!({
            "operation": PROJECT_EVIDENCE_COMMAND,
            "project_id": command.project_id,
            "milestone_id": command.milestone_id,
            "attachment_id": attachment_id,
            "asset_id": command.asset_id,
            "domain_committed": true,
        });
        let result_json = serde_json::to_string(&result).map_err(|error| {
            ServiceError::invalid_operation(format!("serialize evidence result: {error}"))
        })?;
        let attachment = CreateProjectMediaAttachment {
            id: attachment_id,
            project_id: command.project_id.clone(),
            asset_id: command.asset_id.clone(),
            attachment_kind: "evidence".to_owned(),
            task_media_id: None,
            task_id: command.task_id.clone(),
            milestone_id: Some(command.milestone_id.clone()),
            milestone_check_id: None,
            source_task_id: command.task_id.clone(),
            source_execution_id: command.source_run_id.clone(),
            source_validation_id: command.source_validation_id.clone(),
            source_task_version: None,
            source_context_digest: None,
            source_definition_revision_id: None,
            acceptance_check_ids_json: serde_json::to_string(&command.acceptance_check_ids)
                .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
            caption: Some(command.caption.clone()),
            evidence_kind: Some(command.evidence_kind.clone()),
            checksum: Some(command.checksum.clone()),
            availability: "available".to_owned(),
            project_url: Some(format!(
                "/api/v1/projects/{}/media/{}",
                command.project_id, command.asset_id
            )),
            author_type: command.authorization.principal_type.clone(),
            author_id: Some(command.authorization.principal_id.clone()),
            authorization_json: command.authorization.authorization_json.clone(),
            created_at: now,
        };
        let receipt = create_receipt(&context, &result_json);
        let execution = context.action_provenance.as_ref().map(|provenance| {
            let committed_at = now_rfc3339();
            CreateAgentActionExecution {
                id: new_uuid_v4(),
                action_id: provenance.action_id.clone(),
                expected_action_version: provenance.expected_action_version,
                attempt: provenance.attempt,
                status: AgentActionExecutionStatus::Succeeded,
                result_json: Some(result_json.clone()),
                error: None,
                executed_by_type: provenance.executed_by_type.clone(),
                executed_by_id: provenance.executed_by_id.clone(),
                idempotency_key: provenance.execution_idempotency_key.clone(),
                action_status: AgentActionStatus::Executed,
                action_outcome_json: Some(result_json.clone()),
                created_at: committed_at.clone(),
                completed_at: Some(committed_at.clone()),
                updated_at: committed_at,
            }
        });
        let mut receipt = receipt;
        if let Some(execution) = execution.as_ref() {
            receipt.agent_action_execution_id = Some(execution.id.clone());
        }
        SharedMediaRepo::create_project_media_attachment_mutation(
            &*self.db,
            CreateProjectMediaAttachmentMutation {
                attachment,
                expected_milestone_version,
                idempotency_key: command.idempotency_key,
                mutation_fingerprint: context.input_digest().to_owned(),
                authorization_event_id: command.authorization.authorization_event_id,
                command_receipt: Some(receipt),
                action_execution: execution,
            },
        )
        .await
        .map_err(Into::into)
    }

    /// Create the typed Project Document shell through the command-aware
    /// repository method.  REST and native execution both use this boundary;
    /// the first revision remains a separate command because the public REST
    /// contract creates the shell before content is submitted.
    pub async fn create_document(
        &self,
        command: ProjectDocumentCreateCommand,
        action: Option<AgentActionProvenance>,
    ) -> Result<ProjectDocumentRecord> {
        validate_document_create_command(&command)?;
        let context = command_context_for(
            "project.document.create",
            &command.idempotency_key,
            &command.authorization,
            &command,
            action,
            &command.project_id,
            command.expected_project_version,
            "expected_project_version",
        )?;
        self.create_document_with_context(command, context).await
    }

    pub(crate) async fn create_document_with_context(
        &self,
        command: ProjectDocumentCreateCommand,
        context: CommandContext,
    ) -> Result<ProjectDocumentRecord> {
        validate_document_create_command(&command)?;
        validate_document_context(&context, &command.project_id, &command.authorization)?;
        if let Some(document) = self.replay_document_shell(&context).await? {
            return Ok(document);
        }
        authorize_project_principal(&self.db, &command.project_id, &command.authorization).await?;
        let document_id = new_uuid_v4();
        let now = now_rfc3339();
        let result_json = json!({
            "operation": "project.document.create",
            "project_id": command.project_id,
            "document_id": document_id,
            "kind": command.kind,
            "title": command.title,
            "approval_policy": command.approval_policy,
            "expected_project_version": command.expected_project_version,
            "lifecycle": "draft",
            "current_draft_revision_id": Value::Null,
            "current_approved_revision_id": Value::Null,
            "version": 1,
            "created_at": now.clone(),
            "updated_at": now.clone(),
            "domain_committed": true,
        })
        .to_string();
        let (mut receipt, execution) = command_bundle(&context, &result_json);
        if let Some(execution) = execution.as_ref() {
            receipt.agent_action_execution_id = Some(execution.id.clone());
        }
        let document = ProjectOrchestrationRepo::create_project_document_shell_command(
            &*self.db,
            CreateProjectDocumentShellCommand {
                document: CreateProjectDocument {
                    id: result_value(&result_json, "document_id")?,
                    project_id: command.project_id,
                    kind: command.kind,
                    title: command.title,
                    approval_policy: command.approval_policy,
                    created_at: now.clone(),
                    updated_at: now,
                },
                expected_project_version: command.expected_project_version,
                command_receipt: Some(receipt),
                action_execution: execution,
            },
        )
        .await?;
        if document.id != result_value(&result_json, "document_id")? || document.version != 1 {
            return Err(ServiceError::Conflict(
                "Project Document command returned a non-canonical shell".to_owned(),
            ));
        }
        Ok(document)
    }

    /// Append one typed Project Document revision through the command-aware
    /// repository method.  The adapter supplies only typed content and
    /// authorization provenance; scope, base revision, and the frozen result
    /// are owned here.
    pub async fn save_document_revision(
        &self,
        command: ProjectDocumentRevisionCommand,
        action: Option<AgentActionProvenance>,
    ) -> Result<ProjectDocumentRevisionRecord> {
        validate_document_revision_command(&command)?;
        let context = command_context_for(
            "project.document",
            &command.idempotency_key,
            &command.authorization,
            &command,
            action,
            &command.project_id,
            command.expected_document_version,
            "expected_document_version",
        )?;
        self.save_document_revision_with_context(command, context)
            .await
    }

    pub async fn save_document_revision_with_context(
        &self,
        command: ProjectDocumentRevisionCommand,
        context: CommandContext,
    ) -> Result<ProjectDocumentRevisionRecord> {
        validate_document_revision_command(&command)?;
        validate_document_context(&context, &command.project_id, &command.authorization)?;
        if let Some(revision) = self.replay_document_revision(&context).await? {
            return Ok(revision);
        }
        authorize_project_principal(&self.db, &command.project_id, &command.authorization).await?;
        if command.provenance.author.id != command.authorization.principal_id {
            return Err(ServiceError::AuthorizationDenied {
                message: "Document revision provenance principal does not match command principal"
                    .to_owned(),
            });
        }

        let existing =
            ProjectOrchestrationRepo::get_project_document(&*self.db, &command.document_id).await?;
        let Some(document) = existing else {
            return self.create_first_document_revision(command, context).await;
        };
        validate_document_scope(&document, &command.project_id)?;
        if let Some(kind) = command.kind.as_deref() {
            if kind != document.kind {
                return Err(ServiceError::conflict(
                    "Project Document kind does not match the existing Document",
                ));
            }
        }
        if let Some(title) = command.title.as_deref() {
            if title != document.title {
                return Err(ServiceError::conflict(
                    "Project Document title does not match the existing Document",
                ));
            }
        }
        let current_draft = match document.current_draft_revision_id.as_deref() {
            Some(id) => Some(
                ProjectOrchestrationRepo::get_project_document_revision(&*self.db, id)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("project_document_revision", id))?,
            ),
            None => None,
        };
        if let Some(expected_digest) = command.expected_digest.as_deref() {
            if current_draft
                .as_ref()
                .map(|revision| revision.content_digest.as_str())
                != Some(expected_digest)
            {
                return Err(ServiceError::conflict(
                    "the current draft digest changed before this revision was saved",
                ));
            }
        }
        let (base_revision, base_revision_id) = if let Some(base_id) =
            command.base_revision_id.clone()
        {
            let base = ProjectOrchestrationRepo::get_project_document_revision(&*self.db, &base_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("project_document_revision", &base_id))?;
            if base.document_id != command.document_id
                || document.current_draft_revision_id.as_deref() != Some(base_id.as_str())
            {
                return Err(ServiceError::Db(db::DbError::VersionConflict));
            }
            (base.revision, Some(base_id))
        } else {
            if current_draft.is_some() {
                return Err(ServiceError::Db(db::DbError::VersionConflict));
            }
            (0, None)
        };
        let kind = crate::parse_document_kind(&document.kind).ok_or_else(|| {
            ServiceError::invalid_operation("persisted Project Document kind is invalid")
        })?;
        let rendered_view = crate::render_project_document(&document.title, kind, &command.content);
        let content_json = crate::render_project_document_json(&command.content);
        let effective_lifecycle = effective_document_lifecycle(&document, command.lifecycle)?;
        let revision_number: i64 = self
            .db
            .artifact_next_document_revision(&command.document_id)
            .await?;
        let revision_id = new_uuid_v4();
        let created_at = now_rfc3339();
        let result_json = json!({
            "operation": "project.document",
            "project_id": command.project_id,
            "document_id": command.document_id,
            "revision_id": revision_id,
            "revision": revision_number,
            "lifecycle": effective_lifecycle,
            "domain_committed": true,
            "requires_user_authorization": effective_lifecycle == "proposed",
        })
        .to_string();
        let (mut receipt, execution) = command_bundle(&context, &result_json);
        if let Some(execution) = execution.as_ref() {
            receipt.agent_action_execution_id = Some(execution.id.clone());
        }
        let revision = ProjectOrchestrationRepo::append_project_document_revision_command(
            &*self.db,
            AppendProjectDocumentRevisionCommand {
                revision: CreateProjectDocumentRevision {
                    id: revision_id,
                    document_id: command.document_id,
                    expected_document_version: command.expected_document_version,
                    base_revision,
                    base_revision_id,
                    lifecycle: effective_lifecycle.to_owned(),
                    schema_version: crate::PROJECT_DOCUMENT_SCHEMA_VERSION.to_owned(),
                    render_version: crate::PROJECT_DOCUMENT_RENDER_VERSION.to_owned(),
                    content_json,
                    rendered_view: rendered_view.clone(),
                    change_summary: command.change_summary,
                    author_type: command.authorization.principal_type.clone(),
                    author_id: Some(command.authorization.principal_id.clone()),
                    source_refs_json: serde_json::to_string(&command.provenance.source_refs)
                        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
                    content_digest: crate::document_content_digest(&command.content),
                    rendered_digest: crate::document_render_digest(
                        crate::PROJECT_DOCUMENT_RENDER_VERSION,
                        &rendered_view,
                    ),
                    created_at,
                },
                command_receipt: Some(receipt),
                action_execution: execution,
            },
        )
        .await?;
        if revision.revision != revision_number {
            return Err(ServiceError::Conflict(
                "Project Document command returned a non-canonical revision".to_owned(),
            ));
        }
        Ok(revision)
    }

    async fn create_first_document_revision(
        &self,
        command: ProjectDocumentRevisionCommand,
        context: CommandContext,
    ) -> Result<ProjectDocumentRevisionRecord> {
        let kind_text = command.kind.clone().ok_or_else(|| {
            ServiceError::invalid_operation("first Project Document revision kind is required")
        })?;
        let title = command.title.clone().ok_or_else(|| {
            ServiceError::invalid_operation("first Project Document revision title is required")
        })?;
        let approval_policy = command
            .approval_policy
            .clone()
            .unwrap_or_else(|| "user_or_project_agent".to_owned());
        if command.expected_document_version != 1
            || command.base_revision_id.is_some()
            || command.lifecycle != DocumentRevisionLifecycle::Draft
        {
            return Err(ServiceError::Db(db::DbError::VersionConflict));
        }
        crate::parse_document_kind(&kind_text)
            .ok_or_else(|| ServiceError::invalid_operation("Project Document kind is invalid"))?;
        if title.trim().is_empty() || approval_policy.trim().is_empty() {
            return Err(ServiceError::invalid_operation(
                "first Project Document identity is incomplete",
            ));
        }
        let document_id = new_uuid_v4();
        let revision_id = new_uuid_v4();
        let kind = crate::parse_document_kind(&kind_text)
            .ok_or_else(|| ServiceError::invalid_operation("Project Document kind is invalid"))?;
        let rendered_view = crate::render_project_document(&title, kind, &command.content);
        let now = now_rfc3339();
        let result_json = json!({
            "operation": "project.document",
            "project_id": command.project_id,
            "document_id": document_id,
            "revision_id": revision_id,
            "revision": 1,
            "lifecycle": "draft",
            "domain_committed": true,
            "requires_user_authorization": false,
        })
        .to_string();
        let (mut receipt, execution) = command_bundle(&context, &result_json);
        if let Some(execution) = execution.as_ref() {
            receipt.agent_action_execution_id = Some(execution.id.clone());
        }
        let revision = ProjectOrchestrationRepo::create_project_document_command(
            &*self.db,
            CreateProjectDocumentCommand {
                document: CreateProjectDocument {
                    id: result_value(&result_json, "document_id")?,
                    project_id: command.project_id,
                    kind: kind_text,
                    title,
                    approval_policy,
                    created_at: now.clone(),
                    updated_at: now.clone(),
                },
                revision: CreateProjectDocumentRevision {
                    id: result_value(&result_json, "revision_id")?,
                    document_id: result_value(&result_json, "document_id")?,
                    expected_document_version: 1,
                    base_revision: 0,
                    base_revision_id: None,
                    lifecycle: "draft".to_owned(),
                    schema_version: crate::PROJECT_DOCUMENT_SCHEMA_VERSION.to_owned(),
                    render_version: crate::PROJECT_DOCUMENT_RENDER_VERSION.to_owned(),
                    content_json: crate::render_project_document_json(&command.content),
                    rendered_view: rendered_view.clone(),
                    change_summary: command.change_summary,
                    author_type: command.authorization.principal_type.clone(),
                    author_id: Some(command.authorization.principal_id.clone()),
                    source_refs_json: serde_json::to_string(&command.provenance.source_refs)
                        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
                    content_digest: crate::document_content_digest(&command.content),
                    rendered_digest: crate::document_render_digest(
                        crate::PROJECT_DOCUMENT_RENDER_VERSION,
                        &rendered_view,
                    ),
                    created_at: now,
                },
                command_receipt: Some(receipt),
                action_execution: execution,
            },
        )
        .await?;
        if revision.id != result_value(&result_json, "revision_id")? || revision.revision != 1 {
            return Err(ServiceError::Conflict(
                "Project Document command returned a non-canonical first revision".to_owned(),
            ));
        }
        Ok(revision)
    }

    /// Approve one exact current Document revision through the shared command
    /// repository method.  No current revision or digest is inferred.
    pub async fn approve_document(
        &self,
        command: ProjectDocumentApprovalCommand,
        action: Option<AgentActionProvenance>,
    ) -> Result<ProjectDocumentApprovalRecord> {
        let (approval, _) = self.approve_document_with_status(command, action).await?;
        Ok(approval)
    }

    /// Approve one exact current Document revision and report whether the
    /// result came from an existing immutable command receipt.  HTTP adapters
    /// use this distinction to preserve the created-vs-replayed status while
    /// all transports continue to share the same receipt-first command path.
    pub async fn approve_document_with_status(
        &self,
        command: ProjectDocumentApprovalCommand,
        action: Option<AgentActionProvenance>,
    ) -> Result<(ProjectDocumentApprovalRecord, bool)> {
        validate_document_approval_command(&command)?;
        let context = command_context_for(
            "project.document",
            &command.idempotency_key,
            &command.authorization,
            &command,
            action,
            &command.project_id,
            command.expected_document_version,
            "expected_document_version",
        )?;
        self.approve_document_with_context_with_status(command, context)
            .await
    }

    /// Resolve an approval receipt before adapter-level authorization checks.
    /// A replay must compare the complete frozen command identity first, so a
    /// changed authorization envelope receives `idempotency_conflict` instead
    /// of being treated as a new unauthorized request.
    pub async fn replay_document_approval_if_present(
        &self,
        command: &ProjectDocumentApprovalCommand,
    ) -> Result<Option<ProjectDocumentApprovalRecord>> {
        let context = command_context_for_unvalidated(
            "project.document",
            &command.idempotency_key,
            &command.authorization,
            command,
            None,
            &command.project_id,
            command.expected_document_version,
            "expected_document_version",
        )?;
        self.replay_document_approval(&context).await
    }

    pub async fn approve_document_with_context(
        &self,
        command: ProjectDocumentApprovalCommand,
        context: CommandContext,
    ) -> Result<ProjectDocumentApprovalRecord> {
        let (approval, _) = self
            .approve_document_with_context_with_status(command, context)
            .await?;
        Ok(approval)
    }

    pub(crate) async fn approve_document_with_context_with_status(
        &self,
        command: ProjectDocumentApprovalCommand,
        context: CommandContext,
    ) -> Result<(ProjectDocumentApprovalRecord, bool)> {
        validate_document_approval_command(&command)?;
        validate_document_context(&context, &command.project_id, &command.authorization)?;
        if let Some(approval) = self.replay_document_approval(&context).await? {
            return Ok((approval, true));
        }
        authorize_project_principal(&self.db, &command.project_id, &command.authorization).await?;
        let document =
            ProjectOrchestrationRepo::get_project_document(&*self.db, &command.document_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("project_document", &command.document_id))?;
        validate_document_scope(&document, &command.project_id)?;
        validate_document_approval_policy(&document, &command.authorization)?;
        let revision = ProjectOrchestrationRepo::get_project_document_revision(
            &*self.db,
            &command.revision_id,
        )
        .await?
        .ok_or_else(|| {
            ServiceError::not_found("project_document_revision", &command.revision_id)
        })?;
        if revision.document_id != document.id
            || document.current_draft_revision_id.as_deref() != Some(revision.id.as_str())
            || revision.content_digest != command.content_digest
            || revision.rendered_digest != command.rendered_digest
            || !matches!(revision.lifecycle.as_str(), "draft" | "proposed")
        {
            return Err(ServiceError::Db(db::DbError::VersionConflict));
        }
        let approval_id = new_uuid_v4();
        let result_json = json!({
            "operation": "project.document",
            "project_id": command.project_id,
            "document_id": command.document_id,
            "revision_id": command.revision_id,
            "approval_id": approval_id,
            "content_digest": command.content_digest,
            "render_digest": command.rendered_digest,
            "lifecycle": "active",
            "domain_committed": true,
        })
        .to_string();
        let (mut receipt, execution) = command_bundle(&context, &result_json);
        if let Some(execution) = execution.as_ref() {
            receipt.agent_action_execution_id = Some(execution.id.clone());
        }
        let now = now_rfc3339();
        let approval = ProjectOrchestrationRepo::approve_project_document_command(
            &*self.db,
            ApproveProjectDocumentCommand {
                approval: ApproveProjectDocument {
                    id: approval_id,
                    document_id: document.id,
                    revision_id: revision.id,
                    expected_document_version: command.expected_document_version,
                    principal_type: command.authorization.principal_type,
                    principal_id: command.authorization.principal_id,
                    authorization_basis: command.authorization.authorization_basis,
                    authorization_action: command.authorization.authorization_action,
                    explicit_event: command.authorization.authorization_event_id,
                    authorization_occurred_at: command.authorization.authorization_occurred_at,
                    content_digest: command.content_digest,
                    rendered_digest: command.rendered_digest,
                    idempotency_key: command.idempotency_key,
                    created_at: now.clone(),
                    updated_at: now,
                },
                command_receipt: Some(receipt),
                action_execution: execution,
            },
        )
        .await?;
        Ok((approval, false))
    }

    async fn replay_document_shell(
        &self,
        context: &CommandContext,
    ) -> Result<Option<ProjectDocumentRecord>> {
        let Some(receipt) = self.replay_receipt(context).await? else {
            return Ok(None);
        };
        let outcome: Value = serde_json::from_str(&receipt.outcome_json).map_err(|_| {
            ServiceError::Conflict("Project Document replay outcome is invalid".to_owned())
        })?;
        let project_id = result_value(&receipt.outcome_json, "project_id")?;
        if project_id != context.canonical_scope().scope_id() {
            return Err(ServiceError::Db(db::DbError::IdempotencyConflict));
        }
        Ok(Some(ProjectDocumentRecord {
            id: result_value(&receipt.outcome_json, "document_id")?,
            project_id,
            kind: result_value(&receipt.outcome_json, "kind")?,
            title: result_value(&receipt.outcome_json, "title")?,
            lifecycle: result_value(&receipt.outcome_json, "lifecycle")?,
            approval_policy: result_value(&receipt.outcome_json, "approval_policy")?,
            current_draft_revision_id: result_optional_string(
                &outcome,
                "current_draft_revision_id",
            )?,
            current_approved_revision_id: result_optional_string(
                &outcome,
                "current_approved_revision_id",
            )?,
            version: result_i64(&outcome, "version")?,
            created_at: result_value(&receipt.outcome_json, "created_at")?,
            updated_at: result_value(&receipt.outcome_json, "updated_at")?,
        }))
    }

    async fn replay_document_revision(
        &self,
        context: &CommandContext,
    ) -> Result<Option<ProjectDocumentRevisionRecord>> {
        let Some(receipt) = self.replay_receipt(context).await? else {
            return Ok(None);
        };
        let document_id = result_value(&receipt.outcome_json, "document_id")?;
        let revision_id = result_value(&receipt.outcome_json, "revision_id")?;
        let revision =
            ProjectOrchestrationRepo::get_project_document_revision(&*self.db, &revision_id)
                .await?
                .ok_or_else(|| {
                    ServiceError::Conflict("Project Document replay revision is missing".to_owned())
                })?;
        if revision.document_id != document_id {
            return Err(ServiceError::Db(db::DbError::IdempotencyConflict));
        }
        let document = ProjectOrchestrationRepo::get_project_document(&*self.db, &document_id)
            .await?
            .ok_or_else(|| {
                ServiceError::Conflict("Project Document replay shell is missing".to_owned())
            })?;
        if document.project_id != context.canonical_scope().scope_id() {
            return Err(ServiceError::Db(db::DbError::IdempotencyConflict));
        }
        Ok(Some(revision))
    }

    async fn replay_document_approval(
        &self,
        context: &CommandContext,
    ) -> Result<Option<ProjectDocumentApprovalRecord>> {
        let Some(receipt) = self.replay_receipt(context).await? else {
            return Ok(None);
        };
        let approval_id = result_value(&receipt.outcome_json, "approval_id")?;
        let approval = load_document_approval(&self.db, &approval_id)
            .await?
            .ok_or_else(|| {
                ServiceError::Conflict("Project Document replay approval is missing".to_owned())
            })?;
        if approval.principal_id != context.principal().principal_id() {
            return Err(ServiceError::Db(db::DbError::IdempotencyConflict));
        }
        let document =
            ProjectOrchestrationRepo::get_project_document(&*self.db, &approval.document_id)
                .await?
                .ok_or_else(|| {
                    ServiceError::Conflict("Project Document replay shell is missing".to_owned())
                })?;
        if document.project_id != context.canonical_scope().scope_id() {
            return Err(ServiceError::Db(db::DbError::IdempotencyConflict));
        }
        Ok(Some(approval))
    }

    async fn replay_receipt(&self, context: &CommandContext) -> Result<Option<CommandReceipt>> {
        let existing = CommandReceiptRepo::get_command_receipt(
            &*self.db,
            context.principal().principal_type(),
            context.principal().principal_id(),
            context.canonical_scope().scope_type().as_str(),
            context.canonical_scope().scope_id(),
            context.operation(),
            context.idempotency_key(),
            context.input_digest(),
        )
        .await?;
        if existing.is_some() {
            return Ok(existing);
        }
        let existing_digest: Option<String> = self
            .db
            .artifact_command_conflicting_input_digest(
                db::ArtifactCommandConflictingInputDigestInput {
                    principal_type: context.principal().principal_type(),
                    principal_id: context.principal().principal_id(),
                    scope_type: context.canonical_scope().scope_type().as_str(),
                    scope_id: context.canonical_scope().scope_id(),
                    operation: context.operation(),
                    idempotency_key: context.idempotency_key(),
                },
            )
            .await?;
        if existing_digest.is_some_and(|digest| digest != context.input_digest()) {
            return Err(ServiceError::Db(db::DbError::IdempotencyConflict));
        }
        Ok(None)
    }

    async fn replay_evidence(
        &self,
        context: &CommandContext,
    ) -> Result<Option<ProjectMediaAttachment>> {
        let existing = CommandReceiptRepo::get_command_receipt(
            &*self.db,
            context.principal().principal_type(),
            context.principal().principal_id(),
            context.canonical_scope().scope_type().as_str(),
            context.canonical_scope().scope_id(),
            context.operation(),
            context.idempotency_key(),
            context.input_digest(),
        )
        .await?;
        if let Some(receipt) = existing {
            let outcome: Value = serde_json::from_str(&receipt.outcome_json).map_err(|_| {
                ServiceError::Conflict("evidence receipt outcome is invalid".to_owned())
            })?;
            let attachment_id = outcome
                .get("attachment_id")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    ServiceError::Conflict("evidence receipt has no attachment".to_owned())
                })?;
            let attachment = load_project_media_attachment(&self.db, attachment_id)
                .await?
                .ok_or_else(|| {
                    ServiceError::Conflict("evidence receipt attachment is missing".to_owned())
                })?;
            if attachment.project_id != context.canonical_scope().scope_id() {
                return Err(ServiceError::Db(db::DbError::IdempotencyConflict));
            }
            return Ok(Some(attachment));
        }

        // Exact-digest lookup intentionally returns no row for altered input.
        // Detect that identity collision before any domain lookup so the
        // caller receives idempotency_conflict rather than a version error.
        let existing_digest: Option<String> = self
            .db
            .artifact_replay_command_input_digest(db::ArtifactReplayCommandInputDigestInput {
                principal_type: context.principal().principal_type(),
                principal_id: context.principal().principal_id(),
                scope_type: context.canonical_scope().scope_type().as_str(),
                scope_id: context.canonical_scope().scope_id(),
                operation: context.operation(),
                idempotency_key: context.idempotency_key(),
            })
            .await?;
        if existing_digest.is_some_and(|digest| digest != context.input_digest()) {
            return Err(ServiceError::Db(db::DbError::IdempotencyConflict));
        }
        Ok(None)
    }
}
