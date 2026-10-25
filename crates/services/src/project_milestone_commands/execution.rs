//! Project milestone definition, readiness, and release request execution.

use super::*;

impl ProjectMilestoneCommandService {
    #[must_use]
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self { db }
    }

    /// Execute a native Project-Agent milestone/readiness/release-request
    /// proposal.  The adapter contributes only its admitted action and
    /// command context; payload-derived authorization is not trusted here.
    pub async fn execute_project_agent_command(
        &self,
        action: &AgentAction,
        project_id: &str,
        payload: &Value,
        context: &CommandContext,
    ) -> Result<Value> {
        if context.canonical_scope().scope_type() != CommandScopeType::Project
            || context.canonical_scope().scope_id() != project_id
        {
            return Err(ServiceError::invalid_operation(
                "Project milestone command context is outside its canonical Project scope",
            ));
        }
        let payload_action = value_string(payload, "action")?;
        let authorization_action =
            command_authorization_action(context.operation(), payload_action.as_str())?;
        let authorization = native_authorization(action, context, authorization_action);
        match (context.operation(), payload_action.as_str()) {
            (PROJECT_MILESTONE_OPERATION, "define") => {
                let content: MilestoneDefinitionContent = value(payload, "content")?;
                let rendered_view = canonical_json(&content)
                    .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
                let revision = self
                    .define_milestone_with_context(
                        ProjectMilestoneDefinitionCommand {
                            project_id: project_id.to_owned(),
                            milestone_id: None,
                            display_label: optional_string(payload, "display_label"),
                            lifecycle: definition_lifecycle(
                                payload,
                                MilestoneDefinitionLifecycle::Draft,
                            )?,
                            content,
                            rendered_view,
                            render_version: MILESTONE_RENDER_VERSION.to_owned(),
                            change_summary: "Project Agent authored a typed milestone definition"
                                .to_owned(),
                            provenance: value(payload, "provenance")
                                .unwrap_or_else(|_| native_revision_provenance(action, context)),
                            base_revision_id: None,
                            expected_project_version: integer_or_zero(
                                payload,
                                "expected_milestone_version",
                            ),
                            expected_milestone_version: 1,
                            idempotency_key: context.idempotency_key().to_owned(),
                            authorization,
                        },
                        context.clone(),
                    )
                    .await?;
                Ok(json!({
                    "operation": PROJECT_MILESTONE_OPERATION,
                    "project_id": project_id,
                    "milestone_id": revision.milestone_id,
                    "revision_id": revision.id,
                    "revision": revision.revision,
                    "lifecycle": revision.lifecycle,
                    "domain_committed": true,
                    "requires_user_authorization": revision.lifecycle == "proposed",
                }))
            }
            (PROJECT_MILESTONE_OPERATION, "revise") => {
                let content: MilestoneDefinitionContent = value(payload, "content")?;
                let rendered_view = canonical_json(&content)
                    .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
                let revision = self
                    .revise_milestone_with_context(
                        ProjectMilestoneDefinitionCommand {
                            project_id: project_id.to_owned(),
                            milestone_id: Some(value_string(payload, "milestone_id")?),
                            display_label: optional_string(payload, "display_label"),
                            lifecycle: definition_lifecycle(
                                payload,
                                MilestoneDefinitionLifecycle::Proposed,
                            )?,
                            content,
                            rendered_view,
                            render_version: MILESTONE_RENDER_VERSION.to_owned(),
                            change_summary: "Project Agent authored a typed milestone revision"
                                .to_owned(),
                            provenance: value(payload, "provenance")
                                .unwrap_or_else(|_| native_revision_provenance(action, context)),
                            base_revision_id: optional_string(payload, "base_revision_id"),
                            expected_project_version: 0,
                            expected_milestone_version: value_i64(
                                payload,
                                "expected_milestone_version",
                            )?,
                            idempotency_key: context.idempotency_key().to_owned(),
                            authorization,
                        },
                        context.clone(),
                    )
                    .await?;
                Ok(json!({
                    "operation": PROJECT_MILESTONE_OPERATION,
                    "project_id": project_id,
                    "milestone_id": revision.milestone_id,
                    "revision_id": revision.id,
                    "revision": revision.revision,
                    "lifecycle": revision.lifecycle,
                    "domain_committed": true,
                    "requires_user_authorization": revision.lifecycle == "proposed",
                }))
            }
            (PROJECT_MILESTONE_OPERATION, "set_primary") => {
                let project = self
                    .set_primary_milestone_with_context(
                        ProjectPrimaryMilestoneCommand {
                            project_id: project_id.to_owned(),
                            primary_milestone_id: optional_string(payload, "primary_milestone_id"),
                            expected_project_version: value_i64(
                                payload,
                                "expected_milestone_version",
                            )?,
                            idempotency_key: context.idempotency_key().to_owned(),
                            authorization,
                        },
                        context.clone(),
                    )
                    .await?;
                Ok(json!({
                    "operation": PROJECT_MILESTONE_OPERATION,
                    "project_id": project.id,
                    "primary_milestone_id": project.primary_milestone_id,
                    "version": project.version,
                    "domain_committed": true,
                    "requires_user_authorization": false,
                }))
            }
            (PROJECT_READINESS_OPERATION, "evaluate") => {
                let snapshot = self
                    .request_readiness_with_context(
                        ProjectReadinessRequestCommand {
                            project_id: project_id.to_owned(),
                            milestone_id: value_string(payload, "milestone_id")?,
                            expected_milestone_version: value_i64(payload, "milestone_version")?,
                            idempotency_key: context.idempotency_key().to_owned(),
                            authenticated_user_id: None,
                            authorization: native_authorization(
                                action,
                                context,
                                "project.milestone.readiness",
                            ),
                        },
                        context.clone(),
                    )
                    .await?;
                Ok(json!({
                    "operation": PROJECT_READINESS_OPERATION,
                    "project_id": project_id,
                    "milestone_id": snapshot.milestone_id,
                    "readiness_snapshot_id": snapshot.id,
                    "readiness_digest": snapshot.readiness_digest,
                    "result": snapshot.outcome,
                    "status": "computed",
                    "domain_committed": true,
                }))
            }
            (PROJECT_RELEASE_OPERATION, "propose_candidate") => {
                let request = self
                    .request_release_with_context(
                        ProjectReleaseRequestCommand {
                            project_id: project_id.to_owned(),
                            milestone_id: value_string(payload, "milestone_id")?,
                            expected_milestone_version: value_i64(payload, "milestone_version")?,
                            readiness_snapshot_id: value_string(payload, "readiness_snapshot_id")?,
                            readiness_digest: value_string(payload, "readiness_digest")?,
                            status: "pending_user_release_approval".to_owned(),
                            idempotency_key: context.idempotency_key().to_owned(),
                            authorization: native_authorization(
                                action,
                                context,
                                "project.milestone.release.request",
                            ),
                        },
                        context.clone(),
                    )
                    .await?;
                Ok(json!({
                    "operation": PROJECT_RELEASE_OPERATION,
                    "project_id": project_id,
                    "milestone_id": request.milestone_id,
                    "candidate_event_id": request.event_id,
                    "status": request.status,
                    "domain_committed": true,
                    "final_release_created": false,
                }))
            }
            _ => Err(ServiceError::invalid_operation(
                "unsupported Project milestone/readiness/release command",
            )),
        }
    }

    pub async fn define_milestone(
        &self,
        command: ProjectMilestoneDefinitionCommand,
        action: Option<AgentActionProvenance>,
    ) -> Result<ProjectMilestoneRevisionRecord> {
        validate_definition_command(&command, false)?;
        let context = command_context(
            PROJECT_MILESTONE_OPERATION,
            &command.idempotency_key,
            &command.authorization,
            &command,
            action,
            &command.project_id,
            BTreeMap::from([(
                "expected_project_version".to_owned(),
                command.expected_project_version,
            )]),
        )?;
        self.define_milestone_with_context(command, context).await
    }

    pub async fn revise_milestone(
        &self,
        command: ProjectMilestoneDefinitionCommand,
        action: Option<AgentActionProvenance>,
    ) -> Result<ProjectMilestoneRevisionRecord> {
        validate_definition_command(&command, true)?;
        let context = command_context(
            PROJECT_MILESTONE_OPERATION,
            &command.idempotency_key,
            &command.authorization,
            &command,
            action,
            &command.project_id,
            BTreeMap::from([(
                "expected_milestone_version".to_owned(),
                command.expected_milestone_version,
            )]),
        )?;
        self.revise_milestone_with_context(command, context).await
    }

    pub async fn set_primary_milestone(
        &self,
        command: ProjectPrimaryMilestoneCommand,
        action: Option<AgentActionProvenance>,
    ) -> Result<Project> {
        validate_primary_command(&command)?;
        let context = command_context(
            PROJECT_MILESTONE_OPERATION,
            &command.idempotency_key,
            &command.authorization,
            &command,
            action,
            &command.project_id,
            BTreeMap::from([(
                "expected_project_version".to_owned(),
                command.expected_project_version,
            )]),
        )?;
        self.set_primary_milestone_with_context(command, context)
            .await
    }

    pub async fn request_readiness(
        &self,
        command: ProjectReadinessRequestCommand,
        action: Option<AgentActionProvenance>,
    ) -> Result<ProjectReadinessSnapshotRecord> {
        let context = command_context(
            PROJECT_READINESS_OPERATION,
            &command.idempotency_key,
            &command.authorization,
            &command,
            action,
            &command.project_id,
            BTreeMap::from([(
                "expected_milestone_version".to_owned(),
                command.expected_milestone_version,
            )]),
        )?;
        self.request_readiness_with_context(command, context).await
    }

    pub async fn request_release(
        &self,
        command: ProjectReleaseRequestCommand,
        action: Option<AgentActionProvenance>,
    ) -> Result<ProjectReleaseRequestRecord> {
        validate_release_command(&command)?;
        let context = command_context(
            PROJECT_RELEASE_OPERATION,
            &command.idempotency_key,
            &command.authorization,
            &command,
            action,
            &command.project_id,
            BTreeMap::from([(
                "expected_milestone_version".to_owned(),
                command.expected_milestone_version,
            )]),
        )?;
        self.request_release_with_context(command, context).await
    }

    async fn define_milestone_with_context(
        &self,
        mut command: ProjectMilestoneDefinitionCommand,
        context: CommandContext,
    ) -> Result<ProjectMilestoneRevisionRecord> {
        validate_context(
            &context,
            PROJECT_MILESTONE_OPERATION,
            &command.project_id,
            &command.authorization,
        )?;
        validate_definition_command(&command, false)?;
        if let Some(revision) = self.replay_revision(&context, None).await? {
            return Ok(revision);
        }
        authorize_project_principal(&self.db, &command.project_id, &command.authorization).await?;
        // The approved Charter is the governing authority readiness checks
        // every validation against, and validations are always recorded under
        // the Project's current Charter. A first definition that omits the
        // binding could therefore never become ready, so bind it here.
        if command.content.charter_revision.is_none() {
            command.content.charter_revision =
                current_charter_reference(&self.db, &command.project_id).await?;
        }
        validate_definition_references(&self.db, &command.project_id, &command.content).await?;
        if command.expected_project_version <= 0 {
            command.expected_project_version =
                current_project_version(&self.db, &command.project_id).await?;
        }
        let milestone_id = new_uuid_v4();
        let revision_id = new_uuid_v4();
        let now = now_rfc3339();
        let revision = build_revision(
            &command,
            revision_id.clone(),
            milestone_id.clone(),
            0,
            None,
            milestone_definition_lifecycle_name(command.lifecycle),
        )?;
        let check_definitions =
            build_check_definitions(&command, &milestone_id, &revision_id, &revision.created_at);
        let lifecycle = milestone_definition_lifecycle_name(command.lifecycle);
        let result_json = json!({
            "operation": PROJECT_MILESTONE_OPERATION,
            "project_id": command.project_id,
            "milestone_id": milestone_id,
            "revision_id": revision_id,
            "revision": 1,
            "lifecycle": lifecycle,
            "domain_committed": true,
            "requires_user_authorization": lifecycle == "proposed",
        })
        .to_string();
        let (mut receipt, execution) = command_bundle(&context, &result_json);
        if let Some(execution) = execution.as_ref() {
            receipt.agent_action_execution_id = Some(execution.id.clone());
        }
        ProjectOrchestrationRepo::create_project_milestone_command(
            &*self.db,
            CreateProjectMilestoneCommand {
                milestone: CreateProjectMilestone {
                    id: milestone_id,
                    project_id: command.project_id,
                    expected_project_version: command.expected_project_version,
                    // The DB command allocates both values while holding the
                    // Project write lock; these transport placeholders are
                    // never persisted.
                    milestone_sequence: 0,
                    milestone_key: String::new(),
                    display_label: command.display_label,
                    created_at: now.clone(),
                    updated_at: now,
                },
                revision,
                allocate_project_sequence: true,
                check_definitions,
                command_receipt: Some(receipt),
                action_execution: execution,
            },
        )
        .await
        .map_err(Into::into)
    }

    async fn revise_milestone_with_context(
        &self,
        mut command: ProjectMilestoneDefinitionCommand,
        context: CommandContext,
    ) -> Result<ProjectMilestoneRevisionRecord> {
        validate_context(
            &context,
            PROJECT_MILESTONE_OPERATION,
            &command.project_id,
            &command.authorization,
        )?;
        validate_definition_command(&command, true)?;
        if let Some(revision) = self
            .replay_revision(&context, command.milestone_id.as_deref())
            .await?
        {
            return Ok(revision);
        }
        authorize_project_principal(&self.db, &command.project_id, &command.authorization).await?;
        let milestone_id = command.milestone_id.clone().ok_or_else(|| {
            ServiceError::invalid_operation("milestone_id is required for a revision")
        })?;
        let milestone = ProjectOrchestrationRepo::get_project_milestone(&*self.db, &milestone_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project_milestone", milestone_id.clone()))?;
        if milestone.project_id != command.project_id {
            return Err(ServiceError::invalid_operation(
                "milestone revision crosses Project scope",
            ));
        }
        // A revision is a delta on an existing definition, and the Charter it
        // is tied to is not something a revision silently drops. Persisting a
        // NULL binding leaves the milestone impossible to evaluate readiness
        // for, since the approved Charter is its governing authority. The
        // inherited reference is validated below exactly like a supplied one.
        if command.content.charter_revision.is_none() {
            command.content.charter_revision =
                inherited_charter_reference(&self.db, &command.project_id, &milestone_id).await?;
        }
        // A definition created before first definitions were bound has no
        // binding to inherit; its next revision repairs it.
        if command.content.charter_revision.is_none() {
            command.content.charter_revision =
                current_charter_reference(&self.db, &command.project_id).await?;
        }
        validate_definition_references(&self.db, &command.project_id, &command.content).await?;
        if command.expected_milestone_version <= 0 {
            return Err(ServiceError::invalid_operation(
                "expected_milestone_version must be positive",
            ));
        }
        let revisions =
            ProjectOrchestrationRepo::list_project_milestone_revisions(&*self.db, &milestone_id)
                .await?;
        // A draft first revision intentionally leaves the current pointer
        // unset.  The public/runtime projection falls back to the latest
        // immutable revision in that state, so an agent can revise its draft
        // without inventing a different base.  The command DB composite must
        // apply the same latest-revision CAS when the pointer is NULL.
        let current = milestone
            .current_definition_revision_id
            .as_deref()
            .and_then(|id| revisions.iter().find(|revision| revision.id == id))
            .or_else(|| revisions.iter().max_by_key(|revision| revision.revision))
            .ok_or_else(|| ServiceError::conflict("milestone has no current definition"))?;
        if command.base_revision_id.is_none() {
            command.base_revision_id = Some(current.id.clone());
        }
        if command.base_revision_id.as_deref() != Some(current.id.as_str()) {
            return Err(ServiceError::Db(db::DbError::VersionConflict));
        }
        let revision_id = new_uuid_v4();
        let revision = build_revision(
            &command,
            revision_id.clone(),
            milestone_id.clone(),
            current.revision,
            command.base_revision_id.clone(),
            milestone_definition_lifecycle_name(command.lifecycle),
        )?;
        // Re-authoring the current definition verbatim is a no-op: appending
        // a duplicate row would move the current pointer onto an unapproved
        // copy and silently revoke a standing approval with zero semantic
        // change. Only an actual content change, or a draft being promoted,
        // may append a new revision.
        if revision.content_digest == current.content_digest
            && revision.display_label == current.display_label
            && (revision.lifecycle == current.lifecycle || current.lifecycle == "approved")
        {
            return Ok(current.clone());
        }
        let check_definitions =
            build_check_definitions(&command, &milestone_id, &revision_id, &revision.created_at);
        let lifecycle = milestone_definition_lifecycle_name(command.lifecycle);
        let result_json = json!({
            "operation": PROJECT_MILESTONE_OPERATION,
            "project_id": command.project_id,
            "milestone_id": milestone_id,
            "revision_id": revision_id,
            "revision": current.revision + 1,
            "lifecycle": lifecycle,
            "domain_committed": true,
            "requires_user_authorization": lifecycle == "proposed",
        })
        .to_string();
        let (mut receipt, execution) = command_bundle(&context, &result_json);
        if let Some(execution) = execution.as_ref() {
            receipt.agent_action_execution_id = Some(execution.id.clone());
        }
        ProjectOrchestrationRepo::append_project_milestone_revision_command(
            &*self.db,
            AppendProjectMilestoneRevisionCommand {
                revision,
                check_definitions,
                command_receipt: Some(receipt),
                action_execution: execution,
            },
        )
        .await
        .map_err(Into::into)
    }

    async fn set_primary_milestone_with_context(
        &self,
        command: ProjectPrimaryMilestoneCommand,
        context: CommandContext,
    ) -> Result<Project> {
        validate_context(
            &context,
            PROJECT_MILESTONE_OPERATION,
            &command.project_id,
            &command.authorization,
        )?;
        validate_primary_command(&command)?;
        if let Some(project) = self.replay_project(&context).await? {
            return Ok(project);
        }
        authorize_project_principal(&self.db, &command.project_id, &command.authorization).await?;
        let now = now_rfc3339();
        let current_project = ProjectRepo::get_by_id(&*self.db, &command.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", command.project_id.clone()))?;
        if current_project.version != command.expected_project_version {
            return Err(ServiceError::Db(db::DbError::VersionConflict));
        }
        let frozen_project = frozen_project_value(
            &current_project,
            command.primary_milestone_id.as_deref(),
            command.expected_project_version + 1,
            &now,
        );
        let result_json = json!({
            "operation": PROJECT_MILESTONE_OPERATION,
            "project_id": command.project_id,
            "primary_milestone_id": command.primary_milestone_id,
            "version": command.expected_project_version + 1,
            "updated_at": now.clone(),
            "project": frozen_project,
            "domain_committed": true,
            "requires_user_authorization": false,
        })
        .to_string();
        let (mut receipt, execution) = command_bundle(&context, &result_json);
        if let Some(execution) = execution.as_ref() {
            receipt.agent_action_execution_id = Some(execution.id.clone());
        }
        ProjectOrchestrationRepo::set_primary_project_milestone_command(
            &*self.db,
            db::SetPrimaryProjectMilestoneCommand {
                project_id: command.project_id,
                primary_milestone_id: command.primary_milestone_id,
                expected_project_version: command.expected_project_version,
                principal_type: command.authorization.principal_type,
                principal_id: command.authorization.principal_id,
                authorization_basis: command.authorization.authorization_basis,
                authorization_action: command.authorization.authorization_action,
                authorization_occurred_at: command.authorization.authorization_occurred_at,
                explicit_event: command.authorization.authorization_event_id,
                idempotency_key: command.idempotency_key,
                updated_at: now,
                command_receipt: Some(receipt),
                action_execution: execution,
            },
        )
        .await
        .map_err(ServiceError::from)?;
        self.replay_project(&context).await?.ok_or_else(|| {
            ServiceError::Conflict(
                "primary milestone command committed without a frozen receipt outcome".to_owned(),
            )
        })
    }

    async fn request_readiness_with_context(
        &self,
        command: ProjectReadinessRequestCommand,
        context: CommandContext,
    ) -> Result<ProjectReadinessSnapshotRecord> {
        validate_context(
            &context,
            PROJECT_READINESS_OPERATION,
            &command.project_id,
            &command.authorization,
        )?;
        if let Some(record) = self.replay_readiness(&context).await? {
            return Ok(record);
        }
        validate_readiness_command(&command)?;
        authorize_project_principal(&self.db, &command.project_id, &command.authorization).await?;
        let actor = api_types::PrincipalRef {
            kind: if command.authorization.principal_type == "agent" {
                api_types::PrincipalKind::Agent
            } else {
                api_types::PrincipalKind::User
            },
            id: command.authorization.principal_id.clone(),
            display_name: None,
        };
        let authorization = api_types::AuthorizationProvenance {
            principal: actor.clone(),
            authorization_basis: command.authorization.authorization_basis.clone(),
            action: command.authorization.authorization_action.clone(),
            event_id: command.authorization.authorization_event_id.clone(),
            occurred_at: command.authorization.authorization_occurred_at.clone(),
        };
        let snapshot = crate::MilestoneRuntime::new(Arc::clone(&self.db))
            .evaluate_candidate(
                &command.project_id,
                &actor,
                &authorization,
                &command.milestone_id,
                command.expected_milestone_version,
            )
            .await?;
        let result_json = json!({
            "operation": PROJECT_READINESS_OPERATION,
            "project_id": command.project_id,
            "milestone_id": command.milestone_id,
            "readiness_snapshot_id": snapshot.id,
            "readiness_digest": snapshot.readiness_digest,
            "result": readiness_result_name(snapshot.result),
            "domain_committed": true,
        })
        .to_string();
        let (mut receipt, execution) = command_bundle(&context, &result_json);
        if let Some(execution) = execution.as_ref() {
            receipt.agent_action_execution_id = Some(execution.id.clone());
        }
        ProjectOrchestrationRepo::create_project_readiness_snapshot_command(
            &*self.db,
            CreateProjectReadinessSnapshotCommand {
                snapshot: readiness_input(&snapshot, &command, &authorization)?,
                command_receipt: Some(receipt),
                action_execution: execution,
            },
        )
        .await
        .map_err(Into::into)
    }

    async fn request_release_with_context(
        &self,
        command: ProjectReleaseRequestCommand,
        context: CommandContext,
    ) -> Result<ProjectReleaseRequestRecord> {
        validate_context(
            &context,
            PROJECT_RELEASE_OPERATION,
            &command.project_id,
            &command.authorization,
        )?;
        validate_release_command(&command)?;
        if let Some(record) = self.replay_release_request(&context).await? {
            return Ok(record);
        }
        authorize_project_principal(&self.db, &command.project_id, &command.authorization).await?;
        validate_release_readiness_candidate(&self.db, &command).await?;
        let result_json = json!({
            "operation": PROJECT_RELEASE_OPERATION,
            "project_id": command.project_id,
            "milestone_id": command.milestone_id,
            "readiness_snapshot_id": command.readiness_snapshot_id,
            "candidate_event_id": Value::Null,
            "status": command.status,
        });
        let event_id = new_uuid_v4();
        let mut result_json = result_json;
        result_json["candidate_event_id"] = Value::String(event_id.clone());
        result_json["domain_committed"] = Value::Bool(true);
        result_json["final_release_created"] = Value::Bool(false);
        let result_json = result_json.to_string();
        let (mut receipt, execution) = command_bundle(&context, &result_json);
        if let Some(execution) = execution.as_ref() {
            receipt.agent_action_execution_id = Some(execution.id.clone());
        }
        ProjectOrchestrationRepo::create_project_release_request_command(
            &*self.db,
            CreateProjectReleaseRequestCommand {
                request: CreateProjectReleaseRequest {
                    event_id,
                    project_id: command.project_id,
                    milestone_id: command.milestone_id,
                    expected_milestone_version: command.expected_milestone_version,
                    readiness_snapshot_id: command.readiness_snapshot_id,
                    readiness_digest: command.readiness_digest,
                    status: command.status,
                    idempotency_key: command.idempotency_key,
                    created_at: now_rfc3339(),
                },
                command_receipt: Some(receipt),
                action_execution: execution,
            },
        )
        .await
        .map_err(Into::into)
    }

    async fn replay_revision(
        &self,
        context: &CommandContext,
        milestone_id: Option<&str>,
    ) -> Result<Option<ProjectMilestoneRevisionRecord>> {
        let Some(receipt) = replay_receipt(&self.db, context).await? else {
            return Ok(None);
        };
        let revision_id = outcome_string(&receipt, "revision_id")?;
        let revision =
            ProjectOrchestrationRepo::get_project_milestone_revision(&*self.db, &revision_id)
                .await?
                .ok_or_else(|| {
                    ServiceError::Conflict("milestone replay revision is missing".to_owned())
                })?;
        let milestone =
            ProjectOrchestrationRepo::get_project_milestone(&*self.db, &revision.milestone_id)
                .await?
                .ok_or_else(|| {
                    ServiceError::Conflict("milestone replay shell is missing".to_owned())
                })?;
        if milestone_id.is_some_and(|id| id != revision.milestone_id)
            || milestone.project_id != context.canonical_scope().scope_id()
        {
            return Err(ServiceError::Db(db::DbError::IdempotencyConflict));
        }
        Ok(Some(revision))
    }

    async fn replay_project(&self, context: &CommandContext) -> Result<Option<Project>> {
        let Some(receipt) = replay_receipt(&self.db, context).await? else {
            return Ok(None);
        };
        let outcome: Value = serde_json::from_str(&receipt.outcome_json).map_err(|_| {
            ServiceError::Conflict("primary milestone replay outcome is invalid".to_owned())
        })?;
        let project = outcome.get("project").ok_or_else(|| {
            ServiceError::Conflict(
                "primary milestone replay outcome has no frozen Project result".to_owned(),
            )
        })?;
        let project_id = required_value(project, "id")?;
        if project_id != context.canonical_scope().scope_id()
            || required_value(&outcome, "project_id")? != project_id
        {
            return Err(ServiceError::Db(db::DbError::IdempotencyConflict));
        }
        Ok(Some(project_from_value(project)?))
    }

    async fn replay_readiness(
        &self,
        context: &CommandContext,
    ) -> Result<Option<ProjectReadinessSnapshotRecord>> {
        let Some(receipt) = replay_receipt(&self.db, context).await? else {
            return Ok(None);
        };
        let snapshot_id = outcome_string(&receipt, "readiness_snapshot_id")?;
        let row = self
            .db
            .milestone_replay_readiness_source(&snapshot_id, context.canonical_scope().scope_id())
            .await?
            .ok_or_else(|| {
                ServiceError::Conflict("readiness replay snapshot is missing".to_owned())
            })?;
        Ok(Some(map_readiness(row)?))
    }

    async fn replay_release_request(
        &self,
        context: &CommandContext,
    ) -> Result<Option<ProjectReleaseRequestRecord>> {
        let Some(receipt) = replay_receipt(&self.db, context).await? else {
            return Ok(None);
        };
        let event = DomainEventRepo::get_event(&*self.db, &receipt.event_id)
            .await?
            .ok_or_else(|| {
                ServiceError::Conflict("release-request replay event is missing".to_owned())
            })?;
        let payload: Value = serde_json::from_str(&event.payload_json).map_err(|_| {
            ServiceError::Conflict("release-request replay event is invalid".to_owned())
        })?;
        Ok(Some(ProjectReleaseRequestRecord {
            event_id: event.id,
            project_id: required_value(&payload, "project_id")?,
            milestone_id: required_value(&payload, "milestone_id")?,
            expected_milestone_version: payload
                .get("milestone_version")
                .and_then(Value::as_i64)
                .ok_or_else(|| {
                    ServiceError::Conflict(
                        "release-request event has no milestone version".to_owned(),
                    )
                })?,
            readiness_snapshot_id: required_value(&payload, "readiness_snapshot_id")?,
            readiness_digest: required_value(&payload, "readiness_digest")?,
            status: required_value(&payload, "status")?,
            idempotency_key: context.idempotency_key().to_owned(),
            created_at: event.created_at,
        }))
    }
}
