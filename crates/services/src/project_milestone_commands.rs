//! Shared Project milestone/readiness/release-request command boundary.
//!
//! This module is deliberately transport neutral.  REST supplies an
//! authenticated user authorization while native Project-Agent execution
//! supplies an already admitted `CommandContext` and optional
//! `AgentActionProvenance`.  Both paths use the same canonical validation and
//! the transaction-aware DB composites, so a domain row is never committed
//! without its receipt, event, and (when applicable) action outcome.

use std::{collections::BTreeMap, sync::Arc};

use api_types::{
    canonical_digest_with_schema, canonical_json, MilestoneDefinitionContent,
    MilestoneDefinitionLifecycle, RevisionProvenance,
};
use db::{
    new_uuid_v4, now_rfc3339, AgentAction, AgentActionExecutionStatus, AgentActionStatus,
    AppendProjectMilestoneRevisionCommand, CommandReceipt, CommandReceiptRepo,
    CreateAgentActionExecution, CreateCommandReceipt, CreateProjectMilestone,
    CreateProjectMilestoneCheck, CreateProjectMilestoneCommand, CreateProjectMilestoneRevision,
    CreateProjectReadinessSnapshot, CreateProjectReadinessSnapshotCommand,
    CreateProjectReleaseRequest, CreateProjectReleaseRequestCommand, DomainEventRepo, Project,
    ProjectMemberRepo, ProjectMilestoneRevisionRecord, ProjectOrchestrationRepo,
    ProjectReadinessSnapshotRecord, ProjectReleaseRequestRecord, ProjectRepo, SqliteDb,
};
use forge_agent_host::{
    PROJECT_MILESTONE_OPERATION, PROJECT_READINESS_OPERATION, PROJECT_RELEASE_OPERATION,
};
use serde::Serialize;
use serde_json::{json, Value};
use sqlx::Row;

use crate::{
    AgentActionProvenance, AuthorizationProvenance, CommandContext, CommandPrincipal, CommandScope,
    CommandScopeType, ExpectedCommandState, NewCommandContext, ProjectCommandAuthorization, Result,
    ServiceError,
};

pub const PROJECT_MILESTONE_COMMAND: &str = PROJECT_MILESTONE_OPERATION;
pub const PROJECT_READINESS_COMMAND: &str = PROJECT_READINESS_OPERATION;
pub const PROJECT_RELEASE_REQUEST_COMMAND: &str = PROJECT_RELEASE_OPERATION;

const MILESTONE_DEFINITION_SCHEMA: &str = "forge.milestone-definition/v1";
const MILESTONE_RENDER_SCHEMA: &str = "forge.milestone-definition-render/v1";
const MILESTONE_RENDER_VERSION: &str = MILESTONE_RENDER_SCHEMA;
const MILESTONE_CREATE_AUTHORIZATION_ACTION: &str = "project.milestone.create";
const MILESTONE_REVISION_AUTHORIZATION_ACTION: &str = "project.milestone.revision.save";
const MILESTONE_PRIMARY_AUTHORIZATION_ACTION: &str = "project.milestone.primary.set";
const MILESTONE_READINESS_AUTHORIZATION_ACTION: &str = "project.milestone.readiness";
const MILESTONE_RELEASE_REQUEST_AUTHORIZATION_ACTION: &str = "project.milestone.release.request";

#[derive(Debug, Clone, Serialize)]
pub struct ProjectMilestoneDefinitionCommand {
    pub project_id: String,
    pub milestone_id: Option<String>,
    pub display_label: Option<String>,
    pub lifecycle: MilestoneDefinitionLifecycle,
    pub content: MilestoneDefinitionContent,
    pub rendered_view: String,
    pub render_version: String,
    pub change_summary: String,
    pub provenance: RevisionProvenance,
    pub base_revision_id: Option<String>,
    /// The Project version used when defining the first milestone.  For a
    /// revision this field is ignored; `expected_milestone_version` is used.
    pub expected_project_version: i64,
    pub expected_milestone_version: i64,
    pub idempotency_key: String,
    pub authorization: ProjectCommandAuthorization,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectPrimaryMilestoneCommand {
    pub project_id: String,
    pub primary_milestone_id: Option<String>,
    pub expected_project_version: i64,
    pub idempotency_key: String,
    pub authorization: ProjectCommandAuthorization,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectReadinessRequestCommand {
    pub project_id: String,
    pub milestone_id: String,
    pub expected_milestone_version: i64,
    pub baseline_id: String,
    pub baseline_revision_id: String,
    pub release_policy_revision: String,
    pub idempotency_key: String,
    /// REST binds this command to the authenticated user while native
    /// Project-Agent execution leaves the field unset.  The binding is
    /// validated only after replay resolution so an altered authority
    /// envelope on an existing idempotency key returns a conflict rather
    /// than being re-authorized as a new request.
    pub authenticated_user_id: Option<String>,
    pub authorization: ProjectCommandAuthorization,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectReleaseRequestCommand {
    pub project_id: String,
    pub milestone_id: String,
    pub expected_milestone_version: i64,
    pub readiness_snapshot_id: String,
    pub readiness_digest: String,
    pub status: String,
    pub idempotency_key: String,
    pub authorization: ProjectCommandAuthorization,
}

#[derive(Clone)]
pub struct ProjectMilestoneCommandService {
    db: Arc<SqliteDb>,
}

impl ProjectMilestoneCommandService {
    #[must_use]
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self { db }
    }

    /// Execute a native Project-Agent milestone/readiness/release-request
    /// proposal.  The adapter contributes only its admitted action and
    /// command context; payload-derived authorization is not trusted here.
    pub(crate) async fn execute_project_agent_command(
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
                            baseline_id: value_string(payload, "baseline_id")?,
                            baseline_revision_id: value_string(payload, "baseline_revision_id")?,
                            release_policy_revision: value_string(
                                payload,
                                "release_policy_revision",
                            )?,
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
                &command.baseline_id,
                &command.baseline_revision_id,
                &command.release_policy_revision,
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
        let row =
            sqlx::query("SELECT * FROM project_readiness_snapshot WHERE id = ? AND project_id = ?")
                .bind(&snapshot_id)
                .bind(context.canonical_scope().scope_id())
                .fetch_optional(self.db.pool())
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

fn validate_definition_command(
    command: &ProjectMilestoneDefinitionCommand,
    revision: bool,
) -> Result<()> {
    if command.project_id.trim().is_empty()
        || command.idempotency_key.trim().is_empty()
        || command.change_summary.trim().is_empty()
        || command.authorization.principal_type.trim().is_empty()
        || command.authorization.principal_id.trim().is_empty()
        || command.authorization.correlation_id.trim().is_empty()
        || command
            .authorization
            .authorization_event_id
            .trim()
            .is_empty()
        || command.authorization.authorization_basis.trim().is_empty()
        || command.authorization.authorization_action.trim().is_empty()
        || command
            .authorization
            .authorization_occurred_at
            .trim()
            .is_empty()
        || command.content.name.trim().is_empty()
        || command.content.outcome.trim().is_empty()
    {
        return Err(ServiceError::invalid_operation(
            "milestone definition command is incomplete",
        ));
    }
    validate_authorization_action(
        &command.authorization,
        if revision {
            MILESTONE_REVISION_AUTHORIZATION_ACTION
        } else {
            MILESTONE_CREATE_AUTHORIZATION_ACTION
        },
    )?;
    if revision && command.milestone_id.is_none() {
        return Err(ServiceError::invalid_operation(
            "milestone_id is required for a revision",
        ));
    }
    if !revision && command.milestone_id.is_some() {
        return Err(ServiceError::invalid_operation(
            "define cannot include milestone_id",
        ));
    }
    if revision && command.expected_milestone_version < 1 {
        return Err(ServiceError::invalid_operation(
            "expected_milestone_version must be positive",
        ));
    }
    if !revision && command.expected_project_version < 0 {
        return Err(ServiceError::invalid_operation(
            "expected_project_version cannot be negative",
        ));
    }
    if !matches!(
        command.lifecycle,
        MilestoneDefinitionLifecycle::Draft | MilestoneDefinitionLifecycle::Proposed
    ) {
        return Err(ServiceError::invalid_operation(
            "milestone definition lifecycle is not writable",
        ));
    }
    let canonical = canonical_json(&command.content)
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
    if command.rendered_view != canonical {
        return Err(ServiceError::invalid_operation(
            "rendered_view must equal the server canonical milestone definition rendering",
        ));
    }
    if command.render_version != MILESTONE_RENDER_VERSION {
        return Err(ServiceError::invalid_operation(
            "render_version must name the current server milestone renderer",
        ));
    }
    validate_checks(
        &command.content,
        command.lifecycle != MilestoneDefinitionLifecycle::Draft,
    )?;
    Ok(())
}

fn validate_checks(content: &MilestoneDefinitionContent, materialize: bool) -> Result<()> {
    let mut seen = std::collections::HashSet::new();
    for check in &content.acceptance_checks {
        if check.id.trim().is_empty()
            || check.description.trim().is_empty()
            || !seen.insert(&check.id)
        {
            return Err(ServiceError::invalid_operation(
                "milestone acceptance checks require unique stable ids and descriptions",
            ));
        }
        if materialize
            && !matches!(
                check.source_kind,
                api_types::AcceptanceCheckSourceKind::Manual
                    | api_types::AcceptanceCheckSourceKind::PolicyWaiver
            )
        {
            return Err(ServiceError::invalid_operation(
                "this acceptance check source kind is not currently admitted without an authoritative projection",
            ));
        }
    }

    let mut evidence_by_id = std::collections::HashMap::new();
    for requirement in &content.evidence_requirements {
        if requirement.id.trim().is_empty()
            || requirement.description.trim().is_empty()
            || evidence_by_id
                .insert(requirement.id.as_str(), requirement)
                .is_some()
        {
            return Err(ServiceError::invalid_operation(
                "milestone evidence requirements require unique stable ids and descriptions",
            ));
        }
        if requirement.evidence_kind.as_deref().is_some_and(|kind| {
            !matches!(
                kind,
                "screenshot" | "walkthrough_video" | "log" | "report" | "other"
            )
        }) {
            return Err(ServiceError::invalid_operation(
                "milestone evidence_kind must be one of: screenshot, walkthrough_video, log, report, other",
            ));
        }
        if requirement
            .check_definition_revision
            .as_deref()
            .is_some_and(|revision| revision.trim().is_empty())
        {
            return Err(ServiceError::invalid_operation(
                "milestone evidence check_definition_revision cannot be empty",
            ));
        }
        if requirement.required
            && !content
                .acceptance_checks
                .iter()
                .any(|check| check.id == requirement.id)
        {
            return Err(ServiceError::invalid_operation(format!(
                "required evidence '{}' must reference an acceptance check with the same stable id",
                requirement.id
            )));
        }
    }
    if materialize {
        for check in content
            .acceptance_checks
            .iter()
            .filter(|check| check.required)
        {
            if !evidence_by_id
                .get(check.id.as_str())
                .is_some_and(|requirement| requirement.required)
            {
                return Err(ServiceError::invalid_operation(format!(
                    "required acceptance check '{}' requires a required evidence requirement with the same stable id",
                    check.id
                )));
            }
        }
    }
    Ok(())
}

fn command_authorization_action(operation: &str, action: &str) -> Result<&'static str> {
    match (operation, action) {
        (PROJECT_MILESTONE_OPERATION, "define") => Ok(MILESTONE_CREATE_AUTHORIZATION_ACTION),
        (PROJECT_MILESTONE_OPERATION, "revise") => Ok(MILESTONE_REVISION_AUTHORIZATION_ACTION),
        (PROJECT_MILESTONE_OPERATION, "set_primary") => Ok(MILESTONE_PRIMARY_AUTHORIZATION_ACTION),
        (PROJECT_READINESS_OPERATION, "evaluate") => Ok(MILESTONE_READINESS_AUTHORIZATION_ACTION),
        (PROJECT_RELEASE_OPERATION, "propose_candidate") => {
            Ok(MILESTONE_RELEASE_REQUEST_AUTHORIZATION_ACTION)
        }
        _ => Err(ServiceError::invalid_operation(
            "unsupported Project milestone/readiness/release command",
        )),
    }
}

fn definition_lifecycle(
    payload: &Value,
    default: MilestoneDefinitionLifecycle,
) -> Result<MilestoneDefinitionLifecycle> {
    let Some(value) = payload.get("lifecycle") else {
        return Ok(default);
    };
    if value.is_null() {
        return Ok(default);
    }
    match value.as_str() {
        Some("draft") => Ok(MilestoneDefinitionLifecycle::Draft),
        Some("proposed") => Ok(MilestoneDefinitionLifecycle::Proposed),
        _ => Err(ServiceError::invalid_operation(
            "milestone lifecycle must be draft or proposed",
        )),
    }
}

fn milestone_definition_lifecycle_name(value: MilestoneDefinitionLifecycle) -> &'static str {
    match value {
        MilestoneDefinitionLifecycle::Draft => "draft",
        MilestoneDefinitionLifecycle::Proposed => "proposed",
        MilestoneDefinitionLifecycle::Approved => "approved",
        MilestoneDefinitionLifecycle::Superseded => "superseded",
    }
}

fn validate_primary_command(command: &ProjectPrimaryMilestoneCommand) -> Result<()> {
    if command.project_id.trim().is_empty()
        || command.idempotency_key.trim().is_empty()
        || command.expected_project_version < 1
        || command.authorization.principal_type.trim().is_empty()
        || command.authorization.principal_id.trim().is_empty()
        || command.authorization.authorization_basis.trim().is_empty()
        || command.authorization.authorization_action.trim().is_empty()
        || command
            .authorization
            .authorization_event_id
            .trim()
            .is_empty()
        || command
            .authorization
            .authorization_occurred_at
            .trim()
            .is_empty()
    {
        return Err(ServiceError::invalid_operation(
            "primary milestone command is incomplete",
        ));
    }
    validate_authorization_action(
        &command.authorization,
        MILESTONE_PRIMARY_AUTHORIZATION_ACTION,
    )?;
    Ok(())
}

fn validate_readiness_command(command: &ProjectReadinessRequestCommand) -> Result<()> {
    if command.project_id.trim().is_empty()
        || command.milestone_id.trim().is_empty()
        || command.baseline_id.trim().is_empty()
        || command.baseline_revision_id.trim().is_empty()
        || command.release_policy_revision.trim().is_empty()
        || command.idempotency_key.trim().is_empty()
        || command.expected_milestone_version < 1
    {
        return Err(ServiceError::invalid_operation(
            "readiness request is incomplete",
        ));
    }
    if let Some(authenticated_user_id) = command.authenticated_user_id.as_deref() {
        let authorization = &command.authorization;
        if authorization.principal_type != "user"
            || authorization.principal_id != authenticated_user_id
            || authorization.authorization_action != MILESTONE_READINESS_AUTHORIZATION_ACTION
            || authorization.authorization_basis.trim().is_empty()
            || authorization.authorization_event_id.trim().is_empty()
            || authorization.authorization_occurred_at.trim().is_empty()
        {
            return Err(ServiceError::AuthorizationDenied {
                message: "readiness requires an explicit authenticated user authorization event"
                    .to_owned(),
            });
        }
    }
    validate_authorization_action(
        &command.authorization,
        MILESTONE_READINESS_AUTHORIZATION_ACTION,
    )
}

fn validate_release_command(command: &ProjectReleaseRequestCommand) -> Result<()> {
    if command.project_id.trim().is_empty()
        || command.milestone_id.trim().is_empty()
        || command.readiness_snapshot_id.trim().is_empty()
        || command.readiness_digest.trim().is_empty()
        || command.status.trim().is_empty()
        || command.idempotency_key.trim().is_empty()
        || command.expected_milestone_version < 1
    {
        return Err(ServiceError::invalid_operation(
            "release request is incomplete",
        ));
    }
    if command.authorization.principal_type != "agent" {
        return Err(ServiceError::AuthorizationDenied {
            message: "only the bound Project Agent may request a release candidate".to_owned(),
        });
    }
    validate_authorization_action(
        &command.authorization,
        MILESTONE_RELEASE_REQUEST_AUTHORIZATION_ACTION,
    )
}

fn validate_authorization(authorization: &ProjectCommandAuthorization) -> Result<()> {
    if authorization.principal_type.trim().is_empty()
        || authorization.principal_id.trim().is_empty()
        || authorization.correlation_id.trim().is_empty()
        || authorization.authorization_event_id.trim().is_empty()
        || authorization.authorization_basis.trim().is_empty()
        || authorization.authorization_action.trim().is_empty()
        || authorization.authorization_occurred_at.trim().is_empty()
    {
        return Err(ServiceError::invalid_operation(
            "milestone command authorization is incomplete",
        ));
    }
    if !matches!(authorization.principal_type.as_str(), "user" | "agent") {
        return Err(ServiceError::AuthorizationDenied {
            message: "milestone commands accept only user or Project Agent principals".to_owned(),
        });
    }
    Ok(())
}

fn validate_authorization_action(
    authorization: &ProjectCommandAuthorization,
    expected_action: &str,
) -> Result<()> {
    validate_authorization(authorization)?;
    if authorization.authorization_action != expected_action {
        return Err(ServiceError::AuthorizationDenied {
            message: format!("milestone command authorization action must be {expected_action}"),
        });
    }
    Ok(())
}

fn validate_context(
    context: &CommandContext,
    operation: &str,
    project_id: &str,
    authorization: &ProjectCommandAuthorization,
) -> Result<()> {
    if context.operation() != operation
        || context.canonical_scope().scope_type() != CommandScopeType::Project
        || context.canonical_scope().scope_id() != project_id
        || context.principal().principal_type() != authorization.principal_type
        || context.principal().principal_id() != authorization.principal_id
    {
        return Err(ServiceError::invalid_operation(
            "milestone command context does not match its Project authorization",
        ));
    }
    Ok(())
}

fn build_revision(
    command: &ProjectMilestoneDefinitionCommand,
    revision_id: String,
    milestone_id: String,
    base_revision: i64,
    base_revision_id: Option<String>,
    lifecycle: &str,
) -> Result<CreateProjectMilestoneRevision> {
    let canonical = canonical_json(&command.content)
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
    Ok(CreateProjectMilestoneRevision {
        id: revision_id,
        milestone_id,
        expected_milestone_version: if base_revision == 0 {
            1
        } else {
            command.expected_milestone_version
        },
        base_revision,
        base_revision_id,
        lifecycle: lifecycle.to_owned(),
        display_label: Some(
            command
                .display_label
                .clone()
                .unwrap_or_else(|| command.content.name.clone()),
        ),
        outcome: command.content.outcome.clone(),
        included_scope_json: serde_json::to_string(&command.content.included_scope)
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
        excluded_scope_json: serde_json::to_string(&command.content.excluded_scope)
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
        charter_revision_id: command
            .content
            .charter_revision
            .as_ref()
            .map(|reference| reference.revision_id.clone()),
        document_revisions_json: serde_json::to_string(&command.content.document_revisions)
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
        task_selection_json: serde_json::to_string(&command.content.task_ids)
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
        dependencies_json: serde_json::to_string(&command.content.dependencies)
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
        risks_json: serde_json::to_string(&command.content.risks)
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
        acceptance_checks_json: serde_json::to_string(&command.content.acceptance_checks)
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
        evidence_requirements_json: serde_json::to_string(&command.content.evidence_requirements)
            .map_err(|error| {
            ServiceError::invalid_operation(error.to_string())
        })?,
        known_issues_json: serde_json::to_string(&command.content.known_issues)
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
        change_summary: command.change_summary.clone(),
        schema_version: MILESTONE_DEFINITION_SCHEMA.to_owned(),
        render_version: command.render_version.clone(),
        rendered_view: canonical,
        content_digest: canonical_digest_with_schema(MILESTONE_DEFINITION_SCHEMA, &command.content)
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
        rendered_digest: canonical_digest_with_schema(
            MILESTONE_RENDER_SCHEMA,
            &command.rendered_view,
        )
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
        author_type: command.authorization.principal_type.clone(),
        author_id: Some(command.authorization.principal_id.clone()),
        source_refs_json: serde_json::to_string(&command.provenance.source_refs)
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
        created_at: now_rfc3339(),
    })
}

fn build_check_definitions(
    command: &ProjectMilestoneDefinitionCommand,
    milestone_id: &str,
    revision_id: &str,
    created_at: &str,
) -> Vec<CreateProjectMilestoneCheck> {
    if command.lifecycle == MilestoneDefinitionLifecycle::Draft {
        return Vec::new();
    }
    let expected_milestone_version = if command.milestone_id.is_none() {
        1
    } else {
        command.expected_milestone_version
    };
    command
        .content
        .acceptance_checks
        .iter()
        .map(|check| CreateProjectMilestoneCheck {
            id: check.id.clone(),
            project_id: command.project_id.clone(),
            milestone_id: milestone_id.to_owned(),
            definition_revision_id: revision_id.to_owned(),
            expected_milestone_version,
            check_key: check.id.clone(),
            description: check.description.clone(),
            required: check.required,
            source_kind: acceptance_source_kind_name(check.source_kind).to_owned(),
            expected_result: check.expected_result.clone(),
            evidence_required: command
                .content
                .evidence_requirements
                .iter()
                .any(|requirement| requirement.id == check.id && requirement.required),
            created_at: created_at.to_owned(),
            updated_at: created_at.to_owned(),
        })
        .collect()
}

fn acceptance_source_kind_name(value: api_types::AcceptanceCheckSourceKind) -> &'static str {
    match value {
        api_types::AcceptanceCheckSourceKind::TaskValidation => "task_validation",
        api_types::AcceptanceCheckSourceKind::DocumentApproval => "document_approval",
        api_types::AcceptanceCheckSourceKind::Manual => "manual",
        api_types::AcceptanceCheckSourceKind::PolicyWaiver => "policy_waiver",
        api_types::AcceptanceCheckSourceKind::MediaEvidence => "media_evidence",
        api_types::AcceptanceCheckSourceKind::GitRef => "git_ref",
    }
}

fn readiness_input(
    snapshot: &api_types::ReadinessSnapshot,
    command: &ProjectReadinessRequestCommand,
    authorization: &api_types::AuthorizationProvenance,
) -> Result<CreateProjectReadinessSnapshot> {
    let projection_reasons = crate::milestone_runtime::projection_reasons(&snapshot.reasons);
    let reconciliation_projection_json = serde_json::to_string(&projection_reasons.reconciliation)
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
    Ok(CreateProjectReadinessSnapshot {
        id: snapshot.id.clone(),
        project_id: snapshot.project_id.clone(),
        milestone_id: snapshot.milestone_id.clone(),
        definition_revision_id: snapshot.milestone_definition_revision_id.clone(),
        baseline_id: snapshot.baseline_id.clone(),
        baseline_revision_id: snapshot.baseline_revision_id.clone(),
        baseline_digest: snapshot.baseline_digest.clone(),
        release_policy_revision: snapshot.release_policy_revision.clone(),
        release_policy_digest: snapshot.release_policy_digest.clone(),
        input_manifest_json: serde_json::to_string(&snapshot.input_manifest)
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
        event_watermark: snapshot.source_event_watermark.clone(),
        outcome: readiness_result_name(snapshot.result).to_owned(),
        blocking_reasons_json: serde_json::to_string(&snapshot.reasons)
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
        blocker_projection_json: serde_json::to_string(&projection_reasons.blockers)
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
        stale_projection_json: serde_json::to_string(&projection_reasons.stale)
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
        reconciliation_projection_json,
        check_results_json: serde_json::to_string(&snapshot.check_results)
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
        waiver_manifest_json: serde_json::to_string(&snapshot.waiver_ids)
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
        evidence_manifest_json: json!({
            "ids": snapshot.evidence_attachment_ids,
            "digests": snapshot.evidence_digests,
            "availability": snapshot.evidence_availability,
        })
        .to_string(),
        commit_context_json: serde_json::to_string(&snapshot.commit_build_check_context)
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
        computing_policy_revision: snapshot.computing_policy_revision.clone(),
        readiness_digest: snapshot.readiness_digest.clone(),
        principal_type: principal_kind_name(authorization.principal.kind).to_owned(),
        principal_id: authorization.principal.id.clone(),
        authorization_basis: authorization.authorization_basis.clone(),
        authorization_action: authorization.action.clone(),
        authorization_occurred_at: authorization.occurred_at.clone(),
        expected_milestone_version: command.expected_milestone_version,
        explicit_event: authorization.event_id.clone(),
        idempotency_key: command.idempotency_key.clone(),
        created_at: snapshot.computed_at.clone(),
    })
}

fn readiness_result_name(value: api_types::ReadinessResult) -> &'static str {
    match value {
        api_types::ReadinessResult::Ready => "ready",
        api_types::ReadinessResult::Blocked => "blocked",
        api_types::ReadinessResult::Failed => "failed",
        api_types::ReadinessResult::Stale => "stale",
    }
}

fn command_context<T: Serialize>(
    operation: &str,
    idempotency_key: &str,
    authorization: &ProjectCommandAuthorization,
    input: &T,
    action: Option<AgentActionProvenance>,
    project_id: &str,
    versions: BTreeMap<String, i64>,
) -> Result<CommandContext> {
    if idempotency_key.trim().is_empty() {
        return Err(ServiceError::invalid_operation(
            "idempotency key is required",
        ));
    }
    CommandContext::from_authorized_input(
        NewCommandContext {
            principal: CommandPrincipal {
                principal_type: authorization.principal_type.clone(),
                principal_id: authorization.principal_id.clone(),
            },
            canonical_scope: CommandScope {
                scope_type: CommandScopeType::Project,
                scope_id: project_id.to_owned(),
            },
            operation: operation.to_owned(),
            idempotency_key: idempotency_key.to_owned(),
            expected_state: ExpectedCommandState {
                versions,
                digests: BTreeMap::new(),
            },
            authorization_provenance: Some(AuthorizationProvenance {
                policy_result: authorization.policy_result.clone(),
                policy_revision: authorization.policy_revision.clone(),
                policy_digest: authorization.policy_digest.clone(),
                requested_permission: authorization.requested_permission.clone(),
            }),
            action_provenance: action,
            correlation_id: authorization.correlation_id.clone(),
            causation_id: authorization.causation_id.clone(),
            causation_depth: authorization.causation_depth,
        },
        input,
    )
    .map_err(|error| ServiceError::invalid_operation(format!("milestone command digest: {error}")))
}

fn command_bundle(
    context: &CommandContext,
    outcome_json: &str,
) -> (CreateCommandReceipt, Option<CreateAgentActionExecution>) {
    let mut receipt = create_receipt(context, outcome_json);
    let execution = context.action_provenance.as_ref().map(|provenance| {
        let now = now_rfc3339();
        CreateAgentActionExecution {
            id: new_uuid_v4(),
            action_id: provenance.action_id.clone(),
            expected_action_version: provenance.expected_action_version,
            attempt: provenance.attempt,
            status: AgentActionExecutionStatus::Succeeded,
            result_json: Some(outcome_json.to_owned()),
            error: None,
            executed_by_type: provenance.executed_by_type.clone(),
            executed_by_id: provenance.executed_by_id.clone(),
            idempotency_key: provenance.execution_idempotency_key.clone(),
            action_status: AgentActionStatus::Executed,
            action_outcome_json: Some(outcome_json.to_owned()),
            created_at: now.clone(),
            completed_at: Some(now.clone()),
            updated_at: now,
        }
    });
    if let Some(execution) = execution.as_ref() {
        receipt.agent_action_execution_id = Some(execution.id.clone());
    }
    (receipt, execution)
}

fn create_receipt(context: &CommandContext, outcome_json: &str) -> CreateCommandReceipt {
    CreateCommandReceipt {
        id: new_uuid_v4(),
        principal_type: context.principal().principal_type().to_owned(),
        principal_id: context.principal().principal_id().to_owned(),
        scope_type: context.canonical_scope().scope_type().as_str().to_owned(),
        scope_id: context.canonical_scope().scope_id().to_owned(),
        operation: context.operation().to_owned(),
        idempotency_key: context.idempotency_key().to_owned(),
        input_digest: context.input_digest().to_owned(),
        policy_result: context
            .authorization_provenance
            .as_ref()
            .map_or_else(|| "allowed".to_owned(), |value| value.policy_result.clone()),
        correlation_id: context.correlation_id().to_owned(),
        causation_id: context.causation_id.clone(),
        causation_depth: context.causation_depth,
        event_id: String::new(),
        agent_action_execution_id: None,
        outcome_json: outcome_json.to_owned(),
        committed_at: now_rfc3339(),
    }
}

async fn replay_receipt(db: &SqliteDb, context: &CommandContext) -> Result<Option<CommandReceipt>> {
    let exact = CommandReceiptRepo::get_command_receipt(
        db,
        context.principal().principal_type(),
        context.principal().principal_id(),
        context.canonical_scope().scope_type().as_str(),
        context.canonical_scope().scope_id(),
        context.operation(),
        context.idempotency_key(),
        context.input_digest(),
    )
    .await?;
    if exact.is_some() {
        return Ok(exact);
    }
    let existing_digest: Option<String> = sqlx::query_scalar(
        "SELECT input_digest FROM command_receipt
         WHERE scope_type = ? AND scope_id = ? AND operation = ?
           AND idempotency_key = ? LIMIT 1",
    )
    .bind(context.canonical_scope().scope_type().as_str())
    .bind(context.canonical_scope().scope_id())
    .bind(context.operation())
    .bind(context.idempotency_key())
    .fetch_optional(db.pool())
    .await?;
    if existing_digest.is_some_and(|digest| digest != context.input_digest()) {
        return Err(ServiceError::Db(db::DbError::IdempotencyConflict));
    }
    Ok(None)
}

async fn authorize_project_principal(
    db: &SqliteDb,
    project_id: &str,
    authorization: &ProjectCommandAuthorization,
) -> Result<()> {
    match authorization.principal_type.as_str() {
        "user" => {
            let project = ProjectRepo::get_by_id(db, project_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("project", project_id))?;
            if project.owner_id.as_deref() == Some(authorization.principal_id.as_str())
                || ProjectMemberRepo::get_member(db, project_id, &authorization.principal_id)
                    .await?
                    .is_some()
            {
                Ok(())
            } else {
                Err(ServiceError::AuthorizationDenied {
                    message: "principal is not a member of the Project".to_owned(),
                })
            }
        }
        "agent" => {
            let bound: Option<String> = sqlx::query_scalar(
                "SELECT identity_id FROM project_agent_binding
                 WHERE project_id = ? AND identity_id = ? AND state = 'active' LIMIT 1",
            )
            .bind(project_id)
            .bind(&authorization.principal_id)
            .fetch_optional(db.pool())
            .await?;
            if bound.is_some() {
                Ok(())
            } else {
                Err(ServiceError::AuthorizationDenied {
                    message: "agent principal is not actively bound to the Project".to_owned(),
                })
            }
        }
        _ => Err(ServiceError::AuthorizationDenied {
            message: "milestone commands accept only user or Project Agent principals".to_owned(),
        }),
    }
}

async fn validate_definition_references(
    db: &SqliteDb,
    project_id: &str,
    content: &MilestoneDefinitionContent,
) -> Result<()> {
    if let Some(charter) = content.charter_revision.as_ref() {
        validate_charter_reference(db, project_id, charter).await?;
    }
    for document in &content.document_revisions {
        validate_document_reference(db, project_id, document).await?;
    }
    for task_id in &content.task_ids {
        if task_id.trim().is_empty() {
            return Err(ServiceError::invalid_operation(
                "milestone Task references must be non-empty",
            ));
        }
        let owned: Option<i64> =
            sqlx::query_scalar("SELECT 1 FROM task WHERE id = ? AND project_id = ? LIMIT 1")
                .bind(task_id)
                .bind(project_id)
                .fetch_optional(db.pool())
                .await?;
        if owned.is_none() {
            return Err(ServiceError::invalid_operation(
                "milestone Task reference is missing or belongs to another Project",
            ));
        }
    }
    Ok(())
}

async fn validate_charter_reference(
    db: &SqliteDb,
    project_id: &str,
    reference: &api_types::ArtifactRef,
) -> Result<()> {
    validate_artifact_ref_shape(reference, "milestone Charter reference")?;
    let row = sqlx::query(
        "SELECT c.id AS artifact_id, c.current_approved_revision_id,
                r.id AS revision_id, r.lifecycle, r.content_digest,
                r.render_version, r.rendered_digest
         FROM project_charter_revision r
         JOIN project_charter c ON c.id = r.charter_id
         WHERE r.id = ? AND r.charter_id = ? AND c.project_id = ?
         LIMIT 1",
    )
    .bind(&reference.revision_id)
    .bind(&reference.artifact_id)
    .bind(project_id)
    .fetch_optional(db.pool())
    .await?
    .ok_or_else(|| {
        ServiceError::invalid_operation(
            "milestone Charter revision is missing or belongs to another Project",
        )
    })?;
    let current_approved_revision_id: Option<String> =
        row.try_get("current_approved_revision_id")?;
    let lifecycle: String = row.try_get("lifecycle")?;
    if lifecycle != "approved"
        || current_approved_revision_id.as_deref() != Some(reference.revision_id.as_str())
    {
        return Err(ServiceError::Db(db::DbError::VersionConflict));
    }
    validate_artifact_ref_digests(reference, &row, "milestone Charter reference")
}

async fn validate_document_reference(
    db: &SqliteDb,
    project_id: &str,
    reference: &api_types::ArtifactRef,
) -> Result<()> {
    validate_artifact_ref_shape(reference, "milestone Document reference")?;
    let row = sqlx::query(
        "SELECT d.id AS artifact_id, r.id AS revision_id, r.content_digest,
                r.render_version, r.rendered_digest
         FROM project_document_revision r
         JOIN project_document d ON d.id = r.document_id
         WHERE r.id = ? AND r.document_id = ? AND d.project_id = ?
         LIMIT 1",
    )
    .bind(&reference.revision_id)
    .bind(&reference.artifact_id)
    .bind(project_id)
    .fetch_optional(db.pool())
    .await?
    .ok_or_else(|| {
        ServiceError::invalid_operation(
            "milestone Document revision is missing or belongs to another Project",
        )
    })?;
    validate_artifact_ref_digests(reference, &row, "milestone Document reference")
}

fn validate_artifact_ref_shape(reference: &api_types::ArtifactRef, label: &str) -> Result<()> {
    if reference.artifact_id.trim().is_empty()
        || reference.revision_id.trim().is_empty()
        || reference.content_digest.trim().is_empty()
        || reference
            .render_version
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        || reference
            .render_digest
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
    {
        return Err(ServiceError::invalid_operation(format!(
            "{label} is incomplete"
        )));
    }
    Ok(())
}

fn validate_artifact_ref_digests(
    reference: &api_types::ArtifactRef,
    row: &sqlx::sqlite::SqliteRow,
    label: &str,
) -> Result<()> {
    let content_digest: String = row.try_get("content_digest")?;
    let render_version: String = row.try_get("render_version")?;
    let rendered_digest: String = row.try_get("rendered_digest")?;
    if reference.content_digest != content_digest
        || reference
            .render_version
            .as_deref()
            .is_some_and(|value| value != render_version)
        || reference
            .render_digest
            .as_deref()
            .is_some_and(|value| value != rendered_digest)
    {
        return Err(ServiceError::Db(db::DbError::VersionConflict));
    }
    if row
        .try_get::<String, _>("revision_id")
        .ok()
        .is_some_and(|revision_id| revision_id != reference.revision_id)
        || row
            .try_get::<String, _>("artifact_id")
            .ok()
            .is_some_and(|artifact_id| artifact_id != reference.artifact_id)
    {
        return Err(ServiceError::Conflict(format!(
            "{label} does not match its persisted ArtifactRef"
        )));
    }
    Ok(())
}

async fn current_project_version(db: &SqliteDb, project_id: &str) -> Result<i64> {
    sqlx::query_scalar("SELECT version FROM project WHERE id = ?")
        .bind(project_id)
        .fetch_optional(db.pool())
        .await?
        .ok_or_else(|| ServiceError::not_found("project", project_id.to_owned()))
}

fn frozen_project_value(
    project: &Project,
    primary_milestone_id: Option<&str>,
    version: i64,
    updated_at: &str,
) -> Value {
    json!({
        "id": project.id,
        "name": project.name,
        "settings": project.settings,
        "workflow_definition": project.workflow_definition,
        "workflow_template_name": project.workflow_template_name,
        "primary_repo_id": project.primary_repo_id,
        "paused_at": project.paused_at,
        "owner_id": project.owner_id,
        "project_hooks_json": project.project_hooks_json,
        "project_work_epoch": project.project_work_epoch,
        "charter_status": project.charter_status,
        "charter_setup_required": project.charter_setup_required,
        "current_charter_id": project.current_charter_id,
        "current_charter_revision_id": project.current_charter_revision_id,
        "current_charter_version": project.current_charter_version,
        "primary_milestone_id": primary_milestone_id,
        "version": version,
        "created_at": project.created_at,
        "updated_at": updated_at,
    })
}

fn project_from_value(value: &Value) -> Result<Project> {
    Ok(Project {
        id: required_value(value, "id")?,
        name: required_value(value, "name")?,
        settings: required_value(value, "settings")?,
        workflow_definition: required_value(value, "workflow_definition")?,
        workflow_template_name: optional_value_string(value, "workflow_template_name")?,
        primary_repo_id: optional_value_string(value, "primary_repo_id")?,
        paused_at: optional_value_string(value, "paused_at")?,
        owner_id: optional_value_string(value, "owner_id")?,
        project_hooks_json: required_value(value, "project_hooks_json")?,
        project_work_epoch: required_value_i64(value, "project_work_epoch")?,
        charter_status: required_value(value, "charter_status")?,
        charter_setup_required: required_value_bool(value, "charter_setup_required")?,
        current_charter_id: optional_value_string(value, "current_charter_id")?,
        current_charter_revision_id: optional_value_string(value, "current_charter_revision_id")?,
        current_charter_version: required_value_i64(value, "current_charter_version")?,
        primary_milestone_id: optional_value_string(value, "primary_milestone_id")?,
        version: required_value_i64(value, "version")?,
        created_at: required_value(value, "created_at")?,
        updated_at: required_value(value, "updated_at")?,
    })
}

fn optional_value_string(value: &Value, field: &str) -> Result<Option<String>> {
    match value.get(field) {
        Some(Value::Null) | None => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        _ => Err(ServiceError::Conflict(format!(
            "frozen Project outcome has invalid {field}"
        ))),
    }
}

fn required_value_i64(value: &Value, field: &str) -> Result<i64> {
    value
        .get(field)
        .and_then(Value::as_i64)
        .ok_or_else(|| ServiceError::Conflict(format!("frozen Project outcome has no {field}")))
}

fn required_value_bool(value: &Value, field: &str) -> Result<bool> {
    value
        .get(field)
        .and_then(Value::as_bool)
        .ok_or_else(|| ServiceError::Conflict(format!("frozen Project outcome has no {field}")))
}

fn native_authorization(
    action: &AgentAction,
    context: &CommandContext,
    authorization_action: &str,
) -> ProjectCommandAuthorization {
    let provenance = context.authorization_provenance.as_ref();
    let authorization_event_id = context
        .action_provenance
        .as_ref()
        .map(|value| value.action_id.clone())
        .unwrap_or_else(|| context.correlation_id().to_owned());
    let authorization_basis = "project_agent_binding_policy";
    ProjectCommandAuthorization {
        principal_type: context.principal().principal_type().to_owned(),
        principal_id: context.principal().principal_id().to_owned(),
        policy_result: provenance.map_or_else(
            || action.policy_result.to_string(),
            |value| value.policy_result.clone(),
        ),
        policy_revision: provenance.and_then(|value| value.policy_revision.clone()),
        policy_digest: provenance.and_then(|value| value.policy_digest.clone()),
        requested_permission: provenance.and_then(|value| value.requested_permission.clone()),
        correlation_id: context.correlation_id().to_owned(),
        causation_id: context.causation_id.clone(),
        causation_depth: context.causation_depth,
        authorization_event_id,
        authorization_basis: authorization_basis.to_owned(),
        authorization_action: authorization_action.to_owned(),
        authorization_occurred_at: action.created_at.clone(),
        authorization_json: json!({
            "principal": {
                "kind": context.principal().principal_type(),
                "id": context.principal().principal_id(),
            },
            "authorization_basis": authorization_basis,
            "action": authorization_action,
            "event_id": context
                .action_provenance
                .as_ref()
                .map(|value| value.action_id.as_str())
                .unwrap_or_else(|| context.correlation_id()),
            "correlation_id": context.correlation_id(),
            "causation_id": context.causation_id,
            "occurred_at": action.created_at,
        })
        .to_string(),
    }
}

fn build_revision_provenance() -> RevisionProvenance {
    RevisionProvenance {
        author: api_types::PrincipalRef {
            kind: api_types::PrincipalKind::Agent,
            id: String::new(),
            display_name: None,
        },
        source_refs: Vec::new(),
        change_summary: "Project Agent authored a typed milestone definition".to_owned(),
        profile_revision: None,
        operating_skill_revision: None,
        material_diff: None,
    }
}

fn native_revision_provenance(
    action: &AgentAction,
    context: &CommandContext,
) -> RevisionProvenance {
    RevisionProvenance {
        author: api_types::PrincipalRef {
            kind: api_types::PrincipalKind::Agent,
            id: context.principal().principal_id().to_owned(),
            display_name: None,
        },
        profile_revision: None,
        operating_skill_revision: None,
        source_refs: vec![api_types::ProvenanceRef {
            source_kind: api_types::ProvenanceSourceKind::ProjectChat,
            source_id: context
                .action_provenance
                .as_ref()
                .map(|value| value.action_id.clone())
                .unwrap_or_else(|| context.correlation_id().to_owned()),
            revision_id: None,
            digest: None,
            label: None,
            observed_at: Some(action.created_at.clone()),
        }],
        change_summary: "Project Agent authored a typed milestone definition".to_owned(),
        material_diff: None,
    }
}

fn principal_kind_name(value: api_types::PrincipalKind) -> &'static str {
    match value {
        api_types::PrincipalKind::User => "user",
        api_types::PrincipalKind::Agent => "agent",
        api_types::PrincipalKind::Worker => "worker",
        api_types::PrincipalKind::Reviewer => "reviewer",
        api_types::PrincipalKind::Service => "service",
        api_types::PrincipalKind::System => "system",
    }
}

fn value_string(payload: &Value, field: &str) -> Result<String> {
    payload
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| ServiceError::invalid_operation(format!("{field} is required")))
}

fn optional_string(payload: &Value, field: &str) -> Option<String> {
    payload
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
}

fn value_i64(payload: &Value, field: &str) -> Result<i64> {
    payload
        .get(field)
        .and_then(Value::as_i64)
        .ok_or_else(|| ServiceError::invalid_operation(format!("{field} is required")))
}

fn integer_or_zero(payload: &Value, field: &str) -> i64 {
    payload.get(field).and_then(Value::as_i64).unwrap_or(0)
}

fn value<T: serde::de::DeserializeOwned>(payload: &Value, field: &str) -> Result<T> {
    serde_json::from_value(
        payload
            .get(field)
            .cloned()
            .ok_or_else(|| ServiceError::invalid_operation(format!("{field} is required")))?,
    )
    .map_err(|error| ServiceError::invalid_operation(format!("invalid {field}: {error}")))
}

fn outcome_string(receipt: &CommandReceipt, field: &str) -> Result<String> {
    let outcome: Value = serde_json::from_str(&receipt.outcome_json).map_err(|_| {
        ServiceError::Conflict("milestone command receipt outcome is invalid".to_owned())
    })?;
    required_value(&outcome, field)
}

fn required_value(value: &Value, field: &str) -> Result<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| ServiceError::Conflict(format!("command outcome has no {field}")))
}

fn map_readiness(row: sqlx::sqlite::SqliteRow) -> Result<ProjectReadinessSnapshotRecord> {
    Ok(ProjectReadinessSnapshotRecord {
        id: row.try_get("id")?,
        project_id: row.try_get("project_id")?,
        milestone_id: row.try_get("milestone_id")?,
        definition_revision_id: row.try_get("definition_revision_id")?,
        baseline_id: row.try_get("baseline_id")?,
        baseline_revision_id: row.try_get("baseline_revision_id")?,
        baseline_digest: row.try_get("baseline_digest")?,
        release_policy_revision: row.try_get("release_policy_revision")?,
        release_policy_digest: row.try_get("release_policy_digest")?,
        input_manifest_json: row.try_get("input_manifest_json")?,
        event_watermark: row.try_get("event_watermark")?,
        outcome: row.try_get("outcome")?,
        blocking_reasons_json: row.try_get("blocking_reasons_json")?,
        check_results_json: row.try_get("check_results_json")?,
        waiver_manifest_json: row.try_get("waiver_manifest_json")?,
        evidence_manifest_json: row.try_get("evidence_manifest_json")?,
        commit_context_json: row.try_get("commit_context_json")?,
        computing_policy_revision: row.try_get("computing_policy_revision")?,
        readiness_digest: row.try_get("readiness_digest")?,
        principal_type: row.try_get("principal_type")?,
        principal_id: row.try_get("principal_id")?,
        authorization_basis: row.try_get("authorization_basis")?,
        authorization_action: row.try_get("authorization_action")?,
        authorization_occurred_at: row.try_get("authorization_occurred_at")?,
        expected_milestone_version: row.try_get("expected_milestone_version")?,
        explicit_event: row.try_get("explicit_event")?,
        idempotency_key: row.try_get("idempotency_key")?,
        created_at: row.try_get("created_at")?,
    })
}

impl ProjectMilestoneDefinitionCommand {
    #[must_use]
    pub fn native_provenance() -> RevisionProvenance {
        build_revision_provenance()
    }
}
