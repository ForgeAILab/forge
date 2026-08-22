//! Shared Project Charter command boundary.
//!
//! A Project Agent may propose a Charter revision and an authenticated Project
//! owner/admin may approve one.  Both transports enter this service before
//! any Charter, Project, binding, identity, or server-id allocation is read.
//! The durable command receipt is therefore the replay boundary, while the
//! SQLite repository owns the final domain transaction.

use std::{collections::BTreeMap, sync::Arc};

use api_types::{ProductMaturity, ProjectCharterContent, ProjectMode, RevisionProvenance};
use chrono::{DateTime, Utc};
use db::{
    new_uuid_v4, now_rfc3339, AgentActionExecutionStatus, AgentActionStatus,
    ApplyProjectCharterApprovalCommand, CommandReceipt, CommandReceiptRepo,
    CreateAgentActionExecution, CreateCommandReceipt, CreateProjectCharter,
    CreateProjectCharterRevision, CreateProjectCharterRevisionAtomically,
    FinalizeProjectCharterRevisionNoop, ProjectAgentBindingRepo, ProjectCharterApprovalRecord,
    ProjectCharterRecord, ProjectCharterRevisionRecord, ProjectMemberRepo,
    ProjectOrchestrationRepo, ProjectRepo, SqliteDb,
};
use forge_agent_host::PROJECT_CHARTER_ADOPTION_OPERATION;
use serde::Serialize;
use serde_json::{json, Value};

use crate::{
    charter_content_digest, charter_render_digest, current_project_agent_operating_skill_revision,
    evaluate_project_charter_readiness, project_agent_policy_digest, render_and_digest_charter,
    semantic_revision_diff, AgentActionProvenance, AuthorizationProvenance, CommandContext,
    CommandPrincipal, CommandScope, CommandScopeType, ExpectedCommandState, NewCommandContext,
    ProjectCommandAuthorization, Result, ServiceError, CHARTER_READINESS_POLICY_VERSION,
    PROJECT_OPERATING_SKILL_KEY,
};

/// The command name for an explicit user/system adoption or amendment.  The
/// Project Agent proposal operation remains `project.charter.adoption` so a
/// direct user approval cannot collide with an unapproved draft command.
pub const PROJECT_CHARTER_APPROVAL_COMMAND: &str = "project.charter.approval";

const ADOPTION_BOOTSTRAP_GUARD_SCHEMA: &str = "forge.project-charter-adoption/v1";
const PROJECT_CHARTER_SCHEMA_VERSION: &str = "forge.project-charter/v1";
const PROJECT_AGENT_POLICY_REVISION: &str = "forge.project-agent-policy/v1";
const MAX_AUTHORIZATION_CLOCK_SKEW_SECONDS: i64 = 48 * 60 * 60;

#[derive(Debug, Clone, Serialize)]
pub struct ProjectCharterRevisionCommand {
    pub project_id: String,
    pub charter_id: String,
    pub base_revision_id: Option<String>,
    pub expected_digest: Option<String>,
    pub project_mode: String,
    pub maturity: String,
    pub content: ProjectCharterContent,
    pub rendered_view: Option<String>,
    pub render_version: Option<String>,
    pub provenance: RevisionProvenance,
    pub expected_charter_version: i64,
    pub idempotency_key: String,
    pub authorization: ProjectCommandAuthorization,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectCharterApprovalCommand {
    pub project_id: String,
    pub charter_id: String,
    pub revision_id: String,
    pub content_digest: String,
    pub rendered_digest: String,
    pub expected_charter_version: i64,
    pub expected_project_version: i64,
    pub approved_project_name: String,
    pub approved_project_slug: Option<String>,
    pub project_mode: String,
    pub selected_project_agent_identity_id: String,
    pub selected_project_agent_profile_revision_id: String,
    pub selected_project_agent_operating_skill_revision: String,
    pub selected_project_agent_policy_digest: String,
    pub idempotency_key: String,
    pub authorization: ProjectCommandAuthorization,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectCharterRevisionCommandOutcome {
    pub revision: ProjectCharterRevisionRecord,
    pub charter_version: i64,
    pub readiness: api_types::ProjectCharterReadiness,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectCharterApprovalCommandOutcome {
    pub approval: ProjectCharterApprovalRecord,
    pub project_id: String,
    pub project_version: i64,
    pub project_charter_id: String,
    pub project_charter_revision_id: String,
    pub project_agent_binding_id: String,
    pub project_chat_id: String,
    pub bootstrap_message_id: Option<String>,
    pub amendment_id: Option<String>,
}

#[derive(Clone)]
pub struct ProjectCharterCommandService {
    db: Arc<SqliteDb>,
}

impl ProjectCharterCommandService {
    #[must_use]
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self { db }
    }

    /// Persist one Project Agent Charter draft/proposal.  The optional action
    /// provenance makes the same command usable by the native action
    /// executor while the REST/user path can omit it.
    pub async fn save_revision(
        &self,
        command: ProjectCharterRevisionCommand,
        action: Option<AgentActionProvenance>,
    ) -> Result<ProjectCharterRevisionCommandOutcome> {
        validate_revision_envelope(&command)?;
        let context = revision_context(&command, action)?;
        self.save_revision_with_context(command, context).await
    }

    pub(crate) async fn save_revision_with_context(
        &self,
        command: ProjectCharterRevisionCommand,
        context: CommandContext,
    ) -> Result<ProjectCharterRevisionCommandOutcome> {
        validate_revision_envelope(&command)?;
        validate_context(
            &context,
            &command.project_id,
            &command.authorization,
            PROJECT_CHARTER_ADOPTION_OPERATION,
        )?;

        if let Some(receipt) = self.replay_receipt(&context).await? {
            return self.replay_revision(receipt).await;
        }

        validate_revision_command(&command)?;
        self.authorize_fresh_principal(&command.project_id, &command.authorization, false)
            .await?;
        let project = ProjectRepo::get_by_id(&*self.db, &command.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", command.project_id.clone()))?;
        let account_id = if let Some(owner_id) = project.owner_id.clone() {
            owner_id
        } else if command.authorization.principal_type == "agent" {
            sqlx::query_scalar::<_, String>(
                "SELECT owner_id FROM agent_identity WHERE id = ? LIMIT 1",
            )
            .bind(&command.authorization.principal_id)
            .fetch_optional(self.db.pool())
            .await?
            .ok_or_else(|| ServiceError::AuthorizationDenied {
                message: "agent principal has no canonical account owner".to_owned(),
            })?
        } else {
            command.authorization.principal_id.clone()
        };

        let charter =
            ProjectOrchestrationRepo::get_project_adoption_charter(&*self.db, &command.project_id)
                .await?;
        let charter_id = charter.as_ref().map_or_else(
            || {
                if command.charter_id.trim().is_empty() {
                    new_uuid_v4()
                } else {
                    command.charter_id.clone()
                }
            },
            |value| value.id.clone(),
        );
        if charter
            .as_ref()
            .is_some_and(|value| !command.charter_id.is_empty() && value.id != command.charter_id)
        {
            return Err(ServiceError::invalid_operation(
                "Charter command target does not match the Project Charter",
            ));
        }

        let project_setup = project.charter_status == "legacy_unverified"
            && project.charter_setup_required
            && project.current_charter_id.is_none()
            && project.current_charter_revision_id.is_none();
        if charter.is_none() && !project_setup {
            return Err(ServiceError::conflict(
                "Project Charter revision requires a setup Project or an existing Project Charter",
            ));
        }

        let render = render_and_digest_charter(&command.content);
        if command
            .rendered_view
            .as_deref()
            .is_some_and(|value| value != render.rendered_view)
            || command
                .render_version
                .as_deref()
                .is_some_and(|value| value != render.render_version)
        {
            return Err(ServiceError::conflict(
                "Charter revision render does not match the server renderer",
            ));
        }
        // Preserve the public first-draft retry contract even when a client
        // supplies a new idempotency key after losing the original response.
        // This is a semantic no-op: it returns the one committed draft only
        // when every immutable revision field matches exactly. Same-key
        // replay is still resolved by the durable receipt above, and any
        // changed field continues into the normal stale-version conflict.
        if let Some(existing_charter) = charter.as_ref() {
            if command.expected_charter_version == 1
                && existing_charter.version > 1
                && command.base_revision_id.is_none()
            {
                if let Some(draft_id) = existing_charter.current_draft_revision_id.as_deref() {
                    if let Some(draft) =
                        ProjectOrchestrationRepo::get_project_charter_revision(&*self.db, draft_id)
                            .await?
                    {
                        let source_refs_json = serde_json::to_string(
                            &command.provenance.source_refs,
                        )
                        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
                        let exact_replay = draft.charter_id == existing_charter.id
                            && draft.author_type == command.authorization.principal_type
                            && draft.author_id.as_deref()
                                == Some(command.authorization.principal_id.as_str())
                            && draft.base_revision == 0
                            && draft.base_revision_id.is_none()
                            && existing_charter.project_mode == command.project_mode
                            && existing_charter.maturity == command.maturity
                            && draft.content_digest == render.content_digest
                            && draft.rendered_digest == render.render_digest
                            && draft.render_version == render.render_version
                            && draft.rendered_view == render.rendered_view
                            && draft.source_refs_json == source_refs_json
                            && draft.change_summary == command.provenance.change_summary;
                        if exact_replay {
                            let readiness = evaluate_project_charter_readiness(
                                &command.content,
                                parse_project_mode(&command.project_mode)?,
                                parse_maturity(&command.maturity)?,
                                CHARTER_READINESS_POLICY_VERSION,
                                &draft.created_at,
                            );
                            let result_json = serde_json::to_string(&json!({
                                "operation": PROJECT_CHARTER_ADOPTION_OPERATION,
                                "project_id": command.project_id.clone(),
                                "charter_id": existing_charter.id.clone(),
                                "revision_id": draft.id.clone(),
                                "revision": draft.revision,
                                "charter_version": existing_charter.version,
                                "content_digest": render.content_digest.clone(),
                                "render_digest": render.render_digest.clone(),
                                "readiness": readiness.clone(),
                                "domain_committed": true,
                                "requires_user_authorization": true,
                                "semantic_noop": true,
                            }))
                            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
                            let (mut receipt, execution) = command_bundle(&context, &result_json);
                            if let Some(execution) = execution.as_ref() {
                                receipt.agent_action_execution_id = Some(execution.id.clone());
                            }
                            let revision =
                                ProjectOrchestrationRepo::finalize_project_charter_revision_noop(
                                    &*self.db,
                                    FinalizeProjectCharterRevisionNoop {
                                        account_id: account_id.clone(),
                                        project_id: command.project_id.clone(),
                                        charter_id: existing_charter.id.clone(),
                                        revision_id: draft.id.clone(),
                                        content_digest: render.content_digest.clone(),
                                        rendered_digest: render.render_digest.clone(),
                                        command_receipt: receipt,
                                        action_execution: execution,
                                    },
                                )
                                .await?;
                            return Ok(ProjectCharterRevisionCommandOutcome {
                                revision,
                                charter_version: existing_charter.version,
                                readiness,
                            });
                        }
                    }
                }
            }
        }
        let (expected_version, mode, maturity, base_revision) = if let Some(charter) = charter {
            if charter.project_mode != command.project_mode || charter.maturity != command.maturity
            {
                return Err(ServiceError::conflict(
                    "Project Charter mode and maturity are immutable after draft creation",
                ));
            }
            if command.expected_charter_version != 0
                && command.expected_charter_version != charter.version
            {
                return Err(ServiceError::conflict(format!(
                    "the Project Charter changed before this revision was saved; expected version {} but current version is {}",
                    command.expected_charter_version, charter.version
                )));
            }
            let (base, base_id) = if let Some(base_id) = command.base_revision_id.as_deref() {
                let base =
                    ProjectOrchestrationRepo::get_project_charter_revision(&*self.db, base_id)
                        .await?
                        .ok_or_else(|| {
                            ServiceError::not_found("project_charter_revision", base_id)
                        })?;
                let is_current_base = charter.current_draft_revision_id.as_deref() == Some(base_id)
                    || (charter.current_draft_revision_id.is_none()
                        && charter.current_approved_revision_id.as_deref() == Some(base_id));
                if base.charter_id != charter.id || !is_current_base {
                    return Err(ServiceError::Db(db::DbError::VersionConflict));
                }
                if command
                    .expected_digest
                    .as_deref()
                    .is_some_and(|expected| expected != base.content_digest)
                {
                    return Err(ServiceError::conflict(
                        "the current Charter draft digest changed before this revision was saved",
                    ));
                }
                (base.revision, Some(base_id.to_owned()))
            } else {
                if charter.current_approved_revision_id.is_some() {
                    return Err(ServiceError::conflict(
                        "an amendment revision must identify the current approved base revision",
                    ));
                }
                if command
                    .expected_digest
                    .as_deref()
                    .is_some_and(|expected| !expected.is_empty())
                {
                    return Err(ServiceError::conflict(
                        "the first Charter revision cannot include a base digest",
                    ));
                }
                if charter.current_draft_revision_id.is_some() {
                    return Err(ServiceError::Db(db::DbError::VersionConflict));
                }
                (0, None)
            };
            (
                expected_version_with_default(command.expected_charter_version, charter.version),
                mode_string(&command.project_mode)?,
                maturity_string(&command.maturity)?,
                (base, base_id),
            )
        } else {
            if !matches!(command.expected_charter_version, 0 | 1)
                || command.base_revision_id.is_some()
            {
                return Err(ServiceError::Db(db::DbError::VersionConflict));
            }
            (
                1,
                mode_string(&command.project_mode)?,
                maturity_string(&command.maturity)?,
                (0, None),
            )
        };
        let (base_revision_number, base_revision_id) = base_revision;
        let revision_id = new_uuid_v4();
        let created_at = now_rfc3339();
        let source_refs_json = serde_json::to_string(&command.provenance.source_refs)
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        let revision_input = CreateProjectCharterRevision {
            id: revision_id.clone(),
            charter_id: charter_id.clone(),
            expected_charter_version: expected_version,
            project_mode: mode,
            maturity,
            base_revision: base_revision_number,
            base_revision_id,
            lifecycle: "proposed".to_owned(),
            schema_version: PROJECT_CHARTER_SCHEMA_VERSION.to_owned(),
            render_version: render.render_version.clone(),
            content_json: api_types::canonical_json(&command.content)
                .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
            rendered_view: render.rendered_view.clone(),
            change_summary: command.provenance.change_summary.clone(),
            author_type: command.authorization.principal_type.clone(),
            author_id: Some(command.authorization.principal_id.clone()),
            source_message_id: None,
            source_turn_job_id: None,
            source_refs_json,
            content_digest: render.content_digest.clone(),
            rendered_digest: render.render_digest.clone(),
            created_at: created_at.clone(),
            command_receipt: None,
            action_execution: None,
        };
        let readiness = evaluate_project_charter_readiness(
            &command.content,
            parse_project_mode(&command.project_mode)?,
            parse_maturity(&command.maturity)?,
            CHARTER_READINESS_POLICY_VERSION,
            &created_at,
        );
        let revision_number =
            ProjectOrchestrationRepo::list_project_charter_revisions(&*self.db, &charter_id)
                .await?
                .last()
                .map_or(1, |value| value.revision + 1);
        let result_json = serde_json::to_string(&json!({
            "operation": PROJECT_CHARTER_ADOPTION_OPERATION,
            "project_id": command.project_id,
            "charter_id": charter_id,
            "revision_id": revision_id,
            "revision": revision_number,
            "charter_version": expected_version + 1,
            "content_digest": render.content_digest,
            "render_digest": render.render_digest,
            "readiness": readiness,
            "domain_committed": true,
            "requires_user_authorization": true,
        }))
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        let (mut receipt, execution) = command_bundle(&context, &result_json);
        if let Some(execution) = execution.as_ref() {
            receipt.agent_action_execution_id = Some(execution.id.clone());
        }
        let revision = if project_setup && command.base_revision_id.is_none() {
            ProjectOrchestrationRepo::create_project_charter_revision_atomically(
                &*self.db,
                CreateProjectCharterRevisionAtomically {
                    project_id: Some(command.project_id.clone()),
                    genesis_session_id: None,
                    account_id: account_id.clone(),
                    charter: CreateProjectCharter {
                        id: charter_id.clone(),
                        account_id: account_id.clone(),
                        genesis_session_id: None,
                        project_mode: command.project_mode.clone(),
                        maturity: command.maturity.clone(),
                        created_at: created_at.clone(),
                        updated_at: created_at.clone(),
                    },
                    revision: CreateProjectCharterRevision {
                        command_receipt: Some(receipt.clone()),
                        action_execution: execution.clone(),
                        ..revision_input
                    },
                    command_receipt: Some(receipt),
                    action_execution: execution,
                },
            )
            .await?
        } else {
            ProjectOrchestrationRepo::create_project_charter_revision(
                &*self.db,
                CreateProjectCharterRevision {
                    command_receipt: Some(receipt),
                    action_execution: execution,
                    ..revision_input
                },
            )
            .await?
        };
        let charter_version: i64 =
            sqlx::query_scalar("SELECT version FROM project_charter WHERE id = ? LIMIT 1")
                .bind(&revision.charter_id)
                .fetch_one(self.db.pool())
                .await?;
        Ok(ProjectCharterRevisionCommandOutcome {
            revision,
            charter_version,
            readiness,
        })
    }

    /// Apply a user-approved adoption or amendment.  The DB composite owns
    /// the binding rotation, Project Chat bootstrap, pointer CAS, lifecycle
    /// events, and command/action finalization.
    pub async fn approve(
        &self,
        command: ProjectCharterApprovalCommand,
        action: Option<AgentActionProvenance>,
    ) -> Result<ProjectCharterApprovalCommandOutcome> {
        validate_approval_envelope(&command)?;
        let context = approval_context(&command, action)?;
        if let Some(receipt) = self.replay_receipt(&context).await? {
            return self.replay_approval(receipt).await;
        }
        validate_approval_command(&command)?;
        self.authorize_fresh_principal(&command.project_id, &command.authorization, true)
            .await?;

        let project = ProjectRepo::get_by_id(&*self.db, &command.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", command.project_id.clone()))?;
        let account_id = if let Some(owner_id) = project.owner_id.clone() {
            owner_id
        } else if command.authorization.principal_type == "agent" {
            sqlx::query_scalar::<_, String>(
                "SELECT owner_id FROM agent_identity WHERE id = ? LIMIT 1",
            )
            .bind(&command.authorization.principal_id)
            .fetch_optional(self.db.pool())
            .await?
            .ok_or_else(|| ServiceError::AuthorizationDenied {
                message: "agent principal has no canonical account owner".to_owned(),
            })?
        } else {
            command.authorization.principal_id.clone()
        };
        let charter = ProjectOrchestrationRepo::get_project_charter_for_account(
            &*self.db,
            &command.charter_id,
            &account_id,
        )
        .await?
        .ok_or_else(|| ServiceError::not_found("project_charter", command.charter_id.clone()))?;
        if charter.project_id.as_deref() != Some(command.project_id.as_str()) {
            return Err(ServiceError::invalid_operation(
                "Charter approval target crosses Project scope",
            ));
        }
        let revision =
            ProjectOrchestrationRepo::get_project_charter_revision(&*self.db, &command.revision_id)
                .await?
                .ok_or_else(|| {
                    ServiceError::not_found("project_charter_revision", command.revision_id.clone())
                })?;
        if revision.charter_id != charter.id {
            return Err(ServiceError::invalid_operation(
                "Charter approval revision crosses Charter scope",
            ));
        }
        validate_approval_target(&charter, &revision, &command)?;
        validate_selected_agent(
            &self.db,
            &account_id,
            &command.selected_project_agent_identity_id,
            &command.selected_project_agent_profile_revision_id,
            &command.selected_project_agent_operating_skill_revision,
            &command.selected_project_agent_policy_digest,
        )
        .await?;

        let binding =
            ProjectAgentBindingRepo::get_active_project_binding(&*self.db, &command.project_id)
                .await?
                .ok_or_else(|| {
                    ServiceError::not_found("project_agent_binding", command.project_id.clone())
                })?;
        let approval_type = if project.charter_status == "legacy_unverified" {
            "adoption"
        } else if project.charter_status == "charter_backed" {
            "charter_amendment"
        } else {
            return Err(ServiceError::conflict(
                "Project is not in an adoptable Charter state",
            ));
        };
        let approval_id = new_uuid_v4();
        let amendment_id = (approval_type == "charter_amendment").then(new_uuid_v4);
        let bootstrap_message_id = (approval_type == "adoption").then(new_uuid_v4);
        let now = now_rfc3339();
        let (amendment_rationale, amendment_material_diff_json, amendment_affected_records_json) =
            if approval_type == "charter_amendment" {
                let previous_id =
                    charter
                        .current_approved_revision_id
                        .as_deref()
                        .ok_or_else(|| {
                            ServiceError::conflict("Charter amendment has no approved base")
                        })?;
                let previous =
                    ProjectOrchestrationRepo::get_project_charter_revision(&*self.db, previous_id)
                        .await?
                        .ok_or_else(|| {
                            ServiceError::not_found("project_charter_revision", previous_id)
                        })?;
                let previous_content: ProjectCharterContent =
                    serde_json::from_str(&previous.content_json).map_err(|error| {
                        ServiceError::conflict(format!(
                            "approved Charter content is invalid: {error}"
                        ))
                    })?;
                let current_content: ProjectCharterContent =
                    serde_json::from_str(&revision.content_json).map_err(|error| {
                        ServiceError::conflict(format!(
                            "candidate Charter content is invalid: {error}"
                        ))
                    })?;
                let diff = semantic_revision_diff(Some(&previous_content), &current_content);
                let material = json!({
                    "schema_version": diff.schema_version,
                    "changed_sections": diff.changed_sections,
                    "changes": diff.changes.iter().map(|change| json!({
                        "section": change.section,
                        "field": change.field,
                        "before": change.before,
                        "after": change.after,
                    })).collect::<Vec<_>>(),
                })
                .to_string();
                let affected = json!({
                    "project_id": command.project_id,
                    "reconciliation_required": if diff.is_empty() { Vec::<&str>::new() } else {
                        vec!["documents", "decisions", "tasks", "baselines", "milestones", "validations", "releases"]
                    },
                    "governing_charter_revision_id": command.revision_id,
                }).to_string();
                (Some(diff.change_summary()), Some(material), Some(affected))
            } else {
                (None, None, None)
            };
        let project_chat_id: String = sqlx::query_scalar(
            "SELECT id FROM agent_chat WHERE kind = 'project' AND project_id = ? LIMIT 1",
        )
        .bind(&command.project_id)
        .fetch_optional(self.db.pool())
        .await?
        .ok_or_else(|| ServiceError::not_found("agent_chat", command.project_id.clone()))?;
        let bootstrap_content = bootstrap_message_id.as_ref().map(|_| {
            format!(
                "Project Charter adoption approved for Project {}. Charter {} revision {} is now authoritative.",
                command.project_id, command.charter_id, command.revision_id
            )
        });
        let bootstrap_guard = bootstrap_message_id.as_ref().map(|_| {
            json!({
                "schema_version": ADOPTION_BOOTSTRAP_GUARD_SCHEMA,
                "authority": "data_only",
                "project_id": command.project_id,
                "charter_id": command.charter_id,
                "revision_id": command.revision_id,
                "approval_id": approval_id,
                "content_digest": command.content_digest,
                "render_digest": command.rendered_digest,
                "explicit_event": command.authorization.authorization_event_id,
            })
            .to_string()
        });
        let bootstrap_metadata = bootstrap_message_id.as_ref().map(|_| {
            json!({
                "kind": "project_charter_adoption",
                "approval_id": approval_id,
                "charter_id": command.charter_id,
                "revision_id": command.revision_id,
            })
            .to_string()
        });
        let outcome_json = serde_json::to_string(&json!({
            "operation": PROJECT_CHARTER_APPROVAL_COMMAND,
            "project_id": command.project_id,
            "charter_id": command.charter_id,
            "revision_id": command.revision_id,
            "approval_id": approval_id,
            "approval_type": approval_type,
            "project_version": command.expected_project_version + 1,
            "charter_version": command.expected_charter_version + 1,
            "project_agent_binding_id": if binding.state == "agent_setup_required" { binding.id.clone() } else { new_uuid_v4() },
            "project_chat_id": project_chat_id,
            "bootstrap_message_id": bootstrap_message_id,
            "amendment_id": amendment_id,
            "content_digest": command.content_digest,
            "rendered_digest": command.rendered_digest,
            "domain_committed": true,
        }))
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        let outcome: Value = serde_json::from_str(&outcome_json)
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        let replacement_binding_id = outcome
            .get("project_agent_binding_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let approval = db::ApproveProjectCharter {
            id: approval_id.clone(),
            approval_type: approval_type.to_owned(),
            charter_id: command.charter_id.clone(),
            revision_id: command.revision_id.clone(),
            content_digest: command.content_digest.clone(),
            rendered_digest: command.rendered_digest.clone(),
            expected_charter_version: command.expected_charter_version,
            approved_name: Some(command.approved_project_name.clone()),
            approved_slug: command.approved_project_slug.clone(),
            approved_project_mode: command.project_mode.clone(),
            selected_identity_id: Some(command.selected_project_agent_identity_id.clone()),
            selected_profile_id: Some(command.selected_project_agent_profile_revision_id.clone()),
            selected_operating_skill_revision_id: Some(
                command
                    .selected_project_agent_operating_skill_revision
                    .clone(),
            ),
            selected_policy_revision: Some(PROJECT_AGENT_POLICY_REVISION.to_owned()),
            selected_policy_digest: Some(command.selected_project_agent_policy_digest.clone()),
            approving_principal_type: command.authorization.principal_type.clone(),
            approving_principal_id: command.authorization.principal_id.clone(),
            authorization_basis: command.authorization.authorization_basis.clone(),
            authorization_action: command.authorization.authorization_action.clone(),
            explicit_event: command.authorization.authorization_event_id.clone(),
            authorization_occurred_at: command.authorization.authorization_occurred_at.clone(),
            source_action: command.authorization.authorization_action.clone(),
            idempotency_key: command.idempotency_key.clone(),
            event_id: command.authorization.authorization_event_id.clone(),
            created_at: now.clone(),
            updated_at: now.clone(),
        };
        let (mut receipt, execution) = command_bundle(&context, &outcome_json);
        if let Some(execution) = execution.as_ref() {
            receipt.agent_action_execution_id = Some(execution.id.clone());
        }
        let applied = ProjectOrchestrationRepo::apply_project_charter_approval_command(
            &*self.db,
            ApplyProjectCharterApprovalCommand {
                approval,
                project_id: command.project_id.clone(),
                expected_project_version: command.expected_project_version,
                expected_current_charter_revision_id: project.current_charter_revision_id.clone(),
                existing_binding_id: binding.id,
                replacement_binding_id: if binding.state == "agent_setup_required" {
                    None
                } else {
                    replacement_binding_id
                },
                bootstrap_message_id,
                bootstrap_content,
                bootstrap_content_guard_json: bootstrap_guard,
                bootstrap_author_id: (approval_type == "adoption")
                    .then(|| command.authorization.principal_id.clone()),
                bootstrap_correlation_id: (approval_type == "adoption")
                    .then(|| command.authorization.correlation_id.clone()),
                bootstrap_source_metadata_json: bootstrap_metadata,
                amendment_id,
                amendment_rationale,
                amendment_material_diff_json,
                amendment_affected_records_json,
                command_receipt: Some(receipt),
                action_execution: execution,
            },
        )
        .await?;
        Ok(ProjectCharterApprovalCommandOutcome {
            approval: applied.approval,
            project_id: applied.project_id,
            project_version: applied.project_version,
            project_charter_id: applied.project_charter_id,
            project_charter_revision_id: applied.project_charter_revision_id,
            project_agent_binding_id: applied.project_agent_binding_id,
            project_chat_id: applied.project_chat_id,
            bootstrap_message_id: applied.bootstrap_message_id,
            amendment_id: applied.amendment_id,
        })
    }

    async fn replay_receipt(&self, context: &CommandContext) -> Result<Option<CommandReceipt>> {
        CommandReceiptRepo::get_command_receipt(
            &*self.db,
            context.principal().principal_type(),
            context.principal().principal_id(),
            context.canonical_scope().scope_type().as_str(),
            context.canonical_scope().scope_id(),
            context.operation(),
            context.idempotency_key(),
            context.input_digest(),
        )
        .await
        .map_err(Into::into)
    }

    async fn replay_revision(
        &self,
        receipt: CommandReceipt,
    ) -> Result<ProjectCharterRevisionCommandOutcome> {
        let outcome: Value = serde_json::from_str(&receipt.outcome_json)
            .map_err(|_| ServiceError::conflict("Charter revision receipt outcome is invalid"))?;
        let revision_id = outcome
            .get("revision_id")
            .and_then(Value::as_str)
            .ok_or_else(|| ServiceError::conflict("Charter revision receipt has no revision"))?;
        let revision =
            ProjectOrchestrationRepo::get_project_charter_revision(&*self.db, revision_id)
                .await?
                .ok_or_else(|| {
                    ServiceError::conflict("Charter revision receipt target is missing")
                })?;
        let charter_version = outcome
            .get("charter_version")
            .and_then(Value::as_i64)
            .ok_or_else(|| {
                ServiceError::conflict("Charter revision receipt has no frozen version")
            })?;
        let readiness = outcome
            .get("readiness")
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map_err(|_| ServiceError::conflict("Charter revision receipt readiness is invalid"))?
            .ok_or_else(|| ServiceError::conflict("Charter revision receipt has no readiness"))?;
        Ok(ProjectCharterRevisionCommandOutcome {
            revision,
            charter_version,
            readiness,
        })
    }

    async fn replay_approval(
        &self,
        receipt: CommandReceipt,
    ) -> Result<ProjectCharterApprovalCommandOutcome> {
        let outcome: Value = serde_json::from_str(&receipt.outcome_json)
            .map_err(|_| ServiceError::conflict("Charter approval receipt outcome is invalid"))?;
        let approval_id = outcome
            .get("approval_id")
            .and_then(Value::as_str)
            .ok_or_else(|| ServiceError::conflict("Charter approval receipt has no approval"))?;
        let approval =
            ProjectOrchestrationRepo::get_project_charter_approval(&*self.db, approval_id)
                .await?
                .ok_or_else(|| {
                    ServiceError::conflict("Charter approval receipt target is missing")
                })?;
        let project_id = outcome
            .get("project_id")
            .and_then(Value::as_str)
            .ok_or_else(|| ServiceError::conflict("Charter approval receipt has no Project"))?
            .to_owned();
        Ok(ProjectCharterApprovalCommandOutcome {
            project_id: project_id.clone(),
            project_version: outcome
                .get("project_version")
                .and_then(Value::as_i64)
                .unwrap_or_default(),
            project_charter_id: outcome
                .get("charter_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            project_charter_revision_id: outcome
                .get("revision_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            project_agent_binding_id: outcome
                .get("project_agent_binding_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            project_chat_id: outcome
                .get("project_chat_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            bootstrap_message_id: value_string(&outcome, "bootstrap_message_id"),
            amendment_id: value_string(&outcome, "amendment_id"),
            approval,
        })
    }

    async fn authorize_fresh_principal(
        &self,
        project_id: &str,
        authorization: &ProjectCommandAuthorization,
        owner_or_admin: bool,
    ) -> Result<()> {
        match authorization.principal_type.as_str() {
            "user" => {
                let project = ProjectRepo::get_by_id(&*self.db, project_id)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("project", project_id.to_owned()))?;
                let is_owner =
                    project.owner_id.as_deref() == Some(authorization.principal_id.as_str());
                let member = ProjectMemberRepo::get_member(
                    &*self.db,
                    project_id,
                    &authorization.principal_id,
                )
                .await?;
                if !is_owner && member.is_none() {
                    return Err(ServiceError::AuthorizationDenied {
                        message: "principal is not a member of the Project".to_owned(),
                    });
                }
                if owner_or_admin
                    && !is_owner
                    && !member
                        .as_ref()
                        .is_some_and(|value| matches!(value.role.as_str(), "owner" | "admin"))
                {
                    return Err(ServiceError::AuthorizationDenied {
                        message: "Project owner or admin role is required for Charter mutation"
                            .to_owned(),
                    });
                }
                Ok(())
            }
            "agent" => {
                let bound: Option<String> = sqlx::query_scalar(
                    "SELECT identity_id FROM project_agent_binding WHERE project_id = ? AND identity_id = ? AND state = 'active' LIMIT 1",
                )
                .bind(project_id)
                .bind(&authorization.principal_id)
                .fetch_optional(self.db.pool())
                .await?;
                if bound.is_none() {
                    return Err(ServiceError::AuthorizationDenied {
                        message: "agent principal is not actively bound to the Project".to_owned(),
                    });
                }
                let eligible: Option<i64> = sqlx::query_scalar(
                    "SELECT 1 FROM agent_identity WHERE id = ? AND paused = 0 AND archived_at IS NULL LIMIT 1",
                )
                .bind(&authorization.principal_id)
                .fetch_optional(self.db.pool())
                .await?;
                if eligible.is_none() {
                    return Err(ServiceError::AuthorizationDenied {
                        message: "agent principal is paused or archived".to_owned(),
                    });
                }
                Ok(())
            }
            _ => Err(ServiceError::AuthorizationDenied {
                message: "Charter commands accept only user or Project Agent principals".to_owned(),
            }),
        }
    }
}

fn validate_revision_command(command: &ProjectCharterRevisionCommand) -> Result<()> {
    if command.expected_charter_version < 0 {
        return Err(ServiceError::invalid_operation(
            "expected_charter_version must not be negative",
        ));
    }
    if command.authorization.authorization_action != "project_charter.revision.save" {
        return Err(ServiceError::invalid_operation(
            "Project Charter revision authorization action is invalid",
        ));
    }
    validate_authorization_timestamp(&command.authorization.authorization_occurred_at)?;
    parse_project_mode(&command.project_mode)?;
    parse_maturity(&command.maturity)?;
    Ok(())
}

fn validate_approval_command(command: &ProjectCharterApprovalCommand) -> Result<()> {
    for (field, value) in [
        ("project_id", command.project_id.as_str()),
        ("charter_id", command.charter_id.as_str()),
        ("revision_id", command.revision_id.as_str()),
        ("content_digest", command.content_digest.as_str()),
        ("rendered_digest", command.rendered_digest.as_str()),
        (
            "approved_project_name",
            command.approved_project_name.as_str(),
        ),
        (
            "selected_identity_id",
            command.selected_project_agent_identity_id.as_str(),
        ),
        (
            "selected_profile_id",
            command.selected_project_agent_profile_revision_id.as_str(),
        ),
        (
            "selected_skill_revision",
            command
                .selected_project_agent_operating_skill_revision
                .as_str(),
        ),
        (
            "selected_policy_digest",
            command.selected_project_agent_policy_digest.as_str(),
        ),
        ("idempotency_key", command.idempotency_key.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(ServiceError::invalid_operation(format!(
                "{field} is required"
            )));
        }
    }
    if command.expected_charter_version < 1 || command.expected_project_version < 1 {
        return Err(ServiceError::invalid_operation(
            "Charter approval versions must be positive",
        ));
    }
    if command.authorization.authorization_action != "project_charter.approval" {
        return Err(ServiceError::invalid_operation(
            "Project Charter approval authorization action is invalid",
        ));
    }
    validate_authorization_timestamp(&command.authorization.authorization_occurred_at)?;
    parse_project_mode(&command.project_mode)?;
    Ok(())
}

fn validate_revision_envelope(command: &ProjectCharterRevisionCommand) -> Result<()> {
    for (field, value) in [
        ("project_id", command.project_id.as_str()),
        ("idempotency_key", command.idempotency_key.as_str()),
        (
            "authorization principal type",
            command.authorization.principal_type.as_str(),
        ),
        (
            "authorization principal id",
            command.authorization.principal_id.as_str(),
        ),
        (
            "authorization correlation id",
            command.authorization.correlation_id.as_str(),
        ),
        (
            "authorization event id",
            command.authorization.authorization_event_id.as_str(),
        ),
        (
            "authorization basis",
            command.authorization.authorization_basis.as_str(),
        ),
        (
            "authorization action",
            command.authorization.authorization_action.as_str(),
        ),
        (
            "authorization occurred at",
            command.authorization.authorization_occurred_at.as_str(),
        ),
    ] {
        if value.trim().is_empty() {
            return Err(ServiceError::invalid_operation(format!(
                "{field} is required"
            )));
        }
    }
    Ok(())
}

fn validate_approval_envelope(command: &ProjectCharterApprovalCommand) -> Result<()> {
    for (field, value) in [
        ("project_id", command.project_id.as_str()),
        ("idempotency_key", command.idempotency_key.as_str()),
        (
            "authorization principal type",
            command.authorization.principal_type.as_str(),
        ),
        (
            "authorization principal id",
            command.authorization.principal_id.as_str(),
        ),
        (
            "authorization correlation id",
            command.authorization.correlation_id.as_str(),
        ),
        (
            "authorization event id",
            command.authorization.authorization_event_id.as_str(),
        ),
        (
            "authorization basis",
            command.authorization.authorization_basis.as_str(),
        ),
        (
            "authorization action",
            command.authorization.authorization_action.as_str(),
        ),
        (
            "authorization occurred at",
            command.authorization.authorization_occurred_at.as_str(),
        ),
    ] {
        if value.trim().is_empty() {
            return Err(ServiceError::invalid_operation(format!(
                "{field} is required"
            )));
        }
    }
    Ok(())
}

fn validate_authorization_timestamp(value: &str) -> Result<()> {
    let occurred_at =
        DateTime::parse_from_rfc3339(value).map_err(|_| ServiceError::AuthorizationDenied {
            message: "authorization timestamp must be RFC3339".to_owned(),
        })?;
    let skew = (Utc::now() - occurred_at.with_timezone(&Utc))
        .num_seconds()
        .abs();
    if skew > MAX_AUTHORIZATION_CLOCK_SKEW_SECONDS {
        return Err(ServiceError::AuthorizationDenied {
            message: "authorization event is outside the accepted clock-skew window".to_owned(),
        });
    }
    Ok(())
}

fn validate_approval_target(
    charter: &ProjectCharterRecord,
    revision: &ProjectCharterRevisionRecord,
    command: &ProjectCharterApprovalCommand,
) -> Result<()> {
    if command.expected_charter_version != charter.version
        || command.project_mode != charter.project_mode
        || command.content_digest != revision.content_digest
        || command.rendered_digest != revision.rendered_digest
        || revision.charter_id != charter.id
        || !matches!(revision.lifecycle.as_str(), "draft" | "proposed")
    {
        return Err(ServiceError::conflict(
            "the Project Charter approval target is stale or inconsistent",
        ));
    }
    let content: ProjectCharterContent = serde_json::from_str(&revision.content_json)
        .map_err(|error| ServiceError::conflict(format!("Charter content is invalid: {error}")))?;
    if revision.content_digest != charter_content_digest(&content)
        || revision.rendered_digest
            != charter_render_digest(&revision.render_version, &revision.rendered_view)
    {
        return Err(ServiceError::conflict(
            "Charter revision digests are internally inconsistent",
        ));
    }
    if command.approved_project_name.trim() != content.identity.working_name.trim() {
        return Err(ServiceError::conflict(
            "approved project name must match the Charter working name",
        ));
    }
    let readiness = evaluate_project_charter_readiness(
        &content,
        parse_project_mode(&charter.project_mode)?,
        parse_maturity(&charter.maturity)?,
        CHARTER_READINESS_POLICY_VERSION,
        &revision.created_at,
    );
    if readiness.status != api_types::CharterReadinessStatus::Ready {
        return Err(ServiceError::conflict(format!(
            "Charter revision is not ready: {}",
            readiness
                .gaps
                .iter()
                .filter(|gap| gap.blocking)
                .map(|gap| gap.code.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }
    Ok(())
}

type SelectedAgentEligibilityRow = (
    String,
    Option<String>,
    Option<String>,
    String,
    String,
    String,
);

async fn validate_selected_agent(
    db: &SqliteDb,
    account_id: &str,
    identity_id: &str,
    profile_id: &str,
    skill_revision_id: &str,
    expected_policy_digest: &str,
) -> Result<()> {
    let selected: Option<SelectedAgentEligibilityRow> = sqlx::query_as(
        "SELECT p.tool_policy_json, i.owner_id, i.selected_profile_id,
                sr.id, sr.skill_key, os.lifecycle
         FROM agent_profile p
         JOIN agent_identity i ON i.id = p.identity_id
         JOIN operating_skill_revision sr ON sr.id = ?
         JOIN operating_skill os ON os.id = sr.operating_skill_id
         WHERE p.id = ? AND p.identity_id = ? AND i.owner_id = ?
           AND i.paused = 0 AND i.archived_at IS NULL
         LIMIT 1",
    )
    .bind(skill_revision_id)
    .bind(profile_id)
    .bind(identity_id)
    .bind(account_id)
    .fetch_optional(db.pool())
    .await?;
    let Some((policy_json, _owner, selected_profile, selected_skill, skill_key, lifecycle)) =
        selected
    else {
        return Err(ServiceError::conflict(
            "the selected Project Agent is no longer eligible",
        ));
    };
    if selected_profile.as_deref() != Some(profile_id)
        || selected_skill != skill_revision_id
        || skill_key != PROJECT_OPERATING_SKILL_KEY
        || lifecycle != "active"
        || expected_policy_digest != project_agent_policy_digest(&policy_json)
        || current_project_agent_operating_skill_revision(db).await? != skill_revision_id
    {
        return Err(ServiceError::conflict(
            "the selected Project Agent profile, operating skill, or policy is stale",
        ));
    }
    Ok(())
}

fn revision_context(
    command: &ProjectCharterRevisionCommand,
    action: Option<AgentActionProvenance>,
) -> Result<CommandContext> {
    let mut versions = BTreeMap::new();
    versions.insert(
        "expected_charter_version".to_owned(),
        command.expected_charter_version,
    );
    context_from(
        PROJECT_CHARTER_ADOPTION_OPERATION,
        &command.idempotency_key,
        &command.authorization,
        command,
        action,
        &command.project_id,
        versions,
    )
}

fn approval_context(
    command: &ProjectCharterApprovalCommand,
    action: Option<AgentActionProvenance>,
) -> Result<CommandContext> {
    let mut versions = BTreeMap::new();
    versions.insert(
        "expected_charter_version".to_owned(),
        command.expected_charter_version,
    );
    versions.insert(
        "expected_project_version".to_owned(),
        command.expected_project_version,
    );
    context_from(
        PROJECT_CHARTER_APPROVAL_COMMAND,
        &command.idempotency_key,
        &command.authorization,
        command,
        action,
        &command.project_id,
        versions,
    )
}

fn context_from<T: Serialize>(
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
            "command idempotency key is required",
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
    .map_err(|error| {
        ServiceError::invalid_operation(format!("serialize Project Charter command: {error}"))
    })
}

fn validate_context(
    context: &CommandContext,
    project_id: &str,
    authorization: &ProjectCommandAuthorization,
    operation: &str,
) -> Result<()> {
    if context.operation() != operation
        || context.canonical_scope().scope_type() != CommandScopeType::Project
        || context.canonical_scope().scope_id() != project_id
        || context.principal().principal_type() != authorization.principal_type
        || context.principal().principal_id() != authorization.principal_id
    {
        return Err(ServiceError::invalid_operation(
            "Project Charter command context does not match authorization",
        ));
    }
    Ok(())
}

fn command_bundle(
    context: &CommandContext,
    outcome_json: &str,
) -> (CreateCommandReceipt, Option<CreateAgentActionExecution>) {
    let committed_at = now_rfc3339();
    let execution =
        context
            .action_provenance
            .as_ref()
            .map(|provenance| CreateAgentActionExecution {
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
                created_at: committed_at.clone(),
                completed_at: Some(committed_at.clone()),
                updated_at: committed_at.clone(),
            });
    let receipt = CreateCommandReceipt {
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
        agent_action_execution_id: execution.as_ref().map(|value| value.id.clone()),
        outcome_json: outcome_json.to_owned(),
        committed_at,
    };
    (receipt, execution)
}

fn expected_version_with_default(expected: i64, current: i64) -> i64 {
    if expected == 0 {
        current
    } else {
        expected
    }
}

fn mode_string(value: &str) -> Result<String> {
    parse_project_mode(value).map(|_| value.to_owned())
}

fn maturity_string(value: &str) -> Result<String> {
    parse_maturity(value).map(|_| value.to_owned())
}

fn parse_project_mode(value: &str) -> Result<ProjectMode> {
    match value {
        "compact" => Ok(ProjectMode::Compact),
        "standard" => Ok(ProjectMode::Standard),
        _ => Err(ServiceError::invalid_operation(
            "Project Charter project_mode is invalid",
        )),
    }
}

fn parse_maturity(value: &str) -> Result<ProductMaturity> {
    match value {
        "prototype" => Ok(ProductMaturity::Prototype),
        "mvp" => Ok(ProductMaturity::Mvp),
        "production" => Ok(ProductMaturity::Production),
        "critical" => Ok(ProductMaturity::Critical),
        _ => Err(ServiceError::invalid_operation(
            "Project Charter maturity is invalid",
        )),
    }
}

fn value_string(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(Value::as_str).map(str::to_owned)
}
