//! Deterministic execution-baseline rendering and persistence-shape helpers.
//!
//! The database keeps the baseline columns normalized for gate queries.  This
//! module is the typed boundary that derives those columns from one closed API
//! payload and computes the exact digests shown to the approving user.

use api_types::{
    canonical_digest_with_schema, canonical_render_digest, AcceptanceEvidenceRequirement,
    AuthorizationProvenance as ApiAuthorizationProvenance, ExecutionBaseline,
    ExecutionBaselineApproval, ExecutionBaselineContent, ExecutionBaselineLifecycle,
    ExecutionBaselineReleasePolicy, ExecutionBaselineResponse, ExecutionBaselineRevision,
    MilestoneAcceptanceCheck, PrincipalKind, PrincipalRef, RevisionProvenance,
};
use chrono::{DateTime, Utc};
use db::{
    new_uuid_v4, now_rfc3339, AgentActionExecutionStatus, AgentActionStatus,
    ApproveProjectExecutionBaselineCommand, CommandReceiptRepo, CreateAgentActionExecution,
    CreateCommandReceipt, ProjectExecutionBaselineApprovalRecord, ProjectExecutionBaselineRecord,
    ProjectExecutionBaselineRevisionRecord, ProjectMemberRepo, ProjectOrchestrationRepo,
    ProjectRepo, SaveProjectExecutionBaselineRevisionCommand, SqliteDb,
};
use serde::Serialize;
use serde_json::{json, Value};
use sqlx::Row;
use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use crate::{
    AgentActionProvenance, AuthorizationProvenance, CommandContext, CommandPrincipal, CommandScope,
    CommandScopeType, ExpectedCommandState, NewCommandContext, ProjectCommandAuthorization, Result,
    ServiceError,
};

pub const EXECUTION_BASELINE_SCHEMA_VERSION: &str = "forge.execution-baseline/v1";
pub const EXECUTION_BASELINE_RENDER_VERSION: &str = "forge.execution-baseline-render/v1";
pub const EXECUTION_BASELINE_RELEASE_POLICY_SCHEMA: &str =
    "forge.execution-baseline-release-policy/v1";

/// Compute the authority digest for the complete, closed release policy.
/// Callers must never accept a client-supplied opaque policy digest without
/// comparing it with this value.
pub fn release_policy_digest(
    policy: &ExecutionBaselineReleasePolicy,
) -> std::result::Result<String, serde_json::Error> {
    canonical_digest_with_schema(EXECUTION_BASELINE_RELEASE_POLICY_SCHEMA, policy)
}

/// Validate the frozen policy carried by a baseline before it is persisted or
/// proposed by a Project Agent.  This is intentionally shared by the HTTP
/// user path and the server-owned typed action materializer so neither path
/// can accept an opaque revision/digest pair.
pub fn validate_execution_baseline_policy(
    content: &ExecutionBaselineContent,
) -> std::result::Result<(), String> {
    if content.release_policy.schema_version != EXECUTION_BASELINE_RELEASE_POLICY_SCHEMA
        || content.release_policy.revision != content.release_policy_revision
    {
        return Err(
            "the baseline release policy must use the declared Forge schema and revision"
                .to_owned(),
        );
    }
    let computed_policy_digest = release_policy_digest(&content.release_policy)
        .map_err(|error| format!("invalid release policy: {error}"))?;
    if content.release_policy_digest != computed_policy_digest {
        return Err(
            "the release policy digest does not match the complete frozen policy payload"
                .to_owned(),
        );
    }
    if content.release_policy.revision.trim().is_empty() {
        return Err("the frozen release policy revision cannot be empty".to_owned());
    }

    validate_identifier_rules(
        "required_check_definition_revisions",
        &content.release_policy.required_check_definition_revisions,
        true,
    )?;
    validate_literal_rules(
        "reviewer_independence_rules",
        &content.release_policy.reviewer_independence_rules,
        &["independent-reviewer"],
        true,
    )?;
    validate_literal_rules(
        "manual_attestation_rules",
        &content.release_policy.manual_attestation_rules,
        &["manual-attestation"],
        false,
    )?;
    validate_literal_rules(
        "waiver_rules",
        &content.release_policy.waiver_rules,
        &["user-waiver"],
        false,
    )?;
    validate_literal_rules(
        "evidence_kinds",
        &content.release_policy.evidence_kinds,
        &[
            "artifact",
            "ci-log",
            "media",
            "review-report",
            "test-report",
        ],
        true,
    )?;
    validate_literal_rules(
        "evidence_contexts",
        &content.release_policy.evidence_contexts,
        &[
            "commit",
            "external",
            "milestone",
            "project",
            "repository",
            "task",
        ],
        true,
    )?;
    validate_literal_rules(
        "evidence_freshness_rules",
        &content.release_policy.evidence_freshness_rules,
        &[
            "current-baseline",
            "current-charter",
            "current-commit",
            "current-milestone",
        ],
        true,
    )?;
    validate_literal_rules(
        "dependency_rules",
        &content.release_policy.dependency_rules,
        &[
            "dependencies-green",
            "dependencies-reviewed",
            "no-blocked-dependencies",
        ],
        true,
    )?;
    validate_literal_rules(
        "stale_input_rules",
        &content.release_policy.stale_input_rules,
        &["stale-baseline-blocks", "stale-evidence-blocks"],
        true,
    )?;
    validate_literal_rules(
        "forbidden_side_effects",
        &content.release_policy.forbidden_side_effects,
        &[
            "credential-access",
            "cross-project-write",
            "force-push",
            "merge",
            "publish",
            "release",
        ],
        true,
    )?;
    validate_literal_rules(
        "known_issue_rules",
        &content.release_policy.known_issue_rules,
        &[
            "known-issue-blocks",
            "known-issue-waiver",
            "record-known-issue",
        ],
        true,
    )?;
    validate_literal_rules(
        "correction_rules",
        &content.release_policy.correction_rules,
        &[
            "correct-before-release",
            "correction-required",
            "rerun-failed-checks",
        ],
        true,
    )?;
    validate_literal_rules(
        "purge_rules",
        &content.release_policy.purge_rules,
        &[
            "purge-invalid-evidence",
            "purge-revoked-evidence",
            "purge-stale-evidence",
        ],
        true,
    )?;
    Ok(())
}

fn validate_identifier_rules(
    field: &str,
    values: &[String],
    required: bool,
) -> std::result::Result<(), String> {
    if required && values.is_empty() {
        return Err(format!("release policy field '{field}' must not be empty"));
    }
    let mut seen = HashSet::new();
    let mut previous: Option<&str> = None;
    for value in values {
        let trimmed = value.trim();
        if trimmed.is_empty() || trimmed != value {
            return Err(format!(
                "release policy field '{field}' contains an invalid identifier"
            ));
        }
        if !trimmed.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.' | ':')
        }) {
            return Err(format!(
                "release policy field '{field}' contains an invalid identifier"
            ));
        }
        if !seen.insert(trimmed) {
            return Err(format!(
                "release policy field '{field}' contains a duplicate rule"
            ));
        }
        if previous.is_some_and(|previous| previous >= trimmed) {
            return Err(format!(
                "release policy field '{field}' must use canonical lexicographic order"
            ));
        }
        previous = Some(trimmed);
    }
    Ok(())
}

fn validate_literal_rules(
    field: &str,
    values: &[String],
    supported: &[&str],
    required: bool,
) -> std::result::Result<(), String> {
    if required && values.is_empty() {
        return Err(format!(
            "release policy field '{field}' must not be empty; supported: {}",
            supported.join(", ")
        ));
    }
    let mut seen = HashSet::new();
    let mut previous: Option<&str> = None;
    for value in values {
        let trimmed = value.trim();
        if trimmed.is_empty() || trimmed != value || !supported.contains(&trimmed) {
            // These are small closed vocabularies. Naming the offender without
            // naming the alternatives leaves a caller guessing at a fixed list
            // it cannot see, so the supported values travel with the error.
            return Err(format!(
                "release policy field '{field}' contains unsupported rule '{value}'; supported: {}",
                supported.join(", ")
            ));
        }
        if !seen.insert(trimmed) {
            return Err(format!(
                "release policy field '{field}' contains a duplicate rule"
            ));
        }
        if previous.is_some_and(|previous| previous >= trimmed) {
            return Err(format!(
                "release policy field '{field}' must use canonical lexicographic order"
            ));
        }
        previous = Some(trimmed);
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionBaselineRender {
    pub rendered_view: String,
    pub content_digest: String,
    pub render_digest: String,
}

/// Render the exact baseline review target.  JSON is deliberately used as the
/// review view here: it is bounded, deterministic, and preserves every
/// approval-bearing field without a second hand-written Markdown authority.
pub fn render_execution_baseline(
    content: &ExecutionBaselineContent,
) -> std::result::Result<ExecutionBaselineRender, serde_json::Error> {
    let rendered_view = serde_json::to_string_pretty(content)?;
    let content_digest = canonical_digest_with_schema(EXECUTION_BASELINE_SCHEMA_VERSION, content)?;
    let render_digest = canonical_render_digest(EXECUTION_BASELINE_RENDER_VERSION, &rendered_view)?;
    Ok(ExecutionBaselineRender {
        rendered_view,
        content_digest,
        render_digest,
    })
}

/// Encode the typed content into the existing V076 normalized columns.  The
/// source payload is still retained in `source_refs_json` by the API adapter;
/// these projections exist so task admission can query without decoding the
/// entire canonical bundle.
pub fn baseline_column_json(
    content: &ExecutionBaselineContent,
) -> std::result::Result<BaselineColumnJson, serde_json::Error> {
    Ok(BaselineColumnJson {
        document_revisions_json: serde_json::to_string(&content.document_revisions)?,
        plan_items_json: serde_json::to_string(&content.plan_item_ids)?,
        release_policy_json: serde_json::to_string(&json!({
            "revision": content.release_policy_revision,
            "digest": content.release_policy_digest,
            "policy": content.release_policy,
            "reviewer_independence_rules": content.reviewer_independence_rules,
        }))?,
        acceptance_matrix_json: serde_json::to_string(&content.acceptance_evidence_matrix)?,
        capability_classes_json: serde_json::to_string(&content.capability_classes)?,
        risk_classes_json: serde_json::to_string(&content.risk_classes)?,
        adaptive_envelope_json: serde_json::to_string(&content.adaptive_envelope)?,
        elevated_operations_json: serde_json::to_string(&content.elevated_operations)?,
        exclusions_json: serde_json::to_string(&content.exclusions)?,
        rollback_recovery_json: serde_json::to_string(&content.rollback_and_recovery)?,
        // Keep this as an ordered projection.  The definition at index `i`
        // governs the milestone at index `i`; treating these as independent
        // sets would allow a valid definition to be silently paired with the
        // wrong milestone during activation or Task admission.
        milestone_definition_revision_ids_json: serde_json::to_string(
            &content.milestone_definition_revision_ids,
        )?,
        milestone_id: content
            .primary_milestone_id
            .clone()
            .or_else(|| content.milestone_ids.first().cloned()),
        primary_milestone_id: content.primary_milestone_id.clone(),
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaselineColumnJson {
    pub document_revisions_json: String,
    pub plan_items_json: String,
    pub milestone_id: Option<String>,
    pub primary_milestone_id: Option<String>,
    pub release_policy_json: String,
    pub acceptance_matrix_json: String,
    pub capability_classes_json: String,
    pub risk_classes_json: String,
    pub adaptive_envelope_json: String,
    pub elevated_operations_json: String,
    pub exclusions_json: String,
    pub rollback_recovery_json: String,
    pub milestone_definition_revision_ids_json: String,
}

/// Operation names are intentionally distinct even though the native tool
/// currently uses one descriptor.  The command receipt therefore records
/// whether a caller saved a draft, requested approval, approved, or activated
/// a baseline; a replay can never change lifecycle semantics by changing a
/// transport-only action field.
pub const EXECUTION_BASELINE_SAVE_DRAFT_COMMAND: &str = "project.execution_baseline.save_draft";
pub const EXECUTION_BASELINE_PROPOSE_COMMAND: &str =
    "project.execution_baseline.propose_for_approval";
pub const EXECUTION_BASELINE_APPROVE_COMMAND: &str = "project.execution_baseline.approve";
pub const EXECUTION_BASELINE_ACTIVATE_COMMAND: &str = "project.execution_baseline.activate";
const EXECUTION_BASELINE_MANIFEST_SCHEMA: &str = "forge.execution-baseline-manifest/v1";
const MAX_AUTHORIZATION_CLOCK_SKEW_SECONDS: i64 = 48 * 60 * 60;

#[derive(Debug, Clone, Serialize)]
pub struct SaveExecutionBaselineDraftCommand {
    pub project_id: String,
    pub baseline_id: Option<String>,
    pub base_revision_id: Option<String>,
    pub expected_baseline_version: Option<i64>,
    pub content: ExecutionBaselineContent,
    pub rendered_view: String,
    pub render_version: String,
    pub content_digest: String,
    pub render_digest: String,
    pub provenance: RevisionProvenance,
    pub idempotency_key: String,
    pub authorization: ProjectCommandAuthorization,
    #[serde(skip_serializing)]
    pub action: Option<AgentActionProvenance>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProposeExecutionBaselineForApprovalCommand {
    pub project_id: String,
    pub baseline_id: String,
    pub base_revision_id: Option<String>,
    pub expected_baseline_version: i64,
    pub content: ExecutionBaselineContent,
    pub rendered_view: String,
    pub render_version: String,
    pub content_digest: String,
    pub render_digest: String,
    pub provenance: RevisionProvenance,
    pub idempotency_key: String,
    pub authorization: ProjectCommandAuthorization,
    #[serde(skip_serializing)]
    pub action: Option<AgentActionProvenance>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ApproveExecutionBaselineCommand {
    pub project_id: String,
    pub baseline_id: String,
    pub revision_id: String,
    pub expected_baseline_version: i64,
    pub expected_project_version: i64,
    pub content_digest: String,
    pub render_digest: String,
    pub idempotency_key: String,
    pub authorization: ProjectCommandAuthorization,
    #[serde(skip_serializing)]
    pub action: Option<AgentActionProvenance>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ActivateExecutionBaselineCommand {
    pub project_id: String,
    pub baseline_id: String,
    pub revision_id: String,
    pub approval_id: String,
    pub expected_baseline_version: i64,
    pub expected_project_version: i64,
    pub content_digest: String,
    pub render_digest: String,
    pub idempotency_key: String,
    pub authorization: ProjectCommandAuthorization,
    #[serde(skip_serializing)]
    pub action: Option<AgentActionProvenance>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ExecutionBaselineApprovalTarget {
    pub baseline_id: String,
    pub revision_id: String,
    pub revision: i64,
    pub content: ExecutionBaselineContent,
    pub rendered_view: String,
    pub render_version: String,
    pub content_digest: String,
    pub render_digest: String,
    pub provenance: RevisionProvenance,
    pub requires_user_authorization: bool,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ExecutionBaselineCommandOutcome {
    pub baseline_id: String,
    pub revision_id: Option<String>,
    pub approval_id: Option<String>,
    pub lifecycle: String,
    pub baseline_version: i64,
    pub content_digest: Option<String>,
    pub render_digest: Option<String>,
    pub requires_user_authorization: bool,
    pub approval_target: Option<ExecutionBaselineApprovalTarget>,
    pub receipt_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
struct ExecutionBaselineManifest {
    schema: String,
    content: ExecutionBaselineContent,
    rendered_view: String,
    provenance: RevisionProvenance,
}

#[derive(Clone)]
pub struct ExecutionBaselineCommandService {
    db: Arc<SqliteDb>,
}

impl ExecutionBaselineCommandService {
    #[must_use]
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self { db }
    }

    /// Save an immutable `draft` revision.  Drafts validate the canonical
    /// renderer and Project/artifact ownership but may omit approval-only
    /// contract fields.  No approval receipt is created.
    pub async fn save_draft(
        &self,
        command: SaveExecutionBaselineDraftCommand,
    ) -> Result<ExecutionBaselineCommandOutcome> {
        let context = baseline_context(
            EXECUTION_BASELINE_SAVE_DRAFT_COMMAND,
            &command.idempotency_key,
            &command.authorization,
            &command,
            command.action.clone(),
            &command.project_id,
            command.expected_baseline_version.unwrap_or(1),
            command.base_revision_id.as_deref(),
        )?;
        self.save_draft_with_context(command, context).await
    }

    pub(crate) async fn save_draft_with_context(
        &self,
        command: SaveExecutionBaselineDraftCommand,
        context: CommandContext,
    ) -> Result<ExecutionBaselineCommandOutcome> {
        if let Some(outcome) = self.replay(&context).await? {
            return self.outcome_from_receipt(&outcome, &context).await;
        }
        validate_baseline_authorization(
            &command.authorization,
            EXECUTION_BASELINE_SAVE_DRAFT_COMMAND,
            false,
        )?;
        authorize_baseline_principal(&self.db, &command.project_id, &command.authorization).await?;
        validate_baseline_provenance(&command.provenance, &command.authorization)?;
        validate_rendered_candidate(
            &command.content,
            &command.rendered_view,
            &command.render_version,
            &command.content_digest,
            &command.render_digest,
        )?;
        validate_baseline_content(&self.db, &command.project_id, &command.content, false).await?;
        let (baseline_id, expected_version, base_revision) = self
            .resolve_revision_base(
                &command.project_id,
                command.baseline_id.as_deref(),
                command.expected_baseline_version,
                command.base_revision_id.as_deref(),
            )
            .await?;
        let revision_id = new_uuid_v4();
        let columns = baseline_column_json(&command.content)
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        let manifest = manifest_json(
            &command.content,
            &command.rendered_view,
            &command.provenance,
        )?;
        let outcome_json = serde_json::json!({
            "operation": "save_draft",
            "project_id": command.project_id,
            "baseline_id": baseline_id,
            "revision_id": revision_id,
            "lifecycle": "draft",
            "content_digest": command.content_digest,
            "render_digest": command.render_digest,
            "requires_user_authorization": false,
            "domain_committed": true,
        })
        .to_string();
        let (receipt, action_execution) = command_bundle(&context, &outcome_json);
        let _revision = ProjectOrchestrationRepo::save_project_execution_baseline_draft_command(
            &*self.db,
            SaveProjectExecutionBaselineRevisionCommand {
                project_id: command.project_id.clone(),
                baseline_id: Some(baseline_id.clone()),
                revision_id,
                expected_baseline_version: expected_version,
                base_revision,
                base_revision_id: command.base_revision_id.clone(),
                lifecycle: "draft".to_owned(),
                charter_revision_id: command.content.charter_revision.revision_id.clone(),
                document_revisions_json: columns.document_revisions_json,
                plan_items_json: columns.plan_items_json,
                milestone_id: columns.milestone_id,
                milestone_ids_json: serde_json::to_string(&command.content.milestone_ids)
                    .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
                milestone_definition_revision_ids_json: columns
                    .milestone_definition_revision_ids_json,
                primary_milestone_id: columns.primary_milestone_id,
                release_policy_json: columns.release_policy_json,
                release_policy_revision: command.content.release_policy_revision.clone(),
                release_policy_digest: command.content.release_policy_digest.clone(),
                acceptance_matrix_json: columns.acceptance_matrix_json,
                capability_classes_json: columns.capability_classes_json,
                risk_classes_json: columns.risk_classes_json,
                adaptive_envelope_json: columns.adaptive_envelope_json,
                elevated_operations_json: columns.elevated_operations_json,
                exclusions_json: columns.exclusions_json,
                rollback_recovery_json: columns.rollback_recovery_json,
                schema_version: EXECUTION_BASELINE_SCHEMA_VERSION.to_owned(),
                render_version: command.render_version.clone(),
                rendered_view: command.rendered_view.clone(),
                content_digest: command.content_digest.clone(),
                rendered_digest: command.render_digest.clone(),
                source_refs_json: manifest,
                created_at: now_rfc3339(),
                command_receipt: Some(receipt),
                action_execution,
            },
        )
        .await
        .map_err(ServiceError::from)?;
        self.committed_outcome(&context).await
    }

    /// Propose a complete baseline for user approval.  This is deliberately a
    /// different service method and receipt operation from `save_draft`.
    pub async fn propose_for_approval(
        &self,
        command: ProposeExecutionBaselineForApprovalCommand,
    ) -> Result<ExecutionBaselineCommandOutcome> {
        let context = baseline_context(
            EXECUTION_BASELINE_PROPOSE_COMMAND,
            &command.idempotency_key,
            &command.authorization,
            &command,
            command.action.clone(),
            &command.project_id,
            command.expected_baseline_version,
            command.base_revision_id.as_deref(),
        )?;
        self.propose_for_approval_with_context(command, context)
            .await
    }

    pub(crate) async fn propose_for_approval_with_context(
        &self,
        command: ProposeExecutionBaselineForApprovalCommand,
        context: CommandContext,
    ) -> Result<ExecutionBaselineCommandOutcome> {
        if let Some(outcome) = self.replay(&context).await? {
            return self.outcome_from_receipt(&outcome, &context).await;
        }
        validate_baseline_authorization(
            &command.authorization,
            EXECUTION_BASELINE_PROPOSE_COMMAND,
            false,
        )?;
        authorize_baseline_principal(&self.db, &command.project_id, &command.authorization).await?;
        validate_baseline_provenance(&command.provenance, &command.authorization)?;
        validate_rendered_candidate(
            &command.content,
            &command.rendered_view,
            &command.render_version,
            &command.content_digest,
            &command.render_digest,
        )?;
        validate_baseline_content(&self.db, &command.project_id, &command.content, true).await?;
        ensure_reconciliation_clear(&self.db, &command.project_id).await?;
        let (baseline_id, expected_version, base_revision) = self
            .resolve_revision_base(
                &command.project_id,
                Some(&command.baseline_id),
                Some(command.expected_baseline_version),
                command.base_revision_id.as_deref(),
            )
            .await?;
        let revision_id = new_uuid_v4();
        let columns = baseline_column_json(&command.content)
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        let manifest = manifest_json(
            &command.content,
            &command.rendered_view,
            &command.provenance,
        )?;
        let target = ExecutionBaselineApprovalTarget {
            baseline_id: baseline_id.clone(),
            revision_id: revision_id.clone(),
            revision: 0,
            content: command.content.clone(),
            rendered_view: command.rendered_view.clone(),
            render_version: command.render_version.clone(),
            content_digest: command.content_digest.clone(),
            render_digest: command.render_digest.clone(),
            provenance: command.provenance.clone(),
            requires_user_authorization: true,
        };
        let outcome_json = serde_json::json!({
            "operation": "propose_for_approval",
            "project_id": command.project_id,
            "baseline_id": baseline_id,
            "revision_id": revision_id,
            "lifecycle": "proposed",
            "content_digest": command.content_digest,
            "render_digest": command.render_digest,
            "rendered_view": target.rendered_view,
            "requires_user_authorization": true,
            "approval_target": target,
            "domain_committed": true,
        })
        .to_string();
        let (receipt, action_execution) = command_bundle(&context, &outcome_json);
        let _revision = ProjectOrchestrationRepo::propose_project_execution_baseline_command(
            &*self.db,
            SaveProjectExecutionBaselineRevisionCommand {
                project_id: command.project_id.clone(),
                baseline_id: Some(baseline_id),
                revision_id,
                expected_baseline_version: expected_version,
                base_revision,
                base_revision_id: command.base_revision_id.clone(),
                lifecycle: "proposed".to_owned(),
                charter_revision_id: command.content.charter_revision.revision_id.clone(),
                document_revisions_json: columns.document_revisions_json,
                plan_items_json: columns.plan_items_json,
                milestone_id: columns.milestone_id,
                milestone_ids_json: serde_json::to_string(&command.content.milestone_ids)
                    .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
                milestone_definition_revision_ids_json: columns
                    .milestone_definition_revision_ids_json,
                primary_milestone_id: columns.primary_milestone_id,
                release_policy_json: columns.release_policy_json,
                release_policy_revision: command.content.release_policy_revision.clone(),
                release_policy_digest: command.content.release_policy_digest.clone(),
                acceptance_matrix_json: columns.acceptance_matrix_json,
                capability_classes_json: columns.capability_classes_json,
                risk_classes_json: columns.risk_classes_json,
                adaptive_envelope_json: columns.adaptive_envelope_json,
                elevated_operations_json: columns.elevated_operations_json,
                exclusions_json: columns.exclusions_json,
                rollback_recovery_json: columns.rollback_recovery_json,
                schema_version: EXECUTION_BASELINE_SCHEMA_VERSION.to_owned(),
                render_version: command.render_version.clone(),
                rendered_view: command.rendered_view.clone(),
                content_digest: command.content_digest.clone(),
                rendered_digest: command.render_digest.clone(),
                source_refs_json: manifest,
                created_at: now_rfc3339(),
                command_receipt: Some(receipt),
                action_execution,
            },
        )
        .await
        .map_err(ServiceError::from)?;
        self.committed_outcome(&context).await
    }

    /// Create the single-use, principal-bound user approval receipt for the
    /// exact proposed revision. Approval does not activate the baseline.
    pub async fn approve(
        &self,
        command: ApproveExecutionBaselineCommand,
    ) -> Result<ExecutionBaselineCommandOutcome> {
        let context = baseline_context(
            EXECUTION_BASELINE_APPROVE_COMMAND,
            &command.idempotency_key,
            &command.authorization,
            &command,
            command.action.clone(),
            &command.project_id,
            command.expected_baseline_version,
            Some(&command.content_digest),
        )?;
        if let Some(outcome) = self.replay(&context).await? {
            return self.outcome_from_receipt(&outcome, &context).await;
        }
        validate_baseline_authorization(
            &command.authorization,
            EXECUTION_BASELINE_APPROVE_COMMAND,
            true,
        )?;
        authorize_baseline_principal(&self.db, &command.project_id, &command.authorization).await?;
        let revision = self
            .load_revision_for_project(
                &command.project_id,
                &command.baseline_id,
                &command.revision_id,
            )
            .await?;
        validate_persisted_manifest(
            &self.db,
            &command.project_id,
            &command.baseline_id,
            &revision,
            true,
        )
        .await?;
        if revision.content_digest != command.content_digest
            || revision.rendered_digest != command.render_digest
        {
            return Err(ServiceError::conflict(
                "approval does not target the exact proposed baseline review target",
            ));
        }
        ensure_reconciliation_clear(&self.db, &command.project_id).await?;
        let approval_id = new_uuid_v4();
        let outcome_json = serde_json::json!({
            "operation": "approve",
            "project_id": command.project_id,
            "baseline_id": command.baseline_id,
            "revision_id": command.revision_id,
            "approval_id": approval_id,
            "content_digest": command.content_digest,
            "render_digest": command.render_digest,
            "requires_user_authorization": false,
            "domain_committed": true,
        })
        .to_string();
        let (receipt, action_execution) = command_bundle(&context, &outcome_json);
        let _approval = ProjectOrchestrationRepo::approve_project_execution_baseline_command(
            &*self.db,
            ApproveProjectExecutionBaselineCommand {
                id: approval_id.clone(),
                baseline_id: command.baseline_id.clone(),
                revision_id: command.revision_id.clone(),
                expected_baseline_version: command.expected_baseline_version,
                expected_project_version: command.expected_project_version,
                principal_type: command.authorization.principal_type.clone(),
                principal_id: command.authorization.principal_id.clone(),
                authorization_basis: command.authorization.authorization_basis.clone(),
                authorization_action: "project.execution_baseline.approve".to_owned(),
                authorization_occurred_at: command.authorization.authorization_occurred_at.clone(),
                explicit_event: command.authorization.authorization_event_id.clone(),
                content_digest: command.content_digest.clone(),
                rendered_digest: command.render_digest.clone(),
                idempotency_key: command.idempotency_key.clone(),
                created_at: now_rfc3339(),
                updated_at: now_rfc3339(),
                command_receipt: Some(receipt),
                action_execution,
            },
        )
        .await
        .map_err(ServiceError::from)?;
        self.committed_outcome(&context).await
    }

    /// Consume the exact current approval and atomically activate the
    /// baseline, milestone definitions, project pointer, Task governance,
    /// durable event, and command receipt. Only an interactive user may call
    /// this method successfully.
    pub async fn activate(
        &self,
        command: ActivateExecutionBaselineCommand,
    ) -> Result<ExecutionBaselineCommandOutcome> {
        let context = baseline_context(
            EXECUTION_BASELINE_ACTIVATE_COMMAND,
            &command.idempotency_key,
            &command.authorization,
            &command,
            command.action.clone(),
            &command.project_id,
            command.expected_project_version,
            Some(&command.content_digest),
        )?;
        if let Some(outcome) = self.replay(&context).await? {
            return self.outcome_from_receipt(&outcome, &context).await;
        }
        validate_baseline_authorization(
            &command.authorization,
            EXECUTION_BASELINE_ACTIVATE_COMMAND,
            true,
        )?;
        authorize_baseline_principal(&self.db, &command.project_id, &command.authorization).await?;
        let revision = self
            .load_revision_for_project(
                &command.project_id,
                &command.baseline_id,
                &command.revision_id,
            )
            .await?;
        let manifest = validate_persisted_manifest(
            &self.db,
            &command.project_id,
            &command.baseline_id,
            &revision,
            true,
        )
        .await?;
        if revision.content_digest != command.content_digest
            || revision.rendered_digest != command.render_digest
        {
            return Err(ServiceError::conflict(
                "activation does not target the exact persisted baseline review target",
            ));
        }
        ensure_reconciliation_clear(&self.db, &command.project_id).await?;
        let outcome_json = serde_json::json!({
            "operation": "activate",
            "project_id": command.project_id,
            "baseline_id": command.baseline_id,
            "revision_id": command.revision_id,
            "approval_id": command.approval_id,
            "content_digest": command.content_digest,
            "render_digest": command.render_digest,
            "requires_user_authorization": false,
            "domain_committed": true,
        })
        .to_string();
        let (receipt, action_execution) = command_bundle(&context, &outcome_json);
        let _baseline = ProjectOrchestrationRepo::activate_project_execution_baseline_command(
            &*self.db,
            db::ActivateProjectExecutionBaselineCommand {
                project_id: command.project_id.clone(),
                baseline_id: command.baseline_id.clone(),
                revision_id: command.revision_id.clone(),
                approval_id: command.approval_id.clone(),
                expected_baseline_version: command.expected_baseline_version,
                expected_project_version: command.expected_project_version,
                charter_revision_id: manifest.content.charter_revision.revision_id,
                milestone_ids: manifest.content.milestone_ids,
                milestone_definition_revision_ids: manifest
                    .content
                    .milestone_definition_revision_ids,
                primary_milestone_id: manifest.content.primary_milestone_id,
                content_digest: command.content_digest,
                rendered_digest: command.render_digest,
                idempotency_key: command.idempotency_key,
                updated_at: now_rfc3339(),
                command_receipt: Some(receipt),
                action_execution,
            },
        )
        .await
        .map_err(ServiceError::from)?;
        self.committed_outcome(&context).await
    }

    async fn resolve_revision_base(
        &self,
        project_id: &str,
        baseline_id: Option<&str>,
        expected_version: Option<i64>,
        base_revision_id: Option<&str>,
    ) -> Result<(String, Option<i64>, i64)> {
        let Some(baseline_id) = baseline_id else {
            if !matches!(expected_version, None | Some(0)) || base_revision_id.is_some() {
                return Err(ServiceError::conflict(
                    "a first execution-baseline draft must use expected version 0 and cannot name a base revision",
                ));
            }
            return Ok((new_uuid_v4(), None, 0));
        };
        let baseline =
            ProjectOrchestrationRepo::get_project_execution_baseline(&*self.db, baseline_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("execution_baseline", baseline_id))?;
        if baseline.project_id != project_id {
            return Err(ServiceError::NotFound {
                entity: "execution_baseline",
                id: baseline_id.to_owned(),
            });
        }
        if expected_version != Some(baseline.version) {
            return Err(ServiceError::Db(db::DbError::VersionConflict));
        }
        let Some(base_revision_id) = base_revision_id else {
            if baseline.current_revision_id.is_some() {
                return Err(ServiceError::conflict(
                    "an existing execution baseline requires an exact base_revision_id",
                ));
            }
            return Ok((baseline_id.to_owned(), Some(baseline.version), 0));
        };
        if baseline.current_revision_id.as_deref() != Some(base_revision_id) {
            return Err(ServiceError::Db(db::DbError::VersionConflict));
        }
        let revision = ProjectOrchestrationRepo::get_project_execution_baseline_revision(
            &*self.db,
            base_revision_id,
        )
        .await?
        .ok_or_else(|| ServiceError::conflict("execution baseline base revision is stale"))?;
        if revision.baseline_id != baseline_id {
            return Err(ServiceError::Db(db::DbError::VersionConflict));
        }
        Ok((
            baseline_id.to_owned(),
            Some(baseline.version),
            revision.revision,
        ))
    }

    async fn load_revision_for_project(
        &self,
        project_id: &str,
        baseline_id: &str,
        revision_id: &str,
    ) -> Result<ProjectExecutionBaselineRevisionRecord> {
        let baseline =
            ProjectOrchestrationRepo::get_project_execution_baseline(&*self.db, baseline_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("execution_baseline", baseline_id))?;
        if baseline.project_id != project_id {
            return Err(ServiceError::NotFound {
                entity: "execution_baseline",
                id: baseline_id.to_owned(),
            });
        }
        let revision = ProjectOrchestrationRepo::get_project_execution_baseline_revision(
            &*self.db,
            revision_id,
        )
        .await?
        .ok_or_else(|| ServiceError::not_found("execution_baseline_revision", revision_id))?;
        if revision.baseline_id != baseline_id {
            return Err(ServiceError::NotFound {
                entity: "execution_baseline_revision",
                id: revision_id.to_owned(),
            });
        }
        Ok(revision)
    }

    async fn replay(&self, context: &CommandContext) -> Result<Option<db::CommandReceipt>> {
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
        .await
        .map_err(ServiceError::from)?;
        Ok(existing)
    }

    async fn outcome_from_receipt(
        &self,
        receipt: &db::CommandReceipt,
        context: &CommandContext,
    ) -> Result<ExecutionBaselineCommandOutcome> {
        if receipt.scope_type != CommandScopeType::Project.as_str()
            || receipt.scope_id != context.canonical_scope().scope_id()
            || receipt.operation != context.operation()
            || receipt.principal_type != context.principal().principal_type()
            || receipt.principal_id != context.principal().principal_id()
        {
            return Err(ServiceError::Db(db::DbError::IdempotencyConflict));
        }
        let mut outcome: ExecutionBaselineCommandOutcome =
            serde_json::from_str(&receipt.outcome_json).map_err(|_| {
                ServiceError::conflict("execution baseline receipt outcome is invalid")
            })?;
        if outcome.baseline_id.trim().is_empty() {
            return Err(ServiceError::conflict(
                "execution baseline receipt has no baseline_id",
            ));
        }
        // The receipt JSON is the immutable result.  The fallback is only for
        // pre-command receipts created before this service started persisting
        // its receipt id; it never consults mutable baseline projections.
        if outcome.receipt_id.is_none() {
            outcome.receipt_id = Some(receipt.id.clone());
        }
        Ok(outcome)
    }

    async fn committed_outcome(
        &self,
        context: &CommandContext,
    ) -> Result<ExecutionBaselineCommandOutcome> {
        let receipt = self
            .replay(context)
            .await?
            .ok_or_else(|| ServiceError::conflict("execution baseline receipt is missing"))?;
        self.outcome_from_receipt(&receipt, context).await
    }
}

/// Read-only execution-baseline projection and replay metadata shared by all
/// transports.  The repository owns the ordered baseline/revision/approval
/// reads; this service owns the canonical manifest, renderer, digest, schema,
/// and public response materialization.
#[derive(Clone)]
pub struct ExecutionBaselineQueryService {
    db: Arc<SqliteDb>,
}

impl ExecutionBaselineQueryService {
    #[must_use]
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self { db }
    }

    /// Read the authoritative baseline projection after deriving and checking
    /// Project access from the authenticated principal.
    pub async fn get(
        &self,
        project_id: &str,
        principal_id: &str,
    ) -> Result<ExecutionBaselineResponse> {
        let project = ProjectRepo::get_by_id(&*self.db, project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", project_id))?;
        let member_exists = if project.owner_id.as_deref() == Some(principal_id) {
            true
        } else {
            ProjectMemberRepo::get_member(&*self.db, project_id, principal_id)
                .await?
                .is_some()
        };
        if project.owner_id.is_some()
            && project.owner_id.as_deref() != Some(principal_id)
            && !member_exists
        {
            return Err(ServiceError::not_found("project", project_id));
        }

        let baseline = ProjectOrchestrationRepo::get_project_execution_baseline_for_project(
            &*self.db, project_id,
        )
        .await?
        .ok_or_else(|| ServiceError::not_found("execution_baseline", project_id))?;
        self.load_response(&baseline).await
    }

    /// Materialize a command outcome against the same authoritative read
    /// boundary and attach its frozen approval target, if any.
    pub async fn response_for_command(
        &self,
        project_id: &str,
        outcome: ExecutionBaselineCommandOutcome,
    ) -> Result<ExecutionBaselineResponse> {
        let baseline = ProjectOrchestrationRepo::get_project_execution_baseline(
            &*self.db,
            &outcome.baseline_id,
        )
        .await?
        .filter(|baseline| baseline.project_id == project_id)
        .ok_or_else(|| {
            ServiceError::not_found("execution_baseline", outcome.baseline_id.clone())
        })?;
        let mut response = self.load_response(&baseline).await?;
        response.requires_user_authorization = outcome.requires_user_authorization;
        response.approval_target =
            outcome
                .approval_target
                .map(|target| api_types::ExecutionBaselineApprovalTarget {
                    baseline_id: target.baseline_id,
                    revision_id: target.revision_id,
                    revision: target.revision,
                    content: target.content,
                    rendered_view: target.rendered_view,
                    render_version: target.render_version,
                    content_digest: target.content_digest,
                    render_digest: target.render_digest,
                    provenance: target.provenance,
                    requires_user_authorization: target.requires_user_authorization,
                });
        Ok(response)
    }

    /// Classify an HTTP command idempotency key without reimplementing receipt
    /// SQL in the adapter. The command service remains authoritative for
    /// digest equality and frozen replay outcomes.
    pub async fn has_command_receipt(
        &self,
        principal_type: &str,
        principal_id: &str,
        project_id: &str,
        operation: &str,
        idempotency_key: &str,
    ) -> Result<bool> {
        Ok(CommandReceiptRepo::get_command_receipt_by_identity(
            &*self.db,
            principal_type,
            principal_id,
            "project",
            project_id,
            operation,
            idempotency_key,
        )
        .await?
        .is_some())
    }

    async fn load_response(
        &self,
        baseline: &ProjectExecutionBaselineRecord,
    ) -> Result<ExecutionBaselineResponse> {
        let revisions = ProjectOrchestrationRepo::list_project_execution_baseline_revisions(
            &*self.db,
            &baseline.id,
        )
        .await?;
        let current_revision_id = baseline
            .current_revision_id
            .clone()
            .or_else(|| revisions.first().map(|revision| revision.id.clone()));
        let current_revision = match current_revision_id {
            Some(revision_id) => Some(self.render_revision(baseline, &revisions, &revision_id)?),
            None => None,
        };
        let proposed_revision = revisions.first().and_then(|latest| {
            current_revision
                .as_ref()
                .filter(|current| current.id != latest.id)
                .map(|_| latest)
        });
        let proposed_revision = proposed_revision
            .map(|revision| self.render_revision(baseline, &revisions, &revision.id))
            .transpose()?;
        let approvals = ProjectOrchestrationRepo::list_project_execution_baseline_approvals(
            &*self.db,
            &baseline.id,
        )
        .await?;
        let approval = [
            proposed_revision
                .as_ref()
                .map(|revision| revision.id.as_str()),
            current_revision
                .as_ref()
                .map(|revision| revision.id.as_str()),
        ]
        .into_iter()
        .flatten()
        .find_map(|revision_id| {
            approvals
                .iter()
                .find(|approval| {
                    approval.revision_id == revision_id
                        && matches!(approval.lifecycle.as_str(), "active" | "consumed")
                })
                .map(|approval| self.render_approval(baseline, &revisions, approval))
        })
        .transpose()?;

        Ok(ExecutionBaselineResponse {
            baseline: render_baseline(baseline)?,
            current_revision,
            proposed_revision,
            approval,
            approval_target: None,
            requires_user_authorization: false,
        })
    }

    fn render_revision(
        &self,
        baseline: &ProjectExecutionBaselineRecord,
        revisions: &[ProjectExecutionBaselineRevisionRecord],
        revision_id: &str,
    ) -> Result<ExecutionBaselineRevision> {
        let revision = revisions
            .iter()
            .find(|revision| revision.id == revision_id)
            .ok_or_else(|| ServiceError::not_found("execution_baseline_revision", revision_id))?;
        let manifest: ExecutionBaselineManifest = serde_json::from_str(&revision.source_refs_json)
            .map_err(|error| {
                ServiceError::conflict(format!(
                    "persisted execution baseline manifest is invalid: {error}"
                ))
            })?;
        if manifest.schema != EXECUTION_BASELINE_MANIFEST_SCHEMA {
            return Err(ServiceError::conflict(
                "persisted execution baseline manifest has an unknown schema",
            ));
        }
        let rendered = render_execution_baseline(&manifest.content).map_err(|error| {
            ServiceError::conflict(format!("persisted execution baseline is invalid: {error}"))
        })?;
        if revision.schema_version != EXECUTION_BASELINE_SCHEMA_VERSION
            || revision.render_version != EXECUTION_BASELINE_RENDER_VERSION
            || revision.rendered_view != manifest.rendered_view
            || manifest.rendered_view != rendered.rendered_view
            || revision.content_digest != rendered.content_digest
            || revision.rendered_digest != rendered.render_digest
        {
            return Err(ServiceError::conflict(
                "persisted execution baseline does not reproduce its approved review digests",
            ));
        }
        let activated_at = if baseline.lifecycle == "active"
            && baseline.current_revision_id.as_deref() == Some(revision_id)
        {
            Some(baseline.updated_at.clone())
        } else {
            None
        };
        Ok(ExecutionBaselineRevision {
            id: revision.id.clone(),
            baseline_id: revision.baseline_id.clone(),
            project_id: baseline.project_id.clone(),
            revision_number: revision.revision,
            base_revision_id: revision.base_revision_id.clone(),
            lifecycle: parse_baseline_lifecycle(&revision.lifecycle)?,
            schema_version: EXECUTION_BASELINE_SCHEMA_VERSION.to_owned(),
            content: manifest.content,
            rendered_view: manifest.rendered_view,
            render_version: revision.render_version.clone(),
            content_digest: revision.content_digest.clone(),
            render_digest: revision.rendered_digest.clone(),
            provenance: manifest.provenance,
            created_at: revision.created_at.clone(),
            activated_at,
        })
    }

    fn render_approval(
        &self,
        baseline: &ProjectExecutionBaselineRecord,
        revisions: &[ProjectExecutionBaselineRevisionRecord],
        approval: &ProjectExecutionBaselineApprovalRecord,
    ) -> Result<ExecutionBaselineApproval> {
        if approval.baseline_id != baseline.id {
            return Err(ServiceError::not_found(
                "execution_baseline_approval",
                approval.id.clone(),
            ));
        }
        let revision = revisions
            .iter()
            .find(|revision| revision.id == approval.revision_id)
            .ok_or_else(|| {
                ServiceError::not_found("execution_baseline_revision", approval.revision_id.clone())
            })?;
        if approval.content_digest != revision.content_digest
            || approval.rendered_digest != revision.rendered_digest
        {
            return Err(ServiceError::conflict(
                "persisted execution baseline approval does not match its revision digests",
            ));
        }
        let principal = PrincipalRef {
            kind: parse_principal_kind(&approval.principal_type),
            id: approval.principal_id.clone(),
            display_name: None,
        };
        Ok(ExecutionBaselineApproval {
            id: approval.id.clone(),
            baseline_id: approval.baseline_id.clone(),
            revision_id: approval.revision_id.clone(),
            content_digest: approval.content_digest.clone(),
            render_digest: approval.rendered_digest.clone(),
            expected_project_version: approval.expected_project_version,
            approved_by: principal.clone(),
            authorization: ApiAuthorizationProvenance {
                principal,
                authorization_basis: approval.authorization_basis.clone(),
                action: approval.authorization_action.clone(),
                event_id: approval.explicit_event.clone(),
                occurred_at: approval.authorization_occurred_at.clone(),
            },
            approved_at: approval.created_at.clone(),
            idempotency_key: public_idempotency_key(&approval.idempotency_key),
        })
    }
}

fn render_baseline(record: &ProjectExecutionBaselineRecord) -> Result<ExecutionBaseline> {
    Ok(ExecutionBaseline {
        id: record.id.clone(),
        project_id: record.project_id.clone(),
        current_revision_id: record.current_revision_id.clone(),
        lifecycle: parse_baseline_lifecycle(&record.lifecycle)?,
        version: record.version,
        created_at: record.created_at.clone(),
        updated_at: record.updated_at.clone(),
    })
}

fn parse_baseline_lifecycle(value: &str) -> Result<ExecutionBaselineLifecycle> {
    match value {
        "draft" => Ok(ExecutionBaselineLifecycle::Draft),
        "proposed" => Ok(ExecutionBaselineLifecycle::Proposed),
        "approved" => Ok(ExecutionBaselineLifecycle::Approved),
        "active" => Ok(ExecutionBaselineLifecycle::Active),
        "superseded" => Ok(ExecutionBaselineLifecycle::Superseded),
        "revoked" => Ok(ExecutionBaselineLifecycle::Revoked),
        _ => Err(ServiceError::conflict(format!(
            "unknown execution baseline lifecycle: {value}"
        ))),
    }
}

fn parse_principal_kind(value: &str) -> PrincipalKind {
    match value {
        "user" => PrincipalKind::User,
        "agent" => PrincipalKind::Agent,
        "worker" => PrincipalKind::Worker,
        "reviewer" => PrincipalKind::Reviewer,
        "service" => PrincipalKind::Service,
        _ => PrincipalKind::System,
    }
}

fn public_idempotency_key(stored_key: &str) -> String {
    let mut parts = stored_key.splitn(5, ':');
    if parts.next() == Some("forge-idem-v1")
        && parts.next().is_some()
        && parts.next().is_some()
        && parts.next().is_some()
    {
        if let Some(client_key) = parts.next() {
            return client_key.to_owned();
        }
    }
    stored_key.to_owned()
}

// Keeping the canonical command identity components explicit here makes each
// baseline operation's receipt digest auditable at its call site.
#[allow(clippy::too_many_arguments)]
fn baseline_context<T: Serialize>(
    operation: &str,
    idempotency_key: &str,
    authorization: &ProjectCommandAuthorization,
    input: &T,
    action: Option<AgentActionProvenance>,
    project_id: &str,
    expected_version: i64,
    digest_key: Option<&str>,
) -> Result<CommandContext> {
    if operation.trim().is_empty()
        || idempotency_key.trim().is_empty()
        || project_id.trim().is_empty()
        || authorization.principal_type.trim().is_empty()
        || authorization.principal_id.trim().is_empty()
    {
        return Err(ServiceError::invalid_operation(
            "execution baseline command identity is incomplete",
        ));
    }
    let mut digests = BTreeMap::new();
    if let Some(digest) = digest_key.filter(|value| !value.trim().is_empty()) {
        digests.insert("review_target".to_owned(), digest.to_owned());
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
                versions: BTreeMap::from([("baseline_version".to_owned(), expected_version)]),
                digests,
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
    .map_err(|error| ServiceError::invalid_operation(format!("execution baseline digest: {error}")))
}

fn validate_baseline_authorization(
    authorization: &ProjectCommandAuthorization,
    operation: &str,
    user_only: bool,
) -> Result<()> {
    if !matches!(authorization.principal_type.as_str(), "user" | "agent")
        || authorization.principal_id.trim().is_empty()
        || authorization.policy_result.trim().is_empty()
        || authorization.correlation_id.trim().is_empty()
        || authorization.authorization_event_id.trim().is_empty()
        || authorization.authorization_basis.trim().is_empty()
        || authorization.authorization_action != operation
        || authorization.authorization_occurred_at.trim().is_empty()
        || authorization.authorization_json.trim().is_empty()
    {
        return Err(ServiceError::invalid_operation(
            "execution baseline authorization provenance is incomplete or does not name the command",
        ));
    }
    if user_only && authorization.principal_type != "user" {
        return Err(ServiceError::AuthorizationDenied {
            message: "execution baseline approval and activation are interactive-user-only"
                .to_owned(),
        });
    }
    if serde_json::from_str::<Value>(&authorization.authorization_json).is_err() {
        return Err(ServiceError::invalid_operation(
            "execution baseline authorization_json must be valid JSON",
        ));
    }
    let occurred_at = DateTime::parse_from_rfc3339(&authorization.authorization_occurred_at)
        .map_err(|_| ServiceError::invalid_operation("authorization_occurred_at must be RFC3339"))?
        .with_timezone(&Utc);
    if (Utc::now() - occurred_at).num_seconds().abs() > MAX_AUTHORIZATION_CLOCK_SKEW_SECONDS {
        return Err(ServiceError::conflict(
            "execution baseline authorization timestamp is outside the accepted clock-skew window",
        ));
    }
    Ok(())
}

fn validate_baseline_provenance(
    provenance: &RevisionProvenance,
    authorization: &ProjectCommandAuthorization,
) -> Result<()> {
    let kind_matches = match authorization.principal_type.as_str() {
        "user" => provenance.author.kind == PrincipalKind::User,
        "agent" => provenance.author.kind == PrincipalKind::Agent,
        _ => false,
    };
    if provenance.change_summary.trim().is_empty() {
        return Err(ServiceError::invalid_operation(
            "execution baseline provenance.change_summary must be a non-empty summary of this revision",
        ));
    }
    // Echoing the caller's own already-authenticated principal is an input
    // contract, not an authority secret: naming the expected value is what
    // lets the caller correct the payload instead of retrying it unchanged.
    if provenance.author.id.trim().is_empty() || provenance.author.id != authorization.principal_id
    {
        return Err(ServiceError::invalid_operation(format!(
            "execution baseline provenance.author.id must be the authorized principal \"{}\"",
            authorization.principal_id
        )));
    }
    if !kind_matches {
        return Err(ServiceError::invalid_operation(format!(
            "execution baseline provenance.author.kind must be \"{}\"",
            authorization.principal_type
        )));
    }
    Ok(())
}

async fn authorize_baseline_principal(
    db: &SqliteDb,
    project_id: &str,
    authorization: &ProjectCommandAuthorization,
) -> Result<()> {
    match authorization.principal_type.as_str() {
        "user" => {
            let owner_id: Option<String> =
                sqlx::query_scalar("SELECT owner_id FROM project WHERE id = ?")
                    .bind(project_id)
                    .fetch_optional(db.pool())
                    .await?;
            let Some(owner_id) = owner_id else {
                return Err(ServiceError::not_found("project", project_id));
            };
            let member: Option<i64> = sqlx::query_scalar(
                "SELECT 1 FROM project_member WHERE project_id = ? AND user_id = ? LIMIT 1",
            )
            .bind(project_id)
            .bind(&authorization.principal_id)
            .fetch_optional(db.pool())
            .await?;
            if owner_id == authorization.principal_id || member.is_some() {
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
            message: "execution baseline commands accept only user or Project Agent principals"
                .to_owned(),
        }),
    }
}

fn validate_rendered_candidate(
    content: &ExecutionBaselineContent,
    rendered_view: &str,
    render_version: &str,
    content_digest: &str,
    render_digest: &str,
) -> Result<()> {
    if render_version != EXECUTION_BASELINE_RENDER_VERSION {
        return Err(ServiceError::conflict(
            "execution baseline render_version is not the current server renderer",
        ));
    }
    let rendered = render_execution_baseline(content)
        .map_err(|error| ServiceError::invalid_operation(format!("render baseline: {error}")))?;
    if rendered.rendered_view != rendered_view
        || rendered.content_digest != content_digest
        || rendered.render_digest != render_digest
    {
        return Err(ServiceError::conflict(
            "execution baseline content or rendered review target digest is not canonical",
        ));
    }
    Ok(())
}

async fn validate_baseline_content(
    db: &SqliteDb,
    project_id: &str,
    content: &ExecutionBaselineContent,
    complete: bool,
) -> Result<()> {
    let project = sqlx::query(
        "SELECT charter_status, charter_setup_required, current_charter_revision_id
         FROM project WHERE id = ?",
    )
    .bind(project_id)
    .fetch_optional(db.pool())
    .await?
    .ok_or_else(|| ServiceError::not_found("project", project_id))?;
    if complete {
        let charter_status: String = project.try_get("charter_status")?;
        let charter_setup_required: i64 = project.try_get("charter_setup_required")?;
        let current_charter_revision_id: Option<String> =
            project.try_get("current_charter_revision_id")?;
        if charter_status != "charter_backed"
            || charter_setup_required != 0
            || current_charter_revision_id.as_deref()
                != Some(content.charter_revision.revision_id.as_str())
        {
            return Err(ServiceError::conflict(
                "the execution baseline must reference the current approved Project Charter revision",
            ));
        }
        validate_artifact_ref_db(db, project_id, &content.charter_revision, true, true).await?;
    } else {
        validate_artifact_ref_db(db, project_id, &content.charter_revision, true, false).await?;
    }

    if complete
        && (content.plan_item_ids.is_empty()
            || content.milestone_ids.is_empty()
            || content.release_policy_revision.trim().is_empty()
            || content.release_policy_digest.trim().is_empty()
            || content.capability_classes.is_empty()
            || content.risk_classes.is_empty())
    {
        return Err(ServiceError::invalid_operation(
            "execution baseline requires plan items, milestones, release policy, capability classes, and risk classes",
        ));
    }
    let policy_present = !content.release_policy_revision.trim().is_empty()
        || !content.release_policy_digest.trim().is_empty()
        || !content.release_policy.schema_version.trim().is_empty();
    if complete || policy_present {
        validate_execution_baseline_policy(content).map_err(ServiceError::conflict)?;
    }
    validate_milestone_definition_pairs_service(content, complete)
        .map_err(ServiceError::invalid_operation)?;
    for document in &content.document_revisions {
        validate_artifact_ref_db(db, project_id, document, false, complete).await?;
    }
    if !content.milestone_ids.is_empty() {
        for (milestone_id, definition_id) in content
            .milestone_ids
            .iter()
            .zip(&content.milestone_definition_revision_ids)
        {
            let row = sqlx::query(
                "SELECT m.current_definition_revision_id, r.lifecycle, r.charter_revision_id
                 FROM project_milestone m
                 JOIN project_milestone_revision r ON r.milestone_id = m.id
                 WHERE m.id = ? AND m.project_id = ? AND r.id = ? LIMIT 1",
            )
            .bind(milestone_id)
            .bind(project_id)
            .bind(definition_id)
            .fetch_optional(db.pool())
            .await?
            .ok_or_else(|| {
                ServiceError::conflict(
                    "every execution baseline milestone must reference an owned definition revision",
                )
            })?;
            let current_definition_id: Option<String> =
                row.try_get("current_definition_revision_id")?;
            let lifecycle: String = row.try_get("lifecycle")?;
            let charter_revision_id: Option<String> = row.try_get("charter_revision_id")?;
            if complete
                && (current_definition_id.as_deref() != Some(definition_id.as_str())
                    || !matches!(lifecycle.as_str(), "proposed" | "approved")
                    || charter_revision_id.as_deref()
                        != Some(content.charter_revision.revision_id.as_str()))
            {
                return Err(ServiceError::conflict(
                    "every execution baseline milestone must reference its current definition tied to the Charter",
                ));
            }
        }
    }
    validate_baseline_milestone_contract(db, project_id, content, complete).await?;
    Ok(())
}

#[derive(Debug)]
struct ExpectedBaselineRequirement {
    description: String,
    evidence_kind: Option<String>,
    definition_revision_id: String,
}

async fn validate_baseline_milestone_contract(
    db: &SqliteDb,
    project_id: &str,
    content: &ExecutionBaselineContent,
    complete: bool,
) -> Result<()> {
    if !complete {
        return Ok(());
    }

    let policy_revisions = content
        .release_policy
        .required_check_definition_revisions
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    let mut expected = BTreeMap::<String, ExpectedBaselineRequirement>::new();

    for (milestone_id, definition_revision_id) in content
        .milestone_ids
        .iter()
        .zip(&content.milestone_definition_revision_ids)
    {
        if !policy_revisions.contains(definition_revision_id.as_str()) {
            return Err(ServiceError::conflict(format!(
                "release_policy.required_check_definition_revisions must include milestone definition '{definition_revision_id}'"
            )));
        }
        let row = sqlx::query(
            "SELECT r.acceptance_checks_json, r.evidence_requirements_json
             FROM project_milestone m
             JOIN project_milestone_revision r
               ON r.id = ? AND r.milestone_id = m.id
             WHERE m.id = ? AND m.project_id = ?",
        )
        .bind(definition_revision_id)
        .bind(milestone_id)
        .bind(project_id)
        .fetch_optional(db.pool())
        .await?
        .ok_or_else(|| {
            ServiceError::conflict(format!(
                "milestone '{milestone_id}' definition '{definition_revision_id}' is unavailable"
            ))
        })?;
        let checks = serde_json::from_str::<Vec<MilestoneAcceptanceCheck>>(
            &row.try_get::<String, _>("acceptance_checks_json")?,
        )
        .map_err(|error| {
            ServiceError::invalid_operation(format!(
                "milestone '{milestone_id}' acceptance contract is invalid: {error}"
            ))
        })?;
        let evidence = serde_json::from_str::<Vec<AcceptanceEvidenceRequirement>>(
            &row.try_get::<String, _>("evidence_requirements_json")?,
        )
        .map_err(|error| {
            ServiceError::invalid_operation(format!(
                "milestone '{milestone_id}' evidence contract is invalid: {error}"
            ))
        })?;

        for check in checks.iter().filter(|check| check.required) {
            let Some(evidence_requirement) = evidence
                .iter()
                .find(|requirement| requirement.required && requirement.id == check.id)
            else {
                return Err(ServiceError::conflict(format!(
                    "required acceptance check '{}' has no required evidence requirement with the same stable id; revise the milestone definition before proposing the baseline",
                    check.id
                )));
            };
            if expected
                .insert(
                    check.id.clone(),
                    ExpectedBaselineRequirement {
                        description: check.description.clone(),
                        evidence_kind: evidence_requirement.evidence_kind.clone(),
                        definition_revision_id: definition_revision_id.clone(),
                    },
                )
                .is_some()
            {
                return Err(ServiceError::conflict(format!(
                    "acceptance check id '{}' is duplicated across pinned milestone definitions",
                    check.id
                )));
            }
        }

        for requirement in evidence.iter().filter(|requirement| requirement.required) {
            if expected.contains_key(&requirement.id) {
                continue;
            }
            if expected
                .insert(
                    requirement.id.clone(),
                    ExpectedBaselineRequirement {
                        description: requirement.description.clone(),
                        evidence_kind: requirement.evidence_kind.clone(),
                        definition_revision_id: definition_revision_id.clone(),
                    },
                )
                .is_some()
            {
                return Err(ServiceError::conflict(format!(
                    "evidence requirement id '{}' is duplicated across pinned milestone definitions",
                    requirement.id
                )));
            }
        }
    }

    let mut actual = BTreeMap::new();
    for requirement in &content.acceptance_evidence_matrix {
        if actual
            .insert(requirement.id.as_str(), requirement)
            .is_some()
        {
            return Err(ServiceError::conflict(format!(
                "acceptance_evidence_matrix contains duplicate id '{}'",
                requirement.id
            )));
        }
    }
    let expected_ids = expected.keys().cloned().collect::<Vec<_>>();
    let actual_ids = actual.keys().copied().collect::<Vec<_>>();
    if expected_ids.iter().map(String::as_str).collect::<Vec<_>>() != actual_ids {
        return Err(ServiceError::conflict(format!(
            "acceptance_evidence_matrix ids must exactly match the pinned milestone contract; expected [{}], got [{}]",
            expected_ids.join(", "),
            actual_ids.join(", ")
        )));
    }
    for (id, expected_requirement) in expected {
        let requirement = actual[&id.as_str()];
        if !requirement.required {
            return Err(ServiceError::conflict(format!(
                "acceptance_evidence_matrix[{id}].required must be true"
            )));
        }
        if requirement.description != expected_requirement.description {
            return Err(ServiceError::conflict(format!(
                "acceptance_evidence_matrix[{id}].description must equal the pinned milestone description"
            )));
        }
        if requirement.evidence_kind != expected_requirement.evidence_kind {
            return Err(ServiceError::conflict(format!(
                "acceptance_evidence_matrix[{id}].evidence_kind must be {:?}, got {:?}",
                expected_requirement.evidence_kind, requirement.evidence_kind
            )));
        }
        if requirement.check_definition_revision.as_deref()
            != Some(expected_requirement.definition_revision_id.as_str())
        {
            return Err(ServiceError::conflict(format!(
                "acceptance_evidence_matrix[{id}].check_definition_revision must be '{}', got {:?}",
                expected_requirement.definition_revision_id, requirement.check_definition_revision
            )));
        }
    }
    Ok(())
}

fn validate_milestone_definition_pairs_service(
    content: &ExecutionBaselineContent,
    complete: bool,
) -> std::result::Result<(), String> {
    if content.milestone_ids.is_empty() {
        if complete {
            return Err("execution baseline must include at least one milestone".to_owned());
        }
        if !content.milestone_definition_revision_ids.is_empty() {
            return Err(
                "milestone_definition_revision_ids cannot be supplied without milestone_ids"
                    .to_owned(),
            );
        }
        return Ok(());
    }
    if content.milestone_ids.iter().any(|id| id.trim().is_empty()) {
        return Err("milestone_ids must contain non-empty identifiers".to_owned());
    }
    if content.milestone_definition_revision_ids.len() != content.milestone_ids.len() {
        return Err(
            "milestone_ids and milestone_definition_revision_ids must have the same length"
                .to_owned(),
        );
    }
    if content
        .milestone_definition_revision_ids
        .iter()
        .any(|id| id.trim().is_empty())
    {
        return Err(
            "milestone_definition_revision_ids must contain non-empty identifiers".to_owned(),
        );
    }
    if content.milestone_ids.iter().enumerate().any(|(index, id)| {
        content
            .milestone_ids
            .iter()
            .take(index)
            .any(|prior| prior == id)
    }) {
        return Err("milestone_ids must not contain duplicates".to_owned());
    }
    if content
        .milestone_definition_revision_ids
        .iter()
        .enumerate()
        .any(|(index, id)| {
            content
                .milestone_definition_revision_ids
                .iter()
                .take(index)
                .any(|prior| prior == id)
        })
    {
        return Err("milestone_definition_revision_ids must not contain duplicates".to_owned());
    }
    if let Some(primary) = content.primary_milestone_id.as_deref() {
        if !content.milestone_ids.iter().any(|id| id == primary) {
            return Err("primary_milestone_id must be included in milestone_ids".to_owned());
        }
    }
    Ok(())
}

async fn validate_artifact_ref_db(
    db: &SqliteDb,
    project_id: &str,
    reference: &api_types::ArtifactRef,
    charter: bool,
    require_approved: bool,
) -> Result<()> {
    let artifact_kind = if charter { "Charter" } else { "Document" };
    for (field, value) in [
        ("artifact_id", Some(reference.artifact_id.as_str())),
        ("revision_id", Some(reference.revision_id.as_str())),
        ("content_digest", Some(reference.content_digest.as_str())),
        ("render_version", reference.render_version.as_deref()),
        ("render_digest", reference.render_digest.as_deref()),
    ] {
        if value.is_none_or(str::is_empty) {
            return Err(ServiceError::invalid_operation(format!(
                "{artifact_kind} ArtifactRef {field} must be non-empty"
            )));
        }
    }
    let sql = if charter {
        "SELECT c.id AS artifact_id, r.content_digest, r.render_version,
                r.rendered_digest, r.lifecycle
         FROM project_charter_revision r
         JOIN project_charter c ON c.id = r.charter_id
         WHERE r.id = ? AND c.project_id = ?"
    } else {
        "SELECT d.id AS artifact_id, r.content_digest, r.render_version,
                r.rendered_digest, r.lifecycle
         FROM project_document_revision r
         JOIN project_document d ON d.id = r.document_id
         WHERE r.id = ? AND d.project_id = ?"
    };
    let row = sqlx::query(sql)
        .bind(&reference.revision_id)
        .bind(project_id)
        .fetch_optional(db.pool())
        .await?
        .ok_or_else(|| {
            ServiceError::conflict(if charter {
                "Charter revision is not owned by this Project"
            } else {
                "Document revision is not owned by this Project"
            })
        })?;
    let artifact_id: String = row.try_get("artifact_id")?;
    let content_digest: String = row.try_get("content_digest")?;
    let render_version: String = row.try_get("render_version")?;
    let render_digest: String = row.try_get("rendered_digest")?;
    let lifecycle: String = row.try_get("lifecycle")?;
    let persisted = api_types::ArtifactRef {
        artifact_id,
        revision_id: reference.revision_id.clone(),
        content_digest,
        render_version: Some(render_version),
        render_digest: Some(render_digest),
    };
    validate_persisted_artifact_ref(
        reference,
        artifact_kind,
        &persisted,
        &lifecycle,
        require_approved,
    )
}

fn validate_persisted_artifact_ref(
    reference: &api_types::ArtifactRef,
    artifact_kind: &str,
    persisted: &api_types::ArtifactRef,
    lifecycle: &str,
    require_approved: bool,
) -> Result<()> {
    if require_approved && lifecycle != "approved" {
        return Err(ServiceError::conflict(format!(
            "{artifact_kind} revision lifecycle mismatch: expected \"approved\", got {lifecycle:?}"
        )));
    }
    for (field, expected, received) in [
        (
            "artifact_id",
            persisted.artifact_id.as_str(),
            reference.artifact_id.as_str(),
        ),
        (
            "content_digest",
            persisted.content_digest.as_str(),
            reference.content_digest.as_str(),
        ),
        (
            "render_version",
            persisted.render_version.as_deref().unwrap_or_default(),
            reference.render_version.as_deref().unwrap_or_default(),
        ),
        (
            "render_digest",
            persisted.render_digest.as_deref().unwrap_or_default(),
            reference.render_digest.as_deref().unwrap_or_default(),
        ),
    ] {
        if expected != received {
            return Err(ServiceError::conflict(format!(
                "{artifact_kind} ArtifactRef {field} mismatch: expected {expected:?}, got {received:?}"
            )));
        }
    }
    Ok(())
}

async fn ensure_reconciliation_clear(db: &SqliteDb, project_id: &str) -> Result<()> {
    let required: i64 = sqlx::query_scalar(
        "SELECT (
             SELECT COUNT(*) FROM project_reconciliation_record
             WHERE project_id = ? AND state = 'required'
         ) + (
             SELECT COUNT(*) FROM project_canonical_conflict
             WHERE project_id = ?
         )",
    )
    .bind(project_id)
    .bind(project_id)
    .fetch_one(db.pool())
    .await?;
    if required > 0 {
        return Err(ServiceError::conflict(
            "Project reconciliation is required before changing the execution baseline",
        ));
    }
    Ok(())
}

fn manifest_json(
    content: &ExecutionBaselineContent,
    rendered_view: &str,
    provenance: &RevisionProvenance,
) -> Result<String> {
    serde_json::to_string(&ExecutionBaselineManifest {
        schema: EXECUTION_BASELINE_MANIFEST_SCHEMA.to_owned(),
        content: content.clone(),
        rendered_view: rendered_view.to_owned(),
        provenance: provenance.clone(),
    })
    .map_err(|error| {
        ServiceError::invalid_operation(format!("serialize baseline manifest: {error}"))
    })
}

async fn validate_persisted_manifest(
    db: &SqliteDb,
    project_id: &str,
    baseline_id: &str,
    revision: &ProjectExecutionBaselineRevisionRecord,
    complete: bool,
) -> Result<ExecutionBaselineManifest> {
    if revision.baseline_id != baseline_id
        || revision.schema_version != EXECUTION_BASELINE_SCHEMA_VERSION
        || revision.render_version != EXECUTION_BASELINE_RENDER_VERSION
        || matches!(revision.lifecycle.as_str(), "superseded" | "revoked")
    {
        return Err(ServiceError::conflict(
            "persisted execution baseline revision is stale or has an unknown schema",
        ));
    }
    let manifest: ExecutionBaselineManifest = serde_json::from_str(&revision.source_refs_json)
        .map_err(|_| ServiceError::conflict("persisted execution baseline manifest is invalid"))?;
    if manifest.schema != EXECUTION_BASELINE_MANIFEST_SCHEMA
        || manifest.rendered_view != revision.rendered_view
    {
        return Err(ServiceError::conflict(
            "persisted execution baseline manifest does not reproduce its review target",
        ));
    }
    validate_rendered_candidate(
        &manifest.content,
        &manifest.rendered_view,
        &revision.render_version,
        &revision.content_digest,
        &revision.rendered_digest,
    )?;
    validate_baseline_content(db, project_id, &manifest.content, complete).await?;
    Ok(manifest)
}

fn command_bundle(
    context: &CommandContext,
    outcome_json: &str,
) -> (CreateCommandReceipt, Option<CreateAgentActionExecution>) {
    let mut receipt = CreateCommandReceipt {
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
    };
    if let Ok(mut value) = serde_json::from_str::<Value>(outcome_json) {
        if let Some(object) = value.as_object_mut() {
            object.insert("receipt_id".to_owned(), Value::String(receipt.id.clone()));
            receipt.outcome_json =
                serde_json::to_string(&value).unwrap_or_else(|_| outcome_json.to_owned());
        }
    }
    let execution = context.action_provenance.as_ref().map(|provenance| {
        let committed_at = now_rfc3339();
        CreateAgentActionExecution {
            id: new_uuid_v4(),
            action_id: provenance.action_id.clone(),
            expected_action_version: provenance.expected_action_version,
            attempt: provenance.attempt,
            status: AgentActionExecutionStatus::Succeeded,
            result_json: Some(receipt.outcome_json.clone()),
            error: None,
            executed_by_type: provenance.executed_by_type.clone(),
            executed_by_id: provenance.executed_by_id.clone(),
            idempotency_key: provenance.execution_idempotency_key.clone(),
            action_status: AgentActionStatus::Executed,
            action_outcome_json: Some(receipt.outcome_json.clone()),
            created_at: committed_at.clone(),
            completed_at: Some(committed_at.clone()),
            updated_at: committed_at,
        }
    });
    if let Some(execution) = execution.as_ref() {
        receipt.agent_action_execution_id = Some(execution.id.clone());
    }
    (receipt, execution)
}

#[cfg(test)]
mod tests {
    use super::*;
    use api_types::{
        AdaptiveEnvelope, ArtifactRef, ExecutionBaselineContent, ExecutionBaselineReleasePolicy,
    };

    fn content() -> ExecutionBaselineContent {
        let release_policy = ExecutionBaselineReleasePolicy {
            schema_version: EXECUTION_BASELINE_RELEASE_POLICY_SCHEMA.to_owned(),
            revision: "policy-r1".to_owned(),
            required_check_definition_revisions: vec!["check-r1".to_owned()],
            reviewer_independence_rules: vec!["independent-reviewer".to_owned()],
            manual_attestation_rules: vec!["manual-attestation".to_owned()],
            waiver_rules: vec!["user-waiver".to_owned()],
            evidence_kinds: vec!["test-report".to_owned()],
            evidence_contexts: vec!["repository".to_owned()],
            evidence_freshness_rules: vec!["current-commit".to_owned()],
            dependency_rules: vec!["dependencies-green".to_owned()],
            stale_input_rules: vec!["stale-baseline-blocks".to_owned()],
            forbidden_side_effects: vec!["publish".to_owned()],
            known_issue_rules: vec!["record-known-issue".to_owned()],
            correction_rules: vec!["correct-before-release".to_owned()],
            purge_rules: vec!["purge-invalid-evidence".to_owned()],
        };
        ExecutionBaselineContent {
            charter_revision: ArtifactRef {
                artifact_id: "charter".to_owned(),
                revision_id: "charter-r1".to_owned(),
                content_digest: "charter-digest".to_owned(),
                render_version: None,
                render_digest: None,
            },
            document_revisions: Vec::new(),
            plan_item_ids: vec!["plan-1".to_owned()],
            milestone_ids: vec!["milestone-1".to_owned()],
            milestone_definition_revision_ids: vec!["milestone-definition-1".to_owned()],
            primary_milestone_id: Some("milestone-1".to_owned()),
            release_policy_revision: "policy-r1".to_owned(),
            release_policy_digest: release_policy_digest(&release_policy).expect("policy digest"),
            release_policy,
            acceptance_evidence_matrix: Vec::new(),
            capability_classes: vec!["repository_write".to_owned()],
            risk_classes: vec!["low".to_owned()],
            reviewer_independence_rules: Vec::new(),
            elevated_operations: Vec::new(),
            adaptive_envelope: AdaptiveEnvelope {
                allowed_task_operations: vec!["split".to_owned()],
                fixed_outcomes: Vec::new(),
                fixed_acceptance: Vec::new(),
                fixed_risk_classes: vec!["low".to_owned()],
                forbidden_side_effects: Vec::new(),
                elevated_operations: Vec::new(),
            },
            rollback_and_recovery: Vec::new(),
            exclusions: Vec::new(),
        }
    }

    #[test]
    fn render_and_columns_are_stable() {
        let rendered = render_execution_baseline(&content()).expect("render baseline");
        assert!(!rendered.content_digest.is_empty());
        assert!(!rendered.render_digest.is_empty());
        assert!(rendered.rendered_view.contains("repository_write"));
        let columns = baseline_column_json(&content()).expect("columns");
        assert_eq!(columns.milestone_id.as_deref(), Some("milestone-1"));
        assert!(columns.plan_items_json.contains("plan-1"));
        assert_eq!(
            columns.milestone_definition_revision_ids_json,
            r#"["milestone-definition-1"]"#
        );
    }

    #[test]
    fn release_policy_rejects_unknown_and_duplicate_rules() {
        let mut unknown = content();
        unknown.release_policy.evidence_kinds = vec!["arbitrary-rule".to_owned()];
        unknown.release_policy_digest =
            release_policy_digest(&unknown.release_policy).expect("policy digest");
        assert!(validate_execution_baseline_policy(&unknown)
            .expect_err("unknown rule must fail closed")
            .contains("unsupported"));

        let mut duplicate = content();
        duplicate.release_policy.evidence_contexts =
            vec!["repository".to_owned(), "repository".to_owned()];
        duplicate.release_policy_digest =
            release_policy_digest(&duplicate.release_policy).expect("policy digest");
        assert!(validate_execution_baseline_policy(&duplicate)
            .expect_err("duplicate rule must fail closed")
            .contains("duplicate"));
    }

    #[test]
    fn artifact_ref_mismatch_names_the_exact_field_and_values() {
        let persisted = ArtifactRef {
            artifact_id: "charter-quicklist".to_owned(),
            revision_id: "charter-r1".to_owned(),
            content_digest: "fda40eb7".to_owned(),
            render_version: Some("forge.project-charter/v1".to_owned()),
            render_digest: Some("d3e1bba0".to_owned()),
        };
        let mut received = persisted.clone();
        received.render_version = Some("forge.charter-render/v1".to_owned());
        let error =
            validate_persisted_artifact_ref(&received, "Charter", &persisted, "approved", true)
                .expect_err("the mismatched render version must fail closed");
        let ServiceError::Conflict(message) = error else {
            panic!("expected a conflict diagnostic")
        };
        assert_eq!(
            message,
            "Charter ArtifactRef render_version mismatch: expected \"forge.project-charter/v1\", got \"forge.charter-render/v1\""
        );
    }
}
