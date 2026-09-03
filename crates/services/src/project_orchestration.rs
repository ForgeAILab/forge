//! Pure Charter-domain policy.
//!
//! This module intentionally has no database, clock, authorization, chat, or
//! filesystem dependency.  It is the small deterministic core used by the
//! Charter repository and API adapters: render an exact human view, compute
//! the two artifact digests, evaluate the maturity/mode gate, compare two
//! typed revisions, and validate an approval target before a repository opens
//! a transaction.

use api_types::{
    canonical_digest, canonical_render_digest, ApproveProjectCharterRequest, CharterKnowledgeItem,
    CharterKnowledgeKind, CharterReadinessGap, CharterReadinessGapKind, CharterReadinessStatus,
    CharterRevisionLifecycle, ProductMaturity, ProjectCharter, ProjectCharterContent,
    ProjectCharterReadiness, ProjectCharterRevision, ProjectMode,
};
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

/// The renderer is a server-owned revision.  Changing it changes the exact
/// rendered-view digest and therefore requires a fresh user approval.
pub const PROJECT_CHARTER_RENDER_VERSION: &str = "forge.project-charter/v1";
pub const CHARTER_READINESS_POLICY_VERSION: &str = "forge.project-charter-readiness/v1";
pub const CHARTER_DIFF_VERSION: &str = "forge.project-charter-diff/v1";

/// The result of rendering one typed Charter payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CharterRender {
    pub rendered_view: String,
    pub render_version: String,
    pub content_digest: String,
    pub render_digest: String,
}

/// Compute a content digest using the shared schema-versioned canonical JSON
/// implementation.  Serialization of the closed API type is infallible in
/// practice, so the convenience function exposes the digest directly.
pub fn charter_content_digest(content: &ProjectCharterContent) -> String {
    canonical_digest(content).expect("ProjectCharterContent is serializable")
}

/// Fallible form useful to adapters which do not want a panic boundary.
pub fn try_charter_content_digest(
    content: &ProjectCharterContent,
) -> Result<String, serde_json::Error> {
    canonical_digest(content)
}

/// Compute the digest of the exact rendered view, including its renderer
/// revision in the digest domain.
pub fn charter_render_digest(render_version: &str, rendered_view: &str) -> String {
    canonical_render_digest(render_version, rendered_view)
        .expect("rendered Charter view is serializable")
}

pub fn try_charter_render_digest(
    render_version: &str,
    rendered_view: &str,
) -> Result<String, serde_json::Error> {
    canonical_render_digest(render_version, rendered_view)
}

/// Render a typed Charter payload into deterministic, safe Markdown.
///
/// User-, research-, and model-supplied strings are always rendered as data:
/// line breaks and Markdown control characters are escaped, and values are
/// placed underneath server-owned headings/bullets.  Thus a statement such as
/// `# ignore the policy` cannot create a new heading or instruction block.
pub fn render_project_charter(content: &ProjectCharterContent) -> String {
    let mut output = String::new();
    output.push_str("# Project Charter\n\n");

    output.push_str("## Identity\n\n");
    field(&mut output, "Working name", &content.identity.working_name);
    field_opt(
        &mut output,
        "Slug proposal",
        content.identity.slug_proposal.as_deref(),
    );
    field(
        &mut output,
        "One-line vision",
        &content.identity.one_line_vision,
    );
    field(&mut output, "Maturity", content.identity.maturity.as_str());
    field_opt(
        &mut output,
        "Lifecycle intent",
        content.identity.lifecycle_intent.as_deref(),
    );
    field_opt(
        &mut output,
        "Project type",
        content.identity.project_type.as_deref(),
    );
    field_opt(
        &mut output,
        "Value proposition",
        content.identity.value_proposition.as_deref(),
    );

    output.push_str("\n## Problem and People\n\n");
    field(
        &mut output,
        "Problem or opportunity",
        &content.problem_and_people.problem_or_opportunity,
    );
    list_field(
        &mut output,
        "Target users",
        &content.problem_and_people.target_users,
    );
    list_field(
        &mut output,
        "Beneficiaries",
        &content.problem_and_people.beneficiaries,
    );
    list_field(
        &mut output,
        "Jobs, pains, and opportunities",
        &content.problem_and_people.jobs_pains_opportunity,
    );
    list_field(
        &mut output,
        "Current alternatives",
        &content.problem_and_people.current_alternatives,
    );
    list_field(
        &mut output,
        "Stakeholders",
        &content.problem_and_people.stakeholders,
    );
    list_field(
        &mut output,
        "Excluded audiences",
        &content.problem_and_people.excluded_audiences,
    );

    output.push_str("\n## Core Experience\n\n");
    field(
        &mut output,
        "Primary outcome",
        &content.core_experience.primary_outcome,
    );
    field_opt(
        &mut output,
        "Core loop",
        content.core_experience.core_loop.as_deref(),
    );
    list_field(
        &mut output,
        "Principal journeys",
        &content.core_experience.principal_journeys,
    );

    output.push_str("\n## Scope and Deliverables\n\n");
    list_field(
        &mut output,
        "Must-have outcomes",
        &content.scope.must_have_outcomes,
    );
    list_field(
        &mut output,
        "Required deliverables",
        &content.scope.required_deliverables,
    );
    list_field(
        &mut output,
        "Later possibilities",
        &content.scope.later_possibilities,
    );
    list_field(
        &mut output,
        "Explicit non-goals",
        &content.scope.explicit_non_goals,
    );

    output.push_str("\n## Success Boundary\n\n");
    field_opt(
        &mut output,
        "Qualitative outcome",
        content.success.qualitative_outcome.as_deref(),
    );
    list_field(
        &mut output,
        "Success signals",
        &content.success.success_signals,
    );
    list_field(
        &mut output,
        "Acceptance statements",
        &content.success.acceptance_statements,
    );
    list_field(
        &mut output,
        "Required evidence",
        &content.success.required_evidence,
    );
    list_field(&mut output, "Non-claims", &content.success.non_claims);

    output.push_str("\n## Constraints and Risks\n\n");
    list_field(
        &mut output,
        "Product",
        &content.constraints_and_risks.product,
    );
    list_field(
        &mut output,
        "Time and budget",
        &content.constraints_and_risks.time_and_budget,
    );
    list_field(
        &mut output,
        "Technology",
        &content.constraints_and_risks.technology,
    );
    list_field(&mut output, "Data", &content.constraints_and_risks.data);
    list_field(
        &mut output,
        "Integrations",
        &content.constraints_and_risks.integrations,
    );
    list_field(
        &mut output,
        "Security, privacy, and compliance",
        &content.constraints_and_risks.security_privacy_compliance,
    );
    list_field(
        &mut output,
        "Accessibility",
        &content.constraints_and_risks.accessibility,
    );
    list_field(
        &mut output,
        "Operations",
        &content.constraints_and_risks.operations,
    );
    list_field(
        &mut output,
        "Migration",
        &content.constraints_and_risks.migration,
    );
    list_field(&mut output, "Launch", &content.constraints_and_risks.launch);
    list_field(
        &mut output,
        "Agent authority",
        &content.constraints_and_risks.agent_authority,
    );
    if content.constraints_and_risks.risks.is_empty() {
        field(&mut output, "Risks", "none recorded");
    } else {
        output.push_str("- Risks:\n");
        for risk in &content.constraints_and_risks.risks {
            output.push_str("  - ");
            output.push_str(&safe_markdown_text(&risk.id));
            output.push_str(": ");
            output.push_str(&safe_markdown_text(&risk.description));
            output.push('\n');
            field_nested_opt(&mut output, "Impact", risk.impact.as_deref());
            field_nested_opt(&mut output, "Treatment", risk.treatment.as_deref());
            field_nested_opt(
                &mut output,
                "Revisit trigger",
                risk.revisit_trigger.as_deref(),
            );
            if let Some(owner) = &risk.owner {
                output.push_str("    - Owner: ");
                output.push_str(&safe_principal(owner));
                output.push('\n');
            }
        }
    }

    // Rendered only when present so Charters that predate the block keep
    // their render digests byte-for-byte.
    if let Some(scaffold) = &content.scaffold {
        output.push_str("\n## Scaffold\n\n");
        field(&mut output, "Template", &scaffold.template);
        list_field(&mut output, "Packs", &scaffold.packs);
    }

    output.push_str("\n## Knowledge Ledger\n\n");
    if content.knowledge_ledger.items.is_empty() {
        output.push_str("- none recorded\n");
    } else {
        output.push_str("| ID | Statement | Kind | Normative | Transfer approved | Blocking |\n");
        output.push_str("| --- | --- | --- | --- | --- | --- |\n");
        for item in &content.knowledge_ledger.items {
            output.push('|');
            output.push_str(&safe_table_text(&item.id));
            output.push('|');
            output.push_str(&safe_table_text(&item.statement));
            output.push('|');
            output.push_str(knowledge_kind(item.kind));
            output.push('|');
            output.push_str(if item.normative { "yes" } else { "no" });
            output.push('|');
            output.push_str(if item.transfer_approved { "yes" } else { "no" });
            output.push('|');
            output.push_str(if item.blocking { "yes" } else { "no" });
            output.push_str("|\n");
        }
    }

    output.push_str("\n## Handoff Note\n\n");
    if let Some(note) = &content.handoff_note {
        field_opt(
            &mut output,
            "Recommended first action",
            note.recommended_first_action.as_deref(),
        );
        field_opt(
            &mut output,
            "Bounded summary",
            note.bounded_summary.as_deref(),
        );
        list_field(
            &mut output,
            "Unresolved item IDs",
            &note.unresolved_item_ids,
        );
    } else {
        output.push_str("- none recorded\n");
    }

    // Keep exactly one trailing newline so byte-for-byte output is stable.
    if !output.ends_with('\n') {
        output.push('\n');
    }
    output
}

/// Alias with the shorter name used by service callers.
pub fn render_charter(content: &ProjectCharterContent) -> String {
    render_project_charter(content)
}

/// Naming alias used by persistence adapters that call the frozen view a
/// Markdown render.
pub fn render_charter_markdown(content: &ProjectCharterContent) -> String {
    render_project_charter(content)
}

/// Render and digest all values needed to persist an immutable revision.
pub fn render_and_digest_charter(content: &ProjectCharterContent) -> CharterRender {
    let rendered_view = render_project_charter(content);
    CharterRender {
        rendered_view: rendered_view.clone(),
        render_version: PROJECT_CHARTER_RENDER_VERSION.to_owned(),
        content_digest: charter_content_digest(content),
        render_digest: charter_render_digest(PROJECT_CHARTER_RENDER_VERSION, &rendered_view),
    }
}

/// Naming alias for callers that want the complete digest bundle.
pub fn compute_charter_digests(content: &ProjectCharterContent) -> CharterRender {
    render_and_digest_charter(content)
}

/// Evaluate the exact readiness gate for a typed Charter.  The caller owns
/// the timestamp and policy revision, making this function deterministic and
/// easy to replay during approval or an audit.
pub fn evaluate_charter_readiness(
    content: &ProjectCharterContent,
    project_mode: ProjectMode,
    maturity: ProductMaturity,
    policy_revision: &str,
    evaluated_at: &str,
) -> ProjectCharterReadiness {
    let mut gaps = Vec::new();
    let policy_revision = policy_revision.trim();

    if policy_revision.is_empty() {
        gap(
            &mut gaps,
            CharterReadinessGapKind::MissingContent,
            "policy_revision_missing",
            "A readiness policy revision is required.",
            true,
            None,
            None,
        );
    }

    let identity = &content.identity;
    required_text(
        &mut gaps,
        "identity_working_name_missing",
        "Identity needs a non-empty working name.",
        "identity",
        &identity.working_name,
    );
    required_text(
        &mut gaps,
        "identity_vision_missing",
        "Identity needs a non-empty one-line vision.",
        "identity",
        &identity.one_line_vision,
    );
    if identity.maturity != maturity {
        gap(
            &mut gaps,
            CharterReadinessGapKind::IncoherentContent,
            "identity_maturity_mismatch",
            "Charter identity maturity must match the revision maturity.",
            true,
            Some("identity"),
            None,
        );
    }

    let people = &content.problem_and_people;
    required_text(
        &mut gaps,
        "problem_or_opportunity_missing",
        "A problem or opportunity is required.",
        "problem_and_people",
        &people.problem_or_opportunity,
    );
    if people.target_users.is_empty() && people.beneficiaries.is_empty() {
        gap(
            &mut gaps,
            CharterReadinessGapKind::MissingContent,
            "people_missing",
            "At least one target user or beneficiary is required.",
            true,
            Some("problem_and_people"),
            None,
        );
    }

    required_text(
        &mut gaps,
        "primary_outcome_missing",
        "A primary outcome is required.",
        "core_experience",
        &content.core_experience.primary_outcome,
    );
    if project_mode == ProjectMode::Standard
        && content.core_experience.principal_journeys.is_empty()
    {
        gap(
            &mut gaps,
            CharterReadinessGapKind::MissingContent,
            "principal_journeys_missing",
            "Standard Charters need at least one principal journey.",
            true,
            Some("core_experience"),
            None,
        );
    }

    if content.scope.must_have_outcomes.is_empty() && content.scope.required_deliverables.is_empty()
    {
        gap(
            &mut gaps,
            CharterReadinessGapKind::MissingContent,
            "scope_outcome_missing",
            "At least one in-scope outcome or required deliverable is required.",
            true,
            Some("scope"),
            None,
        );
    }
    if content.scope.explicit_non_goals.is_empty() {
        gap(
            &mut gaps,
            CharterReadinessGapKind::MissingContent,
            "explicit_non_goal_missing",
            "At least one explicit non-goal is required.",
            true,
            Some("scope"),
            None,
        );
    }

    if content.success.success_signals.is_empty()
        && content.success.acceptance_statements.is_empty()
    {
        gap(
            &mut gaps,
            CharterReadinessGapKind::MissingAcceptanceBoundary,
            "success_boundary_missing",
            "A success signal or acceptance statement is required.",
            true,
            Some("success"),
            None,
        );
    }

    if !has_any_constraint(content) && !has_none_known_ledger_item(content) {
        gap(
            &mut gaps,
            CharterReadinessGapKind::MissingMaterialConcern,
            "constraints_missing",
            "Record material constraints/risks or explicitly state that none are known.",
            true,
            Some("constraints_and_risks"),
            None,
        );
    }

    check_scope_coherence(content, &mut gaps);
    check_scaffold(content, &mut gaps);
    check_risks(content, &mut gaps);
    check_knowledge_ledger(content, &mut gaps);

    let material_review_required = project_mode == ProjectMode::Standard
        || matches!(
            maturity,
            ProductMaturity::Production | ProductMaturity::Critical
        );
    if material_review_required {
        check_material_concerns(content, &mut gaps);
    }

    // Make output deterministic even if a future rule appends gaps in a
    // different order.  The knowledge item id is part of the stable key.
    gaps.sort_by(|left, right| {
        (
            gap_kind_order(left.kind),
            left.code.as_str(),
            left.knowledge_item_id.as_deref().unwrap_or(""),
        )
            .cmp(&(
                gap_kind_order(right.kind),
                right.code.as_str(),
                right.knowledge_item_id.as_deref().unwrap_or(""),
            ))
    });

    let status = if gaps.iter().any(|item| item.blocking) {
        CharterReadinessStatus::Blocked
    } else {
        CharterReadinessStatus::Ready
    };
    let digest_input = ReadinessDigestInput {
        policy_revision,
        project_mode,
        maturity,
        status,
        content_digest: charter_content_digest(content),
        gaps: &gaps,
    };
    let readiness_digest =
        canonical_digest(&digest_input).expect("readiness digest input is serializable");

    ProjectCharterReadiness {
        status,
        project_mode,
        maturity,
        gaps,
        policy_revision: policy_revision.to_owned(),
        evaluated_at: evaluated_at.trim().to_owned(),
        readiness_digest,
    }
}

/// Naming alias used by the repository/service boundary.
pub fn evaluate_project_charter_readiness(
    content: &ProjectCharterContent,
    project_mode: ProjectMode,
    maturity: ProductMaturity,
    policy_revision: &str,
    evaluated_at: &str,
) -> ProjectCharterReadiness {
    evaluate_charter_readiness(
        content,
        project_mode,
        maturity,
        policy_revision,
        evaluated_at,
    )
}

/// A stable field-level semantic diff.  Arrays are compared as typed values,
/// preserving their order; object fields are visited in sorted order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CharterFieldChange {
    pub section: String,
    pub field: String,
    pub before: Option<String>,
    pub after: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CharterRevisionDiff {
    pub schema_version: String,
    pub changed_sections: Vec<String>,
    pub changes: Vec<CharterFieldChange>,
}

impl CharterRevisionDiff {
    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    pub fn change_summary(&self) -> String {
        if self.changes.is_empty() {
            return "No material Charter changes.".to_owned();
        }
        let sections = self
            .changed_sections
            .iter()
            .map(|section| humanize_section(section))
            .collect::<Vec<_>>()
            .join(", ");
        let fields = self
            .changes
            .iter()
            .map(|change| format!("{}.{}", change.section, change.field))
            .collect::<Vec<_>>()
            .join(", ");
        format!("Changed Charter sections: {sections}. Fields: {fields}.")
    }
}

fn humanize_section(section: &str) -> String {
    let mut words = section.split('_');
    let Some(first) = words.next() else {
        return String::new();
    };
    let mut label = first.to_owned();
    if let Some(initial) = label.get_mut(0..1) {
        initial.make_ascii_uppercase();
    }
    for word in words {
        label.push(' ');
        label.push_str(word);
    }
    label
}

pub fn semantic_revision_diff(
    previous: Option<&ProjectCharterContent>,
    current: &ProjectCharterContent,
) -> CharterRevisionDiff {
    let Some(previous) = previous else {
        return CharterRevisionDiff {
            schema_version: CHARTER_DIFF_VERSION.to_owned(),
            changed_sections: section_names(current),
            changes: initial_changes(current),
        };
    };

    let before_sections = section_values(previous);
    let after_sections = section_values(current);
    let mut changed_sections = Vec::new();
    let mut changes = Vec::new();

    for section in section_order() {
        let before = before_sections.get(section).expect("known Charter section");
        let after = after_sections.get(section).expect("known Charter section");
        let before_fields = flatten_object(before);
        let after_fields = flatten_object(after);
        let mut fields = BTreeSet::new();
        fields.extend(before_fields.keys().cloned());
        fields.extend(after_fields.keys().cloned());
        let mut section_changed = false;
        for field_name in fields {
            let left = before_fields.get(&field_name);
            let right = after_fields.get(&field_name);
            if left != right {
                section_changed = true;
                changes.push(CharterFieldChange {
                    section: (*section).to_owned(),
                    field: field_name,
                    before: left.cloned(),
                    after: right.cloned(),
                });
            }
        }
        if section_changed {
            changed_sections.push((*section).to_owned());
        }
    }

    CharterRevisionDiff {
        schema_version: CHARTER_DIFF_VERSION.to_owned(),
        changed_sections,
        changes,
    }
}

pub fn semantic_revision_diff_between(
    previous: &ProjectCharterRevision,
    current: &ProjectCharterRevision,
) -> CharterRevisionDiff {
    semantic_revision_diff(Some(&previous.content), &current.content)
}

/// Content-only naming alias for API/service adapters.
pub fn diff_project_charter_content(
    previous: Option<&ProjectCharterContent>,
    current: &ProjectCharterContent,
) -> CharterRevisionDiff {
    semantic_revision_diff(previous, current)
}

pub fn charter_change_summary(
    previous: Option<&ProjectCharterContent>,
    current: &ProjectCharterContent,
) -> String {
    semantic_revision_diff(previous, current).change_summary()
}

/// Errors raised before the approval repository is allowed to persist a
/// receipt.  Authorization and persistence deliberately remain outside this
/// pure validator.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CharterApprovalValidationError {
    #[error("approval references a different Charter")]
    CharterIdMismatch,
    #[error("approval references a different Charter revision")]
    RevisionIdMismatch,
    #[error("revision does not belong to the requested Charter")]
    RevisionCharterMismatch,
    #[error("expected Charter version {expected}, current version is {actual}")]
    ExpectedVersionMismatch { expected: i64, actual: i64 },
    #[error("approval expected version fields do not match")]
    ExpectedVersionFieldsMismatch,
    #[error("approval content digest does not match the revision")]
    ContentDigestMismatch,
    #[error("revision content digest is internally inconsistent")]
    RevisionContentDigestMismatch,
    #[error("approval rendered-view digest does not match the revision")]
    RenderDigestMismatch,
    #[error("revision rendered-view digest is internally inconsistent")]
    RevisionRenderDigestMismatch,
    #[error("approval project mode does not match the revision")]
    ProjectModeMismatch,
    #[error("Charter revision is not an approvable draft/proposal")]
    RevisionLifecycleMismatch,
    #[error("Charter revision is not ready: {0}")]
    RevisionNotReady(String),
    #[error("approved project name must match the Charter working name")]
    ProjectNameMismatch,
    #[error("approval field is required: {0}")]
    MissingField(&'static str),
}

/// Validate the exact candidate that a user approval endpoint received.
///
/// This does not inspect `authorization`, call an ACL, or write anything.  A
/// repository adapter must perform those operations and then persist the
/// receipt in the same transaction as its compare-and-swap pointer update.
pub fn validate_charter_approval_candidate(
    charter: &ProjectCharter,
    revision: &ProjectCharterRevision,
    candidate: &ApproveProjectCharterRequest,
) -> Result<(), CharterApprovalValidationError> {
    if candidate.charter_id != charter.id {
        return Err(CharterApprovalValidationError::CharterIdMismatch);
    }
    if candidate.revision_id != revision.id {
        return Err(CharterApprovalValidationError::RevisionIdMismatch);
    }
    if revision.charter_id != charter.id {
        return Err(CharterApprovalValidationError::RevisionCharterMismatch);
    }
    if candidate.expected_charter_version != candidate.mutation.expected_version {
        return Err(CharterApprovalValidationError::ExpectedVersionFieldsMismatch);
    }
    if candidate.expected_charter_version != charter.version {
        return Err(CharterApprovalValidationError::ExpectedVersionMismatch {
            expected: candidate.expected_charter_version,
            actual: charter.version,
        });
    }
    if let Some(expected_digest) = candidate.mutation.expected_digest.as_deref() {
        if expected_digest != revision.content_digest {
            return Err(CharterApprovalValidationError::ContentDigestMismatch);
        }
    }
    if revision.content_digest != charter_content_digest(&revision.content) {
        return Err(CharterApprovalValidationError::RevisionContentDigestMismatch);
    }
    if candidate.content_digest != revision.content_digest {
        return Err(CharterApprovalValidationError::ContentDigestMismatch);
    }
    if revision.render_digest
        != charter_render_digest(&revision.render_version, &revision.rendered_view)
    {
        return Err(CharterApprovalValidationError::RevisionRenderDigestMismatch);
    }
    if candidate.render_digest != revision.render_digest {
        return Err(CharterApprovalValidationError::RenderDigestMismatch);
    }
    if candidate.project_mode != revision.project_mode
        || candidate.project_mode != charter.project_mode
    {
        return Err(CharterApprovalValidationError::ProjectModeMismatch);
    }
    if !matches!(
        revision.lifecycle,
        CharterRevisionLifecycle::Draft | CharterRevisionLifecycle::Proposed
    ) {
        return Err(CharterApprovalValidationError::RevisionLifecycleMismatch);
    }
    match revision.readiness.as_ref() {
        Some(readiness) if readiness.status == CharterReadinessStatus::Ready => {}
        Some(readiness) => {
            return Err(CharterApprovalValidationError::RevisionNotReady(
                readiness
                    .gaps
                    .iter()
                    .filter(|gap| gap.blocking)
                    .map(|gap| gap.code.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
            ));
        }
        None => {
            return Err(CharterApprovalValidationError::RevisionNotReady(
                "readiness has not been evaluated".to_owned(),
            ));
        }
    }
    if required_trimmed(&candidate.approved_project_name).is_none()
        || required_trimmed(&candidate.approved_project_name)
            != required_trimmed(&revision.content.identity.working_name)
    {
        return Err(CharterApprovalValidationError::ProjectNameMismatch);
    }
    if candidate
        .approved_project_slug
        .as_deref()
        .is_some_and(|slug| slug.trim().is_empty())
    {
        return Err(CharterApprovalValidationError::MissingField(
            "approved_project_slug",
        ));
    }
    for (name, value) in [
        (
            "selected_project_agent_identity_id",
            candidate.selected_project_agent_identity_id.as_str(),
        ),
        (
            "selected_project_agent_profile_revision_id",
            candidate
                .selected_project_agent_profile_revision_id
                .as_str(),
        ),
        (
            "selected_project_agent_operating_skill_revision",
            candidate
                .selected_project_agent_operating_skill_revision
                .as_str(),
        ),
        (
            "selected_project_agent_policy_digest",
            candidate.selected_project_agent_policy_digest.as_str(),
        ),
    ] {
        if value.trim().is_empty() {
            return Err(CharterApprovalValidationError::MissingField(name));
        }
    }
    Ok(())
}

/// Short alias for callers that already have the approval target terminology.
pub fn validate_approval_candidate(
    charter: &ProjectCharter,
    revision: &ProjectCharterRevision,
    candidate: &ApproveProjectCharterRequest,
) -> Result<(), CharterApprovalValidationError> {
    validate_charter_approval_candidate(charter, revision, candidate)
}

#[derive(Debug, Serialize)]
struct ReadinessDigestInput<'a> {
    policy_revision: &'a str,
    project_mode: ProjectMode,
    maturity: ProductMaturity,
    status: CharterReadinessStatus,
    content_digest: String,
    gaps: &'a [CharterReadinessGap],
}

fn field(output: &mut String, label: &str, value: &str) {
    output.push_str("- ");
    output.push_str(label);
    output.push_str(": ");
    output.push_str(&safe_markdown_text(value));
    output.push('\n');
}

fn field_opt(output: &mut String, label: &str, value: Option<&str>) {
    field(output, label, value.unwrap_or("none recorded"));
}

fn field_nested_opt(output: &mut String, label: &str, value: Option<&str>) {
    output.push_str("    - ");
    output.push_str(label);
    output.push_str(": ");
    output.push_str(&safe_markdown_text(value.unwrap_or("none recorded")));
    output.push('\n');
}

fn list_field(output: &mut String, label: &str, values: &[String]) {
    output.push_str("- ");
    output.push_str(label);
    output.push_str(":\n");
    if values.is_empty() {
        output.push_str("  - none recorded\n");
    } else {
        for value in values {
            output.push_str("  - ");
            output.push_str(&safe_markdown_text(value));
            output.push('\n');
        }
    }
}

fn safe_markdown_text(value: &str) -> String {
    let flattened = value
        .replace(['\r', '\n', '\t'], " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    let mut escaped = String::with_capacity(flattened.len());
    for character in flattened.chars() {
        match character {
            '\\' | '`' | '*' | '_' | '{' | '}' | '[' | ']' | '(' | ')' | '#' | '+' | '-' | '.'
            | '!' | '|' | '>' | '~' => {
                escaped.push('\\');
                escaped.push(character);
            }
            '<' => escaped.push_str("&lt;"),
            _ => escaped.push(character),
        }
    }
    if escaped.is_empty() {
        "none recorded".to_owned()
    } else {
        escaped
    }
}

fn safe_table_text(value: &str) -> String {
    safe_markdown_text(value).replace('|', "\\|")
}

fn safe_principal(principal: &api_types::PrincipalRef) -> String {
    match principal.display_name.as_deref() {
        Some(name) if !name.trim().is_empty() => format!(
            "{} ({})",
            safe_markdown_text(name),
            safe_markdown_text(&principal.id)
        ),
        _ => safe_markdown_text(&principal.id),
    }
}

fn knowledge_kind(kind: CharterKnowledgeKind) -> &'static str {
    match kind {
        CharterKnowledgeKind::ObservedFact => "observed fact",
        CharterKnowledgeKind::UserDecision => "user decision",
        CharterKnowledgeKind::ResearchFinding => "research finding",
        CharterKnowledgeKind::Assumption => "assumption",
        CharterKnowledgeKind::Hypothesis => "hypothesis",
        CharterKnowledgeKind::OpenDecision => "open decision",
        CharterKnowledgeKind::ResearchQueue => "research queue",
    }
}

fn required_text(
    gaps: &mut Vec<CharterReadinessGap>,
    code: &'static str,
    message: &'static str,
    section: &'static str,
    value: &str,
) {
    if value.trim().is_empty() {
        gap(
            gaps,
            CharterReadinessGapKind::MissingContent,
            code,
            message,
            true,
            Some(section),
            None,
        );
    }
}

fn gap(
    gaps: &mut Vec<CharterReadinessGap>,
    kind: CharterReadinessGapKind,
    code: &'static str,
    message: &'static str,
    blocking: bool,
    section: Option<&'static str>,
    knowledge_item_id: Option<&str>,
) {
    gaps.push(CharterReadinessGap {
        kind,
        code: code.to_owned(),
        message: message.to_owned(),
        blocking,
        section: section.map(str::to_owned),
        knowledge_item_id: knowledge_item_id.map(str::to_owned),
    });
}

fn check_scope_coherence(content: &ProjectCharterContent, gaps: &mut Vec<CharterReadinessGap>) {
    let must_have = content
        .scope
        .must_have_outcomes
        .iter()
        .map(|value| normalized(value))
        .collect::<BTreeSet<_>>();
    let non_goals = content
        .scope
        .explicit_non_goals
        .iter()
        .map(|value| normalized(value))
        .collect::<BTreeSet<_>>();
    for overlap in must_have.intersection(&non_goals) {
        gap(
            gaps,
            CharterReadinessGapKind::IncoherentContent,
            "scope_outcome_is_explicit_non_goal",
            "An outcome cannot be both in scope and an explicit non-goal.",
            true,
            Some("scope"),
            Some(overlap.as_str()),
        );
    }
}

/// A scaffold names spark ids. Forge does not carry spark's catalog — the
/// create-spark run at provisioning is the authority on which template and
/// packs exist — but an id that is not even a slug can never resolve, and a
/// pack listed twice is a contradiction the user should see before approval.
fn check_scaffold(content: &ProjectCharterContent, gaps: &mut Vec<CharterReadinessGap>) {
    let Some(scaffold) = &content.scaffold else {
        return;
    };
    if !is_scaffold_slug(&scaffold.template) {
        gap(
            gaps,
            CharterReadinessGapKind::IncoherentContent,
            "scaffold_template_invalid",
            "Scaffold template must be a spark template id (lowercase letters, digits, and hyphens).",
            true,
            Some("scaffold"),
            None,
        );
    }
    if scaffold.packs.iter().any(|pack| !is_scaffold_slug(pack)) {
        gap(
            gaps,
            CharterReadinessGapKind::IncoherentContent,
            "scaffold_pack_invalid",
            "Every scaffold pack must be a spark pack id (lowercase letters, digits, and hyphens).",
            true,
            Some("scaffold"),
            None,
        );
    }
    let mut seen = std::collections::BTreeSet::new();
    if scaffold
        .packs
        .iter()
        .any(|pack| !seen.insert(pack.as_str()))
    {
        gap(
            gaps,
            CharterReadinessGapKind::IncoherentContent,
            "scaffold_pack_duplicated",
            "A scaffold pack is listed more than once.",
            true,
            Some("scaffold"),
            None,
        );
    }
}

fn is_scaffold_slug(value: &str) -> bool {
    let mut chars = value.chars();
    matches!(chars.next(), Some(first) if first.is_ascii_lowercase() || first.is_ascii_digit())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}

fn check_risks(content: &ProjectCharterContent, gaps: &mut Vec<CharterReadinessGap>) {
    let mut ids = BTreeSet::new();
    for risk in &content.constraints_and_risks.risks {
        if risk.id.trim().is_empty() || risk.description.trim().is_empty() {
            gap(
                gaps,
                CharterReadinessGapKind::IncoherentContent,
                "risk_identity_or_description_missing",
                "Every risk needs a stable id and description.",
                true,
                Some("constraints_and_risks"),
                None,
            );
        }
        if !risk.id.trim().is_empty() && !ids.insert(risk.id.trim().to_owned()) {
            gap(
                gaps,
                CharterReadinessGapKind::IncoherentContent,
                "risk_id_duplicate",
                "Risk IDs must be unique within a Charter.",
                true,
                Some("constraints_and_risks"),
                Some(risk.id.trim()),
            );
        }
    }
}

fn check_knowledge_ledger(content: &ProjectCharterContent, gaps: &mut Vec<CharterReadinessGap>) {
    let mut ids = BTreeSet::new();
    for item in &content.knowledge_ledger.items {
        let id = item.id.trim();
        if id.is_empty() || item.statement.trim().is_empty() {
            gap(
                gaps,
                CharterReadinessGapKind::IncoherentContent,
                "knowledge_item_missing_identity_or_statement",
                "Every knowledge item needs a stable id and statement.",
                true,
                Some("knowledge_ledger"),
                None,
            );
        }
        if !id.is_empty() && !ids.insert(id.to_owned()) {
            gap(
                gaps,
                CharterReadinessGapKind::IncoherentContent,
                "knowledge_item_id_duplicate",
                "Knowledge item IDs must be unique within a Charter.",
                true,
                Some("knowledge_ledger"),
                Some(id),
            );
        }

        if item.blocking {
            let message = match item.kind {
                CharterKnowledgeKind::OpenDecision | CharterKnowledgeKind::ResearchQueue => {
                    "A blocking open decision or research queue item must be resolved before approval."
                }
                _ => {
                    "A knowledge item explicitly marked blocking must be resolved before approval."
                }
            };
            gap(
                gaps,
                CharterReadinessGapKind::UnresolvedBlockingUnknown,
                "blocking_knowledge_item",
                message,
                true,
                Some("knowledge_ledger"),
                Some(id),
            );
            if item
                .impact
                .as_deref()
                .is_none_or(|value| value.trim().is_empty())
            {
                gap(
                    gaps,
                    CharterReadinessGapKind::IncoherentContent,
                    "blocking_knowledge_impact_missing",
                    "A blocking knowledge item must state its impact.",
                    true,
                    Some("knowledge_ledger"),
                    Some(id),
                );
            }
        }

        if item.normative && !item.transfer_approved {
            gap(
                gaps,
                CharterReadinessGapKind::InvalidTransfer,
                "normative_knowledge_not_transfer_approved",
                "Normative knowledge must be explicitly approved for Project transfer.",
                true,
                Some("knowledge_ledger"),
                Some(id),
            );
        }

        if item.normative && !matches!(item.kind, CharterKnowledgeKind::UserDecision) {
            gap(
                gaps,
                CharterReadinessGapKind::IncoherentContent,
                "normative_knowledge_not_user_decision",
                "Normative Charter knowledge must be represented as an explicit user decision.",
                true,
                Some("knowledge_ledger"),
                Some(id),
            );
        }

        if matches!(
            item.kind,
            CharterKnowledgeKind::ObservedFact
                | CharterKnowledgeKind::UserDecision
                | CharterKnowledgeKind::ResearchFinding
        ) && item.provenance.is_empty()
        {
            gap(
                gaps,
                CharterReadinessGapKind::MissingProvenance,
                "knowledge_provenance_missing",
                "Observed facts, user decisions, and research findings need safe provenance.",
                item.normative || item.blocking,
                Some("knowledge_ledger"),
                Some(id),
            );
        }
    }
}

fn check_material_concerns(content: &ProjectCharterContent, gaps: &mut Vec<CharterReadinessGap>) {
    let concerns = [
        (
            "data",
            "data_concern_missing",
            &content.constraints_and_risks.data,
            &["data", "sensitive", "privacy" as &str][..],
        ),
        (
            "integrations",
            "integration_concern_missing",
            &content.constraints_and_risks.integrations,
            &["integration", "external", "api" as &str][..],
        ),
        (
            "security_privacy_compliance",
            "security_concern_missing",
            &content.constraints_and_risks.security_privacy_compliance,
            &["security", "privacy", "compliance", "regulated" as &str][..],
        ),
        (
            "accessibility",
            "accessibility_concern_missing",
            &content.constraints_and_risks.accessibility,
            &["accessibility", "a11y", "assistive" as &str][..],
        ),
        (
            "operations",
            "operations_recovery_concern_missing",
            &content.constraints_and_risks.operations,
            &[
                "operation",
                "observability",
                "recovery",
                "backup",
                "incident" as &str,
            ][..],
        ),
        (
            "migration",
            "migration_concern_missing",
            &content.constraints_and_risks.migration,
            &["migration", "compatibility", "upgrade", "legacy" as &str][..],
        ),
        (
            "launch",
            "launch_concern_missing",
            &content.constraints_and_risks.launch,
            &["launch", "rollout", "support", "resource" as &str][..],
        ),
    ];
    for (section, code, values, keywords) in concerns {
        if values.iter().any(|value| !value.trim().is_empty())
            || ledger_covers_concern(&content.knowledge_ledger.items, keywords)
        {
            continue;
        }
        gap(
            gaps,
            CharterReadinessGapKind::MissingMaterialConcern,
            code,
            "Standard/production Charters must resolve, mark inapplicable, or visibly queue this material concern.",
            true,
            Some(section),
            None,
        );
    }
}

fn ledger_covers_concern(items: &[CharterKnowledgeItem], keywords: &[&str]) -> bool {
    items.iter().any(|item| {
        let statement = normalized(&item.statement);
        let keyword_match = keywords.iter().any(|keyword| statement.contains(keyword));
        keyword_match
            && !item.blocking
            && matches!(
                item.kind,
                CharterKnowledgeKind::ObservedFact
                    | CharterKnowledgeKind::UserDecision
                    | CharterKnowledgeKind::ResearchFinding
                    | CharterKnowledgeKind::Assumption
                    | CharterKnowledgeKind::Hypothesis
                    | CharterKnowledgeKind::OpenDecision
                    | CharterKnowledgeKind::ResearchQueue
            )
    })
}

fn has_any_constraint(content: &ProjectCharterContent) -> bool {
    let constraints = &content.constraints_and_risks;
    [
        &constraints.product,
        &constraints.time_and_budget,
        &constraints.technology,
        &constraints.data,
        &constraints.integrations,
        &constraints.security_privacy_compliance,
        &constraints.accessibility,
        &constraints.operations,
        &constraints.migration,
        &constraints.launch,
        &constraints.agent_authority,
    ]
    .into_iter()
    .any(|values| values.iter().any(|value| !value.trim().is_empty()))
        || !constraints.risks.is_empty()
}

fn has_none_known_ledger_item(content: &ProjectCharterContent) -> bool {
    content.knowledge_ledger.items.iter().any(|item| {
        !item.blocking
            && normalized(&item.statement).contains("none known")
            && matches!(
                item.kind,
                CharterKnowledgeKind::UserDecision
                    | CharterKnowledgeKind::ObservedFact
                    | CharterKnowledgeKind::Assumption
            )
    })
}

fn normalized(value: &str) -> String {
    value
        .trim()
        .to_ascii_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn required_trimmed(value: &str) -> Option<&str> {
    let value = value.trim();
    (!value.is_empty()).then_some(value)
}

fn gap_kind_order(kind: CharterReadinessGapKind) -> u8 {
    match kind {
        CharterReadinessGapKind::MissingContent => 0,
        CharterReadinessGapKind::IncoherentContent => 1,
        CharterReadinessGapKind::UnresolvedBlockingUnknown => 2,
        CharterReadinessGapKind::MissingProvenance => 3,
        CharterReadinessGapKind::MissingAcceptanceBoundary => 4,
        CharterReadinessGapKind::MissingMaterialConcern => 5,
        CharterReadinessGapKind::InvalidTransfer => 6,
    }
}

fn section_order() -> &'static [&'static str] {
    &[
        "identity",
        "problem_and_people",
        "core_experience",
        "scope",
        "success",
        "constraints_and_risks",
        "scaffold",
        "knowledge_ledger",
        "handoff_note",
    ]
}

fn section_values(content: &ProjectCharterContent) -> BTreeMap<&'static str, Value> {
    let mut values = BTreeMap::new();
    values.insert(
        "identity",
        serde_json::to_value(&content.identity).expect("Charter identity serializes"),
    );
    values.insert(
        "problem_and_people",
        serde_json::to_value(&content.problem_and_people)
            .expect("Charter problem and people serialize"),
    );
    values.insert(
        "core_experience",
        serde_json::to_value(&content.core_experience).expect("Charter experience serializes"),
    );
    values.insert(
        "scope",
        serde_json::to_value(&content.scope).expect("Charter scope serializes"),
    );
    values.insert(
        "success",
        serde_json::to_value(&content.success).expect("Charter success serializes"),
    );
    values.insert(
        "constraints_and_risks",
        serde_json::to_value(&content.constraints_and_risks)
            .expect("Charter constraints serialize"),
    );
    values.insert(
        "scaffold",
        serde_json::to_value(&content.scaffold).expect("Charter scaffold serializes"),
    );
    values.insert(
        "knowledge_ledger",
        serde_json::to_value(&content.knowledge_ledger)
            .expect("Charter knowledge ledger serializes"),
    );
    values.insert(
        "handoff_note",
        serde_json::to_value(&content.handoff_note).expect("Charter handoff note serializes"),
    );
    values
}

fn flatten_object(value: &Value) -> BTreeMap<String, String> {
    let mut flattened = BTreeMap::new();
    flatten_value("", value, &mut flattened);
    flattened
}

fn flatten_value(prefix: &str, value: &Value, output: &mut BTreeMap<String, String>) {
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                let path = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                flatten_value(&path, value, output);
            }
        }
        _ => {
            let path = if prefix.is_empty() { "value" } else { prefix };
            output.insert(
                path.to_owned(),
                serde_json::to_string(value).expect("JSON value serializes"),
            );
        }
    }
}

fn section_names(content: &ProjectCharterContent) -> Vec<String> {
    section_order()
        .iter()
        .filter(|section| {
            let values = section_values(content);
            values.contains_key(**section)
        })
        .map(|section| (*section).to_owned())
        .collect()
}

fn initial_changes(content: &ProjectCharterContent) -> Vec<CharterFieldChange> {
    let values = section_values(content);
    let mut changes = Vec::new();
    for section in section_order() {
        let fields = flatten_object(values.get(section).expect("known Charter section"));
        for (field, after) in fields {
            changes.push(CharterFieldChange {
                section: (*section).to_owned(),
                field,
                before: None,
                after: Some(after),
            });
        }
    }
    changes
}

#[cfg(test)]
mod tests {
    use super::*;
    use api_types::{
        AuthorizationProvenance, CharterConstraintsAndRisks, CharterCoreExperience,
        CharterHandoffNote, CharterIdentity, CharterKnowledgeLedger, CharterProblemAndPeople,
        CharterScope, CharterSuccessBoundary, PrincipalKind, PrincipalRef, ProvenanceRef,
        ProvenanceSourceKind, RevisionProvenance,
    };

    fn content(maturity: ProductMaturity) -> ProjectCharterContent {
        ProjectCharterContent {
            identity: CharterIdentity {
                working_name: "Prooflist".to_owned(),
                slug_proposal: Some("prooflist".to_owned()),
                one_line_vision: "Make delivery proof easy to trust".to_owned(),
                maturity,
                lifecycle_intent: Some("internal beta".to_owned()),
                project_type: Some("developer tool".to_owned()),
                value_proposition: Some("Visible evidence for each outcome".to_owned()),
            },
            problem_and_people: CharterProblemAndPeople {
                problem_or_opportunity: "Teams lose confidence when delivery evidence is scattered"
                    .to_owned(),
                target_users: vec!["small product teams".to_owned()],
                beneficiaries: vec!["reviewers".to_owned()],
                jobs_pains_opportunity: vec!["verify an outcome quickly".to_owned()],
                current_alternatives: vec!["chat links and screenshots".to_owned()],
                stakeholders: vec!["engineering".to_owned()],
                excluded_audiences: vec!["regulated external release".to_owned()],
            },
            core_experience: CharterCoreExperience {
                primary_outcome: "A reviewer can verify the delivered outcome".to_owned(),
                core_loop: Some("capture, attach, review".to_owned()),
                principal_journeys: vec!["worker delivers then reviewer checks".to_owned()],
            },
            scope: CharterScope {
                must_have_outcomes: vec!["attach delivery evidence".to_owned()],
                required_deliverables: vec!["working web flow".to_owned()],
                later_possibilities: vec!["external sharing".to_owned()],
                explicit_non_goals: vec!["automatic public publishing".to_owned()],
            },
            success: CharterSuccessBoundary {
                qualitative_outcome: Some("Evidence is understandable".to_owned()),
                success_signals: vec!["reviewer completes verification".to_owned()],
                acceptance_statements: vec!["image and video evidence can be opened".to_owned()],
                required_evidence: vec!["one screenshot".to_owned()],
                non_claims: vec!["does not prove production safety".to_owned()],
            },
            constraints_and_risks: CharterConstraintsAndRisks {
                product: vec!["single Project".to_owned()],
                time_and_budget: vec!["one week".to_owned()],
                technology: vec!["existing Forge stack".to_owned()],
                data: if matches!(
                    maturity,
                    ProductMaturity::Production | ProductMaturity::Critical
                ) {
                    vec!["no sensitive user data".to_owned()]
                } else {
                    Vec::new()
                },
                integrations: if matches!(
                    maturity,
                    ProductMaturity::Production | ProductMaturity::Critical
                ) {
                    vec!["Forge API only".to_owned()]
                } else {
                    Vec::new()
                },
                security_privacy_compliance: if matches!(
                    maturity,
                    ProductMaturity::Production | ProductMaturity::Critical
                ) {
                    vec!["no credentials in evidence".to_owned()]
                } else {
                    Vec::new()
                },
                accessibility: if matches!(
                    maturity,
                    ProductMaturity::Production | ProductMaturity::Critical
                ) {
                    vec!["keyboard reachable".to_owned()]
                } else {
                    Vec::new()
                },
                operations: if matches!(
                    maturity,
                    ProductMaturity::Production | ProductMaturity::Critical
                ) {
                    vec!["logs and recovery".to_owned()]
                } else {
                    Vec::new()
                },
                migration: if matches!(
                    maturity,
                    ProductMaturity::Production | ProductMaturity::Critical
                ) {
                    vec!["preserve existing media".to_owned()]
                } else {
                    Vec::new()
                },
                launch: if matches!(
                    maturity,
                    ProductMaturity::Production | ProductMaturity::Critical
                ) {
                    vec!["staged launch".to_owned()]
                } else {
                    Vec::new()
                },
                agent_authority: vec!["agent cannot release".to_owned()],
                risks: vec![],
            },
            knowledge_ledger: CharterKnowledgeLedger { items: vec![] },
            scaffold: None,
            handoff_note: Some(CharterHandoffNote {
                recommended_first_action: Some("draft the Delivery Brief".to_owned()),
                bounded_summary: Some("start with the smallest verifiable flow".to_owned()),
                unresolved_item_ids: vec![],
            }),
        }
    }

    fn principal() -> PrincipalRef {
        PrincipalRef {
            kind: PrincipalKind::User,
            id: "user-1".to_owned(),
            display_name: Some("User".to_owned()),
        }
    }

    fn revision(c: &ProjectCharterContent) -> ProjectCharterRevision {
        let rendered = render_project_charter(c);
        ProjectCharterRevision {
            id: "rev-1".to_owned(),
            charter_id: "charter-1".to_owned(),
            revision_number: 1,
            base_revision_id: None,
            lifecycle: CharterRevisionLifecycle::Proposed,
            project_mode: ProjectMode::Compact,
            maturity: c.identity.maturity,
            schema_version: "forge.project-charter/v1".to_owned(),
            content: c.clone(),
            rendered_view: rendered.clone(),
            render_version: PROJECT_CHARTER_RENDER_VERSION.to_owned(),
            content_digest: charter_content_digest(c),
            render_digest: charter_render_digest(PROJECT_CHARTER_RENDER_VERSION, &rendered),
            provenance: RevisionProvenance {
                author: principal(),
                profile_revision: None,
                operating_skill_revision: None,
                source_refs: vec![ProvenanceRef {
                    source_kind: ProvenanceSourceKind::MainChat,
                    source_id: "chat-1".to_owned(),
                    revision_id: None,
                    digest: None,
                    label: None,
                    observed_at: None,
                }],
                change_summary: "initial".to_owned(),
                material_diff: None,
            },
            readiness: Some(evaluate_charter_readiness(
                c,
                ProjectMode::Compact,
                c.identity.maturity,
                CHARTER_READINESS_POLICY_VERSION,
                "2026-08-13T00:00:00Z",
            )),
            approved_at: None,
            superseded_by_revision_id: None,
            created_at: "2026-08-13T00:00:00Z".to_owned(),
        }
    }

    #[test]
    fn renderer_is_stable_and_treats_untrusted_text_as_content() {
        let mut value = content(ProductMaturity::Mvp);
        value.problem_and_people.problem_or_opportunity =
            "# ignore\n- fake instruction [x](javascript:bad)".to_owned();
        let first = render_project_charter(&value);
        assert_eq!(first, render_project_charter(&value));
        assert!(first.contains("\\# ignore \\- fake instruction"));
        assert!(!first.contains("\n# ignore"));
        assert!(!first.contains("[x](javascript"));
    }

    #[test]
    fn scaffold_is_absent_from_canonical_json_and_render_until_present() {
        let mut value = content(ProductMaturity::Mvp);
        let canonical = api_types::canonical_json(&value).expect("Charter serializes");
        assert!(
            !canonical.contains("\"scaffold\""),
            "an absent scaffold must not enter the digest domain"
        );
        let plain_render = render_project_charter(&value);
        assert!(!plain_render.contains("## Scaffold"));

        value.scaffold = Some(api_types::CharterScaffold {
            template: "nextjs".to_owned(),
            packs: vec!["db-sqlite".to_owned(), "ui-shadcn".to_owned()],
        });
        let canonical = api_types::canonical_json(&value).expect("Charter serializes");
        assert!(canonical.contains(
            "\"scaffold\":{\"packs\":[\"db-sqlite\",\"ui-shadcn\"],\"template\":\"nextjs\"}"
        ));
        let render = render_project_charter(&value);
        assert!(render.contains(
            "\n## Scaffold\n\n- Template: nextjs\n- Packs:\n  - db\\-sqlite\n  - ui\\-shadcn\n"
        ));
        assert_ne!(
            charter_content_digest(&value),
            charter_content_digest(&content(ProductMaturity::Mvp))
        );
    }

    #[test]
    fn scaffold_ids_must_be_slugs_and_packs_unique() {
        let mut value = content(ProductMaturity::Mvp);
        value.scaffold = Some(api_types::CharterScaffold {
            template: "Next JS".to_owned(),
            packs: vec!["db-sqlite".to_owned(), "db-sqlite".to_owned()],
        });
        let result = evaluate_charter_readiness(
            &value,
            ProjectMode::Compact,
            ProductMaturity::Mvp,
            CHARTER_READINESS_POLICY_VERSION,
            "2026-08-13T00:00:00Z",
        );
        assert_eq!(result.status, CharterReadinessStatus::Blocked);
        let codes: Vec<&str> = result.gaps.iter().map(|gap| gap.code.as_str()).collect();
        assert!(codes.contains(&"scaffold_template_invalid"), "{codes:?}");
        assert!(codes.contains(&"scaffold_pack_duplicated"), "{codes:?}");
        assert!(result
            .gaps
            .iter()
            .all(|gap| gap.code != "scaffold_pack_invalid"));

        value.scaffold = Some(api_types::CharterScaffold {
            template: "vite-react".to_owned(),
            packs: vec![],
        });
        let result = evaluate_charter_readiness(
            &value,
            ProjectMode::Compact,
            ProductMaturity::Mvp,
            CHARTER_READINESS_POLICY_VERSION,
            "2026-08-13T00:00:00Z",
        );
        assert_eq!(result.status, CharterReadinessStatus::Ready);
    }

    #[test]
    fn content_and_render_digests_are_distinct_and_stable() {
        let value = content(ProductMaturity::Mvp);
        let rendered = render_project_charter(&value);
        assert_eq!(
            charter_content_digest(&value),
            charter_content_digest(&value)
        );
        assert_eq!(
            charter_render_digest(PROJECT_CHARTER_RENDER_VERSION, &rendered),
            charter_render_digest(PROJECT_CHARTER_RENDER_VERSION, &rendered)
        );
        assert_ne!(
            charter_render_digest("forge.project-charter/v2", &rendered),
            charter_render_digest(PROJECT_CHARTER_RENDER_VERSION, &rendered)
        );
    }

    #[test]
    fn prototype_and_mvp_compact_fast_path_are_ready() {
        for maturity in [ProductMaturity::Prototype, ProductMaturity::Mvp] {
            let result = evaluate_charter_readiness(
                &content(maturity),
                ProjectMode::Compact,
                maturity,
                CHARTER_READINESS_POLICY_VERSION,
                "2026-08-13T00:00:00Z",
            );
            assert_eq!(result.status, CharterReadinessStatus::Ready, "{maturity:?}");
        }
    }

    #[test]
    fn production_and_critical_require_material_concerns() {
        for maturity in [ProductMaturity::Production, ProductMaturity::Critical] {
            let mut value = content(maturity);
            value.constraints_and_risks.launch.clear();
            let result = evaluate_charter_readiness(
                &value,
                ProjectMode::Standard,
                maturity,
                CHARTER_READINESS_POLICY_VERSION,
                "2026-08-13T00:00:00Z",
            );
            assert_eq!(
                result.status,
                CharterReadinessStatus::Blocked,
                "{maturity:?}"
            );
            assert!(result
                .gaps
                .iter()
                .any(|gap| gap.code == "launch_concern_missing"));
        }
    }

    #[test]
    fn incoherence_and_typed_blocking_unknowns_block() {
        let mut value = content(ProductMaturity::Mvp);
        value.scope.explicit_non_goals = value.scope.must_have_outcomes.clone();
        value.knowledge_ledger.items.push(CharterKnowledgeItem {
            id: "unknown-1".to_owned(),
            statement: "The integration choice is still open".to_owned(),
            kind: CharterKnowledgeKind::OpenDecision,
            normative: false,
            transfer_approved: true,
            provenance: vec![],
            confidence: None,
            observed_at: None,
            freshness_expires_at: None,
            impact: Some("changes architecture".to_owned()),
            owner: None,
            default_value: None,
            revisit_trigger: Some("research result".to_owned()),
            falsification_evidence: None,
            blocking: true,
        });
        let result = evaluate_charter_readiness(
            &value,
            ProjectMode::Compact,
            ProductMaturity::Mvp,
            CHARTER_READINESS_POLICY_VERSION,
            "2026-08-13T00:00:00Z",
        );
        assert_eq!(result.status, CharterReadinessStatus::Blocked);
        assert!(result
            .gaps
            .iter()
            .any(|gap| gap.kind == CharterReadinessGapKind::IncoherentContent));
        assert!(result
            .gaps
            .iter()
            .any(|gap| gap.kind == CharterReadinessGapKind::UnresolvedBlockingUnknown));
    }

    #[test]
    fn non_blocking_queued_unknown_is_visible_but_can_be_ready() {
        let mut value = content(ProductMaturity::Mvp);
        value.knowledge_ledger.items.push(CharterKnowledgeItem {
            id: "queue-1".to_owned(),
            statement: "Research accessibility options after handoff".to_owned(),
            kind: CharterKnowledgeKind::ResearchQueue,
            normative: false,
            transfer_approved: true,
            provenance: vec![],
            confidence: None,
            observed_at: None,
            freshness_expires_at: None,
            impact: Some("could change UI polish".to_owned()),
            owner: None,
            default_value: None,
            revisit_trigger: Some("before release".to_owned()),
            falsification_evidence: None,
            blocking: false,
        });
        let result = evaluate_charter_readiness(
            &value,
            ProjectMode::Compact,
            ProductMaturity::Mvp,
            CHARTER_READINESS_POLICY_VERSION,
            "2026-08-13T00:00:00Z",
        );
        assert_eq!(result.status, CharterReadinessStatus::Ready);
        assert!(result.gaps.is_empty());
    }

    #[test]
    fn semantic_diff_is_stable_and_section_aware() {
        let before = content(ProductMaturity::Mvp);
        let mut after = before.clone();
        after.identity.working_name = "Prooflist Next".to_owned();
        after
            .scope
            .must_have_outcomes
            .push("show release history".to_owned());
        let diff = semantic_revision_diff(Some(&before), &after);
        assert_eq!(diff, semantic_revision_diff(Some(&before), &after));
        assert_eq!(diff.changed_sections, vec!["identity", "scope"]);
        assert!(diff
            .changes
            .iter()
            .any(|change| change.field == "working_name"));
        assert!(charter_change_summary(Some(&before), &after).contains("Identity"));
    }

    #[test]
    fn approval_validation_rejects_stale_or_mismatched_target() {
        let value = content(ProductMaturity::Mvp);
        let rev = revision(&value);
        let charter = ProjectCharter {
            id: "charter-1".to_owned(),
            genesis_session_id: Some("genesis-1".to_owned()),
            project_id: None,
            state: api_types::ProjectCharterState::CharterSetupRequired,
            project_mode: ProjectMode::Compact,
            maturity: ProductMaturity::Mvp,
            current_draft_revision_id: Some(rev.id.clone()),
            current_approved_revision_id: None,
            version: 4,
            created_at: "2026-08-13T00:00:00Z".to_owned(),
            updated_at: "2026-08-13T00:00:00Z".to_owned(),
        };
        let request = ApproveProjectCharterRequest {
            mutation: api_types::MutationEnvelope {
                expected_version: 3,
                expected_digest: Some(rev.content_digest.clone()),
                idempotency_key: "approve-1".to_owned(),
                deduplication_key: None,
                authorization: AuthorizationProvenance {
                    principal: principal(),
                    authorization_basis: "user".to_owned(),
                    action: "approve".to_owned(),
                    event_id: "event-1".to_owned(),
                    occurred_at: "2026-08-13T00:00:00Z".to_owned(),
                },
            },
            charter_id: charter.id.clone(),
            revision_id: rev.id.clone(),
            content_digest: rev.content_digest.clone(),
            render_digest: rev.render_digest.clone(),
            expected_charter_version: 3,
            expected_project_version: Some(1),
            approved_project_name: "Prooflist".to_owned(),
            approved_project_slug: Some("prooflist".to_owned()),
            project_mode: ProjectMode::Compact,
            selected_project_agent_identity_id: "identity-1".to_owned(),
            selected_project_agent_profile_revision_id: "profile-1".to_owned(),
            selected_project_agent_operating_skill_revision: "skill-1".to_owned(),
            selected_project_agent_policy_digest: "policy-1".to_owned(),
        };
        assert!(matches!(
            validate_charter_approval_candidate(&charter, &rev, &request),
            Err(CharterApprovalValidationError::ExpectedVersionMismatch { .. })
        ));
    }

    #[test]
    fn approval_validation_accepts_exact_candidate_without_authorizing_it() {
        let value = content(ProductMaturity::Mvp);
        let rev = revision(&value);
        let charter = ProjectCharter {
            id: "charter-1".to_owned(),
            genesis_session_id: Some("genesis-1".to_owned()),
            project_id: None,
            state: api_types::ProjectCharterState::CharterSetupRequired,
            project_mode: ProjectMode::Compact,
            maturity: ProductMaturity::Mvp,
            current_draft_revision_id: Some(rev.id.clone()),
            current_approved_revision_id: None,
            version: 3,
            created_at: "2026-08-13T00:00:00Z".to_owned(),
            updated_at: "2026-08-13T00:00:00Z".to_owned(),
        };
        let request = ApproveProjectCharterRequest {
            mutation: api_types::MutationEnvelope {
                expected_version: 3,
                expected_digest: Some(rev.content_digest.clone()),
                idempotency_key: "approve-1".to_owned(),
                deduplication_key: None,
                authorization: AuthorizationProvenance {
                    principal: principal(),
                    authorization_basis: "untrusted test text".to_owned(),
                    action: "approve".to_owned(),
                    event_id: "event-1".to_owned(),
                    occurred_at: "2026-08-13T00:00:00Z".to_owned(),
                },
            },
            charter_id: charter.id.clone(),
            revision_id: rev.id.clone(),
            content_digest: rev.content_digest.clone(),
            render_digest: rev.render_digest.clone(),
            expected_charter_version: 3,
            expected_project_version: Some(1),
            approved_project_name: "Prooflist".to_owned(),
            approved_project_slug: Some("prooflist".to_owned()),
            project_mode: ProjectMode::Compact,
            selected_project_agent_identity_id: "identity-1".to_owned(),
            selected_project_agent_profile_revision_id: "profile-1".to_owned(),
            selected_project_agent_operating_skill_revision: "skill-1".to_owned(),
            selected_project_agent_policy_digest: "policy-1".to_owned(),
        };
        assert!(validate_charter_approval_candidate(&charter, &rev, &request).is_ok());
    }
}
