//! Regression coverage for evidence provenance at the readiness boundary.
//!
//! Evidence that names a required check is not enough by itself: it must still
//! describe the Task/execution/definition context used by the current
//! readiness evaluation. These tests intentionally exercise the pure
//! evaluator so the rule cannot be bypassed by a stale overview projection.

use api_types::{
    AcceptanceCheckSourceKind, AcceptanceEvidenceRequirement, ArtifactRef, AuthorizationProvenance,
    CharterRisk, EvidenceAttachment, EvidenceAvailability, EvidenceKind, MilestoneAcceptanceCheck,
    MilestoneDefinitionContent, MilestoneDefinitionLifecycle, MilestoneDefinitionRevision,
    MilestoneLifecycle, PrincipalKind, PrincipalRef, ProjectMilestone, ReadinessInput,
    RevisionProvenance,
};
use serde_json::json;
use services::{
    evaluate_readiness, ReadinessDocumentState, ReadinessEvaluationInput, ReadinessTaskState,
};

fn principal(kind: PrincipalKind, id: &str) -> PrincipalRef {
    PrincipalRef {
        kind,
        id: id.to_owned(),
        display_name: None,
    }
}

fn milestone() -> ProjectMilestone {
    ProjectMilestone {
        id: "milestone-1".to_owned(),
        project_id: "project-1".to_owned(),
        milestone_sequence: 1,
        canonical_id: "M001".to_owned(),
        display_label: Some("M1".to_owned()),
        definition_revision_id: "definition-1".to_owned(),
        lifecycle: MilestoneLifecycle::Active,
        projection_reasons: Vec::new(),
        version: 2,
        created_at: "2026-08-20T00:00:00Z".to_owned(),
        updated_at: "2026-08-20T00:00:00Z".to_owned(),
    }
}

fn definition() -> MilestoneDefinitionRevision {
    MilestoneDefinitionRevision {
        id: "definition-1".to_owned(),
        milestone_id: "milestone-1".to_owned(),
        project_id: "project-1".to_owned(),
        revision_number: 1,
        base_revision_id: None,
        lifecycle: MilestoneDefinitionLifecycle::Approved,
        schema_version: "forge.milestone/v1".to_owned(),
        content: MilestoneDefinitionContent {
            name: "Outcome".to_owned(),
            outcome: "A useful outcome".to_owned(),
            included_scope: vec!["in".to_owned()],
            excluded_scope: Vec::new(),
            charter_revision: Some(ArtifactRef {
                artifact_id: "charter".to_owned(),
                revision_id: "charter-r1".to_owned(),
                content_digest: "charter-digest".to_owned(),
                render_version: None,
                render_digest: None,
            }),
            document_revisions: Vec::new(),
            task_ids: vec!["task-1".to_owned()],
            dependencies: Vec::new(),
            risks: vec![CharterRisk {
                id: "risk-1".to_owned(),
                description: "risk".to_owned(),
                impact: None,
                treatment: None,
                revisit_trigger: None,
                owner: None,
            }],
            acceptance_checks: vec![MilestoneAcceptanceCheck {
                id: "check-1".to_owned(),
                description: "check".to_owned(),
                required: false,
                source_kind: AcceptanceCheckSourceKind::Manual,
                expected_result: "pass".to_owned(),
                latest_result: None,
                latest_result_id: None,
                latest_result_digest: None,
            }],
            evidence_requirements: vec![AcceptanceEvidenceRequirement {
                id: "evidence-1".to_owned(),
                description: "current proof".to_owned(),
                required: true,
                evidence_kind: Some("screenshot".to_owned()),
                check_definition_revision: Some("definition-1".to_owned()),
            }],
            known_issues: Vec::new(),
            target_date: None,
        },
        rendered_view: "Outcome".to_owned(),
        render_version: "v1".to_owned(),
        content_digest: "definition-digest".to_owned(),
        render_digest: "render-digest".to_owned(),
        provenance: RevisionProvenance {
            author: principal(PrincipalKind::User, "user-1"),
            profile_revision: None,
            operating_skill_revision: None,
            source_refs: Vec::new(),
            change_summary: "initial".to_owned(),
            material_diff: None,
        },
        created_at: "2026-08-20T00:00:00Z".to_owned(),
    }
}

fn evidence(
    source_run_id: &str,
    source_task_version: i64,
    context_label: &str,
) -> EvidenceAttachment {
    let context = json!({
        "task_id": "task-1",
        "execution_id": source_run_id,
        "context": context_label,
    });
    let source_context_digest = api_types::canonical_digest_with_schema(
        services::MILESTONE_READINESS_DIGEST_SCHEMA_VERSION,
        &context,
    )
    .expect("evidence context digest");
    EvidenceAttachment {
        id: format!("evidence-{source_run_id}"),
        project_id: "project-1".to_owned(),
        asset_id: format!("asset-{source_run_id}"),
        task_id: Some("task-1".to_owned()),
        source_task_id: Some("task-1".to_owned()),
        source_run_id: Some(source_run_id.to_owned()),
        source_validation_id: None,
        source_task_version: Some(source_task_version),
        source_context_digest: Some(source_context_digest),
        source_definition_revision_id: Some("definition-1".to_owned()),
        milestone_id: Some("milestone-1".to_owned()),
        acceptance_check_ids: vec!["evidence-1".to_owned()],
        caption: "A useful proof".to_owned(),
        kind: EvidenceKind::Screenshot,
        checksum: format!("checksum-{source_run_id}"),
        availability: EvidenceAvailability::Available,
        author: principal(PrincipalKind::Worker, "worker-1"),
        captured_at: "2026-08-20T00:00:00Z".to_owned(),
        version: 1,
        created_at: "2026-08-20T00:00:00Z".to_owned(),
        removed_at: None,
    }
}

fn input(evidence: Vec<EvidenceAttachment>) -> ReadinessEvaluationInput {
    let current_context = json!({
        "task_id": "task-1",
        "execution_id": "run-new",
        "context": "execution-context-new",
    });
    ReadinessEvaluationInput {
        milestone: milestone(),
        definition: definition(),
        source_event_watermark: "event-10".to_owned(),
        computing_policy_revision: "compute-r1".to_owned(),
        input_manifest: vec![ReadinessInput {
            source_kind: "task".to_owned(),
            source_id: "task-1".to_owned(),
            source_version: 2,
            source_digest: "execution-context-new".to_owned(),
            observed_at: "2026-08-20T00:00:00Z".to_owned(),
        }],
        check_results: Vec::new(),
        evidence,
        waiver_ids: Vec::new(),
        task_states: vec![ReadinessTaskState {
            task_id: "task-1".to_owned(),
            version: 2,
            task_type: "task".to_owned(),
            state: "done".to_owned(),
            observed_at: "2026-08-20T00:00:00Z".to_owned(),
        }],
        document_states: vec![ReadinessDocumentState {
            document_id: "doc-1".to_owned(),
            revision_id: "doc-r1".to_owned(),
            version: 1,
            lifecycle: "approved".to_owned(),
            current_approved: true,
            content_digest: "doc-digest".to_owned(),
            observed_at: "2026-08-20T00:00:00Z".to_owned(),
        }],
        commit_build_check_context: vec![
            serde_json::to_string(&current_context).expect("readiness context serializes")
        ],
        definition_contract_reasons: Vec::new(),
        authorization: AuthorizationProvenance {
            principal: principal(PrincipalKind::User, "user-1"),
            authorization_basis: "release-review".to_owned(),
            action: "project.milestone.readiness".to_owned(),
            event_id: "readiness-event".to_owned(),
            occurred_at: "2026-08-20T00:00:00Z".to_owned(),
        },
    }
}

#[test]
fn old_task_run_evidence_cannot_satisfy_current_required_gate() {
    let stale = evaluate_readiness(input(vec![evidence("run-old", 1, "execution-context-old")]))
        .expect("readiness computes");

    assert_ne!(stale.result, api_types::ReadinessResult::Ready);
    assert!(stale
        .reasons
        .iter()
        .any(|reason| reason.code.contains("evidence")));
}

#[test]
fn current_task_run_evidence_satisfies_current_required_gate() {
    let attachment = evidence("run-new", 2, "execution-context-new");
    let current = evaluate_readiness(input(vec![attachment])).expect("readiness computes");

    assert_eq!(current.result, api_types::ReadinessResult::Ready);
}
