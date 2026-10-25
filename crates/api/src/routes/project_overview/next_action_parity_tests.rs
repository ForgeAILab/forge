//! Test overview next actions against registered service and route targets.

use super::*;

#[derive(Debug, Clone, Copy)]
enum NextActionTarget {
    /// A real REST route registered in `crates/api/src/lib.rs`. The
    /// backing handler(s) are referenced by path in
    /// `rest_backed_operations_reference_real_handlers` below.
    Rest,
    /// `required_principal` is `project_agent`/`worker`: the
    /// "Continue with Project Agent" chat link (or ordinary Task
    /// execution) is the real target because that principal already
    /// has its own typed command for the domain action.
    AutomatedByResponsiblePrincipal,
    /// A plain re-fetch of the same query. Only valid for a stale or
    /// unavailable projection -- never for a current canonical
    /// conflict, per design invariant D15/8.1.8.
    Refresh,
}

/// Every operation `next_action()` can currently produce, and its real
/// target. `registered_target` panics on an unregistered operation, so
/// this table is the enforcement point: a new `next_action()` branch
/// that advertises an unregistered operation fails the coverage test
/// below before it can ship as another dead action.
const REGISTRY: &[(&str, NextActionTarget)] = &[
    ("project.charter.adoption", NextActionTarget::Rest),
    ("project.reconciliation.resolve", NextActionTarget::Rest),
    ("project.milestone.revision.save", NextActionTarget::Rest),
    (
        "project.document.reconcile",
        NextActionTarget::AutomatedByResponsiblePrincipal,
    ),
    (
        "project.execution_setup.retry_provisioning",
        NextActionTarget::Rest,
    ),
    ("project.execution_setup.refresh", NextActionTarget::Refresh),
    ("project.execution_setup", NextActionTarget::Rest),
    (
        "task.remediate",
        NextActionTarget::AutomatedByResponsiblePrincipal,
    ),
    (
        "project.milestone.revision.transition",
        NextActionTarget::Rest,
    ),
    ("project.milestone.check.record", NextActionTarget::Rest),
    (
        "project.milestone.check.evaluate",
        NextActionTarget::AutomatedByResponsiblePrincipal,
    ),
    // F15 is closed: pending Decision candidates are now typed
    // `pending_decisions` summaries with a real approve/reject target
    // (`crate::routes::project_documents::approve_decision_candidate` /
    // `reject_decision_candidate`), not an opaque UUID with no handler.
    ("project.decision.resolve", NextActionTarget::Rest),
    ("project.milestone.evidence.attach", NextActionTarget::Rest),
    ("project.milestone.readiness", NextActionTarget::Rest),
    ("project.milestone.release", NextActionTarget::Rest),
    ("project.milestone.create", NextActionTarget::Rest),
    ("project.projection.reconcile", NextActionTarget::Refresh),
];

fn registered_target(operation: &str) -> NextActionTarget {
    REGISTRY
            .iter()
            .find(|(name, _)| *name == operation)
            .unwrap_or_else(|| {
                panic!(
                    "next_action() produced operation '{operation}' with no registered \
                     service/route/UI target -- add it to REGISTRY in project_overview/next_action_parity_tests.rs \
                     and give it a real target before shipping"
                )
            })
            .1
}

/// Compile-time existence proof for every `NextActionTarget::Rest`
/// entry above: referencing a handler by path fails this module to
/// compile if the handler is ever renamed or removed.
#[test]
fn rest_backed_operations_reference_real_handlers() {
    let _ = crate::routes::project_charters::approve_project_charter_revision;
    let _ = crate::routes::reconciliations::resolve_project_reconciliation;
    let _ = crate::routes::milestones::save_milestone_revision;
    let _ = crate::routes::project_execution_setup::retry_provisioning;
    let _ = crate::routes::project_execution_setup::select_worker;
    let _ = crate::routes::project_execution_setup::attach_primary_repository;
    let _ = crate::routes::milestones::record_milestone_check;
    let _ = crate::routes::project_media::attach_evidence;
    let _ = crate::routes::milestones::evaluate_readiness;
    let _ = crate::routes::milestones::release_milestone;
    let _ = crate::routes::milestones::create_milestone;
    let _ = crate::routes::project_documents::approve_decision_candidate;
    let _ = crate::routes::project_documents::reject_decision_candidate;
}

fn base_execution_setup(
    coordination_state: api_types::CoordinationState,
    execution_setup_state: api_types::ExecutionSetupState,
    execution_gate: api_types::ExecutionGate,
) -> api_types::ProjectExecutionSetupResponse {
    api_types::ProjectExecutionSetupResponse {
        project_id: "project-1".to_owned(),
        project_version: 7,
        coordination_state,
        execution_setup_state,
        execution_gate,
        availability: api_types::ProjectExecutionSetupAvailability {
            coordination: api_types::ProjectionStatus::current(),
            execution_setup: api_types::ProjectionStatus::current(),
            execution_gate: api_types::ProjectionStatus::current(),
        },
        primary_repo: None,
        worker: None,
        independent_reviewer: None,
        eligible_workers: Vec::new(),
        eligible_reviewers: Vec::new(),
        setup_requirements: Vec::new(),
        next_action: None,
        provisioning: None,
        execution_blocker: None,
    }
}

fn ready_execution_setup() -> api_types::ProjectExecutionSetupResponse {
    base_execution_setup(
        api_types::CoordinationState::Ready,
        api_types::ExecutionSetupState::Ready,
        api_types::ExecutionGate::Active,
    )
}

struct Fixture {
    charter_setup_required: bool,
    no_milestones: bool,
    execution_setup: api_types::ProjectExecutionSetupResponse,
    milestones: Vec<Value>,
    documents: Vec<Value>,
    releases: Vec<Value>,
    pending_decision_ids: Vec<String>,
    task_counts: Value,
    failed_task_count: i64,
    checks: Value,
    reconciliation_required: bool,
    stale: bool,
}

impl Fixture {
    fn base() -> Self {
        Self {
            charter_setup_required: false,
            no_milestones: false,
            execution_setup: ready_execution_setup(),
            milestones: Vec::new(),
            documents: Vec::new(),
            releases: Vec::new(),
            pending_decision_ids: Vec::new(),
            task_counts: json!({}),
            failed_task_count: 0,
            checks: json!({}),
            reconciliation_required: false,
            stale: false,
        }
    }

    fn action(&self) -> ProjectNextAction {
        next_action(NextActionContext {
            project_id: "project-1",
            project_version: 7,
            charter_setup_required: self.charter_setup_required,
            no_milestones: self.no_milestones,
            execution_setup: &self.execution_setup,
            milestones: &self.milestones,
            documents: &self.documents,
            releases: &self.releases,
            pending_decision_ids: &self.pending_decision_ids,
            task_counts: &self.task_counts,
            failed_task_count: self.failed_task_count,
            checks: &self.checks,
            reconciliation_required: self.reconciliation_required,
            stale: self.stale,
        })
        .expect("fixture is constructed to always trigger exactly one next action")
    }

    fn maybe_action(&self) -> Option<ProjectNextAction> {
        next_action(NextActionContext {
            project_id: "project-1",
            project_version: 7,
            charter_setup_required: self.charter_setup_required,
            no_milestones: self.no_milestones,
            execution_setup: &self.execution_setup,
            milestones: &self.milestones,
            documents: &self.documents,
            releases: &self.releases,
            pending_decision_ids: &self.pending_decision_ids,
            task_counts: &self.task_counts,
            failed_task_count: self.failed_task_count,
            checks: &self.checks,
            reconciliation_required: self.reconciliation_required,
            stale: self.stale,
        })
    }
}

fn default_milestone(id: &str, version: i64) -> Value {
    json!({
        "milestone": {"id": id, "version": version, "projection_reasons": []},
        "definition": {"content": {"evidence_requirements": []}},
        "evidence": [],
        "current_checks": [],
        "latest_readiness": null,
        "readiness_freshness": {"status": "current"},
    })
}

/// A released milestone still reaches this projection through the
/// Project's primary-milestone clause. Its finished evidence and check
/// work must not be re-advertised as a blocking next action.
#[test]
fn released_milestones_project_no_blocking_next_action() {
    let mut fixture = Fixture::base();
    let mut milestone = default_milestone("milestone-released", 1);
    milestone["milestone"]["lifecycle"] = json!("released");
    milestone["definition"]["content"]["evidence_requirements"] =
        json!([{"id": "evidence-1", "required": true}]);
    milestone["evidence"] = json!([]);
    milestone["latest_readiness"] = json!({"result": "ready", "reasons": []});
    fixture.milestones = vec![milestone.clone()];
    assert!(
        fixture.maybe_action().is_none(),
        "a released milestone must not produce a next action"
    );

    // The same milestone still blocks while it is active.
    milestone["milestone"]["lifecycle"] = json!("active");
    let mut fixture = Fixture::base();
    fixture.milestones = vec![milestone];
    assert_eq!(
        fixture
            .maybe_action()
            .expect("active milestone blocks")
            .code,
        "evidence_required"
    );
}

/// Exercise every branch of `next_action()`, and assert its operation
/// has a registered target. This is the parity guarantee: a future
/// branch that advertises an unregistered operation fails here instead
/// of shipping as another dead action.
#[test]
fn every_next_action_operation_has_a_registered_target() {
    let mut scenarios: Vec<(&str, Fixture)> = Vec::new();

    let mut fixture = Fixture::base();
    fixture.charter_setup_required = true;
    scenarios.push(("charter_setup_required", fixture));

    let mut fixture = Fixture::base();
    fixture.reconciliation_required = true;
    scenarios.push(("reconciliation_required", fixture));

    let mut fixture = Fixture::base();
    let mut milestone = default_milestone("milestone-1", 3);
    milestone["milestone"]["projection_reasons"] = json!([{"kind": "reconciliation"}]);
    fixture.milestones = vec![milestone];
    scenarios.push(("milestone_reconciliation", fixture));

    let mut fixture = Fixture::base();
    fixture.documents = vec![json!({"status": "stale"})];
    scenarios.push(("document_reconciliation", fixture));

    let mut fixture = Fixture::base();
    fixture.execution_setup = base_execution_setup(
        api_types::CoordinationState::Ready,
        api_types::ExecutionSetupState::Provisioning,
        api_types::ExecutionGate::PreBaselineReadOnly,
    );
    scenarios.push(("execution_setup_provisioning", fixture));

    let mut fixture = Fixture::base();
    fixture.execution_setup = base_execution_setup(
        api_types::CoordinationState::Ready,
        api_types::ExecutionSetupState::Failed,
        api_types::ExecutionGate::PreBaselineReadOnly,
    );
    scenarios.push(("execution_setup_failed", fixture));

    let mut fixture = Fixture::base();
    fixture.execution_setup = base_execution_setup(
        api_types::CoordinationState::Ready,
        api_types::ExecutionSetupState::Unavailable,
        api_types::ExecutionGate::PreBaselineReadOnly,
    );
    scenarios.push(("execution_setup_unavailable", fixture));

    let mut fixture = Fixture::base();
    fixture.execution_setup = base_execution_setup(
        api_types::CoordinationState::SetupRequired,
        api_types::ExecutionSetupState::SetupRequired,
        api_types::ExecutionGate::PreBaselineReadOnly,
    );
    scenarios.push(("execution_setup_required", fixture));

    let mut fixture = Fixture::base();
    fixture.execution_setup = base_execution_setup(
        api_types::CoordinationState::Ready,
        api_types::ExecutionSetupState::Ready,
        api_types::ExecutionGate::ReconciliationRequired,
    );
    scenarios.push(("execution_gate_reconciliation", fixture));

    let mut fixture = Fixture::base();
    let mut milestone = default_milestone("milestone-2", 1);
    milestone["latest_readiness"] = json!({
        "result": "not_ready",
        "reasons": [{"code": "baseline_check_definition_reconciliation_required"}],
    });
    fixture.milestones = vec![milestone];
    scenarios.push(("acceptance_contract_reconciliation", fixture));

    let mut fixture = Fixture::base();
    let mut milestone = default_milestone("milestone-8", 4);
    milestone["definition"]["id"] = json!("revision-8");
    milestone["definition"]["lifecycle"] = json!("proposed");
    fixture.milestones = vec![milestone];
    scenarios.push(("milestone_definition_approval", fixture));

    let mut fixture = Fixture::base();
    fixture.task_counts = json!({"blocked": 1});
    scenarios.push(("task_blocked_remediation", fixture));

    let mut fixture = Fixture::base();
    fixture.failed_task_count = 1;
    scenarios.push(("task_failure_remediation", fixture));

    let mut fixture = Fixture::base();
    let mut milestone = default_milestone("milestone-3", 1);
    milestone["current_checks"] = json!([{
        "id": "check-1",
        "version": 1,
        "required": true,
        "source_kind": "manual",
        "latest_result": null,
    }]);
    fixture.milestones = vec![milestone];
    scenarios.push(("manual_attestation_required", fixture));

    let mut fixture = Fixture::base();
    fixture.checks = json!({"failed": 1});
    scenarios.push(("validation_failure_remediation", fixture));

    let mut fixture = Fixture::base();
    fixture.checks = json!({"stale": 1});
    scenarios.push(("validation_required", fixture));

    let mut fixture = Fixture::base();
    fixture.pending_decision_ids = vec!["decision-1".to_owned()];
    scenarios.push(("decision_resolution", fixture));

    let mut fixture = Fixture::base();
    let mut milestone = default_milestone("milestone-4", 1);
    milestone["definition"]["content"]["evidence_requirements"] =
        json!([{"id": "req-1", "required": true}]);
    fixture.milestones = vec![milestone];
    scenarios.push(("evidence_required", fixture));

    let mut fixture = Fixture::base();
    fixture.milestones = vec![default_milestone("milestone-5", 1)];
    scenarios.push(("readiness_request", fixture));

    let mut fixture = Fixture::base();
    let mut milestone = default_milestone("milestone-6", 1);
    milestone["latest_readiness"] = json!({"result": "not_ready", "reasons": []});
    fixture.milestones = vec![milestone];
    scenarios.push(("readiness_blocked", fixture));

    let mut fixture = Fixture::base();
    let mut milestone = default_milestone("milestone-7", 1);
    milestone["latest_readiness"] = json!({"result": "ready", "reasons": []});
    fixture.milestones = vec![milestone];
    scenarios.push(("user_release", fixture));

    let mut fixture = Fixture::base();
    fixture.no_milestones = true;
    scenarios.push(("milestone_definition", fixture));

    let mut fixture = Fixture::base();
    fixture.stale = true;
    scenarios.push(("projection_reconciliation", fixture));

    let mut produced: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for (name, fixture) in &scenarios {
        let action = fixture.action();
        assert!(
            !action.route_or_operation.trim().is_empty(),
            "{name} produced an empty operation"
        );
        // registered_target panics with a clear message on a miss, so a
        // plain lookup is the assertion that this operation has a
        // service/route/UI target.
        let _ = registered_target(&action.route_or_operation);
        produced.insert(action.route_or_operation);
    }

    // Reverse direction: every registered operation must actually be
    // reachable from `next_action()` (several scenarios intentionally
    // share one operation under a different `code`, so this is a set
    // comparison, not a 1:1 scenario-to-registry count). A registry
    // entry that no branch can ever produce is stale and must be
    // deleted, not left to imply coverage that does not exist.
    for (operation, _) in REGISTRY {
        assert!(
            produced.contains(*operation),
            "REGISTRY names '{operation}' but no next_action() scenario produces it \
                 -- delete the stale entry or add the scenario that reaches it"
        );
    }

    let reconciliation_action = Fixture {
        reconciliation_required: true,
        ..Fixture::base()
    }
    .action();
    assert_eq!(
        reconciliation_action.route_or_operation,
        "project.reconciliation.resolve"
    );
    assert!(matches!(
        registered_target(&reconciliation_action.route_or_operation),
        NextActionTarget::Rest
    ));
}

#[test]
fn unapproved_definition_revision_asks_the_user_before_worker_evidence() {
    // InkDrop: the Agent proposed definition revision 2, nobody approved
    // it, and the Overview answered with "Attach required evidence" for
    // the Worker plus a stale banner whose only button re-fetched the
    // same projection.
    let mut fixture = Fixture::base();
    let mut milestone = default_milestone("milestone-1", 5);
    milestone["definition"]["id"] = json!("revision-2");
    milestone["definition"]["lifecycle"] = json!("proposed");
    milestone["definition"]["content"]["evidence_requirements"] =
        json!([{"id": "req-1", "required": true}]);
    fixture.milestones = vec![milestone];
    fixture.checks = json!({"missing": 1});
    fixture.stale = true;

    let action = fixture.action();
    assert_eq!(action.code, "milestone_definition_approval");
    assert_eq!(action.required_principal, "user");
    assert_eq!(action.target_type, "milestone_revision");
    assert_eq!(action.target_id, "revision-2");
    assert_eq!(
        action.route_or_operation,
        "project.milestone.revision.transition"
    );
    assert_eq!(action.expected_version, Some(5));
    assert!(action.blocking);

    let mut approved = Fixture::base();
    let mut milestone = default_milestone("milestone-1", 5);
    milestone["definition"]["id"] = json!("revision-2");
    milestone["definition"]["lifecycle"] = json!("approved");
    approved.milestones = vec![milestone];
    approved.checks = json!({"missing": 1});
    assert_eq!(approved.action().code, "validation_required");
}

#[test]
fn legacy_baseline_states_do_not_create_project_next_actions() {
    for gate in [
        api_types::ExecutionGate::BaselineApprovalRequired,
        api_types::ExecutionGate::PreBaselineReadOnly,
    ] {
        let mut fixture = Fixture::base();
        fixture.execution_setup = base_execution_setup(
            api_types::CoordinationState::Ready,
            api_types::ExecutionSetupState::Ready,
            gate,
        );
        assert!(next_action(NextActionContext {
            project_id: "project-1",
            project_version: 7,
            charter_setup_required: fixture.charter_setup_required,
            no_milestones: fixture.no_milestones,
            execution_setup: &fixture.execution_setup,
            milestones: &fixture.milestones,
            documents: &fixture.documents,
            releases: &fixture.releases,
            pending_decision_ids: &fixture.pending_decision_ids,
            task_counts: &fixture.task_counts,
            failed_task_count: fixture.failed_task_count,
            checks: &fixture.checks,
            reconciliation_required: fixture.reconciliation_required,
            stale: fixture.stale,
        })
        .is_none());
    }
}
