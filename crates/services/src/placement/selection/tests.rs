//! Placement selection, readiness, capacity, and context loading tests.

#[test]
fn partial_machine_facts_do_not_skip_full_checks_for_another_role() {
    let mut context = context();
    context.environment_digest = Some("current".into());
    context.environment_checks = vec![serde_json::from_value(serde_json::json!({"name":"cargo","command":"false","scope":"machine","roles":["reviewer"]})).unwrap()];
    context.candidates[0].machine_probe = true;
    context.candidates[0]
        .allowed_run_purposes
        .push(WorkspaceRunPurpose::EnvironmentProbe);
    let mut row = readiness(&context, 0, EnvironmentReadinessStatus::NotReady);
    row.scope_covered = "machine".into();
    context.candidates[0].environment_readiness = Some(row);
    rejected(&context, PlacementFilterCode::EnvironmentProbePending);
    context.candidates[0]
        .environment_readiness
        .as_mut()
        .unwrap()
        .scope_covered = "full".into();
    selected(&context);
}

#[test]
fn probe_capable_daemon_defers_but_legacy_daemon_uses_preflight() {
    let mut context = context();
    context.environment_digest = Some("current".into());
    context.candidates[0].machine_probe = true;
    context.candidates[0]
        .allowed_run_purposes
        .push(api_types::WorkspaceRunPurpose::EnvironmentProbe);
    rejected(&context, PlacementFilterCode::EnvironmentProbePending);
    context.candidates[0].machine_probe = false;
    selected(&context);
    context.candidates[0].machine_probe = true;
    context.environment_admission = EnvironmentAdmission::LaunchPreflight;
    selected(&context);
    context.environment_admission = EnvironmentAdmission::Dispatcher;
    context.environment_has_assets = true;
    selected(&context);
}
#[test]
fn provisioning_requires_machine_checks_and_never_follows_capacity_only_refusal() {
    let mut context = context();
    let mut proposed = context.candidates[0].clone();
    proposed.location.id = "proposed".into();
    proposed.provisioning = true;
    proposed.machine_probe = true;
    proposed.repo_provision = true;
    proposed.allowed_run_purposes.extend([
        api_types::WorkspaceRunPurpose::EnvironmentProbe,
        api_types::WorkspaceRunPurpose::RepoProvision,
    ]);
    context.candidates[0]
        .machine_capacity
        .as_mut()
        .unwrap()
        .max_concurrent_runs = Some(1);
    context.candidates[0]
        .machine_capacity
        .as_mut()
        .unwrap()
        .running_executions = 1;
    context.candidates.push(proposed);
    assert!(ready_location_can_run(&context));
    let refusal = rejected(&context, PlacementFilterCode::MachineCapacity);
    assert_eq!(refusal.rejected_candidates.len(), 1);
    context.candidates[0].executors.clear();
    assert!(!ready_location_can_run(&context));
    rejected(&context, PlacementFilterCode::EnvironmentUnverified);
    context.environment_checks = vec![serde_json::from_value(
        serde_json::json!({"name":"cargo","command":"true","scope":"machine"}),
    )
    .unwrap()];
    context.environment_digest = Some("current".into());
    rejected(&context, PlacementFilterCode::EnvironmentProbePending);
}

#[test]
fn exhausted_provisioning_machine_is_unreachable_and_cannot_pause_project() {
    let mut context = context();
    context.environment_checks = vec![serde_json::from_value(
        serde_json::json!({"name":"cargo","command":"true","scope":"machine"}),
    )
    .unwrap()];
    context.environment_digest = Some("current".into());
    context.candidates[0].provisioning = true;
    context.candidates[0].provision_exhausted = true;
    context.candidates[0].machine_probe = true;
    context.candidates[0].repo_provision = true;
    context.candidates[0].allowed_run_purposes.extend([
        WorkspaceRunPurpose::EnvironmentProbe,
        WorkspaceRunPurpose::RepoProvision,
    ]);
    let refusal = rejected(&context, PlacementFilterCode::ProvisionFailed);
    assert!(refusal.is_deterministic());
    assert!(!crate::placement::is_retryable_admission_refusal(
        &crate::ServiceError::PlacementUnavailable(refusal)
    ));
    assert!(environment_pause_candidates(&context).is_empty());
    context.candidates.push(candidate(&context, "server", None));
    context.candidates[1].environment_readiness =
        Some(readiness(&context, 1, EnvironmentReadinessStatus::NotReady));
    let paused = environment_pause_candidates(&context);
    assert_eq!(paused.len(), 1);
    assert_eq!(paused[0].location.id, "server");
}

#[test]
fn unverified_requires_action_even_when_connection_facts_are_missing() {
    let mut context = context();
    context.candidates[0].provisioning = true;
    context.candidates[0].machine_probe = true;
    context.candidates[0].repo_provision = true;
    context.candidates[0].allowed_run_purposes.extend([
        WorkspaceRunPurpose::EnvironmentProbe,
        WorkspaceRunPurpose::RepoProvision,
    ]);
    context.candidates[0].connected = false;
    context.candidates[0].negotiated_revision = None;
    context.candidates[0].workspace_v1 = false;
    context.candidates[0].allowed_run_purposes.clear();
    let refusal = rejected(&context, PlacementFilterCode::EnvironmentUnverified);
    assert!(refusal.is_deterministic());
    assert!(!crate::placement::is_retryable_admission_refusal(
        &crate::ServiceError::PlacementUnavailable(refusal)
    ));
}

#[test]
fn unfinished_daemon_location_waits_for_verification_instead_of_an_impossible_probe() {
    let mut context = context();
    context.environment_digest = Some("current".into());
    context.candidates[0].location.status = RepoLocationStatus::Unverified;
    context.candidates[0].machine_probe = true;
    context.candidates[0]
        .allowed_run_purposes
        .push(WorkspaceRunPurpose::EnvironmentProbe);
    let mut row = readiness(&context, 0, EnvironmentReadinessStatus::Ready);
    row.scope_covered = "machine".into();
    context.candidates[0].environment_readiness = Some(row);
    let refusal = rejected(&context, PlacementFilterCode::LocationNotReady);
    assert_eq!(
        refusal.rejected_candidates[0].filter_codes,
        [PlacementFilterCode::LocationNotReady]
    );
    assert!(refusal.is_deterministic());
}

use super::*;
use db::AgentStatus;

const NOW: &str = "2026-09-29T00:00:00Z";

fn capabilities() -> ExecutorAdapterCapabilityFacts {
    ExecutorAdapterCapabilityFacts {
        structured_events: true,
        usage: true,
        resume: true,
        cancel_ack: true,
        terminal_observed: true,
    }
}

fn executor() -> ExecutorFacts {
    ExecutorFacts {
        installed: true,
        authenticated: true,
        enabled: true,
        capabilities: capabilities(),
    }
}

fn agent(id: &str, role: &str) -> WorktreeAgent {
    WorktreeAgent {
        role: role.to_owned(),
        required_capabilities: capabilities(),
        agent: Agent {
            id: id.to_owned(),
            name: id.to_owned(),
            description: None,
            profile_id: format!("profile-{id}"),
            backend_kind: "cli".to_owned(),
            executor_type: "codex".to_owned(),
            provider: None,
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            tool_policy_json: "{}".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: Some("owner".to_owned()),
            visibility: "private".to_owned(),
            version: 1,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    }
}

fn context() -> SelectionContext {
    let mut context = SelectionContext {
        environment_admission: EnvironmentAdmission::Dispatcher,
        environment_has_assets: false,
        environment_digest: None,
        environment_checks: Vec::new(),
        task: Task {
            condition: Default::default(),
            id: "task".to_owned(),
            project_id: "project".to_owned(),
            parent_task_id: None,
            assignee_type: None,
            assignee_id: None,
            title: "Task".to_owned(),
            description: None,
            task_type: "implementation".to_owned(),
            status: "todo".to_owned(),
            is_automation: false,
            priority: 0,
            board_position: 0.0,
            subtask_order: None,
            task_state_config: None,
            merge_config: None,
            metadata_json: None,
            plan: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            entry_barrier_json: None,
            review_passed_at: None,
            archived_at: None,
            deleted_at: None,
            version: 1,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
        repo: Repo {
            id: "repo".to_owned(),
            project_id: "project".to_owned(),
            name: "Repo".to_owned(),
            remote_url: Some("https://example.test/repo.git".to_owned()),
            local_path: None,
            default_branch: "main".to_owned(),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
        claiming_agent: agent("coder", "coder"),
        worktree_agents: vec![agent("reviewer", "reviewer"), agent("planner", "planner")],
        candidates: Vec::new(),
        existing_placement: None,
        inherited_root_placement: None,
        needed_run_purposes: vec![WorkspaceRunPurpose::CiStep],
        agent_capacity: AgentCapacity::default(),
    };
    context
        .candidates
        .push(candidate(&context, "mac-location", Some("mac")));
    context
}

fn candidate(context: &SelectionContext, id: &str, daemon: Option<&str>) -> PlacementCandidate {
    PlacementCandidate {
        machine_probe: false,
        repo_provision: false,
        provisioning: false,
        provision_exhausted: false,
        environment_readiness: None,
        location: RepoLocation {
            id: id.to_owned(),
            repo_id: context.repo.id.clone(),
            owner_kind: if daemon.is_some() {
                RepoLocationOwnerKind::Daemon
            } else {
                RepoLocationOwnerKind::Server
            },
            daemon_id: daemon.map(str::to_owned),
            runtime_id: daemon.map(|id| format!("runtime-{id}")),
            path: format!("/checkout/{id}"),
            kind: RepoLocationKind::PrimaryCheckout,
            is_default: false,
            status: RepoLocationStatus::Ready,
            last_verified_at: Some(NOW.to_owned()),
            last_error: None,
            version: 1,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
        execution_daemon_id: daemon.map(str::to_owned),
        embedded_execution: daemon.is_none(),
        connected: true,
        negotiated_revision: Some(api_types::DAEMON_PROTOCOL_REVISION),
        workspace_v1: true,
        plan_transport: true,
        runtime_ready: true,
        visible: true,
        executors: context
            .agents()
            .map(|role| (role.agent.id.clone(), executor()))
            .collect(),
        allowed_run_purposes: vec![WorkspaceRunPurpose::CiStep],
        machine_capacity: Some(MachineCapacity {
            max_concurrent_runs: Some(2),
            ..MachineCapacity::default()
        }),
        disk_pressure: None,
    }
}

#[test]
fn machine_capacity_full_server_selects_free_daemon_and_zero_is_unlimited() {
    let mut context = context();
    let mut server = candidate(&context, "server", None);
    server.machine_capacity.as_mut().unwrap().running_executions = 2;
    context.candidates.insert(0, server);
    let result = selected(&context);
    assert_eq!(result.candidate.execution_daemon_id.as_deref(), Some("mac"));
    assert_eq!(
        result.selection_reason.rejected_candidates[0].filter_codes,
        vec![PlacementFilterCode::MachineCapacity]
    );
    context.candidates.remove(1);
    context.candidates[0]
        .machine_capacity
        .as_mut()
        .unwrap()
        .max_concurrent_runs = Some(0);
    assert!(matches!(
        select_placement(&context),
        SelectionOutcome::Selected(_)
    ));
}

#[test]
fn machine_capacity_selection_enforces_effective_caps() {
    for (reported, admin, expected) in [
        (Some(6), Some(2), 2),
        (Some(3), Some(8), 3),
        (Some(3), None, 3),
    ] {
        let mut context = context();
        let cap = super::super::capacity::effective_machine_cap(reported, admin);
        let capacity = context.candidates[0].machine_capacity.as_mut().unwrap();
        capacity.max_concurrent_runs = cap;
        capacity.running_executions = expected;
        rejected(&context, PlacementFilterCode::MachineCapacity);
        context.candidates[0]
            .machine_capacity
            .as_mut()
            .unwrap()
            .running_executions -= 1;
        selected(&context);
    }
}

#[test]
fn disk_pressure_refuses_a_new_worktree_and_another_machine_takes_the_task() {
    use api_types::DiskPressureKind::{Bytes, Inodes};
    let mut context = context();
    let mut server = candidate(&context, "server", None);
    server.disk_pressure = Some(Bytes);
    context.candidates.insert(0, server);
    // One machine under its floor: the other one is chosen.
    let result = selected(&context);
    assert_eq!(result.candidate.execution_daemon_id.as_deref(), Some("mac"));
    assert_eq!(
        result.selection_reason.rejected_candidates[0].filter_codes,
        vec![PlacementFilterCode::DiskPressure]
    );
    // Every machine under its floor (bytes on one, inodes on the other):
    // a wait for disk that the next scan decides again, never a failure.
    context.candidates[1].disk_pressure = Some(Inodes);
    let refusal = rejected(&context, PlacementFilterCode::DiskPressure);
    assert!(refusal
        .rejected_candidates
        .iter()
        .all(|candidate| candidate.filter_codes == [PlacementFilterCode::DiskPressure]));
    assert!(!refusal.is_deterministic());
    assert!(super::super::machine_precheck::capacity_only_wait(&refusal));
    assert_eq!(
        super::super::CapacityWait::of(&refusal),
        super::super::CapacityWait::Disk
    );
    let error = crate::ServiceError::PlacementUnavailable(refusal);
    assert!(super::super::is_retryable_admission_refusal(&error));
    assert!(super::super::is_machine_capacity_refusal(&error));
    assert!(
        environment_pause_candidates(&context).is_empty(),
        "no Project pause for disk"
    );
    // A machine with room on its disk but no run slot is the nearer
    // exit: the Task says it waits for a slot.
    context.candidates[1].disk_pressure = None;
    let capacity = context.candidates[1].machine_capacity.as_mut().unwrap();
    capacity.max_concurrent_runs = Some(1);
    capacity.running_executions = 1;
    let refusal = rejected(&context, PlacementFilterCode::MachineCapacity);
    assert_eq!(
        super::super::CapacityWait::of(&refusal),
        super::super::CapacityWait::Machine
    );
    // The reading recovers: nothing else has to happen.
    context.candidates[0].disk_pressure = None;
    assert_eq!(selected(&context).candidate.location.id, "server");
}

#[test]
fn disk_pressure_never_refuses_a_run_in_an_existing_ready_worktree() {
    let mut context = context();
    context.candidates[0].disk_pressure = Some(api_types::DiskPressureKind::Bytes);
    rejected(&context, PlacementFilterCode::DiskPressure);
    // The Task's own ready worktree: finishing work is how space comes
    // back, so it runs.
    context.existing_placement = Some(placement(&context.candidates[0]));
    assert_eq!(
        context.existing_placement.as_ref().unwrap().state,
        PlacementState::Ready
    );
    selected(&context);
    // A subtask in its root's ready worktree runs too.
    context.inherited_root_placement = context.existing_placement.take();
    selected(&context);
    // A reservation is not a worktree yet: creating one is new work.
    let mut reserved = context.inherited_root_placement.take().unwrap();
    reserved.state = PlacementState::Reserved;
    context.existing_placement = Some(reserved);
    rejected(&context, PlacementFilterCode::DiskPressure);
}

#[test]
fn machine_capacity_start_rechecks_ready_placement() {
    let mut context = context();
    let capacity = context.candidates[0].machine_capacity.as_mut().unwrap();
    capacity.max_concurrent_runs = Some(1);
    capacity.active_chat_turns = 1;
    context.existing_placement = Some(placement(&context.candidates[0]));
    rejected(&context, PlacementFilterCode::MachineCapacity);
    context.candidates[0]
        .machine_capacity
        .as_mut()
        .unwrap()
        .active_chat_turns = 0;
    selected(&context);
}

fn placement(candidate: &PlacementCandidate) -> WorkspacePlacement {
    WorkspacePlacement {
        id: "placement".to_owned(),
        workspace_id: "workspace".to_owned(),
        task_id: "task".to_owned(),
        agent_id: Some("coder".to_owned()),
        owner_kind: if candidate.location.owner_kind == RepoLocationOwnerKind::Daemon {
            PlacementOwnerKind::Daemon
        } else {
            PlacementOwnerKind::Server
        },
        daemon_id: candidate.location.daemon_id.clone(),
        runtime_id: candidate.location.runtime_id.clone(),
        repo_location_id: candidate.location.id.clone(),
        execution_daemon_id: candidate.execution_daemon_id.clone(),
        workspace_handle: Some("opaque-workspace-handle".to_owned()),
        generation: 4,
        state: PlacementState::Ready,
        selected_by: PlacementSelectedBy::Scheduler,
        selection_reason: "{}".to_owned(),
        reserved_until: None,
        disconnected_at: None,
        failure_cause: None,
        version: 8,
        created_at: NOW.to_owned(),
        updated_at: NOW.to_owned(),
    }
}

fn selected(context: &SelectionContext) -> PlacementSelection {
    select_placement(context).into_result().unwrap()
}

fn readiness(
    context: &SelectionContext,
    index: usize,
    status: EnvironmentReadinessStatus,
) -> ProjectMachineReadiness {
    let mut row = super::super::environment::unknown_record(
        &context.task.project_id,
        EnvironmentMachine::from_location(&context.candidates[index].location),
        &api_types::ProjectEnvironment::default(),
    );
    row.checks_digest = context.environment_digest.clone().unwrap_or_default();
    row.status = status;
    row.failing_checks = vec![db::ReadinessCheckFailure {
        name: "cargo".into(),
        output_tail: "command not found".into(),
    }];
    row
}

#[test]
fn readiness_ready_machine_passes() {
    let mut context = context();
    context.environment_digest = Some("current".into());
    context.candidates[0].environment_readiness =
        Some(readiness(&context, 0, EnvironmentReadinessStatus::Ready));
    assert_eq!(selected(&context).candidate.location.id, "mac-location");
}

#[test]
fn readiness_unfit_machine_rejected_with_checks_and_server_wins() {
    let mut context = context();
    context.environment_digest = Some("current".into());
    context.candidates[0].environment_readiness =
        Some(readiness(&context, 0, EnvironmentReadinessStatus::NotReady));
    context
        .candidates
        .push(candidate(&context, "server-location", None));
    context.candidates[1].environment_readiness =
        Some(readiness(&context, 1, EnvironmentReadinessStatus::Ready));
    let selection = selected(&context);
    assert_eq!(selection.candidate.location.id, "server-location");
    let rejection = &selection.selection_reason.rejected_candidates[0];
    assert_eq!(
        rejection.filter_codes,
        vec![PlacementFilterCode::EnvironmentNotReady]
    );
    assert_eq!(rejection.failing_checks, vec!["cargo"]);
    assert_eq!(
        serde_json::to_value(rejection).unwrap()["filter_codes"][0],
        "environment_not_ready"
    );
}

#[test]
fn machine_capacity_and_environment_failure_do_not_pause_project() {
    let mut context = context();
    context.environment_digest = Some("current".into());
    context.candidates[0].environment_readiness =
        Some(readiness(&context, 0, EnvironmentReadinessStatus::NotReady));
    context.candidates[0]
        .machine_capacity
        .as_mut()
        .unwrap()
        .running_executions = 2;
    let refusal = select_placement(&context).into_result().unwrap_err();
    assert_eq!(
        refusal.rejected_candidates[0].filter_codes,
        vec![
            PlacementFilterCode::MachineCapacity,
            PlacementFilterCode::EnvironmentNotReady
        ]
    );
    assert!(environment_pause_candidates(&context).is_empty());
    assert!(!super::super::machine_precheck::capacity_only_wait(
        &refusal
    ));
    context.candidates[0]
        .machine_capacity
        .as_mut()
        .unwrap()
        .running_executions = 0;
    assert_eq!(environment_pause_candidates(&context).len(), 1);
}

#[test]
fn machine_capacity_wait_ignores_environment_failed_alternative() {
    let mut context = context();
    context.environment_digest = Some("current".into());
    context.candidates[0].environment_readiness =
        Some(readiness(&context, 0, EnvironmentReadinessStatus::NotReady));
    context
        .candidates
        .push(candidate(&context, "server-location", None));
    context.candidates[1].environment_readiness =
        Some(readiness(&context, 1, EnvironmentReadinessStatus::Ready));
    context.candidates[1]
        .machine_capacity
        .as_mut()
        .unwrap()
        .running_executions = 2;
    let refusal = select_placement(&context).into_result().unwrap_err();
    assert!(environment_pause_candidates(&context).is_empty());
    assert!(super::super::machine_precheck::capacity_only_wait(&refusal));
    context.candidates[0].environment_readiness =
        Some(readiness(&context, 0, EnvironmentReadinessStatus::Unknown));
    // Remote unknown readiness is resolved at launch, so it remains a
    // possible free candidate and cannot establish a machine wait.
    assert!(matches!(
        select_placement(&context),
        SelectionOutcome::Selected(_)
    ));
}

#[test]
fn readiness_missing_unknown_and_stale_records_defer() {
    let mut context = context();
    context.candidates = vec![candidate(&context, "server-location", None)];
    context.environment_digest = Some("current".into());
    for row in [
        None,
        Some(readiness(&context, 0, EnvironmentReadinessStatus::Unknown)),
        Some({
            let mut row = readiness(&context, 0, EnvironmentReadinessStatus::Ready);
            row.checks_digest = "old".into();
            row
        }),
    ] {
        context.candidates[0].environment_readiness = row;
        let refusal = select_placement(&context).into_result().unwrap_err();
        assert_eq!(
            refusal.rejected_candidates[0].filter_codes,
            vec![PlacementFilterCode::EnvironmentProbePending]
        );
    }
}

#[test]
fn readiness_no_checks_preserves_selection() {
    let mut context = context();
    context.candidates[0].environment_readiness =
        Some(readiness(&context, 0, EnvironmentReadinessStatus::NotReady));
    assert_eq!(selected(&context).candidate.location.id, "mac-location");
}

#[test]
fn readiness_pinned_unfit_machine_has_no_fallback() {
    let mut context = context();
    context.environment_digest = Some("current".into());
    context.claiming_agent.agent.daemon_id = Some("mac".into());
    context.candidates[0].environment_readiness =
        Some(readiness(&context, 0, EnvironmentReadinessStatus::NotReady));
    context
        .candidates
        .push(candidate(&context, "server-location", None));
    context.candidates[1].environment_readiness =
        Some(readiness(&context, 1, EnvironmentReadinessStatus::Ready));
    let refusal = select_placement(&context).into_result().unwrap_err();
    assert!(refusal
        .rejected_candidates
        .iter()
        .any(|rejection| rejection.daemon_id.as_deref() == Some("mac")
            && rejection.filter_codes == vec![PlacementFilterCode::EnvironmentNotReady]));
}

#[test]
fn readiness_daemon_missing_unknown_and_stale_pass_at_reserve_and_claim() {
    let mut context = context();
    context.environment_digest = Some("current".into());
    for row in [
        None,
        Some(readiness(&context, 0, EnvironmentReadinessStatus::Unknown)),
        Some({
            let mut row = readiness(&context, 0, EnvironmentReadinessStatus::NotReady);
            row.checks_digest = "old".into();
            row
        }),
    ] {
        context.candidates[0].environment_readiness = row;
        context.existing_placement = None;
        let reserved = selected(&context);
        assert_eq!(reserved.candidate.location.id, "mac-location");
        context.existing_placement = Some(placement(&reserved.candidate));
        assert_eq!(
            selected(&context).selection_reason.rule,
            SelectionRule::ExistingPlacement
        );
    }
    context.candidates[0].environment_readiness =
        Some(readiness(&context, 0, EnvironmentReadinessStatus::NotReady));
    for placed in [false, true] {
        context.existing_placement = placed.then(|| placement(&context.candidates[0]));
        assert_eq!(
            select_placement(&context)
                .into_result()
                .unwrap_err()
                .rejected_candidates[0]
                .filter_codes,
            vec![PlacementFilterCode::EnvironmentNotReady]
        );
    }
}

#[test]
fn readiness_pending_preserves_preference_order() {
    let mut context = context();
    context.environment_digest = Some("current".into());
    context
        .candidates
        .push(candidate(&context, "server-location", None));
    assert!(
        select_placement(&context).into_result().is_err(),
        "preferred server pending cannot divert to daemon"
    );
    context.candidates[0].location.is_default = true;
    assert_eq!(
        selected(&context).candidate.location.id,
        "mac-location",
        "a lower-preference pending host does not block the default daemon"
    );
    context.candidates[1].location.is_default = true;
    context.candidates[0].location.is_default = false;
    assert!(
        select_placement(&context).into_result().is_err(),
        "default server pending preserves preference"
    );
    context.candidates[0].location.is_default = true;
    context.candidates[1].location.is_default = false;
    let mut released = placement(&context.candidates[1]);
    released.state = PlacementState::Failed;
    released.workspace_handle = None;
    context.existing_placement = Some(released);
    assert_eq!(
        selected(&context).candidate.location.id,
        "mac-location",
        "a released failed reservation cannot promote a pending server over the default daemon"
    );
}

#[test]
fn readiness_failures_and_last_resort_pause_apply_to_task_roles() {
    let mut context = context();
    context.environment_digest = Some("current".into());
    context.environment_checks = serde_json::from_value(
        serde_json::json!([{"name":"cargo","command":"false","roles":["reviewer"]}]),
    )
    .unwrap();
    context.candidates[0].environment_readiness =
        Some(readiness(&context, 0, EnvironmentReadinessStatus::NotReady));
    assert_eq!(context.claiming_agent.role, "coder");
    assert!(select_placement(&context).into_result().is_ok());
    assert!(environment_pause_candidates(&context).is_empty());
    context.claiming_agent.role = "reviewer".into();
    assert_eq!(
        select_placement(&context)
            .into_result()
            .unwrap_err()
            .rejected_candidates[0]
            .failing_checks,
        vec!["cargo"]
    );
    assert_eq!(environment_pause_candidates(&context).len(), 1);
}

#[test]
fn readiness_pinned_wait_does_not_pause_an_owner_available_to_other_agents() {
    let mut context = context();
    context.environment_digest = Some("current".into());
    context.claiming_agent.agent.daemon_id = Some("mac".into());
    context.candidates[0].environment_readiness =
        Some(readiness(&context, 0, EnvironmentReadinessStatus::NotReady));
    context.candidates.push(candidate(&context, "server", None));
    context.candidates[1].environment_readiness =
        Some(readiness(&context, 1, EnvironmentReadinessStatus::Ready));
    for executor in context.candidates[1].executors.values_mut() {
        executor.installed = false;
    }
    assert!(
        select_placement(&context).into_result().is_err(),
        "the pinned Task still cannot fall back"
    );
    assert!(
        environment_pause_candidates(&context).is_empty(),
        "other Agents can run on the healthy server"
    );
}

#[test]
fn readiness_asset_projects_use_launch_facts_and_direct_claims_skip_pending() {
    let mut context = context();
    context.environment_digest = Some("current".into());
    context.candidates = vec![candidate(&context, "server", None)];
    context.environment_admission = EnvironmentAdmission::LaunchPreflight;
    assert!(select_placement(&context).into_result().is_ok());
    context.environment_admission = EnvironmentAdmission::Dispatcher;
    assert!(select_placement(&context).into_result().is_err());
    context.environment_has_assets = true;
    assert!(select_placement(&context).into_result().is_ok());
    context.candidates[0].environment_readiness =
        Some(readiness(&context, 0, EnvironmentReadinessStatus::NotReady));
    assert!(
        select_placement(&context).into_result().is_ok(),
        "a primary checkout cannot validate staged assets"
    );
    context.candidates[0]
        .environment_readiness
        .as_mut()
        .unwrap()
        .role = Some("coder".into());
    assert!(
        select_placement(&context).into_result().is_err(),
        "actual launch failures remain placement facts"
    );
}

#[test]
fn readiness_offline_alternative_never_changes_pause_with_cached_handshake() {
    let mut context = context();
    context.environment_digest = Some("current".into());
    context.candidates = vec![
        candidate(&context, "server", None),
        candidate(&context, "offline", Some("offline")),
    ];
    context.candidates[0].environment_readiness =
        Some(readiness(&context, 0, EnvironmentReadinessStatus::NotReady));
    context.candidates[1].connected = false;
    assert_eq!(environment_pause_candidates(&context).len(), 1);
    context.candidates[1].negotiated_revision = None;
    context.candidates[1].workspace_v1 = false;
    context.candidates[1].executors.clear();
    assert_eq!(environment_pause_candidates(&context).len(), 1);
}

fn rejected(context: &SelectionContext, code: PlacementFilterCode) -> PlacementUnavailable {
    let error = select_placement(context).into_result().unwrap_err();
    assert!(
        error
            .rejected_candidates
            .iter()
            .any(|candidate| candidate.filter_codes.contains(&code)),
        "{error:?}"
    );
    error
}

#[test]
fn placement_upgrade_refusal_is_transient_with_an_offline_owner_missing_handshake() {
    let mut context = context();
    let upgrade = &mut context.candidates[0];
    upgrade.negotiated_revision = Some(2);
    upgrade.workspace_v1 = false;
    upgrade.allowed_run_purposes.clear();
    for facts in upgrade.executors.values_mut() {
        facts.capabilities = ExecutorAdapterCapabilityFacts::default();
    }
    let mut offline = candidate(&context, "offline-location", Some("offline"));
    offline.connected = false;
    offline.negotiated_revision = None;
    offline.workspace_v1 = false;
    offline.allowed_run_purposes.clear();
    for facts in offline.executors.values_mut() {
        facts.capabilities = ExecutorAdapterCapabilityFacts::default();
    }
    let codes = filter_candidate(&context, &offline);
    assert_eq!(
        codes,
        vec![
            PlacementFilterCode::OwnerUnreachable,
            PlacementFilterCode::WorkspaceProtocolMissing,
            PlacementFilterCode::ExecutorUnavailable,
        ]
    );
    context.candidates.push(offline);
    let refusal = select_placement(&context).into_result().unwrap_err();
    assert!(!refusal.needs_daemon_upgrade());
}

#[test]
fn only_daemon_location_selects_authenticated_cli_roles() {
    let result = selected(&context());
    assert_eq!(result.candidate.execution_daemon_id.as_deref(), Some("mac"));
    assert_eq!(
        result.selection_reason.rule,
        SelectionRule::OnlyEligibleLocation
    );
    assert_eq!(
        serde_json::to_value(result.selection_reason).unwrap(),
        serde_json::json!({"rule": "only_eligible_location", "rejected_candidates": []})
    );
}

#[test]
fn no_compatible_owner_reports_executor_unavailable_without_fallback() {
    let mut context = context();
    context.candidates[0]
        .executors
        .get_mut("coder")
        .unwrap()
        .authenticated = false;
    let error = rejected(&context, PlacementFilterCode::ExecutorUnavailable);
    assert_eq!(error.rejected_candidates.len(), 1);
    assert_eq!(
        error.rejected_candidates[0].daemon_id.as_deref(),
        Some("mac")
    );
    assert_eq!(
        error.rejected_candidates[0].repo_location_id,
        "mac-location"
    );
}

#[test]
fn pinned_agent_selects_its_daemon_and_records_server_rejection() {
    let mut context = context();
    context.claiming_agent.agent.daemon_id = Some("mac".to_owned());
    let mut server = candidate(&context, "server", None);
    server.location.is_default = true;
    context.candidates.push(server);
    let result = selected(&context);
    assert_eq!(result.candidate.execution_daemon_id.as_deref(), Some("mac"));
    assert_eq!(result.selection_reason.rule, SelectionRule::AgentPin);
    assert_eq!(result.selected_by, PlacementSelectedBy::Pin);
    assert_eq!(
        result.selection_reason.rejected_candidates[0].filter_codes,
        vec![PlacementFilterCode::PinMismatch]
    );
}

#[test]
fn pinned_daemon_uses_default_to_break_location_ties() {
    let mut context = context();
    context.claiming_agent.agent.daemon_id = Some("mac".to_owned());
    let mut default = candidate(&context, "z-default", Some("mac"));
    default.location.is_default = true;
    context.candidates.push(default);
    assert_eq!(selected(&context).candidate.location.id, "z-default");
}

#[test]
fn incompatible_pins_of_future_roles_reject_every_owner() {
    let mut context = context();
    context.claiming_agent.agent.daemon_id = Some("mac".to_owned());
    context.worktree_agents[0].agent.daemon_id = Some("linux".to_owned());
    context
        .candidates
        .push(candidate(&context, "linux", Some("linux")));
    let error = rejected(&context, PlacementFilterCode::PinMismatch);
    assert_eq!(error.rejected_candidates.len(), 2);
    assert!(error.rejected_candidates.iter().all(|candidate| candidate
        .filter_codes
        .contains(&PlacementFilterCode::PinMismatch)));
}

#[test]
fn native_reviewer_and_planner_are_rejected_on_daemon_placements() {
    for index in 0..2 {
        let mut context = context();
        context.worktree_agents[index].agent.backend_kind = "native".to_owned();
        rejected(&context, PlacementFilterCode::NativeBackendUnsupported);
    }
    let mut context = context();
    context.claiming_agent.agent.backend_kind = "native".to_owned();
    rejected(&context, PlacementFilterCode::NativeBackendUnsupported);
}

#[test]
fn preparing_reservation_refuses_a_second_agent_claim() {
    let mut context = context();
    context.agent_capacity.reservations = 1;
    rejected(&context, PlacementFilterCode::AgentCapacity);
}

#[test]
fn two_unpinned_running_placements_fill_daemon_session_cap() {
    let mut context = context();
    context.candidates[0]
        .machine_capacity
        .as_mut()
        .unwrap()
        .running_executions = 2;
    assert!(context.agents().all(|role| role.agent.daemon_id.is_none()));
    rejected(&context, PlacementFilterCode::MachineCapacity);
}

#[test]
fn daemon_reservations_and_chat_turns_share_the_session_cap() {
    let mut context = context();
    let capacity = context.candidates[0].machine_capacity.as_mut().unwrap();
    capacity.reservations = 1;
    capacity.active_chat_turns = 1;
    rejected(&context, PlacementFilterCode::MachineCapacity);
}

#[test]
fn all_run_purposes_must_be_allowed() {
    for purpose in [
        WorkspaceRunPurpose::EnvironmentSetup,
        WorkspaceRunPurpose::Hook,
        WorkspaceRunPurpose::CiStep,
    ] {
        let mut context = context();
        context.needed_run_purposes = vec![purpose];
        context.candidates[0]
            .allowed_run_purposes
            .retain(|allowed| *allowed != purpose);
        rejected(&context, PlacementFilterCode::RunPurposeDenied);
        context.candidates[0].allowed_run_purposes.push(purpose);
        selected(&context);
    }
}

#[test]
fn required_run_purposes_follow_review_hooks_and_matching_environment_checks() {
    let settings: ProjectSettings = serde_json::from_value(serde_json::json!({
            "lifecycle_hooks": {"before_work": [{"type": "script", "command": "make setup"}]},
            "environment": {"checks": [{"name": "probe", "command": "command -v rustc", "roles": ["coder"]}]}
        })).unwrap();
    let mut review = ReviewConfig::default();
    review.ci_steps.push("make test".to_owned());
    assert_eq!(
        needed_run_purposes(&review, &settings, &["coder", "reviewer"]),
        vec![
            WorkspaceRunPurpose::EnvironmentSetup,
            WorkspaceRunPurpose::Hook,
            WorkspaceRunPurpose::CiStep
        ]
    );
    assert_eq!(
        needed_run_purposes(&review, &settings, &["planner"]),
        vec![WorkspaceRunPurpose::Hook, WorkspaceRunPurpose::CiStep]
    );
    let settings: ProjectSettings = serde_json::from_value(serde_json::json!({
            "environment": {"env": {"MODE": "test"}, "assets": [{"source": "/model", "target": "model"}]}
        })).unwrap();
    assert!(needed_run_purposes(&ReviewConfig::default(), &settings, &["coder"]).is_empty());
}

#[test]
fn every_required_adapter_fact_is_checked_for_future_roles() {
    for index in 0..5 {
        let mut context = context();
        let facts = &mut context.candidates[0]
            .executors
            .get_mut("reviewer")
            .unwrap()
            .capabilities;
        match index {
            0 => facts.structured_events = false,
            1 => facts.usage = false,
            2 => facts.resume = false,
            3 => facts.cancel_ack = false,
            _ => facts.terminal_observed = false,
        }
        rejected(&context, PlacementFilterCode::CapabilityMissing);
    }
    let mut context = context();
    context.candidates[0]
        .executors
        .get_mut("coder")
        .unwrap()
        .capabilities = ExecutorAdapterCapabilityFacts::default();
    rejected(&context, PlacementFilterCode::CapabilityMissing);
}

#[test]
fn non_plan_roles_do_not_require_plan_transport() {
    for role in ["reviewer", "auditor", "interactive"] {
        let mut context = context();
        context.candidates[0].plan_transport = false;
        context.worktree_agents.push(context.claiming_agent.clone());
        context.claiming_agent.role = role.into();
        assert!(
            select_placement(&context).into_result().is_ok(),
            "{role} remains supported with a coder assigned"
        );
    }
    let mut context = context();
    context.candidates[0].plan_transport = false;
    rejected(&context, PlacementFilterCode::CapabilityMissing);
}

#[test]
fn offline_owner_missing_handshake_is_a_retryable_placement_refusal() {
    let mut context = context();
    let owner = &mut context.candidates[0];
    owner.connected = false;
    owner.negotiated_revision = None;
    owner.workspace_v1 = false;
    owner.allowed_run_purposes.clear();
    for facts in owner.executors.values_mut() {
        facts.capabilities = ExecutorAdapterCapabilityFacts::default();
    }
    let refusal = select_placement(&context).into_result().unwrap_err();
    assert!(super::super::is_retryable_admission_refusal(
        &crate::ServiceError::PlacementUnavailable(refusal)
    ));

    // Once the handshake arrives, an actual capability/policy refusal
    // requires intervention and must not stay queued indefinitely.
    let owner = &mut context.candidates[0];
    owner.connected = true;
    owner.negotiated_revision = Some(api_types::DAEMON_PROTOCOL_REVISION);
    owner.workspace_v1 = true;
    let refusal = select_placement(&context).into_result().unwrap_err();
    assert!(!super::super::is_retryable_admission_refusal(
        &crate::ServiceError::PlacementUnavailable(refusal)
    ));
}

#[test]
fn owner_protocol_location_executor_and_visibility_filters() {
    type RejectionCase = (PlacementFilterCode, fn(&mut SelectionContext));
    let cases: [RejectionCase; 8] = [
        (PlacementFilterCode::OwnerUnreachable, |context| {
            context.candidates[0].connected = false
        }),
        (PlacementFilterCode::OwnerUnreachable, |context| {
            context.candidates[0].runtime_ready = false
        }),
        (PlacementFilterCode::DaemonUpgradeRequired, |context| {
            context.candidates[0].negotiated_revision = Some(2);
            context.candidates[0].connected = false;
        }),
        (PlacementFilterCode::WorkspaceProtocolMissing, |context| {
            context.candidates[0].workspace_v1 = false
        }),
        (PlacementFilterCode::LocationNotReady, |context| {
            context.candidates[0].location.status = RepoLocationStatus::Unverified
        }),
        (PlacementFilterCode::ExecutorUnavailable, |context| {
            context.candidates[0]
                .executors
                .get_mut("coder")
                .unwrap()
                .installed = false
        }),
        (PlacementFilterCode::ExecutorUnavailable, |context| {
            context.candidates[0]
                .executors
                .get_mut("coder")
                .unwrap()
                .enabled = false
        }),
        (PlacementFilterCode::NotVisible, |context| {
            context.candidates[0].visible = false
        }),
    ];
    for (filter, mutate) in cases {
        let mut context = context();
        mutate(&mut context);
        rejected(&context, filter);
    }
}

#[test]
fn shared_mount_requires_current_revision_and_a_ready_location() {
    let mut context = context();
    context.candidates[0].location.owner_kind = RepoLocationOwnerKind::Server;
    context.candidates[0].location.kind = RepoLocationKind::SharedMount;
    context.candidates[0].plan_transport = false;
    context.candidates[0].negotiated_revision = Some(2);
    context.candidates[0].workspace_v1 = false;
    context.candidates[0].connected = false;
    rejected(&context, PlacementFilterCode::DaemonUpgradeRequired);
    context.candidates[0].negotiated_revision = Some(api_types::DAEMON_PROTOCOL_REVISION);
    context.candidates[0].connected = true;
    selected(&context);
    context.candidates[0].location.status = RepoLocationStatus::Unverified;
    rejected(&context, PlacementFilterCode::LocationNotReady);
    context.candidates[0].location.status = RepoLocationStatus::Ready;
    context.candidates[0].location.kind = RepoLocationKind::PrimaryCheckout;
    rejected(&context, PlacementFilterCode::LocationNotReady);
}

#[test]
fn an_embedded_pin_keeps_the_server_as_workspace_owner() {
    let mut context = context();
    let mut server = candidate(&context, "server", None);
    server.execution_daemon_id = Some("embedded".to_owned());
    server.machine_capacity = Some(MachineCapacity::default());
    context.candidates = vec![server];
    context.claiming_agent.agent.daemon_id = Some("embedded".to_owned());
    let selection = selected(&context);
    assert_eq!(
        selection.candidate.location.owner_kind,
        RepoLocationOwnerKind::Server
    );
    assert_eq!(
        selection.candidate.execution_daemon_id.as_deref(),
        Some("embedded")
    );
    assert_eq!(selection.selection_reason.rule, SelectionRule::AgentPin);
}

#[test]
fn a_remote_provider_cannot_reuse_a_server_checkout_without_a_shared_mount() {
    let mut context = context();
    let mut server = candidate(&context, "server", None);
    server.execution_daemon_id = Some("mac".to_owned());
    server.embedded_execution = false;
    server.machine_capacity = Some(MachineCapacity::default());
    context.existing_placement = Some(placement(&server));
    context.candidates = vec![server];
    context.claiming_agent.agent.daemon_id = Some("mac".to_owned());
    rejected(&context, PlacementFilterCode::LocationNotReady);
}

#[test]
fn preference_order_is_default_then_server_then_created_at_and_id() {
    let mut context = context();
    let mut default = candidate(&context, "default", Some("linux"));
    default.location.is_default = true;
    let server = candidate(&context, "server", None);
    context.candidates.extend([default, server]);
    let result = selected(&context);
    assert_eq!(result.candidate.location.id, "default");
    assert_eq!(result.selection_reason.rule, SelectionRule::DefaultLocation);
    context.candidates[1].location.is_default = false;
    let result = selected(&context);
    assert_eq!(result.candidate.location.id, "server");
    assert_eq!(result.selection_reason.rule, SelectionRule::ServerOwned);
    context.candidates.pop();
    context.candidates[1].location.created_at = "2026-09-28T00:00:00Z".to_owned();
    assert_eq!(selected(&context).candidate.location.id, "default");
    context.candidates[1].location.created_at = NOW.to_owned();
    let result = selected(&context);
    assert_eq!(result.candidate.location.id, "default");
    assert_eq!(
        result.selection_reason.rule,
        SelectionRule::DeterministicOrder
    );
    context.candidates.reverse();
    assert_eq!(selected(&context).candidate.location.id, "default");
}

#[test]
fn re_review_reuses_the_existing_owner_handle_and_generation() {
    let mut context = context();
    let existing = placement(&context.candidates[0]);
    context.existing_placement = Some(existing.clone());
    let mut default = candidate(&context, "server", None);
    default.location.is_default = true;
    context.candidates.push(default);
    let result = selected(&context);
    assert_eq!(result.candidate.execution_daemon_id.as_deref(), Some("mac"));
    assert_eq!(result.reused_placement, Some(existing));
    assert_eq!(
        result.selection_reason.rule,
        SelectionRule::ExistingPlacement
    );
}

#[test]
fn subtask_inherits_root_placement_and_rejects_an_incompatible_agent() {
    let mut context = context();
    context.task.parent_task_id = Some("root".to_owned());
    let inherited = placement(&context.candidates[0]);
    context.inherited_root_placement = Some(inherited.clone());
    context.candidates.push(candidate(&context, "server", None));
    let result = selected(&context);
    assert_eq!(result.reused_placement, Some(inherited));
    assert_eq!(
        result.selection_reason.rule,
        SelectionRule::InheritedRootPlacement
    );
    assert_eq!(result.selected_by, PlacementSelectedBy::Inherited);
    context.claiming_agent.agent.daemon_id = Some("other".to_owned());
    rejected(&context, PlacementFilterCode::PinMismatch);
}

#[test]
fn existing_precedes_inherited_and_pin_and_does_not_fall_back_when_offline() {
    let mut context = context();
    context.existing_placement = Some(placement(&context.candidates[0]));
    context.claiming_agent.agent.daemon_id = Some("mac".to_owned());
    let server = candidate(&context, "server", None);
    context.inherited_root_placement = Some(placement(&server));
    context.candidates.push(server);
    assert_eq!(
        selected(&context).selection_reason.rule,
        SelectionRule::ExistingPlacement
    );
    context.candidates[0].connected = false;
    let error = rejected(&context, PlacementFilterCode::OwnerUnreachable);
    assert_eq!(error.rejected_candidates.len(), 2);
    assert!(error
        .rejected_candidates
        .iter()
        .find(|candidate| candidate.repo_location_id == "server")
        .unwrap()
        .filter_codes
        .contains(&PlacementFilterCode::PinMismatch));
}

#[test]
fn disconnected_placement_waits_for_reconciliation_even_with_a_live_socket() {
    let mut context = context();
    let mut existing = placement(&context.candidates[0]);
    existing.state = PlacementState::Disconnected;
    context.existing_placement = Some(existing);
    context.candidates.push(candidate(&context, "server", None));
    rejected(&context, PlacementFilterCode::OwnerUnreachable);
}

#[test]
fn failed_unprepared_reservation_can_be_reselected() {
    let mut context = context();
    let mut failed = placement(&context.candidates[0]);
    failed.state = PlacementState::Failed;
    failed.workspace_handle = None;
    context.existing_placement = Some(failed);
    context.candidates[0].connected = false;
    context.candidates.push(candidate(&context, "server", None));
    assert_eq!(selected(&context).candidate.location.id, "server");
}

#[test]
fn in_flight_reservation_cannot_move_before_its_state_releases_it() {
    for state in [PlacementState::Reserved, PlacementState::Preparing] {
        let mut context = context();
        let mut existing = placement(&context.candidates[0]);
        existing.state = state;
        existing.workspace_handle = None;
        context.existing_placement = Some(existing);
        context.candidates[0].connected = false;
        context.candidates.push(candidate(&context, "server", None));
        rejected(&context, PlacementFilterCode::OwnerUnreachable);
    }
}

#[test]
fn rejection_evidence_includes_all_failed_candidates_and_filter_codes() {
    let mut context = context();
    context.candidates[0].connected = false;
    context.candidates[0].workspace_v1 = false;
    context.candidates[0].visible = false;
    context.candidates.push(candidate(&context, "server", None));
    let result = selected(&context);
    assert_eq!(result.selection_reason.rejected_candidates.len(), 1);
    assert_eq!(
        result.selection_reason.rejected_candidates[0].filter_codes,
        vec![
            PlacementFilterCode::OwnerUnreachable,
            PlacementFilterCode::WorkspaceProtocolMissing,
            PlacementFilterCode::NotVisible
        ]
    );
    context.candidates[1].location.status = RepoLocationStatus::Invalid;
    let error = rejected(&context, PlacementFilterCode::LocationNotReady);
    assert_eq!(error.rejected_candidates.len(), 2);
}

#[test]
fn an_empty_candidate_set_is_structured_unavailable() {
    let mut context = context();
    context.candidates.clear();
    let error = select_placement(&context).into_result().unwrap_err();
    assert_eq!(error.task_id, "task");
    assert_eq!(error.repo_id, "repo");
    assert!(error.rejected_candidates.is_empty());
}

fn load_input<'a>(
    context: &'a SelectionContext,
    server: &'a ServerFacts,
    review: &'a ReviewConfig,
    settings: &'a ProjectSettings,
    handshakes: &'a BTreeMap<String, ConnectionHandshake>,
) -> SelectionLoadInput<'a> {
    SelectionLoadInput {
        task: &context.task,
        repo: &context.repo,
        claiming_agent: &context.claiming_agent,
        worktree_agents: &context.worktree_agents,
        task_owner_id: Some("owner"),
        workspace_id: None,
        inherited_root_workspace_id: None,
        review_config: review,
        project_settings: settings,
        server,
        handshakes,
        fallback_server_location: None,
    }
}

#[tokio::test]
async fn loader_uses_transactional_runtime_facts_and_current_connection_handshake() {
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let db = SqliteDb::new(pool);
    db.server_run_cap
        .set(Some(0), 0, &config::embedded_machine_id());
    sqlx::raw_sql(
            "INSERT INTO daemon (id, machine_id, hostname, os, arch, status, owner_id,
                                 visibility, detected_clis_json, labels_json, created_at, updated_at, max_concurrent_runs)
             VALUES ('mac', 'mac-machine', 'Mac', 'macos', 'aarch64', 'online', NULL,
                     'global', '[{\"kind\":\"codex\",\"availability\":\"authenticated\"}]',
                     '{}', '2026-09-29T00:00:00Z', '2026-09-29T00:00:00Z', 2);
             INSERT INTO runtime (id, daemon_id, kind, workspace_root, status, created_at, updated_at)
             VALUES ('runtime-mac', 'mac', 'local', '/checkout', 'ready',
                     '2026-09-29T00:00:00Z', '2026-09-29T00:00:00Z');",
        )
        .execute(db.pool())
        .await
        .unwrap();
    let context = context();
    let location = &context.candidates[0].location;
    db::RepoLocationRepo::create(
        &db,
        db::CreateRepoLocation {
            id: location.id.clone(),
            repo_id: location.repo_id.clone(),
            owner_kind: location.owner_kind.clone(),
            daemon_id: location.daemon_id.clone(),
            runtime_id: location.runtime_id.clone(),
            path: location.path.clone(),
            kind: location.kind.clone(),
            is_default: false,
            status: RepoLocationStatus::Ready,
            last_verified_at: Some(NOW.to_owned()),
            last_error: None,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .unwrap();
    let registry = DaemonConnectionRegistry::without_handlers();
    let (connection, _outbound) = crate::daemon_transport::DaemonConnection::new("mac".to_owned());
    registry.register("mac".to_owned(), connection.clone());
    let handshake = DaemonHandshakeNotification {
        protocol_revision: api_types::DAEMON_PROTOCOL_REVISION,
        capabilities: api_types::DAEMON_REQUIRED_CAPABILITIES
            .iter()
            .map(|name| (*name).to_owned())
            .chain([
                api_types::DAEMON_CAPABILITY_WORKSPACE.to_owned(),
                api_types::DAEMON_CAPABILITY_PLAN_TRANSPORT.to_owned(),
            ])
            .collect(),
        executor_capabilities: BTreeMap::from([("codex".to_owned(), capabilities())]),
        workspace_run_policy: api_types::WorkspaceRunPolicy {
            allowed_purposes: vec![WorkspaceRunPurpose::CiStep],
        },
    };
    registry.dispatch_incoming_for_connection(
        "mac",
        connection.id(),
        api_types::DaemonFrame::Notification {
            method: api_types::METHOD_DAEMON_HANDSHAKE.to_owned(),
            params: serde_json::to_value(&handshake).unwrap(),
        },
    );
    let handshakes = BTreeMap::from([(
        "mac".to_owned(),
        ConnectionHandshake {
            connection_id: connection.id(),
            handshake,
        },
    )]);
    let server = ServerFacts::default();
    let settings: ProjectSettings = serde_json::from_str("{}").unwrap();
    let review = ReviewConfig {
        ci_steps: vec!["make test".to_owned()],
        ..ReviewConfig::default()
    };
    let mut transaction = db::begin_immediate(db.pool()).await.unwrap();
    let loaded = load_selection_context(
        &db,
        &mut transaction,
        &registry,
        load_input(&context, &server, &review, &settings, &handshakes),
    )
    .await
    .unwrap();
    assert_eq!(
        selected(&loaded).candidate.execution_daemon_id.as_deref(),
        Some("mac")
    );
    assert_eq!(
        loaded.candidates[0]
            .machine_capacity
            .unwrap()
            .max_concurrent_runs,
        Some(2)
    );

    let mut stale_handshakes = handshakes.clone();
    stale_handshakes.get_mut("mac").unwrap().connection_id += 1;
    let loaded = load_selection_context(
        &db,
        &mut transaction,
        &registry,
        load_input(&context, &server, &review, &settings, &stale_handshakes),
    )
    .await
    .unwrap();
    rejected(&loaded, PlacementFilterCode::WorkspaceProtocolMissing);

    sqlx::query("UPDATE runtime SET status = 'degraded' WHERE id = 'runtime-mac'")
        .execute(&mut *transaction)
        .await
        .unwrap();
    let loaded = load_selection_context(
        &db,
        &mut transaction,
        &registry,
        load_input(&context, &server, &review, &settings, &handshakes),
    )
    .await
    .unwrap();
    rejected(&loaded, PlacementFilterCode::OwnerUnreachable);
}
#[test]
fn environment_and_capacity_refusals_are_not_deterministic() {
    let refusal = |codes: Vec<PlacementFilterCode>| PlacementUnavailable {
        task_id: "task".to_owned(),
        repo_id: "repo".to_owned(),
        rejected_candidates: vec![CandidateRejection {
            failing_checks: Vec::new(),
            repo_location_id: "location".to_owned(),
            owner_kind: "server".to_owned(),
            daemon_id: None,
            runtime_id: None,
            filter_codes: codes,
        }],
    };
    for code in [
        PlacementFilterCode::EnvironmentProbePending,
        PlacementFilterCode::EnvironmentNotReady,
        PlacementFilterCode::AgentCapacity,
        PlacementFilterCode::MachineCapacity,
        PlacementFilterCode::OwnerUnreachable,
    ] {
        assert!(!refusal(vec![code]).is_deterministic(), "{code:?}");
    }
    assert!(refusal(vec![PlacementFilterCode::CapabilityMissing]).is_deterministic());
}
