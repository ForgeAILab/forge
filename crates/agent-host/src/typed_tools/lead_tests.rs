//! The lead operations in a native Task session: who is offered them, and
//! what they cost in the stable tool prefix of the session that is.
use super::registry_tests::RecordingProvider;
use super::tests::scope;
use super::*;
use operation_registry::authority::{AuthorityFacts, EffectiveAuthority, Principal, TaskRole};

/// The byte budget of the lead operations in a lead session's tool prefix
/// (the inventory's 2,500 tokens, at four bytes a token).
const LEAD_PREFIX_BUDGET_BYTES: usize = 10_000;

fn permissions() -> BTreeSet<String> {
    BTreeSet::from(["task_read".into(), "read_task".into(), "read_memory".into()])
}

fn authority(role: TaskRole, lead_root_id: Option<&str>) -> EffectiveAuthority {
    EffectiveAuthority::resolve(AuthorityFacts {
        principal: Principal::TaskAgent {
            identity_id: "agent".into(),
            execution_id: "exec".into(),
            role,
            runtime_session_id: Some("session".into()),
            lead_root_id: lead_root_id.map(str::to_owned),
        },
        scope_type: "task".into(),
        scope_id: "scope-1".into(),
        profile_id: "profile".into(),
        layers: vec![permissions()],
        binding_id: None,
        setup_required: false,
        active: true,
    })
}

/// The propose operations a Task session is offered, as composition does it.
fn offered(authority: &EffectiveAuthority) -> Vec<String> {
    let (_, propose) = task_operations();
    filter_operations(
        CanonicalScopeType::Task,
        &propose,
        &permissions(),
        ProjectChatToolContext::default(),
        Some(authority),
    )
}

fn prefix_bytes(operations: Vec<String>) -> usize {
    let tool = ForgeScopeProposeTool::new(
        "agent".into(),
        scope(CanonicalScopeType::Task, WorkspaceAccess::TaskRead),
        operations,
        Arc::new(RecordingProvider::default()),
    );
    let spec = tool.spec();
    spec.name.len() + spec.description.len() + spec.input_schema.to_string().len()
}

#[test]
fn only_a_lead_session_is_offered_the_lead_operations() {
    let lead = offered(&authority(TaskRole::Planner, Some("root")));
    for id in operation_registry::task_lead::IDS {
        assert!(lead.iter().any(|offered| offered == id), "{id}: {lead:?}");
    }
    // Every other Task session: an ordinary planner, a worker, a reviewer,
    // and the projection used when no authority was admitted.
    for role in [TaskRole::Planner, TaskRole::Worker, TaskRole::Reviewer] {
        let ordinary = offered(&authority(role, None));
        assert!(
            ordinary.iter().all(|id| !id.starts_with("lead.")),
            "{role:?}: {ordinary:?}"
        );
    }
    let (_, propose) = task_operations();
    let projected = filter_operations(
        CanonicalScopeType::Task,
        &propose,
        &permissions(),
        ProjectChatToolContext::default(),
        None,
    );
    assert!(
        projected.iter().all(|id| !id.starts_with("lead.")),
        "{projected:?}"
    );
    // A lead without the Task read permission is offered nothing.
    let mut bare = authority(TaskRole::Planner, Some("root"));
    bare.ceiling.clear();
    assert!(offered(&bare).is_empty());
}

#[test]
fn the_lead_operations_fit_their_tool_prefix_budget_and_cost_nobody_else() {
    let planner = offered(&authority(TaskRole::Planner, None));
    let lead = offered(&authority(TaskRole::Planner, Some("root")));
    let (without, with) = (prefix_bytes(planner.clone()), prefix_bytes(lead));
    let added = with - without;
    eprintln!("lead tool prefix: {without} -> {with} bytes, {added} added");
    assert!(
        added <= LEAD_PREFIX_BUDGET_BYTES,
        "the lead operations add {added} bytes to the tool prefix (budget {LEAD_PREFIX_BUDGET_BYTES})"
    );
    assert!(added > 0);
    // Nobody else pays: the ordinary planner's prefix is the Task-record
    // prefix it had before the lead operations existed.
    let before = operation_registry::task_records::IDS
        .iter()
        .map(|id| (*id).to_owned())
        .collect::<Vec<_>>();
    let before = filter_operations(
        CanonicalScopeType::Task,
        &before,
        &permissions(),
        ProjectChatToolContext::default(),
        Some(&authority(TaskRole::Planner, None)),
    );
    assert_eq!(planner, before);
    assert_eq!(without, prefix_bytes(before));
}

/// A lead call reaches the provider in the envelope every Task command uses,
/// with flat fields lifted into the payload.
#[test]
fn a_lead_call_is_normalized_like_every_task_command() {
    let tool = ForgeScopeProposeTool::new(
        "agent".into(),
        scope(CanonicalScopeType::Task, WorkspaceAccess::TaskRead),
        operation_registry::task_lead::IDS
            .iter()
            .map(|id| (*id).to_owned())
            .collect(),
        Arc::new(RecordingProvider::default()),
    );
    let normalized = tool
        .normalize_arguments(json!({
            "operation": "lead.subtask",
            "action": "create",
            "title": "fix the build",
            "dedupe_key": "key",
            "correlation_id": "correlation",
        }))
        .unwrap();
    assert_eq!(
        normalized["payload"],
        json!({"action": "create", "title": "fix the build"})
    );
    let nested = tool
        .normalize_arguments(json!({
            "operation": "lead.recover",
            "payload": {"task_id": "child", "action": {"verb": "retry"}, "version": 3},
            "dedupe_key": "key",
            "correlation_id": "correlation",
            "causation_depth": null,
        }))
        .unwrap();
    assert_eq!(nested["payload"]["action"]["verb"], "retry");
    assert!(nested.get("causation_depth").is_none());
}
