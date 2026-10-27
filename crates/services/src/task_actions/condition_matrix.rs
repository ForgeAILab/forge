//! Pure task condition offer parity matrix.

fn projected_actions(snapshot: &super::TaskSnapshot) -> Vec<api_types::Offer> {
    let mut projected = snapshot.clone();
    projected.task.condition = db::compose_condition(&db::ConditionInput::from(&projected.task));
    let actual = super::available_actions(&projected);
    let expected = super::legacy::available_actions(&super::legacy::TaskSnapshot::from(&projected));
    assert_eq!(
        serde_json::to_value(&actual).unwrap(),
        serde_json::to_value(expected).unwrap(),
        "old action offers must agree for each matrix condition"
    );
    actual
}
use super::*;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

fn task(state: &str) -> Task {
    serde_json::from_value(json!({
            "id":"task", "project_id":"project", "parent_task_id":null, "assignee_type":"agent", "assignee_id":"agent", "title":"Fixture", "description":null, "task_type":"task", "status":state, "is_automation":false, "priority":0, "board_position":0.0, "subtask_order":null, "task_state_config":null, "merge_config":null, "metadata_json":null, "plan":null, "error_annotation":null, "blocked_json":null, "failed_json":null, "entry_barrier_json":null, "review_passed_at":null, "archived_at":null, "deleted_at":null, "version":1, "created_at":"2026-10-02T00:00:00Z", "updated_at":"2026-10-02T00:00:00Z"
        })).expect("Task fixture")
}

pub(super) fn exec(id: &str, role: &str, status: ExecutionStatus, session: bool) -> Execution {
    Execution {
        id: id.to_owned(),
        task_id: "task".to_owned(),
        agent_id: Some("agent".to_owned()),
        role: role.to_owned(),
        status,
        stop_reason: None,
        stopped_by: None,
        resume_policy: None,
        stopped_at: None,
        parent_execution_id: None,
        agent_session_id: session.then(|| "session".to_owned()),
        agent_message_id: None,
        last_activity_at: None,
        prompt: None,
        summary: None,
        logs_path: None,
        before_sha: None,
        after_sha: None,
        error: None,
        executor_config_snapshot_json: None,
        workspace_id: None,
        execution_version: 1,
        lease_owner: None,
        lease_expires_at: None,
        hard_deadline_at: None,
        last_heartbeat_at: None,
        last_progress_at: None,
        created_at: "2026-10-02T00:10:00Z".to_owned(),
        updated_at: "2026-10-02T00:10:00Z".to_owned(),
    }
}

pub(super) fn review(status: ReviewStatus) -> Review {
    Review {
        id: "review".to_owned(),
        task_id: "task".to_owned(),
        execution_id: "candidate".to_owned(),
        reviewer_execution_id: None,
        auditor_execution_id: None,
        attempt_number: 1,
        status,
        step_results_json: "[]".to_owned(),
        started_at: "2026-10-02T00:05:00Z".to_owned(),
        finished_at: None,
        created_at: "2026-10-02T00:05:00Z".to_owned(),
        updated_at: "2026-10-02T00:05:00Z".to_owned(),
    }
}

pub(super) fn rejection(index: usize, from: &str, to: &str) -> TransitionLog {
    serde_json::from_value(json!({
        "id": format!("log-{index}"), "task_id":"task", "from_state":from, "to_state":to,
        "trigger_name":"reject", "triggered_by":"user", "trigger_reason":"fixture",
        "hook_results_json":null, "rejection":true,
        "created_at": format!("2026-10-02T00:0{index}:00Z")
    }))
    .expect("log fixture")
}

#[test]
fn condition_pure_dead_end_matrix() {
    let kinds = [
        FailureKind::MergeConflict,
        FailureKind::TargetRepoDirty,
        FailureKind::DirtyWorktree,
        FailureKind::CiFailed,
        FailureKind::ReviewGateFailed,
        FailureKind::ReviewBudgetExhausted,
        FailureKind::ReviewBlocked,
        FailureKind::ReviewNeedsOwner,
        FailureKind::EnvironmentNotReady,
        FailureKind::RetryExhausted,
        FailureKind::MergeFixBudgetExhausted,
        FailureKind::WorkflowGuardRejected,
        FailureKind::InternalCommandFailed,
        FailureKind::ExecutorFailed,
        FailureKind::WorkspaceFailed,
        FailureKind::WorkspaceResetRequired,
        FailureKind::WorkspaceError,
        FailureKind::BeforeWorkHookTimeout,
        FailureKind::BeforeWorkHookFailed,
        FailureKind::MaxTurnsExceeded,
        FailureKind::ManualStop,
        FailureKind::RecoveryRequired,
        FailureKind::ExecutorUnavailable,
        FailureKind::Unknown,
    ];
    let mut conditions: Vec<String> = vec!["none".to_owned()];
    conditions.extend(kinds.iter().map(|kind| format!("ann:{kind}")));
    for extra in [
        "blocked:retry_exhausted",
        "blocked:review_gate_failed",
        "blocked:merge_fix_budget_exhausted",
        "blocked:executor_failed",
        "blocked:manual_stop",
        "failed_json",
        "legacy_annotation",
        "barrier_blocked",
        "queued",
        "awaiting_human_meta",
        "pr_merge_wait",
    ] {
        conditions.push(extra.to_owned());
    }
    let reviews = [
        "none",
        "running",
        "awaiting",
        "failed",
        "passed",
        "cancelled",
    ];
    let execs = [
        "none",
        "done_session",
        "done_nosession",
        "failed_session",
        "running",
    ];
    let flags = [
        "default",
        "coordination_root",
        "no_agent",
        "deps_unsatisfied",
    ];
    let workflows = [
        ("std", crate::workflow::default_workflow::default_workflow()),
        (
            "auto",
            crate::workflow::default_autonomous_workflow::default_autonomous_workflow(),
        ),
    ];
    let mut full = String::new();
    // key -> (dead combos, total combos, example dims)
    let mut dead: BTreeMap<String, (usize, usize, BTreeSet<String>)> = BTreeMap::new();
    let mut summary: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (wf_name, workflow) in &workflows {
        for state in &workflow.states {
            let role = crate::workflow::effective_role(state).unwrap_or("coder");
            let gate = workflow.states.iter().find(|gate| {
                gate.kind == StateKind::Gate
                    && (gate.name == state.name
                        || gate
                            .gate_config
                            .as_ref()
                            .and_then(|config| config.reject_target.as_deref())
                            == Some(state.name.as_str()))
                    && gate
                        .gate_config
                        .as_ref()
                        .and_then(|config| config.max_rejections)
                        .is_some()
            });
            for condition in &conditions {
                for flag in flags {
                    for review_name in reviews {
                        for exec_name in execs {
                            for exhausted in [false, true] {
                                if exhausted && gate.is_none() {
                                    continue;
                                }
                                let mut task = task(&state.name);
                                let mut metadata = json!({});
                                if let Some(kind) = condition.strip_prefix("ann:") {
                                    task.error_annotation = Some(json!({"type":kind,"blocking_reason":"fixture","recovery_actions":["retry_hook","return_to_implementation"]}).to_string());
                                } else if let Some(kind) = condition.strip_prefix("blocked:") {
                                    task.blocked_json = Some(json!({"kind":kind,"reason":"fixture","created_at":"2026-10-02T00:00:00Z"}).to_string());
                                } else {
                                    match condition.as_str() {
                                        "failed_json" => task.failed_json = Some(
                                            json!({"kind":"executor_failed","reason":"fixture"})
                                                .to_string(),
                                        ),
                                        "legacy_annotation" => {
                                            task.error_annotation = Some(
                                                json!({"message":"old free-form annotation"})
                                                    .to_string(),
                                            )
                                        }
                                        "barrier_blocked" => {
                                            task.entry_barrier_json = Some(
                                                json!({"status":"blocked","state":state.name})
                                                    .to_string(),
                                            )
                                        }
                                        "queued" => metadata["queued_recovery"] = json!({"id":"q"}),
                                        "awaiting_human_meta" => {
                                            metadata["awaiting_human"] = json!(true)
                                        }
                                        "pr_merge_wait" => {
                                            metadata["awaiting_human"] = json!(true);
                                            metadata["awaiting_human_reason"] =
                                                json!("pull_request_merge");
                                        }
                                        _ => {}
                                    }
                                }
                                if metadata
                                    .as_object()
                                    .is_some_and(|object| !object.is_empty())
                                {
                                    task.metadata_json = Some(metadata.to_string());
                                }
                                let mut executions = vec![];
                                match exec_name {
                                    "done_session" => executions.push(exec(
                                        "e1",
                                        role,
                                        ExecutionStatus::Completed,
                                        true,
                                    )),
                                    "done_nosession" => executions.push(exec(
                                        "e1",
                                        role,
                                        ExecutionStatus::Completed,
                                        false,
                                    )),
                                    "failed_session" => executions.push(exec(
                                        "e1",
                                        role,
                                        ExecutionStatus::Failed,
                                        true,
                                    )),
                                    "running" => executions.push(exec(
                                        "e1",
                                        role,
                                        ExecutionStatus::Running,
                                        true,
                                    )),
                                    _ => {}
                                }
                                let latest_review = match review_name {
                                    "running" => Some(review(ReviewStatus::Running)),
                                    "awaiting" => Some(review(ReviewStatus::AwaitingHuman)),
                                    "failed" => Some(review(ReviewStatus::Failed)),
                                    "passed" => Some(review(ReviewStatus::Passed)),
                                    "cancelled" => Some(review(ReviewStatus::Cancelled)),
                                    _ => None,
                                };
                                let mut transition_logs = vec![];
                                if exhausted {
                                    let gate = gate.unwrap();
                                    let config = gate.gate_config.as_ref().unwrap();
                                    for index in 0..config.max_rejections.unwrap() as usize {
                                        transition_logs.push(rejection(
                                            index,
                                            &gate.name,
                                            config
                                                .reject_target
                                                .as_deref()
                                                .unwrap_or("in_progress"),
                                        ));
                                    }
                                }
                                let snapshot = TaskSnapshot {
                                    task,
                                    workflow: workflow.clone(),
                                    executions,
                                    latest_review,
                                    role_assignments: Vec::new(),
                                    transition_logs,
                                    budget_spent: gate
                                        .filter(|_| exhausted)
                                        .map(|g| {
                                            std::collections::HashMap::from([(
                                                db::budget::gate_key(&g.name),
                                                i64::from(
                                                    g.gate_config
                                                        .as_ref()
                                                        .unwrap()
                                                        .max_rejections
                                                        .unwrap(),
                                                ),
                                            )])
                                        })
                                        .unwrap_or_default(),
                                    recovery_budget_limit: 0,
                                    caller: ActionCaller::owner(),
                                    has_agent: flag != "no_agent",
                                    dependencies_satisfied: flag != "deps_unsatisfied",
                                    owner_supports_resume: true,
                                    coordination_root: flag == "coordination_root",
                                    owner_disconnected: false,
                                    project_paused: false,
                                    wait_cause: None,
                                    action_agent_id: None,
                                    planning_approval_ready: true,
                                    advance_target: None,
                                    entry_hooks_running: false,
                                };
                                let offers = projected_actions(&snapshot);
                                let verbs: Vec<String> = offers
                                    .iter()
                                    .map(|offer| {
                                        format!("{}[{}]", offer.action.verb(), offer.reason)
                                    })
                                    .collect();
                                let _ = writeln!(full, "{wf_name}\t{}\t{:?}\t{condition}\t{flag}\trev={review_name}\texec={exec_name}\trej_max={exhausted}\t{}", state.name, state.kind, verbs.join(","));
                                let terminal = state.kind == StateKind::Terminal;
                                let in_flight = exec_name == "running";
                                let only_cancel =
                                    offers.iter().all(|offer| offer.action.verb() == "cancel");
                                let key = format!(
                                    "{wf_name}|{}|{:?}|{condition}|{flag}",
                                    state.name, state.kind
                                );
                                let entry = dead.entry(key).or_default();
                                if !in_flight && !terminal {
                                    entry.1 += 1;
                                    if only_cancel {
                                        entry.0 += 1;
                                        entry.2.insert(format!("rev={review_name},exec={exec_name},rej_max={exhausted}"));
                                    }
                                }
                                if flag == "default" && !terminal {
                                    summary.entry(format!("{wf_name}|{}|{condition}|rev={review_name}|exec={exec_name}|rej_max={exhausted}", state.name)).or_default().extend(verbs.iter().cloned());
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    std::fs::create_dir_all(action_test_evidence_dir()).unwrap();
    std::fs::write(
        action_test_evidence_dir().join("pure-matrix-full.tsv"),
        full,
    )
    .unwrap();
    let mut out = String::new();
    for (key, (count, total, dims)) in &dead {
        if *count == 0 {
            continue;
        }
        if count == total {
            let _ = writeln!(out, "DEAD-ALL\t{key}\t{count}/{total}");
        } else {
            let revs: BTreeSet<_> = dims
                .iter()
                .map(|dim| dim.split(',').next().unwrap().to_owned())
                .collect();
            let execs: BTreeSet<_> = dims
                .iter()
                .map(|dim| dim.split(',').nth(1).unwrap().to_owned())
                .collect();
            let rejs: BTreeSet<_> = dims
                .iter()
                .map(|dim| dim.split(',').nth(2).unwrap().to_owned())
                .collect();
            let _ = writeln!(
                out,
                "DEAD-SOME\t{key}\t{count}/{total}\t{revs:?}\t{execs:?}\t{rejs:?}"
            );
        }
    }
    std::fs::write(action_test_evidence_dir().join("pure-matrix-dead.tsv"), out).unwrap();
}
