//! Tests for mapping in task conditions.

use super::*;
/// A machine-capacity wait names what it waits for: a run slot unless the
/// disposition says `disk`. Both are the same wait to everything else.
#[test]
fn machine_capacity_wait_names_disk_when_its_scope_says_so() {
    let scope = |disposition: serde_json::Value| {
        let condition = compose_condition(&input(
            None,
            None,
            None,
            None,
            Some(json!({ "dispatch_disposition": disposition })),
        ));
        match reasons(&condition).first() {
            Some(ParkReason::Capacity { scope }) => scope.clone(),
            other => panic!("{other:?}"),
        }
    };
    assert_eq!(
        scope(json!({"capability":"machine_capacity"})),
        ConditionCapacityScope::Machine
    );
    assert_eq!(
        scope(json!({"capability":"machine_capacity","capacity_scope":"disk"})),
        ConditionCapacityScope::Disk
    );
    assert_eq!(
        scope(json!({"capability":"machine_capacity","capacity_scope":"something newer"})),
        ConditionCapacityScope::Machine
    );
    assert_eq!(
        serde_json::to_value(ConditionCapacityScope::Disk).unwrap(),
        json!("disk")
    );
}

#[tokio::test]
async fn mapping_table_preserves_known_unknown_and_combined_conditions() {
    let cases = [
        ("clear", ConditionInput::default(), "clear", None),
        (
            "held",
            input(
                Some(json!({"type":"manual_stop","blocked_by":"user:owner"})),
                None,
                None,
                None,
                None,
            ),
            "parked",
            Some("held"),
        ),
        (
            "failed",
            input(
                None,
                None,
                Some(json!({"kind":"executor_failed","details":{"custom":[1,2]}})),
                None,
                None,
            ),
            "failed",
            Some("failure"),
        ),
        (
            "blocked",
            input(
                None,
                Some(json!({"kind":"workspace_error"})),
                None,
                None,
                None,
            ),
            "parked",
            Some("failure"),
        ),
        (
            "budget",
            input(
                Some(json!({"type":"review_budget_exhausted"})),
                Some(json!({"kind":"review_budget_exhausted"})),
                None,
                None,
                None,
            ),
            "parked",
            Some("budget_exhausted"),
        ),
        // M1: the dispatcher does not block on these annotations alone.
        (
            "budget_annotation_only",
            input(
                Some(json!({"type":"review_budget_exhausted"})),
                None,
                None,
                None,
                None,
            ),
            "clear",
            None,
        ),
        (
            "advisory_merge_annotation",
            input(
                Some(json!({"type":"merge_conflict","message":"auto repair"})),
                None,
                None,
                None,
                None,
            ),
            "clear",
            None,
        ),
        (
            "empty_annotation",
            input(Some(json!({})), None, None, None, None),
            "clear",
            None,
        ),
        // m4: a blocking kind that is not a FailureKind.
        (
            "agent_timeout",
            input(
                Some(json!({"type":"agent_timeout"})),
                None,
                None,
                None,
                None,
            ),
            "parked",
            Some("agent_timeout"),
        ),
        (
            "entry_budget",
            input(
                None,
                None,
                None,
                Some(json!({"status":"blocked","blocking_reason":"review retry budget exhausted"})),
                None,
            ),
            "parked",
            Some("budget_exhausted"),
        ),
        (
            "barrier",
            input(
                None,
                None,
                None,
                Some(json!({"state":"review","status":"blocked"})),
                None,
            ),
            "parked",
            Some("entry_blocked"),
        ),
        (
            "old_running",
            input(
                None,
                None,
                None,
                Some(json!({"state":"review","status":"running"})),
                None,
            ),
            "parked",
            Some("unknown_condition"),
        ),
        (
            "human",
            input(
                None,
                None,
                None,
                None,
                Some(json!({"awaiting_human":true,"awaiting_human_reason":"plan_review"})),
            ),
            "parked",
            Some("human_decision"),
        ),
        (
            "capacity",
            input(
                None,
                None,
                None,
                None,
                Some(json!({"dispatch_disposition":{"capability":"machine_capacity"}})),
            ),
            "parked",
            Some("capacity"),
        ),
        (
            "capacity_disk",
            input(
                None,
                None,
                None,
                None,
                Some(
                    json!({"dispatch_disposition":{"capability":"machine_capacity","capacity_scope":"disk"}}),
                ),
            ),
            "parked",
            Some("capacity"),
        ),
        (
            "refusal",
            input(
                None,
                None,
                None,
                None,
                Some(
                    json!({"dispatch_disposition":{"capability":"coder","blocker_digest":"sha256:abc"}}),
                ),
            ),
            "parked",
            Some("dispatch_refusal"),
        ),
        (
            "pause",
            input(
                None,
                None,
                None,
                None,
                Some(
                    json!({"paused_integration":{"state":"merging"},"paused_integration_generation":7}),
                ),
            ),
            "parked",
            Some("project_paused"),
        ),
        (
            "owner",
            input(
                None,
                None,
                None,
                None,
                Some(json!({"owner_wait":{"daemon_id":"d","started_at":"2026-10-06T00:00:00Z"}})),
            ),
            "parked",
            Some("owner_offline"),
        ),
        (
            "environment",
            input(
                None,
                None,
                None,
                None,
                Some(
                    json!({"environment_wait":{"kind":"environment_unverified","machine":{"daemon_id":"d"}}}),
                ),
            ),
            "parked",
            Some("environment"),
        ),
        (
            "placement",
            input(
                None,
                None,
                None,
                None,
                Some(json!({"placement_refusal":{"eligibility_key":"unchanged"}})),
            ),
            "parked",
            Some("placement_denied"),
        ),
        (
            "upgrade",
            input(
                None,
                None,
                None,
                None,
                Some(json!({"daemon_upgrade_refusal":{"daemon_ids":["d"]}})),
            ),
            "parked",
            Some("daemon_upgrade_required"),
        ),
        (
            "plan_wait",
            input(
                None,
                None,
                None,
                None,
                Some(json!({"plan_settlement_wait":{"execution_id":"e"}})),
            ),
            "parked",
            Some("plan_settlement_wait"),
        ),
        (
            "timer",
            input(
                None,
                None,
                None,
                None,
                Some(
                    json!({"deferred_dispatch":{"not_before":"2026-10-06T00:01:00Z","reason":"retry","target_state":"working"}}),
                ),
            ),
            "deferred",
            None,
        ),
        (
            "malformed_deadline",
            input(
                None,
                None,
                None,
                None,
                Some(
                    json!({"deferred_dispatch":{"not_before":"not a timestamp","reason":"retry","target_state":"review"}}),
                ),
            ),
            "clear",
            None,
        ),
        (
            "remote_cancel",
            input(
                Some(
                    json!({"type":"workspace_reset_required","blocking_reason":"pending_remote_cancel"}),
                ),
                None,
                None,
                None,
                None,
            ),
            "parked",
            Some("remote_cancel_pending"),
        ),
        (
            "ci_exhausted",
            input(
                Some(
                    json!({"type":"before_work_hook_failed","blocking_reason":"review_ci_infrastructure_exhausted"}),
                ),
                None,
                None,
                Some(json!({"status":"blocked","state":"review"})),
                None,
            ),
            "parked",
            Some("budget_exhausted"),
        ),
        (
            "unknown",
            input(
                Some(json!({"type":"future_failure","secret_preserved":{"x":true}})),
                None,
                None,
                None,
                None,
            ),
            "clear",
            None,
        ),
        (
            "unknown_blocked_kind",
            input(
                None,
                Some(json!({"kind":"future_failure"})),
                None,
                None,
                None,
            ),
            "parked",
            Some("unknown_condition"),
        ),
        (
            "unknown_key",
            input(
                None,
                None,
                None,
                None,
                Some(json!({"custom_user_key":{"x":1}})),
            ),
            "clear",
            None,
        ),
        (
            "bad_shape",
            input(
                None,
                None,
                None,
                None,
                Some(json!({"deferred_dispatch":[1],"awaiting_human":"yes"})),
            ),
            "clear",
            Some("unknown_condition"),
        ),
        (
            "bad_nested_types",
            input(
                Some(json!({"type":"manual_stop","blocked_by":[1]})),
                None,
                None,
                None,
                Some(json!({"owner_wait":{"daemon_id":[1],"started_at":42}})),
            ),
            "parked",
            Some("held"),
        ),
    ];
    for (name, legacy, expected, primary) in cases {
        let condition = compose_condition(&legacy);
        let value = serde_json::to_value(&condition).unwrap();
        assert_eq!(value["kind"], expected, "{name}");
        if let Some(kind) = primary {
            assert_eq!(
                serde_json::to_value(reasons(&condition)[0]).unwrap()["kind"],
                kind,
                "{name}"
            );
        }
        assert_eq!(
            value["evidence"]["error_annotation"],
            json!(legacy.error_annotation),
            "{name}"
        );
        assert_eq!(
            value["evidence"]["blocked_json"],
            json!(legacy.blocked_json),
            "{name}"
        );
        assert_eq!(
            value["evidence"]["failed_json"],
            json!(legacy.failed_json),
            "{name}"
        );
        assert_eq!(
            value["evidence"]["entry_barrier_json"],
            json!(legacy.entry_barrier_json),
            "{name}"
        );
        let again = compose_condition(&legacy);
        assert_eq!(condition, again, "deterministic {name}");
        assert_eq!(
            decode(&encode(&condition)).unwrap(),
            condition,
            "stored form round-trips {name}"
        );
    }
    for field in 0..5 {
        for raw in ["", "{invalid", "null", "[]", "42", "\"scalar\""] {
            let mut legacy = ConditionInput::default();
            *match field {
                0 => &mut legacy.error_annotation,
                1 => &mut legacy.blocked_json,
                2 => &mut legacy.failed_json,
                3 => &mut legacy.entry_barrier_json,
                _ => &mut legacy.metadata_json,
            } = Some(raw.into());
            let condition = compose_condition(&legacy);
            if field == 0 {
                // M1: the dispatcher dispatches through an annotation it
                // cannot read, so it is evidence, not a park.
                assert!(
                    matches!(&condition, TaskCondition::Clear { evidence }
                        if evidence.error_annotation.as_deref() == Some(raw)),
                    "{field}:{raw}"
                );
                continue;
            }
            assert!(
                matches!(reasons(&condition)[0], ParkReason::UnknownCondition { .. }),
                "{field}:{raw}"
            );
        }
    }
    let combined = input(
        Some(json!({"type":"before_work_hook_failed"})),
        Some(json!({"kind":"retry_exhausted"})),
        None,
        Some(json!({"state":"review","status":"blocked"})),
        Some(
            json!({"owner_wait":{"daemon_id":"d"},"paused_integration":{"state":"review"},"deferred_dispatch":{"not_before":"2026-10-06T00:01:00Z","reason":"wait","target_state":"review"}}),
        ),
    );
    let condition = compose_condition(&combined);
    assert_eq!(reasons(&condition).len(), 5);
    assert!(reasons(&condition)
        .iter()
        .any(|r| matches!(r, ParkReason::BudgetExhausted { .. })));
    assert!(reasons(&condition)
        .iter()
        .any(|r| matches!(r, ParkReason::EntryBlocked { .. })));
    assert!(reasons(&condition)
        .iter()
        .any(|r| matches!(r, ParkReason::OwnerOffline { .. })));
    // retry_exhausted is also written by generic gate budgets. Legacy inputs
    // alone cannot resolve its ledger kind; do not invent an Execution window.
    let ambiguous = input(
        None,
        Some(json!({"kind":"retry_exhausted"})),
        None,
        None,
        None,
    );
    assert!(matches!(
        reasons(&compose_condition(&ambiguous))[0],
        ParkReason::BudgetExhausted {
            budget_kind: None,
            ..
        }
    ));
    let duplicate = input(
        Some(json!({"type":"workflow_loop"})),
        Some(json!({"source":"workflow_loop","reason":"loop"})),
        None,
        None,
        None,
    );
    assert_eq!(reasons(&compose_condition(&duplicate)).len(), 1);
    let ci = input(
        Some(
            json!({"type":"before_work_hook_failed","blocking_reason":"review_ci_infrastructure"}),
        ),
        None,
        None,
        Some(json!({"state":"review","status":"blocked"})),
        Some(
            json!({"deferred_dispatch":{"not_before":"2026-10-06T00:00:05Z","reason":"retry CI","target_state":"review"}}),
        ),
    );
    assert!(matches!(
        compose_condition(&ci),
        TaskCondition::Deferred {
            reason: RetryCause::ReviewCiInfrastructure,
            resume: ConditionContinuation::RetryEntry { .. },
            ..
        }
    ));
}

#[test]
fn every_condition_variant_round_trips_without_fabricated_owners() {
    let evidence = ConditionEvidence::default();
    let source = ConditionSource {
        field: ConditionField::EntryBarrierJson,
        key: None,
    };
    let primary = ParkReason::UnknownCondition {
        source,
        problem: UnknownConditionProblem::UnownedEntry,
    };
    let variants = vec![
        TaskCondition::Clear {
            evidence: evidence.clone(),
        },
        TaskCondition::Entering {
            state: "review".into(),
            epoch: 2,
            step_id: "step".into(),
            phase: "before_enter".into(),
            since: "now".into(),
            evidence: evidence.clone(),
        },
        TaskCondition::Running {
            execution_id: "execution".into(),
            role: "coder".into(),
            epoch: 2,
            since: "now".into(),
            evidence: evidence.clone(),
        },
        TaskCondition::Deferred {
            until: Some("later".into()),
            reason: RetryCause::ExecutionFailure,
            resume: ConditionContinuation::Dispatch {
                target_state: "working".into(),
            },
            evidence: evidence.clone(),
        },
        TaskCondition::Parked {
            primary: primary.clone(),
            additional: vec![],
            resume: ConditionContinuation::Reconcile,
            since: None,
            evidence: evidence.clone(),
        },
        TaskCondition::Failed {
            failure: primary,
            additional: vec![],
            resume: ConditionContinuation::Reconcile,
            since: None,
            evidence: evidence.clone(),
        },
        TaskCondition::Settled {
            outcome: TerminalOutcome::Completed,
            evidence,
        },
    ];
    for condition in variants {
        assert_eq!(
            decode(&serde_json::to_string(&condition).unwrap()).unwrap(),
            condition
        );
    }
}
