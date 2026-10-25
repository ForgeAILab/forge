//! Task action offer and condition eligibility cases.

fn projected_actions(snapshot: &super::TaskSnapshot) -> Vec<api_types::Offer> {
    let mut projected = snapshot.clone();
    projected.task.condition =
        db::map_legacy_condition(&db::LegacyConditionInput::from(&projected.task));
    super::available_actions(&projected)
}

use super::*;
use serde_json::json;

pub(crate) use super::test_support::snapshot;

#[test]
fn renamed_review_gate_keeps_default_review_action_offers() {
    for condition in [
        None,
        Some(FailureKind::ReviewNeedsOwner),
        Some(FailureKind::ReviewBudgetExhausted),
    ] {
        for status in [
            ReviewStatus::Running,
            ReviewStatus::AwaitingHuman,
            ReviewStatus::Failed,
            ReviewStatus::Passed,
            ReviewStatus::Cancelled,
        ] {
            let mut original = snapshot("review", condition);
            original.latest_review = Some(super::condition_matrix::review(status.clone()));
            original.executions = vec![super::condition_matrix::exec(
                "candidate",
                "coder",
                ExecutionStatus::Completed,
                false,
            )];
            let expected = projected_actions(&original);
            let mut renamed = original.clone();
            renamed.task.status = "qa_review".into();
            for state in &mut renamed.workflow.states {
                if state.name == "review" {
                    state.name = "qa_review".into();
                }
                for trigger in state.triggers.values_mut() {
                    if trigger.to == "review" {
                        trigger.to = "qa_review".into();
                    }
                }
            }
            assert_eq!(
                projected_actions(&renamed),
                expected,
                "condition={condition:?}, review={status:?}"
            );
        }
    }
}

#[test]
fn open_item_5_failure_without_a_coder_does_not_offer_an_inapplicable_repair_retry() {
    for state in ["in_progress", "merge_failed"] {
        let mut s = snapshot(state, Some(FailureKind::ExecutorFailed));
        s.task.assignee_id = None;
        s.task.assignee_type = None;
        s.has_agent = false;
        let offers = projected_actions(&s);
        assert!(
            !offers.iter().any(|offer| offer.action.verb() == "retry"),
            "{state}: {offers:?}"
        );
        assert!(offers.iter().any(|offer| offer.action.verb() == "cancel"));
    }
}

/// A merge fix with an Agent is offered while the Project is paused: the
/// accepted retry waits for the pause to end, as it did before.
#[test]
fn open_items_fix_merge_fix_retry_with_an_agent_stays_offered_during_a_pause() {
    let mut s = snapshot("merge_failed", Some(FailureKind::MergeConflict));
    s.project_paused = true;
    let offers = projected_actions(&s);
    assert!(
        offers.iter().any(|offer| offer.reason == "merge_fix_retry"),
        "{offers:?}"
    );
}

pub(crate) fn integration_conditions() -> Vec<db::TaskCondition> {
    use api_types::{
        IntegrationAttemptId, IntegrationDeferralCause, IntegrationPhase, IntegrationReason,
    };
    let id = || IntegrationAttemptId::new("attempt");
    let reasons = vec![
        IntegrationReason::Waiting { attempt_id: id() },
        IntegrationReason::Owned {
            attempt_id: id(),
            phase: IntegrationPhase::Checking,
        },
        IntegrationReason::Repair {
            attempt_id: id(),
            predecessor_attempt_id: None,
            conflict_paths: Some(api_types::IntegrationPaths::bounded(
                ["src/a.rs".to_owned()],
            )),
            repair_paths: api_types::IntegrationPaths::default(),
        },
        IntegrationReason::ReviewRequired {
            attempt_id: id(),
            authority_reason: "authority changed".into(),
        },
        IntegrationReason::CandidateCheckFailed {
            attempt_id: id(),
            check: "unit".into(),
            message: "exit 1".into(),
        },
        IntegrationReason::Deferred {
            attempt_id: id(),
            cause: IntegrationDeferralCause::Infrastructure,
            owner_id: Some("owner".into()),
            message: "transport down".into(),
        },
        IntegrationReason::Applied { attempt_id: id() },
        IntegrationReason::Deferred {
            attempt_id: id(),
            cause: IntegrationDeferralCause::TargetDirty,
            owner_id: Some("owner".into()),
            message: "target dirty".into(),
        },
    ];
    reasons
        .into_iter()
        .map(|reason| {
            db::ConditionFacts {
                task_id: "task".into(),
                state: "merging".into(),
                integration: Some(reason),
                ..Default::default()
            }
            .condition(&db::LegacyConditionInput::default())
        })
        .collect()
}

pub(crate) fn check_conditions() -> Vec<db::TaskCondition> {
    [
        api_types::CheckWaitPhase::Result,
        api_types::CheckWaitPhase::Slot,
        api_types::CheckWaitPhase::InfrastructureExhausted,
    ]
    .into_iter()
    .map(|phase| {
        db::ConditionFacts {
            task_id: "task".into(),
            state: "review".into(),
            check: Some((
                api_types::CheckWait {
                    phase,
                    consumer_id: "consumer".into(),
                    origin: "entry".into(),
                },
                0,
            )),
            ..Default::default()
        }
        .condition(&db::LegacyConditionInput::default())
    })
    .collect()
}

#[test]
fn check_waits_offer_cancel_and_only_exhausted_retries_offer_retry() {
    for state in ["review", "merging", "in_progress"] {
        for condition in check_conditions() {
            let exhausted = condition.check_wait().unwrap().requires_intervention();
            let mut snapshot = snapshot(state, None);
            snapshot.task.condition = condition.clone();
            // A dispatcher wait cause must not hide the owner's retry.
            for wait_cause in [None, Some(api_types::DeniedBy::IdentityPaused)] {
                snapshot.wait_cause = wait_cause;
                let offers = available_actions(&snapshot);
                let verbs: Vec<_> = offers.iter().map(|offer| offer.action.verb()).collect();
                assert_eq!(
                    verbs,
                    if exhausted {
                        vec!["cancel", "retry"]
                    } else {
                        vec!["cancel"]
                    },
                    "{state} {condition:?}"
                );
                if exhausted {
                    assert_eq!(offers[1].reason, "check_infrastructure_exhausted");
                    assert!(offers[1].target_execution_id.is_none());
                }
            }
            // Not presented as a failure of the candidate.
            assert!(condition.read().failure_kind.is_none());
            assert!(!condition.read().hard_failure);
            assert_eq!(condition.is_blocked(), exhausted);
        }
    }
}

#[test]
fn integration_waits_offer_hold_and_cancel_without_agent_recovery() {
    for condition in integration_conditions() {
        let mut snapshot = snapshot("merging", None);
        snapshot.task.condition = condition.clone();
        let offers = available_actions(&snapshot);
        let verbs: Vec<_> = offers.iter().map(|offer| offer.action.verb()).collect();
        // A stopped merge (`deferred`) also offers the Retry that puts
        // the Task back in line; a wait the worker owns does not.
        let deferred = matches!(
            condition.integration_reason(),
            Some(api_types::IntegrationReason::Deferred { .. })
        );
        assert_eq!(
            verbs,
            if deferred {
                vec!["cancel", "hold", "retry"]
            } else {
                vec!["cancel", "hold"]
            },
            "{condition:?}"
        );
        assert_eq!(
            condition.read().failure_kind.is_some(),
            condition
                .integration_reason()
                .unwrap()
                .requires_intervention()
        );
        // A user hold remains an ordinary releaseable primary, with
        // integration waiting as an independent secondary reason.
        let mut held = db::map_legacy_condition(&db::LegacyConditionInput {
            error_annotation: Some(
                json!({"type":"manual_stop","blocking_reason":"held","blocked_by":"user"})
                    .to_string(),
            ),
            ..Default::default()
        });
        if let db::TaskCondition::Parked { additional, .. } = &mut held {
            additional.push(db::ParkReason::Integration {
                reason: condition.integration_reason().unwrap().clone(),
            });
        }
        snapshot.task.condition = held;
        let offers = available_actions(&snapshot);
        assert!(offers.iter().any(|offer| offer.action.verb() == "release"));
        assert!(!offers.iter().any(|offer| offer.action.verb() == "retry"));
        // Integration is secondary to an entry blocker, whose offers
        // remain exactly the same as without integration ownership.
        let input = db::LegacyConditionInput {
            entry_barrier_json: Some(
                json!({"state":"merging","status":"blocked","blocking_reason":"other owner"})
                    .to_string(),
            ),
            ..Default::default()
        };
        snapshot.task.condition = db::map_legacy_condition(&input);
        let expected = available_actions(&snapshot);
        snapshot.task.condition = condition.restate_legacy(&input);
        assert_eq!(available_actions(&snapshot), expected);
    }
}

#[test]
fn a_root_repair_park_offers_corrective_work_at_either_gate() {
    for state in ["review", "merging"] {
        let mut snapshot = snapshot(state, None);
        snapshot.coordination_root = true;
        snapshot.task.condition = integration_conditions()
            .into_iter()
            .find(|condition| {
                matches!(
                    condition.integration_reason(),
                    Some(api_types::IntegrationReason::Repair { .. })
                )
            })
            .unwrap();
        let offers = available_actions(&snapshot);
        let reopen = offers
            .iter()
            .find(|offer| offer.reason == "root_review_reopen")
            .unwrap();
        assert_eq!(reopen.action.verb(), "send_back");
        assert_eq!(reopen.authority, vec![api_types::ActionAuthority::Owner]);
        assert!(offers.iter().all(|offer| offer.action.verb() != "retry"));
    }
}

#[test]
fn review_condition_wins_over_generic_decision_for_owner_and_project_agent() {
    for caller in [
        ActionCaller::owner(),
        ActionCaller {
            project_agent: true,
            ..Default::default()
        },
    ] {
        let mut snapshot = snapshot("review", Some(FailureKind::RecoveryRequired));
        snapshot.caller = caller;
        snapshot.executions.push(super::condition_matrix::exec(
            "candidate",
            "coder",
            ExecutionStatus::Completed,
            false,
        ));
        snapshot.task.metadata_json = Some(json!({"awaiting_human":true}).to_string());
        let offers = projected_actions(&snapshot);
        assert!(!offers.iter().any(|offer| matches!(
            offer.reason.as_str(),
            "gate_waiting_for_decision" | "human_review_decision"
        )));
        assert!(offers.iter().any(|offer| offer.action.verb() == "retry"));
        assert!(offers.iter().any(|offer| offer.action.verb() == "restart"));
    }
}

#[test]
fn a_spent_step_gate_offers_gate_budget_recovery_instead_of_execution_recovery() {
    let mut snapshot = snapshot("lead_check", Some(FailureKind::RetryExhausted));
    snapshot.workflow = crate::workflow::inherited_subtask_workflow::inherited_subtask_workflow();
    snapshot.task.parent_task_id = Some("root".to_owned());
    for (spent, reason) in [
        (0, "execution_retry_exhausted"),
        (2, "retry_budget_exhausted"),
    ] {
        snapshot
            .budget_spent
            .insert(db::budget::gate_key("lead_check"), spent);
        let offers = projected_actions(&snapshot);
        assert_eq!(
            offers
                .iter()
                .find(|offer| offer.action.verb() == "retry")
                .unwrap()
                .reason,
            reason
        );
    }
}

#[test]
fn full_human_review_window_offers_only_budget_reset_retry() {
    let mut snapshot = snapshot("review", None);
    let gate = snapshot
        .workflow
        .states
        .iter_mut()
        .find(|state| state.name == "review")
        .unwrap()
        .gate_config
        .as_mut()
        .unwrap();
    gate.requires_user_approval = Some(true);
    gate.max_rejections = Some(1);
    snapshot.transition_logs = vec![super::condition_matrix::rejection(
        0,
        "review",
        "in_progress",
    )];
    snapshot.budget_spent.insert("review".into(), 1);
    snapshot.latest_review = Some(super::condition_matrix::review(ReviewStatus::AwaitingHuman));
    snapshot.executions = vec![super::condition_matrix::exec(
        "candidate",
        "coder",
        ExecutionStatus::Completed,
        false,
    )];
    let offers = projected_actions(&snapshot);
    let retries = offers
        .iter()
        .filter(|offer| offer.action.verb() == "retry")
        .collect::<Vec<_>>();
    assert_eq!(retries.len(), 1);
    assert_eq!(retries[0].reason, "retry_budget_exhausted");
    assert_eq!(
        retries[0]
            .parameters
            .iter()
            .find(|parameter| parameter.name == "reset_budget")
            .unwrap()
            .boolean_values,
        Some(vec![true])
    );
    assert!(retries[0]
        .parameters
        .iter()
        .all(|parameter| parameter.required_when.is_none()));
}

#[test]
fn implicit_human_review_gate_is_decidable_but_running_or_passed_review_is_not() {
    let mut snapshot = snapshot("review", None);
    snapshot
        .workflow
        .states
        .iter_mut()
        .find(|state| state.name == "review")
        .unwrap()
        .gate_config
        .as_mut()
        .unwrap()
        .requires_user_approval = Some(true);
    assert!(projected_actions(&snapshot)
        .iter()
        .any(|offer| offer.reason == "gate_waiting_for_decision"));
    snapshot.task.metadata_json = Some(json!({"awaiting_human":true}).to_string());
    for status in [ReviewStatus::Running, ReviewStatus::Passed] {
        snapshot.latest_review = Some(super::condition_matrix::review(status));
        assert!(!projected_actions(&snapshot).iter().any(|offer| matches!(
            offer.reason.as_str(),
            "human_review_decision" | "gate_waiting_for_decision"
        )));
    }
}

#[test]
fn dependency_cancelled_writer_shape_offers_only_cancel() {
    let mut snapshot = snapshot("in_progress", Some(FailureKind::WorkflowGuardRejected));
    snapshot.task.error_annotation = Some(
        json!({"type":"workflow_guard_rejected", "blocking_reason":"dependency_cancelled"})
            .to_string(),
    );
    assert_eq!(
        projected_actions(&snapshot)
            .iter()
            .map(|offer| offer.action.verb())
            .collect::<Vec<_>>(),
        vec!["cancel"]
    );
}

#[test]
fn queued_retry_with_paused_agent_and_new_dependency_blocker_still_offers_hold() {
    let mut snapshot = snapshot("in_progress", Some(FailureKind::WorkflowGuardRejected));
    snapshot.has_agent = false;
    snapshot.wait_cause = Some(api_types::DeniedBy::TargetAgentPaused);
    snapshot.task.error_annotation = Some(
        json!({"type":"workflow_guard_rejected", "blocking_reason":"dependency_cancelled"})
            .to_string(),
    );
    snapshot.task.metadata_json = Some(
        json!({crate::deferred_dispatch::QUEUED_RECOVERY_KEY:{"action":{"verb":"retry"}}})
            .to_string(),
    );
    let offers = projected_actions(&snapshot);
    assert!(offers
        .iter()
        .any(|offer| offer.reason == "dispatch_wait" && offer.action.verb() == "hold"));
    assert!(offers.iter().any(|offer| offer.action.verb() == "cancel"));
}

#[test]
fn paused_review_keeps_budget_reset_but_withholds_hold_and_one_shot_retry() {
    let mut snapshot = snapshot("review", Some(FailureKind::ReviewBudgetExhausted));
    snapshot.project_paused = true;
    snapshot.wait_cause = Some(api_types::DeniedBy::ProjectPaused(
        "environment_not_ready".to_owned(),
    ));
    let offers = projected_actions(&snapshot);
    // A Hold would overwrite the exhausted-budget condition; nothing is
    // waiting to be dispatched.
    assert!(!offers.iter().any(|offer| offer.action.verb() == "hold"));
    let reset = offers
        .iter()
        .find(|offer| offer.reason == "retry_budget_exhausted")
        .unwrap();
    assert_eq!(
        reset
            .parameters
            .iter()
            .find(|parameter| parameter.name == "reset_budget")
            .unwrap()
            .boolean_values,
        Some(vec![true])
    );
}

#[test]
fn review_retry_needs_a_completed_implementation_candidate() {
    let mut snapshot = snapshot("review", None);
    assert!(!projected_actions(&snapshot)
        .iter()
        .any(|offer| offer.action.verb() == "retry"));
    for role in ["planner", "reviewer", "auditor", "interactive"] {
        snapshot.executions = vec![super::condition_matrix::exec(
            "not-candidate",
            role,
            ExecutionStatus::Completed,
            false,
        )];
        assert!(
            !projected_actions(&snapshot)
                .iter()
                .any(|offer| offer.action.verb() == "retry"),
            "{role}"
        );
    }
    snapshot.executions = vec![super::condition_matrix::exec(
        "candidate",
        "coder",
        ExecutionStatus::Completed,
        false,
    )];
    assert!(projected_actions(&snapshot)
        .iter()
        .any(|offer| offer.reason == "review_checks_retry"));
}

#[test]
fn general_condition_restart_matches_reset_kinds() {
    for kind in [
        FailureKind::WorkflowGuardRejected,
        FailureKind::CiFailed,
        FailureKind::InternalCommandFailed,
    ] {
        assert!(
            !projected_actions(&snapshot("in_progress", Some(kind)))
                .iter()
                .any(|offer| offer.action.verb() == "restart"),
            "{kind:?}"
        );
    }
}

#[test]
fn every_state_condition_has_a_closed_deterministic_offer_set() {
    let states = [
        "backlog",
        "todo",
        "planning",
        "in_progress",
        "review",
        "merging",
        "merge_failed",
        "done",
        "cancelled",
    ];
    let conditions = [
        None,
        Some(FailureKind::ExecutorFailed),
        Some(FailureKind::RetryExhausted),
        Some(FailureKind::ReviewBudgetExhausted),
        Some(FailureKind::MergeFixBudgetExhausted),
        Some(FailureKind::MergeConflict),
        Some(FailureKind::TargetRepoDirty),
        Some(FailureKind::DirtyWorktree),
        Some(FailureKind::BeforeWorkHookFailed),
        Some(FailureKind::BeforeWorkHookTimeout),
        Some(FailureKind::WorkspaceResetRequired),
        Some(FailureKind::WorkspaceFailed),
        Some(FailureKind::WorkspaceError),
        Some(FailureKind::WorkflowGuardRejected),
        Some(FailureKind::ReviewBlocked),
        Some(FailureKind::ReviewNeedsOwner),
        Some(FailureKind::MaxTurnsExceeded),
        Some(FailureKind::ManualStop),
        Some(FailureKind::RecoveryRequired),
        Some(FailureKind::ExecutorUnavailable),
        Some(FailureKind::Unknown),
    ];
    for state in states {
        for condition in conditions {
            let snapshot = snapshot(state, condition);
            let offers = projected_actions(&snapshot);
            assert_eq!(offers, projected_actions(&snapshot));
            let mut verbs = std::collections::HashSet::new();
            for offer in offers {
                assert!(
                    verbs.insert(offer.action.verb()),
                    "duplicate verb in {state}/{condition:?}"
                );
                assert!(snapshot.caller.permits(&offer.authority));
                assert!(!offer.reason.is_empty());
                assert!(!offer.label.is_empty());
                assert!(matches!(
                    offer.action.verb(),
                    "start"
                        | "hold"
                        | "release"
                        | "retry"
                        | "send_back"
                        | "approve"
                        | "restart"
                        | "cancel"
                ));
            }
        }
    }
}

#[test]
fn old_stored_lists_are_ignored_including_non_enum_strings() {
    let mut original = snapshot("in_progress", Some(FailureKind::ExecutorFailed));
    let expected = projected_actions(&original);
    let mut annotation: serde_json::Value =
        serde_json::from_str(original.task.error_annotation.as_deref().unwrap()).unwrap();
    annotation["recovery_actions"] = json!([
        "return_to_implementation",
        "retry_pr_publication",
        "resume_session"
    ]);
    let parsed: api_types::TaskBlockingAnnotation = serde_json::from_value(annotation.clone())
        .expect("legacy lists never affect deserialization");
    assert!(serde_json::to_value(parsed)
        .unwrap()
        .get("recovery_actions")
        .is_none());
    original.task.error_annotation = Some(annotation.to_string());
    assert_eq!(expected, projected_actions(&original));
}

#[test]
fn authority_filters_every_verb_before_advertising() {
    for state in ["todo", "in_progress", "review", "merge_failed"] {
        for kind in [
            None,
            Some(FailureKind::ExecutorFailed),
            Some(FailureKind::ReviewNeedsOwner),
            Some(FailureKind::RetryExhausted),
            Some(FailureKind::ManualStop),
        ] {
            let mut snapshot = snapshot(state, kind);
            snapshot.caller = ActionCaller::default();
            assert!(projected_actions(&snapshot).is_empty());
            snapshot.caller = ActionCaller {
                project_agent: true,
                ..ActionCaller::default()
            };
            for offer in projected_actions(&snapshot) {
                assert!(!matches!(
                    offer.action,
                    TaskAction::Approve {
                        reason: None,
                        override_checks: Some(true)
                    }
                ));
            }
        }
    }
}

#[test]
fn entry_barrier_and_flow_parks_offer_one_apply_reason_per_verb() {
    let mut snapshot = snapshot("review", Some(FailureKind::BeforeWorkHookFailed));
    snapshot.task.entry_barrier_json =
        Some(json!({"status":"blocked","state":"review"}).to_string());
    let offers = projected_actions(&snapshot);
    assert!(offers
        .iter()
        .any(|offer| offer.reason == "entry_barrier_blocked"));
    assert!(offers
        .iter()
        .any(|offer| offer.reason == "entry_barrier_override"));
    snapshot.task.entry_barrier_json = None;
    snapshot.entry_hooks_running = true;
    assert_eq!(
        projected_actions(&snapshot).len(),
        1,
        "only cancellation while entry checks run"
    );
    snapshot.entry_hooks_running = false;
    snapshot.task.error_annotation =
        Some(json!({"type":"review_needs_owner","blocking_reason":"finding"}).to_string());
    snapshot.executions.push(super::condition_matrix::exec(
        "candidate",
        "coder",
        ExecutionStatus::Completed,
        false,
    ));
    snapshot.latest_review = Some(super::condition_matrix::review(ReviewStatus::Failed));
    let offers = projected_actions(&snapshot);
    assert_eq!(
        offers
            .iter()
            .filter(|offer| offer.action.verb() == "approve")
            .count(),
        1
    );
    assert!(offers
        .iter()
        .any(|offer| offer.reason == "review_needs_owner"));
}

#[test]
fn workflow_loop_keeps_failure_kind_and_offers() {
    let mut old = snapshot("todo", Some(FailureKind::WorkflowLoop));
    old.task.blocked_json = Some(
        r#"{"reason":"loop","blocked_at":"2026-10-01T00:00:00Z","source":"workflow_loop"}"#.into(),
    );
    old.task.condition = db::map_legacy_condition(&db::LegacyConditionInput::from(&old.task));
    let offers = available_actions(&old);
    assert_eq!(
        offers,
        legacy::available_actions(&legacy::TaskSnapshot::from(&old))
    );
    assert_eq!(old.condition(), Some(FailureKind::WorkflowLoop));
    assert!(offers
        .iter()
        .any(|offer| matches!(offer.action, TaskAction::Restart { .. })));
}
