//! Legacy and typed condition reader parity cases.

use super::*;
use serde_json::{json, Value};
#[test]
fn legacy_conditions_keep_parity_and_integration_waits_have_owned_offers() {
    let conditions = [
        ("clear", None, None, None, None),
        ("entering", None, None, None, None),
        ("running", None, None, None, None),
        (
            "deferred",
            None,
            None,
            None,
            Some(
                json!({"deferred_dispatch":{"not_before":"2099-01-01T00:00:00Z","reason":"retry","target_state":"in_progress"}}),
            ),
        ),
        (
            "parked",
            Some(
                json!({"type":"manual_stop","blocking_reason":"held","blocked_by":"user","blocked_at":null,"blocked_execution_id":null,"artifact":null,"message":"held"}),
            ),
            None,
            None,
            None,
        ),
        (
            "failed",
            None,
            None,
            Some(
                json!({"kind":"executor_failed","reason":"failure","created_at":"2026-10-02T00:00:00Z"}),
            ),
            None,
        ),
        ("settled", None, None, None, None),
        ("integration_wait", None, None, None, None),
        ("integration_owner", None, None, None, None),
    ];
    for (kind, a, b, f, m) in conditions {
        let mut s = super::tests::snapshot("in_progress", None);
        s.task.error_annotation = a.map(|v| v.to_string());
        s.task.blocked_json = b.map(|v: Value| v.to_string());
        s.task.failed_json = f.map(|v| v.to_string());
        s.task.metadata_json = m.map(|v| v.to_string());
        let mut condition = db::compose_condition(&db::ConditionInput::from(&s.task));
        if kind == "entering" {
            condition = db::TaskCondition::Entering {
                state: s.task.status.clone(),
                epoch: 1,
                step_id: "step".into(),
                phase: "post_commit_hooks".into(),
                since: s.task.created_at.clone(),
                evidence: condition.evidence().clone(),
            };
        }
        if kind == "running" {
            condition = db::TaskCondition::Running {
                execution_id: "run".into(),
                role: "coder".into(),
                epoch: 1,
                since: s.task.created_at.clone(),
                evidence: condition.evidence().clone(),
            };
        }
        if kind == "settled" {
            condition = db::TaskCondition::Settled {
                outcome: db::TerminalOutcome::Completed,
                evidence: condition.evidence().clone(),
            };
        }
        if kind == "integration_wait" {
            condition = super::tests::integration_conditions().remove(0);
        }
        if kind == "integration_owner" {
            condition = super::tests::integration_conditions().pop().unwrap();
        }
        s.task.condition = condition;
        if s.task.condition.integration_reason().is_none() {
            assert_eq!(
                serde_json::to_value(available_actions(&s)).unwrap(),
                serde_json::to_value(legacy::available_actions(&legacy::TaskSnapshot::from(&s)))
                    .unwrap(),
                "{kind}"
            );
            assert_eq!(s.condition(), legacy_task_condition(&s.task), "{kind}");
        } else {
            // Legacy fields cannot express direct integration ownership.
            // Retain the old parity assertions for every legacy fixture,
            // and assert the owner contract for these two new fixtures.
            let offers = available_actions(&s);
            assert_eq!(
                offers
                    .iter()
                    .map(|offer| offer.action.verb())
                    .collect::<Vec<_>>(),
                if kind == "integration_owner" {
                    // A stopped merge offers the Retry that puts the
                    // Task back in line.
                    vec!["cancel", "hold", "retry"]
                } else {
                    vec!["cancel", "hold"]
                }
            );
            assert_eq!(s.condition().is_some(), kind == "integration_owner");
        }
        let expected = available_actions(&s);
        s.task.error_annotation =
            Some(json!({"type":"workspace_error","blocking_reason":"poison"}).to_string());
        s.task.blocked_json = Some("{}".into());
        s.task.failed_json = Some("{}".into());
        s.task.metadata_json = None;
        assert_eq!(
            serde_json::to_value(available_actions(&s)).unwrap(),
            serde_json::to_value(expected).unwrap(),
            "{kind}: legacy columns are not reader inputs"
        );
    }
}
