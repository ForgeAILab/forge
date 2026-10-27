//! Tests for producer shapes in task conditions.

use super::*;
/// Port of `audit_mapping_shapes`: what real producer shapes map to, now as
/// assertions. M1: nothing the dispatcher dispatches through is parked.
#[test]
fn real_producer_shapes_map_to_what_legacy_blocks_on() {
    let now = (chrono::Utc::now() + chrono::Duration::seconds(30)).to_rfc3339();
    let meta = |value: Value| input(None, None, None, None, Some(value));
    let ann = |value: Value| input(Some(value), None, None, None, None);
    let cases: Vec<(&str, ConditionInput, &str, Option<&str>)> = vec![
        (
            "deferred real rfc3339",
            meta(
                json!({"deferred_dispatch":{"not_before":now,"reason":"board drag dispatch cooldown","target_state":"in_progress"}}),
            ),
            "deferred",
            None,
        ),
        (
            "deferred Z micro",
            meta(
                json!({"deferred_dispatch":{"not_before":"2026-10-05T12:00:00.123456Z","reason":"r","target_state":"todo"}}),
            ),
            "deferred",
            None,
        ),
        (
            "deferred offset -07:00",
            meta(
                json!({"deferred_dispatch":{"not_before":"2026-10-05T12:00:00-07:00","reason":"r","target_state":"todo"}}),
            ),
            "deferred",
            None,
        ),
        (
            "env probe deferral (kind+reason only)",
            meta(
                json!({"environment_wait":{"kind":"environment_probe_pending","machine":{"owner_kind":"server"}},"deferred_dispatch":{"kind":"environment_probe_pending","reason":"environment_probe_pending: server"}}),
            ),
            "parked",
            Some("environment"),
        ),
        (
            "env deferral reason only",
            meta(json!({"deferred_dispatch":{"reason":"environment_probe_pending: server"}})),
            "clear",
            None,
        ),
        (
            "review CI infra co-write",
            input(
                Some(
                    json!({"type":"before_work_hook_failed","blocking_reason":"review_ci_infrastructure","blocked_at":now,"blocked_by":"system:workflow","message":"x"}),
                ),
                None,
                None,
                Some(json!({"state":"review","status":"blocked","blocking_reason":"x"})),
                Some(
                    json!({"deferred_dispatch":{"not_before":now,"reason":"x","target_state":"review"}}),
                ),
            ),
            "deferred",
            None,
        ),
        (
            "deferred + last_execution_failure",
            meta(
                json!({"last_execution_failure_at":now,"last_execution_failure_execution_id":"e","deferred_dispatch":{"not_before":now,"reason":"retry","target_state":"in_progress"}}),
            ),
            "deferred",
            None,
        ),
        (
            "deferred + capacity disposition",
            meta(
                json!({"dispatch_disposition":{"capability":"project_capacity"},"deferred_dispatch":{"not_before":now,"reason":"retry","target_state":"in_progress"}}),
            ),
            "parked",
            Some("capacity"),
        ),
        (
            "ann pull_request_merge",
            ann(json!({"type":"pull_request_merge"})),
            "clear",
            None,
        ),
        (
            "ann merge_fix_ci_failed",
            ann(json!({"type":"merge_fix_ci_failed","message":"m"})),
            "clear",
            None,
        ),
        (
            "ann merge_conflict",
            ann(json!({"type":"merge_conflict"})),
            "clear",
            None,
        ),
        (
            "ann ci_failed",
            ann(json!({"type":"ci_failed"})),
            "clear",
            None,
        ),
        (
            "ann executor_failed",
            ann(json!({"type":"executor_failed"})),
            "clear",
            None,
        ),
        (
            "ann retry_exhausted",
            ann(json!({"type":"retry_exhausted"})),
            "clear",
            None,
        ),
        (
            "ann agent_timeout",
            ann(json!({"type":"agent_timeout"})),
            "parked",
            Some("agent_timeout"),
        ),
        ("ann no type", ann(json!({"message":"only"})), "clear", None),
        ("ann empty object", ann(json!({})), "clear", None),
        (
            "ann kind-not-type",
            ann(json!({"kind":"ci_failed"})),
            "clear",
            None,
        ),
        (
            "ann annotation_type",
            ann(json!({"annotation_type":"ci_failed"})),
            "clear",
            None,
        ),
        (
            "stale nonblocking ann",
            ann(json!({"type":"merge_conflict","blocking":false})),
            "clear",
            None,
        ),
        (
            "blocked empty object",
            input(None, Some(json!({})), None, None, None),
            "parked",
            Some("unknown_condition"),
        ),
        (
            "blocked no kind",
            input(
                None,
                Some(json!({"blocked_by":"user","reason":"x"})),
                None,
                None,
                None,
            ),
            "parked",
            Some("unknown_condition"),
        ),
        (
            "blocked agent_timeout",
            input(
                None,
                Some(json!({"kind":"agent_timeout"})),
                None,
                None,
                None,
            ),
            "parked",
            Some("agent_timeout"),
        ),
        (
            "barrier passed",
            input(
                None,
                None,
                None,
                Some(json!({"state":"review","status":"passed"})),
                None,
            ),
            "parked",
            Some("unknown_condition"),
        ),
        (
            "barrier no status",
            input(None, None, None, Some(json!({"state":"review"})), None),
            "parked",
            Some("unknown_condition"),
        ),
        (
            "barrier running",
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
            "awaiting_human string true",
            meta(json!({"awaiting_human":"true"})),
            "clear",
            Some("unknown_condition"),
        ),
        (
            "awaiting_human false + reason",
            meta(json!({"awaiting_human":false,"awaiting_human_reason":"plan_review"})),
            "clear",
            None,
        ),
        (
            "awaiting_human pull_request_merge",
            meta(json!({"awaiting_human":true,"awaiting_human_reason":"pull_request_merge"})),
            "parked",
            Some("human_decision"),
        ),
        // m5: stays Clear until stage 2 adds the child witness.
        (
            "coordination_review_pending",
            meta(json!({"coordination_review_pending":true,"coordination_review_pending_id":"s"})),
            "clear",
            None,
        ),
        (
            "queued_recovery only",
            meta(json!({"queued_recovery":{"id":"q"}})),
            "clear",
            None,
        ),
        (
            "disposition safe_message only",
            meta(json!({"dispatch_disposition":{"safe_message":"denied"}})),
            "parked",
            Some("dispatch_refusal"),
        ),
        (
            "disposition null",
            meta(json!({"dispatch_disposition":null,"owner_wait":null})),
            "clear",
            None,
        ),
        (
            "owner_wait + deferred + plan wait",
            meta(
                json!({"owner_wait":{"daemon_id":"d","started_at":now},"plan_settlement_wait":{"execution_id":"e"},"deferred_dispatch":{"not_before":now,"reason":"r","target_state":"planning"}}),
            ),
            "parked",
            Some("owner_offline"),
        ),
        (
            "plan wait + deferred",
            meta(
                json!({"plan_settlement_wait":{"execution_id":"e"},"deferred_dispatch":{"not_before":now,"reason":"r","target_state":"planning"}}),
            ),
            "deferred",
            None,
        ),
        (
            "failed + manual_stop block",
            input(
                Some(json!({"type":"retry_exhausted"})),
                Some(json!({"kind":"manual_stop"})),
                Some(json!({"kind":"executor_failed"})),
                None,
                None,
            ),
            "failed",
            Some("failure"),
        ),
    ];
    for (name, legacy, expected, primary) in cases {
        let condition = compose_condition(&legacy);
        let value = serde_json::to_value(&condition).unwrap();
        assert_eq!(value["kind"], expected, "{name}: {value}");
        assert_eq!(
            condition.is_blocked(),
            matches!(expected, "parked" | "failed"),
            "{name}"
        );
        if let Some(kind) = primary {
            assert_eq!(
                serde_json::to_value(reasons(&condition)[0]).unwrap()["kind"],
                kind,
                "{name}: {value}"
            );
        }
    }
    assert!(matches!(
        compose_condition(&meta(
            json!({"last_execution_failure_execution_id":"e","deferred_dispatch":{"not_before":now,"reason":"retry","target_state":"in_progress"}})
        )),
        TaskCondition::Deferred {
            reason: RetryCause::ExecutionFailure,
            resume: ConditionContinuation::Dispatch { .. },
            ..
        }
    ));
    assert!(matches!(
        compose_condition(&meta(
            json!({"plan_settlement_wait":{"execution_id":"e"},"deferred_dispatch":{"not_before":now,"reason":"r","target_state":"planning"}})
        )),
        TaskCondition::Deferred {
            reason: RetryCause::PlanTransport,
            resume: ConditionContinuation::SettlePlan { .. },
            ..
        }
    ));
}

/// M1: an annotation contributes a park reason exactly when the dispatcher's
/// predicate blocks on it, for every FailureKind name and hostile text. The
/// services crate ties the same list to `has_blocking_annotation`.
#[test]
fn annotation_parks_exactly_where_the_dispatcher_blocks() {
    for (name, value) in hostile() {
        let Some(raw) = value else { continue };
        let blocks = serde_json::from_str::<Value>(&raw)
            .ok()
            .and_then(|annotation| annotation.get("type")?.as_str().map(str::to_owned))
            .is_some_and(|kind| BLOCKING_ANNOTATION_KINDS.contains(&kind.as_str()));
        assert_eq!(annotation_blocks_dispatch(Some(&raw)), blocks, "{name}");
        let condition = compose_condition(&ConditionInput {
            error_annotation: Some(raw),
            ..Default::default()
        });
        assert_eq!(condition.is_blocked(), blocks, "{name}: {condition:?}");
        if !blocks {
            assert!(matches!(condition, TaskCondition::Clear { .. }), "{name}");
        }
    }
    for kind in BLOCKING_ANNOTATION_KINDS {
        let condition =
            compose_condition(&input(Some(json!({"type":kind})), None, None, None, None));
        let primary = serde_json::to_value(reasons(&condition)[0]).unwrap();
        assert_ne!(
            primary["kind"], "unknown_condition",
            "{kind} has a typed mapping"
        );
    }
    assert!(!annotation_blocks_dispatch(None));
}
#[test]
fn a_queued_command_keeps_its_continuation_under_a_planning_parent_wait() {
    let mut input = super::ConditionInput {
        metadata_json: Some(serde_json::json!({
            "deferred_dispatch": {"not_before":"2026-10-13T05:00:00Z", "target_state":"lead_check", "reason":"task action queued"},
            "queued_recovery": {"id":"retry"},
        }).to_string()),
        ..Default::default()
    };
    let deferred = super::compose_condition(&input);
    assert!(matches!(&deferred, super::TaskCondition::Deferred { .. }));
    let wait =
        serde_json::json!({"reason":{"ParentWait":{"parent_id":"root", "cause":"planning"}}});
    let parked = super::readers::apply_owner_park(deferred, Some(&wait));
    assert!(matches!(parked,
        super::TaskCondition::Parked {
            primary: super::ParkReason::Parent { parent_id, cause },
            resume: super::ConditionContinuation::ResumeQueuedCommand { intent_id: Some(intent) },
            ..
        } if parent_id == "root" && cause == "planning" && intent == "retry"));
    input.metadata_json = Some(
        serde_json::json!({
            "deferred_dispatch": {"not_before":"2026-10-13T05:00:00Z", "target_state":"lead_check", "reason":"retry deadline"},
        })
        .to_string(),
    );
    assert!(
        matches!(
            super::readers::apply_owner_park(super::compose_condition(&input), Some(&wait)),
            super::TaskCondition::Deferred { .. }
        ),
        "a timer without an accepted command keeps its own continuation"
    );
}
