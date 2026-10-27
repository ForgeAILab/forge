use super::super::*;
use serde_json::json;

const BLOCKED: &str = r#"{"reason":"Automatic workflow loop detected","blocked_at":"2026-10-01T00:00:00Z","source":"workflow_loop"}"#;

#[test]
fn pre_upgrade_workflow_loop_read_preserves_typed_condition_and_material() {
    let condition = compose_condition(&ConditionInput {
        blocked_json: Some(BLOCKED.into()),
        ..Default::default()
    });
    let stored = serde_json::to_string(&condition).unwrap();
    let typed = condition.typed();
    assert!(condition
        .evidence()
        .presentation
        .as_ref()
        .unwrap()
        .interruption
        .is_none());
    let material = material_blocker(&condition);
    assert_eq!(
        serde_json::to_value(&material).unwrap(),
        json!({
            "requires_intervention": true,
            "interruption": {"source":"blocked", "kind":null, "reason":"Automatic workflow loop detected"}
        })
    );
    let public = serde_json::to_value(condition.public()).unwrap();
    assert_eq!(
        public["details"]["interruption"],
        json!({
            "reason":"Automatic workflow loop detected",
            "created_at":"2026-10-01T00:00:00Z",
            "kind":"workflow_loop",
            "source":"workflow_loop"
        })
    );
    assert_eq!(public["details"]["failure_kind"], "workflow_loop");
    assert_eq!(public["details"]["blocked"], true);
    assert_eq!(public["since"], serde_json::Value::Null);
    assert_eq!(condition.typed(), typed);
    assert_eq!(material_blocker(&condition), material);
    assert_eq!(serde_json::to_string(&condition).unwrap(), stored);
}

#[test]
fn pre_upgrade_loop_reader_keeps_failure_precedence_and_rejects_unrelated_blocks() {
    let failed =
        json!({"reason":"failed", "created_at":"2026-10-02T00:00:00Z", "kind":"executor_failed"});
    let condition = compose_condition(&ConditionInput {
        blocked_json: Some(BLOCKED.into()),
        failed_json: Some(failed.to_string()),
        ..Default::default()
    });
    assert_eq!(condition.read().interruption.unwrap().reason, "failed");
    for blocked in [
        r#"{"reason":"other","blocked_at":"2026-10-01T00:00:00Z","source":"other"}"#,
        r#"{"reason":"loop","source":"workflow_loop"}"#,
        r#"{"reason":"loop","blocked_at":"2026-10-01T00:00:00Z","created_at":null,"source":"workflow_loop"}"#,
    ] {
        let condition = compose_condition(&ConditionInput {
            blocked_json: Some(blocked.into()),
            ..Default::default()
        });
        assert!(
            serde_json::to_value(condition.public()).unwrap()["details"]["interruption"].is_null(),
            "{blocked}"
        );
    }
}
