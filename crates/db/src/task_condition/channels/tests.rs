use super::*;

const RESET: &str = r#"{"blocked_at":"at","blocked_by":"system:heartbeat_monitor","blocking_reason":"owner_workspace_reset_required","message":"reset","type":"workspace_reset_required"}"#;
const EXECUTOR: &str = r#"{"type":"executor_failed","blocking_reason":"executor_failed","blocked_by":"system:executor","blocked_at":"at","blocked_execution_id":"execution","artifact":{"kind":"execution","id":"execution","log_path":"log"},"message":"failed"}"#;

#[test]
fn strict_channels_keep_main_bytes_and_reject_unknown_fields_and_tags() {
    for raw in [RESET, EXECUTOR] {
        let annotation: ErrorAnnotation = serde_json::from_str(raw).unwrap();
        assert_eq!(serde_json::to_string(&annotation).unwrap(), raw);
        let mut fields: serde_json::Value = serde_json::from_str(raw).unwrap();
        fields["future"] = true.into();
        assert!(serde_json::from_value::<ErrorAnnotation>(fields).is_err());
        let overlay = StepFailureOverlay {
            task_step_error: None,
            task_step_id: "step".into(),
        };
        let merged = annotation.with_step_failure(overlay.clone()).unwrap();
        // Independent literal port of main's queue merge, including null errors.
        let mut main: serde_json::Value = serde_json::from_str(raw).unwrap();
        main["task_step_id"] = "step".into();
        main["task_step_error"] = serde_json::Value::Null;
        assert_eq!(merged, main.to_string());
        let decoded: ErrorAnnotation = serde_json::from_str(&merged).unwrap();
        assert_eq!(
            decoded,
            ErrorAnnotation::WithStepFailure {
                annotation: Box::new(annotation),
                overlay
            }
        );
        assert_eq!(serde_json::to_string(&decoded).unwrap(), merged);
    }
    for raw in [
        r#"{"type":"future"}"#,
        r#"{"type":"workspace_reset_required","message":"missing required fields"}"#,
    ] {
        assert!(serde_json::from_str::<ErrorAnnotation>(raw).is_err());
    }
    let raw = r#"{"created_at":"at","execution_id":"execution","kind":"internal_command_failed","reason":"failed"}"#;
    let interruption: Interruption = serde_json::from_str(raw).unwrap();
    assert_eq!(serde_json::to_string(&interruption).unwrap(), raw);
    for raw in [
        r#"{"reason":"failure","kind":"future"}"#,
        r#"{"reason":"failure","details":{"future":true}}"#,
        r#"{"reason":false}"#,
    ] {
        assert!(serde_json::from_str::<Interruption>(raw).is_err());
    }
}

#[test]
fn kindless_loop_and_typed_superseded_channels_roundtrip() {
    for raw in [
        r#"{"blocked_at":"at","reason":"loop","source":"workflow_loop"}"#,
        r#"{"created_at":null,"details":{"retry_at":null,"superseded":null},"execution_id":null,"kind":null,"reason":"null fields"}"#,
        r#"{"details":{"assignment_guidance":"assign coder","assignment_required":true,"superseded":{"blocked_json":"historical","error_annotation":null,"failed_json":null,"status":"in_progress"}},"reason":"dependency"}"#,
    ] {
        let interruption: Interruption = serde_json::from_str(raw).unwrap();
        assert_eq!(serde_json::to_string(&interruption).unwrap(), raw);
    }
}
