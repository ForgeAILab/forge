use crate::worker_runtime::queue::writer_parity::case;

#[test]
fn restored_shapes_use_typed_path_and_match_main_bytes() {
    let reason = crate::ServiceError::invalid_operation("restore refused").to_string();
    for sample in [
        "dispatch-state",
        "placement",
        "plan-wait",
        "dispatch-upgrade",
    ] {
        let raw = crate::worker_runtime::queue::writer_parity::channel_sample(sample);
        serde_json::from_str::<super::channels::ErrorAnnotation>(&raw)
            .expect("saved annotation has a strict shape");
        let expected =
            include_str!("../../../../worker_runtime/queue/fixtures/writer-main-94609411.jsonl")
                .lines()
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                .find(|record| record["name"] == format!("restore-{sample}/first"))
                .unwrap();
        let expected: serde_json::Value =
            serde_json::from_str(expected["raw"].as_str().unwrap()).unwrap();
        let expected = expected["annotation"].as_str().unwrap();
        assert_eq!(
            super::restore_action_annotation(Some(&raw), &reason).as_deref(),
            Some(expected),
            "{sample} must restore through the typed path"
        );
        serde_json::from_str::<super::channels::ErrorAnnotation>(expected)
            .expect("restored annotation has a strict shape");
        assert_eq!(
            super::restore_action_annotation(Some(expected), &reason).as_deref(),
            Some(expected),
            "{sample} must also restore an existing blocking_reason"
        );
    }
}

#[tokio::test]
async fn restored_condition() {
    for sample in [
        "fallback",
        "executor",
        "hold",
        "dispatch-state",
        "dispatch-upgrade",
        "placement",
        "plan-wait",
        "detected-ci_failed",
        "review-park-null",
        "script-hook",
        "historical",
    ] {
        case!(&format!("restore-{sample}"), "in_progress", f, {
            let raw = if sample == "fallback" {
                serde_json::json!({"id":"queued"})
            } else {
                let annotation = if sample == "historical" {
                    "{\"type\":\"unknown\",\"recovery_actions\":[\"historical\"],\"extra\":true}"
                        .into()
                } else {
                    crate::worker_runtime::queue::writer_parity::channel_sample(sample)
                };
                serde_json::json!({"id":"queued","error_annotation":annotation,"blocked_json":"{\"kind\":\"manual_stop\",\"reason\":\"saved\",\"created_at\":\"old\"}"})
            };
            db::TaskRepo::mutate_metadata(
                &*f.db,
                &f.task.id,
                None,
                vec![db::TaskMetadataMutation::Set {
                    key: "queued_recovery".into(),
                    value: raw.clone(),
                }],
                &db::now_rfc3339(),
            )
            .await
            .unwrap();
            f.ctx
                .task_service
                .restore_failed_task_action(
                    &f.task,
                    &raw,
                    &crate::ServiceError::invalid_operation("restore refused"),
                )
                .await
                .unwrap();
        });
    }
}

#[tokio::test]
async fn restored_condition_variants() {
    for sample in [
        "reset",
        "lifecycle-reset",
        "remote-cancel",
        "integration-admission",
        "review-candidate",
        "budget",
        "step-workspace_error",
        "demo-reset",
        "failed",
        "failed-historical",
    ] {
        case!(&format!("restore-variant-{sample}"), "in_progress", f, {
            let failed = sample.starts_with("failed");
            let annotation =
                crate::worker_runtime::queue::writer_parity::channel_sample(if failed {
                    "executor"
                } else {
                    sample
                });
            let mut raw = serde_json::json!({"id":"queued","error_annotation":annotation});
            if failed {
                raw["failed_json"] = serde_json::json!(if sample == "failed" {
                    "{\"kind\":null,\"reason\":\"saved\",\"execution_id\":null,\"created_at\":\"old\"}"
                } else {
                    "{\"kind\":\"unknown\",\"extension\":true,\"recovery_actions\":[\"drop\"]}"
                });
            } else {
                raw["blocked_json"] = serde_json::json!(
                    "{\"kind\":\"manual_stop\",\"reason\":\"saved\",\"created_at\":\"old\"}"
                );
            }
            db::TaskRepo::mutate_metadata(
                &*f.db,
                &f.task.id,
                None,
                vec![db::TaskMetadataMutation::Set {
                    key: "queued_recovery".into(),
                    value: raw.clone(),
                }],
                &db::now_rfc3339(),
            )
            .await
            .unwrap();
            f.ctx
                .task_service
                .restore_failed_task_action(
                    &f.task,
                    &raw,
                    &crate::ServiceError::invalid_operation("restore refused"),
                )
                .await
                .unwrap();
        });
    }
}
