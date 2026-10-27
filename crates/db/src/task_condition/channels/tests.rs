use super::*;
use serde_json::Value;
use std::collections::BTreeSet;

const MAIN: &str = include_str!("fixtures/main-13bc2ae1.jsonl");
const RESTORED_MAIN: &str = include_str!("fixtures/main-cc39cac5.jsonl");

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Sample {
    name: String,
    channel: String,
    raw: String,
}

#[test]
fn restored_shapes_keep_main_bytes_and_require_string_blocking_reasons() {
    let mut names = BTreeSet::new();
    for line in RESTORED_MAIN.lines() {
        let sample: Sample = serde_json::from_str(line).unwrap();
        assert!(names.insert(sample.name.clone()));
        assert_eq!(sample.channel, "annotation");
        let annotation: ErrorAnnotation = serde_json::from_str(&sample.raw).unwrap();
        let reason = match (&*sample.name, &annotation.shape) {
            ("restore-dispatch-state", AnnotationShape::DetectedState(value)) => {
                &value.blocking_reason
            }
            ("restore-placement", AnnotationShape::PlacementRefusal(value)) => {
                &value.blocking_reason
            }
            ("restore-plan-wait", AnnotationShape::PlanWait(value)) => &value.blocking_reason,
            ("restore-dispatch-upgrade", AnnotationShape::UpgradeDispatch(value)) => {
                &value.blocking_reason
            }
            _ => panic!("{} selected the wrong shape", sample.name),
        };
        assert!(reason.is_some());
        assert_eq!(serde_json::to_string(&annotation).unwrap(), sample.raw);
        let fields: Value = serde_json::from_str(&sample.raw).unwrap();
        assert_eq!(fields["message"].as_str(), reason.as_deref());
        for invalid in [Value::Null, true.into(), 1.into(), serde_json::json!([])] {
            let mut invalid_fields = fields.clone();
            invalid_fields["blocking_reason"] = invalid;
            assert!(
                serde_json::from_value::<ErrorAnnotation>(invalid_fields).is_err(),
                "{} accepted a non-string blocking_reason",
                sample.name
            );
        }
        let mut extended = fields.clone();
        extended["future_field"] = true.into();
        assert!(serde_json::from_value::<ErrorAnnotation>(extended).is_err());
        let mut unknown_kind = fields;
        unknown_kind["type"] = "future_kind".into();
        assert!(serde_json::from_value::<ErrorAnnotation>(unknown_kind).is_err());
    }
    assert_eq!(names.len(), 4);
}

#[test]
fn every_corpus_annotation_has_exactly_one_field_set() {
    // This guards the observed corpus field sets, not every possible input.
    for corpus in [MAIN, RESTORED_MAIN] {
        let mut checked = 0;
        for line in corpus.lines() {
            let sample: Sample = serde_json::from_str(line).unwrap();
            if sample.channel != "annotation" {
                continue;
            }
            let annotation: ErrorAnnotation = serde_json::from_str(&sample.raw).unwrap();
            let fields = serde_json::to_value(&annotation.shape).unwrap();
            macro_rules! accepts {
                ($($shape:ty),+ $(,)?) => {
                    [$(serde_json::from_value::<$shape>(fields.clone()).is_ok()),+]
                };
            }
            let accepted = accepts![
                BlockingAnnotation,
                HoldAnnotation,
                TimedBlockAnnotation,
                RecoveryExecutionAnnotation,
                RemoteCancelAnnotation,
                MessageAnnotation,
                ReasonAnnotation,
                ReviewCandidateAnnotation,
                ReviewParkAnnotation,
                DetectedAnnotation,
                DetectedStateAnnotation,
                BudgetAnnotation,
                StepFailureAnnotation,
                PlacementRefusalAnnotation,
                PlanWaitAnnotation,
                UpgradeDispatchAnnotation,
            ];
            assert_eq!(
                accepted.into_iter().filter(|accepted| *accepted).count(),
                1,
                "{} has an ambiguous field set",
                sample.name
            );
            checked += 1;
        }
        assert!(checked > 0, "each corpus must contain annotations");
    }
}

#[test]
fn placement_refusal_marker_shares_the_restore_annotation_shape() {
    let sample = MAIN
        .lines()
        .map(|line| serde_json::from_str::<Sample>(line).unwrap())
        .find(|sample| sample.channel == "placement_refusal")
        .unwrap();
    let mut marker: Value = serde_json::from_str(&sample.raw).unwrap();
    assert!(!marker["annotation"]
        .as_object()
        .unwrap()
        .contains_key("blocking_reason"));
    marker["annotation"]["blocking_reason"] = "restored reason".into();
    let ConditionMetadataValue::PlacementRefusal(decoded) =
        ConditionMetadataValue::decode("placement_refusal", &marker.to_string()).unwrap()
    else {
        panic!("placement_refusal must select its own metadata codec");
    };
    assert_eq!(
        decoded.annotation.blocking_reason.as_deref(),
        Some("restored reason")
    );
    marker["annotation"]["blocking_reason"] = Value::Null;
    assert!(ConditionMetadataValue::decode("placement_refusal", &marker.to_string()).is_err());
}

#[test]
fn strict_channels_keep_main_bytes_and_reject_unknown_fields_and_tags() {
    let mut names = BTreeSet::new();
    let mut keys = BTreeSet::new();
    let mut shapes = std::collections::HashSet::new();
    for line in MAIN.lines() {
        let sample: Sample = serde_json::from_str(line).unwrap();
        assert!(names.insert((sample.name.clone(), sample.channel.clone())));
        let encoded = match sample.channel.as_str() {
            "annotation" => {
                let value: ErrorAnnotation = serde_json::from_str(&sample.raw)
                    .unwrap_or_else(|error| panic!("{}: {error}", sample.name));
                shapes.insert(std::mem::discriminant(&value.shape));
                serde_json::to_string(&value).unwrap()
            }
            "interruption" => {
                serde_json::to_string(&serde_json::from_str::<Interruption>(&sample.raw).unwrap())
                    .unwrap()
            }
            "barrier" => {
                serde_json::to_string(&serde_json::from_str::<EntryBarrier>(&sample.raw).unwrap())
                    .unwrap()
            }
            key => {
                let value = ConditionMetadataValue::decode(key, &sample.raw)
                    .unwrap_or_else(|error| panic!("{}: {error}", sample.name));
                assert_eq!(value.key(), key);
                keys.insert(key.to_owned());
                serde_json::to_string(&value).unwrap()
            }
        };
        assert_eq!(encoded.as_bytes(), sample.raw.as_bytes(), "{}", sample.name);
        let mut fields: Value = serde_json::from_str(&sample.raw).unwrap();
        if let Some(fields) = fields.as_object_mut() {
            fields.insert("future_field".into(), true.into());
            let rejected = match sample.channel.as_str() {
                "annotation" => {
                    serde_json::from_value::<ErrorAnnotation>(Value::Object(fields.clone()))
                        .is_err()
                }
                "interruption" => {
                    serde_json::from_value::<Interruption>(Value::Object(fields.clone())).is_err()
                }
                "barrier" => {
                    serde_json::from_value::<EntryBarrier>(Value::Object(fields.clone())).is_err()
                }
                key => {
                    ConditionMetadataValue::decode(key, &Value::Object(fields.clone()).to_string())
                        .is_err()
                }
            };
            assert!(rejected, "{} accepted an unknown field", sample.name);
        }
        if matches!(sample.channel.as_str(), "annotation" | "interruption") {
            for tag in ["future_kind", "unknown", "agent_timeout"] {
                let mut fields: Value = serde_json::from_str(&sample.raw).unwrap();
                fields[if sample.channel == "annotation" {
                    "type"
                } else {
                    "kind"
                }] = tag.into();
                assert!(
                    if sample.channel == "annotation" {
                        serde_json::from_value::<ErrorAnnotation>(fields).is_err()
                    } else {
                        serde_json::from_value::<Interruption>(fields).is_err()
                    },
                    "{} accepted {tag}",
                    sample.name
                );
            }
        }
    }
    assert_eq!(names.len(), 120);
    // Shape coverage makes additions require a deliberate corpus update.
    assert_eq!(shapes.len(), 16);
    assert_eq!(
        ConditionMetadataValue::KEYS,
        super::super::EVIDENCE_METADATA_KEYS
    );
    assert_eq!(
        keys,
        ConditionMetadataValue::KEYS
            .into_iter()
            .map(str::to_owned)
            .collect()
    );
}

#[test]
fn step_overlay_replaces_instead_of_nesting_and_sorts_struct_fields() {
    let mut visited = 0;
    for line in MAIN.lines() {
        let sample: Sample = serde_json::from_str(line).unwrap();
        if sample.channel != "annotation" {
            continue;
        }
        let annotation: ErrorAnnotation = serde_json::from_str(&sample.raw).unwrap();
        let overlay = StepFailureOverlay {
            task_step_error: None,
            task_step_id: "next-step".into(),
        };
        let merged = annotation.with_step_failure(overlay.clone()).unwrap();
        let mut main: Value = serde_json::from_str(&sample.raw).unwrap();
        main["task_step_id"] = "next-step".into();
        main["task_step_error"] = Value::Null;
        assert_eq!(merged, main.to_string(), "{}", sample.name);
        let decoded: ErrorAnnotation = serde_json::from_str(&merged).unwrap();
        assert_eq!(decoded.step_failure(), Some(&overlay));
        let mut typed = annotation.clone();
        typed.set_step_failure(overlay.clone());
        assert_eq!(typed, decoded);
        assert_eq!(decoded.with_step_failure(overlay).unwrap(), merged);
        assert_eq!(
            serde_json::from_str::<ErrorAnnotation>(&serde_json::to_string(&decoded).unwrap())
                .unwrap(),
            decoded
        );
        visited += 1;
    }
    assert!(visited >= 15);
}

#[test]
fn kindless_loop_and_typed_superseded_channels_roundtrip() {
    for raw in [
        r#"{"blocked_at":"at","reason":"loop","source":"workflow_loop"}"#,
        r#"{"blocked_at":"at","reason":null,"source":"workflow_loop"}"#,
        r#"{"created_at":null,"details":{"retry_at":null,"superseded":null},"execution_id":null,"kind":null,"reason":"null fields"}"#,
        r#"{"details":{"assignment_guidance":"assign coder","assignment_required":true,"superseded":{"blocked_json":"historical","error_annotation":null,"failed_json":null,"status":"in_progress"}},"reason":"dependency"}"#,
    ] {
        let interruption: Interruption = serde_json::from_str(raw).unwrap();
        assert_eq!(serde_json::to_string(&interruption).unwrap(), raw);
    }
    for raw in [
        r#"{"reason":"failure","kind":"future"}"#,
        r#"{"reason":"failure","details":{"future":true}}"#,
        r#"{"reason":false}"#,
        r#"{"kind":"future","reason":null}"#,
        r#"{"details":{"future":true},"reason":null}"#,
        r#"{}"#,
    ] {
        assert!(serde_json::from_str::<Interruption>(raw).is_err());
    }
}

#[test]
fn strict_nested_tags_and_copied_decode_depth() {
    for raw in [
        r#"{"owner_kind":"future"}"#,
        r#"{"owner_kind":"server","extra":true}"#,
    ] {
        assert!(serde_json::from_str::<ConditionMachine>(raw).is_err());
    }
    assert!(ConditionMetadataValue::decode("future", "true").is_err());
    assert!(serde_json::from_str::<ErrorAnnotation>(r#"{"type":"future"}"#).is_err());
    assert!(serde_json::from_str::<InterruptionDetails>(r#"{"attempts":3}"#).is_err());
    assert!(serde_json::from_str::<PlanWaitCode>(r#""future""#).is_err());
    assert!(serde_json::from_str::<EnvironmentWaitKind>(r#""future""#).is_err());
    assert!(serde_json::from_str::<AnnotationHook>(
        r#"{"name":"future","cancelled_dependency_ids":[]}"#
    )
    .is_err());
    assert!(
        serde_json::from_str::<EntryBarrier>(r#"{"state":"review","status":"future"}"#).is_err()
    );
    let mut deep = Value::Null;
    for _ in 0..=CopiedJson::MAX_DEPTH {
        deep = Value::Array(vec![deep]);
    }
    let encoded = serde_json::to_string(&deep).unwrap();
    assert!(serde_json::from_str::<CopiedJson>(&encoded).is_err());
    assert!(serde_json::from_str::<CopiedConditionObject>("null").is_err());
    assert!(serde_json::from_str::<SupersededValue>("null").is_err());
    let at_limit = &deep[0];
    assert_eq!(
        serde_json::to_value(serde_json::from_value::<CopiedJson>(at_limit.clone()).unwrap())
            .unwrap(),
        *at_limit
    );
    let mut deep_object = serde_json::json!({"saved": at_limit});
    assert!(serde_json::from_value::<SupersededValue>(deep_object.clone()).is_err());
    deep_object = serde_json::json!({"saved": &at_limit[0]});
    assert!(serde_json::from_value::<SupersededValue>(deep_object).is_ok());
    let carried = r#"{"owner_extension":{"history":[1,true,null]}}"#;
    let value: SupersededValue = serde_json::from_str(carried).unwrap();
    assert!(matches!(value, SupersededValue::Carried(_)));
    assert_eq!(serde_json::to_string(&value).unwrap(), carried);
    let raw = serde_json::json!({"caller":{"custom":[1,true,null]}});
    assert_eq!(
        serde_json::to_value(CopiedJson::new(raw.clone())).unwrap(),
        raw
    );
}

#[test]
fn legacy_mapping_matches_archived_main_without_strict_decode() {
    use super::super::{ConditionField as Field, ConditionInput};
    let corpus = include_str!("fixtures/legacy-main-13bc2ae1.jsonl");
    let mut visited = 0;
    for line in corpus.lines() {
        let record: Value = serde_json::from_str(line).unwrap();
        let text = |key: &str| record[key].as_str().map(str::to_owned);
        let input = ConditionInput {
            error_annotation: text("annotation"),
            blocked_json: text("blocked"),
            failed_json: text("failed"),
            entry_barrier_json: text("barrier"),
            metadata_json: text("metadata"),
            non_text: match record["non_text"].as_str() {
                Some("annotation") => vec![Field::ErrorAnnotation],
                Some("blocked") => vec![Field::BlockedJson],
                Some("failed") => vec![Field::FailedJson],
                Some("barrier") => vec![Field::EntryBarrierJson],
                Some("metadata") => vec![Field::MetadataJson],
                None => vec![],
                Some(field) => panic!("unexpected non-text field: {field}"),
            },
            facts: None,
            non_text_bytes: Vec::new(),
        };
        assert_eq!(
            serde_json::to_string(&crate::compose_condition(&input)).unwrap(),
            record["expected"].as_str().unwrap(),
            "{}",
            record["name"]
        );
        visited += 1;
    }
    assert_eq!(visited, 12);
}

#[test]
fn review_fix_park_variants_cover_main_writes() {
    let mut visited = 0;
    for line in MAIN.lines() {
        let sample: Sample = serde_json::from_str(line).unwrap();
        if !sample.name.starts_with("review-park-") {
            continue;
        }
        let annotation: ErrorAnnotation = serde_json::from_str(&sample.raw).unwrap();
        assert_eq!(serde_json::to_string(&annotation).unwrap(), sample.raw);
        let AnnotationShape::ReviewPark(park) = &annotation.shape else {
            panic!("{} did not select ReviewPark", sample.name);
        };
        assert_eq!(
            park.blocked_execution_id.as_deref(),
            (sample.name == "review-park-execution").then_some("execution")
        );
        let mut missing: Value = serde_json::from_str(&sample.raw).unwrap();
        missing
            .as_object_mut()
            .unwrap()
            .remove("blocked_execution_id");
        let missing: ErrorAnnotation = serde_json::from_value(missing).unwrap();
        assert_ne!(
            std::mem::discriminant(&annotation.shape),
            std::mem::discriminant(&missing.shape)
        );
        visited += 1;
    }
    assert_eq!(visited, 2);
}

#[test]
fn review_fix_copied_values_preserve_large_and_deep_stored_json() {
    let raw = serde_json::json!({"type":"workspace_reset_required","message":"x".repeat(200_000)});
    assert_eq!(
        serde_json::to_value(CopiedJson::new(raw.clone())).unwrap(),
        raw
    );
    let marker = serde_json::json!({
        "code":"daemon_upgrade_required", "daemon_ids":["daemon"],
        "deferred_dispatch":null, "dispatch_disposition":null, "error_annotation":raw,
    });
    let value: DaemonUpgradeRefusal = serde_json::from_value(marker.clone()).unwrap();
    assert_eq!(serde_json::to_value(value).unwrap(), marker);
    let carried =
        SupersededValue::Carried(CopiedConditionObject::new(raw.as_object().unwrap().clone()));
    assert_eq!(serde_json::to_value(&carried).unwrap(), raw);
    assert_eq!(
        serde_json::from_value::<SupersededValue>(raw.clone()).unwrap(),
        carried
    );
    let mut deep = Value::Null;
    for _ in 0..=CopiedJson::MAX_DEPTH {
        deep = serde_json::json!({"saved": deep});
    }
    assert_eq!(
        serde_json::to_value(CopiedJson::new(deep.clone())).unwrap(),
        deep
    );
    let carried = SupersededValue::Carried(CopiedConditionObject::new(
        deep.as_object().unwrap().clone(),
    ));
    assert_eq!(serde_json::to_value(carried).unwrap(), deep);
}

#[test]
fn review_fix_type_first_is_reserved_for_struct_and_literal_writers() {
    let mut checked = 0;
    for line in MAIN.lines() {
        let sample: Sample = serde_json::from_str(line).unwrap();
        if sample.channel != "annotation" {
            continue;
        }
        let annotation: ErrorAnnotation = serde_json::from_str(&sample.raw).unwrap();
        if matches!(
            annotation.shape,
            AnnotationShape::Blocking(_) | AnnotationShape::Message(_)
        ) && annotation.step_failure().is_none()
        {
            continue;
        }
        let mut fields: Value = serde_json::from_str(&sample.raw).unwrap();
        let kind = fields.as_object_mut().unwrap().remove("type").unwrap();
        let rest = fields.to_string();
        let type_first = format!("{{\"type\":{kind},{}", &rest[1..]);
        assert!(
            serde_json::from_str::<ErrorAnnotation>(&type_first).is_err(),
            "{}",
            sample.name
        );
        checked += 1;
    }
    assert!(checked >= 15);
}

#[test]
fn closed_nested_objects_reject_unknown_fields() {
    let cases = [
        ("executor", "annotation", "/artifact"),
        ("dependency-hook", "annotation", "/hook"),
        ("script-hook", "annotation", "/hook"),
        ("placement", "annotation", "/machines/0"),
        ("placement", "annotation", "/rejected_candidates/0"),
        (
            "placement/placement_refusal",
            "placement_refusal",
            "/annotation/machines/0",
        ),
        (
            "environment-ready-wait/environment_wait",
            "environment_wait",
            "/machine",
        ),
        (
            "executor-unavailable",
            "interruption",
            "/details/attempts/0",
        ),
    ];
    for (name, channel, pointer) in cases {
        let sample = MAIN
            .lines()
            .map(|line| serde_json::from_str::<Sample>(line).unwrap())
            .find(|sample| sample.name == name && sample.channel == channel)
            .unwrap();
        let mut value: Value = serde_json::from_str(&sample.raw).unwrap();
        value
            .pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("future_field".into(), true.into());
        let rejected = match channel {
            "annotation" => serde_json::from_value::<ErrorAnnotation>(value).is_err(),
            "interruption" => serde_json::from_value::<Interruption>(value).is_err(),
            key => ConditionMetadataValue::decode(key, &value.to_string()).is_err(),
        };
        assert!(rejected, "{name} accepted an unknown field at {pointer}");
    }
}
