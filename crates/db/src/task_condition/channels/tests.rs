use super::*;
use serde_json::Value;
use std::collections::BTreeSet;

const MAIN: &str = include_str!("fixtures/main-13bc2ae1.jsonl");

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Sample {
    name: String,
    channel: String,
    raw: String,
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
    assert_eq!(names.len(), 118);
    // An exhaustive match makes additions require a deliberate corpus update.
    assert_eq!(shapes.len(), 15);
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
fn strict_nested_tags_and_opaque_bounds() {
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
    assert!(BoundedJson::new(Value::String("x".repeat(BoundedJson::MAX_BYTES))).is_err());
    assert!(BoundedJson::new(Value::String("x".repeat(BoundedJson::MAX_BYTES - 2))).is_ok());
    let mut deep = Value::Null;
    for _ in 0..=BoundedJson::MAX_DEPTH {
        deep = Value::Array(vec![deep]);
    }
    assert!(BoundedJson::new(deep).is_err());
    assert!(BoundedConditionObject::new(Value::Null).is_err());
    let carried = r#"{"owner_extension":{"history":[1,true,null]}}"#;
    let value: SupersededValue = serde_json::from_str(carried).unwrap();
    assert!(matches!(value, SupersededValue::Carried(_)));
    assert_eq!(serde_json::to_string(&value).unwrap(), carried);
    let raw = serde_json::json!({"caller":{"custom":[1,true,null]}});
    assert_eq!(
        serde_json::to_value(BoundedJson::new(raw.clone()).unwrap()).unwrap(),
        raw
    );
}

#[test]
fn legacy_mapping_matches_archived_main_without_strict_decode() {
    use super::super::{LegacyConditionField as Field, LegacyConditionInput};
    let corpus = include_str!("fixtures/legacy-main-13bc2ae1.jsonl");
    let mut visited = 0;
    for line in corpus.lines() {
        let record: Value = serde_json::from_str(line).unwrap();
        let text = |key: &str| record[key].as_str().map(str::to_owned);
        let input = LegacyConditionInput {
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
        };
        assert_eq!(
            serde_json::to_string(&crate::map_legacy_condition(&input)).unwrap(),
            record["expected"].as_str().unwrap(),
            "{}",
            record["name"]
        );
        visited += 1;
    }
    assert_eq!(visited, 12);
}
