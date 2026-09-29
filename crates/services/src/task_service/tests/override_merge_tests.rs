use super::*;

#[test]
fn no_overrides_keeps_agent_config_and_empty_provenance() {
    let agent = json!({"model": "a", "effort": "low"});
    let (config, applied) = merge_config_layers(&agent, &json!({}));

    assert_eq!(config, agent);
    assert_eq!(
        applied,
        OverridesApplied {
            agent: vec!["effort".to_owned(), "model".to_owned()],
            execution: Vec::new(),
        }
    );
}

#[test]
fn execution_override_wins_over_agent_config_and_records_key() {
    let (config, applied) = merge_config_layers(
        &json!({"model": "a", "effort": "low"}),
        &json!({"effort": "high"}),
    );

    assert_eq!(config, json!({"model": "a", "effort": "high"}));
    assert_eq!(applied.agent, vec!["effort", "model"]);
    assert_eq!(applied.execution, vec!["effort"]);
}

#[test]
fn typed_execution_overrides_are_translated_to_config_keys_and_recorded() {
    let execution_layer = execution_overrides_to_config_layer(Some(ExecutionOverrides {
        model_id: Some("gpt-5-codex".to_owned()),
        reasoning_effort: Some("high".to_owned()),
        permission_policy: Some("auto".to_owned()),
        hard_deadline_seconds: None,
    }))
    .expect("execution overrides translate");
    let (config, applied) = merge_config_layers(
        &json!({"model": "gpt-5", "model_reasoning_effort": "medium"}),
        &execution_layer,
    );

    assert_eq!(config["model"], "gpt-5-codex");
    assert_eq!(config["model_reasoning_effort"], "high");
    assert_eq!(config["effort"], "high");
    assert_eq!(config["permission_policy"], "auto");
    assert!(applied.execution.iter().any(|key| key == "model"));
    assert!(applied
        .execution
        .iter()
        .any(|key| key == "model_reasoning_effort"));
    assert!(applied.execution.iter().any(|key| key == "effort"));
    assert!(applied
        .execution
        .iter()
        .any(|key| key == "permission_policy"));
}

#[test]
fn provenance_drops_keys_that_do_not_survive_typed_config_resolution() {
    let applied = OverridesApplied {
        agent: vec![
            "model".to_owned(),
            "sandbox".to_owned(),
            "unknown".to_owned(),
        ],
        execution: vec![
            "model_reasoning_effort".to_owned(),
            "effort".to_owned(),
            "permission_policy".to_owned(),
        ],
    };

    let filtered = applied.retain_config_keys(&json!({
        "model": "gpt-5-codex",
        "sandbox": "danger-full-access",
        "model_reasoning_effort": "high",
        "permission_policy": "auto",
    }));

    assert_eq!(filtered.agent, vec!["model", "sandbox"]);
    assert_eq!(
        filtered.execution,
        vec!["model_reasoning_effort", "permission_policy"]
    );
}

#[test]
fn invalid_or_non_object_execution_override_is_empty() {
    let invalid_execution = parse_config_override_layer("execution overrides", "{");
    let non_object_execution = override_value_or_empty("execution overrides", Some(json!("bad")));

    let (config, applied) = merge_config_layers(&json!({"model": "agent"}), &invalid_execution);
    assert_eq!(config, json!({"model": "agent"}));
    assert!(applied.execution.is_empty());

    let (config, applied) = merge_config_layers(&json!({"model": "agent"}), &non_object_execution);
    assert_eq!(config, json!({"model": "agent"}));
    assert!(applied.execution.is_empty());
}
