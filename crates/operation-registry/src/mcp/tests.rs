use super::*;
use std::collections::BTreeSet;

fn round_trip(name: &str, input: Value) {
    let spec = lookup(name).unwrap();
    let fields = spec.input.schema["properties"].as_object().unwrap();
    assert_eq!(
        input.as_object().unwrap().keys().collect::<BTreeSet<_>>(),
        fields.keys().collect::<BTreeSet<_>>(),
        "all handler fields are declared for {name}"
    );
    let validator = jsonschema::validator_for(&spec.input.schema).unwrap();
    assert!(validator.is_valid(&input), "schema accepts {name}: {input}");
    assert!(
        spec.decode(input.clone()).is_ok(),
        "typed decoder accepts {name}"
    );
    let mut unknown = input.clone();
    unknown["unexpected"] = json!(true);
    assert!(!validator.is_valid(&unknown));
    let error = spec.decode(unknown).unwrap_err();
    assert_eq!(error.operation, name);
    assert!(error.detail.contains("unexpected"));
    assert!(error.to_string().contains("expected"));
    for (field, property) in fields {
        if property["type"]
            .as_array()
            .is_some_and(|kinds| kinds.contains(&json!("null")))
        {
            let mut nullable = input.clone();
            nullable[field] = Value::Null;
            assert!(
                spec.decode(nullable).is_ok(),
                "base accepted null: {name}.{field}"
            );
        }
        if !spec.input.schema["required"]
            .as_array()
            .unwrap()
            .contains(&json!(field))
        {
            let mut omitted = input.clone();
            omitted.as_object_mut().unwrap().remove(field);
            assert!(
                spec.decode(omitted).is_ok(),
                "base accepted omission: {name}.{field}"
            );
        }
    }
}

#[test]
fn forge_register_agent_contract() {
    round_trip(
        "forge_register_agent",
        json!({"name": "Executor", "executor_type": "shell", "daemon_id": null}),
    );
}

#[test]
fn forge_list_agents_contract() {
    round_trip(
        "forge_list_agents",
        json!({"status": "idle", "cursor": null, "limit": 1}),
    );
}

#[test]
fn forge_list_projects_contract() {
    round_trip("forge_list_projects", json!({"cursor": null, "limit": 1}));
}

#[test]
fn forge_create_project_contract() {
    round_trip("forge_create_project", json!({"name": "Project"}));
}

#[test]
fn forge_list_agent_profiles_contract() {
    round_trip(
        "forge_list_agent_profiles",
        json!({"identity_id": "identity"}),
    );
}

#[test]
fn forge_list_agent_sessions_contract() {
    round_trip(
        "forge_list_agent_sessions",
        json!({"identity_id": "identity"}),
    );
}

#[test]
fn forge_get_agent_session_contract() {
    round_trip("forge_get_agent_session", json!({"session_id": "session"}));
}

#[test]
fn forge_get_main_agent_contract() {
    round_trip("forge_get_main_agent", json!({}));
}

#[test]
fn forge_set_main_agent_contract() {
    round_trip(
        "forge_set_main_agent",
        json!({"identity_id": "identity", "expected_version": 0, "autonomy_policy": {}}),
    );
}

#[test]
fn forge_get_project_agent_contract() {
    round_trip("forge_get_project_agent", json!({"project_id": "project"}));
}

#[test]
fn forge_set_project_agent_contract() {
    round_trip(
        "forge_set_project_agent",
        json!({"project_id": "project", "identity_id": "identity", "expected_version": 0, "autonomy_policy": {}, "permission_ceiling": {"allowed": []}, "subscriptions": [], "wake_budget": 0}),
    );
}

#[test]
fn forge_list_agent_chats_contract() {
    round_trip(
        "forge_list_agent_chats",
        json!({"cursor": null, "limit": 1}),
    );
}

#[test]
fn forge_get_agent_chat_contract() {
    round_trip("forge_get_agent_chat", json!({"chat_id": "chat"}));
}

#[test]
fn forge_list_agent_chat_messages_contract() {
    round_trip(
        "forge_list_agent_chat_messages",
        json!({"chat_id": "chat", "before_sequence": 1, "cursor": null, "limit": 1}),
    );
}

#[test]
fn forge_send_agent_chat_message_contract() {
    round_trip(
        "forge_send_agent_chat_message",
        json!({"chat_id": "chat", "content": "Hello", "dedupe_key": null}),
    );
}

#[test]
fn forge_list_agent_handoffs_contract() {
    round_trip(
        "forge_list_agent_handoffs",
        json!({"project_id": "project", "cursor": null, "limit": 1}),
    );
}

#[test]
fn forge_get_agent_handoff_contract() {
    round_trip(
        "forge_get_agent_handoff",
        json!({"project_id": "project", "handoff_id": "handoff"}),
    );
}

#[test]
fn forge_create_agent_handoff_contract() {
    round_trip(
        "forge_create_agent_handoff",
        json!({"project_id": "project", "content": "Handoff", "source_message_id": null, "source_turn_job_id": null, "dedupe_key": "handoff-once"}),
    );
}

#[test]
fn complete_catalog_uses_only_portable_keywords_and_has_bounded_descriptions() {
    let names = CATALOG
        .iter()
        .map(|spec| spec.name)
        .collect::<BTreeSet<_>>();
    assert_eq!(names.len(), 18);
    assert_eq!(names.len(), CATALOG.len());
    fn portable(schema: &Value) {
        let object = schema.as_object().expect("portable schema object");
        for key in object.keys() {
            assert!(
                [
                    "type",
                    "properties",
                    "required",
                    "additionalProperties",
                    "items",
                    "enum",
                    "minimum",
                    "maximum",
                    "minLength",
                    "maxLength",
                    "minItems",
                    "maxItems",
                    "description"
                ]
                .contains(&key.as_str()),
                "nonportable keyword {key}"
            );
        }
        for property in schema["properties"]
            .as_object()
            .into_iter()
            .flat_map(|properties| properties.values())
        {
            portable(property);
        }
        if let Some(items) = schema.get("items") {
            portable(items);
        }
    }
    for spec in CATALOG.iter() {
        portable(&spec.input.schema);
        let descriptor = spec.descriptor(false, true);
        let description = descriptor["description"].as_str().unwrap();
        assert!(!description.contains('\n'));
        assert!(description.len() <= 200, "{} prefix", spec.name);
        assert!(crate::authority::mcp_scope_rule(spec.name).is_some());
    }
}
#[test]
fn strict_mcp_integers_do_not_inherit_native_coercion() {
    for value in [json!("1"), json!(1.0), json!(1.5), json!(true)] {
        assert!(lookup("forge_list_projects")
            .unwrap()
            .decode(json!({"limit":value}))
            .is_err());
    }
    assert!(lookup("forge_set_project_agent")
        .unwrap()
        .decode(json!({"project_id":"p","identity_id":"i","expected_version":0,"wake_budget":-1}))
        .is_err());
}
#[test]
fn opaque_policies_and_executor_extensions_preserve_base_acceptance() {
    for policy in [
        Value::Null,
        json!({}),
        json!([]),
        json!({"allowed":["read_project"]}),
        json!("opaque"),
        json!(true),
    ] {
        assert!(lookup("forge_set_main_agent")
            .unwrap()
            .decode(json!({"identity_id":"i","expected_version":0,"autonomy_policy":policy}))
            .is_ok());
    }
    assert!(lookup("forge_register_agent")
        .unwrap()
        .decode(json!({"name":"n","executor_type":"custom-executor"}))
        .is_ok());
}
