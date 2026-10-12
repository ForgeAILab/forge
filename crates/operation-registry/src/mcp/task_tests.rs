pub(super) const MOVED: usize = 24;

#[test]
fn forge_list_tasks_contract() {
    super::tests::round_trip(
        "forge_list_tasks",
        serde_json::json!({"project_id":"p","cursor":null,"limit":1,"status":["todo"],"sort_by":"board_position"}),
    );
}

#[test]
fn forge_get_task_contract() {
    super::tests::round_trip("forge_get_task", serde_json::json!({"task_id":"t"}));
}

#[test]
fn forge_preview_prompt_contract() {
    super::tests::round_trip(
        "forge_preview_prompt",
        serde_json::json!({"task_id":"t","role":"coder","trigger":"retry"}),
    );
}

#[test]
fn forge_memory_search_contract() {
    super::tests::round_trip(
        "forge_memory_search",
        serde_json::json!({"project_id":"p","query":"q","layer":1,"token_budget":200,"limit":20,"cursor":null}),
    );
}

#[test]
fn forge_memory_get_contract() {
    super::tests::round_trip("forge_memory_get", serde_json::json!({"id":"i","layer":1}));
}

#[test]
fn forge_get_task_diff_contract() {
    super::tests::round_trip("forge_get_task_diff", serde_json::json!({"task_id":"t"}));
}

#[test]
fn forge_list_executions_contract() {
    super::tests::round_trip(
        "forge_list_executions",
        serde_json::json!({"task_id":"t","cursor":null,"limit":1}),
    );
}

#[test]
fn forge_list_task_dependencies_contract() {
    super::tests::round_trip(
        "forge_list_task_dependencies",
        serde_json::json!({"task_id":"t"}),
    );
}

#[test]
fn forge_list_task_dependents_contract() {
    super::tests::round_trip(
        "forge_list_task_dependents",
        serde_json::json!({"task_id":"t"}),
    );
}

#[test]
fn forge_list_sub_tasks_contract() {
    super::tests::round_trip(
        "forge_list_sub_tasks",
        serde_json::json!({"parent_task_id":"t"}),
    );
}

#[test]
fn forge_create_task_contract() {
    super::tests::round_trip(
        "forge_create_task",
        serde_json::json!({"project_id":"p","title":"t","description":null,"parent_task_id":"parent","depends_on_ids":[],"type":"task","priority":1}),
    );
}

#[test]
fn forge_assign_agent_contract() {
    super::tests::round_trip(
        "forge_assign_agent",
        serde_json::json!({"task_id":"t","agent_id":"a"}),
    );
}

#[test]
fn forge_task_action_contract() {
    super::tests::round_trip(
        "forge_task_action",
        serde_json::json!({"task_id":"t","action":{"verb":"retry","fresh_session":false,"refresh_workspace":false,"reset_budget":false,"guidance":"continue","reason":"retry"},"version":1}),
    );
}

#[test]
fn forge_update_task_contract() {
    super::tests::round_trip(
        "forge_update_task",
        serde_json::json!({"task_id":"t","title":"T","description":null,"priority":1,"plan":null,"version":1}),
    );
}

#[test]
fn forge_transition_task_contract() {
    super::tests::round_trip(
        "forge_transition_task",
        serde_json::json!({"task_id":"t","status":"custom_state","version":1}),
    );
}

#[test]
fn forge_add_task_dependency_contract() {
    super::tests::round_trip(
        "forge_add_task_dependency",
        serde_json::json!({"task_id":"t","depends_on_id":"d"}),
    );
}

#[test]
fn forge_remove_task_dependency_contract() {
    super::tests::round_trip(
        "forge_remove_task_dependency",
        serde_json::json!({"task_id":"t","depends_on_id":"d"}),
    );
}

#[test]
fn forge_create_sub_tasks_contract() {
    super::tests::round_trip(
        "forge_create_sub_tasks",
        serde_json::json!({"parent_task_id":"t","subtasks":[{"title":"child","description":null,"assignee_id":null}]}),
    );
}

#[test]
fn forge_reorder_sub_tasks_contract() {
    super::tests::round_trip(
        "forge_reorder_sub_tasks",
        serde_json::json!({"parent_task_id":"t","ordered_ids":["a","b"]}),
    );
}

#[test]
fn forge_follow_up_execution_contract() {
    super::tests::round_trip(
        "forge_follow_up_execution",
        serde_json::json!({"execution_id":"e","message":"continue","agent_id":null,"overrides":{"model_id":null,"reasoning_effort":null,"permission_policy":null,"hard_deadline_seconds":1}}),
    );
}

#[test]
fn forge_get_project_contract() {
    super::tests::round_trip("forge_get_project", serde_json::json!({"project_id":"p"}));
}

#[test]
fn forge_update_project_contract() {
    super::tests::round_trip(
        "forge_update_project",
        serde_json::json!({"project_id":"p","version":1,"name":"P","settings":{},"paused":false}),
    );
}

#[test]
fn forge_update_project_lifecycle_hooks_contract() {
    super::tests::round_trip(
        "forge_update_project_lifecycle_hooks",
        serde_json::json!({"project_id":"p","version":1,"lifecycle_hooks":{"before_work":[{"type":"script","command":"true"}]}}),
    );
}

#[test]
fn forge_project_escalate_contract() {
    super::tests::round_trip(
        "forge_project_escalate",
        serde_json::json!({"need":"Need help","task_ids":[],"dedupe_key":"key"}),
    );
}

#[test]
fn task_input_nullability_and_custom_workflow_states_preserve_base_acceptance() {
    use super::lookup;
    use serde_json::json;
    for field in ["parent_task_id", "priority", "type"] {
        let mut input = json!({"project_id":"p","title":"t"});
        input[field] = json!(null);
        assert!(
            lookup("forge_create_task").unwrap().decode(input).is_err(),
            "non-null when present: {field}"
        );
    }
    for status in [
        json!(null),
        json!("todo, ,custom_phase"),
        json!(["planning", "custom_phase"]),
    ] {
        assert!(lookup("forge_list_tasks")
            .unwrap()
            .decode(json!({"project_id":"p","status":status}))
            .is_ok());
    }
    assert!(lookup("forge_transition_task")
        .unwrap()
        .decode(json!({"task_id":"t","status":"custom_phase","version":1}))
        .is_ok());
    assert!(lookup("forge_preview_prompt")
        .unwrap()
        .decode(json!({"task_id":"t","role":"coder","trigger":null}))
        .is_ok());
    assert!(lookup("forge_list_tasks")
        .unwrap()
        .decode(json!({"project_id":"p","sort_by":"board_position"}))
        .is_ok());
}

#[test]
fn memory_and_follow_up_bounds_are_the_typed_decoder_bounds() {
    use super::lookup;
    use serde_json::json;
    for layer in [0, 4, 256, -1] {
        assert!(lookup("forge_memory_get")
            .unwrap()
            .decode(json!({"id":"i","layer":layer}))
            .is_err());
    }
    for field in ["limit", "token_budget"] {
        for value in [json!(-1), json!(4294967296_u64), json!(1.0), json!("1")] {
            let mut input = json!({"project_id":"p","query":"q"});
            input[field] = value;
            assert!(lookup("forge_memory_search")
                .unwrap()
                .decode(input)
                .is_err());
        }
    }
    for deadline in [
        json!(0),
        json!(-1),
        json!(4294967296_u64),
        json!(1.0),
        json!("1"),
    ] {
        assert!(lookup("forge_follow_up_execution").unwrap().decode(json!({"execution_id":"e","message":"m","overrides":{"hard_deadline_seconds":deadline}})).is_err());
    }
    for overrides in [
        json!(null),
        json!({}),
        json!({"hard_deadline_seconds":null}),
        json!({"hard_deadline_seconds":4294967295_u64}),
    ] {
        assert!(lookup("forge_follow_up_execution")
            .unwrap()
            .decode(json!({"execution_id":"e","message":"m","overrides":overrides}))
            .is_ok());
    }
}

#[test]
fn typed_lifecycle_hooks_keep_defaults_and_opaque_plugin_config() {
    use super::*;
    use serde_json::json;
    let spec = lookup("forge_update_project_lifecycle_hooks").unwrap();
    for config in [
        json!(null),
        json!({}),
        json!([]),
        json!(true),
        json!("opaque"),
    ] {
        let input = json!({"project_id":"p","version":1,"lifecycle_hooks":{"before_work":[{"type":"script","command":"true"},{"type":"plugin","name":"plugin","config":config}]}});
        let McpInput::UpdateProjectLifecycleHooks(params) = spec.decode(input).unwrap() else {
            panic!("typed hooks")
        };
        let LifecycleHookDef::Script {
            timeout_seconds,
            blocking,
            ..
        } = &params.lifecycle_hooks[&LifecycleEvent::BeforeWork][0]
        else {
            panic!("script")
        };
        assert_eq!(timeout_seconds.get(), 30);
        assert!(!blocking);
    }
    for hooks in [
        json!({"unknown_event":[]}),
        json!({"before_work":[{"type":"script","command":"true","timeout_seconds":0}]}),
        json!({"before_work":[{"type":"plugin","name":"plugin","unknown":true}]}),
    ] {
        assert!(spec
            .decode(json!({"project_id":"p","version":1,"lifecycle_hooks":hooks}))
            .is_err());
    }
}

#[test]
fn all_condition_variants_refuse_undeclared_fields_including_start() {
    use super::lookup;
    use serde_json::json;
    for verb in [
        "start",
        "hold",
        "release",
        "retry",
        "send_back",
        "approve",
        "restart",
        "cancel",
    ] {
        let mut action = json!({"verb":verb,"unknown":true});
        if verb == "send_back" {
            action["guidance"] = json!("fix");
        }
        assert!(
            lookup("forge_task_action")
                .unwrap()
                .decode(json!({"task_id":"t","version":1,"action":action}))
                .is_err(),
            "{verb}"
        );
    }
}
