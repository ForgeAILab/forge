//! Typed domain argument normalization and preparation tests.

use super::*;

#[tokio::test]
async fn orchestration_tools_admit_and_unwrap_a_parameters_envelope() {
    let tool = ForgeScopeProposeTool::new(
        "agent-1".to_owned(),
        scope(CanonicalScopeType::Project, WorkspaceAccess::Deny),
        vec![PROJECT_READINESS_OPERATION.to_owned()],
        Arc::new(TestProvider::default()),
    );
    let plain = json!({
        "operation": PROJECT_READINESS_OPERATION,
        "payload": {"action": "evaluate", "milestone_id": "m-1", "milestone_version": 1},
        "dedupe_key": "readiness-1",
        "correlation_id": "readiness-1"
    });
    let spec = tool.spec();
    let validator = jsonschema::validator_for(&spec.input_schema).expect("schema");
    assert!(spec.input_schema["properties"].get("parameters").is_none());
    assert_eq!(
        spec.input_schema["required"],
        json!(["operation", "payload", "dedupe_key", "correlation_id"])
    );
    for arguments in [plain.clone(), json!({"parameters": plain.clone()})] {
        let normalized = tool.normalize_arguments(arguments).unwrap();
        validator
            .validate(&normalized)
            .expect("normalized call validates");
        let prepared = tool
            .prepare(normalized, &test_preparation_context("envelope"))
            .await
            .unwrap();
        assert_eq!(prepared.arguments(), &plain);
    }
    for field in ["operation", "payload", "dedupe_key", "correlation_id"] {
        let mut missing = plain.clone();
        missing.as_object_mut().unwrap().remove(field);
        for arguments in [missing.clone(), json!({"parameters": missing})] {
            let normalized = tool.normalize_arguments(arguments).unwrap();
            if field == "payload" {
                assert!(
                    tool.prepare(normalized, &test_preparation_context("missing-payload"))
                        .await
                        .is_err(),
                    "the registry refuses the empty payload's missing fields"
                );
            } else {
                assert!(
                    !validator.is_valid(&normalized),
                    "missing {field} must fail schema validation"
                );
            }
        }
    }
    // The envelope carries the same shape, not a looser one: a wrong type
    // inside it is unwrapped and then refused by the schema.
    let wrong_type = tool
        .normalize_arguments(json!({"parameters": {"operation": 7}}))
        .unwrap();
    assert!(
        !validator.is_valid(&wrong_type),
        "a non-string operation inside the envelope must fail schema validation"
    );
    // `prepare` keeps its own required-field checks. Recovery hands it
    // stored arguments without schema validation, so these refusals must
    // not depend on the schema having run first.
    let no_operation = tool
        .normalize_arguments(json!({"parameters": {"payload": {}}}))
        .unwrap();
    assert!(!validator.is_valid(&no_operation));
    let refused = tool
        .prepare(no_operation, &test_preparation_context("envelope"))
        .await
        .expect_err("a call missing its operation is refused in prepare");
    assert!(refused.to_string().contains("operation"), "{refused}");
    let mut missing_dedupe = plain.clone();
    missing_dedupe.as_object_mut().unwrap().remove("dedupe_key");
    let missing_dedupe = tool
        .normalize_arguments(json!({"parameters": missing_dedupe}))
        .unwrap();
    assert!(!validator.is_valid(&missing_dedupe));
    let refused = tool
        .prepare(missing_dedupe, &test_preparation_context("envelope"))
        .await
        .expect_err("an enveloped call missing dedupe_key is refused in prepare");
    assert!(refused.to_string().contains("dedupe_key"), "{refused}");
    // The generated envelope and preparation both refuse a null idempotency key.
    let mut null_dedupe = plain.clone();
    null_dedupe["dedupe_key"] = Value::Null;
    let null_dedupe = tool
        .normalize_arguments(json!({"parameters": null_dedupe}))
        .unwrap();
    assert!(
        !validator.is_valid(&null_dedupe),
        "the registry envelope requires a non-null idempotency key"
    );
    let refused = tool
        .prepare(null_dedupe, &test_preparation_context("envelope"))
        .await
        .expect_err("a null dedupe_key is refused in prepare");
    assert!(refused.to_string().contains("dedupe_key"), "{refused}");
}

#[tokio::test]
async fn task_plan_schema_is_compact_nonnullable_and_lifts_flat_payload_fields() {
    let root = tempfile::tempdir().expect("Task workspace");
    let composition = ScopeToolComposition::for_scope_with_permissions(
        "planner-1",
        scope(CanonicalScopeType::Task, WorkspaceAccess::TaskRead),
        Some("planner"),
        Some(root.path().to_str().unwrap()),
        &all_permissions(),
        Some(Arc::new(TestProvider::default())),
    )
    .expect("planner composition");
    let tool = composition
        .tools()
        .into_iter()
        .find(|tool| tool.spec().name == "forge_scope_propose")
        .expect("Task proposal tool");
    let spec = tool.spec();

    let operations = spec.input_schema["properties"]["operation"]["enum"]
        .as_array()
        .expect("Task operation enum");
    assert!(operations.iter().any(|value| value == TASK_PLAN_OPERATION));

    let guidance = spec.input_schema["properties"]["payload"]["description"]
        .as_str()
        .expect("Task payload guidance");
    for visible_contract in [
        "task.plan",
        "action",
        "content",
        "checklist",
        "1048576 UTF-8 bytes",
    ] {
        assert!(
            guidance.contains(visible_contract),
            "Task plan guidance must visibly name {visible_contract}: {guidance}"
        );
    }
    assert_eq!(
        spec.input_schema["properties"]["action"]["type"],
        json!(["null", "string"])
    );
    assert_eq!(
        spec.input_schema["properties"]["content"]["type"],
        json!(["null", "string"])
    );
    assert!(
        spec.input_schema["properties"]["payload"]
            .get("properties")
            .is_none()
    );
    let validator = jsonschema::validator_for(&spec.input_schema).expect("Task tool schema");
    let explicit_null_envelope = json!({
        "parameters": {
            "operation": TASK_PLAN_OPERATION,
            "payload": null,
            "dedupe_key": null,
            "correlation_id": null,
            "action": null,
            "content": null
        }
    });
    assert!(
        !validator.is_valid(
            &tool
                .normalize_arguments(explicit_null_envelope.clone())
                .unwrap()
        ),
        "registered envelope refuses null identifiers in its advertised schema"
    );
    let workspace: Arc<dyn Workspace> = Arc::new(TestWorkspace {
        root: "<none>".to_owned(),
    });
    let refusal = tool
        .prepare(
            tool.normalize_arguments(explicit_null_envelope).unwrap(),
            &command_preparation_context(workspace.clone()),
        )
        .await
        .expect_err("prepare rejects the missing idempotency key in-turn");
    assert!(refusal.to_string().contains("dedupe_key"), "{refusal}");

    let flat = json!({
        "operation": TASK_PLAN_OPERATION,
        "payload": null,
        "action": "write",
        "content": "# Plan\n\n- [ ] Implement the fix\n",
        "dedupe_key": "task-plan-1",
        "correlation_id": "task-plan-1"
    });
    assert!(
        validator
            .validate(&tool.normalize_arguments(flat.clone()).unwrap())
            .is_ok(),
        "provider-facing schema must admit flat task.plan fields"
    );
    let prepared = tool
        .prepare(
            tool.normalize_arguments(flat).unwrap(),
            &command_preparation_context(workspace),
        )
        .await
        .expect("flat task.plan call prepares");
    assert_eq!(prepared.arguments()["payload"]["action"], "write");
    assert_eq!(
        prepared.arguments()["payload"]["content"],
        "# Plan\n\n- [ ] Implement the fix\n"
    );
    assert!(prepared.arguments().get("action").is_none());
    assert!(prepared.arguments().get("content").is_none());
}

#[tokio::test]
async fn generic_recovery_lifts_flat_provider_fields_into_payload() {
    let operation = crate::operation_catalog::TASK_ACTION_OPERATION;
    let tool = ForgeScopeProposeTool::new(
        "agent-1".to_owned(),
        CanonicalScope {
            scope_type: CanonicalScopeType::Project,
            scope_id: "project-1".to_owned(),
            workspace_access: WorkspaceAccess::Deny,
        },
        vec![operation.to_owned()],
        Arc::new(TestProvider::default()),
    );
    let mixed = json!({
        "operation": operation,
        "dedupe_key": "recover-task-1",
        "correlation_id": "recover-task-1",
        "parameters": {
            "operation": operation,
            "task_id": "task-1",
            "version": 1,
            "action": {"verb":"retry","fresh_session":true,"guidance":"the executor stopped"}
        }
    });
    let validator = jsonschema::validator_for(&tool.spec().input_schema).expect("schema");
    assert!(
        validator
            .validate(&tool.normalize_arguments(mixed.clone()).unwrap())
            .is_ok(),
        "provider-facing schema admits mixed flat recovery fields"
    );
    let workspace: Arc<dyn Workspace> = Arc::new(TestWorkspace {
        root: "<none>".to_owned(),
    });
    let prepared = tool
        .prepare(
            tool.normalize_arguments(mixed).unwrap(),
            &command_preparation_context(workspace.clone()),
        )
        .await
        .expect("flat recovery call prepares");
    assert_eq!(prepared.arguments()["operation"], operation);
    assert_eq!(prepared.arguments()["payload"]["task_id"], "task-1");
    assert_eq!(
        prepared.arguments()["payload"]["action"]["guidance"],
        "the executor stopped"
    );
    assert_eq!(prepared.arguments()["payload"]["action"]["verb"], "retry");
    assert!(prepared.arguments().get("parameters").is_none());
    assert!(prepared.arguments().get("task_id").is_none());

    let conflict = tool
        .normalize_arguments(json!({
            "operation": operation,
            "payload": {
                "task_id": "task-1",
                "version": 1,
                "action": {"verb":"cancel"}
            },
            "action": {"verb":"retry","fresh_session":true},
            "dedupe_key": "recover-task-1-conflict",
            "correlation_id": "recover-task-1-conflict"
        }))
        .expect_err("conflicting flat and nested fields are ambiguous");
    assert!(conflict.to_string().contains("conflicts"), "{conflict}");
}

#[tokio::test]
async fn generic_cancellation_lifts_the_versioned_payload_fields() {
    let operation = crate::operation_catalog::TASK_ACTION_OPERATION;
    let tool = ForgeScopeProposeTool::new(
        "agent-1".to_owned(),
        CanonicalScope {
            scope_type: CanonicalScopeType::Project,
            scope_id: "project-1".to_owned(),
            workspace_access: WorkspaceAccess::Deny,
        },
        vec![operation.to_owned()],
        Arc::new(TestProvider::default()),
    );
    let flat = json!({
        "operation": operation,
        "action": {"verb":"cancel"},
        "task_id": "task-1",
        "version": 7,
        "dedupe_key": "cancel-task-1",
        "correlation_id": "cancel-task-1"
    });
    let validator = jsonschema::validator_for(&tool.spec().input_schema).expect("schema");
    assert!(
        validator
            .validate(&tool.normalize_arguments(flat.clone()).unwrap())
            .is_ok(),
        "provider-facing schema admits flat cancellation fields"
    );
    let workspace: Arc<dyn Workspace> = Arc::new(TestWorkspace {
        root: "<none>".to_owned(),
    });
    let prepared = tool
        .prepare(
            tool.normalize_arguments(flat).unwrap(),
            &command_preparation_context(workspace),
        )
        .await
        .expect("flat cancellation call prepares");
    assert_eq!(prepared.arguments()["payload"]["action"]["verb"], "cancel");
    assert_eq!(prepared.arguments()["payload"]["task_id"], "task-1");
    assert_eq!(prepared.arguments()["payload"]["version"], 7);
}
