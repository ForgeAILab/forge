use super::*;
use serde_json::json;

fn source() -> Value {
    json!({"task_id":"t", "project_id":"p", "repo_id":"r", "documents":[],
        "task_scope":{"title":"check", "description":"scope", "task_type":"task", "config":{"review":{"ci_steps":["task-one","task-two"]}}},
        "project_settings":{"default_review_config":{"setup_steps":["prepare"],"ci_steps":["project-ci"],"check_timeout_seconds":9,
            "conformance_checks":[{"id":"linked", "command":"linked-check", "requirement_ids":["task:acceptance"]}]}}})
}
fn config<'a>(
    source: &'a Value,
    environment: &'a ProjectEnvironment,
    hooks: &'a [LifecycleHookDef],
) -> CheckSpecConfiguration<'a> {
    CheckSpecConfiguration {
        source,
        entry_state_config: None,
        environment,
        hooks,
        hook_test_index: None,
        single_environment_check: None,
        event: LifecycleEvent::BeforeWork,
        role: "coder",
        owner_is_daemon: false,
        has_workspace: true,
        whole_run_timeout_seconds: 1800,
        queue_head_bundle: None,
    }
}
fn commands(spec: &CheckSpec) -> Vec<&str> {
    spec.commands
        .iter()
        .map(|c| c.shell_text.as_str())
        .collect()
}
#[test]
fn entry_and_manual_ci_match_effective_configuration_and_unbounded_sequence() {
    let source = source();
    let environment: ProjectEnvironment =
        serde_json::from_value(json!({"env":{"SECRET":"not-in-spec","LANG":"C"}})).unwrap();
    for family in [CheckPurpose::EntryCi, CheckPurpose::ReviewCi] {
        let spec = build_check_spec(family, &config(&source, &environment, &[])).unwrap();
        assert_eq!(commands(&spec), ["task-one", "task-two"]);
        for (index, command) in spec.commands.iter().enumerate() {
            assert_eq!(command.id, format!("ci:{index}"));
            assert_eq!(command.timeout_seconds, None);
            assert_eq!(command.working_directory, CheckWorkingDirectory::TaskRoot);
            assert_eq!(
                command.environment_keys,
                BTreeSet::from(["SECRET".into(), "LANG".into()])
            );
            assert_eq!(command.failure_policy, CheckFailurePolicy::StopBundle);
            assert_eq!(command.shell, "bash -lc");
            assert_eq!(command.cacheability, CheckCacheability::Uncacheable);
        }
        assert_eq!(
            spec,
            build_check_spec(family, &config(&source, &environment, &[])).unwrap()
        );
        assert!(!serde_json::to_string(&spec)
            .unwrap()
            .contains("not-in-spec"));
    }
}
#[test]
fn conformance_matches_existing_governing_context_setup_and_required_order() {
    let source = source();
    let env = ProjectEnvironment::default();
    let context = crate::contract::context_from_source(&source).unwrap();
    let spec = build_check_spec(CheckPurpose::Conformance, &config(&source, &env, &[])).unwrap();
    let expected: Vec<_> = context
        .setup_steps
        .iter()
        .map(String::as_str)
        .chain(context.required_checks.iter().map(|c| c.command.as_str()))
        .collect();
    assert_eq!(commands(&spec), expected);
    assert_eq!(
        commands(&spec),
        ["prepare", "task-one", "task-two", "linked-check"]
    );
    assert_eq!(
        spec.commands[0].failure_policy,
        CheckFailurePolicy::StopBundle
    );
    assert!(spec.commands[1..]
        .iter()
        .all(|c| c.failure_policy == CheckFailurePolicy::Continue));
    assert!(spec.commands.iter().all(|c| c.timeout_seconds == Some(9)
        && c.working_directory == CheckWorkingDirectory::TaskRoot
        && c.environment_keys.is_empty()));
    assert_eq!(
        spec.commands[3].requirement_ids,
        BTreeSet::from(["task:acceptance".into()])
    );
}
#[test]
fn read_only_families_drop_implementation_commands_and_keep_linked_checks() {
    let mut source = source();
    source["task_scope"]["task_type"] = json!("discovery");
    let env = ProjectEnvironment::default();
    for family in [CheckPurpose::EntryCi, CheckPurpose::ReviewCi] {
        assert!(build_check_spec(family, &config(&source, &env, &[]))
            .unwrap()
            .commands
            .is_empty());
    }
    assert_eq!(
        commands(
            &build_check_spec(CheckPurpose::Conformance, &config(&source, &env, &[])).unwrap()
        ),
        ["linked-check"]
    );
}
#[test]
fn lifecycle_families_select_scripts_and_preserve_timeouts_cwd_and_context_keys() {
    let source = source();
    let env = ProjectEnvironment::default();
    let hooks = [
        LifecycleHookDef::Script {
            command: "blocking".into(),
            timeout_seconds: 0,
            blocking: true,
        },
        LifecycleHookDef::Script {
            command: "async".into(),
            timeout_seconds: 7,
            blocking: false,
        },
        LifecycleHookDef::Plugin {
            name: "plugin".into(),
            enabled: true,
            config: None,
        },
    ];
    let mut cfg = config(&source, &env, &hooks);
    let spec = build_check_spec(CheckPurpose::BeforeWork, &cfg).unwrap();
    assert_eq!(commands(&spec), ["blocking"]);
    assert_eq!(spec.commands[0].id, "hook:0");
    assert_eq!(spec.commands[0].timeout_seconds, Some(30));
    assert_eq!(spec.commands[0].environment_keys.len(), 11);
    assert!(spec.commands[0]
        .environment_keys
        .contains("FORGE_EXECUTION_ID"));
    assert_eq!(
        spec.commands[0].working_directory,
        CheckWorkingDirectory::TaskRoot
    );
    cfg.has_workspace = false;
    let spec = build_check_spec(CheckPurpose::Lifecycle, &cfg).unwrap();
    assert_eq!(commands(&spec), ["async"]);
    assert_eq!(spec.commands[0].timeout_seconds, Some(7));
    assert_eq!(
        spec.commands[0].working_directory,
        CheckWorkingDirectory::LifecycleFallback
    );
    assert_eq!(
        spec.commands[0].failure_policy,
        CheckFailurePolicy::Continue
    );
    cfg.event = LifecycleEvent::OnWorkStart;
    assert!(build_check_spec(CheckPurpose::BeforeWork, &cfg)
        .unwrap()
        .commands
        .is_empty());
    assert_eq!(
        commands(&build_check_spec(CheckPurpose::Lifecycle, &cfg).unwrap()),
        ["blocking", "async"]
    );
}
#[test]
fn environment_families_preserve_selection_clamping_cwd_and_keys() {
    let source = source();
    let env: ProjectEnvironment = serde_json::from_value(json!({"env":{"LANG":"C"},"checks":[
        {"name":"all","command":"all-check","timeout_seconds":0},
        {"name":"reviewer","command":"review-check","timeout_seconds":999,"roles":["reviewer"]},
        {"name":"coder","command":"coder-check","timeout_seconds":9,"roles":["coder"]}
    ]}))
    .unwrap();
    let cfg = config(&source, &env, &[]);
    for family in [
        CheckPurpose::EnvironmentPreflight,
        CheckPurpose::EnvironmentHelper,
    ] {
        let spec = build_check_spec(family, &cfg).unwrap();
        assert_eq!(commands(&spec), ["all-check", "coder-check"]);
        assert_eq!(spec.commands[0].timeout_seconds, Some(1));
        assert_eq!(spec.commands[1].timeout_seconds, Some(9));
        let cwd = if family == CheckPurpose::EnvironmentPreflight {
            CheckWorkingDirectory::TaskRoot
        } else {
            CheckWorkingDirectory::SuppliedDirectory
        };
        assert!(spec.commands.iter().all(|c| c.working_directory == cwd
            && c.environment_keys == BTreeSet::from(["LANG".into()])
            && c.failure_policy == CheckFailurePolicy::StopBundle));
    }
    let spec = build_check_spec(CheckPurpose::ReadinessProbe, &cfg).unwrap();
    assert_eq!(
        commands(&spec),
        ["all-check", "review-check", "coder-check"]
    );
    assert_eq!(spec.commands[1].timeout_seconds, Some(300));
    assert!(spec.commands.iter().all(|c| c.working_directory
        == CheckWorkingDirectory::RepositoryOrProbeScratch
        && c.failure_policy == CheckFailurePolicy::Continue));
    assert!(build_check_spec(CheckPurpose::AgentSelected, &cfg)
        .unwrap()
        .commands
        .is_empty());
}

#[test]
fn queue_head_contract_can_hold_either_future_gating_policy_without_a_default() {
    let source = source();
    let env = ProjectEnvironment::default();
    let mut cfg = config(&source, &env, &[]);
    assert!(build_check_spec(CheckPurpose::QueueHeadCi, &cfg).is_err());
    cfg.queue_head_bundle = Some(QueueHeadCheckBundle::CiOnly);
    let ci = build_check_spec(CheckPurpose::QueueHeadCi, &cfg).unwrap();
    assert_eq!(commands(&ci), ["task-one", "task-two"]);
    cfg.queue_head_bundle = Some(QueueHeadCheckBundle::Conformance);
    let all = build_check_spec(CheckPurpose::QueueHeadCi, &cfg).unwrap();
    assert_eq!(
        commands(&all),
        ["prepare", "task-one", "task-two", "linked-check"]
    );
    assert_eq!(ci.purpose, all.purpose);
}

#[test]
fn entry_ci_uses_the_current_custom_workflow_hook_state_not_the_review_state() {
    let source = source();
    let environment = ProjectEnvironment::default();
    let hook_state = json!({"ci_steps":["custom-state-ci"]});
    let mut cfg = config(&source, &environment, &[]);
    cfg.entry_state_config = Some(&hook_state);
    assert_eq!(
        commands(&build_check_spec(CheckPurpose::EntryCi, &cfg).unwrap()),
        ["custom-state-ci"]
    );
    assert_eq!(
        commands(&build_check_spec(CheckPurpose::ReviewCi, &cfg).unwrap()),
        ["task-one", "task-two"]
    );
    // The hook reads a nested `review` object before the state's own keys.
    let nested = json!({"ci_steps":["outer"],"review":{"ci_steps":["nested-ci"]}});
    cfg.entry_state_config = Some(&nested);
    assert_eq!(
        commands(&build_check_spec(CheckPurpose::EntryCi, &cfg).unwrap()),
        ["nested-ci"]
    );
    let not_strings = json!({"ci_steps":[1]});
    cfg.entry_state_config = Some(&not_strings);
    assert_eq!(
        build_check_spec(CheckPurpose::EntryCi, &cfg).unwrap_err(),
        "review ci_steps entries must be strings"
    );
}

#[test]
fn owner_hook_test_and_single_environment_helper_do_not_apply_bulk_filters() {
    let source = source();
    let env:ProjectEnvironment=serde_json::from_value(json!({"checks":[{"name":"reviewer", "command":"review-check", "roles":["reviewer"], "timeout_seconds":6}]})).unwrap();
    let hooks = [LifecycleHookDef::Script {
        command: "blocking".into(),
        timeout_seconds: 5,
        blocking: true,
    }];
    let mut cfg = config(&source, &env, &hooks);
    cfg.hook_test_index = Some(0);
    let spec = build_check_spec(CheckPurpose::Lifecycle, &cfg).unwrap();
    assert_eq!(commands(&spec), ["blocking"]);
    assert_eq!(spec.commands[0].id, "hook:0");
    cfg.single_environment_check = Some(&env.checks[0]);
    let spec = build_check_spec(CheckPurpose::EnvironmentHelper, &cfg).unwrap();
    assert_eq!(commands(&spec), ["review-check"]);
    assert_eq!(spec.commands[0].timeout_seconds, Some(6));
}
