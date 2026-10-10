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
        canonical_policy: false,
        workspace: Some(CheckWorkspaceIdentity {
            workspace_id: "w",
            generation: 1,
        }),
        queue_head_bundle: None,
    }
}
/// The execution digest of a built spec on an unattested owner.
fn digest(spec: &CheckSpec) -> String {
    CheckDigestInput {
        spec: spec.clone(),
        environment: spec
            .commands
            .iter()
            .flat_map(|command| command.environment_keys.iter().cloned())
            .map(|key| (key, CheckEnvironmentValue::Volatile))
            .collect(),
        environment_identity: CheckEnvironmentIdentity::NotAttested,
        execution_revision: CheckExecutionRevision {
            number: 0,
            audit_ref: None,
        },
    }
    .digest()
    .unwrap()
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
    cfg.workspace = None;
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
    // Queue-head CI is entry CI's bundle: one identity, whoever asks.
    assert_eq!(
        digest(&ci),
        digest(&build_check_spec(CheckPurpose::EntryCi, &cfg).unwrap())
    );
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

fn every_family_fixture() -> (Value, ProjectEnvironment, [LifecycleHookDef; 2]) {
    let environment = serde_json::from_value(
        json!({"checks":[{"name":"probe","command":"probe-check","timeout_seconds":5}]}),
    )
    .unwrap();
    let hooks = [
        LifecycleHookDef::Script {
            command: "prepare-worktree".into(),
            timeout_seconds: 5,
            blocking: true,
        },
        LifecycleHookDef::Script {
            command: "notify".into(),
            timeout_seconds: 5,
            blocking: false,
        },
    ];
    (source(), environment, hooks)
}

#[test]
fn worktree_families_carry_the_workspace_identity_and_commit_families_never_do() {
    use CheckFamilyScope::*;
    let (with_setup, environment, hooks) = every_family_fixture();
    let mut without_setup = with_setup.clone();
    without_setup["project_settings"]["default_review_config"]["setup_steps"] = json!([]);
    // The whole classification. `Probe` families follow the workspace only
    // when they run in one.
    let families = [
        (CheckPurpose::EntryCi, &with_setup, None, Commit),
        (CheckPurpose::ReviewCi, &with_setup, None, Commit),
        (CheckPurpose::AgentSelected, &with_setup, None, Commit),
        (
            CheckPurpose::QueueHeadCi,
            &with_setup,
            Some(QueueHeadCheckBundle::CiOnly),
            Commit,
        ),
        (CheckPurpose::Conformance, &without_setup, None, Commit),
        (
            CheckPurpose::QueueHeadCi,
            &without_setup,
            Some(QueueHeadCheckBundle::Conformance),
            Commit,
        ),
        (CheckPurpose::Conformance, &with_setup, None, Worktree),
        (
            CheckPurpose::QueueHeadCi,
            &with_setup,
            Some(QueueHeadCheckBundle::Conformance),
            Worktree,
        ),
        (CheckPurpose::BeforeWork, &with_setup, None, Worktree),
        (CheckPurpose::Lifecycle, &with_setup, None, Worktree),
        (
            CheckPurpose::EnvironmentPreflight,
            &with_setup,
            None,
            Worktree,
        ),
        (CheckPurpose::ReadinessProbe, &with_setup, None, Probe),
        (CheckPurpose::EnvironmentHelper, &with_setup, None, Probe),
    ];
    for purpose in CheckPurpose::ALL {
        assert!(families.iter().any(|(family, ..)| family == purpose));
    }
    for (purpose, source, bundle, expected) in families {
        let build = |workspace: Option<(&str, u64)>, task: &str| {
            let mut source = source.clone();
            source["task_id"] = json!(task);
            let mut cfg = config(&source, &environment, &hooks);
            cfg.event = LifecycleEvent::BeforeWork;
            cfg.queue_head_bundle = bundle;
            cfg.workspace = workspace.map(|(workspace_id, generation)| CheckWorkspaceIdentity {
                workspace_id,
                generation,
            });
            build_check_spec(purpose, &cfg).unwrap()
        };
        let has_setup = std::ptr::eq(source, &with_setup);
        assert_eq!(
            check_family_scope(purpose, bundle, has_setup),
            expected,
            "{purpose:?}"
        );
        let first = build(Some(("w", 1)), "t");
        let other_workspace = digest(&build(Some(("w2", 1)), "t2"));
        let other_generation = digest(&build(Some(("w", 2)), "t"));
        let no_workspace = build(None, "t");
        let no_workspace_other_task = build(None, "t2");
        match expected {
            Commit => {
                assert_eq!(first.scope, CheckScope::Commit, "{purpose:?}");
                // Neither the workspace nor the Task reaches the identity.
                assert_eq!(digest(&first), other_workspace, "{purpose:?}");
                assert_eq!(digest(&first), other_generation, "{purpose:?}");
                assert_eq!(digest(&first), digest(&no_workspace), "{purpose:?}");
                assert_eq!(
                    digest(&first),
                    digest(&no_workspace_other_task),
                    "{purpose:?}"
                );
            }
            Worktree | Probe => {
                assert_eq!(
                    first.scope,
                    CheckScope::Workspace {
                        workspace_id: "w".into(),
                        generation: 1
                    },
                    "{purpose:?}"
                );
                assert_ne!(digest(&first), other_workspace, "{purpose:?}");
                assert_ne!(digest(&first), other_generation, "{purpose:?}");
                if expected == Probe {
                    assert_eq!(no_workspace.scope, CheckScope::Commit);
                } else {
                    // No worktree: still one run per Task, never shared.
                    assert_eq!(
                        no_workspace.scope,
                        CheckScope::Task {
                            task_id: "t".into()
                        }
                    );
                    assert_ne!(digest(&no_workspace), digest(&no_workspace_other_task));
                }
            }
        }
    }
    // A worktree family with neither a workspace nor a Task has no identity.
    let mut anonymous = with_setup.clone();
    anonymous.as_object_mut().unwrap().remove("task_id");
    let mut cfg = config(&anonymous, &environment, &hooks);
    cfg.workspace = None;
    assert!(build_check_spec(CheckPurpose::BeforeWork, &cfg).is_err());
    assert!(build_check_spec(CheckPurpose::EntryCi, &cfg).is_ok());
}

#[test]
fn purpose_selects_the_bundle_and_is_not_in_the_spec_or_its_digest() {
    let (source, environment, hooks) = every_family_fixture();
    let mut cfg = config(&source, &environment, &hooks);
    let entry = build_check_spec(CheckPurpose::EntryCi, &cfg).unwrap();
    let review = build_check_spec(CheckPurpose::ReviewCi, &cfg).unwrap();
    cfg.queue_head_bundle = Some(QueueHeadCheckBundle::CiOnly);
    let queue_head = build_check_spec(CheckPurpose::QueueHeadCi, &cfg).unwrap();
    assert_eq!(entry, review);
    assert_eq!(entry, queue_head);
    assert_eq!(digest(&entry), digest(&review));
    assert_eq!(digest(&entry), digest(&queue_head));
    assert!(!serde_json::to_string(&entry).unwrap().contains("purpose"));
    // No family configured today declares a cleanup step.
    for purpose in CheckPurpose::ALL {
        assert!(!build_check_spec(*purpose, &cfg).unwrap().declares_cleanup);
    }
}

#[test]
fn blank_steps_are_dropped_recorded_and_an_all_blank_list_is_the_empty_auto_pass() {
    let environment = ProjectEnvironment::default();
    let build = |steps: Value| {
        let mut source = source();
        source["task_scope"]["config"]["review"]["ci_steps"] = steps;
        let entry =
            build_check_spec(CheckPurpose::EntryCi, &config(&source, &environment, &[])).unwrap();
        let review =
            build_check_spec(CheckPurpose::ReviewCi, &config(&source, &environment, &[])).unwrap();
        assert_eq!(entry, review);
        entry
    };
    let spec = build(json!(["first", "", "  \t", "second", "\n"]));
    assert_eq!(commands(&spec), ["first", "second"]);
    assert_eq!(spec.commands[1].id, "ci:1");
    // Evidence can still say "step 3 of 5 was blank".
    assert_eq!(spec.configured_commands, 5);
    assert_eq!(spec.blank_commands, [1, 2, 4]);
    let plain = build(json!(["first", "second"]));
    assert_eq!(
        (plain.configured_commands, plain.blank_commands.len()),
        (2, 0)
    );
    assert_eq!(digest(&spec), digest(&plain));

    let all_blank = build(json!(["", " "]));
    let empty = build(json!([]));
    assert!(all_blank.commands.is_empty() && empty.commands.is_empty());
    assert_eq!(all_blank.blank_commands, [0, 1]);
    assert_eq!(digest(&all_blank), digest(&empty));
    assert!(!all_blank.cacheable() && !empty.cacheable());

    // Blank hook and environment-check commands are dropped the same way.
    let hooks = [LifecycleHookDef::Script {
        command: " ".into(),
        timeout_seconds: 5,
        blocking: true,
    }];
    let source = source();
    let spec = build_check_spec(
        CheckPurpose::BeforeWork,
        &config(&source, &environment, &hooks),
    )
    .unwrap();
    assert!(spec.commands.is_empty());
    assert_eq!(
        (spec.configured_commands, spec.blank_commands),
        (1, vec![0])
    );
}

#[test]
fn the_canonical_policy_keeps_the_steps_and_is_its_own_worktree_scoped_identity() {
    let source = source();
    let environment: ProjectEnvironment =
        serde_json::from_value(json!({"env":{"TOKEN":"not-in-spec"}})).unwrap();
    let mut cfg = config(&source, &environment, &[]);
    let frozen = build_check_spec(CheckPurpose::EntryCi, &cfg).unwrap();
    cfg.canonical_policy = true;
    let canonical = build_check_spec(CheckPurpose::EntryCi, &cfg).unwrap();
    assert_eq!(canonical.execution_policy, CANONICAL_CI_POLICY);
    assert_eq!(commands(&canonical), commands(&frozen));
    assert!(canonical.cacheable() && !frozen.cacheable());
    for (command, old) in canonical.commands.iter().zip(&frozen.commands) {
        // Nothing is cleared, so nothing more is declared: the Project keys,
        // the limit and the failure rule are the frozen policy's.
        assert_eq!(command.environment_keys, BTreeSet::from(["TOKEN".into()]));
        assert_eq!(command.environment_keys, old.environment_keys);
        assert_eq!(command.timeout_seconds, None);
        assert_eq!(command.failure_policy, CheckFailurePolicy::StopBundle);
    }
    // The run sees its worktree's ignored files: the worktree is in the
    // identity, so a result never answers for another checkout.
    assert_eq!(frozen.scope, CheckScope::Commit);
    assert_eq!(
        canonical.scope,
        CheckScope::Workspace {
            workspace_id: "w".into(),
            generation: 1
        }
    );
    // No steps is still the empty auto-pass: nothing runs, nothing is reused.
    let mut none = source.clone();
    none["task_scope"]["config"]["review"]["ci_steps"] = json!([]);
    let mut empty = config(&none, &environment, &[]);
    empty.canonical_policy = true;
    let empty = build_check_spec(CheckPurpose::EntryCi, &empty).unwrap();
    assert!(empty.commands.is_empty() && !empty.cacheable());
    // Only entry CI has the canonical policy: asking elsewhere is an error.
    assert!(build_check_spec(CheckPurpose::ReviewCi, &cfg).is_err());
    assert!(build_check_spec(CheckPurpose::Conformance, &cfg).is_err());
    // A daemon-owned checkout, or no worktree, is an ordinary placement: the
    // spec is built with its owner's frozen policy, which always runs.
    let mut daemon = config(&source, &environment, &[]);
    daemon.owner_is_daemon = true;
    let plain = build_check_spec(CheckPurpose::EntryCi, &daemon).unwrap();
    daemon.canonical_policy = true;
    let fallback = build_check_spec(CheckPurpose::EntryCi, &daemon).unwrap();
    assert_eq!(fallback, plain);
    assert_eq!(fallback.execution_policy, "legacy-daemon/1");
    assert!(!fallback.cacheable());
    cfg.workspace = None;
    let bare = build_check_spec(CheckPurpose::EntryCi, &cfg).unwrap();
    assert_eq!(bare.execution_policy, "legacy-server/1");
    assert_eq!(bare, {
        cfg.canonical_policy = false;
        build_check_spec(CheckPurpose::EntryCi, &cfg).unwrap()
    });
}
