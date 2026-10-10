use super::*;

#[test]
fn effects_have_no_persistence_or_task_capability_imports() {
    // Check every production source, not just the module root. A module
    // boundary is used because the existing error/transport types are services
    // types; introducing a new crate would broaden this extraction.
    for (name, source) in [
        ("mod", include_str!("mod.rs")),
        ("types", include_str!("types.rs")),
        ("merge", include_str!("merge.rs")),
        ("rebase", include_str!("rebase.rs")),
        ("check", include_str!("check.rs")),
        ("rpc", include_str!("rpc.rs")),
    ] {
        for forbidden in [
            "db::",
            "sqlx::",
            "events::",
            "SqliteDb",
            "EventBus",
            "TaskService",
            "MergeService",
            "DaemonWorkspaceClient",
            "WorkspaceBackendRouter",
            "TaskRepo",
            "ExecutionRepo",
            "ReviewRepo",
            "task_service::",
            "workflow::",
        ] {
            assert!(
                !source.contains(forbidden),
                "{name} acquired forbidden capability {forbidden}"
            );
        }
        // Substrings miss a capability imported by name through an allowed
        // module (`workspace_backend::ResolvedWorkspace`), so also check
        // identifiers in the code, without its comments.
        let code = source
            .lines()
            .map(|line| line.split("//").next().unwrap_or_default())
            .collect::<Vec<_>>()
            .join("\n");
        for glob in ["crate::*", "super::super", "workspace_backend::*"] {
            assert!(!code.contains(glob), "{name} widens its imports: {glob}");
        }
        for token in code.split(|c: char| !(c.is_alphanumeric() || c == '_')) {
            assert!(
                ![
                    "db",
                    "sqlx",
                    "events",
                    "pool",
                    "task_writer",
                    "task_service",
                    "workflow",
                    "HookContext",
                    "ResolvedWorkspace",
                    "WorkspaceBackend",
                    "DaemonWorkspaceBackend",
                    "EmbeddedWorkspaceBackend",
                    "ReviewRunner",
                ]
                .contains(&token)
                    && !token.ends_with("Repo")
                    && !token.ends_with("Service"),
                "{name} acquired forbidden capability {token}"
            );
        }
    }
    // The socket primitive receives one connection and has no registry capability.
    let rpc = include_str!("rpc.rs");
    assert!(!rpc.contains("DaemonConnectionRegistry"));
    assert!(!rpc.contains("registry."));
}

#[test]
fn check_sequence_preserves_deadline_redaction_exit_and_stop() {
    let workspace = EffectWorkspace {
        workspace_id: "workspace".into(),
        placement_id: "placement".into(),
        generation: 7,
        owner: EffectOwner::Server,
        handle: "handle".into(),
    };
    let commands = vec!["first".into(), "second".into(), "never".into()];
    let env = std::collections::BTreeMap::from([("SECRET".into(), "hidden-value".into())]);
    let mut run = check::CheckRun::new(check::CheckRunInput {
        workspace: &workspace,
        commands: &commands,
        purpose: api_types::WorkspaceRunPurpose::CiStep,
        environment: &env,
        deadline: None,
        max_output_bytes: usize::MAX,
    });
    let first = run.next_command().unwrap();
    assert_eq!(first.index, 0);
    assert_eq!(first.spec.timeout_secs, 0);
    assert_eq!(first.spec.env, env);
    run.completed(
        first,
        RunResult {
            exit_code: 0,
            stdout_tail: "hidden-value".into(),
            stderr_tail: String::new(),
            duration_ms: 0,
        },
    );
    let second = run.next_command().unwrap();
    assert_eq!(second.index, 1);
    run.completed(
        second,
        RunResult {
            exit_code: -1,
            stdout_tail: "é".repeat(3000),
            stderr_tail: "hidden-value".into(),
            duration_ms: 0,
        },
    );
    assert!(run.next_command().is_none());
    let result = run.outcome();
    assert_eq!(result.failed_step_index, Some(1));
    assert_eq!(result.commands.len(), 2);
    assert_eq!(result.commands[1].exit_code, 1);
    assert!(result.commands[1].output_tail.len() <= 4096);
    assert!(!result.commands[0].output_tail.contains("hidden-value"));
    assert!(!result.commands[1].stderr_tail.contains("hidden-value"));
    assert!(result
        .commands
        .iter()
        .all(|step| step.started_at <= step.finished_at));
}

struct Facts {
    responses: std::sync::Mutex<std::collections::VecDeque<Option<String>>>,
    queries: std::sync::Mutex<Vec<api_types::WorkspaceGitQuery>>,
}
#[async_trait::async_trait]
impl rebase::GitFacts for Facts {
    async fn query(
        &self,
        query: api_types::WorkspaceGitQuery,
        _optional: bool,
    ) -> crate::Result<Option<String>> {
        self.queries.lock().unwrap().push(query);
        Ok(self
            .responses
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected Git probe"))
    }
}

#[tokio::test]
async fn stopped_rebase_is_checked_before_ancestry_and_committed_paths_are_unioned() {
    use api_types::{WorkspaceGitQuery, WorkspaceOwnerOperationOutcome};
    let facts = Facts {
        responses: std::sync::Mutex::new(
            [Some("true".into()), Some("old.txt\nnew.txt".into())].into(),
        ),
        queries: Default::default(),
    };
    let result = rebase::recover_rebase(
        &facts,
        &rebase::RebaseRecoveryInput {
            previous_target: Some("target"),
            recorded_target: "target",
            handoff_conflicts: true,
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        result,
        rebase::RebaseRecoveryOutcome::Perform { in_progress: true }
    ));
    assert_eq!(
        *facts.queries.lock().unwrap(),
        vec![WorkspaceGitQuery::RebaseInProgress]
    );
    let result = rebase::finish_rebase_recovery(
        &facts,
        "target",
        true,
        true,
        WorkspaceOwnerOperationOutcome::Conflict {
            details: "resumed interrupted rebase".into(),
            conflict_paths: vec!["new.txt".into()],
        },
    )
    .await
    .unwrap();
    assert!(
        matches!(result, WorkspaceOwnerOperationOutcome::Conflict { conflict_paths, .. } if conflict_paths == vec!["new.txt", "old.txt"])
    );
}

#[tokio::test]
async fn committed_rebase_reconstructs_handoff_without_another_effect() {
    use api_types::WorkspaceOwnerOperationOutcome;
    let facts = Facts {
        responses: std::sync::Mutex::new(
            [
                Some("false".into()),
                Some("".into()),
                Some("file.txt".into()),
            ]
            .into(),
        ),
        queries: Default::default(),
    };
    let result = rebase::recover_rebase(
        &facts,
        &rebase::RebaseRecoveryInput {
            previous_target: Some("target"),
            recorded_target: "target",
            handoff_conflicts: true,
        },
    )
    .await
    .unwrap();
    assert!(
        matches!(result, rebase::RebaseRecoveryOutcome::Recorded(WorkspaceOwnerOperationOutcome::Conflict { details, conflict_paths })
        if details == "resumed committed conflict handoff" && conflict_paths == vec!["file.txt"])
    );
}

#[test]
fn rebase_head_facts_keep_the_observed_object_or_the_existing_missing_head_error() {
    let state = |head_sha| crate::workspace_backend::WorkspaceState {
        exists: true,
        head_sha,
        dirty: false,
        branch: None,
        locked: false,
        active_execution_ids: vec![],
        journaled_execution_ids: vec![],
    };
    let facts = rebase::rebase_head_facts(state(Some("rebased-object".into()))).unwrap();
    assert_eq!(facts.head_sha, "rebased-object");
    assert!(
        matches!(rebase::rebase_head_facts(state(None)), Err(crate::ServiceError::InvalidOperation { message }) if message == "rebased workspace has no HEAD")
    );
}

#[test]
fn canonical_ci_spec_matches_the_existing_check_run_input_and_sequence() {
    let source = serde_json::json!({
        "task_id":"task", "project_id":"project", "repo_id":"repo",
        "task_scope":{"task_type":"task", "config":{"review":{"ci_steps":["first","second"]}}}
    });
    let env = std::collections::BTreeMap::from([("LANG".into(), "C".into())]);
    let environment = api_types::ProjectEnvironment {
        env: env.clone(),
        ..Default::default()
    };
    let spec = review::check_spec::build_check_spec(
        api_types::CheckPurpose::EntryCi,
        &review::check_spec::CheckSpecConfiguration {
            source: &source,
            entry_state_config: None,
            environment: &environment,
            hooks: &[],
            hook_test_index: None,
            single_environment_check: None,
            event: api_types::LifecycleEvent::BeforeWork,
            role: "coder",
            owner_is_daemon: false,
            canonical_policy: false,
            workspace: Some(review::check_spec::CheckWorkspaceIdentity {
                workspace_id: "workspace",
                generation: 1,
            }),
            queue_head_bundle: None,
        },
    )
    .unwrap();
    let workspace = EffectWorkspace {
        workspace_id: "workspace".into(),
        placement_id: "placement".into(),
        generation: 1,
        owner: EffectOwner::Server,
        handle: "handle".into(),
    };
    let commands = vec!["first".into(), "second".into()];
    let mut run = check::CheckRun::new(check::CheckRunInput {
        workspace: &workspace,
        commands: &commands,
        purpose: api_types::WorkspaceRunPurpose::CiStep,
        environment: &env,
        deadline: None,
        max_output_bytes: usize::MAX,
    });
    for expected in &spec.commands {
        let actual = run.next_command().unwrap();
        assert_eq!(actual.spec.command, expected.shell_text);
        assert_eq!(
            actual
                .spec
                .env
                .keys()
                .cloned()
                .collect::<std::collections::BTreeSet<_>>(),
            expected.environment_keys
        );
        assert_eq!(
            actual.spec.timeout_secs,
            expected.timeout_seconds.unwrap_or(0)
        );
        assert_eq!(
            expected.working_directory,
            api_types::CheckWorkingDirectory::TaskRoot
        );
        assert_eq!(
            expected.failure_policy,
            api_types::CheckFailurePolicy::StopBundle
        );
        run.completed(
            actual,
            RunResult {
                exit_code: 0,
                stdout_tail: String::new(),
                stderr_tail: String::new(),
                duration_ms: 0,
            },
        );
    }
    assert!(run.next_command().is_none());
    let outcome = run.outcome();
    // Storage consumes the very same command outcome array, with no conversion
    // into a competing CI representation and no write from the effect itself.
    let evidence = db::CheckResultEvidence {
        outcome: db::CheckResultOutcome::Pass,
        cleanup: db::CheckCleanup::Success,
        commands: outcome.commands,
        output_truncated: false,
        redaction_values: vec![],
        reusable: true,
    };
    assert_eq!(evidence.commands.len(), spec.commands.len());
}

#[tokio::test]
async fn subsecond_check_deadline_is_bounded_instead_of_becoming_zero() {
    let workspace = EffectWorkspace {
        workspace_id: "w".into(),
        placement_id: "p".into(),
        generation: 1,
        owner: EffectOwner::Server,
        handle: "h".into(),
    };
    let commands = vec!["exec sleep 20".into()];
    let env = Default::default();
    for bound in [4096, usize::MAX] {
        let run = check::CheckRun::new(check::CheckRunInput {
            workspace: &workspace,
            commands: &commands,
            purpose: api_types::WorkspaceRunPurpose::CiStep,
            environment: &env,
            deadline: Some(std::time::Duration::from_millis(10)),
            max_output_bytes: bound,
        });
        let command = run.next_command().unwrap();
        assert_eq!(command.spec.timeout_secs, 1);
        let dir = tempfile::tempdir().unwrap();
        let start = std::time::Instant::now();
        assert!(check::run_at(dir.path(), &command.spec).await.is_err());
        assert!(start.elapsed() < std::time::Duration::from_secs(5));
    }
}
