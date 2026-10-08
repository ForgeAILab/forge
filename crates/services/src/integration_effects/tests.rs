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
    }
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
