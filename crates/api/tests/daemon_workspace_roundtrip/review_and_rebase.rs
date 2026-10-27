//! Remote review evidence, CI results, rebases and step-check prompts.

use super::*;

#[tokio::test]
async fn remote_review_uses_owner_evidence_checkouts_and_bounded_ci_output() {
    let fixture = Fixture::new("forge-workspace-review").await;
    let candidate = fixture.candidate().await;
    let env = BTreeMap::from([("FORGE_TEST_SECRET".into(), "secret-value".into())]);
    let output = ReviewWorkspace::run(&fixture.resolved,
        "head -c 700000 /dev/zero | tr '\\000' x; printf '%s' \"$FORGE_TEST_SECRET\"; printf 'stderr' >&2", &env, None)
        .await.expect("bounded workspace command collects its stream");
    assert_eq!(output.exit_code, Some(0));
    assert_eq!(output.stdout.len(), 700000 + "[REDACTED]".len());
    assert!(!output.stdout.contains("secret-value"));
    assert_eq!(output.stderr, "stderr");
    let run = fixture
        .link
        .as_ref()
        .unwrap()
        .requests(METHOD_WORKSPACE_RUN)
        .pop()
        .unwrap();
    assert_eq!(run["timeout_secs"], 1800);
    assert_eq!(run["max_output_bytes"], json!(1024 * 1024));
    let receipt = fixture.receipt(METHOD_WORKSPACE_RUN).await;
    assert!(!receipt["owner_result"]["stdout"]
        .as_str()
        .unwrap()
        .contains("secret-value"));
    let diff = ReviewWorkspace::diff(&fixture.resolved, "main")
        .await
        .unwrap();
    assert!(diff.contains("+candidate"));
    let paths = fixture
        .resolved
        .git_query(
            WorkspaceGitQuery::CandidatePaths {
                base_sha: fixture.base_sha.clone(),
                commit_sha: candidate.clone(),
            },
            false,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(paths, "feature.txt\0");
    assert_eq!(
        fixture
            .resolved
            .backend
            .read(&fixture.resolved.placement, "feature.txt", 1024)
            .await
            .unwrap(),
        b"candidate\n"
    );
    let asset = fixture._daemon_root.path().join("fixture.asset");
    std::fs::write(&asset, "owner asset").unwrap();
    let environment = ProjectEnvironment {
        recheck_interval_seconds: 600,
        env: BTreeMap::new(),
        assets: vec![EnvironmentAsset {
            source: asset.to_string_lossy().into_owned(),
            target: "assets/seed".into(),
        }],
        checks: vec![],
    };
    fixture
        .resolved
        .materialize_assets(&environment)
        .await
        .unwrap();
    assert_eq!(
        fixture
            .resolved
            .backend
            .read(&fixture.resolved.placement, "assets/seed", 1024)
            .await
            .unwrap(),
        b"owner asset"
    );
    assert_eq!(
        fixture
            .run("printf 'dirty\n' > feature.txt; printf 'artifact' > untracked-build")
            .await
            .exit_code,
        0
    );
    let checkout = fixture
        .resolved
        .clean_checkout(&candidate, &environment, true)
        .await
        .unwrap();
    let clean = checkout.run("test ! -e untracked-build && test \"$(cat feature.txt)\" = candidate && test \"$(cat assets/seed)\" = 'owner asset'",
        &BTreeMap::new(), Some(CommandLimits { timeout_secs: 2, max_output_bytes: 1024 })).await.unwrap();
    assert_eq!(clean.exit_code, Some(0));
    checkout.close().await.unwrap();
    fixture.resolved.restore(&candidate).await.unwrap();
    assert_eq!(
        fixture
            .resolved
            .git_query(WorkspaceGitQuery::TrackedChanges, false)
            .await
            .unwrap()
            .unwrap()
            .trim(),
        ""
    );
    let bounded = fixture
        .resolved
        .run(
            "printf 'abcdefghijklmnopqrstuvwxyz'",
            &BTreeMap::new(),
            Some(CommandLimits {
                timeout_secs: 2,
                max_output_bytes: 16,
            }),
        )
        .await;
    assert!(bounded
        .err()
        .unwrap()
        .to_string()
        .contains("output exceeds"));
    for purpose in [
        WorkspaceRunPurpose::Hook,
        WorkspaceRunPurpose::EnvironmentSetup,
    ] {
        let output = fixture
            .resolved
            .backend
            .run(
                &fixture.resolved.placement,
                &RunSpec {
                    purpose,
                    command: "test -f feature.txt && printf owner".into(),
                    env: BTreeMap::new(),
                    timeout_secs: 2,
                    max_output_bytes: 1024,
                },
            )
            .await
            .unwrap();
        assert_eq!(output.exit_code, 0);
        assert_eq!(output.stdout_tail, "owner");
    }
    fixture.assert_server_cannot_resolve_workspace();
    fixture.wait_until_journal_empty().await;
}

#[tokio::test]
async fn remote_ci_pass_and_failure_record_reviews_and_events() {
    use services::workflow::{default_workflow, HookContext, HookResult};
    // Review-entry CI of a daemon-placed Task is a check the durable runner
    // executes on that daemon (legacy policy: always runs, never reused). A
    // run the daemon cannot execute is no verdict and parks the Task on the
    // typed check condition; that path is covered by the check-runner tests.
    for case in ["pass", "fail"] {
        let fixture = Fixture::new("forge-ci-characterization").await;
        let state = &fixture.harness.state;
        let task_id = &fixture.resolved.placement.task_id;
        let task = TaskRepo::get_by_id(&*state.db, task_id, false)
            .await
            .unwrap()
            .unwrap();
        let project = ProjectRepo::get_by_id(&*state.db, &task.project_id)
            .await
            .unwrap()
            .unwrap();
        let mut events = state.event_bus.subscribe();
        let command = if case == "fail" {
            "printf failure; printf diagnostic >&2; exit 7"
        } else {
            "printf first"
        };
        let ctx = HookContext {
            task_id: task_id.clone(),
            project_id: task.project_id.clone(),
            from_state: "in_progress".into(),
            to_state: "review".into(),
            db: Arc::clone(&state.db),
            event_bus: Arc::clone(&state.event_bus),
            gate_config: None,
            workflow: Arc::new(default_workflow::default_workflow()),
            project_version: Some(project.version),
            project_workflow_definition: Some(project.workflow_definition),
            triggered_by: Actor::system(SystemComponent::Test),
            merge_service: None,
            cleanup_scheduler: None,
            task_service: state.task_service.as_ref().clone(),
            daemon_connections: None,
            workspace_exec_locks: None,
            terminal_activity: None,
            workspace_root: fixture.server_root.path().join("workspaces"),
            repo_cache_locks: None,
            workspace_backend_router: Arc::clone(&state.workspace_backend_router),
            workspace_id: Some(fixture.resolved.placement.workspace_id.clone()),
            agent_id: None,
            execution_id: Some(fixture.execution_id.clone()),
            state_config: json!({"ci_steps": [command, "printf second"]}),
        };
        let worker = state
            .task_service
            .check_worker()
            .expect("the runtime composes the check worker");
        let result = services::workflow::actions::run_ci_steps_in_step(&ctx, &worker).await;
        let reviews = ReviewRepo::list_by_task(&*state.db, task_id).await.unwrap();
        assert_eq!(reviews.len(), 1, "{case}: {result:?}");
        let details: Value = serde_json::from_str(&reviews[0].step_results_json).unwrap();
        let task = TaskRepo::get_by_id(&*state.db, task_id, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.status, "review");
        match case {
            "pass" => {
                assert!(matches!(result, HookResult::Ok));
                assert_eq!(reviews[0].status, ReviewStatus::Passed);
                assert_eq!(details["ci_steps"].as_array().unwrap().len(), 2);
                assert!(task.review_passed_at.is_some());
                assert_eq!(details["ci_steps"][1]["output_tail"], "second");
            }
            "fail" => {
                assert!(matches!(result, HookResult::Failed { .. }));
                assert_eq!(reviews[0].status, ReviewStatus::Failed);
                // Default full triage collects every step and reruns the red
                // one; a daemon placement has no base comparison.
                assert_eq!(details["ci_steps"].as_array().unwrap().len(), 2);
                assert_eq!(
                    details["ci_steps"][0]["triage"]["base_status"],
                    "base_unavailable"
                );
                assert_eq!(details["ci_steps"][0]["exit_code"], 7);
                assert_eq!(details["ci_steps"][0]["output_tail"], "failure\ndiagnostic");
                assert!(task.review_passed_at.is_none());
            }
            _ => unreachable!(),
        }
        // The whole entry is ONE `check.run` on the owning daemon under the
        // daemon's frozen policy (always runs, never reused), bounded by an
        // absolute deadline; no command travels as `workspace.run` any more.
        let link = fixture.link.as_ref().unwrap();
        let runs = link.requests(api_types::METHOD_CHECK_RUN);
        assert_eq!(
            runs.len(),
            if case == "fail" { 2 } else { 1 },
            "{case}: one check.run per entry, plus the red step's triage rerun"
        );
        assert_eq!(
            runs[0]["purpose"],
            serde_json::to_value(api_types::WorkspaceRunPurpose::CiStep).unwrap()
        );
        assert_eq!(runs[0]["spec"]["execution_policy"], "legacy-daemon/1");
        assert_eq!(
            runs[0]["spec"]["commands"].as_array().unwrap().len(),
            2,
            "{case}: every configured step is in the spec"
        );
        assert!(runs[0]["deadline"].is_string());
        assert!(
            runs[0]["target"]
                .to_string()
                .contains(&fixture.resolved.placement.id),
            "{case}: the run targets the Task's own placement"
        );
        assert!(link.requests(api_types::METHOD_WORKSPACE_RUN).is_empty());
        // A CI-only failure posts its finding checklist; a pass posts nothing.
        let comments: Vec<String> =
            sqlx::query_scalar("SELECT content FROM task_comment WHERE task_id=?")
                .bind(task_id)
                .fetch_all(state.db.pool())
                .await
                .unwrap();
        match case {
            "pass" => assert!(comments.is_empty(), "{comments:?}"),
            _ => {
                assert_eq!(comments.len(), 1, "{comments:?}");
                assert!(comments[0].starts_with("Review failed"), "{comments:?}");
            }
        }
        let mut review_events = Vec::new();
        while let Ok(event) = events.try_recv() {
            if event.event_type.starts_with("review.") {
                review_events.push(event.event_type);
            }
        }
        assert_eq!(
            review_events,
            match case {
                "pass" => vec!["review.passed"],
                _ => vec!["review.failed"],
            }
        );
        assert_eq!(
            fixture
                .resolved
                .git_query(
                    WorkspaceGitQuery::TargetHead {
                        branch: "main".into()
                    },
                    false
                )
                .await
                .unwrap()
                .unwrap()
                .trim(),
            fixture.base_sha
        );
        fixture.wait_until_journal_empty().await;
    }
}

#[tokio::test]
async fn remote_rebase_clean_conflict_and_interrupted_recovery_preserve_projection() {
    for case in ["clean", "conflict", "restart", "abort"] {
        let mut fixture = Fixture::new("forge-workspace-rebase-characterization").await;
        fixture.candidate().await;
        let target = quote(&fixture.checkout.to_string_lossy());
        let file = if case == "clean" {
            "sibling.txt"
        } else {
            "feature.txt"
        };
        assert_eq!(fixture.run(&format!("printf 'target\\n' > {target}/{file}; git -C {target} add {file}; git -C {target} commit -m target")).await.exit_code, 0);
        if matches!(case, "restart" | "abort") {
            assert_ne!(fixture.run("git rebase main").await.exit_code, 0);
        }
        if case == "restart" {
            fixture.wait_until_journal_empty().await;
            fixture.link.take();
            common::fake_daemon::wait_until_disconnected(
                &fixture.harness.state,
                &fixture.daemon_id,
            )
            .await;
            let (outbound, _discarded) = mpsc::unbounded_channel();
            fixture.runtime = DaemonRuntime::new_owned(
                outbound,
                fixture._daemon_root.path().to_path_buf(),
                ActiveExecutionTracker::default(),
                fixture.daemon_id.clone(),
                WorkspaceRunPolicy {
                    allowed_purposes: vec![
                        WorkspaceRunPurpose::CiStep,
                        WorkspaceRunPurpose::Hook,
                        WorkspaceRunPurpose::EnvironmentSetup,
                    ],
                },
            )
            .unwrap();
            fixture.reconnect().await;
        }
        let before = task_projection(&fixture).await;
        let result = fixture
            .resolved
            .rebase_target("main", case != "abort")
            .await
            .unwrap();
        match case {
            "clean" => assert!(matches!(result, WorkspaceOwnerOperationOutcome::Rebased)),
            "abort" => assert!(
                matches!(result, api_types::WorkspaceOwnerOperationOutcome::Conflict { details, conflict_paths } if details == "aborted interrupted rebase" && conflict_paths.is_empty())
            ),
            _ => assert!(
                matches!(result, WorkspaceOwnerOperationOutcome::Conflict { conflict_paths, .. } if conflict_paths == vec!["feature.txt"])
            ),
        }
        assert_eq!(before, task_projection(&fixture).await);
        assert_eq!(
            fixture
                .resolved
                .git_query(WorkspaceGitQuery::RebaseInProgress, false)
                .await
                .unwrap()
                .unwrap()
                .trim(),
            "false"
        );
        fixture.wait_until_journal_empty().await;
        if case == "clean" {
            let head = fixture
                .resolved
                .git_query(WorkspaceGitQuery::Head, false)
                .await
                .unwrap()
                .unwrap();
            let target = fixture
                .resolved
                .git_query(
                    WorkspaceGitQuery::TargetHead {
                        branch: "main".into(),
                    },
                    false,
                )
                .await
                .unwrap()
                .unwrap();
            assert!(fixture
                .resolved
                .git_query(
                    WorkspaceGitQuery::IsAncestor {
                        base: target.trim().to_owned(),
                        head: head.trim().to_owned()
                    },
                    true
                )
                .await
                .unwrap()
                .is_some());
        }
    }
}

/// The real daemon owns an opaque workspace handle; prompt loading must
/// request its diff and must keep working when that owner goes offline.
#[tokio::test]
async fn remote_step_check_prompt_carries_owner_diff_and_offline_fallback() {
    use services::workflow::dispatch::{
        build_effective_prompt,
        loader::{load_agent_dispatch_context, DispatchContextParams},
        StepDiff,
    };
    let mut fixture = Fixture::new("forge-remote-step-diff").await;
    let db = Arc::clone(&fixture.harness.state.db);
    let root = TaskRepo::get_by_id(&*db, &fixture.resolved.placement.task_id, false)
        .await
        .unwrap()
        .unwrap();
    let now = db::now_rfc3339();
    sqlx::query("UPDATE workspace SET before_sha=? WHERE id=?")
        .bind(&fixture.base_sha)
        .bind(&fixture.resolved.placement.workspace_id)
        .execute(db.pool())
        .await
        .unwrap();
    let step = TaskRepo::create(
        &*db,
        CreateTask {
            id: db::new_uuid_v4(),
            project_id: root.project_id.clone(),
            parent_task_id: Some(root.id.clone()),
            subtask_order: Some(0),
            assignee_type: None,
            assignee_id: None,
            title: "remote step".to_owned(),
            description: Some("deliver the daemon-side change".to_owned()),
            task_type: "task".to_owned(),
            status: "lead_check".to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
    let changed = fixture.run("printf 'only the daemon owns this change\n' > remote-step.txt; git add remote-step.txt; git commit -m step").await;
    assert_eq!(changed.exit_code, 0);
    let workflow = services::workflow::inherited_subtask_workflow::inherited_subtask_workflow();
    let context = async || {
        load_agent_dispatch_context(DispatchContextParams {
            db: Arc::clone(&db),
            router: &fixture.harness.state.workspace_backend_router,
            task_id: &step.id,
            role: "planner",
            state_name: "lead_check",
            state_config: json!({}),
            execution_policy: None,
            workflow: &workflow,
        })
        .await
        .expect("diff availability never fails prompt loading")
    };
    let loaded = context().await;
    assert!(
        matches!(&loaded.step_diff,Some(StepDiff::Loaded { diff, .. }) if diff.contains("+only the daemon owns this change"))
    );
    let (prompt, selection) = build_effective_prompt(&loaded, None, None);
    assert_eq!(
        selection.builder_id,
        services::workflow::dispatch::BUILDER_ID_PLANNER_STEP_CHECK_V1
    );
    assert!(
        prompt.user.contains("+++ b/remote-step.txt")
            && prompt.user.contains("+only the daemon owns this change"),
        "{}",
        prompt.user
    );
    assert!(prompt.user.contains("the root's branch point"));
    assert!(
        !prompt.system.contains("remote-step.txt"),
        "the diff is run input, not the role contract"
    );
    let requests = fixture
        .link
        .as_ref()
        .unwrap()
        .requests(METHOD_WORKSPACE_DIFF);
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0]["placement_id"], fixture.resolved.placement.id);
    fixture.assert_server_cannot_resolve_workspace();

    // Drop the socket and wait for the server to see the offline owner.
    fixture.link.take();
    common::fake_daemon::wait_until_disconnected(&fixture.harness.state, &fixture.daemon_id).await;
    let offline = load_agent_dispatch_context(DispatchContextParams {
        db: Arc::clone(&db),
        router: &fixture.harness.state.workspace_backend_router,
        task_id: &step.id,
        role: "planner",
        state_name: "lead_check",
        state_config: json!({}),
        execution_policy: None,
        workflow: &workflow,
    })
    .await
    .expect("the missing owner is represented in the prompt");
    assert!(matches!(
        offline.step_diff,
        Some(StepDiff::Unavailable { .. })
    ));
    let (prompt, _) = build_effective_prompt(&offline, None, None);
    assert!(
        prompt
            .user
            .contains("The step's diff could not be loaded (")
            && prompt
                .user
                .contains("Read the step's changes in the workspace."),
        "{}",
        prompt.user
    );
    assert!(!prompt.user.contains("+++ b/remote-step.txt"));
    assert!(!fixture.inaccessible_server_path.exists());
}
