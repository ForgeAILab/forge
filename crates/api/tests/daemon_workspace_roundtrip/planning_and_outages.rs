//! Remote plan publication and journal recovery across daemon outages.

use super::*;

#[tokio::test]
async fn remote_lease_expiry_before_socket_disconnect_accepts_journaled_commit_once() {
    assert_outage_order(false).await;
}

#[tokio::test]
async fn remote_socket_disconnect_before_lease_expiry_accepts_journaled_commit_once() {
    assert_outage_order(true).await;
}

#[tokio::test]
async fn remote_plan_checklist_gate_matches_server_placement() {
    use services::workflow::{
        actions::RequirePlanChecklistComplete, default_workflow, HookAction, HookContext,
        HookResult,
    };

    let fixture = Fixture::new("forge-workspace-plan-gate").await;
    let state = &fixture.harness.state;
    let owner_task = TaskRepo::get_by_id(&*state.db, &fixture.resolved.placement.task_id, false)
        .await
        .unwrap()
        .unwrap();
    let now = db::now_rfc3339();
    let server_task = TaskRepo::create(
        &*state.db,
        CreateTask {
            id: db::new_uuid_v4(),
            project_id: owner_task.project_id.clone(),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "Server workspace plan gate".into(),
            description: None,
            task_type: "task".into(),
            status: "in_progress".into(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    let server_path = fixture.server_root.path().join("plan-gate/repo");
    std::fs::create_dir_all(&server_path).unwrap();
    let server_workspace = WorkspaceRepo::create(
        &*state.db,
        CreateWorkspace {
            id: db::new_uuid_v4(),
            task_id: server_task.id.clone(),
            repo_id: WorkspaceRepo::get_by_id(&*state.db, &fixture.resolved.placement.workspace_id)
                .await
                .unwrap()
                .unwrap()
                .repo_id,
            worktree_path: server_path.to_string_lossy().into_owned(),
            branch: workspace::task_branch_name(&server_task.id),
            status: WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
    let context = |task_id: &str, workspace_id: &str| HookContext {
        task_id: task_id.into(),
        project_id: owner_task.project_id.clone(),
        from_state: "in_progress".into(),
        to_state: "review".into(),
        db: Arc::clone(&state.db),
        event_bus: Arc::clone(&state.event_bus),
        gate_config: None,
        workflow: Arc::new(default_workflow::default_workflow()),
        project_version: None,
        project_workflow_definition: None,
        triggered_by: Actor::system(SystemComponent::Test),
        merge_service: None,
        cleanup_scheduler: None,
        task_service: fixture.harness.state.task_service.as_ref().clone(),
        daemon_connections: None,
        workspace_exec_locks: None,
        terminal_activity: None,
        workspace_root: fixture.server_root.path().join("workspaces"),
        repo_cache_locks: None,
        workspace_backend_router: Arc::clone(&state.workspace_backend_router),
        workspace_id: Some(workspace_id.into()),
        agent_id: None,
        execution_id: None,
        state_config: json!({}),
    };
    let daemon_context = context(&owner_task.id, &fixture.resolved.placement.workspace_id);
    let server_context = context(&server_task.id, &server_workspace.id);
    for content in [
        None,
        Some("- [x] complete\n"),
        Some("No checklist\n"),
        Some("- [ ] unfinished\n"),
    ] {
        if let Some(content) = content {
            std::fs::write(server_path.parent().unwrap().join("plan.md"), content).unwrap();
            fixture
                .run(&format!("printf '%s' {} > ../plan.md", quote(content)))
                .await;
        }
        let daemon_result = RequirePlanChecklistComplete.execute(&daemon_context).await;
        let server_result = RequirePlanChecklistComplete.execute(&server_context).await;
        assert_eq!(format!("{daemon_result:?}"), format!("{server_result:?}"));
        match content {
            None => {
                assert!(
                    matches!(daemon_result, HookResult::Skipped { reason } if reason == "no plan checklist")
                );
                assert!(services::plan_artifact::read_plan_with_router(
                    &state.db,
                    &state.workspace_backend_router,
                    &fixture.resolved.placement.workspace_id,
                )
                .await
                .unwrap()
                .is_none());
            }
            Some("- [ ] unfinished\n") => {
                assert!(
                    matches!(daemon_result, HookResult::Failed { reason } if reason.contains("1 unchecked item(s)"))
                );
            }
            Some(_) => assert!(matches!(daemon_result, HookResult::Ok)),
        }
    }
    assert!(!fixture.inaccessible_server_path.exists());
}

#[tokio::test]
async fn remote_plan_owner_publication_restore_and_discard_are_fenced() {
    let fixture = Fixture::new("forge-owner-plan-publication").await;
    let placement = &fixture.resolved.placement;
    let client = fixture.resolved.backend.daemon_client().unwrap();
    let daemon = placement.daemon_id.as_deref().unwrap();
    let params = |operation| WorkspaceOwnerOperationParams {
        fence: WorkspaceMutationFence {
            integration: api_types::WorkspaceIntegrationBinding::TaskStep,
            daemon_id: daemon.into(),
            runtime_id: placement.runtime_id.clone().unwrap(),
            placement_id: placement.id.clone(),
            operation_id: db::new_uuid_v4(),
            generation: placement.generation as u64,
            expected: WorkspaceOperationExpected::BaseSha {
                sha: fixture.base_sha.clone(),
            },
        },
        workspace_handle: placement.workspace_handle.clone().unwrap(),
        operation,
    };
    let initial = params(WorkspaceOwnerOperation::PublishPlan {
        execution_id: "plan-initial".into(),
        content: "- [ ] prior plan\n".into(),
    });
    client.owner_operation(daemon, initial).await.unwrap();
    let publication = params(WorkspaceOwnerOperation::PublishPlan {
        execution_id: "plan-revision".into(),
        content: "- [x] revised plan\n".into(),
    });
    client
        .owner_operation(daemon, publication.clone())
        .await
        .unwrap();
    client
        .owner_operation(daemon, publication.clone())
        .await
        .unwrap();
    assert_eq!(
        fixture
            .resolved
            .backend
            .read(placement, "../plan.md", 1024)
            .await
            .unwrap(),
        b"- [x] revised plan\n"
    );
    let mut stale = publication;
    stale.fence.operation_id = db::new_uuid_v4();
    stale.fence.generation = 0;
    assert!(client.owner_operation(daemon, stale).await.is_err());
    let restore = params(WorkspaceOwnerOperation::RestorePlan {
        execution_id: "plan-revision".into(),
    });
    client
        .owner_operation(daemon, restore.clone())
        .await
        .unwrap();
    client.owner_operation(daemon, restore).await.unwrap();
    assert_eq!(
        fixture
            .resolved
            .backend
            .read(placement, "../plan.md", 1024)
            .await
            .unwrap(),
        b"- [ ] prior plan\n"
    );
    // The next execution can already be active when the previous terminal
    // settlement removes its private plan snapshot and outbox.
    let (path, _) = fixture.resolved.owner_paths().await.unwrap();
    fixture
        .runtime
        .start(ExecutionStartParams {
            task_id: placement.task_id.clone(),
            execution_id: "active-next-turn".into(),
            workspace_path: path.clone(),
            executor_type: "shell".into(),
            executor_config: json!({"executor_type":"shell", "config":{}}),
            prompt: json!({"description":"while [ ! -f finish-next-turn ]; do sleep 0.01; done"}),
            max_turns: None,
            plan_text: None,
        })
        .await
        .unwrap();
    assert!(fixture
        .runtime
        .active_execution_ids()
        .contains(&"active-next-turn".into()));
    client
        .owner_operation(
            daemon,
            params(WorkspaceOwnerOperation::DiscardPlan {
                execution_id: "plan-revision".into(),
            }),
        )
        .await
        .unwrap();
    assert!(fixture
        .resolved
        .backend
        .describe(placement)
        .await
        .unwrap()
        .active_execution_ids
        .contains(&"active-next-turn".into()));
    // End the owner-side fixture turn without depending on cancellation races.
    std::fs::write(PathBuf::from(path).join("finish-next-turn"), "done").unwrap();
    for _ in 0..500 {
        if !fixture
            .resolved
            .backend
            .describe(placement)
            .await
            .unwrap()
            .active_execution_ids
            .contains(&"active-next-turn".into())
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!fixture
        .resolved
        .backend
        .describe(placement)
        .await
        .unwrap()
        .active_execution_ids
        .contains(&"active-next-turn".into()));
    fixture.assert_server_cannot_resolve_workspace();
}

#[tokio::test]
async fn remote_plan_operations_wait_for_workspace_run_and_discard_cleaned_state() {
    let fixture = Fixture::new("forge-plan-command-queue").await;
    let placement = &fixture.resolved.placement;
    let daemon = placement.daemon_id.as_deref().unwrap();
    let client = fixture
        .resolved
        .backend
        .daemon_client()
        .unwrap()
        .clone()
        .with_timeout(Duration::from_millis(10));
    let params = |operation| WorkspaceOwnerOperationParams {
        fence: WorkspaceMutationFence {
            integration: api_types::WorkspaceIntegrationBinding::TaskStep,
            daemon_id: daemon.into(),
            runtime_id: placement.runtime_id.clone().unwrap(),
            placement_id: placement.id.clone(),
            operation_id: db::new_uuid_v4(),
            generation: placement.generation as u64,
            expected: WorkspaceOperationExpected::BaseSha {
                sha: fixture.base_sha.clone(),
            },
        },
        workspace_handle: placement.workspace_handle.clone().unwrap(),
        operation,
    };
    let (path, _) = fixture.resolved.owner_paths().await.unwrap();
    let worktree = PathBuf::from(path);
    for (index, operation) in [
        WorkspaceOwnerOperation::PublishPlan {
            execution_id: "queued-plan".into(),
            content: "- [ ] queued\n".into(),
        },
        WorkspaceOwnerOperation::RestorePlan {
            execution_id: "queued-plan".into(),
        },
    ]
    .into_iter()
    .enumerate()
    {
        let command = format!("touch queue-ready-{index}; while [ ! -e queue-release-{index} ]; do sleep 0.01; done; printf 'owner command finished'");
        let run = fixture.run(&command);
        let settlement = async {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            while !worktree.join(format!("queue-ready-{index}")).exists() {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "owner command acquired the lock"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let request = client.owner_operation(daemon, params(operation));
            tokio::pin!(request);
            let waiting = tokio::time::timeout(Duration::from_millis(50), &mut request).await;
            let blocked = waiting.is_err();
            std::fs::write(worktree.join(format!("queue-release-{index}")), "release").unwrap();
            let result = match waiting {
                Ok(result) => result,
                Err(_) => request.await,
            }
            .unwrap();
            assert!(blocked, "plan RPC waited beyond the ordinary 10ms timeout");
            assert!(matches!(
                result.outcome,
                WorkspaceOwnerOperationOutcome::Applied
            ));
        };
        let (run, ()) = tokio::join!(run, settlement);
        assert_eq!(run.stdout_tail, "owner command finished");
    }
    let cleanup = params(WorkspaceOwnerOperation::DiscardPlan {
        execution_id: "queued-plan".into(),
    });
    let normal = fixture.resolved.backend.daemon_client().unwrap();
    let clean: WorkspaceCleanupResult = normal
        .cleanup(
            daemon,
            WorkspaceCleanupParams {
                fence: cleanup.fence.clone(),
                workspace_handle: cleanup.workspace_handle.clone(),
            },
        )
        .await
        .unwrap();
    assert!(clean.cleaned);
    let discard = || {
        params(WorkspaceOwnerOperation::DiscardPlan {
            execution_id: "queued-plan".into(),
        })
    };
    assert!(matches!(
        client
            .owner_operation(daemon, discard())
            .await
            .unwrap()
            .outcome,
        WorkspaceOwnerOperationOutcome::Applied
    ));
    normal
        .acknowledge(daemon, clean.entry_id.clone())
        .await
        .unwrap();
    normal.retry_acknowledgements(daemon).await.unwrap();
    fixture.wait_until_journal_empty().await;
    assert!(matches!(
        client
            .owner_operation(daemon, discard())
            .await
            .unwrap()
            .outcome,
        WorkspaceOwnerOperationOutcome::Applied
    ));
}
