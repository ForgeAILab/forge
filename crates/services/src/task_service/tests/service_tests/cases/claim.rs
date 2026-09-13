use super::super::*;
use db::{PageRequest, SortBy, SortOrder};

#[tokio::test]
async fn project_agent_identity_can_claim_a_task_role() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace root creates");
    let service = TaskService::new(Arc::clone(&db), event_bus)
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, _repo_id, repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let agent = AgentRepo::get_by_id(&*db, &agent_id)
        .await
        .expect("agent loads")
        .expect("agent exists");
    let setup = ProjectAgentBindingRepo::get_active_project_binding(&*db, &project_id)
        .await
        .expect("setup binding loads")
        .expect("setup binding exists");
    ProjectAgentBindingRepo::replace_project_binding(
        &*db,
        ReplaceProjectAgentBinding {
            project_id: project_id.clone(),
            expected_version: setup.version,
            replacement: CreateProjectAgentBinding {
                id: new_uuid_v4(),
                project_id: project_id.clone(),
                identity_id: Some(agent_id.clone()),
                profile_id: Some(agent.profile_id.clone()),
                state: "active".to_owned(),
                autonomy_policy_json: "{}".to_owned(),
                permission_ceiling_json: r#"{"permissions":["propose_task"]}"#.to_owned(),
                subscriptions_json: "[]".to_owned(),
                wake_budget: 0,
                operating_skill_revision_id: None,
                policy_revision: "default".to_owned(),
                policy_digest: String::new(),
                charter_id: None,
                charter_revision_id: None,
                charter_setup_required: true,
                admission_receipt_id: None,
                charter_approval_id: None,
                created_at: now_rfc3339(),
                updated_at: now_rfc3339(),
            },
            replacement_reason: Some("test orchestration boundary".to_owned()),
        },
    )
    .await
    .expect("Project Agent binding activates");
    let task = service
        .create_task(
            project_id,
            "Project Agent can also work this Task",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");

    let claimed = service
        .claim_task(task.id.clone(), Assignee::Agent(agent_id), None)
        .await
        .expect("Project Agent identity is a usable configured Agent");

    assert_eq!(claimed.task.status, "in_progress");
    assert!(
        WorkspaceRepo::get_by_task_id(&*db, &task.id)
            .await
            .expect("workspace lookup")
            .is_some(),
        "Task role claim creates the Task-scoped workspace"
    );
    assert!(
        git::branch_exists(repo_dir.path(), &::workspace::task_branch_name(&task.id))
            .await
            .expect("branch lookup"),
        "Task role claim creates the Task branch"
    );
}

#[tokio::test]
async fn concurrent_claims_share_workspace_creation_and_lose_at_task_cas() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace root creates");
    let repo_locks = Arc::new(RepoCacheLockManager::new());
    let service = TaskService::new(Arc::clone(&db), event_bus)
        .with_workspace_root(workspace_root.path().to_path_buf())
        .with_repo_cache_locks(repo_locks);
    let (project_id, _repo_id, repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = service
        .create_task(
            project_id,
            "Concurrent workspace claim",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");

    let (left, right) = tokio::join!(
        service.claim_task(task.id.clone(), Assignee::Agent(agent_id.clone()), None),
        service.claim_task(task.id.clone(), Assignee::Agent(agent_id), None)
    );
    let outcomes = [left, right];
    assert_eq!(
        outcomes.iter().filter(|result| result.is_ok()).count(),
        1,
        "exactly one concurrent claim wins"
    );
    let error = outcomes
        .into_iter()
        .find_map(Result::err)
        .expect("one claim loses");
    assert!(
        !error.to_string().contains("branch named") && !error.to_string().contains("worktree add"),
        "the loser must report an admission conflict, not raw Git workspace failure: {error}"
    );
    assert!(WorkspaceRepo::get_by_task_id(&*db, &task.id)
        .await
        .expect("workspace lookup")
        .is_some());
    assert!(
        git::branch_exists(repo_dir.path(), &::workspace::task_branch_name(&task.id))
            .await
            .expect("branch lookup")
    );
}

#[tokio::test]
async fn claim_recovers_task_branch_and_uses_project_primary_repository() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace root creates");
    let service = TaskService::new(Arc::clone(&db), event_bus)
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, repo_id, repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = service
        .create_task(
            project_id,
            "Recover orphan task branch",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");
    let branch = ::workspace::task_branch_name(&task.id);
    run_git(repo_dir.path(), &["branch", &branch]);

    let claimed = service
        .claim_task(task.id.clone(), Assignee::Agent(agent_id), None)
        .await
        .expect("claim recovers existing task branch");

    let workspace = WorkspaceRepo::get_by_task_id(&*db, &task.id)
        .await
        .expect("workspace lookup")
        .expect("workspace exists");
    assert_eq!(workspace.branch, branch);
    assert_eq!(workspace.repo_id, repo_id);
    let lease = WorkspaceLeaseRepo::get_active_for_task(&*db, &task.id)
        .await
        .expect("Workspace lease lookup")
        .expect("Agent claim creates a Workspace lease");
    assert_eq!(lease.repository_binding_id, workspace.repo_id);
    assert!(std::path::Path::new(&workspace.worktree_path).exists());
    assert_eq!(claimed.task.status, "in_progress");
}

#[tokio::test]
async fn claim_rejects_cross_project_primary_repository_before_workspace_creation() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace root creates");
    let service = TaskService::new(Arc::clone(&db), event_bus)
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, _project_repo_id, _project_repo_dir) = seed_project_repo(&db).await;
    let (_other_project_id, other_repo_id, _other_repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("Project loads")
        .expect("Project exists");
    ProjectRepo::update_at_version(
        &*db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(other_repo_id)),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        project.version,
        None,
    )
    .await
    .expect("cross-Project pointer stores for legacy-corruption fixture");
    let task = service
        .create_task(
            project_id.clone(),
            "Reject invalid Project repository authority",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("Task creates without selecting a repository");

    let result = service
        .claim_task(task.id.clone(), Assignee::Agent(agent_id), None)
        .await;

    assert!(matches!(
        result,
        Err(ServiceError::RepoMismatch { project_id: rejected_project })
            if rejected_project == project_id
    ));
    assert!(WorkspaceRepo::get_by_task_id(&*db, &task.id)
        .await
        .expect("Workspace lookup succeeds")
        .is_none());
    assert!(ExecutionRepo::list_by_task(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 10,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Asc,
        },
    )
    .await
    .expect("Execution lookup succeeds")
    .items
    .is_empty());
}

#[tokio::test]
async fn create_claim_and_transition_task() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus));
    let mut rx = event_bus.subscribe();
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    crate::test_support::clear_project_execution_role_defaults(&db, &project_id).await;

    let task = service
        .create_task(
            project_id,
            "Implement services",
            Some("Build task service".to_owned()),
            None,
            Some(10),
            None,
            Some(r#"{"required":true}"#.to_owned()),
            None,
            None,
        )
        .await
        .expect("task creates");
    assert_eq!(task.status, "todo".to_owned());
    assert_eq!(task.priority, 10);
    assert_eq!(rx.recv().await.unwrap().event_type, "task.created");

    let claimed = service
        .claim_task(task.id.clone(), Assignee::Agent(agent_id.clone()), None)
        .await
        .expect("task claims");
    assert_eq!(claimed.task.status, "in_progress".to_owned());
    let coder_assignment = service
        .coder_assignment(&claimed.task.id)
        .await
        .expect("coder assignment loads")
        .expect("coder assignment exists");
    assert_eq!(
        coder_assignment.assignee_type,
        Some(db::AssigneeKind::Agent)
    );
    assert_eq!(coder_assignment.assignee_id, Some(agent_id));
    assert_eq!(claimed.execution.status, ExecutionStatus::Running);
    assert_eq!(rx.recv().await.unwrap().event_type, "task.assigned");

    let review = service
        .transition(
            claimed.task.id.clone(),
            "review".to_owned(),
            claimed.task.version,
        )
        .await
        .expect("task enters review");
    assert!(review.review.is_none());
    assert_eq!(review.task.status, "merging".to_owned());
    let event = rx.recv().await.unwrap();
    assert_eq!(event.event_type, "task.status_changed");
}

#[tokio::test]
async fn claim_assigns_implicit_assignee_and_uses_claim_execution() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus));
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    update_project_workflow(&db, &project_id, &implicit_assignee_workflow()).await;
    let agent_id = seed_agent(&db).await;
    let task = service
        .create_task(
            project_id,
            "Implicit assignee",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");

    let claimed = service
        .claim_task(task.id, Assignee::Agent(agent_id.clone()), None)
        .await
        .expect("task claims");

    let assignment = TaskRoleAssignmentRepo::get_by_task_and_role(
        &*db,
        &claimed.task.id,
        default_roles::ASSIGNEE,
    )
    .await
    .expect("assignment loads")
    .expect("assignee assignment exists");
    assert_eq!(assignment.assignee_type, Some(db::AssigneeKind::Agent));
    assert_eq!(assignment.assignee_id.as_deref(), Some(agent_id.as_str()));
    assert_eq!(claimed.execution.role, default_roles::ASSIGNEE);
    assert_eq!(
        claimed.execution.agent_id.as_deref(),
        Some(agent_id.as_str())
    );
}

#[tokio::test]
async fn claim_uses_custom_workflow_active_target() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), Arc::clone(&event_bus));
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let mut ready = workflow_state("ready", StateKind::Initial, None, StateHooks::default());
    ready.triggers.insert(
        WorkflowTrigger::Accept,
        WorkflowTriggerDefinition {
            to: "coding".to_owned(),
            dispatch: None,
        },
    );
    let mut coding = workflow_state(
        "coding",
        StateKind::Active,
        Some("implementer"),
        StateHooks {
            on_enter: vec![hook("dispatch_role_agent")],
            ..StateHooks::default()
        },
    );
    coding.triggers.insert(
        WorkflowTrigger::Accept,
        WorkflowTriggerDefinition {
            to: "done".to_owned(),
            dispatch: None,
        },
    );
    let workflow = WorkflowDefinition {
        roles: vec![api_types::RoleDefinition {
            name: "implementer".to_owned(),
            display_name: "Implementer".to_owned(),
            description: "Implements the task".to_owned(),
        }],
        states: vec![
            ready,
            coding,
            workflow_state("done", StateKind::Terminal, None, StateHooks::default()),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    };
    update_project_workflow(&db, &project_id, &workflow).await;
    let agent_id = seed_agent(&db).await;
    let task = service
        .create_task(
            project_id,
            "Custom workflow claim",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");
    assert_eq!(task.status, "ready");

    let claimed = service
        .claim_task(task.id, Assignee::Agent(agent_id.clone()), None)
        .await
        .expect("task claims");

    assert_eq!(claimed.task.status, "coding");
    let assignment =
        TaskRoleAssignmentRepo::get_by_task_and_role(&*db, &claimed.task.id, "implementer")
            .await
            .expect("assignment loads")
            .expect("implementer assignment exists");
    assert_eq!(assignment.assignee_type, Some(db::AssigneeKind::Agent));
    assert_eq!(assignment.assignee_id.as_deref(), Some(agent_id.as_str()));
}

#[tokio::test]
async fn claim_ignores_system_only_active_edges() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let mut stalled = workflow_state(
        "stalled",
        StateKind::Active,
        Some("fixer"),
        StateHooks::default(),
    );
    stalled.triggers.insert(
        WorkflowTrigger::Accept,
        WorkflowTriggerDefinition {
            to: "done".to_owned(),
            dispatch: None,
        },
    );
    stalled.triggers.insert(
        WorkflowTrigger::Retry,
        WorkflowTriggerDefinition {
            to: "coding".to_owned(),
            dispatch: None,
        },
    );
    let workflow = WorkflowDefinition {
        roles: vec![api_types::RoleDefinition {
            name: "fixer".to_owned(),
            display_name: "Fixer".to_owned(),
            description: "Fixes stalled work".to_owned(),
        }],
        states: vec![
            stalled,
            workflow_state(
                "coding",
                StateKind::Active,
                Some("fixer"),
                StateHooks::default(),
            ),
            workflow_state("done", StateKind::Terminal, None, StateHooks::default()),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    };
    update_project_workflow(&db, &project_id, &workflow).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "stalled".to_owned()).await;

    let result = service
        .claim_task(task.id, Assignee::Agent(agent_id), None)
        .await;

    match result {
        Err(ServiceError::InvalidOperation { message }) => {
            assert!(message.contains("no claimable active transition"));
        }
        other => panic!("expected invalid operation, got {other:?}"),
    }
}

#[tokio::test]
async fn claim_rejects_conflicting_implicit_assignee_assignment() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    update_project_workflow(&db, &project_id, &implicit_assignee_workflow()).await;
    let agent_a = seed_agent(&db).await;
    let agent_b = seed_agent_with_executor_type(&db, "codex", "{}").await;
    let task = service
        .create_task(
            project_id,
            "Implicit conflict",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");
    TaskRoleAssignmentRepo::assign(
        &*db,
        role_assignment_input(&task.id, default_roles::ASSIGNEE, Some(agent_a), None),
    )
    .await
    .expect("assignee role preassigns");

    let result = service
        .claim_task(task.id.clone(), Assignee::Agent(agent_b), None)
        .await;

    match result {
        Err(ServiceError::Conflict(message)) => {
            assert!(message.contains("role 'assignee' is assigned to a different agent"));
        }
        Err(error) => panic!("expected conflict, got {error:?}"),
        Ok(_) => panic!("expected conflict, got successful claim"),
    }
    let task_after = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(task_after.status, "todo");
}

#[tokio::test]
async fn claim_allows_first_subtask_in_root_workspace() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let service = TaskService::new(Arc::clone(&db), event_bus)
        .with_workspace_root(workspace_root.path().to_path_buf())
        .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default()));
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let root = seed_task_with_status(&db, &project_id, "todo".to_owned()).await;
    let subtask = seed_subtask_with_status(&db, &root, "child", "todo".to_owned(), 0).await;

    let claimed = service
        .claim_task(subtask.id.clone(), Assignee::Agent(agent_id), None)
        .await
        .expect("first subtask claims");

    assert_eq!(claimed.task.status, default_states::IN_PROGRESS);
    let workspace = WorkspaceRepo::get_by_task_id(&*db, &root.id)
        .await
        .expect("workspace lookup succeeds")
        .expect("root workspace exists");
    assert_eq!(
        claimed.execution.workspace_id.as_deref(),
        Some(workspace.id.as_str())
    );
}

#[tokio::test]
async fn claim_rejects_coordination_root_with_subtasks() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service =
        TaskService::new(Arc::clone(&db), event_bus).with_task_executor(Arc::new(NoDiffExecutor));
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let root = seed_task_with_status(&db, &project_id, "todo".to_owned()).await;
    let _subtask = seed_subtask_with_status(&db, &root, "child", "todo".to_owned(), 0).await;

    let result = service
        .claim_task(root.id.clone(), Assignee::Agent(agent_id), None)
        .await;

    assert!(matches!(result, Err(ServiceError::InvalidOperation { .. })));
    let executions = ExecutionRepo::list_by_task_and_role(
        &*db,
        &root.id,
        default_roles::CODER,
        PageRequest {
            cursor: None,
            limit: 10,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("executions load");
    assert!(executions.items.is_empty());
}

#[tokio::test]
async fn default_workflow_assigns_declared_roles_not_assignee() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    update_project_default_roles(&db, &project_id, &agent_id).await;
    let task = service
        .create_task(
            project_id,
            "Default workflow",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");

    service
        .claim_task(task.id.clone(), Assignee::Agent(agent_id), None)
        .await
        .expect("task claims");

    let mut roles = TaskRoleAssignmentRepo::list_by_task(&*db, &task.id)
        .await
        .expect("assignments load")
        .into_iter()
        .map(|assignment| assignment.role_name)
        .collect::<Vec<_>>();
    roles.sort();
    assert_eq!(
        roles,
        vec![
            default_roles::CODER.to_owned(),
            default_roles::PLANNER.to_owned(),
            default_roles::REVIEWER.to_owned(),
        ]
    );
    assert!(!roles.iter().any(|role| role == default_roles::ASSIGNEE));
}
