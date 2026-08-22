use super::super::*;

#[tokio::test]
async fn orchestration_identity_claim_fails_before_workspace_or_branch_creation() {
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
            "Orchestration identity must not claim",
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

    let result = service
        .claim_task(task.id.clone(), Assignee::Agent(agent_id), None)
        .await;

    assert!(matches!(
        result,
        Err(ServiceError::InvalidOperation { message })
            if message.contains("Main and Project Agent identities")
    ));
    assert!(
        WorkspaceRepo::get_by_task_id(&*db, &task.id)
            .await
            .expect("workspace lookup")
            .is_none(),
        "rejected orchestration identity must not create a workspace row"
    );
    assert!(
        !git::branch_exists(repo_dir.path(), &::workspace::task_branch_name(&task.id))
            .await
            .expect("branch lookup"),
        "rejected orchestration identity must not create a task branch"
    );
}

#[tokio::test]
async fn claim_recovers_task_branch_left_by_a_rejected_workspace_attempt() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace root creates");
    let service = TaskService::new(Arc::clone(&db), event_bus)
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, _repo_id, repo_dir) = seed_project_repo(&db).await;
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
    assert!(std::path::Path::new(&workspace.worktree_path).exists());
    assert_eq!(claimed.task.status, "in_progress");
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
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
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
    let task = seed_task_with_status(&db, &project_id, &repo_id, "stalled".to_owned()).await;

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
async fn claim_rejects_subtask() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let root = seed_task_with_status(&db, &project_id, &repo_id, "todo".to_owned()).await;
    let subtask = seed_subtask_with_status(&db, &root, "child", "todo".to_owned(), 0).await;

    let result = service
        .claim_task(subtask.id.clone(), Assignee::Agent(agent_id), None)
        .await;

    assert!(matches!(
        result,
        Err(ServiceError::SubtaskManagedByRoot { task_id, root_task_id })
            if task_id == subtask.id && root_task_id == root.id
    ));
}

#[tokio::test]
async fn claim_root_with_subtask_does_not_dispatch_parent_coder_prompt() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service =
        TaskService::new(Arc::clone(&db), event_bus).with_task_executor(Arc::new(NoDiffExecutor));
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let root = seed_task_with_status(&db, &project_id, &repo_id, "todo".to_owned()).await;
    let _subtask = seed_subtask_with_status(&db, &root, "child", "todo".to_owned(), 0).await;

    let claimed = service
        .claim_task(root.id.clone(), Assignee::Agent(agent_id), None)
        .await
        .expect("root with subtask claims");

    assert_eq!(claimed.task.status, "in_progress");
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
    assert_eq!(
        executions.items.len(),
        1,
        "claim should not create a second initial coder execution for ordered-turn roots"
    );
    assert_eq!(executions.items[0].id, claimed.execution.id);
    assert!(executions.items[0].summary.is_none());
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
