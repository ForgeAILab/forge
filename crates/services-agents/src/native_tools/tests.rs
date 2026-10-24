//! Shared fixtures for native Task record and boundary tests.

use super::*;

struct NativePlanFixture {
    db: Arc<db::SqliteDb>,
    provider: CoordinationToolProvider,
    _temp: tempfile::TempDir,
    agent_id: String,
    runtime_session_id: String,
    task_id: String,
    execution_id: String,
    workspace_id: String,
    worktree: PathBuf,
    scope: CanonicalScope,
}

async fn native_plan_fixture(task_role: &str, execution_role: &str) -> NativePlanFixture {
    native_plan_fixture_with_permissions(task_role, execution_role, "{}").await
}

async fn native_plan_fixture_with_permissions(
    task_role: &str,
    execution_role: &str,
    permissions: &str,
) -> NativePlanFixture {
    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    db::run_migrations(&pool).await.expect("migrations run");
    let db = Arc::new(db::SqliteDb::new(pool));
    let now = db::now_rfc3339();
    let project_id = db::new_uuid_v4();
    let repo_id = db::new_uuid_v4();
    let task_id = db::new_uuid_v4();
    let workspace_id = db::new_uuid_v4();
    let execution_id = db::new_uuid_v4();
    let agent_id = db::new_uuid_v4();
    let profile_id = db::new_uuid_v4();
    let context_scope_id = db::new_uuid_v4();
    let runtime_session_id = db::new_uuid_v4();
    let temp = tempfile::tempdir().expect("temp dir creates");
    let worktree = temp.path().join(&task_id).join("repo");
    std::fs::create_dir_all(&worktree).expect("worktree creates");

    let project = db::ProjectRepo::create(
        &*db,
        db::CreateProject {
            id: project_id.clone(),
            name: "Native plan project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");
    db::RepoRepo::create(
        &*db,
        db::CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "repo".to_owned(),
            remote_url: Some("https://example.invalid/repo.git".to_owned()),
            local_path: None,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo creates");
    db::ProjectRepo::update_at_version(
        &*db,
        db::UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id.clone())),
            paused_at: None,
            updated_at: now.clone(),
        },
        project.version,
        None,
    )
    .await
    .expect("primary repo binds");
    db::TaskRepo::create(
        &*db,
        db::CreateTask {
            id: task_id.clone(),
            project_id: project_id.clone(),
            parent_task_id: None,
            assignee_type: None,
            assignee_id: None,
            title: "Write a native plan".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "planning".to_owned(),
            is_automation: false,
            priority: 0,
            subtask_order: None,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("task creates");
    db::WorkspaceRepo::create(
        &*db,
        db::CreateWorkspace {
            id: workspace_id.clone(),
            task_id: task_id.clone(),
            repo_id,
            worktree_path: worktree.to_string_lossy().into_owned(),
            branch: workspace::task_branch_name(&task_id),
            status: db::WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("workspace creates");
    db::AgentRepo::create_identity_with_profile(
        &*db,
        db::CreateAgentIdentity {
            id: agent_id.clone(),
            name: "native-plan-agent".to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: db::AgentStatus::Busy,
            last_heartbeat_at: Some(now.clone()),
            is_default: false,
            paused: false,
            owner_id: None,
            visibility: "global".to_owned(),
            account_permission_ceiling: permissions.to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        db::CreateAgentProfile {
            id: profile_id.clone(),
            identity_id: agent_id.clone(),
            backend_kind: "native".to_owned(),
            executor_type: "native".to_owned(),
            provider: None,
            model: Some("test-model".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: permissions.to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("agent identity creates");
    db::AgentContextScopeRepo::create_context_scope(
        &*db,
        db::CreateAgentContextScope {
            id: context_scope_id.clone(),
            identity_id: agent_id.clone(),
            scope_type: "task".to_owned(),
            scope_id: task_id.clone(),
            project_id: Some(project_id),
            task_id: Some(task_id.clone()),
            task_role: Some(task_role.to_owned()),
            workspace_access: "task_write".to_owned(),
            authority_json: "{}".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
            workspace_path: None,
        },
    )
    .await
    .expect("context scope creates");
    db::AgentSessionRepo::create_agent_session(
        &*db,
        db::CreateAgentSession {
            id: db::new_uuid_v4(),
            identity_id: agent_id.clone(),
            profile_id,
            context_scope_id,
            backend_kind: "native".to_owned(),
            runtime_session_id: Some(runtime_session_id.clone()),
            status: "running".to_owned(),
            capabilities_json: "{}".to_owned(),
            connection_status: "healthy".to_owned(),
            predecessor_session_id: None,
            last_activity_at: Some(now.clone()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("agent session creates");
    db::ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: execution_id.clone(),
            task_id: task_id.clone(),
            agent_id: Some(agent_id.clone()),
            role: execution_role.to_owned(),
            status: db::ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some(runtime_session_id.clone()),
            agent_message_id: None,
            last_activity_at: Some(now.clone()),
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"native","plan_delivery":"execution_outbox"}"#.to_owned(),
            ),
            workspace_id: Some(workspace_id.clone()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("execution creates");

    let provider = CoordinationToolProvider::new(Arc::clone(&db));
    NativePlanFixture {
        db,
        provider,
        _temp: temp,
        agent_id,
        runtime_session_id,
        task_id: task_id.clone(),
        execution_id,
        workspace_id,
        worktree,
        scope: CanonicalScope {
            scope_type: CanonicalScopeType::Task,
            scope_id: task_id,
            workspace_access: WorkspaceAccess::TaskWrite,
        },
    }
}

mod boundaries;
mod records;
