use crate::Result;
use db::{
    new_uuid_v4, now_rfc3339, AgentListQuery, AgentRepo, AgentStatus, CreateAgent, CreateProject,
    CreateRepo, CreateTask, DaemonRepo, DaemonStatus, PageRequest, Project, ProjectRepo, Repo,
    RepoRepo, SortBy, SortOrder, SqliteDb, SystemSettingRepo, TaskRepo, UpdateDaemonReport,
    UpdateTask, UpsertDaemon, UserRepo,
};

struct DemoTask {
    title: &'static str,
    description: &'static str,
    status: &'static str,
    priority: i64,
    error_annotation: Option<(api_types::FailureKind, &'static str)>,
}

const DEMO_TASKS: &[DemoTask] = &[
    DemoTask {
        title: "Set up CI pipeline",
        description: "Configure GitHub Actions for lint, test, and build.",
        status: "backlog",
        priority: 3,
        error_annotation: None,
    },
    DemoTask {
        title: "Add rate limiting to API",
        description: "Implement token-bucket rate limiting on public endpoints.",
        status: "backlog",
        priority: 5,
        error_annotation: None,
    },
    DemoTask {
        title: "Design database schema for notifications",
        description: "Create tables for user notification preferences and delivery log.",
        status: "todo",
        priority: 7,
        error_annotation: None,
    },
    DemoTask {
        title: "Implement user settings page",
        description: "Build the frontend settings UI with theme toggle and profile editing.",
        status: "todo",
        priority: 5,
        error_annotation: None,
    },
    DemoTask {
        title: "Write integration tests for auth flow",
        description: "Cover login, token refresh, and logout with end-to-end tests.",
        status: "todo",
        priority: 6,
        error_annotation: None,
    },
    DemoTask {
        title: "Refactor error handling middleware",
        description: "Unify error responses across all API routes with proper status codes.",
        status: "in_progress",
        priority: 8,
        error_annotation: None,
    },
    DemoTask {
        title: "Add WebSocket support for live updates",
        description: "Implement real-time task status notifications via WebSocket.",
        status: "in_progress",
        priority: 7,
        error_annotation: None,
    },
    DemoTask {
        title: "Optimize database queries for task listing",
        description: "Add indexes and rewrite slow queries identified in profiling.",
        status: "review",
        priority: 6,
        error_annotation: None,
    },
    DemoTask {
        title: "Fix pagination cursor encoding",
        description: "Cursor was double-encoding base64, causing 400 errors on page 2+.",
        status: "review",
        priority: 9,
        error_annotation: None,
    },
    DemoTask {
        title: "Add dark mode support",
        description: "Implement CSS custom properties for theme switching.",
        status: "done",
        priority: 4,
        error_annotation: None,
    },
    DemoTask {
        title: "Set up logging infrastructure",
        description: "Structured JSON logging with request ID propagation.",
        status: "done",
        priority: 8,
        error_annotation: None,
    },
    DemoTask {
        title: "Initial project scaffolding",
        description: "Workspace structure, CI config, and dev tooling.",
        status: "done",
        priority: 10,
        error_annotation: None,
    },
    DemoTask {
        title: "Migrate user sessions to Redis",
        description: "Move session storage from in-memory to Redis for horizontal scaling.",
        status: "blocked",
        priority: 6,
        error_annotation: Some((
            api_types::FailureKind::WorkspaceResetRequired,
            "workspace reset required: task branch no longer exists in repo",
        )),
    },
    DemoTask {
        title: "Upgrade OpenAPI spec generator",
        description: "Update codegen tooling to v4 for better TypeScript output.",
        status: "blocked",
        priority: 4,
        error_annotation: Some((
            api_types::FailureKind::WorkspaceError,
            "workspace error: repository directory not found on disk",
        )),
    },
];

pub async fn install_demo_data(db: &SqliteDb) -> Result<()> {
    let now = now_rfc3339();
    let project = find_or_create_demo_project(db, &now).await?;
    find_or_create_demo_repo(db, &project.id, &now).await?;
    let agent_id = find_or_create_null_agent(db, &now).await?;
    install_demo_daemon(db, &now).await?;
    install_demo_tasks(db, &project.id, &agent_id, &now).await?;

    tracing::info!("demo data installed");
    Ok(())
}

async fn find_or_create_demo_project(db: &SqliteDb, now: &str) -> Result<Project> {
    let projects = ProjectRepo::list(db, page_request()).await?;
    if let Some(project) = projects
        .items
        .into_iter()
        .find(|project| project.name == "Demo")
    {
        return Ok(project);
    }

    let owner_id = demo_project_owner(db).await?;
    ProjectRepo::create(
        db,
        CreateProject {
            id: new_uuid_v4(),
            name: "Demo".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id,
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await
    .map_err(Into::into)
}

/// Before anyone registers, the Demo Project starts ownerless and the first
/// registration claims it (`AuthService` bootstrap). Once bootstrap has run
/// nothing claims it any more, so it belongs to the earliest administrator:
/// seeding never creates a new ownerless Project, which every signed-in user
/// could see and change.
async fn demo_project_owner(db: &SqliteDb) -> Result<Option<String>> {
    if SystemSettingRepo::get_setting(db, "bootstrap_completed")
        .await?
        .is_none()
    {
        return Ok(None);
    }
    let users = UserRepo::list_users(
        db,
        PageRequest {
            sort_by: SortBy::CreatedAt,
            ..page_request()
        },
    )
    .await?;
    users
        .items
        .into_iter()
        .find(|user| user.is_admin)
        .map(|user| Some(user.id))
        .ok_or_else(|| {
            crate::ServiceError::invalid_operation(
                "demo data needs an administrator account to own the Demo Project",
            )
        })
}

async fn find_or_create_demo_repo(db: &SqliteDb, project_id: &str, now: &str) -> Result<Repo> {
    let repos = RepoRepo::list_by_project(db, project_id, page_request()).await?;
    if let Some(repo) = repos.items.into_iter().find(|repo| {
        repo.name == "demo-repo"
            && repo.remote_url.as_deref() == Some("https://example.com/demo.git")
    }) {
        return Ok(repo);
    }

    RepoRepo::create(
        db,
        CreateRepo {
            id: new_uuid_v4(),
            project_id: project_id.to_owned(),
            name: "demo-repo".to_owned(),
            remote_url: Some("https://example.com/demo.git".to_owned()),
            local_path: None,
            default_branch: "main".to_owned(),
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await
    .map_err(Into::into)
}

async fn find_or_create_null_agent(db: &SqliteDb, now: &str) -> Result<String> {
    let page = AgentRepo::list(
        db,
        AgentListQuery {
            status: None,
            executor_type: Some("null".to_owned()),
            capabilities: Vec::new(),
            page: page_request(),
        },
    )
    .await?;
    if let Some(agent) = page.items.into_iter().find(|a| a.is_default) {
        return Ok(agent.id);
    }

    let agent = AgentRepo::create(
        db,
        CreateAgent {
            id: new_uuid_v4(),
            name: "Null Default".to_owned(),
            description: None,
            executor_type: "null".to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            capabilities_json: "[]".to_owned(),
            config_json: r#"{"delay_seconds":5}"#.to_owned(),
            credential_ref: None,
            daemon_id: None,
            max_concurrent_tasks: 10,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: true,
            paused: false,
            owner_id: None,
            visibility: "global".to_owned(),
            prompt_template: None,
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await?;
    Ok(agent.id)
}

async fn install_demo_daemon(db: &SqliteDb, now: &str) -> Result<()> {
    // The first user to register takes over every ownerless daemon, this one
    // included. A later `--demo` start must register it as that owner's, or
    // the repository refuses the upsert as another user's machine.
    let existing = DaemonRepo::get_by_machine_id(db, "demo").await?;
    let (owner_id, visibility) = existing.map_or_else(
        || (None, "global".to_owned()),
        |daemon| (daemon.owner_id, daemon.visibility),
    );
    let daemon = DaemonRepo::upsert_by_machine_id(
        db,
        UpsertDaemon {
            max_concurrent_runs: None,
            id: new_uuid_v4(),
            machine_id: "demo".to_owned(),
            hostname: "demo".to_owned(),
            os: "demo".to_owned(),
            arch: "demo".to_owned(),
            agent_version: None,
            labels_json: r#"{"demo":"true"}"#.to_owned(),
            status: DaemonStatus::Online,
            registration_token_hash: None,
            owner_id,
            visibility,
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await?;

    DaemonRepo::update_report(
        db,
        UpdateDaemonReport {
            max_concurrent_runs: None,
            id: daemon.id,
            last_report_at: now.to_owned(),
            status: DaemonStatus::Online,
            detected_clis_json: r#"[{"kind":"null","availability":"authenticated","config_path":null,"version":null,"path":null}]"#.to_owned(),
            labels_json: Some(r#"{"demo":"true"}"#.to_owned()),
            updated_at: now.to_owned(),
        },
    )
    .await?;

    Ok(())
}

async fn install_demo_tasks(
    db: &SqliteDb,
    project_id: &str,
    agent_id: &str,
    now: &str,
) -> Result<()> {
    let existing = TaskRepo::list(
        db,
        db::TaskListQuery {
            project_id: project_id.to_owned(),
            q: None,
            statuses: Vec::new(),
            agent_ids: Vec::new(),
            assignee_types: Vec::new(),
            assignee_ids: Vec::new(),
            priority: None,
            include_archived: false,
            include_cancelled: false,
            include_deleted: false,
            page: page_request(),
        },
    )
    .await?;

    let existing_titles: Vec<&str> = existing.items.iter().map(|t| t.title.as_str()).collect();

    for (i, demo_task) in DEMO_TASKS.iter().enumerate() {
        if existing_titles.contains(&demo_task.title) {
            continue;
        }

        let is_active = matches!(demo_task.status, "in_progress" | "review");

        let task_id = new_uuid_v4();
        TaskRepo::create(
            db,
            CreateTask {
                id: task_id.clone(),
                project_id: project_id.to_owned(),
                parent_task_id: None,
                subtask_order: None,
                assignee_type: if is_active {
                    Some("agent".to_owned())
                } else {
                    None
                },
                assignee_id: if is_active {
                    Some(agent_id.to_owned())
                } else {
                    None
                },
                title: demo_task.title.to_owned(),
                description: Some(demo_task.description.to_owned()),
                task_type: "task".to_owned(),
                status: demo_task.status.to_owned(),
                is_automation: false,
                priority: demo_task.priority,
                task_state_config: None,
                merge_config: None,
                plan: None,
                created_at: now.to_owned(),
                updated_at: now.to_owned(),
            },
        )
        .await?;

        if let Some((kind, message)) = demo_task.error_annotation {
            use db::task_condition::channels::{
                AnnotationShape, ErrorAnnotation, MessageAnnotation,
            };
            let annotation = serde_json::to_string(&ErrorAnnotation::new(
                AnnotationShape::Message(MessageAnnotation {
                    kind,
                    message: message.into(),
                }),
            ))
            .expect("demo annotation serializes");
            TaskRepo::update(
                db,
                UpdateTask {
                    id: task_id,
                    expected_version: 1,
                    title: None,
                    description: None,
                    priority: None,
                    merge_config: None,
                    plan: None,
                    error_annotation: Some(Some(annotation)),
                    blocked_json: None,
                    failed_json: None,
                    task_state_config: None,
                    parent_task_id: None,
                    updated_at: now.to_owned(),
                },
            )
            .await?;
        }

        let _position = (i + 1) as f64;
    }

    Ok(())
}

fn page_request() -> PageRequest {
    PageRequest {
        cursor: None,
        limit: 1_000,
        include_total: false,
        sort_by: SortBy::Id,
        sort_order: SortOrder::Asc,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use db::{create_sqlite_pool, run_migrations};

    async fn test_db() -> SqliteDb {
        let pool = create_sqlite_pool("sqlite::memory:")
            .await
            .expect("pool creates");
        run_migrations(&pool).await.expect("migrations run");
        SqliteDb::new(pool)
    }

    /// A `--demo` start after the first registration must not create a new
    /// ownerless Project: the earliest administrator owns it.
    #[tokio::test]
    async fn demo_project_created_after_bootstrap_is_owned_by_the_administrator() {
        let db = std::sync::Arc::new(test_db().await);
        let auth = crate::auth_service::AuthService::new(
            std::sync::Arc::clone(&db),
            b"test-secret".to_vec(),
            4,
        );
        let user = auth
            .register("first@example.com", "password123", None)
            .await
            .expect("first user registers");
        install_demo_data(&db).await.expect("install succeeds");
        let owner: Option<String> =
            sqlx::query_scalar("SELECT owner_id FROM project WHERE name = 'Demo'")
                .fetch_one(db.pool())
                .await
                .expect("demo project exists");
        assert_eq!(owner.as_deref(), Some(user.id.as_str()));
    }

    #[tokio::test]
    async fn test_install_demo_data_idempotent() {
        let db = test_db().await;

        install_demo_data(&db)
            .await
            .expect("first install succeeds");
        install_demo_data(&db)
            .await
            .expect("second install succeeds");

        let task_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task")
            .fetch_one(db.pool())
            .await
            .expect("task count succeeds");

        assert_eq!(task_count, DEMO_TASKS.len() as i64);
    }

    /// What a second `forge --demo` start sees once somebody has registered:
    /// the first user owns every seeded row.
    #[tokio::test]
    async fn test_install_demo_data_idempotent_after_first_user_registers() {
        let db = std::sync::Arc::new(test_db().await);
        install_demo_data(&db)
            .await
            .expect("first install succeeds");

        let auth = crate::auth_service::AuthService::new(
            std::sync::Arc::clone(&db),
            b"test-secret".to_vec(),
            4,
        );
        let user = auth
            .register("first@example.com", "password123", None)
            .await
            .expect("first user registers");

        async fn snapshot(db: &SqliteDb) -> Vec<(String, i64, Option<String>)> {
            let mut rows = Vec::new();
            for table in ["project", "repo", "agent_identity", "daemon", "task"] {
                let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
                    .fetch_one(db.pool())
                    .await
                    .expect("count succeeds");
                let owner: Option<String> = match table {
                    "project" | "agent_identity" | "daemon" => {
                        sqlx::query_scalar(&format!("SELECT MIN(owner_id) FROM {table}"))
                            .fetch_one(db.pool())
                            .await
                            .expect("owner read succeeds")
                    }
                    _ => None,
                };
                rows.push((table.to_owned(), count, owner));
            }
            rows
        }

        let before = snapshot(&db).await;
        for (table, _count, owner) in &before {
            if matches!(table.as_str(), "project" | "agent_identity" | "daemon") {
                assert_eq!(
                    owner.as_deref(),
                    Some(user.id.as_str()),
                    "the first user owns the seeded {table}"
                );
            }
        }

        install_demo_data(&db)
            .await
            .expect("install after the first registration succeeds");

        assert_eq!(snapshot(&db).await, before);
        let daemon = DaemonRepo::get_by_machine_id(&*db, "demo")
            .await
            .expect("daemon read succeeds")
            .expect("demo daemon exists");
        assert_eq!(daemon.owner_id.as_deref(), Some(user.id.as_str()));
        assert_eq!(daemon.visibility, "account");
    }

    #[tokio::test]
    async fn test_demo_creates_null_agent() {
        let db = test_db().await;
        install_demo_data(&db).await.expect("install succeeds");

        let agents = AgentRepo::list(
            &db,
            AgentListQuery {
                status: None,
                executor_type: Some("null".to_owned()),
                capabilities: Vec::new(),
                page: page_request(),
            },
        )
        .await
        .expect("agent list succeeds");

        assert_eq!(agents.items.len(), 1);
        assert_eq!(agents.items[0].name, "Null Default");
    }

    #[tokio::test]
    async fn test_demo_tasks_spread_across_states() {
        let db = test_db().await;
        install_demo_data(&db).await.expect("install succeeds");

        let project = ProjectRepo::list(&db, page_request())
            .await
            .expect("list projects");
        let project_id = &project.items[0].id;

        for status in [
            "backlog",
            "todo",
            "in_progress",
            "review",
            "done",
            "blocked",
        ] {
            let tasks = TaskRepo::list(
                &db,
                db::TaskListQuery {
                    project_id: project_id.clone(),
                    q: None,
                    statuses: vec![status.to_owned()],
                    agent_ids: Vec::new(),
                    assignee_types: Vec::new(),
                    assignee_ids: Vec::new(),
                    priority: None,
                    include_archived: false,
                    include_cancelled: false,
                    include_deleted: false,
                    page: page_request(),
                },
            )
            .await
            .expect("list tasks");
            assert!(
                !tasks.items.is_empty(),
                "expected tasks in state '{status}'"
            );
        }
    }
}

#[cfg(test)]
mod condition_tests;

#[cfg(test)]
#[path = "task_service/dependencies/lane_fixture.rs"]
pub(crate) mod lane_fixture;
