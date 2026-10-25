//! Shared persistence fixtures for recovery and composition tests.
use crate::workflow::default_roles;
use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, CreateProject, CreateRepo,
    CreateTask, CreateTaskRoleAssignment, ProjectRepo, RepoRepo, SqliteDb, Task, TaskRepo,
    TaskRoleAssignmentRepo, TaskStatus, UpdateProject,
};

pub async fn sqlite_db() -> SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    SqliteDb::new(pool)
}

pub async fn seed_project_repo(db: &SqliteDb) -> (String, String) {
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();
    ProjectRepo::create(
        db,
        CreateProject {
            id: project_id.clone(),
            name: "Forge".to_owned(),
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
    RepoRepo::create(
        db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "forge".to_owned(),
            remote_url: Some("https://example.com/forge.git".to_owned()),
            local_path: None,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo creates");
    ProjectRepo::update_at_version(
        db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        ProjectRepo::get_by_id(db, &project_id)
            .await
            .expect("fixture Project lookup")
            .expect("fixture Project exists")
            .version,
        None,
    )
    .await
    .expect("project primary repo updates");
    (project_id, repo_id)
}

pub async fn seed_task(
    db: &SqliteDb,
    project_id: String,
    status: TaskStatus,
    agent_id: Option<String>,
) -> Task {
    seed_task_with_assignee(db, project_id, status, "agent", agent_id).await
}

pub(crate) async fn seed_task_with_assignee(
    db: &SqliteDb,
    project_id: String,
    status: TaskStatus,
    assignee_type: &str,
    assignee_id: Option<String>,
) -> Task {
    let now = now_rfc3339();
    let task = TaskRepo::create(
        db,
        CreateTask {
            id: new_uuid_v4(),
            project_id,
            parent_task_id: None,
            subtask_order: None,
            assignee_type: assignee_id.as_ref().map(|_| assignee_type.to_owned()),
            assignee_id: assignee_id.clone(),
            title: "Recover me".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status,
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
    .expect("task creates");
    if assignee_type == "agent" {
        if let Some(agent_id) = assignee_id {
            TaskRoleAssignmentRepo::assign(
                db,
                CreateTaskRoleAssignment {
                    id: new_uuid_v4(),
                    task_id: task.id.clone(),
                    role_name: default_roles::CODER.to_owned(),
                    assignee_type: Some(db::AssigneeKind::Agent),
                    assignee_id: Some(agent_id),
                    created_at: now.clone(),
                    updated_at: now,
                },
            )
            .await
            .expect("role assignment creates");
        }
    }
    task
}

#[cfg(test)]
pub(crate) mod conditions;
