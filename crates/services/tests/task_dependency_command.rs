//! The Project Agent's prerequisite-edge command.
//!
//! Before this existed, `depends_on_task_ids` could only be set when a Task
//! was created. An Agent re-planning a graph therefore had to cancel and
//! recreate every downstream Task — each with a new id — while the same
//! add/remove capability was already reachable over REST. These cases cover
//! the boundary that matters for handing it to an Agent: the Project comes
//! from the server-derived binding, and neither Task may live anywhere else.

use std::sync::Arc;

use db::{
    create_sqlite_pool, now_rfc3339, run_migrations, CreateProject, CreateTask, ProjectRepo,
    SqliteDb, TaskDependencyRepo, TaskRepo,
};
use events::EventBus;
use services::{TaskDependencyAction, TaskService};

const PROJECT: &str = "dependency-command-project";
const OTHER_PROJECT: &str = "dependency-command-other-project";

async fn fixture() -> (Arc<SqliteDb>, TaskService) {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    let db = Arc::new(SqliteDb::new(pool));
    let now = now_rfc3339();
    for (id, name) in [
        (PROJECT, "Dependency command"),
        (OTHER_PROJECT, "Elsewhere"),
    ] {
        ProjectRepo::create(
            &*db,
            CreateProject {
                id: id.to_owned(),
                name: name.to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("Project creates");
    }
    let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(256)));
    (db, service)
}

async fn seed_task(db: &SqliteDb, id: &str, project_id: &str, title: &str) {
    let now = now_rfc3339();
    TaskRepo::create(
        db,
        CreateTask {
            id: id.to_owned(),
            project_id: project_id.to_owned(),
            parent_task_id: None,
            assignee_type: None,
            assignee_id: None,
            title: title.to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "todo".to_owned(),
            is_automation: false,
            priority: 0,
            subtask_order: None,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("Task creates");
}

#[tokio::test]
async fn an_agent_can_add_and_remove_one_prerequisite_edge() {
    let (db, service) = fixture().await;
    seed_task(&db, "dep-core", PROJECT, "core").await;
    seed_task(&db, "dep-cli", PROJECT, "cli").await;

    let dependent = service
        .perform_project_agent_dependency(PROJECT, "dep-cli", "dep-core", TaskDependencyAction::Add)
        .await
        .expect("the edge is added");
    assert_eq!(dependent.id, "dep-cli");
    assert_eq!(
        TaskDependencyRepo::list_dependencies(&*db, "dep-cli")
            .await
            .expect("dependencies list"),
        vec!["dep-core".to_owned()],
        "cli waits for core"
    );

    service
        .perform_project_agent_dependency(
            PROJECT,
            "dep-cli",
            "dep-core",
            TaskDependencyAction::Remove,
        )
        .await
        .expect("the edge is removed");
    assert!(
        TaskDependencyRepo::list_dependencies(&*db, "dep-cli")
            .await
            .expect("dependencies list")
            .is_empty(),
        "the graph is rewired without either Task changing id"
    );
}

#[tokio::test]
async fn an_agent_bound_to_one_project_cannot_rewire_another() {
    let (db, service) = fixture().await;
    seed_task(&db, "dep-here", PROJECT, "here").await;
    seed_task(&db, "dep-elsewhere", OTHER_PROJECT, "elsewhere").await;

    // Both directions: the Project is server-derived, so neither the
    // dependent nor the prerequisite may come from outside it.
    for (task_id, depends_on) in [("dep-here", "dep-elsewhere"), ("dep-elsewhere", "dep-here")] {
        let error = service
            .perform_project_agent_dependency(
                PROJECT,
                task_id,
                depends_on,
                TaskDependencyAction::Add,
            )
            .await
            .expect_err("a Task outside the bound Project must be refused");
        assert!(
            error.to_string().contains("bound Project"),
            "the refusal must name the boundary: {error}"
        );
    }
    assert!(TaskDependencyRepo::list_dependencies(&*db, "dep-here")
        .await
        .expect("dependencies list")
        .is_empty());
}

#[tokio::test]
async fn a_task_cannot_be_made_to_wait_for_itself() {
    let (db, service) = fixture().await;
    seed_task(&db, "dep-solo", PROJECT, "solo").await;

    let error = service
        .perform_project_agent_dependency(
            PROJECT,
            "dep-solo",
            "dep-solo",
            TaskDependencyAction::Add,
        )
        .await
        .expect_err("a self-edge is never a plan");
    assert!(error.to_string().contains("depend on itself"), "{error}");
}

#[tokio::test]
async fn a_cancelled_task_cannot_become_a_new_prerequisite() {
    let (db, service) = fixture().await;
    seed_task(&db, "dep-live", PROJECT, "live").await;
    seed_task(&db, "dep-dead", PROJECT, "dead").await;
    sqlx::query("UPDATE task SET status = 'cancelled' WHERE id = 'dep-dead'")
        .execute(db.pool())
        .await
        .expect("prerequisite is cancelled");

    let error = service
        .perform_project_agent_dependency(
            PROJECT,
            "dep-live",
            "dep-dead",
            TaskDependencyAction::Add,
        )
        .await
        .expect_err("pointing at a cancelled Task would wedge the dependent immediately");
    assert!(error.to_string().contains("cancelled"), "{error}");
}
