//! A queued dispatcher role command never blocks another Task's admission:
//! role commands for different Tasks are claimed independently, across
//! Projects, Agents and lanes.
use db::{
    CreateProject, CreateTask, EnqueueTaskStep, ProjectRepo, SqliteDb, TaskRepo, TaskStepRepo,
};

async fn task(db: &SqliteDb, project: &str, id: &str) {
    ProjectRepo::create(
        db,
        CreateProject {
            id: project.into(),
            owner_id: None,
            name: project.into(),
            primary_repo_id: None,
            updated_at: db::now_rfc3339(),
            settings: "{}".into(),
            workflow_definition: "{}".into(),
            created_at: db::now_rfc3339(),
        },
    )
    .await
    .unwrap();
    TaskRepo::create(
        db,
        CreateTask {
            id: id.into(),
            project_id: project.into(),
            parent_task_id: None,
            assignee_type: None,
            assignee_id: None,
            title: "t".into(),
            description: None,
            task_type: "task".into(),
            status: "in_progress".into(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            subtask_order: None,
            plan: None,
            updated_at: db::now_rfc3339(),
            created_at: db::now_rfc3339(),
        },
    )
    .await
    .unwrap();
}

async fn enqueue(db: &SqliteDb, task_id: &str, lane: &str, operation: &str, agent: &str) -> String {
    let t = TaskRepo::get_by_id(db, task_id, false)
        .await
        .unwrap()
        .unwrap();
    let epoch: i64 = sqlx::query_scalar("SELECT status_epoch FROM task WHERE id=?")
        .bind(task_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    let id = db::new_uuid_v4();
    db.enqueue_step(&EnqueueTaskStep {
        id: id.clone(),
        task_id: task_id.into(),
        kind: "command".into(),
        payload_json: serde_json::json!({"operation": operation, "admission_agent_id": agent})
            .to_string(),
        causation_step_id: None,
        causation_key: id.clone(),
        chain_id: id.clone(),
        chain_position: 1,
        expected_status: t.status,
        expected_version: t.version,
        expected_epoch: Some(epoch),
        lane: lane.into(),
        available_at: "2026-01-01T00:00:00Z".into(),
    })
    .await
    .unwrap();
    id
}

#[tokio::test]
async fn a_queued_role_command_never_blocks_an_unrelated_admission() {
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let db = SqliteDb::new(pool);
    task(&db, "project-a", "a").await;
    task(&db, "project-b", "b").await;
    // The dispatcher's role command for Project A sits in the long lane, as it
    // does when that Project has environment checks and the four long slots
    // are busy with CI or merge steps.
    let role = enqueue(&db, "a", "long", "reconcile_role", "agent-a").await;
    sqlx::query("UPDATE task_step SET created_at='2000-01-01T00:00:00Z' WHERE id=?")
        .bind(&role)
        .execute(db.pool())
        .await
        .unwrap();
    // An unrelated Project, Agent and lane: a user claim and a state-entry dispatch.
    let mut outcomes = Vec::new();
    for operation in [
        "claim_and_start_task",
        "dispatch_initial_role_execution_with_metadata_and_admission",
        "dispatch_recovery_role",
    ] {
        let step = enqueue(&db, "b", "fast", operation, "agent-b").await;
        let claimed = db
            .claim_step("worker", Some("b"), &db::task_writer::lease_deadline())
            .await
            .unwrap();
        outcomes.push((operation, claimed.is_some()));
        sqlx::query("DELETE FROM task_step WHERE id=?")
            .bind(&step)
            .execute(db.pool())
            .await
            .unwrap();
    }
    // Same, while the role command is claimed and running.
    let running = db
        .claim_step("role-worker", Some("a"), &db::task_writer::lease_deadline())
        .await
        .unwrap();
    enqueue(&db, "b", "fast", "claim_and_start_task", "agent-b").await;
    let claimed = db
        .claim_step("worker", Some("b"), &db::task_writer::lease_deadline())
        .await
        .unwrap();
    assert!(running.is_some(), "the role command itself is claimable");
    assert!(
        outcomes.iter().all(|(_, ok)| *ok) && claimed.is_some(),
        "an unrelated Project's admission must not wait for another Project's role command: {outcomes:?}"
    );
}
