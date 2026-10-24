//! Review step and summary analytics checks.

use super::*;

async fn sqlite_db() -> SqliteDb {
    let pool = crate::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    crate::run_migrations(&pool).await.expect("migrations run");
    SqliteDb::new(pool)
}

async fn seed_project_repo_task(db: &SqliteDb) -> (String, String, String) {
    let now = crate::now_rfc3339();
    let project_id = crate::new_uuid_v4();
    let repo_id = crate::new_uuid_v4();
    let task_id = crate::new_uuid_v4();

    ProjectRepo::create(
        db,
        CreateProject {
            id: project_id.clone(),
            name: "analytics-project".to_owned(),
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
            name: "analytics-repo".to_owned(),
            remote_url: Some("https://example.com/forge-analytics.git".to_owned()),
            local_path: Some("/tmp/forge-analytics-test-repo".to_owned()),
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
            updated_at: crate::now_rfc3339(),
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

    TaskRepo::create(
        db,
        CreateTask {
            id: task_id.clone(),
            project_id: project_id.clone(),
            parent_task_id: None,
            assignee_type: None,
            assignee_id: None,
            title: "Task".to_owned(),
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
    .expect("task creates");

    (project_id, repo_id, task_id)
}

async fn seed_execution(db: &SqliteDb, task_id: &str, created_at: &str) -> String {
    let execution_id = crate::new_uuid_v4();
    ExecutionRepo::create(
        db,
        CreateExecution {
            id: execution_id.clone(),
            task_id: task_id.to_owned(),
            agent_id: None,
            role: "reviewer".to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: created_at.to_owned(),
            updated_at: created_at.to_owned(),
        },
    )
    .await
    .expect("execution creates");
    execution_id
}

async fn seed_review(
    db: &SqliteDb,
    task_id: &str,
    execution_id: &str,
    status: ReviewStatus,
    started_at: &str,
    step_results_json: &str,
) {
    let attempt_number = ReviewRepo::next_attempt_number(db, task_id)
        .await
        .expect("attempt number available");
    ReviewRepo::create(
        db,
        CreateReview {
            id: crate::new_uuid_v4(),
            task_id: task_id.to_owned(),
            execution_id: execution_id.to_owned(),
            attempt_number,
            status,
            step_results_json: step_results_json.to_owned(),
            started_at: started_at.to_owned(),
            created_at: started_at.to_owned(),
            updated_at: started_at.to_owned(),
        },
    )
    .await
    .expect("review creates");
}

#[allow(clippy::too_many_arguments)]
#[tokio::test]
async fn ci_analytics_happy_path() {
    let db = sqlite_db().await;
    let (project_id, _repo_id, task_id) = seed_project_repo_task(&db).await;
    let execution_one = seed_execution(&db, &task_id, "2026-04-10T00:00:00Z").await;
    let execution_two = seed_execution(&db, &task_id, "2026-04-11T00:00:00Z").await;

    seed_review(
            &db,
            &task_id,
            &execution_one,
            ReviewStatus::Passed,
            "2026-04-10T00:00:00Z",
            r#"[
                {"command":"cargo test","exit_code":0,"started_at":"2026-04-10T00:00:00Z","finished_at":"2026-04-10T00:00:10Z"},
                {"command":"cargo clippy","exit_code":1,"started_at":"2026-04-10T00:00:20Z","finished_at":"2026-04-10T00:00:25Z"}
            ]"#,
        )
        .await;
    seed_review(
            &db,
            &task_id,
            &execution_two,
            ReviewStatus::Failed,
            "2026-04-11T00:00:00Z",
            r#"{"ci_steps":[{"command":"cargo test","exit_code":1,"started_at":"2026-04-11T00:00:00Z","finished_at":"2026-04-11T00:00:20Z"}]}"#,
        )
        .await;

    let stats = ProjectAnalyticsRepo::get_project_ci_analytics(&db, &project_id, None, None)
        .await
        .expect("ci analytics computed");

    assert_eq!(stats.len(), 2);
    assert_eq!(stats[0].command, "cargo clippy");
    assert_eq!(stats[0].total_runs, 1);
    assert_eq!(stats[0].pass_count, 0);
    assert_eq!(stats[0].fail_count, 1);
    assert_eq!(stats[0].avg_duration_ms, Some(5000));
    assert_eq!(stats[0].p50_duration_ms, Some(5000));
    assert_eq!(stats[0].p95_duration_ms, Some(5000));
    assert_eq!(
        stats[0].last_run_at.as_deref(),
        Some("2026-04-10T00:00:25Z")
    );

    assert_eq!(stats[1].command, "cargo test");
    assert_eq!(stats[1].total_runs, 2);
    assert_eq!(stats[1].pass_count, 1);
    assert_eq!(stats[1].fail_count, 1);
    assert_eq!(stats[1].avg_duration_ms, Some(15000));
    assert_eq!(stats[1].p50_duration_ms, Some(20000));
    assert_eq!(stats[1].p95_duration_ms, Some(20000));
    assert_eq!(
        stats[1].last_run_at.as_deref(),
        Some("2026-04-11T00:00:20Z")
    );
}

#[tokio::test]
async fn ci_analytics_empty_result() {
    let db = sqlite_db().await;
    let project_id = crate::new_uuid_v4();

    let stats = ProjectAnalyticsRepo::get_project_ci_analytics(&db, &project_id, None, None)
        .await
        .expect("ci analytics computed");

    assert!(stats.is_empty());
}

#[tokio::test]
async fn ci_analytics_date_filter() {
    let db = sqlite_db().await;
    let (project_id, _repo_id, task_id) = seed_project_repo_task(&db).await;
    let execution_old = seed_execution(&db, &task_id, "2026-03-01T00:00:00Z").await;
    let execution_new = seed_execution(&db, &task_id, "2026-04-20T00:00:00Z").await;

    seed_review(
        &db,
        &task_id,
        &execution_old,
        ReviewStatus::Passed,
        "2026-03-01T00:00:00Z",
        r#"[{"command":"cargo test","exit_code":0}]"#,
    )
    .await;
    seed_review(
        &db,
        &task_id,
        &execution_new,
        ReviewStatus::Passed,
        "2026-04-20T00:00:00Z",
        r#"[{"command":"cargo clippy","exit_code":0}]"#,
    )
    .await;

    let stats = ProjectAnalyticsRepo::get_project_ci_analytics(
        &db,
        &project_id,
        Some("2026-04-01T00:00:00Z"),
        Some("2026-04-30T23:59:59Z"),
    )
    .await
    .expect("ci analytics filtered");

    assert_eq!(stats.len(), 1);
    assert_eq!(stats[0].command, "cargo clippy");
    assert_eq!(stats[0].total_runs, 1);
    assert_eq!(stats[0].avg_duration_ms, None);
    assert_eq!(stats[0].p50_duration_ms, None);
    assert_eq!(stats[0].p95_duration_ms, None);
    assert_eq!(stats[0].last_run_at, None);
}

#[tokio::test]
async fn review_summary_happy_path() {
    let db = sqlite_db().await;
    let (project_id, _repo_id, task_id) = seed_project_repo_task(&db).await;
    let execution_one = seed_execution(&db, &task_id, "2026-04-10T00:00:00Z").await;
    let execution_two = seed_execution(&db, &task_id, "2026-04-11T00:00:00Z").await;
    let execution_three = seed_execution(&db, &task_id, "2026-04-12T00:00:00Z").await;

    seed_review(
        &db,
        &task_id,
        &execution_one,
        ReviewStatus::Passed,
        "2026-04-10T00:00:00Z",
        r#"[{"command":"cargo test","exit_code":0},{"command":"cargo clippy","exit_code":0}]"#,
    )
    .await;
    seed_review(
        &db,
        &task_id,
        &execution_two,
        ReviewStatus::Failed,
        "2026-04-11T00:00:00Z",
        r#"{"ci_steps":[{"command":"cargo test","exit_code":1}]}"#,
    )
    .await;
    seed_review(
            &db,
            &task_id,
            &execution_three,
            ReviewStatus::Passed,
            "2026-04-12T00:00:00Z",
            r#"[{"command":"cargo fmt","exit_code":0},{"command":"cargo test","exit_code":0},{"command":"cargo clippy","exit_code":0}]"#,
        )
        .await;

    let summary = ProjectAnalyticsRepo::get_project_review_summary(&db, &project_id, None, None)
        .await
        .expect("review summary computed");

    assert_eq!(summary.total_reviews, 3);
    assert_eq!(summary.passed, 2);
    assert_eq!(summary.failed, 1);
    assert_eq!(summary.cancelled, 0);
    assert_eq!(summary.avg_duration_ms, None);
    assert!((summary.pass_rate - (2.0 / 3.0)).abs() < f64::EPSILON);
}

#[tokio::test]
async fn review_summary_empty_result() {
    let db = sqlite_db().await;
    let project_id = crate::new_uuid_v4();

    let summary = ProjectAnalyticsRepo::get_project_review_summary(&db, &project_id, None, None)
        .await
        .expect("review summary computed");

    assert_eq!(summary.total_reviews, 0);
    assert_eq!(summary.passed, 0);
    assert_eq!(summary.failed, 0);
    assert_eq!(summary.cancelled, 0);
    assert_eq!(summary.avg_duration_ms, None);
    assert_eq!(summary.pass_rate, 0.0);
}

#[tokio::test]
async fn review_summary_date_filter() {
    let db = sqlite_db().await;
    let (project_id, _repo_id, task_id) = seed_project_repo_task(&db).await;
    let execution_old = seed_execution(&db, &task_id, "2026-03-01T00:00:00Z").await;
    let execution_new = seed_execution(&db, &task_id, "2026-04-20T00:00:00Z").await;

    seed_review(
        &db,
        &task_id,
        &execution_old,
        ReviewStatus::Failed,
        "2026-03-01T00:00:00Z",
        r#"[{"command":"cargo test","exit_code":1}]"#,
    )
    .await;
    seed_review(
        &db,
        &task_id,
        &execution_new,
        ReviewStatus::Passed,
        "2026-04-20T00:00:00Z",
        r#"[{"command":"cargo test","exit_code":0},{"command":"cargo clippy","exit_code":0}]"#,
    )
    .await;

    let summary = ProjectAnalyticsRepo::get_project_review_summary(
        &db,
        &project_id,
        Some("2026-04-01T00:00:00Z"),
        Some("2026-04-30T23:59:59Z"),
    )
    .await
    .expect("review summary filtered");

    assert_eq!(summary.total_reviews, 1);
    assert_eq!(summary.passed, 1);
    assert_eq!(summary.failed, 0);
    assert_eq!(summary.cancelled, 0);
    assert_eq!(summary.avg_duration_ms, None);
    assert_eq!(summary.pass_rate, 1.0);
}
