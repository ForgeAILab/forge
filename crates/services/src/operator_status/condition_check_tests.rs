//! Operator status tests for condition checks and workspace ownership.

async fn status(db: &std::sync::Arc<db::SqliteDb>) -> api_types::OperatorStatusResponse {
    super::OperatorStatusService::new_for_test(db.clone())
        .compute_status()
        .await
        .unwrap()
}

/// A check that found nothing to repair is a log line, never a row: the
/// Operations empty state must stay empty on a healthy database.
#[tokio::test]
async fn clean_condition_check_adds_no_operator_row() {
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let db = std::sync::Arc::new(db::SqliteDb::new(pool));
    for _ in 0..3 {
        db.check_task_conditions(db::CONDITION_CHECK_PAGE)
            .await
            .unwrap();
    }
    let checks = db.condition_check_status();
    assert_eq!(checks.ticks, 3);
    assert!(checks.last_pass.is_some(), "a pass completed");
    let status = status(&db).await;
    assert!(status.recent_errors.is_empty());
    assert_eq!(
        status.overall_severity,
        api_types::OperatorSeverity::Healthy
    );
}

#[tokio::test]
async fn a_workspace_root_this_database_does_not_own_is_reported_until_it_does() {
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let db = std::sync::Arc::new(db::SqliteDb::new(pool));
    let record = |state: &'static str| {
        let db = db.clone();
        async move {
            sqlx::query(
                "INSERT INTO system_setting (key, value, updated_at) VALUES (?, ?, ?)
                     ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            )
            .bind(crate::workspace_cleanup::GC_STATUS_KEY)
            .bind(format!(
                r#"{{"state":"{state}","root":"/srv/forge","reason":null}}"#
            ))
            .bind(db::now_rfc3339())
            .execute(db.pool())
            .await
            .unwrap();
        }
    };
    record("claimed_by_other").await;
    let reported = status(&db).await;
    assert_eq!(reported.recent_errors.len(), 1);
    let issue = &reported.recent_errors[0];
    assert_eq!(
        (issue.entity_type.as_str(), issue.entity_id.as_str()),
        ("workspace_gc", "/srv/forge")
    );
    assert!(issue.error.contains("--reclaim-workspace-gc"));
    assert_ne!(
        reported.overall_severity,
        api_types::OperatorSeverity::Healthy
    );
    record("owned").await;
    assert!(status(&db).await.recent_errors.is_empty());
}

/// Only a completed pass that repaired a row is reported, and the row
/// goes away with the next clean pass.
#[tokio::test]
async fn repairing_pass_is_reported_until_the_next_clean_pass() {
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let db = std::sync::Arc::new(db::SqliteDb::new(pool));
    let now = db::now_rfc3339();
    db::ProjectRepo::create(
        &*db,
        db::CreateProject {
            id: "p".into(),
            owner_id: None,
            name: "Conditions".into(),
            primary_repo_id: None,
            updated_at: now.clone(),
            settings: "{}".into(),
            workflow_definition: "{}".into(),
            created_at: now.clone(),
        },
    )
    .await
    .unwrap();
    for id in ["a", "b"] {
        db::TaskRepo::create(
            &*db,
            db::CreateTask {
                id: id.into(),
                project_id: "p".into(),
                parent_task_id: None,
                assignee_type: None,
                assignee_id: None,
                title: id.into(),
                description: None,
                task_type: "task".into(),
                status: "backlog".into(),
                is_automation: false,
                priority: 0,
                task_state_config: None,
                merge_config: None,
                subtask_order: None,
                plan: None,
                updated_at: now.clone(),
                created_at: now.clone(),
            },
        )
        .await
        .unwrap();
    }
    // Corrupt, not a newer encoding: the check restates it.
    sqlx::query("UPDATE task SET condition_json='{}' WHERE id='a'")
        .execute(db.pool())
        .await
        .unwrap();
    db.check_task_conditions(1).await.unwrap();
    assert!(
        status(&db).await.recent_errors.is_empty(),
        "a page that repaired is not yet a completed pass"
    );
    db.check_task_conditions(db::CONDITION_CHECK_PAGE)
        .await
        .unwrap();
    let reported = status(&db).await;
    let rows: Vec<_> = reported
        .recent_errors
        .iter()
        .filter(|row| row.entity_type == "task_condition_invariant")
        .collect();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].severity, api_types::OperatorSeverity::Attention);
    assert_eq!(
        rows[0].error,
        "The last Task condition check repaired 1 of 2 Tasks"
    );
    assert_eq!(
        reported.overall_severity,
        api_types::OperatorSeverity::Attention
    );
    db.check_task_conditions(db::CONDITION_CHECK_PAGE)
        .await
        .unwrap();
    assert!(status(&db).await.recent_errors.is_empty());

    // A condition a newer build wrote is quarantined: never repaired, and
    // reported with its Task id for as long as it exists.
    let newer = r#"{"kind":"from_a_newer_build"}"#;
    sqlx::query("UPDATE task SET condition_json=? WHERE id='b'")
        .bind(newer)
        .execute(db.pool())
        .await
        .unwrap();
    for _ in 0..2 {
        db.check_task_conditions(db::CONDITION_CHECK_PAGE)
            .await
            .unwrap();
        let reported = status(&db).await;
        let rows: Vec<_> = reported.recent_errors.iter().collect();
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0].entity_type, "task_condition_quarantined");
        assert_eq!(rows[0].severity, api_types::OperatorSeverity::Attention);
        assert!(
            rows[0].error.starts_with("1 Task condition(s)") && rows[0].error.contains("(b)"),
            "{}",
            rows[0].error
        );
    }
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT condition_json FROM task WHERE id='b'")
            .fetch_one(db.pool())
            .await
            .unwrap(),
        newer
    );
}
