use super::*;
use db::now_rfc3339;
use services::recovery::test_support::{seed_project_repo, seed_task, sqlite_db};

#[tokio::test]
async fn unchanged_starts_without_live_executions_write_no_recovery_steps_or_dirty_generations() {
    let db = Arc::new(sqlite_db().await);
    let (project, _) = seed_project_repo(&db).await;
    for state in ["in_progress", "merge_failed"] {
        seed_task(&db, project.clone(), state.to_owned(), None).await;
    }
    let snapshot = || async {
        let steps: Vec<(String, String)> =
            sqlx::query_as("SELECT id,status FROM task_step ORDER BY id")
                .fetch_all(db.pool())
                .await
                .unwrap();
        let dirty: Vec<(String, i64, i64, i64)> = sqlx::query_as(
            "SELECT task_id,generation,dirty,external FROM task_schedule_dirty ORDER BY task_id",
        )
        .fetch_all(db.pool())
        .await
        .unwrap();
        (steps, dirty)
    };
    let before = snapshot().await;
    for _ in 0..2 {
        let runtime_db = Arc::new(SqliteDb::new(db.pool().clone()));
        CrashRecovery::new(runtime_db, Arc::new(EventBus::new(16)))
            .run_recovery()
            .await
            .unwrap();
        assert_eq!(snapshot().await, before);
    }
    // Exercise two complete runtime lifetimes on that same database.
    // A paused Project keeps ordinary dispatch quiet during the starts.
    sqlx::query("UPDATE project SET paused_at=? WHERE id=?")
        .bind(now_rfc3339())
        .bind(&project)
        .execute(db.pool())
        .await
        .unwrap();
    let mut previous_start = None;
    let data_dir = tempfile::tempdir().unwrap();
    let config = config::ForgeConfig::with_data_dir(data_dir.path().to_path_buf());
    for _ in 0..2 {
        let runtime_db = Arc::new(SqliteDb::new(db.pool().clone()));
        let runtime = Arc::new(
            crate::runtime::ForgeRuntimeBuilder::from_config(
                runtime_db,
                Arc::new(EventBus::new(64)),
                config.clone(),
            )
            .build(),
        );
        let mut supervisor = crate::runtime::RuntimeSupervisor::new(
            runtime.clone(),
            crate::runtime::RuntimeAssemblyMode::Solo,
        );
        supervisor.start().await.unwrap();
        // Finish legitimate first-start import/scheduler work before
        // comparing the next unchanged runtime lifetime.
        runtime.task_dispatcher.reconcile_all().await.unwrap();
        let ids: Vec<String> = sqlx::query_scalar("SELECT id FROM task ORDER BY id")
            .fetch_all(db.pool())
            .await
            .unwrap();
        for id in ids {
            runtime.task_service.drain(&id).await.unwrap();
        }
        runtime.task_dispatcher.reconcile_all().await.unwrap();
        supervisor.shutdown().await.unwrap();
        let after = snapshot().await;
        if let Some(previous) = previous_start.as_ref() {
            assert_eq!(&after, previous);
        }
        previous_start = Some(after);
    }
}
