use super::*;

#[tokio::test]
async fn workflow_writer_waits_for_hook_lease_and_old_owner_stays_fenced() {
    let db = Arc::new(sqlite_db().await);
    let bus = Arc::new(EventBus::new(8));
    seed_project_repo_and_task(&db, "hook-fence", "start").await;
    let engine = engine(db.clone(), bus.clone());
    engine
        .workflow_execution()
        .transition(
            "hook-fence",
            "step_0",
            1,
            &cascade_chain_workflow(1),
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "start",
            false,
            Default::default(),
        )
        .await
        .unwrap();
    let step = db
        .claim_step(
            "old",
            Some("hook-fence"),
            &(chrono::Utc::now() + chrono::Duration::seconds(60)).to_rfc3339(),
        )
        .await
        .unwrap()
        .unwrap();
    db.start_hook(&step, 0).await.unwrap();
    db.enqueue_task_mutation(
        "hook-fence",
        db::TaskMutation::Sql {
            change: db::ConditionChange::Entry,
            task_id: "hook-fence".into(),
            query: "UPDATE task SET status='start',version=version+1 WHERE id=?".into(),
            arguments: vec![json!("hook-fence")],
        },
    )
    .await
    .unwrap();
    assert_eq!(
        TaskRepo::get_by_id(&*db, "hook-fence", false)
            .await
            .unwrap()
            .unwrap()
            .status,
        "step_0"
    );
    db.finish_hook(&step, 0, "\"Ok\"").await.unwrap();
    sqlx::query("UPDATE task_step SET lease_until='2000-01-01T00:00:00Z' WHERE id=?")
        .bind(&step.id)
        .execute(db.pool())
        .await
        .unwrap();
    TaskStepWorker::new(engine)
        .drain("hook-fence")
        .await
        .unwrap();
    assert!(matches!(
        db.finish_hook(&step, 0, "late").await,
        Err(db::DbError::VersionConflict)
    ));
    assert_eq!(
        TaskRepo::get_by_id(&*db, "hook-fence", false)
            .await
            .unwrap()
            .unwrap()
            .status,
        "start"
    );
}
