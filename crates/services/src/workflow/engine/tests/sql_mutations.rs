use super::*;

#[tokio::test]
async fn removed_dispatch_annotation_retries_preserve_a_queued_manual_stop() {
    for clear in [false, true] {
        let db = Arc::new(sqlite_db().await);
        let id = new_uuid_v4();
        seed_project_repo_and_task(&db, &id, "todo").await;
        let original = json!({"type":"dispatch_failed","message":"old refusal"}).to_string();
        sqlx::query("UPDATE task SET error_annotation=? WHERE id=?")
            .bind(&original)
            .bind(&id)
            .execute(db.pool())
            .await
            .unwrap();
        let blocker_id = db
            .enqueue_task_mutation(
                &id,
                db::TaskMutation::Sql {
                    change: db::ConditionChange::None,
                    task_id: id.clone(),
                    query: "UPDATE task SET title=title WHERE id=?".into(),
                    arguments: vec![json!(id)],
                },
            )
            .await
            .unwrap();
        let blocker = db
            .claim_step(
                "blocker",
                Some(&id),
                &(chrono::Utc::now() + chrono::Duration::seconds(60)).to_rfc3339(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(blocker.id, blocker_id);
        let activity = db.hold_task_step(&blocker);
        let manual = json!({"type":"manual_stop","message":"owner held"}).to_string();
        db.enqueue_task_mutation(
            &id,
            db::TaskMutation::Sql {
                change: db::ConditionChange::Legacy,
                task_id: id.clone(),
                query: "UPDATE task SET error_annotation=?,version=version+1 WHERE id=?".into(),
                arguments: vec![json!(manual), json!(id)],
            },
        )
        .await
        .unwrap();
        let producer_db = db.clone();
        let producer_id = id.clone();
        let producer = tokio::spawn(async move {
            if clear {
                super::super::clear_dispatch_failure_annotation(&producer_db, &producer_id, None)
                    .await
            } else {
                super::super::annotate_dispatch_failure(
                    &producer_db,
                    &producer_id,
                    "todo",
                    "new refusal",
                    None,
                )
                .await
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let notify = db.domain_event_notify();
                let changed = notify.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if db.task_steps(&id).await.unwrap().len() == 3 {
                    break;
                }
                changed.await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            TaskRepo::get_by_id(&*db, &id, false)
                .await
                .unwrap()
                .unwrap()
                .error_annotation
                .as_deref(),
            Some(original.as_str())
        );
        db::task_writer::in_task_step(blocker.clone(), db.execute_task_mutation(&blocker))
            .await
            .unwrap();
        db.release_step(&blocker.id, "blocker").await.unwrap();
        drop(activity);
        producer.await.unwrap().unwrap();
        let task = TaskRepo::get_by_id(&*db, &id, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.error_annotation.as_deref(), Some(manual.as_str()));
        assert_eq!(task.version, 2);
        assert!(db
            .task_steps(&id)
            .await
            .unwrap()
            .iter()
            .all(|s| s.status == "done" && s.attempts == 1));
    }
}
