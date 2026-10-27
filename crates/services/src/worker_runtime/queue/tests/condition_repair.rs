use super::*;

#[tokio::test]
async fn corrupt_condition_is_rebuilt_inside_engine_and_worker_writes() {
    for loop_settlement in [false, true] {
        let (db, worker) = fixture().await;
        sqlx::query("UPDATE task SET condition_json='{}' WHERE id='t'")
            .execute(db.pool())
            .await
            .unwrap();
        let mut input = step("done");
        if loop_settlement {
            input.chain_position = CHAIN_LIMIT + 1;
        }
        db.enqueue_step(&input).await.unwrap();
        let row = worker.drain("t").await.unwrap();
        let raw: String = sqlx::query_scalar("SELECT condition_json FROM task WHERE id='t'")
            .fetch_one(db.pool())
            .await
            .unwrap();
        let condition = db::task_condition::decode(&raw).expect("same-write rebuild");
        let mut c = db.pool().acquire().await.unwrap();
        let facts = db::ConditionFacts::load(&mut c, "t").await.unwrap();
        drop(c);
        let mut legacy = db::ConditionInput::from(&row);
        legacy.facts = Some(facts);
        assert_eq!(condition.typed(), db::compose_condition(&legacy).typed());
        assert_eq!(
            raw,
            serde_json::to_string(&db::compose_condition(&legacy)).unwrap()
        );
        let texts = db.task_diagnostic_texts("t", None, 100).await.unwrap();
        assert_eq!(texts.items.iter().filter(|t| t.text == "{}").count(), 1);
        assert!(texts
            .items
            .iter()
            .filter(|t| t.text == "{}")
            .all(|t| t.occurrence_count == 1));
    }
}
