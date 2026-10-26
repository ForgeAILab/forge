//! Released SQL replay: healthy parity, or main replay plus background repair.
use super::tests::{claim, task};
use super::*;
use crate::{TaskMutation, TaskRepo};
use serde_json::{json, Value};

// v0.14.1 field layout and task_writer.rs's literal queued content SQL.
const CONTENT: &str = r#"{"Sql":{"task_id":"t","query":"UPDATE task SET title='edited',version=version+1 WHERE id=?","arguments":["t"]}}"#;
// v0.14.1 Agent archival role sweep (sqlite/task.rs).
const ASSIGNEE: &str = r#"{"Sql":{"task_id":"t","query":"UPDATE task SET assignee_id=NULL,updated_at=? WHERE id=? AND assignee_type='agent' AND assignee_id=?","arguments":["2026-10-01T00:00:00Z","t","deleted-agent"]}}"#;
// Deployed 1991b306 payload already pinned by contract_tests.rs.
const BLOCKED: &str = r#"{"Sql":{"task_id":"historic","query":"UPDATE task SET blocked_json=?, updated_at=? WHERE id=?","arguments":["{\"kind\":\"executor_failed\",\"reason\":\"historic full reason\",\"created_at\":\"2026-10-01T00:00:00Z\"}","2026-10-01T00:00:00Z","historic"]}}"#;

pub(super) async fn db() -> SqliteDb {
    // Template initialization may call the clock; it must not advance the fixture.
    let _clock = clock();
    let pool = crate::create_sqlite_pool("sqlite::memory:").await.unwrap();
    crate::run_migrations(&pool).await.unwrap();
    let db = SqliteDb::new(pool);
    sqlx::query("INSERT INTO project(id,name,created_at,updated_at) VALUES('p','Conditions','2026-10-25T00:00:00Z','2026-10-25T00:00:00Z')").execute(db.pool()).await.unwrap();
    db
}

pub(super) fn clock() -> crate::test_clock::Guard {
    crate::test_clock::freeze("2026-10-25T00:00:00Z".parse().unwrap())
}

/// Generated event/diagnostic UUIDs are replaced by their row ordinal. All
/// other fields, including source/cache/payload text and clock bytes, are exact.
pub(super) async fn stored(db: &SqliteDb, id: &str) -> Value {
    let raw: String = sqlx::query_scalar(
        "SELECT json_object('condition',t.condition_json,'version',t.version,
         'revision',p.list_revision,'generation',d.generation,
         'title',t.title,'status',t.status,'assignee_id',t.assignee_id,
         'error_annotation',t.error_annotation,'blocked_json',t.blocked_json,
         'failed_json',t.failed_json,'entry_barrier_json',t.entry_barrier_json,
         'metadata_json',t.metadata_json,'updated_at',t.updated_at)
         FROM task t JOIN project p ON p.id=t.project_id
         JOIN task_schedule_dirty d ON d.task_id=t.id WHERE t.id=?",
    )
    .bind(id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    let mut result: Value = serde_json::from_str(&raw).unwrap();
    let diagnostics: Vec<String> = sqlx::query_scalar(
        "SELECT json_object('task_id',task_id,'kind',kind,'source',source,
         'owner_json',owner_json,'text',text,'byte_length',byte_length,'digest',digest,
         'created_at',created_at,'last_seen',last_seen,'occurrence_count',occurrence_count)
         FROM task_diagnostic_text WHERE task_id=? ORDER BY created_at,kind,digest",
    )
    .bind(id)
    .fetch_all(db.pool())
    .await
    .unwrap();
    let events: Vec<String> = sqlx::query_scalar(
        "SELECT json_object('sequence',sequence,'event_type',event_type,
         'entity_type',entity_type,'entity_id',entity_id,'actor_type',actor_type,
         'actor_id',actor_id,'scope_type',scope_type,'scope_id',scope_id,
         'correlation_id',correlation_id,'causation_id',causation_id,
         'causation_depth',causation_depth,'dedupe_key',dedupe_key,
         'payload_json',payload_json,'created_at',created_at)
         FROM domain_event ORDER BY sequence",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    for (key, rows) in [("diagnostics", diagnostics), ("events", events)] {
        result[key] = Value::Array(
            rows.into_iter()
                .enumerate()
                .map(|(i, raw)| {
                    let mut row: Value = serde_json::from_str(&raw).unwrap();
                    row["id"] = json!(format!("generated-{i}"));
                    row
                })
                .collect(),
        );
    }
    result
}

pub(super) fn assert_main(case: &str, actual: Value) {
    let expected = include_str!("fixtures/sql-main-13bc2ae1.jsonl")
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .find(|row| row["case"] == case)
        .unwrap_or_else(|| panic!("missing main case {case}"));
    assert_eq!(actual, expected["stored"], "{case}");
}

async fn replay(payload: &str, id: &str, corrupt: bool, case: &str) {
    let _clock = clock();
    let db = db().await;
    task(&db, id).await;
    if payload == ASSIGNEE {
        sqlx::query("UPDATE task SET assignee_type='agent',assignee_id='deleted-agent' WHERE id=?")
            .bind(id)
            .execute(db.pool())
            .await
            .unwrap();
    }
    let step = claim(&db, id).await;
    crate::task_writer::in_task_step(step, async {
        if corrupt {
            sqlx::query("UPDATE task SET condition_json='{}' WHERE id=?")
                .bind(id)
                .execute(db.pool())
                .await
                .unwrap();
        }
        // Repair time is frozen independently of setup and the replay path.
        let _clock = clock();
        let mutation: TaskMutation = serde_json::from_str(payload).unwrap();
        assert!(matches!(
            mutation,
            TaskMutation::Sql {
                change: ConditionChange::Full,
                ..
            }
        ));
        assert_eq!(mutation.apply(&db).await.unwrap(), json!(1));
        let replayed = stored(&db, id).await;
        assert_main(case, replayed.clone());
        // Full replay already did the repair; the background check is now a no-op.
        assert_eq!(db.check_task_conditions_of(&[id.into()]).await.unwrap(), 0);
        assert_eq!(stored(&db, id).await, replayed);
        let diagnostics = replayed["diagnostics"].as_array().unwrap();
        assert_eq!(diagnostics.len(), usize::from(corrupt));
        if corrupt {
            assert_eq!(diagnostics[0]["text"], "{}");
            assert_eq!(diagnostics[0]["occurrence_count"], 1);
        }
        if payload == CONTENT {
            assert_eq!(
                TaskRepo::get_by_id(&db, id, false)
                    .await
                    .unwrap()
                    .unwrap()
                    .title,
                "edited"
            );
        }
        if payload == ASSIGNEE {
            assert!(TaskRepo::get_by_id(&db, id, false)
                .await
                .unwrap()
                .unwrap()
                .assignee_id
                .is_none());
        }
    })
    .await;
}

#[tokio::test]
async fn full_matches_deployed_blocked_sql_replay() {
    replay(BLOCKED, "historic", false, "released-blocked-healthy").await;
    replay(BLOCKED, "historic", true, "released-blocked-corrupt").await;
}

#[tokio::test]
async fn full_matches_deployed_content_sql_replay_and_background_repair() {
    replay(CONTENT, "t", false, "released-content-healthy").await;
    replay(CONTENT, "t", true, "released-content-corrupt").await;
}

#[tokio::test]
async fn full_matches_deployed_assignee_sql_replay_and_background_repair() {
    replay(ASSIGNEE, "t", false, "released-assignee-healthy").await;
    replay(ASSIGNEE, "t", true, "released-assignee-corrupt").await;
}
