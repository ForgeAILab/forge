use super::*;
use crate::{worker_runtime::WorkerRuntime, AttentionService};
use chrono::{Duration, Utc};
use db::{create_sqlite_pool, run_migrations};
use serde_json::Value;

async fn database() -> Arc<SqliteDb> {
    let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
    run_migrations(&pool).await.unwrap();
    Arc::new(SqliteDb::new(pool))
}

fn timestamp(seconds_ago: i64) -> String {
    (Utc::now() - Duration::seconds(seconds_ago)).to_rfc3339()
}

fn input(project: &str, task: &str, paths: &[&str], at: &str) -> CreateDomainEvent {
    let id = new_uuid_v4();
    CreateDomainEvent {
        id: id.clone(), event_type: "task.transitioned".into(), entity_type: "task".into(),
        entity_id: task.into(), actor_type: "system".into(), actor_id: Some("workflow".into()),
        scope_type: "task".into(), scope_id: task.into(), correlation_id: id.clone(),
        causation_id: None, causation_depth: 0, dedupe_key: Some(id.clone()),
        payload_json: json!({"transition_log_id": id, "project_id": project,
            "from_state": "merging", "to_state": "merge_failed", "trigger_name": null,
            "trigger_reason": format!("{CONFLICT_HANDOFF_MARKER} conflict{CONFLICT_HANDOFF_PATHS_PREFIX}{}", json!(paths)),
            "rejection": false}).to_string(),
        created_at: at.into(),
    }
}

async fn handoff(
    db: &SqliteDb,
    project: &str,
    task: &str,
    paths: &[&str],
    at: &str,
) -> DomainEvent {
    db.append_event(input(project, task, paths, at))
        .await
        .unwrap()
}

async fn run(db: &Arc<SqliteDb>) {
    WorkerRuntime::new(
        Arc::clone(db),
        Arc::new(ConflictHotspotConsumer::new(Arc::clone(db))),
    )
    .run_once(100)
    .await
    .unwrap();
}

async fn detections(db: &SqliteDb) -> Vec<Value> {
    sqlx::query_scalar::<_, String>(
        "SELECT payload_json FROM domain_event WHERE event_type = ? ORDER BY sequence",
    )
    .bind(DETECTED_EVENT)
    .fetch_all(db.pool())
    .await
    .unwrap()
    .into_iter()
    .map(|payload| serde_json::from_str(&payload).unwrap())
    .collect()
}

#[tokio::test]
async fn three_distinct_tasks_detect_once_with_most_recent_first() {
    let db = database().await;
    for (task, ago) in [("task-1", 30), ("task-2", 20), ("task-3", 10)] {
        handoff(
            &db,
            "project-1",
            task,
            &["src/shared.rs", "src/shared.rs"],
            &timestamp(ago),
        )
        .await;
    }
    run(&db).await;
    run(&db).await;
    let events = detections(&db).await;
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0],
        json!({"project_id": "project-1", "path": "src/shared.rs",
        "task_ids": ["task-3", "task-2", "task-1"], "handoff_count": 3, "window_days": 7})
    );
}

#[tokio::test]
async fn another_handoff_refreshes_the_same_path_without_duplicate_replay() {
    let db = database().await;
    for task in ["one", "two", "three", "four"] {
        handoff(&db, "project-1", task, &["shared.rs"], &timestamp(10)).await;
    }
    run(&db).await;
    run(&db).await;
    let events = detections(&db).await;
    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["handoff_count"], 3);
    assert_eq!(events[1]["handoff_count"], 4);
}

#[tokio::test]
async fn repeat_handoffs_and_two_tasks_do_not_detect() {
    for tasks in [
        ["task-1", "task-1", "task-1"],
        ["task-1", "task-2", "task-2"],
    ] {
        let db = database().await;
        for task in tasks {
            handoff(&db, "project-1", task, &["shared.rs"], &timestamp(10)).await;
        }
        run(&db).await;
        assert!(detections(&db).await.is_empty());
    }
}

#[tokio::test]
async fn old_handoff_is_excluded_and_projects_are_independent() {
    let db = database().await;
    handoff(
        &db,
        "project-1",
        "old",
        &["shared.rs"],
        &timestamp(8 * 86400),
    )
    .await;
    for project in ["project-1", "project-2"] {
        for task in ["one", "two"] {
            handoff(&db, project, task, &["shared.rs"], &timestamp(10)).await;
        }
    }
    run(&db).await;
    assert!(detections(&db).await.is_empty());
    handoff(&db, "project-1", "three", &["shared.rs"], &timestamp(5)).await;
    run(&db).await;
    assert_eq!(detections(&db).await.len(), 1);
    handoff(&db, "project-2", "three", &["shared.rs"], &timestamp(5)).await;
    run(&db).await;
    let events = detections(&db).await;
    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["project_id"], "project-1");
    assert_eq!(events[1]["project_id"], "project-2");
}

#[tokio::test]
async fn lockfiles_are_excluded_by_basename() {
    let db = database().await;
    let paths: Vec<String> = UNSPLITTABLE_LOCKFILES
        .iter()
        .map(|name| format!("nested/{name}"))
        .collect();
    let paths: Vec<&str> = paths.iter().map(String::as_str).collect();
    for task in ["one", "two", "three"] {
        handoff(&db, "project-1", task, &paths, &timestamp(10)).await;
    }
    run(&db).await;
    assert!(detections(&db).await.is_empty());
}

#[tokio::test]
async fn punctuation_and_unicode_paths_preserve_json_and_last_prefix() {
    let db = database().await;
    let path = "src/日本語 ; punctuation, \"quoted\".rs";
    for task in ["one", "two", "three"] {
        let mut event = input("project-1", task, &[path], &timestamp(10));
        let mut payload: Value = serde_json::from_str(&event.payload_json).unwrap();
        payload["trigger_reason"] = json!(format!("{CONFLICT_HANDOFF_MARKER} earlier{CONFLICT_HANDOFF_PATHS_PREFIX}discarded{CONFLICT_HANDOFF_PATHS_PREFIX}{}", json!([path])));
        event.payload_json = payload.to_string();
        db.append_event(event).await.unwrap();
    }
    run(&db).await;
    assert_eq!(detections(&db).await[0]["path"], path);
    assert_ne!(incident_key("p", path), incident_key("p", "other"));
    assert_ne!(incident_key("p", path), incident_key("q", path));
    assert!(incident_key("p", &"x".repeat(10000)).len() < 128);
}

#[tokio::test]
async fn non_workflow_actor_and_different_states_are_ignored() {
    let db = database().await;
    let consumer = ConflictHotspotConsumer::new(Arc::clone(&db));
    for change in ["actor", "from", "to", "marker", "json"] {
        let mut event = input("project-1", "task", &["shared.rs"], &timestamp(10));
        let mut payload: Value = serde_json::from_str(&event.payload_json).unwrap();
        match change {
            "actor" => {
                event.actor_type = "agent".into();
                event.actor_id = Some("workflow".into());
            }
            "from" => payload["from_state"] = json!("review"),
            "to" => payload["to_state"] = json!("done"),
            "marker" => {
                payload["trigger_reason"] = json!("plain merge failure; paths_json=[\"shared.rs\"]")
            }
            "json" => payload["trigger_reason"] = json!("[conflict-handoff]; paths_json=broken"),
            _ => unreachable!(),
        }
        event.payload_json = payload.to_string();
        let event = db.append_event(event).await.unwrap();
        assert!(
            matches!(consumer.handle(&event).await.unwrap(), Outcome::Skip),
            "{change}"
        );
    }
    run(&db).await;
    assert!(detections(&db).await.is_empty());
}

#[tokio::test]
async fn rollback_after_effect_before_cursor_commit_replays_without_duplicates() {
    let db = database().await;
    for task in ["one", "two", "three"] {
        handoff(&db, "project-1", task, &["shared.rs"], &timestamp(10)).await;
    }
    let third: i64 = sqlx::query_scalar("SELECT MAX(sequence) FROM domain_event")
        .fetch_one(db.pool())
        .await
        .unwrap();
    sqlx::raw_sql(&format!("CREATE TRIGGER crash_at_cursor BEFORE UPDATE ON event_consumer_cursor WHEN NEW.consumer_name = 'conflict-hotspots' AND NEW.last_sequence = {third} BEGIN SELECT RAISE(ABORT, 'crash before commit'); END;"))
        .execute(db.pool()).await.unwrap();
    let worker = Arc::new(ConflictHotspotConsumer::new(Arc::clone(&db)));
    assert!(WorkerRuntime::new(Arc::clone(&db), worker)
        .run_once(100)
        .await
        .is_err());
    assert!(detections(&db).await.is_empty());
    let cursor = db
        .get_consumer_cursor(CONSUMER_NAME)
        .await
        .unwrap()
        .unwrap();
    assert!(cursor.last_sequence < third);
    sqlx::query("DROP TRIGGER crash_at_cursor")
        .execute(db.pool())
        .await
        .unwrap();
    run(&db).await;
    run(&db).await;
    assert_eq!(detections(&db).await.len(), 1);
    assert!(
        db.get_consumer_cursor(CONSUMER_NAME)
            .await
            .unwrap()
            .unwrap()
            .last_sequence
            >= third
    );
}

#[tokio::test]
async fn repeated_commit_failure_dead_letters_and_advances_atomically() {
    let db = database().await;
    for task in ["one", "two", "three"] {
        handoff(&db, "project-1", task, &["shared.rs"], &timestamp(10)).await;
    }
    sqlx::raw_sql("CREATE TRIGGER reject_detection BEFORE INSERT ON domain_event WHEN NEW.event_type = 'project.conflict_hotspot.detected' BEGIN SELECT RAISE(ABORT, 'injected detection failure'); END;")
        .execute(db.pool()).await.unwrap();
    for _ in 0..8 {
        sqlx::query("UPDATE worker_health SET retry_not_before = '2000-01-01T00:00:00Z'")
            .execute(db.pool())
            .await
            .unwrap();
        run(&db).await;
    }
    assert!(detections(&db).await.is_empty());
    let letters: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM worker_dead_letter WHERE worker_name = 'conflict-hotspots'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(letters, 1);
    assert_eq!(
        db.get_consumer_cursor(CONSUMER_NAME)
            .await
            .unwrap()
            .unwrap()
            .last_sequence,
        sqlx::query_scalar::<_, i64>("SELECT MAX(sequence) FROM domain_event")
            .fetch_one(db.pool())
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn redetection_after_resolution_counts_only_newer_handoffs() {
    let db = database().await;
    let now = now_rfc3339();
    sqlx::query("INSERT INTO project (id, name, settings, workflow_definition, created_at, updated_at) VALUES ('project-1', 'hotspots', '{}', '{}', ?, ?)")
        .bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    for task in ["one", "two", "three"] {
        handoff(&db, "project-1", task, &["shared.rs"], &timestamp(120)).await;
    }
    run(&db).await;
    let attention = AttentionService::new(Arc::clone(&db));
    attention.project_once(100).await.unwrap();
    sqlx::query("UPDATE attention_projection SET status = 'resolved', resolved_at = ?, version = version + 1 WHERE attention_type = 'conflict_hotspot'")
        .bind(timestamp(60)).execute(db.pool()).await.unwrap();
    for task in ["new-one", "new-two"] {
        handoff(&db, "project-1", task, &["shared.rs"], &timestamp(30)).await;
    }
    run(&db).await;
    assert_eq!(detections(&db).await.len(), 1);
    handoff(
        &db,
        "project-1",
        "new-three",
        &["shared.rs"],
        &timestamp(20),
    )
    .await;
    run(&db).await;
    assert_eq!(detections(&db).await.len(), 2);
    attention.project_once(100).await.unwrap();
    handoff(&db, "project-1", "new-four", &["shared.rs"], &timestamp(10)).await;
    run(&db).await;
    let events = detections(&db).await;
    assert_eq!(events.len(), 3);
    assert_eq!(events[2]["handoff_count"], 4);
    let status: String = sqlx::query_scalar(
        "SELECT status FROM attention_projection WHERE attention_type = 'conflict_hotspot'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(status, "open");
}

#[tokio::test]
async fn query_uses_existing_type_sequence_index_and_caps_ids_without_capping_count() {
    let db = database().await;
    let now = timestamp(1);
    let mut last = None;
    for number in 0..12 {
        last = Some(
            handoff(
                &db,
                "project-1",
                &format!("task-{number}"),
                &["shared.rs"],
                &now,
            )
            .await,
        );
    }
    let event = last.unwrap();
    let worker = ConflictHotspotConsumer::new(Arc::clone(&db));
    let Outcome::Done(prepared) = worker.handle(&event).await.unwrap() else {
        panic!("handoff");
    };
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    let plan = sqlx::query(&format!("EXPLAIN QUERY PLAN {HANDOFF_QUERY}"))
        .bind("project-1")
        .bind(&now)
        .bind(WINDOW_DAYS)
        .bind(&now)
        .bind(None::<String>)
        .bind(event.sequence)
        .fetch_all(&mut *tx)
        .await
        .unwrap();
    assert!(plan.iter().any(|row| row
        .get::<String, _>("detail")
        .contains("USING INDEX idx_domain_event_type_sequence")));
    worker.commit(&mut tx, &event, &prepared).await.unwrap();
    tx.commit().await.unwrap();
    let detected = detections(&db).await;
    assert_eq!(detected[0]["handoff_count"], 12);
    assert_eq!(
        detected[0]["task_ids"].as_array().unwrap().len(),
        MAX_TASK_IDS
    );
    assert_eq!(detected[0]["task_ids"][0], "task-11");
}
