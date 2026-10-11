use super::*;
use crate::{worker_runtime::WorkerRuntime, AttentionService};
use chrono::{Duration, Utc};
use db::{create_sqlite_pool, run_migrations};
use serde_json::Value;

async fn database() -> Arc<SqliteDb> {
    let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
    run_migrations(&pool).await.unwrap();
    let db = Arc::new(SqliteDb::new(pool));
    // Model a consumer installed a month before these historical fixtures.
    // Restore the production immutability guard immediately after setup.
    sqlx::query("DROP TRIGGER event_consumer_cutover_immutable_update")
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE event_consumer_cutover SET created_at = ? WHERE consumer_name = 'conflict-hotspots'")
        .bind(timestamp(30 * 86400)).execute(db.pool()).await.unwrap();
    sqlx::raw_sql("CREATE TRIGGER event_consumer_cutover_immutable_update BEFORE UPDATE ON event_consumer_cutover BEGIN SELECT RAISE(ABORT, 'event consumer cutovers are immutable'); END;")
        .execute(db.pool()).await.unwrap();
    db
}

fn timestamp(seconds_ago: i64) -> String {
    (Utc::now() - Duration::seconds(seconds_ago)).to_rfc3339()
}

fn input(project: &str, task: &str, paths: &[&str], at: &str) -> CreateDomainEvent {
    let id = new_uuid_v4();
    CreateDomainEvent {
        id: id.clone(),
        event_type: "task.transitioned".into(),
        entity_type: "task".into(),
        entity_id: task.into(),
        actor_type: "system".into(),
        actor_id: Some("workflow".into()),
        scope_type: "task".into(),
        scope_id: task.into(),
        correlation_id: id.clone(),
        causation_id: None,
        causation_depth: 0,
        dedupe_key: Some(id.clone()),
        payload_json: json!({"transition_log_id": id, "project_id": project,
            "from_state": "merging", "to_state": "merge_failed", "trigger_name": null,
            "trigger_reason": format!("Conflict handed to worker: {}", paths.join(", ")),
            "bridge_kind": "conflict_handoff", "bridge_payload": {"paths": paths},
            "rejection": false})
        .to_string(),
        created_at: at.into(),
    }
}

async fn append_input(db: &SqliteDb, event: CreateDomainEvent) -> DomainEvent {
    let payload: Value = serde_json::from_str(&event.payload_json).unwrap();
    let project = payload["project_id"].as_str().unwrap();
    sqlx::query("INSERT INTO project (id, name, settings, workflow_definition, created_at, updated_at) VALUES (?, ?, '{}', '{}', ?, ?) ON CONFLICT(id) DO NOTHING")
        .bind(project).bind(project).bind(&event.created_at).bind(&event.created_at).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO task (id, project_id, title, status, created_at, updated_at) VALUES (?, ?, ?, 'merge_failed', ?, ?) ON CONFLICT(id) DO NOTHING")
        .bind(&event.entity_id).bind(project).bind(&event.entity_id).bind(&event.created_at).bind(&event.created_at)
        .execute(db.pool()).await.unwrap();
    let owner: String = sqlx::query_scalar("SELECT project_id FROM task WHERE id = ?")
        .bind(&event.entity_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(owner, project, "fixture Task ids are globally unique");
    let actor = event.actor_id.as_ref().map_or_else(
        || event.actor_type.clone(),
        |id| format!("{}:{id}", event.actor_type),
    );
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO transition_log (id, task_id, from_state, to_state, triggered_by, trigger_reason, created_at, bridge_kind, bridge_payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .bind(payload["transition_log_id"].as_str().unwrap()).bind(&event.entity_id)
        .bind(payload["from_state"].as_str().unwrap()).bind(payload["to_state"].as_str().unwrap())
        .bind(actor).bind(payload["trigger_reason"].as_str().unwrap()).bind(&event.created_at)
        .bind(payload["bridge_kind"].as_str()).bind(if payload["bridge_payload"].is_null() { None } else { Some(payload["bridge_payload"].to_string()) })
        .execute(&mut *tx).await.unwrap();
    let recorded = db.append_event_in_tx(&mut tx, &event).await.unwrap();
    tx.commit().await.unwrap();
    recorded
}

async fn handoff(
    db: &SqliteDb,
    project: &str,
    task: &str,
    paths: &[&str],
    at: &str,
) -> DomainEvent {
    append_input(db, input(project, task, paths, at)).await
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
async fn not_yet_projected_episode_skips_later_handoffs() {
    let db = database().await;
    for task in ["one", "two", "three", "four"] {
        handoff(&db, "project-1", task, &["shared.rs"], &timestamp(10)).await;
    }
    run(&db).await;
    run(&db).await;
    let events = detections(&db).await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["handoff_count"], 3);
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
            handoff(
                &db,
                project,
                &format!("{project}-{task}"),
                &["shared.rs"],
                &timestamp(10),
            )
            .await;
        }
    }
    run(&db).await;
    assert!(detections(&db).await.is_empty());
    handoff(
        &db,
        "project-1",
        "project-1-three",
        &["shared.rs"],
        &timestamp(5),
    )
    .await;
    run(&db).await;
    assert_eq!(detections(&db).await.len(), 1);
    handoff(
        &db,
        "project-2",
        "project-2-three",
        &["shared.rs"],
        &timestamp(5),
    )
    .await;
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
async fn punctuation_and_unicode_paths_preserve_json() {
    let db = database().await;
    let path = "src/日本語 ; punctuation, \"quoted\".rs";
    for task in ["one", "two", "three"] {
        append_input(&db, input("project-1", task, &[path], &timestamp(10))).await;
    }
    run(&db).await;
    assert_eq!(detections(&db).await[0]["path"], path);
    assert_ne!(incident_key("p", path), incident_key("p", "other"));
    assert_ne!(incident_key("p", path), incident_key("q", path));
    let project_id = new_uuid_v4();
    let key = incident_key(&project_id, &"x".repeat(10000));
    assert_eq!(key.len(), 141);
    assert!(format!("{key}:detected:{}", new_uuid_v4()).len() <= 256);
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
                payload["bridge_kind"] = Value::Null;
                payload["trigger_reason"] = json!("[conflict-handoff]; paths_json=[\"shared.rs\"]")
            }
            "json" => payload["bridge_payload"] = json!({"paths":"broken"}),
            _ => unreachable!(),
        }
        event.payload_json = payload.to_string();
        let event = append_input(&db, event).await;
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
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM conflict_hotspot_boundary")
            .fetch_one(db.pool())
            .await
            .unwrap(),
        0
    );
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
        .bind(now_rfc3339()).execute(db.pool()).await.unwrap();
    for task in ["new-one", "new-two"] {
        handoff(&db, "project-1", task, &["shared.rs"], &now_rfc3339()).await;
    }
    run(&db).await;
    assert_eq!(detections(&db).await.len(), 1);
    handoff(
        &db,
        "project-1",
        "new-three",
        &["shared.rs"],
        &now_rfc3339(),
    )
    .await;
    run(&db).await;
    assert_eq!(detections(&db).await.len(), 2);
    handoff(&db, "project-1", "new-four", &["shared.rs"], &now_rfc3339()).await;
    run(&db).await;
    let events = detections(&db).await;
    assert_eq!(events.len(), 2);
    assert_eq!(events[1]["handoff_count"], 3);
    // The still-resolved previous item cannot close the newer, unprojected episode.
    attention.project_once(100).await.unwrap();
    let status: String = sqlx::query_scalar(
        "SELECT status FROM attention_projection WHERE attention_type = 'conflict_hotspot'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(status, "open");
}

#[tokio::test]
async fn query_seeks_project_then_transition_time_and_caps_ids_without_capping_count() {
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
    let plan = query_plan(&mut tx, "project-1", &now).await;
    assert_indexed_plan(&plan);
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

// Regression scenarios copied from the independent 3.9(b) audit.

#[tokio::test]
async fn audit39b_pending_refresh_reopens_resolved_item_and_skips_boundary() {
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
    // A fourth handoff must not enqueue a refresh ahead of the user resolution.
    handoff(&db, "project-1", "four", &["shared.rs"], &timestamp(90)).await;
    run(&db).await;
    assert_eq!(detections(&db).await.len(), 1);
    // The user resolves the open item (same UPDATE shape as AttentionService::resolve:
    // source_event_id unchanged), before the next Attention poll.
    sqlx::query("UPDATE attention_projection SET status = 'resolved', resolved_at = ?, version = version + 1 WHERE attention_type = 'conflict_hotspot'")
        .bind(now_rfc3339()).execute(db.pool()).await.unwrap();
    attention.project_once(100).await.unwrap();
    let (status, resolved_at): (String, Option<String>) = sqlx::query_as(
        "SELECT status, resolved_at FROM attention_projection WHERE attention_type = 'conflict_hotspot'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    // One handoff after the resolution must not raise the path again.
    handoff(&db, "project-1", "five", &["shared.rs"], &now_rfc3339()).await;
    run(&db).await;
    let events = detections(&db).await;
    eprintln!("status after stale refresh = {status}, resolved_at = {resolved_at:?}");
    eprintln!("detections = {}", serde_json::to_string(&events).unwrap());
    let boundaries: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM conflict_hotspot_boundary")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(boundaries, 1);
    let (boundary, open): (Option<String>, Option<String>) =
        sqlx::query_as("SELECT resolved_after, open_since FROM conflict_hotspot_boundary")
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(boundary, resolved_at);
    assert!(open.is_none());
    assert_eq!(
        status, "resolved",
        "a refresh emitted before the resolution reopened the item"
    );
    assert_eq!(
        events.len(),
        1,
        "post-resolution detection counted pre-resolution Tasks: {}",
        events.last().unwrap()
    );
}

async fn bulk_transitions(db: &SqliteDb, count: i64, same_project: bool, old: bool) {
    let at = if old {
        timestamp(20 * 86400)
    } else {
        now_rfc3339()
    };
    let project = if same_project {
        "project-1"
    } else {
        "other-project"
    };
    sqlx::query("INSERT INTO project (id, name, settings, workflow_definition, created_at, updated_at) VALUES (?, ?, '{}', '{}', ?, ?) ON CONFLICT(id) DO NOTHING")
        .bind(project).bind(project).bind(&at).bind(&at).execute(db.pool()).await.unwrap();
    sqlx::query(
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 5000)
        INSERT INTO task (id, project_id, title, created_at, updated_at)
        SELECT 'bulk-task-' || i, ?, 'bulk Task', ?, ? FROM n",
    )
    .bind(project)
    .bind(&at)
    .bind(&at)
    .execute(db.pool())
    .await
    .unwrap();
    // Real merge-failed log rows exercise the state/time index, including old
    // rows of the SAME Project. Their marker can never count in this window.
    sqlx::query("WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ?)
        INSERT INTO transition_log (id, task_id, from_state, to_state, triggered_by, trigger_reason, created_at, bridge_kind, bridge_payload)
        SELECT 'bulk-log-' || i, 'bulk-task-' || (1 + i % 5000), 'merging', 'merge_failed', 'system:workflow',
            '[conflict-handoff]; paths_json=[\"a.rs\",\"b.rs\",\"c.rs\",\"d.rs\",\"e.rs\"]', ?, 'conflict_handoff', json_object('paths',json('[\"a.rs\",\"b.rs\",\"c.rs\",\"d.rs\",\"e.rs\"]')) FROM n")
        .bind(count).bind(&at).execute(db.pool()).await.unwrap();
    // Retain the audit's ledger population so a regression to ledger counting
    // would still take work proportional to the 200k irrelevant rows.
    sqlx::query("WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ?)
        INSERT INTO domain_event (id, event_type, entity_type, entity_id, actor_type, actor_id,
            scope_type, scope_id, correlation_id, payload_json, created_at)
        SELECT 'bulk-event-' || i, 'task.transitioned', 'task', 'bulk-task-' || (1 + i % 5000), 'system', 'workflow',
            'task', 'bulk-task-' || (1 + i % 5000), 'bulk', json_object('project_id', ?,
            'from_state', 'merging', 'to_state', 'merge_failed', 'trigger_reason', 'irrelevant bulk row'), ? FROM n")
        .bind(count).bind(project).bind(&at).execute(db.pool()).await.unwrap();
}

async fn query_plan(tx: &mut Transaction<'_, Sqlite>, project: &str, end: &str) -> Vec<String> {
    let end_time = super::timestamp(end).unwrap();
    sqlx::query(&format!("EXPLAIN QUERY PLAN {HANDOFF_QUERY}"))
        .bind(project)
        .bind(Actor::system(SystemComponent::Workflow).display())
        .bind((end_time - Duration::days(WINDOW_DAYS)).to_rfc3339())
        .bind(end)
        .fetch_all(&mut **tx)
        .await
        .unwrap()
        .iter()
        .map(|row| row.get("detail"))
        .collect()
}

fn assert_indexed_plan(plan: &[String]) {
    assert!(
        plan.iter()
            .any(|step| step.contains("SEARCH t USING") && step.contains("project_id=?")),
        "{plan:?}"
    );
    assert!(
        plan.iter().any(|step| step
            .contains("SEARCH tl USING INDEX idx_transition_log_merge_failed")
            && step.contains("created_at>?")
            && step.contains("created_at<?")),
        "{plan:?}"
    );
    assert!(
        !plan.iter().any(|step| step.starts_with("SCAN ")),
        "{plan:?}"
    );
}

async fn time_one_commit(
    rows: i64,
    same_project: bool,
    old: bool,
) -> (std::time::Duration, Vec<String>) {
    let db = database().await;
    bulk_transitions(&db, rows, same_project, old).await;
    let paths = ["a.rs", "b.rs", "c.rs", "d.rs", "e.rs"];
    for task in ["one", "two"] {
        handoff(&db, "project-1", task, &paths, &timestamp(10)).await;
    }
    let event = handoff(&db, "project-1", "three", &paths, &now_rfc3339()).await;
    let worker = ConflictHotspotConsumer::new(Arc::clone(&db));
    let Outcome::Done(prepared) = worker.handle(&event).await.unwrap() else {
        panic!("handoff");
    };
    let mut samples = Vec::new();
    let mut plan = Vec::new();
    for sample in 0..3 {
        let mut tx = db::begin_immediate(db.pool()).await.unwrap();
        if sample == 0 {
            plan = query_plan(&mut tx, "project-1", &event.created_at).await;
        }
        let started = std::time::Instant::now();
        worker.commit(&mut tx, &event, &prepared).await.unwrap();
        samples.push(started.elapsed());
        if sample == 2 {
            tx.commit().await.unwrap();
        } else {
            tx.rollback().await.unwrap();
        }
    }
    assert_eq!(detections(&db).await.len(), 5);
    assert_indexed_plan(&plan);
    samples.sort();
    (samples[1], plan)
}

#[tokio::test]
async fn audit39b_count_cost_scales_with_ledger_since_install_not_window() {
    for (same_project, old) in [(false, false), (false, true), (true, true)] {
        let (small, plan) = time_one_commit(20_000, same_project, old).await;
        let (large, _) = time_one_commit(200_000, same_project, old).await;
        eprintln!("plan = {plan:?}");
        eprintln!("one handoff, 5 paths, same_project={same_project}, old={old}: 20k={small:?}, 200k={large:?}");
        // The old audit's 25.48/240.31 ms fails this bound; the additive 100 ms
        // allowance absorbs shared-runner scheduling noise in debug CI builds.
        assert!(large <= small * 3 + std::time::Duration::from_millis(100),
            "200k irrelevant rows materially increased write-lock time: small={small:?}, large={large:?}");
    }
}

#[tokio::test]
async fn open_episode_does_not_query_history_after_preparation() {
    let db = database().await;
    for task in ["one", "two", "three"] {
        handoff(&db, "project-1", task, &["shared.rs"], &now_rfc3339()).await;
    }
    run(&db).await;
    let event = handoff(&db, "project-1", "four", &["shared.rs"], &now_rfc3339()).await;
    let worker = ConflictHotspotConsumer::new(Arc::clone(&db));
    let Outcome::Done(prepared) = worker.handle(&event).await.unwrap() else {
        panic!("handoff");
    };
    sqlx::query("ALTER TABLE transition_log RENAME TO unavailable_history")
        .execute(db.pool())
        .await
        .unwrap();
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    worker.commit(&mut tx, &event, &prepared).await.unwrap();
    tx.commit().await.unwrap();
    assert_eq!(detections(&db).await.len(), 1);
}

#[tokio::test]
async fn acknowledged_and_snoozed_episodes_do_not_emit_again() {
    let db = database().await;
    for task in ["one", "two", "three"] {
        handoff(&db, "project-1", task, &["shared.rs"], &now_rfc3339()).await;
    }
    run(&db).await;
    AttentionService::new(Arc::clone(&db))
        .project_once(100)
        .await
        .unwrap();
    sqlx::query("UPDATE attention_projection SET status = 'acknowledged', snoozed_until = ?, version = version + 1 WHERE attention_type = 'conflict_hotspot'")
        .bind((Utc::now() + Duration::days(1)).to_rfc3339()).execute(db.pool()).await.unwrap();
    for task in ["four", "five", "six"] {
        handoff(&db, "project-1", task, &["shared.rs"], &now_rfc3339()).await;
    }
    run(&db).await;
    assert_eq!(detections(&db).await.len(), 1);
}

#[tokio::test]
async fn delayed_handoffs_use_their_own_window_and_install_cutover() {
    let db = database().await;
    let cutover: String = sqlx::query_scalar(
        "SELECT created_at FROM event_consumer_cutover WHERE consumer_name = 'conflict-hotspots'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    let cutover = super::timestamp(&cutover).unwrap();
    for (task, seconds) in [("before-one", -10), ("before-two", -5), ("after-one", 1)] {
        handoff(
            &db,
            "project-1",
            task,
            &["shared.rs"],
            &(cutover + Duration::seconds(seconds)).to_rfc3339(),
        )
        .await;
    }
    run(&db).await;
    assert!(detections(&db).await.is_empty());
    for (task, seconds) in [("after-two", 2), ("after-three", 3)] {
        handoff(
            &db,
            "project-1",
            task,
            &["shared.rs"],
            &(cutover + Duration::seconds(seconds)).to_rfc3339(),
        )
        .await;
    }
    run(&db).await;
    let events = detections(&db).await;
    assert_eq!(events.len(), 1); // Processing happens a month after the handoffs.
    assert_eq!(
        events[0]["task_ids"],
        json!(["after-three", "after-two", "after-one"])
    );
}

#[tokio::test]
async fn full_transition_reason_survives_bounded_ledger_reason() {
    let db = database().await;
    let path = format!("src/{}.rs", "x".repeat(1024));
    for task in ["one", "two", "three"] {
        let mut event = input("project-1", task, &[&path], &now_rfc3339());
        let payload: Value = serde_json::from_str(&event.payload_json).unwrap();
        let recorded = append_input(&db, event.clone()).await;
        let mut bounded = payload;
        bounded["trigger_reason"] = json!(bounded["trigger_reason"]
            .as_str()
            .unwrap()
            .chars()
            .take(512)
            .collect::<String>());
        event.payload_json = bounded.to_string();
        // Model CreateDomainEvent::task_transition's ledger bound, without
        // truncating the authoritative source log's full path list.
        sqlx::query("UPDATE domain_event SET payload_json = ? WHERE id = ?")
            .bind(&event.payload_json)
            .bind(recorded.id)
            .execute(db.pool())
            .await
            .unwrap();
    }
    run(&db).await;
    assert_eq!(detections(&db).await[0]["path"], path);
}

#[tokio::test]
async fn project_delete_cascades_episode_state_through_repository_path() {
    let db = database().await;
    for project in ["project-1", "project-2"] {
        for number in 0..3 {
            handoff(
                &db,
                project,
                &format!("{project}-{number}"),
                &["shared.rs"],
                &now_rfc3339(),
            )
            .await;
        }
    }
    run(&db).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM conflict_hotspot_boundary")
            .fetch_one(db.pool())
            .await
            .unwrap(),
        2
    );
    db::ProjectRepo::delete(&*db, "project-1").await.unwrap();
    let projects: Vec<String> =
        sqlx::query_scalar("SELECT project_id FROM conflict_hotspot_boundary")
            .fetch_all(db.pool())
            .await
            .unwrap();
    assert_eq!(projects, vec!["project-2"]);
}
