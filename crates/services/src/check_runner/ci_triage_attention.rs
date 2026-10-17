//! Project-level CI incidents, separate from the Task's blocking findings.
use super::consumer::CheckApplication;
use crate::Result;
use api_types::{CiBaseStatus, CiFailureClass, StepResultEntry};
use db::{DomainEventRepo, SqliteDb, StoredCheckResult};
use serde_json::json;

pub(super) async fn record(
    db: &SqliteDb,
    task: &db::Task,
    application: &CheckApplication,
    entries: &[StepResultEntry],
    base: Option<&StoredCheckResult>,
    setup_count: usize,
    target_tip: bool,
) -> Result<()> {
    let now = db::now_rfc3339();
    let summary: Vec<_> =
        entries
            .iter()
            .filter_map(|entry| {
                entry.triage.as_ref().map(|triage| json!({
        "index":entry.index,"blocking":triage.blocking,"base_status":triage.base_status,
        "rerun_run_id":triage.rerun_run_id,"base_run_id":triage.base_run_id,
    }))
            })
            .collect();
    let event = DomainEventRepo::append_event(
        db,
        db::CreateDomainEvent {
            id: db::new_uuid_v4(),
            event_type: "check.ci_triaged".into(),
            entity_type: "check_run".into(),
            entity_id: application.run.id.clone(),
            actor_type: "system".into(),
            actor_id: Some("ci-triage".into()),
            scope_type: "project".into(),
            scope_id: task.project_id.clone(),
            correlation_id: application.run.id.clone(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(format!("ci-triage:{}", application.consumer_id)),
            payload_json: json!({"task_id":task.id,"run_id":application.run.id,"ci_steps":summary})
                .to_string(),
            created_at: now.clone(),
        },
    )
    .await?;
    for entry in entries {
        let Some(triage) = &entry.triage else {
            continue;
        };
        let mut flaky: Vec<_> = triage
            .failures
            .iter()
            .filter(|f| f.class == CiFailureClass::Flaky)
            .map(|f| f.signature.clone())
            .collect();
        if flaky.is_empty() && triage.class == Some(CiFailureClass::Flaky) {
            flaky.push(String::new());
        }
        for signature in flaky {
            let digest = api_types::canonical_digest_with_schema(
                "forge.flaky-check/1",
                &json!([entry.command, signature]),
            )
            .expect("serializable key");
            incident(
                db,
                task,
                &event,
                "flaky_check",
                &format!("ci-flaky:{}:{digest}", task.project_id),
                &entry.command,
                json!({"signature":signature,"run_id":application.run.id,"triage":triage}),
                &application.consumer_id,
            )
            .await?;
        }
        let base_failed = triage
            .failures
            .iter()
            .any(|f| f.class == CiFailureClass::PreExisting)
            || triage.class == Some(CiFailureClass::PreExistingUnverified)
            || base
                .and_then(|base| base.commands.get(setup_count + entry.index))
                .is_some_and(|step| step.exit_code != 0)
            || matches!(
                triage.base_status,
                CiBaseStatus::BaseUnavailable | CiBaseStatus::BaseRunInvalid
            );
        if base_failed {
            let digest = api_types::canonical_digest_with_schema(
                "forge.base-check/1",
                &json!([entry.command]),
            )
            .expect("serializable key");
            incident(db, task, &event, "base_check_failing", &format!("ci-base:{}:{digest}", task.project_id), &entry.command,
                json!({"base_commit":triage.base_commit,"base_run_id":triage.base_run_id,"base_observed_at":base.map_or(event.created_at.as_str(), |base| base.created_at.as_str()),"base_failures":base.and_then(|base| base.commands.get(setup_count + entry.index)).map(|step| &step.failures),"triage":triage}), &application.consumer_id).await?;
        }
    }
    resolve_base_observation(
        db,
        &task.project_id,
        base,
        setup_count,
        &event.id,
        target_tip,
    )
    .await
}

/// Only a queue-head observation sees the Project target tip. Review entry
/// compares a Task's own (possibly old) merge base, even if it runs later.
async fn resolve_base_observation(
    db: &SqliteDb,
    project: &str,
    base: Option<&StoredCheckResult>,
    setup_count: usize,
    event: &str,
    target_tip: bool,
) -> Result<()> {
    if let Some(base) = base.filter(|_| target_tip) {
        if base
            .commands
            .iter()
            .take(setup_count)
            .all(|step| step.exit_code == 0)
        {
            for step in base
                .commands
                .iter()
                .skip(setup_count)
                .filter(|step| step.exit_code == 0)
            {
                db.resolve_passing_base_attention(project, &step.command, &base.created_at, event)
                    .await?;
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn incident(
    db: &SqliteDb,
    task: &db::Task,
    event: &db::DomainEvent,
    category: &str,
    key: &str,
    command: &str,
    mut details: serde_json::Value,
    occurrence: &str,
) -> Result<()> {
    details["command"] = json!(command);
    details["task"] = json!({"id":task.id,"title":task.title});
    let summary = match category {
        "flaky_check" => format!("Flaky CI check: {command}"),
        _ => format!("Base CI needs attention: {command}"),
    };
    db.record_ci_triage_attention(
        db::CreateAttentionProjection {
            id: db::new_uuid_v4(),
            attention_type: category.into(),
            scope_type: "project".into(),
            scope_id: task.project_id.clone(),
            identity_id: None,
            source_event_id: event.id.clone(),
            priority: 60,
            status: "open".into(),
            summary,
            details_json: details.to_string(),
            dedupe_key: key.into(),
            occurred_at: event.created_at.clone(),
            updated_at: event.created_at.clone(),
            acknowledged_at: None,
            snoozed_until: None,
            resolved_at: None,
            updated_by_user_id: None,
            recommended_action: "inspect_ci".into(),
            source_sequence: Some(event.sequence),
        },
        occurrence,
        &task.id,
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn ci_triage_attention_counts_once_and_resolves_only_on_newer_passing_base() {
        let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let db = SqliteDb::new(pool);
        let at = "2026-10-15T12:00:00Z";
        sqlx::query("INSERT INTO project(id,name,settings,workflow_definition,created_at,updated_at) VALUES('p','p','{}','{}',?,?)").bind(at).bind(at).execute(db.pool()).await.unwrap();
        let event = DomainEventRepo::append_event(
            &db,
            db::CreateDomainEvent {
                id: db::new_uuid_v4(),
                event_type: "check.ci_triaged".into(),
                entity_type: "check_run".into(),
                entity_id: "run".into(),
                actor_type: "system".into(),
                actor_id: None,
                scope_type: "project".into(),
                scope_id: "p".into(),
                correlation_id: "run".into(),
                causation_id: None,
                causation_depth: 0,
                dedupe_key: None,
                payload_json: "{}".into(),
                created_at: at.into(),
            },
        )
        .await
        .unwrap();
        // Exercise the repository's atomic aggregation directly: no Task mutation.
        let projection = db::CreateAttentionProjection {
            id: db::new_uuid_v4(),
            attention_type: "base_check_failing".into(),
            scope_type: "project".into(),
            scope_id: "p".into(),
            identity_id: None,
            source_event_id: event.id.clone(),
            priority: 60,
            status: "open".into(),
            summary: "base failed".into(),
            details_json: json!({"command":"test"}).to_string(),
            dedupe_key: "base".into(),
            occurred_at: at.into(),
            updated_at: at.into(),
            acknowledged_at: None,
            snoozed_until: None,
            resolved_at: None,
            updated_by_user_id: None,
            recommended_action: "inspect_ci".into(),
            source_sequence: Some(event.sequence),
        };
        for (run, task) in [("one", "t1"), ("one", "t1"), ("two", "t2")] {
            db.record_ci_triage_attention(projection.clone(), run, task)
                .await
                .unwrap();
        }
        let raw: String = sqlx::query_scalar(
            "SELECT details_json FROM attention_projection WHERE dedupe_key='base'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        let details: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(details["count"], 2);
        assert_eq!(details["task_ids"], json!(["t1", "t2"]));
        // A recurring flake keeps counting; the stored lists stay bounded.
        let flaky = db::CreateAttentionProjection {
            id: db::new_uuid_v4(),
            attention_type: "flaky_check".into(),
            dedupe_key: "flaky".into(),
            ..projection.clone()
        };
        for occurrence in 0..100 {
            db.record_ci_triage_attention(
                flaky.clone(),
                &format!("run-{occurrence}"),
                &format!("task-{}", occurrence % 70),
            )
            .await
            .unwrap();
        }
        let raw: String = sqlx::query_scalar(
            "SELECT details_json FROM attention_projection WHERE dedupe_key='flaky'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        let recurring: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(recurring["count"], 100);
        assert_eq!(recurring["occurrences"].as_array().unwrap().len(), 64);
        assert_eq!(recurring["occurrences"][63], "run-99");
        assert_eq!(recurring["task_ids"].as_array().unwrap().len(), 64);
        assert_eq!(recurring["task_ids"][63], "task-29");
        let passing_old_merge_base = StoredCheckResult {
            id: "passing-old-base".into(),
            run_id: "base".into(),
            identity_key: "base".into(),
            outcome: db::CheckResultOutcome::Pass,
            cleanup: db::CheckCleanup::NotPerformed,
            certified: false,
            cacheable: false,
            commands: vec![api_types::CheckCommandOutcome {
                index: 0,
                command: "test".into(),
                exit_code: 0,
                stderr_tail: String::new(),
                output_tail: String::new(),
                failures: vec![],
                failures_truncated: false,
                started_at: at.into(),
                finished_at: at.into(),
            }],
            output_truncated: false,
            created_at: "2026-10-15T13:00:00Z".into(),
        };
        resolve_base_observation(&db, "p", Some(&passing_old_merge_base), 0, &event.id, false)
            .await
            .unwrap();
        let status: String = sqlx::query_scalar(
            "SELECT status FROM attention_projection WHERE attention_type='base_check_failing'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(
            status, "open",
            "a later green Task merge-base cannot resolve a red target-tip incident"
        );
        db.resolve_passing_base_attention("p", "test", "2026-10-15T11:00:00Z", &event.id)
            .await
            .unwrap();
        let status: String =
            sqlx::query_scalar("SELECT status FROM attention_projection WHERE dedupe_key='base'")
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(status, "open");
        resolve_base_observation(&db, "p", Some(&passing_old_merge_base), 0, &event.id, true)
            .await
            .unwrap();
        let status: String =
            sqlx::query_scalar("SELECT status FROM attention_projection WHERE dedupe_key='base'")
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(status, "resolved");
    }

    #[tokio::test]
    async fn ci_triage_attention_cached_red_cannot_regress_or_reopen_a_newer_base() {
        let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let db = SqliteDb::new(pool);
        let at = "2026-10-15T12:00:00Z";
        sqlx::query("INSERT INTO project(id,name,settings,workflow_definition,created_at,updated_at) VALUES('p','p','{}','{}',?,?)").bind(at).bind(at).execute(db.pool()).await.unwrap();
        let event = DomainEventRepo::append_event(
            &db,
            db::CreateDomainEvent {
                id: db::new_uuid_v4(),
                event_type: "check.ci_triaged".into(),
                entity_type: "check_run".into(),
                entity_id: "run".into(),
                actor_type: "system".into(),
                actor_id: None,
                scope_type: "project".into(),
                scope_id: "p".into(),
                correlation_id: "run".into(),
                causation_id: None,
                causation_depth: 0,
                dedupe_key: None,
                payload_json: "{}".into(),
                created_at: at.into(),
            },
        )
        .await
        .unwrap();
        let projection = |commit: &str, observed: &str| db::CreateAttentionProjection {
            id: db::new_uuid_v4(),
            attention_type: "base_check_failing".into(),
            scope_type: "project".into(),
            scope_id: "p".into(),
            identity_id: None,
            source_event_id: event.id.clone(),
            priority: 60,
            status: "open".into(),
            summary: "base red".into(),
            details_json:
                json!({"command":"test","base_commit":commit,"base_observed_at":observed})
                    .to_string(),
            dedupe_key: "base".into(),
            occurred_at: at.into(),
            updated_at: at.into(),
            acknowledged_at: None,
            snoozed_until: None,
            resolved_at: None,
            updated_by_user_id: None,
            recommended_action: "inspect_ci".into(),
            source_sequence: Some(event.sequence),
        };
        // Two open per-commit items from an older build converge on the
        // newest row without deleting either historical observation.
        let mut tx = db::begin_immediate(db.pool()).await.unwrap();
        for (key, at) in [
            ("legacy-base", "2026-10-15T11:00:00Z"),
            ("base", "2026-10-15T12:00:00Z"),
        ] {
            let mut legacy = projection(key, at);
            legacy.dedupe_key = key.into();
            legacy.occurred_at = at.into();
            db.insert_attention_in_tx(&mut tx, legacy).await.unwrap();
        }
        tx.commit().await.unwrap();
        db.record_ci_triage_attention(projection("new", "2026-10-15T14:00:00Z"), "new", "task")
            .await
            .unwrap();
        let (rows, open): (i64, i64) = sqlx::query_as("SELECT COUNT(*),SUM(status='open') FROM attention_projection WHERE attention_type='base_check_failing'").fetch_one(db.pool()).await.unwrap();
        assert_eq!((rows, open), (2, 1));

        db.record_ci_triage_attention(projection("old", "2026-10-15T13:00:00Z"), "old", "task")
            .await
            .unwrap();
        let details: String = sqlx::query_scalar(
            "SELECT details_json FROM attention_projection WHERE dedupe_key='base'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&details).unwrap()["base_commit"],
            "new"
        );
        db.resolve_passing_base_attention("p", "test", "2026-10-15T13:30:00Z", &event.id)
            .await
            .unwrap();
        let status: String =
            sqlx::query_scalar("SELECT status FROM attention_projection WHERE dedupe_key='base'")
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(status, "open");
        db.resolve_passing_base_attention("p", "test", "2026-10-15T15:00:00Z", &event.id)
            .await
            .unwrap();
        db.record_ci_triage_attention(
            projection("cached", "2026-10-15T14:30:00Z"),
            "cached",
            "task",
        )
        .await
        .unwrap();
        let status: String =
            sqlx::query_scalar("SELECT status FROM attention_projection WHERE dedupe_key='base'")
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(status, "resolved");
    }
}
