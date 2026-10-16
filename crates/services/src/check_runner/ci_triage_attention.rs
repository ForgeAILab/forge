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
                &json!([entry.command, triage.base_commit]),
            )
            .expect("serializable key");
            incident(db, task, &event, "base_check_failing", &format!("ci-base:{}:{digest}", task.project_id), &entry.command,
                json!({"base_commit":triage.base_commit,"base_run_id":triage.base_run_id,"base_failures":base.and_then(|base| base.commands.get(setup_count + entry.index)).map(|step| &step.failures),"triage":triage}), &application.consumer_id).await?;
        }
    }
    // A newer passing base observation resolves this command's older base
    // incidents, including failures at an older target commit. Cached old
    // evidence cannot resolve a newer incident.
    if let Some(base) = base {
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
                db.resolve_passing_base_attention(
                    &task.project_id,
                    &step.command,
                    &base.created_at,
                    &event.id,
                )
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
        db.resolve_passing_base_attention("p", "test", "2026-10-15T11:00:00Z", &event.id)
            .await
            .unwrap();
        let status: String =
            sqlx::query_scalar("SELECT status FROM attention_projection WHERE dedupe_key='base'")
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(status, "open");
        db.resolve_passing_base_attention("p", "test", "2026-10-15T13:00:00Z", &event.id)
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
