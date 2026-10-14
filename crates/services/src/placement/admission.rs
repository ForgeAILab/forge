//! Reservation lifecycle helpers. Owner I/O never runs under the writer lock.

use db::{
    AttentionRepo, DomainEventRepo, PlacementFailureCause, PlacementState, SqliteDb, TaskRepo,
    UpdateWorkspacePlacement, WorkspacePlacement, WorkspacePlacementRepo,
};
use serde_json::json;
use sqlx::{Sqlite, Transaction};

use crate::Result;

pub(crate) fn placement_update(placement: &WorkspacePlacement) -> UpdateWorkspacePlacement {
    UpdateWorkspacePlacement {
        id: placement.id.clone(),
        expected_version: placement.version,
        agent_id: None,
        owner_kind: None,
        daemon_id: None,
        runtime_id: None,
        repo_location_id: None,
        execution_daemon_id: None,
        workspace_handle: None,
        generation: None,
        state: None,
        selected_by: None,
        selection_reason: None,
        reserved_until: None,
        disconnected_at: None,
        failure_cause: None,
        updated_at: db::now_rfc3339(),
    }
}

pub async fn sweep_expired_reservations(db: &SqliteDb, now: &str) -> Result<u64> {
    let expired: bool = db.workspace_reservations_expired(now).await?;
    if !expired {
        return Ok(0);
    }
    let mut transaction = db::begin_immediate(db.pool()).await?;
    let expired = sweep_expired_reservations_in_tx(&mut transaction, now).await?;
    transaction.commit().await?;
    Ok(expired)
}

pub(crate) async fn sweep_expired_reservations_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    now: &str,
) -> Result<u64> {
    let rows = db::expired_workspace_reservations_in_tx(transaction, now).await?;
    let mut expired = 0;
    for row in rows {
        let id: String = row.id()?;
        let version: i64 = row.version()?;
        let result =
            db::fail_expired_workspace_reservation_in_tx(transaction, now, id, version).await?;
        if result.rows_affected() != 1 {
            return Err(db::DbError::VersionConflict.into());
        }
        expired += result.rows_affected();
    }
    Ok(expired)
}

pub(crate) async fn fail_preparation(
    db: &SqliteDb,
    placement: &WorkspacePlacement,
    cause: PlacementFailureCause,
) -> Result<()> {
    let mut update = placement_update(placement);
    update.state = Some(PlacementState::Failed);
    update.reserved_until = Some(None);
    update.failure_cause = Some(Some(cause));
    WorkspacePlacementRepo::update(db, update).await?;
    Ok(())
}

pub(crate) async fn record_fence_rejection(
    db: &SqliteDb,
    placement: &WorkspacePlacement,
    cause: &PlacementFailureCause,
    message: &str,
) -> Result<()> {
    let Some(task) = TaskRepo::get_by_id(db, &placement.task_id, false).await? else {
        return Ok(());
    };
    let now = db::now_rfc3339();
    let event = DomainEventRepo::append_event(
        db,
        db::CreateDomainEvent {
            id: db::new_uuid_v4(),
            event_type: "workspace.fence_rejected".to_owned(),
            entity_type: "workspace_placement".to_owned(),
            entity_id: placement.id.clone(),
            actor_type: "system".to_owned(),
            actor_id: Some("workspace-admission".to_owned()),
            scope_type: "project".to_owned(),
            scope_id: task.project_id.clone(),
            correlation_id: placement.id.clone(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(format!(
                "workspace-fence:{}:{}:{}:{cause}",
                placement.id, placement.generation, placement.version
            )),
            payload_json: json!({
                "task_id": task.id, "placement_id": placement.id,
                "generation": placement.generation, "failure_cause": cause.to_string(),
            })
            .to_string(),
            created_at: now.clone(),
        },
    )
    .await?;
    AttentionRepo::insert_attention(
        db,
        db::CreateAttentionProjection {
            id: db::new_uuid_v4(),
            attention_type: "execution_failed".to_owned(),
            scope_type: "project".to_owned(),
            scope_id: task.project_id,
            identity_id: placement.agent_id.clone(),
            source_event_id: event.id,
            priority: 80,
            status: "open".to_owned(),
            summary: format!("Workspace owner rejected a server fence: {cause}"),
            details_json: json!({
                "task": {"id": task.id, "title": task.title},
                "placement_id": placement.id, "daemon_id": placement.daemon_id,
                "generation": placement.generation, "failure_cause": cause.to_string(),
                "error": message,
            })
            .to_string(),
            dedupe_key: format!(
                "workspace-fence:{}:{}:{cause}",
                placement.id, placement.generation
            ),
            occurred_at: event.created_at,
            updated_at: now,
            acknowledged_at: None,
            snoozed_until: None,
            resolved_at: None,
            updated_by_user_id: None,
            recommended_action: "inspect".to_owned(),
            source_sequence: Some(event.sequence),
        },
    )
    .await?;
    Ok(())
}

pub(crate) async fn resolve_workspace_attention_in_tx(
    db: &SqliteDb,
    tx: &mut Transaction<'_, Sqlite>,
    task_id: &str,
) -> Result<()> {
    let now = db::now_rfc3339();
    // Existing successful admission and move-on paths already call this resolver.
    // Clear only the machine wait's own deferral, preserving unrelated blockers.
    // Clears only its own marker, so a queued clear is identity-fenced and
    // applies even if the Task changes status first.
    let _queued_or_applied = db
        .clear_workspace_admission_environment_wait_in_tx(tx, task_id)
        .await?;
    db.resolve_workspace_admission_attention_in_tx(
        tx,
        db::ResolveWorkspaceAdmissionAttentionInTxInput {
            resolved_at: &now,
            updated_at: &now,
            review_dedupe_key: format!("review-ci:{task_id}"),
            owner_dedupe_key: format!("task-owner-wait:{task_id}"),
            environment_dedupe_key: format!("task-environment-wait:{task_id}"),
            task_id,
        },
    )
    .await?;
    Ok(())
}

pub(crate) async fn resolve_workspace_attention(db: &SqliteDb, task_id: &str) -> Result<()> {
    let mut tx = db::begin_immediate(db.pool()).await?;
    resolve_workspace_attention_in_tx(db, &mut tx, task_id).await?;
    tx.commit().await?;
    Ok(())
}

pub(crate) async fn resolve_review_ci_attention(db: &SqliteDb, task_id: &str) -> Result<()> {
    let now = db::now_rfc3339();
    db.resolve_workspace_review_ci_attention(&now, format!("review-ci:{task_id}"))
        .await?;
    Ok(())
}

pub(crate) async fn record_wait_attention_in_tx(
    db: &SqliteDb,
    tx: &mut Transaction<'_, Sqlite>,
    task: &db::Task,
    kind: &str,
    reason: &str,
    dedupe_key: &str,
) -> Result<()> {
    let now = db::now_rfc3339();
    let event = DomainEventRepo::append_event_in_tx(
        db,
        tx,
        &db::CreateDomainEvent {
            id: db::new_uuid_v4(),
            event_type: format!("task.{kind}"),
            entity_type: "task".into(),
            entity_id: task.id.clone(),
            actor_type: "system".into(),
            actor_id: None,
            scope_type: "project".into(),
            scope_id: task.project_id.clone(),
            correlation_id: task.id.clone(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(format!("{dedupe_key}:{}", task.version)),
            payload_json: json!({"task_id": task.id, "cause": reason}).to_string(),
            created_at: now.clone(),
        },
    )
    .await?;
    db.record_workspace_wait_attention_in_tx(
        tx,
        db::RecordWorkspaceWaitAttentionInTxInput {
            id: db::new_uuid_v4(),
            kind,
            project_id: &task.project_id,
            event_id: &event.id,
            reason,
            details_json: json!({"task": {"id": task.id, "title": task.title}, "cause": reason})
                .to_string(),
            dedupe_key,
            occurred_at: &now,
            updated_at: &now,
            sequence: event.sequence,
        },
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn placement_fence_attention_keeps_home_available_and_resolves() {
        let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let db = std::sync::Arc::new(SqliteDb::new(pool));
        let (task, placement, _) = crate::recovery::tests::daemon_owned_fixture(&db).await;
        record_fence_rejection(
            &db,
            &placement,
            &PlacementFailureCause::StaleGeneration,
            "reset raced claim",
        )
        .await
        .unwrap();
        let feed = crate::AttentionService::new(
            db.clone(),
            std::sync::Arc::new(crate::WakeTurnConsumer::new(db.clone())),
        )
        .mission_control_home("test-user", None, 50)
        .await
        .unwrap();
        let row = feed
            .needs_attention
            .iter()
            .find(|item| item.details["task"]["id"] == task.id)
            .expect("fence rejection is visible");
        assert_eq!(row.category, api_types::AttentionCategory::ExecutionFailed);
        assert_eq!(row.details["failure_cause"], "stale_generation");
        resolve_workspace_attention(&db, &task.id).await.unwrap();
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM attention_projection WHERE status <> 'resolved' AND dedupe_key LIKE 'workspace-fence:%'").fetch_one(db.pool()).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn review_ci_attention_reopen_clears_snooze_and_acknowledgement() {
        let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let db = SqliteDb::new(pool);
        let (task, _, _) = crate::recovery::tests::daemon_owned_fixture(&db).await;
        let key = format!("review-ci:{}", task.id);
        let mut tx = db::begin_immediate(db.pool()).await.unwrap();
        record_wait_attention_in_tx(
            &db,
            &mut tx,
            &task,
            "execution_failed",
            "CI unavailable",
            &key,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        sqlx::query("UPDATE attention_projection SET acknowledged_at = ?, snoozed_until = ? WHERE dedupe_key = ?")
            .bind("2026-10-01T00:00:00Z").bind("2099-01-01T00:00:00Z").bind(&key).execute(db.pool()).await.unwrap();
        resolve_review_ci_attention(&db, &task.id).await.unwrap();
        sqlx::query("UPDATE task SET version = version + 1 WHERE id = ?")
            .bind(&task.id)
            .execute(db.pool())
            .await
            .unwrap();
        let task = TaskRepo::get_by_id(&db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        let mut tx = db::begin_immediate(db.pool()).await.unwrap();
        record_wait_attention_in_tx(
            &db,
            &mut tx,
            &task,
            "execution_failed",
            "CI unavailable again",
            &key,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        let row: (String, Option<String>, Option<String>, Option<String>) = sqlx::query_as("SELECT status, acknowledged_at, snoozed_until, resolved_at FROM attention_projection WHERE dedupe_key = ?")
            .bind(&key).fetch_one(db.pool()).await.unwrap();
        assert_eq!(row, ("open".into(), None, None, None));
    }
    #[tokio::test]
    async fn environment_wait_move_on_resolves_attention_and_exact_marker() {
        let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let db = SqliteDb::new(pool);
        let (task, placement, _) = crate::recovery::tests::daemon_owned_fixture(&db).await;
        let machine = db::EnvironmentMachine::from_placement(&placement);
        use db::ProjectMachineReadinessRepo;
        let environment: api_types::ProjectEnvironment =
            serde_json::from_value(json!({"checks":[{"name":"cargo","command":"true"}]})).unwrap();
        sqlx::query("UPDATE project SET settings=? WHERE id=?")
            .bind(json!({"environment":environment}).to_string())
            .bind(&task.project_id)
            .execute(db.pool())
            .await
            .unwrap();
        let mut row = crate::placement::environment::unknown_record(
            &task.project_id,
            machine.clone(),
            &environment,
        );
        row.status = db::EnvironmentReadinessStatus::NotReady;
        row.failing_checks = vec![db::ReadinessCheckFailure {
            name: "cargo".into(),
            output_tail: "missing".into(),
        }];
        db.put_readiness(row, None).await.unwrap();
        crate::placement::environment::persist_machine_wait(
            &db,
            &task,
            &machine,
            &["cargo".into()],
        )
        .await
        .unwrap();
        crate::test_support::drain_task_steps(&db, &task.id).await;
        let current = db::TaskRepo::get_by_id(&db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            crate::placement::environment::defer_refusal(
                &db,
                &current,
                &crate::ServiceError::DaemonUnavailable {
                    daemon_id: placement.daemon_id.clone().unwrap()
                },
                None,
            )
            .await
            .unwrap(),
            Some(true)
        );
        let metadata = db::TaskMetadata::parse(
            db::TaskRepo::get_by_id(&db, &task.id, false)
                .await
                .unwrap()
                .unwrap()
                .metadata_json
                .as_deref(),
        )
        .unwrap();
        assert_eq!(
            metadata.extra["deferred_dispatch"]["kind"], "environment_not_ready",
            "offline transport retains the environmental wait"
        );
        let failed:i64=sqlx::query_scalar("SELECT count(*) FROM domain_event WHERE entity_id=? AND event_type='task.execution_failed'").bind(&task.id).fetch_one(db.pool()).await.unwrap();
        assert_eq!(failed, 0);
        resolve_workspace_attention(&db, &task.id).await.unwrap();
        crate::test_support::drain_task_steps(&db, &task.id).await;
        let metadata: Option<String> =
            sqlx::query_scalar("SELECT metadata_json FROM task WHERE id=?")
                .bind(&task.id)
                .fetch_one(db.pool())
                .await
                .unwrap();
        let metadata = db::TaskMetadata::parse(metadata.as_deref()).unwrap();
        assert!(!metadata.extra.contains_key("environment_wait"));
        assert!(!metadata.extra.contains_key("deferred_dispatch"));
        let status: String =
            sqlx::query_scalar("SELECT status FROM attention_projection WHERE dedupe_key=?")
                .bind(format!("task-environment-wait:{}", task.id))
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(status, "resolved");
    }
}
