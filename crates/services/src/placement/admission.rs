//! Reservation lifecycle helpers. Owner I/O never runs under the writer lock.

use db::{
    AttentionRepo, DomainEventRepo, PlacementFailureCause, PlacementState, SqliteDb, TaskRepo,
    UpdateWorkspacePlacement, WorkspacePlacement, WorkspacePlacementRepo,
};
use serde_json::json;
use sqlx::{Row, Sqlite, Transaction};

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
    let mut transaction = db::begin_immediate(db.pool()).await?;
    let expired = sweep_expired_reservations_in_tx(&mut transaction, now).await?;
    transaction.commit().await?;
    Ok(expired)
}

pub(crate) async fn sweep_expired_reservations_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    now: &str,
) -> Result<u64> {
    let rows = sqlx::query(
        "SELECT id, version FROM workspace_placement
         WHERE state IN ('reserved', 'preparing')
           AND julianday(COALESCE(reserved_until, datetime(updated_at, '+10 minutes'))) <= julianday(?)",
    )
    .bind(now)
    .fetch_all(&mut **transaction)
    .await?;
    let mut expired = 0;
    for row in rows {
        let id: String = row.try_get("id")?;
        let version: i64 = row.try_get("version")?;
        let result = sqlx::query(
            "UPDATE workspace_placement SET state = 'failed', failure_cause = 'prepare_failed',
                 reserved_until = NULL, workspace_handle = NULL, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ? AND state IN ('reserved', 'preparing')",
        )
        .bind(now)
        .bind(id)
        .bind(version)
        .execute(&mut **transaction)
        .await?;
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
            attention_type: "workspace_fence_rejected".to_owned(),
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
    sqlx::query("INSERT INTO attention_projection (id, attention_type, scope_type, scope_id,
        source_event_id, priority, status, summary, details_json, dedupe_key, occurred_at,
        updated_at, recommended_action, source_sequence)
        VALUES (?, ?, 'project', ?, ?, 80, 'open', ?, ?, ?, ?, ?, 'recover_task', ?)
        ON CONFLICT(dedupe_key) DO UPDATE SET summary = excluded.summary, details_json = excluded.details_json,
        source_event_id = excluded.source_event_id, status = 'open', resolved_at = NULL,
        updated_at = excluded.updated_at, version = attention_projection.version + 1")
        .bind(db::new_uuid_v4()).bind(kind).bind(&task.project_id).bind(&event.id)
        .bind(reason).bind(json!({"task": {"id": task.id, "title": task.title}, "cause": reason}).to_string())
        .bind(dedupe_key).bind(&now).bind(&now).bind(event.sequence).execute(&mut **tx).await?;
    Ok(())
}
